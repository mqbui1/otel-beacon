package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/yourorg/otel-backend/server"
	"github.com/yourorg/otel-backend/storage"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// ---------------------------------------------------------------------------
	// Storage backend
	// ---------------------------------------------------------------------------
	var backend storage.Backend
	var err error

	switch envOr("DB_DRIVER", "sqlite") {
	case "clickhouse":
		backend, err = storage.NewClickHouseBackend(
			envOr("DB_DSN", "clickhouse://localhost:9000/otel?username=default&password="),
			envInt("RETENTION_DAYS", 30),
		)
	default:
		backend, err = storage.NewSQLiteBackend(envOr("DB_DSN", "otel.db"))
	}
	if err != nil {
		log.Fatalf("backend: %v", err)
	}

	authToken := envOr("AUTH_TOKEN", "")

	store := storage.New(backend,
		storage.WithAlgorithm(storage.AlgoType(envOr("ANOMALY_ALGO", "mad"))),
		storage.WithAnomalyThreshold(envFloat("ANOMALY_THRESHOLD", 3.5)),
		storage.WithRetentionDays(envInt("RETENTION_DAYS", 30)),
		storage.WithOnAnomaly(func(a storage.AnomalyRow) {
			logger.Warn("anomaly detected",
				zap.String("metric", a.MetricName),
				zap.Float64("value", a.Value),
				zap.Float64("score", a.Score),
				zap.String("algorithm", a.Algorithm),
			)
		}),
		storage.WithOnError(func(err error) {
			logger.Error("storage error", zap.Error(err))
		}),
	)
	if err := store.Init(context.Background()); err != nil {
		log.Fatalf("storage init: %v", err)
	}

	// Start async LLM-as-judge evaluation workers (use Bedrock; gracefully
	// degrade to heuristics if AWS credentials are unavailable).
	evalCtx, evalCancel := context.WithCancel(context.Background())
	defer evalCancel()
	server.StartEvalWorker(evalCtx, store, logger)
	server.StartSessionEvalWorker(evalCtx, store, logger)

	// ---------------------------------------------------------------------------
	// TLS (optional — set TLS_CERT_FILE and TLS_KEY_FILE to enable)
	// ---------------------------------------------------------------------------
	var tlsCfg *tls.Config
	certFile, keyFile := envOr("TLS_CERT_FILE", ""), envOr("TLS_KEY_FILE", "")
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalf("load TLS cert: %v", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
		logger.Info("TLS enabled")
	}

	// ---------------------------------------------------------------------------
	// OTLP/HTTP on :4318
	// ---------------------------------------------------------------------------
	httpAddr := envOr("HTTP_ADDR", ":4318")
	otlpHandler := server.AuthMiddleware(authToken, server.NewHTTPServer(store, logger))
	httpSrv := &http.Server{Addr: httpAddr, Handler: otlpHandler, TLSConfig: tlsCfg}
	go func() {
		logger.Info("OTLP/HTTP listening", zap.String("addr", httpAddr))
		var herr error
		if tlsCfg != nil {
			herr = httpSrv.ListenAndServeTLS("", "") // cert already in TLSConfig
		} else {
			herr = httpSrv.ListenAndServe()
		}
		if herr != nil && herr != http.ErrServerClosed {
			logger.Fatal("HTTP error", zap.Error(herr))
		}
	}()

	// ---------------------------------------------------------------------------
	// OTLP/gRPC on :4317
	// ---------------------------------------------------------------------------
	grpcAddr := envOr("GRPC_ADDR", ":4317")
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(4 * 1024 * 1024),
		grpc.UnaryInterceptor(server.AuthInterceptor(authToken)),
	}
	if tlsCfg != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	server.RegisterGRPCServices(grpcSrv, store, logger)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go func() {
		logger.Info("OTLP/gRPC listening", zap.String("addr", grpcAddr))
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Fatal("gRPC error", zap.Error(err))
		}
	}()

	// ---------------------------------------------------------------------------
	// Admin (health + Prometheus) + Query API on :8080
	// ---------------------------------------------------------------------------
	adminAddr := envOr("ADMIN_ADDR", ":8080")
	adminSrv := &http.Server{Addr: adminAddr, Handler: combinedAdmin(store, logger)}
	go func() {
		logger.Info("Admin/Query listening", zap.String("addr", adminAddr))
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("admin error", zap.Error(err))
		}
	}()

	// ---------------------------------------------------------------------------
	// Graceful shutdown — stop servers first, then drain storage queues.
	// ---------------------------------------------------------------------------
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("shutting down")

	grpcSrv.GracefulStop()
	httpSrv.Shutdown(context.Background())
	adminSrv.Shutdown(context.Background())

	if err := store.Close(); err != nil {
		logger.Error("storage close", zap.Error(err))
	}
	logger.Info("shutdown complete")
}

// combinedAdmin merges the admin endpoints and query API onto one mux.
func combinedAdmin(store *storage.Storage, logger *zap.Logger) http.Handler {
	mux := http.NewServeMux()
	// Admin endpoints
	admin := server.NewAdminServer(func() error { return nil })
	mux.Handle("/healthz", admin)
	mux.Handle("/readyz", admin)
	mux.Handle("/metrics", admin)
	// Query API (spans/metrics/logs/anomalies + entities/topology/entity-signals)
	query := server.NewQueryServer(store, logger)
	mux.Handle("/v1/", query)
	// UI — must be last (catches everything not matched above)
	mux.Handle("/", uiHandler())
	return mux
}

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
			return f
		}
	}
	return fallback
}
