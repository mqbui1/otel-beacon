package server

import (
	"compress/gzip"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

const maxBodyBytes = 4 * 1024 * 1024 // 4 MB

// NewHTTPServer returns the OTLP/HTTP handler.
// Wrap with AuthMiddleware and apply http.MaxBytesHandler in main.go.
func NewHTTPServer(store *storage.Storage, logger *zap.Logger) http.Handler {
	mux := http.NewServeMux()
	s := &httpServer{store: store, logger: logger}
	mux.HandleFunc("/v1/traces", s.handleTraces)
	mux.HandleFunc("/v1/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/logs", s.handleLogs)
	return GzipMiddleware(http.MaxBytesHandler(mux, maxBodyBytes))
}

type httpServer struct {
	store  *storage.Storage
	logger *zap.Logger
}

func (s *httpServer) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	td, err := unmarshalTraces(r.Header.Get("Content-Type"), body)
	if err != nil {
		http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.InsertTraces(r.Context(), td); err != nil {
		s.logger.Error("insert traces", zap.Error(err))
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	writeOK(w)
}

func (s *httpServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	md, err := unmarshalMetrics(r.Header.Get("Content-Type"), body)
	if err != nil {
		http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.InsertMetrics(r.Context(), md); err != nil {
		s.logger.Error("insert metrics", zap.Error(err))
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	writeOK(w)
}

func (s *httpServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ld, err := unmarshalLogs(r.Header.Get("Content-Type"), body)
	if err != nil {
		http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.InsertLogs(r.Context(), ld); err != nil {
		s.logger.Error("insert logs", zap.Error(err))
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	writeOK(w)
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

// GzipMiddleware decompresses request bodies sent with Content-Encoding: gzip.
// The OTel Collector sends gzip by default, so this must wrap all OTLP endpoints.
func GzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			gr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "decompress: "+err.Error(), http.StatusBadRequest)
				return
			}
			defer gr.Close()
			r.Body = io.NopCloser(gr)
			r.Header.Del("Content-Encoding")
		}
		next.ServeHTTP(w, r)
	})
}

// AuthMiddleware enforces a static bearer token if token is non-empty.
// Set AUTH_TOKEN="" to disable authentication.
func AuthMiddleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	expected := "Bearer " + token
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != expected {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{}"))
}

func unmarshalTraces(ct string, b []byte) (ptrace.Traces, error) {
	if ct == "application/json" {
		return (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(b)
	}
	return (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
}

func unmarshalMetrics(ct string, b []byte) (pmetric.Metrics, error) {
	if ct == "application/json" {
		return (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(b)
	}
	return (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
}

func unmarshalLogs(ct string, b []byte) (plog.Logs, error) {
	if ct == "application/json" {
		return (&plog.JSONUnmarshaler{}).UnmarshalLogs(b)
	}
	return (&plog.ProtoUnmarshaler{}).UnmarshalLogs(b)
}
