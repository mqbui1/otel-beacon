package server

import (
	"context"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/yourorg/otel-backend/storage"
)

// RegisterGRPCServices registers the three OTLP gRPC services on srv.
func RegisterGRPCServices(srv *grpc.Server, store *storage.Storage, logger *zap.Logger) {
	coltracepb.RegisterTraceServiceServer(srv, &traceServer{store: store, logger: logger})
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsServer{store: store, logger: logger})
	collogspb.RegisterLogsServiceServer(srv, &logsServer{store: store, logger: logger})
}

// AuthInterceptor returns a unary interceptor that enforces a static bearer token.
// Pass token="" to disable.
func AuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return handler(ctx, req)
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		vals := md.Get("authorization")
		if len(vals) == 0 || vals[0] != "Bearer "+token {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(ctx, req)
	}
}

// --- Traces ---

type traceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	store  *storage.Storage
	logger *zap.Logger
}

func (s *traceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	td, err := protoToTraces(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal: %v", err)
	}
	if err := s.store.InsertTraces(ctx, td); err != nil {
		s.logger.Error("insert traces", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "storage: %v", err)
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// --- Metrics ---

type metricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	store  *storage.Storage
	logger *zap.Logger
}

func (s *metricsServer) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	md, err := protoToMetrics(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal: %v", err)
	}
	if err := s.store.InsertMetrics(ctx, md); err != nil {
		s.logger.Error("insert metrics", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "storage: %v", err)
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// --- Logs ---

type logsServer struct {
	collogspb.UnimplementedLogsServiceServer
	store  *storage.Storage
	logger *zap.Logger
}

func (s *logsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	ld, err := protoToLogs(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal: %v", err)
	}
	if err := s.store.InsertLogs(ctx, ld); err != nil {
		s.logger.Error("insert logs", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "storage: %v", err)
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// ---------------------------------------------------------------------------
// Proto → pdata conversion via marshal/unmarshal roundtrip.
// ExportXxxServiceRequest and pdata share the same wire format.
// ---------------------------------------------------------------------------

func protoToTraces(req proto.Message) (ptrace.Traces, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return ptrace.Traces{}, err
	}
	return (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
}

func protoToMetrics(req proto.Message) (pmetric.Metrics, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	return (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
}

func protoToLogs(req proto.Message) (plog.Logs, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return plog.Logs{}, err
	}
	return (&plog.ProtoUnmarshaler{}).UnmarshalLogs(b)
}
