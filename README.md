# otel-beacon

A lightweight, self-contained OpenTelemetry backend with built-in storage, entity-based correlation, service topology, and an observability UI.

Accepts OTLP over gRPC (`:4317`) and HTTP (`:4318`). Stores traces, metrics, and logs in SQLite (or ClickHouse). Serves a full-featured UI on `:8080`.

---

## Features

- **OTLP ingest** — gRPC and HTTP receivers, compatible with any OTel SDK or Collector
- **Storage** — SQLite (default, zero-config) or ClickHouse for higher volume
- **Anomaly detection** — MAD, Z-score, and EWMA algorithms on metric streams
- **Entity framework** — automatically discovers services and hosts from resource attributes; builds a live service topology graph
- **Observability UI** — Traces, Metrics, Logs, Anomalies, and Services tabs
  - Jaeger-style waterfall trace view with span attributes
  - Entity-based correlation: related logs and metrics linked by service identity, not just trace ID
  - SVG service map with call counts, error rates, and avg latency per edge
  - Click any service node to inspect its recent spans, metrics, and logs

---

## Quick Start

### Docker

```bash
docker run -d \
  --name otel-beacon \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e DB_DSN=/data/otel.db \
  ghcr.io/mqbui1/otel-beacon:latest
```

Open **http://localhost:8080** for the UI.

### Build from source

```bash
git clone https://github.com/mqbui1/otel-beacon.git
cd otel-beacon
docker build -t otel-beacon .
```

---

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|---|---|---|
| `DB_DRIVER` | `sqlite` | Storage backend: `sqlite` or `clickhouse` |
| `DB_DSN` | `otel.db` | SQLite file path or ClickHouse DSN |
| `ADMIN_ADDR` | `:8080` | UI + query API listen address |
| `HTTP_ADDR` | `:4318` | OTLP/HTTP listen address |
| `GRPC_ADDR` | `:4317` | OTLP/gRPC listen address |
| `AUTH_TOKEN` | _(none)_ | Bearer token for ingest endpoints |
| `RETENTION_DAYS` | `30` | Data retention in days |
| `ANOMALY_ALGO` | `mad` | Anomaly algorithm: `mad`, `zscore`, `ewma` |
| `ANOMALY_THRESHOLD` | `3.5` | Anomaly detection sensitivity |
| `TLS_CERT_FILE` | _(none)_ | Path to TLS certificate |
| `TLS_KEY_FILE` | _(none)_ | Path to TLS private key |

---

## API

### Query endpoints

```
GET /v1/query/spans?trace_id=&name=&service=&from=&to=&limit=
GET /v1/query/metrics?name=&service=&from=&to=&limit=
GET /v1/query/logs?severity=&trace_id=&service=&from=&to=&limit=
GET /v1/query/anomalies?limit=
```

### Entity + topology endpoints

```
GET /v1/entities?type=service          # all discovered services (or hosts)
GET /v1/topology                       # service call graph with error rates
GET /v1/entity/signals?type=service&id=X  # recent spans, metrics, logs for a service
```

All responses are JSON:
```json
{ "data": [...], "count": 42 }
```

Time filters use nanoseconds (`from`, `to`). Service filter uses `service.name` from resource attributes.

---

## Sending data

Point any OTel SDK or Collector at the backend:

```bash
# SDK / app
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 ./your-app

# Collector
exporters:
  otlphttp:
    endpoint: http://localhost:4318
```

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                     otel-beacon                      │
│                                                      │
│  OTLP/gRPC :4317 ──┐                                 │
│  OTLP/HTTP :4318 ──┼──► Storage (async queue)        │
│                    │     ├─ Span worker               │
│                    │     ├─ Metric worker + anomaly   │
│                    │     ├─ Log worker                │
│                    │     ├─ Entity extractor          │
│                    │     └─ Topology worker (2 min)   │
│                    │           │                      │
│                    │     SQLite / ClickHouse          │
│                    │           │                      │
│  Admin :8080 ──────┼──► Query API + UI                │
│                    │     ├─ /v1/query/*               │
│                    │     ├─ /v1/entities              │
│                    │     ├─ /v1/topology              │
│                    │     └─ /v1/entity/signals        │
└──────────────────────────────────────────────────────┘
```

- Single Go binary, ~15 MB Docker image
- SQLite with WAL mode, single writer, batch transactions
- Topology refreshed every 2 minutes via cross-service span JOIN
- Entity registry updated on every span batch flush
