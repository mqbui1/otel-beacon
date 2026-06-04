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

## Kubernetes deployment (with kubeletstats + k8s metadata)

This section covers deploying an OTel Collector DaemonSet alongside instrumented apps in k8s so that:
- App spans carry full k8s metadata (`k8s.pod.name`, `k8s.deployment.name`, etc.)
- kubeletstats pod/container metrics are collected and correlated to services via the entity framework

### 1. Inject k8s metadata into app pods

The OTel SDK picks up `OTEL_RESOURCE_ATTRIBUTES` at startup. Use k8s downward API to populate k8s labels — but the env var ordering matters: `$(VAR)` substitution only works if the referenced var is declared earlier in the `env` list.

```yaml
env:
  # Downward API vars MUST come before OTEL_RESOURCE_ATTRIBUTES
  - name: POD_NAME
    valueFrom: {fieldRef: {fieldPath: metadata.name}}
  - name: POD_NAMESPACE
    valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
  - name: POD_UID
    valueFrom: {fieldRef: {fieldPath: metadata.uid}}
  - name: OTEL_RESOURCE_ATTRIBUTES
    value: "deployment.environment=production,k8s.pod.name=$(POD_NAME),k8s.namespace.name=$(POD_NAMESPACE),k8s.pod.uid=$(POD_UID)"
  - name: NODE_IP
    valueFrom: {fieldRef: {fieldPath: status.hostIP}}
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "http://$(NODE_IP):4318"  # collector hostPort on the node
```

The `k8sattributes` processor in the collector enriches spans further with `k8s.deployment.name`, `k8s.replicaset.name`, `k8s.node.name` by matching on `k8s.pod.uid`.

### 2. Deploy the OTel Collector DaemonSet

See [`deploy/k8s/otel-collector-k8s.yaml`](deploy/k8s/otel-collector-k8s.yaml) for the full manifest.

Key points:

**kubeletstats auth on k3s/k3d:** The kubelet in k3s rejects audience-bound projected SA tokens (the default `auth_type: serviceAccount`). Use `auth_type: kubeConfig` with a classic long-lived SA token secret instead:

```bash
# 1. Create a classic (non-projected) service account token
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: otel-collector-token
  namespace: <namespace>
  annotations:
    kubernetes.io/service-account.name: otel-collector
type: kubernetes.io/service-account-token
EOF

# 2. Build a kubeconfig from it
TOKEN=$(kubectl get secret otel-collector-token -n <namespace> -o jsonpath='{.data.token}' | base64 -d)
CA=$(kubectl get secret otel-collector-token -n <namespace> -o jsonpath='{.data.ca\.crt}')
kubectl create secret generic otel-collector-kubeconfig -n <namespace> \
  --from-literal=kubeconfig="apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${CA}
    server: https://kubernetes.default.svc.cluster.local
  name: local
contexts:
- context: {cluster: local, user: otel-collector}
  name: local
current-context: local
users:
- name: otel-collector
  user:
    token: ${TOKEN}"
```

**Collector config for kubeletstats:**

```yaml
kubeletstats:
  collection_interval: 30s
  auth_type: kubeConfig            # uses the kubeconfig secret (not projected SA token)
  endpoint: ${env:K8S_NODE_NAME}   # node name only — routes via k8s API server proxy
  insecure_skip_verify: true
  metric_groups: [node, pod, container]
```

> Note: `endpoint` must be the bare node **name** (not a URL). When `auth_type: kubeConfig`, the receiver routes all requests through `<api-server>/api/v1/nodes/<node-name>/proxy/stats/summary`, bypassing direct kubelet auth entirely.

**DaemonSet requirements:**
- `hostNetwork: true` + `hostPID: true` for node-level visibility
- `hostPort: 4317/4318` so pods can reach the collector via `NODE_IP`
- `tolerations: [{operator: Exists}]` to run on all nodes including control-plane
- Mount kubeconfig at `/var/kubecfg/kubeconfig` (not under `/conf/` which is taken by the ConfigMap mount)
- Set `KUBECONFIG=/var/kubecfg/kubeconfig` env var in the container
- Set `K8S_NODE_NAME` from `spec.nodeName` fieldRef

### 3. k8sattributes processor

Configure pod association using `k8s.pod.uid` (required for k3d/k3s where pod-to-node hostPort traffic is NATted, making `from: connection` unreliable):

```yaml
k8sattributes:
  auth_type: serviceAccount
  extract:
    metadata:
      - k8s.pod.name
      - k8s.pod.uid
      - k8s.deployment.name
      - k8s.namespace.name
      - k8s.node.name
      - k8s.replicaset.name
  pod_association:
    - sources: [{from: resource_attribute, name: k8s.pod.ip}]
    - sources: [{from: resource_attribute, name: k8s.pod.uid}]   # primary match for k3d
    - sources: [{from: connection}]
```

### 4. Infrastructure metric correlation

kubeletstats metrics carry `k8s.pod.name` / `k8s.namespace.name` in resource attributes but **not** `service.name`. otel-beacon correlates them via the entity registry:

1. App spans populate the entity registry with both `service.name` and k8s attrs
2. When querying `GET /v1/query/metrics?service=my-svc`, the backend looks up the entity's k8s attrs and ORs them into the WHERE clause
3. Result includes both app metrics (`http.*`, `jvm.*`) and infra metrics (`k8s.pod.*`, `container.*`)

This is reflected in the Metrics tab — use the **Category** filter to select **Infrastructure** to isolate kubelet metrics.

### Quick deploy script

```bash
# On EC2 / host running k3d:
./deploy/deploy-petclinic-k8s.sh
```

See [`deploy/deploy-petclinic-k8s.sh`](deploy/deploy-petclinic-k8s.sh) for the full automated deployment.

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
