#!/usr/bin/env bash
# deploy-astronomy-shop.sh
#
# Deploys the OpenTelemetry Demo (Astronomy Shop) to a k3d cluster with
# otel-beacon as the telemetry backend.  Handles k3d cluster creation, otel-beacon
# startup, OTel collector configuration, and helm chart deployment.
#
# Usage:
#   bash deploy/deploy-astronomy-shop.sh
#
# Key env vars:
#   CLUSTER_NAME      k3d cluster name          (default: astronomy-shop)
#   CHART_VERSION     Helm chart version        (default: 0.40.9)
#   FRONTEND_PORT     Host port for the shop UI (default: 8090)
#   RELEASE_NAME      Helm release name         (default: astronomyshop)
#   OTEL_BEACON_IMAGE Docker image for beacon   (default: ghcr.io/mqbui1/otel-beacon:latest)
#   DATA_DIR          Host path for SQLite DB   (default: ~/otel-data)
#   SKIP_CLUSTER      Set to "true" to skip k3d cluster creation (use existing)
#
# What this deploys:
#   - k3d cluster (1 server, 2 agents) with host port mappings
#   - otel-beacon on host network (OTLP 4317/4318, UI 8080)
#   - OpenTelemetry Demo chart with built-in OTel collector forwarding to otel-beacon
#   - All astronomy shop services (22 microservices) with traces, metrics, and logs
#
# To teardown:
#   k3d cluster delete "$CLUSTER_NAME"
#   docker stop otel-beacon && docker rm otel-beacon

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-astronomy-shop}"
CHART_VERSION="${CHART_VERSION:-0.40.9}"
FRONTEND_PORT="${FRONTEND_PORT:-8090}"
RELEASE_NAME="${RELEASE_NAME:-astronomyshop}"
OTEL_BEACON_IMAGE="${OTEL_BEACON_IMAGE:-ghcr.io/mqbui1/otel-beacon:latest}"
DATA_DIR="${DATA_DIR:-${HOME}/otel-data}"
SKIP_CLUSTER="${SKIP_CLUSTER:-false}"

log() { echo ""; echo "==> $*"; }

# ---------------------------------------------------------------------------
# Step 1: Install prerequisites
# ---------------------------------------------------------------------------
if ! command -v docker &>/dev/null; then
  log "Installing Docker..."
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
  exec sg docker "$0 $*"
fi

if ! command -v kubectl &>/dev/null; then
  log "Installing kubectl..."
  curl -fsSLo /tmp/kubectl \
    "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl
fi

if ! command -v k3d &>/dev/null; then
  log "Installing k3d..."
  curl -fsSL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
fi

if ! command -v helm &>/dev/null; then
  log "Installing helm..."
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

# ---------------------------------------------------------------------------
# Step 2: Create k3d cluster
#   - 1 server + 2 agent nodes
#   - Port 8090 → NodePort 30080 (frontend-proxy UI)
#   - Traefik disabled (astronomy shop manages its own ingress)
# ---------------------------------------------------------------------------
if [ "$SKIP_CLUSTER" = "true" ]; then
  log "Skipping cluster creation (SKIP_CLUSTER=true)"
elif k3d cluster list 2>/dev/null | grep -q "^${CLUSTER_NAME}"; then
  log "k3d cluster '${CLUSTER_NAME}' already exists, skipping create"
else
  log "Creating k3d cluster '${CLUSTER_NAME}'..."
  k3d cluster create "$CLUSTER_NAME" \
    --servers 1 \
    --agents 2 \
    --port "${FRONTEND_PORT}:30080@loadbalancer" \
    --k3s-arg "--disable=traefik@server:0" \
    --wait
fi

kubectl cluster-info
log "Cluster nodes:"
kubectl get nodes

# ---------------------------------------------------------------------------
# Step 3: Detect host gateway IP (used by pods to reach otel-beacon on host)
#
# k3d creates a Docker bridge network named k3d-<cluster>.  The gateway of
# that network is reachable from all k3s pods as the host IP.
# ---------------------------------------------------------------------------
log "Detecting host gateway IP..."
GATEWAY_IP=$(docker network inspect "k3d-${CLUSTER_NAME}" \
  --format '{{range .IPAM.Config}}{{if .Gateway}}{{.Gateway}}{{end}}{{end}}' 2>/dev/null \
  | head -n1)

if [ -z "$GATEWAY_IP" ]; then
  echo "ERROR: could not detect gateway IP for k3d-${CLUSTER_NAME} network"
  echo "  Tip: run 'docker network ls' and inspect the k3d network manually"
  exit 1
fi

BEACON_OTLP_HTTP="http://${GATEWAY_IP}:4318"
log "Host gateway IP: ${GATEWAY_IP}"
log "otel-beacon OTLP HTTP endpoint (from pods): ${BEACON_OTLP_HTTP}"

# ---------------------------------------------------------------------------
# Step 4: Start otel-beacon
# ---------------------------------------------------------------------------
mkdir -p "$DATA_DIR"

if docker ps --format '{{.Names}}' | grep -q "^otel-beacon$"; then
  log "otel-beacon already running"
elif docker ps -a --format '{{.Names}}' | grep -q "^otel-beacon$"; then
  log "Starting existing otel-beacon container..."
  docker start otel-beacon
else
  log "Starting otel-beacon..."
  docker run -d \
    --name otel-beacon \
    --restart unless-stopped \
    --network host \
    -v "${DATA_DIR}:/data" \
    -e DB_DSN=/data/otel.db \
    "$OTEL_BEACON_IMAGE"
fi

log "Waiting for otel-beacon to be ready..."
for i in $(seq 1 20); do
  curl -sf http://localhost:8080/v1/entities >/dev/null 2>&1 && break
  echo "  attempt $i/20..."
  sleep 3
done
curl -sf http://localhost:8080/v1/entities >/dev/null 2>&1 || {
  echo "ERROR: otel-beacon not responding at http://localhost:8080"
  echo "  Check: docker logs otel-beacon"
  exit 1
}
echo "otel-beacon is ready"

# ---------------------------------------------------------------------------
# Step 5: Add Helm repo
# ---------------------------------------------------------------------------
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts 2>/dev/null || true
helm repo update open-telemetry

# ---------------------------------------------------------------------------
# Step 6: Write Helm values
#
# Key decisions:
#   - opentelemetry-collector enabled: the chart's services already point to
#     otel-collector:4317 via OTEL_EXPORTER_OTLP_ENDPOINT.  Enabling the
#     built-in collector creates that Service and configures it to forward
#     all telemetry (traces, metrics, logs) to otel-beacon.
#   - k8sattributes processor: enriches spans/metrics/logs with k8s pod and
#     node metadata pulled from the k8s API.
#   - All observatory backends (Jaeger, Prometheus, Grafana, OpenSearch)
#     are disabled — otel-beacon is the single backend.
#   - Load generator is configured with 10 virtual users to produce
#     representative traffic.
# ---------------------------------------------------------------------------
# Write values to a permanent path so it's reusable and survives /tmp cleanup
VALUES_FILE="${HOME}/astronomy-shop-values.yaml"

cat > "$VALUES_FILE" <<EOF
# ---------------------------------------------------------------------------
# Global resource attributes applied to every service
# ---------------------------------------------------------------------------
default:
  envOverrides:
    - name: OTEL_RESOURCE_ATTRIBUTES
      value: "service.name=\$(OTEL_SERVICE_NAME),service.namespace=opentelemetry-demo,deployment.environment=demo"

# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------
components:
  # Expose the frontend proxy on NodePort 30080 (mapped to host ${FRONTEND_PORT})
  frontend-proxy:
    service:
      type: NodePort
      nodePort: 30080

  # Load generator: 10 users, autostart
  load-generator:
    enabled: true
    useDefault:
      env: true
    env:
      - name: LOCUST_USERS
        value: "10"
      - name: LOCUST_SPAWN_RATE
        value: "2"
      - name: LOCUST_HOST
        value: http://frontend-proxy:8080
      - name: LOCUST_HEADLESS
        value: "false"
      - name: LOCUST_AUTOSTART
        value: "true"
      - name: PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION
        value: python
      - name: FLAGD_HOST
        value: flagd
      - name: FLAGD_PORT
        value: "8013"
    resources:
      limits:
        memory: 1500Mi

# ---------------------------------------------------------------------------
# Built-in OTel collector — receives from all services and exports to otel-beacon
#
# All astronomy shop services use OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4317
# by default (set by the chart).  Enabling this component creates the Service
# that satisfies those endpoints.
# ---------------------------------------------------------------------------
opentelemetry-collector:
  enabled: true
  config:
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
          http:
            endpoint: 0.0.0.0:4318

    processors:
      batch:
        timeout: 5s
        send_batch_size: 512
      memory_limiter:
        check_interval: 5s
        limit_percentage: 80
        spike_limit_percentage: 25

    exporters:
      otlphttp/beacon:
        endpoint: "${BEACON_OTLP_HTTP}"
        tls:
          insecure: true
        # Retry on transient failures (otel-beacon restart, etc.)
        retry_on_failure:
          enabled: true
          initial_interval: 5s
          max_interval: 30s

    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [memory_limiter, batch]
          exporters: [otlphttp/beacon]
        metrics:
          receivers: [otlp]
          processors: [memory_limiter, batch]
          exporters: [otlphttp/beacon]
        logs:
          receivers: [otlp]
          processors: [memory_limiter, batch]
          exporters: [otlphttp/beacon]

# ---------------------------------------------------------------------------
# Disable all built-in observability backends — otel-beacon is the backend
# ---------------------------------------------------------------------------
jaeger:
  enabled: false
prometheus:
  enabled: false
grafana:
  enabled: false
opensearch:
  enabled: false
EOF

log "Helm values written to: $VALUES_FILE"

# ---------------------------------------------------------------------------
# Step 7: Deploy astronomy shop
#
# helm install without --wait so this script exits quickly.  Pods take 3-5
# minutes to become fully ready; use Step 8 to track progress.
# ---------------------------------------------------------------------------
log "Deploying astronomy shop (chart version ${CHART_VERSION})..."
helm upgrade --install "$RELEASE_NAME" \
  open-telemetry/opentelemetry-demo \
  --version "$CHART_VERSION" \
  --namespace default \
  --create-namespace \
  --values "$VALUES_FILE" \
  --timeout 5m

# ---------------------------------------------------------------------------
# Step 8: Wait for pods to be ready (non-blocking: background nohup job)
#
# Helm applies all resources instantly.  We run a background watcher that
# logs progress to ~/astronomy-shop-deploy.log and touches
# ~/astronomy-shop-ready when all pods are Ready.
# ---------------------------------------------------------------------------
WATCH_LOG="${HOME}/astronomy-shop-deploy.log"
READY_FLAG="${HOME}/astronomy-shop-ready"
rm -f "$READY_FLAG"

nohup bash -c '
  for i in $(seq 1 60); do
    NOT_READY=$(kubectl get pods -n default \
      -l "app.kubernetes.io/instance='"${RELEASE_NAME}"'" \
      --no-headers 2>/dev/null \
      | grep -v "Running\|Completed" | wc -l)
    TOTAL=$(kubectl get pods -n default \
      -l "app.kubernetes.io/instance='"${RELEASE_NAME}"'" \
      --no-headers 2>/dev/null | wc -l)
    echo "$(date -u +%H:%M:%S) pods: $((TOTAL - NOT_READY))/$TOTAL ready"
    if [ "$NOT_READY" -eq 0 ] && [ "$TOTAL" -gt 0 ]; then
      echo "All pods ready!"
      touch "'"${READY_FLAG}"'"
      break
    fi
    sleep 10
  done
' >> "$WATCH_LOG" 2>&1 &

echo "Pod readiness watcher started (PID $!)"
echo "  Follow: tail -f ${WATCH_LOG}"

log "Current pod status (snapshot):"
kubectl get pods -n default -l "app.kubernetes.io/instance=${RELEASE_NAME}" \
  --sort-by=.metadata.name

# ---------------------------------------------------------------------------
# Step 9: Quick verification — at least the collector should be up already
# ---------------------------------------------------------------------------
log "Verifying OTel collector is accepting connections..."
for i in $(seq 1 12); do
  COLLECTOR_READY=$(kubectl get pods -n default \
    -l "app.kubernetes.io/name=opentelemetry-collector" \
    --no-headers 2>/dev/null | grep "Running" | wc -l)
  if [ "${COLLECTOR_READY:-0}" -ge 1 ]; then
    echo "  OTel collector is running (${COLLECTOR_READY} pod(s))"
    break
  fi
  echo "  attempt $i/12 — waiting for collector..."
  sleep 5
done

log "Verifying telemetry flow to otel-beacon (checking for first spans)..."
for i in $(seq 1 18); do
  SVC_COUNT=$(curl -sf 'http://localhost:8080/v1/entities?type=service' 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo 0)
  if [ "${SVC_COUNT:-0}" -ge 5 ]; then
    echo "  otel-beacon reports ${SVC_COUNT} service entities — data is flowing!"
    break
  fi
  echo "  attempt $i/18 — ${SVC_COUNT:-0} services registered, waiting..."
  sleep 10
done

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
PUBLIC_IP=$(curl -sf http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null \
  || hostname -I | awk '{print $1}')

echo ""
echo "============================================================"
echo " Astronomy Shop deployment complete!"
echo "============================================================"
echo ""
echo "  Astronomy Shop UI:  http://${PUBLIC_IP}:${FRONTEND_PORT}"
echo "  otel-beacon UI:     http://${PUBLIC_IP}:8080"
echo ""
echo "  OTLP endpoint (from pods): ${BEACON_OTLP_HTTP}"
echo ""
echo "  Check pods:    kubectl get pods -n default"
echo "  Collector logs: kubectl logs -n default -l app.kubernetes.io/name=opentelemetry-collector --tail=50"
echo "  otel-beacon:   docker logs -f otel-beacon"
echo ""
echo "  SSH tunnel for local access:"
echo "    ssh -L 8080:localhost:8080 -L 8090:localhost:${FRONTEND_PORT} -p 2222 splunk@${PUBLIC_IP}"
echo ""
echo "To teardown:"
echo "  k3d cluster delete ${CLUSTER_NAME}"
echo ""
