#!/usr/bin/env bash
# deploy-beacon.sh
#
# Builds otel-beacon from source on the EC2 and runs it as a Docker container
# on the host network (ports 8080/4317/4318).  k3d pods reach it via
# host.k3d.internal:4318.
#
# Usage:
#   bash deploy/deploy-beacon.sh
#
# Environment overrides:
#   REPO_URL   - git repo to clone (default: https://github.com/mqbui1/otel-beacon.git)
#   BUILD_DIR  - where to clone/build    (default: /tmp/otel-beacon-build)
#   IMAGE      - Docker image tag        (default: localhost:9999/otel-beacon:latest)
#   DATA_DIR   - host path for SQLite DB (default: ~/otel-data)

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/mqbui1/otel-beacon.git}"
BUILD_DIR="${BUILD_DIR:-/tmp/otel-beacon-build}"
IMAGE="${IMAGE:-localhost:9999/otel-beacon:latest}"
DATA_DIR="${DATA_DIR:-${HOME}/otel-data}"
CONTAINER_NAME="otel-beacon"

log() { echo ""; echo "==> $*"; }

# ---------------------------------------------------------------------------
# Step 1: Clone or update the repo
# ---------------------------------------------------------------------------
if [ -d "${BUILD_DIR}/.git" ]; then
  log "Updating otel-beacon repo at ${BUILD_DIR}..."
  git -C "$BUILD_DIR" fetch origin
  git -C "$BUILD_DIR" reset --hard origin/main
else
  log "Cloning otel-beacon repo..."
  git clone "$REPO_URL" "$BUILD_DIR"
fi

# ---------------------------------------------------------------------------
# Step 2: Build Docker image
# ---------------------------------------------------------------------------
log "Building otel-beacon Docker image (this takes a few minutes)..."
docker build -t "$IMAGE" "$BUILD_DIR"

# ---------------------------------------------------------------------------
# Step 3: Push to k3d local registry (localhost:9999)
# ---------------------------------------------------------------------------
log "Pushing image to local registry..."
docker push "$IMAGE"

# ---------------------------------------------------------------------------
# Step 4: Stop any existing container and start fresh
# ---------------------------------------------------------------------------
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  log "Removing existing otel-beacon container..."
  docker rm -f "$CONTAINER_NAME"
fi

mkdir -p "$DATA_DIR"

log "Starting otel-beacon on host network..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  --network host \
  -v "${DATA_DIR}:/data" \
  -e DB_DSN=/data/otel.db \
  "$IMAGE"

# ---------------------------------------------------------------------------
# Step 5: Wait for readiness
# ---------------------------------------------------------------------------
log "Waiting for otel-beacon to be ready..."
for i in $(seq 1 20); do
  curl -sf http://localhost:8080/v1/entities >/dev/null 2>&1 && break
  echo "  attempt $i/20..."
  sleep 3
done
curl -sf http://localhost:8080/v1/entities >/dev/null 2>&1 || { echo "ERROR: otel-beacon not responding"; exit 1; }
echo "otel-beacon is ready"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
PUBLIC_IP=$(curl -sf http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null \
            || hostname -I | awk '{print $1}')

echo ""
echo "=========================================="
echo " otel-beacon deployed!"
echo "=========================================="
echo ""
echo "  UI:        http://${PUBLIC_IP}:8080"
echo "  OTLP HTTP: http://host.k3d.internal:4318  (from k3d pods)"
echo "  OTLP gRPC: host.k3d.internal:4317          (from k3d pods)"
echo ""
echo " Next: deploy petclinic"
echo "   SKIP_COLLECTOR=true \\"
echo "   OTEL_BACKEND_ENDPOINT=http://host.k3d.internal:4318 \\"
echo "   bash deploy/deploy-petclinic-k8s.sh petclinic"
echo ""
