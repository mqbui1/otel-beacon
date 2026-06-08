#!/usr/bin/env bash
# bootstrap-ec2.sh
#
# Full one-shot setup for a fresh Ubuntu EC2 instance running otel-beacon + PetClinic on k3d.
#
# Run as the 'splunk' user (or any sudo-capable user):
#   curl -fsSL https://raw.githubusercontent.com/mqbui1/otel-beacon/main/deploy/bootstrap-ec2.sh | bash
# or:
#   scp deploy/bootstrap-ec2.sh splunk@<EC2>:~ && ssh splunk@<EC2> bash bootstrap-ec2.sh
#
# What this does:
#   1. Install Docker, k3d, kubectl
#   2. Create k3d cluster (NodePort 30080 -> host 8090)
#   3. Download OTel Java agent and copy to all k3d nodes
#   4. Start otel-beacon (Docker, persists data in ~/otel-data)
#   5. Run deploy-petclinic-k8s.sh (deploys MySQL + PetClinic + OTel Collector DaemonSet)
#   6. Start loadgen in background
#
# Prerequisites on EC2:
#   - Ubuntu 22.04 or 24.04
#   - Security group: inbound 22 (SSH), 8080 (otel-beacon UI), 8090 (PetClinic UI)
#   - At least 4 vCPU / 8 GB RAM recommended (t3.xlarge or larger)

set -euo pipefail

OTEL_BEACON_IMAGE="${OTEL_BEACON_IMAGE:-ghcr.io/mqbui1/otel-beacon:latest}"
OTEL_AGENT_URL="${OTEL_AGENT_URL:-https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar}"
OTEL_AGENT_LOCAL="/home/splunk/opentelemetry-javaagent.jar"
K3D_CLUSTER_NAME="petclinic"
DATA_DIR="${HOME}/otel-data"

log() { echo ""; echo "==> $*"; }

# ---------------------------------------------------------------------------
# Step 1: Install Docker (if not present)
# ---------------------------------------------------------------------------
if ! command -v docker &>/dev/null; then
  log "Installing Docker..."
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
  # Re-exec with docker group so subsequent docker commands work without sudo
  exec sg docker "$0 $*"
fi

# ---------------------------------------------------------------------------
# Step 2: Install kubectl (if not present)
# ---------------------------------------------------------------------------
if ! command -v kubectl &>/dev/null; then
  log "Installing kubectl..."
  curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl
fi

# ---------------------------------------------------------------------------
# Step 3: Install k3d (if not present)
# ---------------------------------------------------------------------------
if ! command -v k3d &>/dev/null; then
  log "Installing k3d..."
  curl -fsSL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
fi

# ---------------------------------------------------------------------------
# Step 4: Create k3d cluster
#   - 1 server node (control plane + worker)
#   - NodePort 30080 mapped to host port 8090 (PetClinic UI)
#   - Extra /otel mount so the Java agent hostPath volume works
# ---------------------------------------------------------------------------
if k3d cluster list | grep -q "^${K3D_CLUSTER_NAME}"; then
  log "k3d cluster '${K3D_CLUSTER_NAME}' already exists, skipping create"
else
  log "Creating k3d cluster '${K3D_CLUSTER_NAME}'..."
  k3d cluster create "$K3D_CLUSTER_NAME" \
    --port "8090:30080@loadbalancer" \
    --k3s-arg "--disable=traefik@server:0" \
    --volume "/otel:/otel@all"
fi

kubectl cluster-info

# ---------------------------------------------------------------------------
# Step 5: Download OTel Java agent and copy to k3d nodes
# ---------------------------------------------------------------------------
if [ ! -f "$OTEL_AGENT_LOCAL" ]; then
  log "Downloading OTel Java agent..."
  curl -fsSL -o "$OTEL_AGENT_LOCAL" "$OTEL_AGENT_URL"
fi

log "Copying Java agent to k3d nodes..."
sudo mkdir -p /otel
sudo cp "$OTEL_AGENT_LOCAL" /otel/opentelemetry-javaagent.jar
# Also copy into each k3d container node (for the hostPath volume)
for node in $(k3d node list --cluster "$K3D_CLUSTER_NAME" -o json 2>/dev/null | python3 -c "import sys,json; [print(n['name']) for n in json.load(sys.stdin)]" 2>/dev/null || kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  docker cp "$OTEL_AGENT_LOCAL" "${node}:/otel/opentelemetry-javaagent.jar" 2>/dev/null || true
done

# ---------------------------------------------------------------------------
# Step 6: Start otel-beacon
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
  sleep 3
done
echo "otel-beacon is ready"

# ---------------------------------------------------------------------------
# Step 7: Deploy PetClinic + OTel Collector + MySQL
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_SCRIPT="${SCRIPT_DIR}/deploy-petclinic-k8s.sh"

# If the deploy script isn't alongside bootstrap (e.g., downloaded standalone),
# pull it from the repo.
if [ ! -f "$DEPLOY_SCRIPT" ]; then
  log "Downloading deploy-petclinic-k8s.sh..."
  curl -fsSL -o /tmp/deploy-petclinic-k8s.sh \
    "https://raw.githubusercontent.com/mqbui1/otel-beacon/main/deploy/deploy-petclinic-k8s.sh"
  DEPLOY_SCRIPT=/tmp/deploy-petclinic-k8s.sh
  chmod +x "$DEPLOY_SCRIPT"
fi

log "Running deploy-petclinic-k8s.sh..."
OTEL_BACKEND_ENDPOINT="http://host.k3d.internal:4318" bash "$DEPLOY_SCRIPT" petclinic

# ---------------------------------------------------------------------------
# Step 8: Start loadgen
# ---------------------------------------------------------------------------
LOADGEN="$HOME/loadgen.sh"
cat > "$LOADGEN" <<'LOADGEN_SCRIPT'
#!/usr/bin/env bash
# Continuous load generator for PetClinic (NodePort 30080 -> host 8090)
BASE="http://localhost:8090"
while true; do
  wget -qO- "${BASE}/api/customer/owners" >/dev/null 2>&1
  wget -qO- "${BASE}/api/vet/vets" >/dev/null 2>&1
  wget -qO- "${BASE}/api/customer/owners/1" >/dev/null 2>&1
  wget -qO- "${BASE}/api/visit/owners/1/pets/1/visits" >/dev/null 2>&1
  wget -qO- "${BASE}/api/customer/owners?lastName=" >/dev/null 2>&1
  sleep 2
done
LOADGEN_SCRIPT
chmod +x "$LOADGEN"

# Kill any previous loadgen
pkill -f loadgen.sh 2>/dev/null || true

log "Starting loadgen in background..."
nohup bash "$LOADGEN" >> "$HOME/loadgen.log" 2>&1 &
echo "Loadgen PID: $!"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
PUBLIC_IP=$(curl -sf http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<EC2-PUBLIC-IP>")

echo ""
echo "=========================================="
echo " Bootstrap complete!"
echo "=========================================="
echo ""
echo " PetClinic UI:   http://${PUBLIC_IP}:8090"
echo " otel-beacon UI: http://${PUBLIC_IP}:8080"
echo ""
echo " Loadgen running: tail -f ~/loadgen.log"
echo " Pod status:      kubectl get pods -n petclinic"
echo ""
