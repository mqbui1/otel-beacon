#!/usr/bin/env bash
# deploy-travel-planner-bedrock.sh
#
# Copies the multi-agent travel planner to EC2 and starts it as a background
# load generator. Uses Bedrock (claude-3-haiku) if IAM permissions allow,
# otherwise falls back to a built-in simulator that emits identical OTel spans.
#
# Usage:
#   bash deploy/deploy-travel-planner-bedrock.sh
#   bash deploy/deploy-travel-planner-bedrock.sh --delay 30    # slower pace
#   bash deploy/deploy-travel-planner-bedrock.sh --no-bedrock  # force simulator
#
# Requirements on remote host:
#   pip3 install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
#
set -euo pipefail

EC2_HOST="${EC2_HOST:-54.172.198.24}"
EC2_PORT="${EC2_PORT:-2222}"
EC2_USER="${EC2_USER:-splunk}"
EC2_PASS="${EC2_PASS:-Sp1unkH00di3}"
OTEL_ENDPOINT="${OTEL_ENDPOINT:-http://localhost:4318}"
DELAY="${DELAY:-15}"
USE_BEDROCK="true"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse args
for arg in "$@"; do
  case "$arg" in
    --delay=*) DELAY="${arg#*=}" ;;
    --delay)   shift; DELAY="$1" ;;
    --no-bedrock) USE_BEDROCK="false" ;;
  esac
done

log() { echo ""; echo "==> $*"; }

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15"
SCP_OPTS="-P ${EC2_PORT} -o StrictHostKeyChecking=no -o ConnectTimeout=15"
SSH_CMD="sshpass -p '${EC2_PASS}' ssh ${SSH_OPTS} -p ${EC2_PORT} ${EC2_USER}@${EC2_HOST}"

# ---------------------------------------------------------------------------
log "Installing Python dependencies on EC2..."
# ---------------------------------------------------------------------------
eval "$SSH_CMD" "pip3 install --quiet --upgrade \
  opentelemetry-sdk \
  opentelemetry-exporter-otlp-proto-http \
  boto3 2>/dev/null || true"

# ---------------------------------------------------------------------------
log "Copying travel planner script..."
# ---------------------------------------------------------------------------
sshpass -p "${EC2_PASS}" scp ${SCP_OPTS} \
  "${SCRIPT_DIR}/travel-planner-bedrock.py" \
  "${EC2_USER}@${EC2_HOST}:/tmp/travel-planner-bedrock.py"

# ---------------------------------------------------------------------------
log "Stopping any previous instance..."
# ---------------------------------------------------------------------------
eval "$SSH_CMD" "pkill -f travel-planner-bedrock.py 2>/dev/null || true; sleep 1"

# ---------------------------------------------------------------------------
log "Starting travel planner load generator..."
# ---------------------------------------------------------------------------
eval "$SSH_CMD" "nohup env \
  OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_ENDPOINT} \
  OTEL_SERVICE_NAME=travel-planner \
  USE_BEDROCK=${USE_BEDROCK} \
  python3 /tmp/travel-planner-bedrock.py --loadgen --delay ${DELAY} \
  >> ~/travel-planner.log 2>&1 &"

sleep 2

# ---------------------------------------------------------------------------
log "Verifying process is running..."
# ---------------------------------------------------------------------------
PIDS=$(eval "$SSH_CMD" "pgrep -f travel-planner-bedrock.py || true")
if [ -n "$PIDS" ]; then
  echo "  Running (PID: $PIDS)"
else
  echo "  WARNING: Process not found — check ~/travel-planner.log on EC2"
fi

log "Tailing first 20 lines of log..."
eval "$SSH_CMD" "sleep 3 && head -20 ~/travel-planner.log 2>/dev/null || echo '(log not yet available)'"

echo ""
echo "=========================================="
echo " Travel planner deployed!"
echo "=========================================="
echo ""
echo " Logs:    ssh -p ${EC2_PORT} ${EC2_USER}@${EC2_HOST} 'tail -f ~/travel-planner.log'"
echo " Stop:    ssh -p ${EC2_PORT} ${EC2_USER}@${EC2_HOST} 'pkill -f travel-planner-bedrock.py'"
echo " Agents:  curl http://${EC2_HOST}:8080/v1/genai/agents"
echo " Spans:   curl http://${EC2_HOST}:8080/v1/genai/spans?limit=10"
echo " Costs:   curl http://${EC2_HOST}:8080/v1/genai/costs"
echo ""
