#!/usr/bin/env bash
# deploy-travel-planner.sh
#
# Deploys the multi-agent travel planner to k3d on the EC2 instance.
# The app instruments itself with opentelemetry-instrumentation-langchain and
# sends OTel gen_ai.* spans to otel-beacon.
#
# Requirements:
#   - OPENAI_API_KEY  (required — LangChain calls OpenAI)
#   - k3d cluster 'petclinic' already running (bootstrap-ec2.sh)
#   - otel-beacon running on host:4318
#
# Usage:
#   OPENAI_API_KEY=sk-... bash deploy-travel-planner.sh
#
set -euo pipefail

NAMESPACE="${NAMESPACE:-petclinic}"
OTEL_ENDPOINT="${OTEL_ENDPOINT:-http://host.k3d.internal:4318}"
IMAGE="travel-planner:latest"
REPO_URL="https://github.com/signalfx/splunk-otel-python-contrib.git"
REPO_DIR="/tmp/splunk-otel-python-contrib"
APP_DIR="${REPO_DIR}/instrumentation-genai/opentelemetry-instrumentation-langchain/examples/multi_agent_travel_planner"

if [ -z "${OPENAI_API_KEY:-}" ]; then
  echo "ERROR: OPENAI_API_KEY is required"
  exit 1
fi

log() { echo ""; echo "==> $*"; }

# ---------------------------------------------------------------------------
# Clone / update the repo
# ---------------------------------------------------------------------------
if [ -d "$REPO_DIR/.git" ]; then
  log "Updating splunk-otel-python-contrib..."
  git -C "$REPO_DIR" pull --ff-only
else
  log "Cloning splunk-otel-python-contrib (shallow)..."
  git clone --depth=1 "$REPO_URL" "$REPO_DIR"
fi

# ---------------------------------------------------------------------------
# Build Docker image for the travel planner
# ---------------------------------------------------------------------------
log "Building travel-planner Docker image..."

cat > /tmp/travel-planner.Dockerfile <<'DOCKERFILE'
FROM python:3.11-slim

WORKDIR /app

# Install the travel planner and its OTel instrumentation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    opentelemetry-sdk \
    opentelemetry-exporter-otlp-proto-http \
    opentelemetry-instrumentation-langchain \
    "splunk-opentelemetry[all]" 2>/dev/null || \
    pip install --no-cache-dir -r requirements.txt \
    opentelemetry-sdk \
    opentelemetry-exporter-otlp-proto-http \
    opentelemetry-instrumentation-langchain

COPY . .

# Continuously run travel queries to generate telemetry
CMD ["python", "run_loadgen.py"]
DOCKERFILE

# Create a simple loadgen wrapper if it doesn't exist
cat > /tmp/travel_planner_loadgen.py <<'PYEOF'
#!/usr/bin/env python3
"""Continuous load generator for the multi-agent travel planner."""
import time
import random
import subprocess
import sys

QUERIES = [
    "Plan a 5-day trip from New York to Paris in June, budget $3000",
    "Find flights and hotels for a family vacation to Tokyo in August",
    "I need a business trip itinerary: San Francisco to London, 3 nights",
    "Plan a romantic weekend getaway from Chicago to Miami",
    "Backpacker trip: Southeast Asia, 2 weeks, under $1500 total",
    "Conference trip to Berlin from NYC, 4 days including pre-conference day",
    "Honeymoon in Maldives from Los Angeles, 7 nights luxury",
    "Solo travel: Spain and Portugal rail trip, 10 days from Boston",
]

if __name__ == "__main__":
    delay = float(sys.argv[1]) if len(sys.argv) > 1 else 15.0
    while True:
        query = random.choice(QUERIES)
        print(f"Query: {query}", flush=True)
        try:
            result = subprocess.run(
                ["python", "main.py"],
                input=query,
                capture_output=True,
                text=True,
                timeout=120,
                env=dict(__import__("os").environ),
            )
            if result.returncode != 0:
                print(f"ERROR: {result.stderr[:200]}", flush=True)
            else:
                print(f"OK ({len(result.stdout)} chars)", flush=True)
        except subprocess.TimeoutExpired:
            print("TIMEOUT", flush=True)
        except Exception as e:
            print(f"EXCEPTION: {e}", flush=True)
        time.sleep(delay)
PYEOF

cp /tmp/travel_planner_loadgen.py "${APP_DIR}/run_loadgen.py"
cp /tmp/travel-planner.Dockerfile "${APP_DIR}/Dockerfile"

# Build inside k3d's docker context so the image is available to pods
docker build -t "$IMAGE" "$APP_DIR"
# Import image into k3d cluster
k3d image import "$IMAGE" -c petclinic

# ---------------------------------------------------------------------------
# Deploy to k3d as a Kubernetes Deployment
# ---------------------------------------------------------------------------
log "Deploying travel-planner to k3d namespace ${NAMESPACE}..."

kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: travel-planner-secrets
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  OPENAI_API_KEY: "${OPENAI_API_KEY}"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: travel-planner
  namespace: ${NAMESPACE}
  labels:
    app: travel-planner
spec:
  replicas: 1
  selector:
    matchLabels:
      app: travel-planner
  template:
    metadata:
      labels:
        app: travel-planner
    spec:
      containers:
        - name: travel-planner
          image: ${IMAGE}
          imagePullPolicy: Never
          env:
            - name: OPENAI_API_KEY
              valueFrom:
                secretKeyRef:
                  name: travel-planner-secrets
                  key: OPENAI_API_KEY
            # OTel SDK configuration
            - name: OTEL_SERVICE_NAME
              value: "travel-planner"
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "${OTEL_ENDPOINT}"
            - name: OTEL_EXPORTER_OTLP_PROTOCOL
              value: "http/protobuf"
            - name: OTEL_TRACES_EXPORTER
              value: "otlp"
            - name: OTEL_METRICS_EXPORTER
              value: "otlp"
            - name: OTEL_LOGS_EXPORTER
              value: "otlp"
            # Enable gen_ai event capture (prompts + completions in span events)
            - name: OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT
              value: "true"
            # Inject synthetic quality noise for eval testing (20% chance per request)
            - name: TRAVEL_POISON_PROB
              value: "0.2"
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"
EOF

log "Waiting for travel-planner pod to be ready..."
kubectl rollout status deployment/travel-planner -n "${NAMESPACE}" --timeout=120s

echo ""
echo "=========================================="
echo " Travel planner deployed!"
echo "=========================================="
echo ""
echo " Pods:    kubectl get pods -n ${NAMESPACE} -l app=travel-planner"
echo " Logs:    kubectl logs -n ${NAMESPACE} -l app=travel-planner -f"
echo " GenAI:   curl http://localhost:8080/v1/genai/agents"
echo " Costs:   curl http://localhost:8080/v1/genai/costs"
echo " Spans:   curl http://localhost:8080/v1/genai/spans?limit=10"
echo ""
