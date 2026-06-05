#!/usr/bin/env python3
"""Send synthetic OTLP traces, metrics, and logs to the otel-beacon backend."""
import json, random, struct, time, urllib.request

OTLP_ENDPOINT = "http://localhost:4318"
QUERY_ENDPOINT = "http://localhost:8080"
SERVICES = ["frontend", "payment-service", "cart-service", "inventory-service", "shipping-service"]
ROUNDS = 130  # need 100+ points per (entity,metric) to trigger MAD detector

def uid(n=16): return ''.join(f'{random.randint(0,255):02x}' for _ in range(n))
def ns(): return int(time.time() * 1e9)

def post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(f"{OTLP_ENDPOINT}{path}", data=data,
                                  headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"  warn: {path} -> {e}")

# ── Traces ────────────────────────────────────────────────────────────────────
# Topology: frontend → cart-service → inventory-service
#                    → payment-service
#           frontend → shipping-service
CALL_GRAPH = {
    "frontend":           ["cart-service", "payment-service", "shipping-service"],
    "cart-service":       ["inventory-service"],
    "payment-service":    [],
    "inventory-service":  [],
    "shipping-service":   [],
}

def make_resource_attrs(svc):
    node = "node-1" if svc in ("frontend", "payment-service") else "node-2"
    return [
        {"key": "service.name",        "value": {"stringValue": svc}},
        {"key": "k8s.pod.name",        "value": {"stringValue": f"{svc}-pod-abc"}},
        {"key": "k8s.deployment.name", "value": {"stringValue": svc}},
        {"key": "k8s.namespace.name",  "value": {"stringValue": "default"}},
        {"key": "k8s.node.name",       "value": {"stringValue": node}},
    ]

def send_traces(svc, error=False):
    """Send a root span for svc plus child spans for each downstream call."""
    trace_id = uid(16)
    root_span_id = uid(8)
    t = ns()
    root_dur = random.randint(20_000_000, 120_000_000)
    status_code = 2 if error else 1

    resource_spans = [{"resource": {"attributes": make_resource_attrs(svc)}, "scopeSpans": [{"scope": {"name": "test"}, "spans": [{
        "traceId":           trace_id,
        "spanId":            root_span_id,
        "parentSpanId":      "0000000000000000",
        "name":              f"{svc}/handle",
        "kind":              2,  # SERVER
        "startTimeUnixNano": str(t),
        "endTimeUnixNano":   str(t + root_dur),
        "status":            {"code": status_code, "message": "error" if error else ""},
        "attributes":        [{"key": "http.method", "value": {"stringValue": "POST"}}],
    }]}]}]

    # Add CLIENT spans for each downstream service (creates cross-service topology edges)
    for downstream in CALL_GRAPH.get(svc, []):
        child_span_id = uid(8)
        child_dur = random.randint(5_000_000, 60_000_000)
        child_err = error and (downstream == "payment-service")
        resource_spans.append({"resource": {"attributes": make_resource_attrs(downstream)}, "scopeSpans": [{"scope": {"name": "test"}, "spans": [{
            "traceId":           trace_id,
            "spanId":            child_span_id,
            "parentSpanId":      root_span_id,
            "name":              f"{downstream}/handle",
            "kind":              2,  # SERVER
            "startTimeUnixNano": str(t + random.randint(1_000_000, 5_000_000)),
            "endTimeUnixNano":   str(t + child_dur),
            "status":            {"code": 2 if child_err else 1, "message": "error" if child_err else ""},
            "attributes":        [{"key": "http.method", "value": {"stringValue": "GET"}}],
        }]}]})

    post("/v1/traces", {"resourceSpans": resource_spans})

# ── Metrics ───────────────────────────────────────────────────────────────────
def send_metrics(svc, cpu_spike=False, mem_spike=False):
    t = ns()
    cpu = random.uniform(0.7, 0.95) if cpu_spike else random.uniform(0.05, 0.25)
    mem = random.randint(400_000_000, 600_000_000) if mem_spike else random.randint(50_000_000, 150_000_000)
    latency = random.uniform(200, 500) if cpu_spike else random.uniform(10, 60)

    res_attrs = [
        {"key": "service.name",        "value": {"stringValue": svc}},
        {"key": "k8s.pod.name",        "value": {"stringValue": f"{svc}-pod-abc"}},
        {"key": "k8s.deployment.name", "value": {"stringValue": svc}},
    ]

    def gauge(name, unit, val):
        return {"name": name, "unit": unit, "gauge": {"dataPoints": [{
            "timeUnixNano": str(t), "asDouble": val,
            "attributes": [{"key": "k8s.pod.name", "value": {"stringValue": f"{svc}-pod-abc"}}],
        }]}}

    body = {"resourceMetrics": [{"resource": {"attributes": res_attrs}, "scopeMetrics": [{"scope": {"name": "test"}, "metrics": [
        gauge("container.cpu.usage",      "1",  cpu),
        gauge("container.memory.rss",     "By", float(mem)),
        gauge("http.server.duration",     "ms", latency),
    ]}]}]}
    post("/v1/metrics", body)

# ── Logs ──────────────────────────────────────────────────────────────────────
def send_logs(svc, level="INFO"):
    t = ns()
    body = {"resourceLogs": [{"resource": {"attributes": [
        {"key": "service.name",        "value": {"stringValue": svc}},
        {"key": "k8s.pod.name",        "value": {"stringValue": f"{svc}-pod-abc"}},
        {"key": "k8s.deployment.name", "value": {"stringValue": svc}},
    ]}, "scopeLogs": [{"scope": {"name": "test"}, "logRecords": [{
        "timeUnixNano": str(t),
        "severityText": level,
        "body": {"stringValue": f"[{level}] {svc}: {'connection refused to db' if level=='ERROR' else 'request processed'}"},
    }]}]}]}
    post("/v1/logs", body)

# ── Main ──────────────────────────────────────────────────────────────────────
print(f"Sending {ROUNDS} rounds × {len(SERVICES)} services …")
for i in range(ROUNDS):
    for svc in SERVICES:
        spike = (svc == "payment-service") and (i > 100)
        err   = (svc == "payment-service") and (i > 110)
        send_traces(svc, error=err)
        send_metrics(svc, cpu_spike=spike, mem_spike=spike)
    if i % 20 == 0:
        print(f"  round {i}/{ROUNDS}")
    time.sleep(0.05)

# Send some error logs
for svc in SERVICES:
    for _ in range(5):
        send_logs(svc, "ERROR")
    for _ in range(3):
        send_logs(svc, "WARN")
    send_logs(svc, "INFO")

print("Done. Waiting 15s for background workers to process anomalies…")
time.sleep(15)

# Report
def get(path):
    try:
        r = urllib.request.urlopen(f"{QUERY_ENDPOINT}{path}", timeout=5)
        return json.loads(r.read())
    except:
        return {}

spans    = get("/v1/query/spans?limit=1").get("count", 0)
metrics  = get("/v1/query/metrics?limit=1").get("count", 0)
logs     = get("/v1/query/logs?limit=1").get("count", 0)
anomalies= get("/v1/query/anomalies?limit=1").get("count", 0)
incidents= get("/v1/incidents").get("count", 0)
print(f"spans={spans} | metrics={metrics} | logs={logs} | anomalies={anomalies} | incidents={incidents}")
