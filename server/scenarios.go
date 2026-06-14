package server

// ── Scenario simulation controller ───────────────────────────────────────────
// Exposes a small HTTP API so the UI can trigger rca_simulator runs without
// needing shell access. Runs Python as a subprocess — the image must have
// python3 + opentelemetry-sdk + pyyaml installed and rca_simulator at /app/.
//
// Routes (registered via registerScenarioRoutes):
//   GET    /v1/scenarios         → list available scenarios
//   POST   /v1/scenarios/run     → start a scenario run
//   DELETE /v1/scenarios/stop    → interrupt current run
//   GET    /v1/scenarios/status  → current run state
//
// To remove this feature: delete this file, remove registerScenarioRoutes call
// from query.go, and drop the Python layer from the Dockerfile.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

// scenarioMeta is the static catalogue served to the UI.
var scenarioMeta = []map[string]string{
	{
		"name":             "db_slowdown",
		"label":            "DB Slow Query",
		"description":      "Missing index on owners table causes a full table scan — visits-service P99 spikes to 8-12 s, api-gateway starts returning 504s.",
		"affected_service": "visits-service",
		"affected_op":      "SELECT owners",
		"signal":           "span_latency",
		"color":            "#f97316",
	},
	{
		"name":             "error_cascade",
		"label":            "Error Cascade",
		"description":      "Bug in customers-service JWT validation throws NPE for enterprise tokens — 500s cascade through api-gateway to all users.",
		"affected_service": "customers-service",
		"affected_op":      "user.validate",
		"signal":           "span_error_rate",
		"color":            "#ef4444",
	},
	{
		"name":             "cache_miss_storm",
		"label":            "Cache Miss Storm",
		"description":      "In-process Caffeine cache OOM kill — every vets request falls back to H2 DB, driving DB CPU to 100% and latency +200 ms.",
		"affected_service": "vets-service",
		"affected_op":      "GET vets:*",
		"signal":           "span_latency",
		"color":            "#f97316",
	},
	{
		"name":             "payment_timeout",
		"label":            "External Timeout",
		"description":      "Simulated external API partial outage in us-east-1 — visits-service hangs 30 s waiting for the response then returns 504.",
		"affected_service": "visits-service",
		"affected_op":      "external.charge",
		"signal":           "span_error_rate",
		"color":            "#ef4444",
	},
	{
		"name":             "latency_spike",
		"label":            "Latency Spike",
		"description":      "CPU saturation / GC pressure on visits-service — P99 climbs to 3-6 s with no errors. Silent degradation, harder to triage.",
		"affected_service": "visits-service",
		"affected_op":      "visit.schedule",
		"signal":           "span_latency",
		"color":            "#f97316",
	},
	{
		"name":             "oom_crash",
		"label":            "OOM Crash Loop",
		"description":      "vets-service JVM heap exhaustion from memory leak — OutOfMemoryError triggers pod restart loop with alternating error bursts.",
		"affected_service": "vets-service",
		"affected_op":      "vets.list",
		"signal":           "span_error_rate",
		"color":            "#ef4444",
	},
	{
		"name":             "cascading_failure",
		"label":            "Cascading Failure",
		"description":      "H2 database disk full — customers-service fails first, then api-gateway starts returning 500s to all callers including visits-service.",
		"affected_service": "customers-service",
		"affected_op":      "owner.get",
		"signal":           "span_error_rate",
		"color":            "#dc2626",
	},
	{
		"name":             "new_error_sig",
		"label":            "New Error Signature",
		"description":      "customers-service v2.4.0 introduces unhandled AuthorizationException for admin role — a brand-new exception never seen in traces before.",
		"affected_service": "customers-service",
		"affected_op":      "owner.update",
		"signal":           "error_signature",
		"color":            "#a855f7",
	},
	{
		"name":             "span_drop",
		"label":            "Span Drop",
		"description":      "OTel agent sampling misconfiguration on visits-service drops 90% of spans — service is alive but nearly invisible in traces.",
		"affected_service": "visits-service",
		"affected_op":      "visit.schedule",
		"signal":           "trace_drift",
		"color":            "#6b7280",
	},
	{
		"name":             "span_spike",
		"label":            "Traffic Surge",
		"description":      "10x traffic spike from a marketing campaign — all services hit simultaneously, DB starts queuing, P99 climbs, errors follow.",
		"affected_service": "api-gateway",
		"affected_op":      "GET /api/v1/owners",
		"signal":           "span_latency",
		"color":            "#eab308",
	},
	{
		"name":             "combined",
		"label":            "Combined Signals",
		"description":      "Two simultaneous incidents: visits-service latency spike (thread starvation) + vets-service error rate spike (bad feature flag).",
		"affected_service": "api-gateway",
		"affected_op":      "multiple",
		"signal":           "span_error_rate",
		"color":            "#ec4899",
	},
	{
		"name":             "kill_service",
		"label":            "Kill Service",
		"description":      "visits-service pod is killed — it emits no spans, callers get connection refused 503s, and the service disappears from the map.",
		"affected_service": "visits-service",
		"affected_op":      "visit.schedule",
		"signal":           "missing_service",
		"color":            "#6b7280",
	},
	{
		"name":             "new_call_path",
		"label":            "New Call Path",
		"description":      "A feature flag adds visits-service → audit-service — a brand-new topology edge never seen in baseline. Fires trace_drift with no errors.",
		"affected_service": "visits-service",
		"affected_op":      "audit.log",
		"signal":           "trace_drift",
		"color":            "#eab308",
		"default_warmup":   "360",
		"warmup_note":      "6 min warmup lets the fingerprint baseline establish before injecting the new edge",
	},
}

// ScenarioStatus is the JSON body returned by GET /v1/scenarios/status.
type ScenarioStatus struct {
	Running   bool   `json:"running"`
	Scenario  string `json:"scenario,omitempty"`
	Label     string `json:"label,omitempty"`
	Phase     string `json:"phase,omitempty"` // warmup | anomaly | cooldown
	ElapsedS  int    `json:"elapsed_s,omitempty"`
	TotalS    int    `json:"total_s,omitempty"`
	WarmupS   int    `json:"warmup_s,omitempty"`
	AnomalyS  int    `json:"anomaly_s,omitempty"`
	CooldownS int    `json:"cooldown_s,omitempty"`
	Requests  int    `json:"requests,omitempty"`
}

type scenarioManager struct {
	mu      sync.Mutex
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	status  ScenarioStatus
	startAt time.Time
	logger  *zap.Logger
	store   *storage.Storage
}

var _scMgr *scenarioManager

func registerScenarioRoutes(mux *http.ServeMux, store *storage.Storage, logger *zap.Logger) {
	_scMgr = &scenarioManager{logger: logger, store: store}
	mux.HandleFunc("/v1/scenarios", _scMgr.list)
	mux.HandleFunc("/v1/scenarios/run", _scMgr.run)
	mux.HandleFunc("/v1/scenarios/stop", _scMgr.stop)
	mux.HandleFunc("/v1/scenarios/status", _scMgr.statusHandler)
	mux.HandleFunc("/v1/scenarios/reset", _scMgr.reset)
}

func (m *scenarioManager) list(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"scenarios": scenarioMeta})
}

func (m *scenarioManager) statusHandler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	st := m.status
	if st.Running {
		elapsed := int(time.Since(m.startAt).Seconds())
		st.ElapsedS = elapsed
		switch {
		case elapsed < st.WarmupS:
			st.Phase = "warmup"
		case elapsed < st.WarmupS+st.AnomalyS:
			st.Phase = "anomaly"
		default:
			st.Phase = "cooldown"
		}
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

func (m *scenarioManager) run(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Scenario   string  `json:"scenario"`
		RPS        float64 `json:"rps"`
		WarmupS    int     `json:"warmup_s"`
		AnomalyS   int     `json:"anomaly_s"`
		CooldownS  int     `json:"cooldown_s"`
		AnomalyPct float64 `json:"anomaly_pct"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Scenario == "" {
		http.Error(w, "scenario is required", http.StatusBadRequest)
		return
	}
	// Apply defaults
	if req.RPS <= 0 {
		req.RPS = 3
	}
	if req.WarmupS <= 0 {
		req.WarmupS = 20
	}
	if req.AnomalyS <= 0 {
		req.AnomalyS = 90
	}
	if req.CooldownS <= 0 {
		req.CooldownS = 20
	}
	if req.AnomalyPct <= 0 {
		req.AnomalyPct = 0.8
	}

	m.mu.Lock()
	if m.status.Running {
		m.mu.Unlock()
		http.Error(w, "a scenario is already running", http.StatusConflict)
		return
	}
	m.mu.Unlock()

	// Resolve label for status display
	label := req.Scenario
	for _, s := range scenarioMeta {
		if s["name"] == req.Scenario {
			label = s["label"]
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(req.WarmupS+req.AnomalyS+req.CooldownS+60)*time.Second)

	args := []string{
		"-m", "rca_simulator", "run",
		"--scenario", req.Scenario,
		"--rps", fmt.Sprintf("%.1f", req.RPS),
		"--warmup", strconv.Itoa(req.WarmupS),
		"--anomaly", strconv.Itoa(req.AnomalyS),
		"--cooldown", strconv.Itoa(req.CooldownS),
		"--anomaly-pct", fmt.Sprintf("%.2f", req.AnomalyPct),
		"--topology-file", "/app/petclinic-topology.yaml",
	}

	cmd := exec.CommandContext(ctx, "python3", args...)
	cmd.Dir = "/app"
	cmd.Env = append(os.Environ(),
		"OTEL_COLLECTOR_ENDPOINT=http://localhost:4318",
		"PYTHONPATH=/app",
	)
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	m.mu.Lock()
	m.cmd = cmd
	m.cancel = cancel
	m.startAt = time.Now()
	m.status = ScenarioStatus{
		Running:   true,
		Scenario:  req.Scenario,
		Label:     label,
		Phase:     "warmup",
		WarmupS:   req.WarmupS,
		AnomalyS:  req.AnomalyS,
		CooldownS: req.CooldownS,
		TotalS:    req.WarmupS + req.AnomalyS + req.CooldownS,
	}
	m.mu.Unlock()

	if err := cmd.Start(); err != nil {
		cancel()
		m.mu.Lock()
		m.status = ScenarioStatus{}
		m.mu.Unlock()
		m.logger.Error("scenario start failed", zap.String("scenario", req.Scenario), zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	m.logger.Info("scenario started",
		zap.String("scenario", req.Scenario),
		zap.Int("warmup_s", req.WarmupS),
		zap.Int("anomaly_s", req.AnomalyS),
		zap.Int("cooldown_s", req.CooldownS))

	go func() {
		defer cancel()
		buf := make([]byte, 512)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				line := string(buf[:n])
				if idx := strings.Index(line, "requests="); idx >= 0 {
					rest := line[idx+9:]
					if end := strings.IndexAny(rest, " \n\r"); end > 0 {
						rest = rest[:end]
					}
					if v, err2 := strconv.Atoi(strings.TrimSpace(rest)); err2 == nil {
						m.mu.Lock()
						m.status.Requests = v
						m.mu.Unlock()
					}
				}
			}
			if err != nil {
				break
			}
		}
		cmd.Wait()
		m.mu.Lock()
		m.status = ScenarioStatus{}
		m.mu.Unlock()
		m.logger.Info("scenario completed", zap.String("scenario", req.Scenario))
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "started", "scenario": req.Scenario})
}

func (m *scenarioManager) stop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "DELETE or POST required", http.StatusMethodNotAllowed)
		return
	}
	m.mu.Lock()
	cancel := m.cancel
	cmd := m.cmd
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Signal(os.Interrupt)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func (m *scenarioManager) reset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "POST or DELETE required", http.StatusMethodNotAllowed)
		return
	}
	// Stop any running scenario first
	m.mu.Lock()
	cancel := m.cancel
	cmd := m.cmd
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Signal(os.Interrupt)
	}

	if err := m.store.ResetSimulationData(r.Context()); err != nil {
		m.logger.Error("reset simulation data", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	m.logger.Info("simulation data reset")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}
