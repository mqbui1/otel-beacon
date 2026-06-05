package storage

import "encoding/json"

// DetectorSpec defines a named detector auto-provisioned for a service entity.
type DetectorSpec struct {
	Name       string   // human-readable name
	TechStacks []string // nil = all services; otherwise only matching stacks
	SignalType string   // "metric"|"span_error_rate"|"span_latency"
	MetricName string   // for metric signal_type
	WarnMult   float64  // dynamic: warn = mean + WarnMult*stddev
	CritMult   float64  // dynamic: crit = mean + CritMult*stddev
	FixedWarn  float64  // used when insufficient baseline or ThresholdFixed
	FixedCrit  float64
	ThreshType string // "dynamic"|"fixed"|"hybrid"
}

// BuiltinCatalog is the set of detectors auto-provisioned for every discovered service.
var BuiltinCatalog = []DetectorSpec{
	// ── APM — all services ────────────────────────────────────────────────────
	{Name: "Span Error Rate", SignalType: "span_error_rate",
		WarnMult: 2, CritMult: 3, FixedWarn: 0.01, FixedCrit: 0.05, ThreshType: "dynamic"},
	{Name: "P95 Latency Anomaly", SignalType: "span_latency",
		WarnMult: 2, CritMult: 3, FixedWarn: 1000, FixedCrit: 3000, ThreshType: "dynamic"},

	// ── Container — all services ──────────────────────────────────────────────
	{Name: "CPU Utilization High", SignalType: "metric", MetricName: "container.cpu.usage",
		FixedWarn: 0.7, FixedCrit: 0.9, ThreshType: "fixed"},
	{Name: "Memory RSS High", SignalType: "metric", MetricName: "container.memory.rss",
		WarnMult: 2.5, CritMult: 3.5, ThreshType: "dynamic"},

	// ── JVM ───────────────────────────────────────────────────────────────────
	{Name: "JVM Heap High", TechStacks: []string{"jvm"},
		SignalType: "metric", MetricName: "jvm.memory.heap.used",
		FixedWarn: 0.75, FixedCrit: 0.90, ThreshType: "fixed"},
	{Name: "JVM GC Duration High", TechStacks: []string{"jvm"},
		SignalType: "metric", MetricName: "jvm.gc.duration",
		FixedWarn: 200, FixedCrit: 500, ThreshType: "fixed"},
	{Name: "JVM Thread Count High", TechStacks: []string{"jvm"},
		SignalType: "metric", MetricName: "jvm.threads.count",
		FixedWarn: 200, FixedCrit: 500, ThreshType: "fixed"},

	// ── Go runtime ────────────────────────────────────────────────────────────
	{Name: "Goroutine Count High", TechStacks: []string{"go"},
		SignalType: "metric", MetricName: "process.runtime.go.goroutines",
		WarnMult: 2.5, CritMult: 4, FixedWarn: 1000, ThreshType: "dynamic"},
	{Name: "Go GC Pause High", TechStacks: []string{"go"},
		SignalType: "metric", MetricName: "process.runtime.go.gc.pause_total_ns",
		WarnMult: 2, CritMult: 3, FixedWarn: 50, ThreshType: "dynamic"},

	// ── Node.js ───────────────────────────────────────────────────────────────
	{Name: "Event Loop Lag High", TechStacks: []string{"nodejs"},
		SignalType: "metric", MetricName: "process.runtime.nodejs.event_loop.delay",
		FixedWarn: 100, FixedCrit: 500, ThreshType: "fixed"},

	// ── Kubernetes ────────────────────────────────────────────────────────────
	{Name: "Pod CPU Throttling", SignalType: "metric",
		MetricName: "k8s.pod.cpu_limit_utilization",
		FixedWarn: 0.8, FixedCrit: 0.95, ThreshType: "fixed"},
}

// InferTechStack returns tech stack tags for a service entity based on its attrs JSON.
func InferTechStack(attrsJSON string) []string {
	var attrs map[string]any
	if err := json.Unmarshal([]byte(attrsJSON), &attrs); err != nil {
		return nil
	}
	var stacks []string
	lang, _ := attrs["telemetry.sdk.language"].(string)
	switch lang {
	case "java":
		stacks = append(stacks, "jvm")
	case "go":
		stacks = append(stacks, "go")
	case "nodejs", "javascript":
		stacks = append(stacks, "nodejs")
	case "python":
		stacks = append(stacks, "python")
	case "dotnet":
		stacks = append(stacks, "dotnet")
	}
	return stacks
}

// SpecsForEntity returns the detector specs applicable to a service's tech stacks.
func SpecsForEntity(attrsJSON string) []DetectorSpec {
	stacks := InferTechStack(attrsJSON)
	stackSet := make(map[string]bool, len(stacks))
	for _, s := range stacks {
		stackSet[s] = true
	}
	var out []DetectorSpec
	for _, spec := range BuiltinCatalog {
		if len(spec.TechStacks) == 0 {
			out = append(out, spec)
			continue
		}
		for _, ts := range spec.TechStacks {
			if stackSet[ts] {
				out = append(out, spec)
				break
			}
		}
	}
	return out
}
