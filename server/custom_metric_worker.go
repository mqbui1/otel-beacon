package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/yourorg/otel-backend/storage"
)

// EvalCustomMetric runs one custom metric against a span and stores the result.
// client/modelID may be nil/"" — Ollama or heuristic will be used as fallback.
// If action=block and the boolean metric fires, also writes a GuardrailEventRow.
func EvalCustomMetric(ctx context.Context, m storage.CustomMetricDef, gs storage.GenAISpanRow,
	client *bedrockruntime.Client, modelID string,
	store *storage.Storage, logger *zap.Logger) error {

	result := runCustomMetricEval(ctx, m, gs, client, modelID)

	if err := store.SaveCustomMetricResult(ctx, result); err != nil {
		return fmt.Errorf("save custom metric result: %w", err)
	}

	// Block action: boolean metric fired → write guardrail event.
	if m.Action == "block" && result.ValueBool != nil && *result.ValueBool {
		ge := storage.GuardrailEventRow{
			TraceID:   gs.TraceID,
			SpanID:    gs.SpanID,
			CheckType: "custom_metric:" + m.Name,
			Triggered: true,
			Severity:  "high",
			Detail:    "[block] " + result.Reasoning,
			CheckedAt: time.Now().UnixNano(),
		}
		if err := store.FlushGuardrailEvents(ctx, []storage.GuardrailEventRow{ge}); err != nil {
			logger.Debug("flush guardrail event for custom metric", zap.Error(err))
		}
	}
	return nil
}

// EvalCustomMetricsForSpan evaluates all active custom metrics against a span.
// Called from the eval worker after a span arrives.
func EvalCustomMetricsForSpan(ctx context.Context, gs storage.GenAISpanRow,
	client *bedrockruntime.Client, modelID string,
	store *storage.Storage, logger *zap.Logger) {

	metrics, err := store.ListCustomMetrics(ctx)
	if err != nil || len(metrics) == 0 {
		return
	}
	for _, m := range metrics {
		if m.ApplyTo != "span" {
			continue
		}
		_ = EvalCustomMetric(ctx, m, gs, client, modelID, store, logger)
	}
}

// RunCustomMetricOnRecentSpans evaluates a metric against the N most recent spans.
// Called from the HTTP "Run" handler — initialises its own Bedrock client.
func RunCustomMetricOnRecentSpans(ctx context.Context, m storage.CustomMetricDef,
	store *storage.Storage, logger *zap.Logger, limit int) ([]storage.CustomMetricResult, error) {

	if limit <= 0 {
		limit = 50
	}

	var client *bedrockruntime.Client
	var modelID string
	if !ollamaEnabled() {
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-west-2"
		}
		cfg, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(region))
		if err == nil {
			client = bedrockruntime.NewFromConfig(cfg)
			modelID = os.Getenv("BEDROCK_MODEL_ID")
			if modelID == "" {
				modelID = "arn:aws:bedrock:us-west-2:387769110234:application-inference-profile/fky19kpnw2m7"
			}
		}
	}

	spans, err := store.QueryGenAISpans(ctx, storage.GenAIQuery{Limit: limit})
	if err != nil {
		return nil, err
	}

	var results []storage.CustomMetricResult
	for _, gs := range spans {
		result := runCustomMetricEval(ctx, m, gs, client, modelID)
		if err := store.SaveCustomMetricResult(ctx, result); err != nil {
			logger.Debug("save custom metric result", zap.Error(err))
		}
		results = append(results, result)
	}
	return results, nil
}

// runCustomMetricEval is the core eval: Ollama → Bedrock → heuristic.
func runCustomMetricEval(ctx context.Context, m storage.CustomMetricDef, gs storage.GenAISpanRow,
	client *bedrockruntime.Client, modelID string) storage.CustomMetricResult {

	resultID := fmt.Sprintf("cmr-%s-%s", m.MetricID, gs.SpanID)
	result := storage.CustomMetricResult{
		ResultID:    resultID,
		MetricID:    m.MetricID,
		MetricName:  m.Name,
		SpanID:      gs.SpanID,
		TraceID:     gs.TraceID,
		EvaluatedAt: time.Now().UnixNano(),
	}

	scores, err := callCustomMetricLLM(ctx, m, gs, client, modelID)
	if err != nil {
		// Heuristic fallback: condition not detected.
		f := false
		result.ValueBool = &f
		result.ValueScore = 0
		result.Reasoning = "Heuristic fallback (LLM unavailable): condition not detected."
		return result
	}

	if m.OutputType == "boolean" {
		result.ValueBool = &scores.Result
		if scores.Result {
			result.ValueScore = 1
		}
	} else {
		result.ValueScore = scores.Score
	}
	result.Reasoning = scores.Reasoning
	return result
}

type customMetricScores struct {
	Result    bool
	Score     float64
	Reasoning string
}

func callCustomMetricLLM(ctx context.Context, m storage.CustomMetricDef, gs storage.GenAISpanRow,
	client *bedrockruntime.Client, modelID string) (customMetricScores, error) {

	payload, _ := json.Marshal(map[string]string{
		"prompt":     truncate(gs.Prompt, 1500),
		"completion": truncate(gs.Completion, 1500),
	})

	var schemaHint string
	if m.OutputType == "boolean" {
		schemaHint = `{"result": true, "reasoning": "one sentence"}`
	} else {
		schemaHint = `{"result": 0.75, "reasoning": "one sentence"}`
	}

	evalPrompt := fmt.Sprintf(`You are an LLM output quality evaluator applying a custom evaluation criterion.

Custom criterion:
%s

LLM Interaction:
%s

Return ONLY valid JSON:
%s`, m.Prompt, string(payload), schemaHint)

	var raw struct {
		Result    any    `json:"result"`
		Reasoning string `json:"reasoning"`
	}

	var callErr error
	if ollamaEnabled() {
		callErr = callOllamaJSON(ctx, evalPrompt, &raw)
	} else if client != nil {
		callErr = callBedrockCustom(ctx, client, modelID, evalPrompt, &raw)
	} else {
		callErr = fmt.Errorf("no LLM backend available")
	}

	if callErr != nil {
		return customMetricScores{}, callErr
	}

	var s customMetricScores
	s.Reasoning = raw.Reasoning
	switch v := raw.Result.(type) {
	case bool:
		s.Result = v
		if v {
			s.Score = 1
		}
	case float64:
		s.Score = v
		s.Result = v >= 0.5
	case string:
		s.Result = v == "true" || v == "1"
		if s.Result {
			s.Score = 1
		}
	}
	return s, nil
}

func callBedrockCustom(ctx context.Context, client *bedrockruntime.Client, modelID, prompt string, dst any) error {
	reqBody, _ := json.Marshal(map[string]any{
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens":        200,
		"messages":          []map[string]string{{"role": "user", "content": prompt}},
	})
	tCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := client.InvokeModel(tCtx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(modelID),
		ContentType: aws.String("application/json"),
		Body:        reqBody,
	})
	if err != nil {
		return err
	}
	var out struct {
		Content []struct{ Text string `json:"text"` } `json:"content"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil || len(out.Content) == 0 {
		return fmt.Errorf("parse bedrock response")
	}
	return json.Unmarshal([]byte(out.Content[0].Text), dst)
}
