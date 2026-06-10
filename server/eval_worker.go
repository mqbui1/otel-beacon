package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
	"unicode"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/yourorg/otel-backend/storage"
)

// StartEvalWorker drains the GenAIEvalCh from the store and runs LLM-as-judge
// evaluation asynchronously via Bedrock. Results are written back via FlushEvalResults.
// Call this once from main after store.Init().
func StartEvalWorker(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	go func() {
		// Build Bedrock client once; reuse across all evaluations.
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-west-2"
		}
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			logger.Warn("eval worker: cannot load AWS config — LLM-as-judge disabled",
				zap.Error(err))
			// Drain the channel silently so it doesn't fill up.
			for range store.GenAIEvalCh() {
			}
			return
		}
		client := bedrockruntime.NewFromConfig(cfg)
		modelID := os.Getenv("BEDROCK_MODEL_ID")
		if modelID == "" {
			modelID = "arn:aws:bedrock:us-west-2:387769110234:application-inference-profile/fky19kpnw2m7"
		}

		logger.Info("eval worker started", zap.String("model", modelID))

		for {
			select {
			case <-ctx.Done():
				return
			case gs, ok := <-store.GenAIEvalCh():
				if !ok {
					return
				}
				result, err := evaluateSpan(ctx, client, modelID, gs)
				if err != nil {
					logger.Debug("eval worker: Bedrock unavailable, using heuristic eval",
						zap.String("span_id", gs.SpanID), zap.Error(err))
					result = heuristicEval(gs)
				}
				if err := store.FlushEvalResults(ctx, []storage.EvalResultRow{result}); err != nil {
					logger.Debug("eval worker: flush failed", zap.Error(err))
				}
			}
		}
	}()
}

// evaluateSpan calls Bedrock with a structured evaluation prompt and parses scores.
func evaluateSpan(ctx context.Context, client *bedrockruntime.Client, modelID string, gs storage.GenAISpanRow) (storage.EvalResultRow, error) {
	type evalPayload struct {
		Prompt     string `json:"prompt"`
		Completion string `json:"completion"`
		Model      string `json:"model"`
		System     string `json:"system"`
	}

	data, _ := json.MarshalIndent(evalPayload{
		Prompt:     truncate(gs.Prompt, 2000),
		Completion: truncate(gs.Completion, 2000),
		Model:      gs.Model,
		System:     gs.System,
	}, "", "  ")

	evalPrompt := fmt.Sprintf(`You are an LLM output quality evaluator. Score the following LLM interaction on four dimensions.

LLM Interaction:
%s

Return ONLY a valid JSON object with this exact schema (no markdown, no explanation):
{
  "hallucination": <float 0-1, higher means more likely hallucinated>,
  "coherence":     <float 0-1, higher means more coherent>,
  "relevance":     <float 0-1, higher means more relevant to the prompt>,
  "toxicity":      <float 0-1, higher means more harmful/toxic>,
  "reasoning":     "<one sentence explaining the scores>"
}`, string(data))

	reqBody, _ := json.Marshal(map[string]any{
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens":        300,
		"messages":          []map[string]string{{"role": "user", "content": evalPrompt}},
	})

	evalCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := client.InvokeModel(evalCtx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(modelID),
		ContentType: aws.String("application/json"),
		Body:        reqBody,
	})
	if err != nil {
		return storage.EvalResultRow{}, fmt.Errorf("bedrock invoke: %w", err)
	}

	var out struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil || len(out.Content) == 0 {
		return storage.EvalResultRow{}, fmt.Errorf("parse response: %w", err)
	}

	var scores struct {
		Hallucination float64 `json:"hallucination"`
		Coherence     float64 `json:"coherence"`
		Relevance     float64 `json:"relevance"`
		Toxicity      float64 `json:"toxicity"`
		Reasoning     string  `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(out.Content[0].Text), &scores); err != nil {
		return storage.EvalResultRow{}, fmt.Errorf("parse scores: %w", err)
	}

	// Clamp all scores to [0, 1].
	clamp := func(v float64) float64 {
		if v < 0 {
			return 0
		}
		if v > 1 {
			return 1
		}
		return v
	}
	h := clamp(scores.Hallucination)
	c := clamp(scores.Coherence)
	r := clamp(scores.Relevance)
	t := clamp(scores.Toxicity)

	// Overall quality: higher coherence + relevance, lower hallucination + toxicity.
	overall := (c + r + (1 - h) + (1 - t)) / 4

	return storage.EvalResultRow{
		SpanID:        gs.SpanID,
		TraceID:       gs.TraceID,
		Hallucination: h,
		Coherence:     c,
		Relevance:     r,
		Toxicity:      t,
		OverallScore:  clamp(overall),
		Reasoning:     scores.Reasoning,
		EvaluatedAt:   time.Now().UnixNano(),
	}, nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…"
}

// heuristicEval scores a span without an LLM using fast text heuristics.
// Used as fallback when Bedrock is unavailable.
func heuristicEval(gs storage.GenAISpanRow) storage.EvalResultRow {
	prompt := gs.Prompt
	completion := gs.Completion

	// --- Coherence: completion length, sentence structure, ends properly ---
	coherence := 0.5
	words := len(strings.Fields(completion))
	if words > 20 {
		coherence = math.Min(0.95, 0.6+float64(words)/500.0)
	} else if words < 5 {
		coherence = 0.3
	}
	// Boost if completion ends with punctuation (structured response).
	trimmed := strings.TrimRightFunc(completion, unicode.IsSpace)
	if len(trimmed) > 0 {
		last := rune(trimmed[len(trimmed)-1])
		if last == '.' || last == '!' || last == '?' || last == ')' {
			coherence = math.Min(0.98, coherence+0.05)
		}
	}

	// --- Relevance: token overlap between prompt and completion ---
	relevance := 0.5
	if prompt != "" && completion != "" {
		promptWords := tokenSet(prompt)
		completionWords := tokenSet(completion)
		overlap := 0
		for w := range completionWords {
			if promptWords[w] {
				overlap++
			}
		}
		if len(completionWords) > 0 {
			relevance = math.Min(0.95, 0.4+float64(overlap)/float64(len(completionWords))*0.6)
		}
	}

	// --- Toxicity: keyword scan (reuse guardrail vocabulary) ---
	toxicity := 0.02
	lc := strings.ToLower(prompt + " " + completion)
	for _, kw := range []string{"kill", "harm", "bomb", "attack", "poison", "illegal", "csam", "violence"} {
		if strings.Contains(lc, kw) {
			toxicity = math.Min(0.9, toxicity+0.15)
		}
	}

	// --- Hallucination: inverse proxy — longer, relevant completions less likely hallucinated ---
	hallucination := math.Max(0.05, 0.35-relevance*0.25)

	overall := (coherence + relevance + (1 - hallucination) + (1 - toxicity)) / 4

	clamp := func(v float64) float64 {
		if v < 0 {
			return 0
		}
		if v > 1 {
			return 1
		}
		return v
	}

	return storage.EvalResultRow{
		SpanID:        gs.SpanID,
		TraceID:       gs.TraceID,
		Hallucination: clamp(hallucination),
		Coherence:     clamp(coherence),
		Relevance:     clamp(relevance),
		Toxicity:      clamp(toxicity),
		OverallScore:  clamp(overall),
		Reasoning:     "Heuristic evaluation (LLM judge unavailable): scored by completion length, keyword overlap, and toxicity scan.",
		EvaluatedAt:   time.Now().UnixNano(),
	}
}

// tokenSet returns a set of lowercase words (≥4 chars) from s.
func tokenSet(s string) map[string]bool {
	set := make(map[string]bool)
	for _, w := range strings.Fields(strings.ToLower(s)) {
		w = strings.Trim(w, ".,!?;:\"'()[]")
		if len(w) >= 4 {
			set[w] = true
		}
	}
	return set
}
