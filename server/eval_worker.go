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

// StartSessionEvalWorker drains the SessionEvalCh from the store and runs
// session-level LLM-as-judge evaluation asynchronously.
// Backend selection: EVAL_BACKEND=ollama|heuristic|bedrock (default bedrock).
// Call this once from main after store.Init().
func StartSessionEvalWorker(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	go func() {
		backend := os.Getenv("EVAL_BACKEND")

		var client *bedrockruntime.Client
		var modelID string

		if backend != "ollama" && backend != "heuristic" {
			region := os.Getenv("AWS_REGION")
			if region == "" {
				region = "us-west-2"
			}
			cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
			if err != nil {
				logger.Warn("session eval worker: cannot load AWS config — using heuristic", zap.Error(err))
				backend = "heuristic"
			} else {
				client = bedrockruntime.NewFromConfig(cfg)
				modelID = os.Getenv("BEDROCK_MODEL_ID")
				if modelID == "" {
					modelID = "arn:aws:bedrock:us-west-2:387769110234:application-inference-profile/fky19kpnw2m7"
				}
				backend = "bedrock"
			}
		}
		logger.Info("session eval worker started", zap.String("backend", backend))

		for {
			select {
			case <-ctx.Done():
				return
			case sess, ok := <-store.SessionEvalCh():
				if !ok {
					return
				}
				spans, _ := store.QueryGenAISpans(ctx, storage.GenAIQuery{
					SessionID: sess.SessionID,
					Limit:     200,
				})

				var result storage.SessionRow
				var err error
				switch backend {
				case "ollama":
					result, err = callOllamaForSession(ctx, sess, spans)
				case "bedrock":
					result, err = evaluateSession(ctx, client, modelID, sess, spans)
				}
				if backend == "heuristic" || err != nil {
					if err != nil {
						logger.Debug("session eval: primary failed, using heuristic",
							zap.String("session_id", sess.SessionID), zap.Error(err))
					}
					result = heuristicSessionEval(sess, spans)
				}
				if err := store.FlushSessionEval(ctx, result); err != nil {
					logger.Debug("session eval: flush failed", zap.Error(err))
				}
			}
		}
	}()
}

func evaluateSession(ctx context.Context, client *bedrockruntime.Client, modelID string, sess storage.SessionRow, spans []storage.GenAISpanRow) (storage.SessionRow, error) {
	transcript := buildSessionTranscript(sess, spans)

	evalPrompt := fmt.Sprintf(`You are evaluating a multi-step AI agent session. Score on four dimensions.

Session Transcript:
%s

Return ONLY a valid JSON object (no markdown, no explanation):
{
  "action_completion":  <float 0-1, did the agent successfully complete all user goals?>,
  "agent_efficiency":   <float 0-1, did the agent take an efficient path with minimal redundancy?>,
  "conv_quality":       <float 0-1, overall quality and apparent user satisfaction>,
  "user_intent_change": <float 0-1, did the user's primary goal shift significantly during the session?>,
  "reasoning": "<one sentence>"
}`, transcript)

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
		return storage.SessionRow{}, fmt.Errorf("bedrock invoke: %w", err)
	}

	var out struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil || len(out.Content) == 0 {
		return storage.SessionRow{}, fmt.Errorf("parse response: %w", err)
	}

	var scores struct {
		ActionCompletion  float64 `json:"action_completion"`
		AgentEfficiency   float64 `json:"agent_efficiency"`
		ConvQuality       float64 `json:"conv_quality"`
		UserIntentChange  float64 `json:"user_intent_change"`
		Reasoning         string  `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(out.Content[0].Text), &scores); err != nil {
		return storage.SessionRow{}, fmt.Errorf("parse scores: %w", err)
	}

	clamp := func(v float64) float64 {
		if v < 0 { return 0 }
		if v > 1 { return 1 }
		return v
	}
	sess.ActionCompletion = clamp(scores.ActionCompletion)
	sess.AgentEfficiency  = clamp(scores.AgentEfficiency)
	sess.ConvQuality      = clamp(scores.ConvQuality)
	sess.UserIntentChange = clamp(scores.UserIntentChange)
	sess.EvalReasoning    = scores.Reasoning
	sess.EvalAt           = time.Now().UnixNano()
	return sess, nil
}

// buildSessionTranscript assembles a readable transcript from session spans.
func buildSessionTranscript(sess storage.SessionRow, spans []storage.GenAISpanRow) string {
	if len(spans) == 0 {
		return fmt.Sprintf("Session %s: no spans recorded.", sess.SessionID)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "Session: %s | Traces: %d | Spans: %d | Cost: $%.4f\n\n",
		sess.SessionID[:min(16, len(sess.SessionID))], sess.TraceCount, sess.SpanCount, sess.TotalCostUSD)

	// Group spans by trace, preserving insertion order.
	traceOrder := make([]string, 0)
	traceSpans := make(map[string][]storage.GenAISpanRow)
	for _, sp := range spans {
		if _, seen := traceSpans[sp.TraceID]; !seen {
			traceOrder = append(traceOrder, sp.TraceID)
		}
		traceSpans[sp.TraceID] = append(traceSpans[sp.TraceID], sp)
	}

	for i, tid := range traceOrder {
		fmt.Fprintf(&sb, "[Trace %d — %s]\n", i+1, tid[:min(16, len(tid))])
		for _, sp := range traceSpans[tid] {
			agent := sp.AgentName
			if agent == "" { agent = sp.Operation }
			if agent == "" { agent = "llm" }
			prompt := truncate(sp.Prompt, 300)
			completion := truncate(sp.Completion, 300)
			if prompt != "" || completion != "" {
				fmt.Fprintf(&sb, "  [%s] P: %s\n        C: %s\n", agent, prompt, completion)
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

func min(a, b int) int {
	if a < b { return a }
	return b
}

// heuristicSessionEval scores a session without an LLM.
func heuristicSessionEval(sess storage.SessionRow, spans []storage.GenAISpanRow) storage.SessionRow {
	// Action completion: fraction of spans without errors.
	errorCount := 0
	for _, sp := range spans {
		if sp.StatusCode == 2 { // ERROR
			errorCount++
		}
	}
	actionCompletion := 1.0
	if len(spans) > 0 {
		actionCompletion = 1.0 - float64(errorCount)/float64(len(spans))
	}

	// Agent efficiency: inversely proportional to span count (more spans = less efficient).
	// Assume 5 spans is ideal (coordinator + 4 specialists), penalise beyond 20.
	agentEfficiency := math.Max(0.2, 1.0-math.Max(0, float64(sess.SpanCount)-5)/30.0)

	// Conversation quality: proxy via action completion and efficiency.
	convQuality := (actionCompletion + agentEfficiency) / 2.0

	sess.ActionCompletion = math.Min(1.0, actionCompletion)
	sess.AgentEfficiency  = math.Min(1.0, agentEfficiency)
	sess.ConvQuality      = math.Min(1.0, convQuality)
	sess.UserIntentChange = 0.1
	sess.EvalReasoning    = "Heuristic session eval: scored by error rate, span count, and efficiency proxy."
	sess.EvalAt           = time.Now().UnixNano()
	return sess
}

// StartEvalWorker drains the GenAIEvalCh from the store and runs LLM-as-judge
// evaluation asynchronously. Results are written back via FlushEvalResults.
// Backend selection: EVAL_BACKEND=ollama|heuristic|bedrock (default bedrock).
// Call this once from main after store.Init().
func StartEvalWorker(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	go func() {
		backend := os.Getenv("EVAL_BACKEND")

		var client *bedrockruntime.Client
		var modelID string

		if backend != "ollama" && backend != "heuristic" {
			region := os.Getenv("AWS_REGION")
			if region == "" {
				region = "us-west-2"
			}
			cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
			if err != nil {
				logger.Warn("eval worker: cannot load AWS config — using heuristic", zap.Error(err))
				backend = "heuristic"
			} else {
				client = bedrockruntime.NewFromConfig(cfg)
				modelID = os.Getenv("BEDROCK_MODEL_ID")
				if modelID == "" {
					modelID = "arn:aws:bedrock:us-west-2:387769110234:application-inference-profile/fky19kpnw2m7"
				}
				backend = "bedrock"
			}
		}
		logger.Info("eval worker started", zap.String("backend", backend))

		for {
			select {
			case <-ctx.Done():
				return
			case gs, ok := <-store.GenAIEvalCh():
				if !ok {
					return
				}
				var result storage.EvalResultRow
				var err error
				switch backend {
				case "ollama":
					result, err = callOllamaForSpan(ctx, gs)
				case "bedrock":
					result, err = evaluateSpan(ctx, client, modelID, gs)
				}
				if backend == "heuristic" || err != nil {
					if err != nil {
						logger.Debug("eval worker: primary failed, using heuristic",
							zap.String("span_id", gs.SpanID), zap.Error(err))
					}
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

	evalPrompt := fmt.Sprintf(`You are an LLM output quality evaluator. Score the following LLM interaction on eight dimensions.

LLM Interaction:
%s

Return ONLY a valid JSON object with this exact schema (no markdown, no explanation):
{
  "hallucination":         <float 0-1, higher means more likely hallucinated or factually wrong>,
  "coherence":             <float 0-1, higher means more coherent and well-structured>,
  "relevance":             <float 0-1, higher means more relevant to the prompt>,
  "toxicity":              <float 0-1, higher means more harmful or toxic>,
  "correctness":           <float 0-1, higher means more factually accurate>,
  "instruction_adherence": <float 0-1, higher means the model followed the system prompt instructions better>,
  "reasoning_coherence":   <float 0-1, higher means reasoning steps are more logically consistent>,
  "completeness":          <float 0-1, higher means the response addresses all parts of the query>,
  "reasoning":             "<one sentence explaining the scores>"
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
		Hallucination        float64 `json:"hallucination"`
		Coherence            float64 `json:"coherence"`
		Relevance            float64 `json:"relevance"`
		Toxicity             float64 `json:"toxicity"`
		Correctness          float64 `json:"correctness"`
		InstructionAdherence float64 `json:"instruction_adherence"`
		ReasoningCoherence   float64 `json:"reasoning_coherence"`
		Completeness         float64 `json:"completeness"`
		Reasoning            string  `json:"reasoning"`
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
	cor := clamp(scores.Correctness)
	ia := clamp(scores.InstructionAdherence)
	rc := clamp(scores.ReasoningCoherence)
	comp := clamp(scores.Completeness)

	// Overall quality: average of positive signals minus penalties.
	overall := (c + r + cor + ia + rc + comp + (1 - h) + (1 - t)) / 8

	return storage.EvalResultRow{
		SpanID:               gs.SpanID,
		TraceID:              gs.TraceID,
		Hallucination:        h,
		Coherence:            c,
		Relevance:            r,
		Toxicity:             t,
		Correctness:          cor,
		InstructionAdherence: ia,
		ReasoningCoherence:   rc,
		Completeness:         comp,
		OverallScore:         clamp(overall),
		Reasoning:            scores.Reasoning,
		EvaluatedAt:          time.Now().UnixNano(),
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

	// --- Correctness: proxy via relevance + coherence (heuristic only) ---
	correctness := math.Min(0.9, (relevance+coherence)/2)

	// --- Instruction Adherence: if system prompt present and completion is long, likely followed ---
	instructionAdherence := 0.5
	if gs.System != "" && words > 20 {
		instructionAdherence = math.Min(0.85, 0.55+float64(words)/600.0)
	}

	// --- Reasoning Coherence: proxy via coherence score ---
	reasoningCoherence := math.Min(0.9, coherence*0.95)

	// --- Completeness: proxy via word count relative to prompt length ---
	completeness := 0.5
	if len(strings.Fields(prompt)) > 0 {
		ratio := float64(words) / math.Max(1, float64(len(strings.Fields(prompt))))
		completeness = math.Min(0.9, 0.4+ratio*0.15)
	}

	overall := (coherence + relevance + correctness + instructionAdherence + reasoningCoherence + completeness + (1 - hallucination) + (1 - toxicity)) / 8

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
		SpanID:               gs.SpanID,
		TraceID:              gs.TraceID,
		Hallucination:        clamp(hallucination),
		Coherence:            clamp(coherence),
		Relevance:            clamp(relevance),
		Toxicity:             clamp(toxicity),
		Correctness:          clamp(correctness),
		InstructionAdherence: clamp(instructionAdherence),
		ReasoningCoherence:   clamp(reasoningCoherence),
		Completeness:         clamp(completeness),
		OverallScore:         clamp(overall),
		Reasoning:            "Heuristic evaluation (LLM judge unavailable): scored by completion length, keyword overlap, and toxicity scan.",
		EvaluatedAt:          time.Now().UnixNano(),
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
