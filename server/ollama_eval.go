package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/yourorg/otel-backend/storage"
)

// ollamaEnabled reports whether Ollama is configured as the eval backend.
func ollamaEnabled() bool {
	return os.Getenv("EVAL_BACKEND") == "ollama"
}

func ollamaBaseURL() string {
	if u := os.Getenv("OLLAMA_URL"); u != "" {
		return u
	}
	return "http://localhost:11434"
}

func ollamaModelName() string {
	if m := os.Getenv("OLLAMA_MODEL"); m != "" {
		return m
	}
	return "llama3.2:3b"
}

// callOllamaJSON sends prompt to Ollama /api/generate with format=json and
// unmarshals the model's JSON response into dst.
func callOllamaJSON(ctx context.Context, prompt string, dst any) error {
	type reqBody struct {
		Model  string `json:"model"`
		Prompt string `json:"prompt"`
		Stream bool   `json:"stream"`
		Format string `json:"format"`
	}
	type respBody struct {
		Response string `json:"response"`
	}

	body, _ := json.Marshal(reqBody{
		Model:  ollamaModelName(),
		Prompt: prompt,
		Stream: false,
		Format: "json",
	})

	tCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(tCtx, http.MethodPost,
		ollamaBaseURL()+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	r, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("ollama: %w", err)
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama: status %d", r.StatusCode)
	}

	var out respBody
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		return fmt.Errorf("ollama decode: %w", err)
	}
	if err := json.Unmarshal([]byte(out.Response), dst); err != nil {
		return fmt.Errorf("ollama parse scores: %w", err)
	}
	return nil
}

// callOllamaForSpan runs the 8-dimension span eval via Ollama.
func callOllamaForSpan(ctx context.Context, gs storage.GenAISpanRow) (storage.EvalResultRow, error) {
	data, _ := json.MarshalIndent(map[string]string{
		"prompt":     truncate(gs.Prompt, 2000),
		"completion": truncate(gs.Completion, 2000),
		"model":      gs.Model,
		"system":     gs.System,
	}, "", "  ")

	prompt := fmt.Sprintf(`You are an LLM output quality evaluator. Score the following LLM interaction on eight dimensions.

LLM Interaction:
%s

Return ONLY a valid JSON object (no markdown, no explanation):
{
  "hallucination":         0.0,
  "coherence":             0.0,
  "relevance":             0.0,
  "toxicity":              0.0,
  "correctness":           0.0,
  "instruction_adherence": 0.0,
  "reasoning_coherence":   0.0,
  "completeness":          0.0,
  "reasoning":             "one sentence"
}`, string(data))

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
	if err := callOllamaJSON(ctx, prompt, &scores); err != nil {
		return storage.EvalResultRow{}, err
	}

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
		OverallScore:         clamp((c + r + cor + ia + rc + comp + (1 - h) + (1 - t)) / 8),
		Reasoning:            scores.Reasoning,
		EvaluatedAt:          time.Now().UnixNano(),
	}, nil
}

// callOllamaForSession runs the 4-dimension session eval via Ollama.
func callOllamaForSession(ctx context.Context, sess storage.SessionRow, spans []storage.GenAISpanRow) (storage.SessionRow, error) {
	transcript := buildSessionTranscript(sess, spans)

	prompt := fmt.Sprintf(`You are evaluating a multi-step AI agent session. Score on four dimensions.

Session Transcript:
%s

Return ONLY a valid JSON object (no markdown, no explanation):
{
  "action_completion":  0.0,
  "agent_efficiency":   0.0,
  "conv_quality":       0.0,
  "user_intent_change": 0.0,
  "reasoning": "one sentence"
}`, transcript)

	var scores struct {
		ActionCompletion float64 `json:"action_completion"`
		AgentEfficiency  float64 `json:"agent_efficiency"`
		ConvQuality      float64 `json:"conv_quality"`
		UserIntentChange float64 `json:"user_intent_change"`
		Reasoning        string  `json:"reasoning"`
	}
	if err := callOllamaJSON(ctx, prompt, &scores); err != nil {
		return storage.SessionRow{}, err
	}

	clamp := func(v float64) float64 {
		if v < 0 {
			return 0
		}
		if v > 1 {
			return 1
		}
		return v
	}
	sess.ActionCompletion = clamp(scores.ActionCompletion)
	sess.AgentEfficiency = clamp(scores.AgentEfficiency)
	sess.ConvQuality = clamp(scores.ConvQuality)
	sess.UserIntentChange = clamp(scores.UserIntentChange)
	sess.EvalReasoning = scores.Reasoning
	sess.EvalAt = time.Now().UnixNano()
	return sess, nil
}

// callOllamaForRow runs the 8-dimension eval for a dataset row via Ollama.
// Returns rowScores — the same type used by callBedrockForRow.
func callOllamaForRow(ctx context.Context, prompt, completion string) (rowScores, error) {
	payload, _ := json.Marshal(map[string]string{
		"prompt":     truncate(prompt, 2000),
		"completion": truncate(completion, 2000),
	})

	evalPrompt := fmt.Sprintf(`You are an LLM output quality evaluator. Score the following interaction on eight dimensions.

Interaction:
%s

Return ONLY a valid JSON object (no markdown, no explanation):
{
  "hallucination":         0.0,
  "coherence":             0.0,
  "relevance":             0.0,
  "toxicity":              0.0,
  "correctness":           0.0,
  "instruction_adherence": 0.0,
  "reasoning_coherence":   0.0,
  "completeness":          0.0,
  "reasoning":             "one sentence"
}`, string(payload))

	var s rowScores
	if err := callOllamaJSON(ctx, evalPrompt, &s); err != nil {
		return rowScores{}, err
	}
	return s, nil
}
