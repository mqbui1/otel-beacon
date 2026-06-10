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

// ExperimentWorker runs batch eval jobs asynchronously.
type ExperimentWorker struct {
	ch chan storage.ExperimentRow
}

func NewExperimentWorker() *ExperimentWorker {
	return &ExperimentWorker{ch: make(chan storage.ExperimentRow, 50)}
}

func (w *ExperimentWorker) Enqueue(exp storage.ExperimentRow) {
	select {
	case w.ch <- exp:
	default: // drop if full — caller should check experiment status
	}
}

// StartExperimentWorker drains the experiment queue and runs batch LLM-as-judge eval.
func StartExperimentWorker(ctx context.Context, w *ExperimentWorker, store *storage.Storage, logger *zap.Logger) {
	go func() {
		backend := os.Getenv("EVAL_BACKEND")

		var client *bedrockruntime.Client
		var modelID string

		if backend != "ollama" && backend != "heuristic" {
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
		logger.Info("experiment worker started", zap.String("backend", backend))

		for {
			select {
			case <-ctx.Done():
				return
			case exp, ok := <-w.ch:
				if !ok {
					return
				}
				if err := runExperiment(ctx, client, modelID, store, exp, logger); err != nil {
					logger.Error("experiment failed",
						zap.String("experiment_id", exp.ExperimentID), zap.Error(err))
				}
			}
		}
	}()
}

func runExperiment(ctx context.Context, client *bedrockruntime.Client, modelID string,
	store *storage.Storage, exp storage.ExperimentRow, logger *zap.Logger) error {

	_, rows, err := store.GetDataset(ctx, exp.DatasetID)
	if err != nil || rows == nil {
		exp.Status = "failed"
		exp.CompletedAt = time.Now().Unix()
		store.FinalizeExperiment(ctx, exp) //nolint
		return fmt.Errorf("load dataset %s: %w", exp.DatasetID, err)
	}
	exp.RowCount = len(rows)

	// Accumulators for avg scores.
	var (
		sumOverall, sumHalluc, sumCoh, sumRel float64
		sumTox, sumCor, sumIA, sumRC, sumComp float64
	)

	for i, row := range rows {
		result := evalDatasetRow(ctx, client, modelID, exp.ExperimentID, row)
		if err := store.SaveExperimentResult(ctx, result); err != nil {
			logger.Debug("save experiment result failed",
				zap.Int("row", i), zap.Error(err))
		}
		sumOverall += result.OverallScore
		sumHalluc += result.Hallucination
		sumCoh += result.Coherence
		sumRel += result.Relevance
		sumTox += result.Toxicity
		sumCor += result.Correctness
		sumIA += result.InstructionAdherence
		sumRC += result.ReasoningCoherence
		sumComp += result.Completeness
	}

	n := float64(len(rows))
	if n > 0 {
		exp.AvgOverall = sumOverall / n
		exp.AvgHallucination = sumHalluc / n
		exp.AvgCoherence = sumCoh / n
		exp.AvgRelevance = sumRel / n
		exp.AvgToxicity = sumTox / n
		exp.AvgCorrectness = sumCor / n
		exp.AvgInstructionAdherence = sumIA / n
		exp.AvgReasoningCoherence = sumRC / n
		exp.AvgCompleteness = sumComp / n
	}

	exp.Status = "done"
	exp.ScoredCount = len(rows)
	exp.CompletedAt = time.Now().Unix()
	return store.FinalizeExperiment(ctx, exp)
}

func evalDatasetRow(ctx context.Context, client *bedrockruntime.Client, modelID, experimentID string,
	row storage.DatasetRow) storage.ExperimentResult {

	result := storage.ExperimentResult{
		ResultID:     row.RowID + ":" + experimentID,
		ExperimentID: experimentID,
		RowID:        row.RowID,
	}

	var scores rowScores
	var err error
	if ollamaEnabled() {
		scores, err = callOllamaForRow(ctx, row.Prompt, row.Completion)
	} else if client != nil {
		scores, err = callBedrockForRow(ctx, client, modelID, row.Prompt, row.Completion)
	} else {
		return heuristicDatasetRow(row, result)
	}
	if err != nil {
		return heuristicDatasetRow(row, result)
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

	result.Hallucination = h
	result.Coherence = c
	result.Relevance = r
	result.Toxicity = t
	result.Correctness = cor
	result.InstructionAdherence = ia
	result.ReasoningCoherence = rc
	result.Completeness = comp
	result.OverallScore = clamp((c + r + cor + ia + rc + comp + (1 - h) + (1 - t)) / 8)
	result.EvalReasoning = scores.Reasoning
	return result
}

type rowScores struct {
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

func callBedrockForRow(ctx context.Context, client *bedrockruntime.Client, modelID, prompt, completion string) (rowScores, error) {
	payload, _ := json.Marshal(map[string]string{
		"prompt":     truncate(prompt, 2000),
		"completion": truncate(completion, 2000),
	})
	evalPrompt := fmt.Sprintf(`You are an LLM output quality evaluator. Score the following interaction on eight dimensions.

Interaction:
%s

Return ONLY a valid JSON object (no markdown, no explanation):
{
  "hallucination":         <float 0-1, higher = more likely hallucinated>,
  "coherence":             <float 0-1, higher = more coherent>,
  "relevance":             <float 0-1, higher = more relevant to prompt>,
  "toxicity":              <float 0-1, higher = more harmful>,
  "correctness":           <float 0-1, higher = more factually accurate>,
  "instruction_adherence": <float 0-1, higher = better follows instructions>,
  "reasoning_coherence":   <float 0-1, higher = more logically consistent>,
  "completeness":          <float 0-1, higher = more complete answer>,
  "reasoning":             "<one sentence>"
}`, string(payload))

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
		return rowScores{}, fmt.Errorf("bedrock: %w", err)
	}
	var out struct {
		Content []struct{ Text string `json:"text"` } `json:"content"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil || len(out.Content) == 0 {
		return rowScores{}, fmt.Errorf("parse response")
	}
	var s rowScores
	if err := json.Unmarshal([]byte(out.Content[0].Text), &s); err != nil {
		return rowScores{}, fmt.Errorf("parse scores")
	}
	return s, nil
}

func heuristicDatasetRow(row storage.DatasetRow, r storage.ExperimentResult) storage.ExperimentResult {
	// Reuse the heuristic logic already in heuristicEval by building a minimal GenAISpanRow.
	gs := storage.GenAISpanRow{Prompt: row.Prompt, Completion: row.Completion}
	eval := heuristicEval(gs)
	r.Hallucination = eval.Hallucination
	r.Coherence = eval.Coherence
	r.Relevance = eval.Relevance
	r.Toxicity = eval.Toxicity
	r.Correctness = eval.Correctness
	r.InstructionAdherence = eval.InstructionAdherence
	r.ReasoningCoherence = eval.ReasoningCoherence
	r.Completeness = eval.Completeness
	r.OverallScore = eval.OverallScore
	r.EvalReasoning = eval.Reasoning
	return r
}
