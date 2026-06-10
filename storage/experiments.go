package storage

import "context"

// DatasetMeta holds metadata for an uploaded dataset.
type DatasetMeta struct {
	DatasetID string `json:"dataset_id"`
	Name      string `json:"name"`
	EntityID  string `json:"entity_id"`
	RowCount  int    `json:"row_count"`
	CreatedAt int64  `json:"created_at"`
}

// DatasetRow is a single prompt/completion pair in a dataset.
type DatasetRow struct {
	RowID      string `json:"row_id"`
	DatasetID  string `json:"dataset_id"`
	Prompt     string `json:"prompt"`
	Completion string `json:"completion"`
	Context    string `json:"context,omitempty"`
	Expected   string `json:"expected,omitempty"`
}

// ExperimentRow tracks a batch eval run over a dataset.
type ExperimentRow struct {
	ExperimentID            string  `json:"experiment_id"`
	Name                    string  `json:"name"`
	DatasetID               string  `json:"dataset_id"`
	EntityID                string  `json:"entity_id"`
	Status                  string  `json:"status"` // pending|running|done|failed
	RowCount                int     `json:"row_count"`
	ScoredCount             int     `json:"scored_count"`
	AvgOverall              float64 `json:"avg_overall"`
	AvgHallucination        float64 `json:"avg_hallucination"`
	AvgCoherence            float64 `json:"avg_coherence"`
	AvgRelevance            float64 `json:"avg_relevance"`
	AvgToxicity             float64 `json:"avg_toxicity"`
	AvgCorrectness          float64 `json:"avg_correctness"`
	AvgInstructionAdherence float64 `json:"avg_instruction_adherence"`
	AvgReasoningCoherence   float64 `json:"avg_reasoning_coherence"`
	AvgCompleteness         float64 `json:"avg_completeness"`
	CreatedAt               int64   `json:"created_at"`
	CompletedAt             int64   `json:"completed_at"`
}

// ExperimentResult is a per-row eval result for an experiment.
type ExperimentResult struct {
	ResultID             string  `json:"result_id"`
	ExperimentID         string  `json:"experiment_id"`
	RowID                string  `json:"row_id"`
	Prompt               string  `json:"prompt,omitempty"`
	Completion           string  `json:"completion,omitempty"`
	OverallScore         float64 `json:"overall_score"`
	Hallucination        float64 `json:"hallucination"`
	Coherence            float64 `json:"coherence"`
	Relevance            float64 `json:"relevance"`
	Toxicity             float64 `json:"toxicity"`
	Correctness          float64 `json:"correctness"`
	InstructionAdherence float64 `json:"instruction_adherence"`
	ReasoningCoherence   float64 `json:"reasoning_coherence"`
	Completeness         float64 `json:"completeness"`
	EvalReasoning        string  `json:"eval_reasoning"`
	CreatedAt            int64   `json:"created_at"`
}

// Storage passthroughs for datasets and experiments.

func (s *Storage) CreateDataset(ctx context.Context, meta DatasetMeta, rows []DatasetRow) error {
	return s.backend.CreateDataset(ctx, meta, rows)
}

func (s *Storage) ListDatasets(ctx context.Context, entityID string, limit int) ([]DatasetMeta, error) {
	return s.backend.ListDatasets(ctx, entityID, limit)
}

func (s *Storage) GetDataset(ctx context.Context, datasetID string) (*DatasetMeta, []DatasetRow, error) {
	return s.backend.GetDataset(ctx, datasetID)
}

func (s *Storage) CreateExperiment(ctx context.Context, exp ExperimentRow) error {
	return s.backend.CreateExperiment(ctx, exp)
}

func (s *Storage) ListExperiments(ctx context.Context, entityID string, limit int) ([]ExperimentRow, error) {
	return s.backend.ListExperiments(ctx, entityID, limit)
}

func (s *Storage) GetExperiment(ctx context.Context, experimentID string) (*ExperimentRow, error) {
	return s.backend.GetExperiment(ctx, experimentID)
}

func (s *Storage) SaveExperimentResult(ctx context.Context, r ExperimentResult) error {
	return s.backend.SaveExperimentResult(ctx, r)
}

func (s *Storage) FinalizeExperiment(ctx context.Context, exp ExperimentRow) error {
	return s.backend.FinalizeExperiment(ctx, exp)
}

func (s *Storage) GetExperimentResults(ctx context.Context, experimentID string) ([]ExperimentResult, error) {
	return s.backend.GetExperimentResults(ctx, experimentID)
}
