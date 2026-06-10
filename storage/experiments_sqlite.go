package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (b *SQLiteBackend) CreateDataset(ctx context.Context, meta DatasetMeta, rows []DatasetRow) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO datasets (dataset_id, name, entity_id, row_count, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		meta.DatasetID, meta.Name, meta.EntityID, len(rows), time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("insert dataset: %w", err)
	}

	for _, r := range rows {
		if _, err = tx.ExecContext(ctx,
			`INSERT INTO dataset_rows (row_id, dataset_id, prompt, completion, context, expected)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			r.RowID, meta.DatasetID, r.Prompt, r.Completion, r.Context, r.Expected,
		); err != nil {
			return fmt.Errorf("insert dataset row: %w", err)
		}
	}
	return tx.Commit()
}

func (b *SQLiteBackend) ListDatasets(ctx context.Context, entityID string, limit int) ([]DatasetMeta, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT dataset_id, name, entity_id, row_count, created_at FROM datasets`
	args := []any{}
	if entityID != "" {
		q += ` WHERE entity_id = ?`
		args = append(args, entityID)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DatasetMeta
	for rows.Next() {
		var d DatasetMeta
		if err := rows.Scan(&d.DatasetID, &d.Name, &d.EntityID, &d.RowCount, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

func (b *SQLiteBackend) GetDataset(ctx context.Context, datasetID string) (*DatasetMeta, []DatasetRow, error) {
	var meta DatasetMeta
	err := b.db.QueryRowContext(ctx,
		`SELECT dataset_id, name, entity_id, row_count, created_at FROM datasets WHERE dataset_id = ?`,
		datasetID,
	).Scan(&meta.DatasetID, &meta.Name, &meta.EntityID, &meta.RowCount, &meta.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	rows, err := b.db.QueryContext(ctx,
		`SELECT row_id, dataset_id, prompt, completion, context, expected
		 FROM dataset_rows WHERE dataset_id = ? ORDER BY rowid`,
		datasetID,
	)
	if err != nil {
		return &meta, nil, err
	}
	defer rows.Close()
	var out []DatasetRow
	for rows.Next() {
		var r DatasetRow
		if err := rows.Scan(&r.RowID, &r.DatasetID, &r.Prompt, &r.Completion, &r.Context, &r.Expected); err != nil {
			return &meta, nil, err
		}
		out = append(out, r)
	}
	return &meta, out, nil
}

func (b *SQLiteBackend) CreateExperiment(ctx context.Context, exp ExperimentRow) error {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO experiments (experiment_id, name, dataset_id, entity_id, status, row_count, created_at)
		 VALUES (?, ?, ?, ?, 'pending', ?, ?)`,
		exp.ExperimentID, exp.Name, exp.DatasetID, exp.EntityID, exp.RowCount, time.Now().Unix(),
	)
	return err
}

func (b *SQLiteBackend) ListExperiments(ctx context.Context, entityID string, limit int) ([]ExperimentRow, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT experiment_id, name, dataset_id, entity_id, status,
	             row_count, scored_count, avg_overall, avg_hallucination, avg_coherence,
	             avg_relevance, avg_toxicity, avg_correctness, avg_instruction_adherence,
	             avg_reasoning_coherence, avg_completeness, created_at, completed_at
	      FROM experiments`
	args := []any{}
	if entityID != "" {
		q += ` WHERE entity_id = ?`
		args = append(args, entityID)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanExperiments(rows)
}

func (b *SQLiteBackend) GetExperiment(ctx context.Context, experimentID string) (*ExperimentRow, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT experiment_id, name, dataset_id, entity_id, status,
		        row_count, scored_count, avg_overall, avg_hallucination, avg_coherence,
		        avg_relevance, avg_toxicity, avg_correctness, avg_instruction_adherence,
		        avg_reasoning_coherence, avg_completeness, created_at, completed_at
		 FROM experiments WHERE experiment_id = ?`,
		experimentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	exps, err := scanExperiments(rows)
	if err != nil || len(exps) == 0 {
		return nil, err
	}
	return &exps[0], nil
}

func scanExperiments(rows *sql.Rows) ([]ExperimentRow, error) {
	var out []ExperimentRow
	for rows.Next() {
		var e ExperimentRow
		if err := rows.Scan(
			&e.ExperimentID, &e.Name, &e.DatasetID, &e.EntityID, &e.Status,
			&e.RowCount, &e.ScoredCount, &e.AvgOverall, &e.AvgHallucination, &e.AvgCoherence,
			&e.AvgRelevance, &e.AvgToxicity, &e.AvgCorrectness, &e.AvgInstructionAdherence,
			&e.AvgReasoningCoherence, &e.AvgCompleteness, &e.CreatedAt, &e.CompletedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (b *SQLiteBackend) SaveExperimentResult(ctx context.Context, r ExperimentResult) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx,
		`INSERT INTO experiment_results
		 (result_id, experiment_id, row_id, overall_score, hallucination, coherence, relevance,
		  toxicity, correctness, instruction_adherence, reasoning_coherence, completeness, eval_reasoning)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(result_id) DO NOTHING`,
		r.ResultID, r.ExperimentID, r.RowID, r.OverallScore, r.Hallucination, r.Coherence,
		r.Relevance, r.Toxicity, r.Correctness, r.InstructionAdherence, r.ReasoningCoherence,
		r.Completeness, r.EvalReasoning,
	); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx,
		`UPDATE experiments SET scored_count = scored_count + 1, status = 'running'
		 WHERE experiment_id = ?`,
		r.ExperimentID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (b *SQLiteBackend) FinalizeExperiment(ctx context.Context, exp ExperimentRow) error {
	_, err := b.db.ExecContext(ctx,
		`UPDATE experiments SET
		 status = ?, scored_count = ?, avg_overall = ?, avg_hallucination = ?,
		 avg_coherence = ?, avg_relevance = ?, avg_toxicity = ?, avg_correctness = ?,
		 avg_instruction_adherence = ?, avg_reasoning_coherence = ?, avg_completeness = ?,
		 completed_at = ?
		 WHERE experiment_id = ?`,
		exp.Status, exp.ScoredCount,
		exp.AvgOverall, exp.AvgHallucination, exp.AvgCoherence, exp.AvgRelevance,
		exp.AvgToxicity, exp.AvgCorrectness, exp.AvgInstructionAdherence,
		exp.AvgReasoningCoherence, exp.AvgCompleteness, exp.CompletedAt,
		exp.ExperimentID,
	)
	return err
}

func (b *SQLiteBackend) GetExperimentResults(ctx context.Context, experimentID string) ([]ExperimentResult, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT er.result_id, er.experiment_id, er.row_id,
		        er.overall_score, er.hallucination, er.coherence, er.relevance,
		        er.toxicity, er.correctness, er.instruction_adherence, er.reasoning_coherence,
		        er.completeness, er.eval_reasoning, er.created_at,
		        COALESCE(dr.prompt, ''), COALESCE(dr.completion, '')
		 FROM experiment_results er
		 LEFT JOIN dataset_rows dr ON er.row_id = dr.row_id
		 WHERE er.experiment_id = ?
		 ORDER BY er.rowid`,
		experimentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExperimentResult
	for rows.Next() {
		var r ExperimentResult
		if err := rows.Scan(
			&r.ResultID, &r.ExperimentID, &r.RowID,
			&r.OverallScore, &r.Hallucination, &r.Coherence, &r.Relevance,
			&r.Toxicity, &r.Correctness, &r.InstructionAdherence, &r.ReasoningCoherence,
			&r.Completeness, &r.EvalReasoning, &r.CreatedAt,
			&r.Prompt, &r.Completion,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}
