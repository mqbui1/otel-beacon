package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

func generateNarrative(ctx context.Context, result RCAResult) (string, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-west-2"
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return "", fmt.Errorf("aws config: %w", err)
	}

	client := bedrockruntime.NewFromConfig(cfg)

	// Compact error signature summary (top 5 non-baseline only)
	type sigSummary struct {
		Service    string `json:"service"`
		ErrorType  string `json:"error_type"`
		HTTPStatus string `json:"http_status,omitempty"`
		Operation  string `json:"operation"`
		Count      int64  `json:"count"`
		IsNew      bool   `json:"is_new"`
	}
	var sigSummaries []sigSummary
	for _, s := range result.ErrorSignatures {
		if s.IsBaseline {
			continue
		}
		sigSummaries = append(sigSummaries, sigSummary{
			Service: s.Service, ErrorType: s.ErrorType, HTTPStatus: s.HTTPStatus,
			Operation: s.Operation, Count: s.OccurrenceCount, IsNew: true,
		})
		if len(sigSummaries) == 5 {
			break
		}
	}

	// Compact trace fingerprint summary (top 3 non-baseline only)
	type fpSummary struct {
		RootService string `json:"root_service"`
		EdgeList    string `json:"edges"`
		Count       int64  `json:"count"`
		IsNew       bool   `json:"is_new"`
	}
	var fpSummaries []fpSummary
	for _, fp := range result.TraceFingerprints {
		if fp.IsBaseline {
			continue
		}
		fpSummaries = append(fpSummaries, fpSummary{
			RootService: fp.RootService, EdgeList: fp.EdgeList,
			Count: fp.OccurrenceCount, IsNew: true,
		})
		if len(fpSummaries) == 3 {
			break
		}
	}

	type payload struct {
		Entity          string           `json:"entity"`
		Health          EntityHealth     `json:"health"`
		Baseline        EntityHealth     `json:"baseline"`
		Downstream      []NeighborHealth `json:"downstream"`
		Upstream        []NeighborHealth `json:"upstream"`
		CoLocated       []NeighborHealth `json:"co_located,omitempty"`
		Causes          []CauseCandidate `json:"candidate_causes"`
		ErrorSignatures []sigSummary     `json:"new_error_signatures,omitempty"`
		TraceFingerprints []fpSummary    `json:"new_call_paths,omitempty"`
	}
	data, _ := json.MarshalIndent(payload{
		Entity:            result.Entity,
		Health:            result.Health,
		Baseline:          result.Baseline,
		Downstream:        result.Downstream,
		Upstream:          result.Upstream,
		CoLocated:         result.CoLocated,
		Causes:            result.CandidateCauses,
		ErrorSignatures:   sigSummaries,
		TraceFingerprints: fpSummaries,
	}, "", "  ")

	prompt := fmt.Sprintf(`You are an SRE analyzing a production incident. Given the observability data below, produce a root cause analysis with this structure:

**What is failing**: One sentence — service name, error rate or latency vs baseline, severity.
**Likely root cause**: The single most probable cause based on candidate_causes confidence scores and temporal ordering (lag_seconds < 0 means degraded before the focal service). If new_error_signatures or new_call_paths are present, incorporate them.
**Causal chain**: If multiple services are involved, describe the propagation path (e.g. "A failed → B timed out → C returned 503s").
**Next step**: One concrete, specific action to confirm or resolve (not generic advice).

Rules:
- Cite service names and numbers from the data. Do not invent values.
- If candidate_causes is empty but health shows latency/error regression vs baseline, reason from that directly.
- Keep total response under 150 words.

Data:
%s`, string(data))

	reqBody, _ := json.Marshal(map[string]any{
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens":        600,
		"messages":          []map[string]string{{"role": "user", "content": prompt}},
	})

	modelID := os.Getenv("BEDROCK_MODEL_ID")
	if modelID == "" {
		modelID = "arn:aws:bedrock:us-west-2:387769110234:application-inference-profile/fky19kpnw2m7"
	}

	resp, err := client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(modelID),
		ContentType: aws.String("application/json"),
		Body:        reqBody,
	})
	if err != nil {
		return "", fmt.Errorf("bedrock invoke: %w", err)
	}

	var out struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil || len(out.Content) == 0 {
		return "", fmt.Errorf("parse response: %w", err)
	}
	return out.Content[0].Text, nil
}
