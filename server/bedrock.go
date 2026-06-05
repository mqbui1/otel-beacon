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

	// Summarize into a focused payload to keep the prompt small
	type payload struct {
		Entity     string           `json:"entity"`
		Health     EntityHealth     `json:"health"`
		Baseline   EntityHealth     `json:"baseline"`
		Downstream []NeighborHealth `json:"downstream"`
		Upstream   []NeighborHealth `json:"upstream"`
		CoLocated  []NeighborHealth `json:"co_located"`
		Causes     []CauseCandidate `json:"candidate_causes"`
	}
	data, _ := json.MarshalIndent(payload{
		Entity:     result.Entity,
		Health:     result.Health,
		Baseline:   result.Baseline,
		Downstream: result.Downstream,
		Upstream:   result.Upstream,
		CoLocated:  result.CoLocated,
		Causes:     result.CandidateCauses,
	}, "", "  ")

	prompt := fmt.Sprintf(`You are an SRE analyzing an observability incident. Based on the entity correlation data below, write a concise root cause analysis (3-5 sentences). Address:
1. What is failing and how severely (compare health vs baseline)
2. The most likely root cause based on error rates and temporal ordering
3. One concrete next step to investigate or resolve

Entity correlation data:
%s

Be specific with service names and numbers. Do not speculate beyond the data.`, string(data))

	reqBody, _ := json.Marshal(map[string]any{
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens":        400,
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
