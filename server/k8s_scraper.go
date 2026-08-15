package server

// K8s node metrics scraper.
//
// Polls the k8s metrics-server API every 30s and ingests CPU + memory usage
// for each node as otel-beacon metrics with entity_id = "k8s-node-<name>".
// This adds the cross-domain infra layer missing from pure OTLP-only observability.
//
// Requires env vars:
//   K8S_API_URL  — e.g. "https://127.0.0.1:6443" or "https://kubernetes.default.svc"
//   K8S_TOKEN    — bearer token with view access to metrics.k8s.io
//
// If either var is missing the scraper is silently disabled.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

// StartK8sScraper starts the background node-metrics scraper if K8S_API_URL and K8S_TOKEN are set.
func StartK8sScraper(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	apiURL := os.Getenv("K8S_API_URL")
	token := os.Getenv("K8S_TOKEN")
	if apiURL == "" || token == "" {
		logger.Debug("k8s scraper: K8S_API_URL or K8S_TOKEN not set, disabled")
		return
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec — internal k8s API
		},
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		// scrape immediately on start
		scrapeK8sNodes(ctx, client, apiURL, token, store, logger)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				scrapeK8sNodes(ctx, client, apiURL, token, store, logger)
			}
		}
	}()
	logger.Info("k8s scraper: started", zap.String("api_url", apiURL))
}

type k8sNodeMetricsList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Usage struct {
			CPU    string `json:"cpu"`    // e.g. "250m" or "1500000000n"
			Memory string `json:"memory"` // e.g. "512Mi"
		} `json:"usage"`
	} `json:"items"`
}

func scrapeK8sNodes(ctx context.Context, client *http.Client, apiURL, token string, store *storage.Storage, logger *zap.Logger) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL+"/apis/metrics.k8s.io/v1beta1/nodes", nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		logger.Debug("k8s scraper: request failed", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var list k8sNodeMetricsList
	if err := json.Unmarshal(body, &list); err != nil {
		logger.Debug("k8s scraper: parse failed", zap.Error(err))
		return
	}

	now := time.Now().UnixNano()
	var metrics []storage.MetricRow
	for _, item := range list.Items {
		nodeName := item.Metadata.Name
		entityID := "k8s-node-" + nodeName
		ra := fmt.Sprintf(`{"service.name":%q,"k8s.node.name":%q}`, entityID, nodeName)

		if cpuCores := parseK8sCPU(item.Usage.CPU); cpuCores > 0 {
			metrics = append(metrics, storage.MetricRow{
				EntityID:      entityID,
				Name:          "k8s.node.cpu.usage",
				Unit:          "cores",
				Type:          "gauge",
				TimestampNs:   now,
				Value:         cpuCores,
				ResourceAttrs: ra,
				DataAttrs:     `{}`,
			})
		}
		if memBytes := parseK8sMemory(item.Usage.Memory); memBytes > 0 {
			metrics = append(metrics, storage.MetricRow{
				EntityID:      entityID,
				Name:          "k8s.node.memory.usage",
				Unit:          "By",
				Type:          "gauge",
				TimestampNs:   now,
				Value:         memBytes,
				ResourceAttrs: ra,
				DataAttrs:     `{}`,
			})
		}
	}

	if len(metrics) > 0 {
		if err := store.WriteMetrics(ctx, metrics); err != nil {
			logger.Warn("k8s scraper: flush failed", zap.Error(err))
		} else {
			logger.Debug("k8s scraper: ingested node metrics", zap.Int("count", len(metrics)))
		}
	}
}

// parseK8sCPU converts a k8s CPU quantity string to float64 cores.
// Supports: "250m" (millicores), "1500000000n" (nanocores), bare number (cores).
func parseK8sCPU(s string) float64 {
	if s == "" {
		return 0
	}
	if len(s) > 1 && s[len(s)-1] == 'm' {
		var v float64
		fmt.Sscanf(s[:len(s)-1], "%f", &v)
		return v / 1000.0
	}
	if len(s) > 1 && s[len(s)-1] == 'n' {
		var v float64
		fmt.Sscanf(s[:len(s)-1], "%f", &v)
		return v / 1_000_000_000.0
	}
	var v float64
	fmt.Sscanf(s, "%f", &v)
	return v
}

// parseK8sMemory converts a k8s memory quantity string to float64 bytes.
// Supports: Ki, Mi, Gi, K, M, G suffixes, bare number (bytes).
func parseK8sMemory(s string) float64 {
	if s == "" {
		return 0
	}
	suffixes := []struct {
		sfx  string
		mult float64
	}{
		{"Gi", 1024 * 1024 * 1024},
		{"Mi", 1024 * 1024},
		{"Ki", 1024},
		{"G", 1000 * 1000 * 1000},
		{"M", 1000 * 1000},
		{"K", 1000},
	}
	for _, su := range suffixes {
		if len(s) > len(su.sfx) && s[len(s)-len(su.sfx):] == su.sfx {
			var v float64
			fmt.Sscanf(s[:len(s)-len(su.sfx)], "%f", &v)
			return v * su.mult
		}
	}
	var v float64
	fmt.Sscanf(s, "%f", &v)
	return v
}
