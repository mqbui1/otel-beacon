package storage

import (
	"regexp"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Tier-1 guardrails — fast regex-based checks (< 1 ms per span).
// Tier-2 (LLM-as-judge quality scoring) runs asynchronously in the server package.
// ---------------------------------------------------------------------------

var (
	// Prompt injection patterns: attempts to override or hijack LLM instructions.
	rePromptInjection = []*regexp.Regexp{
		regexp.MustCompile(`(?i)ignore\s+(previous|all|above|prior)\s+(instructions?|prompts?|context)`),
		regexp.MustCompile(`(?i)disregard\s+(your|the|all)\s+(instructions?|rules?|guidelines?)`),
		regexp.MustCompile(`(?i)(system\s*prompt|you\s+are\s+now|new\s+persona|act\s+as\s+if)`),
		regexp.MustCompile(`(?i)(jailbreak|dan\s+mode|developer\s+mode|unrestricted\s+mode)`),
		regexp.MustCompile(`(?i)forget\s+(everything|all)\s+(you|i|we)\s+(told|said|wrote)`),
		regexp.MustCompile(`(?i)\[\s*system\s*\]|\<\s*system\s*\>`),
		regexp.MustCompile(`(?i)(do\s+anything\s+now|you\s+have\s+no\s+restrictions)`),
		regexp.MustCompile(`(?i)(reveal|print|output|show)\s+(your\s+)?(system\s+prompt|instructions|context)`),
	}

	// PII patterns.
	rePIIEmail      = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	rePIIPhone      = regexp.MustCompile(`(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}`)
	rePIISSN        = regexp.MustCompile(`\b\d{3}[-\s]\d{2}[-\s]\d{4}\b`)
	rePIICreditCard = regexp.MustCompile(`\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b`)
	rePIIIPv4       = regexp.MustCompile(`\b(?:10|172\.(?:1[6-9]|2\d|3[01])|192\.168)\.\d{1,3}\.\d{1,3}\b`)

	// Toxicity / harmful content keywords (common categories).
	toxicityKeywords = []string{
		"kill yourself", "kys", "die in a fire",
		"i will kill", "i want to kill",
		"bomb", "explosive device", "detonate",
		"synthesize poison", "manufacture drugs",
		"child porn", "csam",
	}

	// Sexism / gender bias patterns.
	reSexism = []*regexp.Regexp{
		regexp.MustCompile(`(?i)(women|girls?|females?)\s+(are|can'?t|cannot|don'?t|shouldn'?t|aren'?t)\s+(as\s+)?(smart|logical|capable|good|strong|rational)`),
		regexp.MustCompile(`(?i)(men|males?|boys?)\s+(are|should)\s+(always|never|only|inherently)`),
		regexp.MustCompile(`(?i)(belong\s+in\s+the\s+kitchen|make\s+me\s+a\s+sandwich)`),
		regexp.MustCompile(`(?i)(bossy|hysterical|emotional|irrational)\s+(bitch|woman|female|girl)`),
		regexp.MustCompile(`(?i)(man\s+up|like\s+a\s+girl|throw\s+like\s+a\s+girl|act\s+like\s+a\s+(man|woman))`),
		regexp.MustCompile(`(?i)(gender\s+pay\s+gap\s+(is\s+)?(fake|myth|not\s+real))`),
	}
)

// checkGuardrails runs all tier-1 checks on a GenAI span and returns any triggered events.
func checkGuardrails(gs GenAISpanRow) []GuardrailEventRow {
	var events []GuardrailEventRow
	now := time.Now().UnixNano()

	// Check prompt (user input) and completion (model output) separately.
	for _, text := range []struct {
		content   string
		checkType string
	}{
		{gs.Prompt, "prompt"},
		{gs.Completion, "completion"},
	} {
		if text.content == "" {
			continue
		}
		lower := strings.ToLower(text.content)

		// --- Prompt injection (only meaningful on prompts, but flag in completions too) ---
		for _, re := range rePromptInjection {
			if re.MatchString(text.content) {
				severity := "high"
				if text.checkType == "completion" {
					severity = "medium"
				}
				events = append(events, GuardrailEventRow{
					SpanID:    gs.SpanID,
					TraceID:   gs.TraceID,
					CheckType: "prompt_injection",
					Triggered: true,
					Severity:  severity,
					Detail:    "Pattern: " + re.String(),
					CheckedAt: now,
				})
				break // one event per span per category
			}
		}

		// --- PII detection ---
		var piiMatches []string
		if rePIIEmail.MatchString(text.content) {
			piiMatches = append(piiMatches, "email")
		}
		if rePIIPhone.MatchString(text.content) {
			piiMatches = append(piiMatches, "phone")
		}
		if rePIISSN.MatchString(text.content) {
			piiMatches = append(piiMatches, "ssn")
		}
		if rePIICreditCard.MatchString(text.content) {
			piiMatches = append(piiMatches, "credit_card")
		}
		if rePIIIPv4.MatchString(text.content) {
			piiMatches = append(piiMatches, "private_ip")
		}
		if len(piiMatches) > 0 {
			events = append(events, GuardrailEventRow{
				SpanID:    gs.SpanID,
				TraceID:   gs.TraceID,
				CheckType: "pii",
				Triggered: true,
				Severity:  "high",
				Detail:    "Detected: " + strings.Join(piiMatches, ", ") + " in " + text.checkType,
				CheckedAt: now,
			})
		}

		// --- Toxicity keyword scan ---
		for _, kw := range toxicityKeywords {
			if strings.Contains(lower, kw) {
				events = append(events, GuardrailEventRow{
					SpanID:    gs.SpanID,
					TraceID:   gs.TraceID,
					CheckType: "toxicity",
					Triggered: true,
					Severity:  "high",
					Detail:    "Keyword match in " + text.checkType,
					CheckedAt: now,
				})
				break
			}
		}

		// --- Sexism / gender bias scan ---
		for _, re := range reSexism {
			if re.MatchString(text.content) {
				events = append(events, GuardrailEventRow{
					SpanID:    gs.SpanID,
					TraceID:   gs.TraceID,
					CheckType: "sexism",
					Triggered: true,
					Severity:  "medium",
					Detail:    "Gender bias pattern in " + text.checkType,
					CheckedAt: now,
				})
				break
			}
		}
	}

	return events
}

// CheckGuardrailsRequest is the payload for the POST /v1/genai/guardrails/check endpoint.
type CheckGuardrailsRequest struct {
	Prompt     string `json:"prompt"`
	Completion string `json:"completion"`
	TraceID    string `json:"trace_id,omitempty"`
	SpanID     string `json:"span_id,omitempty"`
}

// CheckGuardrailsResponse is the result of a synchronous guardrail check.
type CheckGuardrailsResponse struct {
	Triggered bool               `json:"triggered"`
	Events    []GuardrailEventRow `json:"events"`
}

// RunGuardrailCheck performs a synchronous guardrail check on arbitrary text
// (used by the POST /v1/genai/guardrails/check endpoint).
func RunGuardrailCheck(req CheckGuardrailsRequest) CheckGuardrailsResponse {
	gs := GenAISpanRow{
		SpanID:     req.SpanID,
		TraceID:    req.TraceID,
		Prompt:     req.Prompt,
		Completion: req.Completion,
	}
	events := checkGuardrails(gs)
	return CheckGuardrailsResponse{
		Triggered: len(events) > 0,
		Events:    events,
	}
}
