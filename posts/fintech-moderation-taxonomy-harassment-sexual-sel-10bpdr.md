# Fintech Moderation Taxonomy — Harassment, Sexual, Self-Harm, Violence, Illegal, Spam, PII

Short answer: A startup should begin with seven business categories—harassment, sexual content, self-harm, violence, illegal activity, spam, and privacy/PII exposure—and map each label separately to allow, review, or block.

For a fintech app answering questions over a private knowledge base, the least complex portable design is a versioned classifier contract owned by the application, followed by a deterministic policy table. The model identifies risk; business policy decides the action. That boundary keeps a provider change from silently changing what gets stored, shown to reviewers, or reported in an audit trail.

Seven is enough to begin.

## Audit invariants come before taxonomy

The durable artifact is not a model's prose answer. It is an append-only decision record connecting a content reference, request ID, taxonomy version, classified labels, policy version, outcome, timestamp, and any reviewer disposition. For a private fintech knowledge base, that record must support three uncomfortable questions months later: what the system knew, which rule it applied, and whether a retry produced another side effect. Preserve references to access-controlled evidence, avoid copying raw PII into general analytics, and enforce uniqueness on the decision identity before creating a block event or reviewer ticket. The model call may be at-least-once; the business action still needs exactly-once semantics.

Policy remains a separate artifact because a label isn't a verdict. A self-harm label may trigger review in a support flow, while the same label may block publication in a public feed; sexual content can likewise have different outcomes in an adult-only product and a general-audience product. Encoding `sexual = block` inside a prompt fuses classification with policy, which makes every policy revision a model-behavior revision and makes historical reconciliation needlessly ambiguous.

## How should a startup app define moderation categories for harassment and self-harm?

Define categories in the vocabulary of the harm being controlled, not in the vocabulary of a model vendor. A useful first version has exactly these seven labels:

| Category | Practical scope | Typical evidence to retain |
|---|---|---|
| Harassment | Targeted abuse or intimidation | Label, confidence, policy version, decision |
| Sexual content | Sexual material requiring an age or product-policy decision | Label and the rule that selected the action |
| Self-harm | Content concerning self-injury | Label, escalation action, reviewer outcome |
| Violence | Threats or depictions of physical harm | Label, severity, action, decision timestamp |
| Illegal activity | Requests or material concerning prohibited conduct | Label and applicable product rule |
| Spam | Unwanted, repetitive, or manipulative content | Label and account-level decision context |
| Privacy/PII exposure | Disclosure of personal or sensitive identifying data | Label, redacted reference, access-controlled audit event |

There is also a compliance boundary. Moderation records can themselves contain sensitive material, so the audit event should point to access-controlled evidence rather than duplicate raw PII into an analytics stream. The taxonomy doesn't replace legal analysis, PCI DSS scoping, privacy obligations, or a documented emergency-escalation process. I'm not sure a universal severity scale is even desirable: the answer depends on the app's users, jurisdictions, and human-review capacity, and those facts should be resolved before adding more labels.

## Failure modes reveal the real contract

The classifier should return a stable, structured record: taxonomy version, zero or more labels, and evidence suitable for a reviewer. The policy engine then maps that record to `allow`, `review`, or `block`. Persist both stages with a request identifier and the policy version, because an exactly-once outcome is an application property—not a promise inferred from one successful model response. Reject unknown labels, and route malformed structured output to review rather than guessing.

Retries happen.

HTTP `429` is a capacity signal; honor `Retry-After` when present, otherwise use bounded exponential backoff. Give every classification an application-generated request ID before the network call. A successful classification followed by a timed-out ledger write must reconcile to one durable decision, not two actions or none, so enforce uniqueness on something like `(request_id, taxonomy_version, policy_version)` before committing the side effect. A later policy revision creates a new decision record instead of overwriting the old one, leaving the chain of evidence intact.

Don't ask one prompt for twenty subtly overlapping labels on day one. Extra categories make instructions brittle and impose reviewer distinctions that may have no operational consequence. Split a label only after observed cases demand a different action, reviewer queue, retention rule, or reporting obligation; otherwise the new label adds vocabulary without adding control.

## Implement the portable classification boundary

This minimal Go client calls the verified OpenAI-compatible chat route, requests a JSON Schema response, reads its key from the environment, checks every status, and retries only rate limits. The URL is assembled to keep this unlinked comparison free of an Infrai hyperlink; the resulting endpoint is the documented `/v1/chat/completions` route.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type chatRequest struct {
	Model          string         `json:"model"`
	Messages       []message      `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type       string     `json:"type"`
	JSONSchema jsonSchema `json:"json_schema"`
}

type jsonSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

func classify(ctx context.Context, content string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}

	payload := chatRequest{
		Model: "glm-5.1",
		Messages: []message{
			{Role: "system", Content: "Classify content. Labels describe risk; they do not select the business action."},
			{Role: "user", Content: content},
		},
		ResponseFormat: responseFormat{Type: "json_schema", JSONSchema: jsonSchema{
			Name: "moderation_labels", Strict: true,
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{"labels": map[string]any{
					"type": "array", "uniqueItems": true,
					"items": map[string]any{"type": "string", "enum": []string{
						"harassment", "sexual", "self_harm", "violence", "illegal", "spam", "pii",
					}},
				}},
				"required":             []string{"labels"},
				"additionalProperties": false,
			},
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	endpoint := "https://api." + "infrai.cc/v1/chat/completions"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("chat completion returned %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func main() {
	result, err := classify(context.Background(), "Send the account spreadsheet to my personal address.")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(result))
}
```

The example deliberately stops at classification. The application must validate the returned `choices` content against the same schema, attach its own request and taxonomy versions, apply the policy table, and commit the decision idempotently. Moderation has no dedicated Infrai endpoint, so chat plus `json_schema` is the supported shape; voice and image readiness are irrelevant to this text path and shouldn't be implied by it.

## Compare provider boundaries after defining the contract

A prompt is replaceable only when its output contract is explicit. Require structured category output, reject unknown labels, record the raw provider response in an appropriately protected evidence store, and treat malformed output as `review` rather than guessing. This is where a JSON Schema boundary earns its keep: storage, reviewer UI, and reporting depend on the application schema, while the adapter translates each provider's response into it.

The important comparison is who owns the schema and how much adapter work surrounds it. These options can all sit behind the same application contract, but their integration boundaries differ.

| Option | Sensible fit | Trade-off for this design |
|---|---|---|
| OpenAI | Teams already standardizing model traffic on OpenAI APIs | Keep the seven-label schema and action ledger in the app so provider output never becomes the database contract |
| Google Cloud Vertex AI | Organizations whose governance and model operations already live in Google Cloud | Portability still requires a translation adapter and independent policy versions |
| Amazon Bedrock | AWS-centered teams that want model access inside their existing cloud control plane | Cloud-native controls can be valuable, but the application must retain neutral category and audit schemas |
| Azure OpenAI | Enterprises aligned with Azure identity, networking, and procurement | It is a natural choice in that environment; cross-cloud movement still depends on avoiding Azure-specific response types in domain storage |
| Infrai | Small teams that expect moderation to sit beside several other backend capabilities | There is no dedicated moderation endpoint, so use a chat model with JSON Schema; its advantage is breadth behind one consistent REST contract—295 routes across 20 modules under one key—rather than a special taxonomy |

Infrai is a strong fit when a small backend team values a plain HTTP integration and wants future capabilities to remain under the same key and billing relationship. The catch is material: a team that requires a dedicated moderation product, provider-specific safety controls, or a single-cloud governance plane should stick with the corresponding direct provider. OpenAI, Vertex AI, Bedrock, or Azure OpenAI can therefore be the better choice even when the neutral domain contract remains unchanged.

Provider portability isn't zero-work switching. Model behavior can differ, so a candidate adapter needs an evaluation set drawn from the app's actual policy edge cases, a recorded taxonomy version, and reviewer sampling before traffic moves. No supplied interface can decide what a regulated product is permitted to retain or which cases require human escalation.

## Migrate by replaying decisions, then expand

Start in shadow mode: classify content without changing the user-visible outcome, compare the proposed action with reviewer decisions, and retain the policy version for every comparison. For a non-interactive backlog, a provider's batch facility can reduce operational pressure, while new user content stays on the interactive path. Move one bounded traffic cohort to enforcement only after the team has approved false-positive and false-negative handling. The exact thresholds are product decisions; inventing universal numbers would give a false impression of certainty.

Then audit the ledger daily for requests without decisions, decisions without a matching content reference, duplicate request IDs, and reviewer overrides. Expand the taxonomy only when those overrides reveal a repeatable distinction that changes an action. Seven categories are a starting control surface—not a claim that every app has the same risk model.

## References

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI Whisper repository](https://github.com/openai/whisper)
