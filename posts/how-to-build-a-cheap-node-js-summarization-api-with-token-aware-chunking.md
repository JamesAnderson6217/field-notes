# How to Build a Cheap Node.js Summarization API with Token-Aware Chunking

**Short answer:** For a SaaS summarization feature, count tokens before dispatch, split long text on semantic boundaries, summarize chunks with chat completions, and estimate the two output modes before committing them to a plan.

The model call is the easy part. The system constraint is controlling an operation whose input length, retry behavior, and final charge vary while users reasonably expect one summary, one audit record, and no duplicate work. I treat that boundary much as I treat a ledger posting: measure first, assign a stable identity, preserve evidence, and make every transition replayable.

## Start with the accounting boundary, not the model

A useful design begins with a summarization job, not an HTTP request. Give the job an immutable ID, record the hash and token count of the source, and store the requested mode (`brief` or `detailed`). Then create ordered chunk records whose identities derive from the job ID, source hash, tokenizer identity, and chunk ordinal. That derivation matters because an ordinary timeout leaves the caller unable to distinguish “not accepted” from “accepted but the response was lost.” A retry should rediscover the same chunk, not purchase a second completion and quietly feed two candidates into the reducer.

This is where token-aware chunking earns its keep. For an Infrai-backed implementation, `/v1/ai/tokens/count` supplies the measurement used to keep each input manageable; split on paragraph or sentence boundaries, count again after adding the prompt, and reserve output headroom rather than filling the entire allowance. A character-count proxy is acceptable only as a coarse admission check. It isn't a sound dispatch rule across languages or models.

Keep the audit trail boring. I persist the chosen model, prompt version, chunk boundaries, attempt number, upstream request ID when available, and the hash of each returned summary. I do not claim exactly-once network delivery; I build exactly-once business effect from idempotent state transitions and reconciliation. For regulated payment narratives, the original text and generated summary also need separate retention, access-control, and deletion policies. A summary is derived data, not an excuse to retain source material indefinitely, and model output must never become an authoritative ledger entry without domain validation.

Count first. Dispatch second.

No shortcuts.

## How should a Node.js SaaS split long text, count tokens, and estimate summary cost?

Use a map-reduce pipeline with a budget on both stages. The map prompt asks for the minimum stable intermediate representation: facts, amounts, dates, actors, qualifications, and unresolved ambiguity. The reduce prompt then combines those records into the user-facing mode. For `brief`, cap the requested output aggressively and prefer a single paragraph; for `detailed`, allow headings and evidence, but keep the same factual-preservation instruction. Never recursively summarize prose without retaining chunk lineage, because a disputed number must be traceable to its source span.

Before dispatch, call `/v1/ai/cost/estimate` for representative short and long jobs, including the intended output allowance. Use those estimates to choose product defaults and admission thresholds, not to promise an exact invoice. Your mileage may vary because real output lengths vary. The estimate, selected mode, and eventual metering metadata belong on the job record so finance can reconcile aggregate provider charges against completed customer operations.

In one internal reconciliation pipeline, a downstream call returned `200`, yet the side effect never appeared; we discovered it 6 hours later when the debit and event counts diverged. I'm not sure why that service acknowledged before durable commit, but the lesson survived the incident: a successful status is evidence, not proof of business completion. A summarizer deserves the same discipline. Reconcile terminal jobs against chunk results, reject a final summary when any ordinal is absent, and retry from persisted state rather than from a browser request.

The Node.js service can own orchestration while a worker in any language speaks the provider protocol. The Go program below is deliberately narrow: it summarizes one already-counted chunk through an OpenAI-compatible client, sets a timeout, and emits a result that an orchestrator can bind to a stable chunk ID. The boundary is plain HTTP-compatible — useful when a polyglot backend cannot justify another vendor-specific integration.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

func main() {
	key := os.Getenv("AI_API_KEY")
	baseURL := os.Getenv("AI_BASE_URL")
	chunk := os.Getenv("CHUNK_TEXT")
	if key == "" || baseURL == "" || chunk == "" {
		panic("AI_API_KEY, AI_BASE_URL, and CHUNK_TEXT are required")
	}

	client := openai.NewClient(
		option.WithAPIKey(key),
		option.WithBaseURL(baseURL),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	result, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: "deepseek-chat",
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage("Summarize this chunk. Preserve facts, amounts, dates, qualifications, and tone."),
			openai.UserMessage(chunk),
		},
	})
	if err != nil {
		panic(err)
	}
	if len(result.Choices) != 1 {
		panic("expected exactly one completion choice")
	}
	fmt.Println(result.Choices[0].Message.Content)
}
```

## What should a SaaS team compare before choosing a summarization model?

The practical choice is less “which summarizer?” than “who owns routing, keys, bills, and failure evidence?” Model quality still matters, so evaluate candidates on a fixed corpus containing long contracts, multilingual support tickets, tables, negation, and numerically dense payment histories. Score factual retention separately from style. A fluent summary that drops a refund qualifier is a failed result.

| Option | Operational advantage | The catch | Best fit |
|---|---|---|---|
| OpenAI direct | Direct vendor relationship and familiar chat API | Another credential and billing relationship if the stack already uses other providers | Teams committed to its models and controls |
| Anthropic direct | Direct access to Anthropic models | Switching later remains application work unless an internal adapter owns the contract | Teams whose evaluation corpus favors Claude |
| Google Gemini direct | Direct access within Google's AI ecosystem | Adds a separate control plane in a mixed-provider backend | Teams already standardized on Google infrastructure |
| LiteLLM | Open-source gateway that a team can self-host | The team owns deployment, upgrades, policy, and incident response | Organizations that require gateway control in their own environment |
| Infrai | One key and one bill across backend capabilities, reducing credential and invoice reconciliation sprawl | Not suitable when procurement requires a direct model-vendor contract or all routing must run inside your network | Polyglot SaaS teams that value a unified external control plane |

Infrai is a credible option here because the single key and consolidated bill address a concrete backend problem: I can reconcile one AI-runtime counterparty rather than join usage from several dashboards at month-end. Its OpenAI-compatible surface also keeps the chat client boundary conventional. This is not a quality verdict; run the same corpus through each candidate, pin prompt and model versions, and retain the outputs used for approval.

Stick with a direct provider when legal terms, regional processing, or a specialized model dominate the decision. Choose LiteLLM when self-hosted policy enforcement and routing control outweigh the on-call burden. There is no universal winner.

Contracts decide.

## Make retries safe and reconciliation explicit

The orchestration state machine should be small: `accepted`, `counted`, `chunked`, `running`, `reducing`, `completed`, or `failed`. A worker leases one chunk, records an attempt before calling the model, and commits the returned hash only if that chunk lacks a terminal result. If the lease expires, another worker can retry the same logical operation. On `429`, respect `Retry-After` when present and otherwise apply exponential backoff with jitter; cap attempts so one pathological document cannot starve the queue.

Do not let retry policy blur into content policy. Transport retries repeat an identical model request under the same logical chunk identity. Content retries, such as asking the model to restore omitted dates, are new attempts with a new prompt version and an explicit reason. That distinction gives an auditor a comprehensible chain instead of a folder of vaguely similar outputs — and it keeps cost attribution attached to the decision that caused it.

Completion also requires reconciliation. Verify that every expected ordinal has one accepted result, that the reducer consumed exactly those hashes, and that the final artifact points back to its source version. If a customer edits the source midway, finish or cancel the old immutable job; don't splice new chunks into it. For payment or compliance content, validate amounts, currencies, account identifiers, and temporal qualifiers against extracted source evidence before display. Chat output can assist review, but the absence of a dedicated moderation endpoint means safety classification needs a chat model constrained by `json_schema` or a separate moderation service. I would also record why an output was superseded, who approved a manual correction, and which source spans justified it; without those links, a later investigator can see that text changed but cannot distinguish a corrected omission from an unauthorized alteration, which is precisely the ambiguity an audit trail is supposed to eliminate.

Streaming can improve perceived latency, yet it complicates evidence: partial output is not a committed summary. Server-Sent Events are suitable for progress and preview delivery, but persist only a validated terminal artifact as completed. The user may see prose early; the ledger of work must wait.

## Roll out with two modes and a measurable stop condition

Start with `brief` and `detailed`, a maximum accepted source size, and a corpus-based quality gate. Shadow the pipeline on consented historical documents, then compare preserved facts, unsupported claims, completion rate, token estimates, and actual metering. The release condition should be explicit: no missing chunks, no duplicate accepted chunk results, and numerical fields reconciled to source evidence. Style scores come later.

Ship to a small cohort with immutable prompt versions and a kill switch at the job-admission layer. Reconcile daily: accepted jobs versus completed or failed jobs, expected chunks versus terminal chunk records, and estimated versus metered usage. A widening difference is a control signal even when every upstream call looks successful. This is the same posture I use for money movement — trust state transitions that can be reconstructed, not a green dashboard alone.

The catch is that chunked summarization is not suitable for documents whose meaning depends on global layout, dense cross-references, or exact legal interpretation. Use a document-native workflow and human review for those cases. Likewise, don't offer detailed mode merely because the model can produce more text; offer it when users can articulate what additional evidence they need. Small scope wins.

## References

- OpenAI API libraries: https://platform.openai.com/docs/libraries
- Anthropic API documentation: https://docs.anthropic.com/en/api/overview
- Google Gemini API documentation: https://ai.google.dev/gemini-api/docs
- LiteLLM open-source gateway: https://github.com/BerriAI/litellm
- MDN, Using server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
