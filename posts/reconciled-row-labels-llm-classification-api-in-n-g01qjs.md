# Reconciled Row Labels: LLM Classification API in Node.js CSV Workloads

Short answer: a cheap bulk CSV tagging job is a bounded, restartable workflow that accepts each classification exactly once; choose an LLM API only after deterministic rules have failed, and compare candidates with a representative sample rather than a published unit price.

The operational constraint changes the decision. A Node.js process can issue many inexpensive calls and still create an expensive correction exercise if a restart duplicates accepted rows, a prompt revision divides one file into two undocumented populations, or a syntactically valid response assigns a label outside the approved taxonomy. **The decision is therefore to optimize for reconciled outputs, then throughput, then request price.** This is an architecture decision record for that order of priorities, not a ranking of providers.

## Decision and invariants

Treat the import as a ledger with three append-oriented records: a work item, an attempt, and an accepted result. A work item identifies the source file, row, normalized text digest, taxonomy version, and prompt version. An attempt records one dispatch and its outcome. An accepted result refers to one successful attempt and is protected by a uniqueness constraint over the stable work identity and configuration versions.

That distinction permits at-least-once execution without permitting duplicate business results. A worker may lose an acknowledgement after remote inference has completed, so no client can infer exactly-once execution from a network exchange alone. It can, however, retry the work and let a transactional acceptance boundary decide whether the result is new. Exactly once belongs at acceptance.

Do not edit a label in place. When the taxonomy, prompt, or selected model changes, append a newly versioned result and preserve the old evidence; downstream consumers should receive an explicit active version, not whichever value happened to be written last. The required audit trail is compact but non-negotiable: stable row key, input digest, configuration versions, model identifier returned by the adapter, attempt identifier, accepted result, timestamps, and a reference to the retained raw response where policy permits retention.

There is a compliance boundary before there is an API decision. If input text can contain personal, financial, contractual, or otherwise controlled data, the team must settle permitted processing locations, access, retention, deletion, and review before dispatch. An API does not make the resulting system compliant. The application remains responsible for what it sends and what evidence it keeps — and some organizations should keep the entire classification path inside their controlled environment.

## How should a Node.js batch job run bulk CSV tagging through an LLM API?

Keep Node.js at the orchestration boundary: stream rows, validate the schema, create stable keys, persist the manifest, and place eligible work into a bounded queue. Do not load the complete CSV into memory, and do not use output-file position as completion state. A quoted newline, a corrected source export, or a partial write can make positional recovery ambiguous.

Each worker should claim one durable work item, ask an adapter for a classification, validate the response against an enumerated taxonomy, and propose an acceptance record. The adapter owns transport details and provider-specific error classification. The ledger owns business identity and acceptance. Mixing those responsibilities makes retries dangerous because a transport decision can silently become a data mutation.

A request should contain the minimum text needed for the label and a fixed output contract. Free-form explanation is useful only when a reviewer or downstream rule consumes it; otherwise it increases output volume and creates another field that must be validated and retained. Reject unknown labels. Don't repair a near match by lowercasing, trimming punctuation, or choosing the closest allowed value unless that normalization was designed, versioned, and tested in advance. `chargeback` and `charge-back` are different values to a strict downstream system even when a human guesses the intended equivalence.

Concurrency is a control loop, not a launch argument. Begin with a small worker pool, observe completion latency and throttling signals exposed by the chosen API, and raise the bound only while the durable pending count, acceptance rate, and retry rate remain intelligible. Retry only failures that the adapter has classified as transient; add jitter, cap attempts, and preserve every attempt. If the API offers an idempotency mechanism, derive its key from the stable work identity, but retain the local ledger because remote retention and semantics may differ.

Budget before dispatch.

Estimate the job from a representative sample that preserves the long tail of row lengths. Include input and output volume, expected validation rejects, controlled retries, and human review; then place a ceiling on new dispatch while allowing in-flight work to settle. I'm not sure a unit-price comparison can answer the word “cheap” for any real dataset without those measurements. Review labor and duplicate inference may dominate, and only a sample run can resolve that uncertainty.

## Failure boundaries and option comparison

Quiet failures deserve more attention than loud ones. The CSV parser can shift columns after malformed quoting; two workers can race to accept different labels; a deployment can change the taxonomy halfway through a shard; an output writer can acknowledge before durable storage; or a resumed job can submit rows that were already accepted. Every boundary needs both an invariant and a reconciliation query. Compare counts for discovered, eligible, attempted, accepted, rejected, quarantined, and pending records, but do not stop at counts: every accepted result must join to the expected input digest and configuration versions, and every active label must resolve to exactly one accepted result.

Consider a 100,000-row import that stops after the process has dispatched row 61,240. If completion exists only in worker memory, restarting from row 61,241 assumes that every earlier response reached durable storage; restarting from row one assumes none of them did. Both assumptions are unsupported. With a manifest and acceptance ledger, the recovery query selects work identities with no accepted result under the current prompt and taxonomy versions. Some remote calls might have occurred twice after ambiguous acknowledgements, but the database still exposes one accepted business result. The audit record then explains which attempts competed, which result became authoritative, and why downstream publication did not duplicate a row. This is the long paragraph because this is the expensive failure: without a durable join key, neither the CSV output nor an invoice can reconstruct provenance.

| Execution shape | Recovery boundary | Audit quality | Cost control | Suitable use |
|---|---|---|---|---|
| Bounded per-row workers | Resume by stable work identity | Strong when every attempt is journaled | Fine-grained dispatch ceiling | Mixed row sizes or recurring imports |
| Asynchronous managed batch | Resume by submitted manifest and returned identifiers | Strong if both sides are retained | Job-level forecasting | Large, non-urgent, immutable backfills |
| Local model or deterministic classifier | Re-run from pinned artifact and input | Strong if artifact and environment are versioned | Capacity replaces per-call accounting | Controlled data or steady volume |
| One request containing many rows | Repeat or reconstruct the whole payload after ambiguity | Weak unless row correlation is explicit | Fewer requests, larger duplicate-work boundary | Small, disposable datasets |

No row is free if it must be investigated twice.

Category selection matters as well. A reranker orders supplied candidates by relevance; it does not, by that fact alone, establish the closed-taxonomy classification contract required here. A speech-recognition model converts audio into text and addresses a different input problem. Names such as “AI API” hide these distinctions, so evaluate the actual input, output, batching, retention, and error contracts rather than treating adjacent model capabilities as interchangeable.

## Critical acceptance path in Go

The implementation below omits transport routes deliberately. A Node.js service may create the manifest and queue entries, while the same contract can be implemented in any worker language; this Go sketch isolates the part that must survive provider changes. The database implementation of `Accept` must use a transaction and a unique constraint for the work key, digest, prompt version, and taxonomy version.

```go
package tagging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type Item struct {
	Key             string
	Text            string
	PromptVersion   string
	TaxonomyVersion string
}

type Classification struct {
	Label       string
	ModelID     string
	RawResponse json.RawMessage
}

type Classifier interface {
	Classify(context.Context, Item) (Classification, error)
}

type Ledger interface {
	Accepted(context.Context, Item, string) (bool, error)
	BeginAttempt(context.Context, Item, string) (string, error)
	RejectAttempt(context.Context, string, string) error
	Accept(context.Context, string, Item, string, Classification) error
}

func Process(
	ctx context.Context,
	item Item,
	allowed map[string]struct{},
	classifier Classifier,
	ledger Ledger,
) error {
	digest := sha256.Sum256([]byte(item.Text))
	inputHash := hex.EncodeToString(digest[:])

	done, err := ledger.Accepted(ctx, item, inputHash)
	if err != nil || done {
		return err
	}

	attemptID, err := ledger.BeginAttempt(ctx, item, inputHash)
	if err != nil {
		return err
	}

	result, err := classifier.Classify(ctx, item)
	if err != nil {
		if recordErr := ledger.RejectAttempt(ctx, attemptID, err.Error()); recordErr != nil {
			return fmt.Errorf("classification failed: %v; record attempt: %w", err, recordErr)
		}
		return err
	}

	if _, ok := allowed[result.Label]; !ok {
		reason := fmt.Sprintf("label %q is outside the taxonomy", result.Label)
		if err := ledger.RejectAttempt(ctx, attemptID, reason); err != nil {
			return err
		}
		return fmt.Errorf("%s", reason)
	}

	return ledger.Accept(ctx, attemptID, item, inputHash, result)
}
```

The second `Accept` call after a lost acknowledgement should observe the existing accepted tuple and return without creating another business result. Meanwhile, a changed prompt or taxonomy produces a distinct tuple rather than overwriting history. This is the exactly-once mindset in practical form: execution can repeat, evidence remains append-oriented, and publication selects one valid version through an explicit rule.

## Rejected design and its valid use case

Reject a plain script that writes each response directly into a new CSV when labels influence payments, account treatment, compliance review, customer communication, or any other consequential workflow. The file has no authoritative attempt history, concurrent appends complicate recovery, and “last written value” is not an acceptance policy. Reject one giant synchronous prompt for the same class of workload: an ambiguous response or validation failure expands the retry boundary to many rows at once.

The catch is real. A ledger-backed queue adds transactional storage, schema ownership, reconciliation queries, retention controls, deployment discipline, and on-call surface. It is not suitable for a tiny, disposable, de-identified dataset whose labels will be inspected once and never drive an automated decision. Stick with a single-process script there: keep the source immutable, write stable row keys and configuration versions beside every result, cap concurrency, and make the output replaceable.

Deterministic rules are also the better choice when an explicit expression covers the taxonomy with acceptable accuracy. Local execution is the stronger boundary when policy forbids sending the text to a remote processor, although the team then owns model artifacts, capacity, patching, and observability. Managed asynchronous batches fit immutable backfills that can wait; bounded per-row workers fit continuous arrivals and uneven row sizes. **The correct option is the least complicated one that preserves the evidence demanded by the consequence of a wrong label.**

## Sources

- https://docs.cohere.com/docs/rerank-overview
- https://github.com/openai/whisper
