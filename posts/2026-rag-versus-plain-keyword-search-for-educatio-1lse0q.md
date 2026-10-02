# 2026 RAG Versus Plain Keyword Search for Educational Product Catalog PDFs

Use plain keyword search as the shipping baseline for a folder of educational product catalog PDFs, and add retrieval-augmented generation (RAG) only when users need synthesized answers rather than matching passages. **Short answer:** the deciding constraint is evidence handling, not implementation fashion: plain search is simpler to ship in a Node.js service because it has the smaller failure surface and shorter critical path, while RAG earns its extra state only when an answer must combine or explain retrieved text. In 2026, I would record the boundary explicitly, keep one versioned document index behind both modes, and require every generated claim to retain page-level provenance.

This architecture decision record treats correctness the way a ledger service treats money movement. A response needs an idempotency key, an immutable record of its inputs, and a reconciliation path from displayed text back to the PDF pages that authorized it. Exactly-once delivery is not something an application can casually promise across extraction, indexing, retrieval, and generation; the practical target is replay-safe processing plus auditable outcomes.

## Should a product catalog use RAG or plain keyword search?

The workload is a folder of PDFs containing course titles, prerequisites, credit values, enrollment rules, and policy prose. The user may ask `CS 204 prerequisite`, which is retrieval, or `Can a first-year student take CS 204 while completing MATH 110?`, which may require assembling evidence from separate passages. Those are different contracts even though both arrive through one search box.

The decision is to ship a lexical path first and make generation an evidence-gated second stage. The index stores normalized text, document identity, content hash, page number, extraction version, and effective catalog version. A query returns passages with those identifiers. Generation may consume the same passages, but it cannot silently broaden the evidence set or erase provenance.

The invariants are narrow on purpose:

1. The displayed source identifies the exact document version and page used for the answer.
2. Reprocessing identical bytes with the same extraction version produces the same chunk identities.
3. A catalog replacement never creates a mixed answer from superseded and current versions unless the query explicitly asks for comparison.
4. No evidence means no synthesized answer. The system may return no match or ask for a narrower question.
5. A replay with the same request key and index snapshot remains recognizable in the audit trail, even if an upstream call times out and the client retries.

That last condition is an exactly-once mindset, not a magical exactly-once claim. It prevents a retry from becoming an untraceable second answer whose sources differ only because an index update landed between attempts.

## Failure boundaries and compliance limits

PDF ingestion, indexing, retrieval, and answer composition fail independently. Scanned pages can yield no usable text; duplicated catalogs can create conflicting passages; a stale index can cite a withdrawn policy; lexical retrieval can miss a paraphrase; semantic retrieval can surface a related but non-authoritative passage; and generation can state more than its evidence supports. Treating these as one generic `search failed` event destroys the information needed for reconciliation.

The audit record should preserve the request key, query, index snapshot, retrieval mode, ordered chunk IDs, document hashes, page references, generation policy version, disposition, and timestamps. It should not indiscriminately preserve every prompt or document body. Under the GDPR data-minimization principle, personal data must be adequate, relevant, and limited to what is necessary; under FERPA, education records can carry access constraints. Those rules do not prescribe this architecture, but they do rule out using an unlimited prompt log as an informal audit system. Retention, deletion, access control, and regional requirements still require review by the institution's legal and security owners.

Keep the failure boundary visible.

A timeout after an answer is committed is not the same event as a timeout before retrieval. A request ledger can resolve that ambiguity: reserve the idempotency key, bind it to the query hash and index snapshot, record each state transition, and return the committed result on a valid replay. If the same key arrives with a different query hash, reject it rather than guessing.

## Options under the quality-latency constraint

No universal latency number belongs in this ADR because PDF count, index placement, model choice, and concurrency determine it. The useful comparison is the number and nature of critical-path stages, followed by measurements on the institution's own question set.

| Option | Critical path | Quality boundary | Operational burden | Appropriate use |
|---|---|---|---|---|
| Lexical retrieval | Parse query, search index, return passages | Misses wording that shares little vocabulary; preserves exact matched text | One retrieval system plus ingestion | Course codes, named policies, exact prerequisites, browse-and-open workflows |
| Retrieval plus generation | Retrieve passages, compose prompt, generate, validate citations | Can combine passages, but output may exceed or misread evidence | Adds model calls, policy versions, answer auditing, and another timeout boundary | Explanatory questions supported across multiple passages |
| Evidence-gated dual path | Retrieve first, route only eligible questions to generation | Quality depends on routing and evidence thresholds as well as retrieval | Adds a router and evaluation set, while keeping simple queries short | Mixed workloads with exact lookup and cross-passage questions |

The dual path is the selected design. Its trade-off is extra routing policy and evaluation work; for a small product catalog dominated by exact identifiers, that burden is a reason to choose plain keyword search alone. Lexical retrieval also has a clear limitation: it can miss an answer expressed with different vocabulary. RAG has the opposite limitation: it can produce readable prose that exceeds the retrieved evidence, and its additional critical-path work makes it unsuitable when the response contract is an immediate exact lookup. These are architectural boundaries, not product endorsements.

The choice does not rest on a claim that lexical retrieval is always more relevant or that generation is always slower by a fixed amount. It says something testable: exact lookups should not pay for an unnecessary generation stage, and composed answers should not be emitted without attributable evidence. Measure retrieval quality and end-to-end latency separately. Otherwise a fluent answer can conceal poor retrieval, while a fast passage list can conceal missed paraphrases. The same rule applies when a Node.js API owns the request boundary and a Go worker owns ingestion: language choice does not remove the evidence contract.

Evaluation starts with labeled questions drawn from the actual catalogs. Record whether the authoritative page appears in the retrieved set, whether the final answer is fully supported by those pages, whether citations resolve to the indexed snapshot, and the latency distribution for each route. Include adversarial cases: two catalog years with different prerequisites, a course code reused in an appendix, a scanned page, a question with no answer, and two policies that appear to conflict. Reconciliation is the practice of comparing the answer ledger against the indexed evidence after policy or extraction changes.

## Critical path in Go

The following sketch keeps providers out of the domain contract. It omits transport and storage details, but the state transitions are explicit enough to test. Every dependency receives the immutable snapshot selected at reservation time.

```go
package answer

import (
	"context"
	"errors"
)

type Request struct { IdempotencyKey, Query string }
type Chunk struct { ID, DocumentHash, Text string; Page int }
type Result struct { Kind, Answer, Snapshot string; Evidence []Chunk }
type Reservation struct { Snapshot string; Existing *Result }

type Ledger interface {
	Reserve(context.Context, string, string) (Reservation, error)
	Commit(context.Context, string, Result) error
	Fail(context.Context, string, string) error
}
type Retriever interface { Search(context.Context, string, string) ([]Chunk, error) }
type Composer interface { Compose(context.Context, string, []Chunk) (string, error) }
type Router interface { NeedsComposition(string, []Chunk) bool }

type Service struct { ledger Ledger; retriever Retriever; composer Composer; router Router }
var ErrNoEvidence = errors.New("no attributable evidence")

func (s Service) Answer(ctx context.Context, req Request) (Result, error) {
	r, err := s.ledger.Reserve(ctx, req.IdempotencyKey, req.Query)
	if err != nil { return Result{}, err }
	if r.Existing != nil { return *r.Existing, nil }

	chunks, err := s.retriever.Search(ctx, r.Snapshot, req.Query)
	if err != nil {
		_ = s.ledger.Fail(ctx, req.IdempotencyKey, "retrieval_failed")
		return Result{}, err
	}
	if len(chunks) == 0 {
		_ = s.ledger.Fail(ctx, req.IdempotencyKey, "no_evidence")
		return Result{}, ErrNoEvidence
	}

	result := Result{Kind: "passages", Evidence: chunks, Snapshot: r.Snapshot}
	if s.router.NeedsComposition(req.Query, chunks) {
		result.Answer, err = s.composer.Compose(ctx, req.Query, chunks)
		if err != nil {
			_ = s.ledger.Fail(ctx, req.IdempotencyKey, "composition_failed")
			return Result{}, err
		}
		result.Kind = "composed_answer"
	}
	if err := s.ledger.Commit(ctx, req.IdempotencyKey, result); err != nil { return Result{}, err }
	return result, nil
}
```

The important line is not the call to `Compose`; it is the snapshot binding established before retrieval. Without it, a retry can select a new catalog generation and produce a different answer under the same idempotency key. Production code also needs a query hash bound to the reservation, authorization before retrieval, bounded evidence sizes, cancellation, and a ledger state for indeterminate commit outcomes. Those concerns belong in tests because each changes user-visible behavior.

Deployment should begin with shadow routing: calculate whether a question would be composed, but continue returning passages until the labeled evaluation set and access controls are ready. Observability should distinguish ingestion age, extraction failures, retrieval with zero evidence, route choice, composition failures, citation-resolution failures, and commit ambiguity. Aggregate latency by route and index snapshot; never use one average to represent both a one-stage lookup and a multi-stage answer.

## Rejected option and its valid use case

We reject generation for every query. It expands the critical path and the audit surface even for exact course-code lookups, where returning the authoritative passage is the clearer contract. It also makes graceful degradation harder: a retrieval result can still be useful when composition is unavailable, while a generation-only interface may turn the same condition into a total failure.

The rejected option remains valid for a tightly bounded corpus where nearly every accepted question requires cross-passage synthesis, users explicitly want prose answers, all outputs carry resolvable citations, and the operator has evaluated unsupported claims and latency under representative load. In that environment, maintaining two response modes may create more policy complexity than it removes. The choice follows the query distribution, not a slogan.

For the course-catalog folder described here, **ship the passage path first, preserve stable evidence identities, and promote only measured question classes to composition**. This keeps simple lookup simple without pretending that keyword matching can answer every policy question. It also creates a reversible decision: retrieval evaluation, audit records, and page-level provenance remain useful if the generation stage changes later.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- GDPR Article 5, principles relating to processing of personal data: https://eur-lex.europa.eu/eli/reg/2016/679/art_5/oj
- Family Educational Rights and Privacy Act regulations, 34 CFR Part 99: https://www.ecfr.gov/current/title-34/subtitle-A/part-99
