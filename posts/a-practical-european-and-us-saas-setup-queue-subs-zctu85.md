# A Practical European and US SaaS Setup: Queue Subscription, Webhook Ingress, or Polling?

For delayed work in a small SaaS, the useful default is a narrow public HTTPS webhook that durably accepts an event, followed by a worker that pulls scheduled work at a bounded rate. Short answer: choose polling only when the source cannot deliver signed webhooks or the permitted detection delay is comfortably longer than the polling interval; choose queue-to-HTTPS push only when its delivery contract gives the consumer enough backpressure and audit control. The easier setup is the one whose duplicate, delay, and recovery boundaries the team can prove, rather than the one with the fewest boxes on a diagram.

Europe and the US add a placement decision, not a different delivery model. Keep a tenant's accepted payload, schedule, execution log, and replay record in its selected region unless a documented product purpose requires a transfer. A queue is not a compliance boundary by itself. Retention, access, deletion, and subprocessors still need their own review.

## How should a small SaaS choose among public HTTPS webhooks, queue push subscriptions, and polling workers?

Separate ingress from execution before choosing anything. A source webhook and source polling answer how a change reaches the service. A pull worker and a queue push subscription answer how already-accepted work reaches a consumer. Treating them as one comparison produces false choices: a team can receive a webhook and then pull tasks, or poll a source and then dispatch its discoveries through the same delayed-task ledger.

For a modest service with variable inbound traffic, webhook ingestion plus a pull worker usually yields the clearest control surface. The webhook handler has one responsibility: authenticate the request, identify it, and atomically record both the receipt and the future work. The worker decides how many due records to claim, which tenant or account may proceed, and when an overloaded downstream dependency needs a pause. It is deliberately unglamorous.

Polling belongs where it has a concrete advantage. It keeps the service egress-only, it can respect a source's published rate budget, and it is reasonable when the source changes slowly. The catch is that latency becomes a cost and correctness parameter: every interval spends requests even when nothing changed, while a stalled cursor can look exactly like an idle source unless cursor age is measured. Don't choose a five-second interval just to simulate a webhook. First state the maximum acceptable detection delay, the source's cursor semantics, the retry window, and who owns the resulting audit trail.

Queue push to a public consumer can remove a continuously running pull loop and may fit request-oriented deployment environments. It moves admission control to the response path, however. The consumer must return a result that faithfully represents durable acceptance, and it must have an explicit policy for retries, limits, and quarantined messages. Pull is often easier to reason about when one dependency has a strict concurrency or ordering limit because the worker can stop claiming work before the dependency is saturated.

No transport supplies exactly-once business effects by magic. Public delivery can repeat, poll pages can overlap, a lease can expire after an effect has committed, and a process can end after the database commits but before it records its response. Design for at-least-once delivery and protect the business operation with a stable idempotency key plus a unique constraint. For payments, entitlements, and ledger postings, that constraint is part of the product's correctness contract.

## What must cross the durable acceptance boundary?

The acknowledgment line matters more than the queue brand. Before responding successfully to a webhook, store the raw receipt or a controlled payload reference, its source event identity, the intended task identity, and an outbox entry in one database transaction. If the transaction does not commit, do not acknowledge. If a duplicate source identity conflicts with the unique constraint, acknowledge the duplicate without creating a second task. The same rule applies to polling: insert the discovered event and advance the cursor in one transaction. Advancing first risks a permanent gap; inserting first without a duplicate-safe cursor policy risks rework after interruption. A provider-issued opaque cursor is preferable when available. If the source exposes only timestamps, use a stable compound position where possible and deliberately overlap the query window, then let the inbox uniqueness rule collapse repeated observations. This is where an exactly-once mindset is valuable, even though exactly-once transport is not available. The desired result is one durable business effect for one logical event. The proof needs records that survive retries: an immutable inbox identity, an outbox publication state, task attempts, a lease owner and expiry, and the idempotency key used by the final operation. A broker's FIFO or deduplication feature can reduce redundant delivery, but it does not replace the database constraint that protects a financial posting.

The scheduling record should keep `run_at` in UTC and should make a claim explicit. A worker claims a limited batch of due rows, gives each claim a lease, and records the terminal outcome only after the business transaction commits. A lease is recovery machinery, not a promise that two attempts can never overlap; therefore the business operation must remain idempotent after a lease expires.

The ledger is the authority.

Here is a deliberately small Go shape for the boundary. The interface hides storage details, while the important contract is visible: `Accept` commits the inbox and outbox together, and reports a duplicate as a normal result.

```go
package delivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type Event struct {
	ID      string          `json:"id"`
	TaskID  string          `json:"task_id"`
	RunAt   time.Time       `json:"run_at"`
	Payload json.RawMessage `json:"payload"`
}

type Store interface {
	Accept(ctx context.Context, event Event, raw []byte) (duplicate bool, err error)
}

type Receiver struct {
	Store  Store
	Verify func(body []byte, signature string) bool
}

func (h Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if !h.Verify(body, r.Header.Get("X-Webhook-Signature")) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var event Event
	if err := json.Unmarshal(body, &event); err != nil || event.ID == "" || event.TaskID == "" {
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
	}
	if event.RunAt.IsZero() {
		event.RunAt = time.Now().UTC()
	}

	duplicate, err := h.Store.Accept(r.Context(), event, body)
	if err != nil {
		return
	}
	if duplicate {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
```

The missing implementation is intentionally ordinary SQL: an inbox table with a unique source identity, an outbox table with a unique task identity, and a transaction that inserts both. Do not publish before that commit. An outbox dispatcher can safely attempt publication again after a restart because publication is downstream of the authoritative record; the consumer then applies its separate business idempotency key.

## Which delivery model exposes the right operational evidence?

The comparison is less about convenience than about what can be observed and reconciled. In all four models, retain enough evidence to answer three questions later: was an event accepted, was its scheduled action attempted, and did the business effect occur once?

| Model | Main control point | Evidence that must remain | Not suitable when |
| --- | --- | --- | --- |
| Signed webhook plus pull worker | Worker concurrency and claim size | Inbox, outbox, lease, attempts, effect key | A public endpoint is prohibited by the source or security policy |
| Signed webhook plus queue push consumer | Consumer response and delivery policy | Inbox, delivery response, attempts, effect key | Downstream capacity changes faster than the consumer can safely signal |
| Source polling plus worker | Poll interval, cursor, and worker claims | Cursor position, poll attempts, inbox, effect key | Required detection latency is shorter than the allowed poll cadence |
| Database-backed scheduler | Due-row query and leases | Task rows, leases, attempts, effect key | Scheduled work competes materially with core transactional load |

For a new service that already has a relational database, a database-backed scheduler can be the smallest correct first step. Use bounded due-row claims and leases, index the due-work query, and keep execution outside the transaction that selects rows. Add a separate queue when bursts, independent scaling, or failure isolation justify another system to operate. The added component should buy a named property, such as independent buffering or regional separation, rather than an abstract sense of future readiness.

Ordering deserves an equally narrow statement. Serialize only where the business rule demands it, such as per account or ledger. Global ordering tends to turn one local invariant into a throughput bottleneck. FIFO queues can offer ordered processing within their documented grouping and deduplication semantics, but application records still establish the commit order and reconciliation proof. The AWS documentation describes these FIFO capabilities; it does not eliminate the need for a business-level idempotency boundary.

Observability should favor age and completeness over request counts. Track the age of the oldest unclaimed due task, the age of each source cursor, lease expirations, duplicate rate, retry count, and the difference between accepted inbox identities and terminal task outcomes. A 429 response belongs in those records even if a later retry succeeds, because it explains why latency changed. Short graphs are not enough. A periodic reconciliation job should compare the inbox, task ledger, and final business records by identity and report the missing sides.

Regional operation makes that reconciliation more valuable. Keep regional queues, task stores, and replay logs together; route by tenant before acceptance when the product has selected a region. A central scheduler can send a minimal job identifier to the tenant's region, while copying entire payloads into a global control plane expands the retention and deletion surface. The exact legal requirements depend on contracts, data categories, and transfer mechanisms, so counsel must validate the policy. Engineering can make the policy enforceable through tenant routing, retention jobs, access logging, and deletion workflows.

## How can a team roll this out without losing scheduled work?

Begin with a written invariant: one logical source event produces at most one business effect, every accepted event has a traceable terminal disposition, and a delayed task has a region, `run_at`, and replay history. Then run the new path in observation mode, recording duplicate-safe inbox entries and comparing its task decisions against the existing path without executing both effects. This turns migration into a reconciliation exercise instead of a release-day bet.

Next, enable execution for a small tenant cohort, with tenant-scoped idempotency keys and a clear rollback rule that stops new claims while leaving accepted records intact. Replays should use the immutable inbox identity and create a new attempt record; they should never fabricate a new source event. Test duplicate webhook deliveries, overlapping poll windows, reordered events, a canceled client request after commit, worker termination after the effect commit, lease expiry, and clock boundaries around `run_at`.

Finally, move only after the ledger agrees: accepted events reconcile to terminal tasks, terminal financial tasks reconcile to postings, and cursor age remains within the declared delay budget. This approach is not suitable when the team cannot retain the minimum evidence needed for audit or must process a source whose contract offers neither stable identifiers nor a recoverable change feed. In those cases, keep the source integration simpler, limit the supported business action, or require a stronger upstream contract before automating an irreversible effect.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://cloud.google.com/pubsub/docs/overview
