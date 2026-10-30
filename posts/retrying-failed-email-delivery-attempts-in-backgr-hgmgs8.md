# Retrying Failed Email Delivery Attempts in Background Queue Processing: A 5-Step Solution

**Short answer:** the best solution for retrying failed email delivery is a durable background queue with bounded exponential backoff, a stable idempotency key, and a separate reconciliation record; choose a simple scheduler for a small trigger, a task queue for ordinary retries, or a workflow engine when long waits and execution history are first-class requirements.

A failed report email should be retried by a durable worker, but the retry must be a new delivery attempt for the same logical message, not permission to create another message. For a logistics system, I would persist the report intent, assign it an idempotency key, retry transient failures with bounded backoff, and keep the reservation-expiry event separate from the email's delivery state. That separation is the important trade-off: delivery can be delayed while the ledger of held and released capacity remains correct.

The practical target is a daily report, perhaps sent after a fixed hold window has expired for stale reservations. The system needs to answer two different questions: did the reservation transition happen once, and did the notification eventually reach the mail provider? Treating those as one queue item makes recovery ambiguous.

## What should a retry mean for a failed email?

A retry means another attempt to deliver one already-created message. It does not mean rerunning the whole report query, reopening a reservation, or generating a fresh message identifier. The worker should load a durable delivery record, check its current state, and claim the next attempt under a lease or compare-and-set operation.

For a daily logistics report, a useful record has a stable `report_id`, recipient or recipient-group identity, content version, `attempt_count`, `next_attempt_at`, and an outcome class. The content version matters because an operator replaying yesterday's report must not silently replace it with today's data. An audit trail should record the enqueue decision, every attempt, the provider response category, and the terminal decision.

The exactly-once mindset is valuable here, but exactly-once email delivery is not something the application can usually prove across its database and an external mail system. The defensible guarantee is narrower: exactly-once creation of the logical delivery intent in the application, at-least-once attempts at the boundary, and idempotent handling wherever the receiving side offers a key or message identifier.

Keep it boring.

In ledger work, I initially treated a successful queue acknowledgement as evidence that the side effect was complete; the useful correction is to treat acknowledgement as evidence only about the queue transaction. The mail boundary still needs its own state, because a worker can receive a timeout after the provider has accepted the request, and a second attempt can then duplicate the message unless the same logical key is reused. That distinction is small in code and large in reconciliation.

## How do backoff and idempotency work together?

Backoff controls pressure; idempotency controls duplication. They solve different failure modes. A worker can retry too quickly and overload a provider, or it can wait politely while still sending the same logical report twice after a timeout.

Classify outcomes before choosing a delay. A connection timeout, temporary service response, or throttling response is normally retryable after a bounded delay. An invalid recipient, rejected policy, or malformed message should move to a terminal or review state. The classification should use the provider's documented response semantics rather than treating every non-success as interchangeable.

One workable schedule is five attempts with delays of 1 minute, 5 minutes, 30 minutes, 2 hours, and 12 hours, with jitter added so a shared failure does not release every worker at once. Those numbers are policy, not a universal standard. The important properties are a cap, an auditable reason for each transition, and a dead-letter or review path after the cap.

A worker can express the state transition in a small Go example. The database transaction that claims an attempt is the critical part; the send itself is outside that transaction, so a crash between the provider call and the local acknowledgement remains possible.

```go
type Delivery struct {
    ID             string
    IdempotencyKey string
    AttemptCount   int
    NextAttemptAt  time.Time
    State          string
}

func retryable(code int) bool {
    return code == 408 || code == 429 || code >= 500
}

func nextDelay(attempt int) time.Duration {
    delays := []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 12 * time.Hour}
    if attempt < 1 {
        attempt = 1
    }
    if attempt > len(delays) {
        attempt = len(delays)
    }
    return delays[attempt-1]
}
```

The example deliberately does not claim that an HTTP status code alone proves whether a message was accepted. A timeout after the request leaves the outcome unknown. The recovery rule should therefore reuse the same idempotency key, consult the provider's result facility when available, and record an `unknown` outcome for operator review when no such facility exists.

That unknown state deserves more care than a generic retry flag. Suppose the 09:00 report worker claims delivery `report-2026-10-23-warehouse-17`, sends the request, and loses its connection before reading the response. The database knows that an attempt began; it does not know whether the provider queued the message. A blind retry can produce two emails, while suppressing the retry can leave the recipient without the report. The useful design is to preserve the attempt as `unknown`, retry with the same idempotency key only when the boundary supports deduplication or status lookup, and otherwise route the record to a review queue with the original content version attached. The operator can then reconcile the provider result against the stable delivery identifier instead of reconstructing intent from worker logs. This is slower than pretending every timeout is transient, but it makes the trade-off visible and keeps the reservation ledger independent of notification uncertainty.

## Where does reservation expiry fit?

The hold-window job should be its own state transition. At `expires_at`, a transaction can move a reservation from `held` to `expired` only if it is still held; a repeated scheduler tick then finds no eligible row and makes no second capacity release. The daily report consumes that committed fact. It should never be the mechanism that expires the reservation.

This design handles a common queue failure: the worker succeeds in releasing capacity, crashes before publishing the email task, and later reprocesses the expiry record. An outbox row written in the same transaction as the reservation transition gives the publisher a durable fact to deliver. A unique constraint on the transition or outbox event key prevents duplicate logical messages, while the email delivery record handles attempts independently.

The audit record should preserve old states rather than merely overwriting `last_error`. For reconciliation, I want to compare reservation transitions, outbox events, delivery attempts, and provider acknowledgements by stable identifiers. Compliance changes the retention and access rules: email addresses, message content, and delivery metadata may be personal data, so minimization, access control, retention limits, and applicable privacy obligations belong in the design review. A retry queue is not an exemption from those controls.

## What should be compared before choosing a scheduler?

The scheduler is only one part of the system. A periodic operating-system scheduler is adequate for a small, single-host trigger when the durable queue and database enforce the real state machine; cron is a time-based command scheduler, not a delivery ledger. A task queue library can provide delayed jobs and worker coordination, but its retry semantics, acknowledgement timing, and durability must be checked against the queue's documentation. A workflow engine can represent long waits and explicit state, at the cost of a larger operational model.

| Option | Interface | Setup burden | Fits best | Main boundary |
| --- | --- | --- | --- | --- |
| Cron | Time-based command | Low | A small, single-host trigger | It does not provide a delivery ledger |
| Celery | Task and worker API | Moderate | Delayed jobs with worker coordination | Retry and acknowledgement behavior needs explicit review |
| Sidekiq | Worker and retry-set model | Moderate | Failed jobs that need later processing | Its retry store is not the external mail side effect |
| Temporal | Workflow and timer model | Higher | Long waits with execution history | It adds a larger operational model |

My decision rule is to pay for the smallest operational model that can expose the failure state I must reconcile. A daily report with five bounded attempts does not automatically justify a workflow engine; a multi-day hold window, human review, and replayable execution history may justify one.

For a concrete comparison, Celery documents automatic retries and countdown or ETA scheduling, Sidekiq documents retry sets and exponential backoff, and Temporal documents durable timers and workflow execution. Those are different boundaries: a task queue generally manages work attempts, a retry set stores failed jobs for later processing, and a workflow engine models a longer-lived execution history. None of those facts removes the need for an application-level idempotency key around an external email call.

The decision criteria should be measurable in the proposed environment: can operators inspect one logical delivery and its attempts, can workers reclaim a lease after a crash, can the queue survive a broker restart, can scheduled work be bounded by a hold-window deadline, and can a replay preserve the original content version? Also check deployment and developer experience. A simple trigger may be easier to operate, while a distributed scheduler may provide clearer visibility for many workers; the correct choice follows the failure and audit requirements, not the feature count.

## A compact rollout for the daily report

Start with the data model and reconciliation query. Add the reservation transition, outbox event, delivery record, attempt log, and terminal states before enabling automatic retries. Test duplicate scheduler ticks, worker crashes before acknowledgement, timeouts after provider acceptance, throttling, malformed recipients, clock skew around the hold deadline, and replay of an old content version.

Then run the publisher and worker in shadow or review mode, with a bounded retry cap and an explicit queue for unknown outcomes. Monitor age of the oldest pending report, retry rate by outcome class, duplicate-key conflicts, expired reservations without an outbox event, and outbox events without a terminal delivery state. These signals expose correctness gaps more directly than aggregate “email sent” counts.

The final rule is compact: make reservation expiry atomic, make report creation durable, make each delivery intent idempotent, and treat every uncertain provider result as a reconciliation problem. A scheduler can wake the work up. It cannot make an external side effect exactly once by itself.

## Sources

- https://en.wikipedia.org/wiki/Cron
- https://docs.celeryq.dev/en/stable/userguide/tasks.html#retrying
- https://github.com/sidekiq/sidekiq/wiki/Error-Handling
- https://docs.temporal.io/workflows#durable-execution
- https://datatracker.ietf.org/doc/html/rfc9110#name-status-codes
- https://datatracker.ietf.org/doc/html/rfc9457
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
