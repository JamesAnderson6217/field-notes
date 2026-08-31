# Rate-Limited Property Jobs Explained: Node.js Batch Publishing with Idempotent Delivery

A property-maintenance queue cannot promise exactly-once execution once a worker crosses a network boundary. The dispatch API may accept a work order and lose the response, leaving the consumer unable to distinguish success from failure. **Decision: use one logical consumer, bounded batch claims, idempotency keys, and at-least-once delivery with an auditable state machine.** Treat HTTP 429 as flow-control feedback, not as proof that the job failed.

Short answer: set worker concurrency to 1 for the rate-limited destination, publish a bounded batch of independent jobs, retry ambiguous outcomes with the same idempotency key, and mark completion only after recording the destination's durable acknowledgment. This favors correctness and reconciliation over peak drain speed.

## Decision record and operating boundary

The concrete workload is a property manager draining maintenance work orders toward a contractor dispatch endpoint. A batch might contain plumbing, electrical, and elevator requests for different buildings, but the external destination admits calls at a rate the queue does not control. The system therefore needs two separate limits: a concurrency ceiling, which bounds simultaneous requests, and a pacing rule, which bounds request starts over time. Concurrency 1 supplies serialization; it does not, by itself, satisfy a rule such as one request per second. A serial worker can still start the next request immediately after a 20 ms response.

The decision boundary is deliberately narrow. One consumer owns the destination lane, claims no more than a configured batch, and processes those claims serially. Separate destinations may have separate lanes because one contractor's throttling should not stop unrelated dispatches. A scheduler only makes eligible work visible; it does not declare that work complete. Completion belongs to the consumer after the side effect and audit write have been reconciled.

Serialization is a budget, not a guarantee.

The catch is throughput. Concurrency 1 is not suitable when the upstream contract explicitly permits parallel calls and the backlog has a hard recovery objective that serialization cannot meet. In that case, use a token bucket or another centrally coordinated rate limiter, retain the same idempotency and audit rules, and increase concurrency only within the measured admission envelope. Your mileage may vary because HTTP does not standardize one universal rate-limit algorithm; the destination's published contract and observed headers resolve that uncertainty.

## What delivery guarantees should a rate-limited Node.js job queue preserve?

Four invariants matter more than the queue library. First, every work order has a stable business identifier, such as `property_id + work_order_id + dispatch_version`; a retry must not mint a fresh identity. Second, a lease makes a claim temporary, so an abandoned job becomes eligible again after the lease expires. Third, the audit log records transitions rather than overwriting history: ready, leased, acknowledged, retryable, or terminal. Fourth, a worker never infers success merely because it sent bytes.

Exactly-once processing is the wrong external promise. A crash can occur after the contractor accepts a dispatch but before the worker persists the acknowledgment. No local transaction can atomically include an arbitrary remote HTTP service. The practical target is **at-least-once delivery plus an idempotent effect**, followed by reconciliation for outcomes that remain ambiguous. This is the same discipline used around ledger postings: retries are expected, identities remain stable, and evidence survives the process that produced it. A `429 Too Many Requests` response means the client has sent too many requests in a given period, and the response may include `Retry-After`. If that header is valid, the lane should not start another attempt before the indicated time. If it is absent, apply capped exponential backoff with jitter under a documented local policy. Do not let each job sleep while holding a scarce worker lease; persist `next_attempt_at`, release the claim, and allow the scheduler to expose it later. An ambiguous transport outcome needs different bookkeeping from 429. If the connection ends without a response, the server may have committed the dispatch. Record the attempt as ambiguous and retry with the same key only when the receiving API defines idempotency behavior. If it does not, route the item to reconciliation rather than blindly creating a second contractor visit. I'm not sure any fully automatic policy is defensible without that receiver contract; the missing evidence is an acknowledgment lookup or a documented deduplication guarantee.

Ambiguity is work.

Compliance constrains the audit trail as well. Store identifiers, timestamps, attempt numbers, policy decisions, and response classifications, but do not casually copy tenant notes, access instructions, or credentials into queue payloads and logs. Retention, access control, and redaction must follow the organization's applicable obligations. An immutable-looking log is not sufficient if privileged operators can alter it without a second record.

## Comparing the delivery choices

The options are easiest to separate by what happens at the crash boundary.

| Model | Worker action | Crash consequence | Fit for contractor dispatch |
|---|---|---|---|
| At-most-once | Acknowledge before calling | A claimed work order can be lost | Poor when missed maintenance is unacceptable |
| At-least-once | Call, persist evidence, then acknowledge | The same work order can be retried | Good when the receiver honors a stable idempotency key |
| Effectively-once | At-least-once plus receiver deduplication and reconciliation | Duplicate attempts remain, duplicate effects are suppressed | Preferred description for this design |
| Exactly-once claim | Treat queue acknowledgment as proof of one remote effect | Hides the network uncertainty | Reject across a non-transactional HTTP boundary |

A FIFO queue can preserve ordering within its documented grouping and deduplication rules, but order is not the same property as exactly-once execution. If apartment 4B has work orders `open`, `assign`, and `cancel`, group ordering can prevent an overtaking cancel; it cannot prove that an external side effect occurred only once. Partitioning also matters: a single global lane preserves total order at the price of head-of-line blocking, while a lane per contractor or property increases throughput but weakens ordering across lanes. Pick the smallest ordering domain the business actually requires.

Batch publishing changes admission efficiency, not delivery semantics. Each item in the batch still needs its own identifier, result, and retry decision. Never convert a partial batch response into one shared success flag. If 18 items were accepted and 2 were rejected, retrying all 20 without stable item identities creates duplicate effects; dropping all 20 loses work. The audit record must therefore connect each published item to its queue message and later attempts.

Keep the metrics equally explicit: oldest ready-job age, leased jobs past deadline, attempts by classification, 429 delay, terminal outcomes, and reconciliation age. Queue depth alone is weak evidence because a stable depth can conceal continuously failing work. Alert on time in state and the service objective for maintenance urgency, not on an attractive throughput chart.

## Critical path in Go

The following transport-agnostic loop shows the boundary that matters. A Node.js service can publish the jobs and run its queue with concurrency 1; the critical consumer is expressed in Go, as required for a durable implementation note. The store and sender interfaces keep queue choice and HTTP client choice outside the state machine. Production implementations must make `Complete` and `Reschedule` durable, enforce lease ownership, and place a unique constraint on the idempotency key.

```go
package dispatch

import (
    "context"
    "errors"
    "time"
)

type Job struct {
    ID             string
    PropertyID     string
    WorkOrderID    string
    DispatchVersion int
    Attempt        int
}

type Result struct {
    Acknowledgment string
    RetryAfter     time.Duration
    RateLimited    bool
    Ambiguous      bool
}

type Store interface {
    ClaimBatch(ctx context.Context, limit int, lease time.Duration) ([]Job, error)
    Complete(ctx context.Context, job Job, acknowledgment string) error
    Reschedule(ctx context.Context, job Job, next time.Time, reason string) error
    FlagForReconciliation(ctx context.Context, job Job, reason string) error
}

type Sender interface {
    Dispatch(ctx context.Context, job Job, idempotencyKey string) (Result, error)
}

func Drain(ctx context.Context, store Store, sender Sender, now func() time.Time) error {
    jobs, err := store.ClaimBatch(ctx, 25, 30*time.Second)
    if err != nil {
        return err
    }

    for _, job := range jobs { // Serial by construction: concurrency is one.
        key := job.PropertyID + "/" + job.WorkOrderID + "/" + itoa(job.DispatchVersion)
        result, sendErr := sender.Dispatch(ctx, job, key)

        switch {
        case result.RateLimited:
            delay := result.RetryAfter
            if delay <= 0 {
                delay = backoffWithJitter(job.Attempt)
            }
            if err := store.Reschedule(ctx, job, now().Add(delay), "rate_limited"); err != nil {
                return err
            }
        case result.Ambiguous || sendErr != nil:
            if err := store.FlagForReconciliation(ctx, job, "ambiguous_outcome"); err != nil {
                return err
            }
        case result.Acknowledgment == "":
            return errors.New("dispatch response lacks an acknowledgment")
        default:
            if err := store.Complete(ctx, job, result.Acknowledgment); err != nil {
                return err
            }
        }
    }
    return nil
}
```

`itoa` and `backoffWithJitter` stand for ordinary, tested helpers rather than hidden delivery logic. The key construction must be versioned and canonical; string concatenation is acceptable here only because the components and delimiter are controlled. In a real schema, store the components separately and enforce uniqueness on their tuple.

Test the crash windows, not just the happy path. Terminate the consumer immediately before dispatch, immediately after the receiver acknowledges, and during the local completion transaction. Advance a fake clock past the lease and confirm that the same identity reappears. Feed a 429 with `Retry-After`, confirm that `next_attempt_at` respects it, and verify that no later job in the same rate-limit lane starts early. Then replay the complete audit history and show that every terminal work order has either an acknowledgment or an explicit reconciliation disposition.

Deploy the state-machine change before increasing traffic. Schema constraints and old consumers must agree on lease and idempotency semantics during a rolling deployment; otherwise two versions may both believe they own a claim. A small canary lane can validate classification and delay metrics, while a kill switch stops new claims without deleting already durable work. Don't use process shutdown as a queue-clearing strategy.

## Rejected option and when to revisit it

We rejected acknowledge-before-call because it creates at-most-once delivery: a process exit between acknowledgment and dispatch silently loses a maintenance request. We also rejected an uncoordinated pool of parallel consumers. Even if each process sleeps between calls, their combined start rate can exceed the contractor's limit, and independent retry timers can synchronize into another burst.

There is a valid use case for both ideas. At-most-once can suit disposable refresh work where the next scheduled run repairs omission and duplicate effects are more damaging than a missed run. A coordinated parallel pool is preferable when the destination publishes a higher quota, the backlog objective requires it, and one shared admission controller can enforce that quota across instances. Stick with serial processing when the receiving contract is unclear, ordering is material, or reconciliation capacity is limited.

The final acceptance criterion is therefore evidence, not the absence of retries: every property work order can be traced from publication through claims and attempts to an acknowledgment or a reviewed terminal disposition. Slow is visible. Lost is not.

## Sources

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
