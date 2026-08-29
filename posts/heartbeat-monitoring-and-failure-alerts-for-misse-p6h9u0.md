# Heartbeat Monitoring and Failure Alerts for Missed Scheduled Job Runs

Short answer: monitor each expected schedule slot from outside the worker, require separate started and completed heartbeats, and alert on a missing state only after a region-specific grace window; for an AI agent loop, retain a small immutable run ledger and sampled diagnostic traces rather than every verbose event.

The bill is made from more than alert evaluations. It includes telemetry ingestion, indexed storage, retention, queries, and sometimes cross-region transfer. Before choosing a monitor, calculate those terms per scheduled run and assign them to a stable tuple such as `workflow_id`, `schedule_slot`, and `region`. In a logistics system, that makes a missed customs-document classification run both operationally visible and chargeable to the workflow that created it.

This distinction matters because a failed process can emit a heartbeat while still failing its business obligation. A scheduler saying "started" is weak evidence. The useful unit is the expected run, with an auditable transition from due to started to completed and a separate terminal failure state.

Start with a planning model, not a vendor price sheet. Suppose a workflow runs once per minute in two regions and an AI agent emits 12 diagnostic events per run. At an assumed 2 KiB per event, the raw diagnostic stream is `2 x 1,440 x 12 x 2 KiB = 67.5 MiB/day`, or about 2.0 GiB for 30 days before any indexing, replication, or compression. Those numbers are a capacity example, not a benchmark; the point is that event count and retention can dominate even when the heartbeat itself is tiny. The change that moves this term is to split evidence by purpose. Keep one compact ledger row for every expected run: schedule slot, region, attempt, start time, completion time, terminal state, input class, token usage, and attributed cost. Keep detailed prompts, model responses, tool-call payloads, and spans only under a declared sampling and retention policy. The ledger answers whether the job ran and what it cost. The sampled trace answers why a particular run was slow. Cost attribution must survive retries. A retry should carry the same logical run key and a new attempt number; otherwise, a transient retry appears as two independent obligations and finance cannot reconcile the telemetry total with the model-usage total. Aggregate cost at the logical-run level, preserve attempt-level usage underneath it, and make the aggregation idempotent. Exactly once is the accounting invariant even when execution is at least once.

Don't label high-cardinality values such as shipment IDs or free-form error text as metric dimensions. Put those values in the ledger or trace, where access and retention can be controlled, and let alert metrics use bounded dimensions such as workflow, region, state, and error class. The trade-off is deliberate: a compact metric cannot answer every forensic question, while an unrestricted dimension set creates an open-ended indexing obligation.

Retention is therefore a risk decision. NIST SP 800-66r2 describes risk analysis, audit controls, integrity, and transmission security for systems subject to the HIPAA Security Rule; it does not prescribe one universal telemetry-retention period. Even outside health care, that is the right pattern: document the obligation, data classification, access controls, and deletion schedule rather than claiming that 30 or 90 days is automatically compliant.

Keep less detail.

The catch is that aggressive sampling can discard the only payload that explains a rare route-planning failure. For high-value or regulated workflows, retain all terminal-state ledger records and promote traces to full retention when a run is late, failed, duplicated, or manually reviewed. For low-value periodic refreshes, a shorter diagnostic window may be defensible. I'm not sure which boundary fits a particular operator without its incident frequency, contractual audit window, and data-classification policy; those three inputs should settle it.

## How should a scheduled task heartbeat alert on missed runs and failures?

Model absence explicitly. For every schedule slot, the monitor should know that a run is due before any worker reports. A single success ping cannot distinguish a scheduler that never invoked the Node.js process from a process that started and then stopped before reporting. Separate signals create four useful cases.

| Ledger evidence at deadline | Interpretation | Alert action |
| --- | --- | --- |
| No `started` event | Missed invocation | Page once for the logical run |
| `started`, no terminal event | Incomplete or late run | Page after the runtime grace window |
| Explicit `failed` event | Known terminal failure | Page using the recorded error class |
| Repeated terminal event | Duplicate delivery or retry confusion | Deduplicate the page and audit attempts |

Use UTC schedule slots as logical identifiers even when operations teams view local time. EU and US daylight-saving transitions do not then create duplicate or absent keys. Each deployment calculates the same deterministic key from workflow, region, and scheduled UTC instant. A uniqueness constraint on that key plus attempt protects the audit trail, while a compare-and-set terminal transition stops a late success from overwriting a recorded failure.

The alert deadline should be `scheduled_at + expected_start_delay + maximum_runtime + grace`. Set those components from observed distributions and the business deadline, not from a round number copied between jobs. The web.dev guidance for Core Web Vitals uses the 75th percentile as a user-experience assessment threshold, which is a useful example of percentile-based evaluation, but a missed-run alarm has a different loss function: a p75 runtime is usually too early for paging because one quarter of healthy runs may exceed it. Use a higher operational percentile, then cap it below the time at which the logistics result becomes useless. Your mileage may vary — queueing and model latency can have different tails in each region.

A health-check endpoint inside the same process is insufficient for this question. If the scheduler, container, or regional control plane never starts the worker, the endpoint produces no evidence and cannot observe its own absence. Place the expected-run evaluator in a separate failure domain, and have it read the durable ledger. For correlated regional loss, route the page through a path that does not depend solely on the affected region.

Page on business state, not raw noise. An HTTP `429` from a downstream service can be a retryable attempt outcome, while process exit code `137` can indicate forced termination; neither should silently erase the logical run. Record the attempt, apply a bounded retry policy, and send one deduplicated page if the logical run still lacks a valid terminal state at its deadline. A notification key based on workflow, slot, region, and alert class prevents every evaluator pass from creating another incident.

## Cost attribution needs an idempotent run ledger

The following Go example focuses on the record boundary rather than transport. A Node.js worker can emit the same JSON contract, while an independent evaluator consumes it from the durable store selected by the system. There is no product-specific route to copy and no assumption that a process-local timer is authoritative.

```go
package runledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type State string

const (
	Started   State = "started"
	Completed State = "completed"
	Failed    State = "failed"
)

type Event struct {
	RunKey       string    `json:"run_key"`
	WorkflowID   string    `json:"workflow_id"`
	Region       string    `json:"region"`
	ScheduledAt time.Time `json:"scheduled_at"`
	ObservedAt   time.Time `json:"observed_at"`
	Attempt      int       `json:"attempt"`
	State        State     `json:"state"`
	InputTokens  int64     `json:"input_tokens,omitempty"`
	OutputTokens int64     `json:"output_tokens,omitempty"`
	CostMicros   int64     `json:"cost_micros,omitempty"`
}

func LogicalRunKey(workflow, region string, scheduledAt time.Time) string {
	slot := scheduledAt.UTC().Format(time.RFC3339)
	sum := sha256.Sum256([]byte(workflow + "\x00" + region + "\x00" + slot))
	return hex.EncodeToString(sum[:16])
}

func EncodeEvent(workflow, region string, scheduledAt, observedAt time.Time, attempt int, state State) ([]byte, error) {
	if attempt < 1 {
		return nil, fmt.Errorf("attempt must be positive")
	}
	event := Event{
		RunKey:       LogicalRunKey(workflow, region, scheduledAt),
		WorkflowID:   workflow,
		Region:       region,
		ScheduledAt: scheduledAt.UTC(),
		ObservedAt:   observedAt.UTC(),
		Attempt:      attempt,
		State:        state,
	}
	return json.Marshal(event)
}
```

The storage layer still has real work to do. It should reject an event whose immutable identity fields disagree with an existing run, accept replay of the same event without changing totals, and allow only declared state transitions. Cost belongs on attempt records in integer minor units such as micros, then rolls up once to the logical run; floating-point currency and additive reprocessing both undermine reconciliation.

An evaluator scans due ledger rows, not just received heartbeats. Its output should include the expected slot, last accepted state, deadline, and policy version that produced the alert. That policy version is easy to omit and painful during an audit: six months later, the team needs to explain why a page fired under the old 18-minute grace period rather than today's 25-minute value.

Tests should freeze time and cover the boundaries: no start, late start, completion exactly at deadline, completion one unit after deadline, duplicate completion, retry after explicit failure, daylight-saving transitions, and evaluator replay. Deployment tests should also stop the scheduler itself; killing only the job process verifies incomplete-run detection but never proves that missed-run detection works.

## Audit the obligation, then choose what to discard

For each scheduled logistics workflow, define one durable obligation per UTC slot and region, give retries attempt identities beneath it, and require one accepted terminal answer. Then derive latency from scheduled time through completion, derive model cost from reconciled attempt usage, and derive alerts from obligations whose terminal answer is absent or failed at the deadline. This arrangement keeps the heartbeat monitor, the AI agent trace, and the cost ledger from telling three incompatible stories.

It is not suitable when the task has no meaningful schedule, such as a purely event-driven consumer; monitor queue age, event lag, and dead-letter state instead. A simple process-local timer may also be enough for a noncritical single-host script where an external durable ledger costs more operational effort than the missed run warrants. Conversely, stick with an externally evaluated ledger when missed execution has financial, contractual, or safety consequences, or when EU and US deployments must reconcile independently.

The final retention choice is explicit: preserve every compact obligation and terminal transition for the approved audit window, preserve enough aggregated usage to reconcile cost, and expire ordinary verbose traces sooner. When an unusual run fails after its trace has expired, diagnosis may stop at the ledger's error class rather than the exact model response. That loss is the price of bounded telemetry storage; write it into the policy instead of discovering it during an incident.

## References

- web.dev, “Web Vitals”: https://web.dev/articles/vitals
- NIST SP 800-66r2, “Implementing the Health Insurance Portability and Accountability Act (HIPAA) Security Rule”: https://csrc.nist.gov/pubs/sp/800/66/r2/final

## Further reading

The two primary sources above cover percentile-based measurement and risk-based audit controls. Apply their methods to the local service-level objective and compliance scope; neither source supplies a universal missed-run threshold or retention duration.
