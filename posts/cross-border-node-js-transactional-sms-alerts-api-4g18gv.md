# Cross-Border Node.js Transactional SMS Alerts API Status Polling Explained

Short answer: the simplest reliable design is a small transactional outbox, a provider-neutral SMS adapter, and bounded status polling. Store the reset challenge only as a keyed digest, give it a short expiry, and retain the minimum delivery evidence needed to explain a failed reset without keeping message content or a reusable secret. For a media account system operating in the US and EU, the important comparison is not the lowest advertised message price; it is the amount of uncertain delivery state the service leaves your application to reconcile.

The bill is made of message attempts, status lookups, database writes, log ingestion, and retained evidence. Message attempts normally dominate the external work, while an aggressive poll loop can quietly dominate request count: 1 million reset messages with five status checks produce 5 million lookup requests. That is a planning example, not a market price or benchmark. Moving from fixed one-minute polling to a schedule such as 15 seconds, 60 seconds, 5 minutes, and 30 minutes caps the example at four checks per accepted message and concentrates requests near the period in which a status change is operationally useful.

Retention has a second cost, although raw rows are rarely the interesting part. Indexes, replicas, backups, access reviews, and deletion verification all follow the row. I would therefore retain a compact transition ledger for a defined support and security-review window, then aggregate counts and delete phone numbers, provider references, and event-level metadata. The deliberate loss is forensic resolution: after deletion, an operator can establish that a cohort had an elevated failure rate, but cannot reconstruct one subscriber's delivery path. That trade must be recorded, approved, and tested rather than hidden inside a default log-retention setting.

## How should an SMS alerts API poll status for transactional notifications?

A password-reset system has three clocks. The challenge expiry controls how long possession of the code can authorize a reset. The polling horizon controls how long the application seeks a more final transport outcome. The evidence-retention window controls how long operators can investigate an event. Giving all three clocks the same value is convenient and usually wrong.

OWASP recommends that reset tokens be cryptographically random, linked to a user, invalidated after use, stored securely, and expired after an appropriate period. It also recommends consistent responses and timing so the reset endpoint does not disclose whether an account exists. Those constraints mean the application should create the reset request and its dispatch intent atomically, but should never place the clear OTP in the audit ledger.

Consider a capacity model with 1 million requests per month, a 10-minute challenge expiry, four scheduled status checks, and a 30-day event-level retention policy. These values are explicit design inputs, not universal limits. Compare candidate services with this worksheet:

| Term | Measure | Why it changes the decision |
|---|---:|---|
| Initial attempts | 1,000,000 | Every accepted reset creates transport work |
| Maximum lookups | 4,000,000 | Polling can multiply API traffic even when no status changes |
| Retry attempts | policy-dependent | Ambiguous timeouts must not trigger an unbounded second message |
| Retained transitions | observed, not assumed | A transition ledger grows with state changes, not just requests |
| Sensitive fields after 30 days | 0 in this example | Deletion reduces later investigative detail |

The General Data Protection Regulation expresses storage limitation: personal data should be kept no longer than necessary for the purposes for which it is processed, subject to applicable exceptions. It does not supply one universal number of days for SMS delivery records. The controller has to justify the window, enforce it in primary storage and downstream copies, and account for any legal obligation that changes the result.

## Delivery is a reconciled state machine

An HTTP success from a messaging API is evidence that a request was accepted, not proof that a handset displayed a message. Conversely, a client timeout is not proof that the provider rejected it. This ambiguity is where duplicate reset messages begin: the worker sees no response, retries with a fresh identity, and produces two accepted sends for one password-reset request; the subscriber then receives two valid-looking codes and cannot tell which one reflects the latest account state. The audit trail must be able to show one logical dispatch, each network attempt, and the evidence returned for each attempt without pretending that a missing response means nothing happened.

Unknown is a real state.

Use one application-generated idempotency key for the logical dispatch. Persist it before making the network call, reuse it after timeouts, and place a unique constraint on it locally. If a candidate service supports idempotent submission, test the precise scope and lifetime of that behavior; if it does not, the adapter should stop automatic resubmission after an ambiguous result and send the item to reconciliation. An exactly-once mindset is valuable here, provided it is understood as a ledger invariant rather than a promise that a distributed network performs one physical action exactly once. The trade-off is deliberate: reconciliation may delay a second message, but an automatic resend after an ambiguous timeout can create a confusing pair of reset codes.

A useful internal state machine is small: `pending`, `accepted`, `delivered`, `failed`, and `unknown`. Provider-specific states belong in an attached evidence field, not in business logic. Transitions are append-only, and the current state is a projection. Late evidence may move `accepted` to `delivered` or `failed`; it must not revive an expired challenge or authorize a password change.

Delivery reliability also depends on route eligibility, sender registration, destination rules, and the recipient's network. Those conditions vary by jurisdiction and can change independently of code. Require a candidate service to document country coverage, sender identity requirements, status definitions, status retention, and what an unavailable status means. Treat undocumented semantics as `unknown`.

## Polling without turning uncertainty into duplicates

Polling is reasonable when webhooks are unavailable or when the team does not want an inbound public callback surface. It exchanges inbound authentication and replay handling for scheduled outbound work and slower observation. The worker below shows the boundary: the database owns due work and history, while the transport owns submission and status retrieval.

```go
package reset

import (
	"context"
	"errors"
	"time"
)

type DeliveryState string

const (
	Pending   DeliveryState = "pending"
	Accepted  DeliveryState = "accepted"
	Delivered DeliveryState = "delivered"
	Failed    DeliveryState = "failed"
	Unknown   DeliveryState = "unknown"
)

type Transport interface {
	Send(ctx context.Context, idempotencyKey, destination, body string) (string, error)
	Status(ctx context.Context, providerRef string) (DeliveryState, error)
}

type Store interface {
	Due(ctx context.Context, now time.Time, limit int) ([]Dispatch, error)
	RecordAttempt(ctx context.Context, id string, at time.Time) error
	RecordEvidence(ctx context.Context, id string, state DeliveryState, at time.Time) error
}

type Dispatch struct {
	ID          string
	ProviderRef string
	ExpiresAt   time.Time
	Polls       int
}

func Reconcile(ctx context.Context, store Store, tx Transport, now time.Time) error {
	items, err := store.Due(ctx, now, 100)
	if err != nil {
		return err
	}
	for _, item := range items {
		if !now.Before(item.ExpiresAt) || item.Polls >= 4 {
			if err := store.RecordEvidence(ctx, item.ID, Unknown, now); err != nil {
				return err
			}
			continue
		}
		if err := store.RecordAttempt(ctx, item.ID, now); err != nil {
			return err
		}
		state, err := tx.Status(ctx, item.ProviderRef)
		if err != nil {
			continue // A lookup failure is not evidence of message failure.
		}
		if err := store.RecordEvidence(ctx, item.ID, state, now); err != nil {
			return err
		}
	}
	return errors.Join(ctx.Err())
}
```

The batch size of 100 and limit of four polls are policy parameters in this example. Production code also needs leases or row locking so two workers cannot claim the same due item, rate-limit handling, jitter, bounded retries for status reads, and a dead-letter review path. The critical rule is narrower: a status-read error records an observation failure; it does not rewrite delivery as failed and does not mint another OTP.

Keep the OTP path separate. Generate a random value, store a keyed digest with its account binding and expiry, compare in constant time, and consume it transactionally with the password change. Logs should carry the dispatch ID and coarse destination region, never the OTP or full message body.

## A selection exercise that tests the awkward cases

Do not begin with a feature grid. Give each candidate adapter the same contract test and a small set of destinations that your legal and operations teams have approved. The test should cover an accepted submission, invalid destination syntax, an unreachable or controlled test number where available, throttling, credentials without status-read permission, a delayed final status, and a lookup after the documented status-retention boundary.

Then disconnect the client after submission but before its response is read. This is the revealing case. Inspect whether replaying the same idempotency key is defined, whether a provider reference can be recovered, and whether the application can settle the row without sending a second reset. A service that is pleasant on the happy path but leaves this case undefined transfers complexity into your ledger.

Compare results with weighted criteria set before the trial: destination eligibility and documented delivery semantics first; idempotency behavior, status-query limits, data location and deletion controls next; SDK convenience and unit price after those. Ask security and privacy reviewers about international data transfers, processor terms, access controls, and deletion evidence. SMS also deserves an explicit risk decision: NIST SP 800-63B treats use of the public switched telephone network for out-of-band authentication as restricted and tells verifiers to consider indicators such as SIM changes and number porting. A reset flow may require a stronger recovery route for higher-risk accounts.

Deployment should be staged by region and destination class, with dashboards for submissions, ambiguous outcomes, transition latency, terminal failure categories, expired challenges, poll backlog age, and duplicate-key conflicts. Alert on ratios and queue age rather than raw message volume. Reconcile daily: accepted dispatches plus rejected dispatches plus unresolved dispatches must equal attempted logical dispatches for the interval. No row disappears.

The decision rule is plain. Choose the service whose documented boundaries and test results let your team reconcile every logical reset attempt within the required horizon, while collecting the least personal data. Stop polling when the challenge expires or the evidence budget is exhausted; stop retaining event-level records when their approved purpose ends. You will lose some forensic detail. You should know exactly which detail before an incident asks for it.

No row disappears.

## Further reading

- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- NIST SP 800-63B, Authentication and Lifecycle Management: https://pages.nist.gov/800-63-3/sp800-63b.html
- GDPR Article 5, Principles relating to processing of personal data: https://eur-lex.europa.eu/eli/reg/2016/679/art_5/oj
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
