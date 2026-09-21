# Transactional Email API Reliability: Password Reset Flow Lessons for Property Reports

**Short answer:** choose a transactional email API only after it proves one logical send survives retries, its custom domain passes DKIM, SPF, and DMARC checks, and its delivery events support reconciliation across the required US/EU data path.

For a property-management system that emails generated owner reports as attachments, simple setup is useful, but it is not the decision rule. The decisive question is whether the application can reconcile every accepted, bounced, delayed, and ambiguous attempt without sending the same report twice.

**Decision:** place an application-owned outbox and delivery ledger between report generation and the email API. Treat the API's acceptance response as evidence of custody, not proof that the recipient received the message. This architecture keeps authentication, regional processing, attachment limits, webhook behavior, and suppression handling visible during evaluation instead of hiding them behind a successful SDK call.

## What should a transactional email API preserve during a password reset flow?

The password reset flow supplies the familiar baseline: the email is time-sensitive, replay has security consequences, and an API timeout does not reveal whether the message was accepted. A generated property report changes the payload and authorization rules, but not that reliability boundary. Here the unit of work is the business instruction "send report R to recipient C for reporting period P," identified by an application-generated idempotency key. That distinction matters when a worker times out after remote acceptance: blindly repeating the request can create two emails, while blindly marking the first attempt successful can lose a report that was never accepted. The system must retain an auditable ambiguous state until an event, a supported status inquiry, or an operator decision resolves it.

Four invariants define the boundary. One report instruction has one stable idempotency key even if it produces several transport attempts. The approved report artifact is immutable; its SHA-256 digest, byte length, template revision, recipient, and authorization decision belong in the ledger. A remote message identifier is recorded with the attempt state as soon as an accepted response is parsed. Webhook events are append-only observations, deduplicated by an event identifier or by a documented composite key when no identifier exists.

Exactly once is an application objective here, not a property that SMTP promises. SMTP specifies reply codes and transfer behavior, while delivery-status notifications have their own format and semantics; neither turns an API timeout into certainty about the recipient's mailbox. The practical design is at-least-once work processing plus idempotent state transitions, bounded retries, and reconciliation.

Be precise.

This is an explicit trade-off: more durable state in exchange for fewer irreconcilable sends.

The custom domain is another invariant, not a launch-day checkbox. SPF publishes which systems may send on behalf of a domain, DKIM attaches a cryptographic signature to a message, and DMARC evaluates identifier alignment and communicates domain policy. A trial should verify the visible From domain and authenticated identifiers on an actual received message. It should also exercise DNS rotation and failure, because green setup indicators do not establish that future messages will remain aligned.

## Decision record and failure boundaries

The architecture has three durable records: an outbox row for intent, an immutable artifact record for the generated PDF, and an event ledger for transport observations. A worker leases an outbox item, loads the artifact by digest, submits it through a narrow adapter, and records the result. A separate webhook consumer authenticates callbacks under the candidate API's documented scheme, stores the untouched event in restricted storage, and advances only allowed state transitions.

| Option | Duplicate control | Audit and reconciliation | Appropriate use |
| --- | --- | --- | --- |
| Application outbox plus email API | Stable business key and attempt ledger | Intent, artifact, acceptance, and events can be joined | Consequential generated reports |
| Direct API call in the web request | Usually tied to request retries | Weak unless the caller builds another ledger | Low-consequence notifications |
| SMTP relay behind a queue | Queue can deduplicate locally | Downstream detail depends on the relay | Estates with mature SMTP operations |
| Download link instead of attachment | Mail and file access are separate | Strong when downloads are logged | Sensitive or large reports with a portal |

Comparison begins after these boundaries are explicit. For every candidate, record the maximum encoded message size, accepted attachment representations, request-id and idempotency semantics, event retention, webhook redelivery policy, bounce and complaint fields, suppression controls, regional processing commitments, subprocessors, deletion behavior, and the contract covering personal data. Verify these points against current documentation and terms during the trial. A generic claim of EU support does not establish where message content, logs, backups, and support access are handled.

Compliance does not supply a delivery architecture. GDPR Article 5 places purpose limitation, data minimization, storage limitation, integrity, and confidentiality around the attachment and retained copies; Article 32 requires security measures appropriate to risk. Those constraints support storing a digest and controlled artifact reference in operational records instead of copying the PDF into logs, webhook traces, or exception messages. The controller's legal and security review still determines retention and transfer rules.

## The critical path in Go

This boundary accepts an immutable artifact, carries a stable idempotency key, and distinguishes accepted, rejected, and unknown outcomes. An adapter must map documented responses into those states without inventing certainty.

```go
package delivery

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
)

type Outcome string

const (
    Accepted Outcome = "accepted"
    Rejected Outcome = "rejected"
    Unknown  Outcome = "unknown"
)

type Artifact struct {
    Bytes  []byte
    SHA256 string
}

type Request struct {
    IdempotencyKey string
    From, To       string
    Artifact       Artifact
}

type Receipt struct {
    MessageID string
    Outcome   Outcome
}

type Sender interface {
    Send(context.Context, Request) (Receipt, error)
}

type Ledger interface {
    BeginAttempt(context.Context, string, string) error
    FinishAttempt(context.Context, string, Receipt, string) error
}

func Deliver(ctx context.Context, ledger Ledger, sender Sender, req Request) error {
    sum := sha256.Sum256(req.Artifact.Bytes)
    digest := hex.EncodeToString(sum[:])
    if digest != req.Artifact.SHA256 {
        return errors.New("artifact digest mismatch")
    }
    if err := ledger.BeginAttempt(ctx, req.IdempotencyKey, digest); err != nil {
        return fmt.Errorf("begin attempt: %w", err)
    }

    receipt, err := sender.Send(ctx, req)
    if err != nil {
        // The connection can fail after remote acceptance, so preserve uncertainty.
        receipt.Outcome = Unknown
        if recordErr := ledger.FinishAttempt(ctx, req.IdempotencyKey, receipt, err.Error()); recordErr != nil {
            return fmt.Errorf("send error: %v; record error: %w", err, recordErr)
        }
        return fmt.Errorf("delivery outcome unknown: %w", err)
    }
    if receipt.Outcome == Accepted && receipt.MessageID == "" {
        return errors.New("accepted response lacks message identifier")
    }
    return ledger.FinishAttempt(ctx, req.IdempotencyKey, receipt, "")
}
```

Database uniqueness must protect business keys and received event identifiers. Do not hold a database transaction across the network call: record the attempt, perform the call, then record the observation. A lease with an expiry permits recovery after process death, while the stable key lets reconciliation determine whether another submission is warranted.

Keep secrets and personal data out of `detail`. In production, store a classified error code plus a restricted diagnostic reference because response bodies and callbacks can contain recipient addresses. Auditability is not permission to retain everything.

## How should candidates be tested?

Use one controlled custom domain and a matrix crossing US and EU deployment locations with ordinary success, invalid recipient, delayed response, connection reset after request upload, duplicate webhook, out-of-order webhook, expired webhook signature, worker crash after acceptance, and an attachment near the documented limit. Record expected ledger transitions first. A candidate fails the reliability gate if an ambiguous attempt cannot be found and resolved without searching transient logs.

Authentication needs evidence: deployed DNS records, selector ownership, the rotation procedure, received-message headers, the DMARC aggregate-report destination, and the verification date. Aggregate reports expose authentication and alignment results at domain scale; they are not per-message delivery receipts. Section 4.6.4 of RFC 7208 limits SPF terms that cause DNS queries to 10 during an SPF check, so a long chain of third-party includes needs review.

Ten is a hard protocol boundary, not a tuning suggestion.

Do not score opens as delivery. Apple Mail Privacy Protection can download remote content in the background and prevents senders from learning whether a recipient opened a message, which makes an open pixel unsuitable for reconciling a property report. Useful states are intent recorded, API outcome accepted/rejected/unknown, and transport events such as delivered, bounced, or complained. Even `delivered` describes a transport observation, not human reading.

Run the same suite against every shortlisted API and archive redacted evidence with the decision record. Measure reconciliation lag and unresolved attempts under injected faults, but do not import benchmark numbers from another system: attachment size, recipient domains, network placement, and retry policy change the result. A trial that never injects a timeout proves little.

Alert on old unknown outcomes, rising permanent failures, webhook verification failures, queue age, and disagreement between accepted attempts and terminal events. Temporary failures should reduce concurrency and reschedule work with bounded exponential backoff plus jitter. Permanent address failures should not enter that loop. Every manual replay needs an actor, reason, timestamp, and link to the original instruction.

## Rejected option and its valid use case

Sending the attachment directly from the report-generation HTTP request is rejected. It couples a slow external operation to the user's request, makes browser or gateway retries capable of repeating the send, and leaves a gap between PDF creation and transport evidence. Moving the call to an untracked background task does not close that gap.

The design remains valid for disposable, low-consequence messages when duplicates carry negligible harm and no audit trail is promised. It is a poor fit for owner statements, inspection summaries, or arrears reports, where the artifact version and recipient authorization may later need reconstruction.

The final selection is conditional, not a ranking: choose the API whose documented limits fit the report, whose domain authentication can be independently verified, whose event model survives the fault matrix, and whose contractual data path fits the US/EU deployment. Keep the adapter replaceable, but keep the delivery ledger authoritative.

## References

- RFC 5321, Simple Mail Transfer Protocol: https://datatracker.ietf.org/doc/html/rfc5321
- RFC 3464, Delivery Status Notifications: https://datatracker.ietf.org/doc/html/rfc3464
- RFC 6376, DomainKeys Identified Mail Signatures: https://datatracker.ietf.org/doc/html/rfc6376
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Regulation (EU) 2016/679, Articles 5 and 32: https://eur-lex.europa.eu/eli/reg/2016/679/oj
- Apple, Use Mail Privacy Protection: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
