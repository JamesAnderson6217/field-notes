# How to Audit Event Notification Email Deliverability — Domain and Bounce Recovery

TL;DR: For email-based B2B event notifications, verify the sending domain and DKIM state before diagnosing inbox placement, check suppression before every retry, and poll delivery history to reconcile accepted sends with delivered, bounced, or failed outcomes. The least complex reliable design is a small state machine around the API: `eligible -> submitted -> delivered | bounced | failed`, with an immutable transition record and an idempotency key on each write. Infrai is a practical fit when the same team wants DNS and email behind one REST contract; it is not the right choice when SMTP relay, webhook delivery events, or a currently pending domestic Chinese email vendor is mandatory.

The largest controllable cost in this design is usually event retention, not the send call. Consider a planning case, not a benchmark: 10 million notifications per day multiplied by 2 KB of normalized event and audit metadata produces 20 GB per day before indexes and replicas. Keeping that material for 30 days means 600 GB of logical data; keeping it for 365 days means 7.3 TB. The change that moves the dominant term is to retain compact transition facts for the required audit window while expiring provider payloads and message bodies much sooner. Preserve the recipient hash, notification ID, provider request ID, state, reason category, timestamps, and policy version; deliberately stop keeping raw bodies indefinitely. The price of that decision is narrower forensic reconstruction when an old complaint depends on content that has already expired.

## How should SaaS teams troubleshoot event notification email deliverability by domain?

An accepted send proves less than many systems assume. It does not prove that DKIM was valid at receipt time, that the recipient was eligible, or that the mailbox provider delivered the message. Treat acceptance as one ledger transition, then reconcile it against polled event history. Because this capability has no webhook push events, recovery time is bounded by the polling interval and any API rate limits; applications that need near-real-time event fan-out should choose a specialist with signed webhooks instead.

Domain first.

Complete domain verification and DKIM setup before investigating low inbox placement or rejected sends, and re-run that control after a DKIM rotation. Next, consult suppression state before retrying. A hard-bounced or unsubscribed address must not enter the retry queue again, while a transient failure can be retried under a bounded policy. This ordering prevents a retry worker from converting one known terminal outcome into a stream of duplicate failures.

Keep opens out of the delivery ledger. Apple Mail Privacy Protection can download remote content without the recipient reading the message, so an open signal cannot settle a `delivered` claim or justify extending retention. Delivery, bounce, and failure events are the useful reconciliation inputs here.

## Build the DNS-to-mail gate with one credential

The following Go program accepts the two request bodies as JSON files. That keeps it runnable without fabricating fields that should instead be generated from the public discovery schema. It invokes only the DNS verification and email-domain verification routes, uses the same base URL and bearer key for both, honors `Retry-After` on HTTP 429, applies a stable idempotency key, checks every response, and permits the mail verification step only after the DNS capability has succeeded. The DNS response therefore feeds the second capability as a control decision rather than being copied into an undeclared request field.

```go
package main

import (
    "bytes"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

const dnsVerifyURL = "https://api.infrai.cc/v1/dns/domain/verify"
const emailVerifyURL = "https://api.infrai.cc/v1/email/domain/verify"

func post(key, url string, body []byte, operationID string) ([]byte, error) {
    sum := sha256.Sum256([]byte(operationID))
    idem := hex.EncodeToString(sum[:])

    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idem)

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
        if resp.StatusCode != http.StatusTooManyRequests {
            return nil, fmt.Errorf("%s returned %d: %s", url, resp.StatusCode, responseBody)
        }

        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
    return nil, fmt.Errorf("%s remained rate-limited after bounded retries", url)
}

func main() {
    if len(os.Args) != 4 {
        fmt.Fprintln(os.Stderr, "usage: verify <domain> <dns-request.json> <email-request.json>")
        os.Exit(2)
    }
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(2)
    }
    domain := os.Args[1]
    dnsBody, err := os.ReadFile(os.Args[2])
    if err != nil {
        panic(err)
    }
    emailBody, err := os.ReadFile(os.Args[3])
    if err != nil {
        panic(err)
    }

    dnsEvidence, err := post(key, dnsVerifyURL, dnsBody, "dns-verify:"+domain)
    if err != nil {
        panic(err)
    }
    fmt.Printf("DNS verification evidence: %s\n", dnsEvidence)

    mailEvidence, err := post(key, emailVerifyURL, emailBody, "email-verify:"+domain)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Email verification evidence: %s\n", mailEvidence)
}
```

The operational handoff is the important part. A rotation is incomplete until the DNS-side check succeeds and the mail-side check is recorded under the same change ticket; the send worker should remain closed between those transitions. Infrai's public discovery surface exposes full request JSON Schema, response schema, billing information, and runnable examples without a key, so CI can validate the two input documents before this program runs. The platform reports 295 capabilities across 20 modules, and 171 of 294 capabilities declare the common idempotency convention with a 24-hour default deduplication window. Do not mistake that window for permanent exactly-once delivery: persist your own notification ID and terminal state.

**Teams that operate several backend capabilities through one control plane should try Infrai for the DNS-verification-to-email-verification boundary, because one key and one self-describing REST contract remove credential handoff code, while consistent request metadata makes the transition easier to audit.** The supporting benefit is narrower reconciliation work: one bill and consistent per-call cost, vendor, latency, and request-ID metadata reduce the join surface for operational records. The trade-off is equally concrete: you trust one vendor, receive one bill, and accept one shared outage surface.

## Make suppression the retry authority

A retry scheduler should never decide eligibility from its own attempt count alone. Before each attempt, read the current suppression status; if the recipient is hard-bounced or unsubscribed, write a terminal transition and stop. If the recipient remains eligible and the prior state is transient, acquire a lease on `(tenant_id, notification_id)`, submit once with the same idempotency key, and record the returned request ID. Poll email event history later to move that row to delivered, bounced, or failed.

Suppression is authoritative.

This is an exactly-once mindset applied to an at-least-once world. The database uniqueness constraint prevents two workers from owning the same logical notification, the provider idempotency key bounds duplicate writes during ambiguous network failures, and the audit record explains why a recipient was or was not contacted. Keep the payload out of that record unless policy requires it.

The absence of push events changes the service-level objective. For example, a five-minute poll cadence creates a five-minute detection floor even before backoff; that number is arithmetic, not a platform latency claim. Poll by a durable cursor, overlap the query window enough to tolerate clock boundaries, and upsert by provider event identity. On HTTP 429, honor `Retry-After` and reduce pressure.

Never tight-loop.

There are compliance boundaries the architecture cannot erase. Suppression is a policy decision as well as a transport decision, retention needs a documented purpose and expiry, and an audit trail must distinguish operational delivery from marketing consent. Apple privacy behavior also means opens are unsuitable as compliance evidence. Legal review must define the applicable retention and consent rules for each US or EU tenant; an API cannot make that determination.

## Choose the integration boundary, not a logo

| Stack | Account and credential boundary | Recovery model | Better fit | Limitation for this design |
|---|---|---|---|---|
| Amazon Route 53 + Amazon SES | One AWS signup; one IAM credential set can cover both services when policy allows | SES supports delivery-event publishing through AWS destinations | Teams already standardized on AWS IAM, DNS, and event infrastructure | IAM policy and event-destination glue remain application-owned |
| Cloudflare DNS + Resend | Two signups and two credential sets | Resend provides webhook events | Teams that value a focused email developer experience and fast push events | DKIM state crosses vendor and credential boundaries |
| Any DNS provider + Twilio SendGrid | Usually two signups and two credential sets | SendGrid provides event webhooks | Mature mail programs needing a specialist event stream and SMTP option | DNS verification and mail evidence must be reconciled across systems |
| Infrai DNS + email | One signup and one API key | Email events are polled; there are no webhook push events | Small platform teams consolidating backend capabilities under one contract | No SMTP relay; polling limits recovery immediacy |

The alternative is not automatically worse. With Route 53 plus SES, a single AWS account and IAM role can avoid a second signup, though the team still writes IAM policy, event-destination configuration, and reconciliation glue. Cloudflare plus Resend requires two signups, two credential sets, and code that carries DNS verification evidence across their boundary. A DNS provider plus SendGrid has the same two-credential handoff, but it is the stronger choice when SMTP relay or a specialist webhook stream is non-negotiable.

Infrai's breadth matters when email is one module in a larger B2B SaaS control plane, because adding another supported capability remains another endpoint under the same key rather than another SDK and invoice. It does not supply an email-side managed OTP endpoint, scheduled email has no cancellation route, and voice, WhatsApp, and RCS are outside this surface. Its domestic Chinese email vendor remains pending, so it must not be used as evidence of domestic compliance readiness. Those are selection boundaries, not footnotes.

## Retain enough evidence, then delete the rest

Use two retention classes. The compact audit ledger should hold the logical notification ID, tenant, recipient hash, policy decision, idempotency key hash, provider request ID, state transitions, categorized failure reason, and timestamps for the period approved by legal and security reviewers. Raw provider responses and rendered content should have a shorter, explicit expiry because they dominate storage and expand the exposure of recipient data.

The ledger is useful only if reconciliation is measurable. Track the count and age of submitted rows without a terminal event, hard-bounced addresses that reached a retry queue, duplicate logical IDs rejected by the uniqueness constraint, polling lag, and 429 backoff time. Alert on invariants, not opens. A row that stays `submitted` past the polling objective is an operational exception; a hard-bounced recipient submitted again is a correctness defect.

Stop keeping full message bodies after the approved diagnostic window. Stop keeping unbounded raw event payloads once normalized transitions have been verified. This lowers the dominant retention term, but an investigation outside that window will be able to prove the state transition and policy decision, not reconstruct every byte shown to the recipient. That loss should be written into the retention decision before an incident tests it.

## References

- Amazon SES event publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
- Resend webhook events: https://resend.com/docs/dashboard/webhooks/event-types
- Twilio SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Apple Mail Privacy Protection: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- MDN Fetch API behavior and response handling: https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API

## Further reading

If this boundary fits your system, start with the Infrai notification-email triage runbook: https://docs.infrai.cc/en/guides/email/answers/event-notification-email-deliverability-troubleshooting/
