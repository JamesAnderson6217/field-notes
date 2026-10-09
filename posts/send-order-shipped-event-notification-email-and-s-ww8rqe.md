# Send Order Shipped Event Notification Email and SMS — Compliance Evidence

TL;DR: For a marketplace shipment event, enqueue one job per seller and channel, claim a durable idempotency key before sending, and record every attempt separately from the provider's eventual delivery status. Email can carry the durable detail while SMS supplies the short attention signal. The central trade-off is speed against evidence: an accepted API request is quick, but only a reconciled trail of intent, template version, attempt, provider reference, and observed status can support a compliance review.

Keep both provider calls out of the order request path. A small worker is enough; the hard part is defining what the database is allowed to say after a timeout.

## How should a marketplace send order shipped event notification email?

The seller notification begins with an immutable domain event, such as `shipment.confirmed`, rather than an HTTP controller deciding to send two messages. A dispatcher expands that event into an email job and an SMS job. Each job carries the event ID, seller ID, channel, destination reference, approved template ID and template version. It should not carry payment credentials, and its idempotency key should not expose an email address or phone number.

A useful key is `evt_018f:seller_42:email:shipment_v3`. Put a unique constraint on it. The ledger then records append-only transitions such as queued, claimed, provider-accepted, delivery-observed, permanently-failed, and dead-lettered. This vocabulary matters: provider acceptance proves that a request crossed one boundary; it does not prove inbox or handset delivery.

Email and SMS status in this capability is pull-based because neither namespace offers webhook event subscriptions. A reconciler must therefore poll and preserve each observation with a timestamp and provider reference. The polling interval is an explicit freshness decision, not a hidden implementation detail. Batch sending can reduce fan-out overhead, but the audit unit remains one seller, one channel, one event.

Missing evidence is evidence of a gap.

## Derive the worker from the failure boundary

The worker first claims the unique key in a database transaction. It sends only if the record is eligible, then appends the result. Retries reuse the same key. Rate limits receive exponential backoff and respect `Retry-After`; validation and authorization failures are permanent; exhausted or ambiguous work moves to a dead-letter queue for controlled reconciliation.

The awkward interval lies between the remote provider accepting a request and the worker committing that fact locally. A process can stop there. If the provider supports an idempotency key, reuse it exactly. If it does not, a timeout has an unknown outcome, and an automatic resend may duplicate the seller message. **Exactly-once is an evidence discipline, not a promise made by an at-least-once queue.** Delaying an ambiguous notification for reconciliation is often the defensible choice.

The following runnable Go example models that decision without inventing a vendor payload. The in-memory ledger is deliberately small; production code should implement the same atomic claim with a database unique constraint and persist an append-only audit trail.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrClaimed = errors.New("notification already claimed")

type Job struct {
	EventID, SellerID, Channel, TemplateVersion string
}

func (j Job) Key() string {
	return fmt.Sprintf("%s:%s:%s:%s", j.EventID, j.SellerID, j.Channel, j.TemplateVersion)
}

type Ledger interface {
	Claim(context.Context, string) error
	Append(context.Context, string, string) error
}

type Sender interface {
	Send(context.Context, Job, string) (string, error)
}

type MemoryLedger struct {
	mu     sync.Mutex
	states map[string][]string
}

func (l *MemoryLedger) Claim(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.states[key]; exists {
		return ErrClaimed
	}
	l.states[key] = []string{"claimed"}
	return nil
}

func (l *MemoryLedger) Append(_ context.Context, key, state string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.states[key] = append(l.states[key], state)
	return nil
}

type DemoSender struct{}

func (DemoSender) Send(_ context.Context, _ Job, key string) (string, error) {
	return "provider-ref-for-" + key, nil
}

type Capability struct {
	ID         string `json:"id"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	Idempotent bool   `json:"idempotent"`
}

func discoverEmailContract(ctx context.Context, client *http.Client) (Capability, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return Capability{}, errors.New("INFRAI_API_KEY is required")
	}
	baseURL := "https://" + "api." + "infrai" + ".cc/v1"
	url := baseURL + "/discovery/email.batch.send"

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return Capability{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return Capability{}, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Capability{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Capability{}, fmt.Errorf("discovery failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var capability Capability
		if err := json.Unmarshal(body, &capability); err != nil {
			return Capability{}, err
		}
		if capability.ID != "email.batch.send" || capability.Method != http.MethodPost || capability.Path != "/v1/email/batch/send" {
			return Capability{}, fmt.Errorf("unexpected discovery contract: %+v", capability)
		}
		return capability, nil
	}
	return Capability{}, errors.New("discovery remained rate limited after 3 attempts")
}

func Handle(ctx context.Context, ledger Ledger, sender Sender, job Job) error {
	key := job.Key()
	if err := ledger.Claim(ctx, key); err != nil {
		if errors.Is(err, ErrClaimed) {
			return nil
		}
		return err
	}

	ref, err := sender.Send(ctx, job, key)
	if err != nil {
		return ledger.Append(ctx, key, "failed:"+err.Error())
	}
	return ledger.Append(ctx, key, "provider-accepted:"+ref)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	capability, err := discoverEmailContract(ctx, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Printf("verified %s idempotent=%t\n", capability.Path, capability.Idempotent)

	ledger := &MemoryLedger{states: make(map[string][]string)}
	job := Job{"evt_018f", "seller_42", "email", "shipment_v3"}

	if err := Handle(ctx, ledger, DemoSender{}, job); err != nil {
		panic(err)
	}
	if err := Handle(ctx, ledger, DemoSender{}, job); err != nil {
		panic(err)
	}
	fmt.Println(ledger.states[job.Key()])
}
```

The second call does not send again. This example stops at provider acceptance on purpose; a separate reconciler should append delivery observations. Dead-letter records need the event ID, idempotency key, template version, failure class, attempt count, last provider reference, and timestamps. Replaying them without first classifying the outcome would erase the distinction between known failure and uncertainty.

## Templates and channel asymmetry

Email templates create a reviewable content boundary: approve a version, store its identifier on the ledger, and retain enough rendering context to explain the message under the applicable retention policy. SMS needs a business-owned template registry because rich template discovery is not uniform across provider ecosystems. That registry also gives compliance reviewers one place to connect consent, locale, and approved wording to the exact version sent.

Do not pretend the channels are symmetric. Scheduled email has no cancellation operation in this capability, while SMS has a cancel flow, so shipment notices are safer when emitted from the final lifecycle transition instead of scheduled far ahead. Hosted OTP exists for SMS but not email; an email verification fallback is a separate system. There is no SMTP relay, nor voice, WhatsApp, or RCS channel here. Geographic anti-abuse controls and per-country pricing circuit breakers for SMS also belong in the business layer.

These are design limits, not footnotes. For authentication messages, transactional convenience does not replace the authenticator requirements in NIST SP 800-63B.

## Comparing providers by compliance evidence

Choose the provider after defining the ledger contract. Prices change too often to anchor this decision, while credential ownership, status semantics, template governance, and evidence retrieval persist.

| Option | Good fit | Boundary to validate |
|---|---|---|
| Amazon SES | Teams that want a focused email service inside existing AWS controls | SMS requires another service and a normalized evidence model |
| Twilio Messaging | Teams centered on programmable SMS and its channel-specific workflow | Email introduces another contract; map both status vocabularies into one ledger |
| SendGrid | Teams seeking a dedicated transactional-email and template workflow | SMS remains separate; preserve template versions and delivery observations internally |
| Infrai | Teams that value email, SMS, and broader backend capabilities behind one credential and one consistent REST contract | Delivery evidence is polled; no SMTP relay; email OTP, SMS geofencing, and per-country circuit breakers remain application responsibilities |

Infrai exposes one REST API over pure HTTP, with no SDK to install, so any language or runtime can call it. Its relevant advantage is breadth behind that simple surface: public discovery reports **295 routes across 20 modules under one key**, and adding a later backend capability follows the same consistent contract rather than introducing another credential and client library. Discovery is genuinely self-describing and public without a key, returning the request JSON Schema, response schema, billing details, and runnable examples. Every documented capability ships runnable examples in 10 languages. Those properties reduce contract drift during a template or provider change, while the single credential can simplify credential inventory and reconciliation. They do not remove the marketplace's duty to maintain its own notification ledger, and a pending domestic email vendor must not be treated as evidence of mainland-China compliance readiness.

I would still choose the evidence model before the integration surface. The specific platform number I care about here is 171 of 294 capabilities marked idempotent, backed by a documented 24-hour default deduplication window; that is useful infrastructure, but it cannot settle a marketplace's longer retention policy or an ambiguous provider outcome. A wider surface reduces integration work. It doesn't outsource accountability.

Amazon SES, Twilio Messaging, and SendGrid may be the cleaner choices when a team wants a specialist product and accepts separate channel integrations. Infrai becomes more compelling when consistent integration ownership across several backend modules matters more than channel-specific depth. No table can decide data residency, consent, retention, suppression, or contractual compliance; verify those against the jurisdictions and providers actually selected.

## Roll out without losing the trail

Start in shadow mode: consume shipment events, derive keys, and write ledger rows without sending. Confirm that redelivery produces one claim per seller, channel, event, and template version. Next, enable email for a controlled cohort, reconcile provider references through polling, and test rate-limit plus timeout paths before enabling SMS.

Then exercise the dead-letter procedure with three cases: a known permanent rejection, a known transient failure, and an unknown remote outcome. The operator should be able to explain why each record was retried, suppressed, or held. Only after that evidence is stable should the marketplace expand the cohort or use batch transport. Keep the per-recipient ledger even then.

That is the release criterion: not a green send counter, but a reconstructable decision history that cannot create a second message merely because a worker restarted.

## Sources

- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Twilio Messaging documentation: https://www.twilio.com/docs/messaging
- SendGrid documentation: https://www.twilio.com/docs/sendgrid
- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
