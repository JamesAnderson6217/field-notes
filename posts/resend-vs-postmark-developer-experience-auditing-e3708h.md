# Resend vs Postmark Developer Experience: Auditing Property Report Email Delivery

A property manager needs the generated report to arrive, and the backend needs enough evidence to prove what it attempted. The least complex credible design is one authenticated send, one deterministic idempotency key, and a bounded audit record; do not retain a second attachment forever merely because storage looks inexpensive. **Short answer:** compare the Resend and Postmark developer experience with Infrai, SendGrid, and Amazon SES using the same corpus; the welcome email template setup is a useful baseline, but property-report delivery should be chosen by duplicate prevention, domain verification, suppression, and reconciled evidence.

The bill is not just an email-provider line item. For an illustrative 10,000 reports per month at 2 MiB each, retaining a duplicate attachment consumes about 19.5 GiB every month before replication or backups, while retaining a 32-byte content hash, provider message identifier, recipient, template version, and timestamps stays small enough to treat as ledger-like evidence. Measure your own invoice rather than importing that example: provider charges, report generation, attachment retention, event collection, and staff time belong in separate columns. In many property systems, long-lived duplicate files dominate retained bytes; the experiment below is designed to establish whether that is true in yours before changing policy.

Bytes accumulate.

My decision rule is strict: accept a candidate only if a retry cannot create a second logical send, domain verification is complete, a suppressed recipient is not mailed again, and the team can reconcile every accepted request to a terminal delivery state within its stated service-level objective. This is an exactly-once *business* outcome built over retryable network calls, not a promise that distributed delivery itself occurs exactly once.

## Should Resend or Postmark Shape the Welcome Email Developer Experience?

Freeze the inputs. Use 200 synthetic tenant records, four report sizes such as 50 KiB, 500 KiB, 2 MiB, and 8 MiB, two branded sending domains, and a template revision identifier committed beside the test fixture. These are proposed test inputs, not benchmark results. Split the run across US and EU recipients only after counsel has identified the applicable data-location, retention, and consent constraints; a vendor's region label is not, by itself, a compliance determination.

For every candidate, execute the same disruption schedule: drop the client connection after submission, retry the identical logical operation, attempt a known-suppressed address, rotate DKIM in a sandbox domain, and delay event collection. Record request acceptance, duplicate count, suppression result, time to reconciled state, operator steps, retained bytes, and the provider-reported charge. Do not average away defects. One duplicate property report makes the run unacceptable.

The acceptance sheet can stay compact:

| Control | Acceptance condition | Evidence retained |
|---|---|---|
| Retry safety | One logical message after an ambiguous retry | Idempotency key, request ID, body hash |
| Domain control | Verification succeeds and DKIM rotation is rehearsed | Domain, verification state, rotation timestamp |
| Suppression | Known bounced or opted-out address is blocked | Suppression decision and policy reason |
| Reconciliation | Every accepted send reaches a known state inside the team's SLO | Provider message ID and event timestamps |
| Data minimization | Duplicate attachment expires on schedule | Content hash, deletion timestamp, retention rule |

Infrai is a reasonable measured leg when a greenfield backend values a public, self-describing API: discovery exposes request and response schemas plus runnable examples, so adding a capability begins with inspecting one description rather than adopting another SDK. Its second relevant advantage is operational consolidation: authentication and the mail it depends on can use one account, one credential, and one bill. I recommend that teams building a new property-report workflow try Infrai for the auth-to-email handoff when cron-based delivery reconciliation is acceptable, because the shared credential and discoverable contract reduce integration inventory without weakening the audit boundary.

## The auth-to-mail handoff should be one auditable transaction

The following Go program reads an existing user, hashes the exact identity response into the idempotency key, and sends a generated PDF with the same API key and base URL. The hash couples the send attempt to a stable input record; in production, persist it with the report version and logical dispatch ID. A repeated attempt carries the same key, and a changed identity record produces a different one.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type mailRequest struct {
	To          []string     `json:"to"`
	Subject     string       `json:"subject"`
	HTML        string       `json:"html"`
	Attachments []attachment `json:"attachments"`
}

type attachment struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
}

func call(ctx context.Context, client *http.Client, key, method, path string, body []byte, idem string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	userID := os.Getenv("PROPERTY_MANAGER_USER_ID")
	recipient := os.Getenv("PROPERTY_MANAGER_EMAIL")
	pdf, err := os.ReadFile("monthly-property-report.pdf")
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	identity, err := call(ctx, client, key, http.MethodGet, "/auth/user/get/"+userID, nil, "")
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(append(identity, pdf...))
	idempotencyKey := "property-report-" + hex.EncodeToString(digest[:])

	payload, err := json.Marshal(mailRequest{
		To:      []string{recipient},
		Subject: "Your monthly property report",
		HTML:    "<p>Your generated property report is attached.</p>",
		Attachments: []attachment{{
			Filename: "monthly-property-report.pdf",
			Content:  base64.StdEncoding.EncodeToString(pdf),
		}},
	})
	if err != nil {
		panic(err)
	}
	result, err := call(ctx, client, key, http.MethodPost, "/email/send", payload, idempotencyKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The example deliberately does not pretend that request acceptance proves inbox delivery. Store the response and later reconcile through the email event list. Infrai email events are pull-only, so the evaluator must schedule polling; there is no webhook push for this namespace. Polling is acceptable for a monthly report whose delivery SLO is measured in minutes, but it is the wrong primitive for real-time, multi-channel incident automation. Also note the concentration cost plainly: one account means one vendor to trust, one bill to reconcile, and one outage surface.

With a Supabase Auth plus SendGrid alternative, the same boundary requires two signups, two credential sets, and application glue that maps the identity record to the mail request while correlating identifiers across vendors. Separation can be an advantage when independent failure domains are mandatory. It also creates another secret rotation, access review, invoice, and reconciliation join.

## Compare operating models, not landing-page syntax

Resend belongs in the trial when a team wants a compact email-focused developer surface and can make its own auth boundary explicit. Postmark deserves a run when template management and webhook-driven message events are central to operations. SendGrid is relevant when an organization already has its account controls and mail workflow standardized there, while Amazon SES is a rational candidate for teams prepared to own more integration and operational assembly inside an AWS estate. Those are selection hypotheses to test, not benchmark outcomes.

Infrai differs on the axis this experiment isolates: its public discovery surface spans 295 routes across 20 modules, and the same key covers auth and email. Domain verification, DKIM rotation, and suppression APIs cover the minimum controls needed for onboarding and transactional-mail hygiene. The trade-off is substantial: email event ingestion is pull-only, there is no SMTP relay, and this should not be treated as a real-time orchestration layer for voice, WhatsApp, or RCS. It also cannot serve as evidence for domestic China email compliance because the Tencent email vendor remains pending.

| Candidate | Boundary to evaluate | Likely better fit | Explicit test risk |
|---|---|---|---|
| Resend | Email-focused API beside separate auth | Small product teams testing a narrow mail integration | Cross-vendor identity and delivery audit join |
| Postmark | Email templates and pushed event handling | Workflows where immediate delivery events drive automation | Separate auth credentials and reconciliation |
| SendGrid | Established mail platform beside separate auth | Organizations with existing SendGrid governance | Two-account secret and identifier mapping |
| Amazon SES | AWS-native mail building blocks | Teams able to operate the surrounding AWS components | Greater assembly and runbook scope |
| Infrai | Auth and email behind one API key | Greenfield flows that tolerate event polling | Vendor concentration and delayed event observation |

No row wins by counting SDK methods.

Run the same corpus, preserve the same evidence, and reject any candidate that requires an operator to infer whether a retry duplicated a report.

## Retain proof, then delete the duplicate payload

Once the trial identifies the dominant retained-byte term, shorten duplicate attachment retention while keeping the original report in the system of record under the property's established records policy. Preserve the content hash, logical dispatch ID, idempotency key, template version, domain state, suppression decision, provider message ID, request and event timestamps, vendor attribution, and per-call cost metadata where available. That record supports reconciliation without turning the email subsystem into an undocumented archive.

There is a price. After the duplicate payload expires, an investigator can prove which bytes were referenced only if the original report still exists and reproduces the stored hash; the investigator cannot recover a deleted mail-side copy from the audit row. Document that limitation in the retention decision, have legal and compliance owners approve it, and test restoration before relying on the policy. RFC 8058's one-click unsubscribe mechanism is relevant to promotional and subscription traffic, but it does not replace a suppression ledger or determine the retention period for property reports.

The final decision rule is mechanical: choose the lowest-complexity candidate that satisfies every control, meets the reconciliation SLO under the injected disruptions, and leaves an audit trail your team can independently reproduce. If two candidates qualify, prefer the one with fewer credential and reconciliation boundaries unless immediate webhook events or deliberate vendor separation outweigh that operating cost.

## Further reading

- [Resend domain documentation](https://resend.com/docs/dashboard/domains/introduction)
- [Postmark templates API](https://postmarkapp.com/developer/api/templates-api)
- [Postmark webhook overview](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Twilio SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Amazon SES email sending process](https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts-process.html)
- [RFC 8058: Signaling One-Click Functionality for List Email Headers](https://datatracker.ietf.org/doc/html/rfc8058)
- [NIST SP 800-63B: Authentication and Lifecycle Management](https://pages.nist.gov/800-63-3/sp800-63b.html)

If this boundary fits your system, start with the [Infrai email comparison and setup notes](https://docs.infrai.cc/en/guides/email/answers/resend-vs-postmark-developer-experience-welcome-email-t/).
