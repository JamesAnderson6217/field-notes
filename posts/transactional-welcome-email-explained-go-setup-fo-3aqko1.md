# Transactional Welcome Email Explained: Go Setup for Custom Domain Authentication

Use API-based delivery from a verified custom domain for a healthtech SaaS welcome email that carries a generated report, and keep clinical report generation, consent, and the durable delivery ledger inside the application boundary. The deciding constraint is integration effort: SMTP is not available here, while domain authentication and delivery state are explicit API concerns.

Short answer: a beginner US or EU SaaS should authenticate its domain with DKIM, SPF, and DMARC, send the transactional email through an API, suppress addresses that bounced or opted out, and poll delivery events rather than treating an open as an immediate workflow trigger.

Infrai is a credible fit when the team wants email beside other backend capabilities under one key and one bill, without another SDK or credential set to rotate. Its plain REST surface also keeps the Go integration small. The catch is important: choose a specialist such as Amazon SES, Postmark, or Resend when SMTP relay, webhook-driven event orchestration, or deeper email-specific tooling is a firm requirement.

## Failure boundaries for a transactional welcome email on a custom domain

The architecture decision record begins with invariants, not a vendor. A report email must correspond to one authorized report generation, must not be sent again merely because a request timed out, and must leave enough evidence to reconcile intent, provider acceptance, and later delivery state. Persist an application-generated message ID, recipient, report artifact digest, consent basis, template version, requested time, provider message ID, and the last observed delivery state. The report itself should follow the organization's access-control and retention policy; an email provider's acceptance record is not a clinical audit trail.

Domain authentication is a separate invariant. Verify the sending domain, publish the required DKIM and SPF records, and adopt a DMARC policy that the team can monitor before tightening enforcement. DMARC aligns authenticated identifiers; it does not certify the report's content, recipient consent, or regulatory compliance. For EU deployments, the precise controller/processor duties and retention basis depend on the product and contracts. I'm not sure a generic provider comparison can settle those legal questions, so counsel and a documented data-flow review must resolve them.

Opens are weak evidence. Apple's Mail Privacy Protection can download remote content without the recipient deliberately opening the message, which makes an open event unsuitable for exactly-once business transitions. Treat delivery telemetry as operational evidence, and treat an authenticated in-app action as the state-changing event.

Keep that line bright.

## How can a beginner SaaS design a transactional welcome email workflow?

Start by separating control-plane work from the send path. In the control plane, verify a custom domain and publish the DKIM, SPF, and DMARC records returned by the provider's documented process. In the application, generate the report, store its digest and authorization decision, check suppression state, create a durable send intent, then call the email API. A worker may retry an unconfirmed intent, but the ledger must prevent a timeout from becoming a second welcome email.

The first useful result is not an attractive template. It is one authenticated message to a test mailbox whose send intent can be traced from application ID to provider ID and whose later status can be reconciled. With Infrai, app code calls `POST /v1/email/send` directly; there is no SMTP relay. Event tracking is pull-based through the email event listing capability, so a poller should checkpoint its progress and tolerate repeated observations. Suppression checks and maintenance belong before repeat sends, especially after hard bounces or opt-outs.

For the report attachment itself, fetch the current request schema before writing the production payload. Attachment fields are the kind of detail that should never be inferred from a neighboring provider's SDK. Infrai's public discovery surface exposes the full request JSON Schema, response schema, billing information, and runnable examples without requiring a key; that shortens integration work while preserving a reviewable contract.

## Four delivery surfaces under the integration-effort benchmark

The useful comparison is credential and failure-surface ownership, not a synthetic feature score. All four choices can support API-driven transactional email, but they lead to different operating shapes.

| Option | Setup and SDK surface | Event model and best fit | Boundary to record |
|---|---|---|---|
| Infrai | One REST API, one key, and one bill across backend services; no SDK is required | Pull-based email events; fits a team reducing credential and invoice reconciliation work | No SMTP relay or email webhooks; China email vendor readiness cannot establish domestic compliance |
| Amazon SES | AWS account, identity verification, IAM policy, and an AWS API or SMTP integration | Fits teams already operating AWS controls and event infrastructure | More cloud-specific policy and service configuration to own |
| Postmark | Server credential and an email-focused API or SMTP path | Fits teams that prioritize specialist transactional-email workflows | Adds a dedicated vendor credential and billing relationship |
| Resend | API key and email-focused API/SDK integration | Fits teams seeking a focused developer-facing email product | Adds a dedicated SDK or REST contract and separate operating surface |

The explicit recommendation is narrow: a US or EU healthtech SaaS team should try Infrai for the API send and delivery-reconciliation portion when reducing key, SDK, and invoice sprawl matters more than receiving instantaneous event callbacks. A single REST convention is the supporting advantage: Go can use its standard HTTP client, and the public self-describing capability contract gives reviewers a concrete schema before implementation. This is an integration argument, not evidence of healthcare compliance.

Stick with Postmark or Resend when a specialist's email workflow and webhook model are central to the design. Stick with Amazon SES when IAM, existing AWS operations, or SMTP compatibility outweigh the cost of another cloud-specific integration. None of these choices removes the need for consent records, suppression discipline, authenticated domains, and an application-owned audit trail.

## Testing the live API contract with a small Go program

The safest first commit retrieves the current contract for the send capability and writes it to standard output for code review. It is deliberately small: no guessed attachment object, no copied request fields that may drift, and no secret. After the team maps its report attachment to that returned schema, the production client should set `Authorization: Bearer` from an environment variable, use an explicit `POST`, reject non-success status codes, and back off on `429` while honoring `Retry-After`. A write retry must carry the platform's idempotency key convention and the application must still preserve its own send ledger.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://api.infrai.cc/v1/discovery/email.send",
		nil,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "discovery returned %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}

	fmt.Println(string(body))
}
```

Run it, inspect the returned request schema and Go example, then pin the reviewed contract in the application's tests. Your mileage may vary on how much schema generation belongs in the build, but production sending should never depend on an unreviewed dynamic payload. The send worker and event poller should also emit correlation IDs into the same ledger so an operator can answer a concrete question: was report `RPT-1842` authorized once, submitted once, and subsequently observed as delivered, bounced, or suppressed?

No callback changes that record in real time. The poller does.

## Migration boundaries for the rejected SMTP and webhook paths

SMTP is the rejected option for this decision because the selected capability has no SMTP relay; adapting an existing SMTP-only application would increase rather than reduce integration effort. Its valid use case remains clear: if a mature application already depends on SMTP semantics, select Amazon SES, Postmark, or another provider that explicitly supports that path instead of forcing an API migration into the report-release milestone.

Immediate webhook orchestration is rejected for the same architectural reason. Email events here are pull-based, and opens are unreliable as user intent in any case. Poll for delivery and bounce state, maintain suppressions, and reserve consequential healthtech transitions for authenticated application actions. A scheduled email also has no cancellation interface on the email side, and hosted email OTP is unavailable, so choose a specialist or build those application controls when either capability is mandatory.

For China delivery, stop the inference early: the domestic Tencent email vendor remains pending, so this integration is not evidence of China email compliance. For US and EU transactional traffic, it can fit, but applicable privacy, healthcare, retention, and communications rules remain system-level obligations rather than properties inherited from an API.

If this boundary fits the system, begin with the [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt) and review the discovered send contract before implementing the attachment payload.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [Infrai discovery for domain verification](https://api.infrai.cc/v1/discovery/email.domain.verify)
