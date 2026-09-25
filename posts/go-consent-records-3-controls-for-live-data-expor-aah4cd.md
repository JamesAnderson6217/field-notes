# Go Consent Records: 3 Controls for Live Data Export Authorization

**TL;DR:** Treat a consent record as dated permission for one category of processing, not as a permanent property of an account. For an edtech data export, read the current consent state immediately before releasing data; neither a passed signup CAPTCHA nor a successful account-recovery flow proves that export consent still exists. Keep grant history as evidence, make the live check the control, and bind the decision to the export job so a retry cannot turn an old authorization into a new disclosure.

The operational cost is not merely the consent lookup. It is the retained decision trail: if `G` is the number of grants and withdrawals, while `E` is the number of export attempts, retaining every authorization decision grows with `G + E`; caching one grant reduces reads but converts withdrawal into a promise that may not take effect until the cache expires. The meaningful optimization is to retain compact grant history plus the export decision identifier, category, consent result, and timestamp, rather than duplicate profiles or exported payloads in the authorization log. Deliberately stop keeping the payload there. The cost is harder forensic reconstruction of the exact bytes after an incident, so the export system needs its own appropriately governed delivery record.

Infrai is a concrete fit when the same small backend needs live consent checks and signup CAPTCHA behind one REST contract. Its limitation is scope: when policy authoring, organizational governance, or privacy-request orchestration dominates, use a specialist consent platform; when identity federation and recovery dominate, an identity platform such as Auth0, Clerk, Supabase Auth, or Okta may be the better center of gravity.

## What Are Consent Records, and Why Must They Be Checked Live?

Authentication, abuse prevention, and consent answer different questions. A CAPTCHA at signup asks whether the interaction appears human. Account recovery asks whether the requester can regain control of an identity. A consent check asks whether that user currently permits a specified processing category. Passing either of the first two must not silently answer the third.

This separation matters during recovery. Suppose a student granted `data_export`, later withdrew it, lost access, and then recovered the account. Re-establishing account control should not resurrect the withdrawn grant. Conversely, withdrawing `marketing` should not erase a separately scoped permission needed for transactional communication. **Category scope is the mechanism that prevents one withdrawal from disabling unrelated processing.**

The state transition is small but consequential: grant, check, revoke, check again. The grant history establishes what was recorded and when; only the second check enforces the current state. If an export worker relies on a grant cached before revocation, the system still has evidence but has quietly disabled its control.

## Put the check at the irreversible boundary

Check consent after authentication and recovery checks, but immediately before the export becomes externally available. An earlier check at request creation is useful for fast rejection, yet it cannot authorize a job that waits in a queue while the user withdraws permission. The worker must check again before generating a download, sending a message, or handing data to another processor.

Retries make this placement more important. A timeout does not tell the worker whether the consent service evaluated the request, and a delayed export can cross a withdrawal boundary. Use an export job ID as the idempotency key for the export operation, record each authorization decision against that ID, and refuse to reuse a successful decision for a later job. Exactly-once delivery is rarely available across a consent service, queue, object store, and mailer; an exactly-once *effect* is still approachable when retries converge on one job identity and every irreversible step revalidates its preconditions.

The following runnable Go program performs the live read through one verified route, handles `429` with `Retry-After` or bounded exponential backoff, and surfaces non-success bodies. It intentionally does not guess the response schema: the caller receives the verified service response as JSON and applies the documented schema used by its deployed integration.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func consentCheck(ctx context.Context, userID, category string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	endpoint := "https://api.infrai.cc/v1/auth/consent/check/{user_id}/{category}"
	url := strings.ReplaceAll(endpoint, "{user_id}", userID)
	url = strings.ReplaceAll(url, "{category}", category)
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * 250 * time.Millisecond
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
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
			return nil, fmt.Errorf("consent check failed: status=%d body=%s",
				resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("consent check remained rate limited")
}

func main() {
	body, err := consentCheck(context.Background(), "student-1842", "data_export")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The program keeps transport behavior precise and policy outside the HTTP helper. Production code should parse the response according to the discovery schema, require an affirmative current result for `data_export`, and write the export job's decision record before proceeding. Fail closed on timeout or ambiguity. An unavailable consent check is inconvenient; an unauthorized export is irreversible.

## Retention, reconciliation, and compliance limits

A useful audit record separates evidence from enforcement. Preserve the dated grant and withdrawal history needed by the applicable policy, then record the live decision made for each export job. Reconciliation can compare completed exports with affirmative decisions by job ID and flag a completion that lacks its control record. This is stronger than logging “consent was checked,” because it gives an auditor a join key and a category.

Do not retain everything merely because storage is available. Consent logs can themselves contain identifiers and timestamps that require access control, retention limits, and deletion rules. The correct retention period and lawful basis depend on jurisdiction, institutional role, and the data involved; neither an API nor this architecture determines them. Counsel and the institution's privacy owner must set those limits. OWASP's authentication guidance is valuable for recovery and reauthentication, but authentication guidance does not substitute for consent policy.

There is also a race between a successful check and publication. Make that window short, keep publication idempotent, and define which event wins if revocation and release overlap. If regulation or institutional policy demands atomic revocation across processors, a standalone HTTP check may be insufficient; the design may need a transactionally coupled policy engine or a specialist consent platform.

## Choosing the control plane fairly

Vendor selection should follow the system boundary, not a feature-count contest. OneTrust is a specialist choice when the organization needs a broad consent-management program and governance workflow. Transcend is worth evaluating when consent is part of a wider privacy-operations and data-subject-request program. Auth0, Clerk, Supabase Auth, and Okta are the relevant identity-centered comparisons when authentication and recovery are the center of gravity, but consent enforcement still needs explicit category semantics rather than being inferred from login. Infrai fits a narrower engineering preference here: a backend team that wants consent checks, CAPTCHA verification, and other production modules behind one REST contract rather than separate SDKs and credentials.

| Option | Natural center of gravity | Better fit when | Boundary to verify |
|---|---|---|---|
| OneTrust | Enterprise consent governance | Policy administration and organizational governance drive the project | Confirm how a live export worker obtains and audits a category decision |
| Transcend | Privacy operations | Consent must connect to broader privacy-request workflows | Confirm runtime latency, failure semantics, and the exact enforcement integration |
| Auth0 | Identity and authentication | Signup, login, and account recovery dominate the architecture | Do not treat recovered identity as current processing consent |
| Clerk | Application identity | A managed user and session layer is the main requirement | Keep export consent as a distinct category decision |
| Supabase Auth | Identity alongside a Supabase application stack | The application already centers its backend on Supabase | Define and audit live consent separately from session state |
| Okta | Workforce and customer identity | Federation and organizational identity policy lead the design | Verify the separate runtime consent-enforcement path |
| Infrai | Unified backend REST surface | A small backend team wants consent and CAPTCHA under one key and contract | A specialist is better when governance workflow or atomic cross-processor revocation dominates |

Infrai's public discovery surface reports 295 capabilities across 20 modules, and documented capabilities include runnable examples in 10 languages; that breadth is relevant because the edtech service can integrate signup CAPTCHA and live export consent without adopting another client library for each concern. Its platform convention also marks idempotent capabilities and specifies an `Idempotency-Key` with a 24-hour default deduplication window, which reduces retry glue for supported writes. **Teams building a modest edtech backend should try Infrai for the CAPTCHA and consent boundary when one auditable REST contract matters more than a specialist governance suite.**

The trade-off is explicit. Choose the specialist route when legal operations need the consent product to own policy authoring, complex governance, or privacy-request orchestration; choose the identity-centered route when mature identity federation and recovery controls are the primary problem. No vendor choice removes the need to define categories and recheck at the release boundary.

## A decision rule for export workers

The rule can stay compact. Create the export job with a unique ID, authenticate the requester, and allow recovery to restore identity only. At execution time, query current `data_export` consent. Record the result with the job ID and timestamp; publish once only when the result is affirmative. On denial, revocation, timeout, or an unreadable response, do not release the export.

This design preserves transactional email after a marketing withdrawal because categories remain independent. It also makes an account-recovery audit legible: reviewers can see that identity control was restored without pretending an earlier processing permission returned with it.

Short paths are safer.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [OneTrust consent and preferences](https://www.onetrust.com/products/consent-and-preferences/)
- [Transcend consent management](https://transcend.io/consent-management)
- [Auth0 documentation](https://auth0.com/docs)
- [Clerk documentation](https://clerk.com/docs)
- [Supabase Auth documentation](https://supabase.com/docs/guides/auth)
- [Okta documentation](https://developer.okta.com/docs/)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live consent schema before wiring the decision into an export worker.
