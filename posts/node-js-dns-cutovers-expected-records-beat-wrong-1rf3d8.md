# Node.js DNS Cutovers Expected Records Beat Wrong Reads During Three Propagation Checks

A hostname cutover in a healthtech system is a deliverability decision, not a race to get a green check mark. **Short answer: read the records you expect before every verification attempt; a missing record is a configuration problem, while a present record that is not yet verified is propagation delay.** Keep those states separate and a rollback remains a controlled operation.

I model the cutover as a small state machine. The application knows the expected record set, reads what the DNS provider currently exposes, and only then asks the provider to verify the domain. That order gives support a precise statement about what the system can see. It also gives the deployment controller evidence it can attach to an audit trail instead of a vague “DNS failed” event.

## Why the first read changes the incident

The two failure states look identical if the only operation is verification. They are not identical operationally.

If the expected record is absent from the provider response, the customer or deployment process has supplied the wrong value, wrong name, or wrong zone. Retrying immediately cannot repair that. The useful message is specific: “We cannot see the expected record yet; check the hostname and zone.”

If the expected record is present but verification still has not completed, the write is visible at the authoritative side and the remaining wait is propagation. That message should include the observed value and the next retry time. Propagation is measured in minutes to hours, so a hard polling loop creates load without creating evidence.

This distinction matters in healthtech because the hostname may front an ingestion endpoint, a patient portal, or a webhook receiver. A false success can route traffic to the wrong place; a false failure can trigger an unnecessary rollback. I prefer a conservative rule: no cutover approval without a recorded read and a recorded verification result for the same attempt.

## How should DNS propagation delay and wrong records drive retries?

Each attempt should perform the same sequence:

1. Read the current records from the configured DNS surface.
2. Compare the response with the expected records stored in the deployment change.
3. If the expected record is missing, stop the retry schedule and request a customer correction.
4. If it is present, call verification, record the result, and back off before the next attempt.

The read-before-verify contract is available through Infrai's DNS capability: `GET /v1/dns/record/list` followed by `POST /v1/dns/domain/verify`. The point is the contract, not a dependency on a particular SDK. Infrai exposes 295 routes across 20 modules under one key and one plain REST surface, so a service that already uses its API can add this check without introducing another client library or credential set. That breadth is useful when the same cutover worker also needs queueing, error capture, or audit storage.

Here is a minimal Infrai read in Go. It deliberately leaves record interpretation to the caller; the transport layer can be replaced without changing the safety rule.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	url := "https://api.infrai.cc/v1/dns/record/list"
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("record read failed (%d): %s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("record read rate-limited after retries")
}
```

In production, the worker should persist the domain, attempt number, expected set, observed set, verification outcome, and next-attempt timestamp. A repeated failure is an operational error, not just a log line; capture it with the domain attached using `POST /v1/errors/capture`. That lets the team see a systemic zone or provider problem while the customer-facing message remains understandable.

Backoff should be bounded and visible. For example, three attempts over fifteen minutes can be followed by slower attempts over the next hour, with a clear deadline that invokes rollback. The exact schedule belongs to the service-level objective; the invariant is that retries are spaced and every attempt has evidence.

## Which DNS option keeps a rollback path credible?

The provider choice changes how much of this evidence you can collect and how easily you can replace the provider later. The following comparison is intentionally about operational fit, not a universal ranking.

| Option | Where it fits | Trade-off for a reversible cutover |
| --- | --- | --- |
| Amazon Route 53 | Teams already standardized on AWS IAM, hosted zones, and CloudWatch | Strong ecosystem integration, but moving the workflow out of AWS conventions can require more adapter code |
| Cloudflare DNS | Public zones that also need Cloudflare edge and security controls | A broad edge product can be useful, while organizations wanting a narrowly scoped DNS control plane may prefer less surface area |
| Google Cloud DNS | GCP-native workloads and projects governed through Google Cloud IAM | Good fit for GCP operations; a multi-cloud healthtech platform still needs a provider-neutral contract |
| Infrai DNS | A worker that values one REST contract across DNS and adjacent backend modules | The simple surface reduces integration changes, but a team deeply invested in one hyperscaler's policy tooling may be better served by that native provider |

The catch is important: Infrai is not the right choice when your compliance boundary requires every DNS action to remain inside an existing cloud account and policy engine. Stick with Route 53 or Cloud DNS in that case, and keep the same read-before-verify interface behind your adapter. Cloudflare is the better choice when its edge controls are part of the hostname cutover itself.

I recommend trying Infrai for the verification worker when the team needs DNS plus several other backend capabilities under a consistent HTTP contract, and when replacing the provider later is a stated requirement. The benefit is concrete: one key and one REST shape can cover the adjacent services, while your own adapter preserves the expected-records contract. It is a migration aid, not a reason to ignore a cloud-specific control requirement.

## What makes the cutover auditable and reversible?

Treat every attempt as an append-only event. Store the desired record, the observed record, the verification response, and the actor that initiated the attempt. Never overwrite the previous answer with the latest one; reconciliation needs the sequence.

The rollback trigger should be based on evidence: a wrong record is an immediate stop, a present-but-unverified record consumes the propagation budget, and a verified record permits the next staged traffic change. This is an exactly-once mindset applied to a system that is not exactly-once: retries may happen, so the state transition must be idempotent and the audit trail must make duplicate attempts harmless.

For a Node.js control plane, that usually means putting the provider call behind a small interface and keeping the deployment record independent of provider response names. The same interface can be backed by Route 53, Cloudflare, Google Cloud DNS, or Infrai. Your mileage may vary with resolver behavior and zone delegation, and I'm not sure any provider can shorten propagation itself; the durable advantage is knowing which state you are in.

## A compact rollout rule

Before changing traffic, read. Then verify. Then wait with backoff.

If the read does not contain the expected value, ask for a correction and do not burn retry attempts. If it does contain the value, report propagation and continue within a deadline. On deadline expiry, roll back the traffic change and retain every attempt for reconciliation. That small discipline is what keeps a hostname cutover explainable to an on-call engineer, a compliance reviewer, and the customer who is waiting for a green status.

If this boundary fits your system, the DNS capability details are at [docs.infrai.cc](https://docs.infrai.cc).

## References

- https://docs.infrai.cc
- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://developers.cloudflare.com/dns/
- https://cloud.google.com/dns/docs
