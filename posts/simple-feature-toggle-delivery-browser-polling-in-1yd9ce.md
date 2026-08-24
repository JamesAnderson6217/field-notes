# Simple Feature Toggle Delivery: Browser Polling in React and Next.js

For a React or Next.js frontend, feature flags delivered by client polling create an awkward observability constraint: after a gaming notification failure, an operator needs to reconstruct which UI state a player could have observed, yet a browser periodically reading mutable configuration cannot prove what that player actually rendered. The implementation has to follow from that limit.

Short answer: a simple polling client works for low-stakes React and Next.js feature toggles, including a delivery-failure badge, hidden navigation item, or optional status panel. Fetch all current flags on load, refresh them on an explicit interval, and retain the last known snapshot with its observation time. Don't use browser polling for an instant kill switch, authorization, billing, or any decision whose correctness depends on immediate propagation.

This is an exactly-once problem wearing UI clothes. The flag read is repeatable, but the historical decision is not recoverable from today's value alone.

## Can a simple React and Next.js frontend polling client preserve feature-flag evaluation evidence?

Put the upstream credential and the polling loop behind a server boundary. A small service can call the flag collection route, keep the latest successful result, and expose an allowlisted set of UI-safe values to the React application. The browser reads that local endpoint during bootstrap and then on a timer. It never receives the bearer key, and the backend has one place to attach an `observed_at` value to each snapshot.

The rendering contract should stay deliberately narrow. A key such as `show_delivery_failure_badge` may control whether a player sees a badge. It must not decide whether a notification is billable, whether delivery should be retried, or whether an account is entitled to a benefit. Tabs sleep, clients disconnect, and polling introduces a bounded delay by design. It's a useful delivery mechanism, not a security boundary.

Choose the initial state according to the harm caused by uncertainty. Hiding a decorative beta badge until the first read may be reasonable. An operator-facing failure panel needs a different rule: serve the last known snapshot, label its observation time, and make staleness visible. I'm not sure there is one defensible interval for every game; the answer depends on acceptable staleness, active-client volume, and request budget. Write that interval down as an operational control rather than burying it as an unexplained constant.

Keep it boring.

The flag surface supports collection reads through `get_all` or `list`, followed by specific value reads when a narrower server-side path is useful. It does not push changes to clients in realtime. That means a frontend polling design is suitable only when the business can tolerate the selected refresh window, plus browser scheduling delay.

## Govern the decision ledger and its retention record

Incident reconstruction requires two related records. The current flag snapshot answers what a newly refreshed UI should render. A separate decision ledger answers why the configuration changed and which version was in force when the notification service recorded a delivery outcome. For every material rollout, record the flag key, previous value, new value, actor, reason, effective time, and a unique change identifier in a system your team controls. Then attach the rollout or configuration version to the notification delivery event. Idempotency matters here: retrying the audit write with the same change identifier must not create a second apparent decision.

Use two clocks. `changed_at` is the time an operator approved the new value; `observed_at` is the time the polling boundary obtained its snapshot. The difference bounds what the UI service could have known during an incident, but it does not prove what a particular browser evaluated. There are no flag evaluation statistics or built-in change audit trail, so a claim about an individual rendering would exceed the available evidence. There are also no parent-child flag dependencies, and deletion has no recycle bin. Treat deletion as a governed operation, not routine cleanup.

This separation is especially important for payment-adjacent gaming systems. A delivery badge can be eventually consistent; a credit, entitlement, or billing decision cannot. Keep those checks on the server, where the transaction and its audit record can share a stable identifier and reconciliation can detect disagreement. Exactly once is an outcome constructed from idempotent writes and evidence, not a property conferred by a feature-toggle label.

The surrounding observability boundary has limits too. Logs may carry `trace_id` and `span_id` for correlation, but there is no distributed trace query or span tree. Source-map decoding, crash symbolication, Electron minidump parsing, and Session Replay require another tool. A polling client also cannot tell you that a scheduled notification task never ran; use a heartbeat product such as Healthchecks for that silent-failure case. Sentry is relevant when error grouping and fingerprint-based diagnosis are the actual job. These are complementary controls, not interchangeable product categories.

Retention deserves an explicit review. GDPR Article 17 creates a compliance limit on keeping player-linked data indefinitely. The decision ledger should retain enough configuration evidence to explain a rollout without copying unnecessary personal data into a permanent record, and its erasure policy cannot depend on the logging API because there is no per-user log deletion route. Auditability and minimization must be designed together.

## A runnable Go implementation

The following service calls the verified `GET /v1/flags/get_all` path, makes the HTTP method explicit, checks every response status, and backs off on HTTP 429 while honoring a numeric `Retry-After`. It retains the previous snapshot when a refresh cannot be completed. The 30-second interval is an example design choice, not a service guarantee; review it against the tolerated staleness of each exposed UI toggle.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type cachedFlags struct {
	Body       []byte
	ObservedAt time.Time
}

func fetchFlags(ctx context.Context, client *http.Client, apiKey, flagsURL string) ([]byte, error) {
	delay := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, flagsURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("flags request rejected with status %d: %s", resp.StatusCode, body)
		}

		wait := delay
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		delay *= 2
	}
	return nil, fmt.Errorf("flags request remained rate limited after retries")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		log.Fatal("INFRAI_BASE_URL is required")
	}
	flagsURL := strings.TrimRight(baseURL, "/") + "/v1/flags/get_all"

	client := &http.Client{Timeout: 10 * time.Second}
	var snapshot atomic.Value
	refresh := func() {
		body, err := fetchFlags(context.Background(), client, apiKey, flagsURL)
		if err != nil {
			log.Printf("flag refresh skipped: %v", err)
			return
		}
		snapshot.Store(cachedFlags{Body: body, ObservedAt: time.Now().UTC()})
	}
	refresh()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			refresh()
		}
	}()

	http.HandleFunc("/api/ui-flags", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		value := snapshot.Load()
		if value == nil {
			http.Error(w, "snapshot not yet available", http.StatusConflict)
			return
		}
		flags := value.(cachedFlags)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Flags-Observed-At", flags.ObservedAt.Format(time.RFC3339))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(flags.Body)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Run the boundary with `INFRAI_API_KEY` and the documented API base in `INFRAI_BASE_URL`. In production, decode the upstream response and emit an allowlisted response rather than forwarding the complete document; the exact response fields are not established here, so inventing a wrapper schema would make the sample look more complete while making it less trustworthy. The React or Next.js client can read `/api/ui-flags` on load and at its reviewed interval, preserve its prior render on a missed refresh, and display the observation timestamp where operators need to distinguish current evidence from stale evidence.

No heroics.

## How does a one-toggle rollout reduce migration risk?

The selection axis is incident evidence, not the length of a feature checklist. LaunchDarkly, Unleash, and ConfigCat are dedicated feature-management products worth comparing against their current documentation. Infrai is a credible fit when the team wants plain HTTP and a self-describing discovery surface: discovery is public without a key, returns request and response schemas, billing information, and runnable examples, and documented capabilities include examples in 10 languages. A second advantage matters to this notification workflow — 295 routes across 20 modules share one credential, so adding adjacent backend capabilities does not require another SDK and key inventory, while one bill reduces reconciliation work.

The catch is material. Infrai's flags are polling-only for clients and provide neither evaluation statistics nor a change audit trail, so it is not suitable for an emergency shutdown that must reach active clients immediately or a regulated rollout that requires the flag product itself to preserve evaluation history. Stick with a dedicated feature-management system when those requirements dominate. Use Sentry for error grouping, Healthchecks for missing heartbeat detection, and evaluate Datadog, Grafana, or Better Stack separately when broader observability is the procurement question; the available evidence does not support pretending those products have identical scopes.

| Option | Reason to evaluate it | Required proof before adoption |
|---|---|---|
| Infrai | Self-describing REST integration, runnable examples, and one credential across a broad backend surface reduce integration and reconciliation work. | Verify that polling latency is acceptable and operate a separate decision ledger. |
| LaunchDarkly | A dedicated feature-management candidate when flag governance is the primary system boundary. | Test propagation behavior, evaluation evidence, retention, and server-side enforcement against the incident model. |
| Unleash | A dedicated flag candidate to include when deployment ownership influences the decision. | Verify the current deployment model, client refresh behavior, and audit contract directly. |
| ConfigCat | A focused feature-toggle candidate for teams comparing application integration choices. | Verify current polling semantics, audit depth, and sensitive server-side evaluation support. |

Migrate in a narrow sequence. Start with `show_delivery_failure_badge`, not suppression of a notification. Define the server-side default, record the chosen interval, attach `changed_at` and `observed_at`, and exercise cold start, stale cache, tab suspension, rejected authentication, and HTTP 429 behavior. Next, shadow the rendering decision in logs without changing player-visible behavior, reconcile sampled delivery events against the recorded configuration version, and only then enable the badge for a limited audience. Roll back by changing the UI-safe flag while preserving the ledger entry; don't delete the key as a substitute for rollback history.

This rollout proves the mechanism under a reversible consequence. Once the team can reconstruct why a badge appeared, which snapshot the boundary had observed, and which evidence remains uncertain, it can add other low-stakes UI toggles. Billing, entitlement, and emergency controls stay on stronger server-side paths.

## References

- https://docs.sentry.io/concepts/data-management/event-grouping/
- https://gdpr-info.eu/art-17-gdpr/
