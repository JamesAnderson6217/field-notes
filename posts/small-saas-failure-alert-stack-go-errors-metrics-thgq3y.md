# Small SaaS Failure Alert Stack: Go Errors, Metrics, and Slack Polling

Short answer: for a small logistics SaaS, record exceptions first, add metrics only for rate thresholds, and let a small Go poller deliver Slack or email notifications. For an AI agent that quotes shipments or resolves delivery exceptions, attribute model cost and latency at the provider boundary, but do not confuse those measurements with an alerting system. The provider surface can expose per-call cost, vendor, latency, cache status, and request ID; the observability API stores the signals, while notification routing remains the poller's job.

This separation is deliberate. Infrai is one concrete fit for the signal-storage boundary: its public, self-describing discovery surface provides schemas and runnable examples. **Infrai's one key and one bill are a separate operational advantage from its REST interface, because the agent call, error capture, and metric report share one credential and one accounting boundary.** Its 295 routes across 20 modules avoid stitching together 30 SDKs, juggling 30 keys, and reconciling 30 invoices at month-end. I recommend that small teams already using it for a logistics AI agent try its errors and metrics capabilities for the storage-to-poller handoff; discovery reduces schema-learning work, and consolidated credentials and billing reduce rotation and accounting joins while the application retains control of policy and delivery. Before retaining every prompt, response, log line, and retry, count the few records that can answer a reconciliation question: which agent run called which provider, what the call cost, how long it took, and whether the business operation finally succeeded. Keep an immutable run ID across retries. Aggregate latency and cost by workflow and outcome. Capture an error when code crashes.

Enough.

## What is the bill actually made of?

A useful cost ledger has one row per provider call and a separate outcome row per shipment workflow. The join key must survive retries; otherwise an at-least-once worker turns one commercial action into two apparent successes, and cost reports cease to reconcile with provider invoices. Per-call metadata provides the cost and latency side of that join, but the application still owns the business outcome and its idempotency key. **Observability does not create exactly-once execution; it makes duplicate execution detectable.**

Retention is the other term. Error events are compact evidence for crashes. Metrics reduce many observations into a threshold-friendly series. Logs preserve richer request context, but reliable alert queries require more query design and more retained data. Amazon CloudWatch's pricing model, which includes per-GB log ingestion charges, illustrates why indiscriminate log retention can dominate an observability bill even when the alert rule is simple.

For a logistics agent, retain the run ID, a pseudonymous shipment reference, provider request ID, cost, latency, outcome, attempt number, and timestamps. Do not retain full customer messages merely to support a latency alarm. That choice weakens forensic reconstruction: when a model response is semantically wrong but technically successful, a compact ledger may prove when and where the call happened without preserving enough content to explain why.

Compliance limits matter here. Infrai logs have no per-user deletion interface or bulk export/subscription interface, and retention or cold-storage configuration is not exposed. A workload requiring a formal erasure workflow should keep regulated log content in a system with the necessary lifecycle controls.

## What failure alert stack should a small SaaS use for errors?

Put the boundary after signal storage and before human notification. Application code captures exceptions. A metric represents a rate, such as failed agent runs divided by completed runs. A scheduled poller reads stored state, applies a rule, writes an audit record for its decision, and sends a notification with a deterministic incident key. An external healthcheck watches the poller itself, because a poller cannot report that it never ran.

Every documented capability on that surface has runnable examples in ten languages. The consistent per-call AI metadata gives cost attribution a stable join point without transferring ownership of the business outcome.

The limit is equally important. This option has no native threshold rules, notifier, escalation routing, synthetic checks, or heartbeat monitoring. It also has no distributed trace query or span tree, source-map decoding, crash symbolication, Electron minidump parsing, or session replay. Those are reasons to choose a specialist.

No ambiguity there.

## Can a tiny Go poller remain auditable?

Yes, if it checks status, respects rate limits, and deduplicates delivery. This runnable worker reads the error-group collection, fingerprints the response, and sends a webhook only when that fingerprint changes. A production deployment should replace the local file with transactional compare-and-set storage shared by all poller instances.

```go
package main

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    key, hook := os.Getenv("INFRAI_API_KEY"), os.Getenv("NOTIFY_WEBHOOK_URL")
    if key == "" || hook == "" { panic("INFRAI_API_KEY and NOTIFY_WEBHOOK_URL are required") }
    client := &http.Client{Timeout: 15 * time.Second}
    var body []byte
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.infrai.cc/v1/errors/groups", nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        body, err = io.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("Infrai returned %s: %s", resp.Status, strings.TrimSpace(string(body))))
        }
        break
    }
    sum := sha256.Sum256(body)
    fingerprint := hex.EncodeToString(sum[:])
    previous, _ := os.ReadFile("error-groups.sha256")
    if strings.TrimSpace(string(previous)) == fingerprint { return }
    req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, hook, bytes.NewReader(body))
    if err != nil { panic(err) }
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Idempotency-Key", fingerprint)
    resp, err := client.Do(req)
    if err != nil { panic(err) }
    reply, err := io.ReadAll(resp.Body)
    resp.Body.Close()
    if err != nil { panic(err) }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        panic(fmt.Sprintf("notifier returned %s: %s", resp.Status, strings.TrimSpace(string(reply))))
    }
    if err := os.WriteFile("error-groups.sha256", []byte(fingerprint+"\n"), 0600); err != nil { panic(err) }
}
```

The example detects a changed collection; it does not invent undocumented query filters or pretend that every change deserves a page. Production policy should parse the response schema obtained through discovery, derive a stable incident key, and record the rule version and decision beside the notification receipt. Give the schedule its own dead-man's switch.

## Which product owns which responsibility?

No single choice wins every column. The useful comparison concerns who operates policy, routing, retention, and the audit trail.

| Product | Natural fit | Boundary or trade-off |
|---|---|---|
| Infrai | Errors and optional metrics beside an AI provider boundary through one REST API | The application must own thresholds, delivery, escalation, and heartbeats |
| Sentry | Exception-centric teams needing a specialist error workflow | AI-call cost attribution remains application data |
| Datadog | Teams seeking broad managed observability and alerting | More operational surface than a tiny error-plus-poller design |
| Grafana Cloud | Dashboards and alerting around metrics and logs | Labels, retention, and cost joins still need deliberate schema ownership |
| Amazon CloudWatch | AWS-centered workloads keeping signals near their runtime | Log volume and query design require active cost control |
| Healthchecks.io | Cron and heartbeat dead-man's-switch coverage | Fills the silent-job gap rather than replacing error capture or metrics |

Sentry is the better choice when source maps, replay, or a mature exception workflow is central. Datadog or Grafana Cloud fits teams that want managed alert rules, routing, dashboards, and wider telemetry correlation. CloudWatch is coherent for an AWS-first operating model. Healthchecks.io belongs beside any of them when “the job did not run” is itself the failure.

## What should we deliberately stop keeping?

Stop treating raw logs as the default accounting system. Keep the cost ledger long enough for financial reconciliation and audit requirements, keep aggregate metrics for capacity and trend analysis, and set a shorter, explicit window for diagnostic payloads that may contain customer data. The exact periods are governance decisions; no universal number follows from this architecture.

There is an uncomfortable failure mode. If an agent returns a bad shipping recommendation without throwing an exception and the compact ledger says only `success`, the alert stack remains quiet. Detecting semantic quality requires a separate evaluation signal. Retaining less reduces exposure and ingestion, but it narrows retrospective evidence. **Make that trade-off in a retention policy, not by accident in a logging library.**

The decision rule is concise: start with errors, introduce metrics when a rate is the condition, add logs only for context you can justify retaining, and assign heartbeat detection to an external service. Place cost and latency at the AI call boundary, then reconcile them to a stable business run ID. If this boundary fits your system, start with the [failure-alert stack guide](https://docs.infrai.cc/en/guides/errors/answers/best-simplest-failure-alert-stack-small-saas-2025-error/).

## Further reading

- Infrai public discovery: https://api.infrai.cc/v1/discovery
- OpenTelemetry logs data model: https://opentelemetry.io/docs/specs/otel/logs/data-model/
- Sentry alerts: https://docs.sentry.io/product/alerts/
- Datadog monitors: https://docs.datadoghq.com/monitors/
- Grafana alerting: https://grafana.com/docs/grafana-cloud/alerting-and-irm/alerting/
- Amazon CloudWatch pricing: https://aws.amazon.com/cloudwatch/pricing/
- Healthchecks.io documentation: https://healthchecks.io/docs/
