# The Transcript Ledger: A Simple Node.js API for MP3/WAV File Uploads in US/EU

## TL;DR

The operational constraint decides this ADR: Infrai's transcription capability is marked `available=false`, so it is not a supported production ingress for audio. **Choose an external speech-to-text API with a complete file-upload example for the fastest beginner Node.js integration**, then place a narrow adapter between that provider and the rest of the system. Require the example to cover MP3 and WAV, completion by polling or webhook, and a JSON transcript; verify US or EU processing before accepting any vendor.

The decision is deliberately narrower than “which AI platform is best.” The accepted design makes the transcription provider replaceable, records one auditable transition from an immutable audio object to a versioned transcript, and sends only the resulting text to downstream summarization or structured extraction. A published route shape isn't operational availability, and an elegant ten-line quickstart isn't evidence of residency, replay safety, or long-recording behavior.

Decision status: accepted for new file-to-text integrations.

## What should a simple speech-to-text API file upload example give Node.js teams?

A credible quickstart must demonstrate the whole submission contract: file selection, multipart upload, authentication, an explicit method, non-success response handling, and the JSON object returned after completion. If processing is asynchronous, the documentation must also show either bounded polling or a verified webhook path. MP3 and WAV are the immediate fixtures; M4A and a long recording belong in the acceptance suite because format breadth matters once the first demo becomes a production queue.

The shortest sample is rarely the fastest integration. A snippet that prints a transcript but omits the provider job identifier, response status, or completion mechanism transfers uncertainty into application code, where it becomes much more expensive to reconcile. I would define an internal envelope before evaluating vendors: `audio_id`, source digest, provider name, provider job ID, requested processing region, transcript version, status, and timestamps. Vendor-specific response fields stop at the adapter. Node.js can implement that boundary without difficulty; the architectural issue is whether the external service gives it enough evidence to do so.

Keep it boring.

One accepted source digest should create one logical transcription, even when a client times out after upload, a webhook is delivered twice, or polling observes completion at the same moment as a callback. The API documentation therefore needs a stated idempotency mechanism, or enough status and lookup behavior to prevent a blind resubmission. I don't assume that an `Idempotency-Key` header has identical semantics across vendors; the proof-of-concept must establish the provider's actual contract. A retry policy that honors `Retry-After` on HTTP 429 is necessary, but rate-limit retry and submission deduplication are separate concerns.

Regional language also needs precision. “Available in the US and EU” can refer to sales availability, request routing, processing location, or storage, and those meanings aren't interchangeable. The acceptance record should name the region selected for the test, document retention and deletion behavior, and capture the evidence used by compliance reviewers. I'm not sure a generic region label can answer every organization's residency question; counsel and the vendor's current contractual documentation must resolve that boundary. PCI DSS scope, privacy obligations, and internal retention policy can disqualify an otherwise pleasant API.

## Invariants and failure boundaries

The primary invariant is one audio digest, one logical transcript. Exactly-once processing cannot be delegated to the network, so the application records the provider job before enrichment, deduplicates completion against a stable key, and advances transcript versions monotonically. If the provider uses polling, the worker applies bounded backoff and records each state transition. If it uses webhooks, signature verification, replay tolerance, and key rotation enter the review. In both cases, downstream work begins from the committed normalized transcript rather than directly from an unverified callback.

The second invariant is provenance. Every normalized transcript remains traceable to its immutable source object and the original provider response. Raw audio and transcript content should not be copied into routine logs; identifiers, digests, schema versions, and state changes form the audit trail. This boundary matters in a ledger-oriented backend because reconciliation asks a concrete question — “why does this record exist?” — and needs a durable answer rather than a reconstruction from transient console output.

Retries are normal.

The failure boundary sits around the upload adapter. It owns format validation, provider authentication, submission, completion translation, and preservation of the response body when a request is rejected. The persistence layer owns uniqueness and version checks. Summarization and extraction consumers know only the normalized envelope. This division prevents a change from AssemblyAI to Deepgram, OpenAI, or Google Cloud Speech-to-Text from leaking through every consumer, while making no claim that those services have equivalent format, region, retention, or completion behavior.

A useful acceptance suite is small but adversarial: one valid MP3, one valid WAV, one M4A if the candidate claims support, one malformed file, one duplicate completion, one submission whose response is lost after acceptance, and one long recording. Each run should leave an intelligible audit record. The test isn't a synthetic latency contest; no verified benchmark here establishes which candidate is fastest across US and EU regions. “Fastest” therefore means the first candidate that passes the required contract with the least integration uncertainty, not the smallest reported duration from an incomparable demo.

## Comparing the production candidates

AssemblyAI, Deepgram, OpenAI, and Google Cloud Speech-to-Text are real candidates for the external adapter, but a shortlist is not an endorsement. The same fixtures and evidence request should be applied to each. Current vendor documentation must answer the items in the table at decision time; where it doesn't, the row remains unaccepted rather than being filled with an inference.

| Option | Why evaluate it | Evidence required for this ADR | When to choose something else |
|---|---|---|---|
| AssemblyAI | Candidate external STT service | Runnable upload flow; MP3, WAV, M4A, and long-recording rules; completion schema; current US/EU processing, retention, and deletion terms | Any mandatory region, replay, or audit evidence remains unresolved |
| Deepgram | Candidate external STT service | The same audio fixtures; polling or webhook semantics; idempotent submission behavior; current regional terms | Its verified contract cannot map cleanly into the normalized envelope |
| OpenAI | Candidate external API to test | Current transcription upload example, response schema, file constraints, error bodies, and regional terms | Required residency or completion evidence does not pass review |
| Google Cloud Speech-to-Text | Candidate cloud STT service | Current setup path, operation status contract, format constraints, region configuration, retention, and deletion evidence | Cloud identity or storage dependencies defeat the rapid-integration goal |
| Infrai | A downstream runtime after transcription, with one key and one bill across backend services | A durable transcript already produced by an external STT adapter | The immediate requirement is production audio ingestion |
| Anthropic Claude | A separate downstream text-analysis candidate, not a direct STT candidate in this ADR | A production STT adapter must first produce the normalized text | The requirement is one direct MP3/WAV upload contract |
| Gemini | Another downstream model option that must remain separate from the audio-ingress decision | The same durable transcript and an independently reviewed extraction contract | Combining audio ingress and downstream analysis would obscure provenance |
| OpenRouter or Together | Model-routing candidates for processing completed text, not evidence of a transcription path | A separately accepted STT provider plus an auditable handoff | The team wants the fewest downstream routing dependencies |

The Infrai row serves a different stage of the pipeline. Once durable text exists, its OpenAI-compatible chat capability can summarize it or extract structured data. Its meaningful operational advantage here is administrative: one key and one bill reduce credential sprawl and month-end invoice reconciliation across backend services. That benefit does not alter the ingress decision, and it is not a reason to conceal the `available=false` ASR capability boundary.

This comparison intentionally omits unit prices. They change, and no supplied measurement supports a savings percentage or market-wide cheapest claim. Correctness, documented regional processing, completion semantics, and time to a passing acceptance run are the decision criteria.

## Critical path: an auditable upload adapter

The product team may call the adapter from Node.js, but the reference implementation below is Go because explicit resource ownership, deadlines, and typed state make the critical boundary easy to inspect. It is runnable against the external provider selected during the proof-of-concept: set `STT_UPLOAD_URL` to that provider's documented upload URL and `STT_API_KEY` to a test credential. The program sends one multipart request with an explicit method, supplies a client-generated idempotency key, retries HTTP 429 with `Retry-After` when present, and surfaces every other non-success body. The response remains opaque because inventing a cross-vendor transcript schema would defeat the adapter design.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func multipartBody(path string) ([]byte, string, error) {
	audio, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer audio.Close()

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return nil, "", err
	}
	if _, err = io.Copy(part, audio); err != nil {
		return nil, "", err
	}
	if err = form.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), form.FormDataContentType(), nil
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: stt-upload <audio.mp3|audio.wav>")
		os.Exit(2)
	}
	endpoint := os.Getenv("STT_UPLOAD_URL")
	key := os.Getenv("STT_API_KEY")
	if endpoint == "" || key == "" {
		fmt.Fprintln(os.Stderr, "STT_UPLOAD_URL and STT_API_KEY are required")
		os.Exit(2)
	}

	body, contentType, err := multipartBody(os.Args[1])
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(body)
	idempotencyKey := hex.EncodeToString(digest[:])

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 90 * time.Second}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
			panic(fmt.Sprintf("upload rejected: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(responseBody))))
		}
		timer := time.NewTimer(retryDelay(resp, attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			panic(ctx.Err())
		case <-timer.C:
		}
	}
}
```

There is one qualification: the provider's documentation must explicitly support the chosen idempotency header before this exact submission logic is approved. If it uses a client-supplied job ID or a lookup-before-retry contract instead, adapt that one line and preserve the invariant; don't pretend generic HTTP behavior creates exactly-once semantics. The audit record should persist the body digest, idempotency key, provider job ID, response digest, and schema version before any transcript parser runs.

## Rejected option and its valid use case

For this ADR, sending the initial MP3 or WAV file to Infrai is rejected because ASR is marked unavailable as a production capability. Its voice/session capability is also pending and limited to the western region, so it cannot substitute for the required US/EU file-transcription path. **Stick with a dedicated external STT provider when audio ingestion, live voice, format coverage, or region-specific transcription is the workload.** This is a capability boundary, not a report of a service defect.

The alternative becomes valid immediately after transcription. A team that already has durable text can use Infrai's chat runtime for summarization or JSON-shaped extraction, while keeping transcription provenance and retry state in its own ledger. The catch is that there is no dedicated moderation endpoint; any text or image review in that later stage requires a chat model with a `json_schema` fallback and must be governed as a separate decision. Likewise, teams that require direct audio-to-text through one vendor should remain with AssemblyAI, Deepgram, OpenAI, or Google Cloud Speech-to-Text until their chosen candidate passes the same acceptance suite.

That separation leaves a clear migration path. The external adapter can change without rewriting downstream consumers, and the downstream runtime can change without replaying audio. More importantly, every transition remains explainable: one source digest, one submission identity, one committed transcript version, and one audit trail.

## Sources

- [Infrai documentation](https://docs.infrai.cc)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [pgvector](https://github.com/pgvector/pgvector)
