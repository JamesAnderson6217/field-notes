# Explicit Deletion and Retention Rules for Temporary Campaign Images and Videos

Short answer: define a retention policy for temporary campaign media, keep source and derivative identifiers separate, and delete only identifiers that your lifecycle record has confirmed. For a gaming team removing backgrounds from product photos, the moderation decision is more important than the storage vendor: an image that is rejected must not quietly remain beside a generated video that passed review.

This is an architecture decision record, not a list of delete buttons. The invariants are explicit: every source image and generated video has an owner, a purpose, a retention deadline, and an audit event; a derivative never overwrites its source identifier; and a retry cannot turn an uncertain lookup into a destructive guess. The failure boundary is equally clear. If the system cannot prove which asset an identifier names, it records a reviewable failure and leaves the object in place until policy permits a later sweep.

## What does a safe campaign-media lifecycle look like?

Start with the user-visible result. A campaign operator should see that an approved product photo is available for the storefront, while a rejected upload and all temporary derivatives disappear after the declared deadline. “Delete the campaign” is not a sufficient requirement because an image and a generated video may have different review states, owners, and retention clocks.

I model each asset as an immutable ledger row:

| Field | Why it matters |
| --- | --- |
| `asset_id` and `kind` | Prevents an image identifier from being sent to a video operation. |
| `source_asset_id` | Keeps the original distinct from background-removed derivatives. |
| `moderation_state` | Makes unacceptable output a policy decision, not an accidental cleanup side effect. |
| `retain_until` | Gives a deterministic deletion due date. |
| `delete_state` and `deleted_at` | Lets reconciliation distinguish confirmed deletion from an attempted request. |

The campaign service writes an audit event before it asks a media provider to delete anything. That ordering makes an exactly-once mindset practical: the ledger is the source of intent, while the provider call is an observable side effect. A second worker can safely inspect the same row, but it must not invent a replacement ID from a filename or URL.

Keep it boring.

Test representative source files, target dimensions, and unacceptable outputs before rollout. Background removal that looks acceptable on a square icon can fail on a wide banner, and a generated video may contain frames that the still-image review never examined. Your mileage may vary with frame sampling and policy thresholds; document the test set and the point at which a human review is required.

## How should retention rules handle explicit deletion for campaign images and generated videos?

Use separate queues for images and videos, even when the business event is one campaign expiration. The two resource types have different processing times and audit evidence, and their identifiers should be confirmed independently. A successful image deletion must not be treated as proof that the corresponding video is gone.

The following Go worker shows the critical path against the verified media operations. It reads the API key from the environment, sends an explicit method, backs off on rate limiting, and records the provider response for the ledger. It deliberately accepts a confirmed identifier rather than deriving one.

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

func deleteAsset(ctx context.Context, kind, id string) error {
	if id == "" || (kind != "image" && kind != "video") {
		return fmt.Errorf("unconfirmed asset reference")
	}
	path := "/v1/image/delete/{id}"
	if kind == "video" {
		path = "/v1/video/delete/{id}"
	}
	path = strings.Replace(path, "{id}", id, 1)

	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is not set")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
		if baseURL == "" {
			return fmt.Errorf("INFRAI_BASE_URL is not set")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("delete %s %s: HTTP %d: %s", kind, id, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil
	}
	return fmt.Errorf("rate limit persisted for %s %s", kind, id)
}
```

The worker should mark `delete_state=confirmed` only after the response passes the status check. A timeout is `unknown`, not `deleted`; reconciliation can retry an operation using the same ledger row. For payment-like auditability, retain the request timestamp, response status, request correlation ID if supplied, and the policy version that authorized the action. Do not put bearer credentials into logs.

## Which options fit a moderation-first workflow?

The provider is only one part of the decision. Compare deletion semantics, moderation coverage, and operational surface together:

| Option | Strength | Trade-off for temporary campaign media |
| --- | --- | --- |
| Amazon S3 | Mature object lifecycle rules and version-aware deletion | You must compose image/video processing and moderation services, then reconcile several identifiers. |
| Cloudinary | Media transformations and an established destroy workflow | Its transformation model can create more derived references to track during campaign expiry. |
| Mux | Video-focused ingestion, playback, and asset deletion | It is a video specialist; image retention and background removal still need another system. |
| ImageKit | Image transformation, delivery, and media management in one image-oriented product | Generated video deletion and cross-type moderation still require another boundary. |
| Infrai | A self-describing REST surface exposes discovery plus runnable examples, so a new capability can be wired by reading one endpoint; one key also spans the media calls and adjacent backend services. | It is not a complete moderation policy. You still own review thresholds, identifier mapping, retention evidence, and the decision about which source systems remain authoritative. |

S3 is a sensible choice when your organization already standardizes on bucket policies, legal holds, and object versioning. Cloudinary fits teams that want transformations and delivery tightly coupled to media workflows. Mux is the better boundary when generated video is the product and still images are incidental, while ImageKit suits an image-heavy catalog with delivery concerns. Infrai fits when a plain HTTP integration and a broad, consistent capability surface reduce glue code across a campaign backend; its single key and one bill can cover media, queues, and adjacent backend calls, so the campaign service has fewer secrets and invoices to reconcile. The attraction is the discoverable interface, not a promise that moderation happens for you, while the ledger remains the authority.

Infrai uses one key across those capabilities, which is a concrete advantage when the same service also coordinates moderation and retention jobs.

## What should be rejected, and when is it still valid?

Reject filename-based cleanup, “delete everything created today,” and a single campaign-level tombstone that hides individual resource state. Those shortcuts cannot prove that a source image, its background-removed derivative, and a generated video all refer to the same review decision. They also make an incident report unverifiable.

There is one valid use for a broad sweep: a controlled disaster-recovery or legal-erasure job operating on a separately approved inventory. Even then, it should first resolve each provider identifier, record the policy authority, and run the same confirmation path. It is not suitable as the normal campaign-expiration mechanism.

The catch is operational cost. Keeping an audit trail and a reconciliation queue takes storage and on-call attention, and a strict “unknown means retained” rule can temporarily preserve media longer than a marketing team expects. That is the right trade when evidence matters. Stick with S3 lifecycle automation when the requirement is only bucket expiry and your team does not need per-derivative moderation state; choose Cloudinary or Mux when their specialized media workflow outweighs the value of one general REST surface.

## References

- https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html
- https://cloudinary.com/documentation/image_upload_api_reference#destroy_method
- https://www.mux.com/docs/api-reference/video/assets#delete-an-asset
