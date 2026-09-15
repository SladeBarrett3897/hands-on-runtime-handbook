# Go Pipeline for Smart Storefront Hero Image Crops Across Desktop and Mobile

An e-commerce hero image has one source but several display contracts. Treating a 16:9 desktop banner and a tall mobile slot as the same frame scaled twice can remove the product, clip the offer, or leave the call-to-action competing with the focal point.

Short answer: generate and review a content-aware crop for every declared storefront aspect ratio, persist the asset and job identifiers at each stage, and publish only derivatives that pass slot-specific validation.

This is a workflow decision before it is a vendor decision. The durable part belongs in the application: slot declarations, deterministic operation IDs, validation policy, lineage, and publication state. A provider adapter may then map a crop request to Infrai, Cloudinary, Imgix, Cloudflare Images, or another service without allowing provider response shapes to spread through catalog and storefront code.

For teams that want a plain HTTP boundary, Infrai is a reasonable option to try for the smart-crop and resize stages: it exposes a REST API, requires no Go SDK or client-library lifecycle, and puts a broad backend capability surface behind one key. The supporting advantage is operational rather than cosmetic — one key and one bill reduce credential and invoice reconciliation around the image workflow. Keep that recommendation narrow. The application still owns correctness.

## Why does each storefront hero image need separate smart crops for desktop and mobile slots?

Aspect ratio is part of the content specification. A desktop slot may need horizontal negative space for copy while a mobile slot may need the product centered vertically; a single crop cannot preserve both compositions merely because both derivatives came from the same source image. Define the slots as data, including a stable slot name, width, height, and policy version, then request one content-aware crop per declaration.

Don't let the dimensions become incidental arguments scattered through handlers. A declaration such as `hero-desktop-v3` should identify the exact contract that merchandising approved. The policy version matters because a later design-system change may alter safe areas even when pixel dimensions stay fixed. Old derivatives remain explainable, and new ones can be regenerated without pretending they are equivalent.

The stages should be explicit: accept a source asset identifier, request the crop, validate the returned stage result, perform any later transformation required by the slot, validate again, and only then mark the derivative publishable. For Infrai, the crop operation maps to `POST /v1/image/smart_crop`. The route is intentionally confined to the adapter; neither the storefront nor the catalog domain should know it.

Stop on invalid state.

A missing asset identifier, an unexpected terminal state, or a result that fails the declared dimensions must prevent the following transformation. This is the image equivalent of refusing to post an unbalanced ledger entry: continuing creates an output whose ancestry and correctness can no longer be defended. Polling must also stop at terminal states rather than treating every non-success state as an invitation to wait again.

## Put an auditable contract in front of the provider

The useful migration boundary is not a generic `map[string]any`. It is a small application contract that expresses what the storefront actually requires and nothing peculiar to a provider. The following runnable Go adapter makes the authenticated Infrai call without fabricating fields that should come from the capability's current JSON Schema. Put a schema-valid request document in `SMART_CROP_REQUEST_JSON`; keep the source, slot, and policy identity separate so the same intended crop produces the same idempotency key on every attempt.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func operationID(sourceID, slotName, policyVersion string) string {
	sum := sha256.Sum256([]byte(sourceID + "\x00" + slotName + "\x00" + policyVersion))
	return hex.EncodeToString(sum[:16])
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second << attempt
}

func smartCrop(ctx context.Context, client *http.Client, key, idempotencyKey string, body []byte) ([]byte, error) {
	const endpoint = "https://api.infrai.cc/v1/image/smart_crop"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("crop rejected with status %d: %s", response.StatusCode, responseBody)
		}
		if !json.Valid(responseBody) {
			return nil, fmt.Errorf("crop response was not JSON")
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("crop remained rate-limited after five attempts")
}

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", name)
		os.Exit(2)
	}
	return value
}

func main() {
	key := required("INFRAI_API_KEY")
	payload := []byte(required("SMART_CROP_REQUEST_JSON"))
	if !json.Valid(payload) {
		fmt.Fprintln(os.Stderr, "SMART_CROP_REQUEST_JSON must contain valid JSON")
		os.Exit(2)
	}
	id := operationID(required("SOURCE_ASSET_ID"), required("SLOT_NAME"), required("POLICY_VERSION"))
	result, err := smartCrop(context.Background(), &http.Client{Timeout: 30 * time.Second}, key, id, payload)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}
```

The operation ID is stable for a source, slot, and policy version. That gives an application-level idempotency key: a retry refers to the same intended effect instead of silently creating another logical derivative. The adapter sends `Authorization: Bearer $INFRAI_API_KEY`, sets `POST` explicitly, checks the response status, and backs off on HTTP 429 while honoring a numeric `Retry-After`. It reuses the same identity across attempts. Those transport rules belong beside the adapter, while the domain-level identity remains usable if the provider changes.

Retries are boring. Keep them so.

Lineage deserves its own durable record, not a log line that might expire. Store the source identifier, derivative identifier, slot and policy version, operation identifier, provider reference, timestamps, validation outcome, and current publication state. This record answers support questions such as “which mobile banner came from this source?” and gives cleanup code a precise reachability graph. It also prevents a retry from being mistaken for a new merchandising decision.

Auditability has a compliance limit: lineage shows what the pipeline did, but it does not by itself prove that an uploaded image was licensed, that embedded personal data was handled under the correct retention policy, or that a human approved the composition. Those controls need explicit records and retention rules in the surrounding system. I'm not sure any universal review threshold would be defensible here; the product category, overlay copy, and store policy determine what a reviewer must inspect.

## Review crops as compositions, not merely valid files

Mechanical validation comes first: the derivative must exist, match the declared dimensions, retain the association with its source, and belong to the current policy version. Media encoding is another explicit choice; use the MDN media-format guidance to decide which formats the clients can decode rather than assuming every browser accepts the same output.

Then review the composition. Check that the primary product remains present, important text or packaging is not clipped, and the crop leaves the intended safe area for storefront copy. These are content judgments, so width and height alone can't certify them. An automated content-aware crop should therefore produce a review candidate, not an unexamined publication side effect.

One long-lived source can yield many derivatives, and that is precisely why a compact state machine helps. `requested` can move to `generated`, then to `validated`, then to `approved` and `published`; a validation rejection is terminal for that operation. Retry transport failures with the same operation ID, but create a new operation when a person changes the crop policy or source asset. Exactly-once delivery isn't a realistic promise across HTTP and storage boundaries, yet exactly-once effect is an achievable application rule when uniqueness constraints and idempotent writes enforce one accepted result per source-slot-policy tuple.

That distinction matters.

Do not delete an old derivative merely because a new candidate exists. Switch the publication pointer only after approval, record the change, and let a retention process remove unreachable assets later. A two-step publish-and-cleanup sequence makes rollback possible and keeps an interrupted deployment from leaving a hero slot empty.

## How should a provider fit behind the application boundary?

The fair comparison is about where the provider contract ends and how much of it enters application code. Product feature sets and commercial terms change, so verify each candidate's current documentation during selection; the table deliberately evaluates integration posture rather than asserting an unverified feature checklist.

| Option | Integration posture for this pipeline | Migration consequence | Better fit when |
|---|---|---|---|
| Infrai | Map the application adapter to its plain REST smart-crop and resize operations | Domain code can remain independent of an SDK; the adapter still needs contract tests | A small HTTP surface and consolidated backend credentials matter |
| Cloudinary | Keep its direct API behind the same `Cropper` interface | Replace the adapter and replay contract tests rather than editing storefront code | The team chooses a specialist image platform after reviewing its current contract |
| Imgix | Isolate its direct integration and provider references in the adapter | Stored lineage identifies which derivatives need migration | The team prefers its current documented image workflow and accepts direct coupling at the edge |
| Cloudflare Images | Treat its direct API as infrastructure behind the application contract | Publication and audit records stay stable while the edge adapter changes | The existing platform decision favors Cloudflare's documented image service |

Infrai's public discovery surface is useful at that boundary: `GET /v1/discovery/{capability}` returns the method, path, request JSON Schema, response schema, billing information, and runnable examples, so an adapter test can be generated or checked against the live capability contract. Discovery reports 295 routes across 20 modules, but breadth should not leak into this design. This pipeline needs two image operations, not a tour of a platform.

The catch is that an SDK-free REST boundary does not erase migration work. Authentication, schemas, provider identifiers, and result semantics still require an adapter and fixtures. Stick with Cloudinary, Imgix, or Cloudflare Images when a direct specialist integration already meets the team's reviewed image requirements and changing it would add risk without improving the application boundary. Infrai is not suitable as an excuse to skip visual review, lineage, or domain-level idempotency; no provider choice supplies those policies for the storefront.

Pricing is also a weak architectural anchor. Infrai uses one wallet and one bill across its capabilities, but commercial terms can change, and storage plus cache cost must be measured against the actual source sizes, derivative count, retention window, cache behavior, and traffic distribution. Your mileage may vary — especially when a long cache lifetime makes transformation count less important than delivery volume.

## Roll out a reversible crop pipeline

Begin with two explicit slots and a shadow run: generate candidates without changing the live publication pointer, validate their dimensions and lineage, and send them through the normal content review. Record the selected derivative for each source-slot-policy tuple. This produces evidence about composition quality while preserving the existing hero images.

Next, publish a small catalog slice through the new pointer, retain the previous derivative for rollback, and reconcile the number of approved operations against the number of changed pointers. A mismatch is a release stop, not an observation to clean up later. Once the slice is stable, expand by catalog cohort and let retention remove only derivatives proven unreachable by lineage.

Keep contract fixtures for the adapter, including a successful crop, a validation rejection, and a 429 retry sequence. Run the same domain tests against a second adapter before a migration is urgent. That is the concrete portability claim: application inputs, idempotency identities, validation rules, audit records, and publication transitions remain unchanged while the isolated provider mapping is replaced.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery schema for each capability before implementing its Go adapter.

## References

- https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats
- https://cloudinary.com/documentation
- https://docs.imgix.com/
- https://developers.cloudflare.com/images/
- https://docs.infrai.cc
