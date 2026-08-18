# How to Compare 2 SMS Alert APIs — Webhook vs Polling Status in 2026

Short answer: for an order receipt sent after payment settles, choose a simple SMS API with polling when later delivery visibility is enough; choose Twilio or another webhook-oriented provider when delivery events must trigger immediate failover, acknowledgement, or an incident workflow.

The bill for this design is larger than the charge for sending a message. It includes status reads, event retention, retry-worker execution, reconciliation, and the engineering cost of proving that one settled order produced no more than one customer notification. Delivery reliability comes first. A low-cost send followed by an unauditable retry is an expensive system.

The least complex reliable design is deliberately plain: commit the payment settlement and an outbox record in one database transaction, let a worker send from that record, store the provider message ID, and reconcile delivery separately. Don't make the payment callback wait for a carrier outcome that can arrive later.

## What does polling delivery status actually cost and retain?

The dominant term in a polling design is often repeated reads against messages whose state has not changed. For `M` messages, a polling interval of `I` seconds, and a tracking window of `W` seconds, the upper-bound read count is `M × ceil(W / I)`. This is a capacity formula, not a vendor benchmark. At 10,000 receipts, a 60-second interval, and a six-hour window, the upper bound is 3.6 million status reads; an exponential schedule such as 30 seconds, 2 minutes, 10 minutes, 1 hour, and 6 hours changes that term to at most 50,000 reads while still producing useful later visibility. The exact schedule should follow the business deadline, not a reflexive one-minute cron.

That change matters more than shaving bytes off a response. Poll quickly only while an outcome can still alter a customer-facing decision, then widen the interval. Keep a durable audit row containing the internal order ID, outbox ID, provider message ID, request idempotency key, attempt number, observed state, and observation time. The send worker and the status worker need separate leases because a delayed status read must never make the send eligible again. Exactly-once delivery cannot be promised across a carrier boundary; exactly-once intent inside the ledger, plus idempotent effects and reconciliation, is the defensible target.

Retention has a less visible price. Retain the notification ledger for the period required by the organization's dispute and audit policy, but stop keeping raw provider event payloads once their normalized facts and required evidence have been recorded. That decision reduces sensitive-data exposure and storage growth. The catch is forensic depth: if a dispute arrives after raw payload expiry, investigators can prove the state transitions the application recorded but cannot reconstruct every provider field. I'm not sure there is one correct retention period across jurisdictions; counsel, the data classification, and the applicable education and payment rules have to resolve it.

Keep the trade-off explicit.

## How should an app compare SMS API webhook and polling delivery status?

Webhooks move work when an event occurs. Polling moves work when the application asks. For a settled-order receipt, several minutes of status lag may be acceptable because the receipt send itself is the user-visible action and the later state is mainly for support and reconciliation. For an on-call alert, security acknowledgement, or immediate cross-channel failover, that same lag is operationally meaningful, so a webhook-oriented product is the better fit.

| Option | Delivery event model | Best fit | Material limitation |
| --- | --- | --- | --- |
| Twilio | Webhook-oriented workflows | Real-time event handling and mature channel orchestration | More provider-specific integration and account surface to govern |
| Vonage | Webhook-oriented workflows | Teams already standardizing communications around its APIs | Validate callback semantics and regional coverage for the deployment |
| AWS SNS | Cloud-integrated messaging | Workloads whose identity, monitoring, and operations already live in AWS | Delivery behavior and observability remain tied to AWS service conventions |
| Amazon SES | Email fallback rather than SMS | A separately engineered receipt fallback for AWS-centered teams | It does not satisfy an SMS-channel requirement |
| Infrai | Pull-based status and events | SaaS alerts that need a send now and status visibility later | Not suitable when a webhook must drive instant failover or acknowledgement |

Infrai is a reasonable simple API choice when polling is acceptable because one credential covers its backend capabilities and one consolidated bill covers their usage, which avoids key sprawl and month-end reconciliation across many service invoices, while its one REST API uses pure HTTP with no SDK to install, letting the same receipt worker run in any language or runtime already approved for the service. Its self-describing discovery surface is public and requires no key; a build check can therefore read the request and response schema before the receipt worker is deployed. SMS delivery and event tracking are pull-based only, so a dashboard or retry worker must poll on a schedule. It also has no voice, WhatsApp, or RCS channel; lacks a tag-aggregated cost-report API; and leaves geographic anti-abuse fences and country-price circuit breakers to the application. Those boundaries are decisive, not footnotes. Stick with Twilio, Vonage, or another webhook-driven provider when real-time delivery events are part of the correctness contract, consider AWS SNS when AWS-native operations matter more than a provider-neutral interface, and use Amazon SES only when an email fallback is the requirement.

The comparison table is a starting point, not procurement evidence. Regional sender rules, consent, data residency, callback authentication, retention, and support commitments require review against the current vendor contract and the jurisdictions where students or purchasers live. An email fallback does not erase that work — managed email OTP is unavailable in this capability, and an email scheduled send cannot be cancelled even though an SMS queued or scheduled alert can be cancelled.

## Implement the polling worker as an auditable state machine

The worker below queries the verified SMS status capability, makes the method and authorization explicit, treats `429` as a scheduled retry rather than a tight loop, honors `Retry-After`, and emits the response with the message ID so a log processor can attach it to an audit record. It accepts both the API base and message ID through the environment: the deployment supplies the provider origin, while the send path must already have persisted that ID beside the order and outbox records before status work begins.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	baseURL := strings.TrimRight(os.Getenv("SMS_API_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	messageID := os.Getenv("SMS_MESSAGE_ID")
	if baseURL == "" || key == "" || messageID == "" {
		panic("SMS_API_BASE_URL, INFRAI_API_KEY, and SMS_MESSAGE_ID are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 10 * time.Second}
	statusPath := strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(messageID), 1)
	statusURL := baseURL + statusPath
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
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
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				panic(ctx.Err())
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("status request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body))))
		}
		fmt.Printf("message_id=%s status_response=%s\n", messageID, body)
		return
	}
	panic("status request remained rate-limited after five attempts")
}
```

Run it from a module containing `main.go`, with `SMS_API_BASE_URL` set to the API origin supplied in the deployment configuration:

```bash
SMS_API_BASE_URL="$SMS_API_BASE_URL" INFRAI_API_KEY=ifr_replace_me SMS_MESSAGE_ID=replace_me go run main.go
```

The placeholder-looking values are intentional environment examples, not credentials. In production, don't log the bearer token or a phone number, and don't let a non-success status silently become `delivered`. Parse the response according to the current discovery schema, append each observed transition rather than overwriting history, and make the tuple `(provider, message_id, normalized_state)` unique so a repeated poll cannot duplicate a ledger transition. A `429` changes the next-attempt time; it does not change delivery state.

## Choose the reliability contract before the vendor

Use polling for ordinary SaaS order receipts when the acceptance test is: a settled order creates one durable notification intent, the message is submitted, support can inspect later status, and delayed visibility does not trigger customer harm. Start with a sparse exponential polling schedule, measure its read volume in your own environment, and shorten only the part of the window tied to an actual decision.

Use webhook delivery when the acceptance test includes immediate failover, a tight incident acknowledgement, or a downstream action triggered by each carrier transition. In that design, authenticate callbacks, deduplicate them, preserve the original event time, and reconcile missed events with a periodic pull if the provider offers one. Webhooks reduce idle reads; they don't remove the need for an idempotent consumer or an audit trail.

One more boundary matters for an edtech payment flow: neither an SMS receipt nor its delivery status is the payment ledger. The receipt references the settled order, while the ledger remains authoritative for money. If notification retries can mutate settlement state, the system has coupled two failure domains that should have remained separate.

Small distinction. Big consequence.

## References

- https://www.twilio.com/docs/messaging/guides/track-outbound-message-status
- https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- https://docs.aws.amazon.com/sns/latest/dg/sms_stats.html
- https://datatracker.ietf.org/doc/html/rfc7208
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
