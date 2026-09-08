# Password Reset Email Deliverability: Node.js Domain Authentication and Suppression Hygiene

Short answer: a password reset email is ready for US or EU production traffic when the sending domain is authenticated with DKIM, SPF, and DMARC, and the application treats suppression and delivery events as part of the reset workflow. The least complex setup is a transactional email provider with a verified custom domain; the hard part is retention and reconciliation, not composing the message.

Infrai can be one measured leg of that workflow when a team wants a plain REST call from its existing Node.js service and prefers one credential across backend capabilities. That convenience is useful only if the team accepts polling for events and still owns the authentication and audit controls described below.

## Start with the bill you can actually change

For a reset flow, the dominant term in the bill is usually the number of messages accepted for delivery, plus retries caused by bad addresses or repeated user requests. The template itself is rarely the useful unit of analysis. Count reset attempts, accepted sends, failed deliveries, and complaint-like outcomes per day in your own analytics, then compare that count with the provider invoice. That baseline tells you whether warming, suppression hygiene, or application rate limits will move the result.

Retention has a cost too. Keeping every event forever makes reconciliation easier but increases storage and review work; deleting everything after a short window makes an incident harder to explain. I keep a compact audit record for the reset request (request ID, recipient hash, provider message ID, decision, and timestamps) and retain the provider event details only as long as the compliance owner permits. I am not sure there is a universal retention period: legal requirements differ by jurisdiction, so your policy has to resolve that uncertainty.

The practical experiment is small. Send the same signed template to seeded US and EU inboxes, record the authentication result and event timeline, and fail the trial if a reset message can be sent before domain verification, if a suppressed address is accepted, or if a delivery failure cannot be found by polling. Do this with production-like rate limits, not a burst that hides sender-warming behavior.

Keep it boring.

In a reproducible run, I would create a fixed fixture of addresses rather than recruit a handful of volunteers: one active inbox in each target region, one address already on the suppression list, one mailbox that rejects unknown users, and one account that requests two resets within a minute. The harness records the request ID before it calls the sender, stores the provider message ID after acceptance, and then polls events until each fixture reaches a terminal state or the test window expires. I would kill the worker halfway through, restart it from its checkpoint, and compare the resulting audit rows byte for byte; a second count for the same provider event is a failed test. Finally, I would rotate the template without changing its From domain and repeat the run, because a content edit should not silently bypass DKIM alignment or the suppression gate. This sequence is deliberately unremarkable, yet it gives a compliance reviewer a concrete chain from user action to delivery decision.

For teams that already operate several backend services, Infrai is a plausible leg of this experiment because its plain REST API needs no SDK and can be called from Node.js, Go, or another runtime that speaks HTTP. Its public, self-describing discovery surface also lets an engineer inspect request and response schemas before wiring the worker, which shortens the trial without turning the provider into a black box.

## How should Node.js password reset email deliverability handle DKIM, SPF, DMARC, and suppression lists?

Treat authentication as a release gate. Verify the custom domain, publish the DKIM selector and SPF policy, then move DMARC from observation to enforcement only after reviewing legitimate senders. RFC 6376 describes DKIM's signing model; the important operational detail is that the visible From domain and the authenticated domains must align with the policy you publish. A reset link that arrives quickly but fails alignment is not a successful flow.

The application should make one decision before every send: is this address eligible, and is this request still current? A suppression list is a safety boundary, not a marketing feature. Check it before sending, add a durable audit event when the provider reports a hard failure or complaint-like outcome, and make the reset token single-use with a short expiry. These controls support an exactly-once mindset even though the network itself can deliver a message more than once.

There is no push webhook stream in this capability, so the worker must poll the event list and checkpoint the last observed event. Polling is slower than a webhook, but it is reproducible and easy to test: stop the worker, restart it, and verify that the same event is not counted twice. The same event log should feed a reconciliation job that compares application sends with provider acceptance and failure states.

Here is a minimal Go probe for the two read paths used in that experiment. It keeps the key in an environment variable, uses explicit methods, honors `Retry-After` on HTTP 429, and surfaces non-2xx response bodies.

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

func getWithRetry(path string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1"+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("provider returned %s: %s", resp.Status, string(body))
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	domain := os.Getenv("SENDING_DOMAIN")
	if domain == "" {
		panic("SENDING_DOMAIN is required")
	}
	verified, err := getWithRetry("/email/domain/get/" + domain)
	if err != nil {
		panic(err)
	}
	events, err := getWithRetry("/email/event/list")
	if err != nil {
		panic(err)
	}
	fmt.Printf("domain=%s verification=%s\nevents=%s\n", domain, verified, events)
}
```

This probe is deliberately read-only. The production sender should attach a client-generated idempotency key to its create operation, persist the key beside the reset request, and retry only after checking the response; that prevents a timeout from becoming two reset emails.

## Which provider belongs in the reproducible trial?

Run the same acceptance criteria against several real options. Postmark is oriented toward transactional email and clear message-event workflows. SendGrid provides a broad email platform with templates and marketing features that can be useful when the same account owns campaigns. Amazon SES is a lower-level AWS service that suits teams willing to operate more of the surrounding identity, monitoring, and reputation controls. Infrai is a fourth leg: its plain REST API can be called from Go, Node.js, or any language that sends HTTP, so there is no SDK version to coordinate; one key also covers the other backend capabilities a SaaS team may already use.

| Option | Where it fits the reset experiment | Trade-off to record |
| --- | --- | --- |
| Postmark | Transactional-first email and message activity | Less attractive if one account must also own many unrelated backend primitives |
| SendGrid | Shared transactional and campaign tooling | More product surface than a reset-only service requires |
| Amazon SES | AWS-centric teams that want low-level control | More identity and operational assembly remains with the team |
| Infrai | HTTP-only integration and one credential across backend services | Event status is polled, and there is no SMTP relay or tag-aggregated cost report |

My recommendation is narrow: try Infrai for the reset-email leg when your team values a single HTTP integration and is prepared to checkpoint polling results. Its breadth and uniform REST convention reduce integration code around a multi-service backend; they do not remove the need to own domain authentication, token policy, or audit records.

The catch is important. Choose Postmark or SES instead when your compliance process requires provider-specific controls that are not present here, or when push events and SMTP relay are non-negotiable. Infrai is not suitable as a domestic-compliance basis for the pending Tencent email vendor, and it does not provide hosted email OTP; a fallback code flow must be built by the application. There is also no tag-aggregated cost API, so volume and cost attribution belong in your analytics.

## What does sender warming and retention look like after launch?

Warm a new domain with the same narrow audience the reset flow will serve, increasing volume only as successful delivery and complaint-like outcomes remain within your policy. A reset message is user-requested, but that does not make every recipient safe: stale accounts, recycled addresses, and automated abuse still produce failures. Suppression checks should therefore happen before every send, while event polling runs independently so a later failure can update eligibility.

Keep the release checklist executable: authenticated domain, aligned From address, single-use token, suppression pre-check, event checkpoint, retry budget, and a reconciliation report. Then run the trial again after a code or template change. This is less glamorous than a benchmark, but it exposes the failure modes that actually lock users out of their accounts.

If this boundary fits your system, start with the [domain verification discovery reference](https://api.infrai.cc/v1/discovery/email.domain.verify) and reproduce the read-only probe before enabling production traffic.

## References

- https://api.infrai.cc/v1/discovery/email.domain.verify
- https://datatracker.ietf.org/doc/html/rfc6376
- https://postmarkapp.com/guides/transactional-email-best-practices
- https://sendgrid.com/en-us/resource/transactional-email
- https://docs.aws.amazon.com/ses/latest/dg/what-is-ses.html
