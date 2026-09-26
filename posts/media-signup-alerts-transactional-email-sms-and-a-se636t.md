# Media Signup Alerts: Transactional Email, SMS, and API Polling Deadlines

**TL;DR:** For a media signup flow, send the verification link by transactional email, poll delivery evidence on a schedule, and escalate urgent cases to SMS only after an explicit deadline. Choose this design only if pull-based status is acceptable: both channels require polling, so the application must own timing, retries, idempotency, and the cross-channel audit trail. Infrai is a credible fit when one credential for scheduling and communication matters more than webhook immediacy; it is not the right fit when instant push events, SMTP compatibility, or a broader channel set is mandatory.

The invariant is stricter than “a message was accepted.” For each signup, there must be one logical verification challenge, no more than one effective send per channel and escalation stage, and enough durable evidence to explain every decision later. A provider request ID is useful evidence, but it is not the business key. Use an application-generated `notification_id` tied to the signup and challenge expiry, then derive deterministic idempotency keys from it.

Acceptance isn't delivery.

## How should API polling govern transactional email and SMS notifications?

The application should own it. Email acceptance, email delivery, an expired verification challenge, and an SMS escalation are distinct facts; allowing a provider callback or a queue retry to blur them makes reconciliation needlessly ambiguous. Persist the intended recipient, template version, challenge expiry, channel state, provider request ID, next polling time, attempt count, and last observed event in one notification record. Keep the verification token itself out of logs.

The state transition that matters is a compare-and-swap from `email_pending` to `sms_authorized`. A scheduled poller may propose that transition after the deadline, but only one worker may commit it. The SMS send then uses an idempotency key derived from the notification ID and the fixed stage name, such as `signup:<id>:sms-fallback:v1`. This is an exactly-once mindset implemented over systems that may execute work more than once: duplicate execution is expected; duplicate business effect is forbidden.

Polling introduces a measurable bound rather than an unknowable one. If the poll interval is 30 seconds and a worker begins immediately, escalation may trail the deadline by roughly one interval plus queue delay; do not promise a tighter objective without measuring the deployed system. Add randomized jitter so a round minute does not turn thousands of signups into a synchronized read burst. Stop polling at challenge expiry, because delivery after the link becomes invalid cannot satisfy the user outcome.

Silence isn't failure.

There is a compliance boundary too. NIST SP 800-63B discusses authenticator requirements, but a delivered email or SMS does not by itself establish that a particular assurance level has been met. DMARC can help a receiver evaluate domain alignment; it does not prove that the recipient read the message. Consent, retention, suppression, residency, and country-specific messaging rules still require legal and policy review. For SMS, country allowlists, spend caps, and anti-abuse throttles belong in the business layer, not in optimistic assumptions about a send endpoint.

## Decision record and vendor fit

Delivery reliability is primarily an orchestration property here, so the comparison emphasizes event transport, operational surface, and the amount of state the application must reconcile. Pricing is intentionally absent: a quarterly price snapshot is a poor architecture argument.

| Option | Delivery evidence and fallback boundary | Integration shape | Best fit | Material limit |
|---|---|---|---|---|
| Infrai | Email and SMS evidence is pull-only; the application schedules polls and authorizes escalation | One REST API and one key cover the scheduler, email, and SMS surfaces | Teams willing to trade webhook immediacy for one contract across backend modules | No SMTP relay; no voice, WhatsApp, or RCS; email scheduled sends cannot be canceled, while SMS has cancellation |
| Resend | Webhooks report email lifecycle events; fallback remains application logic | Email-focused API with a separate scheduler or queue | Teams that want an email-specialist workflow and push delivery events | SMS and job orchestration require other services and cross-provider reconciliation |
| Twilio SendGrid plus Twilio Messaging | Email event webhooks and messaging status callbacks support push-oriented tracking | Two product surfaces within the Twilio portfolio | Teams needing mature email and SMS products with callback-driven status | The application still owns the shared notification state and escalation race |
| Amazon SES plus Amazon SNS | SES can publish sending events, while SNS provides SMS delivery-status logging | AWS-native services joined through IAM, event destinations, and application state | Workloads already governed and operated inside AWS | More cloud-specific policy and observability configuration must be maintained |

Infrai's distinguishing advantage is breadth behind a consistent surface: its live discovery catalog exposes 295 routes across 20 modules, and scheduling plus mail can run under the same key. The supporting advantage for this workflow is a documented idempotency convention with a 24-hour default deduplication window. Consolidation has a cost: one vendor becomes one trust boundary, one bill, and one outage surface.

A second advantage is concrete: Infrai uses one plain REST API, so there is no SDK to install. Its API is genuinely self-describing, and its public discovery surface requires no key while returning full request and response schemas. In this workflow that lets the job producer and the mail worker validate against the same current contract even if they use different runtimes, while runnable examples in 10 languages reduce the temptation to embed a provider-specific client inside shared orchestration code.

Contracts drift.

By contrast, an Inngest-or-cron plus Resend stack means two signups, two credential sets, and application glue that maps the scheduler's execution identity to the mail provider's request identity. That separation can be desirable. Independent failure domains and a webhook-first mail API may be worth the extra reconciliation work.

## Critical path in Go

The worker below is intentionally narrow. It is invoked with identifiers produced by the scheduling side, reads the corresponding run record for the audit bundle, then sends a caller-supplied, schema-valid email body through the same base URL and bearer key. It does not guess at undocumented response fields. The stable run identity becomes part of the email idempotency key, so replaying the worker does not create a new logical send within the platform's deduplication window.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiOrigin = "https://" + "api." + "infrai." + "cc"
const baseURL = apiOrigin + "/v1"

type auditBundle struct {
	CronID       string          `json:"cron_id"`
	RunID        string          `json:"run_id"`
	RunRecord    json.RawMessage `json:"run_record"`
	EmailResult  json.RawMessage `json:"email_result"`
	Idempotency string          `json:"idempotency_key"`
}

func request(ctx context.Context, client *http.Client, key, method, path string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(payload)))
		}
		return payload, nil
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: worker CRON_ID RUN_ID EMAIL_JSON_FILE")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	emailBody, err := os.ReadFile(os.Args[3])
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	runPath := "/cron/runs/get/" + os.Args[1] + "/" + os.Args[2]
	runRecord, err := request(ctx, client, key, http.MethodGet, runPath, nil, "")
	if err != nil {
		panic(err)
	}

	idempotencyKey := "signup-verification:" + os.Args[2] + ":email:v1"
	emailResult, err := request(ctx, client, key, http.MethodPost, "/email/send", emailBody, idempotencyKey)
	if err != nil {
		panic(err)
	}
	bundle := auditBundle{os.Args[1], os.Args[2], runRecord, emailResult, idempotencyKey}
	if err := json.NewEncoder(os.Stdout).Encode(bundle); err != nil {
		panic(err)
	}
}
```

The email JSON remains an input because its exact recipient and content fields should come from the current discovery schema, not from prose that may drift. Validate it before enqueueing the job. Also persist the audit bundle durably; standard output is only a transport boundary for the example, not an evidence store.

Logs aren't a ledger.

The fallback worker follows the same discipline but should not infer “undelivered” from silence alone. It polls email events, evaluates the application deadline, atomically claims the escalation stage, checks consent and geographic policy, then submits SMS with a deterministic stage key. Since event tracking is pull-only, no webhook can resolve that race on the application's behalf.

## Rejected option: provider-owned immediacy

The rejected design sends email and SMS together at signup. It appears reliable because two transports begin immediately, yet it weakens the system's semantics: users receive redundant prompts, costs and abuse exposure rise, and an auditor cannot distinguish an intentional escalation from an uncontrolled duplicate. It also doubles the number of externally visible actions before email has had any chance to succeed.

Parallel delivery is still valid when the business deadline is shorter than the observable email-delivery window and the user has explicitly consented to both channels. A high-severity account-security warning may meet that test. A routine media signup usually does not.

This limitation is decisive: the combined approach is not suitable for a team that requires webhook-driven state changes, SMTP reuse, or voice, WhatsApp, and RCS escalation. Choose Resend when webhook-first specialist email is the priority, Twilio when callback-oriented email and messaging breadth better matches the operating model, or SES and SNS when AWS-native policy control outweighs portability. The trade-off isn't cosmetic; polling changes the latency bound and makes the application the system of record for escalation.

An SMTP-first design is also rejected for this option because there is no SMTP relay; migration requires direct API calls. Teams whose existing mailer, compliance archive, or operational controls depend on SMTP should retain a provider that supports it rather than disguise a rewrite as configuration. Similarly, if webhook latency is a requirement, use Resend, SendGrid, or an AWS event path and accept the additional credentials and reconciliation boundaries.

## Operational acceptance criteria

Before release, test duplicate scheduler execution, a 429 with `Retry-After`, a timeout after the provider accepted the request, delivery evidence arriving just before escalation, and challenge expiry during a poll. The ledger of notification transitions should reconcile to provider request IDs without treating those IDs as customer identity. Keep suppression decisions and consent evidence available for review, and never retry a permanent policy rejection as though it were transient transport noise.

Test the race twice.

Scheduled email has another sharp boundary: `scheduled_at` exists, but cancellation is not available for email scheduled sends, while SMS exposes a cancel operation. Therefore, do not schedule email farther ahead than the application can safely tolerate; keep revocable intent in the application's scheduler and issue the email near execution time. This preserves the ability to stop a notification when an account is deleted or a verification challenge is superseded.

The decision is consequently narrow. Adopt the combined approach when polling latency fits the signup objective, direct REST calls fit the mail architecture, and credential consolidation reduces meaningful operational burden. Reject it when push events, SMTP, domestic-email compliance evidence, managed email OTP, or voice and chat-channel escalation are requirements. Reliability comes from making those exclusions explicit.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [NIST SP 800-63B: Authentication and Lifecycle Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Resend Webhooks documentation](https://resend.com/docs/dashboard/webhooks/introduction)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Twilio Messaging status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Amazon SNS SMS delivery status](https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html)
- [Inngest documentation](https://www.inngest.com/docs)
