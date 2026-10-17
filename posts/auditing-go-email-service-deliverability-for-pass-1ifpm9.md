# Auditing Go Email Service Deliverability for Password Reset and Welcome (Fintech Attachments)

The decisive trade-off is between a convenient send API and evidence that a financial report reached the mail system without being silently retried, duplicated, or sent after an address became ineligible. **Short answer:** choose an HTTP email service only after a controlled trial proves domain authentication, account-level suppression, bounce and complaint events, attachment handling, and stable idempotency behavior. Password resets and welcome messages can share the transport, but they should not share the same queue, retry policy, or failure budget as generated report attachments. No universal service is best; the best fit is the one whose observed event trail can be reconciled against your own immutable send ledger.

## Which email service protects password reset and welcome deliverability?

An accepted request establishes that the provider received a submission. It does not establish inbox placement, recipient acceptance, or human reading. A report can still bounce, be suppressed, or produce a complaint event later, while a password-reset link may expire before a delayed retry becomes useful. Treating the initial response as delivery collapses several distinct states into one optimistic boolean, which is unacceptable when support, security, and audit teams later ask what happened.

Keep an internal state machine. The minimum useful progression is `created -> accepted -> delivered`, with terminal or review states for `bounced`, `complained`, `suppressed`, and `expired`. Provider events advance that state machine; they do not overwrite history. Store the raw event, normalized event type, provider message identifier, report identifier, recipient digest, and receipt time. Preserve contradictions too, because an out-of-order webhook is evidence rather than bad data to discard.

Exactly once is the goal at the business boundary, not a promise that a network will deliver one HTTP request. A timeout after submission creates an ambiguous outcome: retrying may duplicate the message, while abandoning it may lose the report. The application therefore needs a stable operation key derived from the report version, recipient, and message purpose, plus an outbox record committed with the report-ready transition. Retries reuse that key and consult the send ledger before creating another external submission.

Small distinction, large consequence.

Ambiguity wins.

## Separate identity, purpose, and failure budgets

A dedicated sending domain matters because authentication and reputation attach to identities that receivers can evaluate. It is not a substitute for operational separation. Password resets are security-sensitive and short-lived; welcome mail is expected but rarely urgent; a generated financial report may be large, confidential, and legally significant to the recipient. Put each purpose in its own queue and template version stream, then decide whether subdomains or accounts should also be separated after measuring reputation and access-control needs.

The sending service must support the domain controls your mail policy requires, but selection evidence should come from DNS verification and received-message headers in the trial, not a marketing checklist. The same principle applies to suppressions. A global suppression can prevent repeated harm to a known bad address, yet an unexplained suppression can also hide a security message. Record why an address became suppressed, the originating event, the scope, and any reviewed removal. Never bypass a complaint suppression merely to make a queue look healthy.

For a fintech attachment, generate the file before enqueueing the email, hash its bytes, record its media type and size, and bind those values to a report version. The mail worker should read an immutable object and verify the hash before submission. If the report changes, create a new version and operation key. Do not mutate the attachment behind an already-audited send record.

A practical evaluation matrix follows from those constraints:

| Evidence to collect | Trial method | Reject when |
|---|---|---|
| Domain authentication | Inspect DNS and received headers | Identity cannot be verified end to end |
| Suppression semantics | Send to controlled invalid and suppressed recipients | Cause, scope, or removal history is unavailable |
| Bounce and complaint events | Exercise test paths and replay signed callbacks | Events cannot be deduplicated or correlated |
| API ambiguity | Force a client timeout around submission | A stable operation cannot be reconciled safely |
| Attachment integrity | Compare the received file hash and metadata | Bytes or filename change without an attributable transform |
| Data controls | Review retention, access, deletion, and regional terms | Compliance boundaries cannot be evidenced |

This is also why feature counts are weak decision material. Three services may all expose an HTTP API, yet the consequential difference is how cleanly their acceptance identifiers, asynchronous events, suppression records, and retention controls map into your audit model. If that mapping requires manual inference, the integration is already carrying reconciliation debt.

## Make retries auditable in Go

The transport adapter should be deliberately narrow. It accepts an immutable command and returns an acceptance identifier; webhook ingestion owns subsequent delivery state. The ledger claims the operation before the network call, so two workers cannot intentionally submit it at once. After an ambiguous timeout, the operation remains pending reconciliation rather than being marked failed and immediately resent.

```go
package mail

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
)

type Command struct {
    OperationKey string
    Recipient    string
    Template     string
    Attachment  []byte
}

type Receipt struct {
    ProviderMessageID string
}

type Sender interface {
    Send(context.Context, Command) (Receipt, error)
}

type Ledger interface {
    Claim(context.Context, string, string) (bool, error)
    MarkAccepted(context.Context, string, string) error
    MarkAmbiguous(context.Context, string) error
}

func Submit(ctx context.Context, ledger Ledger, sender Sender, cmd Command) error {
    sum := sha256.Sum256(cmd.Attachment)
    digest := hex.EncodeToString(sum[:])

    claimed, err := ledger.Claim(ctx, cmd.OperationKey, digest)
    if err != nil {
        return err
    }
    if !claimed {
        return nil // Another worker already owns this business operation.
    }

    receipt, err := sender.Send(ctx, cmd)
    if err != nil {
        // A transport error can follow remote acceptance; reconcile before retrying.
        if markErr := ledger.MarkAmbiguous(ctx, cmd.OperationKey); markErr != nil {
            return errors.Join(err, markErr)
        }
        return err
    }
    return ledger.MarkAccepted(ctx, cmd.OperationKey, receipt.ProviderMessageID)
}
```

The example does not claim that a database transaction can encompass an external mail API. It makes the gap explicit. In production, the claim and outbox state belong in the same transaction as the report publication, and the adapter needs request timeouts, bounded retry scheduling, credential rotation, and redacted logging. Recipient addresses and attachment contents should not appear in ordinary application logs.

Webhook processing needs equivalent discipline. Verify authenticity according to the chosen service's documented mechanism, persist the unmodified payload before normalization, deduplicate on a stable event identifier when available, and make state transitions monotonic except through an explicit review path. A later `delivered` event must not erase an earlier complaint record. Retention periods and access to raw events should be set with legal and security owners because compliance limits vary by jurisdiction and by the report's contents.

## Test the evidence chain, not the happy path

Start with a corpus that represents the real workload: password reset, welcome, and a generated report attachment at several expected sizes. Use controlled recipient domains and documented test facilities where available. Record API latency separately from event latency and inbox observation; mixing them produces a reassuring average that answers no operational question.

Then inject failures. Kill the worker after remote acceptance but before the local update. Deliver the same callback twice. Deliver callbacks out of order. Rotate a webhook secret during traffic. Suppress a test recipient, attempt another send, and verify that both the refusal and its reason enter the audit trail. These tests expose the boundary conditions that determine delivery reliability without inventing a production incident or relying on a vendor's aggregate claims.

Observe ratios by message purpose and domain cohort: submission attempts, acceptances, deliveries, hard bounces, complaints, suppressions, ambiguous submissions, and age of the oldest unreconciled operation. Alert on deviations from your own reviewed baseline rather than copying a universal threshold. Password-reset expiry should also be visible, since delivery after the token expires is operational failure even if the receiving server accepted the message.

Cost belongs in the review, but it is secondary to failure handling. Model request volume, attachment transfer and storage, event retention, support effort, and the engineering cost of reconciliation. A nominal send price cannot compensate for missing evidence during an audit.

This boundary matters.

## Roll out without losing the audit trail

Run the candidate adapter in shadow mode first: build commands, validate templates and attachments, and stop before external submission. Next, send only to controlled internal recipients and reconcile every acceptance against callbacks and received headers. Canary one low-risk message purpose before moving password resets or regulated reports, and keep the old adapter available until the new path has cleared a full retry and retention cycle defined by your own policy.

Migration is complete only when dashboards, suppression review, incident procedures, data-retention controls, and rollback have been exercised. **Choose on recoverable ambiguity and durable evidence.** Delivery claims are useful; a ledger that explains each message is defensible.

## Sources

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
