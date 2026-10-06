# Startup Transactional Email Provider: 4 Controls for Welcome Emails and Receipts

TL;DR: For a logistics system that sends an order receipt after payment settles, choose the email provider whose behavior your application can reconcile, not the one with the most attractive headline price. The decisive design is a durable receipt intent, a stable idempotency key, an immutable attempt log, and provider-independent delivery evidence. Evaluate Postmark, Resend, Brevo, Mailgun, and Amazon SES with identical messages and acceptance criteria; never treat an API acceptance response as proof that the customer received mail.

Payment settlement is authoritative, while email is an asynchronous consequence. Putting the send call inside the settlement request couples a financial transition to a remote system that may time out after accepting the message. A blind retry can produce two receipts; declining to retry can produce none. The receipt therefore belongs in an auditable workflow, even though it does not belong in the payment transaction's critical path.

Four controls make that workflow explainable: one durable intent per business event, one deterministic idempotency key, an append-only attempt history, and a reconciled terminal state.

## How should a startup test a transactional email provider for welcome emails?

An email API sits several steps away from the recipient's mailbox. The application can know that it created an intent, that a provider accepted a request, and later that some provider-originated event was observed. Those are different claims. A transport timeout makes the distinction acute because the sender may not know whether the remote service persisted the request.

Acceptance is not delivery.

Model the receipt as a state machine rather than a Boolean `sent` field. `pending` means settlement created an obligation. `submitted` means an attempt obtained a provider message identifier. `delivered`, `bounced`, and `complained` represent later evidence, subject to the semantics documented by the selected provider. `unknown` is a legitimate operational state when evidence is missing or cannot be authenticated.

The transaction that records settled payment should atomically record an outbox item, or publish through a mechanism with equivalent durability. A worker then claims that item and calls an email adapter. This is an at-least-once execution path, so repetition must be harmless. Exactly-once delivery across a database, a provider, and a mailbox is not a credible primitive; exactly-once business intent is the useful target.

That distinction matters.

## Put business identity ahead of transport identity

A receipt key should derive from stable facts such as `order_id`, settlement version, and notification kind. It should not be a random value generated on every worker attempt. Store it under a unique constraint before sending. If two workers observe the same settlement, one wins the insert and the other finds the existing intent.

The adapter should return evidence without pretending submission equals delivery:

```go
package receipt

import (
    "context"
    "errors"
    "fmt"
)

type Message struct {
    From, To, Subject, Text string
    IdempotencyKey          string
}

type Submission struct {
    Provider, MessageID string
}

type Sender interface {
    Submit(context.Context, Message) (Submission, error)
}

type Journal interface {
    BeginAttempt(context.Context, string) (string, error)
    RecordAccepted(context.Context, string, Submission) error
    RecordUncertain(context.Context, string, error) error
}

func SendReceipt(ctx context.Context, sender Sender, journal Journal,
    orderID string, settlementVersion int, to string) error {
    key := fmt.Sprintf("order/%s/settlement/%d/receipt", orderID, settlementVersion)
    attemptID, err := journal.BeginAttempt(ctx, key)
    if err != nil {
        return err
    }

    result, err := sender.Submit(ctx, Message{
        From: "receipts@example.invalid", To: to,
        Subject: "Your order receipt",
        Text: "Payment settled. Your order is confirmed.",
        IdempotencyKey: key,
    })
    if err != nil {
        if recordErr := journal.RecordUncertain(ctx, attemptID, err); recordErr != nil {
            return errors.Join(err, recordErr)
        }
        return err
    }
    return journal.RecordAccepted(ctx, attemptID, result)
}
```

The awkward branch is the valuable one: a timeout is recorded as uncertain, not immediately classified as a failed send. A retry policy can first seek available evidence or wait for a bounded reconciliation interval. Where a provider documents an idempotency mechanism, pass the same business key. Without one, the local journal still prevents concurrent duplicate intent, although it cannot erase uncertainty after remote acceptance and a lost response. That boundary belongs in the comparison record.

Do not place raw message bodies, access tokens, or unnecessary customer data in the journal. Auditability requires enough metadata to establish transitions and correlate evidence, not an unlimited copy of personal content. Retention, access, and deletion controls need review by the organization's compliance owner; no generic article can establish the applicable legal period for a particular company or jurisdiction.

## Compare evidence, not marketing categories

A fair test gives every candidate the same corpus, recipient domains, sending identity, authentication configuration, and observation window. Postmark, Resend, Brevo, Mailgun, and Amazon SES enter as replaceable adapters. Their objective differences are the recorded results below and the contractual or documented boundaries verified at evaluation time. Ranking them without that evidence would be fiction.

| Candidate | Duplicate-control test | Correlation test | Uncertain-submit test | Governance review |
|---|---|---|---|---|
| Postmark | Same-key replay | Match IDs to events | Record timeout outcome | Current terms and docs |
| Resend | Same-key replay | Match IDs to events | Record timeout outcome | Current terms and docs |
| Brevo | Same-key replay | Match IDs to events | Record timeout outcome | Current terms and docs |
| Mailgun | Same-key replay | Match IDs to events | Record timeout outcome | Current terms and docs |
| Amazon SES | Same-key replay | Match IDs to events | Record timeout outcome | Current terms and docs |

This is a protocol, not a scorecard. Populate it from current primary documentation, signed terms, and repeatable tests because features and contractual boundaries can change. Preserve the date, configuration, message-corpus hash, and raw result references beside each conclusion. A deliverability percentage without a controlled population, a time window, and a definition of delivery is not comparable.

Include success, permanent recipient rejection, temporary failure, malformed event payload, repeated event, out-of-order event, worker crash after submission, and response timeout. Verify event authenticity according to current provider documentation before changing receipt state. Deduplicate by a stable event identifier when one exists, while retaining the original observation for audit. Transition rules must prevent a late `submitted` observation from overwriting a terminal outcome.

Price belongs in the worksheet as total operational cost under the measured workload: sending charges, retained evidence, engineering effort, support burden, and migration cost. It should not be the first filter. A nominally inexpensive path that cannot resolve uncertain submissions transfers cost into duplicate messages, manual investigation, and weak reconciliation.

Measure before ranking.

The limitation of this method is deliberate: it will not produce a universal winner, and its journal, reconciler, and failure tests impose engineering and operational work. A startup with low consequence messages and no need to explain individual outcomes may reasonably choose a simpler integration and accept weaker evidence. For settled-order receipts, that trade-off is harder to justify because notification state must remain explainable beside an authoritative payment record.

## Reconciliation is the daily reliability mechanism

The journal should answer four questions without ephemeral logs: which settlement created the obligation, which attempts followed, which external identifiers were observed, and why the current state is believed. Use append-only attempt and event records, then derive current state. Corrections become new facts rather than destructive edits.

Run a periodic reconciler over old `pending`, `submitted`, and `unknown` records. Age thresholds should come from measured event latency and the operational promise made to customers, not an arbitrary constant copied into code. The reconciler may locate evidence, schedule a controlled retry, or route the case for review. It must never rewrite the payment ledger to make notification state look clean.

Metrics follow the state machine: intent age by state, uncertain submissions, terminal failures, event-verification failures, duplicate suppression, and lag from settlement to acceptance. Alert on sustained backlog or age, not a single transient transport error. Correlation begins with the business key and includes internal attempt ID plus external message ID when available; redact or tokenize email addresses in broad-access telemetry.

SMS fallback is a separate consent and compliance decision, not an automatic retry channel for failed email. In the United States, CTIA publishes messaging interoperability and compliance best practices. Teams considering SMS must review rules applicable to their program and jurisdiction before sending, rather than treating a phone number collected for delivery coordination as blanket permission for another message type.

## Roll out without changing settlement semantics

Start by writing receipt intents and journal entries while the existing path remains authoritative, then compare shadow state with actual outcomes. Next, enable the worker for a bounded cohort, inject timeouts and duplicate events, and verify that each settlement still yields one business intent. Expand only after dashboards and reconciliation queues have an operational owner.

Provider migration becomes a routing change at the adapter boundary, with the old correlation path retained until its outstanding attempts reach terminal states or the documented investigation process closes them. Do not resend every unresolved historical item through the new adapter. Reconcile first.

The decision is compact: select the candidate that produces the strongest reproducible evidence under failure tests, fits the organization's governance constraints, and can be replaced without altering payment settlement. Delivery reliability begins in the ledger-like discipline around the send, not in a price table.

## Sources

References:

- Amazon SES documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- CTIA messaging interoperability and compliance best practices: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
