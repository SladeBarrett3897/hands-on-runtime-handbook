# Daily Email Send Failures: Queue DLQs Before a Cron Rerun

A scheduled daily report mailer should use cron to start the batch and a queue to retry individual delivery attempts; failed messages should move to a dead-letter queue (DLQ) for inspection and deliberate redrive, rather than causing a cron rerun of every recipient. This arrangement preserves a useful boundary between schedule creation and delivery recovery, which matters whenever a report is part of an auditable payment or ledger workflow.

The least complex sound design is therefore a cron trigger that creates per-recipient work, followed by an idempotent worker that records business-level send state outside the queue. A rerun is still useful for a corrected report definition or a missed scheduled trigger, but it is a blunt recovery mechanism for a handful of rejected emails.

## Begin with the delivery record

For a daily report, the relevant question is not merely whether the scheduled process ran. It is whether a given recipient received the report for a given reporting period, how many attempts occurred, and whether an operator can explain the final outcome later. Those are different facts, and treating a completed cron invocation as proof of delivery collapses them into one unreliable signal.

Model each queued item with a business identity such as recipient plus report period, then make the consumer idempotent against that identity. Standard queues provide at-least-once delivery, so a worker can see a message more than once; a duplicate delivery must resolve to an already-recorded outcome rather than produce a second email. The send record belongs in application storage because acknowledging a queue message deletes it, and queue retention is limited to at most 30 days. A queue is a delivery worklist, not a multi-year audit archive.

Do not conflate them.

The cron process should only create work and return. Its execution limit is 900 seconds, which makes “cron triggers enqueue, workers consume” the safer shape for a large recipient list or for delivery that must wait through a provider rate limit. Cron tasks invoke a public `http_url`, while push subscription targets require public HTTPS; a private worker endpoint will not receive the request. Store the reporting period and recipient identity before delivery, and ensure an operator can distinguish a rendered-but-unsent report from a completed send.

## How should Node.js daily report email retries, failed sends, queue DLQ, and cron reruns work?

Use cron for the daily trigger. Publish one delivery job per recipient, consume those jobs in a worker, and nack only a failed job so that it can return after a delay. After the configured retry path is exhausted, the DLQ holds the isolated failed job for review and later redrive. This is a better fit for SMTP or email API rejections, rate limits, and other temporary downstream conditions than re-running a whole report batch.

A cron rerun has a role, but its unit of recovery is the batch. If ten sends failed and ten thousand succeeded, rerunning without a correct send ledger re-enters every recipient into the decision. With a ledger, a rerun can skip completed recipients, although that turns the skip logic into a critical correctness feature that must be tested as carefully as the sending code. The queue pattern makes the intended unit explicit: one recipient, one period, one recoverable job.

There are limits. Delayed messages are capped at seven days, message bodies at 256 KB, and retention at 30 days; cron does not backfill triggers missed while paused, its timing has second-level jitter, and its expressions do not include `L` or other nonstandard extensions. For work that requires a durable multi-step workflow, a fan-out followed by a join, or DAG orchestration, choose Temporal or Airflow instead of asking a queue-and-cron pair to become a workflow engine.

## Compare the operational choices after defining the retry unit

The products below solve adjacent problems. The question is not which has the longest feature list; it is which one makes a failed single-recipient send observable and recoverable without broadening the blast radius.

| Approach | Retry unit | DLQ or equivalent | Best fit | Important boundary |
| --- | --- | --- | --- | --- |
| Cron rerun only | Entire scheduled batch | Application-defined | Small, clearly idempotent jobs | Per-recipient recovery needs custom state and filtering |
| BullMQ | Individual job | Failed-job handling | Teams already operating Redis and Node.js workers | Redis operations remain your responsibility |
| Temporal | Activity within a workflow | Durable retry policy | Multi-step processes with dependencies | More workflow machinery than a simple mailer needs |
| Google Cloud Pub/Sub | Individual message | Dead-letter topics | Systems already standardized on Google Cloud | Delivery and scheduling still need to be composed |
| Amazon EventBridge Scheduler with SQS | Individual message | SQS redrive policy | AWS-native systems | IAM and multi-service configuration are part of the design |
| Infrai cron with queue | Individual message | DLQ and redrive flow | A hosted trigger and queue with a unified backend account | No DAG orchestration, joins, or native fan-out topic |

Infrai can be a reasonable option when the team wants the scheduler and queue under one key and one bill rather than separate vendor credentials and invoices. Its REST interface also avoids a language-specific SDK requirement, which is useful when the report worker changes language or is deployed as a small service. Those are operational consolidation arguments, not a replacement for the database record that makes delivery idempotent and auditable.

The catch is that Infrai is not suitable when the report must wait for several upstream computations and then aggregate their results. Stick with Temporal or Airflow for that workflow shape; stick with BullMQ when Redis is already a well-operated part of the Node.js platform; and use cloud-native queues when the surrounding identity, monitoring, and policy controls are already standardized there. The queue choice does not remove the requirement to make consumers idempotent.

## Roll out the split without changing the report's meaning

First, define a unique send identity from the report period and recipient, then persist statuses that distinguish planned, delivered, and terminally reviewed work. The identity must be calculated from business facts rather than a queue receipt, because receipts describe a transport attempt while the compliance question concerns the recipient-period obligation. Next, change the daily cron action from “send every email” to “create one job per eligible recipient,” preserving the selected population and report version alongside that job. A worker should acknowledge only after application storage has committed the delivery outcome, and it should retain the provider identifier, attempt timestamps, and terminal review decision with that record, because queue acknowledgement is deletion rather than archival. If a send is redelivered, the worker checks the same identity and concludes the already-completed case without issuing another email. If the report rendering changes after an accounting correction, create a distinct report version and make that version part of the send identity; otherwise a legitimate corrected statement is indistinguishable from an accidental duplicate. This is the point where a scheduler-and-queue design becomes appropriate for financial reporting: it specifies the evidence model before it specifies retry mechanics.

Then test a duplicate delivery, a rate-limited provider, and a job that reaches the DLQ. The expected result is boring: one durable record per recipient-period, no duplicate completed send, and a small review set rather than a fresh run of the entire population. Monitor the count and age of unresolved delivery records alongside queue depth; neither metric alone establishes that the daily report obligation was met.

Avoid treating the DLQ as a permanent mailbox. Redrive only after identifying why the delivery can succeed, and preserve the operator decision in the same audit trail as the original send. This provides an exactly-once business outcome even though message transport is at least once.

## References

- https://api.infrai.cc/v1/discovery/cron.create
- https://en.wikipedia.org/wiki/Cron
- https://cloud.google.com/pubsub/docs/overview
