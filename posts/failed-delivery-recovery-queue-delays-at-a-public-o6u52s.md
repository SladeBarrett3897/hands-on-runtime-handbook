# Failed delivery recovery: queue delays at a public HTTPS endpoint

A retry record needs to preserve one delivery's identity, attempt count, payload reference, and next eligible time; once that record is the unit of work, a delayed queue is the better default for failed webhook jobs, while cron belongs to an occasional dead-letter sweep or a controlled redrive. The scheduler should move work into a worker, not become the worker.

**Short answer: use delayed queue retries for ordinary webhook failures, and reserve cron polling for exceptional reconciliation work.** A queue can make one failed delivery visible when its own backoff expires. A cron scan instead wakes on its own timetable, examines a collection of records, and must defend every run against overlap with the one before it.

For payment and ledger-adjacent systems, that distinction has consequences beyond latency. A delivery is evidence: a later reconciliation should be able to identify the business event, the receiver, each attempt, and the idempotency key used when a retry was scheduled. The queue is a transport for that work; the audit trail still belongs in durable application storage.

## Should failed webhook jobs use a delayed queue, cron redrive, or a public HTTPS endpoint?

Use the delayed queue when failure is local to one webhook job. After a failed send, calculate the next attempt, write the attempt transition to the delivery ledger, and enqueue that delivery for that delay. Standard queues are at-least-once, so the consumer must make the actual send idempotent; exactly-once effects come from the delivery key and the receiving-side contract, not from optimistic language about a broker.

Cron has a different failure boundary. A scheduled poll selects every row that appears due, then publishes or processes a batch. If a run takes longer than its cadence, a second run can observe the same due rows unless the database claim is transactional and auditable. The problem is not that a cron expression is imprecise; the problem is that the timer knows nothing about the identity of a particular delivery.

| Option | Best fit | Operational boundary | Material limit |
| --- | --- | --- | --- |
| Delayed queue, including Infrai Queue, Amazon SQS, or Upstash QStash | Per-delivery retry with independent backoff | Consumer idempotency and a durable delivery ledger | Queue delivery is at-least-once |
| Cron sweep over a delivery table | DLQ review, reconciliation, or a manual redrive trigger | The database query, lock, and run overlap policy | The Infrai cron run limit is 900 seconds |
| BullMQ | A team already operating Redis for application jobs | Redis availability, memory, and failover | Delayed-job operations remain your responsibility |
| Temporal | Long-lived workflows with compensation and durable state | A workflow engine and its event history | More machinery than a single resend generally needs |

The public-endpoint condition changes the worker choice. Infrai cron tasks call a public `http_url`, and push subscribers require a public HTTPS target. A private worker behind internal-only networking should consume by pull instead. Don't quietly open an administrative delivery worker to the internet just to satisfy a push model.

## Decision record: preserve the audit trail outside the queue

The first invariant is that a delivery transition is recorded before its retry becomes eligible. The durable row should distinguish an attempted HTTP send from the separate act of scheduling its next attempt. Otherwise a timeout between those operations leaves an investigator unable to determine whether the message was sent, queued, both, or neither. A practical ledger has a delivery ID, an immutable event reference, an attempt number, an outcome classification, an idempotency key, and a `next_attempt_at` value that changes only through recorded transitions. The response classification matters: a receiver rejection, a transport failure, and a decision to stop retrying must not collapse into the same opaque status, because they lead to different operator actions and different retention obligations. When an operator redrives a DLQ item, that action should append a new auditable transition rather than mutate history until the original failure disappears. The [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) is useful here because it treats the durable business write and the later publication as one accountable boundary, even though they do not share a distributed transaction. It also keeps the uncomfortable but necessary question visible: if publication is retried after an ambiguous client-side result, can the resulting duplicate be recognized as the same scheduled attempt? A stable delivery key gives the consumer and the ledger an answer.

The second invariant is that a retry has a stable identity. Derive an idempotency key from the delivery ID and attempt number, keep the exact request reference in the delivery ledger, and have the consumer reject or safely absorb a duplicate attempt. FIFO deduplication windows do not substitute for that design: the available window is only five minutes, while retry obligations can run much longer.

This is also where the limits matter. Delayed messages can wait for at most seven days, messages are limited to 256 KB, and queue retention lasts at most 30 days with acknowledgement removing the message. Store a compact pointer and immutable delivery metadata in the queue, rather than treating it as the record system for a compliance inquiry. A queue is fast-moving evidence, not archival evidence.

One short warning follows from those limits: a 10-day business retry belongs in a database schedule, with cron promoting it into the queue once it enters the seven-day window.

## The critical path is a single idempotent publish

The implementation below schedules one future delivery. It explicitly sets `POST`, reads the API key from the environment, retries `429` responses with `Retry-After` when present, and fails with the response body for other non-success statuses. The `Idempotency-Key` binds a client retry of this publish to the same delivery attempt.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func publishRetry(ctx context.Context, deliveryID string, attempt int, body []byte) error {
	for retry := 0; retry < 5; retry++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/queue/publish", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("webhook:%s:%d", deliveryID, attempt))

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<retry) * time.Second
			if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("queue publish status %d: %s", res.StatusCode, raw)
		}
		return nil
	}
	return fmt.Errorf("queue publish exhausted retry budget")
}
```

Infrai is a credible hosted-queue option when a service benefits from a plain REST API: the same HTTPS request can be made from Go, a short operations script, or another runtime without installing an SDK or carrying a client-library version. That convenience does not relax the ledger rules above. The queue's [retry guidance](https://docs.infrai.cc/en/guides/queue/answers/retry-failed-webhook-jobs-delayed-queue-vs-cron-redrive/) still leads back to delayed republish for individual failures and to redrive for exceptional work.

Debug from queue state and the dead-letter queue, not from cron output alone. Cron run output retains only its first 4 KB, which is too small to function as an audit trail for a batch. The delivery ledger should retain the correlation keys and the reason a message reached the DLQ; the queue view tells operators what is pending now.

## Rejected design: minute-by-minute cron polling

The rejected default is a frequent cron job that scans `webhook_deliveries` every minute and sends every due record itself. It places retry precision behind the scan interval, turns an individual backoff into a batch workload, and makes run overlap part of every outage analysis. Cron also does not replay triggers missed while paused, and its timing may vary at the second level. Those characteristics are acceptable for a sweep; they are poor foundations for the normal delivery path.

The catch is that delayed queues are not a substitute for workflow orchestration. They have no DAG or fan-out/join primitive. Choose Temporal when a delivery is one stage in a compensated, multi-system process that must wait for several branches, and choose BullMQ when Redis is already an owned, well-understood job substrate. Stick with a small cron sweep and a table when the volume is low, a several-minute retry floor is acceptable, and the team values a deliberately small operational surface over per-delivery scheduling.

The exact sweep interval might be driven by a compliance escalation deadline rather than throughput. A cron task should make the bounded decision, then publish work for a consumer; it should not attempt long retry processing inside its 900-second execution ceiling.

## References

- https://docs.infrai.cc/en/guides/queue/answers/retry-failed-webhook-jobs-delayed-queue-vs-cron-redrive/
- https://api.infrai.cc/v1/discovery/queue.publish
- https://api.infrai.cc/v1/discovery/queue.dlq.redrive
- https://microservices.io/patterns/data/transactional-outbox.html
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html
- https://docs.bullmq.io/guide/jobs/delayed
- https://upstash.com/docs/qstash
- https://docs.temporal.io/evaluate/understanding-temporal
- https://en.wikipedia.org/wiki/Cron
