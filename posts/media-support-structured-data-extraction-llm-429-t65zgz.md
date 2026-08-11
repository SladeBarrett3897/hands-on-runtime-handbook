# Media Support: Structured Data Extraction, LLM 429 Rate Limits, Queue Backoff, US/EU

Short answer: treat structured JSON extraction as a metered, auditable queue for each tenant, not as a promise that every support ticket will produce one immediate model call. A bounded worker pool, an idempotency key, explicit 429 backoff, and a cost ledger make the failure visible; batch processing is appropriate for old tickets, while interactive tickets need a separately budgeted path. US and EU traffic also need separately documented data flows, because a region label alone is not a compliance decision.

For a media company, support triage has an awkward shape. A short question about a failed playback session may be cheap to process, while a long transcript or attachment-heavy complaint consumes a different amount of inference capacity. If the platform reports only total spend, the team cannot tell whether one tenant is noisy, whether a retry storm is inflating usage, or whether the queue is unfairly delaying a smaller customer. The number to put on the dashboard is not only dollars: it is admitted work, estimated units, actual units, retries, queue age, and terminal outcomes, all grouped by tenant and region. That gives an operator enough evidence to answer a customer question without guessing from an invoice produced later, and it also gives finance a reconciliation path when the provider's usage report and the application's attempt ledger do not line up.

That is a ledger problem.

## What enters the tenant ledger?

The first design artifact should be a tenant cost ledger, not a provider comparison. At admission, store an estimate with the tenant ID, ticket ID, schema version, region, and source-size band. After the attempt, store the usage fields actually returned by the selected API, or mark the amount as pending when the API supplies no usable usage detail. Reconciliation then compares accepted jobs, attempts, terminal results, and billed records. Those are different counts. Treating them as one count is how a support system loses the explanation for a bill.

A tiny support ticket can still have a large accounting footprint when it is retried. Don't charge a tenant by successful responses alone.

The ledger is also the fairness mechanism. A scheduler can reserve capacity for each tenant, reject an estimate that exceeds policy, and expose a reason that support operations can understand. A global cap protects the account; a per-tenant cap prevents one backlog from consuming it. This is a governance boundary before it is an optimization.

## How should structured data extraction handle an LLM 429 rate limit?

It should preserve the ticket's logical identity, admit work against a tenant-aware budget, and defer the attempt when the service says capacity is unavailable. HTTP 429 means the caller must respect the rate-limit boundary; it does not mean the ticket can be silently dropped or duplicated. The worker should honor a valid `Retry-After` value when present, otherwise use exponential backoff with jitter and a finite retry budget.

The invariants matter more than the retry formula:

- One ticket revision and one schema version produce one logical extraction ID.
- Every model call has a separate attempt ID and an append-only audit event.
- A tenant has a visible admission budget, queue age, and estimated cost.
- Only validated JSON reaches the classification store.
- The final business-side write is unique on the extraction ID, so repeated execution cannot create repeated effects.

This is an exactly-once mindset applied at the effect boundary. Network delivery and worker execution are normally at least once; the committed triage result can still be one logical result if the database owns the uniqueness rule. A timeout after remote acceptance is uncertainty, not proof that a second call is safe. I would record that uncertainty instead of hiding it in a generic error counter.

## Four records for one ticket

The HTTP edge validates the ticket envelope and returns a job ID without holding the connection open through inference. The durable queue owns delay and redelivery. A tenant-aware scheduler chooses the next eligible job, while a fixed worker pool owns the concurrency cap. The model adapter classifies status codes, parses the response, and validates the JSON schema. The database commits the result and its accounting event in one transaction.

| Design choice | Good fit | Boundary to document |
|---|---|---|
| Synchronous extraction | A small, interactive ticket that needs a quick triage result | The request path still needs a timeout and must not own long backoff sleeps |
| Durable asynchronous queue | Normal support traffic with tenant fairness and replay needs | Queue age and dead-letter handling become operational obligations |
| Batch submission | Large historical imports where per-ticket latency is not interactive | Submission, status polling, partial results, and reconciliation need durable identities |
| Per-tenant budget | Customers need understandable spend and service allocation | A shared global limit still has to protect the whole account |
| Separate US/EU processing lanes | Contracts or policy require a documented regional boundary | Storage, logs, failover, deletion, and processors must be reviewed together |

No option removes schema validation or auditability. Batch is not a cheaper retry loop, and a regional queue is not automatically a compliant queue.

## A 429 is a state transition

The reader's question names Node.js, but the requested implementation constraint for this note is Go. The language changes; the state machine does not. The adapter below deliberately accepts a generic call result, so it does not invent a provider route, model identifier, or request field. In production, the durable queue invokes this policy and the commit transaction enforces the unique extraction ID.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type Job struct {
	ID     string
	Tenant string
	Region string
	Text   string
}

type AttemptResult struct {
	StatusCode  int
	RetryAfter  string
	JSON        []byte
	UsageUnits  int64
}

type Attempt func(context.Context, Job) (AttemptResult, error)

func retryDelay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	base := time.Second * time.Duration(1<<attempt)
	return base + time.Duration(rand.Int63n(int64(base/2)+1))
}

func extract(ctx context.Context, job Job, call Attempt) ([]byte, int64, error) {
	for attempt := 0; attempt < 6; attempt++ {
		result, err := call(ctx, job)
		if err != nil {
			return nil, 0, err
		}
		if result.StatusCode == 429 {
			timer := time.NewTimer(retryDelay(attempt, result.RetryAfter))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, 0, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if result.StatusCode < 200 || result.StatusCode >= 300 {
			return nil, 0, fmt.Errorf("request status %d", result.StatusCode)
		}
		if len(result.JSON) == 0 {
			return nil, 0, errors.New("empty extraction result")
		}
		return result.JSON, result.UsageUnits, nil
	}
	return nil, 0, errors.New("rate-limit retry budget exhausted")
}

func worker(ctx context.Context, jobs <-chan Job, call Attempt, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		result, units, err := extract(ctx, job, call)
		// Persist job.ID, job.Tenant, job.Region, units, and err in an audit event.
		fmt.Printf("job=%s tenant=%s bytes=%d units=%d err=%v\n", job.ID, job.Tenant, len(result), units, err)
	}
}

func main() {
	jobs := make(chan Job, 8)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go worker(context.Background(), jobs, func(_ context.Context, job Job) (AttemptResult, error) {
			return AttemptResult{StatusCode: 200, JSON: []byte(`{"ticket_id":"` + job.ID + `"}`), UsageUnits: 1}, nil
		}, &wg)
	}
	jobs <- Job{ID: "ticket-1042-v3", Tenant: "tenant-media-7", Region: "EU", Text: "Playback failed after an ad break"}
	close(jobs)
	wg.Wait()
}
```

The two workers are an example cap, not a discovered service limit. I would set the real value from the applicable contract, observed queue age, estimated input size, and reserved retry capacity. I write `429` into the attempt event before scheduling a delay, because an operator needs to distinguish rate limiting from a malformed response. I'm not sure a request-count limit alone is sufficient for any particular model because token limits and usage accounting differ; current provider documentation and the approved regional design must resolve that uncertainty. Your mileage may vary. The invariant is bounded, observable concurrency.

The comment in the example points at the missing production boundary: logging is not persistence. The audit event should include a stable extraction ID, attempt number, status class, delay decision, estimated cost, actual usage when available, and schema-validation result. It should exclude raw customer text from general retry logs. A database transaction should insert the validated result and a committed-cost record together, with a uniqueness constraint on the extraction ID.

## Backlogs run on a different clock

Interactive work and backlog work should share identity and accounting vocabulary, but they should not share the same latency promise. A queue worker can retry an interactive ticket within a bounded window; a batch job can submit many independent inputs, persist the returned batch identity, poll terminal status on a schedule, and reconcile each output separately. Polling must be repeatable. Submission must be idempotent from the application's perspective, even when the remote response is lost.

For US and EU lanes, draw the complete path before selecting a deployment shape: ingress, queue storage, processing, transient buffers, logs, result storage, failover, deletion, and exports. A service may expose a regional endpoint while an operational dependency, backup, or log processor crosses the boundary. Technical configuration cannot establish the legal conclusion. Processor terms, retention behavior, transfer controls, and audit requirements belong with the responsible security and legal owners.

The tenant ledger helps here too. Every cost event carries the region and data-classification decision that governed admission. If a failover would change that decision, the scheduler should reject the move for review or route it through an explicitly approved policy; it should not quietly optimize for queue age.

## The shortcut that loses evidence

I would reject unbounded parallel calls from each incoming HTTP request, with a retry loop sleeping inside the handler. It couples customer traffic to inference capacity, lets retries intensify a shared limit, keeps connections occupied, and gives operators no durable view of queue age or tenant fairness. A process-local semaphore is better than nothing, but it cannot coordinate several service instances or make a completed effect unique.

That pattern is acceptable only for a deliberately small internal tool whose work can be discarded, whose spend is immaterial, and whose data policy permits direct processing. It is not suitable for a multi-tenant media support queue where cost visibility, replay, and audit records are part of the job. Stick with a durable queue and a database-owned commit boundary when a ticket can affect customer communication, entitlement handling, or a financial review.

The decision is therefore operational rather than vendor-shaped: measure admitted work per tenant, cap concurrent attempts, make 429 handling finite and visible, and reconcile every logical extraction. A model response is an input to triage. It is not an authorization to mutate a ledger or send a customer-facing decision without validation and policy checks.

## Further reading

- https://docs.cohere.com/docs/rerank-overview
- https://github.com/openai/whisper
