# Node.js SaaS User Reminder Email and SMS Delivery Under Provider Rate Limits

**Short answer:** For high-volume Node.js SaaS user reminders, use cron to create durable, idempotent work and use rate-limited workers to dispatch email and SMS from that work.

Choose a durable delivery ledger and a bounded dispatcher before choosing a cron expression. That split makes late ticks, repeated runs, and redeliveries accounting problems with records, rather than unexplained notification incidents.

The important unit is one logical delivery, identified before any provider call. A reminder occurrence can be due once while its command is observed more than once. Those statements coexist in distributed systems, and a design that treats them as contradictory will eventually either lose a send after a process stops or create a duplicate after recovery.

Small batches matter.

This note takes a ledger-oriented view because reminders often sit beside payment, account, and compliance flows. Delivery acceptance is useful evidence, but it is not proof of human receipt, and a queue acknowledgement is only proof that a consumer has taken responsibility under the broker's contract. Retain the distinction in the data model; it keeps operational reports and customer-support explanations honest.

## How should a Node.js SaaS schedule user reminders across email and SMS?

Begin with the schedule of record in a database. Store the reminder occurrence, its due timestamp in UTC, the user-local rule that produced it when that rule matters, a payload version, and a stable delivery key for each requested channel. A periodic coordinator reads a bounded half-open interval such as `[watermark, now)`, claims eligible rows, and inserts outbox rows in the same database transaction. The unique constraint belongs on the delivery key, commonly a derivation of reminder identity, occurrence time, and channel. A second coordinator may then repeat the scan without making a second logical delivery.

The coordinator must be allowed to run late, run twice, or stop after committing a batch. Cron is a wake-up mechanism, not the calendar and not the delivery engine. Scheduled workflows have timing and concurrency semantics that must be understood before they are used as a trigger; GitHub documents that scheduled workflows can be delayed during periods of high load. A durable watermark and idempotent insert are what turn that operational variance into a recoverable condition.

An outbox relay publishes only committed rows to a queue. It records enough state to reconcile the handoff: delivery key, publication attempt, broker message identifier if available, timestamps, and the result. Do not publish inside an uncommitted database transaction and assume the two systems share an atomic outcome. They do not. The outbox is the explicit record of that boundary.

For daily volume, use a row-count and elapsed-time cap on every scan. A long outage should create many observable chunks, each with a clear watermark transition, rather than one transaction that competes with normal application traffic and leaves operators guessing how far it got. It is also worth deciding the timezone rule before implementation: calculate a user's intended local send time, persist the resulting occurrence, and then dispatch against UTC. Daylight-saving changes are a product policy that needs a testable definition, not an incidental property of a library call.

## The delivery ledger defines what a retry means

A worker consumes a command, acquires a short lease for its delivery key, performs the external request outside the database transaction, records the attempt result, and acknowledges only after the durable record is updated. RabbitMQ's consumer acknowledgement model permits redelivery when a message remains unacknowledged, which is precisely why acknowledgement cannot be the primary audit record. The durable attempt history is.

An exactly-once business result is the goal; at-least-once command processing is the condition to design for. When a provider accepts a request and the worker stops before recording that acceptance, the next worker faces an ambiguous outcome. Send a stable idempotency key whenever the downstream API offers one. When it does not, preserve the ambiguity and reconcile against a provider acceptance record if the contract provides one. Blindly retrying an unknown external side effect is not a correctness strategy.

```go
type Command struct {
	DeliveryKey string
	Channel     string
	Payload     []byte
}

type Message interface {
	Command() Command
	Ack() error
	Retry() error
}

type Ledger interface {
	Accepted(key string) (bool, error)
	RecordAcceptance(key, providerID string) error
}

type Sender interface {
	Send(channel string, payload []byte, idempotencyKey string) (string, error)
}

func Handle(m Message, ledger Ledger, sender Sender) error {
	command := m.Command()
	accepted, err := ledger.Accepted(command.DeliveryKey)
	if err != nil {
		return err
	}
	if accepted {
		return m.Ack()
	}

	providerID, err := sender.Send(command.Channel, command.Payload, command.DeliveryKey)
	if err != nil {
		return m.Retry()
	}
	if err := ledger.RecordAcceptance(command.DeliveryKey, providerID); err != nil {
		return err
	}
	return m.Ack()
}
```

The code is deliberately incomplete as an application interface: the production implementation needs a conditional lease acquisition and a state transition tied to that lease token. The ordering is the point. No acknowledgement occurs before the result is durable, and a duplicate command finds the delivery identity before it attempts a new side effect.

Keep operational metadata separate from message content where possible. An audit trail usually needs the delivery key, decision time, channel, template version, result class, and correlation identifier; it rarely needs indefinite retention of the full message. Compliance obligations vary by jurisdiction, message class, consent basis, and contract, so a retention period should be a policy decision reviewed with security and legal rather than copied from a queue default. I'm not sure a universal retention duration would be meaningful without those facts.

## Rate limits belong at dispatch, not in the calendar

Provider limits constrain outbound work; they do not alter which reminders were due. Partition commands by channel and provider account, apply a token bucket or equivalent permit allocator at dispatch, and retain the original delivery key through retries. Email and SMS should not consume a shared pool unless that reflects a real external quota. A global worker-concurrency cap protects the fleet, while per-account permits prevent one tenant from monopolizing capacity. The implementation should make a permit a durable decision boundary when a retry can span processes: record which account and channel were selected, the policy version that made the choice, and the time the worker became eligible to try again. This permits a later reconciliation to distinguish work that was due but intentionally waiting for quota from work that was never published, leased by a stopped worker, or classified incorrectly. It also avoids an easy but costly error in multi-tenant systems, where a single global retry delay makes compliant traffic wait behind a tenant whose destination is constrained. A scheduler can keep producing due occurrences while the dispatcher holds them in a ready state, provided the oldest-ready age, estimated drain time, and per-channel permit budget remain visible. When drain time exceeds the service objective, the response may be to raise a provider-approved quota, add workers only where permits exist, or explicitly defer lower-priority message classes; increasing concurrency without a permit merely moves pressure to the external boundary. The calendar should remain deterministic throughout. Each outcome, including a deliberate deferment, deserves an audit record tied to the same delivery key so that a later customer question does not force an operator to reconstruct intent from ephemeral logs.

The failure mode is often hidden by average metrics. A queue can look short while its oldest command violates the delivery objective, or it can look large while all commands are fresh and permitted capacity is rising. Measure due-but-not-enqueued rows, oldest outbox age, broker-ready age, active leases, attempts by result, dispatch latency, and permits available. Reconcile counts by time bucket across eligible occurrences, unique outbox records, published commands, accepted requests, and terminal states. Temporary differences are normal; every difference should have a named state and an owner.

Retry policy needs the same discipline. Classify outcomes into accepted, retryable, and terminal according to the downstream contract, send retryable commands through a delayed path with bounded exponential backoff and jitter, and preserve attempts rather than overwriting them. A dead-letter path is quarantine, not disposal. An authorized replay creates a new audit event while retaining the same logical delivery key, so an operator can answer what was attempted, why it paused, and who resumed it.

Don't scale from CPU alone. Scale on the oldest unprocessed work and the time a lease has been held, because those describe whether the system is meeting the actual timing obligation. Test the boundary by stopping a coordinator after an outbox commit, running two coordinators on one window, ending a worker after the external request, exhausting one channel's permits, and replaying a quarantined command. The assertions should be concrete: one logical identity, ordered attempt history, no acknowledgement before an outcome record, and a reconcilable explanation for every open item.

## Which execution pattern fits the delivery risk?

The comparison is about the failure budget, not a fashionable component. A direct scheduled sender can be adequate when the batch is small, delay and duplication have low consequence, and the maximum execution time is proven. As volume or consequence rises, moving delivery away from the scheduler creates an explicit backpressure boundary and a place to retain evidence.

| Pattern | Durable claim | Backpressure signal | Duplicate control | Trade-off |
|---|---|---|---|---|
| Scheduled process sends inline | Job-owned state | Execution duration | Usually local process state | Simple, but provider latency holds the schedule open |
| Coordinator writes a database work table | Row claim | Oldest eligible row | Unique delivery key | Fewer components, but indexing and polling cadence need attention |
| Transactional outbox plus queue | Database record and broker handoff | Outbox and message age | Unique key and attempt ledger | Strong recovery story, with more states to operate |
| Per-occurrence timer service | Timer identity | Timer backlog | Scheduler-dependent identity | Useful for sparse future events, but migration of many timers is complex |

The catch is operational ownership. A queue and outbox do not erase complexity; they name it, make it measurable, and require runbooks for leases, replay authority, retention, and reconciliation. Stick with a bounded database work table when the team cannot responsibly operate a broker and measured load fits the database's locking and index budgets. A transactional outbox and queue are not suitable when the added operational surface exceeds the consequence of a late or duplicate low-risk reminder.

## How can a team roll out scheduled reminder delivery without changing delivery semantics?

Start by calculating due occurrences and writing audit records while suppressing external dispatch. Compare the new eligible set with the current sender by delivery key and time bucket. After differences have explanations, enable a deterministic small cohort, set permits below the documented provider allowance, and expand by channel while monitoring age and reconciliation deltas.

Deploy schema changes additively so old and new workers can read the transition state. A rollback halts new claims, allows known leases to resolve according to their expiry policy, and preserves queued commands for reconciliation; deleting the evidence makes a later replay decision less defensible. Before broad traffic, rehearse coordinator restart during enqueue, worker termination around external acceptance, and sustained throttling of one channel while the other continues. The rollout is complete when the audit trail, not a dashboard impression, can account for every delivery key.

## References

- https://www.rabbitmq.com/docs/confirms
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
