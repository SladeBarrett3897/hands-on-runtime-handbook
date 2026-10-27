# Marketplace Webinar Results: An API Approach with 3 Freshness Rules for Live Polls

The decision rule for a live poll and webinar results API is narrow: use a durable event log plus replayable snapshots for marketplace notifications, and treat presence as an expiring observation rather than a fact. A connected socket can receive a message; it cannot prove that a buyer is still looking at the offer. That distinction is the difference between an honest live poll and a misleading green dot.

For a marketplace with a live poll, the system should publish an aggregate version, preserve the vote or state transition behind that version, and make every reconnect converge to the same answer. A Node.js API can perform the edge work, but its event history and idempotency boundary still need to be explicit. The audience may be measured in thousands, but fan-out is not the hard part. The hard part is deciding what the system is allowed to claim after a timeout, duplicate delivery, or a process restart.

That is the contract.

## What API approach keeps live poll results honest in a webinar?

I use three invariants for this class of backend. First, a state transition is accepted once per idempotency key, even if the client retries after a lost response. Second, a notification is a projection of committed state, never the write itself. Third, presence has a freshness window and an explicit unknown state. “Online” is a recent observation, not a property stored forever on a user row.

The poll can therefore expose a sequence number, a tally, and a presence estimate without pretending that transport delivery is exactly once. Exactly-once processing is a useful mindset for the ledger and the poll command; the network boundary still needs idempotent consumers and replay. An audit trail should retain the accepted command, actor, subject, sequence, and decision reason. That record makes reconciliation possible when the displayed tally and the accepted commands disagree.

This is also where compliance limits matter. A presence signal should not retain more personal detail than the product needs, and an audit record should define retention and access rules before it is populated. Standards describe transport behavior; they do not grant permission to retain behavioral data indefinitely.

| Option | What it establishes | Failure boundary | Suitable decision |
| --- | --- | --- | --- |
| Polling | A client can fetch a current snapshot | The interval creates staleness and load | Recovery, backfill, and low-frequency views |
| Server-sent events or WebSocket delivery | A server can push changes over a live connection | Disconnects, duplicates, and missed sequence ranges | Low-latency notification fan-out |
| WebRTC data channels | Peers can exchange data after a negotiated connection | Signaling, peer reachability, and topology become application concerns | Direct media or peer-oriented exchange, not the source of truth |

The table is an architecture boundary, not a ranking. A transport can be fast and still be a poor place to store truth.

The trade-off is deliberate: replayable state consumes storage and adds a projection step, while a direct broadcast is easier to start but cannot explain a missed result. For a short-lived poll with no audit requirement, a simpler snapshot may be sufficient. A marketplace that must reconcile accepted actions needs the longer path.

The operational consequence is easy to miss. Suppose the aggregate is version 418 when a viewer disconnects, then the projection worker restarts while two accepted votes are waiting in the outbox. A reconnect that receives version 417 must not overwrite its local state, and a reconnect that asks for version 418 must either receive that snapshot or be told that the cursor has expired and receive a fresh one. The audit trail still records both accepted commands independently of which notification arrived first. That sequence gives support engineers something they can reconcile: command identity, committed sequence, projection version, and the freshness state observed at the edge. Without those distinctions, a dashboard may look current while the system has no defensible explanation for why two viewers saw different answers.

## How should a marketplace distinguish presence from delivery?

A notification service needs two related but separate streams. The first is authoritative state: a committed vote, an offer change, or a membership transition. The second is an observation: a heartbeat, a subscribe event, or a recent acknowledgement. The first belongs in a durable history. The second expires. Conflating them creates false confidence during the exact incidents when operators need accurate answers.

For example, a viewer can submit a poll response, receive a successful write, and lose the notification connection before seeing the updated aggregate. On reconnect, the client should ask for a snapshot at or after its last sequence. The service returns the current projection and a cursor. It does not replay a comforting but obsolete “you are online” message.

Here is the critical path in Go. The storage and transport interfaces are intentionally generic; they are the seams at which a production implementation should add a database transaction, an outbox, a broker, and a connection registry.

```go
package presence

import (
	"context"
	"time"
)

type Command struct {
	Key      string
	ActorID  string
	PollID   string
	ChoiceID string
}

type Event struct {
	ID       string
	Sequence int64
	PollID   string
	ActorID  string
	ChoiceID string
}

type Store interface {
	AcceptOnce(context.Context, Command) (Event, bool, error)
	Snapshot(context.Context, string) (Snapshot, error)
}

type Snapshot struct {
	PollID  string
	Version int64
	Counts  map[string]int64
}

type Publisher interface {
	Publish(context.Context, string, Snapshot) error
}

func Submit(ctx context.Context, store Store, publisher Publisher, cmd Command) error {
	event, accepted, err := store.AcceptOnce(ctx, cmd)
	if err != nil {
		return err
	}
	if !accepted {
		return nil
	}

	snapshot, err := store.Snapshot(ctx, event.PollID)
	if err != nil {
		return err
	}
	return publisher.Publish(ctx, event.PollID, snapshot)
}

func PresenceFresh(lastSeen time.Time, now time.Time, window time.Duration) bool {
	return !lastSeen.IsZero() && now.Sub(lastSeen) <= window
}
```

The `AcceptOnce` boundary is the important line. If a process publishes twice after a retry, consumers still receive the same versioned projection and can discard an older version. If the process accepts a command but fails before publishing, the durable outbox must publish it later; putting the publish inside an HTTP handler without a recovery path creates a silent gap.

## Where does the design fail, and how does it recover?

A live connection can disappear between two writes. A broker can redeliver. A browser can resume with a cursor that the server has already compacted. These are normal boundaries, so the protocol needs explicit answers.

The client stores the latest applied version. A newer snapshot replaces an older one; an older snapshot is ignored. If the requested cursor is unavailable, the server sends a fresh snapshot and a new cursor. This is convergence, not a promise that every transient notification was seen. For a poll tally, convergence is usually the useful guarantee. For a payment ledger, the accepted command and its audit record remain the stronger guarantee.

Presence uses a different rule. Set `last_seen` on a successful observation, apply a short product-defined freshness window, and expose `unknown` when the clock or connection state cannot support a stronger claim. Do not infer “present” from a pending delivery queue. A queued message says something about the server, not the viewer. Clock skew deserves a test too: compare server-owned timestamps where possible, and make the expiry policy deterministic in tests.

The rejected option is a client-only mesh in which viewers relay poll changes to one another. WebRTC 1.0 defines browser APIs for peer connections and data channels, but it does not turn peers into an authoritative tally store, presence registry, or audit log. The approach has a valid use case for direct peer exchange after signaling and for media-oriented collaboration; it is the wrong source of truth for a marketplace decision that must reconcile after disconnection. Its limitation is structural: peers can disappear together, while the marketplace still needs one recoverable account of the result.

## What should be tested before thousands of viewers arrive?

The useful load test is not a single connection count. It is a sequence of state transitions under retries and churn. Generate duplicate command keys, delayed acknowledgements, reconnects with old cursors, and presence observations that expire just before a broadcast. Assert invariants, not only response latency:

- one idempotency key produces at most one accepted event;
- snapshot versions never move backward at a client;
- a reconnecting client reaches the current tally;
- expired presence is not reported as current;
- the audit record identifies the accepted command and sequence.

I would also separate the delivery metrics. Track accepted commands, projection lag, publish failures, reconnect recovery time, duplicate deliveries, and the age distribution of `last_seen`. A single “messages per second” number hides the failure that matters: a system can push quickly while serving a stale or contradictory tally.

Deployment should preserve the replay path during rolling replacement. Drain connections, leave the event history available, and verify that a new instance can consume the same sequence range without applying a command twice. The operational runbook should say how to rebuild a projection, how to quarantine a malformed event, and how an auditor can connect a displayed version to its accepted inputs. Those are ordinary backend duties, not optional polish.

## Decision

For marketplace live polls and in-app notifications, choose a server-authoritative projection with versioned snapshots, idempotent writes, replay on reconnect, and expiring presence observations. Use a push transport to reduce notification delay, but keep transport state outside the source of truth. If the product cannot state what happens after a duplicate, a missed cursor, or an expired heartbeat, it is not ready to claim presence accuracy.

That rule remains useful as the audience grows because it scales the contract before it scales the connection count. The user sees a current answer, the operator can explain its origin, and the audit trail can distinguish an accepted action from a message that merely attempted to travel.

## References

- https://www.w3.org/TR/webrtc/
- https://www.w3.org/TR/eventsource/
- https://www.rfc-editor.org/rfc/rfc9110
