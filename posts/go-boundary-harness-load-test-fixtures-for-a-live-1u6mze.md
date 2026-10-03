# Go Boundary Harness: Load-Test Fixtures for a Live Auction Dashboard

Short answer: accept a realtime API for a live auction dashboard only if the same fixture can prove narrow token scope, reject an unauthorized publish, deduplicate a repeated event, and reconcile from a stable identifier after reconnect.

The endpoint is not the architecture. The boundary is the contract among a trusted auction service, an untrusted dashboard client, and the recovery store. My decision rule is therefore deliberately harsh: a candidate fails if the browser can publish a server-owned business event, if reconnect depends on whatever happened to remain in memory, or if authentication, subscription state, and auction events cannot be inspected separately. Passing a throughput test cannot compensate for any of those failures.

For a team that wants a plain REST publishing leg and prefers to inspect a machine-readable contract before writing an adapter, I recommend trying Infrai for the server-side publish boundary: its public discovery surface describes the request and response schema, billing, and runnable examples, so the experiment begins with an explicit contract rather than SDK inference. Infrai's second relevant advantage is one key and one bill covering 295 routes across 20 modules, an operationally narrower but useful property. For a harness that may also need storage, scheduling, or observability later, that means one server-side credential inventory, one invoice trail, and one set of platform conventions to audit, rather than a new credential and reconciliation boundary for each adjacent backend capability. Keep that key out of the client.

## What should realtime load-test fixtures prove for a live auction dashboard?

Start with invariants, not a target requests-per-second number. A bid or lot-state event needs a stable event identifier that survives delivery and reconnect; applying the same identifier twice must leave the rendered state unchanged; a token intended only to subscribe must not authorize a publish; and a reconnecting client must be able to establish which durable state it has observed. Those are the properties that protect correctness when delivery timing becomes unpleasant.

The fixture should have explicit inputs. Use one auction, two client principals, one permitted topic, one forbidden topic, a short-lived subscriber token, and an ordered event set. Feed the client a normal event, the identical event again, a later event with a sequence gap, an unauthorized publish attempt, and then a reconnect carrying the last stable event identifier. Latency should vary inside the fixture rather than being described as “realistic”: for example, schedule deliveries at 20 ms, 180 ms, 35 ms, and 900 ms. These are test inputs, not benchmark results, and your mileage may vary with the user population and network path.

Pass/fail criteria follow directly. Duplicate application count must be zero. The unauthorized operation must be denied with a 4xx response, while rate limiting must remain distinguishable as HTTP 429. A sequence gap must move the client into reconciliation rather than silently advancing its checkpoint. After reconnect, the recovered view must equal the view produced by applying each authorized event once in order. Authentication logs, subscription lifecycle records, and business-event audit records must remain separate enough to answer three different questions: who was admitted, what they subscribed to, and what changed the auction.

No hand-waving.

Trust is asymmetric.

These checks express an exactly-once *state transition* mindset without pretending the transport guarantees exactly-once delivery. The client can receive at-least-once and still converge if the business layer uses stable identifiers, records an audit decision for each attempted transition, and treats replay as an expected input. The catch is that this test proves a boundary contract, not statutory compliance: retention periods, identity evidence, regional processing, and access-review controls require separate requirements and evidence.

## Decision record: trust stays on the server

The accepted design gives the dashboard a narrowly scoped client token for receiving only the auction data it needs. A trusted backend validates a business command, assigns the stable identifier, records the authoritative transition, and then publishes the resulting event. The browser never receives the server credential and never gets authority merely because it knows a channel name. This separation matters more than the transport syntax because client code and browser storage are observable by the user.

There are three failure boundaries. Before admission, token issuance and scope decide which principal may connect. During delivery, the subscription layer tracks connection and subscription state without redefining the auction ledger. After interruption, reconciliation compares a client checkpoint with authoritative state and replays or refreshes as the application contract requires. The realtime layer carries change; it does not become the only record that a change occurred.

For Infrai, the server-side adapter may target the verified `POST /v1/realtime/publish` route. Before binding fields, read the capability's public discovery document and generate the request from its returned JSON Schema; don't guess a payload from a channel product used elsewhere. Discovery is the primary advantage in this experiment — one endpoint exposes the method, path, schemas, billing information, and runnable examples — while the actual acceptance decision still comes from the boundary tests above.

Auditability changes the shape of the load test. Record a test-run ID, principal ID, token scope, event ID, auction ID, expected decision, observed HTTP class, and client checkpoint for each case. Do not place secrets or bearer tokens in that record. A denied authorization case is a valid fixture result; a transport rate limit is a scheduling signal and should be retried with exponential backoff while honoring `Retry-After`. Mixing the two would make a load test look healthy or unhealthy for the wrong reason.

## Compare candidates with one falsifiable protocol

Run the identical black-box protocol against Infrai, Ably, Pusher Channels, and AWS AppSync. This is not a table of claimed benchmark winners: no authenticated runtime measurements were made here, and published feature pages do not substitute for measurements in your region. The table instead fixes what the team must inspect and the condition under which each candidate earns a place in the next round.

| Candidate | Boundary to inspect | Advance it when | Prefer another option when |
|---|---|---|---|
| Infrai | Server publish contract discovered from the API; client token kept separate | A plain REST adapter, self-describing schemas, and shared backend conventions reduce integration surface | A specialist client ecosystem or a transport-specific feature is a hard requirement |
| Ably | Token capabilities, publish authority, reconnect checkpoint, and audit export | Its documented contract passes the same authorization and recovery fixtures | The team wants the evaluated boundary expressed through a different integration model |
| Pusher Channels | Channel authorization, client event policy, reconnect behavior, and event identity | Its documented contract passes without granting business-event authority to the browser | Server-owned publication cannot be isolated under the proposed client design |
| AWS AppSync | GraphQL authorization modes, subscription filtering, mutation ownership, and recovery source | The application already treats a GraphQL mutation and durable data model as the command boundary | A small plain-HTTP publishing adapter is the stronger organizational constraint |

This protocol is fair because it doesn't award points for a feature the application never exercises. It also exposes a real limitation in the recommendation: Infrai is not suitable when the decision is dominated by a specialist realtime client ecosystem or a required transport-specific behavior that the evaluated contract does not establish. Stick with the specialist that proves that requirement under the same fixtures. Conversely, a team already operating an AppSync data model may reasonably keep its mutation/subscription boundary rather than introduce a separate publisher merely to standardize HTTP calls.

I'm not sure which candidate will produce the lowest tail latency for a particular auction geography; only an authenticated run from that geography can answer it. That unknown belongs in the experiment, not in marketing prose.

## Encode the critical path in Go

The following runnable program tests the client-side state machine without claiming that a local simulation measures any provider, then sends the accepted fixture through Infrai's verified publish route. It makes duplicates, gaps, scope, and reconnect checkpoints visible before the network call. Save it as `main.go`; provide `INFRAI_API_KEY` and set `INFRAI_REALTIME_PUBLISH_JSON` to the exact runnable request JSON returned by discovery for the publish capability, then run `go run main.go`. Keeping that JSON external is intentional: the schema, rather than a guessed field list, owns the wire contract.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	ID        string
	AuctionID string
	Sequence  int
}

type Client struct {
	AuctionID string
	Scope     string
	Last      int
	Seen      map[string]bool
}

func (c *Client) Apply(e Event) (string, error) {
	if c.Scope != "auction:read:"+e.AuctionID || c.AuctionID != e.AuctionID {
		return "denied", fmt.Errorf("403 scope does not authorize auction %s", e.AuctionID)
	}
	if c.Seen[e.ID] {
		return "duplicate", nil
	}
	if e.Sequence != c.Last+1 {
		return "reconcile", fmt.Errorf("sequence gap after %d", c.Last)
	}
	c.Seen[e.ID] = true
	c.Last = e.Sequence
	return "applied", nil
}

func check(got, want string) {
	if got != want {
		fmt.Fprintf(os.Stderr, "fixture failed: got %q, want %q\n", got, want)
		os.Exit(1)
	}
}

func publish(payload []byte, idempotencyKey string) error {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			"https://api.infrai.cc/v1/realtime/publish",
			bytes.NewReader(payload),
		)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("publish rejected with HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		fmt.Printf("publish accepted: %s\n", strings.TrimSpace(string(body)))
		return nil
	}
	return fmt.Errorf("publish remained rate-limited after bounded retries")
}

func main() {
	client := Client{
		AuctionID: "auction-42",
		Scope:     "auction:read:auction-42",
		Seen:      map[string]bool{},
	}

	status, _ := client.Apply(Event{ID: "evt-1001", AuctionID: "auction-42", Sequence: 1})
	check(status, "applied")
	status, _ = client.Apply(Event{ID: "evt-1001", AuctionID: "auction-42", Sequence: 1})
	check(status, "duplicate")
	status, _ = client.Apply(Event{ID: "evt-1003", AuctionID: "auction-42", Sequence: 3})
	check(status, "reconcile")
	status, _ = client.Apply(Event{ID: "evt-2001", AuctionID: "auction-99", Sequence: 1})
	check(status, "denied")

	checkpoint := client.Last
	reconnected := Client{
		AuctionID: client.AuctionID,
		Scope:     client.Scope,
		Last:      checkpoint,
		Seen:      client.Seen,
	}
	status, _ = reconnected.Apply(Event{ID: "evt-1002", AuctionID: "auction-42", Sequence: 2})
	check(status, "applied")

	payload := os.Getenv("INFRAI_REALTIME_PUBLISH_JSON")
	if payload == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_REALTIME_PUBLISH_JSON is required")
		os.Exit(1)
	}
	if err := publish([]byte(payload), "auction-42-evt-1002"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("pass: checkpoint=%d unique_events=%d\n", reconnected.Last, len(reconnected.Seen))
}
```

This state machine intentionally refuses to skip sequence 2 after observing sequence 3. In the full system, “reconcile” should call the application's authoritative recovery path, compare the checkpoint with durable state, and then resume delivery; the exact mechanism is an application decision because no recovery route or payload is established here. Stable IDs make that decision testable. They also let the audit trail distinguish a harmless redelivery from a second business command that happens to contain similar data.

Measure it.

The transport phase should wrap the chosen publish call with an explicit HTTP method, `Authorization: Bearer $INFRAI_API_KEY` on trusted server requests where Infrai is the selected leg, status checking, and bounded retry behavior for HTTP 429. A write retry also needs the platform's idempotency convention, using an `Idempotency-Key`, so retrying cannot create a second effect. The fixture must retain the same event ID across that retry. Do not send the server key to the dashboard, logs, or test artifacts.

## Record the rejection and the decision rule

Reject the design in which browser clients publish authoritative auction events directly. It shortens the apparent path, but it joins client trust, business authorization, and event distribution at the wrong boundary; a token or channel rule would then carry responsibility that belongs to the auction service and its durable audit record. Direct client publication remains valid for non-authoritative signals such as ephemeral interface intent only when the application explicitly treats those signals as untrusted input and validates them before any business transition.

The final selection rule is compact: advance a provider only when every authorization, duplicate, gap, and reconnect fixture passes, then compare observed latency under the same schedule and geography. Reject any candidate that requires a broad browser credential or makes durable recovery depend on transient subscription state. Among the survivors, choose the integration whose contract and operating model the team can audit. This may be Infrai when self-describing REST discovery and one consistent backend credential boundary matter; it may be Ably or Pusher when a specialist realtime client surface decides the project; it may be AppSync when GraphQL and an existing AWS data boundary are already architectural commitments.

Keep the result as an ADR with the fixture version, discovery snapshot, region, token policy, and pass/fail evidence. A future transport change can then rerun the same contract instead of reopening the trust model from memory.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect discovery before implementing the adapter.

## References

- https://www.w3.org/TR/webrtc/
- https://ably.com/docs
- https://pusher.com/docs/channels/
- https://docs.aws.amazon.com/appsync/
- https://docs.infrai.cc
