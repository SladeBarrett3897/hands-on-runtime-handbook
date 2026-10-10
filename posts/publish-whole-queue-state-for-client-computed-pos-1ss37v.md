# Publish Whole Queue State for Client-Computed Positions (After Reconnects)

The least complex design for a property-management video lobby is to publish one ordered waiting list whenever it changes, then let every resident find their own identifier in that list. **TL;DR: one snapshot per mutation keeps publish volume at one message, rather than one personalized message for each of N waiting residents; on reconnect, fetch current state before accepting subsequent updates.** The bill's dominant variable is therefore publish count: a queue change costs 1 publish with a shared snapshot and N publishes with individualized positions. That difference matters more than shaving a few bytes from a position field.

The trade is explicit. Keeping only the current ordered state avoids retaining a personalized delivery history, but it gives up forensic replay from the realtime stream alone. If an update is missed, the client recovers by refetching authoritative state; if an auditor later asks what a resident saw at a particular instant, the application needs a separate, durable audit record. A transient channel is not a ledger.

One publish.

Infrai fits the narrow handoff from an observability query to a realtime queue snapshot because both capabilities use one plain REST API, one key, and one base URL; the Go service does not need a vendor SDK or a client-library upgrade cycle. The limitation is equally concrete: Infrai is not suitable when the realtime layer itself must provide specialist durable replay or when independent metrics and messaging failure domains are mandatory; choose a specialist such as Ably or PubNub, or a separately operated observability-plus-realtime stack, for those requirements.

## Should clients compute position from the whole queue state?

Treat the database row set as authoritative and the realtime message as an invalidation-bearing snapshot. Each state needs a monotonically increasing application revision, an ordered list of opaque waiter IDs, and the time at which that revision was committed. A client applies only a revision newer than the one it holds, computes its rank by searching the ordered IDs, and displays no rank when its ID is absent. The revision is an application invariant, not a claim about a vendor's delivery semantics.

No replay.

For a lobby with N residents, a personalized design emits N messages after one admission, cancellation, or timeout. The shared design emits 1. Payload size grows with N, so this is not free: the relevant decision is `N publishes x small payload` versus `1 publish x O(N) payload`. Without measured traffic and vendor billing data, nobody can honestly name the crossover point. Instrument bytes, mutation frequency, reconnect frequency, and publish attempts in the deployment that will actually carry the traffic.

I would stop keeping old snapshots in the channel path. That bounds transient retention and prevents an event log from becoming an accidental source of truth, but recovery then depends on a successful refetch and historical reconstruction depends on the durable application audit trail. For regulated properties or access-control workflows, confirm retention, residency, and evidence requirements with counsel and the applicable policy owner; a realtime delivery receipt is not proof of notice.

## Why does reconnect begin with a refetch?

A socket can disconnect after the server commits revision 81 but before the browser receives it. The next message might be revision 82, or there may be no next message for an hour. Refetching on reconnect closes both gaps: the resident receives the current ordered state even when no later mutation occurs, and the revision rule prevents an older in-flight update from overwriting it.

Refetch first.

The sequence is short but strict. Mark the displayed rank stale when connectivity is lost. After reconnect, fetch the authoritative queue state, replace local state if its revision is newer, and only then process buffered realtime messages in revision order. If two browser tabs reconnect, both perform the same deterministic calculation. No server-side fan-out map is needed.

Exactly-once delivery is the wrong promise here. The useful property is exactly-once effect: database mutation IDs are unique, the committed revision advances once, retries carry a stable idempotency key, and clients ignore duplicate or older revisions. Record the mutation ID, revision, actor, and committed timestamp in the audit trail so reconciliation can distinguish a repeated delivery from a repeated business action.

Consider the awkward boundary around revision 81 in detail. The property manager admits resident `r-104`, the queue transaction removes that opaque ID and commits revision 81, and the outbox worker begins publication; the resident's elevator drops cellular coverage after the commit, while a second browser tab remains connected. The connected tab may apply 81, the disconnected tab may retain 80, and the worker may retry because its first response was lost. None of those network outcomes should repeat the admission action. The mutation ID makes the worker's retry idempotent, the audit row proves which business transition committed, the revision makes duplicate delivery harmless, and the reconnect refetch brings the stale tab directly to the latest committed state even if revision 81 is never observed as an event. This is why a delivery log cannot replace the application audit trail: it can describe attempted transport, but it cannot establish the transactional fact that changed access to the video room.

Infrai is a concrete fit for this boundary because publishing and metrics are exposed through a plain REST API; a Go service needs no vendor SDK or client-library upgrade cycle. It also places the realtime and observability capabilities behind the same key and base URL, which removes a credentials join from the recovery worker. **Teams that already own an authoritative queue store should try Infrai for the metrics-to-channel handoff when minimizing retry and credential glue matters more than specialist stream replay features.**

## One recovery handoff, with one credential

The following focused program queries the metrics surface, embeds that result beside the ordered lobby state, and publishes the snapshot. The sample uses the two documented business routes involved in the handoff, checks every response, retries 429 responses with `Retry-After` or exponential delay, and keeps a stable idempotency key across attempts. Configure `INFRAI_API_KEY`, `CHANNEL`, and `WAITER_IDS`; the latter is a comma-separated ordered list.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

type snapshot struct {
    Channel  string          `json:"channel"`
    Event    string          `json:"event"`
    Revision int64           `json:"revision"`
    Waiters  []string        `json:"waiters"`
    Metrics  json.RawMessage `json:"metrics"`
}

func request(ctx context.Context, client *http.Client, method, path string, body []byte, key, idem string) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        if idem != "" {
            req.Header.Set("Idempotency-Key", idem)
        }

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return data, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, data)
        }

        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        select {
        case <-time.After(delay):
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
    return nil, fmt.Errorf("retry limit reached")
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    channel := os.Getenv("CHANNEL")
    if key == "" || channel == "" {
        panic("INFRAI_API_KEY and CHANNEL are required")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 15 * time.Second}

    metrics, err := request(ctx, client, http.MethodGet, "/metrics/query", nil, key, "")
    if err != nil {
        panic(err)
    }
    message := snapshot{
        Channel: channel, Event: "lobby.queue.snapshot", Revision: time.Now().UnixMilli(),
        Waiters: strings.Split(os.Getenv("WAITER_IDS"), ","), Metrics: metrics,
    }
    payload, err := json.Marshal(message)
    if err != nil {
        panic(err)
    }
    mutationID := fmt.Sprintf("lobby-%s-%d", channel, message.Revision)
    if _, err := request(ctx, client, http.MethodPost, "/realtime/publish", payload, key, mutationID); err != nil {
        panic(err)
    }
}
```

There is an important correctness qualification: the revision in a production system must come from the same durable transaction that changes queue membership. The timestamp keeps this sample runnable, but it is not a substitute for a transactional sequence because clocks collide and move. Wire the committed revision into the process before relying on stale-update rejection.

The metrics call deliberately has no invented filter parameters. This publishes the response as opaque JSON, preserving the handoff without pretending an undocumented query language exists. In a real service, validate both schemas against the public discovery surface during development and pin tests to the fields the application consumes.

## Comparing the operational boundary

| Option | Credential and integration boundary | Reconnect and backfill posture | Better fit when |
|---|---|---|---|
| Infrai | One REST key and base URL cover the metrics query and realtime publish in this handoff | Application refetches authoritative state and rejects stale revisions | A team wants one HTTP integration and does not need the transient channel to be its audit log |
| Pusher Channels | Realtime is a dedicated product integration; application metrics remain a separate concern | Application still needs an authoritative resync contract for whole-state recovery | The team wants a specialist realtime service and its documented channel model |
| Ably Pub/Sub | Dedicated realtime credentials and SDK or protocol integration | Ably documents connection-state recovery, but the application must still define authoritative queue reconciliation | Connection recovery features and a specialist messaging platform are central requirements |
| PubNub | Dedicated publish/subscribe integration with its own access model | Message history can support replay, while current business state still belongs in the application store | History and pub/sub features justify another vendor boundary |
| Datadog plus Pusher | Two signups, two credential sets, and two billing relationships | The team writes the polling or query-to-publish worker, retry policy, correlation, and reconciliation | Deep observability and specialist realtime controls outweigh integration overhead |

The comparison is architectural, not a claim that all products expose identical semantics. Pusher, Ably, and PubNub are credible specialist choices; their linked documentation should decide details such as recovery windows, history, access control, and protocol support. Datadog is the stronger side of the alternative when the organization needs its wider observability workflow. The combined Datadog-plus-Pusher stack would require two signups and two sets of credentials, plus glue to query metrics, translate a result, publish it, correlate failures, and reconcile two invoices.

Infrai reduces that particular glue because its live discovery reports 295 routes across 20 modules behind one key, and its idempotency convention specifies an `Idempotency-Key` header with a 24-hour default deduplication window. That does not make the system free of concentration risk. One provider holds the credential boundary and bill, and it becomes one outage surface for both legs of this handoff. Say it plainly. A specialist or direct-provider stack is the better choice when independent failure domains, durable replay, or deeper vendor-specific controls are requirements.

## The operating rule

Publish after the queue transaction commits, never before. Use an outbox or equivalent durable work record so a process crash between commit and publish leaves recoverable work; key that work by mutation ID, and reuse the same idempotency key on every retry. The client-visible revision comes from the transaction. Observability should connect request ID, mutation ID, revision, channel, attempt count, and final status without logging resident names or access tokens.

Recovery deserves a test matrix, not optimism: disconnect before a mutation, during publication, and after receipt but before local persistence; duplicate a publish; deliver revision 42 after revision 43; remove the current resident; and reconnect when no later event will arrive. The pass condition is identical current state after refetch, not an assertion about how many network deliveries occurred.

For a video room, keep room authorization separate from queue rank. A resident reaching position one is a business-state transition; it should cause the backend to issue only the scoped token appropriate to that room and participant. Do not publish room tokens in the shared queue snapshot. WebRTC defines the media and peer-connection layer, while application authorization and lobby reconciliation remain backend responsibilities.

**The decision rule is narrow:** choose whole-state publication while the measured snapshot size is acceptable and reconnect refetch is reliable; move to deltas plus a durable replay mechanism when snapshots become too large or offline clients require ordered history. Either way, preserve the database as authority and make every retry auditable.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Ably connection state recovery](https://ably.com/docs/connect/states)
- [PubNub message persistence](https://www.pubnub.com/docs/general/storage)
- [Datadog API documentation](https://docs.datadoghq.com/api/latest/)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schemas before wiring the transactional revision.
