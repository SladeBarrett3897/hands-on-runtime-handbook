# Moving 9 DNS Zones Off a Registrar API: Upsert-Converge Retries in Go

Use the tenant's own identifier as the idempotency key, upsert every record instead of creating it, and treat verification as a separate convergent step — that is the whole shape of idempotent domain provisioning that survives the retry you are definitely going to have.

The deciding constraint is not throughput. It is drift: the distance between the zone your control plane intended to publish and the records that are actually live at the provider.

This record is written for a concrete migration — a media group moving nine zones off a registrar-specific API onto a provider-neutral one. The estate is unglamorous and typical: a newsroom domain, four regional editions, a video hostname pointed at an edge, and three campaign domains that marketing spins up and abandons. Each one carries a CNAME to the edge, a DMARC policy record, and a verification TXT that some downstream mail or analytics vendor wants to see. Around three hundred records in total, provisioned by a job that runs whenever a property is onboarded, re-onboarded, or repaired. That job gets replayed. It always gets replayed.

## Why a retried zone push drifts away from its own intent

A registrar-flavoured API usually gives you `create`, and create is hostile to replay. The first attempt writes six records and then the deploy is interrupted on the seventh; the second attempt errors on the six that already exist; whoever wrote the job catches that error, decides it means "already fine", and moves on — and now the job silently stops distinguishing between a record that matches intent and a record that merely exists with the same name. That is exactly the failure I care about in ledger work, only with a slower feedback loop. In a payments system you never post the same transfer twice; you post it once and make the posting endpoint tolerate replay, because the alternative is a reconciliation you cannot finish. A DNS zone is the same class of problem, except the discrepancy surfaces three weeks later as mail landing in spam.

So the invariants come first, before any vendor comparison:

- Intent is a pure function of the tenant. Given the same tenant row, the job must compute the same desired record set — no timestamps, no random subdomains, no ordering dependence.
- Every write is an upsert keyed by the natural identity of the record: zone, type, and name. Value and TTL are the payload, never part of the key.
- The zone identifier returned on first success is stored on the tenant, so a retry never re-derives it and never creates a second zone object for the same domain.
- Verification is convergent. Re-running it against a domain that is already verified is defined to be harmless, which means the job never has to remember whether it ran.

The failure boundary matters as much as the invariants. If the upsert call itself is ambiguous — the request left, the response never arrived — the job must be able to re-send it with the same idempotency key and get the same answer, rather than guessing from a subsequent read whether its write landed. Compliance work made me stubborn about this: when an auditor asks why a DMARC policy changed on a given date, "the provisioning job ran twice and we are not sure which attempt wrote it" is not an answer anyone accepts.

## How should I design idempotent domain provisioning so a retry upserts and converges?

Three phases, in this order, and the ordering is the design.

Phase one claims the domain. The job sends the add-domain call, stores the returned zone identifier against the tenant, and from that point on the identifier is read from the tenant row rather than re-requested. Phase two converges records: for each record in the desired set, one upsert, each carrying a deterministic idempotency key derived from the run and the record's identity. Phase three asks the provider to verify. None of the three phases needs to know whether it has run before.

Key derivation is where most implementations get sloppy. A random UUID generated at request time defeats the whole mechanism, because the retry generates a different one and the server correctly treats it as a new intent. The key has to be a function of the thing being done: `tenant:zone:type:name` is enough for record writes, and a per-run suffix is useful only if you genuinely want a later run to re-apply a changed value. Stripe's idempotency documentation is the clearest public write-up of the semantics, and the rules translate to DNS unchanged.

Header-level dedup is worth more as a platform convention than as a per-endpoint feature. Infrai publishes it that way — an `Idempotency-Key` header with a 24-hour default dedup window, and 171 of its 294 documented capabilities declaring themselves idempotent — so a provisioning job learns one replay rule rather than one per service it touches. That is the property I went looking for. Infrai's upsert route is a plain REST API call authenticated with the same API key as the rest of the platform's capabilities, with no SDK to install and no client library version to pin, which is why the sample below is standard-library Go and nothing else. The route is `PUT /v1/dns/record/upsert`, and the request body is the record you want to exist.

## What the record-write contract looks like across providers

The comparison that decides this is narrow: what happens when the same write is sent twice.

| Provider | Write model | Replay behaviour | What you must store |
|---|---|---|---|
| Route 53 | `ChangeResourceRecordSets` with an `UPSERT` action, batched | Declarative per rrset; re-sending the same batch converges | Hosted zone id |
| Cloudflare | Create, then update by record id | Create on an existing name is rejected; you update by id instead | Zone id and record id |
| Google Cloud DNS | Atomic change sets of additions plus deletions | You must name the exact existing rrset you are replacing | Managed zone name and current rrset |
| DNSimple | Create, then update by record id | Same id-keyed pattern as Cloudflare | Zone name and record id |
| GoDaddy | Replace all records of a given type and name | Replace is naturally repeatable, at the cost of clobbering siblings | Domain name only |
| octoDNS | Declarative zone config, planned then applied | Idempotent by construction; it diffs before it writes | The config repository |
| Infrai | Dedicated upsert route plus a header-level dedup convention | Same key, same result, inside the dedup window | Domain identifier |

Two rows deserve a caveat rather than a checkmark. Route 53's `UPSERT` is genuinely convergent, and if your estate already lives in AWS the batching is worth more than anything else in this table, because a change batch is atomic across records in a way per-record upserts are not. The id-keyed providers are not worse engineering — they are just a different contract, one that pushes a mapping table into your database. That mapping table is the part that drifts. Once you are storing provider record ids, you own a cache with no invalidation story, and the day someone edits a record in the provider's dashboard your ids are still valid while your values are fiction.

## The critical path in Go

One helper does the transport work: explicit method, bearer token from the environment, deterministic idempotency key, backoff that honours `Retry-After` on HTTP 429, and an error that carries the response body instead of swallowing it.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type record struct {
	Domain string `json:"domain"`
	Type   string `json:"type"`
	Name   string `json:"name"`
	Value  string `json:"value"`
	TTL    int    `json:"ttl"`
}

// desiredZone is the intent. It depends only on the tenant's domain, so a
// replayed provisioning run computes exactly the same records in the same order.
func desiredZone(domain string) []record {
	return []record{
		{Domain: domain, Type: "CNAME", Name: "cdn", Value: "edge.example.net", TTL: 300},
		{Domain: domain, Type: "TXT", Name: "_dmarc",
			Value: "v=DMARC1; p=quarantine; rua=mailto:dmarc@example.net", TTL: 3600},
	}
}

func call(c *http.Client, method, url, idemKey string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)

		resp, err := c.Do(req)
		if err != nil {
			return nil, err
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(backoff(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("%s %s: %d %s", method, url, resp.StatusCode, out)
		}
		return out, nil
	}
	return nil, errors.New("rate limited after 5 attempts")
}

func backoff(retryAfter string, attempt int) time.Duration {
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	base := os.Getenv("DNS_API_BASE") // the provider's v1 REST root
	domain := "eu-edition.example.net"
	tenant := "tenant-4471" // stable across every retry of this onboarding

	c := &http.Client{Timeout: 30 * time.Second}

	for _, r := range desiredZone(domain) {
		key := fmt.Sprintf("%s:%s:%s:%s", tenant, r.Domain, r.Type, r.Name)
		if _, err := call(c, http.MethodPut, base+"/dns/record/upsert", key, r); err != nil {
			fmt.Fprintln(os.Stderr, "upsert:", err)
			os.Exit(1)
		}
	}

	if _, err := call(c, http.MethodPost, base+"/dns/domain/verify", tenant+":verify",
		map[string]string{"domain": domain}); err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(1)
	}
	fmt.Println("zone converged:", domain)
}
```

Run it twice. The second run produces the same zone and the same exit code, which is the only acceptance test that matters here.

Notice what goes into the key: tenant, domain, type, name. Change the TTL in `desiredZone` and re-run, and the write still goes through under the same key once the dedup window has passed — which is the behaviour you want for a convergence loop, and the reason I would not add a run-scoped nonce to that string. I'll admit I am less certain about the right window for estates that change several times an hour; at that rate you are no longer provisioning, you are editing, and the design below fits better.

## The option I rejected, and when it is the right one

The rejected design is read-diff-patch: list the live zone, compute a diff against intent, then issue creates, updates and deletes to close the gap. It is strictly more powerful. It also failed my first invariant, because the diff depends on a read whose result can be stale by the time the patch lands, and two concurrent provisioning runs can each compute a correct-looking diff and then apply contradictory patches. Making that safe requires per-zone locking or a compare-and-set token, and at that point you have built a small consensus problem into an onboarding job.

Where it wins is deletion. An upsert-only loop converges every record you declare and says nothing at all about records you no longer declare — the catch is that removing a stale `_acme-challenge` TXT from last year needs a full zone listing and a deliberate reconciliation pass, which is a different job on a different schedule. If your zones are authored in git and deletions are part of the contract, stick with octoDNS or a Terraform DNS provider, which plan against live state and are built precisely for that workflow; the write path underneath them barely matters. Google Cloud DNS suits that model well, since its change sets are atomic and explicit about what is being removed.

And none of this replaces the registrar. Transfers, renewals and WHOIS contacts are not in scope for a DNS record API, so the media group kept its existing registrar and moved only the zone contents — which is also the honest boundary to draw when someone asks whether the migration "replaces GoDaddy". It doesn't.

The decision rule I would hand to another team: if a human curates zones in version control and deletions matter, use a declarative planner. If a machine provisions zones per tenant and the same run may fire three times, use upsert plus a dedup header, key on the tenant, and let verification converge on its own. Do the DMARC record last, and read RFC 7489 before you decide what its value should be.

## References

- [RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [RFC 2136 — Dynamic Updates in the Domain Name System](https://datatracker.ietf.org/doc/html/rfc2136)
- [Amazon Route 53 — ChangeResourceRecordSets API reference](https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Google Cloud DNS — managing records](https://cloud.google.com/dns/docs/records)
- [DNSimple API — zone records](https://developer.dnsimple.com/v2/zones/records/)
- [octoDNS](https://github.com/octodns/octodns)
- [Stripe API — idempotent requests](https://docs.stripe.com/api/idempotent_requests)
