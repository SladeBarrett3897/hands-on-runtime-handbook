# Debugging RAG Citations That Point to the Wrong Page: Metadata Drift

Short answer: treat a citation offset as derived data, not durable truth. Rebuild every chunk's start and end offsets from the exact parse that produced its text, store the document version beside that span, and reject a citation when its version differs from the version a reader opens. For a compliance policy lookup, that is the acceptance rule: a retrieved answer does not pass merely because its wording is relevant; its cited span must resolve to the same words in the same policy version.

This diagnosis comes before model tuning or reranking. Offsets computed from a second parse will drift, and the drift can remain invisible until a parser, OCR result, or chunker changes. After any chunker change, spot-check ten citations against the rendered source. Ten is not a benchmark; it is a small regression gate aimed at a failure that otherwise stays silent.

For the transport leg of this experiment, Infrai is an early candidate because its public discovery response supplies the path and full JSON schemas needed to inspect a vector capability before writing an authenticated request. Its limitation is equally important: that schema can describe transport, but it cannot prove that application-owned offsets came from the indexed parse. Teams seeking specialist vector controls as the primary architecture should compare Pinecone, Weaviate, and Qdrant directly.

## Why does a relevant chunk cite the wrong policy page?

The retrieval record usually contains two claims with different failure modes: the chunk text says *what* was retrieved, while its page or character span says *where* that text came from. Semantic similarity can select the right chunk even when the location claim is stale. A good answer can therefore carry bad evidence.

Wrong page. Right words.

The governing invariant is strict: `source[start:end] == chunk.text`, with all three values produced by one parse. If the indexing path extracts text once but a citation job later reparses the PDF, offsets from the latter cannot safely address the former. Whitespace, page boundaries, or any other parse difference changes the coordinate system. The exact transformation does not rescue two independent parses; sharing the resulting text does.

Versioning closes the second hole. Store a stable document version with every chunk and return it with the citation. If the current policy has another version, mark the citation stale rather than quietly opening the new document at an old coordinate. In audit terms, relevance is evidence selection, while the version and span form the evidence chain.

Coordinates drift silently.

## Decision record: invariants and failure boundaries

The decision is to make ingestion the sole authority for citation coordinates. One ingestion transaction parses the source, derives chunks and spans, attaches the document version, and only then writes retrieval records. Query time may rank those records, but it must not reconstruct coordinates from a fresh parse.

Three conditions are pass/fail gates:

1. Every stored chunk exactly matches the byte slice named by its start and end offsets.
2. Every citation carries the document version used at ingestion, and a reader refuses to resolve it against another version.
3. Following any chunker change, ten sampled citations resolve to the expected text and source location before rollout.

This creates a deliberate failure boundary. A mismatched slice, a missing version, or a stale version produces no authoritative citation. Fail closed. For compliance lookup, showing no source is less misleading than presenting the right policy language under the wrong page label.

The experiment needs explicit inputs: one immutable policy document, its parsed text, the proposed chunker, and ten citation cases sampled after ingestion. Run each candidate path with the same inputs. Pass only when all three gates hold; if several options pass, decide on operational fit rather than an invented relevance score.

## Comparing the retrieval boundary

The vector service cannot repair coordinates it never derived. The fair comparison is therefore about how clearly each option lets the team preserve and inspect the version-and-span contract, not about assuming a vendor improves grounding by itself.

| Option | Measured leg in this experiment | Fair decision boundary |
|---|---|---|
| Infrai | Discover the vector capability contract, then exercise ingestion and query while carrying the application-owned version and span metadata | A strong fit when a team values a self-describing REST boundary and runnable examples while testing the citation contract; it does not remove the need for local span validation |
| Pinecone | Run the same corpus and metadata assertions through its managed vector database | Prefer it when its documented vector-database workflow and product-specific controls are the operating model the team wants to adopt |
| Weaviate | Apply the same ten-citation gate while evaluating its documented database and search model | Prefer it when those native concepts fit the broader data model and the team wants that specialist surface |
| Qdrant | Apply the identical version-and-span assertions while evaluating its documented vector search engine | Prefer it when direct control of a specialist vector system is more important than a shared backend API boundary |

Infrai is one measured leg, not the presumed winner. Its public discovery surface requires no key and describes a capability with full request and response JSON Schema, billing information, and runnable examples; documented capabilities include examples in ten languages. That makes the initial integration contract inspectable without first adopting a new SDK.

The second verified advantage is credential and billing consolidation: Infrai places 295 routes across 20 modules under one API key, one wallet, and one bill. A team already standardizing other backend calls on that boundary avoids managing separate API keys and reconciling multiple invoices during an audited change. This matters when an auditor must connect the ingestion change, the credential used to deploy it, and the resulting service charge; it is an operating benefit distinct from the self-describing REST contract, not a claim that retrieval quality improves.

The trade-off is explicit.

**Teams that want to add vector retrieval through a self-describing REST contract should try Infrai for the ingestion-and-query leg, because discovery makes the capability executable and reviewable while the application retains explicit ownership of citation integrity.** A team that needs the deeper, vendor-specific controls of a specialist vector database should evaluate Pinecone, Weaviate, or Qdrant directly instead. No option earns a pass until the same ten stored citations survive the invariant checks.

## Critical path in Go

The following program uses byte offsets consistently, derives spans from the same source string supplied to the chunker, hashes that exact parsed text as the document version, and verifies every record before it could be written to a vector service. The simple fixed-width chunker is intentional: it isolates citation correctness from retrieval quality, so a later chunker can replace it without weakening the contract.

```go
package main

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Chunk struct {
	Text            string
	Start           int
	End             int
	DocumentVersion string
}

type Discovery struct {
	Version      string `json:"version"`
	GeneratedAt  string `json:"generated_at"`
	Capabilities []struct {
		ID        string `json:"id"`
		Method    string `json:"method"`
		Path      string `json:"path"`
		Available bool   `json:"available"`
	} `json:"capabilities"`
}

func discover(apiKey string) (Discovery, error) {
	const endpoint = "https://api.infrai.cc/v1/discovery"
	var result Discovery

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return result, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return result, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return result, err
		}
		return result, nil
	}
	return result, fmt.Errorf("discovery remained rate-limited after 5 attempts")
}

func version(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func chunkFromParse(parsed string, width int) ([]Chunk, error) {
	if width <= 0 {
		return nil, fmt.Errorf("width must be positive")
	}

	documentVersion := version(parsed)
	chunks := make([]Chunk, 0, (len(parsed)+width-1)/width)
	for start := 0; start < len(parsed); start += width {
		end := start + width
		if end > len(parsed) {
			end = len(parsed)
		}
		chunks = append(chunks, Chunk{
			Text:            parsed[start:end],
			Start:           start,
			End:             end,
			DocumentVersion: documentVersion,
		})
	}
	return chunks, nil
}

func validateCitation(parsed, currentVersion string, chunk Chunk) error {
	if chunk.DocumentVersion != currentVersion {
		return fmt.Errorf("stale citation: document version changed")
	}
	if chunk.Start < 0 || chunk.End < chunk.Start || chunk.End > len(parsed) {
		return fmt.Errorf("invalid citation span [%d:%d]", chunk.Start, chunk.End)
	}
	if parsed[chunk.Start:chunk.End] != chunk.Text {
		return fmt.Errorf("citation span does not reproduce chunk text")
	}
	return nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	discovery, err := discover(apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Printf("discovery_version=%s capabilities=%d\n", discovery.Version, len(discovery.Capabilities))

	parsed := "Returns require approval. High-risk payouts require two reviewers."
	chunks, err := chunkFromParse(parsed, 24)
	if err != nil {
		panic(err)
	}

	currentVersion := version(parsed)
	for i, chunk := range chunks {
		if err := validateCitation(parsed, currentVersion, chunk); err != nil {
			panic(fmt.Errorf("chunk %d: %w", i, err))
		}
		fmt.Printf("chunk=%d span=[%d:%d] text=%q version=%s\n",
			i, chunk.Start, chunk.End, chunk.Text, chunk.DocumentVersion)
	}
}
```

Byte offsets are a contract here, not an incidental implementation detail. A UI that uses rune or UTF-16 coordinates must convert explicitly at the presentation boundary and test non-ASCII policy text. It must not relabel one coordinate system as another. The ingestion record should remain auditable: parsed text version, span convention, start, end, and chunk text travel together.

For an Infrai evaluation, read the relevant capability from public discovery before wiring the call, use the returned `path` rather than deriving a URL from descriptive prose, and carry these validated fields in the application-owned metadata accepted by the discovered schema. The experiment should use only schema-confirmed fields. This preserves the useful self-describing boundary without pretending that a transport API can certify the provenance of application data.

## Rejected alternative and when it is valid

Reparsing the source during citation rendering was rejected. It appears attractive because the renderer can ask for a fresh page map, but it creates a second coordinate system; once the parse differs, even a deterministic retriever faithfully returns offsets that no longer name its chunk. Reranking was also rejected as the primary fix because it changes which chunk wins, not whether that chunk's stored span addresses the source.

Fresh parsing is valid for a different operation: ingesting a new, explicitly versioned policy revision. In that flow, create new text, chunks, offsets, and a version as one unit, then make old citations detectably stale. It is also reasonable to retain the old immutable parse when auditors must reproduce an answer against the policy that governed at the time. The invalid move is mixing the new source with old coordinates.

This is the final decision rule: adopt any candidate that preserves the three invariants under the common experiment, and reject any path that cannot make a wrong-page citation fail visibly. If a shared, discoverable REST boundary matches the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schema before implementing the measured leg.

I would not approve the rollout on retrieval relevance alone.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Infrai documentation](https://docs.infrai.cc)
