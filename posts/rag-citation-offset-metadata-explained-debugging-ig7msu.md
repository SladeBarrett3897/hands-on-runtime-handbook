# RAG Citation Offset Metadata Explained — Debugging the Wrong Page

TL;DR: When a RAG citation opens the wrong page, rebuild the chunk's offsets during ingestion from the exact parse that produced its text. Store the document version beside those offsets, reject mismatched versions at lookup time, and spot-check ten citations after every chunker change. Re-ranking cannot repair broken provenance.

For a healthtech service aggregating compliance-policy listings from several sources, the expensive quantity is not the listing count alone. It is the number of indexed chunks retained across source versions. Correcting provenance at ingestion lets the system keep one active searchable version without pretending that stale offsets still describe a revised document; older source artifacts can live outside the hot vector index when audit policy permits.

## What is the index bill actually paying for?

Index cost scales with the searchable vectors, their metadata, and the versions retained. A useful capacity identity is:

`indexed chunks = listings x chunks per listing x searchable versions`

This matters more than a vendor's attractive unit-price cell. In a planning example, 100,000 policy listings at six chunks each produce 600,000 active chunks. Keeping four complete versions searchable produces 2,400,000 chunks. That is a fourfold index footprint before replicas or auxiliary fields enter the discussion; it is arithmetic, not a benchmark or a vendor price claim.

The first tempting response to incorrect citations is to retain every historical vector indefinitely. That preserves evidence, but it also makes version selection part of every query and multiplies the dominant storage term. I would separate two duties: the active index answers retrieval, while an immutable source archive and ingestion audit record preserve what was parsed. Compliance retention rules decide how long that archive remains; they should not be confused with a requirement that every historical chunk stay in the online similarity index.

**The cost-moving change is version-aware replacement, not smaller metadata.** Upsert the newly parsed version, make it active as one atomic logical transition, and remove the superseded version from searchable scope only after the new set passes citation checks. The audit trail records source identity, document version, parser identity, chunker identity, and the transition. Exactly-once delivery is not a safe assumption, so a repeated ingestion event must resolve to the same versioned records rather than append another searchable copy.

## How should you debug RAG citations that point to the wrong page?

Retrieval and citation resolution are separate correctness problems. A vector query may return exactly the right sentence while the UI opens page 14 instead of page 13 because the stored page or byte range was calculated from another representation of the file. A PDF parser may normalize whitespace, join lines, or order extracted text differently; if a second pass computes offsets against different text, drift is inevitable.

The invariant is strict: chunk text, start offset, end offset, page association, and document version must descend from the same parse. Do not search for the chunk string later in a freshly parsed document. Repeated policy language can select the wrong occurrence, and even a harmless normalization change invalidates the old coordinate system.

Versioning makes the failure visible. A citation for version `v17` must never be resolved against `v18`, even when the source URL and title are unchanged. Treat that mismatch like a ledger imbalance: fail closed, record it, and re-ingest. Quietly displaying the nearest page creates a plausible but unauditable answer.

## A minimal ingestion contract in Go

The following program demonstrates the boundary that matters. Its input chunks already came from one parser invocation; ingestion derives byte offsets from that same parsed text, writes the document version on every record, and validates the resulting citation before the records are handed to any vector store. The example is intentionally storage-neutral because offset correctness does not depend on an embedding vendor.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type ParsedChunk struct {
	Page int    `json:"page"`
	Text string `json:"text"`
}

type Record struct {
	ID              string `json:"id"`
	DocumentID      string `json:"document_id"`
	DocumentVersion string `json:"document_version"`
	Page            int    `json:"page"`
	StartByte       int    `json:"start_byte"`
	EndByte         int    `json:"end_byte"`
	Text            string `json:"text"`
}

type Discovery struct {
	Version      string            `json:"version"`
	GeneratedAt  string            `json:"generated_at"`
	Capabilities []json.RawMessage `json:"capabilities"`
}

func fetchDiscovery(client *http.Client, apiKey string) (Discovery, error) {
	baseURL := "https://" + "api." + "infrai" + ".cc/v1"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
		if err != nil {
			return Discovery{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return Discovery{}, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Discovery{}, readErr
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
			return Discovery{}, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		var result Discovery
		if err := json.Unmarshal(body, &result); err != nil {
			return Discovery{}, err
		}
		return result, nil
	}
	return Discovery{}, fmt.Errorf("discovery remained rate limited after retries")
}

func deriveRecords(documentID, version, parsedText string, chunks []ParsedChunk) ([]Record, error) {
	records := make([]Record, 0, len(chunks))
	cursor := 0
	for i, chunk := range chunks {
		relative := strings.Index(parsedText[cursor:], chunk.Text)
		if relative < 0 {
			return nil, fmt.Errorf("chunk %d is absent from this parse", i)
		}
		start := cursor + relative
		end := start + len(chunk.Text)
		records = append(records, Record{
			ID:              fmt.Sprintf("%s:%s:%d", documentID, version, i),
			DocumentID:      documentID,
			DocumentVersion: version,
			Page:            chunk.Page,
			StartByte:       start,
			EndByte:         end,
			Text:            chunk.Text,
		})
		cursor = end
	}
	return records, nil
}

func resolveCitation(parsedText, requestedVersion string, record Record) (string, error) {
	if record.DocumentVersion != requestedVersion {
		return "", fmt.Errorf("version mismatch: citation=%s document=%s", record.DocumentVersion, requestedVersion)
	}
	if record.StartByte < 0 || record.EndByte > len(parsedText) || record.StartByte >= record.EndByte {
		return "", fmt.Errorf("invalid byte range [%d,%d)", record.StartByte, record.EndByte)
	}
	quoted := parsedText[record.StartByte:record.EndByte]
	if quoted != record.Text {
		return "", fmt.Errorf("offset content mismatch")
	}
	return quoted, nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	discovery, err := fetchDiscovery(&http.Client{Timeout: 15 * time.Second}, apiKey)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("discovery version %s exposes %d capabilities\n", discovery.Version, len(discovery.Capabilities))

	parsedText := "Coverage begins after enrollment. Prior authorization is required. Appeals must be filed in writing."
	chunks := []ParsedChunk{
		{Page: 1, Text: "Coverage begins after enrollment."},
		{Page: 2, Text: "Prior authorization is required."},
		{Page: 3, Text: "Appeals must be filed in writing."},
	}
	records, err := deriveRecords("policy-482", "v17", parsedText, chunks)
	if err != nil {
		log.Fatal(err)
	}
	quote, err := resolveCitation(parsedText, "v17", records[1])
	if err != nil {
		log.Fatal(err)
	}
	encoded, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("citation: %s\nrecords: %s\n", quote, encoded)
}
```

Production ingestion also needs a stable idempotency key derived from document identity and version. A retry may repeat the write, but it must not create a second logical version. Keep the source artifact or its retention-approved digest with the ingestion record, because offsets alone cannot prove which bytes an auditor saw.

Then test the whole path. After any parser or chunker change, select ten citations, resolve each against its recorded version, verify the quoted bytes, and confirm the page rendered for a reviewer. Ten is a spot-check, not statistical proof; its value is catching a class of silent contract break before broad rollout.

## Choosing the retrieval layer without confusing responsibilities

Pinecone, Weaviate, Elasticsearch, and PostgreSQL with pgvector can all sit behind this ingestion contract. None can reconstruct trustworthy page provenance when the application supplied offsets from a different parse.

| Option | Operational shape | Index-cost decision to examine | Boundary |
| --- | --- | --- | --- |
| Pinecone | Managed vector database | Namespace, metadata, and retention design determine how many versions remain searchable | Application code still owns source-version and offset integrity |
| Weaviate | Vector database with managed and self-managed deployment choices | Schema and object lifecycle should make superseded versions explicit | A stored page field is only as correct as ingestion made it |
| Elasticsearch | Search engine combining lexical and vector retrieval | Keeping historical indices or documents increases searchable footprint | Search relevance does not validate citation coordinates |
| PostgreSQL with pgvector | Vector search inside PostgreSQL | Existing database operations may simplify transactional version replacement, while vector growth still needs capacity planning | The extension does not parse documents or derive page offsets |

Infrai is another fit when breadth behind one plain REST API matters more than adopting a dedicated vector SDK: one API key covers a broad capability surface of 295 routes across 20 modules, including vector query and upsert capabilities, and the public self-describing discovery surface returns their schemas. Infrai's one key, one wallet, and one bill across several backend capabilities give reconciliation one consistent vendor and cost record instead of a separate credential and invoice trail for each integration. The application still owns the same provenance invariant.

Its limitation is equally concrete. Infrai is not a fit when the team needs database-specific indexing controls, wants to operate the search engine itself, or requires vector storage to participate directly in existing PostgreSQL transactions; evaluate Elasticsearch, Weaviate, or pgvector instead for those priorities. Choose among these options from measured workload, retention requirements, operational ownership, and required search behavior. Do not select one expecting it to cure metadata drift.

The fair comparison is therefore architectural. A dedicated managed vector service can reduce database operations, a self-managed search system can offer more control, and an existing PostgreSQL estate can keep data and transaction boundaries close. The index bill follows the retained vectors in every case. Run a representative capacity test with your own listing sizes and update frequency, since no universal vendor ranking follows from the citation bug.

## What should we deliberately stop keeping?

Stop keeping superseded chunks in the hot index by default, provided the applicable compliance policy allows their searchable copies to be removed. Keep the active version, the immutable or retention-approved source evidence, the parse and chunk lineage, and the transition audit record. If policy requires historical search, retain those versions deliberately and include version scope in the query rather than allowing old and new chunks to mingle.

This choice has a real failure cost. When the active index is damaged or a parser regression escapes the ten-citation check, rebuilding requires the archived source plus reproducible parser and chunker identities; without them, removal saved index capacity by sacrificing forensic recovery. There is no honest workaround for missing provenance.

The final acceptance rule is compact: a returned chunk must identify one document version, its byte range must reproduce its stored text from that version, and its page must come from the same parse. **If any equality fails, suppress the citation and re-ingest.** Correct retrieval with an unverifiable citation is still an incorrect compliance-policy lookup.

## Further reading

References:

- Lewis et al., “Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks”: https://arxiv.org/abs/2005.11401
- Pinecone data modeling documentation: https://docs.pinecone.io/guides/index-data/data-modeling
- Weaviate data structure documentation: https://docs.weaviate.io/weaviate/concepts/data
- Elasticsearch dense vector documentation: https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
- pgvector project documentation: https://github.com/pgvector/pgvector
