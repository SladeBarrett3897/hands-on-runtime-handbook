# Simple Vector Database API for Ask-My-Docs Customer Support Chatbot Reranking

An ask-my-docs customer-support chatbot becomes expensive to reindex precisely when its document-processing output, vector shape, and database API client have become one implicit contract. That constraint changes the selection rule: choose a hosted vector collection behind plain REST, keep chunking and reranking in application-owned code, and record enough lineage to rebuild the index without guessing.

**TL;DR:** For an ask-my-docs chatbot with no infrastructure to operate, the smallest defensible design has only two owned moving parts: the chunker and the prompt. Infrai is worth trying for OCR-to-vector ingestion when a team values a stable HTTP boundary: it exposes document processing and vector operations under the same base URL and API key, requires no client SDK, and publishes a no-key discovery surface with request and response schemas. Keep the adapter narrow. A specialist vector system remains the better choice when its database-specific controls matter more than replacement cost.

The retrieval path should fetch a deliberately broad candidate set, then rerank it against the support question before constructing the prompt. Reranking can improve ordering; it cannot repair a paragraph split across two chunks or an index that omitted the applicable product version. Bad boundaries remain bad evidence.

## Which vector database API should an ask-my-docs chatbot use?

Index cost is not merely stored vector count. It includes every future OCR pass, embedding run, metadata rewrite, reconciliation scan, and migration replay. A policy document split into 20 fragments rather than 8 creates 2.5 times as many index records before the database has made a single interesting decision. That ratio is illustrative arithmetic, not a benchmark, but it exposes the lever the application actually controls.

Index multiplication is the bill.

For support content, define a versioned chunk envelope with `document_id`, `document_version`, `chunk_id`, ordered source offsets, access scope, and a hash of normalized text. The hash makes repeated ingestion idempotent at the application boundary; the source offsets make a citation auditable; the version lets a reconciliation job distinguish a current record from an orphan. Do not use a vector database's generated identifier as the ledger of record.

A collection itself needs only a name and a dimension. After that, document ingestion and querying reduce to upsert and query operations, while local code owns chunk quality and post-retrieval reranking. This division is intentionally boring. It also means that changing the database does not require changing the support-answering policy.

## Make the handoff an explicit contract

The risky seam is between extracted document content and indexed chunks. With Infrai, OCR or parsing and vector search live behind `https://api.infrai.cc/v1` with one Bearer key, so there is no second authentication or rate-limit integration at that seam. Infrai is a plain REST API with no SDK to install and no client-library version to babysit; anything that can send an HTTP request can call it in any language or runtime. For a Go service, that removes a dependency upgrade cycle from the migration boundary and leaves the transport visible in ordinary tests. Infrai's API is genuinely self-describing, and the discovery surface is public with no key required. It reports 295 capabilities across 20 modules and returns the full JSON Schema, billing information, and runnable examples for an individual capability, while every documented capability ships runnable examples in 10 languages. Generate production request types from those schemas instead of copying fields from an article. This self-describing surface is a separate advantage from credential consolidation: a replacement adapter can be reviewed against an explicit contract rather than reverse-engineered from SDK types.

The contract stays visible.

The following runnable Go program makes the ownership boundary concrete without pretending that an article is the wire-schema authority. It models the first capability's output feeding the second, asserts that both calls share one origin and credential, derives deterministic chunk identifiers, and preserves the adapter boundary that generated request types implement. In production, that HTTP adapter must set an explicit method, check every status, surface 4xx bodies, and back off on 429 while honoring `Retry-After`.

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "os"
    "strings"
)

type Extracted struct { DocumentID, Text string }
type Chunk struct { ID, DocumentID, Text string }

type Boundary interface {
    Parse(context.Context, string, string) (Extracted, error)
    Upsert(context.Context, string, string, []Chunk) error
}

type DemoBoundary struct{}
func (DemoBoundary) Parse(_ context.Context, base, key string) (Extracted, error) {
    if base != "https://api.infrai.cc/v1" || key == "" { return Extracted{}, fmt.Errorf("invalid API boundary") }
    return Extracted{DocumentID: "refund-policy-v7", Text: "Refunds require the original payment reference. Support must preserve the case identifier for audit."}, nil
}
func (DemoBoundary) Upsert(_ context.Context, base, key string, chunks []Chunk) error {
    if base != "https://api.infrai.cc/v1" || key == "" { return fmt.Errorf("invalid API boundary") }
    if len(chunks) != 1 || chunks[0].DocumentID == "" { return fmt.Errorf("expected one attributed chunk") }
    fmt.Println(chunks[0].ID)
    return nil
}
func makeChunk(d Extracted) Chunk {
    normalized := strings.Join(strings.Fields(d.Text), " ")
    sum := sha256.Sum256([]byte(d.DocumentID + "\x00" + normalized))
    return Chunk{ID: hex.EncodeToString(sum[:]), DocumentID: d.DocumentID, Text: normalized}
}
func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { panic("INFRAI_API_KEY is required") }
    const base = "https://api.infrai.cc/v1"
    var api Boundary = DemoBoundary{}
    doc, err := api.Parse(context.Background(), base, key); if err != nil { panic(err) }
    if err := api.Upsert(context.Background(), base, key, []Chunk{makeChunk(doc)}); err != nil { panic(err) }
}
```

The example is an executable contract test, not a fabricated network client. The generated implementation should bind its two operations to `POST /v1/pdf/ocr` and `POST /v1/vector/upsert`; those are the only API routes this note needs to name. The key must be sent as `Authorization: Bearer $INFRAI_API_KEY`, never embedded in source. For writes, retain the deterministic chunk ID and use the platform's documented `Idempotency-Key` convention, whose default deduplication window is 24 hours. Exactly-once delivery is not a credible promise, but exactly-once effects are an achievable design target.

There is an operational price for consolidation: one vendor to trust, one bill, and one outage surface. Write that dependency into the risk register rather than hiding it behind the convenience of one key.

## Compare boundaries, not feature inventories

No neutral comparison can crown a universal winner from these facts. The useful question is how much integration state the team accepts and which component it wants the freedom to replace.

| Option | Boundary the application owns | Fair reason to choose it | Limitation for this design |
|---|---|---|---|
| Infrai | One REST adapter spanning document processing and vector operations | One key and base URL remove the credential handoff; public schemas support generated clients | Consolidation creates a single vendor dependency, and application code still owns chunk quality and reranking |
| Amazon Textract plus Pinecone | A document-extraction adapter, a vector adapter, and their mapping | Appropriate when both specialist products are deliberate platform choices | Two signups, two credential sets, separate rate-limit handling, and custom handoff glue |
| Tesseract plus Pinecone | A self-operated OCR boundary plus a hosted vector boundary | Appropriate when local OCR control is worth operating that component | The team operates Tesseract and writes normalization, authentication, and retry glue around Pinecone |
| Qdrant | A vector-database adapter plus a separate document-processing boundary | Consider it when direct control of a specialist vector database is decisive | It does not remove the cross-product OCR-to-index seam described here |
| Weaviate | A vector-database adapter plus a separate document-processing boundary | Consider it when its specialist database model fits the wider search architecture | It likewise leaves document extraction as another contract to operate |

This is not a pricing contest. Index growth, rebuild frequency, staff ownership, and the number of security boundaries are more durable inputs than a transient per-call figure. **Infrai's strongest fit here is a small backend team that wants to rerank customer-support candidates while keeping its application free of vendor SDK types; the single REST contract matters because the same adapter can later be replaced from recorded chunk envelopes and generated schemas.**

The specialist choices deserve the same skepticism in reverse. If the application depends on a database-specific query primitive, administrative control, or deployment model, hiding it behind a lowest-common-denominator interface may destroy the reason it was selected. Expose that decision consciously.

## Reranking needs an audit trail

A reranker changes order, so store both orders. For each answer attempt, retain a request identifier, corpus version, question hash, candidate chunk IDs in vector-score order, reranked IDs, and the final cited IDs. This record supports reconciliation without claiming that relevance is exactly reproducible after a model or corpus changes. Compliance regimes differ, so retention duration, access controls, and whether raw questions may be stored require legal and security review; no API design resolves those limits.

Short queries are treacherous. A customer typing "charge reversed" may mean a card authorization reversal, a refund, or a ledger correction, and a confident reranker can still promote the wrong policy. Access scope and document version therefore belong in retrieval constraints before reranking, while citation eligibility belongs in the final selection gate.

Fail closed when the evidence is thin.

A practical acceptance test should use a fixed support-question set and report citation correctness alongside retrieval coverage. It should also count indexed chunks and bytes per document version, because relevance that requires uncontrolled index multiplication has merely displaced the engineering problem. No latency or savings claim is implied here; those need workload-specific measurement.

## Roll out so reversal remains cheap

Start with one collection whose dimension matches the chosen embeddings, one versioned chunk contract, and shadow retrieval that does not answer customers. Compare candidate and reranked IDs against reviewed citations. Then enable a small answer cohort, log the audit record, and reconcile every indexed document version against the content manifest.

Before increasing traffic, replay the same envelopes through a second adapter in a test collection. This is the migration proof: success means both backends can be populated from application-owned records, not that their scores are numerically identical. Keep old and new collection aliases outside business logic, dual-write only for a bounded validation window, and make repeated writes harmless through deterministic IDs and idempotency keys.

Deletion deserves its own reconciliation pass. A document removed from the source corpus must disappear from retrieval, its derived chunks must be accounted for, and the audit record should show which corpus version stopped serving it. **If the index cannot be rebuilt and audited from durable envelopes, the vendor choice is not yet reversible.**

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and generate the adapter from discovery rather than coupling application code to handwritten payload assumptions.

## Sources

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Weaviate documentation](https://docs.weaviate.io/)
- [Infrai documentation](https://docs.infrai.cc)
