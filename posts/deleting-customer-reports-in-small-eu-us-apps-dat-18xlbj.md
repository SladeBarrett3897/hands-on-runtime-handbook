# Deleting Customer Reports in Small EU/US Apps — Database Blob, Bucket, or Disk

Short answer: for authenticated customer reports, keep authorization, retention state, and an immutable deletion audit record in the database, while keeping report bytes in private object storage; choose database blobs only when transactional coupling matters more than independent storage operations, and local disk only when one replaceable node is the accepted durability boundary.

The decision is not really about where an upload is easiest to write. It is about whether an operator can answer four questions after the customer clicks delete: which logical report was targeted, which physical bytes were removed, which copies remain under an explicit retention rule, and which durable record proves the transition. A small B2B SaaS application serving generated reports to authenticated EU and US customers needs that answer before it needs clever upload throughput.

This architecture decision record therefore chooses a database control plane plus a private object byte plane. The boundary is deliberate — authentication grants access to a report record, the record resolves to an opaque storage key, and deletion advances through an idempotent state machine rather than pretending that one successful request makes every trace disappear at once.

## What invariants govern small EU/US app upload storage and database deletion?

Start with identity. A report ID belongs to the application domain; a storage key identifies bytes. They must not be interchangeable, because exposing a predictable key makes the storage namespace part of the authorization model. Every download should first authorize the authenticated tenant against the report record, then resolve the current object version, then return the bytes with an intentional `Content-Disposition` policy. MDN documents the header's `inline` and `attachment` dispositions and its filename parameters; for generated reports, `attachment` is usually the less surprising behavior, while the application still has to sanitize the displayed filename.

The second invariant is monotonic retention state. A report can move from `active` to `delete_requested` to `deleted`; retrying the same command must not resurrect it, duplicate an audit event, or target a newly uploaded object that happens to reuse a human-readable filename. Use a random or content-independent object key and a separate generation number. Once deletion is requested, reads stop at the database boundary even if physical removal is still being confirmed.

Access denial comes first.

The third invariant is evidence without retained content. The audit trail records tenant, report ID, object version, request ID, actor, policy basis, and transition timestamps, but never copies report bytes into an event payload or application log. A deletion ledger that contains the deleted data defeats its own purpose. I don't treat a storage lifecycle rule as the authoritative audit record either: lifecycle management is useful as a backstop for aging objects, as the S3 documentation describes, but an age-based policy does not by itself establish that a particular authenticated deletion command affected the intended logical report.

The failure boundaries follow from those invariants. The database transaction can commit before byte deletion completes, so the worker must retry. A client can repeat a request after losing the response, so the request ID must be unique within the tenant. A report can be regenerated while an older deletion is pending, so deletion must bind to an immutable object version rather than a mutable filename. I'm not sure how long every organization should preserve deletion evidence; that depends on its contracts and applicable obligations, and the answer should be written into policy rather than guessed in handler code. EU and US placement is another input to that policy, not a compliance verdict on its own.

## The options differ most at the deletion boundary

All three locations can hold bytes. Their operational meaning is different.

| Option | Atomicity boundary | Deletion and retention consequence | Valid fit | Principal limitation |
|---|---|---|---|---|
| Private object storage plus database metadata | Metadata and bytes are separate operations | Explicit state machine and retry worker; lifecycle policy can provide a secondary aging control | Authenticated reports that outlive application processes or need independent retention handling | More states to reconcile, and application transactions cannot atomically include object deletion |
| Database blob | Row metadata and bytes can share a database transaction | Logical and physical deletion can be coupled to the row, subject to the database's own backup and retention policy | Small bounded files where transactional simplicity is worth larger database backups and I/O | Report bytes compete with transactional data for database capacity and recovery operations |
| Local application disk | Usually one process or mounted-volume boundary | Deletion is simple only while node identity, replicas, backups, and failover remain simple | A single-node internal deployment whose loss and replacement model explicitly accepts that boundary | Horizontal replicas and ephemeral replacement turn file location into application state |

The selected design is not universally superior. The catch is the reconciliation loop: metadata and object operations cannot be one transaction, so the system must model the gap honestly. Stick with a database blob when reports are strictly bounded, the database backup policy is already the desired retention policy, and restoring byte data together with its row is the governing requirement. Stick with local disk for a deliberately single-node tool when losing or restoring that volume is an accepted system-level event. Neither exception should emerge accidentally from a framework's default upload directory.

Cost belongs in the review, but it is not the decision rule. Compare stored bytes, write and read operations, network transfer, backup amplification, restore time, operator attention, and the cost of proving deletion. The first invoice is visible; the reconciliation work often isn't.

## How should the critical upload and deletion path work?

Treat generation, publication, download, and deletion as commands against one report aggregate. Publication first writes an object under a unique key and then commits metadata that makes the version visible; an uncommitted object is invisible and can be found by a periodic orphan scan. Download never accepts a caller-supplied storage key. Deletion first commits the access-denying state and an outbox job in the same database transaction, after which a worker removes the exact object version and records completion.

That ordering creates a narrow, inspectable uncertainty window. It also means the happy path is not the only path represented in the schema.

The following Go sketch focuses on the deletion command because this is where a one-line `Delete` call usually hides the hardest correctness problem. Repository methods named `BeginDelete` and `MarkDeleted` are expected to use conditional updates; `BeginDelete` also writes the outbox row and audit transition in its database transaction. The storage interface is generic, and the worker treats an absent object as the desired end state, which makes retries safe without confusing transport success with application-level exactly-once execution.

```go
package reports

import (
	"context"
	"errors"
	"time"
)

var ErrObjectAbsent = errors.New("object absent")

type DeleteJob struct {
	TenantID     string
	ReportID     string
	ObjectKey    string
	ObjectVersion string
	RequestID    string
}

type Repository interface {
	// BeginDelete atomically denies reads, appends an audit event, and enqueues one job.
	BeginDelete(ctx context.Context, tenantID, reportID, requestID, actorID string) error
	MarkDeleted(ctx context.Context, job DeleteJob, deletedAt time.Time) error
}

type ObjectStore interface {
	DeleteVersion(ctx context.Context, key, version string) error
}

type Service struct {
	reports Repository
	objects ObjectStore
	now     func() time.Time
}

func (s *Service) RequestDeletion(
	ctx context.Context,
	tenantID, reportID, requestID, actorID string,
) error {
	// A unique (tenant_id, request_id) constraint makes client retries idempotent.
	return s.reports.BeginDelete(ctx, tenantID, reportID, requestID, actorID)
}

func (s *Service) RunDeleteJob(ctx context.Context, job DeleteJob) error {
	err := s.objects.DeleteVersion(ctx, job.ObjectKey, job.ObjectVersion)
	if err != nil && !errors.Is(err, ErrObjectAbsent) {
		return err
	}

	// This conditional transition is also safe when the worker delivers twice.
	return s.reports.MarkDeleted(ctx, job, s.now().UTC())
}
```

Exactly once is an invariant at the domain boundary, not a promise that the network performs one attempt. The unique request ID prevents duplicate command effects; the outbox prevents a committed denial from losing its physical-deletion job; version binding prevents an old job from deleting a replacement; conditional completion prevents duplicate audit transitions. The worker should emit counts for pending age, attempts, absent-object outcomes, and terminal completions, with tenant-safe identifiers that let an operator reconcile the database against storage inventory without putting filenames or report contents into telemetry.

Deployment needs the same discipline. Run the deletion worker independently from request latency, deploy schema changes before code that writes the new state, and test interruption after each boundary: after the state transaction commits, after object removal, and before completion is recorded. A useful test creates version A, requests its deletion, creates version B for the same logical report, delivers the A job twice, and verifies that B remains readable. That test is more valuable than a mock asserting that `DeleteVersion` was called once.

## Why reject direct disk writes for this report service?

Direct writes to the application server's upload directory were rejected because they bind durable customer data to process placement. In a single-node prototype, `os.Remove` feels wonderfully final. Add a second node, replace one instance during deployment, or restore a volume from backup, however, and the system needs a placement registry plus reconciliation semantics that resemble a small storage service. The apparent simplicity moved; it did not survive the failure model.

Local disk still has a valid use case. It is suitable for transient staging when the generated file has not yet been published, the path is outside the authenticated serving surface, cleanup is bounded, and a process or node loss merely causes regeneration. It can also be the final store for a consciously single-node internal application whose owner accepts that availability, recovery, and retention share one volume boundary. It is not suitable for customer reports when nodes are treated as replaceable but files are treated as durable.

Database blobs were also rejected for this particular service, though by a narrower margin. They make the metadata-byte transaction attractive, and for small bounded artifacts that may be the cleanest choice. Here, independent report retention and deletion operations matter more: keeping bytes outside the transactional database avoids making every report part of the same backup, restore, and capacity conversation as authorization and billing records. The decision should be revisited if report sizes become tightly capped, the storage worker becomes operationally disproportionate, or the database retention policy becomes exactly the required report policy.

The acceptance test is plain: after one idempotent command, access is denied immediately; retries converge on removal of the named immutable version; a replacement survives; and the audit record explains the transition without retaining the content. If a proposed storage design cannot demonstrate those properties under interruption, its upload convenience is beside the point.

## References

- MDN, “Content-Disposition”: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- Amazon S3 User Guide, “Managing the lifecycle of objects”: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
