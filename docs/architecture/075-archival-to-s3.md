# 075 Archival to S3

**Status:** draft for architecture review  
**Related docs:** [000-overview](000-overview.md), [025-system-services](025-system-services.md), [050-dsql-storage](050-dsql-storage.md), [070-projection-plane](070-projection-plane.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Purpose

Temporal’s self-hosted archival feature backs up **closed Workflow Execution histories and visibility records** from core persistence to blob storage.[^temporal-archival] Tokeira should support the same category of outcome, but with a design that fits the rest of the architecture:

- archival is **asynchronous**,
- archival is **outside the hot correctness path**,
- archival is **S3-native**,
- archival is **compatible with generous hot retention in DSQL**.

The important point is this:

> **Tokeira should archive because long-tail durability and retention matter, not because DSQL must be pruned aggressively on every close.**

## Design claim

Tokeira should implement archival as a **separate archival service** that exports closed execution data from DSQL to **immutable S3 objects**, records an archival manifest, and only later allows hot-state pruning according to retention policy.

## What should be archived

For a closed execution, the archive set should contain at least:

- execution identity:
  - namespace,
  - workflow ID,
  - run ID,
  - run key,
- execution summary:
  - workflow type,
  - task queue,
  - status,
  - start/close timestamps,
  - history length,
- visibility/search-attribute snapshot sufficient for lookup,
- full event history,
- memo and selected metadata,
- manifest metadata:
  - schema version,
  - archive time,
  - checksums,
  - object list,
  - compression format.

The archive should be self-describing enough that an operator or tool can inspect it without reconstructing the full runtime state.

## What should not happen on the hot path

A workflow close transition should **not** synchronously write to S3.

The hot path should only:

1. commit the close transition in DSQL,
2. emit an archival candidate or close-export intent,
3. return.

Everything else happens later.

This is critical because archival touches external object storage, compression, object layout, retries, and possible multipart upload behavior. None of that belongs on the close-path latency budget.

## Service shape

### `tokeira-archival`

Plain service, internal only.

Responsibilities:

- read archive candidates,
- load closed execution summary and history from DSQL,
- build archive object set,
- upload to S3,
- write archival manifest/marker back to DSQL,
- coordinate later retention pruning.

This service should have independent admission, backoff, and throughput limits.

### Optional orchestration via `tokeira-system`

For very large exports, verification, retries across time, or compliance approval steps, `tokeira-archival` may choose to start or advance a system workflow in `tokeira-system`.

That gives you durable progress tracking without forcing every simple archive export through a workflow runtime.

## Recommended data flow

```text
workflow close
  -> close transition commits in DSQL
  -> archive candidate emitted
  -> archival service claims candidate
  -> read summary/history from DSQL
  -> build compressed archive bundle + manifest
  -> PUT to S3
  -> mark archive success in DSQL
  -> later retention sweep prunes hot data if policy allows
```

## S3 object model

S3 now provides strong read-after-write and list consistency, which makes manifest-based archive writes much simpler than older eventual-consistency assumptions.[^s3-consistency]

I would use a manifest-oriented object layout such as:

```text
s3://<bucket>/<env>/<namespace>/<workflow_id>/<run_id>/
  manifest.json
  summary.json.zst
  history/00000001.jsonl.zst
  history/00000002.jsonl.zst
  visibility.json.zst
```

A few recommendations:

- make object keys deterministic,
- write data objects first,
- write `manifest.json` last as the commit marker,
- include checksums and sizes in the manifest,
- keep schema versions explicit.

That lets readers treat `manifest.json` as the durable statement that the archive is complete.

## Compression and upload strategy

The history payload is the dominant size driver. Tokeira should therefore:

- export history in compressed chunks,
- keep chunk sizes bounded,
- use multipart upload when object size warrants it.

S3 multipart upload is explicitly designed for large objects and lets failed parts be retried independently.[^s3-multipart]

## Retention model

Tokeira should separate **archive** from **purge**.

Recommended states:

- `not_archived`
- `archiving`
- `archived`
- `purge_eligible`
- `purged`

That gives a safe sequence:

1. close execution in DSQL,
2. archive to S3,
3. verify archive success,
4. optionally keep hot DSQL copy for a policy window,
5. purge hot data later.

This is safer than conflating “closed” with “may be deleted from DSQL now.”

## Why DSQL retention can remain generous

Aurora DSQL storage does cost money, but if storage cost is acceptable, there is no reason to force a very aggressive prune policy just to justify archival.

That implies a healthy default stance:

- keep a meaningful hot retention window in DSQL,
- use S3 for longer-term retention and durability,
- let operators retrieve recent closed runs without immediately going to archival storage,
- use lifecycle tiering in S3 for older data.

S3 Lifecycle can transition older objects to lower-cost storage classes and delete expired objects according to policy.[^s3-lifecycle][^s3-transition]

## Bucket policy recommendations

I would default to:

- dedicated archive bucket per environment,
- SSE-KMS,
- versioning enabled,
- lifecycle rules,
- optional Object Lock for regulated environments.

S3 Versioning preserves multiple versions of objects and helps recover from unintended overwrites or deletes.[^s3-versioning] S3 Object Lock can prevent deletion or overwrite for a fixed period or indefinitely when regulatory posture requires it.[^s3-object-lock]

## Archive candidate source

There are two reasonable ways to feed the archival service:

### Close-transition outbox / projection log

Best fit if the archival candidate is already a typed derived effect of close.

### Retention scanner

Best fit if archival is driven by “closed and older than X” rather than “archive immediately after close.”

My recommendation is a hybrid:

- emit a cheap close marker immediately,
- let archival claim it when policy says it is time.

That keeps archival policy flexible without rereading every closed run blindly.

## Retrieval model

Archived history should be retrievable through admin/operator tooling.

Recommended behavior:

- normal recent queries continue to use DSQL visibility,
- archival-aware admin tooling can fetch the S3 manifest and history,
- optional import or rehydrate tools can reconstruct a closed execution view from archive.

The archive should not need to be restored into DSQL just to inspect it.

## Failure handling

Archival must be idempotent.

Rules:

- archive object names are deterministic,
- `manifest.json` is written last,
- re-running archival for the same run is safe,
- prune is never allowed unless archive success is recorded,
- incomplete multipart uploads must be cleaned up by lifecycle policy.

S3 Lifecycle can abort incomplete multipart uploads after a configured number of days, which helps control stray storage costs.[^s3-abort-mpu]

## What belongs in the archive manifest

At minimum:

- archive schema version,
- execution identity,
- archival timestamp,
- object list with checksums,
- compression codec,
- source history length,
- visibility snapshot hash,
- originating Tokeira version or exporter version.

This is the minimum needed for later verification and migration tooling.

## Interaction with projection plane

Archival is adjacent to the projection plane, but not identical to it.

- visibility is a query-optimized read model,
- archival is a long-term retention export.

They may share candidate sources and some metadata, but they should remain separate services because they optimize for different things.

## Review questions

1. Should the default policy archive immediately on close, or only after a configurable grace period?
2. Do we want one-object-per-run archives for small histories, or always a chunked layout?
3. Should archive manifests live only in S3, or should DSQL also store a compact archive record for lookup?
4. Which admin APIs should become archive-aware from the first milestone?

## References

[^temporal-archival]: Temporal self-hosted Archival docs: https://docs.temporal.io/self-hosted-guide/archival  
[^s3-consistency]: Amazon S3 strong consistency: https://aws.amazon.com/s3/consistency/  
[^s3-multipart]: Amazon S3 multipart upload overview: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html  
[^s3-lifecycle]: Amazon S3 lifecycle management: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html  
[^s3-transition]: Amazon S3 lifecycle transition considerations: https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html  
[^s3-versioning]: Amazon S3 Versioning: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html  
[^s3-object-lock]: Amazon S3 Object Lock considerations: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html  
[^s3-abort-mpu]: Abort incomplete multipart uploads with S3 Lifecycle: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html
