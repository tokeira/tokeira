# 070 Projection Plane

**Status:** draft for architecture review  
**Related docs:** [010-history-as-authority](010-history-as-authority.md), [075-archival-to-s3](075-archival-to-s3.md), [080-sql-visibility](080-sql-visibility.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Purpose

The projection plane is how Tokeira exposes *read models* without letting them participate in correctness.

It is responsible for:

- canonical visibility,
- operational rollups,
- optional custom sinks,
- replay, checkpointing, and backfill.

It is **not** responsible for deciding whether a workflow transition happened.

## Design claim

Tokeira should produce a **typed projection log** from authoritative run transitions and then let one or more sinks consume that log independently.

This generalizes Temporal’s documented separation between core persistence and visibility, and it fits naturally with Temporal’s existing ideas around Visibility and Dual Visibility.[^visibility][^dual-visibility]

## Why projections must be separate

Visibility is important operationally, but it is not the same as workflow correctness.

Temporal’s docs describe visibility as the subsystem that lets operators list, filter, and search Workflow Executions.[^visibility] The Web UI depends on this layer for browsing and inspection workflows.[^web-ui] That makes visibility very important, but it also means:

- visibility can lag slightly without changing workflow semantics,
- visibility can be rebuilt,
- different consumers may want different read models,
- the projection plane should scale and fail independently of the authoritative runtime.

## Projection envelope

Every committed authoritative transition should be able to emit one or more `ProjectionOp`s, which are then wrapped into a projection-log record with enough identity to replay safely.

A projection envelope should carry at least:

- namespace ID,
- workflow ID,
- run ID / run key,
- transition sequence,
- event/history counters needed for debugging,
- projection ops,
- partitioning metadata for replay.

The key design idea is that the projection plane should not have to *look back into runtime state* to understand what changed. The authoritative transition already knows that.

## Projection substreams

To keep DSQL-friendly write distribution, the projection log should not be one giant append point. Instead it should be partitioned into **substreams**:

```text
(partition_id, fanout, local_slot, run_key, transition_seq)
```

This has three nice properties:

1. unrelated runs can be spread across write ranges,
2. sinks can checkpoint per substream,
3. per-run ordering is still preserved because `transition_seq` remains monotonic within a run.

## Sinks

Tokeira should support multiple sink types behind one conceptual interface.

### Canonical sink

The default DSQL visibility store that satisfies Temporal-compatible list/filter/count behavior.

### Rollup sink

Low-cardinality aggregates for operator dashboards and capacity views.

### Archive / analytics sink

Examples:

- S3 archival/export objects,
- Parquet to object storage,
- columnar analytics backend,
- domain-specific warehouse tables.

### Custom domain sink

Tenant-specific or product-specific indexes that should not contaminate the canonical visibility schema.

## Checkpoint model

Each sink should keep an independent checkpoint per substream:

```text
checkpoint(sink_id, partition_id, fanout) = last_applied_cursor
```

That means:

- one lagging sink does not block another,
- backfill can be sink-local,
- replay can resume from a known prefix,
- sink implementation can evolve over time.

## Replay semantics

The right consistency model is:

> **Each sink sees a prefix of committed transitions in each assigned substream.**

That is stronger and more useful than “eventually something like the current state appears,” but it avoids forcing a global total order across unrelated runs.

## What belongs in the projection plane

The projection plane should own:

- sink registration,
- projection-log reading,
- batching and apply loops,
- checkpoint advancement,
- replay/backfill tooling,
- per-sink operational metrics.

It should **not** own:

- workflow mutation semantics,
- poller matching,
- shard ownership,
- queue backlog truth.

## Why this avoids Elasticsearch lock-in

Temporal’s current docs already support visibility on SQL backends and document Dual Visibility as a migration mechanism between visibility stores.[^visibility][^dual-visibility][^search-attributes] Tokeira extends that idea:

- the canonical sink is SQL-native,
- additional sinks are optional,
- no query or schema decision in the correctness core assumes Elasticsearch.

This matters because it keeps the architecture open-ended without making the entire system depend on one search product’s indexing model.

## Suggested crate responsibilities

`tokeira-projection` should contain:

- `model.rs`: projection op and envelope types,
- `partition.rs`: substream partitioning and cursor semantics,
- `sink.rs`: sink trait,
- `worker.rs`: projector worker loop,
- `dsql_visibility/`: canonical visibility applier and query support,
- `custom_sink/`: bridge adapters.

## Apply loop

A sink worker should operate roughly like this:

1. read next batch from one substream after checkpoint,
2. apply idempotently to sink storage,
3. persist the advanced checkpoint,
4. repeat.

This should be done in bounded batches with backpressure. Projection must never be allowed to starve authoritative storage traffic.

## Why ops should be typed

A projection op should be semantic and typed, for example:

- `UpsertExecution`
- `CloseExecution`
- `SetSearchAttr`
- `SetMemo`
- `DeleteExecution` (rare / retention flow)
- `IncrementRollup`

This is better than “ship the whole history event into sinks and let them infer meaning,” because:

- sinks stay simpler,
- replay stays cheaper,
- schema changes are more controlled,
- visibility logic remains decoupled from workflow replay.

## Backfill strategy

Backfill should happen in two modes:

### Projection-log replay

Preferred mode when projection log still exists for the desired range.

### Authoritative rebuild

Used when a sink must be rebuilt from farther back than retained projection log. In that case the rebuild reads canonical run state/history and synthesizes projection ops.

The second path is slower, but it preserves the principle that projections are rebuildable.

## Operational metrics

Each sink should report at least:

- batch apply latency,
- checkpoint lag by substream,
- failed apply count,
- replay throughput,
- backlog age/bytes.

These are the core signals for deciding whether the projection plane is healthy without conflating it with workflow correctness.

## Review questions

1. Should rollups live in the same projection log as canonical visibility, or should they derive from visibility rows instead?
2. How long should projection-log retention be before a sink must fall back to authoritative rebuild?
3. Should custom sinks be allowed to define their own partitioning, or must they consume the canonical substream layout?

## References

[^visibility]: Temporal Visibility docs: https://docs.temporal.io/visibility  
[^dual-visibility]: Temporal Dual Visibility docs: https://docs.temporal.io/dual-visibility  
[^search-attributes]: Temporal Search Attributes docs: https://docs.temporal.io/search-attribute  
[^web-ui]: Temporal Web UI docs: https://docs.temporal.io/web-ui
