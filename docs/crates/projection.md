# tokeira-projection

Derived read-model plane for workflow and CHASM visibility. It consumes
versioned projection records, materializes searchable execution rows and
rollups, and serves list and count queries.

## Where it sits

Projection is outside the correctness path. A delayed or rebuildable projection
affects query freshness, not the authoritative history or current execution
state.

## Surface map

| Area | Representative contracts |
|---|---|
| Consumption | `ProjectionWorker`, `ProjectionLog`, per-partition checkpoints, cancellation and retry |
| Application | `ProjectionSink`, `VisibilitySink`, monotonic version checks, deletion tombstones |
| Storage API | `VisibilityStore`, `InMemoryVisibilityStore`, feature-gated `DsqlVisibilityStore` |
| Query API | `VisibilityApi`, `VisibilityQueryService`, workflow and standalone-activity list/count types |
| Filtering | Temporal list-filter parser, typed `CompiledFilter`, schedule-filter compiler |
| Visibility schema | System attributes, custom attribute registry, typed search-attribute indexes |
| Aggregates | Rollup deltas and counts by execution status, workflow type, task queue, and archetype |

## SQL-native advanced visibility

`DsqlVisibilityStore` implements the visibility store and projection sink over
Aurora DSQL. Filter compilation produces typed query plans over system fields
and registered search attributes; queries do not depend on an external search
service. The in-memory store follows the same semantic interfaces for embedded
and test use.

## Invariants

- A sink applies only snapshots newer than the stored authority epoch and source
  transition sequence.
- Deletion retains a tombstone high-water mark so delayed older records cannot
  recreate a visible execution.
- A worker advances its checkpoint only after its sink applies the batch.
- Search-attribute values are normalized and type-checked against the namespace
  registry before indexing.
- Rollups are derived from old and new row membership and can be rebuilt.
- Visibility never decides whether a workflow or CHASM transition committed.

## It does not own

The crate does not persist authoritative history, schedule workflow work,
deliver tasks, or define Temporal wire messages. The edge exposes its query
results; storage supplies the projection log and DSQL connection foundation.

## Pointers

- [Crate root](../../crates/tokeira-projection/src/lib.rs)
- [DSQL visibility store](../../crates/tokeira-projection/src/dsql_store.rs)
- [Architecture overview](../architecture/000-overview.md)
- [Storage](storage.md)
- [Compatibility edge](edge.md)
