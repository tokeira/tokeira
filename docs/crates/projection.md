# tokeira-projection

Projection worker and visibility-oriented sink abstractions. Projection is intentionally separated from the correctness path — a lagging projector is a quality problem, not a correctness failure. This crate has been expanded from a 137-line stub to a full visibility pipeline with real query support.

## Dependencies

- `tokeira-edge` — `VisibilityApi` trait, list/count request/response types
- `tokeira-storage` — `ProjectionLog`, `ProjectionRecord`, `ProjectionContext`
- `tokeira-types` — identity types, search attributes, execution status, projection cursors
- External: `anyhow`, `async-trait`, `async-recursion`, `base64`, `ordered-float`, `serde`, `serde_json`, `time`, `tokio`, `tokio-util`, `tracing`, `uuid`

## Module Structure

| File | Contents |
|---|---|
| `types.rs` | `ExecutionRow`, `AttrId`, `AttrDescriptor`, `SearchAttrType` (7 variants), `FilterExpr` (And/Or/Compare/In/Between/StartsWith), `FieldRef`, `SystemField` (9 fields), `CompareOp`, `FilterValue`, `CompiledFilter`, `SortOrder`, `PageToken` (base64-encoded), `PageBounds`, `RollupDimension`, `RollupDelta`, `RollupCounter`, `GroupByField`, `ListResult`, `CountResult` |
| `store.rs` | `VisibilityStore` trait — upsert/delete execution, search attr index CRUD, rollup accumulation, list/count queries, checkpoint persistence, attr registry |
| `memory.rs` | `InMemoryVisibilityStore` — full implementation with typed search attribute indexes (keyword, keyword_list, int, bool, double, datetime, text), rollup counters, filter evaluation engine |
| `filter.rs` | `compile_filter()` — parses Temporal list-filter syntax into `CompiledFilter` AST. Supports AND/OR, comparison operators (=, !=, <, <=, >, >=), IN, BETWEEN, STARTS_WITH. Resolves system fields and custom search attributes |
| `query_service.rs` | `VisibilityQueryService<S>` implementing `VisibilityApi` — list with pagination, count with group-by, rollup-accelerated counts for status/type/queue |
| `visibility_sink.rs` | `VisibilitySink<S>` — applies `ProjectionRecord`s: upserts execution rows, indexes search attributes with type validation, computes rollup deltas |
| `sink.rs` | `ProjectionSink` trait — `apply(record)` contract for idempotent record processing |
| `worker.rs` | `ProjectionWorker<L, S>` — drives one projection substream with `run_once()` and `run_from_cursor()` (continuous loop with backoff, cancellation, checkpoint persistence) |
| `rollup.rs` | `compute_rollup_deltas()` — computes +1/-1 deltas for ExecutionStatus, WorkflowType, TaskQueue dimensions |

## VisibilityStore Trait

| Method | Purpose |
|---|---|
| `upsert_execution` / `delete_execution` | Execution row CRUD |
| `upsert_search_attr_index` / `remove_search_attr_index` | Typed search attribute index management |
| `accumulate_rollup` | Apply rollup deltas |
| `list_executions` | Filtered, sorted, paginated query |
| `count_executions` | Filtered count with optional group-by |
| `count_from_rollup` | Fast count from pre-computed rollup counters |
| `load_checkpoint` / `save_checkpoint` | Sink checkpoint persistence |
| `resolve_attr` / `register_attr` | Search attribute registry |
| `get_row` | Direct row lookup by run_key |

## InMemoryVisibilityStore

Full in-memory implementation with:

- Per-type search attribute indexes: keyword, keyword_list, int, bool, double, datetime, text (with token-based text indexing)
- Filter evaluation engine supporting all `FilterExpr` variants
- Multi-field sorting (Default, StartTimeAsc/Desc, CloseTimeAsc/Desc)
- Base64-encoded page tokens for cursor-based pagination
- Rollup counter accumulation and dimension-scoped reads
- Checkpoint persistence per sink ID
- Attribute registry with auto-incrementing IDs

## Filter Compiler

`compile_filter()` parses Temporal's list-filter language:

- System fields: WorkflowId, RunId, WorkflowType, TaskQueue, ExecutionStatus, StartTime, CloseTime, HistoryLength, StateTransitionCount
- Custom search attributes resolved via `VisibilityStore::resolve_attr()`
- Type validation ensures filter values match field types
- Recursive AND/OR composition

## ProjectionWorker

Continuous loop with:

- Batch reads from `ProjectionLog`
- Idempotent apply via `ProjectionSink`
- Checkpoint persistence after each successful batch
- Exponential backoff on idle or failure (100ms → 5s)
- `CancellationToken` support for graceful shutdown
- Does not advance checkpoint on sink errors

## Tests

Property tests (proptest) covering: apply correctness, idempotent apply, search attribute indexing, index update cleanup, filter expression round-trip, rollup delta conservation, rollup determinism, checkpoint round-trip, checkpoint-after-apply invariant, row-to-summary mapping. Plus unit tests for query service pagination, count/group-by, and visibility sink behavior.
