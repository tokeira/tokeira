# Requirements Document

## Introduction

This spec implements the DSQL-backed visibility query surface for Tokeira. The `dsql-projection-persistence` spec already implemented the write path (`ProjectionSink::apply`, checkpoint management, `vis_execution` upsert/close). This spec completes the read path and the remaining write-path stubs by replacing the `bail!("projection-visibility spec")` stubs in `DsqlVisibilityStore` (in `tokeira-projection/src/dsql_store.rs`) with real DSQL implementations.

The scope covers five areas:

1. **Query methods (read path)** — `list_executions`, `count_executions`, `count_from_rollup`, `get_row` against DSQL `vis_execution` with filter-to-SQL compilation, cursor-based pagination, and rollup-accelerated counts.
2. **Search attribute registry** — `resolve_attr` and `register_attr` against a new `sa_registry` table.
3. **Search attribute indexing (write path)** — `upsert_search_attr_index` and `remove_search_attr_index` maintaining typed side-index tables and `sa_current`.
4. **Rollup accumulation** — `accumulate_rollup` updating pre-aggregated rollup counters by dimension.
5. **Schema (new DDL)** — Migration files for `sa_registry`, `sa_current`, typed index tables, rollup table, and additional indexes on `vis_execution`.

The authoritative architecture documents are [070-projection-plane](../../../docs/architecture/070-projection-plane.md) and [080-sql-visibility](../../../docs/architecture/080-sql-visibility.md). The `InMemoryVisibilityStore` in `tokeira-projection/src/memory.rs` is the behavioral reference for all store methods. The `VisibilityStore` trait in `tokeira-projection/src/store.rs` defines the contract.

### What This Spec Covers

| Component | Table(s) | Description |
|---|---|---|
| `list_executions` | `vis_execution`, typed index tables | Namespace-scoped list queries with filter compilation, pagination, sort |
| `count_executions` | `vis_execution`, typed index tables | Count queries with optional GROUP BY on system or custom fields |
| `count_from_rollup` | `vis_rollup` | Fast counts from pre-aggregated rollup table |
| `get_row` | `vis_execution` | Single-row lookup by run_key |
| `resolve_attr` | `sa_registry` | Lookup `(namespace_id, attr_name)` → `AttrDescriptor` |
| `register_attr` | `sa_registry` | Insert/upsert `(namespace_id, attr_name, attr_type)` → `AttrId` |
| `upsert_search_attr_index` | `sa_current`, typed index tables | Maintain typed side-index entries for custom search attribute values (all 7 types) |
| `remove_search_attr_index` | `sa_current`, typed index tables | Clean up typed side-index entries when search attributes are removed |
| `accumulate_rollup` | `vis_rollup` | Update pre-aggregated rollup counters with ±1 deltas |
| Filter-to-SQL compiler | — | Translate `CompiledFilter` / `FilterExpr` tree into DSQL-compatible SQL |
| Schema DDL | New migration files | `sa_registry`, `sa_current`, typed index tables, `vis_rollup`, indexes |

### What This Spec Does NOT Cover

- **Projection worker/consumer loop** — already implemented in `tokeira-projection/src/worker.rs`.
- **Checkpoint management** — `load_checkpoint` and `save_checkpoint` already implemented.
- **`vis_execution` upsert/delete** — `upsert_execution` and `delete_execution` already implemented.

### What This Spec Updates From `dsql-projection-persistence`

- **`ProjectionSink::apply`** — the existing implementation in `DsqlVisibilityStore` ignores `search_attr_patch` and never calls `accumulate_rollup`. This spec updates `apply` to mirror the generic `VisibilitySink` flow: resolve and index search attributes, compute rollup deltas, and accumulate them. Without this update, the new search-attribute tables and `vis_rollup` would be unused by the live projection worker.

### Dependencies

- `dsql-projection-persistence` — `DsqlVisibilityStore` struct, `vis_execution` DDL, checkpoint methods, `upsert_execution`, `delete_execution`, `ProjectionSink::apply`.
- `dsql-schema-connection` (Feature 1) — `DsqlConnectionDirector`, codec module, migration runner.
- `tokeira-projection` — `VisibilityStore` trait, `CompiledFilter`, `FilterExpr`, `FieldRef`, `FilterValue`, `CompareOp`, `SortOrder`, `PageBounds`, `PageToken`, `ListResult`, `CountResult`, `GroupByField`, `RollupDimension`, `RollupDelta`, `RollupCounter`, `AttrDescriptor`, `AttrId`, `SearchAttrType`, `SystemField`, `ExecutionRow`.

### Key DSQL Constraints Shaping This Design

- **OCC with Repeatable Read** — queries are read-only but writes (rollup, search attribute) are subject to OCC conflict detection at commit time.
- **No temp tables** — use CTEs and subqueries per DSQL migration guide.
- **One DDL per transaction** — each migration file contains exactly one DDL statement.
- **`CREATE INDEX ASYNC`** — non-blocking index creation for all new indexes.
- **`DbClass::Projection`** — all operations in this spec use `DbClass::Projection` connections.
- **Schema version 1** — new DDL files, not ALTER TABLE migrations.

## Glossary

- **DsqlVisibilityStore**: The DSQL-backed `VisibilityStore` implementation in `tokeira-projection/src/dsql_store.rs`. This spec replaces its stub methods with real DSQL implementations.
- **VisibilityStore**: The trait in `tokeira-projection/src/store.rs` defining the full visibility storage contract: execution CRUD, search attribute registry, typed index management, rollup accumulation, and query methods.
- **CompiledFilter**: A compiled filter expression tree produced by the filter compiler in `tokeira-projection/src/filter.rs`. Contains an optional `FilterExpr` root.
- **FilterExpr**: An enum representing filter operations: `And`, `Or`, `Compare`, `In`, `Between`, `StartsWith`. Each leaf references a `FieldRef` and `FilterValue`.
- **FieldRef**: A reference to either a `SystemField` (column on `vis_execution`) or a `Custom` search attribute (resolved via `sa_registry`, queried via typed index tables).
- **FilterValue**: A typed literal value used in filter comparisons: `String`, `Int`, `Float`, `Bool`, `Datetime`, `Status`.
- **PageToken**: An opaque base64-encoded cursor encoding `(close_time, start_time, run_key, sort_order)` for stable keyset pagination.
- **PageBounds**: A struct carrying `limit` (capped at `MAX_PAGE_SIZE = 1000`) and an optional `after` `PageToken`.
- **ListResult**: The return type of `list_executions`: a `Vec<ExecutionRow>` and an optional `PageToken` for the next page.
- **CountResult**: The return type of `count_executions` and `count_from_rollup`: a `total_count` and a `Vec<RollupCounter>` for group-by results.
- **GroupByField**: An enum referencing either a `SystemField` or a `Custom` search attribute for group-by aggregation.
- **RollupDimension**: An enum with variants `ExecutionStatus`, `WorkflowType`, `TaskQueue` — the low-cardinality dimensions tracked by rollup counters.
- **RollupDelta**: A struct carrying `(namespace_id, dimension, value, delta)` for ±1 rollup counter adjustments.
- **AttrDescriptor**: A struct carrying `(attr_id, attr_type)` returned by `resolve_attr`.
- **AttrId**: A newtype `u64` identifier for a registered search attribute.
- **SearchAttrType**: An enum with variants `Keyword`, `KeywordList`, `Int`, `Bool`, `Double`, `Datetime`, `Text`.
- **sa_registry**: The DSQL table mapping `(namespace_id, attr_name)` to `(attr_id, attr_type)`.
- **sa_current**: The DSQL table tracking the current value of each custom search attribute per `(run_key, attr_id)`. Used by `remove_search_attr_index` to look up the previous value for index cleanup.
- **Typed_Index_Tables**: Per-type side-index tables (`sa_keyword_idx`, `sa_keyword_list_idx`, `sa_int_idx`, `sa_bool_idx`, `sa_datetime_idx`, `sa_double_idx`, `sa_text_token_idx`) enabling selective predicate evaluation on custom search attributes.
- **vis_rollup**: The DSQL table storing pre-aggregated rollup counters keyed by `(namespace_id, dimension, value)`.
- **Filter_SQL_Compiler**: The component that translates a `CompiledFilter` / `FilterExpr` tree into DSQL-compatible SQL WHERE clauses. System fields map to `vis_execution` columns directly. Custom search attribute predicates join against typed side-index tables.
- **ExecutionStatus**: Enum in `tokeira-types` with stable `SMALLINT` encoding via `to_db_smallint()` / `TryFrom<i16>`.
- **DbClass::Projection**: The connection class used for all operations in this spec, keeping projection traffic separate from authoritative commit traffic.

## Requirements

### Requirement 1: Schema DDL — Search Attribute Registry

**User Story:** As a Tokeira developer, I want a `sa_registry` table in DSQL, so that custom search attribute names can be resolved to stable identifiers and types for query compilation and index routing.

#### Acceptance Criteria

1. THE migration SHALL create a `sa_registry` table with columns: `attr_id` (BIGINT, primary key), `namespace_id` (UUID, NOT NULL), `attr_name` (TEXT, NOT NULL), and `attr_type` (SMALLINT, NOT NULL).
2. THE migration SHALL create a unique index on `(namespace_id, attr_name)` to enforce one registration per attribute name per namespace.
3. THE `attr_id` column SHALL use application-generated identifiers (not BIGSERIAL) because DSQL does not support BIGSERIAL.
4. THE `attr_type` column SHALL store a stable numeric encoding of `SearchAttrType` variants.
5. EACH DDL statement SHALL be in its own migration file, following the one-DDL-per-transaction DSQL constraint.
6. ALL indexes SHALL use `CREATE INDEX ASYNC` for non-blocking creation.

### Requirement 2: Schema DDL — Search Attribute Current Value

**User Story:** As a Tokeira developer, I want a `sa_current` table in DSQL, so that the visibility store can track the current value of each custom search attribute per execution for index cleanup during updates.

#### Acceptance Criteria

1. THE migration SHALL create a `sa_current` table with columns: `run_key` (UUID, NOT NULL), `attr_id` (BIGINT, NOT NULL), and `value_data` (BYTEA, NOT NULL).
2. THE table SHALL have a composite primary key on `(run_key, attr_id)`.
3. THE `value_data` column SHALL store the postcard-serialized `SearchAttrValue` for the current attribute value.

### Requirement 3: Schema DDL — Typed Index Tables

**User Story:** As a Tokeira developer, I want typed side-index tables in DSQL, so that custom search attribute predicates can be evaluated efficiently by type.

#### Acceptance Criteria

1. THE migrations SHALL create the following typed index tables, each with a composite primary key on `(namespace_id, attr_id, value, run_key)`:
   - `sa_keyword_idx` with `value` column of type TEXT
   - `sa_keyword_list_idx` with `value` column of type TEXT (stores individual elements from the keyword list)
   - `sa_int_idx` with `value` column of type BIGINT
   - `sa_bool_idx` with `value` column of type BOOLEAN
   - `sa_datetime_idx` with `value` column of type TIMESTAMPTZ
   - `sa_double_idx` with `value` column of type DOUBLE PRECISION
   - `sa_text_token_idx` with `value` column of type TEXT (stores individual lowercase tokens)
2. ALL tables SHALL include `namespace_id` (UUID, NOT NULL), `attr_id` (BIGINT, NOT NULL), and `run_key` (UUID, NOT NULL) columns.
3. EACH table creation SHALL be in its own migration file.

### Requirement 4: Schema DDL — Rollup Table

**User Story:** As a Tokeira developer, I want a `vis_rollup` table in DSQL, so that pre-aggregated counts by low-cardinality dimensions can be maintained for fast count queries.

#### Acceptance Criteria

1. THE migration SHALL create a `vis_rollup` table with columns: `namespace_id` (UUID, NOT NULL), `dimension` (SMALLINT, NOT NULL), `value` (TEXT, NOT NULL), and `counter` (BIGINT, NOT NULL DEFAULT 0).
2. THE table SHALL have a composite primary key on `(namespace_id, dimension, value)`.
3. THE `dimension` column SHALL store a stable numeric encoding of `RollupDimension` variants: `ExecutionStatus = 0`, `WorkflowType = 1`, `TaskQueue = 2`.

### Requirement 5: Schema DDL — Additional Indexes on vis_execution

**User Story:** As a Tokeira developer, I want additional indexes on `vis_execution` for query performance, so that namespace-scoped list queries with common filter patterns are efficient.

#### Acceptance Criteria

1. THE migrations SHALL create indexes to support the common query patterns: namespace + execution status, namespace + task queue, and namespace + start time ordering.
2. ALL indexes SHALL use `CREATE INDEX ASYNC` for non-blocking creation.
3. EACH index creation SHALL be in its own migration file.

### Requirement 6: Search Attribute Registry — resolve_attr

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::resolve_attr` to look up a search attribute descriptor from DSQL, so that the filter compiler can resolve custom attribute names to typed descriptors for query compilation.

#### Acceptance Criteria

1. WHEN a `(namespace_id, attr_name)` pair exists in `sa_registry`, THE DsqlVisibilityStore SHALL return the corresponding `AttrDescriptor` containing the `attr_id` and `attr_type`.
2. WHEN a `(namespace_id, attr_name)` pair does not exist in `sa_registry`, THE DsqlVisibilityStore SHALL return `None`.
3. THE DsqlVisibilityStore SHALL decode the `attr_type` SMALLINT column back to a `SearchAttrType` variant.
4. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
5. FOR ALL `SearchAttrType` variants, encoding to SMALLINT and then decoding SHALL produce the original variant (round-trip property).

### Requirement 7: Search Attribute Registry — register_attr

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::register_attr` to insert or return an existing search attribute registration, so that the projection sink can register attributes during apply and the filter compiler can resolve them during queries.

#### Acceptance Criteria

1. WHEN a `(namespace_id, attr_name)` pair does not exist in `sa_registry`, THE DsqlVisibilityStore SHALL insert a new row with an application-generated `attr_id` and the provided `attr_type`, and return the new `AttrId`.
2. WHEN a `(namespace_id, attr_name)` pair already exists in `sa_registry`, THE DsqlVisibilityStore SHALL return the existing `AttrId` without modifying the row.
3. THE DsqlVisibilityStore SHALL generate `attr_id` values deterministically by hashing `(namespace_id, attr_name)` to a positive i64 (e.g., via `dsql_spread_uuid` lower 63 bits). The same `(namespace_id, attr_name)` always produces the same `attr_id`, making primary-key collisions cryptographically unlikely. IF a primary-key conflict occurs where the existing row has a different `(namespace_id, attr_name)`, THE implementation SHALL return a clear error indicating an `attr_id` hash collision.
4. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
5. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument`.

### Requirement 8: Search Attribute Indexing — upsert_search_attr_index

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::upsert_search_attr_index` to maintain typed side-index entries in DSQL, so that custom search attribute predicates can be evaluated against indexed data.

#### Acceptance Criteria

1. WHEN `upsert_search_attr_index` is called, THE DsqlVisibilityStore SHALL upsert the current value into `sa_current` for the `(run_key, attr_id)` pair, storing the postcard-serialized `SearchAttrValue`.
2. WHEN `upsert_search_attr_index` is called, THE DsqlVisibilityStore SHALL insert a row into the appropriate typed index table based on the `attr_type` parameter.
3. THE DsqlVisibilityStore SHALL support the following type-to-table mappings: Keyword → `sa_keyword_idx`, KeywordList → `sa_keyword_list_idx`, Int → `sa_int_idx`, Bool → `sa_bool_idx`, Datetime → `sa_datetime_idx`, Double → `sa_double_idx`, Text → `sa_text_token_idx`.
4. WHEN a KeywordList value is indexed, THE DsqlVisibilityStore SHALL insert one row per element into `sa_keyword_list_idx`.
5. WHEN a Text value is indexed, THE DsqlVisibilityStore SHALL tokenize the value into lowercase alphanumeric tokens and insert one row per token into `sa_text_token_idx`.
6. THE typed index table inserts SHALL use `INSERT ... ON CONFLICT DO NOTHING` to handle idempotent re-application.
7. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
8. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument`.

### Requirement 9: Search Attribute Indexing — remove_search_attr_index

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::remove_search_attr_index` to clean up typed side-index entries in DSQL, so that stale index entries do not produce incorrect query results.

#### Acceptance Criteria

1. WHEN `remove_search_attr_index` is called, THE DsqlVisibilityStore SHALL read the current value from `sa_current` for the `(run_key, attr_id)` pair.
2. WHEN a current value exists, THE DsqlVisibilityStore SHALL delete the corresponding entries from the appropriate typed index table based on the `attr_type` parameter.
3. WHEN a KeywordList value is being removed, THE DsqlVisibilityStore SHALL delete all per-element rows from `sa_keyword_list_idx`.
4. WHEN a Text value is being removed, THE DsqlVisibilityStore SHALL tokenize the stored value and delete all corresponding token rows from `sa_text_token_idx`.
5. THE DsqlVisibilityStore SHALL delete the `sa_current` row for the `(run_key, attr_id)` pair.
6. WHEN no current value exists in `sa_current` for the `(run_key, attr_id)` pair, THE DsqlVisibilityStore SHALL return `Ok(())` without error.
7. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.

### Requirement 10: Rollup Accumulation — accumulate_rollup

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::accumulate_rollup` to update pre-aggregated rollup counters in DSQL, so that `count_from_rollup` can serve fast counts without scanning `vis_execution`.

#### Acceptance Criteria

1. WHEN `accumulate_rollup` is called with a slice of `RollupDelta` entries, THE DsqlVisibilityStore SHALL upsert each entry into the `vis_rollup` table, adding the delta to the existing counter value.
2. THE upsert SHALL use `INSERT ... ON CONFLICT DO UPDATE SET counter = vis_rollup.counter + EXCLUDED.counter` to atomically apply the delta.
3. THE `dimension` column SHALL be encoded as a stable SMALLINT: `ExecutionStatus = 0`, `WorkflowType = 1`, `TaskQueue = 2`.
4. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
5. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument`.

### Requirement 11: Filter-to-SQL Compilation

**User Story:** As a Tokeira developer, I want a filter-to-SQL compiler that translates `CompiledFilter` / `FilterExpr` trees into DSQL-compatible SQL WHERE clauses, so that `list_executions` and `count_executions` can execute filtered queries against DSQL.

#### Acceptance Criteria

1. THE Filter_SQL_Compiler SHALL translate `FilterExpr::Compare` nodes on system fields into SQL comparisons against `vis_execution` columns, using parameterized bind values.
2. THE Filter_SQL_Compiler SHALL translate `FilterExpr::Compare` nodes on custom search attributes of scalar types (Keyword, Int, Bool, Datetime, Double) into subqueries against the appropriate typed index table, selecting `run_key` values that match the predicate. All operators (`Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`) are supported for scalar types.
3. THE Filter_SQL_Compiler SHALL translate predicates on KeywordList attributes using element-membership semantics against `sa_keyword_list_idx`. Supported operators: `Eq` (any element equals the value), `Ne` (no element equals the value, implemented with `NOT EXISTS` rather than `value <> ...`), `In` (any element is in the set), `StartsWith` (any element starts with the prefix). Range operators (`Lt`, `Le`, `Gt`, `Ge`, `Between`) on KeywordList SHALL be rejected with a descriptive error because ordering over a multi-value set is undefined.
4. THE Filter_SQL_Compiler SHALL translate predicates on Text attributes using normalized token-matching semantics against `sa_text_token_idx`. Query literals are normalized with the same tokenizer/lowercasing used when populating `sa_text_token_idx` (lowercase alphanumeric token extraction). For `Eq` and `Ne`, the query literal is valid only when it normalizes to exactly one token; zero-token or multi-token literals compile/evaluate as no match for `Eq` and as match for `Ne`. For `In`, each candidate literal is normalized independently and candidates that do not normalize to exactly one token are ignored; an empty normalized candidate set matches no rows. Supported operators: `Eq` (any token equals the normalized literal), `Ne` (no token equals the normalized literal, implemented with `NOT EXISTS` rather than `value <> ...`), `In` (any token equals any normalized literal in the set), `StartsWith` (any token starts with the lowercased and LIKE-escaped prefix). Range operators (`Lt`, `Le`, `Gt`, `Ge`, `Between`) on Text SHALL be rejected with a descriptive error because ordering over tokens is undefined.
5. THE Filter_SQL_Compiler SHALL translate `FilterExpr::And` into SQL `AND`, `FilterExpr::Or` into SQL `OR`.
6. THE Filter_SQL_Compiler SHALL translate `FilterExpr::In` into SQL `IN (...)` for system fields, into a typed index subquery with `value IN (...)` for scalar custom attributes, and into element/token-membership subqueries for KeywordList and Text.
7. THE Filter_SQL_Compiler SHALL translate `FilterExpr::Between` into SQL `BETWEEN ... AND ...` for system fields and scalar custom attributes. `Between` on KeywordList or Text SHALL be rejected with a descriptive error.
8. THE Filter_SQL_Compiler SHALL translate `FilterExpr::StartsWith` into SQL `LIKE 'prefix%'` with the prefix properly escaped (escaping `%` → `\%`, `_` → `\_`, `\` → `\\`, using `ESCAPE '\'`) for system fields, and into typed index subqueries with the same pattern for Keyword, KeywordList, and Text attributes.
9. THE Filter_SQL_Compiler SHALL map system fields to `vis_execution` columns: `WorkflowId` → `workflow_id`, `RunId` → `run_id`, `WorkflowType` → `workflow_type`, `TaskQueue` → `task_queue`, `ExecutionStatus` → `execution_status`, `StartTime` → `start_time`, `CloseTime` → `close_time`, `HistoryLength` → `history_length`, `StateTransitionCount` → `state_transition_count`.
10. THE Filter_SQL_Compiler SHALL encode `FilterValue::Status` values using `ExecutionStatus::to_db_smallint()` for SQL comparison against the `execution_status` SMALLINT column.
11. THE Filter_SQL_Compiler SHALL use parameterized queries exclusively — no string interpolation of filter values into SQL text.
12. FOR ALL `CompiledFilter` trees containing system-field, scalar-custom-attribute, or supported KeywordList/Text predicates, the compiled SQL SHALL produce the same result set as the corrected `InMemoryVisibilityStore` filter evaluator for the same data (behavioral equivalence property).

### Requirement 12: List Executions Query — list_executions

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::list_executions` to execute namespace-scoped list queries against DSQL with filtering, sorting, and cursor-based pagination, so that the `VisibilityQueryService` can serve `list_workflows` requests.

#### Acceptance Criteria

1. WHEN `list_executions` is called, THE DsqlVisibilityStore SHALL query `vis_execution` rows filtered by `namespace_id` and the compiled filter's SQL WHERE clause.
2. THE query SHALL apply the requested `SortOrder`: default (`close_time DESC NULLS LAST, start_time DESC, run_key DESC` — matching Rust `Option::cmp` where `None < Some`, so closed executions sort before open ones in descending order), `StartTimeAsc` (`start_time ASC, run_key ASC`), `StartTimeDesc` (`start_time DESC, run_key DESC`), `CloseTimeAsc` (`close_time ASC NULLS FIRST, run_key ASC`), `CloseTimeDesc` (`close_time DESC NULLS LAST, run_key DESC`).
3. WHEN a `PageToken` is provided in `PageBounds`, THE DsqlVisibilityStore SHALL add a cursor predicate to the WHERE clause that excludes rows at or before the token's sort-key tuple, using row-value comparison for stable keyset pagination.
4. THE query SHALL use `LIMIT` equal to `page.limit + 1` (where `page.limit` is capped at `MAX_PAGE_SIZE`). The extra row is used to detect whether more results exist.
5. WHEN the query returns more than `limit` rows, THE DsqlVisibilityStore SHALL construct a `PageToken` from the `limit`-th row's sort-key tuple, return only the first `limit` rows, and include the token in the `ListResult`.
6. WHEN the query returns `limit` or fewer rows, THE DsqlVisibilityStore SHALL return `None` for `next_page_token`.
7. THE DsqlVisibilityStore SHALL decode each result row into an `ExecutionRow`, including `ExecutionStatus` via `TryFrom<i16>` and `Memo` via postcard deserialization.
8. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
9. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument`.

### Requirement 13: Count Executions Query — count_executions

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::count_executions` to execute namespace-scoped count queries against DSQL with optional GROUP BY, so that the `VisibilityQueryService` can serve `count_workflows` requests.

#### Acceptance Criteria

1. WHEN `count_executions` is called without a `group_by` field, THE DsqlVisibilityStore SHALL return the total count of `vis_execution` rows matching the namespace and compiled filter.
2. WHEN `count_executions` is called with a `group_by` field referencing a system field, THE DsqlVisibilityStore SHALL return per-group counts using SQL `GROUP BY` on the corresponding `vis_execution` column.
3. WHEN `count_executions` is called with a `group_by` field referencing a custom search attribute of a scalar type (Keyword, Int, Bool, Datetime, Double), THE DsqlVisibilityStore SHALL join against the appropriate typed index table and group by the attribute value.
4. WHEN `count_executions` is called with a `group_by` field referencing a multi-value custom search attribute (KeywordList, Text), THE DsqlVisibilityStore SHALL return an error because multi-value index tables contain one row per element/token, which would produce incorrect group counts.
5. THE DsqlVisibilityStore SHALL decode `ExecutionStatus` group-by values from SMALLINT back to the `Debug` format string (e.g., `"Running"`, `"Completed"`) to match the `InMemoryVisibilityStore` behavior.
6. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
7. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument`.

### Requirement 14: Count from Rollup — count_from_rollup

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::count_from_rollup` to serve fast counts from the pre-aggregated `vis_rollup` table, so that unfiltered count queries on low-cardinality dimensions avoid scanning `vis_execution`.

#### Acceptance Criteria

1. WHEN `count_from_rollup` is called, THE DsqlVisibilityStore SHALL query the `vis_rollup` table for all rows matching the `namespace_id` and `dimension` (encoded via `to_db_smallint()`).
2. THE DsqlVisibilityStore SHALL sum the `counter` values to produce `total_count` and return individual `(value, counter)` pairs as `RollupCounter` groups.
3. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
4. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument`.

### Requirement 15: Get Row — get_row

**User Story:** As a Tokeira developer, I want `DsqlVisibilityStore::get_row` to return a single `ExecutionRow` by `run_key` from DSQL, so that the visibility sink and query service can look up individual executions.

#### Acceptance Criteria

1. WHEN a `vis_execution` row exists for the given `run_key`, THE DsqlVisibilityStore SHALL return the decoded `ExecutionRow`.
2. WHEN no `vis_execution` row exists for the given `run_key`, THE DsqlVisibilityStore SHALL return `None`.
3. THE DsqlVisibilityStore SHALL decode the row using the same `row_to_execution` helper already used by the existing `get_execution_row` function in `dsql_store.rs`.
4. IF a DSQL error occurs during the query, THE DsqlVisibilityStore SHALL log a warning and return `None` (matching the trait signature which returns `Option`, not `Result`).

### Requirement 16: SearchAttrType Stable Numeric Mapping

**User Story:** As a Tokeira developer, I want a stable numeric mapping for `SearchAttrType` variants to SMALLINT, so that `sa_registry.attr_type` values are durable and consistent across code changes.

#### Acceptance Criteria

1. THE SearchAttrType type SHALL provide a `to_db_smallint` method returning a stable `i16` value for each variant.
2. THE SearchAttrType type SHALL provide a `TryFrom<i16>` implementation that decodes the SMALLINT back to the enum variant.
3. THE numeric mapping SHALL be: `Keyword = 0`, `KeywordList = 1`, `Int = 2`, `Bool = 3`, `Double = 4`, `Datetime = 5`, `Text = 6`.
4. WHEN an unknown `i16` value is encountered during decoding, THE implementation SHALL return an explicit error type.
5. FOR ALL `SearchAttrType` variants, encoding to `i16` and then decoding SHALL produce the original variant (round-trip property).

### Requirement 17: RollupDimension Stable Numeric Mapping

**User Story:** As a Tokeira developer, I want a stable numeric mapping for `RollupDimension` variants to SMALLINT, so that `vis_rollup.dimension` values are durable and consistent across code changes.

#### Acceptance Criteria

1. THE RollupDimension type SHALL provide a `to_db_smallint` method returning a stable `i16` value for each variant.
2. THE RollupDimension type SHALL provide a `TryFrom<i16>` implementation that decodes the SMALLINT back to the enum variant.
3. THE numeric mapping SHALL be: `ExecutionStatus = 0`, `WorkflowType = 1`, `TaskQueue = 2`.
4. WHEN an unknown `i16` value is encountered during decoding, THE implementation SHALL return an explicit error type.
5. FOR ALL `RollupDimension` variants, encoding to `i16` and then decoding SHALL produce the original variant (round-trip property).

### Requirement 18: Tracing Instrumentation

**User Story:** As a Tokeira developer, I want all new DSQL visibility methods instrumented with tracing, so that operational issues can be diagnosed from structured logs.

#### Acceptance Criteria

1. THE DsqlVisibilityStore SHALL annotate all newly implemented `VisibilityStore` trait methods with `tracing::instrument`.
2. THE instrumentation SHALL include relevant parameters (namespace_id, run_key, attr_id, dimension) as span fields where appropriate, excluding large serialized payloads and filter trees.
3. THE Filter_SQL_Compiler SHALL NOT be instrumented at the per-node level, but the top-level compilation function SHALL emit a tracing span.

### Requirement 19: Fix InMemoryVisibilityStore KeywordList and Text Filter Semantics

**User Story:** As a Tokeira developer, I want the in-memory visibility store to use correct Temporal-compatible filter semantics for KeywordList and Text search attributes, so that both the in-memory and DSQL stores implement the same behavioral contract and behavioral equivalence tests are meaningful.

#### Acceptance Criteria

1. WHEN evaluating a filter predicate on a KeywordList attribute with `Eq` or `Ne`, THE InMemoryVisibilityStore SHALL use element-membership semantics: `CustomKeywordList = "a"` matches if any element in the stored list equals `"a"`, and `CustomKeywordList != "a"` matches only if no element in the stored list equals `"a"`. The current behavior of joining the list with `","` and comparing the joined string is incorrect.
2. WHEN evaluating a filter predicate on a KeywordList attribute with `IN`, THE InMemoryVisibilityStore SHALL match if any element in the stored list equals any value in the `IN` set.
3. WHEN evaluating a `StartsWith` predicate on a KeywordList attribute, THE InMemoryVisibilityStore SHALL match if any element in the stored list starts with the prefix.
4. WHEN evaluating a range operator (`Lt`, `Le`, `Gt`, `Ge`) or `Between` predicate on a KeywordList attribute, THE InMemoryVisibilityStore SHALL return an unsupported-operator error, matching the DSQL compiler's rejection.
5. WHEN evaluating a filter predicate on a Text attribute with `Eq` or `Ne`, THE InMemoryVisibilityStore SHALL use token-matching semantics: the query literal is normalized with the same tokenizer/lowercasing used for indexing, `CustomText = "word"` matches if any token extracted from the stored text equals the normalized literal, and `CustomText != "word"` matches only if no token equals the normalized literal. If the query literal normalizes to zero or multiple tokens, `Eq` returns no match and `Ne` returns match. The current behavior of comparing the full string is incorrect.
6. WHEN evaluating a filter predicate on a Text attribute with `IN`, THE InMemoryVisibilityStore SHALL normalize each literal in the set, ignore candidates that normalize to zero or multiple tokens, and match if any token extracted from the stored text equals any remaining normalized literal.
7. WHEN evaluating a `StartsWith` predicate on a Text attribute, THE InMemoryVisibilityStore SHALL lowercase the prefix and match if any token extracted from the stored text starts with the lowercased prefix.
8. WHEN evaluating a range operator (`Lt`, `Le`, `Gt`, `Ge`) or `Between` predicate on a Text attribute, THE InMemoryVisibilityStore SHALL return an unsupported-operator error, matching the DSQL compiler's rejection.
9. THE `search_attr_to_filter` function in `memory.rs` SHALL be replaced with type-aware filter evaluation that handles KeywordList and Text multi-value semantics directly in `eval_expr`, rather than collapsing multi-value attributes into a single `FilterValue::String`.
10. THE `group_value` function in `memory.rs` SHALL use element-membership semantics for KeywordList (joining with `","` is acceptable for group labels since group-by on KeywordList is rejected by Requirement 13.4) and full-string semantics for Text group labels.
