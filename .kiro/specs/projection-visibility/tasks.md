# Implementation Plan: Projection Visibility (DSQL Query Surface)

## Overview

Replace the `bail!("projection-visibility spec")` stubs in `DsqlVisibilityStore` with real DSQL implementations. This covers stable numeric mappings for `SearchAttrType` and `RollupDimension`, 14 new migration files (V029–V042), search attribute registry and indexing, rollup accumulation, filter-to-SQL compilation, query methods (`list_executions`, `count_executions`, `count_from_rollup`, `get_row`), and updating `DsqlVisibilityStore::apply` to write search attributes and rollups. All new code goes into `tokeira-projection/src/dsql_store.rs` and `tokeira-projection/src/types.rs`, plus migration SQL files in `tokeira-storage/migrations/`.

## Tasks

- [ ] 1. Add `thiserror` dependency and stable numeric mappings
  - [ ] 1.0 Add `thiserror` to `tokeira-projection/Cargo.toml`
    - Add `thiserror.workspace = true` to `[dependencies]`
    - Required for `SearchAttrTypeDecodeError` and `RollupDimensionDecodeError` error types

  - [ ] 1.1 Add `SearchAttrType::to_db_smallint` and `TryFrom<i16>` to `tokeira-projection/src/types.rs`
    - Add `SearchAttrTypeDecodeError` error type with `thiserror::Error` derive, following the `ExecutionStatusDecodeError` pattern in `tokeira-types/src/execution.rs`
    - Add `impl SearchAttrType { pub fn to_db_smallint(self) -> i16 }` with mapping: `Keyword=0, KeywordList=1, Int=2, Bool=3, Double=4, Datetime=5, Text=6`
    - Add `impl TryFrom<i16> for SearchAttrType` with the reverse mapping, returning `SearchAttrTypeDecodeError` for unknown values
    - _Requirements: 16.1, 16.2, 16.3, 16.4_

  - [ ] 1.2 Add `RollupDimension::to_db_smallint` and `TryFrom<i16>` to `tokeira-projection/src/types.rs`
    - Add `RollupDimensionDecodeError` error type with `thiserror::Error` derive
    - Add `impl RollupDimension { pub fn to_db_smallint(self) -> i16 }` with mapping: `ExecutionStatus=0, WorkflowType=1, TaskQueue=2`
    - Add `impl TryFrom<i16> for RollupDimension` with the reverse mapping, returning `RollupDimensionDecodeError` for unknown values
    - _Requirements: 17.1, 17.2, 17.3, 17.4_

  - [ ] 1.3 Write stability tests for `SearchAttrType` and `RollupDimension` numeric mappings
    - Add `#[test] fn search_attr_type_database_encoding_is_stable()` asserting exact values for each variant and round-trip
    - Add `#[test] fn search_attr_type_rejects_unknown_database_values()` for values 7, -1, 100
    - Add `#[test] fn rollup_dimension_database_encoding_is_stable()` asserting exact values for each variant and round-trip
    - Add `#[test] fn rollup_dimension_rejects_unknown_database_values()` for values 3, -1, 100
    - _Requirements: 16.3, 16.4, 16.5, 17.3, 17.4, 17.5_

  - [ ] 1.4 Write property test for `SearchAttrType` round-trip (Property 1)
    - **Property 1: SearchAttrType Numeric Round-Trip**
    - **Validates: Requirements 16.1, 16.2, 16.5**
    - Use `proptest` with `prop_oneof!` over all `SearchAttrType` variants
    - Verify `TryFrom::<i16>::try_from(x.to_db_smallint()) == Ok(x)` for all generated variants
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/types.rs`

  - [ ] 1.5 Write property test for `RollupDimension` round-trip (Property 2)
    - **Property 2: RollupDimension Numeric Round-Trip**
    - **Validates: Requirements 17.1, 17.2, 17.5**
    - Use `proptest` with `prop_oneof!` over all `RollupDimension` variants
    - Verify `TryFrom::<i16>::try_from(x.to_db_smallint()) == Ok(x)` for all generated variants
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/types.rs`

- [ ] 2. Checkpoint — Ensure numeric mapping tests pass
  - Run `cargo test -p tokeira-projection` and verify all new stability and property tests pass.

- [ ] 3. Create migration files V029–V042 and update V017
  - [ ] 3.0 Update `V017__idx_vis_execution_ns_close.sql` in-place
    - Change `close_time DESC NULLS FIRST` to `close_time DESC NULLS LAST` to match the default sort order
    - Tokeira targets schema version 1 — in-place DDL update, no separate migration needed
    - _Requirements: 12.2_
  - [ ] 3.1 Create `V029__sa_registry.sql`
    - `CREATE TABLE IF NOT EXISTS sa_registry` with columns: `attr_id BIGINT NOT NULL PRIMARY KEY`, `namespace_id UUID NOT NULL`, `attr_name TEXT NOT NULL`, `attr_type SMALLINT NOT NULL`
    - _Requirements: 1.1, 1.3, 1.4, 1.5_

  - [ ] 3.2 Create `V030__idx_sa_registry_ns_name.sql`
    - `CREATE UNIQUE INDEX ASYNC idx_sa_registry_ns_name ON sa_registry (namespace_id, attr_name)`
    - _Requirements: 1.2, 1.6_

  - [ ] 3.3 Create `V031__sa_current.sql`
    - `CREATE TABLE IF NOT EXISTS sa_current` with columns: `run_key UUID NOT NULL`, `attr_id BIGINT NOT NULL`, `value_data BYTEA NOT NULL`, `PRIMARY KEY (run_key, attr_id)`
    - _Requirements: 2.1, 2.2, 2.3_

  - [ ] 3.4 Create `V032__sa_keyword_idx.sql` through `V038__sa_text_token_idx.sql`
    - Create 7 typed index tables, each with `PRIMARY KEY (namespace_id, attr_id, value, run_key)`:
      - `V032__sa_keyword_idx.sql` — `value TEXT NOT NULL`
      - `V033__sa_keyword_list_idx.sql` — `value TEXT NOT NULL`
      - `V034__sa_int_idx.sql` — `value BIGINT NOT NULL`
      - `V035__sa_bool_idx.sql` — `value BOOLEAN NOT NULL`
      - `V036__sa_datetime_idx.sql` — `value TIMESTAMPTZ NOT NULL`
      - `V037__sa_double_idx.sql` — `value DOUBLE PRECISION NOT NULL`
      - `V038__sa_text_token_idx.sql` — `value TEXT NOT NULL`
    - Each file contains exactly one `CREATE TABLE IF NOT EXISTS` statement
    - _Requirements: 3.1, 3.2, 3.3_

  - [ ] 3.5 Create `V039__vis_rollup.sql`
    - `CREATE TABLE IF NOT EXISTS vis_rollup` with columns: `namespace_id UUID NOT NULL`, `dimension SMALLINT NOT NULL`, `value TEXT NOT NULL`, `counter BIGINT NOT NULL DEFAULT 0`, `PRIMARY KEY (namespace_id, dimension, value)`
    - _Requirements: 4.1, 4.2, 4.3_

  - [ ] 3.6 Create `V040__idx_vis_execution_ns_status.sql`, `V041__idx_vis_execution_ns_start.sql`, and `V042__idx_vis_execution_ns_tq.sql`
    - `V040`: `CREATE INDEX ASYNC idx_vis_execution_ns_status ON vis_execution (namespace_id, execution_status)`
    - `V041`: `CREATE INDEX ASYNC idx_vis_execution_ns_start ON vis_execution (namespace_id, start_time DESC, run_key DESC)`
    - `V042`: `CREATE INDEX ASYNC idx_vis_execution_ns_tq ON vis_execution (namespace_id, task_queue)`
    - _Requirements: 5.1, 5.2, 5.3_

- [ ] 4. Checkpoint — Ensure compilation passes
  - Run `cargo check -p tokeira-storage` and `cargo check -p tokeira-projection --features dsql` and verify the new migration files don't break compilation.

- [ ] 4.5 Fix InMemoryVisibilityStore KeywordList and Text filter semantics
  - [ ] 4.5.1 Fix `eval_expr` in `memory.rs` for KeywordList element-membership
    - Replace the `search_attr_to_filter` path for KeywordList with direct element-membership evaluation in `eval_expr`
    - For `Compare(Eq, "a")`: match if any element in the stored `Vec<String>` equals `"a"`
    - For `Compare(Ne, "a")`: match if no element equals `"a"`
    - For `In(["a", "b"])`: match if any element is in the set
    - For `StartsWith("pre")`: match if any element starts with the prefix
    - For range operators (`Lt`, `Le`, `Gt`, `Ge`) and `Between`: return an unsupported-operator error, matching the DSQL compiler's rejection
    - _Requirements: 19.1, 19.2, 19.3_

  - [ ] 4.5.2 Fix `eval_expr` in `memory.rs` for Text token-matching
    - Replace the `search_attr_to_filter` path for Text with token-based evaluation in `eval_expr`
    - Extract tokens using the existing `InMemoryVisibilityStore::index_text` function
    - For `Compare(Eq, "word")`: match if any token equals `"word"`
    - For `Compare(Ne, "word")`: match only if no token equals `"word"`
    - For `Compare(Eq, literal)` where `literal` normalizes to zero or multiple tokens: return no match
    - For `Compare(Ne, literal)` where `literal` normalizes to zero or multiple tokens: return match
    - For `In(["word1", "word2"])`: match if any token is in the set
    - For `In([...])`: normalize each candidate independently and ignore candidates that normalize to zero or multiple tokens
    - For `StartsWith("pre")`: match if any token starts with the prefix
    - _Requirements: 19.4, 19.5, 19.6_

  - [ ] 4.5.3 Write unit tests for corrected KeywordList and Text filter semantics
    - Test KeywordList element-membership: `["a", "b"]` with filter `= "a"` → matches
    - Test KeywordList element-membership: `["a", "b"]` with filter `= "c"` → does not match
    - Test KeywordList Ne: `["a", "b"]` with filter `!= "a"` → does not match
    - Test KeywordList Ne: `["a", "b"]` with filter `!= "c"` → matches
    - Test KeywordList StartsWith: `["alpha", "beta"]` with filter `STARTS_WITH "al"` → matches
    - Test KeywordList In: `["a", "b"]` with filter `IN ("b", "c")` → matches
    - Test KeywordList range rejection: `["a", "b"]` with filter `> "a"` → returns unsupported-operator error
    - Test KeywordList Between rejection: `["a", "b"]` with filter `BETWEEN "a" AND "c"` → returns unsupported-operator error
    - Test Text token-matching: `"hello world"` with filter `= "hello"` → matches
    - Test Text token-matching: `"hello world"` with filter `= "hello world"` → does not match (not a single token)
    - Test Text Ne: `"hello world"` with filter `!= "hello"` → does not match
    - Test Text Ne: `"hello world"` with filter `!= "missing"` → matches
    - Test Text Ne with multi-token literal: `"hello world"` with filter `!= "hello world"` → matches (invalid equality candidate)
    - Test Text StartsWith: `"hello world"` with filter `STARTS_WITH "hel"` → matches
    - Test Text token-matching: `"Hello World"` with filter `= "hello"` → matches (tokens are lowercased)
    - Test Text In with mixed case: `"Hello World"` with filter `IN ["hello"]` → matches (normalized)
    - Test Text In with mixed case: `"Hello World"` with filter `IN ["HEL"]` → does not match (`"HEL"` normalizes to `"hel"`, no token equals `"hel"`)
    - Test Text range rejection: `"hello"` with filter `> "abc"` → returns unsupported-operator error
    - _Requirements: 19.1, 19.4, 19.5, 19.6, 19.8_

- [ ] 4.6 Checkpoint — Ensure in-memory filter fix tests pass
  - Run `cargo test -p tokeira-projection` and verify all tests pass including the new KeywordList/Text filter tests and existing property tests.

- [ ] 5. Implement search attribute registry methods
  - [ ] 5.1 Implement `resolve_attr` in `dsql_store.rs`
    - Replace the `unsupported("search-attribute descriptor lookup")` stub
    - Query `sa_registry` for `(namespace_id, attr_name)`, return `AttrDescriptor` with decoded `SearchAttrType` via `TryFrom<i16>`
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `namespace_id` and `name` as span fields
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 18.1, 18.2_

  - [ ] 5.2 Implement `register_attr` in `dsql_store.rs`
    - Replace the `unsupported("search-attribute registration")` stub
    - Generate `attr_id` deterministically by hashing `(namespace_id, attr_name)` to a positive i64 using `dsql_spread_uuid` lower 63 bits — same input always produces same ID, making PK collisions cryptographically unlikely
    - Use `INSERT INTO sa_registry ... ON CONFLICT (namespace_id, attr_name) DO NOTHING RETURNING attr_id`
    - If RETURNING yields no rows (conflict), follow up with `SELECT attr_id FROM sa_registry WHERE namespace_id = $1 AND attr_name = $2`
    - If the INSERT fails with a primary-key violation (different `(namespace_id, attr_name)` produced the same `attr_id` hash), return a clear collision error
    - Encode `attr_type` via `to_db_smallint()`
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `namespace_id` and `name` as span fields
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 18.1, 18.2_

  - [ ] 5.3 Write property test for attribute registry round-trip (Property 4)
    - **Property 4: Attribute Registry Register-Then-Resolve Round-Trip**
    - **Validates: Requirements 6.1, 7.1, 7.2**
    - Generate random `(NamespaceId, String, SearchAttrType)` tuples
    - Register via `InMemoryVisibilityStore`, resolve, verify descriptor matches
    - Register again, verify same `AttrId` returned
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/memory.rs`

- [ ] 6. Implement search attribute indexing methods
  - [ ] 6.1 Implement `upsert_search_attr_index` in `dsql_store.rs`
    - Replace the `unsupported("search-attribute index writes")` stub
    - Step 1: Upsert `sa_current` with postcard-serialized `SearchAttrValue` via `codec::encode`
    - Step 2: Insert into the appropriate typed index table based on `attr_type`:
      - `Keyword` → single row in `sa_keyword_idx`
      - `KeywordList` → one row per element in `sa_keyword_list_idx`
      - `Int` → single row in `sa_int_idx`
      - `Bool` → single row in `sa_bool_idx`
      - `Datetime` → single row in `sa_datetime_idx`
      - `Double` → single row in `sa_double_idx`
      - `Text` → tokenize (lowercase alphanumeric, deduplicate), one row per token in `sa_text_token_idx`
    - All typed index inserts use `INSERT ... ON CONFLICT DO NOTHING`
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `run_key`, `namespace_id`, `attr_id` as span fields
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 18.1, 18.2_

  - [ ] 6.2 Implement `remove_search_attr_index` in `dsql_store.rs`
    - Replace the `unsupported("search-attribute index deletes")` stub
    - Step 1: Read current value from `sa_current` via `SELECT value_data WHERE run_key = $1 AND attr_id = $2`; if no row, return `Ok(())`
    - Step 2: Deserialize the stored `SearchAttrValue` via `codec::decode`
    - Step 3: Delete from the appropriate typed index table based on `attr_type`, handling multi-row types (KeywordList, Text) by iterating elements/tokens
    - Step 4: Delete the `sa_current` row
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `run_key`, `namespace_id`, `attr_id` as span fields
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 18.1, 18.2_

  - [ ] 6.3 Write property test for `SearchAttrValue` serialization round-trip (Property 3)
    - **Property 3: SearchAttrValue Serialization Round-Trip**
    - **Validates: Requirements 2.3**
    - Generate random `SearchAttrValue` instances across all variants (Keyword, KeywordList, Int, Bool, Double, Datetime, Text)
    - Verify `codec::decode(codec::encode(x)) == x`
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/dsql_store.rs`

- [ ] 7. Implement rollup accumulation
  - [ ] 7.1 Implement `accumulate_rollup` in `dsql_store.rs`
    - Replace the `unsupported("visibility rollups")` stub
    - For each `RollupDelta` in the slice, execute `INSERT INTO vis_rollup (namespace_id, dimension, value, counter) VALUES ($1, $2, $3, $4) ON CONFLICT (namespace_id, dimension, value) DO UPDATE SET counter = vis_rollup.counter + EXCLUDED.counter`
    - Encode `dimension` via `RollupDimension::to_db_smallint()`
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with entry count as span field
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 18.1, 18.2_

- [ ] 8. Checkpoint — Ensure compilation passes with registry, indexing, and rollup implementations
  - Run `cargo check -p tokeira-projection --features dsql` and verify all new code compiles without errors.

- [ ] 8.5 Update `DsqlVisibilityStore::apply` to write search attributes and rollups
  - [ ] 8.5.1 Update `ProjectionSink::apply` in `dsql_store.rs` to mirror the `VisibilitySink` flow
    - The current `apply` ignores `search_attr_patch` and never calls `accumulate_rollup`
    - After processing ops, iterate over `search_attr_patch` entries: resolve each attribute via `self.resolve_attr`, remove old index via `self.remove_search_attr_index`, insert new index via `self.upsert_search_attr_index`
    - After writing the execution row, compute rollup deltas via `compute_rollup_deltas(previous, &row)` and call `self.accumulate_rollup(&deltas)`
    - Read the previous row via `self.get_row(record.run_key)` before applying ops, to compute correct rollup deltas
    - Reject unknown search attributes and type mismatches with descriptive errors, matching the `VisibilitySink` behavior
    - _Requirements: 8.1, 8.2, 10.1_

- [ ] 9. Implement filter-to-SQL compiler
  - [ ] 9.1 Implement `compile_filter_sql` pure function in `dsql_store.rs`
    - Define `SqlFragment { sql: String, values: Vec<SqlValue> }` and `SqlValue` enum
    - Implement `compile_filter_sql(expr: &FilterExpr, param_offset: usize) -> (SqlFragment, usize)` as a recursive pure function (no I/O, no async)
    - System field predicates compile to direct column comparisons on `vis_execution` using the column mapping from the design
    - Scalar custom attribute predicates (Keyword, Int, Bool, Datetime, Double) compile to `run_key IN (SELECT run_key FROM sa_{type}_idx WHERE ...)` subqueries
    - KeywordList custom attribute positive-membership predicates (`Eq`, `In`, `StartsWith`) compile to `run_key IN (SELECT run_key FROM sa_keyword_list_idx WHERE ...)`; `Ne` compiles to a correlated `NOT EXISTS` anti-semijoin so rows with mixed matching/non-matching elements are not false positives
    - Text custom attribute positive-membership predicates (`Eq`, `In`, `StartsWith`) compile to `run_key IN (SELECT run_key FROM sa_text_token_idx WHERE ...)`; `Ne` compiles to a correlated `NOT EXISTS` anti-semijoin so rows with mixed matching/non-matching tokens are not false positives
    - Text `Eq`/`Ne` literals that normalize to zero or multiple tokens compile to constant `FALSE`/`TRUE` respectively; Text `In` ignores candidates that normalize to zero or multiple tokens and compiles to `FALSE` if no candidates remain
    - Handle all `FilterExpr` variants: `And`, `Or`, `Compare`, `In`, `Between`, `StartsWith`
    - `ExecutionStatus` values encoded via `to_db_smallint()`
    - `RunId` cast to `TEXT` for string comparison: `run_id::TEXT`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 11.11_

  - [ ] 9.2 Implement LIKE prefix escaping helper
    - Escape `\` → `\\`, `%` → `\%`, `_` → `\_` in the prefix (backslash first to avoid double-escaping), then append `%`
    - Used by `FilterExpr::StartsWith` compilation
    - _Requirements: 11.8_

  - [ ] 9.3 Write unit tests for filter-to-SQL compiler
    - Test system field Compare: `WorkflowType = "Foo"` → SQL contains `workflow_type = $N`
    - Test system field status Compare: `ExecutionStatus = Running` → SQL uses `execution_status = $N` with smallint value
    - Test scalar custom attribute Compare: Keyword equality → SQL contains `run_key IN (SELECT run_key FROM sa_keyword_idx ...)`
    - Test KeywordList custom attribute Eq: → SQL contains `run_key IN (SELECT run_key FROM sa_keyword_list_idx ...)`
    - Test KeywordList custom attribute Ne: → SQL contains `NOT EXISTS` against `sa_keyword_list_idx`, not `value <>`
    - Test KeywordList custom attribute In: → SQL contains `sa_keyword_list_idx` with `value IN (...)`
    - Test KeywordList custom attribute StartsWith: → SQL contains `sa_keyword_list_idx` with `LIKE`
    - Test KeywordList custom attribute range (Lt): → returns descriptive error
    - Test KeywordList custom attribute Between: → returns descriptive error
    - Test Text custom attribute Eq: → SQL contains `run_key IN (SELECT run_key FROM sa_text_token_idx ...)` with normalized (lowercased) literal
    - Test Text custom attribute Eq with multi-token literal: → compiles to constant `FALSE`
    - Test Text custom attribute Ne: → SQL contains `NOT EXISTS` against `sa_text_token_idx`, not `value <>`
    - Test Text custom attribute Ne with multi-token literal: → compiles to constant `TRUE`
    - Test Text custom attribute In: → SQL contains `sa_text_token_idx` with normalized literals; `"Hello"` normalizes to `"hello"`
    - Test Text custom attribute In with multi-token candidates: → invalid candidates are ignored; all-invalid candidate set compiles to constant `FALSE`
    - Test Text custom attribute StartsWith: → SQL contains `sa_text_token_idx` with lowercased and LIKE-escaped prefix
    - Test Text custom attribute range (Gt): → returns descriptive error
    - Test And/Or composition: `A AND B` → SQL contains `AND`; `A OR B` → SQL contains `OR`
    - Test In clause: `WorkflowType IN ("A", "B")` → SQL contains `IN ($N, $M)`
    - Test Between clause: `StartTime BETWEEN t1 AND t2` → SQL contains `BETWEEN $N AND $M`
    - Test StartsWith: `WorkflowId STARTS_WITH "prefix"` → SQL contains `LIKE $N`
    - Test StartsWith with special chars: `"a%b_c"` → LIKE pattern is `a\%b\_c%`
    - Test empty filter: `CompiledFilter { expr: None }` → no WHERE clause fragment
    - Test LIKE escape edge cases: empty string → `%`, no special chars → `abc%`, all special chars → `\%\_\\%`
    - Test LIKE escape with backslash in prefix: `a\b` → `a\\b%`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9_

  - [ ] 9.4 Write property test for filter SQL parameterization safety (Property 5)
    - **Property 5: Filter SQL Compiler Parameterization Safety**
    - **Validates: Requirements 11.1, 11.9**
    - Generate random `FilterExpr` trees with system fields and various operators
    - Compile to SQL, verify: (a) SQL contains no literal filter values, (b) number of `$N` placeholders equals number of bind values, (c) parameter indices are sequential from offset
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/dsql_store.rs`

  - [ ] 9.5 Write property test for LIKE prefix escaping correctness (Property 6)
    - **Property 6: LIKE Prefix Escaping Correctness**
    - **Validates: Requirements 11.8**
    - Generate random strings including `%`, `_`, `\` characters
    - Pass through the LIKE escape function
    - Verify: (a) escaped string does not contain unescaped `%`, `_`, or `\` from the original prefix, (b) ends with exactly one `%` wildcard, (c) backslash is escaped first so `\%` in the input becomes `\\%` not `\\\%`
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/dsql_store.rs`

  - [ ] 9.6 Write property test for system field column mapping completeness (Property 7)
    - **Property 7: System Field to Column Name Mapping Completeness**
    - **Validates: Requirements 11.7**
    - Use `prop_oneof!` over all `SystemField` variants
    - Verify the column mapping function returns a non-empty string for each variant
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/dsql_store.rs`

- [ ] 10. Checkpoint — Ensure filter compiler tests pass
  - Run `cargo test -p tokeira-projection` and verify all filter compiler unit tests and property tests pass.

- [ ] 11. Implement query methods
  - [ ] 11.1 Implement `list_executions` in `dsql_store.rs`
    - Replace the `unsupported("visibility list queries")` stub
    - Build SQL query: `SELECT ... FROM vis_execution WHERE namespace_id = $1 AND {filter_where_clause} AND {cursor_predicate} ORDER BY {sort_clause} LIMIT $N`
    - Use `compile_filter_sql` for the filter WHERE clause
    - Implement keyset pagination with cursor predicates per sort order:
      - `Default`: `ORDER BY close_time DESC NULLS LAST, start_time DESC, run_key DESC` (matching Rust `Option::cmp` where `None < Some`)
      - `StartTimeAsc/Desc`, `CloseTimeAsc/Desc` with corresponding row-value comparisons
    - Decode rows via `row_to_execution` helper (already exists)
    - Fetch `limit + 1` rows, return first `limit`, emit `PageToken` only when extra row exists
    - Bind `SqlValue` variants to the query using sqlx
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `namespace_id` and `sort` as span fields
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 18.1, 18.2_

  - [ ] 11.2 Implement `count_executions` in `dsql_store.rs`
    - Replace the `unsupported("visibility count queries")` stub
    - Without group_by: `SELECT COUNT(*) FROM vis_execution WHERE namespace_id = $1 AND {filter_where_clause}`
    - With system field group_by: `SELECT {group_column}, COUNT(*) FROM vis_execution WHERE ... GROUP BY {group_column}`
    - With custom scalar attribute group_by (Keyword, Int, Bool, Datetime, Double): LEFT JOIN against typed index table, GROUP BY native typed value, format group labels in Rust through a shared helper matching `InMemoryVisibilityStore::group_value` (not SQL `::TEXT` casts)
    - With custom multi-value attribute group_by (KeywordList, Text): return an error — multi-value index tables contain one row per element/token, which would produce incorrect group counts
    - Decode `ExecutionStatus` group values from SMALLINT to `Debug` format string (e.g., `"Running"`)
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `namespace_id` as span field
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 18.1, 18.2_

  - [ ] 11.3 Implement `count_from_rollup` in `dsql_store.rs`
    - Replace the `unsupported("visibility rollup count queries")` stub
    - Query `SELECT value, counter FROM vis_rollup WHERE namespace_id = $1 AND dimension = $2`
    - Encode dimension via `to_db_smallint()`
    - Sum counters for `total_count`, return individual `(value, counter)` pairs as `RollupCounter` groups
    - Use `DbClass::Projection` for connection acquisition
    - Add `#[instrument]` with `namespace_id` and `dimension` as span fields
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 18.1, 18.2_

  - [ ] 11.4 Fix `get_row` to use existing `get_execution_row` helper
    - The current `get_row` implementation already acquires a connection and calls `get_execution_row`, but the design notes it should be wired through properly
    - Verify the existing implementation correctly returns the row from `get_execution_row` on the happy path instead of returning `None`
    - No `#[instrument]` needed — the existing implementation already has tracing
    - _Requirements: 15.1, 15.2, 15.3, 15.4_

- [ ] 12. Checkpoint — Ensure compilation passes with all query methods
  - Run `cargo check -p tokeira-projection --features dsql` and verify all new query method code compiles without errors.

- [ ] 13. Write unit tests for query methods and remaining edge cases
  - [ ] 13.1 Write unit tests for `SqlValue` binding and `SqlFragment` construction
    - Verify `compile_filter_sql` produces correct parameter offsets when chaining multiple predicates
    - Verify cursor predicate SQL generation for each `SortOrder` variant
    - _Requirements: 12.2, 12.3_

  - [ ] 13.2 Write unit tests for `count_executions` group-by SQL generation
    - Verify system field group-by produces correct `GROUP BY` column
    - Verify `ExecutionStatus` group-by decodes SMALLINT to `Debug` format string
    - Verify custom scalar attribute group-by produces correct JOIN SQL
    - Verify custom KeywordList or Text group-by returns an error
    - _Requirements: 13.2, 13.3, 13.4, 13.5_

  - [ ] 13.3 Write unit tests for search attribute indexing edge cases
    - Verify `remove_search_attr_index` with no `sa_current` row returns `Ok(())`
    - Verify text tokenization edge cases: empty string → no tokens, all-whitespace → no tokens, single word → one lowercase token
    - Verify attr_id generation is deterministic: same `(namespace_id, attr_name)` always produces the same positive i64 value
    - _Requirements: 9.5, 8.5_

- [ ] 14. Checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-projection` and `cargo test -p tokeira-projection --features dsql` and verify all tests pass including property tests and unit tests.

- [ ] 15. Integration tests (gated behind `dsql-integration` feature)
  - [ ] 15.0 Add `dsql-integration` feature to `tokeira-projection/Cargo.toml`
    - Add `dsql-integration = ["dsql"]` to `[features]`
    - Integration tests use `#[cfg(feature = "dsql-integration")]`

  - [ ] 15.1 Integration test: `register_attr` idempotence
    - Register same `(namespace_id, attr_name, attr_type)` twice, verify same `AttrId` returned
    - _Requirements: 7.1, 7.2_

  - [ ] 15.2 Integration test: `resolve_attr` round-trip
    - Register an attribute, resolve it, verify `AttrDescriptor` matches
    - Resolve a non-existent attribute, verify `None`
    - _Requirements: 6.1, 6.2_

  - [ ] 15.3 Integration test: `upsert_search_attr_index` then `list_executions` with custom filter
    - Insert a `vis_execution` row, register a Keyword attribute, index a value
    - Query with a filter on that keyword, verify the row appears
    - _Requirements: 8.1, 8.2, 8.3, 12.1_

  - [ ] 15.4 Integration test: `remove_search_attr_index` then `list_executions`
    - Index a value, remove it, query with filter, verify the row no longer appears
    - _Requirements: 9.1, 9.2, 9.5_

  - [ ] 15.5 Integration test: `list_executions` pagination cycle
    - Insert multiple `vis_execution` rows, paginate through all pages
    - Verify all rows returned in correct order and total count matches
    - _Requirements: 12.2, 12.3, 12.4, 12.5, 12.6_

  - [ ] 15.6 Integration test: `count_executions` with group_by
    - Insert rows with different statuses, count with `GROUP BY ExecutionStatus`
    - Verify group counts match expected values
    - _Requirements: 13.1, 13.2, 13.4_

  - [ ] 15.7 Integration test: `accumulate_rollup` and `count_from_rollup`
    - Accumulate rollup deltas (+1 and -1), query rollup counts, verify final counters
    - _Requirements: 10.1, 10.2, 14.1, 14.2_

  - [ ] 15.8 Integration test: behavioral equivalence
    - For a fixed dataset including KeywordList and Text search attributes, run the same queries against both `InMemoryVisibilityStore` and `DsqlVisibilityStore`
    - Verify identical results for `list_executions`, `count_executions`, and `count_from_rollup`
    - Include KeywordList element-membership and Text token-matching filter predicates
    - _Requirements: 11.12, 19.1, 19.4_

- [ ] 16. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-projection` and `cargo test -p tokeira-projection --features dsql` and verify all tests pass including property tests and unit tests.
  - If DSQL is available, also run `cargo test -p tokeira-projection --features dsql-integration` to verify integration tests pass.

## Notes

- All tests are required — none are marked optional per project convention.
- The filter-to-SQL compiler is a pure function (no I/O, no async) and can be tested independently without DSQL.
- `get_row` already has a working implementation — task 11.4 verifies it's correctly wired through.
- `DsqlVisibilityStore::apply` must be updated (task 8.5) to write search attributes and rollups, mirroring the generic `VisibilitySink` flow. Without this, the new tables would be unused by the live projection worker.
- Property tests use `proptest` with minimum 100 iterations, already a dev-dependency of `tokeira-projection`.
- Integration tests are gated behind the `dsql-integration` feature flag (defined as `dsql-integration = ["dsql"]`).
- All DSQL operations use `DbClass::Projection` connections.
- 14 migration files (V029–V042) follow the one-DDL-per-transaction DSQL constraint.
- Text tokenization in `upsert_search_attr_index` and `remove_search_attr_index` must match the `InMemoryVisibilityStore::index_text` reference implementation.
- Default sort order uses `close_time DESC NULLS LAST` to match Rust's `Option::cmp` where `None < Some` — closed executions sort before open ones in descending order.
- Pagination uses `LIMIT limit + 1` to detect whether more results exist, returning only the first `limit` rows.
