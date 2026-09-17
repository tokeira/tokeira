# Tasks: CHASM extension visibility

- [x] 1. Add generic projection handle, DTOs, input errors and scope-bound cursors.
  - _Requirements: 2.1–2.5, 3.1–3.5_
  - DONE (2026-09-17): `component_query` owns the read-only fixed-scope handle,
    generic summary/page DTOs and input/store errors. Versioned continuations bind
    namespace, archetype, trimmed query and default order. Registry lookup failures
    retain their source instead of being mislabeled as invalid caller predicates.
- [x] 2. Hydrate DSQL component-list attributes in the row transaction; preserve empty values.
  - _Requirements: 2.2, 2.6, 4.4_
  - DONE (2026-09-17): A component-only list path selects rows and hydrates encoded
    attribute values in one DSQL transaction snapshot. Non-workflow empty lists and
    tokenless text retain an image without introducing a searchable element; the
    original Temporal list mapping and workflow empty-value writes stay unchanged.
- [x] 3. Wire the feature-gated engine accessor over both bootstrap stores and document usage.
  - _Requirements: 1.1–1.4_
  - DONE (2026-09-17): `Engine::chasm_visibility<C>(NamespaceId)` resolves the root
    through the registry and shares the bootstrap store on memory and DSQL paths.
    Public types are re-exported through `chasm`; engine documentation records
    authorization, error, pagination and eventual-consistency boundaries.
- [x] 4. Property 1: generated scoped list/count agreement.
  - _Requirements: 2.1, 2.3, 4.2_
  - DONE (2026-09-17): Generated mixed namespaces/archetypes, tombstones and integer
    attributes prove count equals full traversal and OR predicates cannot escape scope.
- [x] 5. Property 2: generated page traversal and cursor confinement.
  - _Requirements: 3.1–3.3, 4.2_
  - DONE (2026-09-17): Generated page sizes and cursor mutations prove no loss or
    duplicates over unchanged rows and rejection of changed scope/query/version.
    Fixed cases cover bad predicate types, unknown attributes, malformed cursors and
    page-size bounds. All five focused projection checks pass.
- [x] 6. Property 3: generated fidelity/versioning and real adapter repair proof.
  - _Requirements: 2.2, 2.6, 4.3_
  - DONE (2026-09-17): Generated arbitrary status keywords, lifecycle and attributes
    preserve the newer projected version after a stale apply. The acceptance runtime
    rebuilds an empty derived store, with startup-equivalent attribute seeding, through
    the real registry and repair scanner and queries an identical page twice. No
    embedded snapshot restart mechanism is introduced.
- [x] 7. Public builder consumer proof and env-gated live DSQL test.
  - _Requirements: 1.1–1.3, 4.1, 4.4_
  - DONE (2026-09-17): All 15 acceptance tests pass, including public-builder listing
    after success/failure, generation values, open lifecycle, other-namespace exclusion
    and separate activity counts. Unregistered roots fail at the engine boundary.
    The live DSQL fixture covers all seven attribute types, empty/tokenless values,
    pagination, isolation, stale apply, attribute removal and tombstones. No live test
    database URL is configured; database assertions require the env-gated suite.
  - DSQL Playground verification (2026-09-17): Executed the existing visibility and
    registry table definitions and synthetic inserts at
    [AWS DSQL Playground](https://playground.dsql.demo.aws). The new hydration
    `SELECT DISTINCT` with its namespace-qualified join and `ANY(uuid[])` returned
    four encoded attribute images from five index cells, including empty-list and
    tokenless-text images. Scoped default-order listing returned two rows, the
    keyset continuation returned the remaining row, and unfiltered/typed-integer
    counts returned two/one while excluding the other archetype. Empty images
    matched no invented keyword/text value; actual keywords still matched. Fixed
    UUID and array literals substituted for bound parameters; this validates SQL execution,
    not Rust driver binding, codec decoding or concurrent snapshot behavior.
- [x] 8. Changelog, focused/default/feature checks and full workspace finish bar.
  - _Requirements: 2.5, 4.1–4.4_
  - DONE (2026-09-17): `cargo +nightly fmt --all`, `cargo lint --locked`, workspace
    check, nextest, doctests and warning-denied documentation all pass. Workspace
    nextest reports 3,475 passed/2 skipped; the separate default-feature engine run
    reports 106 passed. Engine/projection feature Clippy and engine check pass with
    `chasm-extensions,dsql-integration`; the engine/projection/acceptance feature run
    reports 215 passed/1 skipped. Its existing
    `restart_rearms_retry_and_fires_only_at_its_deadline` test received one nextest
    process-leak diagnostic and passed cleanly without the diagnostic on an isolated
    serial rerun. Feature-enabled engine documentation passes with warnings denied.
    The changelog dry run and whitespace check pass. Live Rust-to-DSQL assertions
    remain unexecuted without a configured database, as recorded above; the separate
    operator-invoked Temporal corpus was not run. Native test linking emits the
    existing macOS deployment-target warnings; toolchain/cache configuration is unchanged.

## Task dependency graph

```text
1 → 2, 3 → 4, 5, 6, 7 → 8
```

## Notes

Owner explicitly excludes embedded in-memory snapshot restart recovery. This slice
adds no Deployment-specific engine type and does not change the v1.31.0 compatibility pin.
