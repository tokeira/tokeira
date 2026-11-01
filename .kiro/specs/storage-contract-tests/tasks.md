# Implementation Plan: Storage Contract Tests

This plan implements the approved [requirements](requirements.md) and approved
[design](design.md). Both documents are approved for implementation. A checked
task must represent implemented and verified work, not a planned test selector.

- [x] 1. Add shared fixture, adapter and replay support
  - [x] 1.1 Register the test-only `contract_tests` module and define the small
    `ContractBackend`, fixture scope, input cases and complete durable observation.
    Reuse existing fixture constructors and oracle helpers; leave their existing
    shared suites intact. Derive expected effects from authored inputs.
    - _Requirements: 1.2, 1.3, 1.4, 5.1, 5.2, 6.1_
  - [x] 1.2 Add the memory adapter under `memory/contract_tests.rs` and the
    feature-gated DSQL adapter under `dsql/run_repository/contract_tests.rs`.
    Implement scoped inspection and cleanup, fresh identities per trial/shrink,
    reserved homes and commit-compatible projection partitions. Fail on foreign
    data. Preserve backend-specific audit, timestamp, encoding and legacy policies.
    Keep connection pool sizes equal and idle eviction disabled.
    - _Requirements: 1.3, 1.5, 5.2, 6.1, 8.8, 8.9_
  - [x] 1.3 Add ignored live entry-point setup with a strict test URL gate. Add
    proptest runner helpers with explicit replay seeds and per-property/backend
    case receipts using existing dependencies. Preserve the original error when
    cleanup fails. Unit-check missing configuration and receipt completeness
    without connecting or printing configuration values.
    - _Requirements: 8.1, 8.2, 8.4, 8.5, 8.7, 8.9_
  - _Requirements: 1.2, 1.3, 1.4, 1.5, 5.1, 5.2, 6.1, 8.1, 8.2, 8.4, 8.5, 8.7, 8.8, 8.9_

- [x] 2. Property test: Property 1 — authored history pages
  - Implement `storage_contract_memory_history` and
    `storage_contract_live_history` using the same generated batch cuts, events,
    principals, cursor/limit inputs and vector oracle, **at least 100 cases per
    backend**. Compare full values and exact-event lookup; no expected history
    obtained from a production reader.
  - Add deterministic zero/absent/terminal, exact-batch, above-1,000-event and
    exact-`HISTORY_READ_PAGE`-multiple examples. Reuse the split-history fixture
    for exact lookup over supported split batches, and the existing reset-copy
    case instead of duplicating that lifecycle.
  - Tag: `// Feature: storage-contract-tests, Property 1: History pages preserve the authored sequence`
  - _Requirements: 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 8.1, 8.2_

- [x] 3. Property test: Property 2 — current-format recovery
  - Implement `storage_contract_memory_recovery` and
    `storage_contract_live_recovery`, **at least 100 cases per backend**, with
    fixture-authored eligibility, multiple homes, varying page sizes and full
    decoded-state comparisons. Follow `next`, including after short pages;
    reject repeated cursors and normalize requested zero to effective one.
  - Add controlled cursor-row deletion and verify later keys remain reachable.
    Test-only row removal is fixture setup, not evidence for run deletion.
  - Tag: `// Feature: storage-contract-tests, Property 2: Current-format recovery traverses every eligible run`
  - _Requirements: 1.3, 1.4, 3.1, 3.2, 3.3, 3.6, 8.1, 8.2_

- [x] 4. Property test: Property 8 — DSQL legacy recovery phases
  - Implement `storage_contract_live_legacy_recovery` with **at least 100 DSQL
    cases**, using dedicated legacy setup hooks and the shared traversal oracle.
    Cover empty, short and exact-full phases and zero limits. A row changed from
    legacy to flagged during traversal must remain recoverable in the flagged
    phase; permit its specified repeat across phases.
  - Record why memory has no equivalent legacy representation.
  - Tag: `// Feature: storage-contract-tests, Property 8: DSQL recovery resumes across legacy phases`
  - _Requirements: 1.4, 1.5, 3.2, 3.3, 3.4, 3.5, 8.1, 8.2_

- [x] 5. Checkpoint: fixture, history and recovery checks
  - Format; run focused storage check/clippy and default memory/oracle nextest
    cases with `--locked`. Compile/list the storage `dsql-integration` tests and
    verify new live entries are ignored by default. Execute the new live gate
    with its URL unset in an isolated process and verify a configuration failure.
    Live-case success remains pending until credentialed archive execution.
  - _Requirements: 1.3, 2.1, 3.1, 8.4, 8.5_

- [x] 6. Add shared projection properties
  - [x] 6.1 Property test: Property 3 — ordered projection passes
    - Implement `storage_contract_memory_projection_pages` and
      `storage_contract_live_projection_pages`, **at least 100 cases per
      backend**. Commit in shuffled key order, test exclusive pages and other
      partition/fanout decoys, insert below a saved cursor and start a new pass.
      Check zero-limit immutability and backend-specific diagnostic timestamps.
    - Tag: `// Feature: storage-contract-tests, Property 3: Projection passes follow keys and discover late images`
    - _Requirements: 1.3, 1.4, 1.5, 4.1, 4.2, 4.6, 8.1, 8.2_
  - [x] 6.2 Property test: Property 4 — exact image retirement
    - Implement `storage_contract_memory_projection_deletion` and
      `storage_contract_live_projection_deletion`, **at least 100 cases per
      backend**, over observed subsets, absent/repeated keys and newer committed
      images. Compare exact remaining values and deletion counts, not just length.
    - Tag: `// Feature: storage-contract-tests, Property 4: Image retirement cannot erase an unobserved version`
    - _Requirements: 1.3, 1.4, 4.3, 4.4, 8.1, 8.2_
  - _Requirements: 1.3, 1.4, 1.5, 4.1, 4.2, 4.3, 4.4, 4.6, 8.1, 8.2_

- [x] 7. Add shared atomic commit properties
  - [x] 7.1 Property test: Property 5 — applied and rejected outcomes
    - Implement `storage_contract_memory_commit_outcomes` and
      `storage_contract_live_commit_outcomes`, **at least 100 cases per
      backend**. Exercise both commit APIs with full state/history/audit/dedupe/
      pointer/timer/dispatch/outbound/projection observations. Assert expected
      post-state on acceptance and unchanged observations on rejection. Probe
      applicable dedupe and current-pointer branches with controlled preconditions.
    - Tag: `// Feature: storage-contract-tests, Property 5: Commit outcomes publish all or none of the attempted writes`
    - _Requirements: 1.3, 1.4, 5.1, 5.2, 5.5, 5.7, 8.1, 8.2_
  - [x] 7.2 Property test: Property 6 — competing writes and starts
    - Implement `storage_contract_memory_competing_commits` and
      `storage_contract_live_competing_commits`, **at least 100 cases per
      backend**. Use barriers and joined writers for distinct same-sequence
      transitions and competing business-identity starts. Admit either winner,
      compare its full write set and prove the loser left no orphan rows. Avoid
      asserting one conflict variant for all legal concurrent interleavings.
    - Tag: `// Feature: storage-contract-tests, Property 6: Competing writers leave one coherent winner`
    - _Requirements: 1.3, 1.4, 5.2, 5.3, 5.4, 5.7, 8.1, 8.2_
  - [x] 7.3 Property test: Property 7 — original lease fence
    - Implement `storage_contract_memory_lease_fence` and
      `storage_contract_live_lease_fence`, **at least 100 cases per backend**.
      Use the original captured fence after deterministic ownership changes,
      exercising both APIs and complete rejected-write observations. Reuse the
      existing DSQL transactional interleavings to cover changes inside the
      commit. Never hold memory's mutex while awaiting a competing holder.
    - Tag: `// Feature: storage-contract-tests, Property 7: An old lease cannot authorize a later transition`
    - _Requirements: 1.2, 1.3, 5.2, 5.6, 5.7, 8.1, 8.2_
  - _Requirements: 1.2, 1.3, 1.4, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 8.1, 8.2_

- [x] 8. Property test: Property 9 — oracle negative controls
  - Implement pure `storage_contract_oracle_rejects_corruption`, **at least 100
    generated cases**, drawing nondegenerate fixtures for all four mutation
    families. First accept each valid observation, then reject truncation,
    wrong ordering, skipped nonempty continuation and partial writes. Include
    an overwritten value with the original row count. Mutate only observation
    copies; verify the mutation changes the targeted invariant.
  - Tag: `// Feature: storage-contract-tests, Property 9: The oracle rejects the failures it claims to detect`
  - _Requirements: 1.4, 8.3_

- [x] 9. Checkpoint: shared semantic suite
  - Run focused storage checks, clippy and all new memory/negative-control
    nextest entries with `--locked`; compile/list the paired DSQL entries.
    Confirm properties 1–7 share their scenario/oracle, all nine property tags
    exist, and recorded default generated counts meet their configured minimum.
    Verify pure negative controls fail on each deliberate corruption.
  - _Requirements: 1.1, 1.3, 1.4, 8.1, 8.2, 8.3, 8.5_

- [x] 10. Complete physical and legacy coverage without duplicating suites
  - [x] 10.1 Add `storage_contract_live_limit_binding` for directly bound
    out-of-range projection/recovery limits and unchanged durable observations.
    Include wrong-home validation as a distinct fixed commit case. Do not impose
    binding or payload limits on memory or on fixed-batch history reads.
    - _Requirements: 1.5, 5.7, 6.1, 6.2_
  - [x] 10.2 Reuse and, only where an asserted invariant is missing, extend the
    existing split-value, bulk reset/deletion/backlog, successor publication,
    transaction-limit and legacy projection-seeding test cases. Preserve their
    physical error classification and backend-specific legacy retention. Resolve
    every selected test to its exact listed name for the coverage matrix.
    - _Requirements: 1.1, 1.2, 1.5, 2.5, 4.5, 6.3, 6.4, 6.5_
  - _Requirements: 1.1, 1.2, 1.5, 2.5, 4.5, 5.7, 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 11. Add multi-page controller-managed acquisition coverage
  - Extend `runtime/dsql_tests.rs` with ignored, strictly gated
    `storage_contract_live_acquisition_spans_recovery_pages`. Prepare placement
    and outbound storage through `prepare_execution_placement` before seeding
    and acquiring. Use the real repository/lease path and more than
    `RECOVERY_PAGE` authored recoverable runs.
  - Re-arm existing recovery pause points to enter the second page while still
    inactive; after recovery completes, compare all seeded deadline trackers
    and their acquisition generation. Retain the existing superseded-generation
    test and its assertion that old handles cannot install state.
  - Ensure both fixtures stop runtime/renewal tasks, release only owned leases
    and clean only owned data on success or error. Add no sleeps or public hooks.
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 8.4, 8.5, 8.8, 8.9_

- [x] 12. Document exact coverage and archive-based live execution
  - Add `docs/testing/storage-contract-tests.md` and update
    `docs/testing/dsql-live-suites.md` with exact paired names, explicit backend
    differences and existing sibling selections. Describe property seeds,
    counts, negative controls, prerequisites and accepted failure categories.
  - Document immutable nextest archive creation/listing and serial `dsql-live`
    execution, with separate ordered commands for projection contracts,
    preparation-sensitive acquisition and later physical/lease suites. Include
    required isolation reruns and credential-gate preflight for older tests.
  - Document a receipt checklist for source/archive identity, feature flags,
    selected/executed counts and outcomes, property counts and cleanup. Keep
    endpoints, resource IDs and credentials private; add no live CI/nightly job.
  - Add a `.changes/unreleased/` record using the existing YAML convention,
    describing verified storage contracts and acquisition coverage. Do not claim
    execution before it occurs. Leave dependencies and lockfile unchanged.
  - _Requirements: 1.1, 1.2, 1.5, 6.1, 6.5, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10_

- [x] 14. Repair transaction-start cancellation exposed by the live suite
  - Record the deterministic unfixed SQLx cancellation reproducer and original
    runtime failures; retain them separately from successful verification.
  - Add permit-owned guarded read-only/read-write transaction starts; route all
    reservoir transaction-entry points and snapshot helpers through the guard.
    Audit remaining raw starts without changing unrelated control-plane APIs.
  - Add Property 10 with at least 100 generated cancellation-state cases and
    deterministic real-DSQL cancellation/reborrow coverage, plus successful
    transaction and rollback-on-drop preservation checks. Use synchronization,
    not sleeps or timing-based cancellation.
  - Rerun the failing acquisition check, the new multi-page acquisition check,
    affected physical/repository suites and the full workspace bar on a new
    immutable source/archive revision. Keep earlier revision evidence explicit.
  - Verify exact fixture cleanup, original bad-flag/slot accounting and absence
    of network work on healthy returns. Add the defect and repair to the change
    record and final PR validation report.
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_

- [x] 13. Checkpoint: complete implementation and execution evidence
  - Run the full root AGENTS.md §10.4 bar with `--locked` where prescribed:
    nightly fmt, workspace lint/check/nextest, doctests and warnings-denied docs.
    Compile/list feature-enabled storage/runtime tests and check Markdown links.
  - Execute the documented immutable-archive live selection against the approved
    ephemeral database, with the existing serial profile. Require actual live
    execution of every selected shared, physical and runtime case, at least 100
    successful cases for each backend/property pair, and all negative controls.
    Reconcile nextest counts with case receipts; report any omission explicitly.
  - Verify fixture cleanup and final ephemeral-resource cleanup in the run record;
    update the coverage matrix and change record with only executed evidence.
    Inspect the complete diff and confirm no schema, dependency, configuration,
    CI or unrelated specification changes before the PR boundary.
  - _Requirements: 1.1, 1.2, 1.3, 1.5, 2.1, 3.1, 4.1, 4.5, 5.2, 6.1, 6.3, 6.4, 6.5, 7.1, 7.2, 7.3, 7.4, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10_

## Task Dependency Graph

IDs identify top-level tasks. Subtasks run in listed order within their parent.
The graph states dependencies; it does not require multiple agents or concurrent
database execution. Runtime and live database tests remain serial. Amendment task
14 precedes the final checkpoint; existing task IDs are retained.

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1"],
  "4": ["3"],
  "5": ["2", "3", "4"],
  "6": ["5"],
  "7": ["6"],
  "8": ["2", "3", "4", "6", "7"],
  "9": ["7", "8"],
  "10": ["9"],
  "11": ["10"],
  "12": ["10", "11"],
  "14": ["11"],
  "13": ["12", "14"]
}
```

## Notes

- Requirements, design and tasks, including the Requirement 9 cancellation
  amendment, are approved. Shared properties stay unchecked until both memory
  and live DSQL verification complete; offline success alone is not completion.
- Properties 1–7 require at least 100 cases **each on each backend**; Property 8
  requires 100 DSQL cases; Properties 9 and 10 each require 100 pure cases. Fixed large
  physical and runtime examples are additional coverage, not counted substitutes.
- Use current code and owning specs to resolve a failed trace. Do not modify an
  approved sibling specification, add limits to memory, or accept a partial oracle
  just to make the suite pass. Surface any necessary contract amendment.
- Keep live run records local/private when they contain operational identifiers.
  Public matrix evidence contains source, archive/test identity and outcomes only.
- If a live property exceeds the existing timeout, split it into named seeded
  batches and preserve the aggregate minimum and exact test-name accounting.
  Do not lower its count or silently change the live profile.
- Rebase once at the PR boundary under the root workflow; rerun affected evidence
  if the reviewed implementation changes. Push and merge follow the existing
  authorization gates, with exact-head human approval required for merge.
