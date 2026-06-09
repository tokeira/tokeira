# Implementation Plan: Temporal Functional Conformance (Tier 2)

Convert the feature design into a series of prompts for a code-generation LLM that will implement
each step with incremental progress. Make sure that each prompt builds on the previous prompts, and
ends with wiring things together. There should be no hanging or orphaned code that isn't integrated
into a previous step. Focus ONLY on tasks that involve writing, modifying, or testing code.

## Overview

This is a **Tier-2, two-repo** spec. The work is split across two checkouts and two toolchains, and
the task list keeps that split explicit:

- **TOKEIRA side (Rust)** — this workspace at `/Users/iw/Projects/tokeira/tokeira`. The wire-coverage
  recorder in `tokeira-edge`, the coverage report generator (joining recorder observations + matrix
  claims via the already-landed `tokeira_compatibility::coverage::resolve` + the per-test ledger),
  the ledger/record data models, and the report-side gates. Rust tasks follow `AGENTS.md`:
  Edition 2024, `cargo +nightly fmt`, `cargo lint`, no `.unwrap()` outside tests, full module/public
  docs. Build/verify with `cargo` — **never** in the fork.
- **FORK side (Go)** — sibling checkout `../temporal`, branch `tokeira/conformance-v1.31.0`, pinned at
  tag `v1.31.0`. The Shape-2 onebox seam override (`tests/testcore/onebox.go`), the `tokeirad`
  subprocess lifecycle glue, and the run-all harness. Go tasks live in `../temporal` and are marked
  **(fork: ../temporal @ tokeira/conformance-v1.31.0)**; they use `go test -tags test_dep`, **never**
  `cargo`, and never edit a test body (only the seam + ledger).

**Sequencing reality.** Tier 2 is manual-initially (R2.8) and in-memory by default (R2.7). The honest
first milestone is *one trivial `WorkflowService` lifecycle test running end-to-end against `tokeirad`
through the seam* (task 6) **before** attempting the full corpus or large files like
`versioning_3_test.go`. The run-all harness (task 8) comes after that seam is proven.

## Tasks

- [x] 1. Tokeira-side foundations: data models and pin gate (Rust, this workspace)
  - [x] 1.1 Define the wire-coverage record data model in `tokeira-edge`
    - Serializable `{ wire_method, status_code, count }` rows plus the aggregate the recorder emits
      over a run; derive `Debug` + `Serialize`/`Deserialize` per AGENTS.md §1; module + public-item docs
    - This is the shape the report later resolves through `tokeira_compatibility::coverage::resolve`
    - _Requirements: 5.1_

  - [x] 1.2 Define the per-test ledger data model (Tokeira owns the report; the ledger file may live in the fork)
    - `{ test_id, category ∈ {pass, real-gap, deliberate-deviation, out-of-public-scope}, rationale,
      evidence_ref }`; `test_id` keyed at per-test granularity — package + test name **including
      `t.Run` sub-test names** so a single failing sub-test is classified independently
    - `evidence_ref` is a tracking-issue link (real-gap), a spec/PR link (deliberate-deviation), or an
      internal-client-surface tag (out-of-public-scope)
    - _Requirements: 3.2, 3.4_

  - [x] 1.3 Implement the pin-consistency check
    - Assert the fork conformance branch tag equals `TEMPORAL_SERVER_COMPAT`
      (`crates/tokeira-build-info/src/pinned.rs` = `1.31.0`); fail loudly on divergence; reject running
      from the fork's `main`
    - _Requirements: 1.1, 1.3, 1.4_

  - [ ]* 1.4 Write property test for pin consistency
    - **Property 2: Pin consistency**
    - **Validates: Requirements 1.1, 1.3, 1.4**

- [x] 2. Wire-coverage tower layer in `tokeira-edge` (Rust, this workspace)
  - [x] 2.1 Implement the `WireCoverageRecorder` aggregator in `tokeira-edge::conformance`
    - In-memory `Arc<WireCoverageRecorder>` aggregating `(wire_method, status_code) -> count`, with a
      deterministic `snapshot() -> WireCoverageRecord`. Conformance-only; never on a production hot path.
    - Decision (revised): capture happens in a tower layer (2.2), NOT in `EdgeInterceptors` — the
      `EdgeInterceptors` recorder handle was reverted because the admission seam lacks the wire path.
    - _Requirements: 5.1, 5.2_

  - [x] 2.2 Implement `WireCoverageLayer` (tower layer) and mount it in `tokeirad` under the conformance flag
    - A tower `Layer`/`Service` in `tokeira-edge` holding `Arc<WireCoverageRecorder>`; on each call read
      the wire path from `req.uri().path()` and the response gRPC status, then `recorder.record(path, code)`.
      This is the faithful capture point — the true `/package.Service/Method` and status at the transport
      boundary, exactly what `coverage::resolve` consumes.
    - Mount in `apps/tokeirad/src/lib.rs` at `Server::builder().layer(...)`, constructed only when the
      conformance flag/env is set; production never installs the layer (zero overhead when off).
    - _Requirements: 5.1, 5.2_

  - [x] 2.3 Implement JSON export of the recorded wire-coverage set over a run
    - The conformance harness/binary snapshots the shared `Arc<WireCoverageRecorder>` and writes the
      `WireCoverageRecord` (task 1.1 model) as JSON so the Rust report can consume it; gated on the
      conformance flag
    - _Requirements: 5.1_

  - [ ]* 2.4 Write unit/property test for recorder enable/disable behaviour
    - No observable records and no overhead when the flag is off; faithful `(method, status_code)`
      capture when on
    - _Requirements: 5.2_

- [ ] 3. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Fork-side onebox seam override (Go — fork: ../temporal @ tokeira/conformance-v1.31.0)
  - [x] 4.1 Implement the `Start()` short-circuit under `TOKEIRA_CONFORMANCE_FRONTEND_ADDR`
    - In `tests/testcore/onebox.go` (`Start()` @ onebox.go:244): when the env var is set, skip
      `startMatching`/`startHistory`/`startFrontend`/`startWorker` (startFrontend @ onebox.go:353) and
      skip `createSystemNamespace()` (tokeirad owns namespace bootstrap)
    - Override is selected at runtime so the unmodified default onebox behaviour is preserved when the
      env var is absent; requires no `tokeirad` binary change beyond the task-2 recorder
    - _Requirements: 2.1, 2.5, 2.6_

  - [x] 4.2 Resolve `FrontendClient()` / `FrontendGRPCAddress()` to the external `tokeirad`
    - Record the external address into `hostsByProtocolByService[grpc][FrontendService]` and build
      `frontendClient = NewWorkflowServiceClient(dial(externalAddr))` (connection seam @ onebox.go:442–443,
      `FrontendGRPCAddress` @ onebox.go:290, `FrontendClient` @ onebox.go:306); nothing downstream of
      `FrontendClient()` changes
    - _Requirements: 2.2_

- [x] 5. Fork-side `tokeirad` subprocess lifecycle glue (Go — fork: ../temporal @ tokeira/conformance-v1.31.0)
  - [x] 5.1 Implement the health-wait as a trivial `WorkflowService` RPC poll
    - **Resolve design caveat 1 first:** do NOT assume the standard gRPC Health Checking Protocol
      (`grpc.health.v1.Health/Check`) is served on the frontend port. Poll a trivial `WorkflowService`
      RPC (e.g. `GetSystemInfo`/`GetClusterInfo`) until it succeeds; do this early so the lifecycle glue
      does not assume an endpoint shape that is not served
    - _Requirements: 2.3_

  - [x] 5.2 Start `tokeirad` as a subprocess (in-memory storage default), inject address, teardown
    - Boot `tokeirad` with in-memory storage before the test cluster, wait via 5.1, inject the address
      into the onebox override, and terminate the subprocess when the run completes; fail fast and tear
      down if health-wait never passes (never silently green)
    - _Requirements: 2.3, 2.4, 2.7_

  - [ ]* 5.3 Add the opt-in DSQL-backed storage variant
    - DSQL run is selectable but is NOT part of the default gate
    - _Requirements: 2.7_

- [x] 6. First milestone — one trivial `WorkflowService` lifecycle test E2E (Go — fork: ../temporal @ tokeira/conformance-v1.31.0)
  - [x] 6.1 Wire and run a single basic workflow-lifecycle test against `tokeirad` through the seam
    - Prove the seam (tasks 4 + 5) end-to-end on one high-signal lifecycle test **before** the full
      corpus or large files like `versioning_3_test.go`; this is the honest first milestone.
    - **Standalone, NOT `FunctionalTestBase`:** the standard base registers its namespace via a direct
      `MetadataManager` write (see task 8.0 finding), which tokeirad never sees. So this milestone test
      is self-contained: start tokeirad via the harness (task 5), set the seam env, register a namespace
      through the frontend `RegisterNamespace` RPC, then run start→poll→complete via `FrontendClient()`.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 7. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Run-all harness (Go — fork: ../temporal @ tokeira/conformance-v1.31.0)
  - [ ] 8.0 Adapt `FunctionalTestBase` setup for the conformance seam (namespace via frontend RPC)
    - **Finding (verified, blocks the corpus):** `FunctionalTestBase.setupCluster` registers its
      namespace by writing **directly to `testCluster.testBase.MetadataManager.CreateNamespace`**
      (`functional_test_base.go:466` `RegisterNamespace`), Temporal's own persistence layer — not a
      gRPC call. Under Shape-2 the onebox boots no Temporal persistence (tokeirad is the backend over
      the wire), so the standard setup writes a namespace tokeirad never sees, and every test relying
      on `s.Namespace()` fails with not-found at the frontend even when the workflow logic is fine.
    - When the conformance seam env is set, `setupCluster` MUST register the namespace through
      tokeirad's frontend `RegisterNamespace` RPC (a WorkflowService surface tokeirad serves; matrix
      group namespace-management) instead of the direct `MetadataManager` write. This is the higher-impact,
      shared-setup change that makes the corpus runnable; it is deliberately NOT in the task-6 milestone.
    - Tests that additionally poke `testBase`/persistence directly in their bodies remain
      out-of-public-scope by construction and are classified as such in the report (not run-blockers).
    - _Requirements: 2.1, 3.1_

  - [ ] 8.1 Execute the entire pinned corpus; never exclude a test from running
    - Operator-invokable (manual initially, R2.8); classification is a report concern, never a run-time
      exclusion — every test in `tests/` executes
    - _Requirements: 3.1, 3.3, 2.8_

  - [ ] 8.2 Capture per-test outcomes (including `t.Run` sub-tests) into the ledger shape (task 1.2)
    - _Requirements: 3.4_

  - [ ]* 8.3 Write meta-test for full-corpus execution
    - **Property 6: Full-corpus execution**
    - **Validates: Requirements 3.1, 3.3**

  - [ ]* 8.4 Write meta-test for unmodified corpus
    - Only the onebox seam override + the ledger differ from upstream `v1.31.0`; no test body edited
    - **Property 1: Unmodified corpus**
    - **Validates: Requirements 1.2, 2.1**

- [ ] 9. Coverage report generator (Rust, this workspace)
  - [ ] 9.1 Join recorder observations through `coverage::resolve` against the matrix claim
    - For each observed RPC, mark `agrees` | `contradicts` | `uncovered` | `unknown-to-matrix` from the
      three-way join (matrix `state`+`expected` via `resolve`, observed `(wire_method, status_code)`,
      Tier-1 evidence); `unknown-to-matrix` is surfaced, never dropped; `uncovered` when claimed
      `Implemented`/`Partial` but never successfully driven
    - Consume the tested `tokeira_compatibility::coverage::resolve(wire_path, dynamic_config, namespace)`
      API (commit 0a4ff6f) — do not re-implement wire↔matrix matching
    - _Requirements: 5.3, 5.4, 5.5, 5.6_

  - [ ] 9.2 Join the per-test ledger into the report and classify each test
    - Every test that ran produces exactly one classified ledger entry in the report
    - _Requirements: 3.2, 3.4_

  - [ ] 9.3 Derive the `out-of-public-scope` internal-client surface from the wire observation
    - When a test's call set hits a beyond-claim / `UnknownToMatrix` surface (`AdminClient`,
      `OperatorClient` beyond the claimed subset, `HistoryClient`, `MatchingClient`, dynamic-config
      client, internal task-poller/cluster hooks), record which surface it touched — mechanical, from
      the recorder, not hand-judgement
    - _Requirements: 3.6_

  - [ ]* 9.4 Write property test for the matrix-join markings
    - `uncovered` when claimed `Implemented`/`Partial` but never driven; `unknown-to-matrix` never dropped
    - **Validates: Requirements 5.5, 5.6**

- [ ] 10. Report gates (Rust report consuming the Go ledger, this workspace)
  - [ ] 10.1 Implement the ledger-totality gate
    - An unclassified non-passing test fails the Tier 2 gate
    - _Requirements: 3.5_

  - [ ]* 10.2 Write property test for ledger totality
    - **Property 3: Ledger totality**
    - **Validates: Requirements 3.1, 3.2, 3.5**

  - [ ] 10.3 Implement the no-silent-scope-inflation gate
    - `out-of-public-scope` must cite the internal client surface (9.3); `deliberate-deviation` must
      cite a spec/PR rationale; `real-gap` must link a tracking issue
    - _Requirements: 3.6, 3.7, 3.8_

  - [ ]* 10.4 Write property test for no silent scope inflation
    - **Property 4: No silent scope inflation**
    - **Validates: Requirements 3.6, 3.7, 3.8**

  - [ ] 10.5 Implement the real-gap monotonicity gate
    - A test classified `real-gap` (expect-fail) that begins passing must flip to a required pass;
      passing while still marked expect-fail fails the gate (stale ledger)
    - _Requirements: 4.1, 4.2_

  - [ ]* 10.6 Write property test for real-gap monotonicity
    - **Property 5: Real-gap monotonicity**
    - **Validates: Requirements 4.1, 4.2**

- [ ] 11. Tier boundary wiring (Rust/docs, this workspace)
  - [ ] 11.1 Reference this spec from the Tier 1 `conformance-harness` design as functional-suite owner
    - Record the complementary (not duplicative) relationship so the two harnesses do not contradict
    - _Requirements: 6.1, 6.2, 6.3_

- [ ] 12. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- This spec spans **two repos**: Rust tasks build/verify with `cargo` in this workspace and follow
  `AGENTS.md` (Edition 2024, `cargo +nightly fmt`, `cargo lint`, no `.unwrap()` outside tests, module +
  public-item docs). Go tasks live in **`../temporal`** on `tokeira/conformance-v1.31.0`, use
  `go test -tags test_dep`, and never edit a test body — only the onebox seam and the ledger.
- Tasks marked with `*` are optional (property tests, meta-tests, and the opt-in DSQL variant) and can
  be skipped for a faster baseline. Core implementation and report gates are never optional.
- The honest path is task 6 (one trivial lifecycle test E2E) **before** task 8 (run-all); do not chase
  the full corpus or `versioning_3_test.go` until the seam is proven.
- Each task references specific requirement clauses for traceability; property sub-tasks are placed
  next to the implementation they validate and annotated with their design Property number.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3", "2.1", "4.1", "5.1"] },
    { "id": 1, "tasks": ["1.4", "2.2", "4.2", "5.2"] },
    { "id": 2, "tasks": ["2.3", "5.3", "6.1"] },
    { "id": 3, "tasks": ["2.4", "8.1"] },
    { "id": 4, "tasks": ["8.2", "9.1"] },
    { "id": 5, "tasks": ["8.3", "8.4", "9.2"] },
    { "id": 6, "tasks": ["9.3", "9.4", "10.1"] },
    { "id": 7, "tasks": ["10.2", "10.3"] },
    { "id": 8, "tasks": ["10.4", "10.5"] },
    { "id": 9, "tasks": ["10.6", "11.1"] }
  ]
}
```
