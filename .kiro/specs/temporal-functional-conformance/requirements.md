# Requirements Document

## Introduction

This feature establishes Tier 2 of Tokeira's conformance model: running Temporal's own functional
test corpus, unmodified, over the real gRPC wire against a running `tokeirad`. It reuses Temporal's
server-grade behavioural assertions (exact history event-shape checks, lifecycle ordering,
error/status mapping) from a fork pinned at `v1.31.0`, swapping only the onebox boot seam so the tests
dial `tokeirad` instead of a fx-booted Temporal cluster (Shape 2). It is distinct from the hermetic,
in-process Tier 1 (`tokeira-conformance`) and is exercised through the `tokeira/temporal` fork rather
than the Tokeira workspace.

Behaviour claims are ground-truthed to Temporal `v1.31.0` per AGENTS.md §8. Tier 2 runs Temporal's
tests as-is and copies no test logic into Tokeira (AGENTS.md Mission).

## Glossary

- **Tier 1:** the in-process Rust harness `tokeira-conformance`.
- **Tier 2:** this feature — Temporal's functional Go suite run against `tokeirad`.
- **The fork:** `tokeira/temporal`, sibling checkout at `../temporal`.
- **Conformance branch:** `tokeira/conformance-v1.31.0`, pinned at tag `v1.31.0`.
- **Onebox:** Temporal's `TemporalImpl` test cluster (`tests/testcore/onebox.go`).
- **Seam:** the onebox `Start()` boot path and `frontendClient` / `FrontendGRPCAddress` resolution.
- **Skip/expect-fail ledger:** the classified record of every non-passing functional test.
- **Wire-coverage recorder:** the `tokeira-edge` interceptor recording `(method, status_code)` served.
- **Public claim surface:** `WorkflowService` plus the explicitly-claimed `OperatorService` subset
  from the compatibility matrix.

## Requirements

### Requirement 1: Pinned, unmodified corpus

**User Story:** As a Tokeira maintainer, I want Tier 2 to run Temporal's functional tests exactly as
published at the targeted release, so that a pass is a true conformance signal and not an artifact of
edited tests.

#### Acceptance Criteria

1. THE fork conformance branch SHALL be pinned at the Temporal tag equal to `TEMPORAL_SERVER_COMPAT`
   (currently `v1.31.0`).
2. THE Tier 2 suite SHALL execute Temporal's `tests/` Go files unmodified except for the onebox seam
   override and the skip/expect-fail ledger; no test body SHALL be edited.
3. WHEN `TEMPORAL_SERVER_COMPAT` is bumped, THE conformance branch SHALL be re-based onto the new
   tag before the new behaviour is claimed conformant.
4. THE Tier 2 corpus SHALL NOT be run from the fork's `main` branch.

### Requirement 2: Shape-2 onebox seam override

**User Story:** As a Tokeira maintainer, I want the onebox to front an external `tokeirad` rather than
boot Temporal's services, so that the unmodified tests exercise Tokeira over the real wire.

#### Acceptance Criteria

1. WHEN the conformance frontend address is configured, THE onebox `Start()` SHALL NOT boot Temporal's
   `matching`, `history`, `frontend`, or `worker` services.
2. WHEN the conformance frontend address is configured, THE onebox SHALL resolve `FrontendClient()` and
   `FrontendGRPCAddress()` to the externally-running `tokeirad` frontend endpoint.
3. THE conformance harness SHALL start `tokeirad` as a subprocess and wait for its frontend health
   check to pass before the test cluster is started.
4. WHEN the test run completes, THE conformance harness SHALL terminate the `tokeirad` subprocess.
5. THE onebox seam override SHALL be selected at runtime (env var or build tag) so the unmodified
   default onebox behaviour is preserved when the override is absent.
6. THE seam override SHALL NOT require changes to the `tokeirad` binary beyond the wire-coverage
   recorder defined in Requirement 5.
7. THE default Tier 2 suite SHALL run `tokeirad` with in-memory storage; a DSQL-backed run SHALL be an
   opt-in variant and SHALL NOT be part of the default gate.
8. THE Tier 2 suite SHALL be runnable manually (operator-invoked) without requiring CI; THE design
   SHALL NOT preclude later cross-repo CI orchestration.

### Requirement 3: Run all tests; classify outcomes in the report

**User Story:** As a Tokeira maintainer, I want every Temporal test to execute and every outcome to be
classified in the report, so that the signal reflects the full corpus rather than a curated subset.

#### Acceptance Criteria

1. THE Tier 2 run SHALL execute every test in the pinned corpus; it SHALL NOT exclude a test from
   execution on the basis of expected scope or expected failure.
2. THE report SHALL classify each test's outcome into exactly one of: `pass`, `real-gap`,
   `deliberate-deviation`, `out-of-public-scope`.
3. THE classification SHALL be a property of the report (an interpretation of a result), never a gate
   on whether a test runs.
4. THE ledger SHALL be keyed at per-test granularity (package + test name, including `t.Run` sub-test
   names), so a single failing sub-test in an otherwise-passing file is classified independently.
5. IF a non-passing test is unclassified in the report, THEN the Tier 2 gate SHALL fail.
6. WHERE a test is classified `out-of-public-scope`, THE report SHALL cite the internal client surface
   it touched (`AdminClient`, `OperatorClient` beyond the claimed subset, `HistoryClient`,
   `MatchingClient`, dynamic-config client, or internal task-poller/cluster hooks), derived from the
   wire-coverage observation.
7. WHERE a test is classified `deliberate-deviation`, THE report SHALL cite a spec or PR rationale.
8. WHERE a test is classified `real-gap`, THE report SHALL link a tracking issue.

### Requirement 4: Real-gap monotonicity

**User Story:** As a Tokeira maintainer, I want fixed gaps to become required passes, so that
conformance only ratchets forward.

#### Acceptance Criteria

1. WHEN a test classified `real-gap` begins passing, THE ledger SHALL require it to be reclassified as
   a required pass.
2. IF a test classified `real-gap` (expect-fail) passes while still marked expect-fail, THEN the Tier 2
   gate SHALL fail, signalling the ledger is stale.

### Requirement 5: Wire-level coverage recording and matrix join

**User Story:** As a Tokeira maintainer, I want to know which RPCs Temporal's suite exercised and with
what outcomes, joined against the compatibility matrix, so that the report is a clear, matrix-backed
verdict rather than a raw status dump.

#### Acceptance Criteria

1. THE `tokeira-edge` layer SHALL provide a wire-coverage recorder that records each `(wire_method,
   status_code)` served while the Tier 2 suite runs.
2. THE recorder SHALL be enabled only under a conformance flag and SHALL add no overhead when disabled.
3. THE report SHALL resolve each observed wire method through the `temporal-compatibility-surface`
   `resolve(wire_path)` API rather than re-implementing wire↔matrix matching.
4. THE report SHALL mark each RPC row as `agrees`, `contradicts`, `uncovered`, or `unknown-to-matrix`
   based on the join of matrix claim (state + expected outcome) and observed `(wire_method,
   status_code)`.
5. WHEN an RPC is claimed `Implemented`/`Partial` but is never successfully driven by the suite, THE
   report SHALL mark it `uncovered`.
6. WHEN the recorder observes a wire method outside the matrix's vocabulary, THE report SHALL mark it
   `unknown-to-matrix` and SHALL NOT drop it.

### Requirement 6: Scope boundary with Tier 1

**User Story:** As a Tokeira maintainer, I want clear tier boundaries, so that the two harnesses do not
duplicate or contradict each other.

#### Acceptance Criteria

1. THE Tier 2 feature SHALL NOT replace or duplicate the Tier 1 in-process per-RPC harness.
2. THE conformance claim SHALL treat Tier 1 (hermetic per-RPC shape + gate) and Tier 2 (functional
   behavioural replay) as complementary evidence.
3. THE Tier 1 `conformance-harness` design SHALL reference this spec as the owner of functional-suite
   replay.
