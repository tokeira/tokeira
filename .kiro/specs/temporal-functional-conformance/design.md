# Design: Temporal Functional Conformance (Tier 2)

## Overview

Tier 2 of Tokeira's three-tier conformance model runs **Temporal's own functional test corpus,
unmodified, over the real gRPC wire against a running `tokeirad`**. Where Tier 1
(`tokeira-conformance`) is hermetic, in-process, per-RPC, and ours, Tier 2 borrows Temporal's
server-grade behavioural assertions — exact `historyrequire` event-shape checks, lifecycle ordering,
error/status mapping — and points them at Tokeira. It directly answers the truest conformance
question: *does Temporal's own `versioning_3_test.go` (and the rest of `tests/`) pass against us?*

This is not a port. Per AGENTS.md Mission ("Do not port Temporal code") and §8 ("reading Temporal
source to determine the contract is required; copying its implementation is forbidden"), Tier 2
**runs** Temporal's Go tests as-is from a pinned fork; it copies no test logic into Tokeira.

**Core posture — run all, classify in the report.** Every test in the corpus *executes*; Tier 2 never
excludes a test from the run. Failures are expected and are *data*: each outcome is classified in the
coverage report (pass / real-gap / deliberate-deviation / out-of-public-scope), and the report joins
those outcomes against the compatibility matrix to produce a clear, insightful verdict. The aim is
total execution plus an interpretable report — not a curated subset that passes.

### Relationship to the other tiers

| Tier | Harness | Proves | Reuses |
|------|---------|--------|--------|
| 1 | `tokeira-conformance` (in-process, Rust) | Per-RPC shape + coverage gate, hermetic, fast (`cargo test`) | nothing — ours |
| 2 | **this spec** — Temporal functional tests vs `tokeirad` | Temporal's server-grade behavioural assertions over the real wire | Temporal `tests/` corpus, unmodified |
| 3 | SDK / `features` suites vs `tokeirad` (future) | Multi-language client behaviour + history replay | Temporal SDK suites |

Tier 1's design carries a Non-Goal pointing here; this spec owns the functional-suite replay.

### Ground-Truth Pins

- **Temporal fork:** `tokeira/temporal` (sibling checkout at `../temporal`), branch
  `tokeira/conformance-v1.31.0`, pinned at tag **`v1.31.0`**. This matches
  `TEMPORAL_SERVER_COMPAT = 1.31.0` in `crates/tokeira-build-info/src/pinned.rs`. The fork's `main`
  tracks upstream `HEAD` (currently post-`v1.31.0`) and is **not** used for conformance: running tests
  newer than the behavioural claim would assert semantics Tokeira does not yet implement, producing
  false failures.
- **Rationale for pinning to the tag, not `main`:** verified that `v1.31.0` is *not* an ancestor of the
  fork's `main`; the corpus on `main` includes post-1.31 behaviour. Tier 2 re-baselines on each
  `TEMPORAL_SERVER_COMPAT` bump — the same maintenance posture as the temporal-dsql fork.

## Architecture

Temporal's functional tests reach the server exclusively through a `WorkflowServiceClient` obtained
from the onebox. The integration point is therefore: **make the address/connection the tests dial be
`tokeirad`'s frontend.** The architecture is a boundary swap — Temporal's test cluster stays Temporal,
but its frontend boot is replaced by a connection to an external `tokeirad`.

### The seam (verified against `../temporal` @ `v1.31.0`)

Verified seam in `tests/testcore/onebox.go @ v1.31.0`:

- `TestCluster` construction calls `newTemporal(t, params)` then `cluster.Start()`
  (`tests/testcore/test_cluster.go:360–361 @ v1.31.0`).
- `func (c *TemporalImpl) Start()` (`onebox.go:244`) boots Temporal's own services:
  `startMatching(); startHistory(); startFrontend(); startWorker()`.
- `startFrontend()` (`onebox.go:353`) fx-boots the frontend, then builds the client
  (`onebox.go:442–443`):
  ```go
  connection := rpcFactory.CreateLocalFrontendGRPCConnection()
  c.frontendClient = workflowservice.NewWorkflowServiceClient(connection)
  ```
- `FrontendClient()` (`onebox.go:306`) and `FrontendGRPCAddress()` (`onebox.go:290`) just return that
  client / the host-map frontend address. Every functional test dials through these.

So the entire integration reduces to two overrides: **(a)** stop `Start()` from booting Temporal's
four services, and **(b)** make `frontendClient` / `FrontendGRPCAddress` resolve to an
externally-running `tokeirad`. Nothing downstream of `FrontendClient()` changes.

### Decision: Shape 2 — external `tokeirad`, thin onebox override

Two shapes were considered:

- **Shape 1 — embed `tokeirad` in the Go onebox process.** Replace the fx-booted frontend with an
  in-process `tokeirad`. Tighter lifecycle control, but drags `tokeirad` into Go's fx/process model for
  no conformance benefit and a larger, harder-to-rebaseline fork.
- **Shape 2 — onebox points at an external `tokeirad` subprocess (CHOSEN).** The onebox skips its own
  service boot and points the host map + frontend connection at an already-running `tokeirad` endpoint
  started by the test harness. Mirrors the temporal-dsql boundary-swap posture (Temporal stays
  Temporal; one boundary is swapped), keeps `tokeirad` a normal external process, and minimises the
  fork surface.

**Chosen: Shape 2.** The fork overrides exactly the two seam points above and leaves the test corpus,
the `WorkflowServiceClient`, and all assertions untouched.

## Components and Interfaces

| Component | Owns | Boundary / does not |
|-----------|------|---------------------|
| **Onebox seam override** (fork) | `Start()` short-circuit + external-frontend resolution | Lives only on `tokeira/conformance-v1.31.0`; never edits a test body |
| **`tokeirad` subprocess harness** (fork) | Start/health-wait/teardown of `tokeirad`; address injection | Reuses the standard `tokeirad` binary; no Tokeira-side process changes |
| **Skip/expect-fail ledger** (fork) | Classification of every non-passing test + rationale | Advisory roadmap stays in Tokeira's tracker, not here |
| **Wire-coverage tower layer** (`tokeira-edge`, mounted in `tokeirad`) | Records `(wire_method, status_code)` served during the run, observed at the gRPC transport boundary | Layer type lives in `tokeira-edge`; the `Arc<WireCoverageRecorder>` is constructed and the layer mounted by `tokeirad` only under the conformance flag. Production never installs the layer (zero overhead). Not wired into `EdgeInterceptors`. |
| **This spec** (Tokeira `.kiro/specs/`) | The conformance *claim*, tiers, triage discipline | The fork is the vehicle; the claim is Tokeira's |

### Shape-2 override behaviour (in the fork, not in Tokeira)

A conformance build mode (selected by env var or build tag, e.g. `TOKEIRA_CONFORMANCE_FRONTEND_ADDR`)
changes onebox behaviour:

1. **`Start()` short-circuit.** When the external-frontend address is set, `Start()` skips
   `startMatching/startHistory/startFrontend/startWorker` and instead records the external address into
   `hostsByProtocolByService[grpc][FrontendService]`, then builds `frontendClient =
   NewWorkflowServiceClient(dial(externalAddr))`. `createSystemNamespace()` is also skipped (tokeirad
   owns namespace bootstrap).
2. **Lifecycle.** The harness starts `tokeirad` as a subprocess (in-memory or DSQL storage) before the
   test cluster, waits for its frontend health check, injects the address, and tears it down after.
3. **`tokeirad` invocation.** Reuses the standard `tokeirad` binary from the Tokeira build — no
   Tokeira-side changes required for Shape 2 beyond the wire-coverage recorder.

The fork branch `tokeira/conformance-v1.31.0` holds these overrides plus the skip ledger; it is
re-based onto each new `vX.Y.0` tag at compat-bump time.

### Run-all, classify-in-report (not skip-filter)

**Every Temporal test in the corpus runs.** Tier 2 does not exclude tests from execution — a test that
exercises surfaces Tokeira does not serve still *runs*, and its outcome is recorded and classified in
the **report**. The classification is an interpretation of a result, never a gate on whether the test
executes. This is the core posture: the suite is exercised in full; failures are data.

Each test's recorded outcome is classified into exactly one category in a per-test **ledger**:

| Category | Meaning | In the report |
|----------|---------|---------------|
| **pass** | Ran and passed. | Counts toward conformance. |
| **real-gap** | Ran and failed; Tokeira should pass this but doesn't yet. | `expect-fail` with a linked tracking issue until fixed, then must flip to pass. |
| **deliberate-deviation** | Ran and failed; Tokeira intentionally differs, with documented rationale. | Recorded with a cited spec/PR rationale; reviewed on each bump. |
| **out-of-public-scope** | Ran and failed; depends on surfaces outside the public claim. | Recorded with the internal client surface it touched (see below). |

#### The out-of-public-scope classification (grounded in the seam)

`startFrontend()` also wires `adminClient`, `operatorClient`, `historyClient`, `matchingClient` from
the same connection (`onebox.go:444–450 @ v1.31.0`). A Shape-2 onebox fronting only `tokeirad`'s
`WorkflowService` leaves those other clients pointing at an endpoint that does not serve them. A test
that calls `AdminClient()`, `OperatorClient()` beyond the claimed subset, `HistoryClient()`,
`MatchingClient()`, the dynamic-config client (`DcClient()`), or internal task-poller /
cluster-membership hooks still **runs**; its failure is *classified* `out-of-public-scope` and the
report records which internal client surface it touched. This classification is mechanical, driven by
the wire-coverage recorder's observation (an `UnknownToMatrix` or beyond-claim RPC appearing in the
test's call set), not by per-test hand-judgement or by excluding the test from the run.
`versioning_3_test.go` will run in full; many of its sub-tests are expected to classify
`out-of-public-scope` in the report.

### Coverage reporting — Approach 1 (tower layer in `tokeira-edge`, joined via the compatibility surface)

Coverage intelligence lives in Tokeira, not the fork (keeps the fork thin). A `WireCoverageLayer` — a
tower `Layer` whose type lives in `tokeira-edge` and which is mounted on the gRPC `Server` by
`tokeirad` only under the conformance flag — records every `(wire_method, status_code)` served while the
Temporal suite runs against `tokeirad`. The layer observes calls at the gRPC transport boundary, so it
reads the true wire path (`/package.Service/Method`) from `req.uri().path()` and the response gRPC
status directly — no reconstruction from the edge's internal `Action` enum, and the wire path is
exactly what `resolve` consumes. Each observed call is then **resolved through the
`temporal-compatibility-surface` API** (`resolve(wire_path) -> RpcClassification`), which is the
single, tested join between the wire and the matrix. The report is a three-way join per RPC:

| Source | Provides |
|--------|----------|
| **Matrix claim** (via `resolve`) | the feature's `state` and `expected` wire outcome |
| **Tier 2 wire observation** (recorder) | the actual `(wire_method, status_code)` the suite drove |
| **Tier 1 evidence** (matrix `evidence`) | any hermetic per-RPC proof already on record |

From the join, each RPC row is marked:

- **agrees** — observed status matches the matrix's expected outcome (e.g. `Implemented` returned OK,
  `Unsupported` returned `Unimplemented`).
- **contradicts** — observed status disagrees with the claim (e.g. `Implemented` returned
  `Unimplemented`) — the highest-signal coverage finding.
- **uncovered** — claimed `Implemented`/`Partial` but the suite never drove it successfully.
- **unknown-to-matrix** — the recorder saw a wire method the matrix does not classify (Admin/Health/
  beyond-claim). Surfaced explicitly, never dropped.

Because the join key and expected-outcome projection come from `temporal-compatibility-surface`, the
report does not re-implement matching; it consumes one tested API. The recorder is enabled by a
conformance flag so it adds no overhead in production.

## Dependencies



Grounded against the actual code (`tokeira-edge`, `apps/tokeirad`, `tokeira-build-info`) and the
pinned fork, not inferred. Split by whether the dependency already exists or is new work.

### External / cross-repo (the vehicle)

- **The pinned Temporal fork** — `tokeira/temporal` @ branch `tokeira/conformance-v1.31.0` (tag
  `v1.31.0`). The hard prerequisite: the entire tier runs from it. Already created.
- **Temporal's Go toolchain + `tests/testcore` build** — compiles and runs the corpus with the onebox
  seam override. Standard for the fork; no Tokeira-side cost.
- **The onebox seam fork edits** — `Start()` short-circuit + external-frontend resolution in
  `tests/testcore/onebox.go`. Lives in the fork; the central new work this spec authors. No Tokeira
  dependency.

### Tokeira-side, already present (verified — not new work)

- **`tokeirad` binary with selectable in-memory storage** — the subprocess the harness fronts (R2.3,
  R2.7). Exists; `build_and_serve` already branches on `infrastructure.storage`.
- **`tokeirad` readiness signal** — `TokeiradReadiness` / `ReadinessRegistry`
  (`apps/tokeirad/src/observability.rs`), plus `HealthService` in `tokeira-edge`
  (`HealthState::Serving`). Gives the harness health-wait (R2.3) something real to poll. See caveat 1.
- **gRPC transport boundary (tower layer seam)** — `tokeirad` builds its tonic `Server` in
  `apps/tokeirad/src/lib.rs` (`Server::builder().layer(...).add_service(...)`), which is where a
  `WireCoverageLayer` mounts. The layer sees the true wire path and response status at the transport
  boundary — the faithful capture point for R5, and exactly what `resolve` consumes. `EdgeInterceptors`
  (`EdgeInterceptors::begin`, the `Action` enum) is deliberately **not** used for capture: it carries a
  snake_case `Action`, not the wire path, and would force a reconstruction. The recorder is a
  conformance-only concern that stays out of the admission hot path.

### Tokeira-side, to build (new work this spec creates)

- **Wire-coverage tower layer in `tokeira-edge`** (R5) — a `WireCoverageLayer` records
  `(wire_method, status_code)` per call at the gRPC transport boundary behind a conformance flag, and
  the binary exports JSON evidence from the shared recorder. Depends on the `tokeirad` server-assembly
  site (`Server::builder()...`) and the edge's `EdgeError → tonic::Status` mapping (which exists). See
  caveat 2.
- **The skip/expect-fail ledger** (R3) — per-test keyed, lives in the fork. New artifact plus its gate
  logic.
- **Subprocess lifecycle glue** (R2.3 / R2.4) — start `tokeirad`, health-wait, inject address, teardown.
  In the fork's test harness.

### Conceptual / sequencing dependencies

- **`TEMPORAL_SERVER_COMPAT` pin** (`crates/tokeira-build-info/src/pinned.rs`, = `1.31.0`) — Tier 2's
  pin must equal it (R1.1, P2). Already aligned.
- **Tier 1 (`tokeira-conformance`)** — **not** a build dependency. The tiers are complementary (R6).
  Tier 2's coverage recorder cross-checks Tier 1's axis-K claims (R5.4), so there is a *reporting*
  linkage, but Tier 2 can be built and run without Tier 1 existing. They may proceed in parallel.
- **The compatibility surface API** (`temporal-compatibility-surface`) — provides
  `resolve(wire_path) -> RpcClassification`, the wire↔matrix join key, `feature_for_rpc`, and the
  expected-outcome projection. This is a **hard reporting dependency**: the coverage report joins
  through it rather than re-implementing matching. It must be implemented first (it is the prior step
  in the plan). The underlying `tokeira-compatibility::FEATURE_MATRIX` already classifies every
  Workflow/Operator RPC; this spec consumes the *query surface* layered on top of it.

**Net:** the one hard external dependency is the pinned fork (done). The one genuinely new Tokeira
artifact is the wire-coverage tower layer plus the recorder it feeds, mounted on the gRPC server only
under the conformance flag.
Everything else is fork-side test-harness work. Nothing blocks starting.

### Caveats to resolve in task breakdown

1. **gRPC health vs internal readiness.** Temporal's onebox/SDKs and a natural harness health-wait
   expect either a successful trivial `WorkflowService` call or the standard gRPC Health Checking
   Protocol (`grpc.health.v1.Health/Check`). Tokeira has a `HealthService` with a `check()` method and
   an HTTP health path, but it is **not yet confirmed** to serve the standard gRPC health protocol on
   the frontend port. If it does not, the harness health-wait (R2.3) should poll a trivial
   `WorkflowService` RPC instead. Resolve which before implementing the lifecycle glue, so the
   health-wait does not assume an endpoint shape that is not served.
2. **Status-code fidelity for the recorder.** R5 records `(method, status_code)`; the edge maps
   `EdgeError → tonic::Status`. The recorder's evidence is only as faithful as that mapping — which is
   itself part of what Tier 1 asserts. Not a blocker, but the recorder's output should be read with
   that coupling in mind.

## Data Models

The persistent data shapes Tier 2 introduces are the **skip/expect-fail ledger** and the **wire
coverage record**. Concrete schemas are finalised during task breakdown; their shape is:

- **Ledger entry:** `{ test_id, category ∈ {pass, real-gap, deliberate-deviation, out-of-public-scope},
  rationale, evidence_ref }` where `evidence_ref` is a tracking-issue link (real-gap), a spec/PR link
  (deliberate-deviation), or an internal-client-surface tag (out-of-public-scope). Every test that runs
  produces a ledger entry; the ledger is keyed at **per-test** granularity — package + test name,
  including `t.Run` sub-test names — so a single failing sub-test in an otherwise-passing file is
  classified on its own.
- **Wire coverage record:** a set of `{ wire_method, status_code, count }` rows produced by the
  `tokeira-edge` recorder over a run, each resolved through the `temporal-compatibility-surface`
  `resolve()` API to its matrix `RpcClassification`, and marked `agrees | contradicts | uncovered |
  unknown-to-matrix` in the report.

## Error Handling

- **`tokeirad` subprocess fails health-check:** the harness fails fast with a clear error and tears
  down; the run is inconclusive, never silently green.
- **Unclassified non-passing test:** fails the Tier 2 gate (P3) rather than being treated as an
  acceptable skip.
- **Stale expect-fail (a real-gap test now passing):** fails the gate (P5) so the ledger cannot drift
  behind the implementation.
- **Pin/compat divergence:** the conformance branch tag ≠ `TEMPORAL_SERVER_COMPAT` fails the gate (P2).

## Testing Strategy

- Tier 2 *is* a test harness; its own correctness is enforced by the properties below plus a small
  meta-test set: ledger totality (every non-pass classified), pin consistency, and recorder
  enable/disable behaviour.
- The default suite runs `tokeirad` with in-memory storage for speed/hermeticity; a DSQL matrix is an
  opt-in variant.
- Initial bring-up runs a small, high-signal subset (e.g. basic workflow lifecycle tests) before
  attempting large files like `versioning_3_test.go`, to validate the seam before confronting the full
  skip-triage surface.

## Correctness Properties

### Property 1: Unmodified corpus

Tier 2 runs Temporal's `tests/` Go files byte-for-byte as pinned at `v1.31.0`; the only fork edits are
the onebox seam override and the ledger, never a test body.

**Validates: Requirements 1.2, 2.1**

### Property 2: Pin consistency

The fork conformance branch's tag equals `TEMPORAL_SERVER_COMPAT`. CI fails if they diverge.

**Validates: Requirements 1.1, 1.3, 1.4**

### Property 3: Ledger totality (every test classified)

Every test that runs produces a ledger entry classified into exactly one of {pass, real-gap,
deliberate-deviation, out-of-public-scope} with a rationale; an unclassified non-passing test fails the
gate. No test is excluded from the run.

**Validates: Requirements 3.1, 3.2, 3.5**

### Property 4: No silent scope inflation

An `out-of-public-scope` classification must cite the internal client surface it touched; a
`deliberate-deviation` classification must cite a spec/PR rationale.

**Validates: Requirements 3.6, 3.7, 3.8**

### Property 5: Real-gap monotonicity

A test classified `real-gap` (expect-fail) that begins passing must be flipped to a required pass; it
may not silently remain expect-fail.

**Validates: Requirements 4.1, 4.2**

### Property 6: Full-corpus execution

Every test in the pinned corpus executes; no test is excluded from the run on the basis of expected
scope or expected failure. Classification happens in the report, never as a run-time exclusion.

**Validates: Requirements 3.1, 3.3**

## Honest Costs

- **Maintained fork pinned at a tag.** Upstream test changes need re-baselining on each
  `TEMPORAL_SERVER_COMPAT` bump. Known cost — same playbook as temporal-dsql, not new risk.
- **Loud initial failures.** Most functional tests fail at first; the per-test ledger and the
  matrix-joined report are what convert that noise into a tracked, reviewable signal.
- **Classification discipline is the integrity surface.** A failure mis-classified as
  out-of-public-scope when it is really a gap would silently inflate the conformance claim. The report
  requires a category + rationale per non-passing test, reviewed at each bump.

## Resolved Decisions

These were open questions during design; now settled:

- **Storage mode — in-memory is the default.** The default Tier 2 suite runs `tokeirad` with
  in-memory storage for speed and hermeticity (no AWS/DSQL dependency, no shared state between runs). A
  DSQL-backed variant is an opt-in matrix for closer-to-production fidelity, not part of the default
  gate.
- **Execution — manual initially, CI to follow.** Tier 2 is run manually (operator-invoked) during
  bring-up while the skip ledger is established and the seam is validated. Cross-repo CI orchestration
  (Tokeira build × pinned fork) is a follow-on once the baseline is stable; the design must not assume
  CI exists on day one, but must not preclude it.
- **Ledger granularity — per-test.** The skip/expect-fail ledger is keyed at individual test
  granularity (package + test name, including sub-tests via `t.Run` names), not per-`Suite`/`Run`
  grouping. This keeps classification precise — a single failing sub-test in an otherwise-passing file
  is tracked on its own merits, and a real-gap fix flips exactly the tests it resolves.
