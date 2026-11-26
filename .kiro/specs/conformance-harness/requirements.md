# Requirements Document: Behavioural Conformance

## Introduction

These requirements derive from [the design](design.md). Behavioural conformance is
Tier 1: independent decision oracles, generated interleavings, recorded differential
cases, simulation faults, and optional TLA+ trace checks, in that order. The target is
Temporal v1.31.0 behaviour on the vendored v1.62.11 wire API. The model is derived
from source at the tag; recordings validate it before it judges Tokeira.

The planned crates are `tokeira-behaviour-model` and `tokeira-behaviour-harness`.
The latter calls the real edge service objects directly in its deterministic fixture.
Tier 2 remains the Go functional suites against `tokeirad` through the fork. Its
Temporal recordings can contribute corpus evidence without moving the suites into
Tier 1. The [conformance definition](../../../docs/conformance/v1.31.0/README.md)
continues to define the public contract.

## Glossary

- **Model:** independent pure decisions and observable state machine for v1.31.0.
- **Rule:** a model decision or transition with a source function, path, line range,
  tag, and source digest.
- **Scenario:** setup, workflow programs, and ordered or overlapping action data.
- **Scripted Worker:** the harness's interpreter of workflow programs and task histories.
- **Lifecycle Table:** legal states and event transitions for a command kind.
- **Direct Cluster:** isolated real edge/runtime/kernel stack on in-memory storage.
- **Scheduler:** seeded selection of tasks at store calls and notification sends.
- **Pass:** one invocation of real background work without its periodic loop.
- **Witness:** invocation/return boundaries, applied commit order, and read observation
  points used to test a legal order of overlapping calls.
- **Replay Case:** versioned scenario, seed, choices, fault choices, and failure identity.
- **Corpus:** checked-in normalized recordings from Temporal built at v1.31.0.
- **Calibration:** successful model and lifecycle-table comparison with the corpus.
- **Source-only Rule:** a rule whose inputs no recording can reach, such as an exact
  instant; it judges on its cited source alone, and its evidence carries that label.
- **Quiescence:** bounded completion of due work and projection to a declared condition.
- **RPC Key:** fully qualified service and method identity derived from descriptors.
- **Manifest:** complete descriptor-derived RPC set with catalog annotations.
- **Axis K:** Tokeira's implementation state in the existing `FEATURE_MATRIX`.
- **Behavioural Evidence:** an executed, checked, passing scenario using a calibrated or
  source-only model rule, or the corpus, with observed RPCs and revision provenance.
- **Sometimes Counter:** count of a declared multi-step situation that must occur.
- **Ledger:** planned page with RPC, decision, and area evidence and verdicts.
- **DSQL Campaign:** local sequential and concurrent cases on an ephemeral DSQL cluster.
- **Fault Adapter:** mapping of scenario steps into the existing turmoil simulation.
- **Trace Check:** finite abstract action validation against a repository TLA+ model.

## Target State and evidence

The full descriptor surface is accounted for. At the current pins it contains 129
methods: 121 from the target release and eight newer wire methods. Newer methods
remain separate from the target denominator. Model gaps remain invariants-only;
coverage never implies that untested decisions agree.

Current integration points are the service objects in
`crates/tokeira-edge/src/grpc/`, the store pause fixture in
`crates/tokeira-runtime/src/runtime/repair_tests.rs`, the crate-private clock and
scanner passes in `crates/tokeira-runtime/src/`, and the existing compatibility
catalog in `crates/tokeira-compatibility/src/matrix.rs`. Design §2 records these seams.
Design §§3 and 10 contain the pinned source evidence for concrete behavioural rules.
No expected result is derived from Tokeira's implementation.

## Contract policy

| Surface | Required policy | Invalid or missing input | Effect |
|---|---|---|---|
| Scenario and replay fields | Versioned, typed, state-valid, deterministic bindings | Reject dangling handles, invalid versions, or empty selections | No passing evidence |
| Rule and lifecycle evidence | Tagged source ranges, and corpus calibration or a stated source-only reason | Reject missing citations, failed calibration, or a missing reason | Block affected behavioural verdicts |
| Corpus records | Canonical header/response/event lines; explicit normalization | Reject malformed, unknown, or unproven records | No partial corpus acceptance |
| Manifest annotations | Complete descriptor/catalog/adapter equality | List missing, stale, duplicate, or unknown keys | Gate fails |
| Ledger records | Separate RPC, decision, and area data with provenance | Reject stale success claims and missing reasons | Preserve gaps and mismatches |
| Budgets and progress | Finite steps, virtual horizon, fair-pass assumptions | Classify exhaustion or missing obligations as incomplete | Nonzero run outcome |

## Requirements

### Requirement 1: Architecture and direct fixture

**User Story:** As a maintainer, I want one layered Tier 1 harness over the real stack,
so that its evidence measures the implementation and complements Tier 2.

**Trace:** Design §§1–2, 5; Properties 7, 20.

#### Acceptance Criteria

1. WHEN the harness enables a layer, THE Harness SHALL require its predecessors in the order decision oracles, generated interleavings, differential runs, simulation faults, and optional trace checks.
2. WHEN a deterministic fixture invokes an RPC, THE Direct Cluster SHALL call `WorkflowServiceGrpc` or `OperatorServiceGrpc` through its service trait without sockets or loopback gRPC.
3. WHEN two fixtures execute, THE Direct Cluster SHALL isolate their stores, identities, and task ownership.
4. WHEN a fixture ends normally or fails, THE Direct Cluster SHALL cancel and join all owned tasks before teardown completes.
5. WHEN history is requested, THE Direct Cluster SHALL drain all pages and preserve the service response or status, including metadata and typed details.
6. WHEN Tier 1 consumes Tier 2 evidence, THE Harness SHALL preserve suite provenance and the shared feature-matrix classification without changing Tier 2 execution or outcome rules.

### Requirement 2: Independent reference model

**User Story:** As a maintainer, I want expectations derived independently from the
target, so that an engine defect cannot validate itself.

**Trace:** Design §§2–3, 7–8; Properties 4, 10, 13.

#### Acceptance Criteria

1. WHEN a model rule is introduced, THE Model SHALL derive its behaviour from v1.31.0 source independently of Tokeira code and specifications, separately from engine fixes.
2. WHEN model dependencies are checked, THE Model SHALL reject dependencies on Tokeira kernel, runtime, edge, storage, and compatibility policy.
3. WHEN a rule catalog is validated, THE Model SHALL require each rule's source function, path, inclusive existing line range at v1.31.0, matching source digest, and evidence for every decision row.
4. WHEN an action lacks a calibrated or source-only model rule, THE Harness SHALL mark that action's coverage as invariants-only rather than model agreement.
5. WHEN a rule that a recording can reach is used for behavioural evidence, THE Harness SHALL require successful calibration against the recorded corpus first.
6. WHEN no recording can reach a rule's inputs, such as an exact instant, THE Harness SHALL accept the rule's cited source alone, with the stated reason, and label that evidence source-only.

### Requirement 3: Scenarios, generation, and shrinking

**User Story:** As a scenario author, I want generated interleavings that shrink to
reproducible cases, so that a failure is small enough to explain.

**Trace:** Design §4; Property 2.

#### Acceptance Criteria

1. WHEN a scenario is decoded, THE Harness SHALL support versioned setup, programs, client calls, polls, responses, heartbeats, advances, passes, overlaps, and bounded quiescence.
2. WHEN scenarios are generated, THE Generator SHALL use `proptest-state-machine` with state-valid handles and explicit negative-request cases.
3. WHEN a failure is shrunk, THE Shrinker SHALL simplify steps, arguments, programs, overlaps, and choices while retaining valid references and the same failure identity.
4. WHEN a failing case is saved, THE Harness SHALL store the explicit scenario, seed, choice vector, fault choices, versions, fingerprints, mode, and failure identity defined in Design §4.
5. WHEN a minimized artifact is replayed with its recorded versions, THE Harness SHALL reproduce its failure without depending on regeneration from the original seed.

### Requirement 4: Deterministic scheduling and time

**User Story:** As a maintainer, I want explicit time and scheduling choices, so that
internal interleavings can be repeated and simplified.

**Trace:** Design §5; Properties 1, 11.

#### Acceptance Criteria

1. WHEN an L2 run executes, THE Scheduler SHALL use a current-thread runtime, paused time, an injected clock, seeded identities, and stable runnable ordering without implicit time advancement.
2. WHEN any in-memory store operation or runtime notification send occurs, THE Scheduler SHALL observe it through the two test-only hooks and apply the recorded scheduling or delivery choice.
3. WHEN a background step executes, THE Direct Cluster SHALL invoke the real single pass through test support with periodic loops disabled.
4. WHEN a bounded case contains two or three overlapping calls on one run, THE Scheduler SHALL enumerate its relevant store schedules or report incomplete exploration if the declared bound is exceeded.
5. WHEN a failed run is repeated, THE Harness SHALL require the same trace and classify any change as nondeterminism before trusting shrinking.

### Requirement 5: Overlap witnesses and store faults

**User Story:** As a maintainer, I want overlapping results checked against a legal
public call order, so that internal races cannot hide behind successful commits.

**Trace:** Design §§5, 7; Properties 9, 12.

#### Acceptance Criteria

1. WHEN calls overlap, THE Harness SHALL record invocation/return boundaries, actual mutation commit order, and read or no-op observation points.
2. WHEN a witness is accepted, THE Checker SHALL preserve completed-before-started order, actual commits, admissible read cuts, and the model's public atomicity contract.
3. WHEN a serialization conflict or timeout-before-apply is injected, THE Checker SHALL require no applied transition from that failed store attempt.
4. WHEN a reply is lost after apply, THE Checker SHALL retain applied or unapplied outcomes only as allowed by subsequent observations and SHALL detect duplicate effects on retry.
5. WHEN notifications are dropped, THE Checker SHALL require discovery within the declared period once the relevant pass receives its fair turn.
6. WHEN a stale outside-lane decision or competing remover acts, THE Checker SHALL require commit revalidation and the row's designated lifecycle authority.

### Requirement 6: Scripted worker and replay

**User Story:** As a scenario author, I want one scripted worker for both systems,
so that worker behaviour does not change the comparison.

**Trace:** Design §§4, 6; Property 8.

#### Acceptance Criteria

1. WHEN either target is driven, THE Scripted Worker SHALL interpret the same program data without embedding an SDK or external workload generator.
2. WHEN a command or environment event is processed, THE Scripted Worker SHALL validate it against a cited lifecycle transition for its kind, including immediate commands and workflow tasks.
3. WHEN a program waits, cancels, abandons, or responds to an activity or Nexus task, THE Scripted Worker SHALL implement the vocabulary and attempt outcomes in Design §4 while retaining abandoned command lifecycles.
4. WHEN final history is replayed, THE Scripted Worker SHALL reproduce live accepted command batches and separately check discarded attempts from their observed task histories.
5. WHEN the cache is dropped after every response, THE Scripted Worker SHALL produce the same commands and query/update responses as cached execution for the same observations.

### Requirement 7: Behaviour and progress checks

**User Story:** As a maintainer, I want checks on outcomes, durability, and progress,
so that well-shaped histories cannot hide lost work or a stuck workflow.

**Trace:** Design §7; Properties 10, 11, 12.

#### Acceptance Criteria

1. WHEN a step completes, THE Checker SHALL compare its response, status, and typed details against calibrated or source-only expectations and report the first semantic divergence.
2. WHEN an L2 run ends, THE Checker SHALL compare complete model and engine histories exactly, including event attributes, IDs, references, and injected times.
3. WHEN a commit becomes observable, THE Checker SHALL check history structure, command lifecycles, at-most-once effects, and Describe consistency with history plus history-free pending state.
4. WHEN projection reaches bounded quiescence, THE Checker SHALL compare List and Count with the eligible run and deletion state.
5. WHEN a finite-timeout execution is drained with advancing time, fair passes, and eventually ending faults, THE Checker SHALL require every run in its lineage to close and every scheduled task to be delivered or time out.
6. WHEN eligible matching work exists for a live compatible poll, THE Checker SHALL fail if the poll remains blocked beyond the relevant discovery period and scheduled pass completion.
7. WHEN a scenario executes in any area, THE Checker SHALL apply all applicable Design §7 invariants, including durability and durable discovery, even if model coverage is absent.

### Requirement 8: Recorder and corpus

**User Story:** As a maintainer, I want recordings from the exact target release,
so that the model and worker are checked against actual server behaviour.

**Trace:** Design §8; Properties 3, 13.

#### Acceptance Criteria

1. WHEN a corpus case is recorded, THE Recorder SHALL use Temporal built from v1.31.0 in the local fork with the tag's SQLite configuration and retain source/configuration provenance.
2. WHEN a sequential scenario is recorded, THE Recorder SHALL use the same worker and explicit observable mappings for clock/pass steps, marking unmappable cases unrecordable without claiming corpus coverage.
3. WHEN a recording is written, THE Recorder SHALL use the versioned UTF-8 line format in Design §8 with one history event per line and distinct header/response records.
4. WHEN normalization runs, THE Recorder SHALL omit generated IDs, tokens, and absolute times only through the field-path allowlist while preserving order, logical relationships, durations, and other semantic data.
5. WHEN corpus data is decoded, THE Corpus Reader SHALL round-trip supported normalized records exactly and reject malformed records, unknown fields, and unknown versions.
6. WHEN a corpus case judges Tokeira, THE Harness SHALL first require the model and covered lifecycle tables to reproduce that case.
7. WHEN CI or a Tier 2 import consumes corpus data, THE Harness SHALL use checked-in recordings with verified target provenance and SHALL reject Tokeira output as expected corpus data.

### Requirement 9: Manifest and state-aware gate

**User Story:** As a maintainer, I want executable evidence for the served surface,
so that a declared implementation cannot pass without being exercised.

**Trace:** Design §9; Properties 5, 6.

#### Acceptance Criteria

1. WHEN the manifest is built, THE Harness SHALL enumerate both public services from `FILE_DESCRIPTOR_SET`, retaining all 129 current methods and the distinct 121-target/eight-newer partition.
2. WHEN surface coverage is verified, THE Gate SHALL require complete unique catalog ownership and equality of descriptor, manifest, and callable-adapter method sets.
3. WHEN an RPC is `Implemented` or `Partial`, THE Gate SHALL require at least one current executed passing calibrated behavioural scenario for that RPC while retaining its actual feature state.
4. WHEN an RPC has another state or disposition, THE Gate SHALL enforce the corresponding mode-specific evidence obligation in Design §9 without treating declared rejection as proof of supported target behaviour.
5. WHEN evidence is evaluated, THE Gate SHALL reject tags without observed checked calls, wrong modes, stale revisions, empty selections, failed cases, and invariants-only runs as behavioural coverage.
6. WHEN GetSystemInfo is exercised, THE Gate SHALL require its capabilities to agree with the existing feature-matrix mapping in every applicable mode.

### Requirement 10: Test strength

**User Story:** As a maintainer, I want unvisited behaviours made visible, so that
generator drift cannot quietly weaken the suite.

**Trace:** Design §9; Property 14.

#### Acceptance Criteria

1. WHEN a lifecycle transition occurs, THE Harness SHALL count it by area and command kind and retain unvisited and uncalibrated rows.
2. WHEN an area finishes its fixed budget, THE Harness SHALL fail if any required sometimes predicate has count zero.
3. WHEN feature pairs occur in one execution lineage, THE Harness SHALL count them and list declared pairs never reached without equating pair coverage with order coverage.
4. WHEN local mutation runs exercise decision modules, THE Harness SHALL retain surviving mutants as oracle gaps in area evidence.

### Requirement 11: Behavioural ledger

**User Story:** As a maintainer, I want evidence by RPC, decision, and area, so that
coverage and unresolved findings remain distinct.

**Trace:** Design §9; Property 15.

#### Acceptance Criteria

1. WHEN model implementation begins, THE Harness SHALL establish the planned `docs/conformance/behavioural-ledger.md` page with the three row kinds and fields in Design §9.
2. WHEN decision evidence is rendered, THE Ledger SHALL retain the target function/path/lines, Tokeira counterpart, test, provenance, any source-only label, and one of the four defined decision verdicts.
3. WHEN a mismatch is resolved per area, THE Ledger SHALL record a correction or precise ignored decision with its reason without converting a mismatch into passing behavioural evidence.
4. WHEN identical evidence is rendered, THE Ledger SHALL produce identical sorted generated rows while preserving explanatory notes and separate scenario, model, corpus, and Tier 2 measures.

### Requirement 12: Dagger and local execution

**User Story:** As a maintainer, I want a bounded PR suite and replayable local runs,
so that regular checks remain practical without concealing incomplete work.

**Trace:** Design §11; Property 16.

#### Acceptance Criteria

1. WHEN a PR is checked, THE Harness SHALL run L1, fixed-budget L2, checked-in L3 replay, and the surface/coverage checks under Dagger using nextest and locked dependencies.
2. WHEN the PR execution budget is configured, THE Harness SHALL allocate fixed per-area work within the initial 270-second warmed execution budget and treat exhaustion as incomplete failure.
3. WHEN local run, replay, record, surface, or ledger operations execute, THE Runner SHALL use the same checks as CI and expose area/RPC/mode/seed selection without allowing a partial run to claim whole-surface success.
4. WHEN an error in Design §11 occurs, THE Harness SHALL return the stated nonzero classification and retain available replay inputs and evidence.
5. WHEN recording, mutation, long generation, DSQL, simulation faults, or TLA+ checks execute, THE Runner SHALL keep them local and outside the bounded PR service requirements.

### Requirement 13: DSQL campaign

**User Story:** As a maintainer, I want evidence on the reference backend, so that
control from an in-memory store is not confused with DSQL fidelity.

**Trace:** Design §11; Property 17.

#### Acceptance Criteria

1. WHEN the first corpus stage completes, THE Campaign SHALL run each active area's sequential cases and overlaps without the scheduler against an ephemeral DSQL cluster.
2. WHEN three more areas or the final area complete, THE Campaign SHALL repeat the active-area cases using the shared worker, model, and invariants.
3. WHEN campaign results are recorded, THE Ledger SHALL distinguish real backend evidence, observed scheduling, incomplete infrastructure outcomes, and teardown from deterministic in-memory results.

### Requirement 14: Fault simulation

**User Story:** As a maintainer, I want the same cases under crashes and ownership
changes, so that acknowledgements and eventual progress survive faults.

**Trace:** Design §§2, 11; Property 18.

#### Acceptance Criteria

1. WHEN L4 is enabled, THE Fault Adapter SHALL map the scenario language into the existing turmoil workspace while preserving its separate lock file.
2. WHEN fault seeds are trusted, THE Fault Adapter SHALL first demonstrate stable repeated-seed replay and resolve replay instability.
3. WHEN crashes, partitions, delivery holds, bounces, lease stalls, or ownership moves occur, THE Checker SHALL apply shared durability and progress invariants with explicit ambiguity and eventual-recovery assumptions.

### Requirement 15: TLA+ traces

**User Story:** As a maintainer, I want optional trace validation against existing
models, so that internal execution can be checked against its design invariants.

**Trace:** Design §11; Property 19.

#### Acceptance Criteria

1. WHEN L5 is selected after L4, THE Trace Adapter SHALL map L2/L4 observations to explicit actions and variables of the repository models listed in Design §11.
2. WHEN a trace is checked, THE Trace Checker SHALL validate its initial state, every next or declared stutter step, and model invariants using TLC.
3. WHEN a mapping is missing, a trace is truncated, or a transition is illegal, THE Trace Checker SHALL reject or mark the trace incomplete without claiming behavioural equivalence or liveness.

### Requirement 16: Area progression

**User Story:** As a maintainer, I want each area to extend the same evidence chain,
so that coverage grows coherently.

**Trace:** Design §§6, 10–11; Property 20.

#### Acceptance Criteria

1. WHEN the harness core is built, THE Harness SHALL begin with the seed scenarios for activities, workflow tasks, timers, and signals and the check that every workflow finishes.
2. WHEN another area is added, THE Harness SHALL follow the eleven-area order in Design §10.
3. WHEN an area claims completion, THE Harness SHALL include its cited model rules, decision cases, scenario vocabulary, calibrated corpus, lifecycle checks, invariants, and strength measures for the catalogue cases.
