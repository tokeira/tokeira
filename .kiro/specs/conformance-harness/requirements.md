# Requirements Document

## Introduction

The conformance harness (`crates/tokeira-conformance`) is an executable oracle for Tokeira's public
gRPC surface. It boots the real in-process stack — tonic gRPC server over `tokeira-edge` over
`TokeiraRuntime` over `InMemoryStore` with projection workers — drives RPCs through real
`WorkflowServiceClient` / `OperatorServiceClient` connections, and asserts both the RPC response and the
emitted `HistoryEvent` sequence against the behaviour of the targeted Temporal release
(`TEMPORAL_SERVER_COMPAT` = v1.31.0; vendored API `TEMPORAL_PROTO_VERSION` = v1.62.11). It then emits a
machine-checked conformance report and enforces a coverage gate over the full RPC surface so coverage
cannot silently regress.

These requirements are **derived from** the approved design document (`design.md`) and state WHAT the
harness must do. The design states HOW. Because the harness is correctness-critical test infrastructure,
its own "correctness" is that its verdicts are sound (it never reports green over an uncovered or
divergent RPC) and its fixture is hermetic (no live AWS, Docker, or network dependency, and no leaked
ports or background tasks). Every Correctness Property in the design maps to one or more acceptance
criterion below.

## Glossary

- **Harness**: The `tokeira-conformance` crate as a whole (library + integration tests + CLI binary).
- **TestCluster**: The fixture that boots the real in-process Tokeira stack and holds the connected
  clients, the `InMemoryStore` handle, and the teardown guard.
- **WorkerPoller**: The worker-side primitive that drives poll → respond loops for workflow tasks,
  activity tasks, and heartbeats over real gRPC.
- **History_Matcher**: The `ExpectedHistory` DSL and its matcher, which assert an expected
  `HistoryEvent` sequence/shape against the actually emitted history.
- **Coverage_Manifest**: The checked-in enumeration (`manifest.toml`) of every targeted RPC, each row
  carrying an `RpcKey`, service, group, and status.
- **Coverage_Gate**: The component that folds registered test tags against the `Coverage_Manifest` and
  produces a coverage verdict.
- **Reconciliation**: The three-way check between the served surface, the `Coverage_Manifest`, and the
  tracker.
- **Served_Surface**: The set of RPC keys derived at runtime from
  `tokeira_proto::public::FILE_DESCRIPTOR_SET` — the authoritative wire shape.
- **Tracker**: `.kiro/specs/api-conformance-tracker/tracker.md`, the human/Codex-maintained
  field-threading plan.
- **Tag_Registry**: The collection of per-test RPC tags (`RpcKey`s) gathered across the suite for the
  `Coverage_Gate` to fold.
- **Conformance_Report**: The first-class output artifact aggregating run metadata, per-RPC verdicts,
  coverage summary, and reconciliation result; emitted as human text or JSON.
- **Conformance_CLI**: The `tokeira-conformance` binary exposing `run`, `coverage`, `list-rpcs`, and
  `verify-manifest`.
- **CI_Pipeline**: The workspace CI workflow (`.github/workflows/ci.yml`) that runs the harness.
- **RpcKey**: A stable RPC identity, the fully-qualified `service/method`
  (e.g. `temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution`).
- **Deferred RPC**: An RPC whose tracker status is `Deferred`; exempt from the coverage requirement
  until promoted.

## Requirements

### Requirement 1: In-process TestCluster fixture

**User Story:** As a conformance-test author, I want a reusable fixture that boots the real Tokeira stack
in-process and tears it down cleanly, so that I can drive RPCs over real gRPC without any external
dependency and without leaking resources between tests.

#### Acceptance Criteria

1. WHEN a test starts a cluster via `TestCluster::builder().start()` or `TestCluster::start_default()`, THE TestCluster SHALL, within 10 seconds, boot the in-process stack (tonic gRPC server, `tokeira-edge`, `TokeiraRuntime`, `InMemoryStore`, and projection workers) and return a `WorkflowServiceClient` and an `OperatorServiceClient` each able to complete at least one RPC without a transport-level connection error, plus an `InMemoryStore` handle.
2. THE TestCluster SHALL use `InMemoryStore` persistence and a loopback (`127.0.0.1`) gRPC endpoint, with no AWS, Docker, or network dependency.
3. WHEN two TestCluster instances run concurrently, THE TestCluster SHALL bind distinct ephemeral loopback ports and own distinct `InMemoryStore` instances, such that state mutated through one instance is not observable through the other.
4. WHEN a test calls `TestCluster::shutdown`, THE TestCluster SHALL, within 5 seconds, release the bound port (such that the port is re-bindable) and cancel every projection worker (such that every projection `JoinHandle` has completed), such that no background task survives the fixture.
5. IF a test panics while holding a TestCluster, THEN THE TestCluster SHALL signal shutdown and abort projection tasks on `Drop`, such that the bound port and background workers are released.
6. THE TestClusterBuilder default configuration SHALL reproduce the existing `spawn_test_server` wiring: one `default` namespace, 16 visibility partitions, 4 runtime execution lanes, and `Default` values for the `LaneConfig`, `TimerScannerConfig`, `WorkflowTimeoutScannerConfig`, and `BacklogConfig` runtime configs.
7. WHEN a test calls `fetch_history` for an existing execution, THE TestCluster SHALL return the full forward `HistoryEvent` sequence read from the authoritative transition log — ascending by `event_id` from 1, contiguous (no gaps), and untruncated — draining all history pages.
8. IF cluster startup fails (for example a port bind failure), THEN THE TestCluster SHALL return `ConformanceError::Startup` rather than panicking.
9. IF a test calls `fetch_history` for an execution that does not exist, THEN THE TestCluster SHALL return `ConformanceError::Rpc` carrying a not-found status rather than an empty sequence or a panic.

### Requirement 2: Worker-poller primitive

**User Story:** As a conformance-test author, I want a worker-poller that polls tasks and responds with
commands, so that I can advance a workflow past `StartWorkflowExecution` and exercise behaviour that is
otherwise unreachable.

#### Acceptance Criteria

1. WHEN a test polls a workflow task through the WorkerPoller and a task is available, THE WorkerPoller SHALL issue a real long-poll and return a `WorkflowTask` carrying a non-empty task token, a positive started event id, the attached decision history, and the workflow execution identifiers (workflow id and run id).
2. WHEN a test responds to a polled workflow task with a command list, THE WorkerPoller SHALL submit `RespondWorkflowTaskCompleted` and return the response, including the case of an empty command list.
3. WHEN a test responds to a polled workflow task with a failure cause, THE WorkerPoller SHALL submit `RespondWorkflowTaskFailed`.
4. WHEN a test polls an activity task through the WorkerPoller and a task is available, THE WorkerPoller SHALL return an `ActivityTask` carrying its task token, activity id, activity type, and input.
5. WHEN a test completes or fails a polled activity task through the WorkerPoller, THE WorkerPoller SHALL submit the corresponding `RespondActivityTaskCompleted` or `RespondActivityTaskFailed` and return success.
6. WHEN a test records an activity heartbeat through the WorkerPoller, THE WorkerPoller SHALL submit `RecordActivityTaskHeartbeat` and return the response, including its cancel-requested flag.
7. WHILE awaiting a task, THE WorkerPoller SHALL bound each long-poll by a configurable poll budget (default 10 seconds; any strictly positive `Duration`) and rely on long-poll plus bounded-retry synchronization rather than explicit sleeps.
8. IF no task becomes available within the poll budget, THEN THE WorkerPoller SHALL return `None` from a `poll_*` call (or `ConformanceError::PollTimeout` from the ergonomic completion helper), distinguishing "no task available" from a hang.
9. IF a poll or respond call fails at the transport layer or is rejected by the server (for example a malformed, unknown, or expired task token), THEN THE WorkerPoller SHALL return `ConformanceError::Transport` or `ConformanceError::Rpc` preserving the `tonic::Status` rather than panicking.

### Requirement 3: History-event assertions

**User Story:** As a conformance-test author, I want to assert the emitted `HistoryEvent` sequence and
shape against the v1.31.0 expected behaviour, so that conformance is proven at the history level and not
only at the RPC response.

#### Acceptance Criteria

1. WHEN a test calls `ExpectedHistory::assert_matches` against emitted history, THE History_Matcher SHALL align expected events to actual `HistoryEvent`s positionally and in order under the active match mode, verify each expected event's `EventType` and any opt-in attribute or placement assertion, and return `Ok` when all expected events match.
2. THE History_Matcher SHALL support match modes `Exact` (the script and the actual history have equal length and every position matches), `Prefix` (the script matches the actual history's leading events and the actual history is at least as long as the script), and `ContiguousSubsequence` (the script matches some contiguous run of actual events).
3. IF the emitted history diverges from the script, THEN THE History_Matcher SHALL return `ConformanceError::HistoryMismatch` whose `step` is the zero-based script index of the first event that diverges in type or attribute and whose `reason` describes the divergence.
4. WHERE an expected event carries a placement or attribute assertion closure (for example signal links on the outer `HistoryEvent.links`, field 302), THE History_Matcher SHALL evaluate the closure against the matched event and, IF the closure returns `Err`, SHALL treat that event's step as a divergence and surface the closure's error text in `reason`.
5. WHERE an `ExpectedHistory` encodes a non-obvious expectation, THE History_Matcher SHALL carry a non-empty `ground_truth` citation string referencing a `proto/upstream/` path or Temporal server source at tag v1.31.0.
6. THE History_Matcher SHALL derive its verdict solely from the emitted `HistoryEvent`s passed to it, reading no runtime, storage, or projection internals.
7. IF the actual history is shorter than the script, or has unequal length under `Exact`, or contains no matching window under `ContiguousSubsequence`, THEN THE History_Matcher SHALL return `ConformanceError::HistoryMismatch` identifying the first unsatisfied script step rather than panicking or indexing out of bounds.

### Requirement 4: Coverage manifest and gate

**User Story:** As a harness maintainer, I want a checked-in manifest enumerating every RPC plus a gate
that fails when a served RPC has no behavioural test, so that coverage cannot silently regress and the
tracker's hand-maintained number is replaced with a machine-verified one.

#### Acceptance Criteria

1. THE Coverage_Manifest SHALL enumerate every RPC of the targeted surface (the 121 RPCs of the WorkflowService and OperatorService surfaces for Temporal v1.31.0 / API v1.62.11), each row carrying a unique `RpcKey` (no `RpcKey` appears twice), a service, a group, and a status drawn from `Implemented`, `Partial`, `Stubbed`, or `Deferred`.
2. WHEN a behavioural test declares the RPC(s) it exercises by tag, THE Tag_Registry SHALL record those `RpcKey`s, including the case where a single test tags multiple RPCs.
3. WHEN the Coverage_Gate folds the Tag_Registry against the Coverage_Manifest, THE Coverage_Gate SHALL pass only if every non-`Deferred` RPC has at least one tagged behavioural test.
4. IF any non-`Deferred` RPC has zero tagged behavioural tests, THEN THE Coverage_Gate SHALL fail and SHALL enumerate every such uncovered RPC by `RpcKey`.
5. THE Coverage_Gate SHALL compute the coverage percentage as the count of non-`Deferred` RPCs with at least one tagged test, divided by the total count of non-`Deferred` RPCs, multiplied by 100, expressed in the range 0.00–100.00 rounded to two decimal places.
6. IF the Coverage_Manifest contains no non-`Deferred` RPC, THEN THE Coverage_Gate SHALL report a coverage percentage of 100.00 and pass.
7. IF the Tag_Registry contains a tag whose `RpcKey` is absent from the Coverage_Manifest, THEN THE Coverage_Gate SHALL fail and SHALL identify that `RpcKey`.

### Requirement 5: Three-way reconciliation

**User Story:** As a harness maintainer, I want the served surface, the manifest, and the tracker
reconciled, so that no RPC can drift between the wire, the executable artifact, and the planning ledger.

#### Acceptance Criteria

1. THE Reconciliation SHALL derive the Served_Surface as the set of RpcKeys, one per service method, enumerated from `tokeira_proto::public::FILE_DESCRIPTOR_SET`.
2. IF the Served_Surface contains one or more RPCs absent from the Coverage_Manifest, THEN THE Reconciliation SHALL return an error listing every such RpcKey in `missing_from_manifest`.
3. IF the Coverage_Manifest and the Tracker disagree on any RPC — an RpcKey present in one but not the other, or an RpcKey present in both with differing status — THEN THE Reconciliation SHALL return an error listing every such mismatch in `tracker_drift`.
4. WHEN the Coverage_Manifest contains every RpcKey of the Served_Surface (equal or proper superset) and the Coverage_Manifest's RpcKey set and per-RPC status are identical to the Tracker's, THE Reconciliation SHALL succeed with both `missing_from_manifest` and `tracker_drift` empty.
5. THE Reconciliation SHALL reconcile RPC identity and status only, parsing the Tracker's RPC tables without consuming the Tracker's field-level detail.
6. IF the Tracker's RPC tables cannot be parsed into an RpcKey-and-status list, THEN THE Reconciliation SHALL return an error indicating the parse failure rather than reporting a clean reconciliation.

### Requirement 6: Conformance report

**User Story:** As a harness maintainer and CI consumer, I want a machine-checked conformance report,
so that each run is auditable, diffable, and able to drive the build outcome.

#### Acceptance Criteria

1. THE Conformance_Report SHALL include run metadata: `temporal_server_compat` (`1.31.0`), `temporal_proto_version` (`v1.62.11`), the `tokeira-conformance` crate version, and an RFC3339-formatted UTC timestamp recording when the run completed.
2. THE Conformance_Report SHALL include (a) one per-RPC row for every RPC in the Coverage_Manifest, each carrying its `RpcKey`, group, status, the count of tagged behavioural tests (an integer of 0 or greater), and exactly one behavioural verdict drawn from `Pass`, `Fail`, `NotCovered`, or `Deferred`; (b) a coverage summary carrying the covered count, the total non-`Deferred` count, the coverage percentage, and a per-group breakdown of covered and total counts for each group; and (c) the reconciliation result.
3. WHEN a Conformance_Report is serialized to JSON and subsequently deserialized, THE Conformance_Report SHALL produce a value equal to the original in every field, with no field omitted, added, or altered.
4. THE Conformance_Report SHALL be emittable in a human-readable text form and in a JSON form, both conveying the same run metadata, per-RPC rows, coverage summary, and reconciliation result.
5. THE `passed()` method of the Conformance_Report SHALL return true if and only if (a) every non-`Deferred` RPC row has verdict `Pass`, (b) every non-`Deferred` RPC has at least one tagged behavioural test (coverage percentage equals 100), and (c) the reconciliation result's `missing_from_manifest` and `tracker_drift` lists are both empty.
6. WHEN the Conformance_Report assigns a row's behavioural verdict, THE Conformance_Report SHALL set it to `Deferred` if the RPC's status is `Deferred`; otherwise to `NotCovered` if the RPC has zero tagged behavioural tests; otherwise to `Fail` if at least one of its tagged behavioural cases failed; otherwise to `Pass`.
7. IF an RPC's behavioural verdict is `Fail`, THEN THE Conformance_Report SHALL include in that row the name of the first failing behavioural case and a reason indicating the divergence that caused the failure.
8. THE `passed()` result SHALL determine the Conformance_CLI exit code (0 when `passed()` is true, a non-zero code when false) and the gate `#[test]` outcome (passing when true, failing when false).

### Requirement 7: CLI surface

**User Story:** As an operator, I want `run`, `coverage`, `list-rpcs`, and `verify-manifest` commands
with correct exit codes, so that I can execute conformance checks locally and in automation.

#### Acceptance Criteria

1. WHEN `run` is invoked without filters, THE Conformance_CLI SHALL boot a TestCluster, execute all tagged conformance cases, and emit a Conformance_Report.
2. WHEN `coverage` is invoked, THE Conformance_CLI SHALL compute and print the coverage gate result by folding the Tag_Registry against the Coverage_Manifest, without re-running cases.
3. WHEN `list-rpcs` is invoked, THE Conformance_CLI SHALL print the Coverage_Manifest.
4. WHEN `verify-manifest` is invoked, THE Conformance_CLI SHALL run only the three-way Reconciliation.
5. WHERE `--format json` is set, THE Conformance_CLI SHALL emit the report as JSON; otherwise (the default) THE Conformance_CLI SHALL emit the human-readable form.
6. IF a produced report has `passed()` equal to false (a failed case, a coverage gap, or manifest drift), THEN THE Conformance_CLI SHALL exit with code 1; WHEN `passed()` is true, THE Conformance_CLI SHALL exit with code 0.
7. WHEN `run` is invoked with `--group` and/or `--rpc` filters, THE Conformance_CLI SHALL execute only the cases matching the filter (the intersection when both are given) and emit a Conformance_Report scoped to them.
8. IF a `run` filter matches zero cases, THEN THE Conformance_CLI SHALL exit with code 1 rather than emitting a passing report over an empty case set.
9. IF the CLI cannot complete a command (for example cluster startup failure, an unparseable Coverage_Manifest or Tracker, or an invalid filter value), THEN THE Conformance_CLI SHALL surface an error indicating the cause and exit with code 1 rather than emitting a passing report.

### Requirement 8: CI integration

**User Story:** As a harness maintainer, I want the coverage gate to run both as a `cargo test` and as a
CLI step that publishes the JSON report, so that local runs catch regressions and CI publishes an
auditable artifact.

#### Acceptance Criteria

1. THE Coverage_Gate SHALL be implemented as a Rust `#[test]` located at `tests/coverage_gate.rs`, executable under `cargo test --workspace` using only the standard cargo toolchain and requiring no additional installed tooling.
2. WHEN the CI_Pipeline runs the conformance step, THE CI_Pipeline SHALL invoke the Conformance_CLI `coverage --format json` command.
3. IF any non-`Deferred` RPC has zero tagged behavioural tests, or the three-way Reconciliation reports drift between the Served_Surface, the Coverage_Manifest, and the Tracker, THEN THE CI_Pipeline SHALL fail the conformance step with a non-zero outcome.
4. THE gate `#[test]` and the CLI `coverage` step SHALL read the identical checked-in Coverage_Manifest (`manifest.toml`) and the identical Tag_Registry, such that both yield the same coverage verdict for a given workspace state.
5. IF any non-`Deferred` RPC has zero tagged behavioural tests, or the three-way Reconciliation reports drift, THEN THE Coverage_Gate `#[test]` SHALL fail, such that `cargo test --workspace` reports a non-zero result.
6. WHEN the conformance step's `coverage --format json` command completes, THE CI_Pipeline SHALL publish the emitted JSON Conformance_Report as a downloadable build artifact, regardless of whether the coverage gate passed or failed.
7. IF the `coverage` command cannot read the Coverage_Manifest or derive the Served_Surface, THEN THE CI_Pipeline SHALL fail the conformance step and surface an error indicating the cause.

### Requirement 9: Maximal coverage intent and Deferred lifecycle

**User Story:** As a harness maintainer, I want the harness to target the full public RPC surface with
`Deferred` RPCs exempt until promoted, so that the gate expresses maximal coverage intent while not
blocking on RPCs that are not yet claimed for v1.31.0 behaviour.

#### Acceptance Criteria

1. THE Coverage_Manifest SHALL enumerate every RPC of the WorkflowService and OperatorService surfaces (all 121 RPCs of the v1.31.0 / v1.62.11 target) as a manifest row, with no RPC of those two services absent.
2. WHERE an RPC has status `Deferred`, THE Coverage_Gate SHALL exempt that RPC from the coverage requirement: it requires no tagged behavioural test, it is excluded from the non-`Deferred` denominator, and the gate does not fail on its account.
3. WHEN a `Deferred` RPC's status changes to any non-`Deferred` status in the Coverage_Manifest, THE Coverage_Gate SHALL require at least one tagged behavioural test for that RPC and SHALL include it in the non-`Deferred` denominator.
4. IF a non-`Deferred` RPC has zero tagged behavioural tests, THEN THE Coverage_Gate SHALL fail and SHALL count that RPC as not covered while retaining it in the non-`Deferred` denominator.
