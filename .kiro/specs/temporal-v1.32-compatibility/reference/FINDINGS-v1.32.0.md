# Temporal v1.32.0 corpus baseline

Status: complete. The baseline is **not clean** and does not support advancing
the advertised compatibility claim from **1.31.0**.

## Measured results

All **138 entrypoints** were attempted, each with a fresh engine, from `2026-09-15T10:39:18Z` to `2026-09-15T12:05:48Z`. The 137 upstream entrypoints produced **887 pass, 1,382 fail, 147 skip and 227 unfinished** outcomes.

The fork smoke test produced 0 / 1 / 0 / 0 outcomes (pass / fail / skip / unfinished). Across both sets, 112 skips are explicit registry exclusions and 35 are native runtime skips. The table preserves that original smoke failure; the corrected test passed in the [separate follow-up](#fork-owned-smoke-follow-up).

Suite dispositions: 40 unchanged-clean, 26 regression, 45 new-suite and 26 out-of-surface. The baseline is **not clean** and does not support advancing the compatibility claim.

The [measurement manifest](baseline-v1.32.0.json) preserves per-suite ownership, predecessor mappings, provenance and artifact hashes. The [complete outcome ledger](outcomes-v1.32.0.json) preserves every captured test identity, including failures and unfinished tests.

Engine binary SHA-256: `08b09f8c94950bb03aa03077d0ceda673f95599cc048ce2293db078a0e7183d2`. The copied harness sources were checked against the committed fork before the run.

## Findings for the delta specs

Completed clean coverage includes public Nexus endpoint management (34 pass
outcomes), worker deployment management (58 passes and five registry skips),
workflow updates, both history modes, priority, poller scaling, speculative
history pagination and relay-task timeouts.

- **Standalone activities:** the new execution-control APIs remain stubs. The
  legacy pause path accepts a second, different request ID where the corpus
  expects `FailedPrecondition`; reset tests expose timeout/failure and attempt
  differences. Standalone starts return `Standalone activity is disabled`, and
  batch update-all selectors are rejected. These cases belong to
  `v132-standalone-activities` (`tests/activity_api_pause_test.go`,
  `tests/activity_api_reset_test.go`, `tests/activity_api_update_test.go`,
  `tests/activity_standalone_test.go`; pause request-ID behavior is implemented
  in `service/history/workflow/activity.go @ v1.32.0`). The batch suites also include
  positive tests that enable stock-disabled activity batch operations; those
  results require the owner to validate the stock rejection separately.
- **Visibility:** `TestAdvancedVisibilitySuite/TestCountGroupByNamespaceDivision`
  is rejected, while `TestCountGroupByWorkflow` exposes different rejection text.
  The target converter allows `TemporalNamespaceDivision` grouping and suppresses
  the implicit default-division filter when that field is referenced. Unsupported
  grouping reports the rejected search attribute. Assign both cases to
  `v132-visibility-query-converter`
  (`common/persistence/visibility/store/query/converter.go @ v1.32.0`).
- **Callbacks:** both HSM and CHASM suites expose the callback allowlist rename:
  the `url_not_configured` and `https_required` cases receive success where they
  expect `InvalidArgument`. The callback limit message still says "workflow"
  where the target validator says "execution". Assign these to `v132-nexus`
  (`chasm/lib/callback/validator.go` and `chasm/lib/callback/config.go @ v1.32.0`).
  The separate `TestScheduledCallbackTokenMigration_LegacyWriteEnvelopeRead`
  failures inspect Temporal's internal completion token and require a cited
  registry disposition, not an engine representation change
  (`tests/callbacks_test.go @ v1.32.0`).
- **Execution lineage:** three continue-as-new/reset cases receive an empty
  `first_execution_run_id`. The target returns the lineage's first run ID on
  the existing-execution paths; assign this to `v132-lifecycle-fidelity`
  (`tests/continue_as_new_test.go` and `service/history/api/startworkflow/api.go
  @ v1.32.0`). The same suite also records a delay-start duration mismatch,
  which needs a focused replay before attributing its cause.
  The explicit-run reset case with a deleted current run also returns an empty
  `WorkflowExtendedInfo.reset_run_id` on the base run
  (`TestResetWorkflowTestSuite/TestResetWorkflowByRunID_CurrentExecutionMissing`,
  `tests/reset_workflow_test.go @ v1.32.0`).
- **Batch metadata:** `TestClientMiscTestSuite/TestListBatchOperations` does not
  observe all three started jobs within the corpus's 15-second bound. This is
  assigned to `v132-batch-operations-and-workers`; the baseline records the list
  failure without assuming which storage or query path caused it
  (`tests/client_misc_test.go @ v1.32.0`).
  The batch-delete cases provide separate evidence for the enum migration:
  `DescribeBatchOperation` returns legacy operation type 4 where the target
  expects `DELETE_WORKFLOW` (16). A `target_executions` selector is rejected as
  missing `visibility_query or executions`. `CountWorkers` reaches its named
  deferred stub. These are all within the same delta's explicit scope
  (`tests/workflow_api_batch_delete_test.go`,
  `tests/workflow_api_batch_terminate_test.go`, and
  `tests/worker_registry_test.go @ v1.32.0`).

- **Nexus error telemetry:** both API modes pass the request/response and existing
  request/latency metric checks, then fail the new `nexus_request_errors` assertion
  in error-outcome cases. The engine emits outcome-labelled request counters but
  no distinct error counter; the scrape adapter has no such sample to rename.
  This is an instrumentation gap for the Nexus owner to triage, not evidence that
  those RPC error assertions failed (`tests/nexus_api_test.go` and
  `service/frontend/nexus_handler.go @ v1.32.0`; engine
  `crates/tokeira-edge/src/metrics.rs` at the measured commit).
- **System Nexus endpoint:** the links suite expects a completed operation but
  receives a failed operation because `__temporal_system` is not found. Its
  primary lineage/link owner depends on `v132-nexus` for the system endpoint
  (`tests/links_test.go @ v1.32.0`). The positive signal-with-start-from-workflow
  suite reaches the same missing endpoint; its default-off posture must still be
  verified as a stock rejection (`tests/signal_with_start_from_workflow_test.go
  @ v1.32.0`).
- **Schedules:** the CHASM-mode public tests pause a `PauseOnFailure` schedule
  after cancellation or termination, and return raw catchup-window values from
  `DescribeSchedule`. The target defaults canceled/terminated failures off and
  describes nonpositive windows as 365 days, with positive values below ten
  seconds clamped to ten seconds. Assign these to `v132-lifecycle-fidelity`
  (`tests/schedule_test.go`, `chasm/lib/scheduler/config.go`,
  `chasm/lib/scheduler/spec_processor.go`, and
  `chasm/lib/scheduler/scheduler.go @ v1.32.0`). These assertions exercise public
  schedule behavior despite the suite's internal implementation-mode name.
  The V1 schedule suite also rejects a schedule-ID filter with
  `unsupported schedule query`. Its lifecycle owner needs the visibility delta
  for the schedule query conversion (`tests/schedule_test.go @ v1.32.0`).
- **Time skipping:** the first fast-forward case rejects a start with
  `missing required field: StartWorkflowExecutionRequest.time_skipping_config`;
  its request to enable time skipping is not delivered by the override bridge.
  These positive tests do not authorize implementing time skipping. The
  `v132-gated-surfaces` owner must verify the target's stock `UNIMPLEMENTED`
  rejection and message for requests carrying the configuration
  (`tests/timeskipping_fast_forward_test.go`, `service/frontend/errors.go`,
  `service/frontend/workflow_handler.go @ v1.32.0`).
- **Restored update coverage:** `TestWorkflowUpdateSuite` completes with 69 pass
  and nine registry-skip outcomes. The restored
  `TestUpdateWithStartSuite/TestReturnUpdateRateLimitError` fails after setting
  the wired maximum-total-updates override to one: its second-update rejection
  assertion receives no error. Assign the failure to lifecycle fidelity for a
  focused replay; keep the obsolete skip removed
  (`tests/update_workflow_test.go @ v1.32.0`).

- **Restored reset coverage:** the direct reset-with-options case returns the
  new pinned-override message without the legacy `behavior` and `pinned_version`
  fields expected alongside it. The batch variant lacks its expected
  workflow-options-updated history event. Lifecycle fidelity owns these results,
  with worker-deployment and batch-operation dependencies. A separate repeated
  reset test receives an empty `workflow_failed` metric capture, which needs
  telemetry triage before inferring a lifecycle error
  (`tests/workflow_reset_test.go @ v1.32.0`).
- **Deployment reactivation:** the reset-to-pinned-version case reports different
  drainage and deployment-version statuses from `DescribeWorkerDeploymentVersion`.
  The target's reactivation handler moves inactive or drained versions into
  draining. Assign this to `v132-worker-deployments`
  (`tests/worker_deployment_version_test.go`,
  `service/worker/workerdeployment/version_workflow.go @ v1.32.0`).
- **One-time versioning overrides:** the new suite panics while its first test
  seeds routing through internal `MatchingService.SyncDeploymentUserData`, a
  method the external adapter does not implement. It therefore does not reach
  that test's public child-override assertions. Keep the suite assigned to
  `v132-worker-deployments`: its owner needs a faithful public setup path or an
  explicit fixture disposition before it can supply coverage for the new
  contract (`tests/versioning_3_one_time_override_test.go:52` and
  `tests/versioning_test_env.go:601 @ v1.32.0`). The recorded panic is a harness
  limitation, not evidence that one-time override behavior was exercised.
  `TestVersioning3FunctionalSuite` encounters the same fixture in
  `TestChildWorkflowExplicitAutoUpgradeOverrideTakesPrecedence`
  (`tests/versioning_3_test.go:2072 @ v1.32.0`). Its later, unstarted leaves are
  absent from the dynamic outcome count, so its smaller baseline count must not
  be read as equivalent coverage of the previous clean matrix.

## Approved scope allocations

The integration seat assigned these public suites on 2026-09-15 to
`v132-lifecycle-fidelity`: workflow-type encoding (`TestWorkflowTypeEncodingSuite`),
speculative-task history pagination (`TestPrematureEosTestSuite`), relay workflow-task
timeouts/attempt counts (`TestRelayTaskTestSuite`), and all-nil memo/search-attribute
message presence (`TestNilSearchAttributeSuite`). Their corpus anchors are
`tests/workflow_type_encoding_test.go`, `tests/premature_eos_test.go`,
`tests/relay_task_test.go`, and `tests/nil_search_attribute_test.go @ v1.32.0`.
The orchestrator's full specs carry these allocations; the placeholder files are
unchanged. Measured outcomes remain in the table even when they pass.

`TestTaskQueueStats_Pri_Suite` public `DescribeTaskQueue` statistics belong to
`v132-batch-operations-and-workers`. Its cache-lifetime method and seven scenario
groups that force internal forwarding or asynchronous matching receive exact
registry exclusions, listed in the [addendum](#http-synchronization-and-scope-addendum-2026-09-15).
The default `NoTaskForwardNoPollForwardAllowSyncSuite` scenario, the non-root
request validation, and the empty-queue statistics case remain active. These use
public setup and assertions; the deployment helpers in this file use frontend RPCs,
not `SyncDeploymentUserData` (`tests/task_queue_stats_test.go @ v1.32.0`).

`TestHttpApiTestSuite/TestHTTPAPIHeaders` is **`expected-until-flip`**: it expects
`1.32.0` while the engine correctly advertises its `1.31.0` compatibility claim.
This is a recorded failure, not a skip, and must be re-verified at Phase 4. The
suite's captured counts, `regression` classification and lifecycle owner remain
unchanged because the baseline also contains other failures. The measurement
manifest has suite-level classifications only, so it receives no header-leaf
classification or rewritten counts.

The close-history HTTP read now carries the single sanctioned
`waitNewEvent=true` synchronization from the v1.31.0 fork. The baseline deliberately
omitted it; the integration seat authorized carrying it forward on 2026-09-15.
Signal admission does not guarantee closure, and long polling requires the flag
(`tests/http_api_test.go:152` and
`service/history/api/getworkflowexecutionhistory/api.go:220 @ v1.32.0`). The
[corpus evidence disclosure](../../../../docs/readiness/corpus-evidence.md#one-synchronized-corpus-assertion)
records the exception and outstanding upstream submission.

`TestActivityApiPause_AttributesToActivityInContextMetadata` now has an exact
registry exclusion. It enables the unwired startup-only
`frontend.contextMetadataSetTrailer` flag, whose default is false and whose value
is captured when constructing the frontend interceptor. The configuration
denominator already classifies it as architecturally excluded
(`tests/activity_api_pause_test.go:990`, `common/dynamicconfig/constants.go:869`,
`service/frontend/fx.go:525 @ v1.32.0`). Its original failed outcome remains in the
captured baseline.

## Measurement contract

The engine is campaign commit `f78b3412ff1f55edc109b162e9cecef0c7c82de4`, built
with `cargo build -p tokeirad --features conformance --locked` on Linux x86_64.
No engine behavior was changed for this baseline. The public compatibility claim
remains **1.31.0**, the campaign target is **1.32.0**, and the vendored API is
**v1.63.5**. The source-only build reports development provenance; its identity is
established separately by the binary SHA-256 and a byte-for-byte comparison of
1,305 Rust, manifest, lockfile and proto inputs plus 77 JSON and SQL inputs
against the engine commit.

The measured conformance fork is `tokeira/conformance-v1.32.0` at
`ea5246188aaae5e5f79e0c885e16279959285273`, directly based on Temporal tag
`v1.32.0` (`d94e34a1ebba5410a2e7d07119a76896909591aa`). Its predecessor is
`tokeira/conformance-v1.31.0` at
`5558d9422d33203d8aff9d42fe6b5663b4b1b1bc`. The harness and client adapters were
reapplied across the lazy-client and dynamic-config lifetime changes. The new
dedicated-cluster guard remains intact. All upstream corpus bodies, `go.mod` and
`go.sum` are byte-identical to the v1.32.0 tag. The toolchain is **go1.26.8**.

The denominator is the complete flat `./tests/` package: **137 upstream
entrypoints plus one fork-owned smoke test**. It does not include the separately
configured multi-cluster, replication or persistence subpackages. The runner
discovers names with `go test -list`, then attempts every discovered entrypoint.
Each runs in its own Go process against a fresh in-memory engine, with
`-count=1 -parallel=1 -tags test_dep`. The timeout is five minutes per entrypoint
and thirty minutes for `TestVersioning3FunctionalSuite`. Tests requiring
non-default configuration still attempt their overrides through the control
bridge; rejected overrides remain visible in the logs. Wire coverage is off.

The inherited runner reused one engine across the corpus. A preliminary run
revealed that a Go timeout can bypass cleanup and leave overrides or workflows
for later suites. That diagnostic run was stopped and is excluded from this
baseline. Fresh engines make the external state disposable when a test process
fails. Supplying an operator-managed frontend still explicitly opts into shared
state; that mode was not used for the measurement reported here.

To reproduce from the fork root with the same engine binary:

```sh
GOTOOLCHAIN=go1.26.8 TOKEIRA_BIN=/path/to/tokeirad \
  TOKEIRA_CONFORMANCE_RESULTS=events.jsonl \
  go run -tags test_dep ./tests/tokeira_conformance_runall/
GOTOOLCHAIN=go1.26.8 go run -tags test_dep \
  ./tests/tokeira_conformance_ledger/ events.jsonl outcomes.json
```

Unset `TOKEIRA_CONFORMANCE_FRONTEND_ADDR` when reproducing the fresh-engine run.
The engine must have the conformance feature. Retained raw evidence lives under
the operator artifact directory `artifacts/temporal-v1.32-baseline/`; it includes
the event stream, engine log, tool output, source manifest and release-to-fork
patch. The patch SHA-256 is
`6d8742902d423dfe83682021bce4f23523b5c8c258ede7f6aece14b5e9bc3a6c`.

## Reading the comparison

The v1.31 reference is the [64-entrypoint release ledger](../../../../docs/readiness/corpus-evidence.md):
1,261 pass outcomes, 22 native skips, 106 registry exclusions, zero failures and
zero unfinished outcomes. Its measured engine was
`cecc27e6dceb5385a4608bf5d5d5172df498fb3d`. The new baseline has a larger
denominator; an old entrypoint outside that ordered plan is not presumed clean.
All 64 previously clean entrypoints have identified v1.32 successors. History,
Nexus and signal suites were split or renamed, and activity-update casing changed.
There are 58 added entrypoint names and 20 removed names relative to the old fork;
those name changes are not themselves evidence of new behavior.

Counts include Go parent outcomes and every emitted subtest identity. They are
not counts of independent assertions. `unfinished` means a test emitted `run`
without a terminal event, usually when its process timed out or panicked. A
crash can also prevent later subtests from starting; those unvisited leaves are
not fabricated as passes or added to the dynamic outcome denominator. Attempting
every top-level entrypoint does not mean every leaf completed.

`regression` means a successor to a clean v1.31 ledger entry has failures or
unfinished outcomes. It can include a newly added assertion or a harness
limitation; it does not establish that this campaign introduced an engine bug.
`new-suite` means newly measured coverage without a clean predecessor in that
ledger, including previously unmeasured modes. `unchanged-clean` means a measured
successor has no failures or unfinished outcomes. `out-of-surface` identifies
internal fixtures or explicitly excluded implementation modes from their source.
A failing successor to a previously clean entry keeps the `regression` label
even when an internal fixture explains the failure. The Nexus owner must resolve
the proposed exclusions for the internal endpoint-suite successors; their past
clean state remains visible.

Owners are triage assignments for the approved delta specs. They do not authorize
new behavior or expand the placeholders. A `registry-skip` disposition includes
its source reason; if a newly discovered case was not registered before this run,
its actual failure or unfinished count remains unchanged in the table. Positive
tests that enable a stock-disabled feature do not expand the compatibility claim:
their owner must verify the stock rejection separately.

## Skip-registry migration

The captured fork's `tests/testcore/tokeira_conformance_skip_audit.json` records a source
citation and disposition for **all 118 inherited entries**: 99 retained, 12
removed, six renamed to their HSM and CHASM successors, and one narrowed. This
produced **112 exact active registry identities** at capture; the nine later
additions are recorded separately in the addendum below.

- Both callback modes now run: `history.enableChasm` and
  `history.enableCHASMCallbacks` default to true in
  `common/dynamicconfig/constants.go @ v1.32.0`.
- The sticky single-partition priority test now uses separate environments, so
  the old workflow-ID collision exclusion was removed
  (`tests/priority_fairness_test.go @ v1.32.0`).
- Continue-as-new thresholds, update rate limits and transient history-size
  thresholds have wired overrides; their old blanket exclusions were removed.
  Reset-with-options cases can use the existing membership adapter
  (`tests/update_workflow_test.go`, `tests/transient_task_test.go`,
  `tests/workflow_reset_test.go @ v1.32.0`).
- Five Nexus exclusions were removed where callbacks are opaque or the case
  tests public behavior. Six retained internal-token or mutable-state cases
  were mapped to both new suites (`tests/nexus_workflow_test.go @ v1.32.0`).
- Update-with-start now excludes only
  `TestUpdateWithStartSuite/TestUpdateIsAbortedByClosingWorkflow/return_retryable_error_after_retry`,
  which installs an in-process retry hook; its public sibling runs
  (`tests/update_workflow_test.go @ v1.32.0`).

Filtered names receive explicit `skip` events with
`Source: tokeira-skip-registry` and their reasons. Go's `-skip` flag emits no event
for excluded tests, so recording those identities separately prevents exclusions
from disappearing from the outcome ledger. Registry events and runtime events for
the same name collapse to one outcome during distillation.

## Fork-owned smoke follow-up

The captured `TestTokeiraConformance_BasicWorkflowLifecycle` failed at
`RegisterNamespace` with `PermissionDenied`. It opened a raw client before
starting the authorization callback listener expected by the conformance-mode
engine. The unavailable callback correctly caused authorization to fail closed.

Fork head `4d71235efae67869a5c2e75fa2836765cf9fd347` fixes only
`tests/tokeira_conformance_test.go`: it initializes the normal test environment,
uses its frontend client, and orders cleanup before stopping an owned engine.
The shared shim, upstream corpus bodies and Go module pins are identical to the
measured commit. Both startup paths passed separately on macOS with Go 1.26.8
and the same engine source built with `--features conformance --locked`:

- Standalone: `go test -tags test_dep -count=1 -timeout 2m -run '^TestTokeiraConformance_BasicWorkflowLifecycle$' ./tests`.
- Runner-owned engine: `go run -tags test_dep ./tests/tokeira_conformance_runsuite/ -timeout 2m '^TestTokeiraConformance_BasicWorkflowLifecycle$'` — one pass, zero failures, zero skips.

These follow-ups use `TOKEIRA_BIN` and no preconfigured frontend. The full Linux
corpus was not rerun after this one-file repair. Its original 2,644 outcomes
remain intact, and the measurement manifest records the separate follow-up
commit and validation hashes.

## Validation

The fork's complete testcore and conformance-tool tests passed on macOS and Linux.
`go vet -tags test_dep` passed for the shim, all tools and the flat corpus.
Repository-wide `make lint-code GOLANGCI_LINT_BASE_REV=v1.32.0
GOLANGCI_LINT_FIX=false` passed with zero new lint issues, including its error-type
vet stage. Upstream corpus integrity and Go formatting checks passed. A real-wire
start-workflow smoke test passed before the corpus run.

All six Rust completion commands passed on macOS on 2026-09-15: nightly
formatting, `cargo lint --locked`, workspace check, nextest with
`--no-fail-fast` (3,371 passed, two existing skips), doctests and rustdoc with
warnings denied. Dependency bans, licenses and sources passed. No engine source
or dependency file changed. Test linking emitted existing macOS deployment-target
warnings from native dependency archives; the lint command had no warnings.
Source-tree offline links passed: 1,200 checked, zero errors, with generated
build and operator-artifact directories excluded.

## Per-suite comparison

| Suite | 1.31.0 Ledger state | v1.32.0 baseline (pass / fail / skip / unfinished) | Classification | Owner |
|---|---|---|---|---|
| `TestAcquireShardSuite` | outside ordered plan | 0 / 4 / 0 / 0 | `out-of-surface` | `registry-skip` — Injects history-shard acquisition faults and inspects in-process retries/logging. Source: `tests/acquire_shard_test.go @ v1.32.0`. |
| `TestActivityAPIBatchCancelClientTestSuite` | new entrypoint/mode | 0 / 6 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityAPIBatchDeleteClientTestSuite` | new entrypoint/mode | 0 / 6 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityAPIBatchResetClientTestSuite` | outside ordered plan | 0 / 6 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityAPIBatchSecurityTestSuite` | outside ordered plan | 0 / 2 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityAPIBatchTerminateClientTestSuite` | new entrypoint/mode | 0 / 12 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityApiBatchUnpauseClientTestSuite` | outside ordered plan | 5 / 0 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityApiBatchUpdateOptionsClientTestSuite` | clean (6.34) | 2 / 2 / 0 / 0 | `regression` | `v132-standalone-activities` |
| `TestActivityApiPauseClientTestSuite` | outside ordered plan | 8 / 12 / 1 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestActivityApiPause_AttributesToActivityInContextMetadata` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Enables the unwired startup-only frontend.contextMetadataSetTrailer flag, whose stock default is false (common/dynamicconfig/constants.go; service/frontend/fx.go; tests/activity_api_pause_test.go @ v1.32.0). Source: `tests/activity_api_pause_test.go @ v1.32.0`. |
| `TestActivityApiResetClientTestSuite` | clean (6.34) | 9 / 16 / 0 / 0 | `regression` | `v132-standalone-activities` |
| `TestActivityApiRulesClientTestSuite` | clean (6.34) | 5 / 0 / 0 / 0 | `unchanged-clean` | `v132-standalone-activities` |
| `TestActivityApiUpdateClientTestSuite` | clean (6.34); renamed/split | 5 / 6 / 0 / 0 | `regression` | `v132-standalone-activities` |
| `TestActivityClientTestSuite` | clean (1.3) | 6 / 0 / 0 / 0 | `unchanged-clean` | `v132-standalone-activities` |
| `TestActivityParityTestSuite` | new entrypoint/mode | 26 / 99 / 1 / 129 | `new-suite` | `v132-standalone-activities` |
| `TestActivityTestSuite` | clean (1.3) | 10 / 0 / 0 / 0 | `unchanged-clean` | `v132-standalone-activities` |
| `TestActivityUpdateExecutionOptionsApi` | new entrypoint/mode | 0 / 5 / 0 / 0 | `new-suite` | `v132-standalone-activities` |
| `TestAddTasksSuite` | outside ordered plan | 0 / 2 / 0 / 1 | `out-of-surface` | `registry-skip` — Injects internal history-task hooks and calls AdminService/HistoryService.AddTasks. Source: `tests/add_tasks_test.go @ v1.32.0`. |
| `TestAdminBatchRefreshWorkflowTasksTestSuite` | outside ordered plan | 0 / 8 / 0 / 0 | `out-of-surface` | `registry-skip` — Exercises AdminService batch task-refresh, outside WorkflowService/OperatorService. Source: `tests/admin_batch_refresh_workflow_tasks_test.go @ v1.32.0`. |
| `TestAdminRebuildMutableState_ChasmDisabled` | outside ordered plan | 0 / 2 / 0 / 0 | `out-of-surface` | `registry-skip` — Exercises AdminService mutable-state rebuild. Source: `tests/admin_test.go @ v1.32.0`. |
| `TestAdminRebuildMutableState_ChasmEnabled` | outside ordered plan | 0 / 2 / 0 / 0 | `out-of-surface` | `registry-skip` — Exercises AdminService mutable-state rebuild. Source: `tests/admin_test.go @ v1.32.0`. |
| `TestAdvancedVisibilitySuite` | clean (4.24) | 19 / 3 / 12 / 0 | `regression` | `v132-visibility-query-converter` |
| `TestAdvancedVisibilitySuiteLegacy` | outside ordered plan | 19 / 3 / 12 / 0 | `new-suite` | `v132-visibility-query-converter` |
| `TestArchivalSuite` | outside ordered plan | 0 / 2 / 0 / 4 | `out-of-surface` | `registry-skip` — Requires Temporal archiver providers and persistence fixtures. Source: `tests/archival_test.go @ v1.32.0`. |
| `TestCallbacksMigrationSuite` | outside ordered plan | 4 / 0 / 0 / 0 | `new-suite` | `v132-nexus` |
| `TestCallbacksSuiteCHASM` | outside ordered plan | 9 / 6 / 0 / 0 | `new-suite` | `v132-nexus` |
| `TestCallbacksSuiteHSM` | clean (5.32) | 9 / 6 / 0 / 0 | `regression` | `v132-nexus` |
| `TestCancelWorkflowSuite` | clean (2.11) | 6 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestChasmSuite` | outside ordered plan | 0 / 49 / 0 / 0 | `out-of-surface` | `registry-skip` — Runs CHASM framework test components through the in-process ChasmContext. Source: `tests/chasm_test.go @ v1.32.0`. |
| `TestChildWorkflowSuite` | clean (3.14) | 8 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestClientDataConverterTestSuite` | clean (9.44) | 1 / 0 / 3 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestClientMiscTestSuite` | clean (9.44) | 24 / 2 / 0 / 0 | `regression` | `v132-batch-operations-and-workers` |
| `TestContinueAsNewTestSuite` | clean (3.15) | 8 / 5 / 0 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestCronTestClientSuite` | clean (3.16) | 2 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestCronTestSuite` | clean (3.16) | 3 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestDLQSuite` | outside ordered plan | 0 / 2 / 0 / 0 | `out-of-surface` | `registry-skip` — Writes Temporal persistence task queues and invokes internal DLQ administration. Source: `tests/dlq_test.go @ v1.32.0`. |
| `TestDeploymentVersionSuite` | clean (8.40) | 62 / 6 / 1 / 0 | `regression` | `v132-worker-deployments` |
| `TestDescribeTestSuite` | clean (3.21) | 3 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestDispatchCancelToWorkerWithEagerActivity` | new entrypoint/mode | 0 / 1 / 0 / 0 | `new-suite` | `v132-gated-surfaces` |
| `TestEagerWorkflowTestSuite` | clean (3.18) | 6 / 0 / 1 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestFairnessAutoEnableSuite` | clean (10.45) | 2 / 0 / 4 / 0 | `unchanged-clean` | `v132-batch-operations-and-workers` |
| `TestFairnessSuite` | clean (10.45) | 2 / 0 / 4 / 0 | `unchanged-clean` | `v132-batch-operations-and-workers` |
| `TestGetHistorySuite_DisableTransitionHistory` | clean (1.8); renamed/split | 5 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestGetHistorySuite_EnableTransitionHistory` | clean (1.8); renamed/split | 5 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestHistoryNodeCleanupSuite` | new entrypoint/mode | 0 / 2 / 0 / 1 | `out-of-surface` | `registry-skip` — Asserts deletion of Temporal history_tree/history_node storage rows. Source: `tests/history_node_cleanup_test.go @ v1.32.0`. |
| `TestHttpApiTestSuite` | clean (9.43) | 5 / 4 / 0 / 2 | `regression` | `v132-lifecycle-fidelity` |
| `TestLinksTestSuite` | clean (5.31) | 2 / 14 / 0 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestMaxBufferedEventSuite` | clean (2.13) | 2 / 0 / 1 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestMirroredIncludeExcludeSpec` | new entrypoint/mode | 0 / 1 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestMirroredIncludeExcludeSpecOnUpdate` | new entrypoint/mode | 0 / 1 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestNamespaceInterceptorTestSuite` | clean (4.28) | 2 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestNamespaceSuite` | clean (4.28) | 8 / 0 / 2 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestNexusAPIValidationTestSuite` | clean (7.36) | 21 / 2 / 0 / 0 | `regression` | `v132-nexus` |
| `TestNexusApiTestSuiteWithLegacyErrorPaths` | clean (7.38) | 18 / 21 / 1 / 0 | `regression` | `v132-nexus` |
| `TestNexusApiTestSuiteWithTemporalFailures` | clean (7.38) | 18 / 21 / 1 / 0 | `regression` | `v132-nexus` |
| `TestNexusEndpointsCommonSuite` | clean (7.35); renamed/split | 0 / 2 / 0 / 0 | `regression` | `v132-nexus` — proposed registry-skip: Compares Temporal persistence-manager and MatchingService endpoint table versions and pagination; public OperatorService coverage remains in TestNexusEndpointsOperatorSuite (tests/nexus_endpoint_test.go @ v1.32.0). Source: `tests/nexus_endpoint_test.go @ v1.32.0`. |
| `TestNexusEndpointsMatchingSuite` | clean (7.35); renamed/split | 15 / 2 / 0 / 0 | `regression` | `v132-nexus` — proposed registry-skip: Exercises internal MatchingService endpoint CRUD, clocks and table-version long polling; public OperatorService coverage remains in TestNexusEndpointsOperatorSuite (tests/nexus_endpoint_test.go @ v1.32.0). Source: `tests/nexus_endpoint_test.go @ v1.32.0`. |
| `TestNexusEndpointsOperatorSuite` | clean (7.35); renamed/split | 34 / 0 / 0 / 0 | `unchanged-clean` | `v132-nexus` |
| `TestNexusMatchingTestSuite` | new entrypoint/mode | 0 / 0 / 0 / 3 | `out-of-surface` | `registry-skip` — Dispatches directly to MatchingService and forces matching topology. Source: `tests/nexus_matching_test.go @ v1.32.0`. |
| `TestNexusStandaloneTestSuite` | new entrypoint/mode | 0 / 69 / 0 / 0 | `new-suite` | `v132-nexus` |
| `TestNexusWorkflowTestSuiteCHASM` | new entrypoint/mode | 19 / 12 / 10 / 0 | `new-suite` | `v132-nexus` |
| `TestNexusWorkflowTestSuiteHSM` | clean (7.37); renamed/split | 19 / 10 / 8 / 12 | `regression` | `v132-nexus` |
| `TestNexusWorkflowUpdateTestSuite` | new entrypoint/mode | 0 / 16 / 0 / 0 | `new-suite` | `v132-nexus` |
| `TestNilSearchAttributeSuite` | outside ordered plan | 4 / 3 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestPartitionScaling_Backlog` | new entrypoint/mode | 1 / 0 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts Temporal partition topology, forwarding and internal queue statistics. Source: `tests/partition_scaling_test.go @ v1.32.0`. |
| `TestPartitionScaling_Down` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts Temporal partition topology, forwarding and internal queue statistics. Source: `tests/partition_scaling_test.go @ v1.32.0`. |
| `TestPartitionScaling_Down_AndStopPolling` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts Temporal partition topology, forwarding and internal queue statistics. Source: `tests/partition_scaling_test.go @ v1.32.0`. |
| `TestPartitionScaling_Down_FromDC` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts Temporal partition topology, forwarding and internal queue statistics. Source: `tests/partition_scaling_test.go @ v1.32.0`. |
| `TestPartitionScaling_Up` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts Temporal partition topology, forwarding and internal queue statistics. Source: `tests/partition_scaling_test.go @ v1.32.0`. |
| `TestPartitionScaling_Up_FromDC` | new entrypoint/mode | 1 / 0 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts Temporal partition topology, forwarding and internal queue statistics. Source: `tests/partition_scaling_test.go @ v1.32.0`. |
| `TestPauseWorkflowExecutionSuite` | outside ordered plan | 1 / 24 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestPollerScalingFunctionalSuite` | clean (4.29) | 5 / 0 / 0 / 0 | `unchanged-clean` | `v132-batch-operations-and-workers` |
| `TestPrematureEosTestSuite` | outside ordered plan | 2 / 0 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestPrioritySuite` | clean (10.45) | 4 / 0 / 0 / 0 | `unchanged-clean` | `v132-batch-operations-and-workers` |
| `TestPurgeDLQTasksSuite` | outside ordered plan | 0 / 3 / 0 / 4 | `out-of-surface` | `registry-skip` — Seeds internal history task queues and invokes AdminService.PurgeDLQTasks. Source: `tests/purge_dlq_tasks_api_test.go @ v1.32.0`. |
| `TestQueryWorkflowSuite` | clean (2.10) | 10 / 0 / 1 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestRawHistorySuite` | clean (1.8) | 4 / 0 / 1 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestRelayTaskTestSuite` | outside ordered plan | 2 / 0 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestResetWorkflowTestSuite` | clean (3.17) | 16 / 2 / 0 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestScheduleCHASM` | outside ordered plan | 4 / 14 / 1 / 43 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestScheduleCHASMWorkflowPauseInteraction` | new entrypoint/mode | 10 / 5 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestScheduleCountsVisibility` | new entrypoint/mode | 0 / 1 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestScheduleCreationRolloutPercent` | new entrypoint/mode | 0 / 0 / 0 / 1 | `out-of-surface` | `registry-skip` — Asserts internal CHASM scheduler creation and rollout buckets via SchedulerClient.DescribeSchedule (tests/schedule_test.go @ v1.32.0). Source: `tests/schedule_test.go @ v1.32.0`. |
| `TestScheduleFarFutureActionTimes` | new entrypoint/mode | 1 / 0 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestScheduleManyCalendars` | new entrypoint/mode | 0 / 1 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestScheduleMigrationDeferredWithRunningWorkflow` | new entrypoint/mode | 0 / 0 / 0 / 1 | `out-of-surface` | `registry-skip` — Asserts migration of internal scheduler workflows to CHASM components. Source: `tests/schedule_migration_test.go @ v1.32.0`. |
| `TestScheduleMigrationTestSuite` | outside ordered plan | 0 / 2 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts migration of internal scheduler workflows to CHASM components. Source: `tests/schedule_migration_test.go @ v1.32.0`. |
| `TestScheduleMigrationV1ToV2NoDuplicateRecentActions` | outside ordered plan | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts migration of internal scheduler workflows to CHASM components. Source: `tests/schedule_migration_test.go @ v1.32.0`. |
| `TestScheduleMigration_NoRunningWorkflows_GeneratorStarts` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts migration of internal scheduler workflows to CHASM components. Source: `tests/schedule_migration_test.go @ v1.32.0`. |
| `TestScheduleMigration_StaleRunningDoesNotSkipPending` | new entrypoint/mode | 0 / 1 / 0 / 0 | `out-of-surface` | `registry-skip` — Asserts migration of internal scheduler workflows to CHASM components. Source: `tests/schedule_migration_test.go @ v1.32.0`. |
| `TestScheduleNextActionTimeVisibility` | new entrypoint/mode | 0 / 1 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestScheduleV1` | clean (5.30) | 38 / 11 / 5 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestScheduleV1WorkflowPauseInteraction` | new entrypoint/mode | 10 / 5 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestSignalWithStartFromWorkflowTestSuite` | new entrypoint/mode | 0 / 2 / 1 / 11 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestSignalWorkflowTestSuiteChasm` | new entrypoint/mode | 12 / 0 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestSignalWorkflowTestSuiteLegacy` | clean (2.9); renamed/split | 12 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestSizeLimitFunctionalSuite` | clean (3.20) | 1 / 0 / 4 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestStandaloneActivityTestSuite` | clean (6.33) | 1 / 300 / 3 / 0 | `regression` | `v132-standalone-activities` |
| `TestStickyTqTestSuite` | clean (1.4) | 3 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestTLSFunctionalSuite` | outside ordered plan | 0 / 3 / 0 / 0 | `out-of-surface` | `registry-skip` — Requires an in-process test cluster provisioned with WithMTLS and TLSConfigProvider; this external baseline is plaintext loopback. Source: `tests/tls_test.go @ v1.32.0`. |
| `TestTaskQueueStats_Pri_Suite` | outside ordered plan | 0 / 13 / 0 / 10 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestTaskQueueSuite` | clean (4.29) | 6 / 4 / 8 / 0 | `regression` | `v132-batch-operations-and-workers` |
| `TestTimeSkippingFastForwardFunctionalSuite` | new entrypoint/mode | 0 / 11 / 0 / 0 | `new-suite` | `v132-gated-surfaces` |
| `TestTimeSkippingPropagationTestSuite` | new entrypoint/mode | 0 / 13 / 0 / 0 | `new-suite` | `v132-gated-surfaces` |
| `TestTimeSkippingTestSuite` | new entrypoint/mode | 1 / 14 / 0 / 0 | `new-suite` | `v132-gated-surfaces` |
| `TestTransientTaskSuite` | clean (1.6) | 4 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestUpdateWithStartSuite` | clean (2.12) | 32 / 8 / 2 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestUpdateWorkflowSdkSuite` | clean (2.12) | 5 / 2 / 0 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestUserMetadataSuite` | clean (4.27) | 5 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestUserTimersTestSuite` | clean (1.5) | 2 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestVersioning3FunctionalSuite` | clean (8.41) | 13 / 2 / 33 / 0 | `regression` | `v132-worker-deployments` |
| `TestVersioning3OneTimeOverrideFunctionalSuite` | new entrypoint/mode | 0 / 2 / 0 / 0 | `new-suite` | `v132-worker-deployments` |
| `TestVersioningFunctionalSuite` | outside ordered plan | 0 / 411 / 5 / 0 | `out-of-surface` | `registry-skip` — Enables V1/V2 worker versioning, disabled in stock v1.32.0; stock rejection remains owned by v132-worker-deployments (common/dynamicconfig/constants.go; tests/versioning_test.go @ v1.32.0). Source: `tests/versioning_test.go @ v1.32.0`. |
| `TestWFTFailureReportedProblemsTestSuite` | clean (3.22) | 5 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkerCommandsTaskSuite` | new entrypoint/mode | 0 / 3 / 0 / 5 | `new-suite` | `v132-gated-surfaces` |
| `TestWorkerDeploymentSuite` | clean (8.39) | 58 / 0 / 5 / 0 | `unchanged-clean` | `v132-worker-deployments` |
| `TestWorkerRegistryTestSuite` | clean (8.42) | 6 / 2 / 0 / 0 | `regression` | `v132-batch-operations-and-workers` |
| `TestWorkflowAPIBatchCancelClientTestSuite` | new entrypoint/mode | 0 / 5 / 0 / 0 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestWorkflowAPIBatchDeleteClientTestSuite` | new entrypoint/mode | 0 / 5 / 0 / 0 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestWorkflowAPIBatchResetClientTestSuite` | new entrypoint/mode | 0 / 5 / 0 / 0 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestWorkflowAPIBatchSignalClientTestSuite` | new entrypoint/mode | 1 / 6 / 0 / 0 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestWorkflowAPIBatchTerminateClientTestSuite` | new entrypoint/mode | 0 / 6 / 0 / 0 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestWorkflowAPIBatchUpdateOptionsClientTestSuite` | new entrypoint/mode | 0 / 5 / 0 / 0 | `new-suite` | `v132-batch-operations-and-workers` |
| `TestWorkflowAliasSearchAttributeTestSuite` | clean (4.26) | 3 / 0 / 0 / 0 | `unchanged-clean` | `v132-visibility-query-converter` |
| `TestWorkflowBufferedEventsTestSuite` | clean (2.13) | 4 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowCompletionPaginationTestSuite` | new entrypoint/mode | 0 / 12 / 0 / 0 | `new-suite` | `v132-gated-surfaces` |
| `TestWorkflowDeleteExecutionSuite` | clean (3.19) | 4 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowFailuresTestSuite` | clean (1.7) | 4 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowMemoTestSuite` | clean (4.25) | 3 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowResetTestSuite` | clean (3.17) | 9 / 4 / 0 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestWorkflowResetWithChildTestSuite` | clean (3.17) | 4 / 0 / 6 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowTaskTestSuite` | clean (1.2) | 9 / 0 / 1 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowTestSuite` | clean (1.1) | 34 / 12 / 0 / 0 | `regression` | `v132-lifecycle-fidelity` |
| `TestWorkflowTimerTestSuite` | clean (1.5) | 3 / 0 / 0 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowTypeEncodingSuite` | new entrypoint/mode | 23 / 0 / 0 / 0 | `new-suite` | `v132-lifecycle-fidelity` |
| `TestWorkflowUpdateSuite` | clean (2.12) | 69 / 0 / 9 / 0 | `unchanged-clean` | `v132-lifecycle-fidelity` |
| `TestWorkflowVisibilityTestSuite` | clean (4.23) | 2 / 0 / 0 / 0 | `unchanged-clean` | `v132-visibility-query-converter` |

## Successor mappings

| v1.32.0 entrypoint | Previous entrypoint(s) |
|---|---|
| `TestAcquireShardSuite` | `TestAcquireShard_OwnershipLostErrorSuite`, `TestAcquireShard_DeadlineExceededErrorSuite`, `TestAcquireShard_EventualSuccess` |
| `TestActivityAPIBatchSecurityTestSuite` | `TestScheduleActivityOnPerNSTQ_Blocked` |
| `TestActivityApiPauseClientTestSuite` | `TestActivityAPIPauseClientTestSuite` |
| `TestActivityApiUpdateClientTestSuite` | `TestActivityAPIUpdateClientTestSuite` |
| `TestChasmSuite` | `TestChasmTestSuite`, `TestChasmTestSuiteLegacy` |
| `TestGetHistorySuite_DisableTransitionHistory` | `TestGetHistoryFunctionalSuite`, `TestRawHistoryClientSuite` |
| `TestGetHistorySuite_EnableTransitionHistory` | `TestGetHistoryFunctionalSuite`, `TestRawHistoryClientSuite` |
| `TestNexusEndpointsCommonSuite` | `TestNexusEndpointsFunctionalSuite` |
| `TestNexusEndpointsMatchingSuite` | `TestNexusEndpointsFunctionalSuite` |
| `TestNexusEndpointsOperatorSuite` | `TestNexusEndpointsFunctionalSuite` |
| `TestNexusWorkflowTestSuiteHSM` | `TestNexusWorkflowTestSuite` |
| `TestNilSearchAttributeSuite` | `TestWorkflowStart_NilSearchAttributesFiltered`, `TestWorkflowStart_AllNilSearchAttributesFiltered`, `TestDescribeWorkflow_NilSearchAttributesNotVisible`, `TestWorkflowStart_NilMemoFiltered`, `TestWorkflowStart_AllNilMemoFiltered`, `TestDescribeWorkflow_NilMemoNotVisible` |
| `TestSignalWorkflowTestSuiteLegacy` | `TestSignalWorkflowTestSuite` |

## HTTP synchronization and scope addendum — 2026-09-15

The integration seat authorized the synchronization, pre-flip classification and
scope allocations above on 2026-09-15. This addendum preserves the original
baseline: all suite counts, classifications and 2,644 captured outcomes are
unchanged. Only the five approved scope-allocation notes changed in
`baseline-v1.32.0.json`; that manifest has no per-leaf classification field, so
`expected-until-flip` is recorded here for the header leaf.

### HTTP rerun

The single-suite runner used engine commit
`d019b6058d713f1fa4345ad3a2f31617de66e2f0`, built on macOS with
`cargo build -p tokeirad --features conformance --locked`, and Go `go1.26.8`.
The engine self-reported `temporal_server 1.31.0` and `git d019b605`.
Its binary SHA-256 is
`74b04f9a7c282c89e101668fce2d0055594fef598553cf92a6d8cdaf7e404495`.
The measured fork commit is `182b30086c399c1803ee3524810cca472864147e`.

```sh
GOTOOLCHAIN=go1.26.8 TOKEIRA_BIN=/path/to/tokeirad \
  go run -tags test_dep ./tests/tokeira_conformance_runsuite/ \
  -timeout 5m '^TestHttpApiTestSuite$'
```

The runner started a fresh engine, with no preconfigured frontend, metrics,
control or authorization callback address. **Nine leaves passed, one failed,
zero skipped and zero unfinished.** The runner's tally includes the failed
suite parent and therefore reads **PASS 9 / FAIL 2 / SKIP 0**. The sole leaf
failure is the header's expected `1.32.0` versus actual `1.31.0` assertion
(`tests/http_api_test.go:361 @ v1.32.0`); it stays active and must pass after the
Phase 4 claim flip. No whole-corpus rerun was performed.

All leaf names below are beneath `TestHttpApiTestSuite/`:

| Leaf | Outcome | Disposition |
|---|---|---|
| `TestHTTPAPIBasics_Protojson` | pass | Explicit close-history wait |
| `TestHTTPAPIBasics_ProtojsonPretty` | pass | Explicit close-history wait |
| `TestHTTPAPIBasics_Shorthand` | pass | Explicit close-history wait |
| `TestHTTPAPIBasics_ShorthandPretty` | pass | Explicit close-history wait |
| `TestHTTPAPIHeaders` | fail | `expected-until-flip`; re-verify at Phase 4 |
| `TestHTTPAPI_OperatorService_ListSearchAttributes` | pass | Public operator HTTP API |
| `TestHTTPAPI_Serves_OpenAPIv2_Docs` | pass | Public API documentation |
| `TestHTTPAPI_Serves_OpenAPIv3_Docs` | pass | Public API documentation |
| `TestHTTPAPIPretty` | pass | Public JSON formatting |
| `TestHTTPHostValidation` | pass | Public HTTP host validation |

Retained runner output and its per-leaf distillation are under the operator
artifact directory `artifacts/t132-baseline-addenda/`. Their hashes are recorded
in `SHA256SUMS`; the runner log SHA-256 is `c401db60684c90dfe499316395712a16df5a4b7de3c6aa6497a4719f8422eebf`.

### Exact registry additions

The fork audit's `post_baseline_addenda` section records the following nine new
identities, separately from its original 118-entry migration review. There are
now **121 active registry identities**; the captured baseline retains its original
112 registry-skip outcomes. The seven scenario exclusions each cover six nested
statistics methods with the same forced internal setup. Their default-scenario
siblings stay active, along with `TestDescribeTaskQueue_NonRoot` and
`TestNoTasks_ValidateStats`; the suite remains owned by
`v132-batch-operations-and-workers`.

Two exact exclusions are:

| Exact test identity | Reason and source at v1.32.0 |
|---|---|
| `TestActivityApiPause_AttributesToActivityInContextMetadata` | Enables unwired startup-only `frontend.contextMetadataSetTrailer`, default false (`tests/activity_api_pause_test.go:990`; `service/frontend/fx.go:525`; `common/dynamicconfig/constants.go:869`). |
| `TestTaskQueueStats_Pri_Suite/TestAddMultipleTasks_ValidateStats_Cached` | Sets `matching.TaskQueueInfoByBuildIdTTL` to one hour and requires stale cached statistics after draining tasks (`tests/task_queue_stats_test.go:137-183`). |

The other seven exact identities all have the prefix
`TestTaskQueueStats_Pri_Suite/TestVersioningSuite/`:

| Scenario suffix | Internal setup |
|---|---|
| `NoTaskForwardNoPollForwardForceAsyncSuite` | Disable synchronous matching |
| `ForceTaskForwardNoPollForwardAllowSyncSuite` | Force writes to partition 11 |
| `ForceTaskForwardNoPollForwardForceAsyncSuite` | Force writes to partition 11 and disable synchronous matching |
| `NoTaskForwardForcePollForwardAllowSyncSuite` | Force polls to partition 5 |
| `NoTaskForwardForcePollForwardForceAsyncSuite` | Force polls to partition 5 and disable synchronous matching |
| `ForceTaskForwardForcePollForwardAllowSyncSuite` | Force writes to partition 11 and polls to partition 5 |
| `ForceTaskForwardForcePollForwardForceAsyncSuite` | Force writes to partition 11, polls to partition 5 and disable synchronous matching |

These scenario names are constructed in `tests/task_queue_stats_test.go:189-197`;
`tests/testcore/matching_behavior.go:39-73 @ v1.32.0` configures 13 partitions for
forwarding and installs `MatchingLBForceWritePartition`,
`MatchingLBForceReadPartition` and `MatchingDisableSyncMatch` hooks. The ordinary
`NoTaskForwardNoPollForwardAllowSyncSuite` uses public deployment registration and
current/ramping-version RPCs, so it remains in the public statistics allocation.
The registry tests verify that the default scenario's six methods and the two
public top-level cases are not filtered, while the internal groups and startup-only
trailer case are excluded exactly.

### Addendum validation

The testcore shim and all four conformance-tool packages passed their tests with
`GOTOOLCHAIN=go1.26.8 go test -tags test_dep`. `go vet -tags test_dep` passed for
those packages and the flat corpus. Source-integrity checks verified that the
HTTP query parameter is the only upstream test-body change, and that `go.mod`
and `go.sum` are unchanged. The audit covers all 121 unique active registry names.
The full fork `make lint-code` passed with zero new issues, including its
error-type vet step; the first cold analysis reported zero issues but exceeded
the ten-minute limit, and the cached rerun completed successfully.

On the engine side, `cargo run -p compatibility-docs --locked -- check-temporal`
passed, and the source-tree offline link check reported zero errors. All captured
counts, classifications, provenance and raw outcomes were compared with the base;
only the five approved manifest allocation notes differ. No engine source,
dependency file or delta placeholder changed.
