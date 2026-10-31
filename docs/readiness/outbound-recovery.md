# Remediation 1.9 delivery ledger

This is the combined remaining item 1.9 delivery after PRs 338–340. The
[approved specification](../../.kiro/specs/outbound-dispatch-recovery/bugfix.md) owns
scope; the [cutover runbook](../operations/outbound-recovery-cutover.md) owns operator
procedure. Architecture 042's unrelated proposals remain proposed.

Status: implementation and verification complete; PR review and exact-head merge
approval remain. See the executed gate results and named corpus outcomes below.

| Path | Implementation | Recovery/preservation evidence |
|---|---|---|
| Child start and release | [child executor](../../crates/tokeira-runtime/src/publisher/outbound/child.rs), [kernel metadata](../../crates/tokeira-kernel/src/outbound.rs) | Persisted seed and accepted policy, original acquisition, gated first task; generated lost creation/confirmation/release reply and duplicate-confirmation checks |
| External signal/cancel | [external executor](../../crates/tokeira-runtime/src/publisher/outbound/external.rs) | Original initiation payload/request ID, target provenance and typed terminal outcomes; generated publication-loss and duplicate-delivery checks |
| Nexus start/cancel | [Nexus executor](../../crates/tokeira-runtime/src/publisher/outbound/nexus.rs), [tracking](../../crates/tokeira-runtime/src/nexus.rs) | Untimed first attempts, persisted backoff and cancellation phase, stale worker outcomes; generated missing-publication and phase fencing checks |
| Completion callbacks | [publisher and scanner](../../crates/tokeira-runtime/src/publisher.rs) | One shared admission authority, original acquisition, shutdown ownership and metadata CAS retry; 10 focused callback tests pass, including 100 generated payload/deadline cases |
| Parent close | [close executor](../../crates/tokeira-runtime/src/publisher/outbound/close.rs) | Exact parent/initiation/first-child chain and latest current pointer; generated restart, reuse and lost delivery/retirement checks |
| Child result | [close executor](../../crates/tokeira-runtime/src/publisher/outbound/close.rs), [kernel transitions](../../crates/tokeira-kernel/src/kernel.rs) | First/final child identity, started-before-terminal ordering, buffering and reset redirects; generated identity/attribute and restart checks |
| Reset | [runtime reset](../../crates/tokeira-runtime/src/runtime/reset.rs), [pure suffix preparation](../../crates/tokeira-kernel/src/reset.rs) | Complete prefix/suffix/options before atomic participant/pointer publication; generated failed/lost publication, request retry and child reconnection checks |
| Oversized successor | [DSQL fallback](../../crates/tokeira-storage/src/dsql/run_repository/successor.rs), [memory fallback](../../crates/tokeira-storage/src/memory/successor.rs) | Close plus frozen intent/reservation, exclusive chain claim, exact publication/retirement; generated competing start, restart and lost publication checks |
| Cross-home effects | [runtime receiver](../../crates/tokeira-runtime/src/runtime/outbound_delivery.rs), [peer transport](../../crates/tokeira-edge/src/grpc/outbound_delivery.rs) | Reference-only RPC, separate audience, source and destination leases, bounded decode/attempts; generated credentials, malformed references and local/remote parity |
| Logical deletion and purge | [memory pins](../../crates/tokeira-storage/src/memory/outbound.rs), [DSQL purge](../../crates/tokeira-storage/src/dsql/run_repository/bulk_write.rs) | Retain close source, target evidence and future child-result/reset proof until settlement; generated deletion/restart/purge interleavings |
| Legacy cutover | [review/apply](../../crates/tokeira-storage/src/outbound_upgrade.rs), [CLI](../../crates/tokeira-engine/src/storage_maintenance.rs) | Stopped-writer confirmation, exact digest/revision, additive legacy metadata, no guessed policy or historical close delivery; generated interrupted scan/repair and large-state tests |

## Required properties

Every property requires at least 100 generated memory/model cases. Live DSQL cases
supplement that count. The linked modules contain the named property tags and real
commit/reload paths; a compile or a filtered-out test is not execution evidence.

| Properties | Location | Verification status |
|---|---|---|
| 1–2: cause/index atomicity; exact intent retirement | [storage properties](../../crates/tokeira-storage/src/memory/outbound_tests.rs) | 100-case properties pass in the final workspace run |
| 3–4: fair discovery; acquisition fencing | [driver properties](../../crates/tokeira-runtime/src/outbound/driver_tests.rs), storage properties | 100-case properties pass in the final workspace run |
| 5–6: stable child identity; ordered release | [crash exploration](../../crates/tokeira-runtime/src/outbound/bug_condition_exploration_tests.rs), [kernel properties](../../crates/tokeira-kernel/tests/support/outbound.rs) | Generated properties pass in the final workspace run |
| 7: definitive external outcomes | Kernel properties, [wire history](../../crates/tokeira-edge/src/translate/history_serializer.rs), crash exploration | Generated properties pass in the final workspace run |
| 8: Nexus incarnation/phase | Kernel properties, [runtime fencing](../../crates/tokeira-runtime/src/runtime/outbound_tests.rs) | Generated properties pass in the final workspace run |
| 9: callback preservation | Crash exploration and [callback integration](../../crates/tokeira-runtime/tests/runtime_nexus.rs) | 10 selected tests passed; new property ran 100 cases |
| 10: parent-close lineage | Crash exploration | Expanded first-run/parent/initiation/reuse property passed 100 cases |
| 11: child-result identity/attributes | Kernel properties, [codec](../../crates/tokeira-storage/src/codec/outbound_tests.rs), wire history | Generated properties pass in the final workspace run |
| 12: atomic reset | [preparation properties](../../crates/tokeira-storage/src/memory/preparation_tests.rs), [lane publication](../../crates/tokeira-runtime/src/lane/tests/outbound_publication.rs), crash exploration | Generated properties pass in the final workspace run |
| 13: reset retry/order/reconnection | Kernel properties and crash exploration | Order/retry checks and expanded child/reset/deletion property passed, 100 cases each |
| 14: successor reservation | [fallback properties](../../crates/tokeira-storage/src/memory/successor_tests.rs) | 100-case properties pass in the final workspace run |
| 15: bounded large values | Codec/preparation properties and [cutover properties](../../crates/tokeira-storage/src/memory/outbound_upgrade_tests.rs) | Earlier codec/publication runs passed; expanded large-cutover property passed 100 cases, including more than 1,000 operations and states above 4 MiB |
| 16: checked legacy preparation | Cutover properties and codec properties | Interrupted continuation, original policy/adoption and expanded current-format inspection passed, 100 cases each |
| 17: unaffected semantics | [successor preservation](../../crates/tokeira-runtime/src/lane/tests/atomic_successors.rs), kernel and callback properties | Expanded separate-clock/metadata/deadline property passed 100 cases |
| 18: peer authority | [auth properties](../../crates/tokeira-auth/src/peer.rs), [frame properties](../../crates/tokeira-edge/src/grpc/outbound_delivery_tests.rs), [runtime parity](../../crates/tokeira-runtime/src/runtime/outbound_delivery_tests.rs) | Purpose/capacity/reference/parity runs passed; expanded real gRPC frame property passed 100 cases |
| 19: retained proof | Storage properties and reset reconnection property | Close-source and destination-dedupe runs passed; future-result/reset properties passed 100 cases each |
| 20: end-to-end crash refinement | Crash exploration, real lane/repository fault seams | All 18 selected expanded properties passed: publication loss, child handshake cuts, external target/source reply loss, Nexus outcome commit loss, close delivery/retirement loss, reset and successor cuts; 100 cases per property |

## Delivery gates

- Live DSQL: **all six selected tests passed** on a fresh ephemeral Aurora DSQL
  cluster using the `default` profile in `eu-west-1`. A locked storage library archive
  with `dsql-integration` ran serially under `dsql-live`: six executed, 569 filtered.
  Bootstrap plus tests took 848.517 seconds. The earlier cutover failure exposed a
  TKUP pointer incorrectly taking inline point-load/scan paths; both readers now
  hydrate its pages in a consistent snapshot. The corrected live test verifies
  rediscovery as well as round-trip values, unsupported envelopes and reclamation.
  All three ephemeral clusters are deleted with absence verified. The disposable
  Namespace devbox was already absent at cleanup; its empty inventory is verified.
- Temporal v1.31.0 selected public-wire corpus: 32 passing leaves, one upstream
  skip and four prescribed registry exclusions, recorded by name below. No test
  bodies or skip rules changed.
- Final workspace bar passed on the disposable verification snapshot: formatting,
  `cargo lint --locked`, `cargo check --workspace --locked`,
  `cargo nextest run --workspace --locked --no-fail-fast` (4,121 passes, two skips),
  `cargo test --workspace --doc --locked`, and warning-denied workspace rustdoc.
  Additional DSQL/conformance-feature all-target lint, two conformance test-lock
  regressions and source-integrity verification passed on the corrected final source.
- Complete offline Markdown link validation passes with zero errors. The configured Changie fragment fields validate.
  Schema V127 and its packaged contract match; V001–V100 remain byte-for-byte
  unchanged. No dependency, lockfile or upstream proto changes.
- The boundary fetch confirmed `main` remains `e057750774e38776019bb6c8e7043ebde902a1d5`.
  The attributed commit and one combined PR are the publication step. Merge requires human
  approval of the exact head and green checks.

The two default workspace ignores are
`sdk_echo_roundtrip_against_post_sync_tokeirad` and
`sdk_scoped_worker_roundtrip_uses_production_authorization_bootstrap` in the
[existing Rust SDK integration suite](../../apps/tokeira-bench/tests/v0_4_integration.rs).
These explicitly ignored integration tests start a server and SDK worker (the
latter also starts a JWKS endpoint); their existing ignore annotations were retained. The
569 unselected live-test cases and 615 unselected conformance-feature cases are
filters, not successful executions.


## Live DSQL results

The [live recovery suite](../../crates/tokeira-storage/src/dsql/run_repository/outbound_live_tests.rs)
supplements the 100-case memory/model properties; it does not replace their case counts.

| Test | Result |
|---|---|
| `dsql::run_repository::atomic_successors_live_tests::atomic_successors_live` | Pass |
| `dsql::run_repository::outbound_live_tests::outbound_live_future_child_result_retains_deleted_parent` | Pass |
| `dsql::run_repository::outbound_live_tests::outbound_live_interrupted_large_legacy_cutover` | Pass |
| `dsql::run_repository::outbound_live_tests::outbound_live_original_fences_and_retained_destination_proof` | Pass |
| `dsql::run_repository::outbound_live_tests::outbound_live_reset_conflict_preserves_all_participants` | Pass |
| `dsql::run_repository::outbound_live_tests::outbound_live_successor_reservation_and_interrupted_publication` | Pass |

## Pinned functional results

These selected tests ran over the public gRPC wire against a local, conformance-feature
`tokeirad` with its memory backend on 31 October 2026. The corpus was an isolated copy
of `tests/` at Temporal `v1.31.0` (`83881961df2bad7d78d93a2d50778dbc8bd1a2fc`),
with the existing runner/skip registry from the reference fork's
`tokeira/conformance-v1.31.0` commit `423ae614e3f471de696e2c890ff0cb05236b0a88`.
Selected test bodies were verified unchanged from the pinned tag. This is focused
regression evidence, not a claim that the entire functional corpus passes or that
these wire tests used DSQL.

Final leaf outcomes: **32 passes, one upstream skip, four prescribed registry
exclusions, zero remaining failures**. Suite/container passes are not counted as
additional tests. The initial callback failures were corrected and all four affected
leaves rerun successfully: base-run reset termination preserves redirected callbacks
in standby (`service/history/workflow/mutable_state_impl.go::processCloseCallbacks`),
and admission removes outer start-event links already attached to Nexus callbacks
(`service/frontend/workflow_handler.go::dedupLinksFromCallbacks`), both at `v1.31.0`.
Focused Rust regressions cover the two corrections, including 100 generated reset
callback cases.

| Test leaf | Final outcome |
|---|---|
| `TestCallbacksSuiteHSM/TestNexusResetWorkflowWithCallback` | Pass |
| `TestCallbacksSuiteHSM/TestWorkflowNexusCallbacks_CarriedOver/ContinueAsNew` | Pass |
| `TestCallbacksSuiteHSM/TestWorkflowNexusCallbacks_CarriedOver/WorkflowFailureRetry` | Pass |
| `TestCallbacksSuiteHSM/TestWorkflowNexusCallbacks_CarriedOver/WorkflowRunTimeout` | Pass |
| `TestCancelWorkflowSuite/TestRequestCancelWorkflowCommandExecution_TargetFinished` | Pass |
| `TestCancelWorkflowSuite/TestRequestCancelWorkflowCommandExecution_TargetNotFound` | Pass |
| `TestCancelWorkflowSuite/TestRequestCancelWorkflowCommandExecution_TargetRunning` | Pass |
| `TestChildWorkflowSuite/TestChildWorkflowExecution` | Pass |
| `TestChildWorkflowSuite/TestCronChildWorkflowExecution` | Pass |
| `TestChildWorkflowSuite/TestRetryChildWorkflowExecution` | Pass |
| `TestContinueAsNewTestSuite/TestChildWorkflowWithContinueAsNew` | Pass |
| `TestContinueAsNewTestSuite/TestChildWorkflowWithContinueAsNewParentTerminate` | Pass |
| `TestContinueAsNewTestSuite/TestContinueAsNewWorkflow` | Pass |
| `TestContinueAsNewTestSuite/TestWorkflowContinueAsNewTaskID` | Pass |
| `TestCronTestSuite/TestCronWorkflow_Failed_Infinite` | Pass |
| `TestNexusWorkflowTestSuite/TestNexusOperationCancelBeforeStarted_CancelationEventuallyDelivered` | Pass |
| `TestNexusWorkflowTestSuite/TestNexusOperationCancelation` | Pass |
| `TestNexusWorkflowTestSuite/TestNexusOperationSyncCompletion_LargePayload` | Pass |
| `TestNexusWorkflowTestSuite/TestNexusOperationSyncNexusFailure` | Pass |
| `TestSignalWorkflowTestSuite/TestSignalExternalWorkflowCommand` | Pass |
| `TestSignalWorkflowTestSuite/TestSignalExternalWorkflowCommand_SignalSelf` | Pass |
| `TestSignalWorkflowTestSuite/TestSignalExternalWorkflowCommand_UnKnownTarget` | Pass |
| `TestSignalWorkflowTestSuite/TestSignalExternalWorkflowCommand_WithoutRunID` | Pass |
| `TestWorkflowResetTestSuite/TestDifferentBaseCurrentClosed` | Pass |
| `TestWorkflowResetTestSuite/TestDifferentBaseCurrentRunning` | Pass |
| `TestWorkflowResetTestSuite/TestRepeatedResets` | Pass |
| `TestWorkflowResetTestSuite/TestSameBaseCurrentClosed` | Pass |
| `TestWorkflowResetTestSuite/TestSameBaseCurrentRunning` | Pass |
| `TestWorkflowResetWithChildTestSuite/TestResetWithChild_AfterChildCompletes` | Pass |
| `TestWorkflowResetWithChildTestSuite/TestResetWithChild_AfterStartingChild` | Pass |
| `TestWorkflowResetWithChildTestSuite/TestResetWithChild_RunningChild_SetWID` | Upstream skip: reset phase 2 disabled in the pinned test |
| `TestWorkflowTestSuite/TestWorkflowRetry` | Pass |
| `TestWorkflowTestSuite/TestWorkflowRetryFailures` | Pass |

The following selected methods are excluded by the existing
`tests/testcore/tokeira_conformance_skip.go` registry at the reference-fork commit
above. They did not execute and are not passes; neither the registry nor corpus was
modified for this delivery.

| Selected method | Prescribed reason |
|---|---|
| `TestNexusWorkflowTestSuite/TestNexusOperationSyncCompletion` | Inspects internal AdminService HSM state after public completion; that internal representation is outside the claim. The large-payload public completion leaf above executed successfully. |
| `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAfterReset` | Assumes Temporal's internal callback-token format. Tokeira's versioned token and operation fencing are a documented representation deviation; owned tests cover recovery. |
| `TestNexusWorkflowTestSuite/TestNexusCallbackAfterCallerComplete` | Existing registry deferral for callback failed-state/last-failure Describe coverage. No pass is claimed for this excluded case. |
| `TestWorkflowResetTestSuite/TestResetWorkflowWithOptionsUpdate` | Requires the internal MatchingClient/version-membership helper, absent from this conformance cluster; its versioned-poller goroutine would panic. Owned reset ordering/options properties pass separately. |
