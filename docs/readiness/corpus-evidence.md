# Tokeira v0.1.0 release evidence — Temporal v1.31.0 functional conformance

This is the gating functional-conformance evidence for Tokeira `v0.1.0`. It records the
ordered public-surface plan in
[`functional-test-order.md`](./functional-test-order.md), driven through the
[`functional-conformance-harness.md`](../testing/functional-conformance-harness.md)
single-suite runner against the exact engine commit that the release tag names.

## Measured identity

| Field | Value |
|---|---|
| Run date | 2026-08-28 |
| Release tag | annotated tag `v0.1.0` (`3c7c7c768e4972013af753f1aca4ff41838091e2`) |
| Tag target / measured engine | `cecc27e6dceb5385a4608bf5d5d5172df498fb3d` |
| Conformance fork | [`tokeira/temporal`](https://github.com/tokeira/temporal) (fork of `temporalio/temporal`), branch `tokeira/conformance-v1.31.0` at `5558d9422d33203d8aff9d42fe6b5663b4b1b1bc` |
| Temporal server compatibility | `1.31.0` |
| Temporal proto | `v1.62.11` |
| Rust toolchain | `1.97.1` |
| Go toolchain | `go1.26.2` |
| Evidence archive | `/workspaces/ev1-final-evidence-cecc27e6-intended` |
| Archive checksum manifest | `SHA256SUMS` · SHA-256 `cf8987d2c470bd2b05fc528b1894908a73f652c0028868617300519df3a97172` |

The proto pin is deliberately ahead of the server claim: Temporal server 1.31.0 ships
API `v1.62.8`, while Tokeira vendors `v1.62.11`. Protobuf wire shapes are
backward-compatible across that range, and RPCs present only in the newer surface are
outside the 1.31.0 behavioural claim — the two pins move independently by design
(`crates/tokeira-build-info/src/pinned.rs`).

The freshly cloned source built this binary and reported:

```text
tokeira 0.1.0+cecc27e6
git cecc27e6
temporal_proto v1.62.11
temporal_server 1.31.0
```

The verbose self-report was:

```text
server_version: 0.1.0+cecc27e6
tokeira_version: 0.1.0
tokeira_git_sha: cecc27e6
temporal_proto_version: v1.62.11
temporal_server_compat: 1.31.0
rust_toolchain: 1.97.1
source_tree_hash: 0000000000000000000000000000000000000000000000000000000000000000
feature_matrix_digest: dev
sdk_matrix_digest: dev
build_mode: dev
```

The Git SHA is the release identity for this development-mode build. The all-zero
`source_tree_hash` and `dev` matrix digests are reported exactly as emitted; they are
not represented as production provenance values.

The engine main ref, the fork branch, and the Odori main ref remained fixed at their
recorded start tips throughout the run.

### The conformance harness interface

The measured binary is built with the engine's conformance feature, which mounts two
harness-only surfaces and nothing else: the wire-coverage recorder (enabled per boot by
environment variable) and the configuration-override bridge, which delivers the
corpus's `WithDynamicConfig` values for a wired key set
(`.kiro/specs/conformance-config-override/`). Neither surface exists in a default
build, and neither adds anything to the production configuration schema. Corpus keys
outside the wired set are precisely the OverrideDynamicConfig-class registry exclusions
listed below — wired keys are honored, unwired keys are excluded by name rather than
silently defaulted.

## Scope and result

The release gate is the in-scope ordered plan, not every top-level Go entrypoint in
the fork. The plan expands to 64 test-bearing entrypoints:

- Tier 5.32 is the public HSM callbacks mode, `TestCallbacksSuiteHSM`.
  `TestCallbacksSuiteCHASM` is a whole-entrypoint registry exclusion because it enables
  CHASM framework internals outside the default v1.31.0 compatibility gate.
- Every discovered top-level entrypoint is either in the ordered plan or classified
  below, by name, with the reason it is out of scope; out-of-plan entrypoints are not
  included in release-gating totals.

The intended ordered plan is clean: every test-bearing entrypoint completed, with no
failure or unfinished outcome.

| Measure | Result |
|---|---:|
| Test-bearing entrypoints | 64 |
| Pass outcomes | 1,261 |
| Corpus-native skip outcomes | 22 |
| Exact fork-registry exclusions under those entrypoints | 106 |
| Fail outcomes | **0** |
| Unfinished outcomes | **0** |

“Clean” therefore means that every active outcome passed or was skipped by the pinned
upstream corpus itself, and every harness exclusion has an exact cited registry entry.
The 106 registry exclusions are counted directly from
`tests/testcore/tokeira_conformance_skip.go` at the pinned fork SHA. The runner applies
them through `go test -skip`, so those names do not appear as runtime `skip` events;
they are reported separately rather than silently folded into the 22 corpus-native
skip outcomes.

## Per-tier summary

| Tier | Entrypoints | Pass | Native skip | Registry exclusions | Fail | Unfinished | Wire rows |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12 | 87 | 0 | 3 | 0 | 0 | 167 |
| 2 | 8 | 144 | 0 | 15 | 0 | 0 | 141 |
| 3 | 12 | 71 | 6 | 7 | 0 | 0 | 169 |
| 4 | 9 | 54 | 5 | 17 | 0 | 0 | 129 |
| 5 | 3 | 37 | 0 | 4 | 0 | 0 | 53 |
| 6 | 5 | 193 | 0 | 3 | 0 | 0 | 125 |
| 7 | 5 | 178 | 0 | 13 | 0 | 0 | 75 |
| 8 | 4 | 453 | 7 | 36 | 0 | 0 | 132 |
| 9 | 3 | 37 | 3 | 0 | 0 | 0 | 54 |
| 10 | 3 | 7 | 1 | 8 | 0 | 0 | off¹ |
| **Total** | **64** | **1,261** | **22** | **106** | **0** | **0** | **1,045** |

¹ Tier 10.45 ran with wire coverage disabled under the pre-authorized observer-effect
exception described below.

## Per-entrypoint evidence

Pass counts include the Go parent outcome where the distiller emits it. “Registry” is
the count of exact pinned-fork exclusions beneath that entrypoint; it is distinct from
runtime corpus-native skips.

| Tier | Entrypoint | Pass | Native skip | Registry | Fail | Unfinished | Wire coverage |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1.1 | `TestWorkflowTestSuite` | 34 | 0 | 0 | 0 | 0 | 23 rows |
| 1.2 | `TestWorkflowTaskTestSuite` | 9 | 0 | 1 | 0 | 0 | 14 rows |
| 1.3 | `TestActivityTestSuite` | 10 | 0 | 0 | 0 | 0 | 20 rows |
| 1.3 | `TestActivityClientTestSuite` | 6 | 0 | 0 | 0 | 0 | 16 rows |
| 1.4 | `TestStickyTqTestSuite` | 3 | 0 | 0 | 0 | 0 | 12 rows |
| 1.5 | `TestUserTimersTestSuite` | 2 | 0 | 0 | 0 | 0 | 10 rows |
| 1.5 | `TestWorkflowTimerTestSuite` | 3 | 0 | 0 | 0 | 0 | 10 rows |
| 1.6 | `TestTransientTaskSuite` | 3 | 0 | 1 | 0 | 0 | 11 rows |
| 1.7 | `TestWorkflowFailuresTestSuite` | 4 | 0 | 0 | 0 | 0 | 15 rows |
| 1.8 | `TestGetHistoryFunctionalSuite` | 9 | 0 | 0 | 0 | 0 | 14 rows |
| 1.8 | `TestRawHistorySuite` | 1 | 0 | 1 | 0 | 0 | 6 rows |
| 1.8 | `TestRawHistoryClientSuite` | 3 | 0 | 0 | 0 | 0 | 16 rows |
| 2.9 | `TestSignalWorkflowTestSuite` | 12 | 0 | 0 | 0 | 0 | 23 rows |
| 2.10 | `TestQueryWorkflowSuite` | 10 | 0 | 1 | 0 | 0 | 18 rows |
| 2.11 | `TestCancelWorkflowSuite` | 6 | 0 | 0 | 0 | 0 | 15 rows |
| 2.12 | `TestWorkflowUpdateSuite` | 68 | 0 | 10 | 0 | 0 | 24 rows |
| 2.12 | `TestUpdateWorkflowSdkSuite` | 6 | 0 | 0 | 0 | 0 | 18 rows |
| 2.12 | `TestUpdateWithStartSuite` | 36 | 0 | 3 | 0 | 0 | 16 rows |
| 2.13 | `TestWorkflowBufferedEventsTestSuite` | 4 | 0 | 0 | 0 | 0 | 14 rows |
| 2.13 | `TestMaxBufferedEventSuite` | 2 | 0 | 1 | 0 | 0 | 13 rows |
| 3.14 | `TestChildWorkflowSuite` | 8 | 0 | 0 | 0 | 0 | 15 rows |
| 3.15 | `TestContinueAsNewTestSuite` | 8 | 0 | 0 | 0 | 0 | 14 rows |
| 3.16 | `TestCronTestSuite` | 3 | 0 | 0 | 0 | 0 | 15 rows |
| 3.16 | `TestCronTestClientSuite` | 2 | 0 | 0 | 0 | 0 | 14 rows |
| 3.17 | `TestResetWorkflowTestSuite` | 17 | 0 | 0 | 0 | 0 | 22 rows |
| 3.17 | `TestWorkflowResetTestSuite` | 10 | 0 | 2 | 0 | 0 | 15 rows |
| 3.17 | `TestWorkflowResetWithChildTestSuite` | 4 | 6 | 0 | 0 | 0 | 15 rows |
| 3.18 | `TestEagerWorkflowTestSuite` | 6 | 0 | 1 | 0 | 0 | 12 rows |
| 3.19 | `TestWorkflowDeleteExecutionSuite` | 4 | 0 | 0 | 0 | 0 | 15 rows |
| 3.20 | `TestSizeLimitFunctionalSuite` | 1 | 0 | 4 | 0 | 0 | 1 row |
| 3.21 | `TestDescribeTestSuite` | 3 | 0 | 0 | 0 | 0 | 12 rows |
| 3.22 | `TestWFTFailureReportedProblemsTestSuite` | 5 | 0 | 0 | 0 | 0 | 19 rows |
| 4.23 | `TestWorkflowVisibilityTestSuite` | 2 | 0 | 0 | 0 | 0 | 13 rows |
| 4.24 | `TestAdvancedVisibilitySuite` | 21 | 1 | 11 | 0 | 0 | 21 rows |
| 4.25 | `TestWorkflowMemoTestSuite` | 3 | 0 | 0 | 0 | 0 | 15 rows |
| 4.26 | `TestWorkflowAliasSearchAttributeTestSuite` | 3 | 0 | 0 | 0 | 0 | 13 rows |
| 4.27 | `TestUserMetadataSuite` | 5 | 0 | 0 | 0 | 0 | 9 rows |
| 4.28 | `TestNamespaceSuite` | 7 | 0 | 2 | 0 | 0 | 14 rows |
| 4.28 | `TestNamespaceInterceptorTestSuite` | 2 | 0 | 0 | 0 | 0 | 11 rows |
| 4.29 | `TestTaskQueueSuite` | 6 | 4 | 4 | 0 | 0 | 15 rows |
| 4.29 | `TestPollerScalingFunctionalSuite` | 5 | 0 | 0 | 0 | 0 | 18 rows |
| 5.30 | `TestScheduleV1` | 18 | 0 | 4 | 0 | 0 | 25 rows |
| 5.31 | `TestLinksTestSuite` | 5 | 0 | 0 | 0 | 0 | 11 rows |
| 5.32 | `TestCallbacksSuiteHSM` | 14 | 0 | 0 | 0 | 0 | 17 rows |
| 6.33 | `TestStandaloneActivityTestSuite` | 174 | 0 | 3 | 0 | 0 | 52 rows |
| 6.34 | `TestActivityApiResetClientTestSuite` | 6 | 0 | 0 | 0 | 0 | 19 rows |
| 6.34 | `TestActivityAPIUpdateClientTestSuite` | 5 | 0 | 0 | 0 | 0 | 15 rows |
| 6.34 | `TestActivityApiBatchUpdateOptionsClientTestSuite` | 3 | 0 | 0 | 0 | 0 | 18 rows |
| 6.34 | `TestActivityApiRulesClientTestSuite` | 5 | 0 | 0 | 0 | 0 | 21 rows |
| 7.35 | `TestNexusEndpointsFunctionalSuite` | 54 | 0 | 0 | 0 | 0 | 22 rows |
| 7.36 | `TestNexusAPIValidationTestSuite` | 23 | 0 | 0 | 0 | 0 | 10 rows |
| 7.37 | `TestNexusWorkflowTestSuite` | 23 | 0 | 11 | 0 | 0 | 21 rows |
| 7.38 | `TestNexusApiTestSuiteWithLegacyErrorPaths` | 39 | 0 | 1 | 0 | 0 | 11 rows |
| 7.38 | `TestNexusApiTestSuiteWithTemporalFailures` | 39 | 0 | 1 | 0 | 0 | 11 rows |
| 8.39 | `TestWorkerDeploymentSuite` | 56 | 3 | 2 | 0 | 0 | 31 rows |
| 8.40 | `TestDeploymentVersionSuite` | 71 | 0 | 1 | 0 | 0 | 50 rows |
| 8.41 | `TestVersioning3FunctionalSuite` | 319 | 4 | 33 | 0 | 0 | 39 rows |
| 8.42 | `TestWorkerRegistryTestSuite` | 7 | 0 | 0 | 0 | 0 | 12 rows |
| 9.43 | `TestHttpApiTestSuite` | 11 | 0 | 0 | 0 | 0 | 17 rows |
| 9.44 | `TestClientMiscTestSuite` | 25 | 0 | 0 | 0 | 0 | 29 rows |
| 9.44 | `TestClientDataConverterTestSuite` | 1 | 3 | 0 | 0 | 0 | 8 rows |
| 10.45 | `TestPrioritySuite` | 3 | 0 | 1 | 0 | 0 | off¹ |
| 10.45 | `TestFairnessSuite` | 2 | 0 | 4 | 0 | 0 | off¹ |
| 10.45 | `TestFairnessAutoEnableSuite` | 2 | 1 | 3 | 0 | 0 | off¹ |

## Wire coverage

Wire coverage was enabled on 61 of the 64 intended boots. Each enabled boot wrote to a
unique absolute per-entrypoint path in the retained archive. Together the files contain
1,045 `(wire_method, status_code)` rows and occupy 157,482 bytes. Every file contains a
successful `WorkflowService/GetSystemInfo` row. The Tier 1.1 self-check parsed cleanly
and produced 23 rows / 3,483 bytes before the run continued.

The wire JSON schema records method, status, and count; it does not embed the
`GetSystemInfo` response payload. Binary identity is independently corroborated by the
build self-report and the provenance line at the start of every one of the 64 suite
logs. All 64 log identities are identical: engine `cecc27e6`, Temporal server `1.31.0`,
and proto `v1.62.11`. This report does not claim payload-level identity inside the wire
JSON itself.

### Tier 10.45 observer-effect exception

Coverage was deliberately off for `TestPrioritySuite`, `TestFairnessSuite`, and
`TestFairnessAutoEnableSuite`. The measured recorder overhead was approximately 100 ms
per observed RPC; these suites issue roughly 800 RPCs while upstream imposes a 60 s
deadline. Enabling the observer would therefore add roughly 80 s and change the result
being measured. The three runs still used fresh `tokeirad` processes with the same
verified binary and produced 7 pass outcomes, 1 corpus-native skip, 0 failures, and 0
unfinished outcomes. No wire files exist for those three runs, by design.

## Corpus-native skips

These 22 skips are authored by the pinned v1.31.0 corpus rather than Tokeira's registry.
The reasons below are the upstream `Skip` text or adjacent upstream comment.

| Tier | Tests | Count | Upstream reason |
|---:|---|---:|---|
| 3.17 | `TestWorkflowResetWithChildTestSuite/TestResetWithChild`<br>`TestWorkflowResetWithChildTestSuite/TestResetWithChild_RunningChild_RandomWID`<br>`TestWorkflowResetWithChildTestSuite/TestResetWithChild_RunningChild_SetWID`<br>`TestWorkflowResetWithChildTestSuite/TestResetWithChild_RunningChild_SetWID_WithRejectDuplicate`<br>`TestWorkflowResetWithChildTestSuite/TestResetWithChild_WithChildID`<br>`TestWorkflowResetWithChildTestSuite/TestResetWithChild_WithChildID_WithRejectDuplicate` | 6 | “Skipping until reset phase 2 is enabled” (`tests/workflow_reset_with_child_test.go @ v1.31.0`). |
| 4.24 | `TestAdvancedVisibilitySuite/TestListWorkflow_OrderBy` | 1 | Only applicable to Elasticsearch; this run used SQL visibility (`tests/advanced_visibility_test.go @ v1.31.0`). |
| 4.29 | `TestTaskQueueSuite/TestPerKeyRateLimit_Default_IsEnforcedAcrossThreeKeys`<br>`TestTaskQueueSuite/TestPerKeyRateLimit_WeightOverride_IsEnforcedAcrossThreeKeys`<br>`TestTaskQueueSuite/TestTaskQueueAPIRateLimitOverridesWorkerLimit`<br>`TestTaskQueueSuite/TestWholeQueueLimit_TighterThanPerKeyDefault_IsEnforced` | 4 | “skip until we make it less flaky” (`tests/task_queue_test.go @ v1.31.0`). |
| 8.39 | `TestWorkerDeploymentSuite/TestConcurrentPollers_ManyTaskQueues_RapidRoutingUpdates_RevisionConsistency` | 1 | “Skipping until we can figure out why this test is flaky in sqlite” (`tests/worker_deployment_test.go @ v1.31.0`). |
| 8.39 | `TestWorkerDeploymentSuite/TestCreateWorkerDeployment_MaxDeploymentsLimit`<br>`TestWorkerDeploymentSuite/TestNamespaceDeploymentsLimit` | 2 | Upstream `Skip`: the limit tests must be separated so other methods do not create deployments in the same namespace (`tests/worker_deployment_test.go @ v1.31.0`). |
| 8.41 | `TestVersioning3FunctionalSuite/TestPinnedCaN_upgradeOnCaN_CrossTQ_Inherit`<br>`TestVersioning3FunctionalSuite/TestPinnedCaN_upgradeOnCaN_CrossTQ_NoInherit`<br>`TestVersioning3FunctionalSuite/TestPinnedCaN_upgradeOnCaN_SameTQ`<br>`TestVersioning3FunctionalSuite/TestUnpinnedCaN_upgradeOnCaN` | 4 | “run after SDK exposes CaN option” (`tests/versioning_3_test.go @ v1.31.0`). |
| 9.44 | `TestClientDataConverterTestSuite/TestClientDataConverter`<br>`TestClientDataConverterTestSuite/TestClientDataConverterFailed`<br>`TestClientDataConverterTestSuite/TestClientDataConverterWithChild` | 3 | Upstream unconditional `SkipNow`; the first is annotated “need to figure out what is going on,” while the other two carry no more specific reason (`tests/client_data_converter_test.go @ v1.31.0`). |
| 10.45 | `TestFairnessAutoEnableSuite/TestUpdateWorkflowExecutionOptions_InvalidatesPendingTask` | 1 | “flaky with autoenable” (`tests/priority_fairness_test.go @ v1.31.0`). |

## Exact fork-registry exclusions

The following 106 names are the exact exclusions under the 64 intended entrypoints,
with the reason recorded by
`tests/testcore/tokeira_conformance_skip.go` at fork SHA `5558d942`. Names sharing the
same suite and reason are grouped, but every registry entry is named. In registry
reasons, “Shape-2” denotes the harness's out-of-process deployment shape — the corpus
driving an external `tokeirad` over the public wire, per
[`functional-conformance-harness.md`](../testing/functional-conformance-harness.md).

#### `TestWorkflowTaskTestSuite`

- `TestWorkflowTaskTestSuite/TestWorkflowTaskHeartbeatingWithEmptyResult` — OUT OF SCOPE (owner decision 2026-07-03): depends on OverrideDynamicConfig(WorkflowTaskHeartbeatTimeout=5s) vs the 30m default (respondworkflowtaskcompleted/api.go:298, constants.go:2427). Per the conformance config-as-constant convention the 30m default is not operationally wrong for tokeira, so the heartbeat timeout does not earn a deployment knob merely to pass a test — same class as the MaxCallbacksPerWorkflow OverrideDynamicConfig skip. Permanent skip, not implemented (no config knob, no PendingWorkflowTask.original_scheduled_at). Tracked in .kiro/specs/transient-wft/ (Item C).

#### `TestTransientTaskSuite`

- `TestTransientTaskSuite/TestTransientWorkflowTaskHistorySize` — requires OverrideDynamicConfig(HistorySizeSuggestContinueAsNew=20KB) to drive SuggestContinueAsNew at a test-sized threshold; tokeira does not support dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)

#### `TestRawHistorySuite`

- `TestRawHistorySuite/TestGetWorkflowExecutionHistory_GetRawHistoryData` — requires suite-level dynamic config SendRawWorkflowHistory=true (default false); the raw path REPLACES parsed History with RawHistory blobs (getworkflowexecutionhistory/api.go:101 @ v1.31.0), so honoring it unconditionally would break every parsed-history consumer, and tokeira does not support dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)

#### `TestQueryWorkflowSuite`

- `TestQueryWorkflowSuite/TestQueryWorkflow_NonStickyMultiPageHistory` — requires OverrideDynamicConfig(MatchingHistoryMaxPageSize=2) to force a multi-page query-task history (the leaf asserts a non-empty NextPageToken, unreachable at any realistic default page size); tokeira does not support dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)

#### `TestWorkflowUpdateSuite`

- `TestWorkflowUpdateSuite/TestCompletedSpeculativeWorkflowTask_DeduplicateID` — calls closeShard to evict mutable state between update completions, exercising registry-rebuild dedupe; requires in-process CloseShard, impossible against the out-of-process engine (CloseShard-class skip).
- `TestWorkflowUpdateSuite/TestContinueAsNew_Suggestion` — requires OverrideDynamicConfig(WorkflowExecutionMaxTotalUpdates=3, SuggestContinueAsNewThreshold=0.5) vs v1.31.0 defaults 2000/0.9 — the asserted SuggestContinueAsNew flip on the second update is unreachable at defaults (needs 1800 updates); tokeira does not support dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)
- `TestWorkflowUpdateSuite/TestFirstNormalWorkflowTask_UpdateResurrectedAfterRegistryCleared` — calls clearUpdateRegistryAndAbortPendingUpdates -> FunctionalTestBase.CloseShard, an in-process history-service admin poke that simulates volatile update-registry loss; tokeira runs out-of-process with no shard-close surface (the nil in-process host SIGSEGVs the harness). CloseShard-class skip.
- `TestWorkflowUpdateSuite/TestScheduledSpeculativeWorkflowTask_LostUpdate` — calls loseUpdateRegistryAndAbandonPendingUpdates -> CloseShard to drop a scheduled speculative WFT's update from the volatile registry; requires in-process CloseShard, impossible against the out-of-process engine (CloseShard-class skip).
- `TestWorkflowUpdateSuite/TestScheduledSpeculativeWorkflowTask_TerminateWorkflow` — ends with env.AdminClient().DescribeMutableState asserting CompletionEventBatchId; the conformance shim has no admin client (nil-interface panic aborts the parallel suite). The public abort-on-terminate surface stays covered by TestUpdateWorkflowSdkSuite/TestTerminateWorkflowAfterUpdateAdmitted. AdminService/DescribeMutableState-class skip.
- `TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartTime` — calls clearUpdateRegistryAndAbortPendingUpdates (CloseShard) to force a stale speculative WFT with a divergent start time; requires in-process CloseShard, impossible against the out-of-process engine (CloseShard-class skip).
- `TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartedId` — calls clearUpdateRegistryAndAbortPendingUpdates (CloseShard) to force a stale speculative WFT with a divergent started id; requires in-process CloseShard, impossible against the out-of-process engine (CloseShard-class skip).
- `TestWorkflowUpdateSuite/TestStaleSpeculativeWorkflowTask_Fail_NewWorkflowTaskWith2Updates` — calls clearUpdateRegistryAndAbortPendingUpdates (CloseShard) to strand a stale speculative WFT before delivering two fresh updates; requires in-process CloseShard, impossible against the out-of-process engine (CloseShard-class skip).
- `TestWorkflowUpdateSuite/TestStartedSpeculativeWorkflowTask_LostUpdate` — calls loseUpdateRegistryAndAbandonPendingUpdates -> CloseShard to drop a started speculative WFT's update from the volatile registry; requires in-process CloseShard, impossible against the out-of-process engine (CloseShard-class skip).
- `TestWorkflowUpdateSuite/TestStartedSpeculativeWorkflowTask_TerminateWorkflow` — ends with env.AdminClient().DescribeMutableState asserting CompletionEventBatchId; the conformance shim has no admin client (nil-interface panic aborts the parallel suite). The public abort-on-terminate surface stays covered by TestUpdateWorkflowSdkSuite/TestTerminateWorkflowAfterUpdateAccepted. AdminService/DescribeMutableState-class skip.

#### `TestUpdateWithStartSuite`

- `TestUpdateWithStartSuite/TestReturnUpdateInFlightLimitError` — requires OverrideDynamicConfig(WorkflowExecutionMaxInFlightUpdates=1) vs the v1.31.0 default 10 to trip the in-flight ResourceExhausted on the second concurrent update; unreachable at the default; tokeira does not support dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)
- `TestUpdateWithStartSuite/TestReturnUpdateRateLimitError` — requires OverrideDynamicConfig(WorkflowExecutionMaxTotalUpdates=1) vs the v1.31.0 default 2000 to trip the total-updates FailedPrecondition on the second update; unreachable at the default; tokeira does not support dynamic-config injection over the wire (established OverrideDynamicConfig-class skip)
- `TestUpdateWithStartSuite/TestUpdateIsAbortedByClosingWorkflow` — closing-workflow retry-once + NotFound->Aborted conversion is a deliberate spec deferral (api-conformance-multi-operation task 6.1: primary UWS paths land first, retry in the wave-8 follow-up task 6.2); the return_retryable_error_after_retry sub-case additionally requires the in-process testhook UpdateWithStartOnClosingWorkflowRetry to force a second abort, which never fires against the out-of-process engine.

#### `TestMaxBufferedEventSuite`

- `TestMaxBufferedEventSuite/TestBufferedEventsMutableStateSizeLimit` — requires OverrideDynamicConfig(MutableStateSizeLimitError=410KB) so 100KB signals exhaust the mutable-state size at a test-sized threshold; unreachable at the v1.31.0 default 8MB, and tokeira does not support dynamic-config injection over the wire. The sibling TestMaxBufferedEventsLimit (count>100 force-close) passes because it relies on the DEFAULT MaximumBufferedEventsBatch=100, not an override (established OverrideDynamicConfig-class skip).

#### `TestWorkflowResetTestSuite`

- `TestWorkflowResetTestSuite/TestBatchResetWithOptionsUpdate` — reset-with-worker-deployment-versioning: same startVersionedPollerAndValidate + CheckTaskQueueVersionMembership matching-RPC dependency as TestResetWorkflowWithOptionsUpdate (nil MatchingClient SIGSEGVs the versioned-poller goroutine). Worker deployment versioning is Tier-4+ scope. Matching-service/versioning-class skip.
- `TestWorkflowResetTestSuite/TestResetWorkflowWithOptionsUpdate` — reset-with-worker-deployment-versioning: startVersionedPollerAndValidate drives a VERSIONED poller and asserts task-queue version membership via the matching-service RPC CheckTaskQueueVersionMembership. tokeira's conformance cluster exposes no standalone MatchingClient (the engine is a single edge process), so GetTestCluster().MatchingClient() is nil and the versioned-poller goroutine SIGSEGVs, aborting the whole binary. Worker deployment versioning is Tier-4+ scope; the reset behaviour itself is covered by the non-versioned reset leaves. Matching-service/versioning-class skip.

#### `TestEagerWorkflowTestSuite`

- `TestEagerWorkflowTestSuite/TestEagerWorkflowStart_TerminateDuplicate` — requires OverrideDynamicConfig(WorkflowIdReuseMinimalInterval=0) to allow an immediate TerminateIfRunning restart; v1.31.0 migrates that reuse policy to TERMINATE_EXISTING and applies the default 1s minimal-reuse gate, while tokeira cannot receive dynamic-config injection over the wire (tests/eager_workflow_start_test.go, common/dynamicconfig/constants.go, service/history/api/workflow_id_dedup.go @ v1.31.0; established OverrideDynamicConfig-class skip)

#### `TestSizeLimitFunctionalSuite`

- `TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByHistoryCountLimit` — requires HistoryCountLimitError=20 (and warn=10) vs the v1.31.0 default 50*1024 events so a short activity/signal history force-terminates; the corpus delivers those values only through WithDynamicConfig, which cannot reach an out-of-process tokeirad (tests/sizelimit_test.go:43-51 and common/dynamicconfig/constants.go:376-384 @ v1.31.0; established OverrideDynamicConfig-class skip)
- `TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByHistorySizeLimit` — requires HistorySizeLimitError=9000 bytes vs the v1.31.0 default 50MiB so ten 900-byte signals force-terminate; the corpus delivers that value only through WithDynamicConfig, which cannot reach an out-of-process tokeirad (tests/sizelimit_test.go:460-461 and common/dynamicconfig/constants.go:360-368 @ v1.31.0; established OverrideDynamicConfig-class skip)
- `TestSizeLimitFunctionalSuite/TestTerminateWorkflowCausedByMsSizeLimit` — requires MutableStateSizeLimitError=1100 bytes vs the v1.31.0 default 8MiB so four small pending activities force-terminate; the corpus delivers that value only through WithDynamicConfig, which cannot reach an out-of-process tokeirad (tests/sizelimit_test.go:326-327 and common/dynamicconfig/constants.go:397-405 @ v1.31.0; established OverrideDynamicConfig-class skip)
- `TestSizeLimitFunctionalSuite/TestWorkflowFailed_PayloadSizeTooLarge` — requires BlobSizeLimitError=1000 vs the v1.31.0 default 2MiB so a 1001-byte marker fails its workflow task; the corpus delivers that value only through WithDynamicConfig, which cannot reach an out-of-process tokeirad (tests/sizelimit_test.go:229-230 and common/dynamicconfig/constants.go:316-324 @ v1.31.0; established OverrideDynamicConfig-class skip)

#### `TestAdvancedVisibilitySuite`

- `TestAdvancedVisibilitySuite/TestBuildIdScavenger_DeletesUnusedBuildId`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId_NotInNamespace`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_ByBuildId_NotInTaskQueue`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_EmptyBuildIds`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_TooManyBuildIds`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_Unversioned_InNamespace`, `TestAdvancedVisibilitySuite/TestWorkerTaskReachability_Unversioned_InTaskQueue`, `TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnCompletion_VersionedWorker`, `TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnReset`, `TestAdvancedVisibilitySuite/Test_BuildIdIndexedOnRetry` — requires the suite's non-default frontend.workerVersioningDataAPIs=true V1 enabled path (and, for versioning rules, frontend.workerVersioningRuleAPIs=true); the Tokeira v1.31.0 conformance decision targets the stock-default PERMISSION_DENIED behavior and excludes deprecated V1/V2 version sets, rules, reachability, and scavenging semantics.

#### `TestNamespaceSuite`

- `TestNamespaceSuite/Test_NamespaceDelete_Protected` — requires a per-test OverrideDynamicConfig(worker.protectedNamespaces, []string{random namespace}) and asserts that policy's FailedPrecondition response (tests/namespace_delete_test.go and common/dynamicconfig/constants.go @ v1.31.0). The conformance override protocol has no string-list value kind and tokeira does not implement this deployment-policy key; the public deletion path remains covered by the sibling leaves. OverrideDynamicConfig-class skip.
- `TestNamespaceSuite/Test_NamespaceDelete_WithMissingWorkflows` — directly calls GetTestCluster().ExecutionManager().DeleteWorkflowExecution to remove mutable state while deliberately leaving visibility rows behind (tests/namespace_delete_test.go @ v1.31.0). Shape-2 has no in-process Temporal ExecutionManager; namespace deletion over the public OperatorService is covered by the sibling namespace-delete leaves. ExecutionManager-class skip.

#### `TestTaskQueueSuite`

- `TestTaskQueueSuite/TestTaskDispatchLatencyMetric_Nexus` — requires the in-process MatchingClient DescribeTaskQueuePartition and DispatchNexusTask RPCs, CaptureMetricsHandler, MatchingForwardTaskDelay test hook, and constrained WithDynamicConfig values (tests/task_queue_test.go @ v1.31.0); Shape-2 fronts the public WorkflowService only, so the leaf otherwise nil-dereferences the unavailable matching client and aborts its process.
- `TestTaskQueueSuite/TestTaskDispatchLatencyMetric_Query`, `TestTaskQueueSuite/TestTaskDispatchLatencyMetric_WorkflowAndActivity` — requires the in-process MatchingClient.DescribeTaskQueuePartition RPC, CaptureMetricsHandler, MatchingForwardTaskDelay test hook, and constrained WithDynamicConfig values (tests/task_queue_test.go @ v1.31.0); Shape-2 fronts the public WorkflowService only, so these matching-internal metric assertions cannot execute against an out-of-process tokeirad.
- `TestTaskQueueSuite/TestTaskQueueRateLimit` — tests Temporal matching implementation modes and partition forwarding by mutating MatchingUseNewMatcher, read/write partition counts, forwarder rate, and AdminMatchingNamespaceTaskqueueToPartitionDispatchRate, then comparing four mode/partition drain budgets (tests/task_queue_test.go:57-160 @ v1.31.0). Tokeira has one matching partition and no old/new matcher mode, so the test premise is outside the public Shape-2 contract; public queue stats and API/worker rate limits remain covered by active sibling leaves.

#### `TestScheduleV1`

- `TestScheduleV1/TestCreatesCHASMSentinel` — directly calls the in-process SchedulerClient to inspect CHASM sentinel reservation for a V1 scheduler workflow (tests/schedule_test.go @ v1.31.0). Shape-2 exposes the public WorkflowService only; SchedulerClient is unavailable and otherwise nil-dereferences, while CHASM migration sentinels are outside the v1.31.0 public compatibility claim.
- `TestScheduleV1/TestNextTimeCache` — white-box inspects Temporal's internal scheduler workflow history, SideEffect marker count, and serialized NextTimeCache (tests/schedule_test.go @ v1.31.0). Those are implementation details of Temporal's workflow-backed scheduler; tokeira's native schedule engine has no equivalent internal history.
- `TestScheduleV1/TestRefresh` — directly reads and signals Temporal's internal scheduler workflow (temporal-sys-scheduler:<schedule-id>) to validate its refresh implementation (tests/schedule_test.go @ v1.31.0). tokeira implements schedules as a native runtime store and engine, so no internal scheduler workflow exists; public Describe/Update/List behavior remains covered by active sibling leaves.
- `TestScheduleV1/TestSkipsCHASMSentinelWhenDisabled` — directly calls the in-process SchedulerClient and toggles EnableCHASMSchedulerSentinels to inspect absence of an internal migration sentinel (tests/schedule_test.go @ v1.31.0). Shape-2 has no SchedulerClient and tokeira's native V1 schedule engine does not model CHASM migration sentinels.

#### `TestStandaloneActivityTestSuite`

- `TestStandaloneActivityTestSuite/TestRequestCancel/RequestValidations/ReasonTooLong`, `TestStandaloneActivityTestSuite/TestTerminate/RequestValidations/ReasonTooLong` — asserts at an OverrideDynamicConfig(BlobSizeLimitError=1000) value; tokeira represents the limit as the pinned-release constant and does not accept dynamic-config injection over the wire, so the 1001-byte reason cannot trip the constant default
- `TestStandaloneActivityTestSuite/TestStart/RequestValidations/InputTooLarge` — asserts at an OverrideDynamicConfig(BlobSizeLimitError=1000) value; tokeira represents the limit as the pinned-release constant and does not accept dynamic-config injection over the wire, so the 1001-byte input cannot trip the constant default

#### `TestNexusWorkflowTestSuite`

- `TestNexusWorkflowTestSuite/TestNexusCallbackAfterCallerComplete` — DEFERRED tokeira behaviour gap (NOT a metric test — it makes no nexus_outbound_requests assertion; the prior 'metrics CaptureHandler' reason was inaccurate): asserts DescribeWorkflowExecution.Callbacks[0].State == CALLBACK_STATE_FAILED and LastAttemptFailure (nexus_workflow_test.go:2455-2458), the completion-callback Describe surface tokeira does not yet populate (UNSUPPORTED_FIELDS.md: callbacks Empty). Callback-lifecycle Describe work, tracked; remove when it lands.
- `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletion`, `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionErrors`, `TestNexusWorkflowTestSuite/TestNexusOperationAsyncFailure` — Asserts Temporal's internal callback-token wire format (CallbackTokenGenerator / NexusOperationCompletion proto) and StateMachineRef.MachineInitialVersionedTransition staleness — an internal representation tokeira deliberately does not adopt (opaque versioned token + op-fencing, nexus.rs:523). The observable contract is covered by tokeira-owned behavioural tests.
- `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAfterReset` — Asserts Temporal's internal callback-token wire format (CallbackTokenGenerator / NexusOperationCompletion proto) via sendNexusCompletionRequest — same DELIBERATE DEVIATION as TestNexusOperationAsyncCompletion (opaque versioned token + op-fencing, nexus.rs:523). The observable contract is covered by tokeira-owned behavioural tests.
- `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAuthErrors`, `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionAuthErrorsNoIdentifier` — uses the in-process auth hook Host().SetOnAuthorize and the in-process metrics CaptureHandler — neither exists against an out-of-process tokeirad
- `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionBeforeStart` — DEFERRED GAP: requires the inbound async Nexus completion-callback surface (callback URL/header/links on the dispatched StartOperation + callback invocation on handler-workflow close); tokeira ships empty callback fields and does not invoke completion callbacks yet — tracked C4b gap, not a conformance claim
- `TestNexusWorkflowTestSuite/TestNexusOperationAsyncCompletionInternalAuth` — requires OverrideDynamicConfig; tokeira does not accept dynamic-config injection over the wire (config-as-constant)
- `TestNexusWorkflowTestSuite/TestNexusOperationSyncCompletion` — the PUBLIC sync-completion behaviour PASSES (NexusOperationCompleted event, the handler Nexus-Link carried onto the event with its event_type, and the result round-trip to "result"); the test THEN white-box-inspects HSM SubStateMachinesByType deletion via the internal temporal.server.api.adminservice.v1.AdminService/DescribeMutableState (with chasm.WorkflowArchetype). tokeira does not serve that internal AdminService (its own coverage classifies AdminService as beyond-claim) and has no HSM sub-state-machine model, so the response is absent; s.NoError is non-fatal, so the next line nil-derefs and PANICS, aborting the parallel suite. Skipped so the rest of the suite runs; the assertion is outside tokeira's public v1.31.0 claim. Segment-boundary matching keeps this from skipping TestNexusOperationSyncCompletion_LargePayload.
- `TestNexusWorkflowTestSuite/TestNexusOperationSystemEndpoint` — DEFERRED GAP: exercises the __temporal_system internal endpoint, which v1.31.0 dispatches in-process via startOnHistoryService (NOT over HTTP). tokeira's outbound Nexus client covers External HTTP endpoints only; the internal system-endpoint surface is deferred and tracked separately — not a public-HTTP-Nexus conformance claim

#### `TestNexusApiTestSuiteWithLegacyErrorPaths`

- `TestNexusApiTestSuiteWithLegacyErrorPaths/TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning` — requires the suite's non-default frontend.workerVersioningDataAPIs=true V1 enabled path (and, for versioning rules, frontend.workerVersioningRuleAPIs=true); the Tokeira v1.31.0 conformance decision targets the stock-default PERMISSION_DENIED behavior and excludes deprecated V1/V2 version sets, rules, reachability, and scavenging semantics.

#### `TestNexusApiTestSuiteWithTemporalFailures`

- `TestNexusApiTestSuiteWithTemporalFailures/TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning` — requires the suite's non-default frontend.workerVersioningDataAPIs=true V1 enabled path (and, for versioning rules, frontend.workerVersioningRuleAPIs=true); the Tokeira v1.31.0 conformance decision targets the stock-default PERMISSION_DENIED behavior and excludes deprecated V1/V2 version sets, rules, reachability, and scavenging semantics.

#### `TestWorkerDeploymentSuite`

- `TestWorkerDeploymentSuite/TestForceCAN_WithOverrideState` — injects the server's internal worker-deployment entity-workflow state (deploymentspb.ForceCANDeploymentSignalArgs.OverrideState, a WorkerDeploymentLocalState) via signal — an internal-surface representation tokeira does not model, not a public API behaviour (tests/worker_deployment_test.go:211-273 @ v1.31.0)
- `TestWorkerDeploymentSuite/TestSetManagerIdentity_WithDeleteVersion` — sets matching.PollerHistoryTTL to 500ms and assumes the two internal worker-deployment entity-workflow manager updates consume that interval before the final delete; tokeira honors the override and retains the cancelled poll admission for the TTL, but its native registry completes both public manager operations before 500ms, so the final delete correctly rejects active pollers. Delaying public RPCs or dropping poller admission early would contradict v1.31.0 (tests/worker_deployment_test.go:1814-1835 and common/dynamicconfig/constants.go:478-483 @ v1.31.0)

#### `TestDeploymentVersionSuite`

- `TestDeploymentVersionSuite/TestForceCAN_WithOverrideState` — injects the server's internal deployment entity-workflow state (deploymentspb.ForceCANDeploymentSignalArgs.OverrideState, a WorkerDeploymentLocalState) via signal — an internal-surface representation tokeira does not model, not a public API behaviour

#### `TestVersioning3FunctionalSuite`

- `TestVersioning3FunctionalSuite/TestActivityTQLags_DependentActivityCompletesOnTheNewVersion`, `TestVersioning3FunctionalSuite/TestAutoUpgradeWorkflows_NoBouncingBetweenVersions`, `TestVersioning3FunctionalSuite/TestChildStartsWithParentRevision_SameTQ_TQLags`, `TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_CrossTQ_Inherit`, `TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_ParentPinnedByOverride`, `TestVersioning3FunctionalSuite/TestChildWorkflowInheritance_PinnedParent`, `TestVersioning3FunctionalSuite/TestContinueAsNewOfAutoUpgradeWorkflow_RevisionNumberMechanics`, `TestVersioning3FunctionalSuite/TestDescribeTaskQueueVersioningInfo`, `TestVersioning3FunctionalSuite/TestDoubleTransition`, `TestVersioning3FunctionalSuite/TestDoubleTransitionFromUnversioned`, `TestVersioning3FunctionalSuite/TestDoubleTransitionFromUnversioned_WithSignal`, `TestVersioning3FunctionalSuite/TestDoubleTransition_WithSignal`, `TestVersioning3FunctionalSuite/TestEagerActivity`, `TestVersioning3FunctionalSuite/TestIndependentUnversionedActivity_Pinned`, `TestVersioning3FunctionalSuite/TestIndependentUnversionedActivity_Unpinned`, `TestVersioning3FunctionalSuite/TestIndependentVersionedActivity_Pinned`, `TestVersioning3FunctionalSuite/TestIndependentVersionedActivity_Unpinned`, `TestVersioning3FunctionalSuite/TestSyncDeploymentUserDataWithRoutingConfig_Update`, `TestVersioning3FunctionalSuite/TestTransitionFromActivity_NoSticky`, `TestVersioning3FunctionalSuite/TestTransitionFromActivity_Sticky`, `TestVersioning3FunctionalSuite/TestTransitionFromWft_NoSticky`, `TestVersioning3FunctionalSuite/TestTransitionFromWft_NoSticky_ToUnversioned`, `TestVersioning3FunctionalSuite/TestTransitionFromWft_Sticky`, `TestVersioning3FunctionalSuite/TestTransitionFromWft_Sticky_ToUnversioned`, `TestVersioning3FunctionalSuite/TestUnpinnedTask_OldDeployment`, `TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_AfterCAN_NoBounceBack`, `TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_ChildNoBounceBack`, `TestVersioning3FunctionalSuite/TestWorkflowRetry_AutoUpgrade_NoBounceBack`, `TestVersioning3FunctionalSuite/TestWorkflowTQLags_DependentActivityStartsTransition` — drives the scenario by calling the internal MatchingService SyncDeploymentUserData/GetTaskQueueUserData path to install or roll back exact per-task-queue routing revisions (tests/versioning_3_test.go @ v1.31.0). That topology-control surface is not a public Temporal API and has no equivalent in Tokeira's runtime-owned Worker Deployment registry; the externally observable V3 routing paths remain active.
- `TestVersioning3FunctionalSuite/TestCheckTaskQueueVersionMembership` — directly tests internal MatchingService.CheckTaskQueueVersionMembership (tests/versioning_3_test.go @ v1.31.0), an internal history-to-matching validation RPC rather than a public WorkflowService contract.
- `TestVersioning3FunctionalSuite/TestMaxVersionsInTaskQueue` — injects versions with internal MatchingService.SyncDeploymentUserData and asserts the matching.maxDeployments internal cache limit (tests/versioning_3_test.go and common/dynamicconfig/constants.go @ v1.31.0); neither surface is public.
- `TestVersioning3FunctionalSuite/TestNexusTask_StaysOnCurrentDeployment` — dispatches the task through internal MatchingService.DispatchNexusTask and mutates Nexus queue routing through SyncDeploymentUserData (tests/versioning_3_test.go @ v1.31.0); neither internal service RPC is part of the public compatibility surface.
- `TestVersioning3FunctionalSuite/TestVersionedQueueUnload` — asserts internal Matching task-queue partition unload/reload and repeatedly reads GetTaskQueueUserData (tests/versioning_3_test.go @ v1.31.0); Tokeira has no matching partition/cache lifecycle and exposes no public equivalent.

#### `TestPrioritySuite`

- `TestPrioritySuite/TestStickyInteraction_SinglePartition` — pinned v1.31.0 corpus lifecycle defect: TestActivity_Basic leaves workflow IDs wf0..wf19 running, then this leaf reuses wf0..wf9 with the default WORKFLOW_ID_CONFLICT_POLICY_FAIL (workflow_handler.go @ v1.31.0); the same sticky-priority leaf passes in isolation and Tokeira also covers the ordering with delivery-plane properties.

#### `TestFairnessSuite`

- `TestFairnessSuite/TestMigration_FromClassic`, `TestFairnessSuite/TestMigration_FromFair`, `TestFairnessSuite/TestMigration_FromPri` — asserts Temporal v1.31.0's classic/new/fair matcher migration topology through internal draining/active physical-queue status (tests/priority_fairness_test.go @ v1.31.0). Tokeira has one runtime-owned delivery broker and no classic/new matcher persistence migration; public priority and fairness ordering tendency leaves remain active.
- `TestFairnessSuite/TestUpdateWorkflowExecutionOptions_InvalidatesPendingTask` — asserts in-process matching-client request/failure metrics, including the Go concrete error type for an obsolete matching task (tests/priority_fairness_test.go @ v1.31.0). Shape-2 observes an external tokeirad and cannot capture Temporal's internal client calls; the same leaf's public Priority update, history, Describe, poll, and stale-dispatch behavior is covered by Tokeira wire-level regression tests.

#### `TestFairnessAutoEnableSuite`

- `TestFairnessAutoEnableSuite/TestMigration_FromClassic`, `TestFairnessAutoEnableSuite/TestMigration_FromFair`, `TestFairnessAutoEnableSuite/TestMigration_FromPri` — asserts Temporal v1.31.0's classic/new/fair matcher migration topology through internal draining/active physical-queue status (tests/priority_fairness_test.go @ v1.31.0). Tokeira has one runtime-owned delivery broker and no classic/new matcher persistence migration; public priority and fairness ordering tendency leaves remain active.

## Whole-entrypoint and plan-boundary classifications

`TestCallbacksSuiteCHASM` is classified as a whole entrypoint by the pinned registry:
it runs the callbacks corpus with `EnableChasm` and `EnableCHASMCallbacks`, while CHASM
framework internals are outside Tokeira's default v1.31.0 compatibility gate. The HSM
sibling remains active and produced 14 pass outcomes. Invoking the CHASM name through
the runner produced “no tests to run” and zero outcomes; it is not counted among the 64
test-bearing entrypoints or the 106 exclusions beneath them.

`TestScheduleCHASM` is outside this release gate because the CHASM scheduler is not
released behavior at v1.31.0: it is off by default and experiment-gated —
`history.enableCHASMSchedulerSentinels` defaults to `false`, and its doc string names
staged-rollout machinery ("must be enabled and propagated in advance of
`EnableCHASMSchedulerCreation`") (`common/dynamicconfig/constants.go:2864-2868 @
v1.31.0`). The test opts in explicitly: its context factory sets that dynamic config
`true` and routes every request with the `chasm-scheduler` experiment header
(`tests/schedule_test.go:51-65 @ v1.31.0`) — Temporal's opt-in experiment mechanism.
The released, default schedule implementation at v1.31.0 is the V1 scheduler, which is
why Tier 5.30 covers `TestScheduleV1`.

### Every discovered entrypoint is accounted for

The ordered plan is drawn around the public v1.31.0 compatibility surface. As a
completeness check, discovery enumerated every top-level test entrypoint in the fork
and cross-checked the plan against it: 21 names sit outside the plan, each
deliberately, and each is classified here so that no entrypoint is silently
unmeasured. None contributes an outcome to the intended archive.

| Entrypoint | Disposition |
|---|---|
| `TestAcquireShard_OwnershipLostErrorSuite` | Outside the ordered plan; history-shard fault-injection/in-process logging test. |
| `TestAcquireShard_DeadlineExceededErrorSuite` | Outside the ordered plan; history-shard fault-injection/in-process logging test. |
| `TestAcquireShard_EventualSuccess` | Outside the ordered plan; history-shard fault-injection/in-process logging test. |
| `TestActivityAPIBatchResetClientTestSuite` | Not named by Tier 6.34's activity-control suite list. |
| `TestScheduleActivityOnPerNSTQ_Blocked` | Standalone internal per-namespace-task-queue security test; not named by the ordered plan. |
| `TestActivityApiBatchUnpauseClientTestSuite` | Not named by Tier 6.34's activity-control suite list. |
| `TestActivityAPIPauseClientTestSuite` | Not named by Tier 6.34's activity-control suite list. |
| `TestAdminRebuildMutableState_ChasmDisabled` | Admin/internal mutable-state rebuild surface; outside the public plan. |
| `TestAdminRebuildMutableState_ChasmEnabled` | Admin/internal mutable-state rebuild plus CHASM; outside the public plan. |
| `TestAdvancedVisibilitySuiteLegacy` | Legacy query-converter mode appears in historical readiness status, but is not named by the final ordered plan and was not imported into this run's totals. |
| `TestChasmTestSuiteLegacy` | CHASM framework-internal legacy converter mode; outside public scope. |
| `TestWorkflowStart_NilSearchAttributesFiltered` | Standalone regression entrypoint; not named by the ordered plan. |
| `TestWorkflowStart_AllNilSearchAttributesFiltered` | Standalone regression entrypoint; not named by the ordered plan. |
| `TestDescribeWorkflow_NilSearchAttributesNotVisible` | Standalone regression entrypoint; not named by the ordered plan. |
| `TestWorkflowStart_NilMemoFiltered` | Standalone regression entrypoint; not named by the ordered plan. |
| `TestWorkflowStart_AllNilMemoFiltered` | Standalone regression entrypoint; not named by the ordered plan. |
| `TestDescribeWorkflow_NilMemoNotVisible` | Standalone regression entrypoint; not named by the ordered plan. |
| `TestScheduleMigrationV1ToV2NoDuplicateRecentActions` | V1-to-CHASM scheduler migration implementation test; outside the public V1 schedule plan. |
| `TestTaskQueueStats_Classic_Suite` | Temporal matching-implementation mode test; not named by the public delivery-policy plan. |
| `TestTaskQueueStats_Pri_Suite` | Temporal matching-implementation mode test; not named by the public delivery-policy plan. |
| `TestTokeiraConformance_BasicWorkflowLifecycle` | Fork-owned harness self-test, not a Temporal behavioural corpus entrypoint. |

## Required disclosures

### One synchronized corpus assertion

Fork commit `5558d942` makes one change to an upstream corpus test:
`tests/http_api_test.go` adds `waitNewEvent=true` to the close-history HTTP request.
`SignalWorkflowExecution` confirms signal admission, not workflow closure. Temporal
v1.31.0 blocks `GetWorkflowExecutionHistory` only when `wait_new_event` is true
(`service/history/api/getworkflowexecutionhistory/api.go:220 @ v1.31.0`). The original
test therefore raced its worker; the fork makes the wait it relies on explicit. This
edit should be offered upstream. Tier 9.43 then completed with 11 pass outcomes, zero
skips, zero failures, and zero unfinished outcomes.

### Tier 10.45 coverage exception

The observer-effect exception is disclosed in the wire section: coverage was disabled
only for the three Tier 10.45 entrypoints because measured instrumentation overhead
would exceed their upstream deadline. Their ordinary test evidence remains complete.

### The gate found a release issue before release

The evidence process surfaced the Nexus async-token/live-queue defect before the final
capture. Engine PR #134 fixed that issue and is included in measured commit
`cecc27e6`. The final Tier 7.37 active run produced 23 passes, zero corpus-native skips,
zero failures, and zero unfinished outcomes; its 11 exact registry exclusions remain
listed above. This is the intended value of the gate: a release-affecting finding was
fixed before the measured commit was tagged, rather than explained away in the final
report.

## Archive contents and integrity

The intended archive named in the header is a scoped copy containing only the
release-gating evidence; it sits beside the append-only source archive
(`/workspaces/ev1-final-evidence-cecc27e6`), which additionally retains every
out-of-scope invocation from the run, unrewritten. The retained intended archive
contains:

- 64 complete suite logs;
- 64 raw Go event streams;
- 64 distilled outcome JSON files;
- 61 wire-coverage JSON files;
- the scoped 64-row status ledger;
- exact normal and verbose binary self-reports; and
- a 260-entry `SHA256SUMS` manifest whose own SHA-256 is recorded in the header.

Every suite log ends in `result: PASS`; every status-ledger tally matches its outcome
JSON; each coverage-enabled wire file parses and contains `GetSystemInfo`; the three
authorized coverage-off entries have no wire file. The original run workspace was
tidied, the evidence archives were retained, and the run task marker was removed.

## Release-gating verdict

**CLEAN for the intended 64-entrypoint ordered plan:** 1,261 pass outcomes, 22
corpus-native skips, 106 exact cited registry exclusions, 0 failures, and 0 unfinished
outcomes, measured against engine `cecc27e6` and fork `5558d942`. This is not presented
as proof that all 100 discovered top-level entrypoints pass; the denominator and its
plan-boundary reconciliation are stated explicitly above.
