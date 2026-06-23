# Functional conformance — suite test order

> The order in which to drive Temporal's functional Go test suites (the Tier-2 corpus) against
> `tokeirad`. Suites are enumerated from the v1.31.0 fork (`../temporal` @ tag `v1.31.0`, `tests/`).
> Scope follows [`../conformance/v1.31.0/`](../conformance/v1.31.0/README.md): in-scope = the GA /
> Public-Preview surface; deferred = under decision/research; out = internal/replication/admin/
> experimental. Status of runs lives in [`conformance.md`](./conformance.md); this is the **plan**.

## Ordering principle

Foundational suites gate everything else. A broken start / workflow-task / activity path cascades into
every later suite's *setup*, so the corpus's failures collapse into a few root causes (FINDINGS). Fix and
verify the foundation first; each tier assumes the tiers above it pass. Within a tier, order is by
dependency then cost.

## In-scope — run in this order

### Tier 1 — Core execution (everything depends on these)

| # | Suite | Exercises |
|---|-------|-----------|
| 1 | `TestWorkflowTestSuite` | Start → WFT → complete; the spine. |
| 2 | `TestWorkflowTaskTestSuite` | WFT lifecycle, completion commands, failures. |
| 3 | `TestActivityTestSuite`, `TestActivityClientTestSuite` | Schedule → start → complete/fail/cancel, heartbeats, timeouts. |
| 4 | `TestStickyTqTestSuite` | Sticky task-queue replay boundary. |
| 5 | `TestUserTimersTestSuite`, `TestWorkflowTimerTestSuite` | Timer start/fire/cancel. |
| 6 | `TestTransientTaskSuite` | Transient WFT / retry. |
| 7 | `TestWorkflowFailuresTestSuite` | Failure propagation + `Failure` object fidelity. |
| 8 | `TestGetHistoryFunctionalSuite`, `TestRawHistorySuite`, `TestRawHistoryClientSuite` | History read + pagination + long-poll. |

### Tier 2 — Messaging & control

| # | Suite | Exercises |
|---|-------|-----------|
| 9 | `TestSignalWorkflowTestSuite` | Signal admission, headers/links, WFT coalescing. |
| 10 | `TestQueryWorkflowSuite` | Query dispatch incl. quiescent-workflow path. |
| 11 | `TestCancelWorkflowSuite` | Cooperative cancel. |
| 12 | `TestWorkflowUpdateSuite`, `TestUpdateWorkflowSdkSuite`, `TestUpdateWithStartSuite` | Update lifecycle, long-poll, update-with-start. |
| 13 | `TestWorkflowBufferedEventsTestSuite`, `TestMaxBufferedEventSuite` | Signal/event buffering during WFT. |

### Tier 3 — Composition & lifecycle

| # | Suite | Exercises |
|---|-------|-----------|
| 14 | `TestChildWorkflowSuite` | Child start/resolve, parent-close policy. |
| 15 | `TestContinueAsNewTestSuite` | CAN linkage + carry-over. |
| 16 | `TestCronTestSuite`, `TestCronTestClientSuite` | Cron scheduling. |
| 17 | `TestResetWorkflowTestSuite`, `TestWorkflowResetTestSuite`, `TestWorkflowResetWithChildTestSuite` | Reset + reapply. |
| 18 | `TestEagerWorkflowTestSuite` | Eager workflow start. |
| 19 | `TestWorkflowDeleteExecutionSuite` | Post-close delete. |
| 20 | `TestSizeLimitFunctionalSuite` | Payload/history size limits. |
| 21 | `TestDescribeTestSuite` | `DescribeWorkflowExecution` pending-* population. |
| 22 | `TestWorkflowTaskReportedProblemsTestSuite` (`TestWFTFailureReportedProblemsTestSuite`) | WFT failure reporting. |

### Tier 4 — Visibility, metadata, namespaces, task queues

| # | Suite | Exercises |
|---|-------|-----------|
| 23 | `TestVisibilityTestSuite`, `TestWorkflowVisibilityTestSuite` | List/Count, basic query surface. |
| 24 | `TestAdvancedVisibilitySuite` | ORDER BY / BETWEEN / STARTS_WITH / keyword IN / null close-time. |
| 25 | `TestWorkflowMemoTestSuite` | Memo round-trip. |
| 26 | `TestWorkflowAliasSearchAttributeTestSuite` | Search-attribute aliasing. |
| 27 | `TestUserMetadataSuite` | `user_metadata` threading. |
| 28 | `TestNamespaceSuite`, `TestNamespaceInterceptorTestSuite` | Namespace CRUD + admission guard. |
| 29 | `TestTaskQueueSuite`, `TestPollerScalingFunctionalSuite` | Describe task queue, partitions, poller scaling. |

### Tier 5 — Schedules, links, callbacks

| # | Suite | Exercises |
|---|-------|-----------|
| 30 | Schedule suite (`tests/schedule_test.go`) | Create/Describe/Update/Patch/Delete/List, calendar/cron round-trip. |
| 31 | `TestLinksTestSuite` | Event links. |
| 32 | Callbacks suite (`tests/callbacks_test.go`) | Completion-callback admission + delivery. |

### Tier 6 — Standalone Activities (Public Preview; gated on by the test)

| # | Suite | Exercises |
|---|-------|-----------|
| 33 | `TestStandaloneActivityTestSuite` | First-class activity executions (C1). |
| 34 | `TestActivityApiResetClientTestSuite`, `TestActivityAPIUpdateClientTestSuite`, `TestActivityApiBatchUpdateOptionsClientTestSuite`, `TestActivityApiRulesClientTestSuite` | Activity control surfaces — **confirm overlap with deprecated aliases** (`excluded.md` §4) before relying on these. |

### Tier 7 — Nexus

| # | Suite | Exercises | Note |
|---|-------|-----------|------|
| 35 | `TestNexusEndpointsFunctionalSuite` | Endpoint CRUD (C4a). | done |
| 36 | `TestNexusAPIValidationTestSuite` | Admission validators (C5). | done |
| 37 | `TestNexusWorkflowTestSuite` | In-workflow Nexus ops, conflict policy. | done |
| 38 | Nexus operation execution (`tests/nexus_api_test.go`) | Task transport + async completion (C4b). | `nexus-async-completion` |

### Tier 8 — Worker Deployments (GA)

| # | Suite | Exercises |
|---|-------|-----------|
| 39 | `TestWorkerDeploymentSuite` | Deployment CRUD. |
| 40 | `TestDeploymentVersionSuite` | Version describe/set current/ramping. |
| 41 | `TestVersioning3FunctionalSuite` | Deployment-based (V3) routing. **Confirm V3 = GA worker-deployment versioning, not legacy.** |
| 42 | `TestWorkerRegistryTestSuite` | Worker heartbeat/registry (worker inventory). |

### Tier 9 — Edges of the public surface

| # | Suite | Exercises |
|---|-------|-----------|
| 43 | `TestHttpApiTestSuite` | HTTP/gRPC-gateway surface. |
| 44 | `TestClientMiscTestSuite`, `TestClientDataConverterTestSuite` | Client-visible misc + data-converter passthrough. |

## Deferred — in v1.31.0 but blocked on a decision/research

Do not run as conformance gates until the owning decision lands.

| Suite | Blocked on |
|-------|-----------|
| `TestVersioningFunctionalSuite` | Worker-versioning V1/V2 — TBD (`decisions.md`). |
| `TestPrioritySuite`, `TestFairnessSuite`, `TestFairnessAutoEnableSuite` | Task Queue Priority & Fairness — architecture research (`delivery.md`). |

## Out of public scope — do not run

Reason in parentheses; see [`excluded.md`](../conformance/v1.31.0/excluded.md).

- **Multi-cluster / replication** (internal): `TestNDCFuncTestSuite`, `TestStreamBasedReplicationTestSuite`,
  `TestHistoryReplicationSignalsAndUpdatesTestSuite`, `TestHistoryReplicationDLQSuite`,
  `TestActivityApiStateReplicationSuite`, `TestUserDataReplicationTestSuite`,
  `TestDeleteExecutionReplicationTestSuite`, `TestReplicationEnableTestSuite`,
  `TestNexusRequestForwardingTestSuite`, `TestNexusStateReplicationTestSuite`,
  `TestFuncClustersTestSuite`, `TestFuncClustersWithRedirectionTestSuite`,
  `TestScheduleMigrationTestSuite`, `TestCallbacksMigrationSuite`,
  `TestWorkflowTaskReportedProblemsReplicationSuite` (all `tests/xdc/`, `tests/ndc/`).
- **Admin / DLQ / internal task** (internal): `TestAddTasksSuite`,
  `TestAdminBatchRefreshWorkflowTasksTestSuite`, `TestDLQSuite`, `TestPurgeDLQTasksSuite`,
  `TestRelayTaskTestSuite`.
- **Experimental / out** : `TestPauseWorkflowExecutionSuite` (experimental), `TestArchivalSuite`
  (experimental), `TestTLSFunctionalSuite` (transport/auth — TBD), `TestChasmSuite`,
  `TestChasmTestSuite` (framework internals), `TestPrematureEosTestSuite` (transport edge).
- **Harness self-tests** (test Temporal's own harness, not the API): `TestFunctionalTestBaseSuite`,
  `TestMetricCaptureSuite`.

> The skip/expect-fail decisions are applied through the conformance fork's skip registry
> (`tests/testcore/tokeira_conformance_skip.go`), each with a cited reason — never by editing a corpus
> test body (see `docs/testing/functional-conformance-harness.md`).

---

## In-process metrics capture for the functional tests

### How Temporal does it

Temporal's metric-asserting functional tests use an **in-process capturing `metrics.Handler`**:

- `common/metrics/metricstest/capture_handler.go` — `CaptureHandler` implements the server's
  `metrics.Handler` (Counter / Gauge / Timer / Histogram + `WithTags`). When a capture is active it
  records every emission into an in-memory snapshot keyed by metric name with tags; when no capture is
  active it discards (an atomic `captureCount` gate → zero allocation off the test path).
- The test cluster installs it at server construction (`tests/testcore/test_cluster.go` →
  `temporalParams.CaptureMetricsHandler = metricstest.NewCaptureHandler()`; exposed via
  `onebox.go`). A test calls `StartCapture()` → drives RPCs → `Snapshot()` and asserts on recordings.
- `tests/testcore/metric_capture.go` wraps that with **Global** vs **Namespace-scoped** capture and
  misuse detection (a namespace-scoped metric queried globally panics, and vice-versa).

The load-bearing fact: **this captures the server's own metrics in-process** because the test cluster
runs the Temporal server *in the same process* as the Go test.

### Why that does not transfer to Tier-2 as-is

Our Tier-2 corpus runs Temporal's Go tests over **real gRPC against an out-of-process `tokeirad`**
(Rust). Temporal's Go `CaptureHandler` lives in the test process and sees **nothing** emitted by
`tokeirad`. So any corpus assertion that reads server metrics via the in-process handler cannot observe
tokeira's metrics — the snapshot is empty.

Two honest consequences:

1. **Most metric assertions are on Temporal-internal server metrics** (task latencies, shard counts,
   persistence timers). These are **not part of the public API behaviour contract** (`supported.md` is
   about RPC behaviour + history + errors, not internal metric names). They should be **skipped via the
   skip registry with a cited reason** ("asserts Temporal-internal server metric; out of public
   behaviour scope"), not made to pass artificially. `TestMetricCaptureSuite` itself is a harness
   self-test and is already out of scope.
2. Where a metric assertion is genuinely a **proxy for observable behaviour** (e.g. "a retry happened",
   "a task timed out"), prefer asserting the **observable** instead — the resulting `HistoryEvent`
   sequence or RPC response — which the corpus already has access to over the wire.

### What tokeira would need if we want true in-process metric assertions

This only applies to a future **Tier-1 in-process oracle** (the `conformance-harness` crate driving an
*embedded* engine in the same process), not the out-of-process Tier-2 path:

- A **swappable metrics seam** in `tokeira-observability`: a handler trait
  (`counter/gauge/timer/histogram` + tags) the engine emits through, with the concrete backend chosen at
  construction. (Confirm whether `tokeira-observability` already exposes such a seam or binds a concrete
  exporter — if the latter, that's the prerequisite change.)
- A **Rust `CaptureHandler` analogue**: an in-memory recording impl with `start_capture` /
  `snapshot` / `stop`, an atomic active-count gate for zero-cost-when-idle, and tag-keyed snapshots —
  a near-direct port of Temporal's shape (shape only; not code).
- Global vs namespace scoping only if our metric set has namespace-tagged metrics worth asserting.

### Recommendation

- **Tier-2 (now):** do not attempt in-process capture against `tokeirad`. Skip internal-metric
  assertions via the registry with cited reasons; re-express behaviour-proxy assertions as history/
  response assertions where the suite allows.
- **Tier-1 (later):** if/when the in-process oracle is built and we want metric-level assertions, add the
  observability seam + a capturing handler. Treat tokeira's own metric names as the contract there, not
  Temporal's — our metrics are defined in `tokeira-observability`, and matching Temporal's internal
  metric names is explicitly **not** a conformance goal.
- Either way, **metric names are not part of the v1.31.0 API claim.** Keep them out of `supported.md`;
  the conformance contract is RPC behaviour, history, and error mapping.
