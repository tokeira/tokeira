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
| 23 | `TestWorkflowVisibilityTestSuite` | List/Count, basic query surface. |
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
| 34 | `TestActivityApiResetClientTestSuite`, `TestActivityAPIUpdateClientTestSuite`, `TestActivityApiBatchUpdateOptionsClientTestSuite`, `TestActivityApiRulesClientTestSuite` | Workflow-scoped activity control and rules — confirmed in-surface for v1.31.0; clean in Tier 6.34. |

### Tier 7 — Nexus

| # | Suite | Exercises | Note |
|---|-------|-----------|------|
| 35 | `TestNexusEndpointsFunctionalSuite` | Endpoint CRUD (C4a). | done |
| 36 | `TestNexusAPIValidationTestSuite` | Admission validators (C5). | done |
| 37 | `TestNexusWorkflowTestSuite` | In-workflow Nexus ops, conflict policy. | done |
| 38 | Nexus operation execution (`tests/nexus_api_test.go`) | Task transport + async completion (C4b). | `nexus-async-completion` |

> The Nexus suites assert server metrics (`nexus_requests`, `nexus_latency`, `nexus_completion_requests`,
> `nexus_task_requests`, `nexus_outbound_requests`, `nexus_request_preprocess_errors`). **Not all are
> bridge work.** A 2026-06-24 audit of the `TestNexusWorkflowTestSuite` metric-gated tests split them:
> *Group A* (the 4 `nexus_outbound_requests` tests — driven by a real caller `StartOperation`) are flipped
> by the shim, honestly; *Group B* (the 3 async-completion tests) assert `HandlerErrorType` behaviour and
> require Temporal's internal callback-token wire format + `StateMachineRef` staleness, which tokeira
> deliberately does not adopt — they stay **skipped, reclassified deliberate-deviation**, not bridge work.
> See [In-process metrics capture](#in-process-metrics-capture-for-the-functional-tests).

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
| `TestPrioritySuite`, `TestFairnessSuite`, `TestFairnessAutoEnableSuite` | Task Queue Priority & Fairness — architecture research (`delivery.md`). |

`TestVersioningFunctionalSuite` (406 tests) is no longer deferred — it is **out of surface** by the
resolved V1/V2 decision (`docs/conformance/v1.31.0/worker-versioning.md`): the suite only passes
against stock after flipping non-default dynamic config
(`frontend.workerVersioningDataAPIs`/`frontend.workerVersioningRuleAPIs`), a surface a
default-configuration v1.31.0 server refuses and tokeira, by design, does not expose. The five V1/V2
RPCs conform as stock-default `PERMISSION_DENIED` rejections instead.

## Out of public scope — do not run

Reason in parentheses; see [`excluded.md`](../conformance/v1.31.0/excluded.md).

- **Multi-cluster / replication** (internal): `TestNDCFuncTestSuite`, `TestStreamBasedReplicationTestSuite`,
  `TestHistoryReplicationSignalsAndUpdatesTestSuite`, `TestHistoryReplicationDLQSuite`,
  `TestActivityApiStateReplicationSuite`, `TestUserDataReplicationTestSuite`,
  `TestDeleteExecutionReplicationTestSuite`, `TestReplicationEnableTestSuite`,
  `TestNexusRequestForwardingTestSuite`, `TestNexusStateReplicationTestSuite`,
  `TestFuncClustersTestSuite`, `TestFuncClustersWithRedirectionTestSuite`,
  `TestScheduleMigrationTestSuite`, `TestCallbacksMigrationSuite`,
  `TestWorkflowTaskReportedProblemsReplicationSuite`, `TestVisibilityTestSuite` (all `tests/xdc/`, `tests/ndc/`).
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

### The problem: corpus metric assertions vs an out-of-process server

Several in-scope suites assert on **server metrics** through Temporal's in-process capturing
`metrics.Handler` (`common/metrics/metricstest`): a test calls
`s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()`, drives RPCs, then asserts on
`capture.Snapshot()[name]`. This is installed at server construction in the test cluster
(`tests/testcore/test_cluster.go`), so it captures the **in-process** server's emissions.

Our Tier-2 corpus runs over **real gRPC against an out-of-process `tokeirad`** (Rust). A Go in-process
handler sees nothing tokeirad emits, so every metric-asserting test method fails — and the blast radius
is concentrated where it hurts: `TestNexusWorkflowTestSuite` (8+ capture sites), `nexus_api_test.go`
(C4b), `task_queue_test.go`, `http_api_test.go`. Skipping these forfeits real coverage in the Nexus
suites. We make them pass instead, honestly.

### The solution: a scrape-backed `CaptureMetricsHandler` shim

`tokeira-observability` exports metrics by **Prometheus scrape** (a process-global
`metrics-exporter-prometheus` recorder + `/metrics` endpoint); OTLP metric *push* is deferred Phase 2 and
not implemented. We exploit the pull model, which matches the corpus's synchronous
`StartCapture → act → Snapshot` pattern exactly.

In the conformance fork's `tokeirad` onebox override, provide a `CaptureMetricsHandler()` whose returned
capture is backed by tokeira's `/metrics`, not an in-process Go handler:

- `StartCapture()` → scrape `/metrics` once; keep as the **baseline**.
- `Snapshot()` / `CollectMetric(name, …)` → scrape `/metrics` **now**, compute the **delta** since
  baseline, and return `CapturedRecording`s keyed by the **Temporal** metric name the test expects.
- Window semantics are reproduced from two scrapes: counters via delta, "≥1" via cumulative, histograms
  (`nexus_latency`, `task_dispatch_latency`) via bucket-count delta. Tags are carried from the Prometheus
  label set.

Pull-on-snapshot is deterministic (no export interval, no batching, no force-flush) and needs **no OTLP
and no Phase-2 work**. Do **not** embed an OTel Collector / OTLP receiver for this — async push fights the
synchronous capture window and depends on unimplemented export.

### Bounded Temporal → tokeira mapping

Only the metrics the corpus actually asserts need mapping (extend as more suites are attempted):

| Temporal metric (asserted) | Used by | tokeira source |
|----------------------------|---------|----------------|
| `nexus_requests` | nexus_workflow, nexus_api | inbound Nexus handler request counter |
| `nexus_latency` (histogram) | nexus_workflow, nexus_api | inbound Nexus handler latency |
| `nexus_task_requests` | nexus_api | Nexus task dispatch counter |
| `nexus_outbound_requests` | nexus_workflow | caller-side Nexus op counter |
| `nexus_completion_requests` | nexus_workflow | async completion delivery (`nexus-async-completion`) |
| `nexus_request_preprocess_errors` | nexus_api | admission/preprocess error counter |
| `task_dispatch_latency` (histogram) | task_queue | matching/dispatch latency |
| `http_service_requests` (`metrics.HTTPServiceRequests`) | http_api | HTTP gateway request counter |

Common tags to preserve: `namespace`, `nexus_endpoint`, and outcome/status.

### Honesty boundary (non-negotiable)

A metric assertion passes **only where tokeira genuinely emits the equivalent signal**:

- tokeira already emits it (check `PROCESS_METRIC_MANIFEST` / the manifest) → map name + tags, pass.
- tokeira does not, but it is a real observable (e.g. `nexus_completion_requests` is exactly what the
  in-flight `nexus-async-completion` path produces) → **add the metric to tokeira** under the manifest
  discipline, then map it. Legitimate conformance work.
- No honest equivalent, or a pure Temporal server-internal with no behavioural meaning → **skip that
  assertion** via the skip registry with a cited reason. Never fabricate a value to turn a test green —
  "a wrong guess behind a green check bakes in non-conformance."
- **A test that also asserts internal *representation* (not just a metric) → keep skipped, reclassify.**
  Canonical case: the `TestNexusWorkflowTestSuite` async-completion tests (Group B) assert
  `HandlerErrorType` behaviour reached only by adopting Temporal's `NexusOperationCompletion` proto token
  wire format + `StateMachineRef` staleness — an internal representation tokeira deliberately does not
  adopt (opaque versioned token + op-fencing, `nexus.rs:523`). The metric is incidental; the bridge
  cannot and must not be used to "pass" them. Deliberate-deviation, covered by tokeira-owned behavioural
  tests.

The mapping therefore grows in lockstep with real Nexus implementation; green means earned. `TestMetricCaptureSuite`
and `TestFunctionalTestBaseSuite` remain harness self-tests and stay out of scope regardless.

### Why not the Go OTel SDK `ManualReader`, and why not OTLP

`ManualReader` only observes a Go `MeterProvider` in the same process; tokeirad is a separate Rust
process, so the harness must observe tokeira's emissions, not Go's. The scrape shim is that observer. An
OTLP collector would also observe out-of-process emissions, but it is async, depends on tokeira's deferred
OTLP push, and bloats the fork — so it is the wrong tool for the synchronous capture contract.
