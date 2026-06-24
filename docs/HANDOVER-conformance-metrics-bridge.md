# Hand-over — scrape-backed metrics bridge for the conformance corpus

**Author:** Kiro · **Date:** 2026-06-23 · **For:** Claude Code
**Repos:** this workspace (`tokeira`, Rust) **and** the pinned fork (`../temporal`, Go, branch
`tokeira/conformance-v1.31.0`).

> **Mission:** make the Tier-2 corpus's **metric-asserting** tests pass against an out-of-process
> `tokeirad`, by feeding tokeira's scraped Prometheus metrics into the corpus's
> `CaptureMetricsHandler` under Temporal's metric names — instead of skipping them. The immediate
> payoff is converting a cluster of currently-skipped `TestNexusWorkflowTestSuite` tests to **passing**,
> which is direct conformance coverage. Design rationale lives in
> [`readiness/functional-test-order.md` → In-process metrics capture](./readiness/functional-test-order.md#in-process-metrics-capture-for-the-functional-tests).

---

## 1. Why & what flips green

The corpus asserts server metrics via the **in-process** `metricstest.CaptureHandler`
(`s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()` → act → `capture.Snapshot()[name]`).
Against an out-of-process `tokeirad` that handler is nil/empty, so today these tests are **skipped** in
`tests/testcore/tokeira_conformance_skip.go`. The purely metric-gated skips this work converts to
**passing** (all in `TestNexusWorkflowTestSuite`):

- `TestNexusOperationAsyncCompletion`, `…AsyncCompletionAfterReset`, `…AsyncFailure`,
  `…AsyncCompletionErrors`
- `TestNexusSyncOperationErrorRehydration`, `TestNexusAsyncOperationErrorRehydration`
- `TestNexusOperationSyncNexusFailure`, `TestNexusCallbackAfterCallerComplete`

Stay skipped (different blockers, **do not** remove): `…AsyncCompletionAuthErrors` /
`…AuthErrorsNoIdentifier` (also need the in-process `SetOnAuthorize` hook — auth is TBD,
`decisions.md`); `…AsyncCompletionInternalAuth` (needs `OverrideDynamicConfig` — config-as-constant).
The same bridge later unblocks metric assertions in `nexus_api_test.go`, `task_queue_test.go`,
`http_api_test.go`.

## 2. Two-repo scope

- **`tokeira` (Rust):** emit any mapped metric that is a real observable but not yet emitted, under the
  `tokeira-observability` manifest discipline. Metrics are Prometheus scrape (`metrics-exporter-prometheus`
  + `/metrics`); OTLP push is deferred — **do not** use it.
- **`../temporal` fork (Go, `tokeira/conformance-v1.31.0`):** the scrape→translate→replay bridge in the
  Shape-2 cluster, and the skip-registry edits.

## 3. The design crux — concrete types + synchronous Snapshot

`Host().CaptureMetricsHandler()` returns the **concrete** `*metricstest.CaptureHandler`; `StartCapture()`
returns the **concrete** `*metricstest.Capture`; the corpus calls `capture.Snapshot()` **synchronously**
(usually before a deferred `StopCapture`). You cannot wrap these by substituting a type. To make
`Snapshot()` reflect tokeira's metrics you must populate that live `CaptureHandler` at the right moment.

**The approach (deterministic): a surgical pull-source seam in `common/metrics/metricstest`.** Add an
optional registered callback that `Capture.Snapshot()` (or the capture's drain) invokes first; in
Shape-2 the fork registers a callback that scrapes tokeira's `/metrics`, computes deltas since
`StartCapture`, translates to Temporal names/tags, and records them into the capture before it returns.
This is a small, stable edit to server-side `metricstest` (not a corpus **test body**), and it gives
exact `StartCapture → act → Snapshot` semantics. It is a **new fork-edit location** beyond the onebox
seam + ledger — note it for rebases; it is the price of determinism.

The mechanics are: scrape `/metrics` (Prometheus text format) → parse → delta vs the
`StartCapture` baseline → map name+labels → record into the `CaptureHandler` so `Snapshot()[temporalName]`
returns matching `CapturedRecording`s with the expected tags.

## 4. Bounded Temporal → tokeira mapping

Only the asserted metrics need mapping (extend as more suites are attempted). Tags to preserve:
`namespace`, `nexus_endpoint`, outcome/status.

| Temporal metric | Asserted by | tokeira source (verify in manifest) |
|-----------------|-------------|--------------------------------------|
| `nexus_requests` | nexus_workflow, nexus_api | inbound Nexus handler request counter |
| `nexus_latency` (hist) | nexus_workflow, nexus_api | inbound Nexus handler latency |
| `nexus_task_requests` | nexus_api | Nexus task dispatch counter |
| `nexus_outbound_requests` | nexus_workflow | caller-side Nexus op counter |
| `nexus_completion_requests` | nexus_workflow | async completion delivery (`nexus-async-completion`) |
| `nexus_request_preprocess_errors` | nexus_api | admission/preprocess error counter |
| `task_dispatch_latency` (hist) | task_queue | matching/dispatch latency |
| `http_service_requests` | http_api | HTTP gateway request counter |

For histograms, read the **actual assertion** (e.g. `task_queue` uses `CollectMetric(name, keep)` then
`len(recordings) > 0` filtered by tag): replay enough synthetic recordings (one per scraped `_count`
delta, carrying the tags) to satisfy presence/count assertions honestly — do not fabricate a
distribution.

## 5. Step plan

1. **Audit tokeira emission (Rust).** For each mapped metric, check `PROCESS_METRIC_MANIFEST` and the
   Nexus/matching/HTTP emission sites: does tokeira already emit an equivalent (name/labels)? Record the
   gap.
2. **Emit the missing real observables (Rust).** Add genuinely-observable metrics under the manifest
   discipline (e.g. `nexus_completion_requests` belongs on the `nexus-async-completion` delivery path),
   with comments citing what they observe. `cargo +nightly fmt` · `cargo lint` · `cargo test -p tokeira-observability` (+ the emitting crate). Commit per tokeira conventions (§7 below).
3. **Bridge (Go fork).** Implement: a `/metrics` scrape client + Prometheus-text parser; baseline/delta;
   the mapping table; replay into the `CaptureHandler`; the Snapshot-timing approach from §3. Put it in
   the Shape-2 cluster (`tests/testcore/tokeira_conformance_cluster.go` / `tokeira_harness.go`), wired so
   `Host().CaptureMetricsHandler()` is backed by it. `tokeirad` must be started with metrics enabled and
   a known observability HTTP bind address that the bridge scrapes.
4. **Skip registry (Go fork).** Remove the eight purely metric-gated entries listed in §1; **keep** the
   auth-hook and dynamic-config ones with their cited reasons.
5. **Verify.** Against a running `tokeirad` (metrics enabled), `GOTOOLCHAIN=go1.26.2 go test -tags
   test_dep -count=1 -run '^TestNexusWorkflowTestSuite$' ./tests/ -v` — the eight flip to pass. Then the
   full run-all + ledger; the report must reclassify these from skip to required-pass.

## 6. Honesty boundary (non-negotiable)

Pass a metric assertion **only** where tokeira genuinely emits the equivalent signal. If it does not but
the signal is a real observable → add it (step 2). If there is no honest equivalent, or it is a pure
Temporal server-internal with no behavioural meaning → leave it skipped with a cited reason. **Never**
fabricate a value to turn a test green — a wrong guess behind a green check bakes in non-conformance.

## 7. Conventions

**tokeira (Rust):** ground-truth to v1.31.0 (`proto/upstream/` + `../temporal` @ tag); **no kernel
additions** (metrics are observability/runtime/edge); comments explain WHY + cite source; `cargo +nightly
fmt` / `cargo lint` / `cargo test`; commit via `fsWrite` to `artifacts/cm-*.txt` then `git commit -F …`
then `rm -rf artifacts`; **never** `git add .`/`-A` (forbidden: `.claude/`,
`.kiro/specs/temporal-functional-conformance/reference/runall-results.json`); push to `main`.

**Fork (Go):** edits confined to the onebox/Shape-2 seam, the ledger/skip registry, and the
`metricstest` pull-source seam; **never a corpus test body** (the corpus stays byte-for-byte upstream
so it rebases on each compat bump). Pin `GOTOOLCHAIN=go1.26.2`. Ground-truth fixes to the tag;
config-as-constant; **raise ambiguity, do not guess**. Commit/push on the fork's `tokeira/conformance-v1.31.0`
branch per the fork's flow (separate repo).

## 8. Definition of done

- The eight metric-gated `TestNexusWorkflowTestSuite` tests pass against an out-of-process `tokeirad`
  (metrics enabled), and their skip-registry entries are removed.
- Any metrics added to tokeira are manifest-valid, documented, and cited; tokeira gates green.
- The bridge is confined to the agreed fork seams; no corpus test body changed.
- run-all + ledger reflects the flips (skip → required-pass); nothing fabricated; remaining skips
  (auth, dynamic-config) keep cited reasons.

## 9. Raise-points

- Any mapped metric with **no honest tokeira equivalent** — raise rather than fake; decide add-vs-skip.
- Auth-gated tests remain blocked on the auth TBD (`decisions.md`); out of scope here.
