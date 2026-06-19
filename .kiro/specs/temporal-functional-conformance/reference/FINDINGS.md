# Tier-2 Functional Conformance — Findings & Codex Hand-off

**Run date:** 2026-06-09
**Corpus:** Temporal functional `tests/` @ `v1.31.0` (fork `tokeira/temporal`, branch
`tokeira/conformance-v1.31.0`)
**Target:** `tokeirad` (in-memory storage), `TEMPORAL_SERVER_COMPAT = 1.31.0`
**Harness:** `tests/tokeira_conformance_runall` (per-entrypoint process isolation) →
`tests/tokeira_conformance_ledger` (per-test outcome distiller).

This document is the complete, data-backed catalogue of what failed and **why**, ground-truthed
to v1.31.0 source, structured so Codex (or any implementer) can pick up a cluster and act without
re-deriving the investigation. Every claim here is from the run's `-json` stream and the cited
v1.31.0 source.

---

## Status ledger — canonical (Kiro is the single contributor)

> This is **the** progress surface for functional conformance. Update it on every change.
> Single contributor: **Kiro** (decided 2026-06-17) — no concurrent writers until the user calls it.
> Detail lives in *Remaining work* and *Detailed findings* below; `api-conformance-tracker/tracker.md`
> is a static RPC index, **not** a progress tracker.
>
> Glyphs: ✅ done · 🟡 in progress · ⬜ not started · ⏸ deferred · ⛔ out of scope.

| Cluster | Area | Status | Measured | Next action |
|---|---|:--:|:--:|---|
| C1 | Standalone / first-class activity RPCs | 🟡 | 42/129 | Stages 1/3/4 landed (reuse-conflict, wire-compat token, describe info+outcome fidelity, count-by-id); remeasure pending. Remaining cross-spec blockers: retry re-dispatch / heartbeat / timeouts (~20, `runtime-activity-*`) |
| C3 | Visibility list/query + search attributes | 🟡 | — | run `TestAdvancedVisibilitySuite`/`…Legacy` (query surface) |
| C2 | Worker deployment / versioning | 🟡 | 19/19 (1 suite) | triage `TestVersioningFunctionalSuite` (+3 suites) |
| C4a | Nexus endpoint **admin CRUD** RPCs | ⬜ | 0/~17 | `api-conformance-nexus-admin` (spec ground-truthed 2026-06-18); clears `TestNexusEndpointsFunctionalSuite` (15) + `TestNexusAPIValidationTestSuite` (2). Prereq: runtime `NexusEndpointRegistry` is static — make it live/store-backed (task 4.2) |
| C4b | Nexus **operation execution** / task transport | ⬜ | 0/~82 | `edge-nexus-task-transport` / `kernel-nexus-operations` / `runtime-nexus-dispatch`; `TestNexusApiTestSuiteWith{TemporalFailures,LegacyErrorPaths}` (80) + `TestNexusWorkflowTestSuite` (2) |
| C9 | `unfinished` panic siblings | ⬜ | 0/267 | fix entrypoint panic; siblings re-resolve |
| C5a | Completion-callback admission validation | ✅ | done | — |
| C5b | Other admission validators (links, …) | ✅ | done | — |
| C6 | Over-rejection (cron, nil/empty SA + memo) | ✅ | done | — |
| C7 | Lifecycle / describe `NotFound` | ⏸ | — | re-triage after C1–C4 |
| C8 | Internal-surface / admin-service tests | ⛔ | — | out of public scope |

**Now:** C1 — fresh-server full-suite measure (2026-06-18, re-confirmed on a clean restart):
**42/129 leaf pass**. Landed and **committed (`59546975`, pushed to `main`)**: Stage 4.2 describe
long-poll deadline (+ unblocked the suite panic), request-field validation on cancel/terminate
(request_id/identity length, +4), and respond-token validation (StaleToken + MismatchedTokenNamespace
on Complete/Fail, +4). Since then (committed): 1.3 reuse/conflict enforcement (`ea0a7051`,
`5b5a7a39`, `2362d7db` — incl. the typed `ActivityExecutionAlreadyStarted`) and 3.1 respond-token
validation extended to the **canceled** path, and the **standalone-activity task token is now
wire-compatible** — a marshaled `tokenspb.Task` carrying a `ChasmComponentRef` (`component_ref`),
hand-defined to the stable field numbers. This clears `MismatchedTokenComponentRef` (the corpus
round-trips the issued token through Temporal's `tasktoken.Serializer`) and adds the second
`token does not match namespace` check; `component_ref` presence is also the standalone-vs-workflow
routing discriminator. **Stage 4 now landed too:** 4.1 describe fidelity — info echoes header /
retry_policy / priority / search_attributes / user_metadata + close_time, and the outcome round-trips
the structured `Failure` (Failed) / `TerminatedFailureInfo` / `CanceledFailureInfo`; 4.2 long-poll
caller-deadline (`59546975`); 4.3 count-by-`ActivityId` (business-id alias). **Next: remeasure the
full suite** (expect a jump from 42), then triage residuals. Blocked cross-spec (~20): retry
re-dispatch (`StaleAttemptToken`, retry/timeout suites) and heartbeat → `runtime-activity-pump` /
`runtime-activity-timeouts`. NB: restart `tokeirad` between full measures —
in-memory state accumulates and reused activity IDs perturb counts.

---

## Conformance run recipe (exact — don't rediscover this)

The corpus lives in the sibling `../temporal` checkout on branch `tokeira/conformance-v1.31.0`.
The suite's in-process `OverrideDynamicConfig(activity.Enabled)` does **not** reach an out-of-process
`tokeirad`, so feature gates MUST be set in the server's **static config**.

```bash
# 1. Build the server (from tokeira repo root)
cargo build -p tokeirad

# 2. One-time: generate a config, pin the address, enable standalone activities
./target/debug/tokeirad --dump-config > /tmp/tokeira-c1.toml
#   then set, in /tmp/tokeira-c1.toml:
#     [infrastructure.network]  grpc_addr = "127.0.0.1:7233"
#     [policy.compatibility]    enable_standalone_activities = true
# (default storage backend is in-memory — correct for the corpus)

# 3. Start tokeirad (background; restart it after every rebuild to load new code)
./target/debug/tokeirad --config /tmp/tokeira-c1.toml
#   expect: "storage backend: in-memory" + "gRPC server listening on 127.0.0.1:7233"

# 4. Run a targeted suite/sub-test from ../temporal (seconds–minutes after first Go compile)
cd ../temporal
TOKEIRA_CONFORMANCE_FRONTEND_ADDR=127.0.0.1:7233 \
  go test -tags test_dep -run '^TestStandaloneActivityTestSuite$' ./tests/ -v
#   sub-test: -run '^TestStandaloneActivityTestSuite$/TestDelete'
#   full corpus (~2h): tests/tokeira_conformance_runall + tests/tokeira_conformance_ledger
```

Loop: edit → `cargo build -p tokeirad` → restart the server (step 3) → re-run the suite (step 4) →
update the Status ledger. Go toolchain: `go1.22.4` confirmed in this environment.

> **The SA gate is C1-only.** `enable_standalone_activities` matters only for C1 (standalone
> activities). For any other cluster, drop it: a config with just `grpc_addr` pinned is enough, and
> step 4 runs that cluster's suite (e.g. C4a: `-run '^TestNexusEndpointsFunctionalSuite$'` /
> `^TestNexusAPIValidationTestSuite$`). The harness wiring (`TOKEIRA_CONFORMANCE_FRONTEND_ADDR` +
> suite name) is identical across clusters.

---

## Remaining work (snapshot — updated 2026-06-17)

Concise status of each root-cause cluster. Full investigation detail (signatures, ground truth,
fix loci) is in **Detailed findings** below. Legend: ✅ done · 🟡 partial · ⬜ outstanding ·
⏸ deferred · ⛔ out-of-public-scope.

| Cluster | Area | Status | What remains / next action | Owner |
|---|---|:--:|---|---|
| **C6** | Over-rejection (cron parse; nil/empty SA + memo filtering) | ✅ | Done. C6b nil SA + memo filtering implemented (`is_temporal_nil_payload`, with tests). C6a: `@every Ns` already handled; fixed cron error fidelity (now returns `InvalidArgument "invalid CronSchedule."` verbatim, not "missing required field") and added the `@midnight` descriptor. Tests added; fmt/clippy clean. Translation-layer verified; full corpus re-run still pending. | edge translate |
| **C5a** | Completion-callback admission validation | ✅ | Done. `validate_completion_callbacks` wired into the Start path (v1.31.0 has a single caller, so no SignalWithStart parity gap); rules 1/2a/2b/2c + header-key lowercasing implemented with verbatim v1.31.0 messages (verified vs `workflow_handler.go:6299` + `components/callbacks/config.go:71 @ v1.31.0`); limits are source-cited constants (per `DECISION-callback-validation.md`). Tested (incl. invalid-scheme, missing-host, url-length, header-size, count-cap). Override-dependent sub-cases are harness-limited; `allowedAddresses` (2 sub-cases) deferred as a deployment-policy decision. | edge |
| **C5b** | Other admission validators (links, versioning info, start fields) | ✅ | **Links complete on all 5 v1.31.0 paths**: Start (combined+deduped request/callback set) plus Signal, RequestCancel, Terminate, SignalWithStart (request links only) — `validateLinks` mirror with verbatim messages and source-cited constants. Versioning-info: the override path is already validated; the legacy `useVersioning`/build-id path is a C2 deliberate-deviation (rule-based replacement). No further concrete "other start field" gap is enumerated — any residual is to be driven by a corpus re-run. | edge |
| **C3** | Visibility list/query + search attributes | 🟡 | List RPCs + projection wired ✅; system/predefined SA seeding + 7 `SystemField` variants (`f5d959d`) ✅; **custom-SA upsert now reaches the visibility-store registry** (`VisibilityRegistryOperatorApi`, `apps/tokeirad/src/lib.rs:128–142`) ✅; visibility plane generalized to versioned snapshots shared by workflows + activities (`chasm-foundation`) ✅. Remaining: run `TestAdvancedVisibilitySuite`/`…Legacy` for residual query-surface gaps (ORDER BY / BETWEEN / STARTS_WITH / keyword `IN` / null close-time). See `DIRECTION-c3-visibility.md`. | projection/edge |
| **C1** | Standalone / first-class activity RPCs | 🟡 | **Substrate + edge RPCs landed via the `chasm-foundation` spec (complete).** All eight `*ActivityExecution` RPCs — start / poll / describe / list / count / request_cancel / terminate / delete — delegate to the `ActivityBridge` (`grpc/workflow_service.rs:2103–2294`; wired at `apps/tokeirad/src/lib.rs:899–919`), and SA executions project to the shared snapshot visibility plane (discoverable via `List`/`CountActivityExecutions`). Gated by `enable_standalone_activities` (**off by default** — matches v1.31.0's gated-off baseline; on = deliberate deviation ahead of baseline, `tokeira-config/src/lib.rs:234`). **Measured 2026-06-17** (`TestStandaloneActivityTestSuite` vs a statically-SA-enabled `tokeirad` — confirmed the suite's in-process `OverrideDynamicConfig(activity.Enabled)` is ignored out-of-process, as predicted): **1 pass / 31 fail**. Bridge serves the RPCs and the happy path works, but conformance is early. Root causes: (1) `DescribeActivityExecution` info under-populated (`last_worker_identity`, `run_state`, `last_started_time`, …); (2) **no SA admission validation** — bad requests (empty/too-long activity_id, invalid run_id, stale/mismatched tokens) hang to `DeadlineExceeded` instead of `InvalidArgument`/`NotFound`; (3) describe proto fidelity (retry-policy/payload `@invalid`); (4) describe long-poll deadline; (5) `CountActivityExecutions` by activity_id. This is `activity-executions-first-class` work; fixing in order, starting with (2). | chasm-foundation ✅ → activity-executions-first-class |
| **C4a** | Nexus endpoint **admin CRUD** RPCs | ⬜ | Endpoint registry CRUD/list only. Spec `api-conformance-nexus-admin` **ground-truthed against v1.31.0 (2026-06-18)**: corrected stale-version code (`FAILED_PRECONDITION`, not `ABORTED`), removed the invented `UNIMPLEMENTED`-on-unsupported-field path, added the full `validateUpsertSpec` rules + verbatim messages (`service/frontend/nexus_endpoint_client.go @ v1.31.0`), the matching-side error codes (`AlreadyExists`/`NotFound`/`FailedPrecondition`, `service/matching/nexus_endpoint_client.go @ v1.31.0`), the six config knobs with defaults, and the frontend/matching-collapse deviation note. Clears `TestNexusEndpointsFunctionalSuite` (15) + `TestNexusAPIValidationTestSuite` (2) ≈ 17. | api-conformance-nexus-admin |
| **C4b** | Nexus **operation execution** / task transport | ⬜ | `PollNexusTaskQueue`, operation lifecycle, Nexus-in-workflow. **Not** in `api-conformance-nexus-admin` (explicit non-goal). `TestNexusApiTestSuiteWith{TemporalFailures,LegacyErrorPaths}` (80) + `TestNexusWorkflowTestSuite` (2). | edge-nexus-task-transport / kernel-nexus-operations / runtime-nexus-dispatch |
| **C2** | Worker deployment / versioning | 🟡 | `TestWorkerDeploymentSuite` (19) done (`2d0f609` +follow-ups; 2 skipped — no dynamic-config injection). Outstanding & untriaged: `TestVersioningFunctionalSuite` (406), `TestDeploymentVersionSuite` (68), `TestVersioning3FunctionalSuite` (13), `TestWorkerRegistryTestSuite` (7). Legacy v0.x version-sets = deliberate-deviation (won't implement). | edge/runtime registry |
| **C7** | Lifecycle / describe `NotFound` | ⏸ | Re-triage after C1–C6 — many are cascades that clear for free. | — |
| **C8** | Internal-surface / admin-service tests | ⛔ | Out-of-public-scope by construction; classify via the scope report, no implementation. | ledger |
| **C9** | `unfinished` panic siblings (267) | ⬜ | Fix the entrypoint panic (nil-deref on the shimmed `testBase`/persistence); siblings then resolve to real pass/fail. | harness/edge |

**Order (single source — mirrors the Status-ledger `Next action` column):**
C1 (in flight) → C3 (cheap re-run) → C2 (triage) → C4a (spec-ready, ~17) → C9 (panic fix —
reclassifies 267 `unfinished` to real pass/fail) → C7 (re-triage) → C4b (far; multi-spec) →
C8 (classify). Done: C5a, C5b, C6.

Run one suite at a time per the **Conformance run recipe** above (no full `runall` — ~2h; targeted
suites are seconds-to-minutes). Measure → fix root causes → re-run → update the ledger.

All fixes remain bound by the **Implementer mandate** and v1.31.0 ground-truth rules in the Detailed
findings below.

---

# Detailed findings (original investigation — full catalogue)

## Implementer mandate (non-negotiable)

These constraints bind every fix derived from this document. They are not style preferences; a fix
that violates one is wrong even if it turns a test green.

1. **Every fix references Temporal v1.31.0 behaviour.** The contract is whatever the targeted
   release does, verified against ground truth (AGENTS.md §8): `proto/upstream/` for wire shape, the
   tagged server source for behaviour (`git -C ../temporal show v1.31.0:<path>`). Cite the verifying
   source (proto path or server source path + tag) in the spec/PR/code comment for any non-obvious
   behaviour decision. Do **not** infer behaviour from SDK docs, blog posts, generated artifacts
   under `target/`, or memory. A fix whose justification is "it makes the test pass" rather than
   "v1.31.0 does X, verified at `<path>@v1.31.0`" is not acceptable.

2. **No kernel additions.** `tokeira-kernel` stays a pure deterministic state machine — no I/O,
   async, storage, metrics, or network, and no new surface added to satisfy these findings. Every
   fix in this catalogue is an **edge** concern (admission validation, translation, RPC wiring) or a
   runtime/storage concern. If a fix appears to need a kernel change, that is a signal to **stop and
   raise it** (see rule 4), not to extend the kernel. Reading the kernel is fine; adding to it is
   not.

3. **Configuration must be raised, never silently invented.** Where v1.31.0 behaviour is governed by
   a config/dynamic-config value (e.g. `CallbackURLMaxLength`, `MaxCallbacksPerWorkflow`,
   `callbacks.AllowedAddresses`), Tokeira needs an equivalent knob with a v1.31.0-faithful default.
   Do **not** hardcode a limit inline. Surface the required configuration surface — its name,
   default, and whether it must be overridable (the functional tests drive several via
   `OverrideDynamicConfig`, so override support is part of the contract) — to the user **before**
   implementing, so the config model is decided deliberately rather than accreting per-fix.

4. **Raise ambiguity; do not guess.** If the v1.31.0 behaviour is unclear, the ground truth is
   contradictory, the cluster's root cause cannot be confirmed from source, or a fix would force a
   choice with architectural/compat implications, **stop and raise it** rather than picking a
   plausible interpretation. A wrong guess that passes the test is worse than a raised question: it
   bakes a non-conformant behaviour behind a green check. Surface the specific uncertainty and what
   would resolve it.

---

## Run summary

| Metric | Value |
|--------|-------|
| Entrypoints executed | 100 / 100 (full corpus, no exclusions) |
| Top-level entrypoints fail / unfinished / pass / skip | 78 / 9 / 2 / 1 |
| Per-test outcomes captured | 1501 |
| — fail | 1194 |
| — unfinished (panic-crash siblings) | 267 |
| — pass | 19 |
| — skip | 21 |

**Read this correctly:** the failure count is *expected and is the point*. Tier-2's posture is
run-all, classify-in-report. The value below is the **clustering**: 1194 failures collapse into a
small number of root causes, most of them a single missing admission-time validation or a single
unimplemented RPC that a whole suite depends on in setup.

---

## Root-cause clusters (ranked by blast radius)

Each cluster lists: the signature, the v1.31.0 ground truth, the tokeira gap, the suggested fix
locus, and the proposed ledger category. Categories use the Tier-2 taxonomy:
`real-gap` (we should pass, don't yet), `deliberate-deviation` (we intentionally differ),
`out-of-public-scope` (depends on an internal surface we don't front).

### C1 — Standalone / first-class activity RPCs unimplemented (real-gap)

> **Update 2026-06-17 — IMPLEMENTED (substrate + bridged RPCs) via the `chasm-foundation` spec
> (complete).** All eight `*ActivityExecution` RPCs (start / poll / describe / list / count /
> request_cancel / terminate / delete) now delegate to the `ActivityBridge`
> (`grpc/workflow_service.rs:2103–2294`, wired at `apps/tokeirad/src/lib.rs:899–919`), gated by
> `enable_standalone_activities` — **off by default** (v1.31.0 gates the chasm/activity frontend off
> and answers `UNIMPLEMENTED`, `chasm/lib/activity/frontend.go @ v1.31.0`; on is a deliberate deviation
> ahead of baseline, `tokeira-config/src/lib.rs:234`). SA executions project to the shared
> versioned-snapshot visibility plane (`ProjectionVisibilitySink`) and are discoverable via
> `List`/`CountActivityExecutions`.
>
> **Measured 2026-06-17 — `TestStandaloneActivityTestSuite`: 1 pass / 31 fail.** Run against a
> `tokeirad` with `enable_standalone_activities = true` set statically in config, with the corpus
> pointed at it via `TOKEIRA_CONFORMANCE_FRONTEND_ADDR` — confirming the predicted caveat that the
> suite's `s.OverrideDynamicConfig(activity.Enabled, true)` is ignored by an out-of-process server. The
> bridge serves the RPCs and the happy-path lifecycle works, but the suite exercises far more rigour.
> Root causes (fix order):
> 1. **No SA admission validation** — empty/too-long `activity_id`, invalid `run_id`, and stale /
>    mismatched / wrong-namespace task tokens are not rejected; the RPC blocks to the caller's deadline
>    and surfaces as `DeadlineExceeded` rather than `InvalidArgument` / `NotFound`
>    (`standalone_activity_test.go:2426` et al.). Edge concern; mirror `chasm/lib/activity @ v1.31.0`.
> 2. **`DescribeActivityExecution` info under-populated** — `last_worker_identity` is empty, and the
>    suite also checks `run_state`, `last_started_time`, `attempt`, `last_failure`, `heartbeat_details`
>    (`standalone_activity_test.go:4831`). Needs the activity component to track worker/run state.
> 3. **Describe response proto fidelity** — retry-policy / payload encoding differs (`@invalid`).
> 4. **Describe long-poll** does not honour the caller deadline
>    (`TestDescribeActivityExecution_DeadlineExceeded`).
> 5. **`CountActivityExecutions` by `activity_id`** returns the wrong count.
>
> Deeper execution-engine semantics remain tracked in `activity-executions-first-class`. Original
> investigation below kept for provenance.

- **Signature:** `Unimplemented desc = start_activity_execution is not implemented; tracked in spec
  activity-executions-first-class` (also `poll_/request_cancel_/terminate_/delete_activity_execution`).
  ~82 direct hits + cascades.
- **Blast radius:** `TestStandaloneActivityTestSuite` (130 sub-tests, total wipe) and parts of the
  activity-API suites.
- **Ground truth:** these are the v1.62 standalone-activity RPCs; behaviour per the
  `activity-executions-first-class` spec already tracked in Tokeira.
- **Gap:** RPCs are stubbed at the edge.
- **Fix locus:** `activity-executions-first-class` spec (already exists). This is the single
  highest-count cluster.
- **Category:** real-gap (tracked) — link the existing spec.

### C2 — Worker Deployment / Versioning registry not configured (real-gap or deliberate-deviation)

- **Signature:** `FailedPrecondition desc = worker deployment registry is not configured for this
  runtime` (~74), plus `Unimplemented desc = Legacy worker versioning API (v0.x version sets) is not
  supported. Use UpdateWorkerVersioningRules` (~21).
- **Blast radius:** `TestVersioningFunctionalSuite` (406 sub-tests — the single largest suite),
  `TestDeploymentVersionSuite` (68), `TestWorkerDeploymentSuite` (19), `TestVersioning3FunctionalSuite`
  (13), `TestWorkerRegistryTestSuite` (7).
- **Ground truth:** v1.31.0 serves worker-deployment + rule-based versioning APIs.
- **Gap:** the deployment registry is not wired into the runtime used by the conformance binary; the
  legacy version-set API is deliberately not implemented (rule-based is the replacement).
- **Fix locus:** decide per-API — deployment registry config is a real-gap (wire it up); legacy
  version-sets is a candidate deliberate-deviation (cite the rule-based replacement).
- **Category:** split — registry = real-gap; legacy version-sets = deliberate-deviation.

#### C2 progress update — 2026-06-10 (TestWorkerDeploymentSuite only)

Scope note: the work below addresses **`TestWorkerDeploymentSuite` (19 sub-tests)** only. The
larger C2 suites — `TestVersioningFunctionalSuite` (406), `TestDeploymentVersionSuite` (68),
`TestVersioning3FunctionalSuite` (13), `TestWorkerRegistryTestSuite` (7) — are **not yet worked**
and remain outstanding.

The headline "registry not configured" signature was already resolved before this batch: the
runtime now attaches the `WorkerDeploymentRepository` by default (`apps/tokeirad/src/lib.rs`
`with_worker_deployment_repository` + edge `with_worker_deployment_runtime`), so the registry serves
unconditionally — correct, since `EnableDeploymentVersions` defaults `true` in v1.31.0. A fresh
`TestWorkerDeploymentSuite` run then split into the sub-clusters below.

**Done (committed `2d0f609`, `617899094`, plus a follow-up batch):**

- **C2.2 deployment-name validation** — `validate_deployment_name` in
  `tokeira-edge/grpc/translate.rs`: empty / length(≤480) / `.` / `:` / `__` in exact v1.31.0 order
  with verbatim messages (`workflow_handler.go:4154`, `worker_versioning.go:555 @ v1.31.0`).
- **C2.3 conflict tokens** — `AlreadyExists` now names the deployment (`util.go:105`); conflict
  token is monotonic across delete→recreate (in-memory per-name high-water-mark + DSQL companion
  table, migration `V048`), mirroring v1.31.0's strictly-increasing timestamp token
  (`workflow.go:248,502`).
- **C2.1 poller auto-create** — versioned `PollWorkflowTaskQueue` lazily registers deployment +
  version via `DeploymentRegistry::register_polled_deployment` with the `_auto_create_` request-id
  prefix; explicit-create collision reports the `(auto-created from worker polls)` provenance
  (`client.go:1228/1230`). `poll_request_to_edge` now reads the v1.62 `deployment_options` field.
- **Describe rendering** — `RoutingConfig.current_version` renders the `__unversioned__` sentinel
  when no current version is set (`client.go:735`, `workflow.go:245`); version summaries are sorted
  by create-time **descending** (`client.go:1784`).
- **Entity-workflow drainage signal** — `SignalWorkflowExecution` to
  `temporal-sys-worker-deployment-version:<name>.<build>` with `sync-drainage-status` is shimmed at
  the edge onto registry drainage state (`DeploymentRegistry::apply_version_drainage`), mirroring
  `version_workflow.go:119 @ v1.31.0`. Tokeira backs the entity-workflow *surface* with the registry
  rather than per-run workflows; this is a translation shim, not a kernel/workflow-model change.
- **Per-task-queue versioning info** — `DescribeTaskQueue.versioning_info` is now populated from the
  registry (`DeploymentRegistry::task_queue_versioning`): a queue's current/ramping version is its
  deployment's current/ramping version iff that version actually polled the queue.
- **Harness fidelity (fork)** — the conformance `FrontendClient` now installs a
  `serviceerror.FromStatus(status.Convert(err))` unary interceptor matching Temporal's real client
  (`common/rpc/grpc.go @ v1.31.0`), so the corpus's `ErrorAs(&*serviceerror.X)` assertions resolve
  on the server's code/message instead of failing on raw `*status.Error`.

**Skipped (deliberate — no dynamic-config injection):**

- `TestDeploymentVersionLimits` and `TestDeleteVersion_ServerDeleteMaxVersionsReached` both
  `OverrideDynamicConfig(MatchingMaxVersionsInDeployment, 1)`. Tokeira's default already matches
  v1.31.0 (100) and the harness cannot deliver dynamic-config overrides to an out-of-process
  `tokeirad`. Skipped by name in the fork's `tests/testcore/tokeira_conformance_skip.go`
  (conformance-mode-only; corpus unmodified). Tokeira does not support dynamic-config injection at
  this time — revisit if/when that capability is added.

**Outstanding:**

- The other C2 suites (`TestVersioningFunctionalSuite`, `TestDeploymentVersionSuite`,
  `TestVersioning3FunctionalSuite`, `TestWorkerRegistryTestSuite`) — not yet triaged against the
  registry-backed implementation.
- Legacy worker-versioning v0.x version-set API remains a deliberate-deviation (rule-based
  replacement); not implemented.
- Post-conformance cleanup (tracked separately): the entity-workflow surface and per-task-queue
  versioning are currently edge shims over the registry; revisit for a first-class model.


### C3 — Visibility list/query RPCs (real-gap) — REFRESHED 2026-06-11

**The original C3 entry below is stale.** It was written off an early run, before the
visibility plane was built and wired. Re-verified against the current tree (2026-06-11):

**What is actually done now:**

- **Legacy list RPCs are implemented, not stubbed.** `list_open`, `list_closed`, `scan`,
  `list_archived`, `list_workflow_executions`, `count_workflow_executions` translate filters
  and delegate to a real `VisibilityApi` (`tokeira-edge/src/grpc/workflow_service.rs`,
  `workflow_service.rs:2738+`; legacy filter→query translation in `grpc/translate.rs`).
- **Projection-backed visibility is wired into the running `tokeirad`** even in in-memory
  mode: `apps/tokeirad/src/lib.rs:453+` builds `InMemoryVisibilityStore`, runs a per-partition
  `ProjectionWorker` + `VisibilitySink`, and serves reads via `VisibilityQueryService`. The
  FINDINGS claim "the conformance run does not exercise the projection store" is no longer true.
- **`GetSearchAttributes` is implemented** (`workflow_service.rs:3087`) over the operator catalog.
- **The whole DSQL query surface is implemented** (the `projection-visibility` spec is COMPLETE:
  migrations `V029`–`V042`, filter-to-SQL compiler, rollups, registry; `tokeira-projection`
  lib tests green).

**What actually remains (the live gap):**

1. **No system/predefined search attributes are seeded.** `InMemoryOperatorApi::new` starts with
   an empty attrs map (`tokeira-edge/src/operator_service.rs:55`) and the visibility store's
   attribute registry starts empty, so `GetSearchAttributes` returns nothing and `compile_filter`
   rejects standard attributes — this is the live root cause of
   `unknown search attribute: TemporalExternalPayloadCount`. Ground truth: the fixed `system`
   (16 fields) + `predefined` internal sets in
   `common/searchattribute/sadefs/constants.go @ v1.31.0` must be registered per namespace at
   startup.
2. **`SystemField` is missing 7 of the 16 system fields** (`tokeira-projection/src/types.rs:130`):
   `ExecutionTime, ExecutionDuration, HistorySizeBytes, ParentWorkflowID, ParentRunID,
   RootWorkflowID, RootRunID`.
3. **Custom search-attribute registration** must flow from `OperatorService` upsert into the
   visibility store registry (confirm the upsert reaches the store, not just the catalog).
4. **Advanced-visibility query-surface coverage** (`ORDER BY`, `BETWEEN`, `STARTS_WITH`,
   keyword-list `IN`, null close-time semantics) needs a targeted check against
   `TestAdvancedVisibilitySuite`/`…Legacy` once #1–#2 land.

**Direction:** seed the v1.31.0 `system` + `predefined` SA sets at startup (operator catalog +
visibility registry) and add the 7 missing `SystemField` variants, then run the C3 suites to
expose any residual query-surface gaps. Detailed Codex playbook:
`reference/DIRECTION-c3-visibility.md`.

> **Update 2026-06-12:** items **#1 (seed system/predefined SAs)** and **#2 (7 missing `SystemField`
> variants)** landed in commit `f5d959d` (new `tokeira-projection/src/system_attrs.rs`).
>
> **Update 2026-06-17:** **#3 (custom-SA upsert reaches the visibility store) is done** — the
> `VisibilityRegistryOperatorApi` wrapper calls `store.register_attr(...)` *before* mutating the
> user-facing catalog (`apps/tokeirad/src/lib.rs:128–142`), so an attribute can't be acknowledged
> without being searchable. The visibility plane was also generalized to versioned snapshots shared by
> workflows + activities (`chasm-foundation`). Only **#4** (run `TestAdvancedVisibilitySuite`/`…Legacy`
> for residual query-surface gaps) remains.

- **Category:** real-gap (SA seeding + system-field coverage). Advanced visibility is in scope —
  it is exercised by the running server; only the seeding/coverage gaps remain.

<details><summary>Original C3 entry (stale — kept for provenance)</summary>

### C3 — Visibility list/query RPCs unimplemented (real-gap)

- **Signature:** `Unimplemented desc = list_open_workflow_executions`; plus `list_closed_…`,
  `scan_…`, advanced-visibility query failures; `unknown search attribute:
  TemporalExternalPayloadCount`; `Should NOT be empty, but was []` (list returned nothing).
- **Blast radius:** `TestAdvancedVisibilitySuite` (32), `TestAdvancedVisibilitySuiteLegacy` (32),
  `TestWorkflowVisibilityTestSuite` (2), `TestWorkflowMemoTestSuite` (3 — setup polls
  `list_open_workflow_executions`), `TestListWorkflow*`.
- **Ground truth:** v1.31.0 serves the open/closed/scan list APIs and the advanced-visibility query
  surface backed by the visibility store.
- **Gap:** the legacy list RPCs are stubbed (`grpc/workflow_service.rs` returns `unimplemented`);
  advanced visibility depends on the projection store which the conformance run does not exercise.
- **Fix locus:** `api-conformance-visibility-legacy` spec (exists) for the list RPCs; advanced
  visibility is broader (projection plane).
- **Category:** real-gap (list RPCs); advanced visibility partly out-of-public-scope where it needs
  the internal projection store.

</details>

### C4 — Nexus RPCs unimplemented (real-gap) — split into admin CRUD (C4a) vs operation execution (C4b)

> **Update 2026-06-18 — scope split.** The original C4 conflated two unrelated bodies of work. They
> are now tracked separately because endpoint admin CRUD (a registry) and Nexus operation execution
> (task transport + in-workflow operations) share no implementation and land in different specs.

- **Signature:** `Unimplemented desc = create_nexus_endpoint` (~44, the five OperatorService endpoint
  RPCs), plus the operation-execution suites that depend on task transport.

**C4a — Nexus endpoint admin CRUD (this spec):**
- **Blast radius:** `TestNexusEndpointsFunctionalSuite` (15), `TestNexusAPIValidationTestSuite` (2) ≈ 17.
- **Ground truth:** `service/frontend/nexus_endpoint_client.go @ v1.31.0` (validation, translation,
  read-after-write, list/name-filter) and `service/matching/nexus_endpoint_client.go @ v1.31.0`
  (server-authored id/version, duplicate detection, version CAS). Error codes pinned:
  duplicate name → `AlreadyExists` (`:100`); missing id → `NotFound` (`:152`, `:218`); version
  mismatch → **`FailedPrecondition`** (`:155-156`, **not `ABORTED`**); worker-target namespace missing
  → `FailedPrecondition`; all spec/id validation → aggregated `InvalidArgument`. Six limit knobs with
  defaults from `common/dynamicconfig/constants.go @ v1.31.0`.
- **Fix locus:** `api-conformance-nexus-admin` — **spec ground-truthed 2026-06-18** (requirements +
  design + tasks rewritten with citations; two factual errors in the prior draft corrected). Ready to
  execute, with one prereq: the runtime `NexusEndpointRegistry` is a static construction-time
  `Arc<HashMap>` (`nexus.rs:72-86`), so Req 3 includes making it live/store-backed and changing
  `resolve` to return an owned value (task 4.2).
- **Category:** real-gap (tracked).

**C4b — Nexus operation execution / task transport (out of this spec):**
- **Blast radius:** `TestNexusApiTestSuiteWithTemporalFailures` (40),
  `TestNexusApiTestSuiteWithLegacyErrorPaths` (40), `TestNexusWorkflowTestSuite` (2) ≈ 82.
- **Ground truth:** v1.31.0 Nexus task transport (`PollNexusTaskQueue` /
  `RespondNexusTaskCompleted` / `RespondNexusTaskFailed`) + the in-workflow Nexus operation lifecycle.
- **Fix locus:** `edge-nexus-task-transport` / `kernel-nexus-operations` / `runtime-nexus-dispatch`
  (+ `nexus-retry-policy`, `nexus-multi-cluster`). **Explicit non-goal of `api-conformance-nexus-admin`.**
- **Category:** real-gap (tracked across the runtime/edge Nexus specs).


### C5 — Missing admission-time validation (real-gap) — the "accept-and-proceed" pattern

This is the most *representative* cluster: tokeira accepts requests Temporal rejects with
`InvalidArgument`. `52` direct `expected: *serviceerror.InvalidArgument` + `41` `error expected but
got nil`.

- **C5a — Completion callbacks** (`TestCallbacksSuiteHSM` 14, `TestCallbacksSuiteCHASM` 14).
  Fully investigated:

  > **Update 2026-06-12 — IMPLEMENTED.** `validate_completion_callbacks` (in `grpc/translate.rs`) is
  > wired into `start_request_to_edge`. v1.31.0 calls `validateWorkflowCompletionCallbacks` from a
  > single site — the Start path (`workflow_handler.go:671 @ v1.31.0`) — so validating only on Start
  > is correct, not a parity gap. Rules implemented with verbatim messages (confirmed against
  > `workflow_handler.go:6299` and `AddressMatchRules.Validate` `components/callbacks/config.go:71 @
  > v1.31.0`): count-cap, URL length, scheme, missing host, header size, plus header-key lowercasing.
  > Limits are source-cited constants (1000 / 8192 / 32) per `DECISION-callback-validation.md`. Tests:
  > `start_request_validates_completion_callbacks` + `start_request_lowercases_completion_callback_headers`
  > (green). The `invalid-scheme` conformance sub-case passes (no override needed); the
  > url-length/header-size/too-many sub-cases are **harness-limited** (rely on `OverrideDynamicConfig`
  > the seam can't deliver); `allowedAddresses` (`url not configured`, `https required`) remains
  > **deferred** as a deployment-policy decision. Original investigation below kept for provenance.

  - **Ground truth:** `WorkflowHandler.validateWorkflowCompletionCallbacks`
    (`service/frontend/workflow_handler.go @ v1.31.0`) + `AddressMatchRules.Validate`
    (`components/callbacks/config.go @ v1.31.0`). Rules: URL scheme ∈ {http,https}; URL length ≤
    `CallbackURLMaxLength`; header Σ(k+v) ≤ `CallbackHeaderMaxSize`; count ≤ `MaxCallbacksPerWorkflow`;
    URL must match a configured address pattern; non-`AllowInsecure` patterns reject http.
  - **Gap:** tokeira's edge translates callbacks straight through (`translate/to_internal.rs`,
    `callbacks_to_edge`) with **zero validation**, and has none of the four dynamic-config knobs
    (`FrontendCallbackURLMaxLength`, `FrontendCallbackHeaderMaxSize`, `MaxCallbacksPerWorkflow`,
    `callbacks.AllowedAddresses` — a list of `{Pattern, AllowInsecure}`).
  - **Fix locus:** new `validate_completion_callbacks` edge helper invoked from the
    `StartWorkflowExecution` admission path + four config values, overridable via dynamic config (the
    tests drive them with `OverrideDynamicConfig`). No kernel/runtime change.
  - **Category:** real-gap.
- **C5b — Other admission validators:** the same shape recurs for links, versioning info, and other
  start fields. Each test asserting `InvalidArgument` that got `nil` is an instance.

  > **Update 2026-06-12 — links done (partial C5b).** `validate_links` + `collect_admission_links`
  > in `grpc/translate.rs`, wired into `start_request_to_edge`, mirror `WorkflowHandler.validateLinks`
  > and the deduped `allLinks` assembly (`service/frontend/workflow_handler.go:6230,6260,675 @ v1.31.0`):
  > count cap (`MAX_LINKS_PER_REQUEST=10`), per-link `encoded_len()` size (`LINK_MAX_SIZE=4000`),
  > WorkflowEvent namespace/workflowId/runId + event-ref type/id rule, BatchJob job-id, and
  > `unsupported link variant` for Activity/NexusOperation/unset (a behavioural tightening — tokeira
  > previously accepted those on Start). Test `start_request_validates_links`; fmt + clippy clean.
  > **Still open:** the 4 other v1.31.0 `validateLinks` call sites (signal-with-start and three more)
  > are not yet wired, and the versioning-info / other start-field validators remain.

  > **Update 2026-06-12 (cont.) — links COMPLETE.** The 4 remaining `validateLinks` call sites are now
  > wired: `signal_request_to_edge`, `cancel_request_to_edge`, `terminate_request_to_edge`,
  > `signal_with_start_request_to_edge` each call `validate_links(&req.links)` (request links only — no
  > callback combination, matching `workflow_handler.go:2183,2228,2356,2433 @ v1.31.0`). Regression
  > test `signal_cancel_terminate_paths_validate_links`. Versioning-info: the override path is already
  > validated and the legacy `useVersioning` path is a C2 deliberate-deviation; no further concrete
  > "other start field" gap is enumerated (drive any residual from a corpus re-run). C5b closed.



### C6 — Over-rejection: tokeira rejects requests Temporal accepts (real-gap, opposite polarity)

> **Update 2026-06-12 — RESOLVED.** C6b (nil/empty SA + memo filtering) is implemented:
> `memo_to_domain`/`search_attributes_to_domain` filter `is_temporal_nil_payload` (json `null`/`[]`),
> citing `common/payload/payload.go:94 @ v1.31.0`, with unit tests. C6a: the cron suite's `@every Ns`
> is handled by `parse_every_descriptor`; the remaining defect was error *fidelity* —
> `validate_client_cron_schedule` masked parser errors as "missing required field" instead of
> v1.31.0's `InvalidArgument "invalid CronSchedule."` (`backoff.ValidateSchedule @ v1.31.0`). Fixed to
> propagate the verbatim message, and added the `@midnight` descriptor (robfig `ParseStandard` alias
> for `@daily`). Tests added in `schedule.rs` + `grpc/translate.rs`; fmt + clippy clean. Original
> investigation notes below kept for provenance.

The inverse of C5 and easy to miss: tokeira returns `InvalidArgument` where v1.31.0 succeeds.

- **C6a — Cron schedule** (`TestCronTestSuite`, `TestCronTestClientSuite`):
  `InvalidArgument desc = missing required field: StartWorkflowExecutionRequest.cron_schedule` on a
  request that carries a *valid* cron string. tokeira's start translation
  (`grpc/translate.rs::start_request_to_edge` → `validate_client_cron_schedule`) is rejecting a cron
  expression Temporal accepts. **Investigate the cron parser/validation** — likely a parser
  mismatch against the cron dialect v1.31.0 accepts.
- **C6b — Nil/empty search attributes** (`TestWorkflowStart_NilSearchAttributesFiltered`,
  `..._AllNilSearchAttributesFiltered`, `..._NilMemoFiltered`):
  `InvalidArgument desc = missing required field: SearchAttributes: invalid payload data`. v1.31.0
  *filters* nil/empty search-attribute and memo entries rather than rejecting; tokeira treats an
  all-nil payload as invalid. **Fix:** filter nil entries before validation, matching v1.31.0's
  filtering behaviour (these dedicated tests exist precisely to pin it).
- **Category:** real-gap (both) — these are correctness bugs, not missing features.

### C7 — Workflow lifecycle / describe gaps surfaced as NotFound (mixed)

- **Signature:** `NotFound desc = <ns>/<workflowId>` from `DescribeWorkflowExecution`,
  `GetWorkflowExecutionHistory`, terminate-by-limit, etc.
- **Blast radius:** `TestDescribeTestSuite`, `TestSizeLimitFunctionalSuite`,
  `TestWorkflowDeleteExecutionSuite`, parts of many lifecycle suites.
- **Interpretation:** these need deeper per-test triage — some are downstream of a workflow that
  never started because an earlier call failed (cascade from C1–C6), others are genuine
  describe/history gaps. **Do not bulk-classify; triage per test after C1–C6 are fixed**, since
  fixing the upstream clusters will clear many of these for free.
- **Category:** defer classification until C1–C6 land (many are cascades).

### C8 — Internal-surface / admin tests (out-of-public-scope)

- **Signature:** suites that drive `AdminService`/`HistoryService`/`MatchingService` directly, or
  poke `testBase`/persistence in the body.
- **Blast radius:** `TestAddTasksSuite`, `TestAdminBatchRefreshWorkflowTasksTestSuite`,
  `TestAcquireShard_*`, `TestPurgeDLQTasksSuite`, `TestDLQSuite`, `TestAdminRebuildMutableState_*`,
  `TestTransientTaskSuite`.
- **Interpretation:** these depend on internal surfaces the Shape-2 onebox does not front, and are
  out-of-public-scope **by construction**. The wire-coverage scope derivation
  (`tokeira-edge::conformance::scope`) names which internal client each touched — use it to populate
  the `InternalSurface` evidence mechanically.
- **Category:** out-of-public-scope (cite the internal surface from the scope report).

### C9 — `unfinished` outcomes (panic-crash siblings) — not a cluster, a consequence

- **267 `unfinished`** outcomes are sibling sub-tests of an entrypoint that panicked mid-run (e.g.
  the schedule-migration suite's nil-deref). They are not independent failures; they are tests that
  *ran* but whose process died before reporting. Under per-entrypoint isolation the panic no longer
  cascades across entrypoints, but it still truncates siblings *within* the crashing entrypoint.
- **Action:** fix the panic (a nil-deref on the shimmed `testBase`/persistence under Option-B), and
  these resolve to real pass/fail. Until then they are correctly captured as `unfinished`, not lost.

---

## Suggested order of attack

Single source: the **Status ledger `Next action` column** and the **Order** line beneath the ledger
(top of this doc). This section intentionally holds no second ordering — keeping one avoids drift.

---

## Mechanical classification: feeding the ledger

The report side is built and tested (tasks 9–10). To turn this catalogue into an enforced ledger:

1. Run the corpus → `tokeira-conformance-results.json`.
2. Distil → per-test `outcomes.json` (task 8.2 shape: `{test_id, outcome, elapsed_seconds}`).
3. Author a ledger (`LedgerEntry[]`) classifying every non-passing `test_id` per the clusters above:
   - `real-gap` → `EvidenceRef::TrackingIssue` (link the child spec, e.g.
     `activity-executions-first-class`).
   - `deliberate-deviation` → `EvidenceRef::SpecOrPr` (e.g. legacy version-sets replacement).
   - `out-of-public-scope` → `EvidenceRef::InternalSurface` (from the scope report, mechanical).
4. Join (task 9.2 `join_test_ledger`) + run gates (task 10 `evaluate_all_gates`). The gates fail the
   run on any unclassified non-pass (totality), mis-cited evidence (scope inflation), or a
   `real-gap` that now passes (monotonicity).

This is how the 1194 failures become a reviewable, drift-proof ledger rather than noise.

---

## Ground-truth references (v1.31.0)

- Callback validation: `service/frontend/workflow_handler.go @ v1.31.0`
  (`validateWorkflowCompletionCallbacks`, `validateCallbackURL`); `components/callbacks/config.go @
  v1.31.0` (`AddressMatchRules.Validate`, `AddressMatchRule.Allow`).
- Read these from the local checkout: `git -C ../temporal show v1.31.0:<path>`.
- Per AGENTS.md §8: `proto/upstream/` is authoritative for wire shape; the tagged server source is
  authoritative for behaviour. Never infer from generated artifacts under `target/`.
