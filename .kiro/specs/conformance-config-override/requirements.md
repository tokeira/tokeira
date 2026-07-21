# Requirements Document: Conformance-Only Dynamic-Config Override

## Introduction

This spec adds a **conformance-only** mechanism that lets Temporal's functional test corpus deliver
its `OverrideDynamicConfig(setting, value)` calls to an **out-of-process `tokeirad`** over a control
RPC, so that corpus leaves whose behaviour is governed by a non-default dynamic-config value can run
against tokeira instead of being skipped.

tokeira's production posture is deliberately **config-as-constant**: each Temporal dynamic-config
default is pinned as a hardcoded Rust constant, `RuntimeConfig` is built from `Default` (never from
TOML), and `tokeirad` takes no env vars on invocation (`AGENTS.md` §Configuration). The functional
harness originally recorded the consequence: `OverrideDynamicConfig` could not reach an
out-of-process `tokeirad`, so such leaves were either independent tagged runs or classified skips.
The conformance control service and fork bridge implemented by this spec now deliver supported
overrides to the external process; kernel-excluded and unwired settings retain those fallback paths.

This spec **does not change that production posture**. It introduces a surface that exists **only in a
`conformance`-feature build** and is **inert unless explicitly mounted**, through which the harness can
set/clear per-setting overrides for the duration of a test. Production `tokeirad` contains none of it.

The mechanism is bounded by a hard line: the **pure kernel** (`tokeira-kernel`) never reads it.
Kernel-consulted constants stay compile-time constants, because a runtime-mutable value read inside the
deterministic transition engine would make history replay non-deterministic. The overridable set is
exactly the values consulted **live at request time** in the runtime / edge / projection planes.

**Compatibility authority:** `TEMPORAL_SERVER_COMPAT = 1.31.0`. The set of override *keys* and their
value types are the Temporal v1.31.0 `dynamicconfig` settings (`common/dynamicconfig/constants.go @
v1.31.0`). This spec adds no new engine behaviour that a client can observe in production; its only
observable effect is inside a conformance build.

### Relationship to sibling work

- **`temporal-functional-conformance`** owns the corpus, the drive-to-green campaign, the skip
  registry, and the "conventions for acting on a run." This spec is an **enabling capability** for that
  campaign: it converts a recurring skip class into real coverage. It does **not** replace the
  "independent runs" convention (still used for feature modes and for kernel-const cases) or the skip
  registry (still used for genuinely-unbridgeable cases).
- **The conformance metrics bridge** (`docs/HANDOVER-conformance-metrics-bridge.md`, retired to git
  history; `tests/testcore/tokeira_metrics_bridge.go`) is the **direct precedent**: it makes an in-process
  Temporal test seam (`CaptureMetricsHandler`) work against out-of-process `tokeirad` by pointing it at
  a tokeira endpoint, under a strict honesty boundary. This spec applies the same pattern to
  `OverrideDynamicConfig` and adopts the same honesty boundary.
- **`tokeira-compatibility-service`** (`proto/tokeira/compatibility/v1/`, buffa/connect-rust) is the
  precedent for a **tokeira-owned, non-Temporal Connect-RPC service**; the control service mirrors its
  shape.

### Ground truth (agent-verified, this investigation)

- **Kernel is pure and takes no config.** `BasicKernel` is a zero-field unit struct and
  `Kernel::apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject>` carries no
  config/limits parameter (`crates/tokeira-kernel/src/kernel.rs`). Kernel tunables are module-level
  `const`s. `RuntimeConfig` (`crates/tokeira-runtime/src/runtime/mod.rs:423`) is mechanical-only
  (lane count, scanner intervals) and is never threaded into the kernel.
- **Kernel-consulted config constants (the excluded set):** `CONTINUE_AS_NEW_MIN_INTERVAL = 1s`
  (`kernel.rs:123`, v1.31.0 `WorkflowIdReuseMinimalInterval`); `MAX_BUFFERED_EVENTS = 100`
  (`kernel.rs:5142`, v1.31.0 `MaximumBufferedEventsBatch`).
- **Runtime/edge/projection consult sites (the overridable set):**
  `MAX_BUFFERED_QUERIES_PER_RUN = 1` (`crates/tokeira-runtime/src/buffered_queries.rs`, v1.31.0
  `MaxBufferedQueryCount`) — already carries a `#[cfg(test)] buffer_unchecked` hook anticipating a
  raised limit; `MAX_PAGE_SIZE = 1000` (`crates/tokeira-projection/src/types.rs`) consulted by
  `legacy_page_size` in `crates/tokeira-edge/src/grpc/translate.rs`; the inline WFT default `10s`,
  `DEFAULT_QUERY_TIMEOUT = 10s`, `DEFAULT_UPDATE_TIMEOUT = 30s`, and the `20s`
  `HistoryLongPollExpirationInterval` in the edge.
- **Not enforced anywhere (necessary-but-not-sufficient):** the size-limit settings
  (`MutableStateSizeLimitError` 8MiB, `BlobSizeLimitError` 2MiB, `HistorySizeLimitError` 50MiB,
  `HistoryCountLimitError` 50×1024, `HistorySizeSuggestContinueAsNew` 4MiB) have **no consult site** in
  tokeira today — an override cannot bite until the enforcement exists.
- **The harness seam:** `FunctionalTestBase.OverrideDynamicConfig(setting dynamicconfig.GenericSetting,
  value any) (cleanup func())` (`tests/testcore/functional_test_base.go:645`),
  `TestCluster.OverrideDynamicConfig` (`tests/testcore/test_cluster.go:636`), and the tokeira onebox
  seam files (`tests/testcore/onebox.go`, `tokeira_conformance_cluster.go`, `tokeira_harness.go`,
  `tokeira_metrics_bridge.go`, `tokeira_conformance_skip.go`).
- **Temporal dynamic-config value types** are heterogeneous and namespace-filtered (e.g.
  `NewNamespaceIntSetting`, `NewNamespaceDurationSetting`, `NewGlobalBoolSetting` in
  `common/dynamicconfig/constants.go @ v1.31.0`).

## Glossary

- **Dynamic config (Temporal):** a named, typed, namespace/globally-scoped setting that Temporal reads
  live at request time; the corpus overrides these via `OverrideDynamicConfig` for test-only values.
- **Config-as-constant (tokeira):** the convention of pinning each Temporal dynamic-config *default* as
  a hardcoded Rust constant; production has no knob.
- **Consult site:** a code location that reads a pinned constant to make a decision (e.g. the buffered-
  query capacity check).
- **Consult-site accessor:** the small function a consult site calls instead of the bare constant; in a
  production build it returns the constant, in a conformance build it consults the override registry.
- **Override registry:** the process-global, conformance-only store of `setting key → typed value`
  overrides.
- **Control service:** the conformance-only Connect-RPC service exposing set/clear/reset of overrides.
- **`conformance` feature:** the Cargo feature that compiles in the registry, the control service, and
  the consult-site indirection; **off** in production builds.
- **Wired key:** a Temporal setting key that has a real tokeira consult-site accessor reading it; only
  wired keys can be honoured.
- **Excluded (kernel) set:** constants consulted inside `tokeira-kernel`; never overridable.
- **Fork bridge:** the tokeira-specific onebox seam in the pinned Temporal fork that turns an
  `OverrideDynamicConfig` call into control-service RPCs.
- **Honesty boundary:** the rule that an override only takes effect where tokeira genuinely honours it;
  an unsupported override never fabricates behaviour and never turns a test green.

## Target State

**Becomes supported (conformance builds only):**
- A `conformance` Cargo feature that compiles in (a) a process-global override registry, (b) a
  Connect-RPC control service, and (c) consult-site accessors at the wired sites.
- The control service, when explicitly mounted on its own loopback listener, honours
  set/clear/reset of scalar and structured-JSON overrides for **wired, non-kernel** setting keys.
- The pinned Temporal fork's out-of-process `OverrideDynamicConfig` delivers to the control service and
  cleans up per test.
- An initial wired key (`MaxBufferedQueryCount`) proving the loop, then incremental wiring of
  page-size, long-poll interval, and the reported-problems threshold (unblocking Tier 3.22 leaf 4).
- Callback admission limits and the structured `component.callbacks.allowedAddresses` rule list are
  wired at the edge for Tier 5.32. The structured form is conformance-only transport; it does not add
  a production config field.

**Stays out of scope / unchanged:**
- Production `tokeirad`: no registry, no service, no indirection, no new listener, no env var, no TOML
  knob. Behaviour is byte-identical to today.
- The pure kernel: unchanged; kernel constants are not overridable.
- Values with no enforcement in tokeira (the size limits): remain Unsupported until their enforcement
  is implemented under their own specs; this spec does not implement enforcement.
- Namespace-scoped override *granularity* beyond global: deferred unless a corpus leaf requires it
  (the corpus overrides are effectively global per single-namespace test).
- General/production dynamic configuration: explicitly a non-goal. This is test infrastructure.

**Sanctioned exception:** a conformance build admits a process-global mutable config surface, which the
production `config-as-constant` posture otherwise forbids. It is justified only because it is
compile-gated out of production and because it converts a documented skip class into real conformance
coverage (see Requirement 0).

## Evidence From Current Code

| Concern | Where it lives today | Anchor |
|---|---|---|
| Kernel is pure, no config param | `crates/tokeira-kernel/src/kernel.rs` (`BasicKernel`, `Kernel::apply`) | zero-field struct; `apply(&self, loaded, command)` |
| Kernel const — CaN min interval | `crates/tokeira-kernel/src/kernel.rs:123` | `CONTINUE_AS_NEW_MIN_INTERVAL` |
| Kernel const — max buffered events | `crates/tokeira-kernel/src/kernel.rs:5142` | `MAX_BUFFERED_EVENTS` |
| Runtime const — buffered query cap | `crates/tokeira-runtime/src/buffered_queries.rs` | `MAX_BUFFERED_QUERIES_PER_RUN` + `#[cfg(test)] buffer_unchecked` |
| Projection const — page size | `crates/tokeira-projection/src/types.rs` | `MAX_PAGE_SIZE`; edge `legacy_page_size` |
| Edge inline defaults | `crates/tokeira-edge/src/grpc/translate.rs` | WFT `10s`, `DEFAULT_QUERY_TIMEOUT`, `DEFAULT_UPDATE_TIMEOUT`, long-poll `20s` |
| Mechanical runtime config (never to kernel) | `crates/tokeira-runtime/src/runtime/mod.rs:423` | `RuntimeConfig` |
| Tokeira-owned Connect-RPC precedent | `crates/tokeira-compatibility-service/`, `proto/tokeira/compatibility/v1/` | buffa/connect-rust |
| Out-of-process test-seam precedent | fork `tests/testcore/tokeira_metrics_bridge.go` | scrape-backed `CaptureMetricsHandler` |
| Harness override entry points | fork `tests/testcore/functional_test_base.go:645`, `test_cluster.go:636` | `OverrideDynamicConfig` |
| Skip registry (fallback) | fork `tests/testcore/tokeira_conformance_skip.go` | OverrideDynamicConfig-class skips |

**Target authority:** the Temporal v1.31.0 `dynamicconfig` settings and their value types
(`common/dynamicconfig/constants.go @ v1.31.0`) define the key namespace and per-key value type the
control service accepts.

## Override-target policy

Every value the corpus is known to override is classified by plane and disposition. "Overridable"
means a live-at-request-time consult site can read the registry; "Kernel-excluded" means it is
consulted in the pure kernel and stays a constant; "Not-enforced" means tokeira has no consult site yet
so an override is Unsupported until enforcement lands under another spec.

| Temporal setting key | tokeira site (plane) | Disposition | Notes |
|---|---|---|---|
| `history.MaxBufferedQueryCount` | `buffered_queries.rs` (runtime) | **Overridable — first consumer** | already anticipated by a test hook |
| `frontend.historyMaxPageSize` / matching page size | `MAX_PAGE_SIZE` (projection) → edge | **Overridable** | unblocks the 2.10 multi-page skip |
| `history.longPollExpirationInterval` (20s) | edge inline | **Overridable** | live-read at poll admission |
| default WFT / query / update timeouts | edge inline | **Overridable** | live-read at translate |
| `system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute` (5) | reported-problems (runtime, `reported_problems_threshold()` live-read) | **Overridable — wired (Tier 3.22 proving consumer)** | leaf 4 un-skipped; threshold read live at the derive-on-read consult site (0→2 mid-run). Accessor landed; live two-run verification tracked separately |
| `frontend.callbackURLMaxLength` (1000) | callback admission (edge) | **Overridable — Tier 5.32** | live-read before validating each Nexus callback URL |
| `frontend.callbackHeaderMaxLength` (8192) | callback admission (edge) | **Overridable — Tier 5.32** | live-read before validating aggregate callback header size |
| `system.maxCallbacksPerWorkflow` (32) | callback admission (edge) | **Overridable — Tier 5.32** | live-read before per-callback validation |
| `component.callbacks.allowedAddresses` | callback admission (edge) | **Overridable — Tier 5.32, structured JSON** | fork serializes the v1.31.0 address-rule list as JSON; production policy remains a separate decision |
| `history.workflowIdReuseMinimalInterval` (1s) | `CONTINUE_AS_NEW_MIN_INTERVAL` (kernel) | **Kernel-excluded** | separate decision (independent-run build or kernel→runtime move) |
| `history.maximumBufferedEventsBatch` (100) | `MAX_BUFFERED_EVENTS` (kernel) | **Kernel-excluded** | no corpus leaf overrides it today |
| `limit.mutableStateSize.error`, `limit.blobSize.error`, `limit.historySize.error`, `limit.historyCount.error`, `limit.historySize.suggestContinueAsNew` | none | **Not-enforced** | Unsupported until enforcement exists |

---

## Requirement 0 — Adjudication: sanction the conformance-only override surface

This spec introduces new, cross-cutting surface into an engine whose production posture forbids
runtime config. Implementation is gated on explicit owner acceptance of the boundary.

### 0.1 What this admits

A conformance-feature build gains a **process-global mutable config surface** (the override registry)
and a **control listener**, which the production `config-as-constant` / no-env-var / `RuntimeConfig::
default` posture otherwise forbids. It is admitted only because it is **compile-gated out of
production** and is **inert unless explicitly mounted**, and because it converts a documented skip class
(OverrideDynamicConfig-class) into real conformance coverage — including reconfiguration-within-a-run
leaves (e.g. Tier 3.22 `TestWFTFailureReportedProblems_DynamicConfigChanges`) that "independent runs"
cannot cover.

### 0.2 New surface inventory

- A new Cargo **`conformance` feature** (workspace-level, off by default) gating everything below.
- A new **`tokeira-conformance-control`** crate (feature-gated): the override registry + the
  Connect-RPC control service (buffa/connect-rust), with its proto under `proto/tokeira/conformance/
  v1/` — mirroring the `tokeira-compatibility-*` split.
- **Consult-site accessors** replacing bare-constant reads at the wired sites (runtime/edge/
  projection), compiling to the constant when the feature is off.
- A **fork seam** (`tests/testcore/tokeira_dynamic_config_bridge.go`) mirroring the metrics bridge;
  onebox-only, never a corpus test-body edit.

### 0.3 Boundaries that must be sanctioned

1. **Kernel exclusion.** The pure kernel never reads the registry; kernel-consulted constants stay
   constants (Requirement 4). This is also the determinism/replay-safety boundary.
2. **Production isolation.** The feature-off build contains none of the surface; even a feature-on
   build binds nothing unless a mount flag is set; the service is never added to the production
   Temporal gRPC router (Requirement 3).
3. **Honesty boundary.** Only wired, non-kernel keys are honoured; an unsupported override never
   fabricates behaviour and never turns a test green (Requirement 5).

### 0.4 Acceptance of Requirement 0

Requirement 0 is accepted when the owner confirms: (a) a conformance-only mutable-config surface,
compile-gated out of production, is a sanctioned exception to config-as-constant; (b) the new
`conformance` feature, the `tokeira-conformance-control` crate, the `proto/tokeira/conformance/v1/`
proto, and the fork bridge are sanctioned new surface; (c) the kernel-exclusion boundary is correct and
`WorkflowIdReuseMinimalInterval` / `MaximumBufferedEventsBatch` remain constants (their leaves handled
separately); (d) the honesty boundary — Unsupported for unwired/kernel/not-enforced keys, with the fork
falling back to the skip registry — is the binding rule.

**Accepted (2026-07-09, owner):** confirmed via the Tier 3.22 takeover directive — build the control
mechanism per this spec (metrics-bridge precedent) and use it to **deprecate and remove** Codex's
boot-time `TOKEIRA_CONFORMANCE_REPORTED_PROBLEMS_THRESHOLD` env var. The **reported-problems threshold**
(`system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute`) is the proving `Wired` consumer
(standing in for the illustrative `MaxBufferedQueryCount`-first phasing). Because the control RPC applies
overrides at runtime, it additionally unlocks leaf 4 (`TestWFTFailureReportedProblems_DynamicConfigChanges`,
mid-run 0→2), which is **un-skipped in scope**. `connectrpc` pinned at `0.8.1`. Kernel exclusion and the
honesty boundary stand as written.

---

## Requirement 1 — Conformance-gated override registry

**User story:** As a conformance engineer, I want a process-global override store that consult sites
read, so that a test-set value takes effect in `tokeirad` without changing any pinned default, and so
that production builds contain none of it.

**Acceptance criteria:**
1. WHEN `tokeirad` (or any workspace crate) is built without the `conformance` feature THE override
   registry, the control service, and every consult-site indirection SHALL be absent from the
   compiled artifact.
2. WHERE the `conformance` feature is enabled THE consult-site accessor for a wired key SHALL return
   the registry's override value if one is set, and otherwise the pinned default constant.
3. WHERE the `conformance` feature is disabled THE consult-site accessor SHALL evaluate to exactly the
   pinned default constant (no branch, no global read).
4. THE pinned default constant SHALL remain the single source of truth for the value; an override is a
   test-time shadow that SHALL never mutate or replace the default.
5. THE registry SHALL key overrides by the Temporal setting key string (e.g.
   `"history.MaxBufferedQueryCount"`) so the fork can pass `setting.Key()` verbatim.
6. THE registry SHALL be safe for concurrent reads during request handling and concurrent writes from
   the control service.

## Requirement 2 — Connect-RPC control service

**User story:** As the fork harness, I want an RPC to set/clear/reset overrides, so that an
out-of-process `tokeirad` can receive the values the corpus would set in-process.

**Acceptance criteria:**
1. THE control service SHALL expose `SetDynamicConfigOverride(key, value)`,
   `ClearDynamicConfigOverride(key)`, and `ResetDynamicConfigOverrides()`.
2. THE override value SHALL be a typed union covering the Temporal dynamic-config value kinds used by
   the corpus: `int64`, `double`, `bool`, `string`, `Duration`, and a JSON string for structured Go
   values whose schema is validated by the wired consumer.
3. WHEN `SetDynamicConfigOverride(key, value)` names a wired, non-kernel key AND the value's type
   matches that key's expected type THE registry SHALL record the value AND every subsequent consult of
   that key SHALL observe it (read live at request time).
4. IF the value's type does not match the key's expected type THEN the service SHALL respond
   `InvalidArgument` AND SHALL NOT record it.
5. WHEN `ClearDynamicConfigOverride(key)` is received THE registry SHALL drop that key's override AND
   subsequent consults SHALL observe the pinned default.
6. WHEN `ResetDynamicConfigOverrides()` is received THE registry SHALL drop all overrides.
7. THE service SHALL be built on buffa/connect-rust with its proto under `proto/tokeira/conformance/
   v1/`, mirroring `tokeira-compatibility-service`; no checked-in generated code.

## Requirement 3 — Gating and production isolation

**User story:** As the engine owner, I need certainty that this surface can never affect production, so
that admitting a mutable config path for tests does not erode the config-as-constant guarantee.

**Acceptance criteria:**
1. WHEN the `conformance` feature is disabled THE control service and registry SHALL NOT exist in the
   binary (compile-time absence, not a runtime guard).
2. WHERE the `conformance` feature is enabled AND the control mount is not explicitly requested (flag/
   config) THE control service SHALL NOT bind any listener.
3. THE control service SHALL NEVER be added to the production Temporal gRPC router (the
   `.add_service(...)` set in `apps/tokeirad/src/lib.rs`); WHEN mounted it SHALL bind a separate,
   loopback-only listener distinct from the public frontend.
4. THE mechanism SHALL NOT read any environment variable or TOML key in a production build, preserving
   the `no env vars on invocation` and `RuntimeConfig::default` conventions.
5. WHILE no override is set for a key THE observable behaviour of `tokeirad` at that consult site SHALL
   be identical to a build without the `conformance` feature.

## Requirement 4 — Kernel purity boundary (excluded set)

**User story:** As the engine owner, I require the pure kernel to stay pure and replay-deterministic,
so that a test override can never make the same history replay differently.

**Acceptance criteria:**
1. THE pure kernel (`tokeira-kernel`) SHALL NOT read the override registry and SHALL NOT gain a config
   parameter for this mechanism; kernel-consulted constants (`CONTINUE_AS_NEW_MIN_INTERVAL`,
   `MAX_BUFFERED_EVENTS`) SHALL remain compile-time constants.
2. WHEN a `SetDynamicConfigOverride` names a kernel-consulted key (e.g.
   `history.workflowIdReuseMinimalInterval`, `history.maximumBufferedEventsBatch`) THE service SHALL
   respond `Unsupported` AND SHALL NOT record it.
3. THE overridable set SHALL be exactly the values consulted **live at request time** in the runtime/
   edge/projection planes; no override SHALL change a value that has already been committed into a
   run's authoritative history.
4. WHERE a corpus leaf depends on overriding a kernel-excluded key (e.g. the Tier 3.18
   `WorkflowIdReuseMinimalInterval=0` leaf) THE resolution SHALL be an independent-run build or a
   separate design decision — out of scope for this spec, and such leaves SHALL remain skipped here.

## Requirement 5 — Honesty boundary

**User story:** As a reviewer, I need "green" to mean earned, so that this mechanism can never pass a
test by fabricating behaviour tokeira does not implement.

**Acceptance criteria:**
1. IF `SetDynamicConfigOverride` names a key with no wired consult-site accessor THEN the service SHALL
   respond `Unsupported` AND SHALL NOT record it.
2. WHEN the fork bridge receives an `Unsupported` (or `InvalidArgument`) response THE bridge SHALL fall
   back to the existing skip-registry behaviour for that test (skipped with a cited reason), and SHALL
   NOT allow the test to proceed as if the override took effect.
3. THE set of wired keys SHALL grow only as a genuine consult-site accessor is added; a key becomes
   honourable **only** when a real site reads it — mirroring the metrics-bridge honesty boundary.
4. A `Not-enforced` setting (e.g. a size limit tokeira does not enforce) SHALL be `Unsupported`, never
   silently honoured; enforcing it is other specs' work, not this one's.

## Requirement 6 — Fork harness bridge

**User story:** As the corpus, I want `OverrideDynamicConfig` to "just work" against out-of-process
`tokeirad`, so that leaves needing a non-default value run instead of skipping — without editing corpus
test bodies.

**Acceptance criteria:**
1. WHEN a test calls `OverrideDynamicConfig(setting, value)` against an out-of-process tokeira cluster
   THE bridge SHALL call `SetDynamicConfigOverride(setting.Key(), coerce(value))` and SHALL return a
   `cleanup` closure that calls `ClearDynamicConfigOverride(setting.Key())`.
2. WHEN a test (or sub-test) tears down THE bridge SHALL call `ResetDynamicConfigOverrides()` so that no
   override leaks across tests.
3. THE bridge SHALL live in the tokeira onebox/Shape-2 seam (`tests/testcore/tokeira_*.go`), mirroring
   `tokeira_metrics_bridge.go`, and SHALL NOT edit any corpus test body.
4. WHERE the control service is unreachable or the override is `Unsupported`/`InvalidArgument` THE
   bridge SHALL apply the Requirement 5.2 fallback (skip), never a silent no-op that lets the test
   proceed.

## Requirement 7 — First consumer and phased wiring

**User story:** As the implementer, I want a proven end-to-end slice first and then incremental wiring,
so that the loop is validated before breadth is added.

**Acceptance criteria:**
1. THE first wired consult site SHALL be `MaxBufferedQueryCount` (`buffered_queries.rs`), demonstrating
   `Set → consult observes override → Clear → consult observes default` end-to-end.
2. Subsequent consult sites SHALL be wired incrementally, each as a consult-site accessor plus a
   registry key entry: page size (`MAX_PAGE_SIZE`/matching page size), long-poll interval, and default
   timeouts.
3. THE reported-problems threshold
   (`system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute`) SHALL be wired such that Tier
   3.22 `TestWFTFailureReportedProblems_DynamicConfigChanges` becomes runnable (the 0→2 mid-run change
   observed live), once the reported-problems feature itself exists.
4. Each newly wired key SHALL be reflected in the override-target policy table (this document) so the
   supported set is auditable.
5. THE callback URL/header/count limits and `component.callbacks.allowedAddresses` SHALL be read live
   by callback admission in the edge; WHEN no conformance override exists THE three limits SHALL use
   the v1.31.0 defaults and the production allowed-address posture SHALL remain unchanged.

## Requirement 8 — Non-regression and testing

**User story:** As the engine owner, I need proof that production is untouched and the mechanism is
correct, so that this test infrastructure is safe to carry.

**Acceptance criteria:**
1. THE production (no-`conformance`) build SHALL be behaviourally identical at every consult site; a
   property SHALL assert that each consult-site accessor equals its pinned constant when the feature is
   off.
2. A property SHALL assert override lifecycle: after `Set(k, v)` a consult of `k` observes `v`; after
   `Clear(k)` it observes the default; after `Reset()` all keys observe defaults.
3. A property SHALL assert value-type round-trips for each supported value kind (`int64`, `double`,
   `bool`, `string`, `Duration`, structured JSON), and that a type-mismatched `Set` is rejected without
   recording.
4. A property SHALL assert the honesty boundary: a `Set` for an unwired key, a kernel-excluded key, or
   a not-enforced key is `Unsupported` and records nothing.
5. THE public Temporal gRPC surface SHALL be unchanged whether or not an override is active at an
   unwired site (the registry only shadows wired sites).
6. `cargo test --workspace` SHALL be green; the first corpus suite that consumes an override SHALL pass
   two clean consecutive runs; `cargo clippy --workspace --all-targets` and `cargo doc` SHALL be green
   for both feature-on and feature-off builds.
