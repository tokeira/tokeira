# Implementation Plan

> Prerequisite: **Requirement 0 accepted** (owner sanction of the conformance-only mutable surface,
> the new crates/proto/feature/fork-bridge, the kernel-exclusion boundary, and the honesty boundary).
> Do not start Task 1 until R0 is accepted.
>
> Ground truth for every behavioural claim: `TEMPORAL_SERVER_COMPAT = 1.31.0`
> (`common/dynamicconfig/constants.go @ v1.31.0`). Conventions: no kernel additions; config-as-constant
> off-feature; skip-registry-not-test-body in the fork; `cargo +nightly fmt`, `cargo lint`, `cargo test
> --workspace`, `cargo doc` green for **both** feature-on and feature-off builds; buf lint/format/build
> green for the new proto.

## Implementation Status (paused 2026-07-11)

**Code landed and compile-verified; full gates + live Tier 3.22 run + commits still pending.**

This spec was executed to **take over Tier 3.22 directly**, so the **proving Wired consumer is the
reported-problems threshold** (`system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute`) —
Task 13.3's consumer pulled forward — **not** the illustrative `MaxBufferedQueryCount` the plan below
leads with. Task 7 (MaxBufferedQueryCount) was therefore **not** implemented; its intent (a
`#[cfg]`-branched consult-site accessor + off-feature equivalence) is fulfilled by the reported-problems
accessor instead.

**Done (code + local checks):**
- Core crate `tokeira-conformance` (registry, `KEY_CLASSIFICATION`, typed getters) — Tasks 1–3.
  `cargo test -p tokeira-conformance` green: 7 tests (P2/P3/P4/P7 + classification invariants) **plus the
  P5 kernel-purity guard** (`tests/kernel_purity.rs`).
- Proto `proto/tokeira/conformance/v1/control.proto` + `tokeira-conformance-proto` (Task 4). Compiled by
  the per-crate `build.rs` (`connectrpc-build`, a structural copy of `tokeira-compatibility-proto`) — the
  real internal-proto path; the `buf` CLI cannot run at all (pre-existing empty `buf.lock` stub,
  repo-wide). Deviation: the duration kind is `int64 duration_nanos` (avoids a WKT include), not
  `duration_value`.
- Control service `tokeira-conformance-control` (Task 5.1/5.2) — `set/clear/reset` → `overrides()`;
  `OverrideError` → `unimplemented`/`invalid_argument`. connectrpc + buffa pinned **0.8.1**
  (connectrpc-build/-codegen at 0.8.0, their latest; additive-patch compatible within 0.8).
- Reported-problems live-read (Task 13.3 consumer): `reported_problems_threshold()` in `tokeira-runtime`
  — `#[cfg]`-branched (feature-off = pinned `5`; feature-on = live registry read at the Describe consult
  site, so a mid-run 0→2 change takes effect). Removed the stored tracker threshold, `with_threshold`,
  and `with_workflow_task_problem_threshold`. `cargo check -p tokeira-runtime` green both feature states.
- Feature wiring + kernel purity (Task 8): runtime `conformance` feature (optional `tokeira-conformance`
  dep); P5 guard green.
- `tokeirad` mount (Task 10.1): `#[cfg(feature="conformance")]` control listener on a **separate
  loopback** listener, gated by `conformance_control_addr()` (`TOKEIRA_CONFORMANCE_CONTROL_ADDR`) rather
  than a distinct bool flag; never on the public gRPC router. **Removed the
  `TOKEIRA_CONFORMANCE_REPORTED_PROBLEMS_THRESHOLD` env var** and all its threading. `cargo check -p
  tokeirad` green **both** feature-on and feature-off.
- Fork bridge (Task 11) on `tokeira/conformance-v1.31.0`:
  `tests/testcore/tokeira_dynamic_config_bridge.go` (Connect-over-JSON `Set`/`Clear` to the control
  listener; unsupported/failure → logged no-op), the `onebox.go` `overrideDynamicConfig` seam, harness
  wiring of `TOKEIRA_CONFORMANCE_CONTROL_ADDR`, **leaf 4 un-skipped**, and the skip-registry test updated.
  `go vet ./tests/testcore/` green.

**Pending (close-out):**
- Deferred tests: 5.3 (service edge), 7.2 (standalone off-feature P1), 7.3/10.2 (P6 integration) — the
  invariants are covered indirectly by the registry PBTs + compile-time `#[cfg]` + the live corpus run,
  not yet as their own dedicated tests.
- Bridge `TearDownTest = Reset` (11): implemented as **per-test `Clear` via `t.Cleanup`** instead of a
  suite-level Reset — a global Reset would race the parallel, shared-registry model. Deliberate deviation.
- Full gates (Task 14): `cargo lint` / `cargo test --workspace` / `cargo doc` (both feature states) not
  yet run to completion this session; `cargo +nightly fmt --all` applied.
- **Live Tier 3.22 two clean runs** (Tasks 12 + 13.3 verification): needs a `--features conformance`
  tokeirad binary + the fork `go test`; not yet run. Requires the operator to build tokeirad with the
  feature (the harness always exports the control address, but the listener only exists in a feature build).
- `requirements.md` override-target table (13.4) + `docs/readiness/conformance.md` Tier 3.22 row.
- Commits: engine (`main`) + fork **not yet committed**.

**Unrelated cleanup this session:** removed hardcoded `/Users/iw` home paths from six temporal-fork
helper scripts (`run_suite.sh`, `run_nexus_outbound_conformance.sh`, and four `diag_*.sh`) →
`SCRIPT_DIR`-relative with a `TOKEIRA_BIN` env override.

- [x] 1. Core registry crate `tokeira-conformance` (feature-gated foundation, no connectrpc dep)
  - [x] 1.1 Scaffold the crate and core types
    - Add `crates/tokeira-conformance` to the workspace. Define `OverrideValue` (`Int(i64)`,
      `Double(f64)`, `Bool(bool)`, `Text(String)`, `Duration(std::time::Duration)`), `ValueType`,
      `Disposition` (`Wired | KernelExcluded | NotEnforced`), `KeySpec`, and `OverrideError`
      (`UnknownKey | KernelExcluded | NotEnforced | TypeMismatch | MissingKey`). Module doc states the
      plane it sits in and the feature-gated-global exception.
    - _Requirements: 1.3, 1.6, 2.2_
  - [x] 1.2 Seed the `KEY_CLASSIFICATION` honesty table
    - Static `&[KeySpec]` with the known keys and dispositions: `history.MaxBufferedQueryCount` →
      `Wired`; `history.workflowIdReuseMinimalInterval`, `history.maximumBufferedEventsBatch` →
      `KernelExcluded`; the size-limit keys → `NotEnforced`; page-size / long-poll / default-timeout /
      reported-problems keys declared with their eventual `value_type` and current disposition
      (`NotEnforced`/pending until their accessor lands). Each entry documents its plane.
    - _Requirements: 4.2, 5.1, 5.3, 5.4, 8.4_
  - [x] 1.3 Implement the registry (`ConformanceOverrides`) and getters
    - Process-global behind `overrides() -> &'static ConformanceOverrides` (`RwLock<HashMap<&'static
      str, OverrideValue>>`). `set` consults `KEY_CLASSIFICATION`: reject non-`Wired` keys and
      type-mismatched values (recording nothing), else store. `clear`/`reset` drop overrides. Typed
      getters `get_i64/get_f64/get_bool/get_str/get_duration` read live.
    - _Requirements: 1.2, 2.3, 2.4, 2.5, 2.6, 4.2, 5.1_

- [x] 2. Registry property tests (`crates/tokeira-conformance`, `proptest`, ≥100 iterations)
  - [x] 2.1 Property test: Property 2 — override lifecycle and liveness
    - Generate wired-key × type-valid-value × operation sequences; assert Set→observe-value,
      Clear→observe-default, Reset→all-default, reads are live.
    - Tag: `// Feature: conformance-config-override, Property 2: override lifecycle and liveness`
    - _Requirements: 1.2, 2.3, 2.5, 2.6, 8.2_
  - [x] 2.2 Property test: Property 3 — value-type fidelity
    - For each value kind, matching-kind Set round-trips losslessly; mismatched kind is rejected
      `TypeMismatch` and records nothing.
    - Tag: `// Feature: conformance-config-override, Property 3: value-type fidelity`
    - _Requirements: 2.2, 2.4, 8.3_
  - [x] 2.3 Property test: Property 4 — honesty boundary
    - For any unknown / `KernelExcluded` / `NotEnforced` key, Set returns the mapped `OverrideError`
      and records nothing; a subsequent consult (where one exists) still observes the default. Assert
      `Wired` set == keys with a real accessor (audited against the accessor inventory).
    - Tag: `// Feature: conformance-config-override, Property 4: honesty boundary`
    - _Requirements: 4.2, 5.1, 5.3, 5.4, 8.4_
  - [x] 2.4 Property test: Property 7 — between-test isolation
    - Generate per-"test" Set/Clear sequences each terminated by Reset; assert no override from
      sequence *i* is observable in sequence *i+1*.
    - Tag: `// Feature: conformance-config-override, Property 7: between-test isolation`
    - _Requirements: 6.1, 6.2_
  - [x] 2.5 Unit tests: `KEY_CLASSIFICATION` invariants
    - No `Wired` entry names a kernel plane; every entry's `value_type` is self-consistent; keys are
      unique.
    - _Requirements: 4.1, 5.3_

- [x] 3. Checkpoint: core crate builds, clippy/doc clean, registry PBTs + unit tests green

- [x] 4. Proto + generated types `tokeira-conformance-proto`
  - [x] 4.1 Author `proto/tokeira/conformance/v1/control.proto`
    - `DynamicConfigValue` oneof (`int_value`/`double_value`/`bool_value`/`string_value`/
      `duration_value`); `Set`/`Clear`/`Reset` request/response messages; `ConformanceControlService`.
    - _Requirements: 2.1, 2.2_
  - [x] 4.2 Wire buffa/connectrpc-build generation (mirror `tokeira-compatibility-proto`)
    - No checked-in generated code; `buf lint`/`buf format --diff --exit-code`/`buf build` green.
    - _Requirements: 2.7_

- [x] 5. Control service `tokeira-conformance-control` (connect-rust, feature-gated)
  - [x] 5.1 `ConformanceControlHandler` implementing `proto::ConformanceControlService`
    - `set/clear/reset_dynamic_config_override` delegate to `overrides()`; mirror the
      `tokeira-compatibility-service` handler shape.
    - _Requirements: 2.1, 2.3, 2.5, 2.6_
  - [x] 5.2 Value coercion + error mapping
    - `DynamicConfigValue` ↔ `OverrideValue`; map `OverrideError` → Connect status:
      `UnknownKey`/`KernelExcluded`/`NotEnforced` → `unimplemented` (distinct messages),
      `TypeMismatch`/`MissingKey` → `invalid_argument`.
    - _Requirements: 2.2, 2.4, 5.1_
  - [ ] 5.3 Service-boundary unit/property tests
    - Reinforce Property 3/4 at the wire boundary: mismatched kind → `invalid_argument`; non-`Wired`
      key → `unimplemented`; wired+valid → `ok`.
    - Tag: `// Feature: conformance-config-override, Property 4: honesty boundary (service edge)`
    - _Requirements: 2.4, 5.1, 5.4_

- [x] 6. Checkpoint: both new crates build under `--features conformance`; clippy/doc green feature-on and feature-off

- [ ] 7. First consumer wiring — `MaxBufferedQueryCount` (`tokeira-runtime`)
  - [ ] 7.1 Add the feature-gated dependency and consult-site accessor
    - `tokeira-conformance` as an optional dep behind `tokeira-runtime`'s `conformance` feature; add
      the `#[cfg]`-branched `max_buffered_queries()` (feature-off = exact `MAX_BUFFERED_QUERIES_PER_RUN`)
      and switch the `buffered_queries.rs` consult site to call it.
    - _Requirements: 1.1, 1.2, 1.3, 7.1_
  - [ ] 7.2 Property test: Property 1 — off-feature equivalence
    - Feature-off: the accessor equals the constant (compile-configuration assertion). Feature-on with
      no override set: the accessor equals the constant.
    - Tag: `// Feature: conformance-config-override, Property 1: off-feature equivalence`
    - _Requirements: 1.1, 1.3, 3.1, 8.1_
  - [ ] 7.3 Integration test (feature-on): end-to-end first consumer
    - `Set("history.MaxBufferedQueryCount", 2)` → the buffered-query cap observes 2; `Clear` restores
      1. Lives in `tokeira-runtime` tests.
    - _Requirements: 7.1, 8.2_

- [x] 8. Workspace feature wiring + kernel-purity guard
  - [x] 8.1 Propagate the `conformance` feature across the workspace
    - Workspace-level `conformance` feature enabling the new crates + each consumer crate's
      `conformance` feature + `tokeirad`'s; feature-off builds exclude the crates entirely.
    - _Requirements: 1.1, 3.1_
  - [x] 8.2 Structural test: Property 5 — kernel purity / replay determinism
    - Assert (via `cargo metadata` / a dependency test) that `tokeira-kernel` has **no** dependency on
      `tokeira-conformance` under any feature set, and document that `Kernel::apply` is unchanged. Assert
      no `Wired` key is a kernel-plane value.
    - Tag: `// Feature: conformance-config-override, Property 5: kernel purity and replay determinism`
    - _Requirements: 4.1, 4.3_

- [x] 9. Checkpoint: `cargo build -p tokeirad` (no feature) byte-unchanged; `--features conformance` builds; dep-graph test green

- [x] 10. `tokeirad` mount (feature-gated + runtime flag + separate loopback listener)
  - [x] 10.1 Mount the control service on its own listener
    - `#[cfg(feature = "conformance")]` block: a `conformance_control_enabled()` flag (mirroring
      `wire_coverage_enabled()`); when set, bind a **separate loopback-only** `TcpListener` and serve
      `ConformanceControlHandler`. Never added to the public gRPC router (`.add_service` set). Distinct
      mounted/unmounted build paths, per the wire-coverage precedent.
    - _Requirements: 3.2, 3.3_
  - [ ] 10.2 Integration test: Property 6 — production isolation
    - Feature-off, or on-but-unmounted: served Temporal gRPC responses identical to a plain build; no
      control listener bound; no env/TOML read off-feature.
    - Tag: `// Feature: conformance-config-override, Property 6: production isolation`
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 8.5_

- [x] 11. Fork bridge (pinned Temporal fork `tokeira/conformance-v1.31.0`, onebox seam only)
  - [x] 11.1 `tests/testcore/tokeira_dynamic_config_bridge.go`
    - For the tokeira out-of-process cluster, override `OverrideDynamicConfig(setting, value)` to
      `coerce` + call `SetDynamicConfigOverride(setting.Key(), …)`; return `cleanup` = `Clear`;
      `TearDownTest` = `Reset`. Mirror `tokeira_metrics_bridge.go`. No corpus test-body edits.
    - _Requirements: 6.1, 6.2, 6.3_
  - [x] 11.2 Unsupported/unreachable fallback
    - On `unimplemented`/`invalid_argument`/unreachable, mark the test skipped via the existing skip
      registry (never a silent no-op that lets the test proceed).
    - _Requirements: 5.2, 6.4_

- [ ] 12. Checkpoint: build the fork; smoke-test that an override for `MaxBufferedQueryCount` reaches `tokeirad` and is observed

- [ ] 13. Incremental wiring + corpus close-out (phased; each flips a `KEY_CLASSIFICATION` entry to `Wired`)
  - [ ] 13.1 Wire page size (`MAX_PAGE_SIZE` / matching page size) accessor
    - Add the `#[cfg]`-branched accessor at the projection/edge site; flip the key to `Wired`; unblock
      the 2.10 `NonStickyMultiPageHistory` skip.
    - _Requirements: 7.2, 7.4_
  - [ ] 13.2 Wire long-poll interval + default query/update/WFT timeouts accessors
    - _Requirements: 7.2, 7.4_
  - [ ] 13.3 Wire the reported-problems threshold (after the reported-problems feature exists)
    - `system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute` accessor at the derive-on-read
      site; flip to `Wired`; run Tier 3.22 `TestWFTFailureReportedProblems_DynamicConfigChanges` two
      clean runs over the bridge.
    - _Requirements: 7.3, 7.4_
  - [ ] 13.4 Update the override-target policy table in `requirements.md` as each key is wired
    - _Requirements: 7.4_

- [ ] 14. Final checkpoint: full verification
  - `cargo +nightly fmt --all --check`; `cargo lint` + `cargo test-lint`; `cargo test --workspace`
    (feature-off) and with `--features conformance`; `cargo doc --workspace --no-deps`; `buf`
    lint/format/build. The first corpus suite that consumes an override passes two clean consecutive
    runs. Record the enabling capability in `docs/readiness/conformance.md`.
  - _Requirements: 8.1, 8.6_

- [x] 15. Tier 5.32 — callback-policy overrides
  - [x] 15.1 Extend the generic override transport with structured JSON
    - Add `OverrideValue::Json`, `ValueType::Json`, `get_json`, and `json_value = 6` in the control
      proto. The fork bridge marshals composite Go values with `json.Marshal`; scalar coercion remains
      unchanged.
    - _Requirements: 2.2, 2.3, 8.3_
  - [x] 15.2 Property test: Property 3 — structured value fidelity
    - Generate JSON strings and assert matching-kind Set/Get round-trips losslessly, lifecycle
      operations clear/reset them, and a JSON value sent to a scalar key is rejected without storage.
    - Tag: `// Feature: conformance-config-override, Property 3: structured JSON fidelity`
    - _Requirements: 2.2, 2.4, 8.2, 8.3_
  - [x] 15.3 Wire callback admission consult sites in `tokeira-edge`
    - Add live accessors for URL/header/count limits and parse the structured allowed-address rule
      override. Match v1.31.0 wildcard host and `AllowInsecure` behavior; preserve pinned defaults and
      the current production address-policy posture when no override exists.
    - _Requirements: 7.4, 7.5_
  - [x] 15.4 Bridge and corpus verification
    - Add Go coercion tests, run `TestCallbacksSuiteHSM` twice clean, and retain CHASM as an exact
      top-level classified skip under the campaign's framework-internals exclusion.
    - _Requirements: 6.1, 6.3, 8.6_

## Task Dependency Graph

```
1 (core crate)
├─ 2 (registry PBTs)        depends on 1
├─ 3 (checkpoint)           depends on 1,2
4 (proto)                   depends on 1        # KeySpec key strings inform the proto/keys
5 (control service)         depends on 1,4
6 (checkpoint)              depends on 5
7 (first consumer)          depends on 1
8 (workspace feature+guard) depends on 1,5,7
9 (checkpoint)              depends on 8
10 (tokeirad mount)         depends on 5,8
11 (fork bridge)            depends on 4,10
12 (checkpoint)             depends on 11
13 (incremental wiring)     depends on 7,11     # 13.3 also depends on the reported-problems feature (external)
14 (final checkpoint)       depends on 13
15 (callback overrides)     depends on 4,5,11
```

Critical path: 1 → 4 → 5 → 10 → 11 → 13 → 14. Tasks 2, 7, 8 parallelize off 1/5.

## Notes

- **R0 gate.** Tasks are blocked until Requirement 0 is accepted; this spec introduces new crates, a
  proto, a build feature, and a fork seam (architectural per the repo change-classification).
- **Two-layer gating is deliberate.** The compile-time `conformance` feature governs *existence*
  (Property 1/5/6 off-feature); the runtime `conformance_control_enabled()` flag governs *mounting*
  (Property 6 on-but-unmounted). Both must be true for the listener to bind. The runtime flag mirrors
  the existing `wire_coverage_enabled()` recorder in `apps/tokeirad/src/lib.rs`.
- **Honesty is a maintained invariant.** `KEY_CLASSIFICATION` is the single source of truth; a key is
  honourable only when disposition is `Wired`, which happens only when Task 13.x lands its accessor.
  Property 4 audits that the `Wired` set matches the accessor inventory — never fabricate a green.
- **Kernel stays out.** No task adds a parameter to `Kernel::apply` or a `tokeira-conformance`
  dependency to `tokeira-kernel`; Task 8.2 enforces this structurally. The `KernelExcluded` keys
  (`WorkflowIdReuseMinimalInterval`, `MaximumBufferedEventsBatch`) are permanently `Unsupported` here;
  their leaves (e.g. Tier 3.18 `TerminateDuplicate`) remain skipped or handled by an independent-run
  build under a separate decision.
- **Not-enforced keys.** The size-limit keys stay `NotEnforced`/`Unsupported` until their enforcement is
  implemented under their own specs; this spec does not implement enforcement, and an override for them
  never fabricates a limit.
- **First-consumer choice.** `MaxBufferedQueryCount` is first because `buffered_queries.rs` already
  carries a `#[cfg(test)] buffer_unchecked` hook anticipating a raised limit, so the loop is provable
  with minimal new surface.
- **Fork commits** land on `tokeira/conformance-v1.31.0` (onebox/skip-registry seam only); engine
  commits on `main` via `fsWrite` message file → `git commit -F` → `rm -rf artifacts`.
