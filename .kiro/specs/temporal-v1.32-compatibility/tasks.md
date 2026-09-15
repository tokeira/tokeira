# Implementation Plan

Slices F1, F2, F3 are dispatched to Codex as separate worktrees; the orchestrator
reviews each PR against Target_Release before the integration seat merges. Delta specs
(task 5) are authored by the orchestrator and implemented by Codex after approval.

- [x] 1. F1 — proto resync to `v1.63.5` (branch `agent/claude/t132-proto-sync`, continued by Codex)
  - [x] 1.1 Create placeholder spec directories
    - `.kiro/specs/v132-standalone-activities/`, `v132-batch-operations-and-workers/`,
      `v132-visibility-query-converter/`, `v132-worker-deployments/`, `v132-nexus/`,
      `v132-lifecycle-fidelity/`, `v132-gated-surfaces/`, each with a `.placeholder.md`
      carrying the owner, the scope line from Requirement 9.1, and the corpus anchors.
    - _Requirements: 2.2, 9.1_
  - [x] 1.2 Atomic resync commit
    - `cargo run -p proto-sync -- v1.63.5`; restore
      `proto/upstream/temporal/server/api/adminservice/v1/service.proto` from `HEAD`;
      refresh `proto/upstream/temporalproto/openapi/` from `temporalio/api` at
      `v1.63.5` with the README SHA table; `cargo run -p proto-sync -- generate`;
      `-- check` clean. One commit, no other changes. (Prepared by the orchestrator on
      the branch.)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  - [x] 1.3 Compile-drift fix
    - Type-path moves for `TimeSkippingConfig`; `#[allow(deprecated)]` with a D2 comment
      on `poller_group_infos` and `StartBatchOperationRequest.executions`; new enum arms
      that preserve today's behaviour; `cargo check --workspace --locked` green.
    - _Requirements: 1.6, 5.1, 5.2_
  - [x] 1.4 Deferred stubs for the six RPCs
    - Bracketed blocks in `crates/tokeira-edge/src/grpc/workflow_service.rs` per owning
      delta spec; `debug!` only.
    - _Requirements: 3.1, 3.2_
  - [x] 1.5 Capability literals
    - Eight `NamespaceCapabilities` booleans, `SystemCapabilities.server_scaled_provider_cloud_run`,
      `NamespaceInfo.Limits.workflow_task_completion_size_limit_error = 0`, written
      verbatim at every construction site.
    - _Requirements: 4.1, 4.2_
  - [x] 1.6 Response defaults and inventory
    - Emit protobuf defaults for every row in the audit's response table;
      `UNSUPPORTED_FIELDS.md` rows for every dropped request field;
      `binding-inventory.json` regenerated; `public.rs` OpenAPI comments at `v1.63.5`.
    - _Requirements: 5.3, 5.4, 5.5, 1.8_
  - [x] 1.7 Pins
    - `TEMPORAL_PROTO_VERSION = "v1.63.5"`; rewrite the tracked-ahead note; claim
      unchanged.
    - _Requirements: 1.7, 1.9_
  - [x] 1.8 Property test: Property 1 — proto pin parity
    - In `tokeira-build-info`; skips with a documented reason when
      `proto/UPSTREAM_VERSION` is absent (published crate).
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 1: proto pin parity`
    - _Requirements: 1.2, 1.7_
  - [x] 1.9 Feature matrix ownership
    - Six RPCs owned once with `FeatureState::Stubbed` and delta-spec evidence;
      `matrix_classifies_every_upstream_rpc` and `every_rpc_is_owned_once` green.
    - _Requirements: 3.3, 3.5_
  - [x] 1.10 Property test: Property 6 — deferred stubs answer uniformly
    - Extend `crates/tokeira-edge/tests/grpc_deferred_handlers.rs` to the six RPCs,
      ≥100 generated request payloads each.
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 6: deferred stubs answer uniformly`
    - _Requirements: 3.1, 3.2_
  - [x] 1.11 Property test: Property 7 — capability literals match the policy table
    - Golden over `DescribeNamespace` and `GetSystemInfo` translation plus a source scan
      rejecting `..Default::default()` on capability structs.
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 7: capability literals match the policy table`
    - _Requirements: 4.1, 4.2_
  - [x] 1.12 Property tests: Property 3 and Property 4 — audit structure
    - Extend `crates/tokeira-edge/tests/surface_audit_structure.rs` to parse
      `.kiro/specs/temporal-v1.32-compatibility/design.md` with the same table grammar;
      enforce deferred-owner directories and kernel-free wire-through rows.
    - Tags: `// Feature: temporal-v1.32-compatibility, Property 3: every deferred surface has an owner directory`
      and `… Property 4: wire-through rows are kernel-free`
    - _Requirements: 2.2, 2.3, 2.5_
  - [x] 1.13 Property 9 — reproducible bindings
    - `tools/proto-sync/tests/reproducible.rs` green at the resync commit (existing test).
    - _Requirements: 1.4_
  - [x] 1.14 Checkpoint: §10.4 bar green on a devbox; amend the Surface_Audit for any
    row the resynced tree contradicted, in the same commit as the fix.
    - Local macOS validation passed on 2026-09-14 for `64ad90d4`: all six §10.4
      commands, 3,365 nextest tests passed (2 existing skips), doctests passed
      (20 existing ignored examples), and rustdoc passed with warnings denied.
      Proto reproducibility checks also passed. Audit amendments landed in that
      commit; the prepared atomic resync is `24cbf23a`.
    - Devbox validation (Linux x86_64) passed on 2026-09-14 for `819da6d3`: fmt
      with CI's `nightly-2026-06-16`, lint, workspace check, all 3,365 nextest tests
      (2 existing skips), doctests (1 passed, 20 existing ignored examples), and
      rustdoc with warnings denied. The complete test run used `--no-fail-fast`.
    - The initial nextest run failed the unchanged runtime test
      `backlog::tests::property_drain_routes_entries_to_the_correct_broker`
      (`crates/tokeira-runtime/src/backlog.rs`, 50 ms poll deadline), leaving 941
      tests unrun. Its saved case (`logical_seq = 1`, `attempt = 3`) passed both
      the focused replay and the subsequent complete workspace run. The failing
      seed was `eeac8264b343600b1a2378972d6bb22d2fac5aed1a24df3968cab5df9bd9bcdf`;
      the remote regression file retained it for replay. F1 makes no runtime
      change; the intermittent failure remains a follow-up risk.
    - _Requirements: 1.6, 2.4_

- [x] 2. F3 (engine side) — target pin and gate (branch `agent/codex/t132-target-pin`)
  - [x] 2.1 Add `TEMPORAL_SERVER_TARGET = "1.32.0"` with its doc comment; expose through
    `build.rs` and `lib.rs`.
    - _Requirements: 8.1, 8.2, 8.3_
  - [x] 2.2 Pin gate compares against `v{TEMPORAL_SERVER_TARGET}`; messages name the
    constant.
    - _Requirements: 7.6_
  - [x] 2.3 Confirm the `BumpTrailer` probe ignores the target constant; add a probe test.
    - _Requirements: 8.4_
  - [x] 2.4 Property test: Property 2 — target pin never trails the claim
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 2: target pin never trails the claim`
    - _Requirements: 8.2, 8.3, 8.5_
  - [x] 2.5 Checkpoint: bar green.
    - Local macOS validation passed on 2026-09-14 for `f6305ebc`, rebased onto
      campaign head `41b3ede0` (merged F1): all six §10.4 commands, 3,370 nextest
      tests passed (2 existing skips), doctests (2 passed, 20 existing ignored
      examples), and rustdoc with warnings denied. The complete nextest rerun
      used `--no-fail-fast`.
    - The first rebased nextest run passed 3,369 tests and timed out the unchanged
      `backlog::tests::property_drain_routes_entries_to_the_correct_broker`
      (`crates/tokeira-runtime/src/backlog.rs`) at 180 seconds. The focused replay
      passed in 0.184 seconds; the subsequent full run passed it in 0.231 seconds.
      F1 already records this test as intermittent; F3 changes no runtime code,
      and that test remains a follow-up risk.
    - Dependency bans/licenses/sources passed. Offline source links passed with
      the gitignored `.tokeira-build/` output excluded: the unfiltered check found
      a missing documentation file in a generated scoped-workspace README.

- [x] 3. F2 — configuration denominator at `v1.32.0` (branch `agent/codex/t132-config-denominator`)
  - [x] 3.1 Run the extractor at tag `v1.32.0`; commit
    `crates/tokeira-compatibility/data/temporal-v1.32.0-settings.json`; assert the
    declarations outside `constants.go` are present.
    - _Requirements: 6.1, 6.2_
  - [x] 3.2 Author `temporal-v1.32.0-classification.json`
    - Carry forward unchanged keys with re-verified anchors; disposition for every added
      key; change notes for removed and renamed keys; both defaults and the owning delta
      spec for every default flip (Requirement 12).
    - _Requirements: 6.3, 6.4, 6.5, 12.1–12.5_
  - [x] 3.3 Switch `configuration.rs` to the `v1.32.0` files; delete the `v1.31.0` files;
    keep the conformance cross-check green.
    - _Requirements: 6.6_
  - [x] 3.4 Render `docs/conformance/v1.32.0/temporal-configuration.md` via
    `tools/compatibility-docs`, labelled as the target-pin denominator.
    - `write-temporal` and `check-temporal` scope generation to this inventory,
      preserving the existing v1.31.0 operator reference and configuration example.
    - _Requirements: 6.6_
  - [x] 3.5 Property test: Property 8 — denominator exactness at `v1.32.0`
    - Re-point the existing `configuration-policy` Property 3 and Property 4 tests.
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 8: denominator exactness at v1.32.0`
    - _Requirements: 6.1, 6.3, 6.7_
  - [x] 3.6 Checkpoint: bar green.
    - Source audit at `v1.32.0`: 683 declarations (627 in `constants.go`, 56
      elsewhere), 84 added keys, 14 retired keys, seven rename/consolidation
      targets, and 12 changed default expressions. Extraction of both `v1.31.0`
      and `v1.32.0` reproduced their checked snapshots byte for byte; the
      unchanged extractor's Go tests passed.
    - Linux x86_64 validation passed on 2026-09-15: all six §10.4 commands,
      using CI's `nightly-2026-06-16` formatter and nextest `--no-fail-fast`.
      All 3,371 tests passed (2 existing skips), including the previously
      intermittent backlog drain property. The generated-provisioner test
      passed in 174 seconds under its existing longer timeout. Doctests and
      rustdoc with warnings denied passed; `check-temporal` passed. The Linux
      code, data, and generated inventory matched the final local files by SHA-256.
    - Local validation passed: nightly formatting, workspace check, all 49
      focused compatibility/documentation tests, scoped document generation and
      drift check, cargo-deny bans/licenses/sources, and source-tree offline links
      (excluding generated `.tokeira-build` output). The six-command completion
      bar ran on Linux; it was not duplicated in full on macOS.
    - The v1.31.0 claim, runtime defaults, and existing v1.31.0 documents remain
      unchanged. The Nexus and worker-deployment deltas own migration of the two
      retired keys with live conformance overrides; the ledger verifies those
      explicitly without counting them as v1.32.0 declarations.

- [x] 4. F3 (fork side) — `tokeira/conformance-v1.32.0` and the baseline
  - [x] 4.1 Branch from tag `v1.32.0`; re-apply the Harness_Shim and fork tooling.
    - _Requirements: 7.1_
  - [x] 4.2 Port the shim across the `tests/testcore` delta (`WithTimeout`,
    `overrideDynamicConfig` split, `dedicatedClusterGuard`, new files); `go vet` and
    the shim's own tests green.
    - _Requirements: 7.2_
  - [x] 4.3 Pin the toolchain to `go 1.26.8`.
    - _Requirements: 7.4_
  - [x] 4.4 Re-verify every skip-registry entry's reason at `v1.32.0`; drop entries whose
    reason no longer holds.
    - _Requirements: 7.3_
  - [x] 4.5 Baseline run of the whole corpus against unchanged `tokeirad`; write
    `reference/FINDINGS-v1.32.0.md` in the row shape of the design.
    - _Requirements: 7.5_
  - [x] 4.6 Assign every regression and new suite to a delta spec; raise anything that
    fits none.
    - _Requirements: 7.5, 9.4_
    - [Recorded baseline](reference/FINDINGS-v1.32.0.md): all 138 flat-package
      entrypoints attempted on 2026-09-15, each against a fresh engine at
      `f78b3412ff1f55edc109b162e9cecef0c7c82de4`. The 137 upstream entrypoints
      produced 887 pass, 1,382 fail, 147 skip and 227 unfinished outcomes.
      All 26 regressions and 45 newly measured suites have owners; five public
      scope allocations and two HTTP gate decisions are raised in the findings.
    - Fork head: `4d71235efae67869a5c2e75fa2836765cf9fd347`; the complete capture
      used its parent `ea5246188aaae5e5f79e0c885e16279959285273`. The follow-up
      repairs only the fork-owned smoke test's authorization initialization;
      standalone and runner-owned startup both passed separately. Captured
      baseline outcomes remain unchanged.
    - All 118 inherited exclusions were audited: 99 retained, 12 removed,
      six renamed and one narrowed, yielding 112 active exact identities.
      Go shim/tool tests passed on macOS and Linux; vet and the full fork lint
      passed. All six Rust completion commands passed on macOS (3,371 nextest
      passes, two existing skips), alongside dependency and offline-link checks.
      No engine behavior or dependency changed; the claim remains 1.31.0.

- [ ] 5. Delta specs (orchestrator authors; integration seat approves; Codex implements)
  - [ ] 5.1 `v132-standalone-activities` (D1) — spec PR, approval, implementation.
    - _Requirements: 9.1, 9.2, 9.3, 12.1_
  - [ ] 5.2 `v132-batch-operations-and-workers` (D2)
    - _Requirements: 9.1, 9.2, 9.3_
  - [ ] 5.3 `v132-visibility-query-converter` (D3)
    - _Requirements: 9.1, 9.2, 9.3, 12.3_
  - [ ] 5.4 `v132-worker-deployments` (D4)
    - _Requirements: 9.1, 9.2, 9.3, 12.4_
  - [ ] 5.5 `v132-nexus` (D5)
    - _Requirements: 9.1, 9.2, 9.3_
  - [ ] 5.6 `v132-lifecycle-fidelity` (D6)
    - _Requirements: 9.1, 9.2, 9.3, 12.2_
  - [ ] 5.7 `v132-gated-surfaces` (D7)
    - _Requirements: 9.1, 9.2, 9.3, 12.5_

- [ ] 6. Corpus drive-to-green on `tokeira/conformance-v1.32.0`
  - [ ] 6.1 Append the new suites to `docs/readiness/functional-test-order.md` as
    tiers 11 onward, each naming its delta spec.
    - _Requirements: 10.2_
  - [ ] 6.2 Regression sweep of tiers 1.1–10.45 as delta implementations land; three
    runs each; Ledger rows updated.
    - _Requirements: 10.1, 10.3, 10.4_
  - [ ] 6.3 New tiers driven clean; Ledger rows added.
    - _Requirements: 10.1, 10.3, 10.4_

- [ ] 7. Claim flip
  - [ ] 7.1 `TEMPORAL_SERVER_COMPAT = "1.32.0"` with the `Server-Compat-Bump:` trailer;
    PR body carries the Upstream Releases table (`v1.31.1`, `v1.31.2`, `v1.32.0`), the
    matrix delta, and the disposition table.
    - _Requirements: 11.1, 11.2_
  - [ ] 7.2 `FeatureOrigin::TemporalV1_32`; re-verify or re-cite every `@ v1.31.0`
    evidence reference; refresh digests.
    - _Requirements: 11.3_
  - [ ] 7.3 `docs/conformance/v1.32.0/` five files; remove `docs/conformance/v1.31.0/`.
    - _Requirements: 11.4, 12.6_
  - [ ] 7.4 Update every document in Requirement 11.5 and the two SVG diagrams.
    - _Requirements: 11.5_
  - [ ] 7.5 Re-verify the `deployment_registry.rs` behaviour citations at `v1.32.0`.
    - _Requirements: 11.6_
  - [ ] 7.6 Re-target the Ledger and the tracker header (123 RPCs at `v1.63.5`).
    - _Requirements: 11.5_
  - [ ] 7.7 Checkpoint: monotonicity probes and Property 2 (target equals claim) green;
    bar green.
    - _Requirements: 11.7, 8.5_

## Task Dependency Graph

```json
{
  "1.1": [], "1.2": [], "1.3": ["1.2"], "1.4": ["1.3"], "1.5": ["1.3"], "1.6": ["1.3"],
  "1.7": ["1.3"], "1.8": ["1.7"], "1.9": ["1.4"], "1.10": ["1.4"], "1.11": ["1.5"],
  "1.12": ["1.1"], "1.13": ["1.2"], "1.14": ["1.8", "1.9", "1.10", "1.11", "1.12", "1.13"],
  "2.1": [], "2.2": ["2.1"], "2.3": ["2.1"], "2.4": ["2.1"], "2.5": ["2.2", "2.3", "2.4"],
  "3.1": [], "3.2": ["3.1"], "3.3": ["3.2"], "3.4": ["3.3"], "3.5": ["3.3"], "3.6": ["3.4", "3.5"],
  "4.1": [], "4.2": ["4.1"], "4.3": ["4.1"], "4.4": ["4.2"], "4.5": ["4.2", "4.3", "4.4", "2.5"], "4.6": ["4.5"],
  "5.1": ["1.14", "4.6"], "5.2": ["1.14", "4.6"], "5.3": ["1.14", "4.6"], "5.4": ["1.14", "4.6"],
  "5.5": ["1.14", "4.6"], "5.6": ["1.14", "4.6"], "5.7": ["1.14", "4.6"],
  "6.1": ["4.6"], "6.2": ["5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7"], "6.3": ["6.1", "6.2"],
  "7.1": ["6.3", "3.6"], "7.2": ["7.1"], "7.3": ["7.1"], "7.4": ["7.1"], "7.5": ["7.1"], "7.6": ["7.1"],
  "7.7": ["7.2", "7.3", "7.4", "7.5", "7.6"]
}
```

## Notes

- Every task lands on the campaign branch `compat/temporal-1.32`; every PR targets it.
  The integration seat merges `origin/main` into the branch to keep it current and
  merges the branch into `main` once, after task 7.7 (Requirement 11.8).
- Tasks 1–3 are disjoint by file set and run in parallel worktrees. Task 4's fork work
  is independent of the engine repo except for 2.x (the gate must accept the target
  tag before a run is attempted).
- The orchestrator prepared task 1.2 on `agent/claude/t132-proto-sync`; Codex continues
  the branch for 1.3–1.14 (declared dependency per `AGENTS.md` §10.2).
- Delta-spec placeholders are created in F1 so Property 3 holds from the first green
  commit; the full specs replace them as they are approved.
- Recorded tool gap for `proto-upstream-sync`: a sync wipes the Tokeira-owned
  AdminService proto and the OpenAPI documents; the restore steps in 1.2 are the
  interim procedure.
- The `api-go` sibling checkout is not part of this campaign.
