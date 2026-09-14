# Implementation Plan

Slices F1, F2, F3 are dispatched to Codex as separate worktrees; the orchestrator
reviews each PR against Target_Release before the integration seat merges. Delta specs
(task 5) are authored by the orchestrator and implemented by Codex after approval.

- [ ] 1. F1 — proto resync to `v1.63.5` (branch `agent/claude/t132-proto-sync`, continued by Codex)
  - [ ] 1.1 Create placeholder spec directories
    - `.kiro/specs/v132-standalone-activities/`, `v132-batch-operations-and-workers/`,
      `v132-visibility-query-converter/`, `v132-worker-deployments/`, `v132-nexus/`,
      `v132-lifecycle-fidelity/`, `v132-gated-surfaces/`, each with a `.placeholder.md`
      carrying the owner, the scope line from Requirement 9.1, and the corpus anchors.
    - _Requirements: 2.2, 9.1_
  - [ ] 1.2 Atomic resync commit
    - `cargo run -p proto-sync -- v1.63.5`; restore
      `proto/upstream/temporal/server/api/adminservice/v1/service.proto` from `HEAD`;
      refresh `proto/upstream/temporalproto/openapi/` from `temporalio/api` at
      `v1.63.5` with the README SHA table; `cargo run -p proto-sync -- generate`;
      `-- check` clean. One commit, no other changes. (Prepared by the orchestrator on
      the branch.)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  - [ ] 1.3 Compile-drift fix
    - Type-path moves for `TimeSkippingConfig`; `#[allow(deprecated)]` with a D2 comment
      on `poller_group_infos` and `StartBatchOperationRequest.executions`; new enum arms
      that preserve today's behaviour; `cargo check --workspace --locked` green.
    - _Requirements: 1.6, 5.1, 5.2_
  - [ ] 1.4 Deferred stubs for the six RPCs
    - Bracketed blocks in `crates/tokeira-edge/src/grpc/workflow_service.rs` per owning
      delta spec; `debug!` only.
    - _Requirements: 3.1, 3.2_
  - [ ] 1.5 Capability literals
    - Eight `NamespaceCapabilities` booleans, `SystemCapabilities.server_scaled_provider_cloud_run`,
      `NamespaceInfo.Limits.workflow_task_completion_size_limit_error = 0`, written
      verbatim at every construction site.
    - _Requirements: 4.1, 4.2_
  - [ ] 1.6 Response defaults and inventory
    - Emit protobuf defaults for every row in the audit's response table;
      `UNSUPPORTED_FIELDS.md` rows for every dropped request field;
      `binding-inventory.json` regenerated; `public.rs` OpenAPI comments at `v1.63.5`.
    - _Requirements: 5.3, 5.4, 5.5, 1.8_
  - [ ] 1.7 Pins
    - `TEMPORAL_PROTO_VERSION = "v1.63.5"`; rewrite the tracked-ahead note; claim
      unchanged.
    - _Requirements: 1.7, 1.9_
  - [ ] 1.8 Property test: Property 1 — proto pin parity
    - In `tokeira-build-info`; skips with a documented reason when
      `proto/UPSTREAM_VERSION` is absent (published crate).
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 1: proto pin parity`
    - _Requirements: 1.2, 1.7_
  - [ ] 1.9 Feature matrix ownership
    - Six RPCs owned once with `FeatureState::Stubbed` and delta-spec evidence;
      `matrix_classifies_every_upstream_rpc` and `every_rpc_is_owned_once` green.
    - _Requirements: 3.3, 3.5_
  - [ ] 1.10 Property test: Property 6 — deferred stubs answer uniformly
    - Extend `crates/tokeira-edge/tests/grpc_deferred_handlers.rs` to the six RPCs,
      ≥100 generated request payloads each.
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 6: deferred stubs answer uniformly`
    - _Requirements: 3.1, 3.2_
  - [ ] 1.11 Property test: Property 7 — capability literals match the policy table
    - Golden over `DescribeNamespace` and `GetSystemInfo` translation plus a source scan
      rejecting `..Default::default()` on capability structs.
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 7: capability literals match the policy table`
    - _Requirements: 4.1, 4.2_
  - [ ] 1.12 Property tests: Property 3 and Property 4 — audit structure
    - Extend `crates/tokeira-edge/tests/surface_audit_structure.rs` to parse
      `.kiro/specs/temporal-v1.32-compatibility/design.md` with the same table grammar;
      enforce deferred-owner directories and kernel-free wire-through rows.
    - Tags: `// Feature: temporal-v1.32-compatibility, Property 3: every deferred surface has an owner directory`
      and `… Property 4: wire-through rows are kernel-free`
    - _Requirements: 2.2, 2.3, 2.5_
  - [ ] 1.13 Property 9 — reproducible bindings
    - `tools/proto-sync/tests/reproducible.rs` green at the resync commit (existing test).
    - _Requirements: 1.4_
  - [ ] 1.14 Checkpoint: §10.4 bar green on a devbox; amend the Surface_Audit for any
    row the resynced tree contradicted, in the same commit as the fix.
    - _Requirements: 1.6, 2.4_

- [ ] 2. F3 (engine side) — target pin and gate (branch `agent/codex/t132-target-pin`)
  - [ ] 2.1 Add `TEMPORAL_SERVER_TARGET = "1.32.0"` with its doc comment; expose through
    `build.rs` and `lib.rs`.
    - _Requirements: 8.1, 8.2, 8.3_
  - [ ] 2.2 Pin gate compares against `v{TEMPORAL_SERVER_TARGET}`; messages name the
    constant.
    - _Requirements: 7.6_
  - [ ] 2.3 Confirm the `BumpTrailer` probe ignores the target constant; add a probe test.
    - _Requirements: 8.4_
  - [ ] 2.4 Property test: Property 2 — target pin never trails the claim
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 2: target pin never trails the claim`
    - _Requirements: 8.2, 8.3, 8.5_
  - [ ] 2.5 Checkpoint: bar green.

- [ ] 3. F2 — configuration denominator at `v1.32.0` (branch `agent/codex/t132-config-denominator`)
  - [ ] 3.1 Run the extractor at tag `v1.32.0`; commit
    `crates/tokeira-compatibility/data/temporal-v1.32.0-settings.json`; assert the
    declarations outside `constants.go` are present.
    - _Requirements: 6.1, 6.2_
  - [ ] 3.2 Author `temporal-v1.32.0-classification.json`
    - Carry forward unchanged keys with re-verified anchors; disposition for every added
      key; change notes for removed and renamed keys; both defaults and the owning delta
      spec for every default flip (Requirement 12).
    - _Requirements: 6.3, 6.4, 6.5, 12.1–12.5_
  - [ ] 3.3 Switch `configuration.rs` to the `v1.32.0` files; delete the `v1.31.0` files;
    keep the conformance cross-check green.
    - _Requirements: 6.6_
  - [ ] 3.4 Render `docs/conformance/v1.32.0/temporal-configuration.md` via
    `tools/compatibility-docs`, labelled as the target-pin denominator.
    - _Requirements: 6.6_
  - [ ] 3.5 Property test: Property 8 — denominator exactness at `v1.32.0`
    - Re-point the existing `configuration-policy` Property 3 and Property 4 tests.
    - Tag: `// Feature: temporal-v1.32-compatibility, Property 8: denominator exactness at v1.32.0`
    - _Requirements: 6.1, 6.3, 6.7_
  - [ ] 3.6 Checkpoint: bar green.

- [ ] 4. F3 (fork side) — `tokeira/conformance-v1.32.0` and the baseline
  - [ ] 4.1 Branch from tag `v1.32.0`; re-apply the Harness_Shim and fork tooling.
    - _Requirements: 7.1_
  - [ ] 4.2 Port the shim across the `tests/testcore` delta (`WithTimeout`,
    `overrideDynamicConfig` split, `dedicatedClusterGuard`, new files); `go vet` and
    the shim's own tests green.
    - _Requirements: 7.2_
  - [ ] 4.3 Pin the toolchain to `go 1.26.8`.
    - _Requirements: 7.4_
  - [ ] 4.4 Re-verify every skip-registry entry's reason at `v1.32.0`; drop entries whose
    reason no longer holds.
    - _Requirements: 7.3_
  - [ ] 4.5 Baseline run of the whole corpus against unchanged `tokeirad`; write
    `reference/FINDINGS-v1.32.0.md` in the row shape of the design.
    - _Requirements: 7.5_
  - [ ] 4.6 Assign every regression and new suite to a delta spec; raise anything that
    fits none.
    - _Requirements: 7.5, 9.4_

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
