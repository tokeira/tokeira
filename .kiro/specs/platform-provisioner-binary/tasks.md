# Implementation Plan: Platform Provisioner Binary

## Overview

Implement the minimal foundational set: provenance stamping, the binding/mismatch gate, the integrity
manifest with checksum verification, the upgrade/migration boundary (registry may start empty),
snapshot-based upgrade rollback (with the IaC framework's Delete path hardened to fail closed as its
prerequisite), optional binary retention for S3 remote state, the specialized `tkp` lifecycle binary, the
`tkr` launcher that resolves and checksum-verifies the bound `tkp`, and the durable deployment lock.
Heavier mechanisms (automated **binary self-update**, signing infrastructure, the one-binary-vs-SDK
multi-consumer decision) are out of scope.

## Tasks

- [ ] 1. Data models in `tokeira-state`
  - [x] 1.1 Add `ProvenanceStamp`, `BuildMode`, `Target`, `BinaryArtifactDescriptor`, `IntegrityManifest`;
        `ProvenanceStamp` carries `version` + `git_sha` + `source_tree_hash` + `build_mode` and distinguishes
        `Unknown` from a concrete version. DONE via task 13.1 — defined in the new `crates/tokeira-provisioner`
        (not `tokeira-state`'s manifest, to keep generic state free of provisioner concepts); `Unknown` is
        `binding: Option<ProvenanceStamp> = None`, never coerced. Round-trips (Property 1).
  - [ ] 1.2 Wire them into the existing manifest write/read path, preserving the distinct state-format
        `schema_version`. Property: provenance round-trips (Property 1). *Envelope wiring is follow-on (see
        13.1 note): the envelope is defined + storable; persisting it into the deployment store is pending
        tasks 8/12.*

- [x] 2. Provenance stamping
  - [x] 2.1 Stamp the running provisioner from `tokeira-build-info` (`TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`,
        `SOURCE_TREE_HASH`, `BUILD_MODE`); `source_tree_hash` is the authoritative drift key (Property 6).
        DONE — `ProvenanceStamp::current(recorded_at)` reads build-info; `BuildMode::from_build_info` maps
        the `BUILD_MODE` string (unknown → advisory `Dev`). Tests `current_stamp_reads_build_info`,
        `build_mode_parses_build_info_strings`. *The "on every state-document write" wiring is follow-on
        with the envelope persistence (tasks 8/12).*
  - [x] 2.2 On remote-state init, write the stamp before any resource create — Day-0 mandatory versioning;
        there is no create path that leaves state unstamped (Req 1.2). DONE — `tkp init` (`tkp/src/init.rs`)
        writes the Day-0 envelope (binding = the running `ProvenanceStamp::current`, integrity manifest,
        `config_revision` 0, `deployment_id` from config) via a CAS create; refuses an already-initialized
        deployment. This closes the bootstrap: every applying verb previously refused an unstamped
        deployment (`Unknown`), and `init` is the entry point that stamps it (invoked by `tkr deployment
        create`, task 9.2). Tests `init_stamps_the_envelope_with_binding_and_integrity`,
        `init_refuses_an_already_initialized_deployment`; end-to-end init → apply → describe smoke-verified.

- [ ] 3. Binding gate (regime by build mode)
  - [x] 3.1 `check_binding(recorded, running) -> Match | DevIterate | Mismatch | Downgrade | ModeRegression
        | Unknown`; computed for every binding-aware verb (plan surfaces it, applying verbs gate on it).
        DONE — in `crates/tokeira-provisioner` (natural home now that `ProvenanceStamp` lives there, rather
        than orchestrator/iac). `source_tree_hash` is the authoritative key; version ordering only
        distinguishes `Downgrade` from `Mismatch` (both refuse). 9 tests covering every regime.
  - [x] 3.2 Versioned deployment: proceed on `Match`; refuse `Mismatch`/`Downgrade`/`Unknown`; refuse
        `ModeRegression`. Properties 2, 5. DONE (verdict semantics) — `BindingVerdict::proceeds()` returns
        true only for `Match`/`DevIterate`; `is_authoritative()` only for `Match`. *Gating these verdicts
        inside the applying verbs is task 8.3 (needs `tkp`).*
  - [x] 3.3 Dev deployment + dev binary → `DevIterate`: permissive, re-stamp advisory dev stamp, warn; a
        `dev` stamp never yields authoritative `Match`. Property 7. DONE (verdict) — `DevIterate` proceeds
        but is not authoritative. *The re-stamp + warn happen in the verb wiring (task 8.3, `tkp`).*

- [x] 4. Integrity manifest + verification
  - [x] 4.1 Record version + per-target `sha256` (+ optional `retrieval_ref`) in the CAS-guarded manifest
        at stamp time. DONE — `tkp/src/init.rs::running_integrity_manifest` reads `current_exe`, SHA-256s
        it, and records a `BinaryArtifactDescriptor { version, target, sha256, size_bytes }` (target triple
        captured via `apps/tkp/build.rs` → `env!("TKP_TARGET")`) in the envelope's `IntegrityManifest` at
        `init` (stamp) time. Verified by the `init` test (integrity present, non-empty sha256).
  - [x] 4.2 Verify a retrieved binary's checksum against the manifest before execution; abort on
        mismatch. Property 3. DONE — `IntegrityManifest::verify_artifact(bytes, target)` (and
        `BinaryArtifactDescriptor::verify(bytes)`) compute the SHA-256 and abort with
        `IntegrityError::ChecksumMismatch`, or `TargetNotFound` when the manifest has no entry for the
        target. `descriptor_for(target)` looks up by target; `sha256_hex` exported. Tests:
        `matching_bytes_verify`, `tampered_bytes_abort`, `unknown_target_is_not_found`,
        `sha256_hex_is_stable_and_lowercase`. (The launcher wiring — verify before exec — is `tkr`, task 9.)

- [ ] 5. Upgrade/migration boundary
  - [ ] 5.1 Add the `MigrationRegistry` keyed by **state-schema transition** (`from_schema → to_schema`)
        and the version-transition entry point; run forward migration before mutation on upgrade only when
        the schema changes (a new `source_tree_hash` at the same schema is a re-stamp). Forward-only.
  - [~] 5.2 Refuse downgrade (ordering by monotonic version/build id, never by hash); refuse a same-semver
        /different-hash apply; refuse a missing migration for a required schema transition. Property 4.
        DONE (version/mode refusals) — `tokeira_provisioner::evaluate_upgrade` refuses downgrade,
        same-semver/different-hash, and re-stamp-to-dev (via a shared numeric `version::compare_versions`,
        never by hash); allows a versioned advance or a dev→versioned promotion. 6 tests. *The
        missing-migration refusal is pending the `MigrationRegistry` (5.1).*
  - [x] 5.3 On upgrade, the first act is one atomic commit: flip the binding to B, capture the [A final]
        `RollbackCheckpoint` (prior snapshot + stamp + **full integrity manifest** + config ref), and open
        the operation marker — *before* any provider mutation (Req 4.5, 9.1). DONE.
        `DeploymentStateEnvelope::begin_upgrade(to, operation_id, recorded_at)` captures [A final]
        (from_provenance = A / from_integrity / from_infra_head + from_runtime_head / from_config_ref), flips
        the binding to B, and opens the `UpgradeInFlight` marker — mutating the envelope for the caller to
        persist in one CAS commit; `close_operation` clears the marker (keeping the flipped binding +
        checkpoint). Test `begin_upgrade_captures_checkpoint_and_flips_binding`.

- [ ] 6. S3 binary retention (optional path)
  - [ ] 6.1 In the S3 state store, optionally persist the binary blob keyed by `version`+`target`
        alongside state documents.
  - [ ] 6.2 Retrieve + checksum-verify before execution (reuses 4.2). Property 3 (5.3).

- [ ] 7. Measure and record the optimized binary size
  - [ ] 7.1 `cargo build --release` the provisioner; record the stripped size and the linked AWS SDK
        client set; note the trim/`opt-level`/UPX levers in the design's size section.

- [ ] 8. Provisioner binary + specialized CLI (`apps/tkp`, binary `tkp`) (Requirements 6, 7)
  - [~] 8.1 Create the `apps/tkp` binary crate + its own clap surface scoped to the deployment lifecycle
        (no operator/global verbs). DONE (scaffold): `apps/tkp` created with a clap surface —
        `describe` (implemented) plus `apply`/`upgrade`/`rollback`/`resume` stubs that report
        "not yet implemented". Links `tokeira-provisioner` + `tokeira-state` (what `describe` needs); the
        full engine/platform/`tokeira-aws` linkage lands with the applying verbs (8.3+). A minimal
        deployment-level **envelope store** (`Box<dyn DeploymentStore<DeploymentStateEnvelope>>`, a local
        `CasStore` under `{dir}/state/envelope`) is the first slice of the 13.1 envelope wiring; cloud will
        select `S3StateStore` via the platform seam.
  - [x] 8.2 `describe`: read-only report of identity, recorded provenance (or `unknown`), binding
        verdict, integrity manifest, and state facts; human + `--json`; never gates (Req 6.1, 6.5). DONE.
        Reports the running provisioner (`ProvenanceStamp::current` from build-info), the deployment
        envelope (id / schema / config_revision), the recorded binding (or `unstamped (Unknown)`), the
        `check_binding` verdict + whether it proceeds / is authoritative, the integrity manifest summary,
        state-head presence, and operation/lock status. Tests: uninitialized→Unknown-refuses,
        recorded-Match, dev-binary-on-versioned→ModeRegression. Smoke-verified.
  - [~] 8.3 Embed the binding gate (task 3) in the applying verbs `apply`, `destroy`, `scale`,
        `schema setup`, `image push|mirror`: versioned deployments refuse on non-`Match` (no override);
        dev deployments take the `DevIterate` re-stamp+warn path (Req 6.2). `plan` surfaces the verdict
        and annotates without refusing (Req 2.5). DONE (first increment — `apply`): `tkp/src/gate.rs`
        `evaluate_gate` → `GateOutcome::{Proceed{authoritative}, Refuse{verdict, reason}}` implements the
        3.2/3.3 policy; `tkp apply` (`tkp/src/apply.rs`) evaluates the gate **before any mutation**
        (versioned refuses non-`Match`; dev warns + proceeds on `DevIterate`; unstamped → `Unknown` refuses
        — Day-0 stamping is `create`'s job, task 2.2), then runs the real `InfraEngine` apply (wired to the
        **local** platform — empty plan, exercising the `DeploymentStore` seam) and **re-stamps** the
        envelope (records the running binding, advances `config_revision` — task 14.2). Tests: 5 gate cases +
        `apply_refuses_an_unstamped_deployment`, `apply_proceeds_on_dev_iterate_and_restamps`; CLI
        smoke-verified. *Remaining: multi-platform dispatch (compose/ecs), the service/runtime apply, and
        the other applying verbs (`destroy`/`scale`/`schema`/`image`), plus `plan`-surfaces-verdict.*
  - [~] 8.4 `upgrade`: atomic ownership transfer first (task 5.3) — flip binding → B, capture [A final]
        (incl. A's prior configuration-revision ref), open the marker, before any mutation; then run
        state-schema migrations, apply B's plan. DONE (first increment): `tkp upgrade` (`tkp/src/upgrade.rs`)
        loads the envelope, requires a recorded A (refuses unstamped), `evaluate_upgrade(A, B)` (refuse or
        VersionedAdvance/Promotion), then the **atomic ownership transfer** (`begin_upgrade` + one CAS save,
        before any mutation), then applies B's plan (local infra apply — shared with `apply`), then
        `close_operation` + save. Tests: `upgrade_refuses_an_unstamped_deployment`,
        `upgrade_refuses_versioned_to_dev_restamp`. *Remaining: state-schema migrations (task 5.1),
        multi-platform apply, the audit change log, the advisory baseline drift gate, and `resume` of an
        interrupted transfer.* MAY record an **ids-only audit change log** (`id + op`,
        no before-images) for observability; rollback needs no before-images (Proposal 002). Close the
        marker. Handles the versioned advance and the dev → versioned promotion; refuses downgrade,
        same-semver/different-hash, and re-stamp back to `dev`; the only verb that authoritatively advances
        the recorded version (Req 4.5, 4.6, 6.3, 6.5). Optional advisory baseline gate: refuse-and-surface
        live drift from [A final] (Req 4.7). No `adopt` verb. Property 15.
  - [~] 8.5 `rollback`: **definition-driven — B delete-only → re-pin → A forward-reconcile** (Req 9,
        Proposal 002). DONE (first increment): `tkp rollback` (`tkp/src/rollback.rs`) fail-closes if there is
        no `[A final]` checkpoint, runs the B delete-only pass (empty for local; `destroy_selected` wires
        here for real platforms), commits the **re-pin to A** in one CAS save
        (`DeploymentStateEnvelope::begin_rollback` restores A's binding + state heads + retained
        configuration-revision ref and opens the `RollbackInFlight` marker), then A reconciles (local infra
        apply of the retained revision), then `complete_rollback` clears the marker and consumes the
        checkpoint. Tests: `begin_rollback_repins_to_checkpoint_and_completes`,
        `begin_rollback_without_checkpoint_errors`, `rollback_refuses_without_a_checkpoint`,
        `rollback_repins_to_the_checkpoint_engine`. *Remaining: the two-binary orchestration (`tkr` relaunches
        A for the reconcile), the real `destroy_selected` delete-only over live resources, both-binary
        checksum verification, and holding the operation lock across the sequence (12.2).* Preconditions
        (exhaustive, before any destructive work): acquire the remote
        operation lock (task 12), verify **both** binaries' checksums, confirm the checkpoint + retained
        prior configuration revision exist, persist the operation marker. Undo (B): `destroy_selected`
        (task 11.3) over the ids B created (`keys(S_B) − keys(S_A)`) — reverse-dep order, fail-closed,
        idempotent (absent ⇒ done); no before-images, no restore trait. Re-pin: atomic commit binding → A
        (CAS, superseding not destroying `S_B`), close/advance the marker. Reconcile (A): A runs
        `refresh_state` (observe live) then its **ordinary `apply` of the retained prior configuration
        revision `R_a`** over the current state — updating B's modifications toward `R_a`, re-creating B's
        deletions (ordinary `create`), and deleting anything absent from `R_a` via `known`-not-`desired`.
        Resumable from the marker; whole sequence under the operation lock; no reverse migration
        (Req 9.2–9.7). Property 9. Depends on tasks 11, 12.

- [ ] 9. `tkr` launcher + surface separation (Requirement 7)
  - [ ] 9.1 Add the `tkr` launcher seam with the four **launch classes**: **bound** (recorded binary,
        verified against the recorded integrity manifest), **candidate-upgrade** (operator/release-resolved
        B, verified against external build/release metadata), **dev-candidate** (current local dev build),
        **rollback** (bound B then retained A) — resolve, checksum-verify, re-exec (reusing the
        Dagger-style re-exec in `apps/tkr/src/commands/image/mod.rs`); abort on mismatch (Req 7.1, 7.2).
  - [ ] 9.2 Relocate the deployment-lifecycle verbs to forward to `tkp` (build phase: move, don't shim);
        keep `tkr deployment create|list|use|lock|unlock|destroy`, dev/ci/compat/workstation, `version`,
        `config`, and `image build` owned by `tkr`; `image push|mirror` forward to `tkp` (Req 7.3, 7.4).

- [ ] 10. Deployment lock (Requirement 8)
  - [ ] 10.1 Add `tkr deployment lock [<name>]` / `unlock`: write/clear a durable `lock.toml` (name +
        identity fingerprint) in the deployments registry root; `unlock` requires `--yes` (Req 8.1, 8.5).
  - [ ] 10.2 Enforce the lock before the launcher: mutating commands refuse against any non-locked
        deployment; read verbs are never blocked; a stale (missing) lock and a changed identity
        fingerprint both fail closed (Req 8.2, 8.3, 8.4). Property 8.

- [x] 11. IaC framework hardening in `tokeira-iac` (Requirements 10, 12; underpins rollback)
  - [x] 11.1 Fail-closed delete: in `apply_changes` / `destroy_changes`, a Delete whose `ResourceId` is
        absent from `known` returns an error (`IacError::UnknownResourceDelete`) instead of removing it from
        state without deleting the live resource. Property 10 — done + test
        (`fail_closed_delete_refuses_unknown_resource`); all 23 `tokeira-iac` tests green.
  - [x] 11.2 Authoritative describe: replace `Option<ResourceState>` with `DescribeResult { Present |
        Absent | Unsupported }`; prune state only on `Absent`, never on `Unsupported`; drive `delete()`
        from persisted state on `Unsupported` (provider-NotFound = success). Property 10 (Req 10.3).
        Done. `DescribeResult` enum + trait signature in `tokeira-iac`; `refresh_state` leaves state
        untouched on `Unsupported` (new `RefreshStatus::Unknown`), destroy loop drives `delete(current)`
        from persisted state on `Unsupported`. New iac tests `unsupported_describe_deletes_from_persisted_state`
        and `unsupported_describe_is_not_pruned_on_refresh`. Classified all 52 `Ok(None)` paths across 31+
        impls via a 23-agent audit (provider-confirmed not-found → `Absent`; stub/missing-prerequisite/no-query
        → `Unsupported`). Migrated every impl + consumer across ~12 crates (tokeira-aws 27 impls + 10 internal
        idempotency consumers; ecs/compose/compose-syn/compose-deployment/local/remote-workstation/orchestrator
        wrappers, delegators, and test stubs). Delete idempotency for the unconditional-`Unsupported` stubs
        (whose destroy now calls `delete` instead of skipping): hardened `ssm:DeleteParameter` and
        `ecs:DeleteService` to swallow not-found; s3 DeleteObject already idempotent, CloudMap delete is a
        no-op, managed DsqlIamRole delegates to the already-tolerant `IamRole::delete`, ECS
        DeregisterTaskDefinition is idempotent by nature. Full workspace build green; all affected crates'
        tests green (iac 27, aws 21, ecs 59, compose-deployment 28, compose-syn 16+43, orchestrator 4,
        remote-workstation 5, local 7, compose 5).
        Residual (separate enhancement, not blocking): the 5 stub `describe` impls (`s3_object`,
        `ssm_parameter`, the 3 `ecs_service` resources) return `Unsupported` as a fail-safe — they should
        eventually gain real provider-querying `describe` so refresh can confirm presence/absence.
  - [x] 11.3 Forward-engine capabilities that make definition-driven rollback correct (Proposal 002 —
        supersedes the `apply_inverse_delta` / `StateDrivenRestore` approach; **do not** build
        `AppliedDelta` before-images, a restore trait, `restore_to_state`/`recreate_from_state`, or
        `apply_inverse_delta`). Two general apply features (they benefit *every* apply, not just rollback):
        - [x] **11.3a Replacement.** DONE. `ChangeKind::Replace` + `InternalChange::Replace` (delete-then-
          recreate) for an **immutable-field** change; a resource opts in by returning `Replace` from
          `diff` (backward-compatible — a resource that never does behaves as today). `apply_changes`
          handles it at the resource's forward-topo slot (delete `current`, remove+save, then `create`,
          insert+save), so dependencies exist and dependents reconcile against the new resource. `tkr`
          plan display shows `±`. Test `replace_deletes_then_recreates`.
        - [x] **11.3b Destructive-change classification.** DONE (engine layer). `ChangeKind::is_destructive`
          (Delete | Replace), `Change::is_destructive`, and `destructive_changes()` / `plan_is_destructive()`
          helpers (re-exported). Tests `destructive_changes_selects_delete_and_replace`,
          `non_destructive_plan_is_not_flagged`. The `--yes` **enforcement** is deferred to the CLI/`tkp`
          (plan → classify → confirm → apply) since the engine cannot prompt.
        - [x] **11.3c `Engine::destroy_selected(known, ids, ctx, saver)`.** DONE. Delete-only over a named
          id-set on the current state (no refresh), reusing the shared `destroy_changes` path: reverse-dep
          order, fail-closed (`UnknownResourceDelete` for an id ∉ `known`), idempotent (id ∉ state skipped),
          others untouched. Tests `destroy_selected_deletes_only_named_ids`,
          `destroy_selected_fails_closed_on_unknown_id`.
        - [x] **11.3d Runtime half.** DONE. `Platform` gains `supports_delete()` (default `false`) and
          `delete_service(name, manifests)` (default **fail-closes** with `RuntimeError::Platform`); the
          compose Platform implements both (delegating to the renamed inherent `remove_service`, which is
          idempotent). `ServiceEngine::destroy_services(services, platform, ctx, state)` is the runtime
          counterpart of `destroy_selected`: fail-closed pre-flight (refuse the whole pass if the platform
          can't delete), reverse-dependency-order teardown, removes each from `RuntimeState`. A's reconcile
          re-applies `R_a`'s services through the existing forward `apply_manifests` — no `Service` restore
          capability, no runtime before-images (Proposal 002). Tests
          `destroy_services_fails_closed_when_platform_cannot_delete`,
          `destroy_services_deletes_in_reverse_dependency_order`.
  - [x] **Task 11.3 COMPLETE** (a/b/c/d): infra forward-engine replacement + destructive classification +
        `destroy_selected`, and the runtime-half `Platform` service delete + `ServiceEngine::destroy_services`.
        All tested; full workspace build clean. Do **not** build `AppliedDelta` before-images, a restore
        trait, or `apply_inverse_delta` — superseded by Proposal 002.
  - [x] 11.4 Composition validation: unique module/resource ids, `desired ⊆ known`, delete ids ∈ known,
        deps present unless external, no cycles — refuse before any plan/apply/destroy/rollback Delta.
        Property 12. Done: `IacError::CompositionInvalid` + `validate_composition()` hooked into all 7
        composition entry points (plan / apply / destroy / `plan_for_modules` / `apply_for_modules` /
        `plan_destroy` / `destroy_for_modules`); cycles caught via `collect_resources_from(known)`. Tests
        `composition_refuses_duplicate_resource_id`, `composition_refuses_desired_module_not_in_known`.
        Integration check (orchestrator + compose-deployment + compose-syn) green — no real composition
        trips the new validation.
  - [x] 11.5 Audit existing callers for reliance on the old fail-open behaviour. Precondition for 8.5.
        Done — finding: **no supported flow relies on fail-open.** Every engine driver (`tkr`
        infra/workstation, orchestrator) routes through `InfraEngine::compose`, which sets
        `known = infra_modules(config, ModuleSelection::All)` — a structural superset of `desired`; no
        caller hand-rolls a non-superset `known`. Module-set membership is config-independent for ECS and
        remote-workstation (fixed candidate lists; only `selection` filters). The sole config-conditional
        module is compose / compose-syn's **DSQL**, gated on `config.storage == Dsql`
        (`platforms/compose/src/lib.rs:172`). The only ways state can outlive `known` are (a) flipping
        `config.storage` away from `Dsql`, or (b) editing an identity field that determines a `ResourceId`
        (`project_name` / `workstation_id` / `vpc_id` / `region`) — all destructive/identity changes, not
        routine knobs. In each such case the OLD fail-open *silently dropped a live cloud resource from
        state* (latent orphan/billing bug); fail-closed now surfaces `UnknownResourceDelete`, with an
        explicit scoped `destroy` of the removed module as the correct remediation. No regression; 8.5
        precondition satisfied.

- [ ] 12. Remote operation lock (Requirement 11)
  - [x] 12.1 Add a renewable remote operation lock to `tokeira-state` (S3 lease / explicit record; local
        fs lock); acquire→renew→release around every mutating `tkp` command. Property 11. DONE.
        `tokeira_state::OperationLock` is built over `Box<dyn StateBackend>`, so **one** primitive serves
        both the cloud (S3 lock object via `S3Backend`) and local dev (a filesystem lock file via
        `LocalBackend`): the `OperationLease` record (holder / token / acquired_at / renewed_at /
        expires_at / released) is stored under a dedicated key with the backend's CAS `read_manifest` /
        `write_manifest`, and mutual exclusion is that CAS plus a time lease a superseded holder cannot
        renew. `acquire` **refuses** (`StateError::Locked`) while a lease is active and takes over an
        absent/expired/released one; `renew` returns `StateError::LockLost` on takeover; `release` is
        idempotent. Distinct from the short per-save lease inside `S3StateStore`. Tests:
        `acquire_renew_release_cycle`, `second_acquire_refused_while_held`, `release_lets_next_acquire_succeed`,
        `expired_lease_is_taken_over`, `renew_after_takeover_fails_lock_lost`.
  - [ ] 12.2 Hold one continuous lock across the whole `rollback` sequence (B delete-only → re-pin →
        A forward-reconcile) so no writer interleaves at the handoff (Req 11.3). *Needs `tkp` (the
        rollback orchestration) — deferred until the binary exists; the 12.1 primitive is ready.*

- [x] 13. Deployment state envelope + the authoritative remote store (foundational; decided)
  - [x] 13.1 Define the deployment-level `DeploymentStateEnvelope` — DONE. New crate
        `crates/tokeira-provisioner` (depends on `tokeira-state` for `SnapshotRef`) holds the whole
        provisioner Data-Models set: `BuildMode`, `Target`, `ProvenanceStamp`, `BinaryArtifactDescriptor`,
        `IntegrityManifest`, `ChangeOp`/`ChangeLogEntry`/`ChangeLog` (ids-only audit, no before-images),
        `RollbackCheckpoint` (spans both `from_infra_head`/`from_runtime_head` + the load-bearing
        `from_config_ref`), `OperationKind`/`Operation` (phase + resumable progress + optional audit log),
        `OperationLock`, and `DeploymentStateEnvelope { schema_version, deployment_id, binding
        (Option = Unknown, never coerced), integrity, config_revision, checkpoint, operation, lock,
        infra_head, runtime_head, effective_config_ref }`. The envelope implements `Default` + `Validate`
        and is proven storable via `S3StateStore` (bounds test `envelope_is_storable`) — it **rides on the
        store**, which natively backs the `SnapshotRef` heads, the immutable checkpoint, and the lease.
        `SnapshotRef` gained `PartialEq/Eq`. Tests: default-is-valid-and-unbound, serde round-trip,
        schema-version rejection (0 / too-new), storable-bounds. (This front-runs tasks 2/3, which add the
        *logic* that populates `ProvenanceStamp`/`IntegrityManifest`; this task defines the shared models.)
        NOTE: the envelope is defined and storable; **wiring the engines/`tkp` to actually persist it** (so
        `infra_head`/`runtime_head` point at the infra/runtime `S3StateStore` snapshots) is follow-on work
        with the operation lock (task 12) and upgrade/rollback orchestration (task 8).
  - [x] 13.2 Abstract the engine state seam over two stores: `CasStore` over `LocalBackend` (local/compose
        dev) and `S3StateStore` (remote, snapshot/lease, authoritative). DONE. New
        `tokeira_state::DeploymentStore<T>` trait (`load() -> (T, version)`, `save(doc, expected) -> version`)
        impl'd for both stores — `CasStore` matches natively; `S3StateStore` self-manages CAS via its lease
        (version = manifest ETag, `expected` advisory) via a new `S3StateStore::load_with_version`. The
        `Deployment` seam now returns `Box<dyn DeploymentStore<InfraState|RuntimeState>>` (was
        `Box<dyn StateBackend>`); `InfraEngine`/`DeployEngine` hold `Arc/Box<dyn DeploymentStore<…>>` and no
        longer wrap in `StateStore`. Local/compose/compose-syn/remote-workstation wrap `CasStore`-over-
        `LocalBackend` (byte-identical layout preserved); **ECS employs `S3StateStore`** (a generic
        `s3_state_store<T>`; missing-clients fallback errors loudly), replacing the `CasStore`-over-`S3Backend`
        single-doc stopgap. Tests `cas_store_as_deployment_store_round_trips`,
        `cas_store_as_deployment_store_rejects_stale_version`. Full workspace build clean; all affected crates
        green (tokeira-state 9, orchestrator 4, ecs 59, + platforms). The dead `InfraStateStore`/
        `RuntimeStateStore = S3StateStore<…>` aliases in `document.rs` are now superseded by the seam (left
        in place; the engines use `dyn DeploymentStore`, not the aliases).
  - Note: the operator-facing **`tkr remote-state` option (let an arbitrary deployment opt into remote
        state) is HELD/deferred** — store choice stays platform-determined (ECS → remote; local/compose →
        local), exactly as `create_infra_store` already fixes the backend per platform today.

- [ ] 14. Engine identity vs configuration revision (Requirement 13)
  - [x] 14.1 Scope the binding `source_tree_hash` to the engine/resource-implementation surface, excluding
        desired-state configuration; a config change must not change the engine identity (Req 13.1).
        Property 14. DONE — the invariant is structural: `source_tree_hash` is `tokeira-build-info`'s digest
        of the workspace **code**, while a deployment's desired-state config is operator **data** (its
        `deployment.toml` / `.tkd` in the deployment dir), never part of the workspace source — so a config
        change cannot change the engine identity and cannot gate. The binding keys only on
        `source_tree_hash`; config is tracked by the separate `config_revision`. Property 14 test
        `config_refinement_keeps_the_engine_binding_and_advances_revision` (repeated same-engine applies keep
        the recorded `source_tree_hash` and advance `config_revision`). *Narrowing the digest to only the
        engine crates (vs the whole workspace) is a build-system refinement — 14.4 direction — but Property
        14 holds regardless because config is already excluded.*
  - [ ] 14.2 On `apply`, advance `config_revision` and record `effective_config_ref`; `describe` reports
        engine stamp + `config_revision` (Req 13.2).
  - [ ] 14.3 Add config-revision revert: a same-engine `apply` of a prior recorded config revision — not an
        `upgrade`, not a two-binary rollback (Req 13.3).
  - [ ] 14.4 Architectural direction (note, not a single task): express platform desired-state definition
        as runtime config/data wherever possible so refinement is a plan, not a rebuild; reserve `tkp`
        rebuilds for behavioral engine/resource-impl changes (Req 13.4).

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["13", "13.1", "13.2", "11", "11.1", "11.2", "11.3", "11.4", "11.5", "1", "1.1", "1.2"] },
    { "wave": 2, "tasks": ["2", "2.1", "2.2", "4", "4.1", "12", "12.1", "14", "14.1"] },
    { "wave": 3, "tasks": ["3", "3.1", "3.2", "3.3", "4.2"] },
    { "wave": 4, "tasks": ["5", "5.1", "5.2", "5.3"] },
    { "wave": 5, "tasks": ["6", "6.1", "6.2"] },
    { "wave": 6, "tasks": ["8", "8.1", "8.2", "8.3", "8.4", "8.5", "12.2", "14.2", "14.3"] },
    { "wave": 7, "tasks": ["9", "9.1", "9.2", "10", "10.1", "10.2"] },
    { "wave": 8, "tasks": ["7", "7.1"] }
  ]
}
```

## Notes

- Provenance is recorded at the manifest/snapshot level (coarse, one stamp per snapshot), not per
  `ResourceState`. This is the simplification the binary model enables: migrations run once per version
  transition at the upgrade boundary, not per-resource on every apply.
- **Delta is the single mutation primitive.** `plan` previews it, `apply` enacts it, and `upgrade`/
  `rollback` are compositions of it over different `(desired, state, binary)` triples — not bespoke
  operations. A Delta spans create/update/delete; rollback is *two* Deltas because it crosses an
  authorship boundary, governed by: **no binary interprets a state representation authored by another
  version.**
- **Rollback is definition-driven, split into delete-only (B) + forward-reconcile (A)** (Proposal 002).
  B **deletes only the resources it created** — only B can name and delete its own resource kinds (A's
  older code may not even recognize them) — needing no before-images because delete is already
  state-driven. The binding then re-pins to A, and A observes live state (`refresh_state`) and
  **forward-applies its retained prior configuration revision**, reconciling B's updates and re-creations
  from A's own config. Neither binary reinterprets the other's recorded state; A observes shared live
  infrastructure it can `describe`.
- **Engine identity vs configuration revision (refine without re-minting).** The binding key
  (`source_tree_hash`) covers the engine/resource-implementation surface only; the deployment's
  desired-state config is a separate `config_revision`. Everyday refinement (scaling, image refs, module
  parameters, resources) is an ordinary `apply` against the same `tkp` — a plan, recorded as a new config
  revision, no new build and no `upgrade`. A new `tkp` version (and the upgrade/rollback machinery) is
  needed only for a behavioral engine change. Reverting config is a same-engine apply, not a rollback.
  Architectural lever: express platform definition as config (data) so refinement is a plan, not a rebuild.
- **Owner decisions (both now settled).** (1) State-envelope store — **DECIDED: `S3StateStore` (remote,
  snapshot/lease) is authoritative and the `DeploymentStateEnvelope` is its manifest; `CasStore`-over-
  `LocalBackend` stays the local/compose dev path. ECS employs remote-state (`S3StateStore`), replacing its
  `CasStore`-over-`S3Backend` single-doc stopgap. The operator-facing `tkr remote-state` toggle is HELD —
  store choice stays platform-determined** (task 13). (2) Runtime/service rollback — **DECIDED: `rollback`
  covers services/images, not infra-only** — spanning both `infra_head` and `runtime_head`. Under
  Proposal 002 this needs no state-driven restore: B deletes the services/images it created (requiring the
  deploy-engine `Platform` to gain a **service delete**), and A re-applies its retained prior revision's
  services through the existing forward `apply_manifests`. A re-asserts infra *and* runtime config; no
  `Service` restore capability or before-images.
- The authoritative drift key is `source_tree_hash` (whole-workspace digest from `tokeira-build-info`),
  not the semver — a developer can forget to bump the version; the source digest cannot be forgotten. The
  semver and git SHA are human-facing labels. `dev` builds carry a sentinel hash and are non-authoritative
  by construction (the dev path ignores a dirty worktree), so they never bind authoritatively.
- Versioning is mandatory from Day 0: `tkr deployment create` always stamps, so there is no unstamped
  state and no `adopt` verb. Strictness is conditioned on the recorded build mode — a **dev deployment**
  iterates freely (`DevIterate`: a dev binary re-applies, re-stamps, warns — the bring-up loop), a
  **versioned deployment** is strict (apply requires source-tree `Match`; drift → `upgrade`). The first
  real exercise of this is the ECS platform bring-up, which runs entirely in the dev regime; its expected
  fix-and-reapply churn is absorbed by `DevIterate` without versioning friction.
- The provisioner is a **new, deployment-married binary** (`apps/tkp`, binary `tkp` — small-form sibling
  of `tkr`) with a specialized lifecycle CLI — `describe`, `plan`, `apply`, `destroy`, `scale`, `schema`,
  `status`, deployment-scoped `image push|mirror`, and the version-transition verbs `upgrade`/`rollback`.
  It
  is **not** `tkr` with fewer flags presented to operators; operators drive `tkr`'s existing command
  structure and `tkr` forwards lifecycle verbs to the checksum-verified `tkp`. `tkr` never mutates a
  deployment directly. There is **no** apply-anyway override; a non-`Match` binding is resolved by the
  matching `tkp` or `upgrade`. The recorded stamp is written by `tkr deployment create` (initial),
  `tkr deployment upgrade` (authoritative advance / dev → versioned promotion), and
  `tkr deployment rollback` (re-pin to a prior checkpoint); a dev-deployment apply also re-stamps the
  advisory dev stamp (`DevIterate`). There is no `adopt` verb.
- **Upgrade/rollback reuse the engine's existing plan/apply — no new planning.** Upgrade's first act is an
  *atomic ownership transfer* (flip binding → B, capture [A final], open the operation marker) before any
  mutation, so the recorded binding always names the operating binary and a crash recovers as B, never A —
  no "pending" binding state (binding is two-valued; the operation marker carries "in flight"). B MAY
  record an ids-only audit change log as it commits — but not before-images. Rollback is definition-driven
  (Proposal 002): B **deletes what it created** (`destroy_selected`, no restore trait), the binding
  re-pins to A, and A observes live state then **forward-applies its retained prior configuration
  revision**. The one new *engine* mechanic is **replacement** (immutable-field change → delete+recreate)
  plus destructive-change gating in `plan` — general apply features, not rollback-only. The advisory
  baseline gate (refuse-and-surface live drift from [A final]) stays advisory — no cross-version
  authoritative reconcile.
- The deployment **lock** (`tkr deployment lock`/`unlock`) is a durable, cross-session mis-apply guard
  (name + identity fingerprint in `lock.toml`), orthogonal to the version-binding gate: it confines
  mutation to the locked deployment, never blocks reads, and fails closed on a stale/changed lock.
- Image scoping: `image push`/`mirror` are deployment-scoped (they target the deployment's own ECR and
  write back its config) and live on the provisioner surface; `image build` builds from workspace sources
  and stays on `tkr`.
- The state-format `schema_version` (serialization shape) and the CAS generation (concurrency token) are
  distinct from the provisioner provenance version; do not conflate the three.
- Trust always flows from the CAS-guarded manifest checksum, never from a stored or fetched binary blob.
- Out of scope (follow-on specs): automated self-update (download, atomic swap, rollback); release
  signing and key management; the single-shared-binary vs provisioner-as-SDK decision for multi-consumer
  reuse (e.g. Odori); per-target build/distribution matrix beyond recording checksums.
