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
  - [ ] 1.1 Add `ProvenanceStamp`, `BuildMode`, `Target`, `BinaryArtifactDescriptor`, `IntegrityManifest`
        to the state manifest model; `ProvenanceStamp` carries `version` + `git_sha` + `source_tree_hash`
        + `build_mode` and distinguishes `Unknown` from a concrete version.
  - [ ] 1.2 Wire them into the existing manifest write/read path, preserving the distinct state-format
        `schema_version`. Property: provenance round-trips (Property 1).

- [ ] 2. Provenance stamping
  - [ ] 2.1 On every state-document write, stamp the running provisioner version from `tokeira-build-info`
        (`TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`, `SOURCE_TREE_HASH`, `BUILD_MODE`); `source_tree_hash` is the
        authoritative drift key (Property 6).
  - [ ] 2.2 On remote-state init (`tkr deployment create`), write the stamp before any resource create —
        Day-0 mandatory versioning; there is no create path that leaves state unstamped (Req 1.2).

- [ ] 3. Binding gate (regime by build mode)
  - [ ] 3.1 Add `check_binding` in `tokeira-orchestrator` / `tokeira-iac` returning
        `Match | DevIterate | Mismatch | Downgrade | ModeRegression | Unknown`; computed for every
        binding-aware verb (plan surfaces it, applying verbs gate on it).
  - [ ] 3.2 Versioned deployment: proceed on `Match`; refuse `Mismatch`/`Downgrade`/`Unknown` with no
        override (resolve via matching binary or `upgrade`); refuse `ModeRegression` (dev binary on a
        versioned deployment). Properties 2, 5.
  - [ ] 3.3 Dev deployment + dev binary → `DevIterate`: apply permissively, re-stamp the advisory dev
        stamp, emit a non-authoritative warning (the bring-up loop). A `dev` stamp never yields the
        authoritative `Match`. Property 7.

- [ ] 4. Integrity manifest + verification
  - [ ] 4.1 Record version + per-target `sha256` (+ optional `retrieval_ref`) in the CAS-guarded manifest
        at stamp time.
  - [ ] 4.2 Verify a retrieved binary's checksum against the manifest before execution; abort on
        mismatch. Property 3.

- [ ] 5. Upgrade/migration boundary
  - [ ] 5.1 Add the `MigrationRegistry` keyed by **state-schema transition** (`from_schema → to_schema`)
        and the version-transition entry point; run forward migration before mutation on upgrade only when
        the schema changes (a new `source_tree_hash` at the same schema is a re-stamp). Forward-only.
  - [ ] 5.2 Refuse downgrade (ordering by monotonic version/build id, never by hash); refuse a same-semver
        /different-hash apply; refuse a missing migration for a required schema transition. Property 4.
  - [ ] 5.3 On upgrade, capture a `RollbackCheckpoint` (prior snapshot ref + prior stamp + prior **full
        integrity manifest** + prior effective-config ref) before migrating (Req 9.1).

- [ ] 6. S3 binary retention (optional path)
  - [ ] 6.1 In the S3 state store, optionally persist the binary blob keyed by `version`+`target`
        alongside state documents.
  - [ ] 6.2 Retrieve + checksum-verify before execution (reuses 4.2). Property 3 (5.3).

- [ ] 7. Measure and record the optimized binary size
  - [ ] 7.1 `cargo build --release` the provisioner; record the stripped size and the linked AWS SDK
        client set; note the trim/`opt-level`/UPX levers in the design's size section.

- [ ] 8. Provisioner binary + specialized CLI (`apps/tkp`, binary `tkp`) (Requirements 6, 7)
  - [ ] 8.1 Create the `apps/tkp` binary crate linking `tokeira-iac`,
        `tokeira-deploy-engine`, `tokeira-orchestrator`, `tokeira-aws`, the platform crates, and
        `tokeira-state`; its own clap surface scoped to the deployment lifecycle (no operator/global
        verbs) (Req 7.3).
  - [ ] 8.2 `describe`: read-only report of identity, recorded provenance (or `unknown`), binding
        verdict, integrity manifest, and state-format/CAS facts; human + `--json`; never gates (Req 6.1,
        6.5).
  - [ ] 8.3 Embed the binding gate (task 3) in the applying verbs `apply`, `destroy`, `scale`,
        `schema setup`, `image push|mirror`: versioned deployments refuse on non-`Match` (no override);
        dev deployments take the `DevIterate` re-stamp+warn path (Req 6.2). `plan` surfaces the verdict
        and annotates without refusing (Req 2.5).
  - [ ] 8.4 `upgrade`: the migration boundary (task 5) — run forward migrations, re-stamp provenance +
        integrity; handle both the versioned advance and the dev → versioned promotion; refuse downgrade
        and refuse re-stamping back to `dev`; the only verb that authoritatively advances the recorded
        version (Req 6.3, 6.5). There is no `adopt` verb (Day-0 versioning leaves no unstamped state).
  - [ ] 8.5 `rollback`: undo the upgrade plan, then reconcile (Req 9). Preconditions (exhaustive, before
        any destructive work): acquire the remote operation lock (task 12), verify **both** binaries'
        checksums, verify every resource in the recorded plan is still instantiable by B, persist a
        `RollbackOperation` marker. Undo (B): `apply_inverse_plan` (task 11.3) of the recorded upgrade
        plan over the full `S_B` — delete B's creates, revert B's updates to checkpoint state, re-create
        B's deletes from checkpoint state; reverse-dep order; idempotent. Re-pin: restore the checkpoint
        (CAS, superseding not destroying `S_B`), re-pin A. Reconcile (A): A's ordinary apply re-asserts A's
        own config over the restored checkpoint. Resumable from the marker; whole sequence under the
        operation lock; no reverse migration (Req 9.2–9.7). Property 9. Depends on tasks 11, 12.

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

- [ ] 11. IaC framework hardening in `tokeira-iac` (Requirements 10, 12; underpins rollback)
  - [ ] 11.1 Fail-closed delete: in `apply_changes` / `destroy_changes`, a Delete whose `ResourceId` is
        absent from `known` returns an error instead of removing it from state without deleting the live
        resource (today: "removing from state only" / "skipping delete"). Property 10.
  - [ ] 11.2 Authoritative describe: replace `Option<ResourceState>` with `DescribeResult { Present |
        Absent | Unsupported }` (or contractually "unimplemented describe errors"); prune state only on
        `Absent`, never on `Unsupported`; drive `delete()` from persisted state on `Unsupported`
        (provider-NotFound = success). Property 10 (Req 10.3).
  - [ ] 11.3 `Engine::apply_inverse_plan(known, recorded_plan, ctx)`: apply the inverse of a recorded
        plan over the **full** `ctx.state` — delete recorded creates, revert recorded updates to their
        prior state, re-create recorded deletes from their prior state; every touched id required present
        in `known`; reverse dependency order; state mutated only after each op succeeds. (`destroy_selected`
        — refs/id-set delete over the full state — is the delete-only sub-case.) The mechanic B's undo uses.
  - [ ] 11.4 Composition validation: unique module/resource ids, `desired ⊆ known`, delete ids ∈ known,
        deps present unless external, no cycles — refuse before any plan/apply/destroy/rollback Delta.
        Property 12.
  - [ ] 11.5 Audit existing callers for reliance on the old fail-open behaviour. Precondition for 8.5.

- [ ] 12. Remote operation lock (Requirement 11)
  - [ ] 12.1 Add a renewable remote operation lock to `tokeira-state` (S3 lease / explicit record; local
        fs lock); acquire→renew→release around every mutating `tkp` command. Property 11.
  - [ ] 12.2 Hold one continuous lock across the whole `rollback` sequence (B-undo → restore →
        A-reconcile) so no writer interleaves at the handoff (Req 11.3).

- [ ] 13. Deployment state envelope (Requirement; foundational — see Open Decisions)
  - [ ] 13.1 Define the deployment-level envelope/manifest owning provenance (engine identity) + integrity
        + `config_revision` + rollback checkpoint + lock + infra/runtime snapshot refs + effective-config
        ref under one revision, and reconcile it with the active store path (`CasStore`/`S3Backend`
        single-doc vs `S3StateStore` snapshot/lease model) — decide which store is authoritative before
        bolting metadata on.

- [ ] 14. Engine identity vs configuration revision (Requirement 13)
  - [ ] 14.1 Scope the binding `source_tree_hash` to the engine/resource-implementation surface, excluding
        desired-state configuration; a config change must not change the engine identity (Req 13.1).
        Property 14.
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
    { "wave": 1, "tasks": ["13", "13.1", "11", "11.1", "11.2", "11.3", "11.4", "11.5", "1", "1.1", "1.2"] },
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
- **Rollback's two operations split by capability, not just authorship.** B reverses the *entire* upgrade
  plan (delete its creates, revert its updates, re-create its deletes) because only the binary that made a
  change has the implementation and dependency versions to reverse it — A's older code may be unable to
  read or revert a resource B mutated through, e.g., a newer AWS SDK. A then re-asserts its own config over
  the restored checkpoint. Neither binary reverses a change it did not make.
- **Engine identity vs configuration revision (refine without re-minting).** The binding key
  (`source_tree_hash`) covers the engine/resource-implementation surface only; the deployment's
  desired-state config is a separate `config_revision`. Everyday refinement (scaling, image refs, module
  parameters, resources) is an ordinary `apply` against the same `tkp` — a plan, recorded as a new config
  revision, no new build and no `upgrade`. A new `tkp` version (and the upgrade/rollback machinery) is
  needed only for a behavioral engine change. Reverting config is a same-engine apply, not a rollback.
  Architectural lever: express platform definition as config (data) so refinement is a plan, not a rebuild.
- **Open decisions for the owner** (largest-leverage, not Kiro's to settle): (1) the deployment state
  envelope (task 13) — define it inline vs as its own sub-spec, and which store backs it; (2)
  runtime/service rollback — whether `rollback` spans `RuntimeState.services`/`images` (extend the service
  engine with delete semantics) or is explicitly infra-only per platform. Both shape the work most.
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
- **Upgrade rollback reuses the engine's existing plan/apply — it adds no new planning.** The contribution
  is: *the upgrade was a plan (`S_A → S_B`); rollback undoes it, then reconciles.* Two operations because
  reversing a change requires the binary that made it: **B applies the inverse of its recorded upgrade
  plan** (delete its creates, revert its updates to checkpoint state, re-create its deletes from checkpoint
  state — the full inverse, only B can), then the checkpoint is re-pinned, then **A re-asserts its own
  config** with its ordinary apply. Migrations stay forward-only. Made safe by the fail-closed Delete
  hardening (task 11). Distinct from the binary-self-update rollback non-goal. The new engine mechanic is
  `apply_inverse_plan` (task 11.3); everything else is existing plan/apply — and the upgrade must record
  its plan with before-images so it can be inverted.
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
