# Design

## Overview

Tokeira provisions infrastructure through an IaC framework (`tokeira-iac`, `tokeira-deploy-engine`,
`tokeira-state`, `tokeira-orchestrator`, `tokeira-aws`) on which the platforms are built. This spec binds
each deployment to the exact code that provisions it, so a change to a resource implementation can never
silently re-interpret existing state and drift live infrastructure.

Two binaries, two roles:

- **`tkr` — the operator cockpit.** One globally-installed, version-current CLI used across all
  deployments: the deployment registry, developer/CI/compatibility/workstation tasks, workspace image
  builds, and the *launcher* that runs a deployment's provisioner. `tkr` never mutates a deployment's
  infrastructure itself.
- **`tkp` — the deployment-married provisioner.** A small, version-stamped binary that owns one
  deployment's infrastructure/service lifecycle. It is built from the same workspace as `tkr`, stamped
  with a whole-tree source digest, and (for S3 state) retained with the deployment so the deployment
  carries its own manager.

Four ideas carry the design, in order of weight:

1. **One mutation primitive — the Delta.** The engine computes and applies plans (create/update/delete);
   `apply`, `upgrade`, and `rollback` are all compositions of that one primitive. No bespoke planning.
2. **Source-tree hash is the binding authority.** A deployment records the whole-tree source digest of
   the provisioner that may mutate it; a mismatch is gated, never silently applied.
3. **The mutating binary is the bound binary.** The operator drives lifecycle through `tkr`, but `tkr`
   resolves and runs the deployment's stamped `tkp` — so the bytes that mutate a deployment are exactly
   the bytes its integrity manifest names.
4. **Rollback forward-reconciles toward the retained prior configuration revision, not an inverse of what
   the upgrade committed** (Proposal 002). The prior revision is a deterministic, hermetic definition that
   is already retained; rollback restores it and lets the forward engine reconcile toward it. The
   superseded binary B deletes what it created (delete is already state-driven, so no recorded
   before-images are needed), the binding re-pins to A, and A observes live state (`refresh_state`) and
   forward-applies its retained prior revision — never reinterpreting B's recorded state.

Scope is the minimal foundational set: provenance, binding, integrity, the upgrade/migration boundary,
rollback, and binary retention for S3 state. Out of scope (follow-on): automated binary self-update,
release-signing infrastructure, and the single-shared-binary-vs-SDK multi-consumer decision.

## Architecture

`tkp` is a new binary crate (`apps/tkp`) spanning the engine-decoupled crates: `tokeira-iac` (engine),
`tokeira-deploy-engine`, `tokeira-orchestrator`, `tokeira-aws`, the platform crates, and `tokeira-state`.
The work sits at three seams and touches no durable-execution engine crate (kernel/runtime/storage):

- **State (`tokeira-state`).** A deployment-level state envelope binds provenance, integrity, the rollback
  checkpoint, the operation lock, and the infra/runtime state under one revision; the S3 store optionally
  retains the provisioner binary keyed by version + target.
- **Operation (`tokeira-orchestrator` / `tokeira-iac`).** Each applying entry point resolves the recorded
  provenance, runs the binding gate, and (on upgrade/rollback) the checkpoint machinery, then proceeds
  through the existing plan-confirm-apply flow.
- **Launcher (`tkr`).** For a lifecycle action `tkr` resolves the binary to run (by launch class),
  checksum-verifies it, and execs it — reusing the re-exec pattern already used for the Dagger session
  dance in `apps/tkr/src/commands/image/mod.rs`.

The engine already plans and applies; the framework gains three correctness mechanics that protect every
caller: **fail-closed delete**, **authoritative `describe`**, and **composition validation**.

## The Delta primitive

The framework has one mutation primitive. A **Delta** is the set of resource changes
(`Create | Update | Delete | NoChange`) that `compute_changes(desired, state)` produces by comparing a
desired shape against a current state, scoped to a binary's `known` resource universe, executed in
dependency order (forward for create/update, reverse for delete). `plan` previews a Delta; `apply` enacts
it. A Delta routinely spans create *and* update *and* delete — there is no deletes-only special case.

Every lifecycle verb is a composition of Delta applications over a different `(desired, state, binary)`
triple — not a new engine capability:

| Operation | desired | state | binary |
|-----------|---------|-------|--------|
| `apply` | current config → resources | live | the bound binary |
| `upgrade` | new (B) config → resources | live | candidate B |
| `rollback` — undo | ∅ (delete B's creations: `keys(S_B) − keys(S_A)`) | live (`S_B`) | superseded B |
| `rollback` — reconcile | retained prior revision `R_a` → resources | live (observed via `refresh_state`) | prior A |

One invariant governs *which binary* applies a given Delta:

> **A binary drives only changes over a config/state representation it authored; it may observe shared
> live infrastructure, but never reinterprets another binary's recorded state.**

This is why `rollback` is two operations (Proposal 002): the superseded binary B **deletes what it
created** — a delete-only Delta over `keys(S_B) − keys(S_A)`, which needs no recorded before-images
because delete is already state-driven and B alone can name its own kinds; then the binding re-pins to A,
and A observes live state and **forward-applies its retained prior configuration revision `R_a`**,
reconciling B's remaining updates and re-creations from A's own config. Identity-key arithmetic
(`keys(S_B) − keys(S_A)`) is the only cross-version comparison, because it compares `ResourceId`s, never
state bodies; A never reads B's recorded state, only live infrastructure it can `describe`.

## Versioning and binding

### The stamp

Every deployment is versioned from creation; there is no unstamped state and no opt-out, so there is no
"adopt a legacy deployment" verb. A `ProvenanceStamp` is sourced verbatim from `tokeira-build-info` and
recorded in the deployment's state:

| Field | Source | Role |
|-------|--------|------|
| `source_tree_hash` | `SOURCE_TREE_HASH` | **Authoritative binding key — over the engine/resource-implementation surface.** A deterministic digest over the code that decides *how* a plan is computed and applied (the engine and resource-impl crates), not over the deployment's desired-state configuration. A change to any of that code perturbs it; refining configuration does not (see *Engine identity vs configuration revision*). |
| `version` | `TOKEIRA_VERSION` | Human semver label; corroborates, never the sole authority. |
| `git_sha` | `TOKEIRA_GIT_SHA` | Human revision label, for tracing the stamp to a commit. |
| `build_mode` | `BUILD_MODE` | `versioned` (manifest build, authoritative) or `dev` (local fallback, non-authoritative). |

The artifact `sha256` in the integrity manifest is the digest of the *built binary* — what is verified
before a retrieved binary executes. The source-tree hash says *what source produced* the binary; the
`sha256` says *which exact bytes* to run. Both are recorded.

### Build modes set the regime

Versioning is universal; the recorded `build_mode` selects how strict a deployment's gate is:

- A **versioned deployment** is the regime every real environment runs in: an apply requires a
  `source_tree_hash` match; drift refuses and routes through `upgrade`; a downgrade refuses.
- A **`dev` deployment** is the specialized mode for iterating on a never-before-deployed platform (ECS is
  the first). A `dev` binary applies to it freely, re-stamping the advisory dev stamp with a
  non-authoritative warning — the bring-up loop, with no `upgrade` ceremony. A dev deployment is stamped
  like any other but makes no no-drift guarantee, and says so. A `dev` build's digest is a sentinel and is
  dirty-worktree-blind, so it can never bind authoritatively.

Promoting a dev deployment to versioned (bring-up → stable) is an `upgrade` with a versioned binary; a
`dev` binary against a versioned deployment is refused.

### The binding gate

`check_binding(running, recorded)` yields `Match | DevIterate | Mismatch | Downgrade | ModeRegression |
Unknown`. `Match` (versioned, equal source tree) and `DevIterate` (dev binary on a dev deployment, with a
re-stamp + warning) proceed. Everything else refuses with no override: `Mismatch` routes to `upgrade`;
`Downgrade` and `ModeRegression` (dev binary on a versioned deployment) are hard refusals; `Unknown` (a
missing stamp, only possible from corruption or a foreign writer) fails closed. Ordering ("newer/older")
is decided by a monotonic version/build identity, never by the hash — a hash is an equality key.

## Engine identity vs configuration revision

The binding key deliberately covers only the **engine/resource-implementation surface** — not the
deployment's desired-state configuration. This is what lets an operator refine a deployment continuously
without minting a new `tkp` for every change. The two identities are orthogonal:

- **Engine identity** (`source_tree_hash`) — the code that determines *how* a plan is computed and
  applied. It is the binding authority. A new `tkp` build/version is minted *only* when it changes, and an
  engine change is the only thing that requires `upgrade` (with its checkpoint and rollback machinery).
- **Configuration revision** — the deployment's desired-state definition (which modules, their
  parameters, resource definitions, image refs, scaling) as runtime data the bound `tkp` reads. Refining
  it is an ordinary `apply` — a plan (create/update/delete) — recorded as a monotonic revision in the
  state envelope. It does **not** change the engine identity and does **not** gate.

The justification is exactly the binding gate's purpose: the gate exists to stop *engine code* from
silently reinterpreting state or mutating resources differently. A configuration change cannot do that —
it asks the unchanged, trusted engine to converge to a different desired shape, which is a normal safe
plan. So configuration has no place in the binding key; including it would over-gate, forcing `upgrade`
ceremony for safe desired-state changes.

**One engine version manages an evolving sequence of config revisions.** `describe` reports both axes —
the engine stamp (rarely changes) and the current config revision (changes per apply) — so version
stamping stays reliable on both without a profusion of `tkp` builds. The split also draws the `apply` /
`upgrade` line cleanly:

- `apply` — same engine, possibly-new configuration → plan → apply. The common path; it carries *all*
  parametric refinement (scaling, image refs, module parameters, adding/removing resources via config).
- `upgrade` — a new *engine* identity → the gated transition, migration, and checkpoint. Rare, and only
  for a genuine behavioral code change.

**This also bounds the heavy rollback.** The two-binary rollback is needed only across an *engine*
transition. Reverting a bad *configuration* change is an ordinary same-engine `apply` of the prior
config revision — one binary, no checkpoint — because the engine is unchanged and owns the whole plan.
Config has no engine-managed restorable state of its own: the desired config is the operator's source of
truth, so a "config revert" is just applying the prior config revision, not an engine verb that would
compete with that source and be silently undone by the next ordinary apply. Proposal 002 generalizes
exactly this reasoning to the engine-upgrade case: since the prior configuration revision is retained and
the definition is deterministic, upgrade rollback too is a *forward apply of the retained prior revision*
(after B deletes its creations and the binding re-pins to A) — not a recorded-delta inversion.

**The boundary, stated honestly.** This works to the extent platform definition is *configuration (data)*
rather than *compiled code*. A genuine resource-*implementation* change — a new resource kind, changed
apply logic, a dependency/SDK bump — is a real engine change and correctly mints a version; that is
exactly what the binding and rollback fidelity protect. The design's lever is therefore to express
platform desired-state definition declaratively (as config the bound binary reads) wherever possible, so
that everyday refinement is a plan rather than a rebuild; reserve `tkp` rebuilds for behavioral changes;
and use a `dev` deployment for active platform-*code* development.

### Configuration realization: the `.tkd` interpreter

The configuration-revision axis above is realized concretely by the **rust-via-`syn` `.tkd` interpreter**
(platform-config-dsl Proposal 004; prototyped in `platforms/compose-syn`). A deployment's platform
*structure* — its modules, services, wiring, and knobs — is authored in a small Rust subset (`.tkd`),
parsed and interpreted by the bound `tkp` at runtime into a `Deployment` the engine applies. That is what
places platform structure on the **config-revision** axis rather than the engine-identity one: editing the
`.tkd` is an ordinary `apply` (the engine converges to a new desired shape), not a `tkp` rebuild.

Two reinforcements of the binding model:

- **The interpreter, the builder vocabulary, and the kind library are engine identity** — compiled into
  `tkp`, covered by `source_tree_hash`. A genuine resource-implementation change (a new kind, changed
  `realize`/apply logic) perturbs the hash and correctly mints a version; the `.tkd` cannot.
- **The interpreted subset *enforces* the binding invariant.** A reject-by-default allow-list lets a `.tkd`
  only *name* the versioned vocabulary — it cannot define a new resource kind, perform I/O, or alter apply
  logic. So a `.tkd` edit is structurally incapable of becoming an engine-identity change; the gate's
  guarantee holds by construction, not policy. (Create-time-immutable inputs are marked `#[create]` and
  enforced as a config-revision constraint, orthogonal to the engine binding.)

The operator path is therefore `tkr … use <name>` → forward verb to the bound `tkp` → `tkp` loads +
interprets the deployment's `.tkd` → adapts it to `tokeira_orchestrator::Deployment` → the Delta engine
applies — with the `.tkd` as the config revision the bound binary reads.

## Command surfaces and launch classes

The operator only ever types `tkr`. `tkr` owns the registry and global tasks outright and *forwards*
lifecycle verbs to the bound `tkp`:

- **Owned by `tkr`:** `deployment create | list | use | lock | unlock | destroy`, `dev`, `ci`, `compat`,
  `workstation`, `version`, `config`, and `image build` (builds from workspace sources; needs the source
  tree, not a deployment).
- **Forwarded to `tkp`:** `infra plan|apply|destroy`, `deploy plan|apply`, `scale`, `schema`,
  `image push|mirror`, and `deployment describe | upgrade | rollback`.

`tkp` carries a specialized lifecycle surface only (`describe`, `plan`, `apply`, `destroy`, `scale`,
`schema`, `status`, `image push|mirror`, `upgrade`, `rollback`) — never the operator/global surface.
`describe` is the deployment-scoped counterpart to `tkr version`: it reports identity, recorded
provenance, the binding verdict, the integrity manifest, and state facts, honors `--json`, and never
gates (it must work precisely when the applying verbs would refuse). `image push`/`mirror` are
deployment-scoped (they target the deployment's own ECR and write back its config); `image build` is a
workspace concern and stays on `tkr`.

`tkr` resolves which binary to run by **launch class**:

| Class | When | Binary | Verified against |
|-------|------|--------|------------------|
| **Bound** | normal versioned mutation | the recorded binary | recorded integrity manifest; gate must be `Match` |
| **Candidate-upgrade** | `upgrade` | operator/release-resolved B (the manifest still records A) | external CI/release/build metadata |
| **Dev-candidate** | apply to a `dev` deployment | the current local dev build | gate permits `DevIterate` |
| **Rollback** | `rollback` | bound B (undo), then retained A (reconcile) | B from the manifest; A from the checkpoint |

The split is load-bearing: `upgrade` cannot run the recorded binary (A cannot know how to advance to B),
and dev iteration cannot re-run the recorded dev binary. For **bound** mutations the mutating binary is
exactly the one the manifest records — that is the structural binding guarantee.

## Upgrade and rollback

The engine plans and applies create/update/delete; that machinery is unchanged and does the heavy
lifting. Upgrade and rollback add only orchestration on top of it.

**Binding is two-valued; an operation marker carries "in flight."** The recorded binding names exactly the
binary authorized to operate — `A` *or* `B`, never a third "pending" value. Whether an upgrade or
rollback is mid-flight is a separate **operation marker** (`UpgradeInFlight | RollbackInFlight | none`)
recording the phase and resumable progress (and, optionally, an ids-only audit change log — never
before-images). The marker — not the binding — gates the deployment to
`resume`/`rollback`/`describe` while an operation is open; the remote operation lock provides mutual
exclusion. Splitting these two concerns removes any ambiguous binding state.

**Upgrade transfers ownership atomically, *before* mutating.** The first act of `upgrade` is a single CAS
commit that, together: flips the binding to **B**, captures a clone of **[A final]** as the checkpoint
(A's snapshot + provenance + integrity manifest + config ref + retained-binary ref), and opens
`UpgradeInFlight`. Only after that commit does B touch the provider. So the recorded binding is *always*
the binary doing the work — a crash at any point recovers as B (recovery reads binding = B plus the open
marker, and resumes or rolls back); A never runs against B-shaped infrastructure. B then runs any
state-schema migration and applies its plan. It MAY record an **ids-only change log** (`id + op`) for
audit and richer `plan`/`describe` output — but rollback needs **no before-images**, because it restores
A's retained prior configuration revision, not an inverse of what B committed (Proposal 002). The marker
closes on success. Upgrade is the only verb that authoritatively advances the
recorded version; it refuses a downgrade, a same-semver/different-hash apply (a forgotten version bump),
an unbridged schema migration, and re-stamping back to `dev`.

**The rollback baseline is the retained prior configuration revision `R_a`** — a deterministic, hermetic
definition retained at upgrade (Proposal 002), not a set of recorded before-images. As a stricter option
an upgrade MAY first compare live against the recorded [A final] and **refuse-and-surface** material drift
("reconcile before upgrading"); this baseline gate is **advisory** — a cross-version consistency check,
never a licence for B to authoritatively reconcile A's drift, which would breach the authorship discipline.

**Rollback deletes B's creations, re-pins to A, then A forward-reconciles toward `R_a`** — two
operations, sharp division of responsibility (Proposal 002):

- **Undo — B deletes what it created.** B removes the resources it added (`keys(S_B) − keys(S_A)`,
  recorded as *ids only*), in reverse dependency order, fail-closed and idempotent (absent ⇒ done). This
  reuses the existing state-driven `delete` path, so it needs **no new capability and no before-images**.
  **Only B can** delete resources of **B-introduced kinds** — kinds A cannot even name (Req 10
  fail-closed) — which is precisely why B, not A, performs this step. B reads only its own recorded
  id-set, never A's state.
- **Re-pin + reconcile — A re-applies its retained revision over live.** Re-pinning to A is the symmetric
  atomic commit (binding → A, marker closed). A then runs `refresh_state` to **observe live provider
  truth itself** and forward-applies `R_a` — updating resources B modified back to `R_a`'s desired,
  re-creating resources B deleted (ordinary `create`; *new/empty* for stateful kinds — see limitations),
  and deleting anything still present but absent from `R_a` via the `known`-not-`desired` split. A never
  reinterprets B's recorded state; it observes shared live infrastructure and drives it to its own config.

This honors the (re-scoped) authorship invariant: B drives only deletions of ids it recorded; A drives
only its own config over live state it observes by `describe`; neither reinterprets the other's recorded
state representation. Migrations stay forward-only — rollback restores the retained prior revision, never
reverse-migrates.

**Resumable, not atomic at the provider.** Live infrastructure is not transactional: a delete can succeed
and the next fail, or the process can crash mid-sequence. Both upgrade and rollback check preconditions
before any destructive work, hold the remote operation lock for the whole sequence, and record progress
in the operation marker (phase + resumable step markers) so an interruption resumes rather than leaving
a half-applied deployment. Every step is idempotent. Because the binding already names the operating
binary, a recovering process always relaunches the correct one.

**Scope (decided).** Rollback covers **infrastructure *and* runtime/service state** (services + images),
not infra-only, spanning both `infra_head` and `runtime_head`. Under Proposal 002 this needs **no
state-driven-restore**: B deletes the services/images it created (which does require the deploy-engine
`Platform` to gain a **delete** for a service's running workload — the one genuinely-new runtime surface),
and A's reconcile re-applies `R_a`'s services through the existing forward `apply_manifests` path (apply
*is* the state-driven reconcile). No `Service` restore capability and no runtime before-images are
required — only a platform delete for B's delete-only undo.

**Honest limitations.** Rollback reverts to the checkpoint: state recorded under B after the upgrade is
not represented — it is recovery, not a merge. A resource B introduced of a kind it can no longer
instantiate (config drift since the upgrade) refuses rather than dropping silently. Resources created
entirely out-of-band — in no snapshot — stay orphaned, because no engine reconciles what it never
recorded; `describe`/`status` surface them. Rollback requires both binaries and A's checkpoint config;
S3 binary + config retention is what makes it self-contained.

## Operation safety

- **Deployment lock (mis-apply guard).** `tkr` separates a soft default (`use`) from a hard guard
  (`lock`). When a deployment is locked, every *mutating* command may target only the locked deployment;
  a command aimed elsewhere refuses before the launcher runs. The lock is a durable `lock.toml` (deployment
  name + a stable identity fingerprint: deployment id + state-backend identity — bucket/prefix/account/
  region) read at the start of every invocation, surviving sessions. The fingerprint deliberately
  **excludes** `source_tree_hash`, which changes on every upgrade; the recorded hash is advisory display
  only. Read verbs are never blocked. `unlock` requires confirmation.
- **Remote operation lock (concurrency).** Distinct from `lock.toml`: a renewable mutual-exclusion lease
  in the shared state store (S3 lease / explicit record; local filesystem lock), acquired around every
  mutating command and held continuously across a multi-phase `rollback`, so two provisioners (another
  workstation, CI) cannot make conflicting provider-side changes. CAS prevents silent state overwrite but
  not concurrent provider mutation; the lock does.
- **Fail-closed delete.** A Delete whose `ResourceId` is absent from `known` is an error, never a removal
  from state without deleting the live resource. State is removed only after `delete()` succeeds (or the
  resource is confirmed authoritatively absent).
- **Authoritative `describe`.** `describe` distinguishes authoritative-absent from not-implemented
  (`DescribeResult { Present | Absent | Unsupported }`); state is pruned only on `Absent`. On
  `Unsupported`, `delete()` is driven from persisted state (provider-NotFound treated as success), never a
  silent prune — so a stubbed `describe` cannot orphan a live resource.
- **Composition validation.** Before any plan/apply/destroy/rollback the engine validates the composition:
  unique module and resource ids, `desired ⊆ known`, every delete id present in `known`, dependencies
  present unless declared external, no cycles. A duplicate id would otherwise let the resource map route a
  delete to the wrong resource.
- **Day-0 bootstrap.** "Stamp before any resource" is impossible when the state store itself is a
  resource. The invariant is: the first committed deployment snapshot is stamped before any *non-state*
  managed resource is created — the remote-state bootstrap resource may come first, then the stamped empty
  snapshot, then everything else.
- **Threat model.** The integrity manifest is an accidental-integrity and provenance-binding control: it
  defends against corrupt downloads and wrong binaries, and assumes the state store is trusted for writes.
  It does not defend against an attacker who can rewrite the state manifest; stronger supply-chain
  resistance needs a CI-signed release manifest with a key embedded in `tkr`/`tkp` (a follow-on non-goal).

## Components and Interfaces

- **Provenance reader/writer** (`tokeira-state`) — stamps the running version into the state envelope;
  reads it back as `ProvenanceStamp` (concrete or `Unknown`).
- **Binding gate** (`tokeira-orchestrator`) — `check_binding(running, recorded)`; applying ops consult it
  and refuse on any non-`Match`/`DevIterate` verdict.
- **Integrity verifier** — computes/compares per-target `sha256` against the manifest; gates execution of
  any retrieved binary.
- **Launcher** (`tkr`) — resolves a binary by launch class, checksum-verifies, execs; performs no mutation
  itself.
- **Migration registry** (`tokeira-iac`) — forward-only migrations keyed by state-schema transition; run
  at the upgrade boundary, including the dev → versioned promotion.
- **Upgrade/rollback orchestration** (`tkp`) — atomic ownership transfer + checkpoint capture (incl. the
  prior configuration-revision ref) on upgrade; on rollback, drives B's delete-only undo of its creations,
  the atomic re-pin to A, and A's forward re-apply of the retained prior revision, under the operation
  lock and a resumable operation marker (Proposal 002).
- **Forward-engine replacement + destructive `plan` gating** (`tokeira-iac`) — the engine's diff/apply
  gains **replacement** (an immutable-field change becomes delete+recreate) and `plan` surfaces
  destructive changes requiring explicit `--yes`. These are **general apply features** (any immutable
  change needs them), and they are what makes forward-reconcile rollback correct without a per-kind
  restore surface. `Engine::destroy_selected` — a refs/id-set delete over the full state — is the
  delete-only primitive B uses to remove its creations. **Rollback spans runtime/service state**
  (services + images) as well as infra: B's delete-only undo requires the deploy-engine `Platform` to gain
  a **service delete**, while A's reconcile re-applies `R_a`'s services through the existing forward
  `apply_manifests` — no `Service` restore capability and no before-images (Proposal 002 supersedes the
  `apply_inverse_delta` / state-driven-restore approach of Proposal 001).
- **Fail-closed delete + authoritative `describe` + composition validation** (`tokeira-iac`) — the three
  framework correctness mechanics described above.
- **Remote operation lock** (`tokeira-state` + `tkp`) — renewable mutual-exclusion lease.
- **Deployment lock** (`tkr`) — the durable `lock.toml` mis-apply guard.
- **S3 binary store** (`tokeira-state` S3 backend) — optional persist/retrieve of the binary blob, verified
  via the integrity verifier.

## Data Models

- **ProvenanceStamp** — `{ version, git_sha, source_tree_hash, build_mode: BuildMode, recorded_at }`. A
  missing stamp is an explicit `Unknown`, never coerced to a concrete version.
- **BuildMode** — `Versioned | Dev`. Only `Versioned` is authoritative.
- **Target** — a Rust target triple (`aarch64-unknown-linux-musl`, `aarch64-apple-darwin`, …); `os/arch`
  alone is not precise enough for an executable artifact.
- **BinaryArtifactDescriptor** — `{ version, target: Target, sha256, retrieval_ref: Option<String>,
  size_bytes }`.
- **IntegrityManifest** — `{ provisioner_version, artifacts: Vec<BinaryArtifactDescriptor> }`. CAS-guarded,
  cannot be silently rewritten.
- **DescribeResult** — `Present(ResourceState) | Absent | Unsupported`. Replaces `Option<ResourceState>` so
  the engine distinguishes provider-absent (prune-safe) from not-implemented (must not prune).
- **MigrationRegistry** — ordered `Migration { from_schema: u32, to_schema: u32, apply }`, keyed by state
  schema, forward-only. A new `source_tree_hash` at the same schema needs no migration.
- **ChangeLog** (optional, audit only) — an **ids-only** record of what an apply committed: per change,
  `{ id, op: Created | Updated | Deleted }`, **no before-images**. Recorded for observability and richer
  `plan`/`describe` output; it is **never** the rollback mechanism (Proposal 002, Decision 4). Rollback is
  driven by the retained prior configuration revision, not by inverting a recorded delta.
- **RollbackCheckpoint** — the cloned **[A final]**: `{ from_provenance: ProvenanceStamp,
  from_integrity: IntegrityManifest, from_snapshot: SnapshotRef, from_config_ref: Option<String>,
  recorded_at }`. Captured atomically at the start of `upgrade`. Carries the full prior integrity manifest
  (all targets — rollback may run from a different operator platform) and A's config ref — which is now
  **load-bearing**: rollback restores A's prior configuration revision from `from_config_ref` and
  forward-reconciles toward it (Proposal 002).
- **Operation** — the in-flight marker: `{ operation_id, kind: UpgradeInFlight | RollbackInFlight,
  phase, progress }` (resumable step markers; optionally an ids-only `ChangeLog`, never before-images).
  While present it gates the deployment to
  `resume`/`rollback`/`describe`; it records progress so an interrupted upgrade or rollback resumes; it
  closes on success. `None` in steady state.
- **OperationLock** — `{ holder, acquired_at, renewed_at, expires_at, operation }`. The remote
  mutual-exclusion lease (distinct from the operator `lock.toml`).
- **DeploymentStateEnvelope** — the single deployment-level authority: `{ schema_version, deployment_id,
  binding: ProvenanceStamp, integrity, config_revision: u64, checkpoint: Option<RollbackCheckpoint>,
  operation: Option<Operation>, lock: Option<OperationLock>, infra_head: SnapshotRef,
  runtime_head: SnapshotRef, effective_config_ref: Option<String> }`. `binding` is **two-valued in
  practice** — it names exactly the binary authorized to operate (`A` or `B`), flipped atomically by
  `upgrade`/`rollback`, never a third "pending" value; whether an operation is mid-flight is the separate
  `operation` marker. `binding` is the engine identity (changes only on upgrade); `config_revision` +
  `effective_config_ref` are the desired-state identity (incremented by each ordinary `apply`). The two
  are orthogonal — `describe` reports both. **The envelope is the manifest of `S3StateStore`** (the
  authoritative remote, snapshot/lease store): its `SnapshotRef` heads, immutable [A final] checkpoint, and
  `OperationLock` lease are that store's native primitives, which the single-doc `CasStore` cannot hold.
  `CasStore`-over-`LocalBackend` remains the local/compose dev path; **ECS employs remote-state**
  (`S3StateStore`), replacing its `CasStore`-over-`S3Backend` single-doc stopgap. The engine state seam is
  abstracted over both stores so a platform selects its store, not just its backend; the operator-facing
  `tkr remote-state` toggle is **held** — store choice stays platform-determined (task 13).

## Binary artifact: size and storage

A complete provisioner links ~14 AWS SDK service clients (`ec2`, `ecs`, `eks`, `iam`, `s3`, `autoscaling`,
`elasticloadbalancingv2`, `dynamodb`, `dsql`, `ecr`, `secretsmanager`, `servicediscovery`, `ssm`, `sts`)
plus tokio/rustls/hyper/serde, with `aws-sdk-ec2` dominant. Under the current release profile
(`lto = "fat"`, `codegen-units = 1`, `strip = "symbols"`, `panic = "abort"`) a realistic estimate is
~50–80 MB. Levers: trimming linked clients to those a build's platforms use is the largest reduction;
`opt-level = "z"` trades speed for size; UPX yields ~20–35 MB at a cold-start and scanner-trust cost (not
recommended by default for a cloud-privileged binary). The exact figure is measured and recorded.

Integrity metadata (version + per-target `sha256` + optional `retrieval_ref`) is always in the CAS-guarded
manifest — the trust anchor regardless of where the blob lives. For S3 state the binary blob may also be
co-located (keyed by version + target), making the deployment self-contained. Trust never flows from the
stored blob: a retrieved binary is verified against the manifest `sha256` before execution. The manifest
records checksums for all built targets even when only one blob is co-located.

## Correctness Properties

### Property 1: Provenance round-trips

*For any* state envelope the provisioner writes, reading it back SHALL yield a parseable `ProvenanceStamp`
carrying the writing provisioner's version, git SHA, and source-tree hash.

**Validates: Requirements 1.1, 1.2**

### Property 2: A non-matching binding is never silently mutated

*For any* running binary whose binding verdict is not `Match` (versioned) or `DevIterate` (dev), an
applying operation SHALL NOT mutate; resolution requires the matching binary or a deliberate upgrade, with
no override.

**Validates: Requirements 2.1, 2.2**

### Property 3: Checksum gate before execution

*For any* binary obtained to manage a deployment, IF its `sha256` does not equal the manifest descriptor
for its target, THEN it SHALL NOT be executed and the operation SHALL abort.

**Validates: Requirements 3.3, 5.3, 7.2**

### Property 4: No downgrade

*For any* running version older than the deployment's recorded version (by monotonic version/build
identity, never by hash), the provisioner SHALL refuse to operate.

**Validates: Requirements 4.2**

### Property 5: Missing provenance is unknown, not a match

*For any* state lacking a provenance stamp, the comparison against a concrete running binary SHALL evaluate
to `Unknown` and fail closed, never to a match.

**Validates: Requirements 1.3, 2.2**

### Property 6: Source-tree drift is detected regardless of version label

*For any* two builds whose workspace source trees differ, their `source_tree_hash` SHALL differ and
`check_binding` SHALL NOT yield `Match` — even when their `version` label is identical.

**Validates: Requirements 1.4, 2.1**

### Property 7: A dev-mode stamp never authoritatively binds

*For any* `dev`-mode stamp, `check_binding` SHALL NOT yield `Match`: a dev binary on a dev deployment is
`DevIterate` (permissive, re-stamp + warn, non-authoritative); a dev binary on a versioned deployment is
`ModeRegression` (refuse). Only a versioned binary on a matching versioned deployment is `Match`.

**Validates: Requirements 1.5, 2.2, 2.3**

### Property 8: A lock confines mutation to the locked deployment

*For any* active deployment lock and *any* mutating command targeting a deployment other than the locked
one (by identity), the command SHALL refuse before launching `tkp`; *for any* read command, the lock SHALL
NOT block it.

**Validates: Requirements 8.2, 8.3**

### Property 9: Rollback forward-reconciles toward the retained prior revision; authorship preserved

*For any* `rollback` (Proposal 002): the superseded binary B **deletes the resources it created**
(`keys(S_B) − keys(S_A)`, ids only, fail-closed, idempotent) — B alone can name its own kinds, and delete
is already state-driven, so no before-images are read; the binding then re-pins to A atomically; the prior
binary A observes live state via `refresh_state` and **forward-applies its retained prior configuration
revision `R_a`**, reconciling B's updates and re-creations from A's own config. Neither binary
reinterprets the other's recorded state representation (A may observe shared live infrastructure it can
`describe`). `rollback` aborts if either binary's `sha256` mismatches, holds the operation lock across the
sequence, and resumes from a durable marker after interruption; every step is idempotent.

**Validates: Requirements 9.2, 9.3, 9.7**

### Property 15: Ownership transfer is atomic; the binding always names the operating binary

*For any* `upgrade`, the binding flips to B in a single commit (with the [A final] checkpoint captured and
the operation marker opened) *before* any provider mutation; *for any* crash during upgrade or rollback,
recovery reads a binding that names exactly the binary that was operating — there is no "pending" binding
under which a different binary could run. (Symmetrically, `rollback` flips the binding to A atomically.)

**Validates: Requirements 4.5**

### Property 10: A Delete never silently orphans

*For any* Delete whose `ResourceId` is absent from `known`, the framework SHALL error and SHALL NOT remove
it from state; *for any* resource whose `describe()` is `Unsupported`, the framework SHALL NOT prune it. A
Delete either invokes `delete()` or fails — it never drops state while leaving the live resource.

**Validates: Requirements 10.1, 10.2, 10.3**

### Property 11: Mutations are mutually exclusive

*For any* two concurrent mutating commands against the same deployment, at most one holds the remote
operation lock; the second refuses or waits before any provider-side work.

**Validates: Requirements 11.1, 11.2**

### Property 12: Composition validation precedes any mutation

*For any* composition with a duplicate `ResourceId`, a `desired` resource absent from `known`, a delete id
absent from `known`, or a dependency cycle, the engine SHALL refuse before computing or applying a Delta.

**Validates: Requirements 12.1**

### Property 13: The deployment lock survives a versioned upgrade

*For any* deployment lock and *any* `upgrade` that changes the deployment's `source_tree_hash`, the lock
SHALL remain valid — its fingerprint excludes the hash — so a legitimate upgrade never reads as "locked
identity changed."

**Validates: Requirements 8.4**

### Property 14: Configuration refinement does not change the engine binding

*For any* `apply` that changes only the deployment's configuration (not the engine), the binding verdict
SHALL remain `Match`, the recorded engine `source_tree_hash` SHALL be unchanged, no new `tkp` version is
required, and the `config_revision` SHALL advance. Reverting to a prior config revision is a same-engine
`apply`, never an `upgrade` or a two-binary rollback.

**Validates: Requirements 13.1, 13.2**

## Error Handling

| Condition | Handling |
|-----------|----------|
| `deployment create` | Always stamps before any non-state resource (Day-0 versioning); `build_mode` reflects the building binary (Req 1.2). |
| Missing/`Unknown` provenance on existing state | Cannot arise from a tokeira-created deployment; treated as corrupt/foreign and failed closed (Req 1.3). |
| Dev deployment + dev binary (`DevIterate`) | Apply proceeds, re-stamps the advisory dev stamp, warns non-authoritative (Req 2.3). |
| Versioned deployment, non-matching binary | Refuse; resolve by the matching binary or `upgrade`. A dev binary against a versioned deployment (`ModeRegression`) is refused outright (Req 2.2). |
| Running version < recorded | Refuse; surface downgrade — no override (Req 4.2). |
| Same semver, different `source_tree_hash` | Refuse a normal apply (forgotten version bump); not an ordered upgrade (Req 4.4). |
| Retrieved binary checksum ≠ manifest | Abort before execution (Req 3.3, 5.3, 7.2). |
| Upgrade with an unbridged state-schema migration | Refuse; surface the gap (Req 4.3). |
| `rollback`, missing checkpoint or either binary unavailable/checksum-mismatched | Refuse; instruct the operator to supply the matching binary (Req 9.3). |
| `rollback`, a B-authored resource kind B can no longer instantiate | Refuse (fail closed); surface it rather than dropping it (Req 9.6). |
| `upgrade` begins | First act is one atomic CAS commit: binding → B, capture [A final] checkpoint, open the operation marker — *before* any provider mutation; a crash thereafter recovers as B, never A (Req 4). |
| `upgrade`, live diverges from recorded [A final] | Advisory baseline gate MAY refuse-and-surface ("reconcile before upgrading"); it never lets B authoritatively reconcile A's drift (Req 4). |
| `upgrade` or `rollback` interrupted | Resume from the operation marker (`phase` + resumable step markers; every step idempotent); the binding already names the operating binary, so recovery relaunches the correct one (Req 9.7). |
| Delete id absent from `known` | Error; never remove from state without deleting the live resource (Req 10.1). |
| `describe()` `Unsupported` | Do not prune; drive `delete()` from persisted state or fail (Req 10.3). |
| Two concurrent mutations of one deployment | Second refuses/waits on the remote operation lock (Req 11). |
| Composition has a duplicate id / cycle / `desired ∉ known` | Refuse before any Delta (Req 12). |
| Mutating command targets a non-locked deployment, or a stale/changed-identity lock | Refuse before launching `tkp` (Req 8). |
| Resource created out-of-band (in no snapshot) | Not reconciled by rollback; surfaced via `describe`/`status` as drift (Req 9.5). |
| CAS conflict writing the envelope | Re-read and retry per `tokeira-state` CAS; never force-overwrite. |

## Testing Strategy

- **Unit:** stamp serialization round-trip; manifest descriptor encode/decode; binding verdict over all
  mode/hash pairs (incl. `Unknown`); checksum verify pass/fail; forward-engine replacement (immutable-field
  change → delete+recreate) and destructive-change gating in `plan`; delete-only `destroy_selected` over an
  id set.
- **Property (proptest):** Properties 1–15, tagged to their requirements.
- **Engine/config separation:** a config-only `apply` keeps the binding `Match` and the engine
  `source_tree_hash` unchanged while advancing `config_revision`; reverting to a prior config revision is a
  same-engine apply (no upgrade, no two-binary rollback).
- **Integration (no live AWS):** against the in-memory CAS store — write-stamp → reopen-with-changed-hash
  → assert gate; persist + retrieve a binary blob and assert verify/abort; atomic-ownership crash recovery
  (inject a crash immediately after `upgrade`'s first CAS commit but before any provider mutation, then
  assert recovery resolves the binding to B and never A — Property 15); rollback dependency-state test
  (a reverted resource reads dependency state from the full snapshot); resumable rollback (crash mid-undo
  resumes from the marker); two-process remote-lock test (second mutation refuses before provider work);
  fail-closed delete and `Unsupported`-describe never prune.
- **Launcher:** `upgrade` launches candidate B (not recorded A); a dev deployment is applied by a
  different local dev candidate; a bound mutation launches exactly the recorded binary; checksum mismatch
  aborts before exec.
- No tests require live AWS credentials or network.
