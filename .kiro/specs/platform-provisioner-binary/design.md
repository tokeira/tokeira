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
4. **Rollback is undo-the-plan, not reverse-migration.** An upgrade is a recorded plan; rollback applies
   its inverse (by the binary that authored it), then the prior binary re-asserts its config.

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
| `rollback` — undo | inverse of B's recorded upgrade plan | live (`S_B`) | superseded B |
| `rollback` — reconcile | checkpoint (A) config → resources | restored `S_A` | prior A |

One invariant governs *which binary* applies a given Delta:

> **A binary reverses only the changes it made, and reads only state it authored.**

This is why `rollback` is two operations: an upgrade plan applied by B can only be reversed by B (the same
resource implementations and dependency versions that applied a change are required to reverse it), and
the prior binary A re-asserts its own config afterward. Identity-key arithmetic (`keys(S_B) − keys(S_A)`)
is the only permitted cross-version comparison, because it compares `ResourceId`s, never state bodies.

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

**This also bounds the heavy rollback.** The two-binary inverse-plan rollback is needed only across an
*engine* transition. Reverting a bad *configuration* change is an ordinary same-engine `apply` of the
prior config revision — one binary, no checkpoint, no inverse-plan — because the engine is unchanged and
owns the whole plan. (A `tkp` verb may revert to a recorded prior config revision directly.)

**The boundary, stated honestly.** This works to the extent platform definition is *configuration (data)*
rather than *compiled code*. A genuine resource-*implementation* change — a new resource kind, changed
apply logic, a dependency/SDK bump — is a real engine change and correctly mints a version; that is
exactly what the binding and rollback fidelity protect. The design's lever is therefore to express
platform desired-state definition declaratively (as config the bound binary reads) wherever possible, so
that everyday refinement is a plan rather than a rebuild; reserve `tkp` rebuilds for behavioral changes;
and use a `dev` deployment for active platform-*code* development.

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

**Upgrade** records a `RollbackCheckpoint`, runs any migration the *state-schema* transition requires
(not every source change — a new digest at the same schema is a re-stamp), advances the recorded stamp,
and applies B's plan. It is the only verb that authoritatively advances the recorded version. It refuses a
downgrade, a same-semver/different-hash apply (a forgotten version bump), an unbridged schema migration,
and re-stamping a deployment back to `dev`.

**Rollback** is *undo the upgrade's plan, then reconcile* — two operations with a sharp division of
responsibility:

- **Undo — B reverses *all* of its own changes.** The upgrade was a plan; B applies its inverse: delete
  what it created, revert what it updated (to the checkpoint state), and re-create what it deleted (from
  the checkpoint state). **Only B can.** Reversing a change requires the same resource implementation and
  dependency versions that applied it — if B updated a resource through, say, a newer AWS SDK crate than A
  carries, A's older code may be unable to read, diff, or revert it. B is therefore responsible for the
  complete inverse of its plan, restoring live infrastructure *and* state to the checkpoint `S_A`. It runs
  before the re-pin (after which B is refused).
- **Reconcile — A re-asserts its own config.** Re-pinned, A runs its ordinary plan/apply against the
  restored checkpoint, confirming its authority and converging any residual difference between the
  checkpoint and A's current config. After B's full undo this is typically a confirming pass; A only ever
  operates on checkpoint state A authored, never on a B-shaped resource.

This honors the authorship invariant on both sides: B reverses only what B did; A re-asserts only A's
config. Migrations stay forward-only — rollback restores the retained checkpoint, never reverse-migrates.

**Resumable, not atomic.** Live infrastructure is not transactional: a delete can succeed and the next
fail, or the process can crash between the undo and the re-pin. So rollback checks all preconditions
before any destructive work, holds the remote operation lock across the whole undo → re-pin → reconcile
sequence, and records progress in a durable `RollbackOperation` marker so an interruption resumes rather
than leaving a half-rolled-back deployment.

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
- **Upgrade/rollback orchestration** (`tkp`) — records the checkpoint and the forward plan on upgrade;
  applies the inverse plan (B) and the reconcile (A) on rollback, under the operation lock and a resumable
  marker.
- **`Engine::apply_inverse_plan`** (`tokeira-iac`) — applies the inverse of a recorded plan: delete
  recorded creates, revert recorded updates to their prior state, re-create recorded deletes from their
  prior state, over the full current state in inverse dependency order. (`Engine::destroy_selected` — a
  refs/id-set delete over the full state — is the delete-only sub-case.)
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
- **RecordedPlan** — the forward Delta an `upgrade` applied, with before-images sufficient to invert
  (full prior `ResourceState` for updated and deleted resources). Inverting yields the rollback undo.
- **RollbackCheckpoint** — `{ from_provenance: ProvenanceStamp, from_integrity: IntegrityManifest,
  from_snapshot: SnapshotRef, from_config_ref: Option<String>, recorded_plan: RecordedPlan, recorded_at }`.
  Carries the full prior integrity manifest (all targets — rollback may run from a different operator
  platform) and A's config ref (A re-derives its desired from its own config).
- **RollbackOperation** — `{ operation_id, phase: RollbackPhase, reverted: BTreeSet<ResourceId> }`. The
  durable resumability marker.
- **OperationLock** — `{ holder, acquired_at, renewed_at, expires_at, operation }`. The remote
  mutual-exclusion lease.
- **DeploymentStateEnvelope** — the single deployment-level authority: `{ schema_version, deployment_id,
  provenance, integrity, config_revision: u64, rollback: Option<RollbackCheckpoint>,
  lock: Option<OperationLock>, infra_head: SnapshotRef, runtime_head: SnapshotRef,
  effective_config_ref: Option<String> }`. `provenance` is the engine identity (changes only on an
  engine upgrade); `config_revision` + `effective_config_ref` are the desired-state identity (incremented
  by each ordinary `apply`). The two are orthogonal — `describe` reports both. Reconciling this envelope
  with the active store path is the largest open decision (see Notes in tasks).

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

### Property 9: Rollback reverses only the changes each binary made

*For any* `rollback`: the superseded binary B applies the inverse of its recorded upgrade plan (delete its
creates, revert its updates to the checkpoint state, re-create its deletes from the checkpoint state) over
state B authored; the prior binary A applies only its own config over the restored checkpoint; neither
binary reverses a change it did not make. `rollback` aborts if either binary's `sha256` mismatches, holds
the operation lock across the sequence, and resumes from a durable marker after interruption.

**Validates: Requirements 9.2, 9.3, 9.7**

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
| `rollback` interrupted | Resume from the `RollbackOperation` marker (`phase`, `reverted`); never half-applied silently (Req 9.7). |
| Delete id absent from `known` | Error; never remove from state without deleting the live resource (Req 10.1). |
| `describe()` `Unsupported` | Do not prune; drive `delete()` from persisted state or fail (Req 10.3). |
| Two concurrent mutations of one deployment | Second refuses/waits on the remote operation lock (Req 11). |
| Composition has a duplicate id / cycle / `desired ∉ known` | Refuse before any Delta (Req 12). |
| Mutating command targets a non-locked deployment, or a stale/changed-identity lock | Refuse before launching `tkp` (Req 8). |
| Resource created out-of-band (in no snapshot) | Not reconciled by rollback; surfaced via `describe`/`status` as drift (Req 9.5). |
| CAS conflict writing the envelope | Re-read and retry per `tokeira-state` CAS; never force-overwrite. |

## Testing Strategy

- **Unit:** stamp serialization round-trip; manifest descriptor encode/decode; binding verdict over all
  mode/hash pairs (incl. `Unknown`); checksum verify pass/fail; inverse-plan construction from a recorded
  plan.
- **Property (proptest):** Properties 1–14, tagged to their requirements.
- **Engine/config separation:** a config-only `apply` keeps the binding `Match` and the engine
  `source_tree_hash` unchanged while advancing `config_revision`; reverting to a prior config revision is a
  same-engine apply (no upgrade, no two-binary rollback).
- **Integration (no live AWS):** against the in-memory CAS store — write-stamp → reopen-with-changed-hash
  → assert gate; persist + retrieve a binary blob and assert verify/abort; rollback dependency-state test
  (a reverted resource reads dependency state from the full snapshot); resumable rollback (crash mid-undo
  resumes from the marker); two-process remote-lock test (second mutation refuses before provider work);
  fail-closed delete and `Unsupported`-describe never prune.
- **Launcher:** `upgrade` launches candidate B (not recorded A); a dev deployment is applied by a
  different local dev candidate; a bound mutation launches exactly the recorded binary; checksum mismatch
  aborts before exec.
- No tests require live AWS credentials or network.
