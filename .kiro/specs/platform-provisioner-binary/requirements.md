# Requirements Document

## Introduction

Tokeira provisions infrastructure through an extensible IaC framework (`tokeira-iac`,
`tokeira-deploy-engine`, `tokeira-state`, `tokeira-orchestrator`, `tokeira-aws`) on which the platforms
are built. Today nothing binds a provisioned deployment to the exact code that produced it: a change to a
resource implementation can silently re-interpret existing state and drift live infrastructure on the
next apply.

This spec adopts a **complete platform-provisioner binary married to the deployment**. The provisioner —
**`tkp`**, a small-form sibling of `tkr` — is a standalone, optimized binary that owns the IaC engine,
the platforms, and the AWS resource implementations. `tkr` remains the operator's global, version-current
front door with its existing command structure; for lifecycle verbs it launches the deployment's bound
`tkp` (checksum-verified) rather than mutating directly. Each deployment's remote state records the
provisioner version that may manage it; a mismatch is gated, never silently applied; the bound binary's
identity is recorded tamper-evidently so it can be verified; version changes happen only at a deliberate
upgrade/migration boundary; and an operator can **lock** a deployment so that `tkr`'s power cannot land a
change on the wrong environment.

**Versioning is mandatory from Day 0.** There is no unstamped state and no opt-out: creation always
stamps, so there is never a legacy deployment to "adopt" (the spec has no `adopt` verb). Because the
first real platform bring-up (ECS) will churn the source tree through many fix-and-reapply cycles,
strictness is conditioned on the deployment's recorded build mode: a **dev deployment** iterates freely
(a `dev` binary re-applies and re-stamps with a non-authoritative warning), while a **versioned
deployment** is strict (apply requires a source-tree match; drift goes through `upgrade`). Versioning is
universal; the gate never strangles the dev bring-up loop.

Scope here is the **minimal foundational set**: provenance, binding, integrity, the migration boundary,
snapshot-based upgrade rollback, and optional binary retention for S3 remote state. Heavier mechanisms
(automated **binary self-update** with atomic swap and its own rollback, release signing infrastructure
and key management, and the single-shared-binary vs provisioner-as-SDK multi-consumer decision) are
explicit non-goals here and are deferred to follow-on specs. Note the distinction: *deployment-upgrade*
rollback (reverting a deployment's recorded version + state to a retained checkpoint) is in scope;
*binary self-update* rollback (a binary swapping itself) is not.

## Glossary

- **Provisioner (`tkp`)** — the standalone optimized binary (small-form sibling of `tkr`) containing the
  IaC engine, the platforms, and the AWS resource implementations; the only artifact that mutates a
  deployment's infrastructure. Carries a specialized lifecycle CLI; operators normally reach it through
  `tkr`, which forwards lifecycle verbs to it.
- **Operator cockpit (`tkr`)** — the global, version-current operator CLI: deployment registry,
  developer/CI/compatibility/workstation tasks, workspace image builds, and the launcher that resolves
  and executes a deployment's bound provisioner. Never mutates deployment infrastructure directly.
- **Launcher** — the `tkr` seam that, for a deployment-lifecycle action, resolves the bound provisioner,
  verifies its checksum against the integrity manifest, and executes it.
- **Selection** — the soft default deployment `tkr` targets when `--deployment` is omitted (set by
  `tkr deployment use`).
- **Deployment lock** — a durable, cross-session guard (name + identity fingerprint) that confines every
  mutating `tkr` command to one deployment; set by `tkr deployment lock`, cleared by `tkr deployment
  unlock`.
- **Deployment** — a provisioned set of resources tracked by remote state.
- **Provenance stamp** — the provisioner version (semver + git SHA + whole-tree source digest + build
  mode) recorded in a state document.
- **Source-tree hash** — a deterministic digest over the entire workspace source tree
  (`tokeira-build-info::SOURCE_TREE_HASH`); the authoritative key for detecting provisioner drift.
- **Build mode** — `versioned` (manifest build, authoritative provenance) or `dev` (local fallback,
  non-authoritative).
- **Dev deployment / versioned deployment** — a deployment whose *recorded* stamp is `dev` (iteration
  regime: a `dev` binary may re-apply freely, re-stamping with a warning) or `versioned` (strict regime:
  apply requires a source-tree `Match`; drift goes through `upgrade`).
- **DevIterate** — the binding verdict for a `dev` binary against a `dev` deployment: a permissive,
  non-authoritative apply that re-stamps and warns (the platform bring-up loop).
- **Binding** — the association between a deployment's state and the provisioner version permitted to
  manage it.
- **Integrity manifest** — the CAS-guarded record of provisioner version plus per-target content
  checksums and an optional retrieval reference.
- **Migration boundary** — the deliberate version-transition point at which state is migrated forward.
  Migrations are forward-only.
- **Rollback checkpoint ([A final])** — the prior state cloned atomically at the start of `upgrade`
  (prior snapshot, provenance, full integrity manifest, config ref) so a later `rollback` can re-pin to
  it and forward-reconcile toward its **configuration revision** (the `config ref`, now load-bearing).
  Rollback restores by re-applying that retained revision, not by inverting recorded before-images
  (Proposal 002).
- **Applied delta** — an optional, ids-only **audit** record (`id + op`, no before/after images) of the
  changes an apply/upgrade committed, kept for observability and richer `plan`/diff. Under Proposal 002
  it is **not** the rollback mechanism (rollback re-applies the retained configuration revision) and does
  not carry before-images.
- **Binding / operation marker** — the binding names the single binary authorized to operate (`A` or `B`,
  flipped atomically, never "pending"); the separate operation marker records an in-flight
  upgrade/rollback (phase + resumable progress) and gates resume/rollback until it closes.
- **Rollback** — the deliberate revert to [A final] by **forward reconciliation toward the retained
  configuration revision**, never by inverting recorded before-images and never a reverse migration
  (Proposal 002). For an engine-identity upgrade it is two operations: the superseded binary **deletes
  what it created** (`keys(S_B) − keys(S_A)` — it alone can remove resources of kinds it introduced;
  deletion is state-driven), the binding is atomically re-pinned to the prior binary, and that binary
  observes live (`refresh_state`) and re-applies its own configuration revision to reconcile the
  remainder. For a same-engine configuration change it is a single ordinary `apply` of the prior revision.
- **Fail-closed deletion** — the framework rule that a Delete whose resource is absent from the `known`
  set errors rather than silently dropping it from state; closes a latent silent-orphan footgun, and is
  relied on by rollback's undo.
- **Delta** — the framework's single mutation primitive: the set of resource changes
  (`Create | Update | Delete | NoChange`) computed by comparing a desired shape against a current state
  over a binary's `known` universe. `plan` previews a Delta; `apply` enacts it; `upgrade` and `rollback`
  are compositions of Deltas over different `(desired, state, binary)` triples.
- **Authorship invariant** — no binary computes or applies a Delta over a *state representation* authored
  by a different version; a binary MAY observe shared **live** infrastructure via `describe`/`refresh_state`
  and reconcile toward its own configuration revision (Proposal 002). Identity-key arithmetic
  (`keys(S_B) − keys(S_A)`) bounds the resources the superseded binary must delete on rollback.
- **Engine identity** — the binding `source_tree_hash`, computed over the engine/resource-implementation
  surface only; changes only on a behavioral code change, and is the sole thing requiring `upgrade`.
- **Configuration revision** — the deployment's desired-state definition (modules, parameters, resource
  definitions, image refs, scaling) as runtime data, recorded as a monotonic revision; refined freely by
  ordinary `apply`, orthogonal to the engine identity.
- **Launch class** — which binary `tkr` runs for a lifecycle action: **bound** (the recorded binary, for
  normal versioned mutations), **candidate-upgrade** (operator/release-resolved B, verified externally),
  **dev-candidate** (the current local dev build), or **rollback** (B then retained A).
- **Remote operation lock** — a renewable mutual-exclusion lease in the shared state store, serializing
  concurrent mutations; distinct from the local `lock.toml` mis-apply guard.
- **Deployment state envelope** — the single deployment-level authority binding provenance, integrity,
  rollback checkpoint, infra+runtime state, and snapshot refs under one revision.
- **Target** — an (operating system, architecture) pair a provisioner binary is built for.
- **Remote state** — the persisted deployment state (`tokeira-state`: CAS store, or S3 store).

## Requirements

### Requirement 1: Provisioner provenance in state (mandatory from Day 0)

**User Story:** As an operator, I want every deployment versioned from its first moment, so that I can
always tell what is managing a deployment and detect code drift, with no unstamped escape hatch.

#### Acceptance Criteria

1. WHEN the provisioner writes or updates a state document, THEN it SHALL record its own version (semver +
   git SHA + source-tree digest + build mode) in that document.
2. WHEN remote state is initialized for a new deployment, THEN the **first committed deployment state
   snapshot SHALL be stamped before any non-state managed resource is created**; the remote-state
   bootstrap resource (e.g. the state bucket) MAY be created first, then the stamped empty snapshot
   committed, then all other resources proceed — so no deployment with real managed infrastructure ever
   exists without provenance, without requiring the impossible "stamp before the state store exists."
3. WHERE a state document carries no stamp (only possible from corruption or a foreign writer, since
   creation always stamps), THE provisioner SHALL treat it as `Unknown` and fail closed — applying
   commands refuse — rather than assuming it matches the running version.
4. WHEN the provisioner records provenance, THEN the stamp SHALL comprise the version (semver), the git
   SHA, the whole-tree source digest (`source_tree_hash`), and the build mode, AND the source digest
   SHALL be the authoritative key for drift comparison, so that a change to any constituent crate is
   detected even when the version label is unchanged.
5. WHERE a deployment's recorded build mode is `dev`, THE provisioner SHALL treat its provenance as
   non-authoritative (no no-drift guarantee) and SHALL NOT evaluate it as an authoritative `Match`;
   advancing it to authoritative status SHALL occur only through a deliberate upgrade with a `versioned`
   binary.

### Requirement 2: Deployment binding gate (regime depends on build mode)

**User Story:** As an operator, I want a versioned deployment protected from mutation by any non-matching
binary — with no override — while a dev deployment stays ergonomic to iterate on during platform bring-up,
so that safety is strict where it matters and friction is absent where it would only obstruct.

#### Acceptance Criteria

1. WHEN an applying operation (`apply`, `destroy`, `scale`, `schema setup`, `image push`/`mirror`)
   begins, THEN the provisioner SHALL compare its build mode and source-tree digest to the deployment's
   recorded provenance before any mutation.
2. WHERE the recorded deployment is `versioned` AND the running binary does not match it, THE provisioner
   SHALL surface the mismatch and SHALL refuse to apply mutations, AND no flag SHALL override that
   refusal; resolution is the matching `versioned` binary or a deliberate upgrade. A `dev` running binary
   against a `versioned` deployment SHALL be refused outright (no regression to an unversioned manager).
3. WHERE the recorded deployment is `dev` AND the running binary is `dev`, THE provisioner SHALL permit
   the apply (the bring-up iteration path), re-stamp the recorded dev stamp to the running one, and emit a
   visible non-authoritative warning; this SHALL NOT require an upgrade.
4. WHEN a `versioned` running binary matches a `versioned` recorded deployment, THEN the operation SHALL
   proceed under the normal plan-confirm-apply flow.
5. WHEN a read-only plan (`plan`) runs under any non-`Match` binding, THEN the provisioner SHALL surface
   the verdict and render the plan annotated as non-matching, WITHOUT refusing and WITHOUT mutating.

### Requirement 3: Integrity manifest

**User Story:** As an operator, I want the bound provisioner's identity and checksum recorded in
tamper-evident state, so that a binary retrieved to manage the deployment can be verified before
execution.

#### Acceptance Criteria

1. WHEN the provisioner stamps provenance, THEN it SHALL record its version and a content checksum per
   built target in the CAS-guarded manifest.
2. WHERE a retrieval reference for the binary is known, THE provisioner SHALL record it in the manifest.
3. WHEN a provisioner binary is obtained to manage a deployment, THEN its checksum SHALL be verified
   against the manifest before execution, AND a mismatch SHALL abort the operation.

### Requirement 4: Upgrade and migration boundary

**User Story:** As an operator, I want version changes to happen only at a deliberate upgrade step that
migrates state forward, so that upgrades are controlled rather than implicit.

#### Acceptance Criteria

1. WHEN an operator performs a provisioner upgrade for a deployment, THEN the new version SHALL run any
   registered migration **keyed by state-schema transition** (`from_schema → to_schema`) forward before
   any mutation; a new `source_tree_hash` with the *same* state schema requires no migration (re-stamp
   after the gate).
2. IF the running version is older than the deployment's recorded version, THEN the provisioner SHALL
   refuse to operate and SHALL surface the downgrade. Ordering ("older/newer") SHALL be decided by a
   monotonic version/build identity, NEVER by `source_tree_hash` (a hash is an equality key, not an
   ordering key).
3. WHERE no migration is registered for a state-schema transition that the upgrade requires, THE
   provisioner SHALL refuse and surface the gap; where the state schema is unchanged it SHALL treat the
   transition as identity.
4. WHERE the running binary has the same recorded version (semver) but a different `source_tree_hash`,
   THE provisioner SHALL refuse a normal apply (a forgotten version bump) rather than treat it as an
   ordered upgrade.
5. WHEN `upgrade` begins, THEN its first act SHALL be a single atomic commit that transfers the binding to
   the candidate binary B, captures the prior **[A final]** as the rollback checkpoint, and opens an
   operation marker — *before* any provider mutation — so that the recorded binding always names the
   binary performing work and a crash recovers under B, never A. The binding SHALL be two-valued (the
   bound binary, A or B), never a third "pending" value; whether an operation is in flight is the separate
   operation marker.
6. AS B applies its plan, THEN it MAY record an **applied delta** — an ids-only audit log (`id + op`, no
   before/after images) of the changes it commits, for observability and `plan`/diff. Rollback does not
   consume it: rollback re-applies the retained prior **configuration revision** (Requirement 9,
   Proposal 002), not an inverse of recorded before-images.
7. WHERE the running binary, before upgrading, finds live state materially diverged from the recorded
   [A final], THE provisioner MAY refuse-and-surface the drift (an advisory baseline gate); it SHALL NOT
   authoritatively reconcile another version's drift.

### Requirement 5: Binary retention for S3 remote state

**User Story:** As an operator using S3 remote state, I want the bound provisioner binary retained with
the deployment, so that I can manage or destroy it for its full lifetime without depending on an external
release channel.

#### Acceptance Criteria

1. WHERE remote state is S3, THE provisioner MAY persist the binary artifact for its target alongside the
   state documents.
2. WHEN a binary artifact is persisted to remote state, THEN its trust SHALL derive from the manifest
   checksum (Requirement 3), not from the stored blob.
3. WHEN a persisted binary is retrieved, THEN it SHALL be verified against the manifest checksum before
   execution.

### Requirement 6: Provisioner lifecycle CLI

**User Story:** As an operator, I want the provisioner to expose a small, specialized lifecycle CLI —
including one command that fully describes a deployment — with an unambiguous boundary between reading,
mutating, and advancing the recorded stamp, so that I can see drift before I act and never mutate across
a version boundary by accident.

#### Acceptance Criteria

1. WHEN the operator runs `tkr deployment describe` (forwarded to `tkp`), THEN the provisioner SHALL
   report the deployment identity, the recorded provenance (or explicit unknown), the binding verdict of
   the running binary, the integrity manifest, and the state-format/CAS facts, AND it SHALL NOT mutate
   state NOR refuse on a non-matching binding.
2. WHEN an applying command (`tkr infra apply`/`destroy`, `tkr deploy apply`, `tkr scale`,
   `tkr schema setup`, `tkr image push`/`mirror`) begins, THEN the launched `tkp` SHALL enforce the
   binding gate (Requirement 2): on a `versioned` deployment refuse unless `Match` (no override); on a
   `dev` deployment with a `dev` binary apply and re-stamp with a non-authoritative warning.
3. WHEN the operator runs `tkr deployment upgrade` with a `versioned` binary, THEN it SHALL run the
   forward migration and re-stamp provenance (Requirement 4) — covering both a versioned advance and the
   dev → versioned promotion — AND it SHALL be the only command that *authoritatively* advances the
   recorded version, AND it SHALL refuse a downgrade and SHALL refuse to re-stamp a deployment back to
   `dev`.
4. WHEN `tkr deployment describe` is invoked with `--json`, THEN it SHALL emit the same facts as
   machine-readable output.
5. THE set of commands that write the recorded stamp SHALL be exactly `tkr deployment create` (initial,
   always — Day-0 versioning), `tkr deployment upgrade` (authoritative advance / promotion),
   `tkr deployment rollback` (re-pin to a prior checkpoint), and the advisory `DevIterate` re-stamp on a
   dev-deployment apply; no other command SHALL write it, and there is
   no `adopt` verb (Day-0 versioning leaves no unstamped state to adopt).

### Requirement 7: Operator/provisioner surface separation

**User Story:** As an operator, I want a clean separation between the global `tkr` cockpit and the
deployment-specific provisioner `tkp`, so that the binary that mutates a deployment is always the exact
stamped binary married to it, and neither surface carries the other's concerns.

#### Acceptance Criteria

1. THE operator surface `tkr` SHALL NOT itself mutate a deployment's infrastructure; lifecycle mutation
   SHALL be performed only by the deployment's bound `tkp`.
2. WHEN an operator invokes a deployment-lifecycle action through `tkr`, THEN `tkr` SHALL resolve the
   binary by **launch class** — **bound** (recorded binary, for normal versioned mutations), verified
   against the recorded integrity manifest; **candidate-upgrade** (operator/release-resolved B), verified
   against external CI/release/build metadata since B is not yet recorded; **dev-candidate** (current
   local dev build) for a `dev` deployment; **rollback** (bound B then retained A) — and execute it; a
   checksum mismatch SHALL abort before execution. `tkr` SHALL NOT itself mutate.
3. THE `tkp` CLI SHALL be specialized to the deployment lifecycle and SHALL NOT carry the operator/global
   surface (developer/CI tasks, compatibility, workstation, deployment registry, or workspace
   `image build`); its lifecycle verb structure SHALL align with `tkr`'s so that forwarding is
   transparent.
4. WHERE an image operation targets a deployment's own registry (`image push`, `image mirror`), THE
   `tkp`/deployment surface SHALL own it; WHERE an image operation builds from workspace sources
   (`image build`), THE operator surface `tkr` SHALL own it.

### Requirement 8: Deployment lock (mis-apply guard)

**User Story:** As an operator, I want to lock a specific deployment so that subsequent `tkr` commands
cannot mutate any other deployment, with the lock stable across sessions, so that `tkr`'s power never
turns into an accidental change against the wrong environment.

#### Acceptance Criteria

1. WHEN the operator runs `tkr deployment lock [<name>]`, THEN `tkr` SHALL durably record the locked
   deployment's name and an identity fingerprint in the deployments registry, surviving process and
   session restarts.
2. WHILE a lock is active, WHEN a mutating command targets a deployment other than the locked one (via
   `--deployment` or the soft selection), THEN `tkr` SHALL refuse before launching `tkp`, with no
   per-command override.
3. WHILE a lock is active, WHEN a read-only command (`describe`, `plan`, `status`, `list`, `version`)
   runs against any deployment, THEN `tkr` SHALL NOT block it.
4. IF the locked deployment no longer exists, OR its identity fingerprint no longer matches the recorded
   one, THEN `tkr` SHALL refuse mutating commands and surface the discrepancy rather than transferring
   the lock or ignoring it (fail closed).
5. WHEN the operator runs `tkr deployment unlock`, THEN `tkr` SHALL clear the lock only after explicit
   confirmation, AND re-targeting SHALL require an explicit unlock or an explicit re-lock to another
   deployment.

### Requirement 9: Upgrade rollback

**User Story:** As an operator, I want to revert a deployment to its pre-upgrade checkpoint when an
upgrade goes wrong, using the retained prior provisioner, so that the upgrade boundary is recoverable
without reverse migrations.

#### Acceptance Criteria

1. WHEN `tkr deployment upgrade` runs, THEN it SHALL capture the rollback checkpoint ([A final]: prior
   snapshot ref, prior provenance stamp, prior **full integrity manifest** for all targets, and prior
   effective-config ref) in the same atomic commit that transfers the binding to B (Requirement 4.5),
   before any provider mutation.
2. WHEN the operator runs `tkr deployment rollback`, THEN it SHALL apply **two operations**: (a) the
   superseded (B) binary **deletes the resources it created** (`keys(S_B) − keys(S_A)`), which it alone
   can remove (they may be of kinds only B knows) — deletion is state-driven, so no before-images are
   read; then (b) a single atomic commit re-pins the binding to A and closes the operation marker; then
   (c) the prior (A) binary observes live infrastructure (`refresh_state`) and **runs its ordinary apply
   of the retained prior configuration revision**, reconciling B's remaining updates and re-creations
   from A's own config. Neither binary SHALL reverse a change it did not make nor reinterpret the other
   binary's recorded state representation (Proposal 002).
3. WHEN `tkr deployment rollback` runs, THEN it SHALL verify the checksum of **both** binaries it will
   execute against their integrity manifests before executing either, AND it SHALL refuse if no checkpoint
   exists, either binary is unavailable or checksum-mismatched, or the retained prior **configuration
   revision** is missing or does not compile under the prior (A) binary (Proposal 002).
4. THE rollback SHALL NOT run a reverse migration; migrations remain forward-only, and rollback restores
   the retained pre-upgrade checkpoint instead.
5. WHEN rollback reverts to the checkpoint, THEN it SHALL surface that state recorded after the upgrade is
   not represented, AND that resources recorded in no snapshot (created out-of-band) are not reconciled.
6. THE upgrade SHALL retain the prior **configuration revision** (Requirement 9.1's config ref) as the
   authoritative before-state for rollback; rollback reconciles forward toward that revision rather than
   inverting a recorded delta, so recorded before-images are NOT required (Proposal 002).
7. WHEN rollback performs destructive work, THEN preconditions SHALL be exhaustively checked first, the
   whole B-undo → re-pin → A-reconcile sequence SHALL hold the remote operation lock (Requirement 11), and
   progress SHALL be recorded in a durable marker so an interrupted rollback is resumable.

### Requirement 10: Fail-closed deletion in the IaC framework

**User Story:** As a platform engineer, I want the IaC framework to refuse a deletion it cannot actually
perform rather than silently forget the resource, so that no operation — rollback or otherwise — can
leave a live resource orphaned by dropping it from state.

#### Acceptance Criteria

1. WHEN the framework computes a Delete for a resource whose `ResourceId` is absent from the `known`
   resource set, THEN it SHALL return an error and SHALL NOT remove that resource from state.
2. THE framework SHALL NOT remove a resource from state on a Delete unless that resource's `delete()` was
   invoked (or the resource was confirmed authoritatively absent from the provider).
3. `Resource::describe()` SHALL distinguish authoritative-absent from not-implemented (a `DescribeResult`
   of `Present | Absent | Unsupported`, or the contract that an unimplemented `describe` errors and never
   returns absent); the framework SHALL prune state only on `Absent`, and SHALL NOT prune on `Unsupported`
   — for `Unsupported` it SHALL drive `delete()` from persisted state (treating provider-NotFound as
   success) or fail, never silently prune.

### Requirement 11: Remote operation lock (concurrency)

**User Story:** As an operator, I want concurrent mutations of the same deployment serialized in the
shared state store, so that two provisioners (another workstation, CI) cannot make conflicting
provider-side changes — separately from the local mis-apply guard.

#### Acceptance Criteria

1. WHEN any mutating `tkp` command begins, THEN it SHALL acquire a remote operation lock in the
   deployment's state store (S3 lease / explicit lock record; local filesystem lock), renew it during
   long operations, and release it on completion.
2. WHILE a remote operation lock is held, a second mutating process against the same deployment SHALL
   refuse (or wait) before performing any provider-side work.
3. THE whole multi-phase `rollback` sequence (B-undo → restore → A-reconcile) SHALL be performed under a
   single continuously-held operation lock, so no writer can interleave at the handoff.

### Requirement 12: Composition validation

**User Story:** As a platform engineer, I want the composition validated before any plan or mutation, so
that a malformed composition cannot route a delete to the wrong resource or apply a partial graph.

#### Acceptance Criteria

1. WHEN the engine begins plan/apply/destroy/rollback, THEN it SHALL validate the composition first —
   unique module names, unique resource ids, `desired ⊆ known`, every delete id present in `known`,
   dependencies present unless declared external, no cycles — and SHALL refuse before computing or
   applying any Delta if validation fails.

### Requirement 13: Engine identity vs configuration revision

**User Story:** As an operator, I want to refine a deployment's configuration (scaling, image refs, module
parameters, resources) continuously without minting a new provisioner version or running an upgrade, while
version stamping stays reliable, so that everyday refinement is a normal apply rather than a profusion of
`tkp` builds.

#### Acceptance Criteria

1. THE binding `source_tree_hash` SHALL be computed over the engine/resource-implementation surface (the
   code that determines how a plan is computed and applied), NOT over the deployment's desired-state
   configuration; a configuration change SHALL NOT change the engine identity and SHALL NOT gate.
2. WHEN an operator refines configuration and runs `apply`, THEN the bound provisioner (binding `Match`)
   SHALL plan and apply it as an ordinary create/update/delete, advance a monotonic `config_revision` in
   the state envelope, and NOT require a new `tkp` version; `describe` SHALL report both the engine stamp
   and the current `config_revision`.
3. WHEN an operator reverts a configuration change, THEN it SHALL be a same-engine `apply` of a prior
   config revision, NOT an `upgrade` and NOT a two-binary rollback.
4. A new `tkp` version (and the `upgrade`/rollback machinery) SHALL be required only when the engine
   identity changes (a resource-implementation/behavioral change), never for desired-state refinement.
