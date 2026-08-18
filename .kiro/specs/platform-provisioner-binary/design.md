# Design

## Overview

Tokeira provisions infrastructure through an IaC framework (`tokeira-iac`, `tokeira-deploy-engine`,
`tokeira-state`, `tokeira-orchestrator`, `tokeira-aws`) on which the platforms are built. This spec binds
each deployment to the exact code that provisions it, so a change to a resource implementation can never
silently re-interpret existing state and drift live infrastructure.

Two binaries, two roles:

- **`tkr` — the operator cockpit.** One globally-installed, version-current CLI used across all
  deployments: the deployment registry, developer/CI/compatibility tasks, workspace image
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
   the upgrade committed**. The prior revision is a deterministic, hermetic definition that
   is already retained; rollback restores it and lets the forward engine reconcile toward it. The
   superseded binary B deletes what it created (delete is already state-driven, so no recorded
   before-images are needed), the binding re-pins to A, and A observes live state (`refresh_state`) and
   forward-applies its retained prior revision — never reinterpreting B's recorded state.

Scope is the minimal foundational set: provenance, binding, integrity, the upgrade/migration boundary,
rollback, and binary retention for S3 state. Out of scope (follow-on): automated binary self-update,
release-signing infrastructure, and the single-shared-binary-vs-SDK multi-consumer decision.

## `tkp` command structure

`tkp` is married to one deployment and exposes a **lifecycle-only** surface. The operator/global surface —
`deployment create|list|use|lock|unlock|destroy`, `dev`, `ci`, `compat`, `version`,
`config`, `schema`, and all image ops (`image build`/`push`/`mirror`) — lives only on `tkr`. `tkp`'s verbs are **namespaced to mirror `tkr`**, so an
operator only ever types `tkr` and forwarding is a transparent pass-through (`tkr infra plan` →
`tkp infra plan`; Req 7.3):

```
tkp --deployment-dir <dir> <command>

  # Substrate — the infrastructure the deployment stands on
  infra plan             preview the infrastructure Delta; read-only, never mutates
  infra apply            reconcile infrastructure to desired; gated + locked
  infra destroy --yes    tear down all provisioned resources; gated + locked; irreversible

  # Workload — the services (tokeirad) that run on the substrate
  deploy plan            preview the workload Delta; read-only
  deploy apply           reconcile the workload to desired; gated + locked
  scale <dim>=<n> …      change workload capacity; a config-revision + workload apply; gated + locked
  logs <service>         stream logs for one logical service; read-only
  port-mappings <svc>    print live published port mappings; read-only

  # Definition & configuration — the interpreted `.tkd` and the rendered server config
  definition …           show/check the deployment definition; read-only
  config seed            render + seed the deployment's server configuration

  # Revisions & ownership — advance, restore, recover
  describe [--json]      identity + provenance + binding + state; two views; never gates
  revert --to <rev>      restore a prior config revision; same-engine re-apply; gated + locked
  upgrade                advance to a new engine identity (candidate B); gated + locked (Req 9)
  rollback               forward-reconcile to the retained prior revision
```

(`init` also exists, hidden from help: the Day-0 stamp + manifest write, invoked only as an internal
inception step of `tkr deployment create` — see the first structural rule below.)

Recovery from an interrupted `upgrade`/`rollback` is **re-running that same verb** — its steps are
idempotent and read the operation marker to skip completed work; there is no separate `resume` verb.

Full behaviour, outputs, gating, and locking for each verb are specified in
**§Command behaviour and outputs**. Five structural rules the shape encodes:

- **No operator `init`.** The Day-0 provenance stamp + integrity manifest are written as an *internal* step
  of `tkr deployment create` inception (Req 6.5), not a verb an operator types.
- **`describe` never gates.** It is the deployment-scoped counterpart to `tkr version` and must report
  precisely when the applying verbs would refuse — so diagnosis works on a drifted or mismatched deployment.
- **Verbs are complete but conditionally realized.** The surface is the same for every platform; where a
  platform cannot honor a verb (e.g. `scale` on a platform with no scale dimension), the verb returns a
  first-class **`NotApplicable`** result — never a missing subcommand and never a crash.
- **Database schema is out of scope.** `tkp` provisions the DSQL cluster as an *infrastructure resource*,
  but applying the database schema/migrations is application-runtime state owned by `tokeira-storage` and
  applied when `tokeirad` connects — not a provisioning step. So `tkp` exposes **no `schema` verb** and does
  **not** link `tokeira-storage`; this also keeps a schema migration from masquerading as an
  engine-identity change (it is neither engine code nor desired-state config that `tkp` reconciles).
- **Image supply is entirely `tkr`.** `image build`, `image push`, and `image mirror` all live on `tkr` —
  `tkp` provisions the registry (an infra resource) but never populates it, so it carries **no image verb**.
  A `tkr image push` writes the resolved digest into the deployment's config; `tkp` reconciles that on its
  next apply as an ordinary config revision, so image supply needs no `tkp` surface.

Every command takes `--deployment-dir` (the deployment it operates on); because `tkr` places `tkp` *inside*
that dir, the same bytes always operate the same deployment. Which binary actually runs for a given verb —
bound, candidate-upgrade, dev-candidate, or rollback — is decided by **launch class**
(§Command surfaces and launch classes).

## Architecture

`tkp` is the `tokeira-tkp` library (the platform-agnostic shell) statically composed with one selected
platform library and one selected Definition Frontend library through a **generated composition root** —
a tiny cargo package (`tokeira-bound-provisioner`, bin `tkp`) whose manifest declares exactly those three
dependencies and whose `main.rs` binds them explicitly (`bound_provisioner_main!`). The composition spans
the engine-decoupled crates: `tokeira-iac` (engine), `tokeira-deploy-engine`, `tokeira-orchestrator`,
`tokeira-aws`, the platform crates, and `tokeira-state`.
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

This is why `rollback` is two operations: the superseded binary B **deletes what it
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
compete with that source and be silently undone by the next ordinary apply. The same reasoning generalizes to the engine-upgrade case: since the prior configuration revision is retained and
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
(the `tkd` Definition Frontend of `tokeira-platform-definition`). A deployment's platform
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
  `version`, `config`, `schema` (DSQL schema setup/status — a `tkr`-native command that
  connects to the deployment's store directly; **never a `tkp` verb**, so `tkp` needs no `tokeira-storage`
  link), and **all** image operations — `image build` (workspace sources), `image push` (the deployment's
  workload image to its registry), and `image mirror` (external base/dependency images). `tkp` carries no
  image verb.
- **Forwarded to `tkp`:** `infra plan|apply|destroy`, `deploy plan|apply`, `scale`, and
  `deployment describe | upgrade | rollback`.

`tkp` carries the lifecycle-only surface enumerated in [§`tkp` command structure](#tkp-command-structure)
(namespaced to mirror `tkr`) — never the operator/global surface.
`describe` is the deployment-scoped counterpart to `tkr version`: it reports identity, recorded
provenance, the binding verdict, the integrity manifest, and state facts, honors `--json`, and never
gates (it must work precisely when the applying verbs would refuse). **All** image operations
(`image build`, `image push`, `image mirror`) stay on `tkr`; `tkp` provisions the registry but never
populates it — a `tkr image push` writes the digest into config, which `tkp` reconciles on its next apply.

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

## Command behaviour and outputs

Every verb runs inside one of two shell envelopes; the `ProvisionerPlatform` seam supplies only the
resource realization, so the guarantees below hold uniformly across platforms.

**Mutating-verb contract — identical for every mutating verb.** The shell wraps each in one sequence:

1. **Resolve** the recorded provenance + integrity manifest from the state envelope.
2. **Gate** — run the binding gate (§The binding gate); a non-`Match` verdict refuses *before any mutation*,
   except the classes that explicitly permit a candidate (`DevIterate`, `upgrade`, `rollback`).
3. **Lock** — acquire the deployment operation lock (§Operation safety): one writer, mutually exclusive.
4. **Plan** — compute the Delta for the verb's `(desired, state, universe)` triple.
5. **Confirm** — print the Delta; require confirmation unless `--auto-approve` (or `--yes` for irreversible
   verbs).
6. **Apply** — enact the Delta in dependency order; **fail-closed** on delete (§Property 10).
7. **Record** — advance `config_revision`, snapshot the revision's config source, record
   `effective_config_ref`, write the new envelope revision atomically; release the lock.
8. **Report** — emit a `describe`-style summary of the resulting state (operator view; `--json` for machine).

**Read-only verbs** (`describe`, `infra plan`, `deploy plan`) run steps 1, 2, 4 only — resolve, evaluate the
gate *for report* (never to block), compute the Delta, print. No lock, no mutation, and they **must succeed
even on a gate mismatch**, so an operator can diagnose a refusal.

| Verb | Class | Gate | Lock | Behaviour & primary output |
|------|-------|------|------|----------------------------|
| `describe` | read-only | report | no | Two views (below). Never blocks. |
| `infra plan` | read-only | report | no | Compute + print the infrastructure Delta and the binding verdict; `--json` emits both as data. |
| `infra apply` | mutating | block | yes | Reconcile infrastructure to desired via the contract. Output: applied Delta, new `config_revision`, post-state summary. |
| `infra destroy` | mutating | block | yes | Reverse Delta over the full known universe; requires `--yes`. Fail-closed: an unknown/unremovable resource aborts rather than orphaning. Envelope status → `Destroyed`. |
| `deploy plan` | read-only | report | no | As `infra plan`, over the workload (tokeirad services) universe. |
| `deploy apply` | mutating | block | yes | Reconcile the workload to desired. Output as `infra apply`. |
| `scale` | mutating | block | yes | Fold `<dim>=<n>` into a config revision, then a workload apply. Output: capacity Delta + new revision. `NotApplicable` if the platform has no scale dimension. |
| `revert --to <rev>` | mutating | block | yes | Restore revision `<rev>`'s retained config source and re-apply with the **same** engine; produces a *new* forward revision equal to `<rev>` (Req 13.3). Refuses a non-prior or unretained target. |
| `upgrade` | mutating | permit `Candidate` | yes | Runs candidate **B** (launch class Candidate-upgrade); migrates A→B, re-records the manifest for B, stamps the transition. Sequence in §Upgrade and rollback. |
| `rollback` | mutating | permit `Rollback` | yes (continuous) | B undoes what it created; the binding re-pins to A; A forward-reconciles its retained prior revision. One lock across the whole sequence (12.2). |

An interrupted `upgrade`/`rollback` has **no dedicated recovery verb**: re-running the same verb resumes it
(idempotent steps keyed to the marker's recorded phase), and while a marker is open only that verb,
`rollback` (to abort an interrupted upgrade forward to A), and `describe` are permitted — every other
mutating verb refuses.

**`describe` — two views.** `describe` answers "what is this deployment, and can it be operated?" in two
registers over the *same* envelope, and never gates in either:

- **Operator view (default).** A tight human summary for day-to-day work: deployment name + id, platform,
  storage, **short** engine identity, binding status (`Bound·Match` / `Mismatch` / `Unbound` / `DevIterate`),
  current `config_revision`, service/health status, and the last operation + its outcome. It answers "is
  this healthy and safe to act on" at a glance — no checksums, no digests.
- **Verification / debug view (`--json`; `--verbose` for the human-readable equivalent).** The full
  auditable record: the complete `EngineIdentity` (source-closure digest, `Cargo.lock`-closure digest,
  toolchain, build-container digest, features, profile), the **entire integrity manifest** (per-artifact
  SHA-256), `effective_config_ref`, the source-snapshot ref, the operation marker + lock holder, the
  retained-revision list, and the envelope revision. This is what verifies a binding by hand and debugs a
  refusal. `--json` always emits this view (stable, machine-parseable); the operator view is human-only.

**Output conventions.** Every verb honors `--json`; a refusal/failure exits non-zero with a typed reason
(`GateMismatch`, `LockHeld`, `NotApplicable`, `Interrupted`, …). Read-only verbs never lock or mutate;
`--yes` is required only by irreversible verbs (`infra destroy`), and `--auto-approve` skips the plan
confirmation on the others.

**What lives in a deployment directory, and what each verb writes.** All verbs operate over one
authoritative document and a small set of sibling files:

```
<deployment-dir>/
├── definition.tkd            # live config SOURCE (definition-carrying platforms) ─┐ exactly one is
├── deployment.toml           # live config SOURCE (local)                         ─┘ the "live config file"
├── tkp                       # the deployment-married provisioner binary
└── state/
    ├── envelope/             # CAS store of the DeploymentStateEnvelope (THE authoritative doc)
    ├── config-revisions/
    │   └── <n>/<basename>    # retained config SOURCE for revision n (revert's raw material)
    ├── infra/                # infra state docs (infra_head → here)
    └── deploy/               # runtime/service state docs (runtime_head → here)
```

- `describe` writes **nothing** — a pure read of `state/envelope`.
- `apply`/`scale` write the live config's new retained revision under `config-revisions/`, the infra or
  runtime state docs (+ heads), and the envelope (`config_revision`+1, `effective_config_ref`).
- `revert` restores the target revision's retained source into the **live config file**, reconciles, then
  writes as an ordinary apply — monotonic-forward: reverting to `N` mints a *new* revision whose content
  equals `N`'s, so history stays append-only and a revert is itself revertable.
- `upgrade` writes the envelope **twice** — (i) the atomic ownership transfer in one CAS commit *before
  any provider mutation* (checkpoint set, `binding → B`, marker open, `integrity → B`), then (ii) the
  marker close — plus B's state docs. It never touches `config_revision` or `config-revisions/`: an
  upgrade is an engine change, not a config change. The checkpoint is retained past close.
- `rollback` deletes B's creations from live resources, writes the envelope **twice** — the atomic re-pin
  (binding/integrity/heads/config-ref → A, marker open), then complete (marker + checkpoint cleared) —
  plus A's reconciled state docs.

Every mutating verb ends in a `store.save(&envelope, &version)` — a compare-and-swap commit against the
loaded version, so a concurrent writer surfaces as a CAS conflict, never a silent overwrite.

## Upgrade and rollback

The engine plans and applies create/update/delete; that machinery is unchanged and does the heavy
lifting. Upgrade and rollback add only orchestration on top of it.

**Binding is two-valued; an operation marker carries "in flight."** The recorded binding names exactly the
binary authorized to operate — `A` *or* `B`, never a third "pending" value. Whether an upgrade or
rollback is mid-flight is a separate **operation marker** (`UpgradeInFlight | RollbackInFlight | none`)
recording the phase and resumable progress (and, optionally, an ids-only audit change log — never
before-images). The marker — not the binding — gates the deployment while an operation is open: only the
in-flight verb (`upgrade` or `rollback`, whose re-run resumes it), `rollback` (to abort an interrupted
upgrade forward to A), and `describe` are permitted; the remote operation lock provides mutual exclusion.
Splitting these two concerns removes any ambiguous binding state.

**Upgrade transfers ownership atomically, *before* mutating.** The first act of `upgrade` is a single CAS
commit that, together: flips the binding to **B**, captures a clone of **[A final]** as the checkpoint
(A's snapshot + provenance + integrity manifest + config ref + retained-binary ref), and opens
`UpgradeInFlight`. Only after that commit does B touch the provider. So the recorded binding is *always*
the binary doing the work — a crash at any point recovers as B (recovery reads binding = B plus the open
marker, and resumes or rolls back); A never runs against B-shaped infrastructure. B then runs any
state-schema migration and applies its plan. It MAY record an **ids-only change log** (`id + op`) for
audit and richer `plan`/`describe` output — but rollback needs **no before-images**, because it restores
A's retained prior configuration revision, not an inverse of what B committed. The marker
closes on success. Upgrade is the only verb that authoritatively advances the
recorded version; it refuses a downgrade, a same-semver/different-hash apply (a forgotten version bump),
an unbridged schema migration, and re-stamping back to `dev`.

**The rollback baseline is the retained prior configuration revision `R_a`** — a deterministic, hermetic
definition retained at upgrade, not a set of recorded before-images. As a stricter option
an upgrade MAY first compare live against the recorded [A final] and **refuse-and-surface** material drift
("reconcile before upgrading"); this baseline gate is **advisory** — a cross-version consistency check,
never a licence for B to authoritatively reconcile A's drift, which would breach the authorship discipline.

**Rollback deletes B's creations, re-pins to A, then A forward-reconciles toward `R_a`** — two
operations, sharp division of responsibility:

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
not infra-only, spanning both `infra_head` and `runtime_head`. This needs **no
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

- **Provenance reader/writer** (`tokeira-deployment`, persisted through `tokeira-state`) — stamps the
  running version into the state envelope; reads it back as `ProvenanceStamp` (concrete or `Unknown`).
- **Binding gate** (`tokeira-deployment`) — `check_binding(running, recorded)`; applying ops consult it
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
  lock and a resumable operation marker.
- **Forward-engine replacement + destructive `plan` gating** (`tokeira-iac`) — the engine's diff/apply
  gains **replacement** (an immutable-field change becomes delete+recreate) and `plan` surfaces
  destructive changes requiring explicit `--yes`. These are **general apply features** (any immutable
  change needs them), and they are what makes forward-reconcile rollback correct without a per-kind
  restore surface. `Engine::destroy_selected` — a refs/id-set delete over the full state — is the
  delete-only primitive B uses to remove its creations. **Rollback spans runtime/service state**
  (services + images) as well as infra: B's delete-only undo requires the deploy-engine `Platform` to gain
  a **service delete**, while A's reconcile re-applies `R_a`'s services through the existing forward
  `apply_manifests` — no `Service` restore capability and no before-images.
- **Fail-closed delete + authoritative `describe` + composition validation** (`tokeira-iac`) — the three
  framework correctness mechanics described above.
- **Remote operation lock** (`tokeira-state` + `tkp`) — renewable mutual-exclusion lease.
- **Deployment lock** (`tkr`) — the durable `lock.toml` mis-apply guard.
- **Binary retention + bundle CAS** (`tokeira-deployment` over `tokeira-state` backends) — `BinaryStore`
  retains the deployment's own bytes; `BundleStore` is the identity-keyed CAS the obtain step consults.
  Every retrieval re-verifies via the integrity verifier; repository-boundary distribution defers to the
  Deployment Repository TUF model.

Where the pieces live:

| Concern | Crate | Where |
|---------|-------|-------|
| CLI dispatch (namespaced verbs) | `tokeira-tkp` | `cli.rs` |
| `describe` (two views) | `tokeira-tkp` | `describe.rs` (`DescribeReport`), `described.rs` |
| Verb bodies | `tokeira-tkp` | `apply.rs`, `revert.rs`, `upgrade.rs`, `rollback.rs`, `deploy.rs`, `scale.rs`, `destroy.rs` |
| Binding gate | `tokeira-tkp` | `gate.rs` (`evaluate_gate`) |
| Platform identity + admission | `tokeira-tkp` | `platform.rs` (`BoundPlatform`, `Admitted`) |
| Config-revision retention | `tokeira-deployment` | `config_history.rs` (`config_file`, `snapshot`, `is_retained`, `restore`) |
| Operation lock wrapper | `tokeira-deployment` | `lock.rs` (`with_operation_lock`) |
| Envelope + state-machine | `tokeira-deployment` | `lib.rs` (`DeploymentStateEnvelope`, `RollbackCheckpoint`, `Operation`, `begin_upgrade`, `begin_rollback`, `complete_rollback`, `close_operation`, `ProvenanceStamp::current`) |
| Upgrade decision | `tokeira-deployment` | `upgrade.rs` (`UpgradeDecision`, `evaluate_upgrade`) |
| Two-binary rollback orchestration | `tkr` | `apps/tkr/src/launcher.rs` (`launch_rollback`) |
| CAS state store | `tokeira-state` | `CasStore`, `LocalBackend`, `SnapshotRef` |

## Data Models

- **ProvenanceStamp** — `{ version, git_sha, source_tree_hash, build_mode: BuildMode, recorded_at }`. A
  missing stamp is an explicit `Unknown`, never coerced to a concrete version.
- **BuildMode** — `Versioned | Dev`. Only `Versioned` is authoritative.
- **Target** — a Rust target triple (`aarch64-unknown-linux-musl`, `aarch64-apple-darwin`, …); `os/arch`
  alone is not precise enough for an executable artifact.
- **BinaryArtifactDescriptor** — `{ target: Target, sha256, retrieval_ref: Option<String>,
  size_bytes }`. The artifact carries no version of its own: its key half is the enclosing manifest's
  engine identity.
- **IntegrityManifest** — `{ engine_identity: Option<EngineIdentity>, authority,
  artifacts: Vec<BinaryArtifactDescriptor> }`, keyed by `EngineIdentity × target` with the semver kept as
  a human-facing label only (`None` identity is a pre-identity native dev build). CAS-guarded, cannot be
  silently rewritten.
- **DescribeResult** — `Present(ResourceState) | Absent | Unsupported`. Replaces `Option<ResourceState>` so
  the engine distinguishes provider-absent (prune-safe) from not-implemented (must not prune).
- **MigrationRegistry** — ordered `Migration { from_schema: u32, to_schema: u32, apply }`, keyed by state
  schema, forward-only. A new `source_tree_hash` at the same schema needs no migration.
- **ChangeLog** (optional, audit only) — an **ids-only** record of what an apply committed: per change,
  `{ id, op: Created | Updated | Deleted }`, **no before-images**. Recorded for observability and richer
  `plan`/`describe` output; it is **never** the rollback mechanism. Rollback is
  driven by the retained prior configuration revision, not by inverting a recorded delta.
- **RollbackCheckpoint** — the cloned **[A final]**: `{ from_provenance: ProvenanceStamp,
  from_integrity: IntegrityManifest, from_infra_head: Option<SnapshotRef>, from_runtime_head:
  Option<SnapshotRef>, from_config_ref: Option<String>, recorded_at }`. Captured atomically at the start of
  `upgrade`. The `from_*_head` snapshots pin [A final]'s infra and runtime state (rollback spans both
  engines). Carries the full prior integrity manifest (all targets — rollback may run from a different
  operator platform) and A's config ref — which is **load-bearing**: rollback restores A's prior
  configuration revision from `from_config_ref` and forward-reconciles toward it. Defined in
  `crates/tokeira-deployment`.
- **Operation** — the in-flight marker: `{ operation_id, kind: UpgradeInFlight | RollbackInFlight,
  phase, progress }` (resumable step markers; optionally an ids-only `ChangeLog`, never before-images).
  While present it gates the deployment to the in-flight verb (re-run resumes it), `rollback`, and
  `describe`; it records progress so an interrupted upgrade or rollback resumes on re-run; it closes on
  success. `None` in steady state.
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

Integrity metadata (engine identity + per-target `sha256` + optional `retrieval_ref`) is always in the
CAS-guarded manifest — the local execution gate regardless of where the blob lives. The binary blob may
also be retained with the deployment (keyed by identity + target), making the deployment self-contained.
Trust never flows from the stored blob: a retrieved binary is verified against the manifest `sha256`
before execution. The manifest records checksums for all built targets even when only one blob is
co-located.

**Anything crossing a repository boundary defers to the Deployment Repository TUF model** — the
definitive provenance and integrity story (`.kiro/specs/deployment-repository/`). Publishing a deployment
and fetching one verify through TUF metadata and the Deployment Claim before any local manifest role
begins; the envelope's integrity manifest then remains the deployment-local gate the launcher and `tkp`
enforce on every execution.

## Per-platform provisioner and three-part provenance (Req 14)

`tkp` is **not one universal binary** — it is composed, per deployment, from three ingredients:

```
tkp(compose, tkd)  =  the provisioner shell (tokeira-tkp: verb contract, gate, lock, envelope, dispatch)
                   +  ONE platform library (tokeira-compose-deployment: kinds, realization, providers)
                   +  ONE Definition Frontend library (tokeira-platform-definition: the .tkd interpreter)
```

The **platform and frontend are compiled in**, so they *are* engine identity; the `.tkd` a deployment
carries is **data**. That gives the clean three-part provenance:

| Part | What | Where it lives | Changes are |
|------|------|----------------|-------------|
| **engine** | IaC engine + resource providers + the shell | compiled → `EngineIdentity` | an upgrade (Req 4/9) |
| **platform + frontend** | kind library + vocabulary + interpreter | compiled → `EngineIdentity` | an upgrade (Req 4/9) |
| **definition** | the `.tkd` | data → `config_revision` | an ordinary apply/revert (Req 13) |

`engine + platform + frontend` → the binding's `source_tree_hash`/`EngineIdentity`; `definition` → the
digested `config_revision`. Two deployments on the same composition can therefore share **the same `tkp`
bytes** and differ only by their `.tkd`.

**The composition is generated, not shipped.** No platform carries a bin target and there are no
`apps/tkp-*` crates. `tkr` discovers the selected platform and frontend through their
`[package.metadata.tokeira.*]` descriptors (cargo metadata supplies every package coordinate — a
descriptor cannot inject Rust paths or arbitrary dependencies), then renders a **generated composition
root**: package `tokeira-bound-provisioner`, bin `tkp`, exactly three path dependencies (shell, selected
platform, selected frontend) and a `main.rs` that binds them explicitly through
`tokeira_tkp::bound_provisioner_main!` — the expected platform/format identities are checked at compile
time. The root is a disposable build input, an ordinary member of the scoped source workspace
(§Reproducible build); its rendered bytes are digested into the engine identity as
`generated_root_digest`, so the selection itself is provenance.

**Naming and placement.** The constructed binary is always `tkp` — construction plumbing never leaks into
operator view. The **placed** binary is `<deployment>/tkp` (Req 14.4); `tkr deployment create` builds the
generated root for the discovered selection, places the bytes, and the launcher runs `<deployment>/tkp`
(Req 6.5; the deployment-married provisioner).

## Reproducible build: Dagger, source snapshot, and bundles (Req 15, 16)

Producing a `tkp` is a single **Dagger** function that runs identically locally and on a CI agent —
Tokeira already builds its runtime image through Dagger (`tokeira-build`), so this extends
an existing boundary. The trust posture in one line: **caching accelerates, admission decides** — a
cached or downloaded artifact earns use only by re-verification against its identity, never by where it
came from. The load-bearing points:

- **`build_provisioner(source_snapshot, request) -> ProvisionerBundle`** — validate the closure, compute
  `EngineIdentity`, build `tkp` for the requested targets, run tests, checksum + measure artifacts, package
  the bundle. `create` **requests a bundle**; Dagger decides *reuse | download | build*, and Tokeira decides
  *admission* (identity interchangeable, authority sufficient, not revoked, checksum re-verified). Caching
  accelerates production; it never grants admission.
- **`EngineIdentity` is closure-scoped** — the digest is over the provisioner's *dependency closure* + its
  own source subtree, **not** the whole workspace. If it hashed the shared `Cargo.lock` or whole tree, a
  `tkr`-only dep bump would re-key every `tkp` identity and force a rebuild+rebind (an upgrade) across all
  deployments. This is the make-or-break scoping for bundle/CAS identity. The *binding* stamp's
  `source_tree_hash` remains the whole-workspace digest from `tokeira-build-info` — it over-approximates
  Req 13.1's engine surface (the safe direction: it never includes desired-state configuration, but
  out-of-closure churn can still route a deployment through `upgrade`); unifying the binding key onto the
  closure-scoped identity is an open decision.
- **Build authority is orthogonal to build mode.** `LocalDeveloper` (native `cargo` for the dev loop —
  fast, `kache`-accelerated per-worktree) vs `TrustedCi` (hermetic Dagger, cache-by-identity). The trusted,
  cache-by-identity bundle is a **cold hermetic** build (pure w.r.t. its declared inputs); the two build
  worlds do not share a compile cache and don't need to.

**Source snapshotting** is the fidelity anchor (Req 16), and it matters concretely because the dev
environment runs several AI agents mutating source concurrently, each in its own worktree. A build must not
derive identity from a live tree:

```
snapshot (immutable, content-addressed)  ->  EngineIdentity (over the snapshot)  ->  build (consumes the snapshot)
```

atomic w.r.t. source. The pure-Rust git SDK `gix` provides the snapshot in-process. For a dirty
`LocalDeveloper` tree the primitive is a **temporary-index `write-tree`**: stage
the provisioner closure's worktree content (tracked staged + unstaged changes) and write a
content-addressed `tree` —
leaving the working tree, the real index, and every ref untouched. (The
porcelain **`git stash` is not usable** — it reverts the tree and writes `refs/stash` — and even
`git stash create` omits untracked files, so it is only the nearest intuition, not the mechanism.)
**Untracked `.rs` within the closure are refused by default**: `create` fails and lists them, so nothing
that determines identity is silently omitted; they are swept in only under an explicit `--include-untracked`
For `TrustedCi`, no dirty snapshot exists — the request pins an immutable,
reachable, protected commit. The snapshot ref + digest are recorded in the request and the bound deployment's
provenance — so the exact source is auditable, and per-worktree isolation plus the create-time snapshot
together give per-agent fidelity.

**The frozen tree is a complete, valid cargo workspace.** The workspace `Cargo.toml` and `Cargo.lock` are
frozen **closure-scoped**: members are exactly the closure's crates plus the generated composition root (an
ordinary member — never a detached package with a private lock). The scoped lock is authored by **cargo
itself** — exact membership is cargo's feature unification over the scoped member set, which no derivation
short of cargo's own resolution reproduces. The staging is seeded with the workspace's authoritative
`Cargo.lock` and resolved offline, so cargo can only prune the admitted versions, never consult a
registry; the result is validated to contain nothing outside the resolved closure. `--locked` then
verifies the scoped lock is exact for the build and for the closure's tests, in the one shared workspace.
This is also what keeps the frozen source closure-scoped in fact — out-of-closure workspace churn
(members, `tkr`-only dep bumps) leaves the frozen bytes, and therefore the identity, unchanged.

**Identity keys on the tree; the commit is only an audit wrapper.** `write-tree` yields a **`tree`** oid —
pure content, the thing `EngineIdentity`'s source digest keys on. Wrapping it with `commit-tree` (parent =
`HEAD`) gives a *reachable, auditable* handle, and is the right move — but a commit embeds author/committer
timestamps, so committing the same tree twice yields **different commit oids**. Identity must therefore key
on the tree, never the commit. Four consequences settle the design:

- **Deterministic wrapper.** The snapshot commit uses a fixed synthetic identity (`tkp-snapshot
  <noreply@tokeira.io>`) and fixed timestamps (the parent's commit time, or epoch 0), so identical
  `(tree, parent)` → identical commit. Committer identity is *supplied*, not read from git config (which is
  routinely unset in CI).
- **Unborn / detached `HEAD`.** `HEAD` may not resolve (fresh repo, mid-rebase, detached). The parent is
  provenance-only and never semantically load-bearing, so a missing `HEAD` falls back cleanly to a
  **parentless** snapshot commit.
- **GC vs retention.** A dangling commit is prunable by `git gc`. Because the built bytes are captured into
  the ProvisionerBundle and identity keys on the tree, the default is to **record the oid only** (a
  best-effort audit handle); a lightweight ref (`refs/tokeira/snapshots/<engine-identity>`) pins it **only
  under `TrustedCi`**, where durable audit matters.
- **Not stash, not submodule content.** This is why `git stash` is rejected above; likewise `write-tree`
  captures submodule/LFS entries as gitlinks/pointers, not content — irrelevant to `tkp`'s pure-Rust
  closure, noted for completeness.

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

*For any* `rollback`: the superseded binary B **deletes the resources it created**
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
| `upgrade` or `rollback` interrupted | Re-run the interrupted verb; its idempotent, marker-driven steps resume from the recorded `phase` (no separate `resume` verb). The binding already names the operating binary, so recovery relaunches the correct one (Req 9.7). |
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
