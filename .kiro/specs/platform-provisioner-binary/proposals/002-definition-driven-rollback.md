# Proposal 002 — Definition-driven rollback (forward reconcile toward the retained revision)

- **Status:** **Accepted** — the chosen rollback mechanism. All decisions below are resolved (§Resolved decisions).
- **Supersedes:** [001 — state-driven restore (`apply_inverse_delta`)](./001-state-driven-restore.md). 001 is **rejected as the rollback mechanism**; only its recorded change-log survives, demoted to the optional **ids-only audit** artifact (Decision 4). 001 and 002 encode incompatible Req 9 mechanisms; 002 is the one adopted.
- **Refines:** task 11.3 (rollback capability); unblocks task 8.5 (rollback orchestration).
- **Requires a requirements change (adopted):** Req 4.6, Req 9.2, Req 9.6, and the glossary entries *Rollback* / *Applied delta* (§6) are revised per this proposal. 001 implemented Req 9 as written; 002 corrects the mechanism Req 9 described.
- **Owner area:** `crates/tokeira-iac` (forward engine), `crates/tokeira-orchestrator`, and `tkp` rollback orchestration.

## Thesis

Rollback should be **derived from the deployment definition**, not from a recorded inverse of what an
apply did. The system already retains, per the *Configuration revision* glossary entry and Req 9.1's
"prior effective-config ref", the **prior configuration revision** — a deterministic, hermetic
rust-via-`syn` definition (`platform-config-dsl` Proposal 003) that compiles to a `Composition`. And the
IaC engine is already a **forward reconciler**: it drives live infrastructure toward a desired state,
creating/updating what's desired and — via the desired-vs-`known` split — deleting what is no longer
desired.

Given those two facts, rollback is: **restore the prior configuration revision and let the forward
engine reconcile toward it.** No recorded before-images, no per-kind inverse capability. The prior
revision *is* the before-image, and it is authoritative and lossless for everything the definition
determines — which is precisely what rollback should target.

## Why not the inverse delta (Proposal 001)

001 is a faithful, careful implementation of Req 9 as written. The objection is to the requirement's
*mechanism*, on three grounds:

1. **It creates a second source of truth for "what the prior state was."** 001 records `before`-images
   in an `AppliedDelta`, alongside the already-retained prior configuration revision. Two
   representations of the same prior intent that can **diverge** — and 001 itself documents that the
   before-images are stale or structurally absent for the 16 `Unsupported`-describe kinds (001 §"Verified
   facts", §6 risk 2). The retained revision has exactly one representation and no describe dependency.

2. **Its central capability is large, mostly inert, and fail-closed.** `StateDrivenRestore`
   (`restore_to_state` / `recreate_from_state`) is 001's "real work" (001 §6 risk 1), lands **fail-closed
   for every non-opted kind** (001 §7.1), and requires refactoring each kind's `create`/`update` to
   source from a passed state plus a per-kind round-trip proof. A forward reconcile toward the retained
   revision needs **none** of it: `create`/`update` already drive toward config, which is what the
   revision *is*.

3. **It doesn't actually solve the genuinely hard cases — and neither can anything.** A deleted DSQL
   cluster's *data* cannot be restored by `recreate_from_state` any more than by a forward re-create; an
   immutable-field revert needs *replacement* either way. 001 relocates these into per-kind restore
   methods with a fail-closed posture; 002 puts them in the **forward engine** (replacement + destructive
   confirmation), where **ordinary applies need them too** — so the cost is shared, not rollback-only.

## The one thing the inverse delta *was* buying — and how 002 keeps it

The delta is not arbitrary: it lets the **superseded binary B undo its own changes without any binary
interpreting the other engine's config or state** (the *Authorship invariant*: no binary computes a
Delta over a representation authored by another). That boundary is real across an **engine-identity
upgrade** (A→B are different engines). A naive "re-pin to A and let A reconcile from B's state" would
have A interpret B-authored state — the invariant's exact prohibition.

002 respects it, by splitting the concern:

- **B's role collapses to delete-only.** Deleting a resource is **already state-driven** — 001's own
  first verified fact — so B needs *no new capability* to remove what it created, including resources of
  **B-introduced kinds** that A cannot even name for deletion (which is precisely why B, not A, must do
  it; Req 10 fail-closed).
- **A reconciles over *live*, not over B's state.** After re-pin, A runs `refresh_state` (describe) to
  observe provider truth itself, then forward-applies the retained prior revision R_a. A never
  reinterprets B's recorded state representation — it observes reality and drives it to its own config.
  Authorship-clean.

So the two-operation shape Req 9.2 already prescribes (B-undo → re-pin → A-reconcile) **survives**; only
B's undo changes — from "invert a recorded applied delta" to "delete what B created" — and A's reconcile
(Req 9.2(c), already in the spec) becomes the **load-bearing** step rather than a trailing re-assert.

## Verified facts this design rests on

- **Delete is already state-driven** (001 §"Verified facts"; `Resource::delete(current, ctx)` is driven
  from a cloned `ResourceState`). B's delete-only undo needs no new trait.
- **The forward loop already deletes `known`-not-`desired`** (IaC engine contract; AGENTS "IaC Engine
  Contracts"). Re-applying a prior revision R_a naturally deletes resources a newer revision added, for
  every kind in A's `known` universe.
- **`create`/`update` drive toward `self` (config), not a passed state** (001 §"Verified facts";
  `ssm_parameter::update` ignores `_current`). This is a *liability* for 001 (it needs a state arg) and
  an *asset* for 002 (config-driven is exactly reconcile-toward-the-revision).
- **The prior configuration revision is already retained** — Req 9.1 checkpoints the "prior
  effective-config ref"; the glossary defines *Configuration revision* as a recorded monotonic revision.
  002 makes this ref **load-bearing** rather than incidental.
- **The definition is deterministic and hermetic** (`platform-config-dsl` Proposal 003 §4): R_a compiles
  to the same `Composition` every time, with no I/O. This is what makes "restore the revision and
  reconcile" *sound* — re-applying R_a provably reproduces [A final]'s desired resources.
- **`refresh_state` writes state only on `Present`** (001 §"Verified facts"). A's post-re-pin reconcile
  inherits the same `Unsupported`-describe blind spots 001 has — this is a shared limit, not a new one.

## The two rollback classes (and why the common one is trivial)

- **Configuration rollback (same engine identity — the common case).** An operator makes a bad config
  edit (Req: config is "refined freely by ordinary apply"). Rollback = restore the prior revision and
  `apply`. **One operation, one binary, no delta, no re-pin.** This is the everyday rollback and 002
  makes it fall out of the ordinary forward path for free. 001 does not distinguish this case and pays
  the full inverse-delta cost for it.
- **Engine-upgrade rollback (cross engine A→B — the rare case).** The two-operation sequence of §"how
  002 keeps it": B delete-only → re-pin to A → A `refresh_state` + forward-apply R_a.

## Design

### Rollback algorithm (upgrade case)

1. **Preconditions (fail-closed, no mutation)** — unchanged from Req 9.3 / 001 Pass 1: checkpoint
   exists; both binaries verified against their integrity manifests; operation marker consistent.
2. **B deletes what it created** — B is still bound. Delete-only over the set of resources B created
   (recorded as *ids only*; see Decision 4), in reverse dependency order, fail-closed, idempotent
   (absent ⇒ done). Reuses the existing `delete_one`/`destroy_selected` path. No before-images, no
   restore trait.
3. **Atomic re-pin to A**, advance the operation marker (Req 9.2(b)).
4. **A reconciles toward R_a** — A runs `refresh_state` (observe live), then `apply` of the retained
   prior configuration revision R_a over the current state. The forward engine:
   - updates resources B modified back to R_a's desired (ordinary `update` toward config);
   - recreates resources B deleted from R_a's desired (ordinary `create` — *new/empty* for stateful
     kinds; see risks);
   - deletes anything still present but absent from R_a (via `known`-not-`desired`).
5. **Resume** — the whole sequence holds the remote operation lock and records progress in the durable
   marker (Req 9.7); an interrupted rollback re-enters at the marked step; every step is idempotent.

### Forward-engine work this requires (shared with ordinary apply)

- **Replacement.** An immutable-field change (whether forward or on rollback) means delete+recreate.
  Today `update` re-applies config in place and does not replace. This must land in the **forward
  engine's diff/apply**, gated behind describe-detected immutability — and it benefits *every* apply, not
  just rollback. (001 hides this inside per-kind `recreate_from_state`; 002 surfaces it where it's
  generally needed.)
- **Destructive-change confirmation in `plan`.** Replacement and delete-recreate are destructive; the
  forward `plan` must surface them and require explicit confirmation (`--yes`) — again a general apply
  safety feature, not rollback-only.

## Requirements change (the load-bearing edit)

002 cannot proceed under Req 9 as written. Proposed edits (snapshot the spec first, per repo policy,
before applying — this proposal does not itself edit `requirements.md`):

- **Req 4.6** — *drop* "record an applied delta … each carrying the observed before-image and the
  after-image, so rollback can invert what was committed." Replace with: an apply MAY record an
  **ids-only change log** for audit/plan (before-images not required for rollback).
- **Req 9.2** — redefine the undo: "(a) the superseded binary B **deletes the resources it created**
  (delete is state-driven; before-images not required); (b) atomically re-pin to A; (c) A observes live
  (`refresh_state`) and **forward-applies the retained prior configuration revision** over the current
  state, reconciling B's updates and re-creations from A's own config."
- **Req 9.6** — *drop* "record its applied plan with before-images sufficient to invert it." Replaced by
  the retained configuration revision (Req 9.1's config ref becomes load-bearing).
- **Glossary** — *Rollback*: "B deletes its creations; the binding re-pins to A; A forward-reconciles
  toward the retained prior configuration revision." *Applied delta*: demote to an optional audit record
  (ids + op), not a before-image store.
- **Unchanged:** Req 9.1 (checkpoint incl. config ref — now central), 9.3 (preconditions), 9.4 (no
  reverse migration), 9.5 (post-upgrade state / out-of-band resources not reconciled), 9.7 (lock +
  durable marker + resume), Req 10 (fail-closed deletion — relied on for B's B-only-kind deletes).

## Risks and honest limits (several shared with 001)

1. **Data loss on delete→recreate is inherent and unsolved by either proposal.** A stateful resource B
   deleted, recreated on rollback from R_a's config, comes back **empty** — identical to 001's
   `recreate_from_state`. 002 is honest that recreate ≠ restore; the mitigation is destructive-change
   confirmation in `plan`, not a false promise of content restoration.
2. **Authorship re-scope.** 002 has A reconcile over **live-via-`describe`**, not over B's recorded
   state. This is authorship-clean *only where A can describe the resource*; `Unsupported`-describe kinds
   are the same fail-closed blind spot 001 carries. Owner must accept this re-scoping of the invariant
   (from "no binary reads another's *state*" to "no binary reads another's *state representation* — but
   may observe shared live infrastructure").
3. **Blast radius is revision-granular, not surgical.** Forward-reconciling R_a reverts to the *whole*
   prior desired state, not just the resources the upgrade touched. For a **linear** revision history
   (each apply = one recorded revision) this equals "undo the upgrade"; within-revision selective revert
   is a **forward edit**, not a rollback (Resolved decision 1).
4. **Determinism dependency.** Soundness rests on R_a deterministically reproducing [A final]'s desired
   (Proposal 003 §4). If a future definition construct introduced non-determinism, this breaks — so the
   003 hermeticity guarantee must hold as an invariant, not a convenience.
5. **Forward-engine replacement is real work.** It is, but it is *general* apply work (any immutable
   change needs it) rather than the rollback-only, per-kind, mostly-fail-closed surface of 001 Phase 4.

## Phased landing (workspace green at each step)

1. **Phase 1 — config rollback (same-engine).** `tkp` restores a prior configuration revision and runs
   ordinary `apply`. No engine change. Delivers the *common* rollback immediately.
2. **Phase 2 — forward-engine replacement + destructive `plan` gating.** General apply feature; unblocks
   correct rollback of immutable changes.
3. **Phase 3 — upgrade rollback orchestration (task 8.5).** B delete-only pass (ids-only created set) →
   re-pin → A `refresh_state` + apply R_a, under the operation lock with a resumable marker.
4. **Phase 4 (optional) — audit change log.** Keep an ids+op `ChangeLog` recorded on apply for
   observability and richer `plan`/diff output — *not* a rollback mechanism, no before-images (Decision 4).

## Resolved decisions

All decisions are settled as follows — the §Risks recommendations adopted in full.

1. **Rollback granularity — RESOLVED: linear / revision-granular.** Rollback restores a prior *whole*
   configuration revision and reconciles forward; there is no surgical within-revision rollback. Any
   "undo part of a revision" need is a **forward edit** (author + apply a new revision), not a rollback.
   This matches the retained-revision model and the deterministic definition (Proposal 003 §4); no
   scenario was found where surgical rollback is *required* rather than served by a forward edit.
2. **Authorship — RESOLVED: A reconciles over live via `refresh_state`.** After re-pin, A observes
   provider truth by `describe` and reconciles toward R_a; it never reinterprets B's recorded state
   representation. The invariant is re-scoped from "no binary reads another's *state*" to "no binary
   reads another's *state representation* — but may observe shared live infrastructure."
   `Unsupported`-describe kinds are the accepted, shared fail-closed blind spot.
3. **Replacement + destructive confirmation — RESOLVED: in the forward engine.** Immutable-field
   replacement (delete+create) and destructive-change confirmation in `plan` land in the **forward
   engine**, benefiting every apply — not a rollback-only, per-kind restore surface.
4. **Recorded delta — RESOLVED: audit only.** An **ids-only** change log (`id + op`, no before-images)
   MAY be recorded on apply for observability and richer `plan` output. It is **never** the rollback
   mechanism. This is all that survives of 001.
5. **Requirements edit — RESOLVED: adopted.** The Req 4.6 / 9.2 / 9.6 / glossary revisions in §6 are the
   adopted wording; `requirements.md` is to be updated to match (snapshotting the spec first per repo
   policy). This edit is a prerequisite for task 11.3 proceeding under 002.

---

*001 asks "how do we invert what the upgrade did?" 002 asks "why invert, when we retained the
deterministic definition of where we were, and the engine already drives forward toward a definition?"
The residual hard parts (replacement, destructive confirmation, delete-recreate data loss) are real
under both — 002 puts them in the forward engine where ordinary applies need them too, instead of in a
rollback-only, per-kind, fail-closed capability surface. **002 is the adopted mechanism; 001 is
superseded.***
