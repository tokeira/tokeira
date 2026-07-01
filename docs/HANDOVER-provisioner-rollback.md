# Hand-over — provisioner rollback: direction decided (definition-driven)

**Author:** Kiro · **Date:** 2026-07-01 · **For:** Claude (continuing `platform-provisioner-binary`)

## TL;DR

The rollback mechanism is **decided**: rollback is **definition-driven** — restore the retained prior
**configuration revision** and let the **forward engine reconcile** toward it. It is **not** an inverse
of a recorded `AppliedDelta`. **Proposal 002 is accepted; Proposal 001 (state-driven restore) is
superseded** as the rollback mechanism (only its recorded change-log survives, demoted to an optional
ids-only *audit* record).

Read [`proposals/002-definition-driven-rollback.md`](../.kiro/specs/platform-provisioner-binary/proposals/002-definition-driven-rollback.md)
in full — it has the rationale, the algorithm, the phased plan, and the resolved decisions.

## Why (one paragraph)

The `.tkd` deployment definition is a deterministic, hermetic, **retained** config revision (config-dsl
Proposal 003), and the IaC engine is already a forward reconciler (desired-vs-`known`, deletes what's no
longer desired). So the prior revision *is* the before-image — authoritative and lossless for everything
the definition determines. Inverting a recorded delta adds a **second, divergence-prone** representation
of the prior state plus a large, mostly fail-closed per-kind `StateDrivenRestore` surface — to do what
re-applying a retained revision already does. `design.md` already contains the seed of this in
*"This also bounds the heavy rollback"* (config revert = ordinary same-engine apply of the prior config);
002 simply extends that same reasoning to the engine-upgrade case.

## The mechanism (what to build)

- **Config rollback (same engine — common):** restore the prior revision, `tkp apply`. One binary, no
  checkpoint, no delta. (design.md already says this.)
- **Upgrade rollback (cross engine A→B):** two operations, respecting authorship —
  1. **B deletes what it created** (`keys(S_B) − keys(S_A)`); delete is already state-driven, so **no new
     capability, no before-images**. B alone can remove resources of kinds only B knows.
  2. **Re-pin binding → A**, close the marker.
  3. **A observes live (`refresh_state`) and re-applies the retained prior configuration revision**,
     reconciling B's remaining updates/re-creations from A's own config.

## Resolved decisions (all settled — 002 §Resolved decisions)

1. **Linear / revision-granular** rollback. Surgical within-revision revert is a *forward edit*, not a rollback.
2. **Authorship re-scoped:** A reconciles over **live via `describe`/`refresh_state`**, never over B's recorded state representation. `Unsupported`-describe kinds are the accepted shared blind spot.
3. **Replacement + destructive-change confirmation live in the forward engine** (benefit every apply) — not a per-kind restore surface.
4. **Recorded delta = audit only** (ids + op, no before-images). Never the rollback path.
5. **Requirements edited to match** (done — see below).

## What I changed this session

- **`requirements.md` — DONE (updated to 002).** Revised: glossary *Rollback*, *Applied delta*,
  *Rollback checkpoint*, *Binding / operation marker*, *Authorship invariant*; and Req **4.6**, **9.2**,
  **9.3**, **9.6**. All inverse/before-image language is now negated/corrected; no diagnostics.
  Pre-edit snapshot: `/tmp/tokeira-spec-snapshots/20260701-114934-provisioner/requirements.md`.
- **`002-definition-driven-rollback.md` — authored + marked Accepted**, 001 superseded.
- **`001-state-driven-restore.md` — left in place**, now superseded (kept for provenance).

## What is now STALE and needs reconciling to 002 (Claude's action)

I deliberately did **not** touch these (large, and you're active in them). They still describe the
inverse-delta model and contradict the updated `requirements.md`:

- **`design.md`:**
  - Principle **4** ("Rollback inverts the applied delta, not reverse-migration") → rewrite to
    definition-driven forward reconcile.
  - The **verbs table** rows `rollback — undo` / `rollback — reconcile` (currently "inverse of B's
    `AppliedDelta`") → B deletes its creates; A re-applies prior revision.
  - §**"Rollback inverts the applied delta, then A reconciles"** and §**"Verifying [A final]"** (observed
    before-images) → the before-image grounding goes away; the retained revision is the baseline.
  - §**"This also bounds the heavy rollback"** already agrees for config — extend it to upgrades.
- **`tasks.md`:**
  - **8.4** — `upgrade` records the applied delta *with before/after per change* → **ids-only audit** log.
  - **8.5** — `rollback`: "invert the applied delta" → **B delete-only → re-pin → A re-apply retained
    revision**; drop "verify every resource in the applied delta is still instantiable by B".
  - **11.3** — **drop** `apply_inverse_delta` + `StateDrivenRestore`/`restore_to_state`/`recreate_from_state`;
    **replace** with forward-engine **replacement** (immutable-field → delete+create) + **destructive-change
    confirmation in `plan`** (general apply features). `destroy_selected` (delete-only) is still useful.
  - **13.1** — `operation` marker payload "with applied delta" → phase + resumable progress (+ optional audit log).

## Do / Don't

- **Do:** implement 002's phases (config-rollback first; forward-engine replacement + destructive
  `plan` gating; upgrade orchestration; optional audit log). Reconcile design.md + tasks.md as above.
- **Don't:** build `AppliedDelta` before-images, `StateDrivenRestore`, `apply_inverse_delta`, or per-kind
  restorers. That is the superseded 001 path.

## Open (owner may still weigh in)

- The **forward-engine replacement** capability (task 11.3's replacement) is net-new engine work and the
  one real implementation cost of 002 — but it's needed for ordinary applies too, so it belongs there.
- Nothing else is open on the rollback mechanism; the five decisions above are settled.
