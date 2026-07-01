# Proposal 001 — State-driven restore (`apply_inverse_delta`) for rollback

- **Status:** Proposed (design; gated on the scope decision in §7)
- **Refines:** task 11.3 (`Engine::apply_inverse_delta` + state-driven restore, both engines); unblocks task 8.5 (rollback orchestration)
- **Owner area:** `crates/tokeira-iac` (infra), `crates/tokeira-deploy-engine` + `crates/tokeira-orchestrator` (runtime/service)
- **Provenance:** synthesized from a 3-design judge panel (minimal-default-impls / opt-in-capability-trait / pragmatic-phased), adversarially scored and merged. All claims below were verified against current code.

## Why

Rollback inverts the `AppliedDelta` an upgrade recorded: delete what it created, restore what it
updated to the recorded before-image, re-create what it deleted from the recorded before-image — over
the full current state, in inverse dependency order, fail-closed. The hard part: both the infra
`Resource` and the deploy-engine `Service` are **forward/config-driven** — they drive a live resource
toward `self`'s config, and cannot drive it toward an arbitrary recorded *target state* (the
before-image). That target-state capability is the "central open implementation decision."

## Verified facts this design rests on

- `Resource::delete(current, ctx)` is **already state-driven** (engine.rs drives it from a cloned
  `ResourceState`). Inverting a recorded **Created** therefore needs **no new capability** — just delete.
- `create`/`update` read their target from `&self` (config), not from a passed state (e.g.
  `ssm_parameter::update` ignores `_current` and calls `self.create`). So **restore-Updated** and
  **recreate-Deleted** are the genuinely missing capability.
- The forward apply loop commits **creates+updates in forward topo order, then deletes in reverse topo
  order**. Therefore a delta appended in commit order and inverted by **literal reverse of recorded
  order** is the provably-correct inverse — no re-topo-sort of a heterogeneous record set (which is
  ill-defined: Created lives in the "after" graph, Deleted in the "before" graph).
- `refresh_state` writes `ctx.state` only on `DescribeResult::Present`; on `Unsupported` it leaves
  persisted state untouched. The 16 `Unsupported` describe sites bound which kinds can faithfully opt
  into restore (their before-image may be stale or, like `ssm_parameter` omitting the secret value,
  structurally unrestorable). Those kinds **stay fail-closed by design.**
- `ServiceState` stores only `name/module/manifest_count/desired_hash/last_applied` — **not** manifest
  bodies. The runtime delta must carry manifests inline.
- 34 `impl Resource`, 1 `impl Service` (a mock).

## 1. `AppliedDelta` data model

New `crates/tokeira-iac/src/delta.rs`, re-exported from `lib.rs`, generic over `Id`/`Img` so the same
shape serves both engines:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op")]
pub enum ChangeRecord<Id, Img> {
    Created { id: Id, after: Img },
    Updated { id: Id, before: Img, after: Img },
    Deleted { id: Id, before: Img },
}

/// Ordered committed ops, in FORWARD (literal commit) order. Inversion walks
/// `changes` in reverse. `recorded_by` is opaque provenance used by tkp (not the
/// engine) to enforce the authorship invariant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct AppliedDelta<Id, Img> {
    pub recorded_by: Option<String>,
    pub changes: Vec<ChangeRecord<Id, Img>>,
}

pub type InfraAppliedDelta = AppliedDelta<crate::ResourceId, crate::ResourceState>;
```

**Recording (additive, zero caller churn).** Thread a private `Option<&mut InfraAppliedDelta>` recorder
through `apply_changes`/`destroy_changes`. Add one public sibling
`apply_with_known_recording(...) -> (Vec<Change>, InfraAppliedDelta)`; existing methods delegate with
`None`. Emission is **strictly post-success, after the existing `ctx.state` mutation and `saver` call**,
so every recorded change corresponds to a committed, persisted live mutation. `before` comes from the
refresh (an *observed* live image **only for `Present` kinds**).

## 2. Capability shape — DECISION: opt-in trait + one defaulted bridge

```rust
// crates/tokeira-iac/src/restore.rs
#[async_trait::async_trait]
pub trait StateDrivenRestore: Resource {
    /// Drive the live resource (currently `current`, the after-image) back to
    /// `target` (the before-image). Inverse of an Updated. MUST be idempotent.
    async fn restore_to_state(&self, current: &ResourceState, target: &ResourceState,
        ctx: &ProvisionContext) -> Result<ResourceState, IacError>;
    /// Re-create to match `target` (the before-image). Inverse of a Deleted. Idempotent.
    async fn recreate_from_state(&self, target: &ResourceState,
        ctx: &ProvisionContext) -> Result<ResourceState, IacError>;
}

// The ONLY addition to the core trait — a defaulted bridge (stable Rust can't
// upcast dyn Resource -> dyn StateDrivenRestore without unstable trait_upcasting):
pub trait Resource: Send + Sync {
    // ... existing unchanged ...
    fn as_state_driven_restore(&self) -> Option<&dyn StateDrivenRestore> { None }
}
```

All 34 impls inherit `None` and **compile untouched**. New `IacError::RestoreUnsupported { resource_id,
resource_type }` mirrors the existing `UnknownResourceDelete` fail-closed posture.

**Why (b) over (a) default-erroring methods:** (a) makes every impl *nominally* restore-capable, so a
future impl that adds real `create` but forgets `restore` silently inherits the erroring default and
fails mid-rollback — the exact "introduced of a kind it can no longer instantiate" hazard, at the worst
time. (b) makes the capability a compile-time, greppable, type-level fact and collapses fail-closed to
one engine branch. **Why not (c) registry:** a second source of truth that drifts; and modeling
restore-Updated as delete+recreate would destroy live data (DSQL clusters, S3 buckets) to revert a tag.

## 3. `apply_inverse_delta` algorithm

```rust
impl Engine {
    pub async fn apply_inverse_delta(&self, known: &[&dyn Resource], delta: &InfraAppliedDelta,
        ctx: &mut ProvisionContext, saver: Option<&StateSaver>) -> Result<Vec<Change>, IacError>;
}
```

**Pass 1 — pre-flight, fail-closed, NO mutation:** build `resource_map` from `known`; every
`record.id()` must be in it (else `RestoreUnsupported` — same contract as `UnknownResourceDelete`); every
`Updated`/`Deleted` requires `as_state_driven_restore().is_some()` (else `RestoreUnsupported`). Returning
here leaves state + all providers untouched.

**Pass 2 — invert in LITERAL reverse of recorded order**, mutating state only after each op succeeds
(`delta.changes.iter().rev()`):
- **Created{id, after} → DELETE.** `current = state.get(id)`; absent ⇒ done (idempotent). Else
  `delete(&current)`; remove from state; save. Shares the `delete_one` helper with `destroy_selected`.
- **Updated{id, before, after} → RESTORE.** Short-circuit if current ≈ `before`. Else
  `restore_to_state(&cur, before, ctx)`; insert; save.
- **Deleted{id, before} → RE-CREATE.** Short-circuit if present ≈ `before`. Else
  `recreate_from_state(before, ctx)`; insert; save.

First provider error returns immediately with state reflecting every succeeded op; `tkp` resumes from
the operation marker and re-invokes — idempotent short-circuits skip already-inverted records. A public
`destroy_selected(known, ids, ctx, saver)` is added for the delete-only sub-case.

## 4. Service / runtime side (greenfield, structurally parallel — simpler)

`Platform::apply_manifests` is **already a state-driven reconcile** and manifests are self-describing, so
restore/recreate reduce to "re-apply the recorded before-image manifests." Only inverting a Created needs
genuinely new surface (a platform delete).

- **`ServiceImage { name, module, manifests: Vec<Value>, desired_hash }`** — superset of `ServiceState`,
  adding the manifests it lacks. `type RuntimeAppliedDelta = AppliedDelta<String, ServiceImage>`.
- **`Platform`** gains `delete_service(name, manifests)` (default `Err` ⇒ fail-closed refuse) +
  `supports_delete() -> bool` (default `false`).
- **`Service`** gains the defaulted bridge `as_state_driven_service()`; a `StateDrivenService` sub-trait
  with `manifests_for(target)` covers kinds that must recompute rather than replay stored manifests
  (most replay, so most never implement it).
- **`apply_services_recording`** captures `before`/`after` `ServiceImage`s (manifests inline).
- **`ServiceEngine::apply_inverse_delta`** mirrors infra: pre-flight (name resolves; Created requires
  `supports_delete`); reverse-order invert (Created → `delete_service`; Updated/Deleted →
  `apply_manifests(before.manifests)`).

## 5. Phased landing (workspace green at each step)

1. **Phase 1** — `delta.rs` (types + serde round-trip test). Additive. Green.
2. **Phase 2** — `restore.rs` (`StateDrivenRestore`), defaulted `Resource::as_state_driven_restore`,
   `IacError::RestoreUnsupported`. 34 impls compile unchanged. Green.
3. **Phase 3 (SHIPPABLE infra half of 11.3)** — `apply_inverse_delta` + `delete_one` +
   `destroy_selected` + `apply_with_known_recording`. Stub-resource tests: Created inverts via delete;
   non-capable Updated/Deleted fail-closes pre-mutation; capable stub round-trips; unknown-id refusal;
   idempotent re-run. Green with **zero production opt-ins**.
4. **Phase 4 (opt-in real restorers, demand-driven)** — per kind the first upgrade touches: refactor
   `create`/`update` to source from a passed `ResourceState`, impl `StateDrivenRestore`, add the
   one-line bridge override + a round-trip property test. Each an isolated PR; non-opted kinds stay
   fail-closed.
5. **Phase 5 (runtime greenfield)** — `ServiceImage`, `RuntimeAppliedDelta`, `Platform::delete_service`
   + `supports_delete`, `Service::as_state_driven_service` + `StateDrivenService`,
   `apply_services_recording`, `ServiceEngine::apply_inverse_delta`; implement on the compose Platform.
6. **Phase 6 (tkp wiring, task 8.5 — out of scope for 11.3)** — persist deltas into the operation
   marker; invert runtime-then-infra under one operation lock; enforce authorship via `recorded_by`.

## 6. Risks

1. **Per-kind restore correctness is the real work (Phase 4).** `update` reads `self`, not state — each
   opt-in needs `create`/`update` refactored to a state arg, and round-trip only holds if `properties`
   losslessly round-trips through `describe`.
2. **Before-image fidelity needs `describe == Present`.** `Unsupported` kinds carry stale before-images;
   some (ssm_parameter omits the secret value) are structurally unrestorable → stay fail-closed.
3. **Crash-consistency:** record only after op succeeds AND state saved; idempotent re-runs cover a lost
   trailing record.
4. **`recreate_from_state` physical-id assumption:** many providers can't recreate under the recorded
   id (S3 names in a deletion window, provider-assigned ARNs) → per-kind audit + decision §7.4.
5. **Runtime state-model:** `ServiceImage` carries manifests inline; do not bloat steady-state
   `ServiceState`.

## 7. Owner decisions (sign-off before coding the gated phases)

1. **Where 11.3 is DONE.** (A) end of Phase 3 — engine capability + recording + fail-closed, zero real
   restorers, runtime deferred; or (B) include the first-upgrade priority restorers (Phase 4) and/or the
   Service half (Phase 5). *Rec: (A)* — but accept that at Phase 3 rollback **refuses for every non-opted
   kind** (rolls back nothing concrete) until a real restorer lands. If the first upgrade must roll back
   specific resources, scope those kinds in.
2. **Which kinds get real restorers first** (bounds Phase 4). *Rec:* the minimal set from the first real
   upgrade's diff — likely **DSQL cluster + S3 remote-state**, possibly an ECS service; exclude
   `Unsupported`-describe / non-reconstructable kinds.
3. **Restore refactor per opted-in kind.** (A) refactor `create`/`update` to source from a passed state
   (one path, one round-trip test); or (B) hand-write restore/recreate. *Rec: (A)* where create/update
   re-apply config; (B) for awkward kinds.
4. **Unreachable in-place restore** (immutable fields / non-recreatable id). (A) fail-closed + operator
   remediation; or (B) delete+recreate fallback. *Rec: (A)* — never destroy a live resource to revert
   an in-place change; defer (B).
5. **Idempotency equality key.** (A) structural `(physical_id + properties)` / `desired_hash`; or (B)
   semantic via `diff()`. *Rec:* (A) but route the Resource compare through `diff()` to avoid false
   skips from JSON key-ordering.
6. **Authorship identity.** Keep the engine binary-agnostic; enforce `recorded_by` vs the envelope
   binding in `tkp` (exact-equal initially). Owner defines binding identity (version + engine kind)
   before 11.3 wires into 8.5.
7. **Runtime manifests in the delta.** Carry manifests inline in `ServiceImage` (rec) vs enrich
   `ServiceState`. *Rec:* inline-only; do not bloat steady-state state.
