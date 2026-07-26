# Pending Decisions (TBD) — v1.31.0 conformance surface

> Part of [the v1.31.0 conformance definition](./README.md). These surfaces are **present in Temporal
> v1.31.0** but their place in the conformance surface is **not yet decided**. They are neither in
> [`supported.md`](./supported.md) nor [`excluded.md`](./excluded.md) until a decision is recorded here.
> This page exists so the open questions are visible and tracked, not silently resolved. Resolved
> decisions move to their own record and are listed at the bottom.

**No decisions are currently open.**

## Resolved decisions

- **Task Queue Priority and User Fairness** — resolved 2026-07-24: in-surface. Priority-aware task
  delivery uses the v1.31.0 stock defaults (`matching.useNewMatcher=true`, five levels, default key 3).
  Weighted User Fairness is supported but retains the stock-default disabled posture
  (`matching.enableFairness=false`, `matching.autoEnableV2=false`); the conformance-only override bridge
  exercises enabled and auto-enable modes. Tokeira realizes the behavior in its runtime delivery
  broker and durable backlog rather than introducing Temporal matching/history service objects, and
  the pure kernel retains no scheduler state. `UpdateTaskQueueConfig` is live, atomic, and
  kind-isolated; its current store remains volatile while the broader production configuration and
  durability proposal is under owner review. Owning record:
  [`.kiro/specs/task-queue-priority-fairness/`](../../../.kiro/specs/task-queue-priority-fairness/).
- **Authentication and authorization** — resolved 2026-07-16: in-surface, in two layers. The
  stock-default layer (no-op claim mapper + no-op authorizer: every call allowed, no principal on
  events) is conformant by construction today and is the release gate. The configured layer —
  default JWT claim mapper + default authorizer + Principal Attribution, gated by the same knobs
  as v1.31.0 (in tokeira: static `[policy.authorization]` TOML, presence-enables; the
  dynamic-config key names are honoured only via the conformance-only override registry, never
  in production) — is scoped in `.kiro/specs/authorization-foundation/`, which also adds the
  tokeira-native AWS IAM presigned-STS bearer (product surface, not v1.31.0 surface).
  Transport stance: bearer-only at the edge, TLS terminated upstream; mTLS-derived identity and
  the Go plugin points are out of surface. The original TBD text's
  `system.enablePrincipalAttribution` claim was found to be **wrong** (no such key exists in
  v1.31.0 — the actual gate is `frontend.enablePrincipalPropagation`); the correction is part of
  the record. Full record with the factual case: [`authorization.md`](./authorization.md).
- **Worker Versioning V1 and V2** — resolved 2026-07-12: conformance targets the GA Worker
  Deployment APIs only; the five deprecated V1/V2 RPCs stay in-surface solely as their
  stock-default rejections (the exact `PERMISSION_DENIED` errors a default-configuration
  v1.31.0 server produces). Full record with the factual case:
  [`worker-versioning.md`](./worker-versioning.md).

## Related pages

- [`supported.md`](./supported.md) — the decided in-surface set.
- [`excluded.md`](./excluded.md) — the decided out-of-surface set.
- [`authorization.md`](./authorization.md) — the resolved authentication/authorization decision record.
- [`worker-versioning.md`](./worker-versioning.md) — the resolved worker-versioning decision record.
