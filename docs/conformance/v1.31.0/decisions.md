# Pending Decisions (TBD) — v1.31.0 conformance surface

> Part of [the v1.31.0 conformance definition](./README.md). These surfaces are **present in Temporal
> v1.31.0** but their place in the conformance surface is **not yet decided**. They are neither in
> [`supported.md`](./supported.md) nor [`excluded.md`](./excluded.md) until a decision is recorded here.
> This page exists so the open questions are visible and tracked, not silently resolved. Resolved
> decisions move to their own record and are listed at the bottom.

## 1. Authentication and authorization — TBD (before first release)

**Temporal v1.31.0 states:** authentication and authorization are implemented as server **interceptors**
(an `Authorizer` and `ClaimMapper`), gated by configuration — there is no auth gRPC service. v1.31.0
adds **Principal Attribution**: a server-computed, immutable `Principal` (`Type`/`Name`, e.g.
`jwt/alice@company.com`) on history events, derived from authenticated context, enabled via
`system.enablePrincipalAttribution` (off by default). The default `Authorizer` populates `Principal` from
the JWT `sub` claim.

**Decision required:** whether and how authentication/authorization (and Principal Attribution) are part
of the conformance surface. This must be resolved **before the first release**, because it determines
whether a deployment can restrict access and whether history events carry trustworthy attribution.

## Resolved decisions

- **Worker Versioning V1 and V2** — resolved 2026-07-12: conformance targets the GA Worker
  Deployment APIs only; the five deprecated V1/V2 RPCs stay in-surface solely as their
  stock-default rejections (the exact `PERMISSION_DENIED` errors a default-configuration
  v1.31.0 server produces). Full record with the factual case:
  [`worker-versioning.md`](./worker-versioning.md).

## Related pages

- [`supported.md`](./supported.md) — the decided in-surface set.
- [`excluded.md`](./excluded.md) — the decided out-of-surface set.
- [`worker-versioning.md`](./worker-versioning.md) — the resolved worker-versioning decision record.
