# Pending Decisions (TBD) — v1.31.0 conformance surface

> Part of [the v1.31.0 conformance definition](./README.md). These surfaces are **present in Temporal
> v1.31.0** but their place in the conformance surface is **not yet decided**. They are neither in
> [`supported.md`](./supported.md) nor [`excluded.md`](./excluded.md) until a decision is recorded here.
> This page exists so the open questions are visible and tracked, not silently resolved.

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

## 2. Worker Versioning V1 and V2 — TBD

**Temporal v1.31.0 states** these are **deprecated**:

- **V1 (build-ID compatibility / version sets):** `UpdateWorkerBuildIdCompatibility`,
  `GetWorkerBuildIdCompatibility`.
- **V2 (assignment / versioning rules):** `UpdateWorkerVersioningRules`, `GetWorkerVersioningRules`,
  `GetWorkerTaskReachability`.

The v1.31.0 release notes present the GA **Worker Deployment** APIs (in [`supported.md`](./supported.md))
as the path forward for worker versioning.

**Decision required:** whether the deprecated V1/V2 surface is part of the conformance surface (for
clients still using it) or whether conformance targets only the GA Worker Deployment APIs.

## Related pages

- [`supported.md`](./supported.md) — the decided in-surface set.
- [`excluded.md`](./excluded.md) — the decided out-of-surface set.
