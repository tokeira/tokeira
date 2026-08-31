# tokeira-managed-dsql

Crash-safe lifecycle ownership for a dedicated Aurora DSQL cluster used by an
embedded Tokeira engine.

## Where it sits

This cross-cutting crate is the narrow AWS control-plane boundary for embedded
startup. It owns managed cluster identity and lifecycle, while storage owns
database connections and schema and `tokeira-engine` owns startup composition.

## Surface map

| Area | Representative contracts |
|---|---|
| Lifecycle | `ManagedDsqlLifecycle`, `CreateOrRecoverRequest`, `ResolvedCluster`, `UsableCluster`, `ClusterAction` |
| Durable descriptor | `ClusterDescriptorStore`, `LocalClusterDescriptorStore`, versioned descriptor state, `DsqlClientToken` |
| Canonical identity | `CanonicalClusterIdentity`, identity validation |
| AWS seam | `DsqlControlPlane`, `AwsDsqlControlPlane`, cluster observations and typed control errors |
| Time and retries | `StartupDeadline`, `LifecycleEnvironment`, bounded `RetryPolicy` |
| Administration | `ManagedDsqlAdmin`, `DestroyPlan`, `ExplicitConfirmation`, `DestroyReport` |

## Contracts

- A durable creation token makes create replay idempotent after interruption.
- The descriptor stores canonical cluster identity; tags and connection
  endpoints are not discovery or identity.
- Startup either creates, recovers, or validates the explicit cluster and
  returns typed readiness under one deadline.
- Only an active `UsableCluster` may proceed to storage-owned schema checks.
- Ordinary engine startup and shutdown do not receive deletion capability.
- Destruction follows plan, explicit confirmation bound to that exact plan,
  revalidation, and apply. A changed descriptor or cloud observation makes the
  plan stale.
- Confirmed destruction is idempotent and records a durable destroyed state.

## It does not own

The crate does not implement workflow storage, DSQL SQL, schema migrations,
runtime execution, CLI presentation, listener setup, or kernel semantics. It
also does not infer permission to create or delete resources from an endpoint.

## Pointers

- [Crate root](../../crates/tokeira-managed-dsql/src/lib.rs)
- [Lifecycle state machine](../../crates/tokeira-managed-dsql/src/lifecycle.rs)
- [Plan-bound administration](../../crates/tokeira-managed-dsql/src/admin.rs)
- [Engine facade](engine.md)
- [Storage](storage.md)
