# tokeira-auth

Transport-independent identity and authorization primitives for the
Temporal-compatible edge.

## Where it sits

The crate supports the compatibility edge without depending on HTTP, gRPC,
runtime, storage, or kernel code. Transport adapters turn credentials into the
strings accepted by `ClaimMapper`, classify the intended call, and pass the
result to `Authorizer`.

## Surface map

| Area | Representative contracts |
|---|---|
| Claims | `Claims`, `AuthPrincipal`, Temporal-compatible `Role` bits |
| Authentication seam | `ClaimMapper`, `MultiSourceClaimMapper`, `AuthError` |
| JWT | `JwtAuthenticator`, `JwtIssuerProfile`, `JwksKeyProvider` |
| AWS IAM | `StsAuthenticator`, presigned STS validation helpers |
| Call policy | `CallClassification`, `CallTarget`, `Scope`, `Access` |
| Authorization | `Authorizer`, `DefaultAuthorizer`, `AuthzDecision` |
| Grants | `Grant`, `GrantRule`, `GrantRules`, glob patterns |
| Worker attenuation | `WorkerScope`, Worker operation and target matching |

## Contracts

- Authentication failures retain an internal diagnostic while the edge exposes
  the targeted Temporal release's generic denial behaviour.
- Roles and default authorization follow the pinned Temporal server semantics,
  including cluster- and namespace-scoped decisions.
- Unknown scopes and unknown access classifications fail closed.
- Worker-scoped credentials can authorize only the exact Worker operation,
  namespace, queue, deployment, and build constraints they carry.
- Grant matching adds roles to claims; it does not perform a transport action.
- A successful decision may return a server-computed principal for durable
  history attribution.

## It does not own

The crate does not parse gRPC metadata, choose RPC classifications, store users,
issue credentials, persist principals, or enforce a decision at a network
boundary. Those are adapter and edge responsibilities.

## Pointers

- [Crate root](../../crates/tokeira-auth/src/lib.rs)
- [Compatibility edge](edge.md)
- [Compatibility metadata](compatibility.md)
- [Architecture decisions](../architecture/005-decisions-and-boundaries.md)
