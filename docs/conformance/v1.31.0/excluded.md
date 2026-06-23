# Out-of-Scope Surface — Temporal v1.31.0 (definition)

> Part of [the v1.31.0 compliance definition](./README.md). This page defines what is **deliberately
> excluded from the v1.31.0 conformance claim, and why.** It is **definitional, not a status report** —
> an excluded surface is excluded *by design*, not "not done yet". Things that are in the claim but not
> yet achieved live in [`supported.md`](./supported.md) (the target) and
> [`../../readiness/conformance.md`](../../readiness/conformance.md) (measured progress).
>
> Two distinct axes of exclusion appear in this folder. This page is the **public-API** axis (what an
> SDK/operator/tool can reach). The **kernel-internal** axis — public RPCs that exist but are not kernel
> commands (e.g. read-only describes, delivery-layer records) — is in
> [`command-surface.md` Part 4](./command-surface.md#part-4-temporal-apis-deliberately-excluded-from-the-kernel).
> A read-only RPC like `QueryWorkflow` is *excluded from the kernel* yet *in the public claim*; do not
> conflate the two axes.

## 1. Authentication and authorization — not supported

Tokeira does **not** implement Temporal's authentication or authorization layer. There is no
`Authorizer`, no `ClaimMapper`, no JWT/token validation, and no per-namespace/per-API access control
enforcement.

This is not a missing RPC: Temporal has **no auth gRPC service**. Authentication and authorization are
implemented as server **interceptors** that wrap every WorkflowService/OperatorService call, gated by
server configuration. Tokeira admits requests without that gate.

- **Consequence:** every in-scope RPC is reachable by any caller that can reach the gRPC port. A tokeira
  deployment must be fronted by network-level controls (private networking, mTLS termination, a proxy)
  if access restriction is required.
- **Why excluded:** auth is a deployment/policy concern orthogonal to durable-execution correctness, and
  the v1.31.0 *behavioural* contract for the in-scope RPCs is identical whether or not an authorizer is
  configured. Adding it later does not change any in-scope RPC's observable behaviour.
- **Scope note:** this also excludes the API-key / mTLS claim-mapping surfaces and any
  authorization-driven error path (e.g. `PermissionDenied` from an authorizer).

## 2. Internal / admin service surfaces

Temporal's topology exposes services beyond the public two. Tokeira deliberately collapses that topology,
so tests or tools that drive these are out of the public claim by construction:

- **`AdminService`** in its entirety — cluster/shard/membership administration, DLQ management, history
  manipulation, add/list internal tasks.
- **`HistoryService` / `MatchingService` driven directly** — these are Temporal-internal RPC boundaries
  between server roles. Tokeira does not expose them; their behaviour is an implementation detail of a
  multi-role topology tokeira does not have.
- **Persistence / `testBase` pokes** — direct mutable-state inspection/mutation used by Temporal's own
  tests.

The kernel-internal history-engine APIs that fall here (DLQ, task management, replication notifications,
verification/consistency checks, rebuild/import recovery tooling) are enumerated with rationale in
[`command-surface.md` Part 4](./command-surface.md#part-4-temporal-apis-deliberately-excluded-from-the-kernel).

## 3. Multi-cluster replication, failover, and remote routing

- `OperatorService` **remote-cluster registry CRUD** (`AddOrUpdateRemoteCluster`, `RemoveRemoteCluster`,
  `ListClusters`) is metadata-only and tracked, but it does **not** imply replication.
- The replication **behaviour** — cross-cluster history replication, namespace failover, remote task
  routing, replication DLQ — is out of scope. Tokeira is a single-cluster engine.
- Cross-*cluster* Nexus routing is likewise deferred (intra-cluster Nexus is in scope; see
  [`supported.md` → Nexus](./supported.md#nexus)).

## 4. Dead-letter-queue (DLQ) management

`GetDLQMessages`, `PurgeDLQMessages`, `MergeDLQMessages` and the replication DLQ surfaces are
internal/operational task-management concerns, not public durable-execution behaviour. Excluded.

## 5. Legacy and deprecated surfaces

These exist in the v1.31.0 proto for backward compatibility but are deliberately **not** implemented;
their non-deprecated replacements are the in-scope surface:

- **Worker-versioning v0.x build-ID compatibility version-sets** — `UpdateWorkerBuildIdCompatibility`,
  `GetWorkerBuildIdCompatibility`. Replaced by the v2 rule-based worker versioning
  (`Update/GetWorkerVersioningRules`), which **is** in scope.
- **Deployment v0.x (deprecated)** — `DescribeDeployment`, `ListDeployments`, `GetDeploymentReachability`,
  `GetCurrentDeployment`, `SetCurrentDeployment`. Replaced by the worker-deployment family, which is in
  scope.
- **`ScanWorkflowExecutions`** — deprecated visibility scan; the in-scope `List/Count` surface replaces it.

Excluding the deprecated alias while supporting its replacement keeps the surface honest: an SDK pinned
to a current client never calls these.

## 6. RPCs only in the vendored proto (tracked ahead)

Tokeira vendors proto `v1.62.11`, which is newer than the `v1.62.8` that server v1.31.0 ships. The RPCs
present only in `v1.62.11` are **not** part of the v1.31.0 behavioural claim. Today these are the 8 Nexus
**operation-execution** RPCs:

`StartNexusOperationExecution`, `DescribeNexusOperationExecution`, `PollNexusOperationExecution`,
`ListNexusOperationExecutions`, `CountNexusOperationExecutions`, `RequestCancelNexusOperationExecution`,
`TerminateNexusOperationExecution`, `DeleteNexusOperationExecution`.

They are wired as `deferred_unary!` (return `UNIMPLEMENTED`) and tracked separately; bumping the vendored
proto does not move the v1.31.0 server-compat claim. (See `AGENTS.md` §8 and the README's two-pins note.)

## 7. `temporal` CLI commands that target excluded surfaces

The `temporal` CLI is a thin client over the public gRPC surface. Commands that map onto **in-scope**
RPCs (e.g. `temporal workflow …`, `temporal activity …`, `temporal schedule …`, `temporal batch …`,
`temporal operator search-attribute …`, `temporal operator namespace …`, `temporal operator nexus
endpoint …`) are covered to the extent their underlying RPCs are.

Commands that map onto **excluded** surfaces are correspondingly out of scope:

- `temporal operator cluster …` (remote-cluster/replication behaviour beyond registry CRUD).
- Any admin/DLQ tooling (`tdbg` / AdminService-backed commands).
- Auth-dependent flows — the CLI's `--tls*`, API-key, and authorization-header options are accepted on
  the wire but **not enforced** server-side (see §1); they do not change in-scope behaviour.
- `temporal server …` lifecycle commands (tokeira is its own server; these target the bundled Temporal
  dev server).

This is a consequence of the RPC partition above, not a separate decision: the CLI inherits the surface.

## Cross-reference

- [`supported.md`](./supported.md) — the in-scope surface (the target / denominator).
- [`command-surface.md`](./command-surface.md) — kernel-internal command/event exclusions (the other axis).
- [`../../readiness/conformance.md`](../../readiness/conformance.md) — measured progress (the numerator).
