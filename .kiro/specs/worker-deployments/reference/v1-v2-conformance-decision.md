# Worker Versioning V1/V2 — decision record

> Historical decision record owned by the Worker Deployments spec. The
> authoritative public outcome is summarized in
> [`supported.md`](../../../../docs/conformance/v1.31.0/supported.md#worker-deployments).
> This record resolves whether the **deprecated Worker Versioning V1
> (build-ID version sets) and V2 (versioning rules)** surfaces are part of the conformance
> surface, or whether conformance targets only the GA **Worker Deployment** APIs.
> Decided **2026-07-12**. Every claim below was verified against ground truth — the v1.31.0
> server source, the `v1.62.8` proto, Temporal's release notes and docs — and independently
> re-verified before recording.

## The decision

**Conformance targets the GA Worker Deployment surface only. The five V1/V2 RPCs remain
in-surface solely in their stock-default form: they are admitted and rejected with the exact
errors a default-configuration v1.31.0 server produces.** The enabled-path semantics (version
sets, versioning rules, rule-computed reachability — reachable only through non-default dynamic
config) are **out of surface**.

Concretely, tokeira answers:

| RPC | Response |
|-----|----------|
| `UpdateWorkerBuildIdCompatibility` | `PERMISSION_DENIED` — `Worker versioning v0.1 (Version Set-based, deprecated) is disabled on this namespace.` |
| `GetWorkerBuildIdCompatibility` | same v0.1 message |
| `UpdateWorkerVersioningRules` | `PERMISSION_DENIED` — `Worker versioning v0.2 (Rules-based, deprecated) is disabled on this namespace.` |
| `GetWorkerVersioningRules` | same v0.2 message |
| `GetWorkerTaskReachability` | the **v0.2** message (see the quirk below) |

Each status carries a `temporal.api.errordetails.v1.PermissionDeniedFailure` detail with an
empty `reason`, matching `serviceerror.NewPermissionDenied(message, "")`, and is emitted
**before any field validation** — inside the handler, upstream checks the gate first, before
task-queue normalization or request-field validation. (Namespace *existence* is a different
layer upstream: the frontend's `NamespaceValidatorInterceptor` runs before every handler, so an
empty namespace gets `INVALID_ARGUMENT: Namespace not set on request.` and an unregistered one
gets `NOT_FOUND` before the gate is ever consulted. tokeira does not model that interceptor —
a pre-existing, repo-wide surface difference on degenerate inputs, not specific to these RPCs —
so tokeira returns the gate rejection for those inputs too.)

## Why: the factual case

### 1. Stock v1.31.0 rejects all five RPCs by default

The V1/V2 API surface is disabled out of the box, per namespace, by dynamic config:

- `frontend.workerVersioningDataAPIs` — default **`false`** — gates
  `UpdateWorkerBuildIdCompatibility`, `GetWorkerBuildIdCompatibility`, **and**
  `GetWorkerTaskReachability` (`common/dynamicconfig/constants.go:1054-1058`,
  `service/frontend/workflow_handler.go:5337,5379,5491` @ v1.31.0).
- `frontend.workerVersioningRuleAPIs` — default **`false`** — gates
  `UpdateWorkerVersioningRules`, `GetWorkerVersioningRules` (`constants.go:1064-1068`,
  `workflow_handler.go:5412,5452`).

When the gates are off the frontend returns `PERMISSION_DENIED` with fixed messages that
themselves declare the deprecation (`service/frontend/errors.go:131-132`). Within the handler,
the gate check runs immediately after the request-nil check and **before** the handler's
namespace lookup, task-queue normalization, or any field validation — a call with an invalid
task queue or missing fields on a registered namespace gets `PERMISSION_DENIED`, not
`INVALID_ARGUMENT`. (Namespace existence is validated *earlier* by the frontend's
`NamespaceValidatorInterceptor` — `service/frontend/fx.go:276`,
`common/rpc/interceptor/namespace_validator.go:308-313` — so on stock an empty or unregistered
namespace errors before the gate; see "What this decision does NOT exclude".)

**The quirk:** `GetWorkerTaskReachability` is gated by the **v0.1 data flag** but returns the
**v0.2 error message** (`workflow_handler.go:5491-5493`). Field-level conformance reproduces
this exactly.

tokeira deliberately ships close-to-zero configuration
([`tokeira-configuration.md`](../../../../docs/conformance/v1.31.0/tokeira-configuration.md)),
so "default dynamic config" is the only coherent behavioural baseline. The rejection **is** the
conformant behaviour; implementing the enabled path would require adding configuration tokeira
has chosen not to have.

### 2. V1/V2 never reached GA and are sunsetted with a published removal date

- **v1.24.0** deprecated V1 (build-ID compatibility) in favour of V2 — which was itself labeled
  *Experimental* and config-gated from birth.
- **v1.28.0** deprecated all five RPCs together, in the same release where Worker Deployments
  entered Public Preview — V1/V2 were deprecated before their replacement was even GA.
- **v1.31.0** release notes declare exactly the nine Worker Deployment RPCs "fully GA" and
  declare the five V1/V2 RPCs **officially sunsetted**, with removal slated for **v1.32.0**
  ("Will be removed in server version v1.32.0." appears on all five in api ≥ `v1.62.13`).
- On **Temporal Cloud**, V1 (the "2023 draft") was never made available at all; V2 (the "2024
  draft") was an opt-in pre-release only. Worker-Deployment-based versioning reached GA on
  2026-03-30.

Implementing V1/V2 would mean building, from scratch, a feature that never reached GA in its
lifetime and has a published death date one server release after the conformance target.

### 3. No SDK worker touches these RPCs implicitly

In every official SDK (Go, Java, TypeScript, Python, .NET, and the Rust core underlying
TS/Python/.NET/Ruby), the five RPCs exist only as explicitly **deprecated client wrappers**
pointing at the Worker Deployment API. Workers carry versioning data purely as poll-request
*fields* (`worker_version_capabilities`, `deployment_options`) — never by calling these RPCs.
The Temporal CLI marks all its build-ID/rules commands deprecated. A user who explicitly calls
a deprecated method against a rejecting server gets a clean, synchronous typed error
(`serviceerror.PermissionDenied`, reconstructed from the status detail) — no worker crash loops.

### 4. Known consumers need none of it

- **tokeira-odori** uses no versioning surface; its Rust SDK workers send GA
  `deployment_options` on every poll (a GA-side field obligation, unaffected by this decision).
- The sibling worker-compute provider's worker-fleet specs are built entirely on the GA deployment surface
  (`WorkerDeploymentVersionInfo.compute_config`, `WorkerDeploymentInfo.manager_identity`,
  `DescribeWorkerDeployment(Version)`), with zero references to version sets, rules, or
  reachability.

### 5. The cost asymmetry is extreme

Faithful enabled-path V1 means version sets with HLC merge semantics, task-queue user-data
persistence, a build-ID scavenger, and visibility integration (~1,100 lines of upstream matching
core); faithful V2 adds durable rules, per-queue limits, and visibility-backed reachability. The
406-test `TestVersioningFunctionalSuite` only passes against a stock server **after flipping
non-default dynamic config** — it validates a surface stock servers do not serve. Meanwhile the
GA deployment surface is already implemented end-to-end in tokeira (DSQL-backed repository,
runtime registry, entity-equivalent routing).

## What this decision does NOT exclude

Excluding the five RPCs does not excuse four pieces of shared field machinery that a stock
default-config v1.31.0 server serves regardless. These stay in
[`supported.md`](../../../../docs/conformance/v1.31.0/supported.md)
under the field-fidelity bar:

1. **`WorkerVersionStamp` / `binary_checksum` acceptance-and-echo** on WFT completion — feeds
   `most_recent_worker_version_stamp`, the `BuildIds` search attribute, reset points, and the
   **non-deprecated** reset-by-build-id target.
2. **Poller validation for deprecated `worker_version_capabilities`** —
   `frontend.workerVersioningWorkflowAPIs` defaults to **`true`**, so stock *accepts*
   `use_versioning=true` pollers by default; the deprecated capabilities field is also a legacy
   carrier of GA deployment identity (`deployment_series_name`).
3. **`GetSystemInfo` capabilities** — stock unconditionally advertises
   `BuildIdBasedVersioning: true` (`workflow_handler.go:3387`) even with the data/rules APIs
   disabled; Core-based SDK workers change their wire shape based on this bit.
4. **`DescribeTaskQueue` ENHANCED mode + `report_task_reachability`** — V2-shaped surface
   embedded in a GA RPC, ungated in stock. With the write RPCs denied, `VersioningData` is
   permanently empty, so the observable behaviour degenerates to the single unversioned
   pseudo-build-id entry and empty-rules reachability. (The proto marks ENHANCED mode itself
   deprecated.)

## Consequences in tokeira

- The five gRPC handlers return the exact stock-default `PERMISSION_DENIED` statuses
  (`crates/tokeira-edge/src/grpc/workflow_service.rs`, builders in
  `crates/tokeira-edge/src/grpc/errors.rs`). Before this decision tokeira deviated in **both**
  directions: V1 returned `UNIMPLEMENTED`, and V2 was a live, *accepting* in-memory
  implementation — more permissive than stock default.
- The non-durable in-memory V2 rules store (`VersioningRuleStore`) and its dispatch
  integration (assignment-rule stamping at start/schedule, redirect-rule rewriting at publish)
  are **removed**. With the write RPCs rejected the store could never be populated; deleting it
  removes dead state rather than hardening it.
- `TestVersioningFunctionalSuite` (406 tests) is formally **out of surface**: it requires
  non-default dynamic config (`frontend.workerVersioningDataAPIs` /
  `frontend.workerVersioningRuleAPIs` = true) that tokeira, by design, does not expose. GA
  coverage continues via the Worker Deployment suites
  ([`functional-test-order.md`](../../../../docs/readiness/functional-test-order.md), Tier 8).

## The counter-case, weighed

The strongest argument for fuller V1/V2 support: the conformance definition says "same RPCs
admitted, same error codes", and a stock server *does* admit these RPCs — some self-hosted user
who explicitly opted into V2 rules could be mid-migration. That argument succeeds only on the
narrow front this decision adopts (stock-default rejection semantics, byte-identical). The
enabled path fails on every other axis: near-empty user population (explicit opt-in required,
"not yet stable" at the pinned proto, never GA anywhere, never available on Cloud), a published
v1.32.0 removal upstream, and a 406-test validation burden for a surface stock servers refuse
by default.

## Related pages

- [`supported.md`](../../../../docs/conformance/v1.31.0/supported.md) — the in-surface
  set (GA Worker Deployments; shared field
  machinery above).
- [`excluded.md`](../../../../docs/conformance/v1.31.0/excluded.md) — the enabled-path
  V1/V2 semantics, recorded as a deprecated
  exclusion.
- [Public conformance definition](../../../../docs/conformance/v1.31.0/README.md) —
  no conformance-surface decisions are currently open.
