# What "Tokeira targets Temporal v1.31.0" means

> **This is a definitional document, not a status report.** It describes *what conformance to Temporal
> v1.31.0 entails* — the surface and its maturity, stated in Temporal's own terms — so an operator, SDK
> user, or evaluator can understand the shape and bound of the target. It does **not** assert how much
> has been built. For measured progress, see
> [`../../readiness/conformance.md`](../../readiness/conformance.md).

## The goal, in one sentence

The goal is an original, from-scratch durable-execution engine whose **observable public API behaviour**
matches **Temporal server v1.31.0**, so that Temporal SDKs, operators, and tooling work against it
unmodified.

## The two pins that define the contract

`crates/tokeira-build-info/src/pinned.rs`:

- **`TEMPORAL_SERVER_COMPAT = 1.31.0`** — the Temporal server release whose **behaviour** is the
  authority for every API-behaviour question (field semantics, error/status mapping, defaulting,
  lifecycle ordering, inheritance). This is the behavioural contract.
- **`TEMPORAL_PROTO_VERSION = v1.62.11`** — the vendored `temporalio/api` proto tag built against (the
  **wire** shape). It is intentionally ahead of the proto shipped by server 1.31.0 (`v1.62.8`); RPCs
  present only in the newer proto are **not** part of v1.31.0.

These move independently: proto is wire compatibility; server-compat is behavioural compatibility.

## What "conforming at the API level" means

Conformance is not "the RPC returns something". It is: a Temporal **SDK, operator, or tool** drives the
public gRPC surface and observes the **same behaviour** v1.31.0 produces for the same input lineage — the
same RPCs admitted, the same request-field semantics/defaulting/validation, the same `HistoryEvent`
sequence and response shapes, and the same error/status mapping on failure paths. RPC presence is
necessary but not sufficient; **field-level** fidelity is part of the bar.

The surface is defined across three companion pages, partitioned by Temporal's own designations:

- **[`supported.md`](./supported.md)** — the v1.31.0 surface that conformance targets: the
  `WorkflowService` + `OperatorService` RPCs (121 at API `v1.62.8`) by feature area, with each area's
  Temporal maturity (GA / Public Preview). **Nexus** (GA in v1.31.0), **Worker Deployments** (GA), and
  **Standalone Activities** (Public Preview) are called out.
- **[`excluded.md`](./excluded.md)** — what is outside the surface, with factual reasons: features
  Temporal labels **experimental / pre-release**, **internal** (non-public) surfaces, and RPCs **absent
  from v1.31.0** (the `v1.62.11`-only Nexus operation-execution RPCs).
- **[`tokeira-configuration.md`](./tokeira-configuration.md)** — the current Feature Catalog,
  empty-configuration defaults, exact enablement actions, and every accepted production field.

No conformance-surface decision is currently open. Resolved deliberation lives with
the owning feature specs, while this folder retains the concise authoritative outcome.

## Empty Configuration guarantee

An empty `tokeirad` TOML document is valid. It selects the documented safe compatibility
posture: priority remains active, User Fairness and Standalone Activities remain disabled,
authorization is a stock-compatible no-op until an identity source is configured, and no
emergency restriction is active. Tokeira accepts typed product policy, not Temporal's raw
dynamic-config key namespace. The complete operator contract and canonical example are in
[`tokeira-configuration.md`](./tokeira-configuration.md) and
[`../../../config.example.toml`](../../../config.example.toml).

## Resolved policy outcomes

- **Task Queue Priority and User Fairness:** in-surface. Priority uses the v1.31.0
  stock defaults. Weighted User Fairness is available through
  `[policy.task_queues] enable_fairness = true` and remains disabled under Empty
  Configuration. `UpdateTaskQueueConfig` policy is durable and kind-isolated.
- **Authentication and authorization:** in-surface as stock-default no-op parity plus
  configured bearer authentication, authorization, and Principal Attribution. TLS
  terminates upstream; AWS IAM bearer authorization is a Tokeira-native extension.
  [Decision record](../../../.kiro/specs/authorization-foundation/reference/v1.31.0-conformance-decision.md).
- **Worker Versioning V1/V2:** the five deprecated RPCs conform only as their exact
  stock-default rejections; GA Worker Deployments are the supported replacement.
  [Decision record](../../../.kiro/specs/worker-deployments/reference/v1-v2-conformance-decision.md).

## How the contract is established (ground truth)

Every behavioural decision is verified against v1.31.0 ground truth, never memory or SDK docs:

1. **Wire shape** — the vendored protos in `proto/upstream/` (authoritative; never generated artifacts).
2. **Runtime behaviour** — Temporal server source at tag `v1.31.0` (read from the pinned fork).
3. **Maturity** — Temporal's v1.31.0 release notes for each feature's GA / public-preview /
   experimental / deprecated designation.

The discipline (`AGENTS.md` §8): *reading* Temporal source to determine the contract is required;
*copying* its implementation is forbidden. The test of correctness is "does the response match what
v1.31.0 would return for the same execution lineage?"

## Contents of this folder

- [`supported.md`](./supported.md) — the v1.31.0 surface conformance targets (GA + Public Preview),
  in Temporal's terms.
- [`excluded.md`](./excluded.md) — what is outside the surface (experimental/pre-release, internal,
  absent from v1.31.0), with reasons.
- [`temporal-configuration.md`](./temporal-configuration.md) — the verified complete
  v1.31.0 denominator: 613 source-aware dynamic declarations plus the relevant static groups.
- [`tokeira-configuration.md`](./tokeira-configuration.md) — Tokeira feature availability,
  defaults, enablement, live policy, and every strict production configuration field.

A **functional conformance report** — the measured outcome of replaying Temporal's functional suites
against this surface — will join this folder once it exists. It is not present yet; until then, measured
progress lives in [`../../readiness/conformance.md`](../../readiness/conformance.md).
