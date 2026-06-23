# What "Tokeira targets Temporal v1.31.0" means

> **This is a definitional document, not a status report.** It describes *what full Temporal v1.31.0
> compatibility entails* and the surface tokeira commits to — so an operator, SDK user, or evaluator
> can understand the shape and bound of the claim. It does **not** assert how much tokeira has yet
> achieved. For that — the honest, measured progress — see
> [`../../readiness/conformance.md`](../../readiness/conformance.md).

## The claim, in one sentence

Tokeira is an original, from-scratch durable-execution engine whose **observable public API behaviour**
matches **Temporal server v1.31.0**, so that Temporal SDKs, operators, and tooling work against tokeira
unmodified.

## The two pins that define the contract

`crates/tokeira-build-info/src/pinned.rs`:

- **`TEMPORAL_SERVER_COMPAT = 1.31.0`** — the Temporal server release whose **behaviour** is the
  authority for every API-behaviour question (field semantics, error/status mapping, defaulting,
  lifecycle ordering, inheritance). This is the behavioural contract.
- **`TEMPORAL_PROTO_VERSION = v1.62.11`** — the vendored `temporalio/api` proto tag tokeira builds
  against (the **wire** shape). It is intentionally ahead of the proto shipped by server 1.31.0
  (`v1.62.8`); RPCs present only in the newer proto are **not** part of the 1.31.0 behavioural claim.

These move independently: proto is wire compatibility; server-compat is behavioural compatibility.

## What is in scope of the claim

- The **public gRPC API surface** Temporal SDKs and operators use — `WorkflowService` and
  `OperatorService` — admitted and translated at tokeira's compatibility edge.
- The **observable behaviour** of those RPCs: the history events they produce, the errors/status codes
  they return, defaulting and validation, and lifecycle ordering — matched to what v1.31.0 does for the
  same execution lineage.
- The **state-mutating command + history-event surface** (the engine core) — see
  [`command-surface.md`](./command-surface.md).

## What is out of scope (and why)

- **Internal/admin surfaces** tokeira does not front (`AdminService`, `HistoryService`,
  `MatchingService` driven directly, persistence/`testBase` pokes). These are implementation-internal
  to Temporal's topology; tokeira deliberately collapses that topology, so tests that drive them are
  out of the public claim by construction.
- **Legacy worker-versioning v0.x version-sets** — deliberately not implemented; rule-based worker
  versioning is the replacement.
- **Multi-cluster replication / failover / remote routing** beyond metadata CRUD.
- RPCs that exist only in the newer vendored proto (`v1.62.11`) but not in server `1.31.0` (e.g. some
  Nexus operation RPCs) — tracked separately, not part of the 1.31.0 behavioural claim.

## How the contract is established (ground truth)

Every behavioural claim is verified against v1.31.0 ground truth, never memory or SDK docs:

1. **Wire shape** — the vendored protos in `proto/upstream/` (authoritative; never generated artifacts).
2. **Runtime behaviour** — Temporal server source at tag `v1.31.0` (read from the pinned fork).

This is the same discipline the engine's `AGENTS.md` §8 and the conformance Implementer Mandate bind
all work to: *reading* Temporal source to determine the contract is required; *copying* its
implementation is forbidden. The test of correctness for any tokeira mechanism is "does tokeira's
response match what v1.31.0 would return for the same execution lineage?"

## How compliance is demonstrated

Three complementary tiers (see `../../readiness/conformance.md` for the live state of each):

1. **Compatibility surface** — a queryable `FEATURE_MATRIX` classifying every RPC, plus pinned
   provenance surfaced via `GetSystemInfo` and `tokeirad --version`.
2. **Tier-1 in-process oracle** — drives RPCs over real gRPC and asserts both responses **and** the
   `HistoryEvent` sequence against v1.31.0, behind a 121-RPC coverage gate.
3. **Tier-2 functional corpus** — replays Temporal's own functional Go test suites, unmodified, over
   gRPC against a running `tokeirad` pinned at v1.31.0.

## Contents of this folder

- [`command-surface.md`](./command-surface.md) — the state-mutating command set and history-event
  surface: every Temporal history-service state mutation and the tokeira command that realises it, the
  emitted events, and the deliberate exclusions. This is the engine-core half of the definition.

_Further per-service definition pages (WorkflowService / OperatorService RPC behaviour, error mapping,
visibility query surface) are added here as they are written; this README is the lead-through._
