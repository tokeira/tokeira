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

## What "fully conforming at the API level" means

Conformance is not "the RPC returns something". It is: a Temporal **SDK, operator, or tool** drives
tokeira over the public gRPC surface and observes the **same behaviour** v1.31.0 would produce for the
same input lineage — the same RPCs admitted, the same request-field semantics/defaulting/validation, the
same `HistoryEvent` sequence and response shapes, and the same error/status mapping on failure paths.
RPC presence is necessary but not sufficient; **field-level** fidelity is part of the bar.

The surface this comprises is defined in two companion pages, split by what is in vs. out of the claim:

- **[`supported.md`](./supported.md)** — the **in-scope** surface (the *denominator*): the
  `WorkflowService` + `OperatorService` RPCs (121 at API `v1.62.8`) by service and feature area, with
  **Standalone Activities** and **Nexus** called out explicitly.
- **[`excluded.md`](./excluded.md)** — the **out-of-scope** surface, with reasons: **auth** (not
  supported), internal/admin services, multi-cluster replication, DLQ, legacy/deprecated surfaces, the
  `v1.62.11`-only RPCs tracked ahead, and the `temporal` CLI commands that inherit those exclusions.
- **[`command-surface.md`](./command-surface.md)** — the kernel-level state-mutating command and
  history-event surface that realises the workflow state machine (the engine-core half of the claim).

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

- [`supported.md`](./supported.md) — the **in-scope** API surface: the v1.31.0 conformance claim by
  service and feature area, with Standalone Activities and Nexus called out. The denominator.
- [`excluded.md`](./excluded.md) — the **out-of-scope** surface with reasons: auth, internal/admin
  services, multi-cluster replication, DLQ, legacy/deprecated surfaces, `v1.62.11`-only RPCs, and the
  affected `temporal` CLI commands.
- [`command-surface.md`](./command-surface.md) — the state-mutating command set and history-event
  surface: every Temporal history-service state mutation and the tokeira command that realises it, the
  emitted events, and the deliberate kernel-level exclusions. The engine-core half of the definition.

These three pages together are the definition; this README is the lead-through. Measured progress
against them is tracked in [`../../readiness/conformance.md`](../../readiness/conformance.md).
