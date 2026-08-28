# Tokeira

[![ci](https://github.com/tokeira/tokeira/actions/workflows/ci.yml/badge.svg)](https://github.com/tokeira/tokeira/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/tokeira-signature-dark.png">
    <img src="docs/assets/tokeira-signature.png" width="600" alt="Tokeira brand signature — a hand-brushed ink hare nosing a clay-orange dandelion clock, beside the hand-lettered Tokeira wordmark">
  </picture>
</p>

A Temporal-compatible durable execution engine, built in Rust and specialized
for Amazon Aurora DSQL.

## Mission

Preserve the public [Temporal](https://temporal.io) contract that SDKs,
operators, and tooling depend on — WorkflowService, OperatorService, workflow
history semantics, the replay model, task lifecycle, sticky execution,
polling, retries, signals, timers, Continue-As-New. Collapse
internal correctness around a single authoritative per-run transition log.

Tokeira is a product from scratch, not a fork: the architecture is informed by
Temporal, but the implementation is original — where behaviour must match, it
is verified against the pinned Temporal release, never copied from it. It is
not a service-by-service port of Temporal's Frontend / History / Matching /
Worker layout: per-workflow event history is the only semantic ordering
domain, and everything else — queue ordering, delivery ordering, visibility
ordering — is derived. Aurora DSQL is the design centre, not a pluggable
afterthought.

### Design Principles

**History is the authority.** Every state-changing request becomes a per-run
transition that appends history, updates the run summary, and emits derived
effects atomically. The system never relies on an external queue write as the
canonical record that work exists.

**Per-run total order, not global total order.** Tokeira enforces a total
order per workflow run, plus explicit causal edges across runs and side
effects. Queue delivery and visibility are derived domains.

**Delivery is ephemeral-first.** Worker polling and sync matching live
primarily in memory. Durable backlog is a fallback and recovery aid, not the
default path.

**Visibility is a projection.** The projection plane owns read models and
operates outside the correctness path. A lagging projection is a quality
problem, not a correctness failure.

**The kernel is pure.** The deterministic state machine transforms commands
into transitions with no I/O, no storage access, and no delivery concerns.

**Configuration stays minimal.** Prefer policies and auto-tuning over exposed
mechanical knobs.

## Temporal Conformance

Tokeira carries two independent compatibility pins
([`crates/tokeira-build-info/src/pinned.rs`](crates/tokeira-build-info/src/pinned.rs)):

- **Temporal server compatibility: v1.31.0** — the release whose public API
  *behaviour* Tokeira aims to match. Behaviour questions are resolved against
  that release, not guessed.
- **Temporal API: v1.62.11** — the vendored proto surface Tokeira builds
  against ([`proto/upstream/`](proto/upstream/)).

The pins are tracked independently on purpose: vendored protos may advance
ahead of the behavioural claim, and updating protos never silently raises it.

Conformance is measured, not asserted:

- **Compatibility matrices.** Every WorkflowService and OperatorService RPC is
  classified (`Implemented` / `Partial` / `Experimental` / `Stubbed` /
  `Unsupported`) in a checked-in `FEATURE_MATRIX`; SDK support claims live in
  `SDK_MATRIX` with evidence and verification state. `tkr compat show`
  inspects both. See
  [docs/conformance/compatibility.md](docs/conformance/compatibility.md).
- **Functional corpus replay.** Temporal's own functional Go test suites,
  pinned at v1.31.0, are replayed over the real gRPC wire against a running
  `tokeirad`. See
  [docs/testing/functional-conformance-harness.md](docs/testing/functional-conformance-harness.md).
- **The v0.1.0 release evidence.**
  [docs/readiness/corpus-evidence.md](docs/readiness/corpus-evidence.md) — the
  ordered corpus replay measured against the exact commit `v0.1.0` names:
  1,261 passes, 0 failures, every exclusion cited to Temporal's source.
- **A public ledger.**
  [docs/readiness/conformance.md](docs/readiness/conformance.md) records
  exactly what has been verified, what is implemented but unmeasured, and what
  is outstanding — against
  [docs/conformance/v1.31.0/](docs/conformance/v1.31.0/README.md), which
  defines what full v1.31.0 compliance means.

## Architecture

Tokeira is organized into three planes:

- **Compatibility edge** — admits and translates requests. Exposes
  WorkflowService, OperatorService, and health endpoints; performs
  authn/authz, namespace lookup, and request-ID handling. Owns no workflow
  semantics.
- **Authoritative runtime and storage** — owns correctness: shard/bundle
  ownership and fencing, lane-local execution of workflow actors, durable
  state transitions, durable timers, and derived dispatch.
- **Projection plane** — owns read models: SQL visibility, rollups, and
  custom sinks with independent checkpoints and replay. Outside the
  correctness path.

Design documents live in
[docs/architecture/](docs/architecture/000-overview.md), with a navigable
reference for the seven engine crates in [docs/crates/](docs/crates/README.md).

## Run Tokeira

### Embedded — inside your process

The engine runs in your application. No TCP listener, no port, no daemon —
the Temporal Rust SDK connects over an in-memory duplex:

```rust
// A Temporal-compatible engine, in-process.
let engine = tokeira_engine::Engine::embedded().await?;

// Hand its endpoint to the Temporal Rust SDK:
//   ConnectionOptions::service_override(engine.service_override())
// and every SDK worker and client in the process speaks to it directly.
```

Storage grows with you: in-memory with snapshots, then managed Aurora DSQL —
the same engine either way, selected by configuration. Until the crates reach
crates.io, take a git dependency on the
[`v0.1.0` tag](https://github.com/tokeira/tokeira/releases/tag/v0.1.0).

Building AI agents? [Odori Agents](https://github.com/tokeira/tokeira-odori)
is a minimal Rust agent framework built on embedded Tokeira.

### The server and its platforms — in development

The same engine runs standalone as `tokeirad` — the conformance evidence
drives it over live gRPC — and an operator lifecycle is taking shape in-tree
around it: `tkr` manages named deployments under an explicit
**plan → confirm → apply** contract, across platform definitions for
bare-host `local`, Docker Compose (with a provisioned
Mimir · Loki · Grafana · Alloy observability stack), ECS, and EKS.

This operator surface is under active development and sits outside the
v0.1.0 support claim; support statements will follow as it hardens.
Explore: [docs/platforms/](docs/platforms/README.md).

## Security

Authentication and authorization live at the compatibility edge; secrets are
redacted by default; `unsafe` Rust is denied workspace-wide.
Vulnerability reporting and the full posture: [SECURITY.md](SECURITY.md).

## Development

Standard cargo workflows on a pinned stable toolchain;
`cargo test --workspace` needs no AWS credentials and no Docker. Guide:
[docs/development.md](docs/development.md).

## Built with Claude, Codex, and Kiro

Tokeira was built by a fleet: one grateful owner ❤️ collaborating with three hugely capable agents —
[Claude](https://claude.com) (Anthropic), [Codex](https://openai.com/codex)
(OpenAI), and [Kiro](https://kiro.dev) (AWS) — working concurrently in this
repository under a written contract, [AGENTS.md](AGENTS.md): spec-driven
development, ground-truth verification against the pinned Temporal release,
a compiler-enforced quality bar, and serial human review of every merge.

The agents carried a large share of the engineering — architecture drafts,
implementation, tests, the conformance drive, and review of one another's
work — and that contribution is recorded where engineering credit belongs:
in the history. Every commit with agent involvement names its agents in
`Co-authored-by:` / `Assisted-by:` trailers. Kiro deserves particular
credit for the architecture, requirements specification, and technical
design of the system, developed in close collaboration from the first
commit. The fleet mechanics live in
[docs/agents/](docs/agents/concurrent-agents.md).

## Contributing

Issues and pull requests are welcome; discuss substantial changes in an issue
first. The quality bar, PR process, and conformance-harness runbook are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache-2.0](LICENSE)
