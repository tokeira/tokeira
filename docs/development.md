# Development

Building and testing Tokeira itself. For the contribution process and the
pre-PR quality bar, see [CONTRIBUTING.md](../CONTRIBUTING.md); for the binding
engineering rules (lint wall, kernel purity, documentation standards), see
[AGENTS.md](../AGENTS.md).

## Prerequisites

- **Rust** via [rustup](https://rustup.rs) — the stable toolchain is pinned by
  `rust-toolchain.toml` and picked up automatically. Formatting uses
  nightly-only rustfmt options, so also install a nightly toolchain
  (`rustup toolchain install nightly`).
- **protoc** — `tokeira-proto` compiles the vendored Temporal protos at build
  time (`apt install protobuf-compiler` / `brew install protobuf`).
- **Docker** — only for the compose platform and container image work; the
  default test suite does not need it.
- **Dagger 0.20+** — only for `tkr image` build/push/mirror commands.

The default test suite requires no live AWS credentials and no Docker.

## Build and test

```bash
cargo build                          # build all crates
cargo test --workspace               # unit + integration tests
cargo lint                           # alias for: clippy --workspace --all-targets
cargo +nightly fmt --all             # format (nightly-only options)
cargo doc --workspace --no-deps      # API docs
```

A cold workspace build takes 10–20 minutes on a laptop; incremental builds are
typically 10–30 seconds. If cold-build time is blocking your iteration loop,
use the [remote workstation](remote-workstation.md).

### Fast inner loop

- `cargo check -p <crate>` and `cargo clippy -p <crate> --all-targets` give
  seconds-fast feedback on a single crate.
- `cargo nextest run -p <crate> -E 'test(<name>)'` isolates one test per
  process and kills hangs at 180 s (`.config/nextest.toml`). Doctests still
  need `cargo test --doc`.
- Some tests panic intentionally; only harness-reported failures are real
  problems.

## Testing conventions

- Unit tests are co-located per module (`#[cfg(test)]`); property-based tests
  use `proptest` for config validation, serialization round-trips, and
  dependency ordering.
- No explicit sleeps in tests — synchronize with channels, `tokio::sync::Notify`,
  or condition variables.
- Behavioural conformance against Temporal is validated separately from
  `cargo test` by the
  [functional conformance harness](testing/functional-conformance-harness.md).

## Repository layout

```
apps/         tokeirad (server) · tkr (operator CLI) · tokeira-controller ·
              tokeira-autoscaler · tokeira-bench
crates/       engine   types · proto · kernel · chasm{,-derive,-activity} ·
                       storage · runtime · projection · edge · observability · auth
              compat   build-info · compatibility{,-proto,-service} ·
                       conformance{,-proto,-control}
              deploy   state · iac · deploy-engine · config · orchestrator · tkd ·
                       k8s · aws · compose · build · provisioner{,-cli} ·
                       autoscaler · controller · remote-workstation · dagger-client
platforms/    local · compose · ecs · eks
tools/        tkw (fleet worktrees) · proto-sync · simulation
proto/        upstream/ — vendored Temporal protos (the authoritative wire shape)
.kiro/specs/  feature specs                 spec/     TLA+/refinement stack
scenarios/    end-to-end samples
docs/         architecture · adr · platforms · conformance · readiness · testing ·
              crates · agents
```

The workspace `Cargo.toml` member list is authoritative; this is orientation.
The engine's seven core crates are documented in [docs/crates/](crates/README.md).

## See also

- [Remote workstation](remote-workstation.md) — fast Rust builds on a
  provisioned Graviton4 instance
- [CONTRIBUTING.md](../CONTRIBUTING.md) — quality bar and PR process
- [AGENTS.md](../AGENTS.md) — the engineering contract, for humans and agents
