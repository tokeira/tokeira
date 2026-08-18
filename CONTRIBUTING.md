# Contributing

Thanks for your interest in Tokeira. The project moves quickly; for anything
substantial, open an issue to discuss the change before writing code.

## Ground Rules

- [AGENTS.md](AGENTS.md) is the engineering contract for this repository —
  binding for human and AI contributors alike. It covers the lint wall, kernel
  purity, Temporal ground-truthing, documentation standards, and the
  concurrent-agent git protocol.
- Feature work is spec-driven: `.kiro/specs/<feature>/` holds requirements,
  design, and tasks. Check the spec before changing behaviour it covers.
- Temporal API behaviour is never guessed: it is verified against the pinned
  release (AGENTS.md §8) before a spec or an implementation claims it.

## Development Setup

See [docs/development.md](docs/development.md) for prerequisites, the build and
test loop, and the repository layout.

## Quality Bar

Run before any push or PR — CI enforces the same set:

```bash
cargo +nightly fmt --all
cargo lint --locked                # clippy: workspace + all targets
cargo check --workspace --locked
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

CI additionally runs `cargo-deny` (bans/licenses/sources), an offline link
check over every Markdown file, and `git diff --exit-code` — builds must not
modify checked-in files. Everything runs `--locked`: dependency movement is a
reviewed change, never a CI side effect.

## Pull Requests

- Branch from `main`; keep each PR to one coherent change.
- The PR body states what and why, the validation commands actually run (name
  anything skipped, and why), and known risks.
- PRs are reviewed and merged serially by a maintainer.
- If AI agents authored or assisted with part of the change, credit them with
  `Co-authored-by:` / `Assisted-by:` trailers (AGENTS.md §11).

## Functional Conformance Harness

Tokeira's Tier-2 conformance replays Temporal's own functional test corpus
(pinned at the `TEMPORAL_SERVER_COMPAT` tag) over the real gRPC wire against a
running `tokeirad`. It is operator-invoked and lives in the sibling Temporal
fork, not in `cargo test`. See
[docs/testing/functional-conformance-harness.md](docs/testing/functional-conformance-harness.md)
for what it proves, how it works, and the runbook (build, run the full corpus,
run a single suite in isolation, and distil per-test outcomes).
