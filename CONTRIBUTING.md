# Contributing

Use `tkr workstation` when local Rust builds are the bottleneck:

```bash
tkr workstation up
tkr workstation remote-exec cargo build --workspace
tkr workstation stop
```

The workstation is intended for compute-heavy builds and tests. Stop it when
finished; persistent cache and repository volumes survive `stop`, but
instance-store build output does not.

## Functional conformance harness

Tokeira's Tier-2 conformance replays Temporal's own functional test corpus
(pinned at the `TEMPORAL_SERVER_COMPAT` tag) over the real gRPC wire against a
running `tokeirad`. It is operator-invoked and lives in the sibling Temporal
fork, not in `cargo test`. See `docs/testing/functional-conformance-harness.md`
for what it proves, how it works, and the runbook (build, run the full corpus,
run a single suite in isolation, and distil per-test outcomes).
