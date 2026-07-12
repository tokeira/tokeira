# Functional conformance harness (Tier 2)

Tokeira's strongest conformance signal: Temporal's own functional Go test corpus, run **unmodified**
and pinned at the `TEMPORAL_SERVER_COMPAT` tag (currently `v1.31.0`), over the real gRPC wire against
a running `tokeirad`. If Temporal's server-grade behavioural assertions pass against Tokeira, the
behaviour is conformant by Temporal's own definition — not ours.

This is an **operator-invoked** harness. It is deliberately **not** part of `cargo test`: it requires
a built `tokeirad`, a checkout of the pinned Temporal fork, and the Go toolchain, and it runs the full
upstream corpus rather than Tokeira unit tests.

## Where it sits: the three-tier model

| Tier | What it proves | Mechanism |
|------|----------------|-----------|
| 1 | Per-RPC shape + matrix claim, hermetic and fast | `crates/tokeira-conformance` (in-process, `cargo test`) |
| **2** | **Temporal's own behavioural assertions over the real wire** | **this harness — forked corpus vs `tokeirad`** |
| 3 | Multi-language SDK behaviour + history replay (future) | Temporal SDK suites |

Tier 2 borrows Temporal's `tests/` corpus; it copies no test logic into Tokeira. Its posture is
**run-all, classify-in-report**: every test in the corpus executes, and failures are *data* —
classified (pass / real-gap / deliberate-deviation / out-of-public-scope) in a coverage report, never
silently excluded from the run.

## Architecture

The harness fronts an external `tokeirad` as the cluster the corpus talks to ("Shape-2 seam"). The
forked onebox's service boot is short-circuited; `FrontendClient()` is pointed at the running
`tokeirad`, and the unmodified functional suites drive it over standard gRPC. The fork edits are
confined to the onebox seam and a per-test ledger — never a test body — so the corpus stays
byte-for-byte upstream and rebases cleanly on each compat bump.

Three tools live in the fork:

- **run-all executor** — enumerates every top-level entrypoint and runs each in its **own** `go test`
  process. Per-entrypoint isolation means a panicking test cannot abort the rest of the corpus (full
  execution is guaranteed; a single crash truncates only its own siblings). Emits a combined
  `go test -json` stream.
- **single-suite runner** — the iteration counterpart: runs one suite (or an arbitrary `-run` regexp)
  against a booted-or-reused `tokeirad` on the same shared lifecycle and skip registry, and prints a
  per-leaf PASS/FAIL/SKIP tally. Replaces the former `run_suite.sh` bash harness.
- **outcome distiller** — reduces the `-json` stream to one outcome per test, at per-test granularity
  including `t.Run` sub-tests (`pass` / `fail` / `skip` / `unfinished`).

The Tokeira-side report join (`tokeira-edge::conformance`) resolves observed wire traffic and per-test
outcomes against the compatibility matrix to produce the classified coverage report and gates.

## Prerequisites

- A built `tokeirad` binary (`cargo build -p tokeirad` → `target/debug/tokeirad`).
- A checkout of the pinned Temporal fork on the `tokeira/conformance-v1.31.0` branch (sibling repo).
- Go toolchain (the fork builds with `-tags test_dep`).

## Running

All commands below run from the **fork** checkout unless noted. `<tokeira-workspace>` is the path to
this repository; `<temporal-fork>` is the pinned Temporal fork checkout.

**Go toolchain.** Runs use the version the corpus's `go.mod` requires (matching
`TEMPORAL_SERVER_COMPAT`) — currently **`go1.26.2`** — not whatever `go` is first on PATH. The run-all
runner pins it explicitly (`GOTOOLCHAIN=go1.26.2`) for every `go test` it spawns; set the same env for
any manual command below.

**Out-of-scope skips.** A small, curated set of corpus tests cannot run against an out-of-process
`tokeirad` (they depend on `OverrideDynamicConfig`, which the harness cannot deliver to it) or touch an
internal surface the Shape-2 onebox does not front. These are registered by name in
`tests/testcore/tokeira_conformance_skip.go` with a cited reason — **never** by editing a corpus test
body. The registry is applied two ways: the shared `SetupTest`/`SetupSubTest` hooks skip whole methods
and `s.Run` sub-tests, and the run-all runner additionally derives a `go test -skip` regexp from the
registry (`testcore.ConformanceSkipRegexp`) so raw `t.Run` sub-tests — which testify cannot intercept —
are skipped too, by leaf only. Skipped tests still emit a `skip` outcome the ledger classifies; nothing
is silently dropped.

```bash
# 1. Build tokeirad (this Tokeira workspace)
cargo build -p tokeirad   # binary: target/debug/tokeirad

# 2. Run the full corpus (fork workspace, branch tokeira/conformance-v1.31.0).
#    Either let the harness boot tokeirad via TOKEIRA_BIN, or point it at an
#    already-running frontend via TOKEIRA_CONFORMANCE_FRONTEND_ADDR.
TOKEIRA_BIN=<tokeira-workspace>/target/debug/tokeirad \
  go run -tags test_dep ./tests/tokeira_conformance_runall/

# 3. Distil the -json stream into per-test outcomes
go run -tags test_dep ./tests/tokeira_conformance_ledger/ \
  tokeira-conformance-results.json outcomes.json
```

Run a single suite in isolation (fast iteration while fixing one cluster) with the **single-suite
runner** — the iteration counterpart to the run-all executor. It boots-or-reuses `tokeirad` on the same
shared lifecycle, applies the same skip registry, and prints a per-leaf PASS/FAIL/SKIP tally:

```bash
# Boot tokeirad from the binary (use the `--features conformance` build for override-driven suites):
TOKEIRA_BIN=<tokeira-workspace>/target/debug/tokeirad \
  go run -tags test_dep ./tests/tokeira_conformance_runsuite/ '^TestCronTestSuite$'

# …or reuse an already-running frontend:
TOKEIRA_CONFORMANCE_FRONTEND_ADDR=127.0.0.1:7233 \
  go run -tags test_dep ./tests/tokeira_conformance_runsuite/ '^TestCronTestSuite$'
```

The runner takes an arbitrary `-run` regexp (a single leaf works too, e.g.
`'^TestNexusWorkflowTestSuite/TestNexusOperationSyncNexusFailure$'`) and an optional `-timeout`
(default 8m). To drive `go test` directly against an already-running frontend instead, apply the
registry's skips yourself so the manual run matches the runner (omit `-skip` to see the out-of-scope
tests fail):

```bash
TOKEIRA_CONFORMANCE_FRONTEND_ADDR=127.0.0.1:7233 \
GOTOOLCHAIN=go1.26.2 \
  go test -tags test_dep -count=1 -run '^TestCronTestSuite$' ./tests/ -v
```

## Interpreting results

A large number of failing tests is **expected** on an incomplete surface and is the point of the
run-all posture — the value is in the clustering and classification, not the raw pass rate. Failures
fall into recurring root causes (missing admission validation, unimplemented RPCs, feature modes that
belong in their own runs). Each non-passing test is classified in the coverage report; the report
gates enforce that every non-pass is classified, cites the right evidence, and that a `real-gap` test
which begins passing is flipped to a required pass.

## Conventions for acting on a run

Fixes derived from a conformance run are bound by these rules:

- **Ground-truth every fix to the targeted Temporal release.** Verify behaviour against the pinned
  tag's source and the vendored protos; cite the source. Never infer from SDK docs or memory.
- **No kernel additions.** Conformance fixes are edge / runtime / storage concerns. A fix that seems
  to need the kernel is a signal to stop and raise it.
- **Config defaults to a constant, not a knob.** A behaviour governed by a Temporal dynamic-config
  default is represented as the pinned-release default as a hardcoded constant. It becomes real
  Tokeira config only when it is a genuine deployment policy *and* the fixed default would be
  operationally wrong — and then it is raised as a deliberate decision, not added silently.
- **Feature modes are independent runs.** A non-default behavioural mode Tokeira claims is exercised
  by booting `tokeirad` in that mode and running the corpus (or its subset) as a separate, tagged
  run — not by injecting per-test dynamic-config overrides.
- **Raise ambiguity; do not guess.** A wrong guess behind a green check bakes in non-conformance.

## Compatibility pin

The corpus is pinned to the tag matching `TEMPORAL_SERVER_COMPAT` (`crates/tokeira-build-info`). The
fork branch must be at that tag; running from the fork's `main` is rejected. On a compat bump, the
fork rebases onto the new tag and the run is re-baselined.
