# Requirements Document: Tracing Span-Lifecycle Hygiene (Observability)

## Introduction

This spec addresses a class of defects in Tokeira's **`tracing` instrumentation** that surfaced during
load testing and, separately, as a flaky test in the workspace suite. Both are symptoms of the same
underlying fragility — **span lifecycle management across the `tracing` / `tracing-opentelemetry` /
`tracing-subscriber` stack** — but they differ sharply in severity and are scoped as separate work
items below.

The headline defect is **P0**: under a bench at concurrency 100, `tokeirad` **aborted** with a panic
originating *inside the tracing subscriber*, not in Tokeira's own logic. Observability is best-effort
telemetry. It must never be able to take down the process it observes. Under this workspace's release
profile that guarantee is currently violated by construction (see Ground Truth), which is why this is a
spec and not a patch.

This work owns no public-API behaviour, so §8 (Temporal v1.31.0 ground truth) does not apply. The
authorities here are the upstream `tracing` crates' documented span-lifecycle contracts and our own
instrumentation code; cite the crate/version and the code path for every non-obvious decision (§9).

### Ground truth (verified 2026-07-08)

**Symptom A — the process abort (P0).** A bench session drove `tokeirad` hard enough to surface a latent
race in span teardown:

```
thread 'tokio-rt-worker' panicked at tracing-subscriber-0.3.23/src/registry/sharded.rs:317:
assertion `left != right` failed: tried to clone a span (Id(49540145656889403)) that already closed
```

`sharded.rs:317` is `clone_span` in `tracing-subscriber`'s sharded `Registry`. The assertion fires when
a span's reference count is already `0` (closed) at the moment something increments it — a
clone-after-close on a span id, i.e. a use-after-free of a registry slot. Concurrency 20 survived;
concurrency 100 did not — more in-flight spans, more teardown races.

**Why it is fatal, not merely noisy.** The release profile sets `panic = "abort"`
(`Cargo.toml:129-133`). There is therefore **no `catch_unwind` escape hatch**: a panic anywhere in the
subscriber's span path — on any tokio worker thread, mid-poll — aborts the whole process. The bench used
`./target/release/tokeirad`, so the subscriber panic killed `tokeirad` outright. (Dev/test builds unwind,
which is why Symptom B below manifests as a failed assertion, not an abort.)

**The load-bearing versions and layers.**

| Crate | Pinned | Role |
|---|---|---|
| `tracing` | 0.1.44 | span/event macros, `#[instrument]` |
| `tracing-core` | 0.1.36 | callsite/interest, dispatch |
| `tracing-subscriber` | 0.3.23 | sharded `Registry` (the panicking component) |
| `tracing-opentelemetry` | 0.32.1 | OTel span-lifecycle layer, `OpenTelemetrySpanExt` |

The production subscriber (`crates/tokeira-observability/src/tracing.rs`) is a **single global**
`Registry().with(reload EnvFilter).with(otel_layer).with(fmt_layer)` — architecturally correct (one
subscriber, dynamic level via `reload::Layer`, never swapped). So the abort is **not** a
subscriber-swapping bug. The suspect is the **`tracing-opentelemetry` layer**, which manages OTel span
lifecycle *out of band* (it holds span references the sharded registry does not account for and is a
documented source of clone-after-close panics under concurrency), amplified by **`#[instrument]` volume
on cancellable hot paths**. Instrument density is highest exactly where the load lands:
`crates/tokeira-storage/src/dsql/run_repository/leases.rs` (8), `load.rs` (7),
`crates/tokeira-projection/src/dsql_store.rs` (5) — DSQL I/O futures that get retried/aborted under
contention. `OpenTelemetrySpanExt` is used on cross-task boundaries in
`crates/tokeira-runtime/src/publisher.rs` and `crates/tokeira-edge/src/grpc/tracing_interceptor.rs`.

**The `RUST_LOG=warn` workaround.** Lowering the level made the bench survive. This is *diagnostic
confirmation, not a fix*: at `warn` the `#[instrument]` (INFO) callsites resolve to `Interest::never`, so
no spans are allocated into the sharded registry and the teardown race has nothing to race. It confirms
the trigger is **span volume through the registry**, and it leaves the process one accidental
`RUST_LOG=info` away from the same abort.

**Symptom B — the test flake (P2).** `cargo test --workspace` intermittently fails
`crates/tokeira-runtime/src/lane.rs::tests::handle_message_records_kernel_and_storage_span_attributes`
("kernel transition span should be emitted"). It passes in isolation and on re-run. Root cause is
**test hygiene, not the prod stack**: three tests (`handle_message_records_…`,
`handle_message_uses_kernel_transition_span_name`, `lane_processing_span_records_origin_trace_context`)
each install their **own** subscriber via thread-local `set_default` / `with_default` while other tests
run under the no-op default. `tracing`'s callsite interest cache and the `DefaultGuard` install/drop
rebuild are process-global, so concurrently pushing/popping thread-local dispatchers races on shared
state. This is the exact anti-pattern the *production* code correctly avoids. (An earlier investigation
mis-attributed the flake to no-op interest poisoning; that hypothesis failed to reproduce —
`set_default` does rebuild interest — so the real cause is concurrent guard install/drop, not first-hit
poisoning. Do not re-chase the poisoning theory.)

### Non-negotiable invariant

**Observability is best-effort and MUST NOT be able to terminate the process it observes.** In a release
build (`panic = "abort"`) this is an absolute, not an aspiration: there is no unwinding backstop, so it
must hold by construction — the tracing stack must not panic on any reachable path. Correctness of
Tokeira never depends on a span being recorded; a span-lifecycle defect must degrade to *lost telemetry*,
never to a process abort.

---

## Requirement 0: Approach — Diagnostic-First, Root-Cause the P0 (BLOCKING)

**Decision to accept before implementation.** This spec proposes to (a) treat the process abort as the
priority and root-cause it rather than paper it with `RUST_LOG`, (b) bump the `tracing-*` dependency set
(an AGENTS "Architectural: new/changed dependency" change) as the highest-leverage first move, and (c)
adopt the panic-safety invariant above as a standing rule for the observability crate.

**Acceptance criteria.**

1. The owner accepts that observability panicking the process is a defect of its own severity class,
   independent of whether the corpus/bench "passes" at lower concurrency.
2. The owner accepts a **diagnostic-first** order of work: reproduce → bump deps → re-measure →
   only then restructure instrumentation. No structural instrumentation change lands before the
   dependency bump is measured, so we do not rebuild what an upstream fix already resolved.
3. The owner accepts that the `tracing-subscriber` / `tracing-opentelemetry` version bump is in scope
   under the AGENTS "Architectural (changed dependency)" classification, with the usual care
   (changelog review, `cargo test --workspace`, re-bench).

**Status: PROPOSED — awaiting owner review (`owner-review.md`).** Until accepted, `RUST_LOG=warn`
remains the documented operational mitigation for load testing and the workspace flake stays a known,
documented issue (it does not abort; it fails an assertion under unwind).

---

## Requirement 1 (P0): Observability must not be able to abort the process

**User story.** As an operator load-testing `tokeirad`, I need the tracing stack to lose telemetry
rather than abort the process, so that an instrumentation defect can never masquerade as a Tokeira
availability failure.

**Acceptance criteria.**

1. A load run at the concurrency that previously aborted (≥100) completes without any panic originating
   in `tracing-subscriber` / `tracing-opentelemetry`, at the default (`info`) level — i.e. **without**
   relying on `RUST_LOG=warn` to suppress span allocation.
2. The specific `sharded.rs` "clone a span that already closed" assertion does not recur across the
   agreed soak (see Design for duration/concurrency).
3. The root cause is identified and documented (upstream fix via version bump, an instrumentation misuse
   we removed, or both), with the verifying evidence (repro + before/after) recorded in
   `implementer-response.md`. "It stopped reproducing" without an attributed cause is not acceptance.
4. If the root cause cannot be fully eliminated by dependency currency + instrumentation fixes, the spec
   must surface that explicitly and the owner decides on the residual-risk posture (e.g. isolating OTel
   export, or the `panic = "abort"` trade-off for the observability path) — it is **not** silently left
   to `RUST_LOG`.

## Requirement 2: Dependency currency for the tracing stack

**User story.** As a maintainer, I need the `tracing-*` crates current, because span-lifecycle and
sharded-registry fixes ship there regularly and 0.3.23 / opentelemetry 0.32.1 predate several.

**Acceptance criteria.**

1. `tracing-subscriber` and `tracing-opentelemetry` (and `tracing` / `tracing-core` as required by their
   semver) are bumped to the current compatible releases; the OpenTelemetry SDK/exporter crates are
   bumped in lockstep as `tracing-opentelemetry` requires.
2. The bump is reviewed against upstream changelogs for the relevant sharded-registry / span-close /
   OTel-lifecycle fixes, and the relevant fix (if any) is cited in the PR/`implementer-response.md`.
3. `cargo test --workspace` is green after the bump; any API drift in the observability crate is
   absorbed (the observability crate is the only intended blast radius).

## Requirement 3: `#[instrument]` discipline on hot, cancellable paths

**User story.** As a maintainer, I need instrumentation on high-concurrency, cancellable futures to be
cheap and lifecycle-safe, because that is where the teardown race lives.

**Acceptance criteria.**

1. The `#[instrument]` sites on the DSQL hot paths (`run_repository/*`, `projection/dsql_store.rs`) are
   reviewed; any that provide low diagnostic value relative to their per-poll span cost are removed or
   demoted to a level filtered out by default (so they are `Interest::never` in a normal `info` run).
2. Remaining hot-path instrumentation is documented (WHY it stays, §9) so the density is a decision, not
   an accident.
3. No behavioural change to Tokeira: instrumentation edits are observability-only.

## Requirement 4: Cross-task span-capture audit (`OpenTelemetrySpanExt`)

**User story.** As a maintainer, I need to be sure no span handle is captured and used across a task
boundary in a way that can outlive its close, because that is the canonical clone-after-close trigger.

**Acceptance criteria.**

1. Every `OpenTelemetrySpanExt` / `Span::current()` / `.instrument(span)` site that crosses a
   `tokio::spawn` or a cancellable (`select!` / abort) boundary is enumerated
   (`publisher.rs`, `edge/grpc/tracing_interceptor.rs`, and any others found).
2. Each is confirmed to either propagate the span correctly (owning handle moved into the future) or is
   fixed. Findings and fixes are recorded.

## Requirement 5 (P2): Test-subscriber hygiene

**User story.** As a maintainer, I need `cargo test --workspace` to be deterministic, so span-assertion
tests are trustworthy CI signal rather than a known flake.

**Acceptance criteria.**

1. The three subscriber-installing tests in `lane.rs` no longer install competing process-global
   subscribers concurrently. They adopt one of: the `tracing-test` (`#[traced_test]`) / `tracing-mock`
   pattern; or `future.with_subscriber(dispatch)` (async-correct — follows the future) combined with
   serialization of subscriber-installing tests.
2. The bespoke `SpanCapture` + `set_default` dance is retired or reduced to the idiomatic minimum; the
   choice mirrors the production principle ("one subscriber; never swap; test via a scoped/attached
   subscriber, not the global default").
3. `cargo test --workspace` is deterministic across repeated parallel runs (verify with several
   consecutive full runs and a `--test-threads=1` run), with the fix *demonstrated* against a
   reproduction, not asserted.

---

## Out of scope

- Changing the workspace-wide `panic = "abort"` strategy. It may be *raised* under Requirement 1.4 as a
  residual-risk option, but flipping it is a separate decision with its own blast radius.
- Any change to what Tokeira does (state, kernel, edge behaviour). This spec is observability-only.
- OTLP transport / collector / dashboard changes beyond what a dependency bump forces.

## References

- `Cargo.toml:129-133` (`[profile.release] panic = "abort"`).
- `crates/tokeira-observability/src/tracing.rs` (global subscriber construction).
- `crates/tokeira-storage/src/dsql/run_repository/` (highest `#[instrument]` density).
- `crates/tokeira-runtime/src/publisher.rs`, `crates/tokeira-edge/src/grpc/tracing_interceptor.rs`
  (`OpenTelemetrySpanExt`).
- `crates/tokeira-runtime/src/lane.rs` tests (Symptom B).
- AGENTS.md §change-classification ("Architectural | new/changed dependency | Spec update or approval").
