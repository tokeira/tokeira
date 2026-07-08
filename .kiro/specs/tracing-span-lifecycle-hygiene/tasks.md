# Tasks: Tracing Span-Lifecycle Hygiene

Requirements: [requirements.md](./requirements.md). Design: [design.md](./design.md).

> **Blocked on Requirement 0.** Do not start Phase 1 until the owner accepts the diagnostic-first
> approach and the dependency bump (AGENTS "Architectural: changed dependency"). Until then,
> `RUST_LOG=warn` is the documented operational mitigation for load testing and the workspace flake stays
> a documented known issue.

Observability-only: no kernel/edge/runtime *behaviour* changes. `cargo test --workspace` and the
functional conformance corpus stay green throughout. Cite the crate/version and code path for every
non-obvious decision (§9). Every remediation step is **measured against a reproduction**, not asserted —
"it stopped happening" is not acceptance.

## Phase 0 — Decision

- [ ] 0.1 Owner accepts Requirement 0 (diagnostic-first; dep bump in scope; observability-must-not-abort
  is a defect class of its own). Record in `owner-review.md`.

## Phase 1 — Reproduce the P0 (attribution gate)

- [ ] 1.1 Stand up the bench repro: release build (`panic = "abort"`), `apps/tokeira-bench` at
  concurrency 100, default `info` level. Confirm whether it aborts and capture the `sharded.rs:317`
  backtrace. (Req 1.1)
- [ ] 1.2 Script N repeats and record the **abort rate** (a rate, not a single pass) as the baseline the
  fix must move. If it will not reproduce on available hardware, document that and design the tightest
  proxy (e.g. a targeted stress harness over the DSQL `#[instrument]` paths with OTel enabled). (Req 1.1)

## Phase 2 — Cheapest lever: dependency currency

- [ ] 2.1 Bump `tracing-subscriber` + `tracing-opentelemetry` (+ transitive `tracing` / `tracing-core`,
  OTel SDK/exporter in lockstep) to current compatible releases. (Req 2.1)
- [ ] 2.2 Review the intervening changelogs for sharded-registry / span-close / OTel span-lifecycle
  fixes; cite the one that maps to `sharded.rs:317` if present. (Req 2.2)
- [ ] 2.3 `cargo test --workspace` green; absorb any observability-crate API drift; confirm blast radius
  stayed in `tokeira-observability` (+ the two `OpenTelemetrySpanExt` sites). (Req 2.3)
- [ ] 2.4 Re-run the Phase-1 repro; record before/after abort rate. If zero → go to Phase 5 (attribute +
  test hygiene). If non-zero → Phase 3. (Req 1.1)

## Phase 3 — Instrumentation density (only if P0 persists)

- [ ] 3.1 Enumerate the `#[instrument]` sites on the hot paths (`run_repository/*`,
  `projection/dsql_store.rs`); for each, record diagnostic value vs per-poll span cost. (Req 3.1)
- [ ] 3.2 Remove/demote the low-value hot-path sites (level filtered out of a default `info` run);
  WHY-comment the ones that stay. Re-measure against the repro. (Req 3.1, 3.2)

## Phase 4 — Cross-task span capture (only if P0 persists)

- [ ] 4.1 Enumerate `OpenTelemetrySpanExt` / `Span::current()` / `.instrument(span)` sites crossing a
  `tokio::spawn` or cancellable (`select!`/abort) boundary (`publisher.rs`,
  `edge/grpc/tracing_interceptor.rs`, any others). (Req 4.1)
- [ ] 4.2 Fix any that can outlive their close; re-measure. (Req 4.2)

## Phase 5 — Attribute & (if needed) residual risk

- [ ] 5.1 Record the named root cause + before/after evidence in `implementer-response.md`. (Req 1.3)
- [ ] 5.2 If the P0 is not fully eliminated, surface the residual risk and present the isolation /
  `panic = "abort"` trade-off options to the owner for decision — do **not** leave it to `RUST_LOG`.
  (Req 1.4)

## Phase 6 — Test-subscriber hygiene (P2, parallelizable with Phase 2+)

- [ ] 6.1 Reproduce the `lane.rs` flake deterministically (e.g. a first-sorting test that installs a
  competing subscriber; run `--test-threads=1` + a parallel loop). (Req 5.3)
- [ ] 6.2 Apply the chosen fix: `tracing-test`/`tracing-mock`, or `future.with_subscriber(dispatch)` +
  serialization of the subscriber-installing tests. Retire/serialize the bespoke `SpanCapture`+`set_default`
  path. (Req 5.1, 5.2)
- [ ] 6.3 Demonstrate determinism against the repro; then confirm several consecutive `cargo test
  --workspace` parallel runs + a `--test-threads=1` run are green. Remove any repro scaffolding. (Req 5.3)

## Phase 7 — Documentation

- [ ] 7.1 Add the panic-safety invariant and the `#[instrument]`-on-hot-paths guidance to the
  `tokeira-observability` module docs so density stays a decision, not an accident. (Design §5)
