# Design: Tracing Span-Lifecycle Hygiene

Requirements: [requirements.md](./requirements.md).

This is a **diagnostic-and-remediation** design, not a feature design. The unknown is the exact
root cause of the P0 abort; the design fixes the *method* (how we reproduce, attribute, and verify)
and enumerates the remediation levers in the order we will pull them.

## 1. Failure model

### 1.1 The abort (Requirement 1)

`tracing-subscriber`'s `Registry` stores spans in a sharded slab. Each span id maps to a slot with a
generation counter and a reference count. The lifecycle is:

- `new_span` → allocate a slot, refcount = 1.
- `clone_span` (a `Span` handle is cloned, or a layer takes a reference) → refcount += 1.
- `try_close` (a handle drops) → refcount -= 1; at 0 the slot is closed and may be reused.

`sharded.rs:317`'s `assert(left != right)` in `clone_span` fires when the slot's refcount is already 0:
someone incremented a slot that had already closed. Two ways to get there:

1. **A layer holds a span reference the registry has already closed.** `tracing-opentelemetry` tracks
   OTel spans keyed off tracing span ids and manipulates them on `on_enter` / `on_close`; if its view of
   a span's liveness diverges from the registry's under concurrency (or across a version boundary where
   the two disagree about close ordering), it can drive a clone against a closed slot. This is the
   primary hypothesis — the OTel layer is the only component holding span references out of band.
2. **A `Span` handle outlives the span's close and is then cloned.** `#[instrument]` on an async fn
   stores the span in the future; `Span::current().clone()` / `.instrument(span)` / `OpenTelemetrySpanExt`
   captured into a `tokio::spawn`ed or cancellable future can produce a clone after the originating frame
   closed. High `#[instrument]` density on cancellable DSQL futures is the volume multiplier.

Both are consistent with "survives at 20, aborts at 100" and with "`RUST_LOG=warn` makes it vanish"
(no INFO spans → no slab traffic → nothing to race).

### 1.2 The test flake (Requirement 5)

Not the sharded-registry refcount bug — it is the *interest/dispatch* global-state race. Three tests
install thread-local subscribers (`set_default` / `with_default`) and drop their `DefaultGuard`s
concurrently. Guard install/drop triggers a process-global `rebuild_interest_cache`, and the
`kernel.transition` callsite is hit by *every* `handle_message` test. Concurrent install/drop of
competing dispatchers is a documented `tracing` test footgun. Production never does this (one global
subscriber, reload filter), so the fix is to make the tests obey the same discipline.

## 2. Method (applies to Requirement 1)

Attribution before remediation. We do not accept "it stopped happening."

1. **Reproduce deterministically enough to measure.** Stand up the bench path that aborted
   (`apps/tokeira-bench`, release build, `panic = "abort"`) at concurrency 100 with default `info`
   level. Capture: whether it aborts, and the panic backtrace. If flaky, script N repeats and record the
   abort rate — a rate is a measurement; a single pass is not.
2. **Bisect the cheap lever first (Requirement 2).** Bump `tracing-subscriber` + `tracing-opentelemetry`
   (+ transitively `tracing`/`tracing-core`, OTel SDK/exporter). Re-run the repro. Record before/after
   abort rate. Read the intervening changelogs for sharded-registry / span-close / OTel-lifecycle fixes
   and cite the specific one if it maps to `sharded.rs:317`.
3. **If it persists, bisect the instrumentation (Requirements 3, 4).** With the repro in hand, remove /
   demote the hottest `#[instrument]` sites and re-measure; enumerate and fix any cross-task span capture
   (§1.1 case 2). Each change is measured against the repro, not asserted.
4. **Attribute and record.** The accepted outcome is a named cause + before/after evidence in
   `implementer-response.md`.

## 3. Remediation levers, in order

| Lever | Requirement | Cost | Why this order |
|---|---|---|---|
| Version bump | R2 | low | Most span-lifecycle bugs are fixed upstream; cheapest to try, highest prior probability. |
| `#[instrument]` demotion on hot paths | R3 | low | Cuts registry traffic (the volume trigger) with no behaviour change; also a standing perf win. |
| Cross-task capture fix | R4 | med | Eliminates the misuse case if present. |
| Isolate OTel export / residual-risk posture | R1.4 | high | Only if the above do not close it; owner-decided. |

### 3.1 On why we cannot just guard it

Under `panic = "abort"` there is no `catch_unwind`: the abort happens before any unwinding, so there is
no in-process backstop that can turn a subscriber panic into "lost telemetry." The guarantee in
Requirement 1 must therefore hold **by construction** — the code path must not panic. That is why the
remediation is root-cause (versions + usage), not a wrapper. The residual-risk option (R1.4) is about
*isolation* — e.g. keeping the OTel export off the hot poll path via the SDK's batch exporter and a
dedicated runtime — or an explicit owner decision on the `panic = "abort"` trade-off for this crate;
neither is a code-level `try/catch`.

### 3.2 What "isolate OTel export" would mean (only if needed)

The batch span processor already runs export off-thread. The residual hazard, if it survives the bump,
is the *layer's registry interaction on the hot path* (`on_enter`/`on_close`), which a batch exporter
does not move. If it comes to this, options for the owner: pin to a known-good `tracing-opentelemetry`
range; reduce the layer's registry footprint (fewer instrumented spans reaching it, per R3); or accept
the residual risk with monitoring. This subsection exists so the P0 is never silently downgraded to
`RUST_LOG`.

## 4. Test-hygiene design (Requirement 5)

Preferred: adopt **`tracing-test`** (`#[traced_test]`) as a dev-dependency for the span-assertion tests.
It installs a single per-test subscriber correctly (span-scoped, not a competing global) and is the
idiomatic 2026 choice used across the `tracing` ecosystem.

If a new dev-dependency is unwanted, the minimal in-tree fix:

- **Async correctness:** replace `set_default(&dispatch); f().await` with
  `f().with_subscriber(dispatch).await` (`tracing::instrument::WithSubscriber`) so the subscriber follows
  the future across await/thread boundaries rather than relying on a thread-local.
- **Determinism:** serialize the subscriber-installing tests (a module `Mutex` or `serial_test`) so their
  guard install/drop cannot race the global interest cache.

Both approaches must be **demonstrated against a reproduction** of the flake (e.g. a temporary
first-sorting test that installs a competing subscriber, run under `--test-threads=1` and in a parallel
loop) — verify, then remove the scaffolding. The earlier attempt asserted a fix without a repro and was
wrong; do not repeat that.

Retire the bespoke `SpanCapture` layer only insofar as the chosen approach makes it redundant; if it
stays, its lifecycle (install/drop) must be serialized.

## 5. Blast radius & guardrails

- **Behaviour:** none. This spec changes observability and tests only; `cargo test --workspace` (and the
  functional conformance corpus) must stay green throughout, and no kernel/edge/runtime *semantics*
  change. The dependency bump's only intended source blast radius is `tokeira-observability` (+ the two
  `OpenTelemetrySpanExt` sites and the `lane.rs` tests).
- **Verification:** `cargo test --workspace`, a scoped clippy on changed crates, the bench repro at
  concurrency 100 on `info`, and (R5) repeated parallel + serial test runs.
- **Docs:** update the observability crate's module docs with the panic-safety invariant and the
  `#[instrument]`-on-hot-paths guidance so the density stays a decision (§9).

## 6. Open questions for owner review

1. Accept the dependency bump under the AGENTS "Architectural (changed dependency)" gate?
2. Appetite for `tracing-test` as a dev-dependency vs the in-tree `with_subscriber` + serialize fix?
3. If the P0 survives dependency + instrumentation work, which residual-risk posture (isolation vs the
   `panic = "abort"` trade-off) is acceptable?
