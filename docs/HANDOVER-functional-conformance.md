# Hand-over — drive the functional conformance corpus to green

**Author:** Kiro · **Date:** 2026-06-24 · **For:** Claude Code (long continuous session)
**Repos:** this workspace (`tokeira`, Rust — most fixes land here) **and** the pinned fork
(`../temporal`, Go, branch `tokeira/conformance-v1.31.0` — harness seams + skip registry).

> **Mission:** work each in-scope Temporal functional suite to a **clean pass**, in the order set by
> [`readiness/functional-test-order.md`](./readiness/functional-test-order.md). This is **fix-to-green,
> not record-state**: run a suite, fix the real gaps it exposes, and **repeat the suite until it passes
> clean**, then move to the next. Failures are work to be done, not data to file.

---

## 1. "Pass clean" — the per-suite exit criterion

A suite passes clean when **every test in it is either green or a classified skip** (out-of-public-scope
or deliberate-deviation) **with a cited reason** — zero `real-gap` failures, zero `unfinished`. A
`real-gap` test that starts passing is flipped to required-pass. You do not leave a suite until it meets
this bar.

## 2. Preconditions (confirm before starting)

- **Metrics bridge merged** (Group A passes; metric-gated suites aren't false-failing — see
  `HANDOVER-conformance-metrics-bridge.md`).
- **`tokeirad` built:** `cargo build -p tokeirad` → `target/debug/tokeirad`.
- **Fork in place:** `../temporal` on `tokeira/conformance-v1.31.0`, at the `v1.31.0` tag (running from the
  fork's `main` is rejected).
- **Go toolchain pinned:** `GOTOOLCHAIN=go1.26.2` for every `go test` (the run-all runner sets it; set it
  for manual runs too).

## 3. Read first

- [`readiness/functional-test-order.md`](./readiness/functional-test-order.md) — the **order** (tiers),
  the in/deferred/out partition, and the metrics-capture approach (incl. the Group A/B split).
- [`testing/functional-conformance-harness.md`](./testing/functional-conformance-harness.md) — the
  **operating manual**: prerequisites, run-all/ledger commands, single-suite iteration, the skip
  registry, and the binding **"Conventions for acting on a run."**
- [`conformance/v1.31.0/{supported,excluded,decisions}.md`](./conformance/v1.31.0/README.md) — the **scope
  boundary** (what is in, out, and under decision).
- [`readiness/conformance.md`](./readiness/conformance.md) + the cluster investigation +
  Implementer Mandate in `…/temporal-functional-conformance/reference/FINDINGS.md`.
- [`readiness/edge-unimplemented.md`](./readiness/edge-unimplemented.md) — real-gaps frequently map to
  this worklist or to an `api-conformance-*` spec; fix under the owning spec where one exists.
- `AGENTS.md` — the binding repo rules (§8 ground truth, §9 docs, commits, never-commit list).

## 4. The drive-to-green loop (per suite, in tier order)

1. **Run** the suite — single-suite for iteration, run-all + ledger for the baseline:
   ```bash
   TOKEIRA_CONFORMANCE_FRONTEND_ADDR=127.0.0.1:7233 GOTOOLCHAIN=go1.26.2 \
     go test -tags test_dep -count=1 -run '^TestSuiteName$' ./tests/ -v
   ```
2. **Classify** every non-pass: `pass` / `real-gap` / `deliberate-deviation` / `out-of-public-scope`.
3. **Act:**
   - `real-gap` → **fix it** in **edge / runtime / storage**, ground-truthed to v1.31.0, **no kernel
     additions**. Where it maps to a known item (`edge-unimplemented.md`, an `api-conformance-*` spec),
     fix under that spec's discipline.
   - `out-of-public-scope` / `deliberate-deviation` → **skip with a cited reason in the registry**
     (`tests/testcore/tokeira_conformance_skip.go`) — **never edit a corpus test body**.
   - `unfinished` (panic) → fix the entrypoint panic so the siblings resolve to real outcomes, then
     reclassify them.
4. **Re-run** the suite. **Repeat 2–4 until it passes clean** (§1).
5. **Record** the suite green in `readiness/conformance.md`; advance to the next tier.

## 5. Conventions (binding — both repos)

- **Ground truth = v1.31.0.** `proto/upstream/` for wire shape; `git -C ../temporal show v1.31.0:<path>`
  / `git grep <pat> v1.31.0` for behaviour. Cite the source in the fix. Never web-search/memory.
- **No kernel additions.** Conformance fixes are edge/runtime/storage. A fix that seems to need the
  kernel is the signal to **stop and raise** (this is the Group B signal — see §6).
- **Config defaults to a constant, not a knob.** Pin the v1.31.0 default as a hardcoded constant; promote
  to real config only when it is genuine deployment policy, and then raise it as a decision.
- **Feature modes are independent runs.** A non-default mode is exercised by booting `tokeirad` in that
  mode and running the corpus subset as a separate tagged run — not via per-test dynamic-config overrides.
- **Skip registry only; never test bodies.** The corpus stays byte-for-byte upstream so it rebases on
  each compat bump. Every skip carries a cited reason.
- **`GOTOOLCHAIN=go1.26.2`** for all `go test`.
- **tokeira commits:** message via `fsWrite` to `artifacts/cm-*.txt` → `git commit -F …` → `rm -rf
  artifacts`; **never** `git add .`/`-A` (forbidden: `.claude/`,
  `.kiro/specs/temporal-functional-conformance/reference/runall-results.json`); push to `main`.
- **Fork commits:** on `tokeira/conformance-v1.31.0` per the fork's flow; edits confined to the
  onebox/Shape-2 seam, the ledger/skip registry, and the `metricstest` pull-source seam — never a test
  body.

## 6. When a suite won't go clean

If a `real-gap` resists after a couple of honest attempts, diagnose the **root cause** rather than
patching. If the green path requires **kernel work** or **re-opening a settled design decision** (the
canonical case: Group B's async-completion tests, which would need Temporal's internal token wire format +
`StateMachineRef` staleness) — **stop and raise**. Do **not** force-pass, and do **not** port Temporal
internals to chase a number. Reclassify as `deliberate-deviation` only when it is genuinely an
internal-representation coupling, with citation; the observable contract is then covered by tokeira-owned
behavioural tests.

## 7. Scope guards — do NOT "fix" these

- Surfaces in `excluded.md` (experimental/pre-release, internal/admin/replication/DLQ) and TBDs in
  `decisions.md` (auth, worker-versioning V1/V2) → **cited skips, not fixes**.
- **Group B** async-completion token tests → stay `deliberate-deviation`; do not re-open the token design.
- Deferred tiers (priority/fairness, versioning V1/V2) → out until their owning decision lands.

## 8. Definition of done

- **Per suite:** passes clean (§1).
- **Overall:** every in-scope tier suite (functional-test-order Tiers 1–9) passes clean; deferred and
  out-of-scope suites carry cited skips; `readiness/conformance.md` reflects the green state; run-all +
  ledger shows **no unclassified non-pass**.

## 9. Context map

- `readiness/functional-test-order.md` — the order + scope + metrics approach (start each tier here).
- `testing/functional-conformance-harness.md` — how to run + conventions for acting on a run.
- `conformance/v1.31.0/{supported,excluded,decisions}.md` — the scope boundary (never cross it).
- `readiness/conformance.md` — where suite-green status is recorded.
- `readiness/edge-unimplemented.md` + `api-conformance-*` specs — where many real-gaps are owned.
- `AGENTS.md` — the binding rules.
