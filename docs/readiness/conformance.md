# Conformance Readiness — Temporal v1.31.0

> Sibling of [`delivery.md`](./delivery.md). **This is the single status surface for how far tokeira
> has progressed toward Temporal v1.31.0 compliance.** It is the *numerator*. The *denominator* — what
> full v1.31.0 compliance **means** — is defined in [`../conformance/v1.31.0/`](../conformance/v1.31.0/README.md).
>
> Supersedes the status tables previously scattered across `temporal-functional-conformance/reference/
> FINDINGS.md` (its Status ledger) and `api-conformance-tracker/tracker.md` (its progress counts). Those
> remain as **detail-by-reference**: FINDINGS for the deep per-cluster investigation + Implementer
> Mandate; tracker for the RPC→spec index.

**Target:** `TEMPORAL_SERVER_COMPAT = 1.31.0`, proto `v1.62.11` (`crates/tokeira-build-info/src/pinned.rs`).
**Last updated:** 2026-06-22 · **Single contributor for status:** Kiro.

## How to read this

Every area carries one of three honest states — we do **not** assert compliance we have not measured:

- ✅ **Verified** — exercised and passing (unit/property/golden, or the Tier-2 functional corpus, or a
  cited end-to-end run). Evidence noted.
- 🟡 **Implemented, unverified** — code exists and the happy path works, but the full conformance
  surface has not been measured against the corpus.
- ⬜ **Outstanding** — not implemented / not started.
- ⏸ **Deferred** · ⛔ **Out of public scope** (depends on an internal surface tokeira does not front).

A green check in code is not compliance; a measured pass against v1.31.0 ground truth is. (See the
Implementer Mandate in FINDINGS — "a wrong guess behind a green check bakes in non-conformance.")

## The three conformance tiers

| Tier | What it proves | Owning spec | State |
|------|----------------|-------------|:-----:|
| Compatibility surface/metadata | The claim is explicit, queryable, pinned | `temporal-compatibility`, `temporal-compatibility-surface` | 🟡 |
| Tier-1 in-process oracle | Responses + history match v1.31.0, RPC coverage gate | `conformance-harness` (`tokeira-conformance` crate) | ⬜ not built |
| Tier-2 functional corpus | Temporal's own Go suites pass over gRPC | `temporal-functional-conformance` | 🟡 partial |

## Tier-2 functional corpus — cluster status

From the canonical run (corpus @ v1.31.0; 100 entrypoints; 1501 per-test outcomes: 1194 fail / 267
unfinished / 19 pass / 21 skip at the 2026-06-09 baseline, then targeted fixes below).

| Cluster | Area | State | Measured | What remains |
|---------|------|:-----:|:--------:|--------------|
| C4a | Nexus endpoint admin CRUD | ✅ | impl; 13 edge tests | Operator-measure the ~17 admin tests; author proptests P1–P4. |
| C5a | Completion-callback admission validation | ✅ | done | `allowedAddresses` (2 sub-cases) deferred (deployment policy). |
| C5b | Other admission validators (links, …) | ✅ | done | Residuals driven by a corpus re-run. |
| C6 | Over-rejection (cron, nil/empty SA+memo) | ✅ | done | Full corpus re-run pending. |
| C1 | Standalone / first-class activity RPCs | 🟡 | **1 pass / 31 fail** | SA admission validation, Describe fidelity, count-by-id; chasm-activity timeout/retry (~20 tests) needs a spec. Owned by `activity-executions-first-class`. |
| C2 | Worker deployment / versioning | 🟡 | 19/19 (1 suite) | **Untriaged: `TestVersioningFunctionalSuite` (406), `TestDeploymentVersionSuite` (68), `TestVersioning3FunctionalSuite` (13), `TestWorkerRegistryTestSuite` (7).** Legacy v0.x version-sets = deliberate deviation. |
| C3 | Visibility list/query + search attributes | 🟡 | — | Run `TestAdvancedVisibilitySuite`/`…Legacy` for residual query surface (ORDER BY / BETWEEN / STARTS_WITH / keyword IN / null close-time). |
| C4b | Nexus operation execution / task transport | 🟡 | 2 suites unmeasured | Async completion-callback delivery (`nexus-async-completion`, in progress); permanent e2e gRPC round-trip test; **measure `TestNexusApiTestSuiteWith{TemporalFailures,LegacyErrorPaths}` (40+40, never run) + `TestNexusWorkflowTestSuite` (2).** |
| C7 | Lifecycle / describe `NotFound` | ⏸ | — | Re-triage after C1–C4 (many are cascades). |
| C9 | `unfinished` panic siblings | ⬜ | 0/267 | Fix the entrypoint panic; 267 siblings then resolve to real pass/fail. |
| C8 | Internal-surface / admin-service tests | ⛔ | — | Out of public scope by construction. |

**Biggest unknowns (unmeasured denominator):** C2 versioning (~494 tests untriaged), C4b Nexus
op-execution (~82 unmeasured), C9 (267 unfinished). Until these are measured, the overall conformance
percentage is **not** known — establishing real denominators is the next audit priority.

**Order of attack:** C1 → C3 (cheap re-run) → C2 (triage) → C9 (panic fix, reclassifies 267) → C7
(re-triage) → C4b (async-completion + e2e + measure 82). Done: C4a, C5a, C5b, C6.

## Tier-1 + compatibility infra — outstanding

- **`conformance-harness`**: the `tokeira-conformance` crate is **not yet built** — TestCluster
  fixture, WorkerPoller, ExpectedHistory matcher, the 121-RPC coverage manifest + gate, three-way
  reconciliation, report, CLI, CI wiring.
- **`temporal-compatibility`** (tasks.md, 9 top-level tasks open): matrix-completeness properties (3),
  kernel `cfg_feature!` adoption (4), edge `dispatch_rpc` adoption (5), the Buffa/connect-rust
  compatibility service (7), `tkr compat` (8) and `tkr ci` (9) CLIs, the Dagger compatibility module
  (10) + versioned build/lockfile policy (11), final verification (14). Release-process aspects feed
  [`infra.md`](./infra.md).
- **`temporal-compatibility-surface`**: ✅ complete — the queryable matrix spine both tiers consume.

## Active conformance specs (detail-by-reference)

- `nexus-async-completion` — async Nexus completion (C4b blocker for Odori's durable path); Wave 0 done,
  Wave 1 handed to Claude (`docs/HANDOVER-nexus-async-completion.md`).
- `workflow-id-conflict-policy-concurrency` — done (conflict-policy concurrency; `TestNexusWorkflowTestSuite`).
- `temporal-functional-conformance/reference/FINDINGS.md` — deep per-cluster investigation, ground-truth
  citations, and the Implementer Mandate. **Status lives here; investigation detail lives there.**
