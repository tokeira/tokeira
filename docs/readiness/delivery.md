# Tokeira Delivery Readiness

> **This is the current, definitive readiness document for tokeira.** It is the single
> entry point for "where are we, and what is left, to ship." Each workstream below has a
> focused sibling doc in this folder; this page is the index and the at-a-glance status.
> If another document disagrees with this one about *status*, this one wins — fix the other.

**Last updated:** 2026-06-22 · **Maintained by:** Kiro (single contributor for status)

## What "delivery" covers

Five independent workstreams gate a tokeira release. They are tracked separately because they
have different owners, cadences, and definitions of done:

| # | Workstream | Sibling doc | One-line status |
|---|------------|-------------|-----------------|
| 1 | **Conformance** — Temporal v1.31.0 compatibility | [`conformance.md`](./conformance.md) | Partial; core surface in place, large areas unmeasured. |
| 2 | **Release / Infra** — build provenance, proto/compat pinning, release process | [`infra.md`](./infra.md) | Direction defined; CLI/CI automation outstanding. |
| 3 | **Repo migration** — move to the `tokeira` GitHub org | [`repo-migration.md`](./repo-migration.md) | Not started (owner input needed). |
| 4 | **Odori** — agentic coding workflow on the tokeira spine | [`odori.md`](./odori.md) | Nexus dependency chain verified; 2.4 build in progress. |
| 5 | **tokeira.io** — public site (Cloudflare Worker) | [`tokeira-io.md`](./tokeira-io.md) | Owner input needed (Claude Code + Design). |

## 1. Conformance (Temporal v1.31.0)

tokeira claims behavioural compatibility with **Temporal server v1.31.0** (`TEMPORAL_SERVER_COMPAT`),
building against vendored proto **v1.62.11** (`TEMPORAL_PROTO_VERSION`). The **definition of what that
claim means** — the surface and behaviour an interested party should expect — lives in
[`../conformance/v1.31.0/`](../conformance/v1.31.0/README.md). **Our progress toward it** (what is
verified, implemented-but-unverified, or outstanding) is tracked in [`conformance.md`](./conformance.md).

Headline: the core public API surface, admission validation, visibility, worker-deployment basics, and
Nexus endpoint admin + synchronous task transport are in place; versioning suites, much of Nexus
operation execution, and a panic-truncated set of tests remain unmeasured. See `conformance.md`.

### Research efforts ahead of delivery

- **Task Queue Priority & Fairness.** Temporal v1.31.0 makes task-queue priority and fairness GA — a new
  matcher component enabled by default, priority keys respected by default, and per-queue/namespace/
  cluster fairness via `matching.enableFairness`. We need to understand what this means for the tokeira
  architecture (the runtime broker / lane model and how priority and fairness map onto it) before
  deciding how it enters the conformance surface. Tracked here as a pre-delivery research effort, not yet
  a decision in [`../conformance/v1.31.0/decisions.md`](../conformance/v1.31.0/decisions.md).

## 2. Release / Infrastructure

The release-process direction (build provenance, proto-version monotonicity, the compatibility-claim
bump workflow, the Dagger CI substrate) is captured in [`infra.md`](./infra.md). The broader
tagging/release-management strategy is deliberately deferred.

## 3. Repo migration to the `tokeira` GitHub org

Tracked in [`repo-migration.md`](./repo-migration.md). Owner input required — scaffolded only.

## 4. Odori

The agentic coding workflow hosted on tokeira's durable spine. The engine-side Nexus dependency chain
Odori needs is verified end-to-end (endpoint admin, worker-targeted dispatch, sync round-trip); the
durable async path depends on the `nexus-async-completion` work (in progress). See [`odori.md`](./odori.md).

## 5. tokeira.io

The public landing site, deployed via a Cloudflare Worker (Claude Code + Design work). Owner input
required — scaffolded only. See [`tokeira-io.md`](./tokeira-io.md).
