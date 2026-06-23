# Tokeira Readiness — Tasks

> **The countdown clock.** One simple, always-current checklist of what stands between here and the
> first release. Keep it short and scannable — detail lives in the sibling docs
> ([delivery](./delivery.md), [conformance](./conformance.md), [infra](./infra.md),
> [futures](./futures.md)). Refined incrementally; not everything is captured yet.
>
> Last updated: 2026-06-23

## 1. Conformance (v1.31.0)

- [ ] Resolve the two open scope decisions — auth/authz, worker-versioning V1/V2 ([decisions](../conformance/v1.31.0/decisions.md))
- [ ] Land async Nexus completion delivery (`nexus-async-completion`) — in progress
- [ ] Build the Tier-1 conformance oracle (`conformance-harness` crate) — not started
- [ ] Measure the unmeasured denominators (versioning, Nexus op-execution, the 267 unfinished)

## 2. Release / Infra

- [ ] `tkr compat bump` engine + provenance / version-monotonicity gate ([infra](./infra.md))
- [ ] Dagger CI substrate (`pipeline-foundation`)

## 3. Repo migration → `tokeira` GitHub org

- [ ] Owner input + plan ([repo-migration](./repo-migration.md))

## 4. Odori

- [ ] 2.4 build — the real Nexus path — in progress ([odori](./odori.md))

## 5. tokeira.io

- [ ] Owner input ([tokeira-io](./tokeira-io.md))
