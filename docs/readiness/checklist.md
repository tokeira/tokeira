# Tokeira Readiness — Tasks

> **The countdown clock.** One simple, always-current checklist of what stands between here and the
> first release. Keep it short and scannable — detail lives in the sibling docs
> ([delivery](./delivery.md), [conformance](./conformance.md), [infra](./infra.md),
> [futures](./futures.md)). Refined incrementally; not everything is captured yet.
>
> Last updated: 2026-06-23

## Conformance (v1.31.0)

- [ ] Land async Nexus completion delivery (`nexus-async-completion`) — in progress
- [ ] Build the Tier-1 conformance oracle (`conformance-harness` crate) — not started
- [ ] Measure the unmeasured denominators (versioning, Nexus op-execution, the 267 unfinished)


## Odori

- [ ] 2.4 build — the real Nexus path — in progress ([odori](./odori.md))


## tokeira.io

- [ ] Owner input ([tokeira-io](./tokeira-io.md))


## Release / Infra

- [ ] `tkr compat bump` engine + provenance / version-monotonicity gate ([infra](./infra.md))
- [ ] Platform-provisioner binary — `tkp` lifecycle binary, provenance/binding/integrity, `tkr` launcher + deployment lock + upgrade rollback ([spec](../../.kiro/specs/platform-provisioner-binary/requirements.md))
- [ ] Dagger CI substrate (`pipeline-foundation`)


## Repo migration → `tokeira` GitHub org

- [ ] Owner input + plan ([repo-migration](./repo-migration.md))


## Conformance 2

- [ ] Resolve the remaining open scope decision — auth/authz ([decisions](../conformance/v1.31.0/decisions.md))
- [x] Worker-versioning V1/V2 scope decision — resolved 2026-07-12: GA Worker Deployments only; the five deprecated RPCs conform as stock-default rejections ([worker-versioning](../conformance/v1.31.0/worker-versioning.md))
