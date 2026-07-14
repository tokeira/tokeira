# Implementation Plan: Platform Configuration DSL

## Overview

The platform is authored as an interpreted **`.tkd`** deployment definition (Rust subset, parsed by
`syn`) and interpreted at plan/apply time by the shared, platform-agnostic **`tokeira-tkd`** crate, which
each platform binds through a `HostBridge`. The interpreter, the two live bridges (`compose-syn`, `eks`),
the kinds, the builder vocabulary, admission (`#[create]`/`#[require]`), output references, writeback, the
orchestrator adapter, the fidelity oracle, and `tkr deployment create` (writes `definition.tkd` +
`metadata.json` + empty `state/`) are **built**. What remains is the live `tkp` interpret→apply path, the
retarget gate, reconciling the `storage`/`region` duplication, the property-test suite, and the roadmap
items.

> **The bespoke plan is superseded.** The earlier tasks 1–15 targeted a `logos`/`chumsky`/`ariadne`
> compiler, a `Composition` IR, a `KindLibrary`/`Realizer`/`DslPlatform` framework, and
> `tokeira-platform-dsl`/`compose-dsl`/`ecs-dsl` crates. **None of those crates exist** — the design
> pivoted to the `.tkd` interpreter (Proposals 003/004) and extracted it into `tokeira-tkd`. The
> superseded plan is recorded in [`proposals/HISTORY.md`](./proposals/HISTORY.md).

Status legend: `[x]` done and verified, `[~]` implemented but not independently verified here, `[ ]` not
started. Tasks reference the requirements in `requirements.md` and the properties in `design.md`.

## Built

- [x] 1. Shared interpreter `crates/tokeira-tkd` _Requirements: 1, 3, 11_
  - [x] 1.1 `syn` parse + `schema::collect` (type/fn tables; `#[create]`/`#[require]` extraction)
  - [x] 1.2 `subset::check` — reject-by-default allow-list, run **before** evaluation _Requirements: 2, 3, 12_
  - [x] 1.3 `eval` — the AST walk; one `Value<H>` model (scalars/Vec/Tuple/Opt/Struct/Enum/Host) _Requirements: 3_
  - [x] 1.4 `admission` — `config()` host-free guard, `#[require]` eval, `#[create]` `retarget_check` _Requirements: 4_
  - [x] 1.5 `HostBridge` trait — the platform seam (kinds, builder verbs, `cx` reads); no `Box<dyn Any>` _Requirements: 2, 9_
  - [x] 1.6 No operator-reachable panic (every path returns `EvalError`); no-panic fuzz test _Requirements: 12_
- [x] 2. Compose bridge `platforms/compose-syn` (`ComposeBridge`) — the reference platform _Requirements: 9_
  - [x] 2.1 `bridge.rs` — `HostObj` handle enum, per-kind constructors, builder-verb + `cx` method shims
  - [x] 2.2 `kinds.rs` — `LocalStateDir`/`DsqlCluster`/`DynamoDbTable`/`ObservabilityConfigFiles`/`Service`, each `Kind::realize` → engine resource _Requirements: 2_
  - [x] 2.3 `builder.rs` — the vocabulary (`Deployment`/`module`/`resource`/`service`/`writeback`/`output`, `Vol`); a service lowers to both an infra `Resource` and a deploy-engine `Service` _Requirements: 2_
  - [x] 2.4 `context.rs` — `Cx { project_name, region }` + author-side path helpers (`state`/`config`/`docker_sock`); `deployment_dir` not surfaced to the `.tkd` _Requirements: 13_
  - [x] 2.5 `definition.tkd` — the shipped compose definition (`DEFAULT_TKD`); output-reference writeback (dotted-key) _Requirements: 1, 5, 6_
  - [x] 2.6 `adapter.rs` — `TkdDeployment`/`TkdConfig` → `tokeira_orchestrator::{Deployment, Ops, PlatformConfig}`; `collect_writeback` resolves deferred `Output` handles _Requirements: 6, 7_
  - [x] 2.7 Fidelity oracle — interpreted `.tkd` == compiled `definition.rs` == engine `ComposeDeployment` (InMemory + DSQL): workload shape, namespaces, per-module resource identity, writeback _Requirements: 9 (Property 8)_
  - [x] 2.8 `tkr deployment create --platform compose` writes `definition.tkd` + `metadata.json` (identity + `storage` + `status = Created`) + empty `state/` (`apps/tkr/src/deployment_dir.rs` `create`, `metadata.rs`). (Prototypical `tokeirad.toml` seeding + `--storage`/`--region` bake-in are outstanding — task 4.2/4.4.) _Requirements: 8.1_
- [x] 3. Second bridge `platforms/eks` (`EksBridge`) on the shared interpreter — proves multi-platform _Requirements: 9_
  - [x] 3.1 `EksBridge` + EKS kinds; `.tkd` config structs (`ServiceManifest`, `IngressRule`) decomposed via the bridge

## Remaining

- [ ] 4. `tkr`/`tkp` live apply + create-completeness (create-persist is partial — task 2.8) _Requirements: 8, 11_
  - [ ] 4.1 `tkp` interprets the persisted `definition.tkd` against the injected `Cx` and drives infra/deploy `plan`/`apply` end-to-end (wire the adapter's live-apply handles — `register_infra_extensions`/`register_deploy_extensions` — the `ComposePlatform`/AWS clients they currently stub) + the writeback into `tokeirad.toml` at apply _Requirements: 8.3_
  - [ ] 4.2 `create` seeds a **prototypical `tokeirad.toml`** for compose (call `prototypical_server_config(storage, region)`; currently only `local`/`ecs` seed it) so operators can edit server-config defaults before the first apply _Requirements: 8.1_
  - [ ] 4.3 Retarget gate: on re-apply, `retarget_check` runs against the recorded prior config value before reconcile; a changed `#[create]` field (`storage`) refuses _Requirements: 4.3, 13.2_
  - [ ] 4.4 **Reconcile `storage`/`region`** (Roadmap R5): make the `.tkd`'s `config()` the source of truth — patch the seeded `.tkd` from `--storage`/`--region` at create (or derive `metadata.json.storage` from the `.tkd`); wire `Cx.project_name` (from the deployment `name`) and `Cx.region` (from the recorded config), which are currently constructed only in tests _Requirements: 8.6, 13.1, 13.2_
  - [ ] 4.5 Checkpoint: `tkr deployment create` → `tkp` `infra apply`/`deploy apply` stands up a compose-syn deployment (in-memory) end-to-end _Requirements: 8, 9_
- [ ] 5. Property-based tests (proptest), tagged `// Feature: platform-config-dsl, Property N` _Requirements: as noted_
  - [ ] 5.1 P1 deterministic interpretation given `Cx` _Requirements: 3_
  - [ ] 5.2 P2 out-of-subset rejected before evaluation _Requirements: 2, 3, 12_
  - [ ] 5.3 P3 `config()` host-free _Requirements: 4_
  - [ ] 5.4 P4 `#[create]` retarget vs reconcile _Requirements: 4, 11_
  - [ ] 5.5 P5 `#[require]` gates admission _Requirements: 4_
  - [ ] 5.6 P6 service → both infra resource + deploy workload; sole constructor _Requirements: 2_
  - [ ] 5.7 P7 output references resolve at apply, not interpretation _Requirements: 5_
  - [ ] 5.8 P8 fidelity — interpreted `.tkd` == compiled reference (extend the existing oracle to a proptest) _Requirements: 9_
  - [ ] 5.9 P9 writeback projects only declared keys _Requirements: 6_
  - [ ] 5.10 P10 no ambient authority; never panics _Requirements: 12_
  - [ ] 5.11 P11 digest stable + retained-definition round-trip _Requirements: 7, 8_

## Roadmap (see `requirements.md` → Roadmap)

Each is an engine-identity change (a `tkp` upgrade), not a `.tkd`-only edit.

- [ ] R1. Multi-file `.tkd` composition with fail-closed import containment (a new subset item + resolver; digest over the file set) _Requirements: R1_
- [ ] R2. Declared context providers (`context {}` / `env` / `env.secret`, `Secret<T>` taint) beyond the implicit `Cx` _Requirements: R2_
- [ ] R3. Migrate the remaining compiled platforms (`compose`, `ecs`, `local`) onto the `.tkd` interpreter, at parity (ECS exercises richer output-reference wiring) _Requirements: R3_
- [ ] R4. Typed `|t: &mut TokeiraConfig|` writeback closure replacing the dotted-key strings _Requirements: R4_
- [ ] R5. Reconcile `storage`/`region` between `metadata.json` and the `.tkd` `config()` (make the `.tkd` authoritative); wire `Cx.project_name`/`Cx.region` — landed as task 4.3 _Requirements: R5_

## Notes

- **Engine identity vs configuration revision.** The interpreter (`tokeira-tkd`), each platform's builder
  vocabulary + kinds + bridge are compiled Rust — the engine identity keyed by the provisioner's
  `source_tree_hash` (owned by `platform-provisioner-binary`). The `.tkd` is the config revision. The
  reject-by-default subset is what makes a `.tkd` edit structurally incapable of becoming an
  engine-identity change.
- **Persistence/retention is the provisioner's.** This spec owns the `.tkd` artifact shape and the
  interpreter; storage, versioning, the deployment state envelope, and the monotonic `config_revision`
  are `platform-provisioner-binary`'s (task 4 is the create-time hand-off and the interpret→apply path).
- **The compiled `definition.rs` is retained** in `compose-syn` as the differential fidelity oracle; it
  is not a second source of truth.
