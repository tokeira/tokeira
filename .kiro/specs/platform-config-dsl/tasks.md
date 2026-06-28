# Implementation Plan: Platform Configuration DSL

## Overview

Build the platform-DSL compiler (`tokeira-platform-dsl`) and the `compose-dsl` platform that compiles a
deployment definition into the engine's composition. The compiler frontend (lex → parse → resolve →
type-check → evaluate → neutral `Composition` IR) and the `compose-dsl` compile/translate core are done;
the `Deployment`/`Ops` impl is written but unverified. Remaining: per-field typing + secret taint +
validation parity, multi-file `use` import assembly with containment, the deferred kind→resource
realization (observability config files, DSQL infra, images, writeback, ops verbs), the RuntimeContext
precedence rule, `tkr` wiring, and — after a second platform exists — extracting the reusable DSL-platform
runtime + realizer trait.

Status legend: `[x]` done and verified, `[~]` written but not yet verified, `[ ]` not started. Existing
unit tests are example-based; the property-based tests the design mandates are task 14 and are still owed.

## Tasks

- [x] 1. Lexer, AST, diagnostics (`tokeira-platform-dsl`)
  - [x] 1.1 `logos` token set, longest-match operators, whitespace/comment skip _Requirements: 6.1_
  - [x] 1.2 Untyped, span-carrying AST for the whole grammar _Requirements: 1.1_
  - [x] 1.3 `Diag`/`Severity` carrying no resolved context value _Requirements: 6.1, 12.3_
  - [x] 1.4 `lex` entrypoint collecting all lex errors in one pass _Requirements: 6.2_
- [x] 2. Recursive-descent parser
  - [x] 2.1 Parse program/items/modules/match/kind-instances/expressions _Requirements: 1.1_
  - [x] 2.2 Multi-error recovery, synchronising at item boundaries _Requirements: 6.2_
  - [x] 2.3 String-or-ident field keys; contextual keyword keys (e.g. `image:`)
- [x] 3. Kind library + resolver
  - [x] 3.1 `KindSchema`/`KindLibrary` (category, typed fields, outputs); `compose()` library _Requirements: 2.1, 9.1_
  - [x] 3.2 Whole-program name resolution _Requirements: 3.1_
  - [x] 3.3 Duplicate top-level declaration detection _Requirements: 13.5_
  - [x] 3.4 Kind/field existence (unknown kind, unknown/missing field) _Requirements: 2.2, 2.3, 3.2_
- [x] 4. Type checker (core); follow-ups extend it
  - [x] 4.1 Sum-variant validation (input defaults + match arms) _Requirements: 5.1, 5.2_
  - [x] 4.2 Output-reference validation (`<resource>.<output>`, `<module>.<resource>.<output>`) _Requirements: 15.3_
  - [x] 4.3 Per-field value typing against kind field types _Requirements: 3.2, 5.1_
  - [x] 4.4 `Secret<T>` taint flow (no diagnostic leak; only into secret-accepting params) _Requirements: 12.3_
  - [x] 4.5 Validation-parity constraints (canonical ports, cpu/memory pairing) _Requirements: 5.1_
- [x] 5. Evaluator → neutral `Composition` IR
  - [x] 5.1 Inputs (defaults/overrides; required-unbound error), lets _Requirements: 8.3_
  - [x] 5.2 Conditional module presence (`when`), `match` with payload binding _Requirements: 5.2_
  - [x] 5.3 Builtins, `++`/path-join/record spread, `is`, `ctx` access _Requirements: 4.1, 4.2_
  - [x] 5.4 Output references lower to deferred `Value::Output` _Requirements: 15.2, 15.4_
- [x] 6. Multi-file `use` import assembly with containment
  - [x] 6.1 Relative, downward-only `use`; canonicalised path within deployment root; depth ≤ 1 _Requirements: 13.2, 13.3_
  - [x] 6.2 Acyclic, path-sorted deterministic composition; cross-file duplicate-decl _Requirements: 13.4, 13.5_
  - [x] 6.3 Content digest over sorted `(relative_path, sha256)` _Requirements: 13.6_
  - [x] 6.4 Compile resource bounds (file count/bytes/depth/AST nesting) _Requirements: 12.4_
- [x] 7. `compose-dsl` compile + translate core
  - [x] 7.1 `compile_deployment`/`compile_source` run the pipeline, halt on first error phase _Requirements: 6.1_
  - [x] 7.2 `translate_services`: `Composition` IR → `tokeira_compose::ComposeService` _Requirements: 10.2_
  - [x] 7.3 Author the canonical compose `.platform` definition shipped in `platforms/compose-dsl` — the
        platform author's structural artifact (parity with the current `ComposeConfig`-driven compose),
        using the modular `use` layout (task 6). Not a generated starter; checked into the platform crate _Requirements: 10.1, 10.2, 16.1_
- [x] 8. `compose-dsl` `Deployment`/`Ops` impl (in-memory compose case)
  - [x] 8.1 `ComposeDslConfig` plan (translate at load); `Deployment` reads it infallibly
  - [x] 8.2 `infra_modules`/`services` from the plan; bootstrap local-state module
  - [x] 8.3 `register_infra_extensions` connects `ComposePlatform`; local state backends
  - [x] 8.4 `Ops` valid_services + desired_replicas from the plan
  - [x] 8.5 Verify build + tests; remove the unused `StorageKind` import + silencer (cleanup)
- [x] 9. Deferred kind→resource realization
  - [x] 9.1 `ObservabilityConfigFiles` → config-files resource (REAL GAP: observability services currently
        mount config files that are never generated) _Requirements: 10.1, 10.2_
  - [x] 9.2 DSQL infra: `DsqlCluster` + two `DynamoDbTable` coordination tables (storage=dsql) _Requirements: 10.1_
  - [x] 9.3 Images: `Build`/`Mirror` → deploy-engine `Image`s _Requirements: 10.1_
  - [x] 9.4 Writeback resolution: `collect_writeback` from provisioned state _Requirements: 10.2_
  - [x] 9.5 `Ops` scale/logs/port_mappings for compose-dsl
- [ ] 10. `tokeira-platform` framework crate + `Realizer` seam (adopts Proposal 001)
  - [ ] 10.1 New crate `crates/tokeira-platform`; define `Realizer`, `RealizeContext`,
        `PlatformOpsBackend`, `RealizeError` — the run-time half of a kind, paired to `KindSchema` by name _Requirements: 7.3, 10.1_
  - [ ] 10.2 Generic `DslPlatform<R>` implementing `Deployment` + `Ops`: the plan, `DslOwnedResource`,
        the local-state bootstrap, generic writeback resolution (OutputRef → state) _Requirements: 7.1, 7.2, 10.2_
  - [ ] 10.3 Generic `ConfigurationRevision` (compiled `Composition` + `RealizeContext` + writeback) as
        `Deployment::Config` — one config type for every DSL platform _Requirements: 8.2, 11.1_
  - [ ] 10.4 `RuntimeContext` resolution + precedence in the framework: recorded-identity inputs
        authoritative; ambient `deployment_dir`/`home` host-supplied and never persisted; an ambient
        value conflicting with a recorded identity → retarget confirmation (folds the former task 10) _Requirements: 14.8, 14.9_
  - [ ] 10.5 create-persist (materialize the authored set) + loader (compile the persisted `platform/` +
        `inputs.toml`); the `platform/` definition root and the `inputs.toml` input-bindings snapshot _Requirements: 11.1, 11.3, 13.1, 13.3, 16.3_
- [ ] 11. Slim `compose-dsl` to a realizer over the framework
  - [ ] 11.1 Move `KindLibrary::compose()` out of `tokeira-platform-dsl` into `compose-dsl`; the compiler
        keeps only the schema *types* (no platform specifics) _Requirements: 2.1, 9.1_
  - [ ] 11.2 `FieldSpec` compile-time defaults; push observability/DSQL conventions (URLs, ports,
        retention, table/cluster names) into kind defaults + the `.platform`, so the realizer reads typed
        fields instead of embedding constants _Requirements: 5.1, 8.1_
  - [ ] 11.3 `ComposeRealizer` (`realize_resource`/`realize_service`/`realize_image`,
        `register_infra_extensions`) + `ComposeOps` (`PlatformOpsBackend`), reusing `tokeira-compose`/`-aws` _Requirements: 10.1, 10.2_
  - [ ] 11.4 Re-express `compose-dsl` as `DslPlatform<ComposeRealizer>` + the authored set + `platform()`;
        the crate drops to ~150 lines; compose parity tests stay green _Requirements: 10.1, 10.3_
- [ ] 12. `ecs-dsl` realizer — the second instance that validates the seam (before finalizing 10/11)
  - [ ] 12.1 ECS kind library: `KindSchema`s + output schemas (type/secrecy/availability) for the ECS kinds _Requirements: 10.1, 15.3_
  - [ ] 12.2 Thin `EcsRealizer` against the framework; confirm `register_*`/`PlatformOpsBackend`/output-ref
        shapes generalise (Docker vs AWS); adjust the `Realizer` trait if the seam needs it _Requirements: 7.3, 10.1_
- [ ] 13. `tkr` wiring against the generic framework (`DslPlatform<R>` / `ConfigurationRevision`)
  - [ ] 13.1 `PlatformKind::ComposeDsl` (orchestrator) + `CliPlatformKind` variant _Requirements: 1.2_
  - [ ] 13.2 `tkr deployment create`: persist the authored `platform/` set + write `inputs.toml`; seed
        `tokeirad.toml`. Retention/versioning is owned by the `platform-provisioner-binary` (tkp) spec _Requirements: 11.1, 16.3_
  - [ ] 13.3 `deployment_dir.rs`: `PlatformDeploymentConfig::ComposeDsl(Box<ConfigurationRevision>)`; the
        loader compiles the persisted `platform/` + `inputs.toml` — no per-platform config type _Requirements: 1.2, 16.3_
  - [ ] 13.4 Dispatch arms for the generic platform across infra/deploy/image/observability + `PlatformOps` _Requirements: 1.1_
  - [ ] 13.5 Checkpoint: `tkr deployment create / infra apply / deploy apply` drive a compose-dsl deployment end-to-end (in-memory) _Requirements: 10.2_
- [ ] 14. Property-based tests (proptest), tagged `// Feature: platform-config-dsl, Property N`
  - [ ] 14.1 P1 compilation pure/total _Requirements: 4.1, 4.3_
  - [ ] 14.2 P2 deterministic execution given context _Requirements: 4.2_
  - [ ] 14.3 P3 unknown kind/field/missing required _Requirements: 2.2, 2.3, 3.1, 3.2_
  - [ ] 14.4 P4 unresolved names _Requirements: 3.1_
  - [ ] 14.5 P5 validation parity _Requirements: 5.1, 5.2, 5.3_
  - [ ] 14.6 P6 no partial composition on error _Requirements: 3.3_
  - [ ] 14.7 P7 lowering preserves identity; service→both; sole constructor _Requirements: 7.1, 7.3_
  - [ ] 14.8 P8 engine composition invariants _Requirements: 7.2_
  - [ ] 14.9 P9 diagnostics located + recovered _Requirements: 6.1, 6.2_
  - [ ] 14.10 P10 value-only edits change only values _Requirements: 8.2_
  - [ ] 14.11 P11 unbound/mistyped inputs _Requirements: 8.3_
  - [ ] 14.12 P12 no version pin; derived _Requirements: 9.2_
  - [ ] 14.13 P13 compose parity _Requirements: 10.1, 10.2, 10.3_
  - [ ] 14.14 P14 retained definition round-trips _Requirements: 11.1, 11.3_
  - [ ] 14.15 P15 no ambient authority / closed context _Requirements: 12.1, 12.5_
  - [ ] 14.16 P16 secrets declared, never read or echoed _Requirements: 12.2, 12.3_
  - [ ] 14.17 P17 import containment fail-closed _Requirements: 13.2, 13.3_
  - [ ] 14.18 P18 bounded compile _Requirements: 12.4_
  - [ ] 14.19 P19 deterministic file-set composition + digest _Requirements: 13.4, 13.5, 13.6_
  - [ ] 14.20 P20 context implicit+declared; no provider naming in composition _Requirements: 14.1, 14.2, 14.4_
  - [ ] 14.21 P21 output references create edges; resolve at apply _Requirements: 15.2, 15.3_
  - [ ] 14.22 P22 recorded identity context authoritative over ambient _Requirements: 14.8, 14.9_
  - [ ] 14.23 P23 applies compile the persisted copy, not the live platform-crate file _Requirements: 16.1, 16.3, 16.5_
- [ ] 15. `.platform` deployment-definition on-disk layout contract
  - [ ] 15.1 Define the root-definition filename (`compose.platform`) and the rule that compilation
        begins from it; the loader resolves the file set from this entry point _Requirements: 13.1_
  - [ ] 15.2 Define the deployment-root layout: definition files at the root and at most one directory
        level below it (depth ≤ 1), enforced as a load-time/compile-time diagnostic _Requirements: 13.3_
  - [ ] 15.3 Define the retained file set: the complete `(relative_path, sha256)` set the provisioner
        records and re-materializes on the deployment root, paired with the compiling `tkp` version
        _Requirements: 11.1, 11.3_

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1", "1.1", "1.2", "1.3", "1.4", "2", "2.1", "2.2", "2.3"] },
    { "wave": 2, "tasks": ["3", "3.1", "3.2", "3.3", "3.4"] },
    { "wave": 3, "tasks": ["4", "4.1", "4.2", "4.3", "4.4", "4.5"] },
    { "wave": 4, "tasks": ["5", "5.1", "5.2", "5.3", "5.4"] },
    { "wave": 5, "tasks": ["7", "7.1", "7.2", "15", "15.1", "15.2", "15.3"] },
    { "wave": 6, "tasks": ["8", "8.1", "8.2", "8.3", "8.4", "8.5"] },
    { "wave": 7, "tasks": ["6", "6.1", "6.2", "6.3", "6.4", "9", "9.1", "9.2", "9.3", "9.4", "9.5", "10", "10.1", "10.2"] },
    { "wave": 8, "tasks": ["7.3", "10", "10.1", "10.2", "10.3", "10.4", "10.5"] },
    { "wave": 9, "tasks": ["11", "11.1", "11.2", "11.3", "11.4", "12", "12.1", "12.2"] },
    { "wave": 10, "tasks": ["13", "13.1", "13.2", "13.3", "13.4", "13.5"] },
    { "wave": 11, "tasks": ["14", "14.1", "14.2", "14.3", "14.4", "14.5", "14.6", "14.7", "14.8", "14.9", "14.10", "14.11", "14.12", "14.13", "14.14", "14.15", "14.16", "14.17", "14.18", "14.19", "14.20", "14.21", "14.22", "14.23"] }
  ]
}
```

## Notes

- **The `.platform` files are introduced across several tasks, not one.** The read path
  (`ROOT_DEFINITION = "compose.platform"`) landed in 7.1; the canonical definition is authored in the
  platform crate (7.3); the multi-file `use` set is task 6; `tkr deployment create` persists the authored
  definition (13.2); the loader compiles the persisted file set (13.3).
- **`.platform` authoring & persistence model.** A `.platform` definition is a platform-author artifact
  checked into the owning platform crate (e.g. `platforms/compose-dsl/`), not an operator starter generated
  at create time. At `tkr deployment create` the operator selects a platform and supplies inputs; create
  persists the authored definition (the full `(relative_path, sha256)` file set) into deployment state, and
  every subsequent apply compiles that persisted copy rather than re-reading the crate. This is what lets a
  deployment pin its platform definition independently of later edits to the crate. The persistence and
  retention contract itself — storage location, versioning against the compiling `tkp`, re-materialization
  onto a deployment root — is owned by the `platform-provisioner-binary` (tkp) spec; this spec defines only
  the artifact (task 15) and the create-time hand-off (13.2).
  Task 15 pins the previously-implicit on-disk *layout contract* those tasks all rely on — the
  root-definition filename, the depth-≤-1 rule, and the retained `(relative_path, sha256)` file set —
  so the contract has one authoritative home rather than being inferred from the read/loader code.
- **Task 8 + 9.1 are complete and verified** (12 passing tests, clippy + nightly-fmt clean). Earlier this
  note flagged 9.1 as the real gap in task 8 — `deployment.rs` now realizes `ObservabilityConfigFiles`
  via `DslOwnedResource`, so in-memory compose stands up the full observability stack, not just bare
  services + `tokeirad`. The remaining realization gaps are 9.2 (DSQL infra), 9.3 (images), 9.4
  (writeback), and 9.5 (Ops verbs).
- **RuntimeContext precedence (Req 14.8/14.9) is settled in the spec:** recorded identity values
  (`region`, `account`) are authoritative; ambient machine-local values (`deployment_dir`, `home`) are
  host-supplied and unpersisted; a host value conflicting with a recorded identity value requires explicit
  operator confirmation (mirrors the deployment-lock mis-apply guard).
- **Proposal 001 is adopted in full** (`proposals/001-platform-framework-and-realizer.md`; see design.md's
  governing adoption note). Tasks 10–13 above are its realization: task 10 extracts the generic
  `tokeira-platform` framework (the `Realizer` seam, `DslPlatform<R>`, `ConfigurationRevision`, ctx
  precedence, create-persist/loader); task 11 slims `compose-dsl` to a kind library + `ComposeRealizer` +
  authored `.platform`; task 12 is the `ecs-dsl` realizer that validates the seam against a second
  (AWS) platform before the compose move is finalized; task 13 points `tkr` at the generic
  `DslPlatform<R>`/`ConfigurationRevision`, so a future DSL platform needs no new `tkr` dispatch type.
- **Open owner decision:** whether the reusable runtime lands in `tokeira-platform-dsl` itself or a new
  `tokeira-platform-dsl-runtime` crate (compiler-vs-runtime separation) — settle at task 12.
- The hand-rolled parser (vs `chumsky`) remains revisitable if the grammar grows; `ariadne` rendering can
  replace the `line:col` renderer without changing the contract.
