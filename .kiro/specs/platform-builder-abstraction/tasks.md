# Implementation Plan: Platform Builder Abstraction

This plan implements the approved requirements and design in dependency order. Every property test uses
the workspace-standard `proptest` infrastructure, runs at least 100 cases, and carries the exact feature
and property tag shown below. Checkpoints are mandatory completion gates for the preceding slice.

- [ ] 1. Establish the shared vocabulary and `tokeira-platform` crate boundary
  - [x] 1.1 Add `crates/tokeira-platform` to the workspace with the documented module surface
    - Add module documentation and public-item contracts for `artifact`, `author`, `binding`, `catalog`,
      `config`, `context`, `definition`, `error`, `graph`, `ops`, `projection`, and `selection`.
    - Add only the approved provider- and frontend-neutral dependencies; do not add Monty or depend on
      `tokeira-tkd`, a provider crate, a platform crate, `tkr`, or the provisioner CLI.
    - _Requirements: 1.9–1.14, 5.1–5.5, 14.21, 14.44, 14.48_
  - [ ] 1.2 Replace closed shared selection vocabulary with validated identifiers
    - Add serde-transparent `PlatformId` and `DefinitionFormatId` values in the shared orchestrator
      vocabulary, including canonical lower-kebab validation and actionable invalid-id errors.
    - Preserve any legacy `PlatformKind` use only behind the out-of-scope Local launch path until its
      later migration; do not introduce a new compiled platform or format inventory.
    - _Requirements: 3.9, 3.24–3.26, 3.45, 3.47–3.48, 8.12, 14.46–14.49_
  - [x] 1.3 Add format-neutral source and diagnostic models
    - Implement `RelativeDefinitionPath`, `DefinitionSourceName`, `DefinitionSource`, `SourceRange`,
      `FrontendDiagnostic`, and format-aware configuration identity inputs.
    - Reject escaping/non-canonical deployment-relative paths while permitting an explicitly selected
      authoring path that is never persisted as deployment metadata.
    - _Requirements: 3.4, 3.9, 3.17–3.20, 3.33, 3.40, 3.45–3.48, 14.12–14.14, 14.46–14.49_

- [x] 2. Implement the language-neutral Authoring Contract and graph core
  - [x] 2.1 Implement host-free author values and located Serde admission
    - Add `AuthorNode`, `AuthorValue`, enum-body shapes, opaque context tokens, and the Serde deserializer
      used by `ConfigContract` and typed provider-kind admission.
    - Preserve source ranges through field/variant/range failures without retaining any frontend runtime
      value or introducing another `Box<dyn Any>`.
    - _Requirements: 1.10–1.12, 2.1–2.4, 4.2–4.6, 8.4, 8.6–8.7, 8.10–8.11_
  - [x] 2.2 Implement owned graph handles and immutable graph completion
    - Add deployment, module, resource, output, kind, context, and context-value handles with unforgeable
      owner identity, take-once kind cells, deterministic declaration order, and read-only inspection.
    - Implement namespaces, modules, resources, workloads, writeback, output validation, identity
      uniqueness, known-target validation, and module-cycle detection.
    - _Requirements: 7.1–7.11, 8.1–8.3, 14.5, 14.7_
  - [x] 2.3 Implement `AuthorSession<P>` and its discoverable schema
    - Add standard associated functions, receiver methods, field dispatch, kind construction, context
      access, frontend results, final deployment-handle admission, and complete supported-name errors.
    - Ensure the session exposes no provider client, state store, filesystem writer, or arbitrary
      platform callback and performs no I/O.
    - _Requirements: 8.1–8.3, 8.5–8.11, 14.10–14.11_
  - [x] 2.4 Implement typed `PlatformConfig`, `PlatformContext`, and `PlatformBinding` contracts
    - Support platform-supplied validation, deterministic placeholder facts for authoring checks, typed
      context projections/tokens, selected first-party catalogs, state policy, services, artifacts,
      images, inspection renderers, and ops declarations.
    - Validate binding/catalog/service/artifact/provider/bootstrap/inspection identity uniqueness once.
    - Keep the binding independent of Definition Format and Definition Frontend.
    - _Requirements: 2.1–2.10, 4.15, 6.1–6.6, 8.5, 8.9, 8.12, 14.9–14.10_
  - [x] 2.5 Add the static `DefinitionFrontend<P>` and `DefinitionEngine<P, F>` contracts
    - Admit the frontend's config `AuthorNode` and returned deployment handle, finish the graph, compute
      format-plus-source configuration identity, and expose the pure verification entrypoint.
    - Reject a source/frontend format mismatch before parsing, provider access, or state access.
    - _Requirements: 1.10–1.14, 3.18, 3.34–3.40, 3.45–3.48, 8.10–8.12_

- [ ] 3. Add required Authoring Contract and graph property tests
  - [x] 3.1 Property test: Property 1 — graph declarations preserve order and reject foreign handles
    - Generate declaration sequences and cross-graph handle substitutions; assert order and unchanged
      graphs on rejection for at least 100 cases.
    - Tag: `// Feature: platform-builder-abstraction, Property 1: graph declarations preserve order and reject foreign handles`
    - _Requirements: 7.1–7.8, 7.10, 14.5, 14.7_
  - [ ] 3.2 Property test: Property 2 — finished graphs are exactly the well-formed graphs
    - Compare `finish` against a small reference validator over generated valid graphs and one-fault
      mutations for uniqueness, known targets, and module acyclicity.
    - Tag: `// Feature: platform-builder-abstraction, Property 2: finished graphs are exactly the well-formed graphs`
    - _Requirements: 7.7, 7.11, 14.7_
  - [ ] 3.3 Property test: Property 5 — platform config admission round-trips and rejects surplus input
    - Generate valid Compose/ECS/EKS config shapes plus unknown-field and invalid-policy mutations; prove
      frontend-neutral `AuthorNode` admission and no graph/provider effect on rejection.
    - Tag: `// Feature: platform-builder-abstraction, Property 5: platform config admission round-trips and rejects surplus input`
    - _Requirements: 2.1–2.4, 8.4, 14.9_
  - [x] 3.4 Property test: Property 6 — platform context exposure is immutable and allow-listed
    - Generate contexts and access sequences; prove repeatable typed projections, immutable state, and
      rejection of undeclared names without exposing paths, clients, credentials, or provider handles.
    - Tag: `// Feature: platform-builder-abstraction, Property 6: platform context exposure is immutable and allow-listed`
    - _Requirements: 2.5–2.8, 8.5, 8.9, 14.10_

- [ ] 4. Implement canonical provider kinds, verification, projection, selection, and writeback
  - [x] 4.1 Add the provider-owned kind and delivery registration seams
    - Implement `ProviderKind`, `KindRegistration::typed`, `ProviderKindCatalog`, placement, declared
      outputs, desired manifests, realization, and closed first-party selection without dynamic loading.
    - Keep provider inputs free of frontend hosts/values and platform topology names.
    - _Requirements: 4.1–4.15, 5.1–5.5, 8.3, 14.11, 14.22_
  - [x] 4.2 Implement complete pure definition verification
    - Realize the full resource set without clients/state and call `verify_resources`; accumulate every
      dangling dependency and non-describing resource in deterministic order.
    - Withhold a kind until its resource truthfully performs live description once prerequisites exist.
    - _Requirements: 3.34–3.39, 4.16–4.18, 9.17, 14.27–14.28_
  - [ ] 4.3 Implement `FrameworkDeployment<P>` and logical-to-physical placement
    - Realize resources in definition order, inject realized dependency ids, preserve namespaces, and
      project workloads only when their provider uses a separate deploy-engine universe.
    - Keep EKS manifest bundles on the infrastructure path and delegate clients, stores, hydration,
      images, provider refresh, mutation, and artifact delivery through registered provider contracts.
    - _Requirements: 7.3–7.5, 7.9–7.10, 9.1, 9.9, 9.11–9.16_
  - [x] 4.4 Implement shared module selection and selected-state isolation
    - Compute all/prerequisite/dependent closure with deterministic definition order; reject empty,
      unknown, or unrepresentable selection before execution.
    - Scope infrastructure, workload, writeback, reporting, and state replacement to one effective
      selection while retaining unrelated state.
    - _Requirements: 9.2–9.8, 14.8_
  - [x] 4.5 Implement explicit writeback resolution
    - Resolve literals and outputs through the realized physical-resource index, preserve declaration
      order, emit only declared keys, and apply the accepted omission behavior for absent values.
    - _Requirements: 7.5–7.6, 9.10–9.15, 14.6_
  - [ ] 4.6 Transport provider reachability and mutation failures without reinterpretation
    - Preserve `PlatformIssue` component/fact/evidence/direction, no-change plan outcomes, complete report
      rendering, and bare non-zero exit semantics; retain hard actionable apply/destroy errors.
    - Treat a downstream endpoint scheduled for first creation differently from an unexpectedly
      unreachable recorded substrate.
    - _Requirements: 9.18–9.26, 11.22, 12.22, 13.19–13.20, 14.30–14.34_

- [ ] 5. Add required provider, projection, verification, and state property tests
  - [ ] 5.1 Property test: Property 3 — typed kind admission is schema-total
    - Generate host-free author trees for every selected kind and compare admission to Serde, provider
      validation, context-token exclusion, and declared-output reference models.
    - Tag: `// Feature: platform-builder-abstraction, Property 3: typed kind admission is schema-total`
    - _Requirements: 4.1–4.8, 8.3, 8.6–8.7, 14.11, 14.22_
  - [x] 5.2 Property test: Property 4 — provider realization preserves logical placement
    - Generate verified resource DAGs and assert deterministic traversal plus exact logical identity,
      owning module, dependency ids, and naming inputs at every provider realization.
    - Tag: `// Feature: platform-builder-abstraction, Property 4: provider realization preserves logical placement`
    - _Requirements: 4.5, 4.7–4.9, 7.3–7.5, 14.5, 14.11_
  - [x] 5.3 Property test: Property 8 — definition verification is complete and pure
    - Generate complete describing sets and one-fault resource sets; assert all-and-only findings,
      deterministic order, and zero provider/state/filesystem calls.
    - Tag: `// Feature: platform-builder-abstraction, Property 8: definition verification is complete and pure`
    - _Requirements: 3.34–3.39, 4.16–4.18, 9.17, 14.27–14.28_
  - [x] 5.4 Property test: Property 9 — module selection computes the required closure
    - Compare plan/apply and destroy selection against a reference graph closure over generated DAGs,
      including no selector, invalid selector, deduplication, and order.
    - Tag: `// Feature: platform-builder-abstraction, Property 9: module selection computes the required closure`
    - _Requirements: 9.1–9.8, 14.8_
  - [x] 5.5 Property test: Property 10 — writeback is explicit, ordered, and physically resolved
    - Generate writebacks and state maps; compare literals, physical output resolution, omission cases,
      order, and the exact declared key set against a reference model.
    - Tag: `// Feature: platform-builder-abstraction, Property 10: writeback is explicit, ordered, and resolved through physical state`
    - _Requirements: 7.5–7.6, 9.10–9.15, 14.6_
  - [ ] 5.6 Property test: Property 13 — reachability issues are lossless no-change outcomes
    - Generate provider failures/evidence and assert no changes, verbatim evidence, grounded direction,
      one complete report, and a bare non-zero process result.
    - Tag: `// Feature: platform-builder-abstraction, Property 13: reachability issues are lossless no-change outcomes`
    - _Requirements: 9.18–9.24, 11.22, 12.22, 13.19, 14.30–14.32_
  - [x] 5.7 Property test: Property 14 — partial reconciliation preserves unrelated state
    - Generate graphs, state maps, and effective selections; assert byte-equal unrelated entries after
      selected replacement.
    - Tag: `// Feature: platform-builder-abstraction, Property 14: partial reconciliation preserves unrelated state`
    - _Requirements: 9.4–9.8_

- [ ] 6. Checkpoint: framework core and provider-neutral tests are green
  - Run `cargo +nightly fmt --all`, focused clippy/check/tests for `tokeira-platform` and touched
    provider-neutral crates with `--locked`, and documentation with warnings denied.
  - Confirm all tests remain hermetic and require no Docker, AWS, DSQL, or Kubernetes credentials.
  - _Requirements: 14.23–14.24_

- [ ] 7. Extract the current `.tkd` frontend onto the Authoring Contract
  - [ ] 7.1 Invert the dependency and implement `TkdFrontend`
    - Make `tokeira-tkd` depend on `tokeira-platform`; keep parsing, subset checking, evaluation,
      runtime values, spans, `#[create]`, and `#[require]` in `tokeira-tkd`.
    - Implement one adapter from `tokeira_tkd::Value`/`HostObj` to `AuthorNode`, `AuthorHandle`,
      `AuthorSession<P>`, and `FrontendDiagnostic`; do not leave standard platform bridges behind.
    - _Requirements: 1.10–1.12, 8.1–8.11, 14.44–14.45_
  - [x] 7.2 Publish the trusted `tkd` frontend descriptor and conventional export
    - Add `package.metadata.tokeira.definition-frontend` with format `tkd`, contract version, extension,
      and safe `definition.tkd` convention plus `lib.rs::frontend()`.
    - Do not publish or admit `tkdp`, add Monty, or encode a platform name in the descriptor.
    - _Requirements: 1.12–1.14, 3.24–3.26, 3.47–3.48, 8.12, 14.46, 14.48–14.49_
  - [ ] 7.3 Add `.tkd` behavioral-parity integration tests
    - Exercise current Compose/ECS/EKS definition fixtures through `TkdFrontend` and prove equal admitted
      config, graph, located errors, retarget/require behavior, and no provider/state access.
    - _Requirements: 1.12, 3.34–3.40, 8.6, 8.10–8.11, 14.45_

- [ ] 8. Implement trusted platform/frontend discovery and static provisioner assembly
  - [ ] 8.1 Implement source and published platform catalogs in `tkr`
    - Decode recognized workspace Cargo metadata and admitted published descriptors through one
      `PlatformCatalog`; validate ids, unique default, launch class, binding contract, source precedence,
      and conventional `binding()` export coordinates.
    - Retain Local only as an external catalog-selected legacy launch class; do not edit
      `platforms/local/src` or encode Local as a name branch.
    - _Requirements: 1.9, 3.24–3.26, 3.47–3.48, 14.15_
  - [ ] 8.2 Implement source and published Definition Frontend catalogs
    - Resolve `DefinitionFormatId` through recognized workspace metadata or admitted published entries;
      validate contract, extension, safe default path, one library target, no binary target, and source
      precedence.
    - Keep platform descriptors independent of formats and frontend descriptors independent of platforms.
    - _Requirements: 1.12–1.14, 3.24–3.26, 3.40, 3.47–3.48, 8.12, 14.46, 14.48–14.49_
  - [x] 8.3 Generate the disposable one-platform, one-frontend composition root
    - Make `tokeira-build` generate deterministic `Cargo.toml` and `main.rs` using the conventional
      platform binding, frontend export, and `tokeira-provisioner-cli` macro.
    - Resolve the source/lock closure from exactly CLI + selected platform + selected frontend, include
      generated overlays in engine identity, and reject missing/ambiguous/bin-owning packages.
    - _Requirements: 3.12–3.15, 3.24–3.26, 14.15, 14.49_
  - [ ] 8.4 Extend bundle descriptors and admission evidence
    - Record selected platform, Definition Format, both private contract versions, generated-root digest,
      and source/lock closure evidence in source and published bundle paths.
    - Reject request/catalog/seed/bundle/generated-root identity disagreement before placement.
    - _Requirements: 3.12–3.15, 3.24–3.26, 3.28, 3.45, 3.47–3.48, 14.15, 14.46, 14.49_
  - [ ] 8.5 Remove hard-coded platform build routing
    - Delete the Compose-only `SEED_PACKAGE`, direct platform binary build, closed platform selection from
      the migrated path, and direct Compose/ECS dependencies used for forwarding/build resolution.
    - Prove an EKS descriptor is selectable and a synthetic second frontend resolves in isolated tests
      without changing a `tkr` enum or match arm.
    - _Requirements: 3.24–3.28, 3.47–3.48, 14.15, 14.20–14.21, 14.46_

- [ ] 9. Add required configuration-identity and discovery/assembly property tests
  - [ ] 9.1 Property test: Property 7 — configuration identity follows admitted semantics
    - Generate format ids, byte edits, paths, and unrelated runtime inputs; prove equal format/bytes are
      stable, unequal formats separate equal bytes, same-format edits do not re-key the engine, and format
      changes do not re-key the Platform Binding.
    - Tag: `// Feature: platform-builder-abstraction, Property 7: configuration identity follows admitted semantics`
    - _Requirements: 3.7, 3.17–3.20, 3.45–3.46, 14.12, 14.47, 14.49_
  - [ ] 9.2 Property test: Property 22 — catalog resolution and assembly select one platform and frontend
    - Generate trusted platform/frontend catalogs and package-target shapes; compare validation,
      selection, generated bytes, exact dependency roots, identity sensitivity, and mismatch refusal to a
      reference model.
    - Tag: `// Feature: platform-builder-abstraction, Property 22: catalog resolution and assembly select exactly one platform and frontend`
    - _Requirements: 3.12–3.15, 3.24–3.26, 3.45, 3.47–3.48, 8.12, 14.15, 14.46, 14.49_

- [ ] 10. Wire the generic Bound Provisioner and deployment-directory lifecycle
  - [ ] 10.1 Make `tokeira-provisioner-cli` a platform/frontend-neutral library entrypoint
    - Implement `BoundPlatform<P, F>`, `run`, and `bound_provisioner_main!`; statically accept one binding
      and one frontend while retaining lifecycle parsing, binding gates, locks, state envelopes,
      reports, upgrade, rollback, describe, and exit behavior in the shell.
    - Remove central platform/frontend features, concrete imports, and any committed central `tkp.rs`.
    - _Requirements: 3.12–3.26, 3.33–3.40, 9.23–9.24, 14.15, 14.20–14.21_
  - [ ] 10.2 Persist format and live relative path in deployment metadata
    - Extend `metadata.json`, bundle admission, and `tkp.manifest.json` checks with Definition Format and
      the safe live relative path without treating registry metadata as desired topology.
    - Verify platform/format/engine agreement before definition evaluation, state access, or provider I/O.
    - _Requirements: 3.3–3.14, 3.26, 3.45–3.48, 14.13, 14.46, 14.49_
  - [ ] 10.3 Materialize external Definition Seeds and publish creation atomically
    - Resolve a versioned seed by platform, format, and engine identity; encode admitted create-time
      choices, stage its declared source path plus metadata/runtime/state/evidence/provisioner artifacts,
      validate through staged `tkp`, and publish the complete directory and `.latest` as one boundary.
    - Remove embedded definition constants and `include_str!` seed paths from the creation/build flow.
    - _Requirements: 3.3–3.6, 3.27–3.33, 11.15–11.16, 14.13–14.15, 14.19_
  - [ ] 10.4 Load, digest, snapshot, and restore the recorded live definition
    - On each definition-aware command, read only the recorded deployment-root source or explicitly
      selected retained revision, evaluate through the selected frontend, and preserve format/path in
      configuration history.
    - Refuse cross-format restore and retain ordinary same-engine reconciliation after restoring a valid
      revision.
    - _Requirements: 3.17–3.20, 3.26, 3.33–3.40, 3.45–3.48, 14.12–14.14_
  - [ ] 10.5 Implement explicit-format standalone definition checking
    - Support `definition check --definition <path> --format <id>` without deployment state; resolve the
      format through the trusted catalog and run the same frontend/framework verification path.
    - _Requirements: 3.34–3.40, 3.47–3.48_
  - [ ] 10.6 Forward lifecycle commands through verified deployment-local bytes
    - Make `tkr` own command parsing, selection, confirmation, local locking, verified launch, and
      argument forwarding while performing no convergence or definition interpretation in process.
    - Replace definition-file-presence forwarding inference with recorded platform launch class and
      format metadata.
    - _Requirements: 3.1–3.2, 3.12–3.17, 3.22–3.26, 14.14_

- [ ] 11. Add required creation-transaction property and lifecycle integration tests
  - [ ] 11.1 Property test: Property 23 — creation publication is all-or-nothing
    - Generate every seed, definition, bundle, provisioner, inspection-render, staging, and publication
      failure point; assert no final directory/`.latest` on failure and one complete format-consistent
      directory on success.
    - Tag: `// Feature: platform-builder-abstraction, Property 23: creation publication is all-or-nothing`
    - _Requirements: 3.3–3.6, 3.9, 3.12–3.14, 3.28–3.33, 3.48, 14.13–14.15, 14.46_
  - [ ] 11.2 Add deployment-directory lifecycle integration coverage
    - Cover create, first binding, integrity checks, live reload, config revision snapshots, same-format
      restore, mismatch refusal, selection, check/plan/apply/destroy, writeback, issues, and argument
      forwarding using temporary directories and fake provider seams.
    - _Requirements: 3.1–3.48, 9.1–9.26, 14.13–14.15, 14.30–14.32, 14.46_

- [ ] 12. Implement shared service/artifact delivery, content coupling, operations, and inspection
  - [ ] 12.1 Implement platform-owned service and artifact catalogs
    - Add immutable service/image/command/port/health/placement/configuration/delivery documents and
      operational/inspection artifact declarations without moving product content into the framework or
      provider crates.
    - _Requirements: 6.1–6.15, 14.36–14.39_
  - [ ] 12.2 Implement provider delivery and deterministic content coupling
    - Add typed document validation/canonicalization, provider realization, operational materialization,
      consumer receipts, and domain-separated identities over consumed non-secret content.
    - Couple authoritative `tokeirad.toml` and other consumed content to workload desired state while
      excluding credentials/secrets from manifests, state, evidence, and configuration identity.
    - _Requirements: 6.7–6.8, 6.16–6.21, 6.23, 11.5–11.8, 11.20–11.21, 12.20–12.21, 13.18_
  - [ ] 12.3 Implement provider-neutral operations declarations and provider executors
    - Add catalog-bound logical service requests, supported inventories, typed operation registrations,
      local-port overrides, and provider-owned Docker/AWS/Kubernetes discovery and transport.
    - Keep process/session lifecycle in `tkr`, ECS Exec separate, and desired capacity changes in
      definition plus plan/apply; add no direct scaling method.
    - _Requirements: 6.14, 10.1–10.19, 14.16–14.17, 14.38_
  - [ ] 12.4 Implement atomic inspection publication with disjoint write boundaries
    - Validate deployment-relative targets, render without reading the prior output, use same-directory
      safe temporary files and atomic replacement, and distinguish post-commit publication failure from
      convergence failure.
    - Enforce no operational/inspection publication during plan/check/operations/rollback/destroy and no
      inspection read in any lifecycle/provider path.
    - _Requirements: 3.21, 3.41–3.44, 6.21–6.23, 11.25–11.33, 14.40–14.43_

- [ ] 13. Add required content, canonicalization, operations, and write-boundary property tests
  - [ ] 13.1 Property test: Property 11 — content coupling is deterministic, sensitive, and secret-free
    - Generate consumed content and secret mutations; assert stable equal identities, changed desired
      state for changed non-secret bytes, and total secret exclusion.
    - Tag: `// Feature: platform-builder-abstraction, Property 11: content coupling is deterministic, sensitive, and secret-free`
    - _Requirements: 6.16–6.20, 11.5, 11.20–11.21, 12.20–12.21, 13.18, 14.29_
  - [ ] 13.2 Property test: Property 12 — provider canonicalization preserves platform semantic content
    - Generate valid provider documents and assert idempotent canonicalization plus semantic equality with
      platform-owned input; reject semantic additions/removals/substitutions.
    - Tag: `// Feature: platform-builder-abstraction, Property 12: provider canonicalization preserves platform semantic content`
    - _Requirements: 6.1–6.13, 6.21–6.23, 14.39_
  - [ ] 13.3 Property test: Property 15 — operations declarations are catalog-bound and deterministic
    - Generate platform service/ops catalogs and local overrides; assert membership, duplicate-free stable
      inventory, unknown-name reporting, and unchanged remote targets.
    - Tag: `// Feature: platform-builder-abstraction, Property 15: operations declarations are catalog-bound and deterministic`
    - _Requirements: 6.14, 10.2, 10.6–10.16, 14.16–14.17, 14.38_
  - [ ] 13.4 Property test: Property 24 — artifact write boundaries are disjoint
    - Generate lifecycle verbs and artifact declarations; assert exact permitted publications/consumers
      and that inspection bytes are never lifecycle/provider input.
    - Tag: `// Feature: platform-builder-abstraction, Property 24: artifact write boundaries are disjoint`
    - _Requirements: 3.21, 3.41–3.44, 6.21–6.23, 11.8, 11.28–11.32_

- [ ] 14. Checkpoint: frontend, catalogs, shell, delivery, and operations are green
  - Run formatting and focused clippy/check/tests/docs with `--locked` for `tokeira-platform`,
    `tokeira-tkd`, `tokeira-build`, `tokeira-provisioner`, `tokeira-provisioner-cli`, `tkr`, and touched
    provider crates.
  - Prove generated roots compile, default tests are hermetic, the dependency graph contains no Monty,
    and no concrete migrated platform/frontend is linked centrally.
  - _Requirements: 14.15, 14.21–14.24, 14.44–14.49_

- [ ] 15. Migrate and clean Compose first
  - [ ] 15.1 Move reusable Compose/AWS/local-state authored capabilities to provider owners
    - Implement canonical typed kinds, live description, delivery, Docker reachability classification,
      published-port discovery, and log mechanics in `tokeira-compose`/`tokeira-aws` as appropriate.
    - _Requirements: 4.1–4.18, 10.3, 11.5–11.6, 11.10–11.11, 11.22–11.23_
  - [ ] 15.2 Reduce `platforms/compose/src` to the four conventional files
    - Implement typed config, immutable context, pure ops declarations, and one binding in `config.rs`,
      `context.rs`, `ops.rs`, and `lib.rs`.
    - Delete builder, bridge, adapter, interpreter, kind, provisioner, image-mechanics, compiled
      `definition.rs`, snapshot oracle, binary entrypoint, exports, and obsolete dependencies.
    - _Requirements: 2.1–2.16, 11.12–11.19, 14.1–14.4, 14.18–14.20_
  - [ ] 15.3 Preserve the accepted Compose graph and provider behavior
    - Express in-memory and DSQL module/resource/workload structure through the live `definition.tkd` and
      provider catalogs; preserve ids, replicas, namespaces, state keys, writeback, volume/config
      dependencies, reachability, logs, and ports.
    - Replace compiled-definition differential tests with direct structural and behavioral assertions.
    - _Requirements: 11.1–11.13, 11.18, 11.20–11.23_
  - [ ] 15.4 Retain Compose-owned service and observability assets
    - Keep service manifests, images, generated config, Alloy/Mimir/Loki/Grafana dashboards/alerts, and
      templates in the Compose package outside `src`; use provider delivery only for mechanics.
    - _Requirements: 6.1–6.11, 6.15–6.23, 11.7–11.8, 11.20–11.24_
  - [ ] 15.5 Publish deterministic non-authoritative `docker-compose.yml`
    - Render the complete Compose service model at creation and after committed apply, include the
      generated/ignored-edits notice, atomically replace it, and preserve prior bytes on failed apply.
    - Prove plan and operator edits cannot affect plan/apply/operations/destroy and keep any private
      provider ledger under state.
    - _Requirements: 11.25–11.33, 14.40–14.43_

- [ ] 16. Add required Compose migration property and integration tests
  - [ ] 16.1 Property test: Property 16 — Compose graph migration has storage-mode parity
    - Generate valid Compose configs across in-memory/managed/preexisting DSQL and assert exact accepted
      graph, realization ids, manifests, dependencies, writeback, replicas, volumes, and state namespace.
    - Tag: `// Feature: platform-builder-abstraction, Property 16: Compose graph migration has storage-mode parity`
    - _Requirements: 11.2–11.4, 11.9, 11.18, 11.23_
  - [ ] 16.2 Property test: Property 17 — Compose inspection is deterministic and non-authoritative
    - Generate verified Compose desired graphs and operator edits; assert valid/stable/secret-free output,
      exact write boundaries, ignored edits, and atomic replacement behavior.
    - Tag: `// Feature: platform-builder-abstraction, Property 17: Compose inspection projection is deterministic and non-authoritative`
    - _Requirements: 3.21, 3.43–3.44, 11.25–11.33, 14.40–14.43_
  - [ ] 16.3 Add Compose creation/apply/plan/operations integration coverage
    - Parse golden inspection bytes through the Compose model; inject publication and Docker failures;
      verify lifecycle, content coupling, live mappings, and four-file package boundaries.
    - _Requirements: 10.3, 10.6–10.10, 11.1–11.33, 14.18, 14.40–14.43_

- [ ] 17. Checkpoint: Compose migration is complete and green
  - Run formatting and focused lint/check/test/doc commands with `--locked` for the framework, CLI/build,
    Compose platform/provider, AWS capabilities used by Compose, and `tkr` creation paths.
  - Confirm Compose is the first completed migration and no retired source, fixture, dependency, embedded
    seed, or compiled oracle remains.
  - _Requirements: 11.1, 11.14–11.19, 14.18–14.24_

- [ ] 18. Migrate ECS and complete the accepted production-readiness spec
  - [ ] 18.1 Implement the required reusable AWS capabilities and live evidence
    - Move ECS cluster/capacity/task/service/Cloud Map/ALB/IAM/network/DSQL/S3/ECR capabilities and SSM/log
      executors into `tokeira-aws`; implement live S3 object description with verbatim SDK evidence.
    - _Requirements: 4.1–4.18, 9.18–9.25, 10.4, 12.10–12.18, 12.22, 14.30–14.33_
  - [ ] 18.2 Reduce `platforms/ecs/src` to the four conventional files
    - Retain typed topology/security/capacity config, immutable identity/AWS facts, the canonical six-entry
      ops topology, one binding, and ECS-owned content outside `src`; remove platform-local framework,
      provider clients, direct scaling, and provisioner ownership.
    - _Requirements: 2.1–2.16, 6.12, 10.11, 12.1–12.9, 12.19–12.25, 14.1–14.4, 14.18–14.20_
  - [ ] 18.3 Preserve the six-stage ECS graph and delivery semantics
    - Implement `remote-state → networking → dsql → cluster → observability → services`, private
      networking, capacity providers, Service Connect, internal ALB, DSQL, S3 state/config, images,
      dashboards/alerts, writeback, and deterministic prerequisite/dependent closure.
    - _Requirements: 12.1–12.9, 12.19–12.24_
  - [ ] 18.4 Complete logs, port access, and administrative ownership
    - Project the exact grafana/mimir/loki/edge-api/edge-poll/controller inventory, direct-instance versus
      remote-host SSM access, Loki-first logs, and break-glass declarations through provider mechanics.
    - Keep ECS Exec as separate `tkr` orchestration and capacity as definition plus plan/apply.
    - _Requirements: 10.4, 10.11, 10.14–10.19, 12.19, 12.23–12.25_
  - [ ] 18.5 Satisfy every `ecs-production-readiness` property and qualification claim
    - Port existing sibling-spec tests/evidence to the new owners without weakening, silently replacing,
      or marking any functional readiness item complete before its evidence exists.
    - _Requirements: 1.6, 1.8, 12.1–12.25, 14.25, 14.35_

- [ ] 19. Add required ECS migration property and production-readiness tests
  - [ ] 19.1 Property test: Property 18 — ECS migration preserves the accepted graph and endpoint model
    - Generate valid ECS configs/selections and compare the six-stage DAG, closure/order, and exact six
      operation tuples to the sibling-spec reference model.
    - Tag: `// Feature: platform-builder-abstraction, Property 18: ECS migration preserves the accepted graph and endpoint model`
    - _Requirements: 12.1–12.9, 12.19, 12.23–12.24, 14.17, 14.35_
  - [ ] 19.2 Run the complete hermetic ECS readiness integration matrix
    - Exercise SSM direct/remote paths, Loki-first logs, private topology, live S3 description, content
      publication coupling, issues, and every accepted production-readiness property through fake AWS
      seams; retain separately required live qualification evidence.
    - _Requirements: 12.1–12.25, 14.17, 14.30–14.35_

- [ ] 20. Checkpoint: ECS migration and production readiness are complete and green
  - Run formatting and focused lint/check/test/doc commands with `--locked` across framework, shell/build,
    ECS, AWS, provider contracts, and all `ecs-production-readiness` suites.
  - Confirm every readiness ledger claim has evidence and no Compose regression or platform-local generic
    machinery remains.
  - _Requirements: 12.1–12.25, 14.23–14.25, 14.35_

- [ ] 21. Migrate EKS without broadening its readiness scope
  - [ ] 21.1 Move reusable AWS and Kubernetes capabilities to their provider owners
    - Implement canonical EKS cluster/Pod Identity/DSQL/network resources in `tokeira-aws` and namespace,
      manifest-bundle/resource delivery, live Kubernetes describe/apply/delete, logs, and port forwarding
      in `tokeira-k8s`.
    - _Requirements: 4.1–4.18, 10.5, 13.10–13.17_
  - [ ] 21.2 Reduce `platforms/eks/src` to the four conventional files
    - Retain typed AWS/EKS/Kubernetes config, immutable identity/AWS/cluster/namespace facts, pure logical
      operations, one binding, and EKS-owned manifests/assets outside `src`.
    - Delete builders, bridges, kind wrappers, `k8s_resource.rs`, provider mechanics, and platform-local
      provisioner/adapter ownership.
    - _Requirements: 2.1–2.16, 6.13, 13.1–13.3, 13.10–13.17, 13.21, 14.1–14.4, 14.18–14.20_
  - [ ] 21.3 Preserve the one-path EKS topology and staged reachability
    - Implement `remote_state → foundation → cluster`, S3 state, private access, Pod Identity, DSQL
      writeback, namespace/workload topology, and Kubernetes objects as infrastructure resources only.
    - Distinguish first-creation downstream cluster absence from unexpectedly unreachable recorded AWS or
      Kubernetes substrate.
    - _Requirements: 13.4–13.9, 13.18–13.20, 14.34_
  - [ ] 21.4 Preserve EKS logs, port forwarding, content ownership, and unfinished status
    - Project logical Kubernetes log and Service targets through `tokeira-k8s`; keep manifests, ConfigMaps,
      images, dashboards, and alerts EKS-owned.
    - Do not claim unfinished topology/provider/wiring/live qualification complete merely because source
      ownership moved.
    - _Requirements: 1.5, 1.8, 6.13, 10.12–10.16, 13.1–13.21, 14.25_

- [ ] 22. Add the required EKS migration property and integration tests
  - [ ] 22.1 Property test: Property 19 — EKS preserves one-path topology and staged reachability
    - Generate valid EKS configs and reachability states; assert exact ordered graph, infrastructure-only
      Kubernetes projection, no-change recorded-substrate issues, and plannable first creation.
    - Tag: `// Feature: platform-builder-abstraction, Property 19: EKS migration preserves one-path topology and staged reachability`
    - _Requirements: 13.4–13.9, 13.18–13.20, 14.34_
  - [ ] 22.2 Add EKS provider and operations integration coverage
    - Exercise S3/AWS/EKS/Kubernetes fake seams, Pod Identity, manifests, content coupling, DSQL writeback,
      typed logs/ports, recorded failure, first creation, and four-file package boundaries.
    - _Requirements: 10.5, 10.12–10.16, 13.1–13.21, 14.34_

- [ ] 23. Checkpoint: EKS migration is complete within its accepted scope
  - Run formatting and focused lint/check/test/doc commands with `--locked` across framework, shell/build,
    EKS, AWS, Kubernetes, and affected sibling-spec suites.
  - Confirm retained unfinished EKS work remains visibly unfinished and Compose/ECS behavior stays green.
  - _Requirements: 13.1–13.21, 14.23–14.25_

- [ ] 24. Remove transitional code and align repository specifications/documentation
  - [ ] 24.1 Enforce final ownership and dependency boundaries
    - Delete every obsolete builder/bridge/adapter/kind wrapper/provisioner binary, compatibility shim,
      embedded definition, closed migrated platform/format branch, direct central concrete dependency,
      stale fixture, comment, and export.
    - Add workspace inventory tests for exact four-file platform source sets, no platform/frontend binary,
      no cross-platform assets, no frontend runtime in provider/platform crates, and no Monty dependency.
    - _Requirements: 2.11–2.16, 3.23–3.27, 4.6–4.10, 8.7–8.12, 14.1–14.4, 14.18–14.22, 14.36–14.37, 14.44, 14.48_
  - [ ] 24.2 Amend superseded sibling-spec ownership and completed-task claims
    - Update `platform-config-dsl`, `platform-provisioner-binary`, `ecs-production-readiness`, and
      `platform-eks` only where this implementation changes source ownership or invalidates a completed
      task; preserve every unrelated decision, property, open task, and evidence trail.
    - _Requirements: 1.6–1.8, 11.17–11.18, 14.25_
  - [ ] 24.3 Update final architecture and operator documentation
    - Document `tokeira-platform` as definition-language-neutral, `tokeira-tkd` as the only implemented
      frontend, Monty-backed `.tkdp` as forthcoming separately specified support, and custom kinds as a
      revisitable deferral.
    - Document platform-owned service/artifact content, provider-owned mechanics, recorded format/path,
      external seeds, generated one-platform/one-frontend provisioners, and non-authoritative inspection
      artifacts without resurfacing a platform-owned `tkd` artifact.
    - _Requirements: 1.13–1.14, 3.3–3.6, 5.5–5.6, 6.1–6.23, 14.26, 14.48_

- [ ] 25. Add required final boundary property tests
  - [ ] 25.1 Property test: Property 20 — platform packages obey the ownership boundary
    - Generate package inventories and validate the three migrated real packages; assert exact four-file
      `src`, no binary/seed/cross-platform content, and absence of framework/frontend/provider mechanics.
    - Tag: `// Feature: platform-builder-abstraction, Property 20: platform packages obey the ownership boundary`
    - _Requirements: 2.10–2.16, 3.23, 3.27, 4.10, 8.8, 14.1–14.4, 14.18–14.21, 14.37_
  - [ ] 25.2 Property test: Property 21 — framework and provider dependencies do not invert
    - Generate dependency/import inventories and validate the real workspace; assert no concrete
      platform/provider/frontend in the framework, inward `tokeira-tkd` dependency, no frontend values in
      providers, no misplaced product content, and no Monty package.
    - Tag: `// Feature: platform-builder-abstraction, Property 21: framework and provider dependencies do not invert`
    - _Requirements: 1.10–1.14, 4.6–4.7, 5.1–5.4, 6.5–6.10, 8.7–8.12, 14.21–14.22, 14.36–14.37, 14.44, 14.48_

- [ ] 26. Final checkpoint: full implementation and documentation bar is green
  - Run `cargo +nightly fmt --all`.
  - Run `cargo lint --locked`.
  - Run `cargo check --workspace --locked`.
  - Run `cargo test --workspace --locked`.
  - Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Run the repository's offline Markdown link check, assert builds do not dirty the tree, and verify all
    24 property tags are present with at least 100 generated cases.
  - _Requirements: 14.23–14.26, 14.35, 14.40–14.49_

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["2"],
  "4": ["2"],
  "5": ["4"],
  "6": ["3", "5"],
  "7": ["6"],
  "8": ["7"],
  "9": ["8"],
  "10": ["4", "7", "8", "9"],
  "11": ["10"],
  "12": ["4", "10"],
  "13": ["12"],
  "14": ["11", "13"],
  "15": ["14"],
  "16": ["15"],
  "17": ["16"],
  "18": ["17"],
  "19": ["18"],
  "20": ["19"],
  "21": ["20"],
  "22": ["21"],
  "23": ["22"],
  "24": ["23"],
  "25": ["24"],
  "26": ["25"]
}
```

## Notes

- Compose is the first platform migration; ECS follows and includes all accepted production-readiness
  work; EKS follows without broadening or prematurely completing its sibling specification.
- `platforms/local` remains outside this migration. Its catalog-selected legacy launch class is the only
  transitional exception and does not authorize edits to Local source.
- This work implements only `DefinitionFormatId = "tkd"` through `TkdFrontend`. It deliberately adds no
  Monty dependency, `.tkdp` descriptor, Python facade, or `.tkdp` completion claim. The neutral Authoring
  Contract and independent frontend assembly are the preparation for that separately specified work.
- Platform packages own their complete service and artifact content. Provider crates own canonical kinds,
  resource implementations, delivery, live description, and operational transport. The framework owns
  only common authoring, graph, verification, projection, selection, identity, and publication mechanics.
- Property-test tasks are mandatory implementation work. A platform slice is not complete until its
  properties, integrations, source-boundary checks, and applicable sibling-spec evidence are green.
- Do not mark a sibling task or readiness claim complete merely because its code moved. Amend status only
  after the accepted behavior and evidence exist under the new owner.
