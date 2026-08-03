# Implementation Plan: Minimal Platform Boundary

Compose is the proof platform. Work proceeds from correctness through the reduced seam,
then removes unused framework surface, migrates Compose, integrates `tkr`, and reconciles
the specification. ECS, EKS, and Local platform crates are not changed.

The checkpoint for every reviewable commit is:

```bash
cargo +nightly fmt --all
cargo lint --locked
cargo check --workspace --locked
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

- [x] 1. Correct provider-kind validation and content identity

  - [x] 1.1 Split `ProviderKind` input validation from invocation-bound realization.
  - [x] 1.2 Make definition verification borrow the exact evaluated definition and call
    only `validate_input` over its exact resource set.
  - [x] 1.3 Remove fabricated definition-check deployment ids, paths, and tags.
  - [x] 1.4 Realize the verified set once with real identity, logical placement,
    dependency ids, dependency content identities, and tags.
  - [x] 1.5 Type the `ConfigurationIdentity` algorithm while preserving its exact
    serialized algorithm and digest shape.
  - [x] 1.6 Put reusable AWS kinds in one file per kind under
    `crates/tokeira-aws/src/kinds/`.
  - [x] 1.7 Run the complete commit checkpoint.

  **DONE:** commit `e8f222ea` (`fix(platform): validate kinds before realization`),
  checkpoint green.

- [x] 2. Collapse the definition frontend seam

  - [x] 2.1 Replace the session/object protocol with `DefinitionFrontend::evaluate` and
    one `FrontendOutput { config, graph }` result.
  - [x] 2.2 Replace `AuthorNode` with `LocatedValue` and retain only admitted Serde
    shapes plus source ranges.
  - [x] 2.3 Retain one owned definition source and one borrowed frontend source.
  - [x] 2.4 Move `RelativeDefinitionPath` beside the other definition identity types in
    `tokeira-orchestrator`.
  - [x] 2.5 Keep TKD handles and the name-to-operation table private to the evaluator;
    build and finish the structural graph there.
  - [x] 2.6 Delete public handle, token, schema, receiver, associated-function, and
    mirrored argument/result machinery.
  - [x] 2.7 Prove typed context evaluation produces an immediately admitted config and
    completed graph.
  - [x] 2.8 Run the complete commit checkpoint.

  **DONE:** commit `a810e89e` (`refactor(platform): collapse the definition frontend seam`),
  checkpoint green.

- [x] 3. Prune the shared platform framework

  - [x] 3.1 Delete shared config/context contracts, string-dispatched platform context,
    and the platform binding aggregate.
  - [x] 3.2 Delete shared provider execution/set/state policy/binding machinery.
  - [x] 3.3 Delete framework deployment/config, service/image catalogs, workload,
    desired/canonical document, and delivery abstractions.
  - [x] 3.4 Delete operational artifact catalogs/receipts, inspection registries,
    provider operations, session plans, buffered logs, no-change helper, and shared
    weak/strong graph handles.
  - [x] 3.5 Retain only graph/source/diagnostic/config/context/kind/content/inspection
    utilities with concrete consumers.
  - [x] 3.6 Move module-selection closure to `tokeira-iac`.
  - [x] 3.7 Keep state-store lifecycle and selection on the existing orchestration seam.
  - [x] 3.8 Confirm non-test `crates/tokeira-platform/src` remains below 2,500 lines.
  - [x] 3.9 Run the complete commit checkpoint.

  **DONE:** commit `09d35c64` (`refactor(platform): prune the shared framework`),
  checkpoint green. The resulting shared crate is approximately 2,118 non-test lines.

- [x] 4. Migrate Compose as the proof platform

  - [x] 4.1 Reduce the source layout to `config.rs`, `context.rs`, `ops.rs`, `lib.rs`,
    and platform-owned `services`, `images`, and `observability` modules.
  - [x] 4.2 Delete the Compose builder, bridge, adapter, interpreter, provisioner module,
    platform binary, and embedded default-definition constant.
  - [x] 4.3 Own Compose config validation, typed context, closed kind set, services,
    images, observability, operations, and inspection rendering in the platform crate.
  - [x] 4.4 Wire the transient graph through concrete IaC modules and direct provider
    resource calls.
  - [x] 4.5 Preserve rendered configuration as an ordinary resource; retain every
    consumer dependency and desired-state content digest.
  - [x] 4.6 Separate the provider-private desired manifest ledger from deterministic
    operator-facing `docker-compose.yml` bytes.
  - [x] 4.7 Stream logs and forward follow/tail behavior to Docker; keep port mappings
    concrete.
  - [x] 4.8 Prove pure definition checking, storage-mode graph parity, content coupling,
    and deterministic non-authoritative inspection without Docker.
  - [x] 4.9 Run the complete commit checkpoint.

  **DONE:** commit `bf3d3fd1` (`refactor(compose): migrate onto reduced platform boundary`),
  checkpoint green.

- [x] 5. Resolve and build Compose through the `tkr` catalog

  - [x] 5.1 Consolidate workspace and published discovery into one normalized
    `PlatformCatalog` descriptor model.
  - [x] 5.2 Remove launch class from permanent descriptors and keep current in-process
    Local/ECS routing isolated in `apps/tkr/src/legacy.rs`.
  - [x] 5.3 Generate one static root with exactly the provisioner shell, selected
    platform, and selected frontend as direct dependencies.
  - [x] 5.4 Normalize and admit the exact Cargo lock closure; include exact generated
    root, lock, source closure, selected ids, and contracts in evidence.
  - [x] 5.5 Reduce `BoundPlatform` to identity/evidence admission and concrete provisioner
    forwarding.
  - [x] 5.6 Remove the direct Compose dependency, embedded seed module, seed package,
    direct platform binary fallback, and definition-file routing heuristic from `tkr`.
  - [x] 5.7 Record platform, format, and relative definition path in `metadata.json`.
  - [x] 5.8 Stage the seed, build and marry `tkp`, run the exact staged binary's
    definition check, and publish the deployment transaction atomically.
  - [x] 5.9 Route existing catalog-backed deployments by recorded definition metadata.
  - [x] 5.10 Prove generated Compose create/check and creation rollback behavior.
  - [x] 5.11 Run the complete commit checkpoint.

  **DONE:** commit `629c29de` (`refactor(tkr): resolve compose through the platform catalog`),
  checkpoint green.

- [x] 6. Reconcile the specification and executable property ledger

  - [x] 6.1 Replace requirements with the current reduced contract and Compose proof
    scope only.
  - [x] 6.2 Replace design text with the surviving source, graph, kind, identity,
    inspection, discovery, assembly, provenance, and Compose components.
  - [x] 6.3 State explicitly that `FrontendOutput` is transient, in-memory,
    non-serialized, and not desired-state authority.
  - [x] 6.4 Retain only live Properties 2, 5, 7, 8, 9, 11, 16, 17, 22, and 23.
  - [x] 6.5 Add direct IaC tests for prerequisite/dependant module-selection closure and
    empty/unknown rejection (Property 9).
  - [x] 6.6 Prove verification performs no invocation-bound work and successful
    execution realizes every verified resource exactly once (Property 8).
  - [x] 6.7 Attach property tags to the retained evidence and keep generic
    provider-issue rendering outside this feature's property set.
  - [x] 6.8 Verify the named removed types, files, constants, and routing heuristics are
    absent and confirm ECS, EKS, and Local platform crates have no diff.
  - [x] 6.9 Run the complete commit checkpoint, then after the final Property 8
    evidence addition rerun format, lint, workspace check, and the focused
    `tokeira-platform` suite.

  **DONE:** requirements, design, task ledger, and property evidence describe the final
  tree. The complete bar was green immediately before the final Property 8 proof; its
  post-proof format, lint, workspace check, and focused platform suite are green.

## Live property evidence

- [x] Property 2 — structural graph completion is exact.
- [x] Property 5 — config admission is pure Serde admission.
- [x] Property 7 — configuration identity is byte stable.
- [x] Property 8 — verification is pure and execution uses the verified set.
- [x] Property 9 — module selection is the required directional closure.
- [x] Property 11 — content coupling is deterministic and sensitive.
- [x] Property 16 — Compose storage modes preserve graph parity.
- [x] Property 17 — Compose inspection is deterministic and non-authoritative.
- [x] Property 22 — catalog selection determines one static generated root.
- [x] Property 23 — deployment publication is all-or-nothing.

## Final checkpoint

- [x] Every reviewable commit follows the required ordering.
- [x] Every surviving shared abstraction has a concrete consumer.
- [x] Compose creates, checks, plans, applies, destroys, streams logs, and resolves port
  mappings through the generated bound provisioner.
- [x] The complete §10.4 bar is green for the implementation tree; the final isolated
  Property 8 proof is covered by post-change format, lint, workspace check, and focused
  platform tests.
