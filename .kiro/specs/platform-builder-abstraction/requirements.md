# Requirements: Minimal Platform Boundary

## 1. Scope and ownership

This specification defines the language-neutral boundary used to evaluate a deployment
definition and hand a checked structural graph to one concrete platform. Docker Compose
is the proof platform. The ECS, EKS, and Local platform implementations are outside this
specification.

### Requirement 1: Keep the shared platform surface minimal

**User story:** As a platform author, I want a small shared contract so that the
platform owns its actual services, artifacts, provider calls, and operations.

#### Acceptance criteria

1. WHEN a definition is evaluated, THE `tokeira-platform` crate SHALL provide only:
   located author values, definition source types, diagnostics, typed config admission,
   the structural graph and its validation, provider-kind validation and placement,
   configuration/content identities, the universal invocation context, and safe
   inspection publication.
2. THE `tokeira-platform` crate SHALL NOT define shared service, image, delivery,
   operation, state-store, or inspection-renderer catalogs.
3. THE concrete platform SHALL own its config type, typed platform context, services,
   images, observability resources, operational behavior, provider calls, and
   inspection bytes.
4. THE orchestration layer SHALL retain the state-store lifecycle and construction
   seam. A concrete platform MAY select a store through the existing
   `tokeira-orchestrator::Deployment` contract, but `tokeira-platform` SHALL NOT add a
   state abstraction.
5. THE non-test Rust source under `crates/tokeira-platform/src` SHALL remain below
   2,500 lines.
6. WHEN a shared abstraction has no concrete Compose consumer, THE implementation
   SHALL omit it.

## 2. Definition frontend

### Requirement 2: Evaluate through one completed structural result

**User story:** As a definition-frontend author, I want one language-neutral result so
that evaluator internals do not become a platform protocol.

#### Acceptance criteria

1. WHEN a frontend evaluates source, THE `DefinitionFrontend` contract SHALL accept a
   typed platform context, a borrowed definition source, and the platform's compile-time
   kind functions.
2. WHEN evaluation succeeds, THE frontend SHALL return exactly one `FrontendOutput`
   containing a `LocatedValue` config and a completed `VerifiedGraph`.
3. THE returned structure SHALL exist only transiently in memory, SHALL NOT implement a
   persistence format, SHALL NOT be serialized, and SHALL NOT become a second
   desired-state authority.
4. WHEN evaluation or structural completion fails, THE frontend SHALL return a
   `FrontendDiagnostic` with its source identity and available source range.
5. THE shared boundary SHALL expose one owned `DefinitionSource` and one borrowed
   `FrontendSource` view, without additional owned/borrowed wrapper layers.
6. THE shared boundary SHALL expose `DefinitionSourceName` values for deployment-relative
   sources and standalone authoring paths.
7. THE TKD frontend SHALL keep evaluator handles and its name-to-operation table private
   to `tokeira-tkd` and SHALL build the structural graph inside that evaluator.
8. THE platform and `tokeira-platform` SHALL NOT receive an evaluator handle protocol,
   handle tokens, token interning, runtime schemas, or mirrored evaluator arguments and
   results.

### Requirement 3: Admit only Serde-shaped located values

**User story:** As a platform author, I want config decoding to use ordinary Serde
semantics while retaining useful source locations.

#### Acceptance criteria

1. THE `LocatedValue` tree SHALL carry an optional `SourceRange` at each node.
2. THE value tree SHALL admit unit, Boolean, signed integer, floating-point, string,
   sequence, option, ordered map, named struct, unit variant, and newtype variant shapes.
3. THE value tree SHALL NOT contain context-token or evaluator-handle variants.
4. WHEN a `LocatedValue` is decoded, THE decoder SHALL use the Serde data model and
   SHALL attach the most specific available source range to a decoding error.
5. WHEN a platform config is admitted, THE framework SHALL decode it into the
   platform-owned Serde struct and run the platform's pure validation function.
6. WHEN config decoding or validation fails, THE framework SHALL report the failure
   without invoking providers, reading state, or fabricating invocation facts.

## 3. Structure, identity, and invocation

### Requirement 4: Validate the complete structural graph

**User story:** As an operator, I want malformed deployment structure rejected before
provider work so that execution receives one coherent graph.

#### Acceptance criteria

1. THE graph builder SHALL preserve namespace, module, resource, dependency, and
   writeback declaration order.
2. WHEN graph completion finds duplicate namespaces, modules, resources, or writeback
   keys, THE graph builder SHALL reject the graph.
3. WHEN a module dependency names an unknown module or a module declared later, THE
   graph builder SHALL reject the graph.
4. WHEN a resource names an unknown module, unknown resource dependency, or undeclared
   provider output, THE graph builder SHALL reject the graph.
5. WHEN module or resource dependencies are cyclic, THE graph builder SHALL reject the
   graph.
6. WHEN all invariants hold, THE graph builder SHALL return an immutable
   `VerifiedGraph` containing the exact declared nodes and ordering.
7. THE completed graph SHALL remain transient and in memory; persisted desired state
   SHALL remain the provider/orchestration state already used for execution.

### Requirement 5: Preserve stable definition and content identities

**User story:** As an operator, I want identities to be deterministic and stable so
that provenance and desired-state comparisons remain trustworthy.

#### Acceptance criteria

1. WHEN `ConfigurationIdentity` is computed, THE digest SHALL depend only on the
   admitted definition format and exact source bytes.
2. THE configuration identity SHALL be independent of source path, deployment state,
   and runtime context.
3. THE internal algorithm representation SHALL be typed.
4. WHEN serialized as JSON, `ConfigurationIdentity` SHALL retain the byte-stable shape
   `{"algorithm":"sha256-v1","digest":"<lowercase sha256>"}`.
5. WHEN `ContentIdentity` is computed, THE result SHALL be deterministic and domain
   separated.
6. THE framework SHALL use content identities as comparison evidence and SHALL NOT
   treat them as a place to store secret content.

### Requirement 6: Keep universal and platform context separate

**User story:** As a platform author, I want typed invocation data so that platform
facts are explicit and cannot be discovered through string dispatch.

#### Acceptance criteria

1. THE universal `InvocationContext` SHALL contain exactly `deployment_id`,
   `deployment_uuid`, and `deployment_dir`.
2. THE shell SHALL construct the universal invocation context from admitted deployment
   metadata and the selected deployment directory.
3. THE concrete platform SHALL construct its typed context from the universal facts and
   any platform-owned runtime facts.
4. Environment, region, account, credentials, provider clients, and other provider facts
   SHALL remain owned by the concrete platform or provider crate that uses them.
5. THE shared boundary SHALL NOT provide string-dispatched context fields or methods.

## 4. Provider kinds and execution

### Requirement 7: Separate kind validation from realization

**User story:** As an operator, I want `definition check` to be pure and execution to
use real invocation identity.

#### Acceptance criteria

1. THE `ProviderKind` contract SHALL expose pure input validation separately from
   invocation-bound realization.
2. WHEN `definition check` runs, THE provisioner SHALL evaluate the definition, admit
   config, validate the graph, and call `validate_input` for every declared kind.
3. `definition check` SHALL NOT call `desired_manifest` or `realize`, create state
   directories, contact providers, or invent deployment identity, paths, or tags.
4. WHEN execution begins, THE verified definition SHALL be realized exactly once with
   the real deployment identity, deployment directory, logical placement, dependency
   resource identities, dependency content identities, and platform/provider tags.
5. THE resource set realized for execution SHALL be exactly the resource set validated
   by `definition check`; neither stage SHALL add or omit a declaration.
6. THE graph SHALL accept only output names declared by the concrete provider kind.
7. THE platform SHALL supply kinds as a closed compile-time first-party set without a
   string-keyed plugin registry or type erasure.

### Requirement 8: Keep reusable provider kinds beside provider resources

**User story:** As a provider maintainer, I want authored resource kinds beside the
provider implementation so that mappings are reusable without becoming platform code.

#### Acceptance criteria

1. THE `tokeira-aws` crate SHALL expose reusable kinds from `src/kinds/`.
2. EACH AWS kind SHALL occupy one file named for the resource it describes, including
   `dsql_cluster.rs` and `dynamodb_table.rs`.
3. AWS kind modules SHALL map typed author input to the corresponding provider-owned
   resource semantics.
4. A platform SHALL compose the provider kinds it supports into its own compile-time
   kind enum and constructor table.

### Requirement 9: Preserve content coupling as ordinary resources

**User story:** As an operator, I want a service plan to change when consumed rendered
configuration changes.

#### Acceptance criteria

1. WHEN a platform renders configuration consumed by services, THE rendered
   configuration SHALL be represented as an ordinary provider resource.
2. EACH consuming service SHALL retain a resource dependency on that configuration
   resource.
3. EACH consuming service's desired manifest SHALL carry the configuration resource's
   content digest.
4. WHEN rendered configuration content changes, THE content digest in every consuming
   service desired state SHALL change deterministically.
5. THE implementation SHALL preserve this coupling without a shared artifact catalog,
   artifact receipt, delivery projection, or delivery key abstraction.

## 5. Compose proof platform

### Requirement 10: Own the Compose implementation in the Compose crate

**User story:** As a Compose platform maintainer, I want the implementation to read as
concrete Compose code rather than adapters around a generic platform framework.

#### Acceptance criteria

1. THE `platforms/compose/src` tree SHALL consist of `config.rs`, `context.rs`, `ops.rs`,
   `lib.rs`, and platform-owned `services`, `images`, and `observability` modules.
2. THE Compose crate SHALL own its config validation, typed context, kind set, service
   manifests, image behavior, observability resources, log stream, port mappings, and
   inspection rendering.
3. THE Compose crate SHALL call `tokeira-compose`, `tokeira-aws`, orchestration, IaC,
   deployment-engine, and state crates directly where their concrete contracts are
   needed.
4. THE Compose crate SHALL export a conventional `provisioner(frontend)` constructor
   for generated composition roots and SHALL NOT own a platform-specific `tkp` binary.
5. THE Compose crate SHALL own its default `definition.tkd` seed as a package source
   file at the selected frontend descriptor's default relative path, not as a Rust
   constant.
6. WHEN logs are requested, THE Compose implementation SHALL return an asynchronous
   stream and SHALL pass the requested follow and tail behavior to Docker.
7. WHEN port mappings are requested, THE Compose implementation SHALL resolve them
   through its concrete operations module.
8. WHEN Compose receives an explicit module selection for plan or apply, THE concrete
   path SHALL use `tokeira-iac` to expand the transitive prerequisite closure.
9. WHEN Compose receives an explicit module selection for destroy, THE concrete path
   SHALL use `tokeira-iac` to expand the transitive dependant closure.
10. WHEN an explicit module selection is empty or names an unknown module, THE IaC
    closure function SHALL reject it before engine execution.

### Requirement 11: Preserve Compose behavior without Docker in the proof suite

**User story:** As a reviewer, I want deterministic no-Docker evidence that the reduced
boundary preserves Compose behavior.

#### Acceptance criteria

1. FOR in-memory, managed DSQL, and preexisting DSQL config, THE evaluated Compose graph
   SHALL preserve the expected storage-mode module shape.
2. `definition check` SHALL succeed for the reference definition without creating
   provider or state artifacts.
3. THE Compose proof suite SHALL demonstrate configuration-resource dependencies and
   digest sensitivity for every consuming service.
4. THE provider-facing desired service ledger SHALL remain private to execution.
5. THE operator-facing `docker-compose.yml` SHALL be a deterministic projection of the
   provider-facing desired service manifests.
6. THE inspection projection SHALL be non-authoritative: editing it SHALL NOT change a
   subsequent evaluation, plan, or desired manifest.
7. WHEN published, inspection bytes SHALL use the shared safe-relative-path and atomic
   replacement utility.
8. THE storage-mode, content-coupling, definition-check, and inspection tests SHALL run
   without a Docker daemon.

## 6. Discovery, assembly, and operator integration

### Requirement 12: Discover open platform and frontend identities

**User story:** As an operator, I want selection derived from package descriptors so
that `tkr` does not require a platform enum branch for Compose.

#### Acceptance criteria

1. THE system SHALL represent platform and definition-format identities with validated,
   inventory-free `PlatformId` and `DefinitionFormatId` values.
2. THE system SHALL represent definition source extensions and deployment-relative
   definition paths in `tokeira-orchestrator`.
3. THE build layer SHALL discover platform and frontend descriptors from Cargo metadata.
4. THE descriptor model SHALL contain one normalized representation for workspace and
   published sources.
5. THE catalog SHALL provide workspace and published loaders over that normalized
   descriptor model.
6. WHEN a descriptor package owns a binary target, lacks its conventional library
   export, duplicates an identity, or violates its declared source path, THE discovery
   layer SHALL reject it.
7. A descriptor SHALL NOT encode a permanent platform launch class.

### Requirement 13: Assemble and evidence one bound provisioner

**User story:** As an operator, I want a deployment-time provisioner bound to exact
source and dependency evidence.

#### Acceptance criteria

1. WHEN a platform and frontend are selected, THE build layer SHALL generate one Cargo
   root with exactly the provisioner shell, selected platform, and selected frontend as
   direct dependencies.
2. THE generated root SHALL call the conventional platform and frontend library exports.
3. `BoundProvisionerSource` SHALL retain the generated manifest, generated main source,
   normalized lockfile, admitted source closure, and selected identities.
4. THE generated-root identity SHALL cover the binding contracts and exact generated
   manifest, main source, and lockfile bytes.
5. THE bundle evidence SHALL record the generated-root, source-closure, lock-closure,
   platform, format, and contract identities used for the deployment-time build.
6. THE normalized lockfile SHALL admit only external packages in the selected workspace
   closure and SHALL be materialized unchanged for `cargo build --locked`.
7. `BoundPlatform` SHALL remain a thin identity/evidence admission wrapper over the
   concrete provisioner implementation.

### Requirement 14: Create and route Compose deployments through the catalog

**User story:** As a `tkr` user, I want Compose selected and built from catalog evidence
so that the operator CLI contains no embedded Compose implementation.

#### Acceptance criteria

1. WHEN Compose is selected for deployment creation, `tkr` SHALL resolve the platform,
   definition format, and seed through the normalized catalog.
2. `tkr` SHALL NOT depend directly on the Compose platform crate or embed a Compose seed.
3. `tkr` SHALL stage the selected definition, record platform, format, and relative
   definition path in `metadata.json`, generate and build the bound provisioner, marry
   the exact binary and evidence into the staged deployment, run the staged provisioner's
   `definition check`, and then publish the deployment directory atomically.
4. WHEN any creation step fails, THE final deployment directory and latest-selection
   metadata SHALL remain unpublished.
5. WHEN an existing deployment has recorded definition metadata, `tkr` SHALL route
   provisioner operations to its married `tkp` binary based on metadata, not definition
   file presence.
6. Standalone definition checking SHALL require an explicitly selected open platform id
   and definition-format id and SHALL run through a generated bound provisioner.
7. Compose create, check, plan, apply, destroy, logs, and port-mapping operations SHALL
   execute through the generated bound provisioner.
8. THE Local and ECS in-process operator routes MAY remain isolated in `apps/tkr/src/legacy.rs`;
   those routes SHALL NOT determine the permanent descriptor or catalog model.

## 7. Verification

### Requirement 15: Keep implementation, properties, and ledger aligned

**User story:** As a reviewer, I want the specification and test evidence to describe
only the code in the tree.

#### Acceptance criteria

1. THE design SHALL define correctness properties only for the surviving surface.
2. EACH retained property SHALL identify its executable test evidence.
3. Tests for absent framework machinery SHALL be absent.
4. THE task ledger SHALL record the implementation order and completed checkpoints.
5. BEFORE each reviewable commit, THE implementation SHALL pass
   `cargo +nightly fmt --all`, `cargo lint --locked`,
   `cargo check --workspace --locked`, `cargo test --workspace --locked`, and
   `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
