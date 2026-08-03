# Design: Minimal Platform Boundary

## Overview

The platform boundary carries authored structure from one selected definition frontend
to one concrete provisioner. It does not model a platform's services, images,
operations, delivery, state, or inspection content.

Compose proves the boundary end to end:

```text
Cargo metadata
    -> PlatformCatalog selects Compose + TKD
    -> generated Cargo root binds shell + Compose + TKD
    -> deployment-time tkp evaluates definition.tkd
    -> LocatedValue config is admitted as ComposeConfig
    -> VerifiedGraph<ComposeKind> is input-validated
    -> exact graph is realized with deployment identity
    -> concrete Compose resources enter the existing IaC/orchestration engines
    -> provider state remains authoritative
    -> docker-compose.yml is an optional deterministic inspection projection
```

The frontend result is a transient Rust value. It is neither a serialized composition
IR nor persisted desired state. Provider and orchestration state remain the authority
used by plan, apply, and destroy.

## Design goals

- Keep only language-neutral authoring structure in `tokeira-platform`.
- Make config validation pure and provider-kind realization invocation-bound.
- Preserve declaration order and reject incomplete or ambiguous graphs.
- Let a platform own every service, image, operation, manifest, and inspection byte it
  produces.
- Discover platform/frontend packages from Cargo metadata and assemble one static
  provisioner without permanent platform dispatch.
- Preserve exact build, source, lock, platform, format, and definition provenance.

## Scope boundary

### `tokeira-platform` owns

- `LocatedValue`, its supported Serde shapes, source ranges, and deserializer;
- owned and borrowed definition source types;
- frontend and framework diagnostics;
- the transient structural graph and its validation;
- pure config admission;
- `ProviderKind`, `KindFunctions`, and `PlacementContext`;
- invocation-bound realization of the exact verified resource set;
- `ConfigurationIdentity` and `ContentIdentity`;
- the three universal invocation facts; and
- safe atomic publication of platform-rendered inspection bytes.

### A concrete platform owns

- its Serde config and pure semantic validation;
- its typed runtime context;
- its closed provider-kind set;
- its infrastructure resources and modules;
- services, images, and observability resources;
- provider client construction and calls;
- logs, port mappings, and other operations;
- its provider-facing desired manifests; and
- deterministic operator-facing inspection rendering.

### Existing engines retain

- `tokeira-orchestrator`: infrastructure/deployment coordination, state-store lifecycle,
  and the store-selection seam on `Deployment`;
- `tokeira-iac`: planning, resource state, module-selection closure, resource execution,
  and writeback file updates;
- `tokeira-deploy-engine`: runtime service and image execution; and
- `tokeira-state`: concrete state-store implementations.

`tokeira-platform` contains no parallel versions of these responsibilities.

## Components and interfaces

### 1. Source identities and safe relative paths

`tokeira-orchestrator` owns the inventory-free identifiers and deployment-relative path
vocabulary:

```rust
PlatformId
DefinitionFormatId
DefinitionSourceExtension
RelativeDefinitionPath
```

`PlatformId` and `DefinitionFormatId` validate canonical open identifiers without
encoding a known inventory. `RelativeDefinitionPath` accepts only portable UTF-8 normal
components below a deployment root. The same admitted path type is used for recorded
definition paths and inspection targets.

`tokeira-platform::definition` adds source presentation:

```rust
enum DefinitionSourceName {
    DeploymentRelative(RelativeDefinitionPath),
    AuthoringPath(PathBuf),
}

struct DefinitionSource {
    format: DefinitionFormatId,
    source_name: DefinitionSourceName,
    bytes: Arc<[u8]>,
}

struct FrontendSource<'a> {
    source_name: &'a DefinitionSourceName,
    bytes: &'a [u8],
}
```

There is one owned source and one borrow. Source paths are diagnostic/persistence
identity; exact source bytes are evaluation and configuration-identity input.

### 2. Located config values

`LocatedValue` contains `ValueShape` and an optional `SourceRange`. `ValueShape` mirrors
only the Serde shapes needed by admitted platform configs:

```text
unit | bool | i128 | f64 | string | sequence | option | ordered map
named struct | named enum(unit or newtype variant)
```

The tree contains no frontend handles or context tokens. Its deserializer translates
directly into a platform-owned `Deserialize` type. Decode errors retain the most
specific range encountered.

`admit_config` performs exactly two steps:

1. deserialize the `LocatedValue` into the platform config;
2. run `fn(&Config) -> Result<(), ConfigError>`.

Both steps are pure.

### 3. Definition frontend

The complete public seam is:

```rust
trait DefinitionFrontend {
    fn format(&self) -> &DefinitionFormatId;

    fn evaluate<C, K>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        kinds: KindFunctions<K>,
    ) -> Result<FrontendOutput<K>, FrontendDiagnostic>
    where
        C: Serialize,
        K: ProviderKind + 'static;
}

struct FrontendOutput<K> {
    config: LocatedValue,
    graph: VerifiedGraph<K>,
}
```

The context is a concrete platform type. The frontend may serialize it into its private
evaluator representation, but the shared boundary provides no field or method dispatch.

The TKD implementation owns its evaluator values, handles, and name-to-operation table.
Calls such as module creation, resource construction, dependency declaration, output
selection, and writeback mutate a private `StructuralGraphBuilder`. The builder must
finish successfully before `FrontendOutput` can be returned. No evaluator runtime state
crosses the seam.

`FrontendDiagnostic` is the sole frontend failure type and includes the selected
format, source name, message, and optional source range.

### 4. Structural graph

`StructuralGraphBuilder<K>` records:

- namespaces in declaration order;
- modules and ordered module dependencies;
- concrete `K` resource declarations and ordered resource dependencies; and
- ordered writeback declarations containing literals or checked output references.

`finish` validates all findings together:

- namespace, module, logical resource, and writeback-key uniqueness;
- known module dependency targets;
- module dependencies declared before their consumers;
- known resource-owning modules;
- known resource dependency targets;
- acyclic module and resource graphs; and
- output names declared by the referenced `ProviderKind`.

Success produces `VerifiedGraph<K>`. The graph has no serialization contract and no
stable storage schema.

### 5. Provider-kind admission and realization

The provider-kind seam is intentionally concrete:

```rust
trait ProviderKind {
    fn kind_name(&self) -> &'static str;
    fn validate_input(&self) -> Result<(), KindError>;
    fn declared_outputs(&self) -> &'static [&'static str];
    fn desired_manifest(&self, placement: &PlacementContext) -> Value;
    fn realize(&self, placement: &PlacementContext)
        -> Result<Box<dyn tokeira_iac::Resource>, KindError>;
}
```

`KindFunctions<K>` is three compile-time functions: name membership, defaults, and
typed decode. The concrete platform chooses `K`; there is no dynamically populated
registry.

Verification borrows an `EvaluatedDefinition` and calls only `validate_input` for every
resource. `VerifiedDefinition` therefore identifies the exact graph that passed pure
validation without copying or replacing its resource set.

Execution calls `VerifiedDefinition::realize` once. Dependencies are realized in
topological order while the returned resource vector retains declaration order. Each
kind receives:

```rust
struct PlacementContext {
    deployment_id: String,
    deployment_dir: PathBuf,
    module: String,
    logical_id: String,
    dependencies: Vec<ResourceId>,
    dependency_content: BTreeMap<ResourceId, ContentIdentity>,
    tags: BTreeMap<String, String>,
}
```

The realization returns resources, a logical-to-physical identity index, and
provider-owned desired manifests for the same exact graph.

### 6. Identities

`ConfigurationIdentity` hashes the selected format and exact source bytes using a
domain-separated, length-framed SHA-256 input. Its internal algorithm is an enum. Its
serialized representation remains:

```json
{"algorithm":"sha256-v1","digest":"<lowercase sha256>"}
```

`ContentIdentity` hashes explicit non-secret bytes with an explicit domain. Provider
resource manifests use it to couple consumers to content resources without a shared
artifact system.

### 7. Invocation context

The universal context is deliberately closed:

```rust
struct InvocationContext {
    deployment_id: String,
    deployment_uuid: Uuid,
    deployment_dir: PathBuf,
}
```

The shell derives these fields from deployment metadata and the admitted deployment
root. Compose maps them into `platforms/compose/src/context.rs::Context`. Provider or
environment facts are added only by concrete platform code that owns them.

### 8. Inspection publication

The platform renders deterministic bytes. The shared utility accepts those bytes and a
`RelativeDefinitionPath`, rejects escaping/symlinked parents, writes a same-directory
temporary file, flushes and syncs it, and atomically renames it over the target. It
returns the path and exact `ContentIdentity` of the published bytes.

The utility has no renderer registry, artifact catalog, receipt store, or delivery
model. Publication does not make the file authoritative.

### 9. Reusable AWS kinds

Reusable AWS author inputs live beside AWS provider resources:

```text
crates/tokeira-aws/src/kinds/
  mod.rs
  dsql_cluster.rs
  dynamodb_table.rs
```

Each file contains one typed mapping. A platform includes supported mappings in its own
closed kind enum; unsupported kinds are absent at compile time.

## Compose implementation

### Source layout

```text
platforms/compose/src/
  config.rs
  context.rs
  images/mod.rs
  lib.rs
  observability/mod.rs
  ops.rs
  services/mod.rs
```

- `config.rs` owns `ComposeConfig`, storage variants, and pure validation.
- `context.rs` owns the typed authoring/runtime context derived from
  `InvocationContext`.
- `services/mod.rs` owns the Compose kind enum, Docker service resources, rendered
  configuration resource, provider-facing manifests, and `docker-compose.yml`
  projection.
- `images/mod.rs` owns Compose image behavior.
- `observability/mod.rs` owns platform observability resources and rendered config.
- `ops.rs` owns Docker-backed log streaming and port mappings.
- `lib.rs` binds evaluation, verification, realization, existing orchestration engines,
  state-store selection, plan/apply/destroy, writeback, and inspection publication.

The selected frontend descriptor names the default relative source path, and the Compose
package supplies its seed at that path. The generated root calls the conventional
`provisioner(frontend)` library export. The platform crate owns no binary entrypoint.

### Content coupling

Rendered observability configuration is an ordinary resource. A consuming service:

1. depends on that resource's `ResourceId`;
2. receives its `ContentIdentity` through `PlacementContext::dependency_content`; and
3. embeds the prefixed digest in its desired environment.

Changing the rendered config therefore changes the consumer desired manifest and is
visible to normal IaC diffing. There is no side channel or artifact receipt.

### Provider ledger and inspection projection

Execution owns a private map from `ResourceId` to provider desired manifest. It is the
input used for concrete resource diffing. `services::inspection_bytes` deterministically
projects its Compose service subset into operator-facing YAML. The projection is not
read during evaluation, planning, or execution.

### Module selection

Compose converts its realized module nodes to concrete `tokeira-iac::Module` values and
calls `tokeira_iac::expand_module_selection`:

- plan/apply expand an explicit selection over transitive prerequisites;
- destroy expands over transitive dependants; and
- empty or unknown explicit selections fail before engine execution.

This graph closure belongs to IaC and is merely wired by Compose.

### Operations

`ProvisionerPlatform::log_stream` delegates to `ops.rs`, which passes follow/tail
arguments to Docker and returns an asynchronous stream. Port mapping delegates to the
same concrete operations module. No platform-wide operation enum or JSON operation
round-trip is involved.

## Discovery and static assembly

### Cargo descriptor discovery

`tokeira-build` reads Cargo metadata for platform and frontend descriptor tables. A
platform descriptor records its open identity, default status, private binding-contract
version, and package coordinates. A frontend descriptor records its open format
identity, source extension, default relative source path, frontend-contract version, and
package coordinates. Catalog resolution confirms that the selected platform package
supplies a seed at the selected frontend's default path.

Discovery rejects:

- duplicate platform or format identities;
- descriptor packages without conventional library targets;
- descriptor packages that own binary targets;
- mismatched package/source paths; and
- contract-version mismatches.

### Normalized catalog

`apps/tkr/src/catalog.rs::PlatformCatalog` normalizes either workspace descriptors or
authority-admitted published locators into the same `PlatformDescriptor` and
`FrontendDescriptor` model. Resolution selects identities independently and then proves
that the selected source family contains the requested pair.

There is no launch-class field. The current in-process Local/ECS route is isolated in
`apps/tkr/src/legacy.rs` and does not affect descriptor admission.

### Generated composition root

For a workspace pair, `assemble_bound_provisioner` creates a disposable Cargo package
whose only direct dependencies are:

1. `tokeira-provisioner-cli`;
2. the selected platform library; and
3. the selected frontend library.

Its generated `main.rs` invokes `bound_provisioner_main!` with the selected ids and
conventional exports. Cargo metadata normalizes the lock closure offline. Admission
rejects external packages outside the selected workspace closure. Native and hermetic
builders materialize the exact normalized lock and build with `--locked`.

`BoundProvisionerSource` owns the exact generated manifest, main source, lockfile,
closure, contract versions, and selected ids. It derives one `BoundProvisionerEvidence`
covering generated-root, source-closure, and lock-closure digests. Bundle admission and
deployment metadata retain that evidence.

### Thin bound platform

`BoundPlatform<P>` checks the compile-time expected platform id and admitted bundle
evidence, then forwards the `ProvisionerPlatform` contract to `P`. It does not model
services, images, operations, or platform configuration.

## `tkr` lifecycle

### Creation

For a catalog-backed Compose deployment, `tkr`:

1. resolves the selected platform and format;
2. creates a hidden staging directory;
3. copies the platform-owned seed to its catalog-declared relative path;
4. writes `metadata.json` with platform, format, relative path, identity, and binding
   evidence;
5. snapshots the admitted source closure;
6. generates and builds the bound provisioner;
7. marries the binary, manifest, and evidence into the staged deployment;
8. runs that exact binary's `definition check`; and
9. atomically publishes the deployment directory and latest selection.

Failure before publication removes the staging directory and leaves the public
deployment name and latest-selection metadata unchanged.

### Routing

Recorded definition metadata is the routing fact. Presence or absence of a file with a
known extension is not a routing signal. Catalog-backed operations execute the married
binary and forward its exit status and streams. Standalone checking constructs a bound
root from explicit platform and format ids.

## Error handling

- Identifier, source-extension, and relative-path failures are admission errors.
- Frontend parse/evaluation failures are `FrontendDiagnostic` values.
- Config decode and semantic validation failures are `ConfigError` values with ranges
  where available.
- Graph completion accumulates deterministic `GraphFinding` values.
- Kind input validation accumulates `VerificationFinding::InvalidInput` values.
- Realization identifies the logical resource and provider kind that failed.
- Catalog and composition failures name the selected identity, package, or closure fact
  that could not be admitted.
- Inspection failures distinguish target preparation, staging, escape, and publication.

## Correctness properties

### Property 2: Structural graph completion is exact

For any sequence of graph declarations, `finish` succeeds exactly when names are
unique, every dependency and output target is known, module dependencies point
backward in declaration order, and both dependency graphs are acyclic. On success, all
declaration order is preserved.

**Evidence:** `crates/tokeira-platform/src/tests.rs` graph completion tests.

### Property 5: Config admission is pure Serde admission

For every admitted config-shaped `LocatedValue`, decoding followed by the platform's
validator is the only config admission work. Unknown or invalid input is rejected with
its nearest range and no provider or state effects occur.

**Evidence:** `located_config_admission_is_serde_backed_and_pure`, Compose config tests,
and `definition_check_is_provider_and_state_free`.

### Property 7: Configuration identity is byte stable

For equal format and source bytes, configuration identity and serialized bytes are
equal. Path, context, and state do not affect the result. The algorithm label is always
`sha256-v1`.

**Evidence:** `configuration_identity_serialization_remains_byte_stable` and
configuration-identity unit tests.

### Property 8: Verification is pure and execution uses the verified set

For every evaluated definition, verification invokes `validate_input` for every and
only declared resource. It performs no realization. A successful execution realizes
every and only those verified declarations once using real invocation placement.

**Evidence:** `verification_is_pure_and_realization_uses_the_exact_verified_set_once`,
`invalid_kind_input_never_reaches_invocation_bound_work`, and
`definition_check_is_provider_and_state_free`.

### Property 9: Module selection is the required closure

For every valid module DAG and explicit selection, prerequisite expansion returns
exactly the transitive prerequisite closure in declaration order, while dependant
expansion returns exactly the transitive dependant closure. Empty and unknown explicit
selections fail.

**Evidence:** `tokeira-iac` module-selection closure tests and Compose plan/destroy
wiring.

### Property 11: Content coupling is deterministic and sensitive

For equal domain and content, `ContentIdentity` is equal. Changing the domain or content
changes the identity. Every Compose service that consumes rendered configuration both
depends on its resource and embeds its digest; changing the config changes consumer
desired state.

**Evidence:** `content_identity_is_deterministic_and_domain_separated` and
`configuration_content_is_coupled_to_every_consumer`.

### Property 16: Compose storage modes preserve graph parity

For in-memory storage, the graph omits DSQL and retains local state, runtime, and
observability. For managed or preexisting DSQL, it additionally contains DSQL while
preserving the same Compose service resource set.

**Evidence:** `storage_modes_preserve_the_reference_graph_shape`.

### Property 17: Compose inspection is deterministic and non-authoritative

For one provider desired-manifest set, repeated rendering produces identical bytes.
Publishing or editing `docker-compose.yml` does not alter reevaluation or desired
manifests, and the projection is never persisted as provider state.

**Evidence:** `inspection_projection_is_deterministic_and_non_authoritative` and
`inspection_publication_is_atomic_and_uses_definition_path_admission`.

### Property 22: Catalog selection determines one static root

For one admitted platform, frontend, and source closure, catalog resolution and assembly
produce one deterministic root with exactly three direct dependencies. Changing the
platform, format, contracts, generated bytes, lock bytes, or source snapshot rekeys the
corresponding evidence.

**Evidence:** `tokeira-build` descriptor and generated-assembly property tests plus
`apps/tkr/src/catalog.rs` loader/resolution tests.

### Property 23: Deployment publication is all-or-nothing

For any failure before the staged bound provisioner passes definition checking, the
public deployment directory and latest-selection metadata remain unchanged. Success
publishes the complete married deployment exactly once.

**Evidence:** `creation_transaction_hides_staging_and_rolls_back_latest_failure` and
`catalog_selection_creates_and_checks_with_the_generated_compose_provisioner`.

## Verification strategy

### Focused suites

- `cargo test -p tokeira-platform --locked`
- `cargo test -p tokeira-iac --locked`
- `cargo test -p tokeira-tkd --locked`
- `cargo test -p tokeira-build --locked`
- `cargo test -p tokeira-compose-deployment --locked`
- `cargo test -p tkr --locked`

The Compose proof tests do not require Docker. Docker-backed operations remain concrete
integration behavior exercised only when explicitly invoked.

### Commit checkpoint

Every reviewable commit runs:

```bash
cargo +nightly fmt --all
cargo lint --locked
cargo check --workspace --locked
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```
