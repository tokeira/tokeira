# Definition-backed platform provider contract

This checklist defines the minimum evidence expected from a Tokeira infrastructure and
service provider. It is derived from the framework contracts and the current Compose
implementation; it is intended to be reused for ECS, EKS, and later platforms.

The framework owns interpretation, planning, confirmation, state, ordering, writeback,
retarget admission, and publication. A provider supplies vocabulary and substrate
realization through those seams; it does not build a parallel lifecycle. The package
boundaries and IaC obligations are stated in the
[engineering reference](../agents/engineering-reference.md#iac-engine-contracts), and
the source/engine boundary is stated in the
[definition programming guide](../provisioning/deployment-definitions.md#the-language-and-engine-contract).

## Checklist

- [ ] **One pure platform declaration.** The package exports `platform() ->
  PlatformDeclaration`; constructing it performs no filesystem, provider-client, or
  network work. The declaration supplies its namespaces, optional live operations,
  reachability probe, and integration object. Evidence:
  [`PlatformDeclaration`](../../crates/tokeira-platform/src/declaration.rs) and
  [`platforms/compose::platform`](../../platforms/compose/src/lib.rs).
- [ ] **Closed, typed authoring vocabulary.** Every definition kind is admitted through
  a declared namespace and decoder; unknown kinds and fields fail closed. Resources and
  services use stable logical identities and explicit dependency handles rather than
  reconstructing provider identifiers. Evidence:
  [`Namespace`](../../crates/tokeira-platform/src/definition.rs), the guide's
  [explicit-dependency contract](../provisioning/deployment-definitions.md#explicit-dependencies),
  and [`tokeira-compose::kinds`](../../crates/tokeira-compose/src/kinds/mod.rs).
- [ ] **Honest substrate probe.** `PlatformExecution::probe` returns `Ok(None)` only
  when the platform's meaningful reachability precondition is satisfied, a typed
  `PlatformIssue` for a degradable provider failure, and `Err` for non-provider
  failures. Providers whose substrate has no truthful deployment-wide probe document
  why operation-local errors are the authoritative answer. Evidence:
  [`PlatformExecution`](../../crates/tokeira-platform/src/declaration.rs) and
  [`ComposeExecution`](../../crates/tokeira-compose/src/execution.rs).
- [ ] **Honest operations capability.** Supplying `Ops` means implementing logs, live
  port mappings, and scale in provider terms. Omitting it is an explicit capability
  result, not permission to route around the declaration. Evidence:
  [`Ops`](../../crates/tokeira-platform/src/declaration.rs) and
  [`DockerOps`](../../crates/tokeira-compose/src/ops.rs).
- [ ] **Integration through standard contexts.** Provider clients and shared handles
  enter only through the framework's infra, deploy, and image extension-registration
  seams. `service_platform` returns the deploy-plane manifest applier. Evidence:
  [`PlatformIntegration`](../../crates/tokeira-platform/src/declaration.rs) and
  [`ComposeIntegration`](../../crates/tokeira-compose/src/execution.rs).
- [ ] **Canonical server configuration.** Runtime writebacks target the
  `tokeira-config` `TokeiraConfig` document (`tokeirad.toml`) and its canonical dotted
  keys. A deployment-owned `ServerConfig` graph node establishes ordering and content
  identity; each substrate owns only delivery to its containers or processes. Unknown
  fields remain refused. Evidence: the
  [configuration boundary](../agents/engineering-reference.md#configuration),
  [`TokeiraConfig`](../../crates/tokeira-config/src/lib.rs),
  [`ServerConfig`](../../crates/tokeira-deployment/src/server_config.rs), and Compose's
  [runtime wiring](../../platforms/compose/deployment.tkd).
- [ ] **Complete desired/known IaC lifecycle.** Every manageable resource implements
  stable identity, dependencies, desired manifest, create/update/delete, live describe,
  and diff. `describe` reports `Present`, confirmed `Absent`, or explicitly
  `Unsupported`; it never fabricates live equality. Definitions declare everything
  required for deletion and refresh. Evidence: the
  [IaC contract](../agents/engineering-reference.md#iac-engine-contracts) and
  [`refresh_state`](../../crates/tokeira-iac/src/engine.rs).
- [ ] **Separate, self-describing service plane.** Definition kinds realize
  `Service` values whose manifests contain every substrate coordinate the applier
  needs. The platform applies idempotently, checks live drift, declares deletion
  support only when teardown is idempotent, and never deploys workloads from infra
  apply. Evidence: [`deploy_engine::Service`](../../crates/tokeira-deploy-engine/src/service.rs),
  [`deploy_engine::Platform`](../../crates/tokeira-deploy-engine/src/platform.rs), and
  the [Compose applier](../../crates/tokeira-compose/src/lib.rs).
- [ ] **Provider-accurate rollout inputs.** Network placement, roles, load-balancer or
  ingress attachments, service discovery, capacity, config delivery, and task/process
  identity appear in the desired service model and are reconciled on create and update.
  Live drift checks compare the same owned surface. This follows from the
  self-describing manifest contract above; provider defaults are acceptable only when
  they are deliberately outside the owned surface.
- [ ] **Catalog-backed source packaging.** Cargo platform metadata declares the id,
  engine compatibility, default format, every shipped definition root, and companion
  content roots. Staging and retained revisions therefore carry the exact source set a
  bound provisioner interprets. Evidence: the
  [Compose catalog descriptor](../../platforms/compose/Cargo.toml) and
  [`ConfigSource`](../../crates/tokeira-deployment/src/deployment.rs).
- [ ] **Fully interpreted, modular definitions.** Each advertised frontend has one
  pure `config()` and one `deployment(cfg, cx)`, focused companion parts where the
  graph is substantial, stable names, explicit handles, and cold-reader documentation.
  Tests evaluate, verify, and realize the shipped source through the real platform
  declaration without contacting the provider. Evidence: the guide's
  [program shape](../provisioning/deployment-definitions.md#program-shape),
  [authoring workflow](../provisioning/deployment-definitions.md#authoring-workflow),
  and [Compose definition tests](../../platforms/compose/tests/definition.rs).
- [ ] **Frontend parity and retarget admission.** When both `.tkd` and `.tkdp` are
  advertised they produce the same typed configuration, graph, desired manifests, and
  create-time identity. Changes to `#[create]`/equivalent fields are refused as
  retargets rather than reconciled in place. Evidence: the guide's
  [Python-form contract](../provisioning/deployment-definitions.md#the-python-form-tkdp),
  [`Frontend::retarget_check`](../../crates/tokeira-platform/src/definition.rs), and
  [Compose parity tests](../../platforms/compose/tests/definition.rs).

## Evidence standard

A provider meets an item only when the behavior is implemented and exercised by a
hermetic test. A documented intention, a persisted desired hash without a live read, or
a default trait method that assumes current is not implementation evidence. Provider
API calls belong behind pure builders/comparators so default tests require no provider
credentials or network.
