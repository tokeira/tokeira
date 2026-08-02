# Platform Builder Abstraction Requirements

## Introduction

Tokeira's current `.tkd` definition frontend is shared through `tokeira-tkd`, but the deployment graph,
kind realization, authoring bridge, and projection into the engines remain mixed together inside platform
crates. Compose and EKS duplicate their builder and bridge machinery, while the existing ECS platform
implements the same orchestration concerns through a compiled path. Platform crates also own the service
manifests, image choices, configuration templates, dashboards, alerts, and related artifacts that express
how their services run on that platform; those are intentional platform content, not abstraction debris.

This feature determines the generic/concrete boundary from the requirements of Compose, ECS, and EKS,
then extracts the shared platform machinery into `crates/tokeira-platform`. Compose migrates first, ECS
follows the accepted `ecs-deployment` and `ecs-production-readiness` behavior, and EKS migrates last. This
workstream owns both the cross-platform implementation boundary and completion of the accepted ECS
production-readiness work; the platform specs retain authority for their provider topology, security,
runtime placement, and qualification behavior.

Forthcoming work will add Python-syntax `.tkdp` deployment definitions evaluated by Pydantic's Monty as
an embedded Rust library. Monty's merged
[`@dataclass` implementation](https://github.com/pydantic/monty/pull/626) establishes the intended typed
authoring basis. This workstream does not implement `.tkdp` or add Monty, but it must establish a
definition-language-neutral authoring contract so that `.tkdp` can reuse the same platform bindings,
provider kinds, graph invariants, verification, and engine projection without reopening the platform
boundary.

The target preserves the deployment-married provisioner contract while correcting its packaging. `tkr`
is the operator cockpit: it owns the deployment registry, creation, selection and locking, provisioner
acquisition and placement, launch-class selection, checksum verification, and transparent command
forwarding. The executable placed at `<deployment-dir>/tkp` remains the only process permitted to mutate
that deployment. `tokeira-provisioner-cli` owns that executable's lifecycle shell, binding gate, operation
lock, state envelope, configuration history, and command orchestration. A platform crate supplies a
library binding to the shared framework; it does not contain `src/bin/tkp.rs`, a provisioner workflow, or
binary-assembly policy.

The deployment definition is also kept distinct from executable code. In this workstream,
`definition.tkd` is the standalone, operator-maintained topology definition at the deployment root. Its
recorded definition-format descriptor and relative path make that filename a property of the selected
`.tkd` frontend rather than a permanent platform-framework constant; forthcoming `.tkdp` support will use
its own recorded source. `tkr deployment create` materializes the initial content without compiling the
file into `tkr`, `tkp`, or the platform crate. Thereafter the bound `tkp` loads the recorded
deployment-local file for checks, plans, applies, snapshots, and reverts. Neither a platform
`DEFAULT_TKD` constant nor an `include_str!` of any definition is part of the target.
`tokeirad.toml` remains a separate authoritative runtime-server configuration file: it is not a second
topology definition, but its content is a realization input wherever provider delivery mounts or
publishes it to a service.

The adopted platform convention is consequently small and uniform. `config.rs` owns the typed
platform-policy and desired-input contract; `context.rs` owns immutable runtime information supplied to
one definition evaluation; `ops.rs` owns only the topology-specific declarations needed to resolve log
and port-forward targets; and `lib.rs` assembles and exports the platform binding. Platform-owned service
manifests and artifacts may live as package assets outside `src/`. Provider API mechanics, graph
construction, kind dispatch, and provisioner mechanics live in their respective shared owners; ownership
of platform service content does not move with those mechanics.

Deployment-directory outputs have two different purposes. Operational delivery artifacts, such as
rendered configuration mounted into a service, are derived from platform-owned desired content and are
consumed by the provider or workload. Inspection artifacts, such as Compose's `docker-compose.yml`, are
deterministic operator-facing projections of realized desired state and are never consumed by Tokeira.
Neither class becomes a desired-state authority merely because it is materialized on disk.

Canonical mappings from authored kinds to provider resources live beside the provider resources, such as
`tokeira-aws::kinds` beside `tokeira-aws::resources`. Platform crates select first-party capabilities;
they do not reimplement provider kinds. Custom platform kinds and an external kind-extension mechanism
are outside the current implementation scope, but this is a present-scope decision rather than a
permanent prohibition.

## Glossary

- **Platform_Framework (`tokeira-platform`)** — The provider- and definition-language-neutral
  implementation in `crates/tokeira-platform` of the Authoring_Contract, shared deployment graph, builder
  handles, kind dispatch, realization traversal, definition-verification orchestration, and reusable
  engine projection mechanics. It does not parse `.tkd` or `.tkdp`, embed a definition runtime, or
  introduce the retired neutral `Composition` IR.
- **Operator_Cockpit (`tkr`)** — The global CLI that owns the deployment registry, operator command
  surface, local deployment selection and lock, provisioner acquisition, verified launch, and command
  forwarding; it does not perform provider convergence itself.
- **Provisioner_Shell (`tokeira-provisioner-cli`)** — The shared implementation of `tkp` lifecycle verbs,
  binding and mutation gates, operation locking, state-envelope updates, configuration-revision history,
  and reports, parameterized by a Platform_Binding and selected Definition_Frontend.
- **Bound_Provisioner (`tkp`)** — The verified executable placed at `<deployment-dir>/tkp`, assembled for
  exactly one selected platform and one selected Definition_Frontend and married to the deployment's
  engine identity. It is not sourced from a platform-local `src/bin/tkp.rs`.
- **Deployment_Directory** — The operator-visible directory containing the one recorded live definition
  source (`definition.tkd` in this workstream), `metadata.json`, `tokeirad.toml`, the bound `tkp`,
  applicable bundle evidence, state/configuration history, and derived operational and inspection
  artifacts.
- **Definition_Format** — A validated identifier and source-file convention recorded with the deployment
  and bound to one Definition_Frontend; `tkd` is implemented here and `tkdp` is forthcoming.
- **Definition_Frontend** — A syntax-specific parser, checker, evaluator, value adapter, and located
  diagnostic producer that drives the language-neutral Authoring_Contract without owning graph,
  provider, platform, lifecycle, or persistence semantics.
- **Tkd_Frontend (`tokeira-tkd`)** — The Definition_Frontend implemented in this workstream, which parses,
  checks, admits, and evaluates the supported Rust-syntax `.tkd` subset without naming engine or provider
  types.
- **Tkdp_Frontend** — The forthcoming, separately specified Definition_Frontend for Python-syntax `.tkdp`
  definitions, implemented with Pydantic's Monty embedded Rust library and the same Authoring_Contract.
- **Authoring_Contract** — The language-neutral operations, host-free values, opaque handles, and
  diagnostics through which a Definition_Frontend supplies Platform_Config input and constructs a
  Deployment_Graph.
- **Deployment_Definition** — The one operator-maintained source recorded by Definition_Format and
  relative path at the Deployment_Directory root and loaded by the Bound_Provisioner for each
  definition-aware lifecycle verb.
- **Definition_Seed** — The independently generated or packaged initial source, Definition_Format, and
  relative path from which `tkr` creates a deployment's live definition; it is not embedded in an
  executable or retained as a second authority.
- **Platform_Config** — The platform-specific, serializable configuration contract defined in
  `config.rs`, including defaults, validation, and the platform choices admitted from the
  Deployment_Definition. It is not persisted as a second desired-state file.
- **Platform_Context** — The typed, invocation-scoped information defined in `context.rs` and injected
  while a Deployment_Definition is evaluated.
- **Platform_Ops** — The platform-facing declaration of logical log targets and port-forward endpoints,
  including topology-specific service resolution and supported-name errors; provider API mechanics remain
  in provider crates.
- **Platform_Binding** — The value exported by `lib.rs` that supplies Platform_Config, Platform_Context,
  selected first-party catalogs, state/provider wiring contracts, and Platform_Ops to the shared
  Platform_Framework and Provisioner_Shell.
- **Operational_Endpoint** — A logical service access record containing the provider-neutral identity and
  the platform-specific target/transport facts required for a provider to establish a private tunnel.
- **Deployment_Graph** — The ordered namespaces, modules, module dependencies, resources, optional
  workloads, and declared writeback entries produced from a Deployment_Definition.
- **Module_Handle** — An opaque reference returned when a module is declared and accepted when a resource
  or workload is added to that module.
- **Resource_Handle** — An opaque reference returned when a resource is declared and used to create
  deferred output references.
- **Output_Reference** — A logical `(module, resource, output)` reference resolved against realized
  resource identity and post-apply `InfraState`.
- **Provider_Resource** — A concrete provider lifecycle implementation of `tokeira_iac::Resource`, such
  as an AWS DSQL cluster, Compose service, or Kubernetes manifest bundle.
- **Provider_Kind** — A first-party, safe authored capability colocated with its provider and responsible
  for typed input, provider validation, declared outputs, and conversion to a Provider_Resource.
- **Kind_Dispatch** — The generic translation between host-free Authoring_Contract values and a selected
  Provider_Kind; syntax-specific conversion into those values belongs to the selected
  Definition_Frontend rather than a provider or platform.
- **Placement_Context** — Generic realization information supplied by the Deployment_Graph, including
  logical identity, owning module, and declared dependencies.
- **Platform_Service** — A platform-owned declaration of one deployed service, including its logical
  identity, image selection, command, ports, health behavior, configuration delivery, provider manifest,
  and placement relationships.
- **Platform_Artifact** — A platform-owned manifest or asset used by its services, including configuration
  templates, dashboards, alert rules, and provider-specific supporting documents.
- **Operational_Delivery_Artifact** — A reproducible provider- or workload-consumed materialization of
  platform-owned desired content, such as a rendered configuration file mounted into a service. It is
  operationally used but is not a desired-state input.
- **Inspection_Artifact** — A valid, deterministic, operator-facing projection of realized desired state
  that Tokeira never reads as configuration, provider state, or a lifecycle input.
- **Provider_Delivery** — Provider-owned mechanics that validate, convert where necessary, publish, and
  apply Platform_Service and Platform_Artifact content through Docker, AWS, or Kubernetes APIs without
  taking ownership of that content.
- **Content_Coupling** — The provider-delivery invariant that a workload consuming generated, mounted, or
  published configuration carries a deterministic identity of that content in its desired representation,
  so content changes produce a workload change rather than leaving the running consumer stale.
- **Definition_Verification** — The pure pass over a complete realized resource set that refuses
  non-describing resources and dependency edges whose targets are absent before provider or state access.
- **Platform_Issue** — A provider-classified inability to reach a platform component during planning,
  transported without reinterpretation as component, fact, verbatim SDK evidence, and an optional
  evidence-established direction; an issue-carrying plan contains no changes.
- **Behavioral_Parity** — Equality of externally observable desired state before and after migration:
  module order and dependencies, resource identities, resource dependencies, namespaces, workloads,
  desired replicas, manifests, writeback, and operational behavior.
- **ECS_Spec_Set** — `.kiro/specs/ecs-deployment/` plus
  `.kiro/specs/ecs-production-readiness/`, which own ECS topology, configuration, AWS policy, operator
  endpoint inventory, and production qualification except where this feature explicitly supersedes
  platform-local framework or provisioner packaging. This workstream implements and completes the
  accepted ECS_Spec_Set rather than treating it as externally deferred work.

## Target State

The creation and execution flow is:

```text
tkr deployment create
  |-- stages metadata.json, the recorded definition file, tokeirad.toml, state/
  |-- obtains one-platform, one-frontend provisioner bundle
  |-- validates the definition and stages required Inspection_Artifacts through the selected engine
  |-- stages <deployment-dir>/tkp plus bundle evidence
  |-- atomically publishes the complete deployment directory
  `-- launches the Bound_Provisioner's first-run initialization

tkr <lifecycle command>
  `-- verified launcher
        `-- <deployment-dir>/tkp --deployment-dir <deployment-dir> ...
              `-- tokeira-provisioner-cli lifecycle shell
                    |-- loads the deployment-root source recorded in metadata
                    |-- injects Platform_Context
                    |-- evaluates through the selected Definition_Frontend + Platform_Framework
                    |-- realizes Provider_Kind -> Provider_Resource
                    `-- drives tokeira-iac / deploy engine / state
```

This workstream admits only `Definition_Format = "tkd"`, whose live source is `definition.tkd` and whose
selected frontend is Tkd_Frontend. The same flow deliberately names no `.tkd` parser or syntax contract;
forthcoming `.tkdp` work will add a Monty-backed format and frontend without changing platform bindings.

The Deployment_Directory contract is:

| Entry | Created by | Read or changed by | Authority |
|---|---|---|---|
| recorded live definition (`definition.tkd` here; forthcoming `definition.tkdp`) | `tkr deployment create` from a non-embedded Definition_Seed | operator edits; Bound_Provisioner validates, evaluates, digests, snapshots, and explicitly restores through the recorded Definition_Frontend | sole topology definition and revisioned source for its recorded Definition_Format |
| `metadata.json` | `tkr` | `tkr` registry operations and Provisioner_Shell definition routing | deployment name, UUID, selected platform, Definition_Format, live definition path, storage hint, status, timestamps; routing metadata is never desired topology |
| `tokeirad.toml` | `tkr` seeds | operator may edit server settings; successful apply may write declared outputs; Provider_Delivery fingerprints the consumed bytes | authoritative runtime server configuration and a content-coupled service input, separate from desired topology |
| `tkp` | `tkr` bundle/acquisition pipeline | verified and executed by the `tkr` launcher | deployment-married engine executable |
| `tkp.manifest.json` | `tkr` when a bundle is admitted | `tkr` verifies before launch; `tkp` self-verifies before its first mutation | bundle identity, checksums, provenance, and test evidence |
| `state/` or the selected remote state namespace | `tkr` creates the local root; Bound_Provisioner initializes binding/state before first mutation | Provisioner_Shell and engines update through CAS/locks | binding, integrity, engine state, config revisions, retained binaries, operation markers |
| `state/config-revisions/<n>/<definition-file>` | Bound_Provisioner after initial binding and each successful apply | Bound_Provisioner reads for explicit same-engine, same-format revert | retained historical configuration revision, never a live second source |
| operational delivery artifacts such as rendered `config/` content | during apply | declared provider or workload consumers use the materialized bytes; provider delivery regenerates | derived operational output, never desired-state authority |
| inspection artifacts such as Compose `docker-compose.yml` | platform-owned renderer during creation; lifecycle after successful apply | operators and external inspection tooling may read; Tokeira never reads | derived operator projection, never desired-state or provider-state authority |
| parent `.latest` and `lock.toml` | `tkr` | `tkr` selection and mis-apply guard | cockpit-local targeting metadata |

Compose, ECS, and EKS use no `deployment.toml` after migration. In this workstream their platform choices
and desired topology are represented in `definition.tkd`; the lifecycle contract is expressed in terms of
the recorded Deployment_Definition so forthcoming `.tkdp` support does not create a second path.
Runtime server settings remain authoritative in `tokeirad.toml`. Provider_Delivery may read those runtime
settings only to derive the content identity and delivery representation required by a consuming service;
that does not make the file a graph source.

The concrete needs established by the platform specifications and current implementation are:

| Concern | Compose | ECS | EKS |
|---|---|---|---|
| Provider substrate | Docker Compose plus optional AWS DSQL | AWS ECS on EC2, private VPC, DSQL, internal ALB, capacity providers, Service Connect and SSM | AWS foundation plus a private EKS cluster and live Kubernetes server-side apply |
| State | deployment-local CAS and local-state resource | S3-native state, with a bootstrap remote-state module | S3-native state, with a bootstrap remote-state module |
| Required graph | `local-state`, optional `dsql`, `observability`, `runtime`, with storage-dependent dependencies | `remote-state → networking → dsql → cluster → observability → services`; named selection uses generic dependency closure | `remote_state → foundation → cluster`; Kubernetes objects remain `iac::Resource` values on one InfraEngine path |
| Platform_Config | Docker project/delivery policy, storage mode, DSQL choices, service placement/exposure choices | project/environment/region, networking, capacity, DSQL, ECS placement, endpoint and security policy | project/environment/account/region, network/EKS/node policy, DSQL, namespace and Kubernetes placement policy |
| Platform_Context | deployment identity, optional AWS region, and deployment-root anchors not exposed as arbitrary paths | recorded deployment UUID, environment, resolved AWS account/region, and immutable naming/admission facts | recorded deployment identity, AWS account/region, namespace/cluster facts, and host-only deployment-root plumbing |
| Service delivery | Compose services, bind mounts, operational generated config, and an operator inspection projection | ECS task/service definitions, S3-published config, ECR image references | Kubernetes Deployments, Services, ServiceAccounts, ConfigMaps and platform manifests |
| Content coupling | generated-config declaration identity and mounted `tokeirad.toml` byte identity change consuming service manifests | published configuration identity changes consuming ECS task/service desired state | ConfigMap or published-configuration identity changes consuming Kubernetes workload desired state |
| Plan reachability | an unreachable external Docker daemon is a Platform_Issue | an unreachable AWS substrate needed to describe recorded resources is a Platform_Issue | an unreachable recorded AWS or Kubernetes substrate is a Platform_Issue; a downstream cluster not yet created during staged first creation is not |
| Logs | logical service maps to Docker/Compose log target | logical service maps to the accepted Loki-first and break-glass policy | logical service maps to the Kubernetes namespace/workload/container log target |
| Port forwarding | live published host/container mappings from Docker | six canonical private endpoints projected to direct-instance or remote-host SSM access | Kubernetes namespace/service/port target projected to the kube port-forward transport |
| Deliberate non-ops | desired replica changes are definition edits followed by plan/apply | desired count and capacity changes are definition edits followed by plan/apply; ECS Exec/admin orchestration belongs to `tkr` plus AWS mechanics | provider-side scaling mechanics do not expand Platform_Ops; desired capacity remains configuration unless a separately accepted command contract says otherwise |

The ECS Platform_Ops inventory is fixed by `ecs-production-readiness` and must contain these projections:

| Logical service | Remote TCP port | Capacity-provider target | Access mode |
|---|---:|---|---|
| `grafana` | 3000 | `cp-grafana` | direct instance |
| `mimir` | 9009 | `cp-mimir` | direct instance |
| `loki` | 3100 | `cp-loki` | direct instance |
| `edge-api` | 7233 | `cp-edge-api` | remote Service Connect host |
| `edge-poll` | 7234 | `cp-edge-poll` | remote Service Connect host |
| `controller` | 7240 | `cp-control` | remote Service Connect host |

Compose declares the logical services `tokeirad`, `mimir`, `loki`, `grafana`, and `alloy`; its remote
ports are provider-observed published mappings rather than a second platform table. EKS declares the
logical service-to-namespace/workload/container mapping for the accepted real process topology; its
service ports come from its platform-owned service manifests rather than being copied into Platform_Ops.

The platform specifications remain authoritative for the facts in this matrix. Their historical ownership
claims—such as EKS having no `config.rs`, a platform shipping `src/bin/tkp.rs`, or ECS owning a DAG closure
algorithm—are superseded where they conflict with this feature.

The ownership contract for this feature is:

| Concern | Generic/framework owner | Provider/mechanics owner | Platform owner |
|---|---|---|---|
| Definition creation | `tkr` and a non-embedded, format-bearing Definition_Seed mechanism | provider defaults may contribute generated mechanics | selected platform identity and platform-owned initial service content |
| Definition execution/history | Provisioner_Shell + selected Definition_Frontend + Platform_Framework | — | Platform_Binding supplied as a library value; no definition-language implementation |
| Definition verification | Platform_Framework invokes the provider-neutral verification pass over the complete realization | Provider_Resource truthfully declares live-describe capability | no platform-local verification algorithm |
| Provisioner acquisition/launch | `tkr` + shared bundle pipeline | — | selected platform is a build input, not a binary owner |
| Provisioner lifecycle | `tokeira-provisioner-cli` | state/provider engines behind contracts | Platform_Binding realizes desired state |
| Configuration | shared definition decoding/admission protocol | provider capability defaults | `config.rs` types, defaults, platform validation, service/artifact choices |
| Runtime information | immutable injection and safe field dispatch | provider acquisition helpers where applicable | `context.rs` fields and accessors |
| Graph construction | namespaces, modules, dependencies, resources, workloads, writeback, handles | — | declarations made by the Deployment_Definition |
| Kind dispatch | name/catalog lookup, typed value conversion, take-once handle mechanics | Provider_Kind catalog | selection of permitted first-party catalogs |
| Resource realization | traversal, placement, output lookup | Provider_Kind to Provider_Resource conversion | no duplicate resource mapping |
| Services and images | graph orchestration and catalog validation | provider manifest types and apply mechanics | Platform_Service catalog, image choices, commands, ports, health, placement, capacity, exposure |
| Dashboards, alerts, and templates | opaque artifact traversal | publication/apply mechanics | Platform_Artifact content, selection, and configuration |
| Provider manifests | invocation and desired-state projection | Provider_Delivery mechanics, including Content_Coupling support | manifest content and provider/topology policy |
| Operational delivery artifacts | lifecycle ordering and dependency traversal | materialization and declared provider/workload consumption | desired content and service-consumption declaration |
| Inspection artifacts | declared write boundaries and reusable atomic publication | no provider-state or reconciliation use | provider-specific representation and renderer |
| Engine projection | module wrapping, selection, namespaces, writeback, optional workloads | provider engine implementation | only demonstrated exceptional policy |
| Platform reachability | issue transport, no-change plan outcome, report/exit behavior | provider SDK seam classifies component, fact, verbatim evidence, and grounded direction | no platform-local rendering or generic error translation |
| Logs and port forwarding | `tkr` command orchestration and common result types | Docker/AWS/Kubernetes mechanics | `ops.rs` logical target inventory and topology-specific resolution |
| Scaling and exec/admin | `tkr` command workflow or definition edit followed by plan/apply | provider mechanics | no implicit expansion of Platform_Ops |

The adopted platform source convention is:

```text
platforms/<platform>/src/
├── lib.rs
├── config.rs
├── context.rs
└── ops.rs
```

The four-file rule applies to Rust source under `src/`, not to the whole platform package. A platform may
and ordinarily will own asset directories beside `src/`, such as `manifests/`, `templates/`,
`dashboards/`, and `alerts/`. Those files are inputs selected by its Platform_Binding, not shared product
artifacts and not provider API implementations.

The reasoning behind the convention is:

| Module | Why it remains platform-owned | What it must not own |
|---|---|---|
| `lib.rs` | There must be one discoverable assembly point selecting the platform config, context, provider catalogs, state policy, platform-owned service/artifact catalogs, delivery contracts, and ops declarations. | graph algorithms, Definition_Frontend dispatch, provider clients, lifecycle verbs, or provider API mechanics |
| `config.rs` | Compose, ECS, and EKS admit materially different provider/topology choices; their typed defaults and validation are engine behavior and need one explicit contract. | a second persisted config file, provider I/O, ambient runtime discovery, or embedded definition bytes |
| `context.rs` | A definition needs immutable invocation facts that cannot safely be operator-authored, and each platform needs a different subset. Keeping this seam explicit prevents ambient filesystem/cloud state from becoming hidden configuration. | mutation, persistence, arbitrary environment exposure, provider API implementations, or a new untyped extension bag |
| `ops.rs` | All three platforms need logs and private port access, but the logical target is Docker service/port, ECS capacity-provider/SSM endpoint, or Kubernetes namespace/service/port. The platform must declare that topology. | CLI parsing, session/process management, Docker/AWS/Kubernetes API mechanics, desired-capacity mutation, or a second copy of the platform service catalog |

An additional platform-local source module is not part of the convention. Introducing one requires a
requirements amendment identifying the platform-only invariant that cannot live in the Platform_Framework,
a provider crate, or the platform's declarative service/artifact assets.

Out of scope are changes to the `.tkd` language, implementation of the forthcoming Monty-backed `.tkdp`
frontend, a neutral `Composition` IR, migration of the local platform, and a public/custom kind plugin
system. The Authoring_Contract is the common command/value seam into the already-required
Deployment_Graph, not a second serialized desired-state model or revival of the retired IR. The `.tkdp`
and custom-kind decisions are explicitly revisitable through their own accepted specifications.

## Evidence From Current Code

- `crates/tokeira-tkd/src/lib.rs` is the current engine-agnostic Tkd_Frontend core, generic over
  `HostBridge`; it owns no provider resource or engine projection. The target retains its Rust-syntax
  responsibilities while moving the shared Authoring_Contract and graph semantics below that bridge.
- Pydantic Monty PR
  [`#626`](https://github.com/pydantic/monty/pull/626) merged native in-sandbox `@dataclass` support while
  keeping native definition classes distinct from host-supplied dataclasses that dispatch to Rust. That
  distinction is the ground-truth reason forthcoming `.tkdp` belongs in a Definition_Frontend adapter and
  does not belong in platform crates or provider kinds.
- No `crates/tokeira-platform` crate exists today. The duplicated platform-framework behavior identified
  below is therefore extracted into that new crate; the name does not revive the neutral `Composition`
  IR from the retired `platform-config-dsl` proposal.
- `apps/tkr/src/deployment_dir.rs` owns deployment name resolution, `metadata.json`, `.latest`, initial
  directory creation, provisioner placement, and the forwarded/in-process distinction. Its module-level
  layout comment and its Local/ECS `deployment.toml` branches are transitional and do not describe the
  three-platform target.
- `apps/tkr/src/launcher.rs` resolves and verifies the deployment-local `tkp` and forwards the selected
  deployment directory and lifecycle verb. `apps/tkr/src/commands/deployment.rs` owns registry operations,
  creation, bundle selection, locking, and launch orchestration.
- `crates/tokeira-provisioner-cli/src/lib.rs` and `src/cli.rs` own the generic `tkp` lifecycle shell and
  `ProvisionerPlatform` seam. `src/init.rs`, `src/apply.rs`, and `src/config_history.rs` own initial
  binding, configuration revision advancement, and retained
  `state/config-revisions/<n>/<basename>` snapshots.
- `crates/tokeira-provisioner-cli/src/definition.rs` implements read-only `definition check`, while
  `platforms/compose/src/provisioner.rs::definition_check` currently performs platform-local realization
  and invokes `tokeira_iac::verify_resources`; the target moves that orchestration into the
  Platform_Framework.
- `crates/tokeira-iac/src/engine.rs` defines `PlatformIssue`, carries issues on `PlanOutcome`, and verifies
  the two resource-set conditions required by the operator-output contract: every resource can describe
  live state and every resource dependency names a member of the realized set.
- `crates/tokeira-provisioner-cli/src/plan.rs` renders an issue-carrying outcome and converts it to a typed
  non-zero refusal after emitting the complete report. `crates/tokeira-compose/src/lib.rs` owns the
  Docker SDK error classification and passes SDK evidence through verbatim.
- `crates/tokeira-compose/src/lib.rs` currently reads and writes one YAML path as an internal service
  ledger. The Compose provisioner points that path at `state/compose-services.yaml`, while
  `platforms/compose/src/ops.rs` still expects `<deployment-dir>/docker-compose.yml`. The target separates
  private provider state from the operator-facing Compose projection rather than making one file serve
  both contracts.
- `apps/tkr/src/prototypical.rs` currently embeds `platforms/compose/definition.tkd`, while
  `platforms/compose/src/lib.rs` exposes `DEFAULT_TKD`; both are target violations because they compile the
  definition into executable code.
- `platforms/compose/Cargo.toml`, `src/bin/tkp.rs`, and `src/provisioner.rs` currently make the platform the
  binary owner. This implements the earlier `platform-provisioner-binary` Requirement 14.2 packaging,
  which this feature supersedes while preserving that spec's deployment-married binding, verification,
  lifecycle, and three-part provenance behavior.
- `platforms/compose/src/builder.rs` and `platforms/eks/src/builder.rs` duplicate module/resource/output/
  writeback handles, ordered graph storage, resource realization, and physical-resource lookup.
- `platforms/compose/src/bridge.rs` and `platforms/eks/src/bridge.rs` duplicate host handles, take-once kind
  mechanics, standard builder verbs, output/writeback conversion, context dispatch, and finish behavior.
- `platforms/compose/src/adapter.rs` implements module selection, module wrapping, namespaces, services,
  desired replicas, and writeback resolution that pending ECS/EKS `.tkd` adapters would otherwise copy.
- `platforms/compose/src/builder.rs` now couples generated configuration to its consuming services with
  both a resource dependency and a canonical-content digest. `platforms/compose/src/kinds.rs` couples
  mounted `tokeirad.toml` bytes to the consuming service manifest. These are accepted behavioral
  invariants to preserve while generic graph wiring moves; the Compose manifest content remains owned by
  the Compose platform.
- `crates/tokeira-aws/src/resources/` already owns the concrete AWS lifecycle implementations. Compose
  and EKS wrap the same DSQL and DynamoDB resources but inject different identity, module, and mode
  policies, demonstrating that current `kinds.rs` files mix provider mapping with platform decisions.
- `crates/tokeira-aws/src/resources/s3_object.rs` declares `describes() == false` because its current
  `describe` is a stub. Definition_Verification therefore makes a real live-description implementation a
  prerequisite for admitting the S3-published configuration used by production ECS.
- `platforms/compose/src/images/` and `platforms/ecs/src/images/` express different repository,
  publication, and writeback decisions. Their similarity does not transfer ownership out of the platform;
  only reusable provider mechanics are candidates for extraction.
- `platforms/compose/{templates,dashboards,alerts}/` are legitimate Compose-owned assets.
  `platforms/ecs/src/modules/observability.rs` currently reaches directly into those directories; the
  cross-platform import is the defect, and ECS must own the artifacts its services deploy.
- `platforms/ecs/src/lib.rs` currently mixes configuration, AWS-client acquisition, state selection,
  module projection, service topology, scaling, logs, and provider calls. The ECS_Spec_Set requires the
  concrete private topology, but its module-selection closure and provisioner-binary placement are shared
  concerns under this feature.
- `platforms/eks/src/` currently contains `builder.rs`, `bridge.rs`, `kinds.rs`, `k8s_resource.rs`, and
  `manifests.rs`, but lacks the accepted `config.rs`/`ops.rs` convention. The manifest content is
  platform-owned; the builder, bridge, provider-resource mechanics, and engine adapter are not.
  `platform-eks` Requirements 3, 5, 9, 10, 11, and 12 establish the concrete
  Kubernetes/AWS/state/operation needs; this feature changes who implements the mechanics without moving
  ownership of the desired manifests.
- `platform-eks` Requirement 14 extracted only the Interpreter, while task 2.3 explicitly created a
  second builder and task 5.1 plans a second adapter.
- `platform-config-dsl` Proposal 001 previously identified a `tokeira-platform` seam, but
  `proposals/HISTORY.md` records that its bespoke-language framework never existed. The current `.tkd`
  implementation and forthcoming `.tkdp` frontend both use the crate's language-neutral
  Authoring_Contract without adopting that historical architecture or serialized IR.
- Commit `750cbf1c` retired the legacy Compose platform and renamed `compose-syn` to `compose`; older
  `platform-config-dsl` text still requires the retired compiled `definition.rs` oracle.
- `ecs-production-readiness` Requirement 2 establishes one ECS endpoint inventory and the distinction
  between direct-instance and remote-host SSM access. Its task 1.3 places generic graph closure in ECS and
  Requirement 6 names `tkp-ecs`; those placement decisions are reconciled by this feature without
  weakening the required behavior.
- `platform-eks` Requirement 10 requires live logs and port-forwarding through Kubernetes, while current
  Compose code provides Docker logs and published mappings. Together with the ECS endpoint inventory,
  this is sufficient evidence to retain `ops.rs` as a platform declaration seam rather than provisional
  scaffolding.

## Contract Policy

| Surface | Target owner | Invalid ownership signal | Persistence or side-effect impact |
|---|---|---|---|
| `crates/tokeira-platform` | Platform_Framework | names a concrete platform, provider client, concrete Definition_Frontend/runtime, historical neutral IR, provisioner lifecycle verb, or artifact body | admits authoring operations and projects realized graphs in memory before delegating side effects |
| Definition_Frontend | syntax/runtime-owning frontend crate | owns provider/platform semantics, graph invariants, lifecycle state, or a second desired-state representation | parses, checks, evaluates, adapts values and emits located diagnostics while driving the Authoring_Contract in memory |
| Definition_Format descriptor | trusted source/published catalog and bundle vocabulary | missing or non-canonical id/path, unsupported contract, conflicting frontend coordinates, or untrusted executable locator | selects the live source convention and statically assembled frontend; participates in identity and routing but never desired topology |
| `lib.rs` | platform crate | contains graph algorithms, Definition_Frontend dispatch, provider lifecycle, provisioner workflow, or provider API mechanics | exports one Platform_Binding, platform service/artifact catalogs, and the public config/context/ops types |
| `config.rs` | platform crate | performs I/O, owns provider clients, embeds definition bytes, or creates a second desired-state file | supplies the typed platform contract used to admit the deployment definition in memory |
| `context.rs` | platform crate | persists ambient values or performs provider mutations | values are injected per definition execution and are not definition storage |
| `ops.rs` | platform crate | duplicates provider mechanics, generic command orchestration, scaling mutation, or the Platform_Service catalog | declares logical log and port-forward targets without provider side effects |
| Deployment_Definition | deployment directory | embedded through `include_str!`, exported as a constant, or compiled into any executable | created by `tkr`, edited by the operator, read/snapshotted by Bound_Provisioner |
| Bound_Provisioner assembly | `tkr` bundle/build pipeline + `tokeira-provisioner-cli` entrypoint | platform `[[bin]]`, `src/bin`, platform-local main, multi-platform artifact, or runtime inference of definition format from source presence | produces one-platform, one-frontend bytes placed as `<deployment>/tkp` and verified before use |
| Provisioner workflow | `tokeira-provisioner-cli` | platform-local gate, envelope, config history, lock, lifecycle parser, or upgrade/rollback orchestration | reads and mutates deployment state through the shared workflow |
| Provider_Kind | owning provider crate | imports Definition_Frontend runtime/value types or hard-codes platform topology | realizes provider desired state without owning deployment persistence |
| Kind_Dispatch | Platform_Framework | appears separately in Compose/ECS/EKS | evaluates in memory and performs no provider side effects |
| Definition_Verification | Platform_Framework over the provider-neutral `Resource` contract | platform-local traversal, provider calls, state reads/writes, or admission of a non-describing kind | produces findings only; a refusal prevents planning or mutation |
| Platform_Issue classification | provider SDK seam | platform crate invents provider facts, rewrites SDK evidence, or supplies ungrounded remediation | produces a typed no-change refusal outcome without provider mutation |
| Platform_Issue reporting | Provisioner_Shell and explanation/report owners | platform-local rendering or an additional error line after the report | emits the complete report and returns a non-zero process status |
| Platform_Service | owning platform package | generic/provider crate fixes a platform's images, commands, ports, health, placement, or manifest content | selected into the Deployment_Graph and handed to Provider_Delivery |
| Platform_Artifact | owning platform package | shared artifact owner, cross-platform asset import, or provider crate containing a platform dashboard/template/alert | loaded from the platform package and handed to Provider_Delivery |
| Operational_Delivery_Artifact | Provider_Delivery materializing platform-owned content | generated file becomes an independent config source or silently changes platform semantics | consumed only by its declared provider/workload consumer and regenerated from desired content |
| Inspection_Artifact | owning platform renderer plus shared publication mechanics | any lifecycle or provider path reads it as desired state, provider state, or reconciliation input | created or refreshed only at declared write boundaries; operator inspection has no side effects |
| Provider_Delivery | owning provider crate | provider mechanics define or silently replace platform service/artifact content | validates and applies platform-owned content through provider APIs |
| Content_Coupling | Provider_Delivery | a consumer depends only on a stable path/resource id while content can change | changes the consumer's desired representation when consumed non-secret content changes |
| Definition_Seed | shared creation/template mechanism | live platform-crate file, embedded string, missing/unsupported Definition_Format, or second post-create authority | used once to materialize the recorded deployment-root definition source |

### Platform_Issue field policy

| Field | Target policy | Invalid value or ownership | Persistence or side-effect impact |
|---|---|---|---|
| `component` | provider-owned stable name of the unreachable component | empty value or a generic layer renaming the provider component | identifies the issue in reports and evidence indexes; performs no mutation |
| `fact` | provider-owned operator-facing statement of what failed | speculation about cause or remediation | rendered as the issue statement; performs no mutation |
| `evidence` | the provider SDK error text transported verbatim | rewriting, blending, or replacing the SDK evidence | retained in the explanation model/artifact; performs no mutation |
| `direction` | optional next step only when the evidence establishes it | guessed daemon state, credentials diagnosis, or other unsupported remediation | omitted when unsupported; performs no mutation |

The precedence over earlier ownership statements is:

| Existing specification statement | Position adopted here | Behavior retained |
|---|---|---|
| `platform-provisioner-binary` Requirement 14.1: one platform per `tkp` | retained | each artifact contains exactly one Platform_Binding and its provider closure |
| `platform-provisioner-binary` Requirement 14.2: the platform ships `src/bin/tkp.rs` | superseded | `tokeira-provisioner-cli` still owns the lifecycle shell; `tkr` still obtains, binds, verifies, and launches deployment-married bytes |
| `platform-config-dsl`: platform ships/embeds `DEFAULT_TKD` | superseded | `definition.tkd` is the sole editable configuration revision for the currently implemented Tkd_Frontend and is evaluated on every applicable verb |
| `platform-config-dsl`: historical `tokeira-platform` neutral composition framework | superseded | the crate name is adopted for a definition-language-neutral Authoring_Contract plus graph/dispatch/projection capability, without the historical serialized IR or bespoke language |
| `platform-eks` Requirement 1: no `config.rs`, platform-local builder/bridge/kinds/manifests | superseded | EKS configuration choices and all accepted AWS/Kubernetes desired behavior remain |
| `platform-eks` Requirement 2.5: any unreachable cluster still produces a plan | refined by staged reachability | a downstream cluster not yet created may be planned; failure to reach a recorded substrate needed for live description yields a Platform_Issue and no record-based changes |
| `platform-eks` Requirement 10.1: scaling lives in the platform ops surface | superseded pending an independently accepted direct-scaling contract | live log and port-forward behavior remain required |
| `ecs-production-readiness` task 1.3: ECS-local graph resolver | superseded | requested/effective selection, closure direction, ordering, and refusal semantics remain required in the Platform_Framework |
| `ecs-production-readiness` Requirement 6/task 9: source binary `tkp-ecs` from the ECS platform | superseded | one-platform engine identity, hermetic build/admission, placement as `<deployment>/tkp`, retention, and first-run integrity verification remain required |
| `operator-explanation/output-templates.md`: platform-issue and definition-verification rules | retained | issue-carrying plans contain no changes; non-describing resources and dangling dependencies never reach a plan |

## Requirements

### Requirement 1: Ground the boundary in all three platforms

**User Story:** As an architecture reviewer, I want the abstraction derived from Compose, ECS, and EKS
requirements, so that code is not generalized from one platform or forced across incompatible behavior.

#### Acceptance Criteria

1. THE design SHALL use the Compose/ECS/EKS needs matrix in Target State as its minimum responsibility
   inventory.
2. WHEN a platform specification expresses functional behavior through a historical source layout, THE
   design SHALL preserve the behavior rather than the historical file placement.
3. THE design SHALL classify every current platform source responsibility as Platform_Framework,
   provider capability, platform-owned service/artifact content, Platform_Config, Platform_Context,
   Platform_Ops, or obsolete code.
4. THE design SHALL compare the concrete log-target and port-forward requirements of Compose, ECS, and
   EKS before fixing the Platform_Ops interface.
5. IF an accepted platform behavior cannot be represented by the shared contracts, THEN the requirements
   SHALL retain an explicit platform-owned exception before implementation.
6. WHEN the ECS_Spec_Set changes before design approval, THE comparison SHALL be reconciled against its
   accepted functional requirements.
7. THE design SHALL map every current Compose and EKS source module and every relevant ECS source module
   to its target owner or deletion.
8. THE design SHALL identify each sibling-spec ownership clause superseded by this feature without
   marking unrelated platform behavior complete.
9. THE Platform_Framework SHALL be implemented as the workspace crate `crates/tokeira-platform`.
10. THE `tokeira-platform` crate SHALL implement the accepted definition-language-neutral
    Authoring_Contract and framework without introducing the retired neutral `Composition` IR or
    implementing another deployment language in the framework.
11. THE design SHALL include Platform_Issue transport, Definition_Verification, and Content_Coupling in
    the responsibility comparison rather than treating the landed Compose implementations as
    platform-local exceptions.
12. THE current implementation SHALL supply Tkd_Frontend as the only admitted Definition_Frontend while
    keeping its parser, subset, evaluator, and runtime values outside the Platform_Framework contract.
13. THE requirements and design SHALL identify forthcoming `.tkdp` support through Pydantic's embedded
    Monty Rust library as a separately specified Definition_Frontend over the same Authoring_Contract.
14. THE current implementation SHALL NOT add Monty or claim `.tkdp` execution as complete.

### Requirement 2: Small and conventional platform source surface

**User Story:** As a platform author, I want a small, predictable source layout, so that a new first-party
platform implements only configuration, execution context, and demonstrated operational differences.

#### Acceptance Criteria

1. THE platform crate SHALL define its Platform_Config in `src/config.rs`.
2. THE Platform_Config SHALL contain the typed platform choices, defaults, serialization contract, and
   pure validation required to admit the platform's Deployment_Definition.
3. THE Platform_Config SHALL NOT create or require a second persisted desired-state file beside the one
   recorded live Deployment_Definition.
4. THE Platform_Config SHALL NOT perform provider access or ambient runtime discovery.
5. THE platform crate SHALL define its Platform_Context in `src/context.rs`.
6. THE Platform_Context SHALL expose only the specialized typed runtime facts admitted for that platform's
   Deployment_Definition execution.
7. THE Platform_Context SHALL remain immutable for one definition evaluation.
8. THE Platform_Context SHALL keep host paths, clients, credentials, and provider handles unavailable to
   operator-authored definition code unless a separately accepted field contract requires a safe
   projection.
9. THE platform crate SHALL define its log-target and port-forward declarations in `src/ops.rs`.
10. THE platform crate SHALL expose one Platform_Binding, its platform-owned service/artifact catalogs,
    and its public config/context/ops types through `src/lib.rs`.
11. THE platform crate SHALL contain exactly `lib.rs`, `config.rs`, `context.rs`, and `ops.rs` under
    `src/` after migration.
12. THE platform crate SHALL NOT declare a binary target or a platform-local executable entry point.
13. THE platform crate SHALL NOT retain builder, bridge, adapter, provider-kind, provisioner, or provider
    API implementation modules.
14. IF a future platform requires another source module, THEN an approved requirements amendment SHALL
    identify the platform-only invariant and explain why none of the established owners can contain it.
15. THE four-file platform source convention SHALL permit platform-owned manifests, templates,
    dashboards, alerts, and equivalent declarative assets outside `src/`.
16. THE platform package SHALL NOT import service or artifact content from another platform package.

### Requirement 3: Deployment directory and `tkr`/`tkp` lifecycle

**User Story:** As a deployment operator, I want the cockpit, bound provisioner, and deployment-directory
artifacts to have explicit ownership, so that the exact executable and definition governing a mutation
are always evident.

#### Acceptance Criteria

1. THE Operator_Cockpit SHALL own deployment create, list, use, destroy, selection, local locking,
   provisioner acquisition, verified launch, and command forwarding.
2. THE Operator_Cockpit SHALL NOT perform infrastructure or workload convergence in process.
3. WHEN a Compose, ECS, or EKS deployment is created in this workstream, THE Operator_Cockpit SHALL create
   the Tkd_Frontend source `definition.tkd`, `metadata.json`, `tokeirad.toml`, and the deployment state
   root before first-run initialization.
4. WHEN the initial Deployment_Definition is materialized, THE Operator_Cockpit SHALL obtain its content
   and Definition_Format from a non-embedded Definition_Seed selected for the recorded platform.
5. THE Definition_Seed mechanism SHALL NOT use `include_str!`, a platform `DEFAULT_TKD` constant, or
   executable-linked definition bytes of any supported format.
6. WHEN creation succeeds, THE Deployment_Definition SHALL exist as exactly one live
   `<deployment-dir>/definition.tkd` file for the currently implemented Tkd_Frontend.
7. THE Deployment_Definition SHALL contain the per-deployment desired values and declared structure used
   to compute a configuration revision.
8. THE deployment SHALL NOT persist a `deployment.toml` desired-state source after its platform migration.
9. THE `metadata.json` registry SHALL record deployment identity, selected platform, Definition_Format,
   and live definition relative path without becoming a second desired-state definition.
10. WHEN Platform_Context is constructed, THE Bound_Provisioner SHALL permit recorded metadata to supply
    immutable identity facts without treating mutable registry fields as operator desired state.
11. THE `tokeirad.toml` file SHALL remain the runtime server configuration rather than a deployment graph
    source.
12. WHEN a provisioner bundle is admitted, THE Operator_Cockpit SHALL place its selected one-platform,
    one-frontend executable at `<deployment-dir>/tkp`.
13. WHEN bundle evidence exists, THE Operator_Cockpit SHALL place the corresponding
    `tkp.manifest.json` beside the Bound_Provisioner.
14. WHEN the Bound_Provisioner is first launched, THE Bound_Provisioner SHALL verify its applicable
    integrity evidence before recording the deployment binding.
15. WHEN the initial deployment binding commits, THE Provisioner_Shell SHALL record binding and integrity
    state before any non-state provider resource is mutated.
16. WHEN an operator invokes a definition-aware lifecycle command, THE Operator_Cockpit SHALL forward the
    matching verb, deployment directory, confirmation, output, and selection arguments to the verified
    launch class.
17. WHEN the Bound_Provisioner receives a definition-aware lifecycle command, THE Provisioner_Shell SHALL
    load the live deployment-root source at the recorded definition path for that invocation.
18. WHEN a Deployment_Definition is evaluated, THE Platform_Framework SHALL inject the platform's
    immutable Platform_Context.
19. WHEN a configuration revision commits, THE Provisioner_Shell SHALL retain the source at
    `state/config-revisions/<revision>/<definition-file>` or the equivalent key in the selected state
    store while retaining its Definition_Format identity.
20. WHEN an explicit same-engine revert is requested, THE Provisioner_Shell SHALL restore the selected
    retained definition revision before ordinary reconciliation.
21. WHEN apply emits Operational_Delivery_Artifacts or Inspection_Artifacts, THE engines SHALL treat
    those files as reproducible outputs rather than desired-state inputs.
22. THE Provisioner_Shell SHALL own lifecycle parsing, binding gates, remote operation locking, state
    envelope transitions, configuration history, describe, upgrade, rollback, and reports.
23. THE platform crate SHALL NOT implement any Provisioner_Shell responsibility.
24. THE `tokeira-provisioner-cli`-owned Bound_Provisioner entrypoint SHALL assemble exactly one selected
    Platform_Binding and one selected Definition_Frontend into each produced artifact.
25. WHEN multiple platforms or Definition_Formats are supported, THE produced Bound_Provisioner SHALL NOT
    become a runtime multi-platform or multi-frontend dispatcher.
26. WHEN the selected platform and Definition_Frontend are resolved, THE lifecycle SHALL use the
    identities recorded at creation rather than infer either from the presence or extension of a
    definition file.
27. THE platform crate and package SHALL NOT ship a default Deployment_Definition of any format as
    platform-owned source or runtime authority.
28. THE Definition_Seed SHALL be a versioned external template asset or generated artifact addressed by
    selected platform, Definition_Format, and engine identity outside the platform crate and executable
    bytes.
29. WHEN create-time storage, region, or other admitted choices are supplied, THE Definition_Seed
    materialization SHALL encode them in the initial Deployment_Definition before validation.
30. WHEN a Definition_Seed is materialized, THE Operator_Cockpit SHALL validate the resulting
    Deployment_Definition through the selected one-platform engine before publishing the deployment.
31. IF seed resolution, definition validation, bundle admission, provisioner staging, or required
    create-time artifact rendering or staging fails, THEN THE Operator_Cockpit SHALL leave no committed
    deployment directory or `.latest` selection.
32. WHEN all create-time artifacts have been staged successfully, THE Operator_Cockpit SHALL publish the
    complete Deployment_Directory as one creation boundary.
33. WHEN the deployment has been published, THE Bound_Provisioner SHALL read only the deployment-local
    definition or an explicitly selected retained revision rather than the original Definition_Seed.
34. WHEN `definition check` is invoked, THE Provisioner_Shell SHALL ask the selected Definition_Frontend
    to parse, check, and evaluate the source through the Authoring_Contract before the Platform_Framework
    admits and verifies the complete realized resource set in memory.
35. THE Definition_Verification pass SHALL perform no provider calls or state reads or writes.
36. IF a realized resource dependency names no member of the complete realized set, THEN THE
    Platform_Framework SHALL refuse verification with both resource identities in the finding.
37. IF a realized resource cannot describe live state when its prerequisites are present, THEN THE
    Platform_Framework SHALL refuse verification with the resource identity and provider kind in the
    finding.
38. WHEN Definition_Verification produces findings, THE Provisioner_Shell SHALL render them as the
    definition-check result in the selected output mode.
39. WHEN a definition-check result does not verify, THE Bound_Provisioner entrypoint SHALL return a
    non-zero process status after the complete report is emitted.
40. WHEN `definition check --definition <path>` is invoked, THE Provisioner_Shell SHALL verify that source
    with an explicitly resolved Definition_Format in authoring mode without requiring deployment state.
41. WHEN apply writes declared outputs into `tokeirad.toml`, THE deployment workflow SHALL retain the file
    as the authoritative runtime-server configuration rather than convert it into a graph definition.
42. WHEN an Operational_Delivery_Artifact is materialized, THE lifecycle SHALL permit only its declared
    provider or workload consumer to use it operationally.
43. THE lifecycle SHALL NOT read an Inspection_Artifact during definition checking, planning, applying,
    operations, rollback, or destroy.
44. WHEN plan is invoked, THE lifecycle SHALL NOT create, refresh, or overwrite an
    Operational_Delivery_Artifact or Inspection_Artifact.
45. WHEN ConfigurationIdentity is computed, THE identity SHALL cover the Definition_Format identifier and
    exact admitted source bytes so equal bytes interpreted by different frontends cannot share an
    identity.
46. WHEN a source revision is restored, THE Provisioner_Shell SHALL refuse a retained revision whose
    Definition_Format differs from the Bound_Provisioner's selected Definition_Frontend.
47. WHEN Definition_Format support is resolved from source or published artifacts, THE Operator_Cockpit
    SHALL use trusted descriptor metadata rather than a compiled enum, platform-name branch, or arbitrary
    untrusted runtime path.
48. IF requested Definition_Format metadata and the Definition_Seed, bundle, Bound_Provisioner, or live
    deployment metadata disagree, THEN the lifecycle SHALL refuse before definition evaluation, provider
    access, or state mutation.

### Requirement 4: Provider-owned canonical kinds

**User Story:** As a provider maintainer, I want canonical authored capabilities beside their resource
implementations, so that platforms reuse one safe mapping instead of wrapping the same provider resource.

#### Acceptance Criteria

1. THE owning provider crate SHALL define the Provider_Kind for each reusable first-party
   Provider_Resource capability.
2. THE Provider_Kind SHALL define its safe authored input contract.
3. THE Provider_Kind SHALL define provider-level input validation.
4. THE Provider_Kind SHALL define its declared output names.
5. THE Provider_Kind SHALL convert its typed input and realization context into the corresponding
   Provider_Resource.
6. THE Provider_Kind SHALL NOT depend on Definition_Frontend host/value types, `HostBridge`, Monty runtime
   types, or syntax-specific field maps.
7. THE Provider_Kind SHALL NOT hard-code a Compose, ECS, or EKS module name or topology convention.
8. WHEN a Provider_Kind is realized, THE Platform_Framework SHALL supply its logical identity, owning
   module, and declared dependencies through Placement_Context.
9. WHEN a platform needs a reusable provider capability, THE platform SHALL select the owning provider's
   canonical Provider_Kind instead of implementing a platform-local wrapper.
10. WHEN Compose, ECS, and EKS migrations are complete, THE platform crates SHALL contain no provider
    resource mapping module.
11. THE `tokeira-aws` crate SHALL own the canonical authored mappings for reusable AWS resources used by
    ECS and EKS or by Compose DSQL.
12. THE `tokeira-compose` crate SHALL own the canonical authored mappings and delivery mechanics for
    Docker Compose resources.
13. THE `tokeira-k8s` crate SHALL own the canonical authored mappings and delivery mechanics for
    Kubernetes resources and manifest bundles.
14. THE Provider_Kind catalog SHALL be derived from the capabilities exported by its provider rather than
    from a platform-maintained resource-kind registry.
15. WHEN `lib.rs` assembles a Platform_Binding, THE platform SHALL select permitted first-party provider
    catalogs without redefining their entries.
16. THE Provider_Resource underlying an admitted Provider_Kind SHALL truthfully declare whether its
    `describe` implementation performs a live provider query when prerequisites are present.
17. IF a Provider_Resource can only return an unconfirmed or stub description when its prerequisites are
    present, THEN THE owning provider catalog SHALL withhold that Provider_Kind from verified definitions.
18. WHEN a reusable provider capability is required by an accepted platform, THE owning provider crate
    SHALL implement live description before that platform is declared complete.

### Requirement 5: Current posture on custom kinds

**User Story:** As a Tokeira maintainer, I want the current implementation focused on first-party kinds,
so that unused extension contracts do not enlarge the architecture prematurely.

#### Acceptance Criteria

1. THE current Platform_Framework SHALL support the first-party provider catalogs required by Compose,
   ECS, and EKS.
2. THE current Platform_Framework SHALL NOT implement a public custom-kind plugin API.
3. THE current Platform_Framework SHALL NOT implement dynamic kind loading or runtime third-party
   registration.
4. THE current Platform_Framework SHALL NOT promise compatibility for external kind implementations.
5. IF a concrete future requirement for custom kinds is accepted, THEN THE extension contract SHALL be
   designed as separately reviewed work.
6. THE documentation SHALL describe custom kinds as deferred from the current scope rather than
   permanently prohibited.

### Requirement 6: Platform-owned service manifests and artifacts

**User Story:** As a platform author, I want my platform to own the complete desired content for its
services, so that shared framework and provider mechanics cannot erase platform-specific deployment
intent.

#### Acceptance Criteria

1. THE platform package SHALL own the Platform_Service catalog for every service that platform deploys.
2. THE Platform_Service catalog SHALL define logical identity, image selection, command, ports, health
   behavior, configuration delivery, provider manifest content, and placement relationships.
3. THE platform package SHALL own the Platform_Artifact content used by its services.
4. THE Platform_Artifact content SHALL include the platform's applicable configuration templates,
   dashboards, alert rules, and provider-specific supporting manifests.
5. THE Platform_Binding SHALL select the platform's Platform_Service and Platform_Artifact catalogs from
   that platform package.
6. THE Platform_Framework SHALL treat Platform_Service and Platform_Artifact values as platform-owned
   desired content rather than define their product semantics.
7. THE owning provider crate SHALL define Provider_Delivery mechanics for validating and applying the
   platform-owned desired content.
8. WHEN Provider_Delivery canonicalizes a provider document, THE provider crate SHALL preserve the
   platform-owned semantic content.
9. THE platform package SHALL NOT import Platform_Service or Platform_Artifact content from another
   platform package.
10. THE implementation SHALL NOT extract service manifests or artifacts into a shared owner solely
    because two platforms currently contain similar content.
11. THE Compose platform SHALL own its Compose service manifests, image choices, generated configuration,
    dashboards, alerts, and templates.
12. THE ECS platform SHALL own its task/service manifests, image publication choices, generated or
    published configuration, dashboards, alerts, and templates.
13. THE EKS platform SHALL own its Kubernetes workload manifests, image choices, ConfigMaps, dashboards,
    alerts, and templates.
14. THE Platform_Ops declarations SHALL reference logical service identities from the owning platform's
    Platform_Service catalog rather than define a second service catalog.
15. THE platform configuration or Deployment_Definition SHALL select service profiles, placement,
    capacity, exposure, and provider delivery policy.
16. WHEN a workload consumes generated, mounted, or published configuration, THE Provider_Delivery SHALL
    carry a deterministic identity of the consumed non-secret content in the workload's desired
    representation.
17. WHEN consumed configuration content changes while its path or provider resource identity remains
    stable, THE Provider_Delivery SHALL produce a changed workload desired representation.
18. WHEN consumed configuration content is unchanged, THE Content_Coupling mechanism SHALL produce the
    same content identity.
19. THE Content_Coupling mechanism SHALL exclude credential and secret bytes from provider manifests,
    recorded state, explanation evidence, and configuration digests.
20. WHEN `tokeirad.toml` is delivered to a service, THE Provider_Delivery SHALL derive Content_Coupling
    from the bytes of the authoritative deployment-local runtime configuration.
21. WHEN Provider_Delivery materializes an Operational_Delivery_Artifact, THE platform package SHALL
    retain ownership of the desired content and the provider crate SHALL retain ownership of publication
    and consumption mechanics.
22. WHEN a platform publishes an Inspection_Artifact, THE platform package SHALL own the projection's
    provider-specific representation and shared mechanics SHALL own only atomic file publication.
23. THE Platform_Framework and provider crates SHALL NOT infer desired Platform_Service or
    Platform_Artifact content by reading a materialized Operational_Delivery_Artifact or
    Inspection_Artifact.

### Requirement 7: Shared deployment graph and handles

**User Story:** As a platform definition author, I want one builder model for modules, resources,
outputs, and writeback, so that the same declaration has the same graph meaning on every platform.

#### Acceptance Criteria

1. WHEN a deployment is constructed, THE Platform_Framework SHALL record required namespaces in
   declaration order.
2. WHEN a module is declared, THE Platform_Framework SHALL record its logical name and ordered module
   dependencies.
3. WHEN a resource is added through a Module_Handle, THE Platform_Framework SHALL record its logical id,
   owning module, declaration order, and selected Provider_Kind.
4. WHEN a resource is declared, THE Platform_Framework SHALL return a Resource_Handle bound to that
   logical module and resource identity.
5. WHEN an output is requested from a Resource_Handle, THE Platform_Framework SHALL return an
   Output_Reference bound to the handle's module, resource, and requested output name.
6. WHEN writeback is declared, THE Platform_Framework SHALL record the dotted key and literal or
   Output_Reference in declaration order.
7. IF a resource or workload is added through a handle not owned by the deployment, THEN THE
   Platform_Framework SHALL return an actionable invariant error.
8. THE Platform_Framework SHALL provide read-only graph inspection for adapters, structural assertions,
   and graph-invariant tests.
9. WHEN a provider-backed workload is declared, THE Platform_Framework SHALL record its logical service
   identity, owning module, dependencies, desired capacity, platform-owned Platform_Service content, and
   selected Provider_Delivery.
10. WHERE a platform has no separate deploy-engine workload universe, THE Platform_Framework SHALL permit
    provider-delivered workloads to remain ordinary `iac::Resource` values.
11. WHEN the graph is completed, THE Platform_Framework SHALL validate unique module names, unique
    resource identities, known dependency targets, and acyclic dependencies before provider or state
    mutation.

### Requirement 8: Language-neutral authoring and kind dispatch

**User Story:** As a platform maintainer, I want every Definition_Frontend to drive common handles and
first-party catalogs through one Authoring_Contract, so that platforms do not copy dispatch code or gain
definition-language responsibilities.

#### Acceptance Criteria

1. THE Platform_Framework SHALL own the language-neutral authoring operations and opaque identities for
   deployments, modules, resources, outputs, and take-once Provider_Kind values.
2. THE Platform_Framework SHALL own dispatch for the standard module, resource, output, and writeback
   builder verbs.
3. THE Platform_Framework SHALL construct Provider_Kind values through the selected first-party catalog.
4. WHEN the selected Definition_Frontend produces configuration input, THE Platform_Framework SHALL admit
   its host-free Authoring_Contract value against the selected Platform_Config contract without
   persisting another config artifact.
5. THE Platform_Framework SHALL expose Platform_Context fields through a typed platform-supplied context
   contract.
6. THE Platform_Framework SHALL reject unknown kinds, methods, fields, and invalid receiver types with
   diagnostics retaining the selected Definition_Frontend's source location.
7. THE Platform_Framework SHALL NOT use runtime reflection or introduce an additional `Box<dyn Any>`
   context.
8. WHEN Compose, ECS, and EKS migrations are complete, THE platform crates SHALL contain no independent
   `HostBridge`, Monty adapter, or other Definition_Frontend implementation for standard graph behavior.
9. WHEN a platform exposes a specialized config or context field, THE shared dispatch SHALL obtain its
   schema and accessor through the Platform_Binding rather than a concrete-platform match.
10. THE Definition_Frontend SHALL own conversion between its syntax/runtime values and host-free
    Authoring_Contract values without exposing those runtime values to provider or platform crates.
11. WHILE a Definition_Frontend evaluates author code, THE Authoring_Contract SHALL permit only in-memory
    configuration and Deployment_Graph construction and SHALL perform no provider calls or state I/O.
12. THE Platform_Binding SHALL remain independent of the selected Definition_Format and
    Definition_Frontend so the same Compose, ECS, or EKS binding can support forthcoming `.tkdp` without
    platform changes.

### Requirement 9: Shared verification, engine projection, and writeback

**User Story:** As an engine maintainer, I want verification and graph-to-engine projection implemented
once, so that platforms cannot drift in admission, selection, ordering, resource lookup, reachability, or
writeback behavior.

#### Acceptance Criteria

1. WHEN infrastructure modules are requested, THE Platform_Framework SHALL wrap declared modules with
   their names, dependencies, and on-demand resource realization.
2. WHEN no module selector is supplied, THE Platform_Framework SHALL select every declared module.
3. IF a supplied module selector is empty or names an unknown module, THEN THE Platform_Framework SHALL
   refuse before provider or state access.
4. WHEN plan or apply supplies named modules, THE Platform_Framework SHALL select exactly those modules
   plus their transitive prerequisites.
5. WHEN destroy supplies named modules, THE Platform_Framework SHALL select exactly those modules plus
   their transitive dependents.
6. WHEN an effective module selection is produced, THE Platform_Framework SHALL preserve definition
   declaration order after deduplication.
7. IF a caller or platform binding cannot represent a requested selection, THEN THE shared command path
   SHALL refuse rather than substitute all modules.
8. WHEN a selected reconciliation executes, THE Platform_Framework SHALL retain unrelated recorded state
   and desired definition content.
9. WHEN required namespaces are requested, THE Platform_Framework SHALL preserve their declaration order.
10. WHEN a writeback value is literal, THE Platform_Framework SHALL emit the literal unchanged.
11. WHEN a writeback value is an Output_Reference, THE Platform_Framework SHALL resolve it through the same
   Provider_Kind realization used by module construction.
12. WHEN the physical resource and named string property exist in `InfraState`, THE Platform_Framework
   SHALL emit the resolved value under its declared key.
13. IF a logical resource, physical state entry, named property, or string value is absent, THEN THE
   Platform_Framework SHALL omit that writeback entry.
14. WHEN multiple writeback entries resolve, THE Platform_Framework SHALL preserve declaration order.
15. THE Platform_Framework SHALL emit only explicitly declared writeback keys.
16. THE Platform_Framework SHALL delegate provider clients, state stores, images, hydration, and
    provider operations through contracts justified by the three-platform comparison.
17. IF Definition_Verification fails, THEN THE shared command path SHALL refuse before provider access or
    state mutation.
18. IF planning cannot reach a provider component required to describe recorded managed state, THEN THE
    shared plan path SHALL return a Platform_Issue outcome containing no planned changes.
19. WHEN a provider SDK seam classifies a Platform_Issue, THE provider layer SHALL populate all required
    Platform_Issue fields according to the field policy.
20. WHEN a provider SDK error becomes Platform_Issue evidence, THE provider layer SHALL preserve the SDK
    error text verbatim.
21. IF SDK evidence does not establish a corrective direction, THEN THE provider layer SHALL omit the
    Platform_Issue direction.
22. WHEN the Platform_Framework transports a Platform_Issue, THE Platform_Framework SHALL preserve the
    provider-owned fact, evidence, and direction without reinterpretation.
23. WHEN a plan outcome carries any Platform_Issue, THE Provisioner_Shell SHALL emit the complete issue
    report without action/change sections.
24. WHEN the complete Platform_Issue report has been emitted, THE Bound_Provisioner entrypoint SHALL
    return a bare non-zero process status without an additional error line.
25. IF apply or destroy cannot reach a provider required for mutation, THEN THE provider execution path
    SHALL refuse before provider mutation with an actionable error.
26. IF a downstream provider endpoint is itself a desired resource not yet present during first creation,
    THEN THE shared plan path SHALL treat downstream live description as not yet applicable rather than as
    failure to reach recorded managed state.

### Requirement 10: Operational boundary

**User Story:** As an operator, I want consistent commands backed by correct provider behavior, so that
generic command handling does not erase platform-specific service and runtime semantics.

#### Acceptance Criteria

1. THE Operator_Cockpit SHALL own log and port-forward command parsing, confirmation where required,
   output formatting, session lifecycle, cancellation, and error presentation.
2. THE common operational contract SHALL define provider-neutral service names, log-target requests,
   Operational_Endpoint records, supported-name reporting, and local-port overrides.
3. THE Docker provider layer SHALL own Compose service inspection, log retrieval, and live published-port
   discovery.
4. THE AWS provider layer SHALL own ECS/container-instance discovery, SSM request construction, session
   launch, ECS log access, and applicable break-glass mechanics.
5. THE Kubernetes provider layer SHALL own namespace/workload lookup, pod log retrieval, and kube
   port-forward transport.
6. THE Platform_Ops implementation SHALL map a logical service to the provider target required by the
   selected platform topology.
7. THE Platform_Ops implementation SHALL return one deterministic supported-service inventory for logs
   and port forwarding.
8. IF a requested service is absent from that inventory, THEN THE common operational contract SHALL
   return an actionable error containing the supported names.
9. WHEN Compose resolves logs, THE Compose Platform_Ops SHALL identify the Docker Compose service target.
10. WHEN Compose resolves port access, THE Compose Platform_Ops SHALL request live published mappings for
    the selected service rather than declare a second static port table.
11. WHEN ECS resolves operator access, THE ECS Platform_Ops SHALL project the six accepted service,
    remote-port, capacity-provider, and access-mode entries from one canonical inventory.
12. WHEN EKS resolves logs, THE EKS Platform_Ops SHALL identify the Kubernetes namespace, workload,
    container, and supported log source for the logical service.
13. WHEN EKS resolves port access, THE EKS Platform_Ops SHALL identify the Kubernetes namespace, service,
    and remote port required by kube port forwarding.
14. WHEN a local-port override is supplied, THE common operational layer SHALL change only the local
    listener while preserving the resolved remote target and port.
15. THE Platform_Ops implementation SHALL NOT call Docker, AWS, Kubernetes, process, or network APIs
    directly.
16. THE Platform_Ops implementation SHALL NOT mutate desired capacity or execute administrative commands.
17. WHEN desired service capacity changes for Compose or ECS, THE deployment workflow SHALL represent the
    change as a reviewed Deployment_Definition revision followed by plan and apply.
18. IF a future direct scaling command is accepted for a platform, THEN a separate requirements amendment
    SHALL define its safety and ownership contract before Platform_Ops expands.
19. WHEN ECS Exec or on-demand admin execution is invoked, THE Operator_Cockpit SHALL orchestrate the
    command through AWS provider mechanics without adding execution methods to Platform_Ops.

### Requirement 11: Compose-first migration and cleanup

**User Story:** As a Compose maintainer, I want the renamed `.tkd` platform migrated first, so that its
legacy and duplicated machinery is removed before cloud platform migrations.

#### Acceptance Criteria

1. THE implementation SHALL migrate Compose before ECS and EKS.
2. WHEN Compose uses in-memory storage, THE migrated graph SHALL preserve
   `local-state → runtime → observability`.
3. WHEN Compose uses DSQL storage, THE migrated graph SHALL add the managed-or-preexisting `dsql` module
   with `local-state → dsql → observability → runtime` and its accepted writeback.
4. WHEN Compose is migrated, THE implementation SHALL preserve logical resource ids, physical ids,
   workloads, desired replicas, namespaces, and declared writeback.
5. WHEN Compose services mount generated configuration, THE Compose Provider_Delivery SHALL preserve the
   dependency preventing Docker from creating a directory at the bind source.
6. WHEN Compose desired manifests are assembled, THE Compose Provider_Delivery SHALL preserve canonical
   Compose service, network, volume, command, and dependency content.
7. WHEN Compose generated observability configuration is realized, THE Compose-owned Platform_Artifact
   delivery SHALL preserve the accepted Alloy, Mimir, Loki, Grafana, dashboard, and alert content.
8. WHEN Compose apply writes rendered `config/` content, THE Compose Provider_Delivery SHALL treat it as
   an Operational_Delivery_Artifact without modifying `definition.tkd`.
9. WHEN Compose state is created, THE shared state contract SHALL preserve existing local store paths and
   keys.
10. WHEN Compose logs are requested, THE migrated implementation SHALL use the logical-to-Compose-service
    declaration in `ops.rs` and Docker provider mechanics.
11. WHEN Compose port access is requested, THE migrated implementation SHALL return the provider-observed
    host address, host port, container port, and protocol mappings.
12. THE Compose Platform_Config SHALL admit Docker project/delivery policy, storage mode, DSQL choices,
    and service placement/exposure choices used by the Deployment_Definition.
13. THE Compose Platform_Context SHALL supply deployment identity, optional AWS region, and host-only
    deployment-root anchors required for realization.
14. THE Compose platform SHALL remove its local builder, bridge, adapter projection, kind mappings,
   provisioner implementation, and binary entry point.
15. THE Compose platform SHALL remove embedded-definition constants and embedded-definition loading.
16. THE Operator_Cockpit SHALL remove the `include_str!` path that compiles the Compose definition into
    `tkr`.
17. THE Compose platform SHALL remove the retired compiled `definition.rs` oracle and historical snapshot
   fixture.
18. THE Compose tests SHALL replace compiled-definition differential checks with direct structural and
    behavioral assertions over deployment-directory definitions.
19. THE Compose source tree SHALL retain exactly `lib.rs`, `config.rs`, `context.rs`, and `ops.rs`.
20. WHEN generated Compose configuration content changes, THE Compose Provider_Delivery SHALL change the
    desired representation of every service that consumes that content.
21. WHEN deployment-local `tokeirad.toml` bytes change, THE Compose Provider_Delivery SHALL change the
    desired representation of every service that mounts that file.
22. WHEN Docker is unreachable during a plan that requires live description, THE Docker provider layer
    SHALL classify the failure as a Platform_Issue according to Requirement 9.
23. WHEN Compose Definition_Verification runs, THE Platform_Framework SHALL verify the same complete
    realized resource set used for Compose engine projection.
24. WHEN Compose source cleanup is complete, THE Compose package SHALL retain ownership of its service
    manifests and declarative asset directories outside the four-file `src/` surface.
25. WHEN Compose deployment creation has validated the initial Deployment_Definition, THE Compose-owned
    Inspection_Artifact renderer SHALL stage `<deployment-dir>/docker-compose.yml` before the
    Operator_Cockpit publishes the complete Deployment_Directory.
26. THE generated `docker-compose.yml` SHALL be a valid, deterministic Docker Compose document containing
    the complete realized Compose service model for the represented definition revision.
27. THE generated `docker-compose.yml` SHALL identify itself as generated operator-inspection output and
    state that edits are ignored and may be overwritten.
28. WHEN a Compose apply succeeds, THE lifecycle SHALL atomically refresh `docker-compose.yml` to
    represent the successfully applied definition revision.
29. IF a Compose apply fails, THEN THE lifecycle SHALL leave the previously published
    `docker-compose.yml` unchanged.
30. WHEN Compose plan is invoked, THE lifecycle SHALL NOT create, refresh, or overwrite
    `docker-compose.yml`.
31. THE Compose lifecycle, Docker Provider_Delivery, and Platform_Ops SHALL NOT read
    `<deployment-dir>/docker-compose.yml` as desired state, provider state, or an operational input.
32. WHEN an operator edits the generated `docker-compose.yml`, THE edit SHALL NOT affect any subsequent
    plan, apply, operation, or destroy and the next declared refresh SHALL replace the edited bytes.
33. IF the Docker provider retains a private service ledger, THEN it SHALL store that ledger under the
    deployment state namespace and SHALL NOT alias it to the operator-facing `docker-compose.yml`.

Acceptance Criteria 11.17 and 11.18 supersede the compiled-oracle clauses in `platform-config-dsl`
Requirement 9, Design Property 8, tasks 2.7 and 5.8, and the note claiming `definition.rs` is retained.

### Requirement 12: ECS platform needs and migration

**User Story:** As the ECS spec owner, I want the production ECS topology to consume the common framework
through the adopted platform surface, so that its AWS-specific behavior remains concrete without owning a
second framework.

#### Acceptance Criteria

1. THE ECS migration SHALL implement the complete functional behavior accepted in the ECS_Spec_Set.
2. THE ECS_Spec_Set SHALL remain authoritative for private AWS topology, security-group isolation,
   resource naming, IAM admission, endpoint inventory, recovery, and production qualification.
3. THE platform abstraction feature SHALL remain authoritative for provisioner packaging, generic graph
   construction, module-selection closure, kind dispatch, provider-kind ownership, engine projection, and
   artifact traversal/delivery contracts without owning ECS artifact content.
4. WHEN ECS is migrated, THE graph SHALL preserve the accepted
   `remote-state → networking → dsql → cluster → observability → services` dependency order.
5. WHEN named ECS modules are requested, THE shared Platform_Framework SHALL compute prerequisite or
   dependent closure according to Requirement 9 rather than an ECS `selection.rs` implementation.
6. THE ECS Platform_Config SHALL admit project/environment/region, network, capacity-provider, DSQL,
   service placement, endpoint, tagging, and security policy required by the ECS_Spec_Set.
7. THE ECS Platform_Context SHALL supply recorded deployment UUID, environment, resolved AWS account and
   region, and immutable naming/admission facts required during realization.
8. THE ECS Platform_Ops SHALL own the canonical six-entry operator endpoint inventory required by
   `ecs-production-readiness` Requirement 2.
9. THE ECS Platform_Ops SHALL own the topology-specific logical-service mapping for the accepted log
   source policy.
10. WHEN ECS needs clients, resource operations, IAM evidence readers, state stores, SSM transport, or
    provider discovery, THE AWS/provider layer SHALL provide the mechanics.
11. WHEN ECS emits task definitions, service definitions, images, configuration objects, dashboards, or
    alerts, THE implementation SHALL obtain the desired content from the ECS platform package.
12. WHEN ECS desired counts or capacity policy change, THE operator workflow SHALL use a
    Deployment_Definition edit followed by plan/apply rather than restore direct `tkr scale` mutation.
13. THE ECS platform SHALL remove its compiled graph builders, provider lifecycle code, generic selection
    code, and provisioner entrypoint after migration.
14. THE ECS source tree SHALL retain exactly `lib.rs`, `config.rs`, `context.rs`, and `ops.rs`.
15. THE implementation SHALL amend `ecs-production-readiness` Requirement 6 and tasks 9/11/12 to remove
    the platform-owned `tkp-ecs` entrypoint while preserving verified one-platform bundle behavior.
16. THE implementation SHALL amend the ECS specs wherever generic selection, provider mechanics,
    platform service/artifact ownership, or direct-scaling claims conflict with this feature.
17. IF the ECS_Spec_Set changes the functional abstraction inputs before design approval, THEN the design
    SHALL be reconciled before ECS implementation proceeds.
18. THE ECS integration SHALL be sequenced after the ECS definition work in version-control and merge
    order.
19. WHEN this workstream is complete, THE ECS implementation SHALL satisfy every accepted production
    qualification and evidence requirement in `ecs-production-readiness`.
20. WHEN ECS publishes service or observability configuration through S3, THE AWS S3 object
    Provider_Resource SHALL perform a live provider description when its prerequisites are present.
21. WHEN ECS-published configuration content changes, THE ECS Provider_Delivery SHALL change the desired
    task or service representation of every consumer.
22. IF AWS is unreachable while ECS planning requires live description of recorded managed state, THEN
    THE AWS provider layer SHALL classify the failure as a Platform_Issue according to Requirement 9.
23. THE implementation SHALL NOT defer an accepted ECS production-readiness requirement merely because
    its historical implementation owner moved into the Platform_Framework or an owning provider crate.
24. WHEN ECS migration is complete, THE ECS Platform_Binding SHALL select ECS-owned service manifests,
    image choices, and artifacts from the ECS platform package.
25. THE ECS platform package SHALL NOT import Compose-owned dashboards, alerts, templates, or service
    manifests.

### Requirement 13: EKS migration without broadening EKS scope

**User Story:** As the EKS spec author, I want EKS to use the same graph, provider kinds, and projection,
so that its no-deploy-engine path does not require copied platform machinery.

#### Acceptance Criteria

1. WHEN EKS is migrated, THE implementation SHALL replace its duplicated builder and bridge with the
   Platform_Framework.
2. THE EKS Platform_Config SHALL admit project/environment/account/region, S3 state, VPC/EKS/node, DSQL,
   namespace, service-placement, observability, and private-networking policy required by `platform-eks`.
3. THE EKS Platform_Context SHALL supply recorded deployment identity, resolved AWS account/region,
   namespace/cluster facts, and host-only deployment-root plumbing required during realization.
4. WHEN EKS uses AWS resources, THE implementation SHALL use canonical `tokeira-aws` Provider_Kind
   mappings.
5. WHEN EKS uses Kubernetes objects, THE implementation SHALL use canonical `tokeira-k8s` Provider_Kind
   or Provider_Delivery mechanics without transferring ownership of EKS manifest content.
6. THE EKS implementation SHALL preserve the `remote_state → foundation → cluster` module graph.
7. THE EKS implementation SHALL preserve the single InfraEngine path in which Kubernetes objects are
   `iac::Resource` values.
8. THE EKS implementation SHALL preserve the absence of Compose-style bind volumes and deploy-engine
   workloads.
9. THE EKS implementation SHALL preserve S3 state, private EKS API access, Pod Identity, DSQL writeback,
   namespace creation, and live server-side apply required by the accepted EKS requirements.
10. THE EKS Platform_Ops SHALL own logical-to-Kubernetes log target declarations.
11. THE EKS Platform_Ops SHALL own logical-to-Kubernetes port-forward target declarations.
12. WHEN EKS performs logs or port forwarding, THE `tokeira-k8s` provider SHALL perform the Kubernetes API
    mechanics.
13. THE EKS platform SHALL remove local builder, bridge, provider-kind, Kubernetes resource-mechanics,
    adapter, provisioner, and embedded-definition implementations after migration.
14. THE EKS source tree SHALL retain exactly `lib.rs`, `config.rs`, `context.rs`, and `ops.rs`.
15. THE implementation SHALL amend `platform-eks` Requirement 1's no-`config.rs` and platform-local source
    layout claims to the convention in Requirement 2.
16. THE implementation SHALL amend `platform-eks` Requirement 2's platform-dispatch packaging and
    Requirement 10's direct-scaling ownership without weakening its accepted live log/port behavior.
17. THE EKS migration SHALL NOT mark unrelated unfinished topology, provider, wiring, or live
    qualification tasks complete.
18. WHEN EKS-delivered configuration content changes, THE Kubernetes Provider_Delivery SHALL change the
    desired workload representation of every consumer.
19. IF a recorded EKS or Kubernetes substrate required for live description is unreachable, THEN THE
    owning AWS or Kubernetes provider layer SHALL classify the failure as a Platform_Issue according to
    Requirement 9.
20. IF the EKS cluster is itself absent and scheduled for creation during first creation, THEN THE shared
    plan path SHALL preserve the staged creation plan according to Requirement 9.26.
21. WHEN EKS migration is complete, THE EKS Platform_Binding SHALL select EKS-owned workload manifests,
    image choices, ConfigMaps, and artifacts from the EKS platform package.

### Requirement 14: Completeness, tests, and documentation

**User Story:** As a repository maintainer, I want executable and documented ownership boundaries, so
that temporary migration code does not become another platform-specific framework.

#### Acceptance Criteria

1. WHEN a platform migration is complete, THE platform SHALL contain no private implementation of shared
   graph handles, standard Kind_Dispatch, generic module wrapping, or writeback resolution.
2. WHEN all migrations are complete, THE workspace SHALL contain one implementation of every shared
   builder and engine-projection invariant.
3. THE implementation SHALL avoid compatibility shims whose only purpose is preserving removed
   platform-local builder APIs.
4. THE implementation SHALL remove dead files, dependencies, comments, tests, fixtures, and module
   exports made obsolete by migration.
5. THE property tests SHALL verify declaration-order preservation and provider realization traversal for
   all generated valid Deployment_Graph values.
6. THE property tests SHALL verify the writeback rules in Requirement 9 for all generated output
   references and infrastructure states.
7. THE property tests SHALL verify known dependency targets, acyclic module dependencies, and identity
   uniqueness for all generated graph shapes.
8. THE property tests SHALL verify prerequisite closure, dependent closure, deterministic order, and
   refusal of invalid module selections for all generated module DAGs and selector states.
9. THE property tests SHALL verify Platform_Config serialization round-trips and unknown-field rejection
   for generated valid/invalid Compose, ECS, and EKS configuration values.
10. THE property tests SHALL verify that Platform_Context injection is immutable and limited to each
    platform's declared fields for generated context values and access requests.
11. THE property tests SHALL verify that each Provider_Kind input/output schema and resource realization
    agree for generated valid provider inputs.
12. THE property tests SHALL verify that definition-source edits under one Definition_Format advance
    configuration identity without changing the Bound_Provisioner or selected Platform_Binding identity.
13. THE integration tests SHALL verify create, first-run initialization, live-definition loading,
    retained-revision snapshotting, and explicit restore against a temporary Deployment_Directory.
14. THE integration tests SHALL verify that `tkr` forwards lifecycle arguments to verified
    deployment-local `tkp` bytes without interpreting the Deployment_Definition in process.
15. THE bundle tests SHALL verify that central assembly produces one-platform, one-frontend provisioners
    without any platform `[[bin]]` or `src/bin` target.
16. THE operational tests SHALL verify that Compose, ECS, and EKS resolve logical services through
    `ops.rs` and delegate provider mechanics without direct provider calls from the platform crate.
17. THE ECS operational tests SHALL verify that generic endpoint projection and SSM access planning use
    the same six-entry inventory.
18. THE boundary tests SHALL verify that every migrated platform `src/` directory contains exactly
    `lib.rs`, `config.rs`, `context.rs`, and `ops.rs`.
19. THE boundary tests SHALL verify that no migrated platform or executable embeds a
    Deployment_Definition of any Definition_Format.
20. THE boundary tests SHALL verify that no migrated platform defines a binary target or provisioner
    workflow.
21. THE boundary tests SHALL verify that the Platform_Framework names no concrete Compose, ECS, EKS,
    Tkd_Frontend, Tkdp_Frontend, or Monty runtime type.
22. THE boundary tests SHALL verify that provider kinds import no Definition_Frontend host, value,
    field-map, or runtime type.
23. THE default workspace suite SHALL exercise framework and migration tests without live Docker, AWS,
    DSQL, or Kubernetes credentials.
24. WHEN implementation is complete, THE workspace SHALL pass the repository formatting, lint, check,
    test, and documentation command bar.
25. WHEN implementation changes an existing spec's ownership or completed-task claim, THE implementation
    SHALL amend that spec while preserving its decision trail.
26. THE final documentation SHALL identify `crates/tokeira-platform` as the
    definition-language-neutral framework, Tkd_Frontend as the only frontend implemented by this
    workstream, and Monty-backed `.tkdp` as forthcoming separately specified support while distinguishing
    the architecture from the retired bespoke-DSL proposal of the same name.
27. THE property tests SHALL verify that Definition_Verification accepts every generated complete set of
    describing resources with closed dependency edges.
28. THE property tests SHALL verify that Definition_Verification reports every generated non-describing
    resource and dangling dependency without provider or state access.
29. THE property tests SHALL verify that identical consumed configuration produces identical
    Content_Coupling identity and changed non-secret content produces a changed consumer desired
    representation.
30. THE plan integration tests SHALL verify that an unreachable provider required for live description
    produces Platform_Issue fields without planned changes.
31. THE report integration tests SHALL verify that a Platform_Issue plan emits one complete report and a
    bare non-zero process status without a duplicate error line.
32. THE provider tests SHALL verify that SDK evidence passes through Platform_Issue classification
    verbatim and ungrounded direction is omitted.
33. THE AWS provider tests SHALL verify live S3 object description before ECS S3-published configuration
    is admitted by Definition_Verification.
34. THE EKS integration tests SHALL distinguish a downstream cluster scheduled for first creation from a
    recorded substrate that is unexpectedly unreachable.
35. WHEN ECS migration is declared complete, THE verification evidence SHALL satisfy the accepted
    `ecs-production-readiness` qualification matrix.
36. THE boundary tests SHALL verify that Platform_Framework and provider crates contain no Compose, ECS,
    or EKS service manifest, dashboard, alert, or configuration-template content.
37. THE boundary tests SHALL verify that no migrated platform package imports Platform_Service or
    Platform_Artifact content from another platform package.
38. THE property tests SHALL verify that every Platform_Ops logical service identity belongs to the same
    platform's Platform_Service catalog.
39. THE provider tests SHALL verify that Provider_Delivery canonicalization preserves the semantic
    content of generated valid platform manifests.
40. THE Compose creation integration tests SHALL verify that the published deployment contains a valid,
    deterministic `docker-compose.yml` with the generated-output notice and complete realized service
    model.
41. THE Compose apply integration tests SHALL verify that successful apply atomically refreshes the
    Inspection_Artifact and failed apply leaves the previously published bytes unchanged.
42. THE Compose plan integration tests SHALL verify that plan leaves `docker-compose.yml` unchanged.
43. THE Compose lifecycle tests SHALL verify that operator edits to `docker-compose.yml` do not affect
    plan, apply, operations, or destroy and are replaced at the next successful refresh.
44. THE boundary tests SHALL verify that dependency direction runs from Tkd_Frontend to the
    Platform_Framework Authoring_Contract and not from the Platform_Framework to `tokeira-tkd` or Monty.
45. THE Tkd_Frontend integration tests SHALL verify that current `.tkd` definitions produce the same
    admitted Platform_Config and Deployment_Graph through the Authoring_Contract as the accepted
    pre-extraction behavior.
46. THE bundle tests SHALL verify that Definition_Format identity agrees across trusted descriptor,
    Definition_Seed, bundle, Bound_Provisioner, and deployment metadata before evaluation.
47. THE property tests SHALL verify that equal source bytes under unequal valid Definition_Format
    identifiers produce unequal ConfigurationIdentity values.
48. THE workspace dependency graph SHALL NOT include Monty until a separately accepted `.tkdp`
    specification authorizes its implementation.
49. THE property and bundle tests SHALL verify that changing Definition_Format advances configuration
    and Bound_Provisioner engine identity without changing the selected Platform_Binding identity.
