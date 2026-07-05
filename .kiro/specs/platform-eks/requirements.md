# Requirements Document

## Introduction

Tokeira deploys through platform crates built on the engine-decoupled framework
(`tokeira-iac`, `tokeira-deploy-engine`, `tokeira-state`, `tokeira-orchestrator`, `tokeira-aws`) and
driven by the deployment provisioner **`tkp`**. Two AWS-shaped platforms exist today: `platforms/ecs`
(a classic, non-`syn` ECS platform, not yet wired into `tkp`) and `platforms/compose-syn` (the
`syn`-DSL reference, wired into `tkp` and applied live). There is no Kubernetes platform.

This spec adds **`platforms/eks`**: a first-class tokeira platform that provisions tokeira on **AWS EKS
with Aurora DSQL**, authored in the **`syn` deployment DSL** (Proposals 003/004) and driven end-to-end by
**`tkp`**. It is the migration of a standalone EKS/DSQL deployment workspace
into tokeira, re-expressed against tokeira's existing framework rather than carried as a fork. The proven
EKS mechanics of that source workspace (EKS Auto Mode, Karpenter, Pod Identity, the DSQL PrivateLink
connection endpoint, the Alloy native-sidecar / arm64-affinity / downward-API pod shape) are the
**structural template**; the service set deployed is **tokeira's own**.

Two new shared crates land with this work. **`crates/tokeira-tkd`** holds the platform-agnostic `syn`
interpreter, extracted from `platforms/compose-syn/src/interp/` so `compose-syn` and `eks` share one
interpreter rather than divergent copies; extracting it — and refactoring `compose-syn` onto it with its
fidelity preserved — is a **prerequisite** of `platforms/eks` (each platform supplies its own kind
registry to the shared interpreter). **`crates/tokeira-k8s`** holds the live-apply `kube` platform +
`k8s-openapi` manifest helpers, the direct analog of how the Docker live-apply platform lives in
`crates/tokeira-compose` rather than inside `platforms/compose-syn`. `platforms/eks` itself holds the
`syn` config, kinds, builder, context, `definition.tkd`, adapter, and the `k8s-openapi` manifest builders —
the analog of `platforms/compose-syn`.

**Scope boundary.** This spec owns the EKS platform, the new `tokeira-tkd` and `tokeira-k8s` crates, the
refactor of `platforms/compose-syn` onto `tokeira-tkd`, and their `tkp`/`tkr` wiring. It **extracts** the
existing `syn` interpreter into `tokeira-tkd` **without changing its semantics**; it does **not**
re-specify the *design* of that interpreter, its subset, or the kind-registry mechanics (owned by
`platform-config-dsl` Proposals 003/004). It also **defers to** and does not redefine the provisioner
lifecycle, binding gate, integrity, upgrade/rollback, and locks (owned by `platform-provisioner-binary`).
It reuses the AWS resource implementations in `tokeira-aws` unchanged. There is **no deferral of live
apply**: `tkp apply` against an EKS deployment must drive real `kube` server-side apply, and day-2
scale/logs/port-forward must be live.

**Authorities.** Wire/behaviour of the AWS and Kubernetes surfaces is ground-truthed against: the source
EKS deployment workspace (the proven EKS mechanics), the tokeira framework crates it targets,
and the `platforms/compose-syn` + `platforms/ecs` precedents. EKS Auto Mode / Karpenter / Pod Identity /
DSQL PrivateLink API shapes are ground-truthed against the source workspace's resource implementations
(which already call the live AWS SDK) and re-verified against `tokeira-aws`'s copies.

**Topology currency (verified 2026-07).** The source workspace predates several AWS releases, so its
choices were re-checked against current AWS/Kubernetes documentation. Confirmed still current: **EKS Auto
Mode** with a `karpenter.sh/v1` NodePool referencing the EKS-managed default NodeClass in group
`eks.amazonaws.com` ([Create a Node Pool for EKS Auto Mode](https://docs.aws.amazon.com/eks/latest/userguide/create-node-pool.html)); **Aurora DSQL** (GA May 2025) with IAM-token auth and PrivateLink interface endpoints
([DSQL GA](https://aws.amazon.com/about-aws/whats-new/2025/05/amazon-aurora-dsql-generally-available/),
[DSQL PrivateLink](https://docs.aws.amazon.com/en_us/aurora-dsql/latest/userguide/privatelink-managing-clusters.html));
**Graviton4** families `m8g/c8g/r8g` (still the latest generation); Kubernetes **native sidecars** (stable
since 1.33) ([Sidecar Containers](https://v1-34.docs.kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/));
and **Pod Identity**. One change: EKS now supports Kubernetes up to **1.36**
([EKS 1.36](https://aws.amazon.com/about-aws/whats-new/2026/06/amazon-eks-distro-kubernetes-version-1-36))
and recommends creating on the latest available version
([version lifecycle](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html)), so the
cluster version default moves `1.33 → 1.36` and stays operator-configurable. Content was rephrased for
compliance with licensing restrictions.

## Glossary

- **`platforms/eks`** — the new tokeira EKS platform crate (the `syn` DSL surface + `k8s-openapi` manifest
  builders + the `orchestrator::Deployment`/`Ops` adapter). Analog of `platforms/compose-syn`.
- **`crates/tokeira-tkd`** — the new shared crate holding the platform-agnostic `syn` interpreter
  (value model, subset, schema, eval, admission, `interpret()` orchestration), extracted from
  `platforms/compose-syn/src/interp/`. Each platform supplies its own kind registry + builder + kinds +
  `Cx`; the interpreter core is shared. Analog role to how `tokeira-compose` factors the shared runtime.
- **`crates/tokeira-k8s`** — the new shared crate holding the live `kube` platform (`apply`, `watch`,
  `scale`, `logs`, `port-forward`) and shared `k8s-openapi` manifest helpers. Analog of
  `crates/tokeira-compose`.
- **`KubePlatform`** — the `kube::Client`-backed live-apply handle registered by `tkp` onto the provision
  context, the analog of `ComposePlatform`. K8s workload resources consult it in their lifecycle methods.
- **`tkp`** — the deployment provisioner binary. Detects a deployment's platform, interprets its config
  revision, and drives the `InfraEngine`/`DeployEngine`; owns binding/gate/lock/upgrade (per
  `platform-provisioner-binary`).
- **`.tkd`** — the interpreted `syn` deployment definition (Rust syntax, parsed by `syn`, walked at
  runtime). The deployment's **configuration revision**; an operator edit is an ordinary `apply`, not an
  engine-identity change (Proposal 003 §7).
- **kind** — an author type named in the `.tkd` that realizes to one engine construct: an `iac::Resource`
  (AWS resource or K8s object) or a deploy-engine workload. Analog of `platforms/compose-syn/src/kinds.rs`.
- **EKS Auto Mode** — EKS-managed compute where Karpenter provisions nodes on demand; enabled via
  `ComputeConfigRequest.enabled(true)` with `general-purpose`+`system` node pools and a node role
  (source `crates/aws/src/resources/eks.rs`).
- **Karpenter NodePool** — the `karpenter.sh/v1 NodePool` K8s manifest constraining scheduling to arm64
  on-demand instances of the configured families (source `crates/k8s/src/lib.rs::build_node_pool`).
- **Pod Identity association** — an EKS association binding a Kubernetes ServiceAccount to an IAM role, so
  pods receive the role's credentials with no OIDC provider and no long-lived keys (source
  `crates/aws/src/resources/pod_identity_association.rs`; present in `tokeira-aws`).
- **DSQL connection endpoint** — the Interface VPCE to a DSQL cluster's PrivateLink service, whose
  `private_hostname` is the preferred in-VPC DSQL endpoint (source
  `crates/aws/src/resources/dsql_connection_endpoint.rs`; present in `tokeira-aws`).
- **Alloy native sidecar** — the log/metric collector run as a Kubernetes 1.33 native sidecar (an
  init container with `restartPolicy: Always`) in each workload pod (source
  `crates/project/src/manifests/temporal.rs::alloy_sidecar`).
- **Headless service** — a `clusterIP: None` Service with `publishNotReadyAddresses: true` used for
  stable pod peer discovery; in the source it backs Temporal ringpop membership.
- **Module staging** — the ordered infra module set. Source: `remotestate → foundation → visibility →
  temporal → loom`. Here: **`remote_state → foundation → cluster`** (visibility dropped, loom out of
  scope; the source's `temporal` module becomes `cluster`). The bootstrap `remote_state` module creates
  the S3 state bucket (the deployment persists state in S3, per Requirement 12).
- **Writeback / hydration** — projecting discovered infra outputs (DSQL endpoint/ARN, coordination table
  names) back into the tokeira server config (`TokeiraConfig`), and filling empty config fields from state
  before resource assembly (source `crates/project/src/writeback.rs`; `platforms/ecs/src/lib.rs`).
- **tokeira topology** — the decomposed tokeira service set: `edge-api`, `edge-poll`, `runtime`,
  `projection`, `controller`, `autoscaler`, `admin`, plus `mimir`, `loki`, `grafana` (source
  `platforms/ecs/src/{lib.rs,services.rs}`).

## Target State

**Becomes supported:**

- A `tkp`-driven EKS platform that provisions the private-only AWS foundation (VPC with private subnets,
  VPC endpoints, security groups, IAM roles, ECR, DynamoDB coordination tables, Secrets Manager, managed
  or adopted Aurora DSQL + its PrivateLink connection endpoint) and an EKS Auto Mode cluster with a
  Karpenter arm64 NodePool.
- Live Kubernetes apply of tokeira's decomposed service topology (edge-api/edge-poll/runtime/projection/
  controller/autoscaler/admin) plus the observability stack (mimir/loki/grafana + Alloy sidecars), with
  Pod Identity ServiceAccounts, arm64 node affinity, and the DSQL connection env contract.
- The full lifecycle through `tkp` (`init`/`plan`/`apply`/`destroy`/`revert`/`upgrade`/`rollback`) plus
  day-2 `scale` (in tokeira's startup order), `logs`, and `port-forward` against the live cluster.
- The deployment authored as a `.tkd` config revision an operator edits and re-applies.

**Out of scope / dropped:**

- **Loom** (the source `loom` module/services/images) — out of scope for the first milestone.
- **Benchmark** service set — out of scope for the first milestone unless trivially inherited from the
  observability wiring.
- **Provisioner lifecycle mechanics** (binding gate, integrity, upgrade/rollback, locks) — owned by
  `platform-provisioner-binary`; this spec consumes them, it does not redefine them.
- **The `syn` interpreter *design*** (value model, subset, registry mechanics) — owned by Proposals
  003/004. This spec **extracts** the existing implementation into `tokeira-tkd` semantics-preserved (see
  Requirement 14); it does not redesign it.
- **Binary self-update, release signing** — non-goals (deferred by `platform-provisioner-binary`).

**Sanctioned exceptions:**

- Realize-time non-hermeticity under DSQL (reading live process env / AWS credentials at realize) is
  accepted at the `Cx`/kind boundary, exactly as `platforms/compose-syn` accepts it (Proposal 004 §10).

## Evidence From Current Code

**Reused unchanged (target lives here):**

- `crates/tokeira-aws/src/resources/` already implements every AWS resource the EKS deployment needs:
  `eks.rs`, `pod_identity_association.rs`, `vpc.rs`, `vpc_endpoint.rs`, `security_group.rs`, `iam_role.rs`,
  `dsql_cluster.rs`, `dsql_connection_endpoint.rs`, `dynamodb_table.rs`, `s3_bucket.rs`,
  `secrets_manager_secret.rs`, `ecr_repository.rs`.
- `crates/tokeira-orchestrator/src/lib.rs` — the `Deployment`/`Ops`/`PlatformConfig` traits and
  `InfraEngine`/`DeployEngine` the adapter targets. `PlatformKind` enum is `{Local, Compose, Ecs}`.
- `crates/tokeira-deploy-engine/src/lib.rs` — `Service::manifests() -> Vec<serde_json::Value>` and
  `Platform::apply_manifests(&[Value])`, the seam K8s manifests flow through.
- `platforms/compose-syn/src/adapter.rs` — the `TkdDeployment` pattern: `orchestrator::Deployment`/`Ops`/
  `PlatformConfig` realized from `interp::interpret(source, cx)`, `infra_modules` from `module_names`,
  `services` from `realize_workloads`, `collect_writeback` resolving `WbValue::Output` against
  `InfraState`.
- `platforms/compose-syn/src/{kinds.rs,builder.rs,context.rs,definition.rs,interp/}` and
  `platforms/compose-syn/definition.tkd` — the `syn` platform shape to mirror.
- `apps/tkp/src/platform.rs` — the binding+dispatch seam: `enum Platform { Local, ComposeSyn }`, detection
  by `definition.tkd` presence, and `open_compose_syn_engine` registering `ComposePlatform` onto the
  provision context via `set_extension`. Comment states "ECS is deferred."
- `platforms/ecs/src/{config.rs,services.rs,lib.rs}` — the tokeira topology (service names, ports, capacity,
  observability), DSQL hydrate/writeback pattern, and S3 state store selection to mirror.

**Structural template (ground truth for the EKS mechanics), the source EKS deployment workspace:**

- `crates/aws/src/resources/eks.rs` — EKS Auto Mode create (private API, compute config, access entries).
- `crates/k8s/src/lib.rs::build_node_pool`, `crates/k8s/src/{apply,watch,scale,logs,portforward}.rs`,
  `crates/k8s/src/namespace.rs` — the `kube` client machinery + Karpenter NodePool + `SCALE_UP_ORDER`.
- `crates/project/src/manifests/temporal.rs` — the pod shape: Alloy native sidecar, arm64 node affinity,
  pod anti-affinity, ClusterIP + headless services, Pod Identity ServiceAccount, downward-API
  `POD_IP`/broadcast-address env, and the DSQL connection env contract (`temporal_env_vars`).
- `crates/project/src/modules/{foundation.rs,temporal.rs}` + `crates/project/src/writeback.rs` +
  `crates/project/src/policies.rs` — module composition, IAM trust/inline policies, and the DSQL
  endpoint-preferred writeback/hydration.
- `crates/config/src/model.rs` — the config surface to translate into `syn` config types (the tokeira
  sections; the source's per-service sections become the tokeira topology).

**Authoritative sources for the target `syn` shape and lifecycle:**

- `.kiro/specs/platform-config-dsl/proposals/003-*.md`, `004-*.md` — the `syn` DSL and interpreter.
- `.kiro/specs/platform-provisioner-binary/{requirements,design}.md` — the `tkp` lifecycle this platform
  plugs into.

## Config Surface Policy (`EksConfig`, the `syn` config types)

These config types are authored **in `definition.tkd`** (Proposal 003 §3 — the DSL is the config), not as a
serde `config.rs`; the interpreter models them generically from the `.tkd`'s own struct/enum AST. The
"Error if invalid" column below is realized as `#[require]` attributes evaluated at admission, and unknown
fields are rejected by the interpreter subset — not by a Rust `validate()` or serde `deny_unknown_fields`.

Each config element is translated from the source `ProjectConfig` (`crates/config/src/model.rs`), reconciled
against `platforms/ecs/src/config.rs` (whose `EcsConfig` is a classic serde/TOML struct — the shape is
mirrored, the serialization mechanism is not). The source's per-service sections become the tokeira service
topology; only the sections tokeira needs are carried.

| Config element | Target policy | Error if invalid | Source anchor |
|---|---|---|---|
| `project` (name, region, environment, account_id, tags) | Carried; seeds `Cx` + resource tags | Empty region/name rejected at load | model.rs `ProjectSection`; ecs `EcsConfig` |
| `state` (bucket, key_prefix) | Carried; drives `S3StateStore` key layout | — | model.rs `StateSection`; ecs `state_bucket_name` |
| `vpc` (cidr, availability_zones) | Carried; private-only, one /24 per AZ | Empty cidr or empty AZs rejected | model.rs `VpcSection`; ecs `NetworkingConfig` |
| `eks` (version, namespace, node_families, kms_key_arn?, deletion_protection, bootstrap_admin_permissions, cluster_admin_principal_arn?) | Carried (arm64 Graviton4 families default `[m8g,c8g,r8g]`; version default `1.36` — latest EKS-supported as of 2026-07, operator-configurable) | — | model.rs `EksSection` |
| `dsql` (mode inferred managed/preexisting, endpoint/arn, pool + reservoir + rate_coordination + conn_lease knobs) | Carried; `max_idle_conns` MUST equal `max_conns` (DSQL survival invariant) | `max_idle_conns != max_conns` rejected | model.rs `DsqlSection` |
| `ecr` (pull_through_rules) | Carried if retained; else the two repos only | — | model.rs `EcrSection` |
| `services` (per-tokeira-service: image, desired_count/replicas, cpu, memory, grpc_port?, metrics_port) | The tokeira service topology (from ecs) | Non-canonical grpc/metrics ports rejected | ecs `ServiceConfigs` |
| `observability` ({mimir,loki,grafana,alloy}_image + cpu/memory + retention) | Carried | — | model.rs `ObservabilitySection`; ecs `ObservabilityConfig` |
| `debug` (cloudwatch_logs, log_retention_days) | Carried; gates the optional `logs` VPC endpoint | — | model.rs `DebugSection` |

## AWS Resource Kind Policy (all reused from `tokeira-aws`)

| `.tkd` kind | `tokeira-aws` resource | Module | Notes |
|---|---|---|---|
| `Vpc` | `vpc` | foundation | private-only; emits `subnet_ids` |
| `VpcEndpoint` (Gateway/Interface) | `vpc_endpoint` | foundation | s3+dynamodb gateway; ecr/sts/eks-auth/dsql/secretsmanager interface; logs iff `debug.cloudwatch_logs` |
| `SecurityGroup` | `security_group` | foundation | eks-nodes-sg (membership+grpc+metrics self) and vpc-endpoints-sg (443, 5432) |
| `IamRole` | `iam_role` | foundation | cluster-role, auto-node-role, and per-service Pod-Identity task roles (the tokeira-task role carries DSQL + DynamoDB access) |
| `DsqlCluster` | `dsql_cluster` | foundation | managed vs preexisting inferred from endpoint/arn presence |
| `DsqlConnectionEndpoint` | `dsql_connection_endpoint` | foundation | Interface VPCE; `private_hostname` is the preferred endpoint |
| `DynamoDbTable` | `dynamodb_table` | foundation | rate-limiter + conn-lease (pk hash, ttl_epoch) |
| `S3Bucket` | `s3_bucket` | foundation | mimir + loki buckets |
| `SecretsManagerSecret` | `secrets_manager_secret` | foundation | grafana admin |
| `EcrRepository` | `ecr_repository` | foundation | tokeira server + tool images |
| `EksCluster` | `eks` | cluster | Auto Mode, private API, arm64 node role |
| `PodIdentityAssociation` | `pod_identity_association` | cluster | one per service ServiceAccount |
| `Namespace` | (K8s object; see `tokeira-k8s`) | cluster | Kubernetes namespace as an `iac::Resource`, dep on the EKS cluster |

## Kubernetes Manifest Policy (`platforms/eks` builders + `tokeira-k8s`)

| Manifest | Target | Source anchor |
|---|---|---|
| Karpenter `NodePool` | arm64, on-demand, configured instance families, EKS Auto Mode default NodeClass | `k8s/src/lib.rs::build_node_pool` |
| per-service `Deployment` | main container + Alloy native sidecar (init `restartPolicy: Always`), arm64 node affinity, pod anti-affinity, Pod-Identity ServiceAccount, dynamic-config volume, downward-API `POD_IP`/broadcast env | `project/src/manifests/temporal.rs` |
| per-service `Service` (ClusterIP) | grpc + metrics ports, topology-aware routing | `manifests/temporal.rs::build_temporal_service` |
| per-service headless `Service` | `clusterIP: None`, `publishNotReadyAddresses: true` — **only where a tokeira service's clustering requires peer discovery** (verified against tokeira's membership model) | `manifests/temporal.rs::build_temporal_headless_service` |
| `ServiceAccount` per Pod-Identity task | one per service | `manifests/temporal.rs::build_temporal_service_account` |
| observability manifests (mimir/loki/grafana/alloy) | Deployments + Services + config (Askama-rendered) | `manifests/observability.rs`, `manifests/configmaps.rs` |

## Writeback / Hydration Policy

| Key | Source resource / property | Notes |
|---|---|---|
| `infrastructure.storage = "dsql"` | (const, under DSQL) | mirrors compose-syn |
| `infrastructure.dsql.endpoint` | connection-endpoint `private_hostname`, else cluster `cluster_endpoint` | endpoint-preferred (source `writeback.rs::private_hostname_or_endpoint`) |
| `infrastructure.dsql.region` | config `project.region` | |
| `infrastructure.dsql.arn` | cluster `cluster_arn` | |
| `infrastructure.dsql.rate_limiter_table` / `conn_lease_table` | DynamoDb table `name` | coordination tables |

## Requirements

### Requirement 1: EKS platform crate authored in the `syn` DSL

**User Story:** As a platform author, I want an `platforms/eks` crate shaped exactly like
`platforms/compose-syn`, so that the EKS deployment is a `.tkd` config revision an operator edits and
re-applies, not compiled engine code.

#### Acceptance Criteria

1. THE platform SHALL be a crate `platforms/eks` exposing a `syn` config-and-structure surface
   (`kinds.rs`, `builder.rs`, `context.rs`, `bridge.rs`, `definition.tkd`, `adapter.rs`) mirroring
   `platforms/compose-syn`. Per Proposal 003 §3 (**the DSL *is* the config**), the config types, their
   `config()` defaults, and their `#[create]`/`#[require]` attributes are authored in `definition.tkd` —
   there is no separate serde `config.rs`.
2. WHEN the EKS `.tkd` is interpreted, THEN it SHALL name only the author vocabulary (the builder verbs +
   the EKS kinds + `cx.*`) and the config types it defines, and SHALL contain no filesystem, environment,
   time, or network access (the hermetic subset of Proposal 003 §4 / 004 §6).
3. THE crate SHALL retain a compiled `definition.rs` as a differential oracle for `definition.tkd` (mirroring
   `platforms/compose-syn`), and SHALL hand-pin `syn = { version = "2", features = ["full",
   "extra-traits"] }` and `proc-macro2 = "1"` (not via `cargo add`).
4. THE crate SHALL obtain the `syn` interpreter by depending on the shared `crates/tokeira-tkd` crate
   (Requirement 14), supplying its own kind registry; it SHALL NOT copy or fork the interpreter into
   `platforms/eks`.

### Requirement 2: `tkp` binds, dispatches, and live-applies the EKS platform

**User Story:** As an operator, I want `tkp` to drive the EKS deployment end-to-end — interpret its `.tkd`,
plan, and apply live to AWS and Kubernetes — so that the provisioner (not `tkr`, not a standalone binary)
owns the lifecycle.

#### Acceptance Criteria

1. THE `tkp` `Platform` enum SHALL gain an `Eks` variant, and `orchestrator::PlatformKind` SHALL gain an
   `Eks` variant.
2. WHEN `tkp` resolves a deployment's platform, THEN it SHALL distinguish EKS from compose-syn by a
   recorded platform discriminator (the deployment envelope/metadata set at create), NOT by `definition.tkd`
   presence alone, since both platforms use a `.tkd`.
3. WHEN `tkp` opens the EKS infra engine for an applying verb, THEN it SHALL register a live `KubePlatform`
   handle onto the provision context via `set_extension`, exactly as `open_compose_syn_engine` registers
   `ComposePlatform`.
4. WHEN `tkp apply` runs against an EKS deployment, THEN Kubernetes objects SHALL be applied live via
   server-side apply through the registered `KubePlatform`; there SHALL be no manifest-only or
   apply-deferred mode.
5. WHEN `tkp plan` runs against an EKS deployment with no reachable cluster, THEN it SHALL still produce a
   plan (K8s resource `describe` returning `Unsupported`/absent yields Creates), mirroring how compose-syn
   plans without Docker.
6. THE EKS platform SHALL obtain `tkp`'s existing lifecycle verbs (`init`/`plan`/`apply`/`destroy`/
   `revert`/`upgrade`/`rollback`) by dispatch, and SHALL NOT re-implement binding, gate, lock, or
   provenance (owned by `platform-provisioner-binary`).

### Requirement 3: `tokeira-k8s` — the live Kubernetes platform crate

**User Story:** As a platform author, I want the `kube` client machinery and `k8s-openapi` helpers in a
shared `tokeira-k8s` crate, so that the Kubernetes layer is reusable and `platforms/eks` stays a thin
`syn` surface, mirroring how `tokeira-compose` houses the Docker platform.

#### Acceptance Criteria

1. THE crate `crates/tokeira-k8s` SHALL provide a `KubePlatform` implementing the live-apply operations:
   server-side apply, deployment readiness wait, scale, logs, and port-forward, over a `kube::Client`.
2. THE crate SHALL provide shared `k8s-openapi` manifest helpers (standard labels, the Karpenter NodePool
   builder, the namespace-as-`iac::Resource`) and SHALL pin `k8s-openapi` to a single Kubernetes API
   feature version.
3. WHEN a Kubernetes workload resource's lifecycle method (`create`/`update`/`describe`/`delete`) runs,
   THEN it SHALL obtain the `KubePlatform` from the provision-context extension bag and SHALL fail
   with a clear error if it is absent during an apply (as compose container resources do for
   `ComposePlatform`).
4. `KubePlatform` SHALL NOT be constructed inside the `.tkd` or the interpreter; it is mechanic plumbing
   `tkp` registers (Proposal 003 §2 ownership).

### Requirement 4: Infrastructure module staging (no visibility)

**User Story:** As an operator, I want the EKS infrastructure staged in dependency order matching
tokeira's DSQL-for-projection posture, so that provisioning is minimal and correctly ordered.

#### Acceptance Criteria

1. THE infra module set SHALL be `remote_state → foundation → cluster`, with the bootstrap `remote_state`
   module creating the S3 state bucket, `foundation` depending on `remote_state`, and `cluster` depending
   on `foundation`.
2. THE module set SHALL contain no separate visibility module; visibility is served by the projection
   plane on DSQL (Requirement 8).
3. THE `foundation` module SHALL own: the VPC, VPC endpoints, security groups, IAM roles, the managed-or-
   adopted DSQL cluster and its connection endpoint, the DynamoDB coordination tables, the S3 observability
   buckets, the Grafana secret, and the ECR repositories.
4. THE `cluster` module SHALL own: the EKS cluster, the Kubernetes namespaces, and the Pod Identity
   associations, and SHALL depend only on `foundation` (never on a visibility module).
5. THE module dependency graph SHALL be a DAG with unique module names and unique resource ids
   (composition validation, per `tokeira-iac`).

### Requirement 5: AWS resources reused from `tokeira-aws`

**User Story:** As a platform author, I want the EKS platform to wire the existing `tokeira-aws` resource
implementations rather than port new ones, so that the AWS layer stays single-sourced.

#### Acceptance Criteria

1. THE EKS kinds SHALL realize to the existing `tokeira-aws` resources (`eks`, `pod_identity_association`,
   `vpc`, `vpc_endpoint`, `security_group`, `iam_role`, `dsql_cluster`, `dsql_connection_endpoint`,
   `dynamodb_table`, `s3_bucket`, `secrets_manager_secret`, `ecr_repository`) and SHALL NOT introduce
   duplicate AWS resource implementations in `platforms/eks`.
2. WHERE a `tokeira-aws` resource is found to be missing a capability the EKS deployment needs, THE gap
   SHALL be filed and fixed in `tokeira-aws` (a shared-crate change), NOT worked around inside
   `platforms/eks`.
3. THE cluster/node IAM roles and per-service task roles SHALL carry only the trust and inline policies the
   deployed services require; the tokeira-task role SHALL include DSQL and DynamoDB access.
4. THE DSQL cluster kind SHALL operate managed when no endpoint/arn is configured and adopt a preexisting
   cluster when they are, matching the source's inferred mode.
5. THE EKS cluster version SHALL default to the latest EKS-supported Kubernetes version (1.36 as of
   2026-07) and SHALL remain operator-configurable, per EKS guidance to create clusters on the latest
   available version.

### Requirement 6: Kubernetes manifests for the tokeira pod shape

**User Story:** As a platform author, I want each tokeira service rendered as the proven pod shape (arm64,
Pod Identity, Alloy sidecar, correct broadcast address), so that services schedule on Graviton and collect
telemetry the same way the source deployment does.

#### Acceptance Criteria

1. WHEN a service Deployment is built, THEN it SHALL include an Alloy native sidecar (a Kubernetes 1.33
   init container with `restartPolicy: Always`), arm64 node affinity, pod anti-affinity by service label,
   a Pod-Identity `ServiceAccount`, and downward-API env exporting the pod IP as the broadcast address.
2. WHEN a `tokeirad` Deployment is built, THEN its DSQL connection contract SHALL be delivered via a
   mounted `tokeirad.toml` ConfigMap (located by `TOKEIRA_CONFIG`/`--config`), whose
   `infrastructure.dsql.*` is filled by writeback (Requirement 9). `tokeirad` reads all configuration —
   including the entire DSQL contract (endpoint, region, IAM roles, coordination tables, pool) — from that
   file and reads no per-field DSQL env (`apps/tokeirad/src/lib.rs`: `TokeiraConfig::resolve`). The main
   container env therefore carries only the config path and the downward-API broadcast address
   (`TOKEIRA_NODE_HOST ← status.podIP`), not a Temporal-style `TEMPORAL_SQL_*` env contract.
3. THE platform SHALL build a Karpenter `NodePool` constraining scheduling to arm64 on-demand instances of
   the configured families, referencing the EKS Auto Mode default NodeClass.
4. WHEN a service is exposed, THEN a ClusterIP `Service` SHALL publish its gRPC and metrics ports; a
   headless `Service` SHALL be provided only where the tokeira service's clustering model requires stable
   peer discovery (to be determined against tokeira's membership implementation).
5. THE Kubernetes namespaces required by the deployment SHALL be created as `iac::Resource`s depending on
   the EKS cluster, and SHALL be reported by the adapter's `required_namespaces`.
6. THE cluster's scheduling and pod conventions SHALL follow current EKS practice (verified 2026-07): EKS
   Auto Mode with a `karpenter.sh/v1` NodePool referencing the `eks.amazonaws.com` default NodeClass,
   Graviton4 `m8g/c8g/r8g` node families, and Kubernetes native sidecars (stable since 1.33).

### Requirement 7: tokeira service topology (real process topology)

**User Story:** As an operator, I want the EKS platform to deploy tokeira's actual process topology, so
that every deployed workload maps to a real tokeira binary rather than an invented role split.

_Ground truth: `tokeirad` (`apps/tokeirad`) is a single monolithic process — `build_and_serve` always
wires the full edge + runtime + projection stack; there is no role selector. `tokeira-controller` and
`tokeira-autoscaler` are separate binaries. The decomposed `edge-api/edge-poll/runtime/projection/admin`
set listed in `platforms/ecs/src/services.rs` is a scaffold that does not correspond to real binaries and
is not used here._

#### Acceptance Criteria

1. THE deployed service set SHALL be tokeira's real process topology: `tokeirad` (the edge+runtime+
   projection process, run as N controller-joined runtime nodes), `tokeira-controller` (the active-active
   placement controller), and `tokeira-autoscaler` (the leader-elected autoscaler), plus the observability
   services `mimir`, `loki`, `grafana` (and their Alloy sidecars). `tokeirad` SHALL NOT be split into
   per-role services; horizontal scale is additional `tokeirad` replicas.
2. `tokeirad` SHALL join the controller via `infrastructure.placement.controller_endpoint` (the ClusterIP
   DNS name of `tokeira-controller`), publish the tokeira canonical gRPC and metrics ports, and advertise
   its per-pod membership address via `TOKEIRA_NODE_HOST` (Requirement 6). `tokeira-controller` SHALL
   expose a ClusterIP Service; `tokeira-autoscaler` needs no inbound Service.
3. THE service startup/dependency ordering SHALL be a DAG: `tokeira-controller` first (a `tokeirad` node
   needs a reachable controller), then `tokeirad`; observability (`mimir`/`loki`) independent, `grafana`
   after `mimir`/`loki`, and `tokeira-autoscaler` after `tokeira-controller` and `mimir`.

### Requirement 8: Visibility via the DSQL-backed projection plane

**User Story:** As an operator, I want tokeira's visibility served by its projection plane materializing
read models into Aurora DSQL, so that the deployment runs on a single datastore with nothing separate to
size, secure, or operate for visibility.

#### Acceptance Criteria

1. THE `projection` service SHALL materialize tokeira's visibility read models into DSQL — the projection
   plane owned by `tokeira-projection` — reading from the same Aurora DSQL cluster the `foundation` module
   provisions for persistence.
2. THE deployment SHALL serve both persistence and visibility from that single DSQL cluster, and SHALL
   provision no separate visibility or search datastore.
3. THE service env SHALL carry only the DSQL connection contract for persistence and projection.

### Requirement 9: Writeback and hydration

**User Story:** As an operator, I want discovered DSQL identity projected back into the tokeira server
config after apply, so that the running services connect to the right cluster and coordination tables.

#### Acceptance Criteria

1. WHEN infra apply completes, THEN the platform's `collect_writeback` SHALL emit the DSQL endpoint
   (preferring the connection endpoint's `private_hostname`, falling back to the cluster endpoint), the
   cluster ARN, the region, and the two coordination table names — and no other keys.
2. WHEN config is hydrated from state before resource assembly, THEN empty DSQL endpoint/ARN fields SHALL
   be filled from the applied state, mirroring the source `hydrate_config`.
3. THE writeback values SHALL resolve deferred resource-output handles (`WbValue::Output`) against the
   post-apply `InfraState`, as `platforms/compose-syn`'s adapter does.

### Requirement 10: Live day-2 operations

**User Story:** As an operator, I want live scale, logs, and port-forward against the EKS cluster, so that
running a deployment is fully operable through the provisioner path with no deferred capability.

#### Acceptance Criteria

1. WHEN scaling up, THEN the platform SHALL patch Deployment replicas and wait for readiness in tokeira's
   required startup order; scaling down SHALL reverse that order.
2. WHEN `logs` is requested for a service, THEN the platform SHALL return the service's recent logs from
   the live cluster (via Loki and/or the Kubernetes API), consistent with how `platforms/ecs::logs`
   resolves logs.
3. WHEN `port-forward` is requested, THEN the platform SHALL establish a live port-forward to the named
   service via the `kube` client.
4. WHERE a day-2 verb requires a reachable cluster, THE platform SHALL surface a clear, actionable error
   when the cluster or credentials are unavailable, and SHALL NOT silently succeed.

### Requirement 11: Private-only networking and least-privilege security

**User Story:** As a security-conscious operator, I want the EKS deployment private-only with least-
privilege identity, so that it exposes no public surface and uses no long-lived credentials.

#### Acceptance Criteria

1. THE VPC SHALL have no public subnets and no internet gateway; AWS service access SHALL be via VPC
   endpoints, and the EKS API SHALL be private (`endpoint_public_access(false)`).
2. THE deployment SHALL create no `Ingress`, `LoadBalancer` Service, or other internet-facing surface;
   operator access SHALL be via `port-forward` only.
3. security group ingress rules SHALL be scoped to specific sources (VPC CIDR or self-reference) and SHALL
   NOT use `0.0.0.0/0`.
4. service pods SHALL obtain AWS credentials via Pod Identity (ServiceAccount → IAM role) with no OIDC
   provider and no static keys, and DSQL access SHALL use IAM authentication.

### Requirement 12: State persistence and platform config

**User Story:** As an operator, I want EKS deployment state persisted in S3 and a prototypical config
generated at create, so that state is durable and a new deployment starts from a valid `.tkd`.

#### Acceptance Criteria

1. THE EKS adapter SHALL select the S3-native `S3StateStore` for infra and runtime state (as
   `platforms/ecs` does), keyed by project + environment.
2. WHERE AWS clients are not yet registered, THE state store SHALL fail loudly rather than silently persist
   nowhere (mirroring the ecs `MissingAwsClientsBackend`).
3. THE platform SHALL implement `PlatformConfig` so `tkr … create` writes a valid default EKS `.tkd`
   (`prototypical_config`) and a matching default `tokeirad.toml` (`prototypical_server_config`), the DSQL
   variant pre-filling the DSQL endpoint/region placeholders.

### Requirement 13: Fidelity and verification

**User Story:** As a reviewer, I want the interpreted `.tkd` proven equivalent to the compiled definition
and the composition validated, so that the platform's desired-state is trustworthy before any live apply.

#### Acceptance Criteria

1. THE crate SHALL include a fidelity test proving the interpreted `definition.tkd` produces a deployment
   byte-identical (workloads, namespaces, per-module resource shape, writeback keys/values) to the compiled
   `definition.rs`, for the default and DSQL configurations (mirroring `platforms/compose-syn`'s
   `fidelity`/`fidelity_interp`).
2. THE config authored in `definition.tkd` SHALL round-trip through the interpreter without loss (the
   evaluated `config()` value equals the authored literal) and SHALL reject an unknown config field via the
   interpreter subset's exact-set struct-literal check (Proposal 004 §18, fix #4) — the `syn`-model analog
   of serde `deny_unknown_fields`. There is no separate TOML platform-config to round-trip; the only TOML
   is the *server* config (`tokeirad.toml`/`TokeiraConfig`) that writeback fills.
3. THE `#[create]`-marked config fields (e.g. storage/DSQL mode) SHALL be treated as create-time-immutable:
   a changed `#[create]` field on re-apply SHALL be refused as a retarget, per Proposal 003 §7.
4. Generated Kubernetes manifests SHALL round-trip through `serde_json` without loss, and the service
   dependency graph SHALL be acyclic.

### Requirement 14: Shared `tokeira-tkd` interpreter crate (prerequisite)

**User Story:** As a platform author, I want the `syn` interpreter extracted into a shared
`crates/tokeira-tkd` that both `platforms/compose-syn` and `platforms/eks` depend on, so that one
interpreter serves every `syn` platform and `platforms/eks` builds on it rather than a forked copy.

#### Acceptance Criteria

1. THE crate `crates/tokeira-tkd` SHALL hold the platform-agnostic `syn` interpreter — the value model,
   subset (the reject-by-default allow-list), schema (`TypeTable`/`FnTable` + `#[create]`/`#[require]`),
   evaluator, admission, and the `interpret()` orchestration — moved from
   `platforms/compose-syn/src/interp/` with its behaviour unchanged.
2. THE interpreter SHALL be parameterized over a platform-supplied host bridge (the kind registry plus the
   platform's builder/kinds/`Cx` types), so each `syn` platform supplies its own kinds while sharing the
   interpreter core; `tokeira-tkd` SHALL NOT name any platform's concrete kinds.
3. WHEN `platforms/compose-syn` is refactored to depend on `tokeira-tkd`, THEN its existing interpreter and
   fidelity tests (`fidelity`, `fidelity_interp`, `subset`, `admission`, `interp_edges`) SHALL remain
   green, and the interpreted `definition.tkd` SHALL stay byte-identical to the compiled `definition.rs`.
4. `platforms/compose-syn` and `platforms/eks` SHALL both depend on `tokeira-tkd`, and neither SHALL retain
   a private copy of the interpreter.
5. THE `tokeira-tkd` extraction and the `compose-syn` refactor SHALL land and be green **before** the
   interpreter-backed work in `platforms/eks` begins; this ordering SHALL be reflected as the first task
   block in `tasks.md`.
6. THE interpreter's no-panic security invariant (no operator-reachable `panic!`/`unreachable!`; malformed
   `.tkd` input yields `Diagnostics`, never a panic) SHALL be preserved by the extraction, with the
   existing fuzz/property coverage carried into `tokeira-tkd`.
