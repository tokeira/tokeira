# Requirements Document

## Introduction

Tokeira deploys through definition-backed platform packages built on the platform framework
(`tokeira-platform`, `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-state`, `tokeira-orchestrator`,
`tokeira-aws`) and driven by the deployment provisioner **`tkp`**. Two AWS-shaped platforms exist today
at the full standard: `platforms/compose` (the reference) and `platforms/ecs` (both ship modular `.tkd`
and `.tkdp` definition sets, a pure `platform()` declaration, and dual-frontend parity tests). The
provider contract they satisfy is written down in
[docs/platforms/provider-contract.md](../../../docs/platforms/provider-contract.md). There is no
completed Kubernetes platform.

This spec completes **`platforms/eks`**: a first-class tokeira platform that provisions tokeira on
**AWS EKS with Aurora DSQL**, authored as modular `.tkd` and `.tkdp` definition sets and driven
end-to-end by `tkp`. The crate originates as the migration of a standalone EKS/DSQL deployment
workspace into tokeira, re-expressed against tokeira's framework rather than carried as a fork. The
proven EKS mechanics of that source workspace (EKS Auto Mode, Karpenter, Pod Identity, the DSQL
PrivateLink connection endpoint, the Alloy native-sidecar / arm64-affinity / downward-API pod shape)
are the **structural template**; the service set deployed is **tokeira's own**. The migrated
groundwork already in-tree: `crates/tokeira-k8s` (the live `kube` platform + manifest helpers) and the
EKS kind set in `platforms/eks/src/` (twelve AWS kinds realizing to `tokeira-aws` resources unchanged,
plus the `Namespace` / `NodePool` / `ServiceDeployment` Kubernetes kinds).

**Scope boundary.** This spec owns the EKS platform package (definitions, declaration, execution,
integration, adapter surfaces, catalog descriptor), its `tkp`/`tkr` wiring, and the EKS-specific use of
`crates/tokeira-k8s`. It **defers to** and does not redefine: the definition frontends and their
admission/retarget semantics (owned by `tokeira-platform-definition` and its specs), the provisioner
lifecycle (binding, locks, upgrade/rollback — owned by `platform-provisioner-binary`), and the
deployment-owned config-document machinery (`tokeira-deployment`). It reuses the AWS resource
implementations in `tokeira-aws` unchanged. There is **no deferral of live apply**: `tkp apply` against
an EKS deployment must drive real `kube` server-side apply, and day-2 scale/logs/port-forward must be
live.

**Authorities.** Wire/behaviour of the AWS and Kubernetes surfaces is ground-truthed against: the
source EKS deployment workspace (the proven EKS mechanics), the tokeira framework crates this platform
targets, and the `platforms/compose` + `platforms/ecs` precedents. EKS Auto Mode / Karpenter / Pod
Identity / DSQL PrivateLink API shapes are ground-truthed against the source workspace's resource
implementations (which call the live AWS SDK) and re-verified against `tokeira-aws`'s copies.

**Topology currency (verified 2026-07; re-verify at implementation).** Confirmed current: **EKS Auto
Mode** with a `karpenter.sh/v1` NodePool referencing the EKS-managed default NodeClass in group
`eks.amazonaws.com`
([Create a Node Pool for EKS Auto Mode](https://docs.aws.amazon.com/eks/latest/userguide/create-node-pool.html));
**Aurora DSQL** with IAM-token auth and PrivateLink interface endpoints
([DSQL PrivateLink](https://docs.aws.amazon.com/en_us/aurora-dsql/latest/userguide/privatelink-managing-clusters.html));
**Graviton4** families `m8g/c8g/r8g`; Kubernetes **native sidecars** (stable since 1.33)
([Sidecar Containers](https://v1-34.docs.kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/));
and **Pod Identity**. EKS supports Kubernetes up to **1.36**
([EKS 1.36](https://aws.amazon.com/about-aws/whats-new/2026/06/amazon-eks-distro-kubernetes-version-1-36))
and recommends creating on the latest available version, so the cluster version defaults to `1.36` and
stays operator-configurable. Upstream Kubernetes 1.36 additionally enables **`HPAScaleToZero` by
default** (previously feature-gated since v1.16): the HPA may target zero replicas, with an external
metric source required for automatic wake-up. Manual scale-to-zero (`replicas: 0`) has always been
admitted by Kubernetes and is the path this spec's day-2 scale verb takes; tokeira is unusually
well-suited to it — execution state is durable in DSQL, so an idle deployment's `tokeirad` replicas
can genuinely reach zero without loss (Requirement 9.3). Automatic scale-to-zero is an operator
affordance 1.36 unlocks, outside this spec.

## Glossary

- **`platforms/eks`** — the tokeira EKS platform package: the definition sets (`.tkd` + `.tkdp`), the
  content/observability kinds, `platform() -> PlatformDeclaration`, the catalog descriptor, and tests.
  Analog of `platforms/ecs`.
- **`crates/tokeira-k8s`** — the shared crate holding the live `kube` platform (`KubePlatform`:
  server-side apply, readiness wait, scale, logs, port-forward) and shared `k8s-openapi` manifest
  helpers (`standard_labels`, `build_node_pool`, `NamespaceResource`). Analog of `tokeira-compose`'s
  Docker layer. Already in-tree.
- **`KubePlatform`** — the `kube::Client`-backed live-apply handle registered onto the provision
  context's extension bag during applying operations, the analog of the Compose Docker handle. K8s
  workload resources consult it in their lifecycle methods.
- **`tkp`** — the deployment provisioner. Discovers platforms and definition frontends from workspace
  catalog metadata (`apps/tkr/src/platform_discovery.rs`, `tokeira-build` descriptors), interprets a
  deployment's definition revision, and drives the `InfraEngine`/`DeployEngine`.
- **definition set** — the platform's authored deployment description: a root document wiring focused
  parts, in both supported frontends (`.tkd` via the interpreted-Rust frontend, `.tkdp` via the Python
  frontend), evaluated by `tokeira-platform-definition`. The definition **is** the configuration: the
  config types, their defaults, and their `#[create]` markers are authored in the definition set;
  there is no serde platform-config file.
- **kind** — an author type admitted through a declared `Namespace` that realizes to one engine
  construct: an `iac::Resource` (AWS resource or K8s object) or a deploy-engine service.
- **`ServerConfig` node** — the deployment-owned definition kind (`tokeira_deployment` namespace,
  `crates/tokeira-deployment/src/server_config.rs`) giving every platform the same authored graph node
  for the deployment's `tokeirad.toml`: ordering and content identity, with delivery owned by the
  platform's service kinds.
- **EKS Auto Mode** — EKS-managed compute where Karpenter provisions nodes on demand; enabled via
  `ComputeConfigRequest.enabled(true)` with `general-purpose`+`system` node pools and a node role
  (`tokeira-aws/src/resources/eks.rs`).
- **Karpenter NodePool** — the `karpenter.sh/v1 NodePool` manifest constraining scheduling to arm64
  on-demand instances of the configured families (`tokeira-k8s`: `build_node_pool`).
- **Pod Identity association** — an EKS association binding a Kubernetes ServiceAccount to an IAM role,
  so pods receive the role's credentials with no OIDC provider and no long-lived keys
  (`tokeira-aws/src/resources/pod_identity_association.rs`).
- **DSQL connection endpoint** — the Interface VPCE to a DSQL cluster's PrivateLink service, whose
  `private_hostname` is the preferred in-VPC DSQL endpoint
  (`tokeira-aws/src/resources/dsql_connection_endpoint.rs`).
- **Alloy native sidecar** — the log/metric collector run as a Kubernetes native sidecar (an init
  container with `restartPolicy: Always`) in each workload pod (source workspace:
  `crates/project/src/manifests/temporal.rs::alloy_sidecar`).
- **Module staging** — the ordered infra module set:
  `remote_state → images → networking → dsql → cluster → observability → services`, mirroring the ECS
  definition's modular decomposition. The bootstrap `remote_state` module creates the S3 state bucket.
- **Writeback / hydration** — projecting discovered infra outputs (DSQL endpoint/ARN, region,
  coordination table names) into the deployment's `TokeiraConfig` through the definition's declared
  writebacks, persisted platform-side after infra apply.
- **tokeira topology** — tokeira's real process set: `tokeirad` (the monolithic edge+runtime+projection
  process, N replicas), `tokeira-controller`, `tokeira-autoscaler`, plus `mimir`, `loki`, `grafana`
  with Alloy sidecars (`apps/tokeirad`, `apps/tokeira-controller`, `apps/tokeira-autoscaler`).

## Target State

**Becomes supported:**

- A `tkp`-driven EKS platform that provisions the private-only AWS foundation (VPC with private
  subnets, VPC endpoints, security groups, IAM roles, ECR, DynamoDB coordination tables, Secrets
  Manager, managed or adopted Aurora DSQL + its PrivateLink connection endpoint) and an EKS Auto Mode
  cluster with a Karpenter arm64 NodePool.
- Live Kubernetes apply of tokeira's real process topology plus the observability stack, with Pod
  Identity ServiceAccounts, arm64 node affinity, Alloy native sidecars, and config delivered through
  the `ServerConfig` node's ConfigMap projection.
- The full lifecycle through `tkp` (plan/apply/destroy and the provisioner verbs it dispatches) plus
  day-2 `scale` (in tokeira's startup order), `logs`, and `port-forward` against the live cluster.
- The deployment authored as modular, fully documented `.tkd` and `.tkdp` definition sets an operator
  edits and re-applies, with dual-frontend parity enforced by test.

**Out of scope / dropped:**

- **Benchmark** service set — out of scope for the first milestone unless trivially inherited from the
  observability wiring.
- **Provisioner lifecycle mechanics** (binding, locks, upgrade/rollback) — owned by
  `platform-provisioner-binary`; consumed, not redefined.
- **Definition-frontend semantics** (admission, retarget mechanics, part rules) — owned by
  `tokeira-platform-definition`'s specs; consumed, not redefined.
- **Public ingress of any kind** — the deployment is private-only by design (Requirement 10).

**Sanctioned exceptions:**

- Realize-time non-hermeticity under DSQL (reading live AWS credentials at realize) is accepted at the
  kind boundary, exactly as `platforms/compose` and `platforms/ecs` accept it.

## Evidence From Current Code

**Reused unchanged (target lives here):**

- `crates/tokeira-aws/src/resources/` implements every AWS resource the EKS deployment needs: `eks`,
  `pod_identity_association`, `vpc`, `vpc_endpoint`, `security_group`, `iam_role`, `dsql_cluster`,
  `dsql_connection_endpoint`, `dynamodb_table`, `s3_bucket`, `secrets_manager_secret`,
  `ecr_repository`.
- `crates/tokeira-k8s` — `KubePlatform` (server-side apply, readiness, scale, logs, port-forward),
  `standard_labels`, `build_node_pool`, `NamespaceResource`.
- `platforms/eks/src/kinds.rs` — the migrated EKS kind set realizing to `tokeira-aws` resources.
- `crates/tokeira-deployment/src/server_config.rs` — the deployment-owned `ServerConfig` node and the
  `tokeira_deployment` namespace both existing platforms admit.
- `docs/platforms/provider-contract.md` — the twelve-point provider checklist this platform must
  satisfy; `platforms/ecs` + `platforms/compose` are the passing precedents.
- `apps/tkr/src/platform_discovery.rs` + `tokeira-build` descriptors — catalog discovery: a platform
  joins by its `[package.metadata.tokeira.platform]` descriptor and platform-declared source seeds;
  no enum is edited anywhere.
- `platforms/ecs/{deployment.tkd, definition.tkdp, *.tkd, *.tkdp, tests/definition.rs}` — the modular
  dual-frontend definition shape and the parity harness to mirror.

**Structural template (ground truth for the EKS mechanics), the source EKS deployment workspace:**

- `crates/aws/src/resources/eks.rs` — EKS Auto Mode create (private API, compute config, access
  entries).
- `crates/k8s/src/lib.rs::build_node_pool`, `crates/k8s/src/{apply,watch,scale,logs,portforward}.rs`,
  `crates/k8s/src/namespace.rs` — the `kube` client machinery + Karpenter NodePool + `SCALE_UP_ORDER`.
- `crates/project/src/manifests/temporal.rs` — the pod shape: Alloy native sidecar, arm64 node
  affinity, pod anti-affinity, ClusterIP + headless services, Pod Identity ServiceAccount,
  downward-API `POD_IP`/broadcast-address env.
- `crates/project/src/modules/{foundation.rs,temporal.rs}` + `crates/project/src/writeback.rs` +
  `crates/project/src/policies.rs` — module composition, IAM trust/inline policies, and the DSQL
  endpoint-preferred writeback/hydration.
- `crates/config/src/model.rs` — the source config surface translated into the definition's config
  model.

## Config Surface Policy (the definition's config model)

The config types are authored **in the definition set** (the definition is the config), mirrored
identically across `.tkd` and `.tkdp`. Unknown fields are rejected by each frontend's admission;
create-time-immutable identity carries `#[create]`. Elements are translated from the source
`ProjectConfig` (`crates/config/src/model.rs`) and reconciled against the ECS definition's model.
Server-side tuning (DSQL pool/reservoir knobs, rate coordination) is **not** platform configuration —
it lives in the deployment's `tokeirad.toml` (`TokeiraConfig`), delivered through the `ServerConfig`
node; the `max_idle_conns == max_conns` invariant is enforced by `TokeiraConfig` validation, not here.

| Config element | Target policy | Error if invalid | Source anchor |
|---|---|---|---|
| `environment`, `region`, tags model | Carried; seeds context + resource tags | Empty region rejected at admission | model.rs `ProjectSection`; ecs `platform.tkd` |
| `networking` (vpc cidr, availability_zones) | Carried; private-only, one subnet per AZ | Empty cidr/AZs rejected | model.rs `VpcSection`; ecs `networking.tkd` |
| `eks` (version, namespace, node_families, kms_key_arn?, deletion_protection, bootstrap_admin_permissions, cluster_admin_principal_arn?) | Carried (Graviton4 families default `[m8g,c8g,r8g]`; version default `1.36`, operator-configurable) | — | model.rs `EksSection` |
| `dsql` (mode as a shaped enum: Managed / Preexisting{endpoint, arn}) | Carried; `#[create]` (storage identity is create-time-immutable) | Retarget refused on `#[create]` change | model.rs `DsqlSection`; ecs `dsql.tkd` |
| `images` (server + tool images, pull policy) | Carried; ECR repositories derived | — | model.rs `EcrSection`; ecs `images.tkd` |
| `services` (per-service: image, replicas, cpu, memory) | tokeira's real topology only (Requirement 6) | Non-canonical gRPC/metrics ports rejected | ecs `services.tkd` |
| `observability` ({mimir,loki,grafana,alloy} image + cpu/memory + retention) | Carried; images pinned to the workspace-current observability pins | — | model.rs `ObservabilitySection`; ecs `observability.tkd` |
| `debug` (cloudwatch_logs, log_retention_days) | Carried; gates the optional `logs` VPC endpoint | — | model.rs `DebugSection` |

## AWS Resource Kind Policy (all reused from `tokeira-aws`)

| Kind | `tokeira-aws` resource | Module | Notes |
|---|---|---|---|
| `Vpc` | `vpc` | networking | private-only; emits `subnet_ids` |
| `VpcEndpoint` (Gateway/Interface) | `vpc_endpoint` | networking | s3+dynamodb gateway; ecr/sts/eks-auth/dsql/secretsmanager interface; logs iff `debug.cloudwatch_logs` |
| `SecurityGroup` | `security_group` | networking | eks-nodes-sg (membership+grpc+metrics self) and vpc-endpoints-sg (443, 5432) |
| `IamRole` | `iam_role` | dsql / cluster | cluster-role, auto-node-role, and per-service Pod-Identity task roles (the tokeirad task role carries DSQL + DynamoDB access) |
| `DsqlCluster` | `dsql_cluster` | dsql | Managed vs Preexisting per the shaped config enum |
| `DsqlConnectionEndpoint` | `dsql_connection_endpoint` | dsql | Interface VPCE; `private_hostname` is the preferred endpoint |
| `DynamoDbTable` | `dynamodb_table` | dsql | rate-limiter + conn-lease (pk hash, ttl_epoch), `{project}-dsql-rate-limiter` / `-conn-lease` naming |
| `EcrRepository` | `ecr_repository` | images | tokeira server + tool images |
| `S3Bucket` | `s3_bucket` | observability | mimir + loki buckets |
| `SecretsManagerSecret` | `secrets_manager_secret` | observability | grafana admin |
| `EksCluster` | `eks` | cluster | Auto Mode, private API, arm64 node role |
| `PodIdentityAssociation` | `pod_identity_association` | cluster | one per service ServiceAccount |
| `Namespace` | `tokeira-k8s` `NamespaceResource` | cluster | Kubernetes namespace as an `iac::Resource`, dep on the EKS cluster |

## Kubernetes Manifest Policy (`platforms/eks` builders + `tokeira-k8s`)

| Manifest | Target | Source anchor |
|---|---|---|
| Karpenter `NodePool` | arm64, on-demand, configured instance families, EKS Auto Mode default NodeClass | `tokeira-k8s::build_node_pool` |
| per-service `Deployment` | main container + Alloy native sidecar (init `restartPolicy: Always`), arm64 node affinity, pod anti-affinity, Pod-Identity ServiceAccount, config volume, downward-API broadcast env | source `manifests/temporal.rs` |
| per-service `Service` (ClusterIP) | grpc + metrics ports | source `manifests/temporal.rs` |
| headless `Service` | `clusterIP: None`, `publishNotReadyAddresses: true` — only where a tokeira service's clustering requires stable peer discovery (verified against tokeira's membership model) | source `manifests/temporal.rs` |
| `ServiceAccount` per Pod-Identity task | one per service | source `manifests/temporal.rs` |
| observability manifests (mimir/loki/grafana/alloy) | Deployments + Services + rendered content through the platform's content kinds | source `manifests/{observability,configmaps}.rs`; ecs observability content pattern |

## Writeback Policy

| Key | Source resource / property | Notes |
|---|---|---|
| `infrastructure.storage = "dsql"` | (const, under DSQL) | mirrors compose + ecs |
| `infrastructure.dsql.endpoint` | connection-endpoint `private_hostname`, else cluster `cluster_endpoint` | endpoint-preferred |
| `infrastructure.dsql.region` | config region | |
| `infrastructure.dsql.arn` | cluster `cluster_arn` | |
| `infrastructure.dsql.rate_limiter_table` / `conn_lease_table` | DynamoDb table `name` | coordination tables |

## Requirements

### Requirement 1: A definition-backed platform at the provider contract

**User Story:** As a platform author, I want `platforms/eks` shaped exactly like `platforms/ecs`, so
that the EKS deployment is a definition revision an operator edits and re-applies, satisfying the
written provider contract.

#### Acceptance Criteria

1. THE package SHALL export a pure `platform() -> PlatformDeclaration` (no filesystem, provider-client,
   or network work at construction) supplying its namespaces (the EKS kinds, the
   `tokeira_deployment` server-config namespace, the platform's observability content namespace, and
   the AWS namespace), its probe, and its integration object.
2. THE package SHALL carry a catalog descriptor (`[package.metadata.tokeira.platform]`, id `eks`,
   engine pinned to the workspace version, definition seeds for both formats) so `tkp`/`tkr` discover
   it through the standard workspace catalog; no platform enum or dispatch table SHALL be edited.
3. THE deployment SHALL be authored as **modular definition sets in both frontends** — a root document
   wiring focused parts (`.tkd`) and a peer `.tkdp` projection — with every part documented for a cold
   operator (what it owns, what it authors vs derives, its dependencies and why).
4. THE package SHALL satisfy every applicable item of
   [docs/platforms/provider-contract.md](../../../docs/platforms/provider-contract.md), and its PR
   SHALL cite the checklist item-by-item.

### Requirement 2: `tkp` binds, dispatches, and live-applies the EKS platform

**User Story:** As an operator, I want `tkp` to drive the EKS deployment end-to-end — evaluate its
definition, plan, and apply live to AWS and Kubernetes — so that the provisioner owns the lifecycle.

#### Acceptance Criteria

1. WHEN `tkp` resolves a deployment bound to the `eks` platform id, THEN it SHALL evaluate the
   deployment's definition revision through the recorded frontend and construct the platform from its
   declaration, exactly as it does for `compose` and `ecs`.
2. WHEN an applying verb opens the EKS engines, THEN a live `KubePlatform` handle SHALL be registered
   onto the provision context's extension bag, and Kubernetes workload resources SHALL obtain it from
   the bag in their lifecycle methods, failing with a clear error if it is absent during apply.
3. WHEN `tkp apply` runs against an EKS deployment, THEN Kubernetes objects SHALL be applied live via
   server-side apply through the registered `KubePlatform`; there SHALL be no manifest-only or
   apply-deferred mode.
4. WHEN `tkp plan` runs with no reachable cluster, THEN it SHALL still produce a plan (K8s resource
   `describe` yielding absent produces Creates), mirroring how compose plans without Docker.
5. THE platform SHALL obtain the provisioner lifecycle by dispatch and SHALL NOT re-implement binding,
   locks, or provenance.

### Requirement 3: Infrastructure module staging

**User Story:** As an operator, I want the EKS infrastructure staged in the modular dependency order
the definition norm establishes, so that provisioning is minimal, legible, and correctly ordered.

#### Acceptance Criteria

1. THE infra module set SHALL be
   `remote_state → images → networking → dsql → cluster → observability → services`-shaped: exactly one
   dependency-free bootstrap module (`remote_state`, creating the S3 state bucket), `images` and
   `networking` after it, `dsql` after `networking`, `cluster` after `dsql`, `observability` after
   `cluster` and `images`, and the service workloads last.
2. THE module set SHALL contain no separate visibility module; visibility is served by the projection
   plane on DSQL (Requirement 7).
3. THE module dependency graph SHALL be a DAG with unique module names and unique resource ids, and
   module/resource dependencies SHALL point backward in declaration order.

### Requirement 4: AWS resources reused from `tokeira-aws`

**User Story:** As a platform author, I want the EKS platform to wire the existing `tokeira-aws`
resource implementations rather than port new ones, so that the AWS layer stays single-sourced.

#### Acceptance Criteria

1. THE EKS kinds SHALL realize to the existing `tokeira-aws` resources per the kind policy table and
   SHALL NOT introduce duplicate AWS resource implementations in `platforms/eks`.
2. WHERE a `tokeira-aws` resource is found to be missing a capability the EKS deployment needs, THE gap
   SHALL be filed and fixed in `tokeira-aws` (a shared-crate change), NOT worked around inside
   `platforms/eks`.
3. THE cluster/node IAM roles and per-service task roles SHALL carry only the trust and inline policies
   the deployed services require; the tokeirad task role SHALL include DSQL and DynamoDB access.
4. THE DSQL cluster kind SHALL operate managed or adopt a preexisting cluster per the shaped config
   enum (never inferred from optional-field presence).
5. THE EKS cluster version SHALL default to the latest EKS-supported Kubernetes version (1.36 as
   verified 2026-07; re-verify at implementation) and SHALL remain operator-configurable.

### Requirement 5: Kubernetes manifests for the tokeira pod shape

**User Story:** As a platform author, I want each tokeira service rendered as the proven pod shape, so
that services schedule on Graviton and collect telemetry the way the source deployment proved out.

#### Acceptance Criteria

1. WHEN a service Deployment is built, THEN it SHALL include an Alloy native sidecar (an init container
   with `restartPolicy: Always`), arm64 node affinity, pod anti-affinity by service label, a
   Pod-Identity `ServiceAccount`, and downward-API env exporting the pod IP as the broadcast address.
2. THE deployment's `tokeirad.toml` SHALL be authored as the `ServerConfig` node (`tokeira_deployment`
   namespace), whose dependency identity orders the service workloads; the EKS delivery mechanism
   SHALL be a ConfigMap mounted into the `tokeirad` pod and located by `TOKEIRA_CONFIG`, with
   `infrastructure.dsql.*` filled by writeback (Requirement 8). `tokeirad` reads its entire
   configuration from that file (`apps/tokeirad`: `TokeiraConfig::resolve`); the main container env
   SHALL carry only the config path and the downward-API broadcast address (`TOKEIRA_NODE_HOST`), no
   per-field DSQL env.
3. THE platform SHALL build a Karpenter `NodePool` constraining scheduling to arm64 on-demand instances
   of the configured families, referencing the EKS Auto Mode default NodeClass.
4. WHEN a service is exposed, THEN a ClusterIP `Service` SHALL publish its gRPC and metrics ports; a
   headless `Service` SHALL be provided only where the tokeira service's clustering model requires
   stable peer discovery (verified against tokeira's membership implementation).
5. THE Kubernetes namespaces required by the deployment SHALL be created as `iac::Resource`s depending
   on the EKS cluster.

### Requirement 6: tokeira service topology (real process topology)

**User Story:** As an operator, I want the EKS platform to deploy tokeira's actual process topology, so
that every deployed workload maps to a real tokeira binary.

_Ground truth: `tokeirad` (`apps/tokeirad`) is a single monolithic process — the full edge + runtime +
projection stack; there is no role selector. `tokeira-controller` and `tokeira-autoscaler` are separate
binaries._

#### Acceptance Criteria

1. THE deployed service set SHALL be: `tokeirad` (N controller-joined runtime replicas),
   `tokeira-controller`, `tokeira-autoscaler`, plus `mimir`, `loki`, `grafana` with Alloy sidecars.
   `tokeirad` SHALL NOT be split into per-role services; horizontal scale is additional replicas.
2. `tokeirad` SHALL join the controller via its configured controller endpoint (the ClusterIP DNS name
   of `tokeira-controller`), publish the canonical gRPC and metrics ports, and advertise its per-pod
   membership address via the downward-API broadcast env. `tokeira-controller` SHALL expose a ClusterIP
   Service; `tokeira-autoscaler` needs no inbound Service.
3. THE service startup/dependency ordering SHALL be a DAG: `tokeira-controller` before `tokeirad`;
   `mimir`/`loki` independent; `grafana` after `mimir`/`loki`; `tokeira-autoscaler` after
   `tokeira-controller` and `mimir`.

### Requirement 7: Visibility via the DSQL-backed projection plane

**User Story:** As an operator, I want tokeira's visibility served by its projection plane on the same
Aurora DSQL cluster, so that the deployment runs on a single datastore.

#### Acceptance Criteria

1. THE deployment SHALL serve both persistence and visibility from the single DSQL cluster the `dsql`
   module provisions, and SHALL provision no separate visibility or search datastore.
2. THE service configuration SHALL carry only the DSQL connection contract for persistence and
   projection.

### Requirement 8: Writeback and hydration

**User Story:** As an operator, I want discovered DSQL identity projected into the deployment's
`TokeiraConfig` after infra apply, so that running services connect to the right cluster and
coordination tables.

#### Acceptance Criteria

1. THE definition SHALL declare exactly the five writebacks of the writeback policy table (endpoint
   preferred from the connection endpoint's `private_hostname`, cluster endpoint fallback), and infra
   apply SHALL persist them into the deployment's `TokeiraConfig` through the standard platform-side
   writeback machinery; no other keys.
2. THE declared writeback keys SHALL be canonical `TokeiraConfig` keys (rejected otherwise by
   `deny_unknown_fields` on persist), identical to the compose/ecs key set.

### Requirement 9: Live day-2 operations

**User Story:** As an operator, I want live scale, logs, and port-forward against the EKS cluster
through the standard operations seam, so that a running deployment is fully operable.

#### Acceptance Criteria

1. THE declaration SHALL supply `Ops` implemented in provider terms: scale patches Deployment replicas
   and waits for readiness in tokeira's startup order (reverse order scaling down); logs return the
   service's recent logs from the live cluster; port-forward establishes a live forward via the `kube`
   client.
2. WHERE a day-2 verb requires a reachable cluster, THE platform SHALL surface a clear, actionable
   error when the cluster or credentials are unavailable, and SHALL NOT silently succeed.
3. THE scale verb SHALL admit zero as a target replica count for `tokeirad` and the observability
   services (execution state is durable in DSQL; scaling back up SHALL respect the startup ordering of
   Requirement 6.3). Scale-to-zero SHALL be refused for `tokeira-controller` while any `tokeirad`
   replica runs (runtime nodes need a reachable controller). Automatic scale-to-zero (HPA/KEDA wiring)
   is an operator concern outside this spec; the platform's contract is that manual zero is admissible
   and safe.

### Requirement 10: Private-only networking and least-privilege security

**User Story:** As a security-conscious operator, I want the EKS deployment private-only with
least-privilege identity, so that it exposes no public surface and uses no long-lived credentials.

#### Acceptance Criteria

1. THE VPC SHALL have no public subnets and no internet gateway; AWS service access SHALL be via VPC
   endpoints, and the EKS API SHALL be private.
2. THE deployment SHALL create no `Ingress`, `LoadBalancer` Service, or other internet-facing surface;
   operator access SHALL be via `port-forward` only.
3. Security-group ingress rules SHALL be scoped to specific sources (VPC CIDR or self-reference) and
   SHALL NOT use `0.0.0.0/0`.
4. Service pods SHALL obtain AWS credentials via Pod Identity with no OIDC provider and no static keys,
   and DSQL access SHALL use IAM authentication.

### Requirement 11: State persistence and deployment creation

**User Story:** As an operator, I want EKS deployment state persisted in S3 and a new deployment
created from the shipped definition set, so that state is durable and creation stages a working
definition.

#### Acceptance Criteria

1. THE EKS platform SHALL select the S3-native state store for infra and deploy state, keyed by project
   + environment, failing loudly when AWS clients are not registered.
2. WHEN `tkr … create` selects the eks platform, THEN it SHALL stage the shipped definition set for the
   chosen format — the root document and **all** companion parts and content — so the created
   deployment plans without further staging.

### Requirement 12: Dual-frontend parity and definition verification

**User Story:** As a reviewer, I want both frontends proven to produce the identical deployment and the
composition validated, so that the platform's desired state is trustworthy before any live apply.

#### Acceptance Criteria

1. THE package SHALL include a parity test proving the `.tkd` and `.tkdp` sets evaluate to identical
   configuration, graph (modules, resources, dependencies), writebacks, and infrastructure desired
   manifests, for the default and both DSQL configurations (the `platforms/ecs`
   `assert_definition_parity` pattern).
2. THE shipped definition sets SHALL evaluate in tests against the real declaration (module order,
   resource census, writeback keys), and the evaluated config SHALL equal the authored defaults.
3. Changing a `#[create]`-marked config field SHALL be refused as a retarget by the frontend's
   admission; non-`#[create]` changes SHALL reconcile.
4. Generated Kubernetes manifests SHALL round-trip through `serde_json` without loss, and the service
   dependency graph SHALL be acyclic.
