# ECS platform

The ECS platform is a definition-backed AWS provider for a private ECS-on-EC2
deployment: Aurora DSQL, one capacity provider per workload family, Service Connect,
an internal ALB for the two edge services, and Mimir/Loki/Grafana/Alloy observability.
The bound `tkp` owns interpretation and lifecycle; `platforms/ecs` supplies the source
set and declaration, while `crates/tokeira-ecs` supplies realization.

This page records the M1 audit against the reusable
[platform provider contract](../provider-contract.md). It is an implementation status
statement, not a live-AWS acceptance result.

## Definition source set

The default TKD source is modular:

- `deployment.tkd` — operator defaults and the cross-part wiring diagram;
- `platform.tkd` — configuration-only structs and enums;
- `helpers.tkd` — repeated security-group, capacity-plane, role, and workload
  assemblies;
- `remote_state.tkd` — the bootstrap bucket;
- `images.tkd` — project-owned ECR repositories;
- `networking.tkd` — private endpoints and internal ALB listener;
- `dsql.tkd` — managed/adopted DSQL and private endpoint identities;
- `cluster.tkd` — ECS cluster defaults;
- `observability.tkd` — storage and shipped observability content; and
- `services.tkd` — Service Connect and autoscaler wiring.

The catalog also stages `observability/` as companion content, so retained revisions
render and upload their own dashboards and alert rules. The definition tests evaluate,
verify, and realize both DSQL provenance modes through the real declaration without AWS
credentials.

ECS does not yet advertise a `.tkdp` root. The Python frontend supports companion
parts, but its create-time retarget admission surface has not landed yet. A faithful
peer is therefore a larger slice: the complete ten-document translation, exact
graph/config/manifest parity tests, and frontend admission rather than a partial or
monolithic source that weakens the shipped TKD contract.

## Configuration and writeback

`tokeirad.toml` is represented by the deployment-owned `ServerConfig` kind. DSQL infra
apply writes only canonical `TokeiraConfig` paths:

- `infrastructure.storage`;
- `infrastructure.dsql.endpoint` and `.region`;
- `infrastructure.dsql.runtime_role_arn` and `.admin_role_arn`; and
- `infrastructure.dsql.rate_limiter_table` and `.conn_lease_table`.

Core server workloads declare the `ServerConfig` resource as an infrastructure
dependency. Their ECS task definitions receive the same generic loader contract used by
the binaries: `TOKEIRA_CONFIG=env:TOKEIRA_CONFIG_CONTENT`, the document content, and a
digest that makes configuration movement explicit in the desired manifest. Controller
and autoscaler use different config schemas and remain a named follow-up; injecting
`TokeiraConfig` into them would be a type error, not reuse.

## Network and workload realization

All tasks use `awsvpc` in private subnets with public IP assignment disabled. Every
workload manifest resolves its subnet set and workload-family security group from
recorded infrastructure state. Security-group ingress uses the private VPC CIDR because
Service Connect, ALB, and dependency traffic crosses workload-family groups; a
self-source rule would admit only same-group traffic.

The edge API and poll services resolve and attach their IP target groups. Create and
update reconcile task definition, capacity provider, scheduling, placement constraint,
Service Connect, private network configuration, load balancer, execute-command policy,
and desired replicas. Live drift compares the same owned surface against the latest task
definition revision. Delete is idempotent and treats an absent or inactive service as
complete.

## Provider-contract verdict

| Contract item | Verdict | Current evidence |
|---|---|---|
| Pure declaration | **MEETS** | [`platform()`](../../../platforms/ecs/src/lib.rs) assembles four namespaces plus execution/integration without I/O. |
| Typed kinds and explicit graph dependencies | **MEETS** | [`kinds`](../../../crates/tokeira-ecs/src/kinds/mod.rs), [`EcsWorkload`](../../../crates/tokeira-ecs/src/kinds/workload.rs), and the evaluated [definition test](../../../platforms/ecs/tests/definition.rs). |
| Probe semantics | **MEETS, documented substrate deviation** | [`EcsExecution`](../../../crates/tokeira-ecs/src/execution.rs) returns no deployment-wide issue because region is manifest-owned and AWS reachability is operation-local. |
| Standard integration contexts | **MEETS** | [`EcsIntegration`](../../../crates/tokeira-ecs/src/execution.rs) relies on the framework-installed `AwsClients` bundle selected by the `tokeira_aws` namespace. |
| Logs/ports/scale operations | **FALLS SHORT — framework finding** | The declaration deliberately has `ops: None`: [`DeploymentRef`](../../../crates/tokeira-platform/src/declaration.rs) carries name and directory, not the authored region/cluster required for an AWS query. Smallest follow-up: give `Ops` a read-only admitted-definition/config view, then implement all three verbs together. |
| Canonical server config and writeback | **MEETS for `TokeiraConfig` consumers** | Shared [`ServerConfig`](../../../crates/tokeira-deployment/src/server_config.rs), ECS [manifest delivery](../../../crates/tokeira-ecs/src/services.rs), and canonical writebacks in [`deployment.tkd`](../../../platforms/ecs/deployment.tkd). Controller/autoscaler documents remain below. |
| IaC lifecycle and describe honesty | **FALLS SHORT** | Core AWS resources describe live state, but [`ObservabilityArtifacts`](../../../platforms/ecs/src/observability.rs) and its SSM-backed Alloy config can return `DescribeResult::Unsupported`; persisted state is retained honestly, but out-of-band object/parameter drift is not visible. |
| Self-describing deploy plane | **MEETS** | [`EcsWorkload::manifests`](../../../crates/tokeira-ecs/src/services.rs) resolves roles, private network, edge target group, and config content from recorded infra state. |
| Rollout, live drift, and delete | **MEETS** | [`EcsPlatform`](../../../crates/tokeira-ecs/src/execution.rs) reconciles create/update, compares live service ownership including the latest task revision, and implements idempotent delete. |
| Catalog and companion content | **MEETS for TKD** | [`Cargo.toml`](../../../platforms/ecs/Cargo.toml) declares the default root and `observability` content; the source set is retained by the framework. |
| Modular, documented, realized definition | **MEETS for TKD** | The root and nine focused parts describe ownership and dependencies; tests evaluate both managed and adopted DSQL and realize every kind. |
| TKD/TKDP parity and cross-format retarget | **ABSENT — frontend finding** | No ECS `.tkdp` source is advertised, and [`DefinitionFrontend::retarget_check`](../../../crates/tokeira-platform/src/definition.rs) records that TKDP create-time admission has not landed. This is a sized follow-up, not an inferred parity claim. |

## Named follow-ups

1. **ECS definition TKDP peer and admission:** land TKDP create-time retarget admission,
   translate the complete modular source set, and add exact config, graph, writeback,
   desired-manifest, and retarget parity tests.
2. **Service-owned auxiliary config documents:** add deployment-owned graph nodes and
   ECS delivery for `controller.toml` and `autoscaler.toml`; do not pass
   `TokeiraConfig` to binaries that reject that schema.
3. **AWS generated-content describes:** use `HeadObject`/`GetParameter` to make
   observability artifact and Alloy parameter drift live-visible.
4. **Definition-aware Ops framework seam:** expose admitted read-only authored
   coordinates to `Ops`, then restore ECS logs, port mappings, and scale through the
   declaration rather than the legacy route.
5. **M2 live-AWS acceptance:** validate endpoint reachability, IAM policy sufficiency,
   ALB registration/health, Service Connect, rollout convergence, and teardown in an
   operator-driven AWS environment.

## See also

- [Definition-backed provider contract](../provider-contract.md)
- [Deployment definition programming guide](../../provisioning/deployment-definitions.md)
- [Production observability](../observability.md)
