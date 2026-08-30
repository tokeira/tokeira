# ECS platform

The ECS platform is a definition-backed AWS provider for a private ECS-on-EC2
deployment: Aurora DSQL, one capacity provider per workload family, Service Connect,
an internal ALB for the two edge services, and Mimir/Loki/Grafana/Alloy observability.
The bound `tkp` owns interpretation and lifecycle; `platforms/ecs` supplies the source
sets and declaration, while `crates/tokeira-ecs` supplies realization.

## Definition source set

Both definition formats are modular peer projections. The Rust root is
`deployment.tkd`; the Python root is `definition.tkdp`. Each resolves the same focused
parts in its own format:

- `deployment.tkd` / `definition.tkdp` — operator defaults and the cross-part wiring
  diagram;
- `platform.tkd` / `platform.tkdp` — configuration-only structs, enums, and
  dataclasses;
- `helpers.tkd` / `helpers.tkdp` — repeated security-group, capacity-plane, role, and
  workload assemblies;
- `remote_state.tkd` / `remote_state.tkdp` — the bootstrap bucket;
- `images.tkd` / `images.tkdp` — project-owned ECR repositories;
- `networking.tkd` / `networking.tkdp` — private endpoints and internal ALB listener;
- `dsql.tkd` / `dsql.tkdp` — managed/adopted DSQL and private endpoint identities;
- `cluster.tkd` / `cluster.tkdp` — ECS cluster defaults;
- `observability.tkd` / `observability.tkdp` — storage and shipped observability
  content; and
- `services.tkd` / `services.tkdp` — Service Connect and autoscaler wiring.

The catalog also stages `observability/` as companion content, so retained revisions
render and upload their own dashboards and alert rules. Definition tests evaluate,
verify, and realize both formats through the real declaration without AWS credentials,
and compare their configuration, graph, writeback, and infrastructure desired-manifest
projections.

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
and autoscaler use different config schemas; they do not receive this document because
injecting `TokeiraConfig` into those binaries would be a type error.

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

## Current limitations

- The Python frontend does not yet enforce the Rust frontend's `#[create]` retarget
  admission. Cross-format source and realized-manifest parity is tested, but a Python
  definition edit currently relies on kind validation rather than create-time field
  refusal.
- The declaration has no logs, port, or scale operations. Those AWS queries require
  authored region and cluster coordinates, while the current
  [`DeploymentRef`](../../../crates/tokeira-platform/src/declaration.rs) carries only
  deployment name and directory.
- [`ObservabilityArtifacts`](../../../platforms/ecs/src/observability.rs) and the
  SSM-backed Alloy configuration can return `DescribeResult::Unsupported`; persisted
  state remains available, but out-of-band object or parameter drift is not visible.
- Controller and autoscaler still need service-owned configuration documents matching
  their own schemas.
- Live AWS verification covers endpoint reachability, IAM sufficiency, ALB health,
  Service Connect, rollout convergence, and teardown; it is operator-driven rather than
  part of the hermetic test suite.

## See also

- [Definition-backed provider contract](../provider-contract.md)
- [Deployment definition programming guide](../../provisioning/deployment-definitions.md)
- [Production observability](../observability.md)
