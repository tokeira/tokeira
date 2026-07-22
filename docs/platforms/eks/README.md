# EKS Platform

The EKS platform provisions Tokeira on AWS EKS with Aurora DSQL. It deploys
the same decomposed service topology as the [ECS platform](../ecs/README.md), but it is
authored and driven differently: the entire deployment — infrastructure,
services, storage, observability, wiring — is described by one
`definition.tkd` file in the `syn` deployment DSL and operated end to end by
the deployment provisioner `tkp`. See
[deployment definitions](../iac/deployment-definitions.md) for the dialect and the
provisioner lifecycle.

## Shape

- **EKS Auto Mode** cluster (Kubernetes 1.36 by default, operator-
  configurable) with a `karpenter.sh/v1` NodePool referencing the EKS-managed
  default NodeClass, pinned to arm64 — services run on Graviton
  (`m8g`/`c8g`/`r8g` families) with explicit node affinity.
- **Private-only AWS foundation**: VPC with private subnets and interface
  endpoints, security groups, IAM roles, ECR, DynamoDB coordination tables,
  Secrets Manager, and S3 deployment state — no public ingress.
- **Aurora DSQL** persistence (managed, or adopted preexisting), reached
  through a PrivateLink connection endpoint with IAM-token authentication.
- **Pod Identity** ServiceAccounts for AWS access from pods — no static
  credentials in the cluster.
- The Tokeira service set — `edge-api`, `edge-poll`, `runtime`, `projection`,
  `controller`, `autoscaler`, `admin` — plus the observability stack
  (`mimir`, `loki`, `grafana`), with **Alloy running as a native sidecar** in
  each pod.

## Lifecycle

An operator edits the `.tkd` definition and re-applies; the provisioner owns
the whole loop:

```bash
tkp init --deployment-dir ~/deployments/prod-eks    # bind the definition, create revision 1
tkp plan --deployment-dir ~/deployments/prod-eks    # read-only preview of what would change
tkp apply --deployment-dir ~/deployments/prod-eks   # provision AWS, server-side apply to EKS
```

`tkp` carries the full lifecycle — `init` / `plan` / `apply` / `destroy` /
`revert` / `upgrade` / `rollback` — with every applied definition retained as
a config revision that `revert` can restore. Day-2 `scale` (respecting
Tokeira's startup order), `logs`, and `port-forward` run live against the
cluster.

## See also

- [Platform support matrix](../README.md)
- [Deployment definitions](../iac/deployment-definitions.md) — the `.tkd` dialect
  and the `tkp` provisioner lifecycle
- [ECS platform](../ecs/README.md) — the same topology on ECS, `tkr`-driven
- [Production observability](../observability.md)
