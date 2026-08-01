# ECS Platform

The ECS platform deploys Tokeira on AWS ECS with Graviton4 instances, Aurora
DSQL persistence, and a full observability stack (Mimir, Loki, Grafana, Alloy).
All services run in private subnets with no public ingress — operator access is
via SSM Session Manager port forwarding and ECS Exec.

Its current operator implementation uses `deployment.toml` and compiled in-process `tkr`
handlers. It does not yet supply the custom TKD vocabulary and provenance-bound platform
`tkp` required by the uniform platform architecture; this page documents the available
current route rather than a second platform contract.

## Lifecycle

```bash
# Create
tkr deployment create --name prod --platform ecs --storage dsql --region eu-west-2

# Mirror upstream observability images into project-owned ECR
tkr image mirror --yes

# Build and push tokeirad to ECR
tkr image build --tag v2026-05-22
tkr image push --tag v2026-05-22 --yes

# Provision infrastructure (VPC, DSQL, ECS cluster, ALB, observability)
tkr infra plan
tkr infra apply --yes

# Apply DSQL schema migrations
tkr schema setup --yes

# Deploy services (starts at 0 replicas, then scales up)
tkr deploy apply --yes
tkr scale up

# Operations
tkr scale status
tkr logs edge-api --follow --tail 50
tkr logs runtime --tail 20

# Port forwarding (via SSM — no public endpoints)
tkr port-forward grafana                    # localhost:3000 → Grafana
tkr port-forward edge-api                   # localhost:7233 → gRPC frontend
tkr port-forward edge-api --local-port 8080 # custom local port
tkr port-forward mimir                      # localhost:9009 → Mimir query
tkr port-forward loki                       # localhost:3100 → Loki query

# Remote exec into a running container
tkr exec runtime                            # interactive shell in runtime container
tkr exec edge-api -- cat /etc/tokeirad.toml # run a command
tkr exec grafana --container grafana        # specify container name

# Admin commands (scales admin service 0→1, executes, scales back to 0)
tkr admin schema status
tkr admin diagnostics runtime

# Module-scoped infrastructure operations
tkr infra apply --yes --module dsql
tkr infra apply --yes --module observability
tkr infra destroy --yes --module observability

# Observability smoke test
tkr observability check

# Tear down
tkr scale down
tkr infra destroy --yes
tkr deployment destroy --name prod --yes
```

## Infrastructure modules

The ECS platform organizes infrastructure into ordered modules:

| Module | Resources |
|--------|-----------|
| **networking** | VPC, private subnets, NAT Gateway, VPC interface endpoints (ECS, ECR, S3, SSM, Cloud Map) |
| **dsql** | Aurora DSQL cluster (managed or preexisting), DSQL VPC endpoints, IAM roles (runtime + admin) |
| **cluster** | ECS cluster, capacity providers (ASGs per service class), Service Connect namespace |
| **observability** | Mimir, Loki, Grafana, Alloy services + dashboards + alert rules |
| **services** | Tokeira application services (edge-api, edge-poll, runtime, projection, controller, autoscaler, admin) |

Modules are applied in dependency order and destroyed in reverse.

## Service topology

| Service | Capacity Provider | Instance Type | Replicas | Purpose |
|---------|-------------------|---------------|----------|---------|
| edge-api | edge-api | c8g.large | 2 | gRPC frontend (SDK clients) |
| edge-poll | edge-poll | c8g.large | 2 | Worker polling endpoint |
| runtime | runtime | c8g.large | 3 (daemon) | Lane execution, timers, dispatch |
| projection | projection | c8g.large | 1 | Visibility workers |
| controller | control | c8g.large | 1 | Placement controller |
| autoscaler | control | c8g.large | 1 (co-located) | Scaling decisions |
| admin | control | c8g.large | 0 (on-demand) | Schema migrations, diagnostics |
| mimir | mimir | m8g.large | 1 | Metrics store |
| loki | loki | m8g.large | 1 | Log store |
| grafana | grafana | c8g.medium | 1 | Dashboards |

## Recommended ECS + DSQL lifecycle

```bash
# 1. Create deployment config
tkr deployment create --name prod --platform ecs --storage dsql --region eu-west-2

# 2. Mirror observability images to ECR (Mimir, Loki, Grafana, Alloy, AWS CLI, BusyBox)
tkr image mirror --yes

# 3. Build and push tokeirad
tkr image build --tag v2026-05-22
tkr image push --tag v2026-05-22 --yes

# 4. Provision infrastructure
tkr infra apply --yes

# 5. Apply schema
tkr schema setup --yes

# 6. Deploy and scale
tkr deploy apply --yes
tkr scale up

# 7. Verify
tkr observability check
tkr port-forward grafana
```

## Port forwarding

Port forwarding uses SSM Session Manager — no public endpoints, no SSH keys, no
bastion hosts. Requires `session-manager-plugin` installed locally and VPC
network access (the NAT Gateway provides outbound for SSM).

Available services: `grafana`, `edge-api`, `edge-poll`, `controller`, `mimir`,
`loki`.

## Remote exec

`tkr exec` uses ECS Exec (backed by SSM) to run commands inside running
containers. Each service's task definition has `enableExecuteCommand = true`
and the init process enabled.

```bash
tkr exec <service>                    # interactive /bin/sh
tkr exec <service> -- <command...>    # run a command and exit
tkr exec <service> --container <name> # target a specific container (e.g., alloy sidecar)
```

## Admin commands

`tkr admin` is a convenience wrapper for one-shot administrative operations. It
scales the `admin` service from 0→1, waits for the task to reach RUNNING,
executes the command via ECS Exec, streams output, then scales back to 0. This
avoids keeping an admin container running permanently.

```bash
tkr admin schema setup
tkr admin schema migrate 5
tkr admin diagnostics runtime
```

## See also

- [Platform support matrix](../README.md)
- [Production observability](../observability.md)
- [Deployment configuration and the `tkr` command surface](../../provisioning/deployment-configuration.md)
