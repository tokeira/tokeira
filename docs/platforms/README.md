# Platforms

Tokeira deploys onto pluggable *platforms* — target environments the tooling
can provision, deploy to, and operate. Every platform runs the same `tokeirad`
binary under the same review contract (plan first, confirm, then apply); the
platform decides where processes run and what surrounds them. The lifecycle
itself is described in the
[deployment configuration guide](iac/configuration.md).

New here? Start with the [quick start](quick-start.md).

## Support matrix

| Platform | Runs `tokeirad` as | Storage | Observability stack | Best for |
|----------|--------------------|---------|---------------------|----------|
| [`local`](local/README.md) | Bare host process | in-memory or Aurora DSQL | None | Fastest dev loop |
| [`compose`](compose/README.md) | Docker Compose stack | in-memory or Aurora DSQL | Mimir, Loki, Grafana, Alloy | Integration work, local soak testing, dashboards |
| [`ecs`](ecs/README.md) | AWS ECS services on Graviton4, private subnets only | Aurora DSQL | Mimir, Loki, Grafana, Alloy | Production-shaped deployments |
| [`eks`](eks/README.md) | Kubernetes workloads on an EKS Auto Mode cluster (Graviton, private subnets) | Aurora DSQL | Mimir, Loki, Grafana, Alloy sidecars | Production-shaped Kubernetes deployments |

The classic platforms are created through `tkr`, choosing platform and storage
at creation time:

```bash
tkr deployment create --name <name> --platform <local|compose|ecs> --storage <in-memory|dsql>
```

The definition-driven platforms (`compose`, [`eks`](eks/README.md)) are
authored as a `definition.tkd` and operated by the provisioner `tkp` instead —
see [deployment definitions](iac/deployment-definitions.md).

## Platform crates

| Crate | Purpose |
|-------|---------|
| `platforms/local` | Bare-process local execution — spawns `tokeirad` directly |
| `platforms/compose` | Docker Compose stack with observability services (Mimir, Loki, Grafana, Alloy) |
| `platforms/compose` | The compose stack realized from an interpreted `definition.tkd` deployment definition — see [deployment definitions](iac/deployment-definitions.md) |
| `platforms/ecs` | AWS ECS: networking, DSQL, cluster, observability, and service modules |
| `platforms/eks` | The EKS platform, authored in the `syn` deployment DSL and driven by `tkp` — the Kubernetes sibling of `ecs` |

## See also

- [Quick start](quick-start.md)
- [Deployment configuration and the `tkr` command surface](iac/configuration.md)
- [Deployment definitions](iac/deployment-definitions.md) — the `.tkd` dialect and
  `tkp` provisioner lifecycle
- [Observability](observability.md)
