# Platforms

The platform architecture is a matched custom language and engine: every platform
supported by `tokeirad` should define its own TKD vocabulary and ship the
platform-specific, provenance-bound `tkp` that interprets and realizes it. `tkr`
constructs or obtains that engine, applies admission policy, places it with the
deployment, and verifies the married bytes for versioned mutation.

Current operator coverage does not yet implement that contract uniformly. Compose and
ECS have definition-backed TKD/TKP chains. Local remains available through compiled
in-process `tkr` handlers, while EKS currently supplies bridge and kind components
without a complete platform engine or operator route. The matrix below is a statement
of present mechanics, not an alternative platform architecture.

New here? Start with the [quick start](quick-start.md). For the language binding,
provenance chain, and command model, read [Provisioning](../provisioning/README.md).

## Support matrix

| Platform | Runs `tokeirad` as | Storage | Operator path | Status |
|---|---|---|---|---|
| [`local`](local/README.md) | Bare host process | In-memory or Aurora DSQL | `deployment.toml`, in-process `tkr` handlers | Available |
| [`compose`](compose/README.md) | Docker containers with Mimir, Loki, Grafana, and Alloy | In-memory or Aurora DSQL | `definition.tkd`, deployment-local Compose `tkp` | Available |
| [`ecs`](ecs/README.md) | AWS ECS services in private subnets | Aurora DSQL | `deployment.tkd` / `definition.tkdp`, deployment-local ECS `tkp` | Available (live-AWS acceptance pending) |
| [EKS components](eks/README.md) | Kubernetes-oriented platform vocabulary | Aurora DSQL model | TKD bridge and kinds only | No complete provisioner or operator route |

Create one of the available operator platforms with:

```bash
tkr deployment create \
  --name <name> \
  --platform <local|compose|ecs> \
  --storage <in-memory|dsql>
```

Compose and ECS creation seed their TKD source set, place a deployment-local `tkp`, and
forward supported lifecycle commands. Local creation seeds `deployment.toml` and
executes inside `tkr`. The file layouts and routing table are in
[deployment configuration](../provisioning/deployment-configuration.md).

## Platform crates

| Crate | Purpose |
|---|---|
| `platforms/local` | Bare-process local execution and in-process platform configuration. |
| `platforms/compose` | Interpreted Compose definition, `HostBridge`, orchestrator adapter, `ProvisionerPlatform`, and `tkp` binary. |
| `platforms/ecs` | Interpreted ECS definition, AWS bridge, orchestrator adapter, `ProvisionerPlatform`, and `tkp` binary. |
| `platforms/eks` | EKS `HostBridge` and AWS/Kubernetes kind vocabulary; it does not assemble a complete `ProvisionerPlatform` or `tkp`. |

A platform bridge is one implementation component, not an availability claim. A complete
definition-backed platform also needs a builder-to-orchestrator adapter, provider-ready
resource realization, a `ProvisionerPlatform`, a `tkp` binary target, and `tkr` routing.
See
[how a platform supplies a custom TKD](../provisioning/deployment-definition-patterns.md#how-a-platform-supplies-a-custom-tkd).

## See also

- [Quick start](quick-start.md)
- [Provisioning](../provisioning/README.md) — the `tkr`/`tkp`/`tkd` triad
- [Deployment configuration](../provisioning/deployment-configuration.md) — registry,
  file layouts, and command surface
- [Deployment definition programming guide](../provisioning/deployment-definitions.md) —
  abstract language and authoring rules
- [Definition patterns and current practice](../provisioning/deployment-definition-patterns.md) —
  source-backed bridge, adapter, and platform assembly
- [Observability](observability.md)
