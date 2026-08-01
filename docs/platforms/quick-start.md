# Quick Start

From a clean checkout to a running Temporal-compatible server:

```bash
# Install the operator CLI (building requires protoc — see the development guide)
cargo install --path apps/tkr

# Create and start a local deployment
tkr deployment create --name dev --platform local --storage in-memory
tkr deploy apply --yes
```

That is the whole thing: `tokeirad` is now running as a host process, serving
the Temporal gRPC surface — point a Temporal SDK worker at it. No containers,
no cloud account, no schema step. `tkr config show` prints the effective
configuration; Ctrl-C stops the server.

## Where next

- [Platform support](README.md) — Local, Compose, and ECS operator paths, plus
  the implementation status of the EKS components.
- [Provisioning](../provisioning/README.md) — the `tkr`/`tkp`/`tkd` triad.
- [Deployment configuration](../provisioning/deployment-configuration.md) — the
  deployment model and `tkr` command surface.
- [Development guide](../development.md) — prerequisites and the build/test
  loop for working on Tokeira itself.
