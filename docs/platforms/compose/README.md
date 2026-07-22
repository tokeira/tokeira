# Compose Platform

The compose platform runs a full Docker Compose stack: `tokeirad` plus an
observability suite (Mimir, Loki, Grafana, Alloy). Requires Docker.

## Lifecycle

```bash
# Create
tkr deployment create --name dev-compose --platform compose --storage in-memory

# Build the tokeirad image (compose reads it from the local Docker image store)
tkr image build

# Provision infrastructure (creates containers via bollard)
tkr infra plan
tkr infra apply --yes

# Deploy services
tkr deploy apply --yes

# Operations
tkr scale status
tkr logs tokeirad --follow --tail 50
tkr logs grafana --tail 20
tkr port-forward grafana

# Module-scoped operations
tkr infra apply --yes --module observability
tkr infra destroy --yes --module observability

# Tear down
tkr infra destroy --yes
tkr deployment destroy dev-compose --yes
```

## Modules

The compose platform organizes services into two modules:

- **runtime** — `tokeirad`
- **observability** — `mimir`, `loki`, `grafana`, `alloy` (pinned to
  `grafana/mimir:3.0.6`, `grafana/loki:3.7.1`, `grafana/grafana-oss:12.4.3`,
  `grafana/alloy:v1.16.0`)

## Why the build step is separate

`tkr deploy apply` does not invoke the image builder — it requires
`tokeirad:latest` to already exist in the local Docker image store. This keeps
the deploy path deterministic and fast: a repeat deploy does not rebuild.
Re-run `tkr image build` whenever you want a fresh `tokeirad` binary in the
compose stack, then `tkr deploy apply --yes --force` to recreate services that
sit behind an unchanged tag.

## Storage and schema

The lifecycle above uses `--storage in-memory`, which needs no schema setup.
Compose also supports Aurora DSQL through the `dsql` infrastructure module.
DSQL deployments use `deployment.toml` for platform storage intent and
`tokeirad.toml` writeback for the server runtime endpoint/region.

### Recommended compose + DSQL lifecycle

```bash
# Create DSQL-backed compose config. Region defaults to us-east-1.
tkr deployment create --name dev-dsql --platform compose --storage dsql --region us-east-1

# Build the local runtime image used by compose.
tkr image build

# Provision or adopt only the DSQL cluster first.
tkr infra apply --yes --module dsql

# Apply all DSQL migrations against the written-back endpoint.
tkr schema setup --yes

# Provision observability/runtime resources and deploy services.
tkr infra apply --yes
tkr deploy apply --yes
```

The two-phase infra flow keeps storage provisioning separate from service
startup: `tokeirad` connects to DSQL during boot and will fail fast if the
endpoint or schema is missing. A one-shot `tkr infra apply --yes` also works,
but run `tkr schema setup --yes` before `tkr deploy apply`.

### Preexisting clusters

For preexisting clusters, set `[dsql] mode = "preexisting"` and
`endpoint = "...dsql.<region>.on.aws"` in `deployment.toml` before
`tkr infra apply --module dsql`. The module records the endpoint and skips
provider deletion. AWS credentials must be available through the standard local
provider chain; compose mounts `~/.aws` into the `tokeirad` container and
forwards simple provider-chain environment variables.

## See also

- [Platform support matrix](../README.md)
- [Production observability](../observability.md) — what the stack
  collects, provisions, and alerts on
- [Compose + DSQL performance analysis](../../compose-dsql-performance.md)
- [Deployment definitions](../iac/deployment-definitions.md) — the `compose-syn`
  platform realizes the same stack from an interpreted `definition.tkd`
