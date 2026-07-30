# Deployment Configuration

`tkr` is the single entry point for deployment lifecycle, infrastructure
management, and developer workflows. It manages named deployments stored under
`$XDG_STATE_HOME/tokeira/tkr/`.

## The deployment model

A *deployment* is a named pairing of a platform (`local`, `compose`, `ecs`)
with a storage engine (`in-memory`, `dsql`), plus everything provisioned for
it. Each deployment owns two configuration files:

- `deployment.toml` — platform intent: storage mode, region, module
  configuration, service images.
- `tokeirad.toml` — server runtime configuration consumed by `tokeirad`.
  Infrastructure steps write back discovered values (for example the DSQL
  endpoint) so the server boots against what was actually provisioned.

Commands act on the *active* deployment, selected with `tkr deployment use
<name>`. To guard against mis-applying to the wrong environment,
`tkr deployment lock [<name>]` pins every mutating command to one deployment
until `tkr deployment unlock --yes`.

### Definition-driven platforms

`compose` and [`eks`](../eks/README.md) are authored differently: the
whole deployment — infrastructure, services, storage, observability, wiring —
is one `definition.tkd` file interpreted by the provisioner `tkp`
(`init` / `plan` / `apply` / `destroy` / `revert` / `upgrade`), rather than
assembled from the `tkr` subcommands below. The review contract is identical:
plan first, confirm, then apply. See
[deployment definitions](deployment-definitions.md).

## Review before action

The CLI follows **plan → confirm → apply**; silent mutations are a bug.
`tkr infra plan` shows what would change before `tkr infra apply`;
`tkr deploy plan` does the same for service manifests. Destructive operations
(`infra destroy`, `deployment destroy`, `scale down`) require `--yes` or
interactive confirmation.

## Command surface

```
tkr
├── dev
│   └── build · test [--crate <name>] · check · lint · fmt · docs
├── deployment
│   ├── create --name <name> --platform <local|compose|ecs> --storage <in-memory|dsql> [--region <region>]
│   ├── list
│   ├── use <name>
│   ├── lock [<name>] · unlock --yes
│   └── destroy <name> --yes
├── image
│   ├── list [--source-type <build|mirror>] [--json]
│   ├── build [--arch <arm64|amd64>] [--tag <version>]
│   ├── push --tag <version> [--image <name>] [--yes]
│   └── mirror [--image <name>] [--yes]
├── infra
│   ├── plan [--module <name>]
│   ├── apply --yes [--module <name>]
│   ├── destroy --yes [--module <name>]
│   └── status
├── deploy
│   ├── plan
│   ├── apply --yes [--force]
│   └── status
├── schema
│   └── setup --yes · status · validate
├── scale
│   └── up [<service>] [<replicas>] · down [<service>] [<replicas>] · status
├── logs <service> [--follow] [--tail <n>]
├── port-forward <service> [--local-port <port>]
├── exec <service> [--container <name>] [-- <command...>]
├── config
│   └── show
├── compat
│   └── show [--json] · diff --a <left.json> --b <right.json>
├── ci
│   └── check · build · lock-update        (scaffolded until the Dagger module lands)
├── observability
│   └── check [--timeout-seconds <n>]
├── workstation
│   ├── up · stop · destroy --yes · ssh · status · list · bootstrap
│   ├── remote-exec [--workstation <id>] -- <command...>
│   ├── idle [--defer <duration>]
│   ├── github-key <add|remove|list>
│   └── code <sync|push> [--branch <name>]
├── admin <command...>
└── version [--verbose] [--json]
```

This tree is representative; `tkr --help` is authoritative.

## Lifecycle ordering

The full lifecycle is: build/mirror images → provision infrastructure → apply
schema → deploy services → scale. Platforms skip the stages they do not need:

| Platform | Ordering |
|----------|----------|
| [`local`](../local/README.md) | `deployment create` → `deploy apply` (no infra, no images) |
| [`compose`](../compose/README.md) | `image build` → `infra apply` → [`schema setup`] → `deploy apply` |
| [`ecs`](../ecs/README.md) | `image mirror` → `image build` + `image push` → `infra apply` → `schema setup` → `deploy apply` → `scale up` |
| [`eks`](../eks/README.md) | definition-driven: `tkp init` → `tkp plan` → `tkp apply` (provisions AWS and applies to the cluster in one lifecycle) |

Infrastructure is organized into named modules (for example `dsql`,
`observability`, `services`), applied in dependency order and destroyed in
reverse; `--module <name>` scopes a plan, apply, or destroy to one module. The
per-platform guides walk each lifecycle end to end.

## Image management

`tkr image` manages the image plane: building `tokeirad` from source, pushing
built images to ECR, and mirroring upstream observability images into
project-owned ECR.

```bash
# Build tokeirad:latest without requiring an active deployment
tkr image build

# Build an amd64 image and additionally tag it
tkr image build --arch amd64 --tag v1.2.3

# Enumerate mirror images for the active deployment
tkr image list --source-type mirror --json

# Mirror every upstream observability image into project-owned ECR (ECS only)
tkr image mirror --yes

# Push tokeirad to ECR and write back services.*.image fields (ECS only)
tkr image push --tag v2026-03-21 --yes
```

Lifecycle ordering is explicit. For ECS, run `tkr image mirror` before
`tkr infra apply`, and run `tkr image build` plus `tkr image push --tag
<version>` before `tkr deploy apply`. For compose, run `tkr image build` before
`tkr deploy apply` so `tokeirad:latest` exists locally.

Image commands that build, push, or mirror use Dagger. Install Dagger 0.20 or
newer; the CLI re-executes under `dagger run` when a Dagger session is absent.
ECS push and mirror also require AWS credentials with ECR permissions:
`ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:PutImage`,
`ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`,
`ecr:CreateRepository`, `ecr:DescribeRepositories`, `ecr:PutLifecyclePolicy`,
`ecr:TagResource`, `ecr:ListTagsForResource`, and `ecr:GetLifecyclePolicy`.

To add a new image, start in `platforms/{compose,ecs}/src/images/` and
implement `tokeira_deploy_engine::image::Image`. Build recipes live as
hardcoded free functions in `tokeira-build`.

## Schema

DSQL-backed deployments apply schema migrations explicitly: `tkr schema setup
--yes` applies all migrations against the written-back endpoint, `tkr schema
status` reports the applied version, and `tkr schema validate` checks the
migration set itself. Run `schema setup` after the storage module is
provisioned and before the first `deploy apply` — `tokeirad` fails fast at boot
if the endpoint or schema is missing. The storage design, including the
forward-only migration discipline, is described in
[DSQL storage](../../architecture/050-dsql-storage.md).

## Operating a deployment

Day-2 operation uses the same CLI against the active deployment:

| Command | What it does |
|---------|--------------|
| `tkr scale up/down/status` | Scale services and report replica state; `scale down` requires confirmation |
| `tkr logs <service> [--follow] [--tail <n>]` | Stream or tail a service's logs |
| `tkr port-forward <service> [--local-port <p>]` | Reach private services (Grafana, Mimir, Loki, the gRPC edge) — SSM-based on ECS, no public endpoints |
| `tkr exec <service> [-- <cmd>]` | Interactive shell or one-shot command in a running ECS container (ECS Exec) |
| `tkr admin <command...>` | One-shot admin operations (`schema status`, `diagnostics runtime`, …) — scales the admin service 0→1, runs, scales back to 0 |
| `tkr schema setup/status/validate` | Apply and inspect DSQL schema migrations |
| `tkr config show` | Show the active deployment's effective configuration |
| `tkr observability check` | Validate observability configuration and smoke-check reachable backends |
| `tkr compat show` | Inspect the binary's Temporal compatibility matrices |

On the definition-driven platforms the same day-2 verbs — `scale`, `logs`,
`port-forward` — run through `tkp` against the live cluster.

## The deployment stack

The deployment tooling is its own set of crates, separate from the engine:

| Crate | Purpose |
|-------|---------|
| `tokeira-state` | Deployment state persistence — CAS store and S3 store |
| `tokeira-iac` | Generic infrastructure lifecycle engine — plan/apply/destroy with dependency ordering |
| `tokeira-deploy-engine` | Service lifecycle engine — manifest planning, platform apply, image tracking |
| `tokeira-config` | Server runtime configuration — TOML loading, validation, redaction |
| `tokeira-orchestrator` | Deployment orchestration facade — connects IaC and deploy engines to platform specializations |
| `tokeira-compose` | Docker Compose provider — bollard-based container lifecycle |
| `tokeira-aws` | AWS resource implementations |

## See also

- [Platform support matrix](../README.md) and the per-platform
  guides: [local](../local/README.md), [compose](../compose/README.md),
  [ECS](../ecs/README.md)
- [Deployment definitions](deployment-definitions.md) — the
  `.tkd` dialect and `tkp` provisioner lifecycle
