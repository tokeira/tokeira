# Tokeira

A Temporal-compatible durable execution engine built in Rust and specialized for Aurora DSQL.

Tokeira preserves the public Temporal contract that SDKs, operators, and tooling depend on — WorkflowService, OperatorService, workflow history semantics, replay model, task-start/completion semantics, sticky execution, polling, retries, signals, timers, Continue-As-New, and archival — while changing the internal architecture to collapse correctness around a single authoritative per-run transition log.

Tokeira currently tracks Temporal API `v1.62.11`; the compatibility smoke target is `temporalio-sdk` v0.4 worker liveness against the in-memory server.

This is not a service-by-service port of Temporal's Frontend / History / Matching / Worker layout. Workflow durability comes from event history; queue delivery is an implementation detail with weaker ordering guarantees. Tokeira makes that distinction explicit: per-workflow event history is the only semantic ordering domain, and everything else — internal queue ordering, delivery ordering, visibility update ordering — becomes derived.

## Design Principles

**History is the authority.** Every state-changing request becomes a per-run transition that appends history, updates the run summary, and emits derived effects atomically. The system never relies on an external queue write as the canonical record that work exists.

**Per-run total order, not global total order.** Tokeira enforces a total order per workflow run, plus explicit causal edges across runs and side effects. Queue delivery and visibility are derived domains.

**Delivery is ephemeral-first.** Worker polling and sync matching live primarily in memory. Durable backlog is a fallback and recovery aid, not the default path.

**Visibility is a projection.** The projection plane owns read models and operates outside the correctness path. A lagging projection is a quality problem, not a correctness failure.

**The kernel is pure.** The deterministic state machine transforms commands into transitions with no I/O, no storage access, and no delivery concerns.

**Configuration stays minimal.** Prefer policies and auto-tuning over exposed mechanical knobs.

## Architecture

Tokeira is organized into three planes:

**Compatibility edge** — admits and translates requests. Exposes WorkflowService, OperatorService, and health endpoints. Performs authn/authz, namespace lookup, and request ID handling. Gates long polls before they reach deeper runtime resources.

**Authoritative runtime and storage** — owns correctness. Shard/bundle ownership and fencing, lane-local execution of workflow actors, durable state transitions, durable timers, activity state, task-start validation, and derived dispatch intents.

**Projection plane** — owns read models. SQL visibility, rollups, operational summaries, and custom sinks with independent checkpoints and replay.

## Workspace

### Core Crates

| Crate | Purpose |
|-------|---------|
| `tokeira-types` | Shared identifiers and durable-domain value types |
| `tokeira-proto` | Wire types for public and internal control-plane protocols |
| `tokeira-kernel` | Pure deterministic workflow transition engine |
| `tokeira-storage` | Persistence interfaces, in-memory dev store, and DSQL storage |
| `tokeira-runtime` | Lane-based orchestration, delivery broker, sweepers, timer scanners |
| `tokeira-edge` | Compatibility shell — thin translation layer for public APIs |
| `tokeira-projection` | Projection workers, visibility query service, and visibility API types |

### Infrastructure Crates

| Crate | Purpose |
|-------|---------|
| `tokeira-state` | Deployment state persistence — CAS store and S3 store |
| `tokeira-iac` | Generic infrastructure lifecycle engine — plan/apply/destroy with dependency ordering |
| `tokeira-deploy-engine` | Service lifecycle engine — manifest planning, platform apply, image tracking |
| `tokeira-config` | Server runtime configuration — TOML loading, validation, redaction |
| `tokeira-orchestrator` | Deployment orchestration facade — connects IaC and deploy engines to platform specializations |
| `tokeira-compose` | Docker Compose provider — bollard-based container lifecycle |
| `tokeira-aws` | AWS resource implementations |

### Platform Crates

| Crate | Purpose |
|-------|---------|
| `platforms/local` | Bare-process local execution — spawns tokeirad directly |
| `platforms/compose` | Docker Compose stack with observability services (Mimir, Loki, Grafana, Alloy) |

### Applications

| Binary | Purpose |
|--------|---------|
| `tokeirad` | Server process — wires kernel, runtime, storage, edge, and projection into one binary |
| `tkr` | CLI — deployment lifecycle, infrastructure management, and developer workflows |

## Quick Start

```bash
# Install tkr
cargo install --path apps/tkr

# Create and start a local deployment
tkr deployment create --name dev --platform local --storage in-memory
tkr deploy apply --yes
```

## `tkr` — Operator and Developer CLI

`tkr` is the single entry point for deployment lifecycle, infrastructure management, and developer workflows. It manages named deployments stored under `$XDG_STATE_HOME/tokeira/tkr/`.

### Command Tree

```
tkr
├── deployment
│   ├── create --name <name> --platform <local|compose|ecs> --storage <in-memory|dsql>
│   ├── list
│   ├── use <name>
│   └── destroy <name> --yes
├── infra
│   ├── plan [--module <name>]
│   ├── apply --yes [--module <name>]
│   ├── destroy --yes [--module <name>]
│   └── status
├── deploy
│   ├── plan
│   ├── apply --yes
│   └── status
├── image
│   ├── list [--source-type <build|mirror>] [--json]
│   ├── build [--arch <arm64|amd64>] [--tag <version>]
│   ├── push --tag <version> [--image <name>] [--yes]
│   └── mirror [--image <name>] [--yes]
├── schema
│   ├── setup --yes
│   ├── status
│   └── validate
├── scale
│   ├── up [<service>] [<replicas>]
│   ├── down [<service>] [<replicas>]
│   └── status
├── logs <service> [--follow] [--tail <n>]
├── port-forward <service>
├── config
│   └── show
├── workstation
│   ├── up [--workstation <id>]
│   ├── remote-exec [--workstation <id>] -- <command...>
│   ├── ssh [--workstation <id>]
│   ├── status [--workstation <id>]
│   ├── stop [--workstation <id>]
│   ├── destroy [--workstation <id>] --yes
│   ├── bootstrap [--workstation <id>]
│   ├── idle [--workstation <id>] [--defer <duration>]
│   ├── github-key <add|remove|list>
│   └── code
│       ├── sync [--branch <name>]
│       └── push [--branch <name>]
├── dev
│   ├── build
│   ├── test [--crate <name>]
│   ├── check
│   ├── lint
│   ├── fmt
│   └── docs
└── version
```

### Local Platform

The local platform runs `tokeirad` as a bare child process on the host. No containers, no Docker, no observability stack. This is the fastest path from zero to a running server for development and testing.

```bash
# Create
tkr deployment create --name dev --platform local --storage in-memory

# Start tokeirad (blocks, inherits stdio, forwards SIGINT)
tkr deploy apply --yes

# In another terminal
tkr config show
tkr version
```

No `infra` step is needed — the local platform has no infrastructure to provision. `deploy apply` spawns `tokeirad` directly.

For DSQL persistence, replace `in-memory` with `dsql` and configure the DSQL endpoint in `tokeirad.toml` before starting.

### Compose Platform

The compose platform runs a full Docker Compose stack: `tokeirad` plus an observability suite (Mimir, Loki, Grafana, Alloy). Requires Docker.

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

The compose platform organizes services into two modules:

- **runtime** — `tokeirad`
- **observability** — `mimir`, `loki`, `grafana`, `alloy` (pinned to `grafana/mimir:3.0.6`, `grafana/loki:3.7.1`, `grafana/grafana-oss:12.4.3`, `grafana/alloy:v1.16.0`)

**Why the build step is separate.** `tkr deploy apply` does not invoke the image builder — it requires `tokeirad:latest` to already exist in the local Docker image store. This keeps the deploy path deterministic and fast: a repeat deploy does not rebuild. Re-run `tkr image build` whenever you want a fresh `tokeirad` binary in the compose stack.

**Storage and schema.** The example above uses `--storage in-memory`, which needs no schema setup. Compose also supports Aurora DSQL through the `dsql` infrastructure module. DSQL deployments use `deployment.toml` for platform storage intent and `tokeirad.toml` writeback for the server runtime endpoint/region.

##### Recommended compose+DSQL lifecycle:

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

The two-phase infra flow keeps storage provisioning separate from service startup: `tokeirad` connects to DSQL during boot and will fail fast if the endpoint or schema is missing. A one-shot `tkr infra apply --yes` also works, but run `tkr schema setup --yes` before `tkr deploy apply`.

For preexisting clusters, set `[dsql] mode = "preexisting"` and `endpoint = "...dsql.<region>.on.aws"` in `deployment.toml` before `tkr infra apply --module dsql`. The module records the endpoint and skips provider deletion. AWS credentials must be available through the standard local provider chain; compose mounts `~/.aws` into the `tokeirad` container and forwards simple provider-chain environment variables.

### Image Management

`tkr image` manages the image plane: building `tokeirad` from source, pushing built images to ECR, and mirroring upstream observability images into project-owned ECR.

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

Lifecycle ordering is explicit. For ECS, run `tkr image mirror` before `tkr infra apply`, and run `tkr image build` plus `tkr image push --tag <version>` before `tkr deploy apply`. For compose, run `tkr image build` before `tkr deploy apply` so `tokeirad:latest` exists locally.

Image commands that build, push, or mirror use Dagger. Install Dagger 0.20 or newer; the CLI re-executes under `dagger run` when a Dagger session is absent. ECS push and mirror also require AWS credentials with ECR permissions: `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:PutImage`, `ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`, `ecr:CreateRepository`, `ecr:DescribeRepositories`, `ecr:PutLifecyclePolicy`, `ecr:TagResource`, `ecr:ListTagsForResource`, and `ecr:GetLifecyclePolicy`.

To add a new image, start in `platforms/{compose,ecs}/src/images/` and implement `tokeira_deploy_engine::image::Image`. Build recipes live as hardcoded free functions in `tokeira-build`.

## Rust Development

### Local development

```bash
cargo build                              # build all crates
cargo clippy --workspace --all-targets   # lint
cargo test                               # unit tests
cargo +nightly fmt                       # format
cargo doc --workspace --no-deps          # generate docs
```

The workspace has 150+ crates. A cold `cargo build --workspace` takes 10–20 minutes on a laptop; incremental builds are typically 10–30 seconds. If cold-build time is blocking your iteration loop, use the remote workstation (below).

### Remote workstation for Rust builds

`tkr workstation` provisions a dedicated Graviton4 `c8gd.8xlarge` (32 vCPU, 64 GiB RAM, 1900 GiB NVMe) in `eu-west-2` for fast Rust compilation. Cold workspace builds complete in under 2 minutes. Access is via AWS Systems Manager Session Manager — no SSH keys, no public ingress.

#### Workstation lifecycle

| Command | What it does |
|---------|-------------|
| `tkr workstation up` | Creates a new workstation (first run) or resumes a stopped one. Runs bootstrap, waits for readiness. |
| `tkr workstation stop` | Stops the instance. EBS volumes persist; NVMe (`/work/target`, `/work/sccache`) is erased. Public IP released. |
| `tkr workstation destroy --yes` | Terminates the instance, deletes EBS volumes, removes IAM role and security group. Irreversible. |
| `tkr workstation bootstrap` | Forces a bootstrap refresh (re-installs toolchain, cargo tools) without destroying the instance. Triggered automatically on `up` when drift is detected. |
| `tkr workstation status` | Shows state, uptime, cost rate, bootstrap fingerprint, volume IDs. |
| `tkr workstation list` | Enumerates all workstations in the account with state and cost. |
| `tkr workstation idle --defer 2h` | Extends the idle-shutdown window (prevents auto-stop during long unattended builds). |

The workstation stops automatically after 30 minutes of idle (configurable). A forgotten instance does not silently accumulate cost.

#### Development workflow

A typical day working with the remote workstation:

```bash
# 1. Bring the workstation online (creates on first run, resumes thereafter)
tkr workstation up

# 2. (First time only) Add a GitHub deploy key so you can push from the workstation
tkr workstation github-key add --repo <owner>/tokeira

# 3. Sync code to the workstation (clones on first run, pulls thereafter)
tkr workstation code sync
tkr workstation code sync --branch feature/my-work   # specific branch

# 4. Build
tkr workstation remote-exec cargo build --workspace

# 5. Test
tkr workstation remote-exec cargo test --workspace

# 6. Lint
tkr workstation remote-exec cargo clippy --workspace --all-targets

# 7. Format check
tkr workstation remote-exec cargo +nightly fmt --all --check

# 8. (Optional) Interactive debugging session
tkr workstation ssh
# You're now in a shell on the workstation at /work/tokeira
cargo test -p tokeira-kernel -- --nocapture
exit

# 9. Push results back to origin
tkr workstation code push
tkr workstation code push --branch feature/my-work   # specific branch

# 10. Stop when done for the day
tkr workstation stop
```

**What happens on first `up`:**
- Instance created, EBS volumes provisioned and attached
- Cloud-init bootstrap runs (~5–8 minutes): installs Rust toolchain (stable + nightly from `rust-toolchain.toml`), cargo tools (`nextest`, `deny`, `sccache`, `protoc`, `buf`, `uv`, `ripgrep`, `fd`, `mold`, `lld`, `gh`), mounts NVMe, clones the repository
- `tkr workstation up` blocks until bootstrap completes (polls via SSM)

**What happens on subsequent `up` (resume):**
- Instance started (~30 seconds to reach `running`)
- Bootstrap fingerprint checked — if your `rust-toolchain.toml` changed, a refresh runs automatically
- NVMe reformatted (target/ and sccache are cold); EBS volumes intact (cargo registry, rustup, repo checkout preserved)

#### Storage tiering

| Tier | Mount | Survives stop? | Contents |
|------|-------|----------------|----------|
| Local NVMe | `/work` (root), `/work/target`, `/work/sccache` | No | `CARGO_TARGET_DIR`, sccache cache |
| Cache EBS (30 GiB) | `/work/cache` → `~/.cargo`, `~/.rustup` | Yes | Crate registry, toolchains, cargo tools |
| Repo EBS (40 GiB) | `/work/repo` → `/work/tokeira` | Yes | Repository checkouts, uncommitted work |

The first build after a resume is a cold build (NVMe wiped), but cargo's incremental state on the Cache EBS gives most of the benefit. Subsequent builds within a session hit sccache on NVMe for maximum speed.

#### GitHub credentials (opt-in)

By default the workstation carries no GitHub credentials. To push branches or create PRs:

```bash
# Add a workstation-scoped deploy key (public key uploaded via your local gh auth)
tkr workstation github-key add --repo <owner>/tokeira

# List active deploy keys
tkr workstation github-key list

# Remove when no longer needed (or automatically on destroy)
tkr workstation github-key remove --repo <owner>/tokeira
```

The deploy key is per-workstation, per-repository. Your primary GitHub token never reaches the workstation.

#### Cost model (~$387/month at 20 working days, 10 active hours/day)

| Component | Active | Stopped | Monthly |
|-----------|--------|---------|---------|
| c8gd.8xlarge | $1.88/hr | $0 | ~$376 |
| EBS (90 GiB) | $0.01/hr | $0.01/hr | ~$7 |
| Elastic IP (transient) | $0.005/hr | $0 (released) | ~$1 |

#### Prerequisites

- AWS credentials with EC2, IAM, and SSM permissions
- `session-manager-plugin` installed locally (`brew install session-manager-plugin` on macOS)
- A VPC with at least one public subnet (the workstation uses a transient public IP for egress; no NAT Gateway required)

See `.kiro/specs/remote-workstation/` for the full spec (requirements, design, tasks).

## Architecture Documentation

Detailed design documents are in `docs/architecture/`:

- [000 — Overview](docs/architecture/000-overview.md) — system shape, design principles, crate map
- [010 — History as Authority](docs/architecture/010-history-as-authority.md) — the core invariant
- [020 — Kernel](docs/architecture/020-kernel.md) — deterministic state transition contract
- [030 — Runtime Lanes](docs/architecture/030-runtime-lanes.md) — execution and delivery
- [035 — Placement and Membership](docs/architecture/035-placement-and-membership.md) — queue-aware placement, DSQL fencing
- [050 — DSQL Storage](docs/architecture/050-dsql-storage.md) — persistence design
- [070 — Projection Plane](docs/architecture/070-projection-plane.md) — read models and visibility

## Acknowledgements

The architecture, requirements specification, and technical design of this project were developed in close collaboration with [Kiro](https://kiro.dev), which made significant contributions to the design and realisation of the system.

## License

[MIT](LICENSE)
