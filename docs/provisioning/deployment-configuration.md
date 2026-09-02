# Deployment configuration and the `tkr` command surface

`tkr` manages named deployments under its application state directory. On macOS this is
`~/Library/Application Support/tokeira/tkr/`; on other systems the location follows the
platform application-state convention selected by the `directories` crate.

The architectural deployment shape is a custom `definition.tkd` vocabulary married to
the platform-specific, provenance-bound `tkp` that defines and realizes it. Current
operator coverage also retains an in-process shape while platforms move to that contract.
A deployment records identity and selection metadata, desired configuration, server
runtime configuration, and deployment-engine state. Today Local and ECS use
`deployment.toml` with in-process handlers; Compose uses `definition.tkd` and a
deployment-local engine. EKS does not yet provide a complete operator shape.

## Create and select a deployment

```bash
# Zero-dependency default: local plus in-memory storage
tkr deployment create --name dev

# Definition-backed Compose deployment
tkr deployment create \
  --name compose-dev \
  --platform compose \
  --storage in-memory

# In-process ECS deployment
tkr deployment create \
  --name production \
  --platform ecs \
  --storage dsql \
  --region us-east-1

tkr deployment list
tkr deployment use compose-dev
```

Without `--deployment NAME`, commands use the `.latest` selection written by create or
`deployment use`. Names are normalized to lowercase filesystem-safe entries.

Definition-backed deployments can place all authoritative provisioner state in one
pre-existing S3 prefix:

```bash
tkr deployment create \
  --name compose-dev \
  --platform compose \
  --state-bucket company-tokeira-state \
  --state-region eu-west-2 \
  --state-prefix deployments/compose-dev \
  --dev-engine
```

`--state-bucket`, `--state-region`, and `--state-prefix` must be supplied together;
omitting all three selects local state. The selection covers the envelope,
infrastructure state, runtime state, and renewable operation lease, and is recorded in
both `metadata.json` and the signed Deployment Claim so `deployment fetch` reconnects a
second seat to the same state. It does not move workflow history or DSQL storage.

The bucket is an operator-owned prerequisite. Tokeira neither changes its bucket policy
or lifecycle nor deletes the deployment prefix on destroy; the destroy path captures
the retained S3 URI before removing the local locator and reports it after teardown
succeeds. See
[State and convergence](../iac/state-and-convergence.md#remote-placement) for the object
layout, permissions, multi-seat verification, and retention contract.

`tkr deployment lock [NAME]` adds a separate mis-apply guard: mutating commands refuse a
different deployment until `tkr deployment unlock --yes` clears it. This registry lock
is distinct from TKP's renewable operation lock and from state-store CAS.

## Two directory shapes

### In-process: Local and ECS

```text
<registry>/<name>/
├── deployment.toml
├── metadata.json
├── state/
└── tokeirad.toml
```

`deployment.toml` contains platform desired configuration. `tkr` loads it into the
platform's typed config and runs command handlers in-process.

### Forwarded: Compose

```text
<registry>/<name>/
├── definition.tkd
├── metadata.json
├── tkp
├── tokeirad.toml
└── state/
```

`definition.tkd` is desired deployment data interpreted by the provisioner. `tkp` is the
provisioner married to this deployment. `tkr` detects this path by the definition file's
presence and forwards the lifecycle operations documented in
[`tkr` and `tkp`](tkr-and-tkp.md).

A forwarded deployment intentionally has no `deployment.toml`. An in-process-only
handler fails with a domain error rather than reporting a confusing missing file.

## File ownership

| Entry | Primary writer | Meaning |
|---|---|---|
| `metadata.json` | `tkr` | Registry identity, platform and storage labels, status, and timestamps. |
| `.latest` in the registry root | `tkr` | Name selected when no global `--deployment` is supplied. |
| `deployment.toml` | `tkr` template, then operator | Desired platform config for an in-process deployment. |
| `definition.tkd` | `tkr` template, then operator | Desired config and structure for a forwarded deployment. |
| `tokeirad.toml` | `tkr` template and command paths that explicitly persist derived server config | Runtime config consumed by `tokeirad`; it is not deployment-engine state. |
| `tkp` | `tkr` create/upgrade/rollback launcher | Deployment-local provisioner executable. |
| `state/` | Provisioner and convergence engines | Envelope, operation lock, retained config, infrastructure state, runtime state, and retained binaries as applicable. |

Infrastructure outputs can hydrate an in-memory platform config and can produce derived
writeback values. Persistence is a host-command responsibility; a calculated writeback
is not itself proof of provider convergence.

## Review before action

The operator contract is **plan → review → apply**:

```bash
tkr infra plan
tkr infra apply
```

For a forwarded deployment, those commands become `tkp infra plan` and `tkp infra apply`
against the same directory. Apply without `--yes` performs a plan pass and refuses only
when it finds destructive changes. Pass `--yes` after review to authorize deletes or
replacements and avoid the extra plan pass.

Full deployment teardown is one ordered operation:

```bash
tkr deployment destroy --name compose-dev --yes
```

`deployment destroy` removes workloads first, infrastructure second, and the local
deployment records last. A failure retains the directory and its state so the operation
can be retried without orphaning whatever remains.

The two live planes can also be removed independently while retaining the deployment:

```bash
tkr --deployment compose-dev deploy destroy --yes
tkr --deployment compose-dev infra destroy --yes
```

## Routing by platform

| Platform | Desired source | Lifecycle executor | Available through `tkr deployment create` |
|---|---|---|---:|
| Local | `deployment.toml` | `tkr` in-process handlers | Yes |
| Compose | `definition.tkd` | Deployment-local Compose `tkp` | Yes |
| ECS | `deployment.toml` | `tkr` in-process handlers | Yes |
| EKS implementation components | No complete operator deployment source | No complete TKP route | No |

The EKS crate's TKD bridge and kinds do not add an EKS value to `tkr`'s current
`--platform` enum and do not provide a `ProvisionerPlatform` or `tkp` target.

## Forwarded and in-process commands

Some command names are deliberately shared while their executor differs:

| `tkr` surface | In-process behavior | Forwarded behavior |
|---|---|---|
| `definition check` | Refuses because `deployment.toml` is configured, not interpreted. | Launches TKP definition check. `--path` can check a definition without a deployment. |
| `infra plan/apply/destroy` | Runs platform infrastructure handlers. | Launches matching TKP infra verb. |
| `infra status` | Runs in-process status. | Launches TKP describe. |
| `deploy plan/apply/destroy` | Runs workload engine handlers. | Launches the matching TKP deploy verb. |
| `deploy status` | Runs in-process status. | Launches TKP describe. |
| `scale up/down` | Runs platform operations. | Launches TKP scale; Compose returns `NotApplicable`. |
| `scale status` | Runs platform status. | Launches TKP describe. |
| `deployment describe/apply/upgrade/rollback` | These provisioner lifecycle verbs expect a forwarded directory. | Launches the trust-aware TKP flow. |
| `logs`, `port-forward`, `exec`, `schema` | Runs typed in-process handlers where the platform supports them. | No TKP forwarding path. |

The global `--deployment NAME` selects the target. `--json` and `--detail` control
operator reports and cross into forwarded read-only commands. `--explanation PATH` can
persist a standalone explanation model for plan/apply where supported.

## Representative command tree

The concise tree below emphasizes deployment operation. `tkr --help` and each
subcommand's help are authoritative for flags and additional developer, CI,
compatibility, diagnostics, image, and observability commands.

```text
tkr [--deployment NAME] [--json] [--detail]
├── deployment
│   ├── create [--name NAME] [--platform local|compose|ecs]
│   │          [--storage in-memory|dsql] [--region REGION]
│   │          [--bundle --build-image IMAGE@sha256:DIGEST]
│   │          [--state-bucket BUCKET --state-region REGION --state-prefix PREFIX]
│   ├── list
│   ├── use NAME
│   ├── lock [NAME]
│   ├── unlock --yes
│   ├── destroy --name NAME --yes
│   ├── describe
│   ├── apply [--yes]
│   ├── upgrade
│   └── rollback
├── definition
│   └── check [--path FILE_OR_DIRECTORY]
├── infra
│   ├── plan [--module NAME] [--explanation PATH]
│   ├── apply [--yes] [--module NAME] [--explanation PATH]
│   ├── destroy --yes [--module NAME]
│   └── status
├── deploy
│   ├── plan [--explanation PATH]
│   ├── apply [--yes] [--force] [--explanation PATH]
│   └── status
├── scale
│   └── up [SERVICE] [REPLICAS] | down [SERVICE] [REPLICAS] | status
├── logs SERVICE [--follow] [--tail N]
├── port-forward SERVICE [--local-port PORT]
├── exec SERVICE [--container NAME] [-- COMMAND...]
├── schema
│   └── setup --yes | status | validate
└── config
    └── show
```

`--module` and `--force` are in-process options. Forwarding preserves an option only when
the TKP command has the corresponding contract.

## Lifecycle examples

### Local

```bash
tkr deployment create --name local-dev
tkr deploy plan
tkr deploy apply
```

Local has no infrastructure stage. Its server process is managed by the in-process local
platform.

### Compose

```bash
tkr deployment create --name compose-dev --platform compose --storage in-memory
tkr definition check
tkr infra plan
tkr infra apply
tkr deployment describe
```

Creation has already realized the provisioner envelope and retained revision `0`; the
first apply only converges the interpreted definition. Compose workloads are
infrastructure resources, so deploy plan/apply are an
alternate namespace over the same realized universe rather than a separate runtime
store transition.

### ECS

```bash
tkr deployment create \
  --name production \
  --platform ecs \
  --storage dsql \
  --region us-east-1

tkr image mirror --yes
tkr image build --arch arm64 --tag release
tkr image push --tag release --yes
tkr infra plan
tkr infra apply --yes
tkr schema setup --yes
tkr deploy plan
tkr deploy apply --yes
tkr scale up
```

ECS uses the compiled in-process platform, including its image, infrastructure, schema,
workload, and day-2 handlers.

## Image and schema configuration

`tkr image` manages desired image workflows separately from deployment convergence.
Building uses Dagger; ECS push and mirror also require appropriate ECR access. Runtime
state recording of `repository:tag` is not proof that an image was built or published.

DSQL-backed in-process deployments apply schema migrations explicitly with `tkr schema
setup --yes`, inspect them with `schema status`, and validate the migration set with
`schema validate`. The storage contract is documented in
[DSQL storage](../architecture/050-dsql-storage.md).

## Operating and inspecting

Use these read paths before mutation or when a gate refuses:

- `tkr deployment list` identifies registry entries and the selected deployment;
- `tkr config show` renders effective in-process configuration;
- `tkr definition check` verifies a forwarded deployment source;
- `tkr infra plan` and `tkr deploy plan` show desired changes;
- `tkr deployment describe` reports the forwarded provisioner binding and state facts;
- `--detail` adds evidence; and
- `--json` emits the complete structured report where the command supports the output
  contract.

## Further reading

- [Provisioning overview](README.md) — `tkr`/`tkp`/`tkd` responsibilities.
- [`tkr` and `tkp`](tkr-and-tkp.md) — exact forwarding and launcher trust.
- [Deployment definition programming guide](deployment-definitions.md) — abstract
  authoring and interpreter mechanics.
- [Definition patterns and current practice](deployment-definition-patterns.md) —
  concrete source idioms and platform vocabulary assembly.
- [The provisioner](provisioner.md) — envelope, binding, locks, and revisions.
- [Platform support](../platforms/README.md) — environments and implementation status.
