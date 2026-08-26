# Compose platform

Compose is the current complete definition-backed platform chain. At creation, `tkr`
compiles the selected definition frontend, Compose lifecycle implementation, Docker
realization, and shared interpreter and transition machinery into a deployment-local
`tkp`. This `tkp` is the deployment's bound provisioner: the exact engine assembled for
the selected platform and definition format. It gives the configuration its meaning and
realizes its lifecycle. `tkr` owns the provisioner's construction or discovery,
placement, launch verification, and repository publication.

The canonical TKD (`rust-syn` syntax) source set is:

- `deployment.tkd` — the root, configuration values, and module wiring;
- `platform.tkd` — the Compose configuration and shared authoring types; and
- `observability.tkd` — the observability module and service declarations.

The package also provides `definition.tkdp`, the same Compose configuration and
deployment graph expressed through the TKDP (`python` syntax) frontend. The selected
frontend and platform are bound into the placed provisioner; neither `tkr` nor the
retained definition performs runtime platform dispatch.

## Choosing the definition format

Compose declares TKD (`rust-syn` syntax) as its default. When `deployment create` is run
without `--format`, `tkr` selects `deployment.tkd` and builds the placed provisioner with
the TKD frontend:

```bash
tkr deployment create \
  --name dev-compose \
  --platform compose \
  --dev-engine
```

Select TKDP (`python` syntax) explicitly with `--format tkdp`; `tkr` then selects
`definition.tkdp` and builds the provisioner with the TKDP frontend:

```bash
tkr deployment create \
  --name dev-compose-python \
  --platform compose \
  --format tkdp \
  --dev-engine
```

The chosen format and configuration path are recorded with the deployment. Later
definition checks and lifecycle commands use that recorded frontend rather than selecting
again. The format cannot be changed after creation; moving between TKD and TKDP requires
a new deployment. `--format tkd` is accepted when an explicit statement of the default
is useful.

## Creating a deployment

The default engine path is a verified hermetic bundle. It requires a digest-pinned build
container because that container is part of the engine identity:

```bash
tkr deployment create \
  --name dev-compose \
  --platform compose \
  --build-image IMAGE@sha256:DIGEST
```

For local platform development, `--dev-engine` builds the generated bound provisioner
with the workspace toolchain instead. It still places both `tkp` and
`tkp.manifest.json`, but the manifest records native, non-hermetic, local-developer
provenance:

```bash
tkr deployment create \
  --name dev-compose \
  --platform compose \
  --dev-engine
```

Definition-backed platform creation requires either `--dev-engine` or `--build-image`.

Creation fully materializes the deployment before making its directory visible. It
places and verifies the engine, admits the complete source set, writes the initial
`tokeirad.toml`, records the binding and integrity evidence, retains configuration
revision `0`, and creates the deployment's initial repository publication. It does not
create provider resources, start services, or produce apply-time rendered outputs.

## Lifecycle

Compose has separate infrastructure and workload planes. The normal development path is:

```bash
# Create and select the deployment
tkr deployment create \
  --name dev-compose \
  --platform compose \
  --dev-engine

# Build the local image referenced by the shipped definition
tkr image build

# Check the complete deployment-bound definition
tkr definition check

# Review and realize infrastructure and rendered configuration
tkr infra plan
tkr infra apply

# Review and realize Docker services
tkr deploy plan
tkr deploy apply

# Inspect provisioner identity, binding, revision, and state facts
tkr deployment describe --detail

# Remove services, infrastructure, and then local deployment records
tkr deployment destroy --name dev-compose --yes
```

Apply without `--yes` performs the plan needed to enforce the destructive-change gate.
Deletes and replacements require `--yes`.

The two live planes can be torn down independently while retaining the deployment:

```bash
tkr --deployment dev-compose deploy destroy --yes
tkr --deployment dev-compose infra destroy --yes
```

`deployment destroy` is the aggregate operation. It removes services first,
infrastructure second, and local records only after both live-plane operations succeed.
A failure retains the deployment directory and state so teardown can be retried.

## Definition and modules

The `config()` function in `deployment.tkd` is the operator-visible desired
configuration. The root wires that value into four modules:

- **`local_state`** — deployment-local state roots;
- **`dsql`** — conditional Aurora DSQL and coordination resources;
- **`runtime`** — the rendered server configuration and `tokeirad` service; and
- **`observability`** — Mimir, Loki, Grafana, Alloy, and their rendered configuration.

The `dsql` module exists only when `Storage::Dsql` is selected. Module and resource
ordering is evaluated after interpretation. Infrastructure commands accept `--module`
and forward it to TKP; workload commands deliberately do not expose module filtering.

Fields marked `#[create]` in `platform.tkd`, including storage and AWS region, are
deployment identity. Their values must be present in the configuration admitted at
creation and cannot be changed by a later apply. The current discovered-platform CLI
does not map `--storage dsql` or `--region` into the Compose configuration; it rejects
those flags. For a source-workspace development deployment that needs DSQL today, author
the DSQL values in `platforms/compose/deployment.tkd` before running `deployment create`.
Do not treat a post-creation storage edit as an ordinary reconciliation.

Read the
[deployment definition programming guide](../../provisioning/deployment-definitions.md)
for the admitted language and interpreter model. See
[definition patterns and current practice](../../provisioning/deployment-definition-patterns.md)
for the source-backed Compose builder and module idioms.

## Docker and image behavior

Compose runs `tokeirad` and the observability stack as Docker containers. Docker must be
reachable for workload planning and mutation, logs, and port mapping inspection.
`deploy plan` reads live containers and resolves each desired image before reporting a
change; according to the manifest's policy, that read-only workload operation may
populate Docker's image cache. A connection failure or image-pull stream failure is
rendered as a typed operator report and stops at the first failed service.

Every service declares a Compose-compatible `pull_policy` in the definition. Supported
values are `always`, `never`, `missing`, `daily`, `weekly`, and
`every_<duration>`; `if_not_present` is accepted as an alias for `missing`. The `build`
policy is refused because Tokeira's service manifest does not carry a Compose build
configuration. The shipped definition uses `never` for the locally built `tokeirad`
image and `missing` for registry-hosted observability images.

Image building is separate from convergence. `tkr image build` does not require an
active deployment and does not edit `deployment.tkd`. A definition identifies its
desired image by tag, so rebuilding different bytes behind the same tag does not change
the desired manifest. The forwarded Compose path has no `--force` apply contract; prefer
a new tag and an explicit definition edit:

```bash
tkr image build --tag dev-2
# Edit deployment.tkd: image: "tokeirad:dev-2".into()
tkr definition check
tkr deploy plan
tkr deploy apply
```

## DSQL configuration and writeback

The definition models managed and preexisting Aurora DSQL through
`Storage::Dsql(DsqlStorage { ... })`. A DSQL deployment realizes the cluster and its two
coordination tables in the infrastructure plane. As described above, those create-time
values must be part of the configuration admitted at deployment creation; the current
Compose create route does not accept them through `--storage` and `--region`.

After a successful infrastructure apply, TKP resolves the definition's declared
writebacks and persists them into `tokeirad.toml` before advancing the retained
configuration revision. These include the storage mode, DSQL endpoint and region, and
the rate-limiter and connection-lease table names.

The `tkr schema` commands remain in-process handlers and have no forwarded Compose path.
Do not use the legacy `deployment.toml` Compose/DSQL sequence as the recipe for a
definition-backed deployment.

## Operations available through `tkr`

| Command | Compose behavior |
|---|---|
| `tkr definition check` | Launches the bound TKP and fully interprets the deployment source set. `--definition PATH` is the separate deployment-free syntax check. |
| `tkr infra plan/apply/destroy` | Forwards to TKP's infrastructure lifecycle; `--module` is supported. |
| `tkr deploy plan/apply/destroy` | Forwards to TKP's workload lifecycle. |
| `tkr deployment describe/apply/upgrade/rollback` | Uses the trust-aware provisioner launcher. `deployment apply` is the infrastructure apply spelling. |
| `tkr deployment destroy --name NAME` | Runs ordered workload and infrastructure teardown, then removes local records. |
| `tkr scale up/down` | Forwards to TKP; Compose reports that it exposes no scale dimension. |
| `tkr infra status`, `deploy status`, `scale status` | Render TKP `describe`. |
| `tkr logs` | Forwards to TKP's Docker log stream. |
| `tkr port-forward` | Reports provider-published port mappings; `--local-port` is not available on the bound path. |
| `tkr exec`, `schema` | In-process-only; there is no forwarded Compose path. |

## See also

- [Platform support matrix](../README.md)
- [Provisioning](../../provisioning/README.md) — the `tkr`/`tkp`/`tkd` triad
- [Deployment definition programming guide](../../provisioning/deployment-definitions.md) —
  abstract language and authoring rules
- [Definition patterns and current practice](../../provisioning/deployment-definition-patterns.md) —
  Compose source, bridge, and adapter idioms
- [`tkr` and `tkp`](../../provisioning/tkr-and-tkp.md) — forwarding, launch trust,
  upgrade, and rollback
- [Production observability](../observability.md) — what the stack collects and alerts on
