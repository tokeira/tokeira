# Compose platform

Compose is the current complete custom-TKD platform chain. Its `definition.tkd`
vocabulary, bridge, adapter, lifecycle implementation, Docker realization, and shared
interpreter and transition machinery are compiled into the platform-owned Compose `tkp`.
That deployment-local engine gives the definition its meaning; `tkr` owns its
construction or discovery, placement, binding initialization, and launch verification.

The default create path copies a native development binary and records a pre-identity
stamp. Add `--bundle --build-image IMAGE@sha256:DIGEST` to request the current versioned
path: `tkr` freezes the Compose seed package's reachable closure, derives
`EngineIdentity`, obtains a tested and attested target artifact, and places both `tkp` and
`tkp.manifest.json`. The [provisioning guide](../../provisioning/README.md) distinguishes
these guarantees in detail.

Compose runs `tokeirad` and the observability stack as Docker containers. A reachable
Docker daemon is required for apply and destroy. Plan can still report desired changes
when live Docker description is unavailable.

## Lifecycle

```bash
# Create definition.tkd, tokeirad.toml, metadata, state/, and a local tkp
tkr deployment create \
  --name dev-compose \
  --platform compose \
  --storage in-memory

# Build the runtime image referenced by the seeded definition
tkr image build

# Check, review, and apply through the bound provisioner
tkr definition check
tkr infra plan
tkr infra apply

# Inspect provisioner identity, binding, revision, and state facts
tkr deployment describe --detail

# Tear down provider resources before deleting the registry directory
tkr infra destroy --yes
tkr deployment destroy --name dev-compose --yes
```

Creation stamps the envelope and retains the complete authored source set as revision 0
before the deployment directory becomes visible. The first apply therefore performs only
`tkp infra apply`. A non-destructive plan does not require `--yes`; deletes and
replacements do.

`tkr deploy plan/apply` is also forwarded, but Compose models containers as
infrastructure resources, so those verbs realize the same desired universe as the infra
verbs. Prefer the infra spelling when reasoning about the complete Compose stack.

## Definition and modules

The seeded `definition.tkd` contains both operator config and deployment structure. The
canonical definition organizes the desired model into:

- **`local_state`** — deployment-local state and rendered configuration roots;
- **`dsql`** — conditional Aurora DSQL and coordination resources;
- **`observability`** — Mimir, Loki, Grafana, Alloy, and their rendered config; and
- **`runtime`** — `tokeirad` containers and their dependencies.

The `dsql` module exists only when the definition's `Storage` config selects DSQL. Module
and resource ordering is evaluated by the convergence engine after interpretation.

Forwarded Compose operations currently apply `ModuleSelection::All`. Although the shared
`tkr infra` parser exposes `--module`, that option has no TKP counterpart and is not a
Compose scoping mechanism.

Read the
[deployment definition programming guide](../../provisioning/deployment-definitions.md)
for the admitted language and interpreter model. See
[definition patterns and current practice](../../provisioning/deployment-definition-patterns.md)
for the source-backed Compose builder and module idioms.

## Image changes

Image building is separate from convergence. `tkr image build` does not require an
active deployment and does not edit `definition.tkd`.

A definition identifies the desired image by tag. Rebuilding different bytes behind the
same tag does not change that desired value, and the forwarded TKP surface has no
`--force` flag. To make an image change explicit, build a new tag and update the image
field in `definition.tkd`, then plan and apply.

```bash
tkr image build --tag dev-2
# Edit definition.tkd: image: "tokeirad:dev-2".into()
tkr definition check
tkr infra plan
tkr infra apply
```

## DSQL configuration

Select DSQL through the `Storage::Dsql` value in `definition.tkd`; a Compose deployment
created with `--storage dsql --region REGION` receives that shape in its seeded
`config()` literal. The definition can request a managed cluster or describe a
preexisting cluster using its endpoint and ARN fields.

A DSQL definition declares deferred writeback from resource outputs to server config,
and the Compose adapter can calculate those values from applied infrastructure state.
The TKP platform seam returns committed change identities rather than a writeback payload,
so the forwarded apply does not persist calculated updates into `tokeirad.toml`. Verify
and complete the server runtime config before starting a DSQL-backed server.

The `tkr schema` commands are in-process handlers and are not forwarded for a
`definition.tkd` deployment. Do not use the in-process Compose/DSQL sequence from an
older `deployment.toml` deployment as an operator recipe for this path.

## Operations available through `tkr`

| Command | Compose behavior |
|---|---|
| `tkr definition check` | Fully parses and interprets the definition in memory. |
| `tkr infra plan/apply/destroy` | Forwards to the deployment-local TKP and its Compose `ProvisionerPlatform`. |
| `tkr deploy plan/apply` | Forwards to TKP; delegates to the same infrastructure universe. |
| `tkr deployment describe/apply/upgrade/rollback` | Uses the trust-aware provisioner launcher. |
| `tkr scale up/down` | Forwards to TKP, which returns `NotApplicable` because Compose exposes no scale dimension. |
| `tkr infra status`, `deploy status`, `scale status` | Render TKP `describe`. |
| `tkr logs`, `port-forward`, `exec`, `schema` | In-process-only; no forwarded Compose path. |

## See also

- [Platform support matrix](../README.md)
- [Provisioning](../../provisioning/README.md) — the `tkr`/`tkp`/`tkd` triad
- [Deployment definition programming guide](../../provisioning/deployment-definitions.md) —
  abstract language and authoring rules.
- [Definition patterns and current practice](../../provisioning/deployment-definition-patterns.md) —
  Compose source, bridge, and adapter idioms.
- [`tkr` and `tkp`](../../provisioning/tkr-and-tkp.md) — forwarding, launch trust,
  upgrade, and rollback
- [Production observability](../observability.md) — what the stack collects and alerts on
