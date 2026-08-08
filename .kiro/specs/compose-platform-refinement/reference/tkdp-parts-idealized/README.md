# tkdp-parts-idealized — the authored surface, beyond the current dialect

Direction, not contract: what a `.tkdp` definition should read like once the
vocabulary ladder lands. The parts mechanism itself — real Python modules,
the registered `tokeira` facade, the boundary refusals, the Monty seam — is
adopted contract and lives in this spec's `design.md`; these files carry
only what is still ahead of the dialect. Where this sketch and the spec
documents disagree, the spec documents win.

The governing position stays: tkd is idiomatic Rust, tkdp is idiomatic
Python, naming consistent across the two languages, and renaming these
files to `.py` changes nothing about what they mean.

## What this sketch adds to the dialect

- **Provenance-true imports.** Vocabulary module names are the crate names
  they come from, normalized: `tokeira_platform` (Context, Deployment,
  Module — the definition contract), `tokeira_provisioner` (the
  provisioner's kinds: LocalStateDir, ServerConfig, Service and its
  companions State, Config, Environment, ObservabilityConfiguration),
  `tokeira_aws` (provider kinds: DsqlCluster, DynamoDbTable). Single level,
  flat names, in both languages — in Rust these are literally crate paths.
  Today's dialect has one `tokeira` facade; the split is the
  namespace-aware-vocabulary work.
- **`platform` is the conventional model part.** The platform's model of
  itself — the `Compose` configuration type and its sub-model — lives in
  `platform.tkdp`, authored by the platform and shipped into the definition
  dir with the source set; a platform upgrade refreshes the platform-owned
  parts along with the rest of the set. Root and parts share it:
  `from platform import Compose`. Platform-specific vocabulary
  (DockerSocket) lives there too.
- **Observed entry points, restated.** The root declares
  `config() -> Compose` — the platform's defaults, constructed from the
  model part — and `deployment(cfg: Compose, cx: Context) -> Deployment`.
  Nothing else is observed.
- **Parts enter through `define`.** Convention, not contract:
  `define(cfg, cx, d, …handles) -> None` — configuration first, context,
  the deployment being described, then the typed module handles the part
  builds on.
- **The service model.** `Service` carries the workload description —
  image, replicas, published ports, volumes (`State(sub=…, at=…)` for
  state-dir subpaths, `Config(sub=…, at=…)` for config artifacts out of the
  definition dir, `DockerSocket()` where the platform needs it),
  environment entries, command — the shape the plane split realizes.

## The mirror where the languages part

| tkd | tkdp |
|---|---|
| `use tokeira_platform::Context;` | `from tokeira_platform import Context` |
| `platform.tkd` model part, `pub struct Compose` | `platform.tkdp` model part, `@dataclass class Compose` |
| a part and a root item may share a name (module and value namespaces are distinct) | the from-form is the safe spelling; a plain `import` the root would shadow is refused with a pointer to the from-form |

## Open questions the sketch carries

- `depends_on=["mimir", "loki"]` beside handle-deps states ordering twice —
  Compose's runtime ordering versus the graph's provision ordering. Decide
  whether both survive, and if so, what each means.
- The service companions (State, Config, Environment) ride with Service in
  `tokeira_provisioner` here; the platform-source-set work may want `Config`
  volume sources bound to source-set paths more formally.
- `platform` shadows a CPython stdlib module name outside the sandbox
  (Monty has no `platform` builtin, so in-sandbox resolution is
  unambiguous).
