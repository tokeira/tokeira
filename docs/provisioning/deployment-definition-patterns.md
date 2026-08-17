# Definition patterns and current practice

This guide shows how the abstract
[deployment-definition programming model](deployment-definitions.md) appears in the
current Compose implementation. Compose is the complete definition-backed proof: it
supplies a canonical definition, typed configuration and context, a closed first-party
kind set, a concrete provisioner, and the catalog-selected `tkr` route.

The selected definition frontend evaluates source into one completed structural graph.
Frontend handles are private implementation details; platform code receives only the
completed graph and the host-free configuration value.

## Pattern map

| Concern | Current example | Source |
|---|---|---|
| Separate editable values from deployment structure | `config()` and `deployment()` | [Compose deployment root](../../platforms/compose/deployment.tkd) |
| Author the configuration shape once, in the definition | `struct Compose` and kind input validation | [Compose platform part](../../platforms/compose/platform.tkd) |
| Receive platform runtime facts | the framework-supplied evaluation context | [Engine](../../crates/tokeira-tkp/src/engine.rs) |
| Select modules from config variants | Conditional DSQL module | [Compose deployment root](../../platforms/compose/deployment.tkd) |
| Keep host paths out of source | Logical `Volume` values | [Compose kinds](../../crates/tokeira-compose/src/kinds/mod.rs) |
| Preserve configuration coupling | Resource dependencies plus content digests | [Compose kinds](../../crates/tokeira-compose/src/kinds/mod.rs) |
| Decode kinds from the declared vocabulary | `Vocabulary::of` over the declared kind sets | [Platform declaration](../../crates/tokeira-platform/src/declaration.rs) |
| Keep checking and execution on one evaluation path | `evaluate_with_context()` | [Compose provisioner](../../platforms/compose/src/lib.rs) |
| Bind one platform and frontend into `tkp` | Generated composition root | [Provisioner composition](../../crates/tokeira-build/src/composition.rs) |

## Keep configuration and topology separate

The Compose definition puts operator-editable values in ordinary config structs and
keeps structural graph construction in `deployment()`:

```rust
struct Compose {
    #[create]
    storage: Storage,
    tokeirad: Tokeirad,
    observability: Observability,
}

fn deployment(cfg: Compose, cx: Context) -> Deployment {
    let d = Deployment::new(&["default"]);
    let local_state = d.module("local_state", vec![]);
    local_state.resource("dir", LocalStateDir {});

    // Conditional resources and services follow.
    d
}
```

This split gives each half one job:

- `config()` remains host-free, comparable operator data;
- `deployment()` owns modules, resources, dependencies, output references, writeback,
  and context use.

The frontend returns the config as `LocatedValue`; Compose immediately admits it into
`ComposeConfig` through serde with unknown fields denied and then applies pure platform
validation.

## Use enum variants to select structure

The canonical Compose source models storage as an enum. Only the DSQL variant adds the
DSQL module and resources:

```rust
if let Storage::Dsql(storage) = &cfg.storage {
    let dsql = d.module("dsql", vec![local_state]);
    let cluster = dsql.resource(
        "cluster",
        DsqlCluster {
            identity: format!("{}-compose", cx.project_name),
            region: storage.region.clone(),
            mode: storage.mode.clone(),
            endpoint: storage.endpoint.clone(),
            arn: storage.arn.clone(),
        },
    );

    d.writeback(
        "infrastructure.dsql.endpoint",
        cluster.output("cluster_endpoint"),
    );
}
```

This is preferable to constructing an always-present resource with sentinel strings.
The variant states both the data shape and whether the desired module exists, so checking
and execution make the same structural decision.

The `#[create]` marker records that changing storage is intended to be a retarget
decision. It should not be described as an automatic apply refusal until the command
host enforces that policy against the prior admitted configuration.

## Use stable modules and explicit dependency layers

Compose groups resources under named modules and declares prerequisites by module name:

```rust
let local_state = d.module("local_state", vec![]);
let runtime = d.module("runtime", vec![local_state]);
let observability = d.module("observability", vec![local_state, runtime]);
```

The names become part of operation selection, state ownership, and explanation. Keep
them stable. Add module edges for ordering between groups; do not rely on source order.

Resource dependencies are separate. Services consuming rendered observability files
depend on the configuration resource explicitly:

```rust
let config_files = observability.resource(
    "config_files",
    ObservabilityConfiguration { /* ... */ },
);

observability.resource(
    "mimir",
    Service { /* ... */ },
    vec![config_files],
);
```

During placement, the service retains that resource dependency and carries the
configuration's `ContentIdentity` digest in its desired environment. This makes a
content change visible to planning without reviving a separate artifact catalog.
`Service.depends_on` is different again: it records Docker Compose service start order.

## Use platform defaults for sparse kinds

The Compose service vocabulary has many optional fields. Definitions overlay the fields
that matter onto `Service::EMPTY`:

```rust
Service {
    image: cfg.tokeirad.image.clone(),
    replicas: cfg.tokeirad.replicas,
    publish: vec![cfg.tokeirad.grpc_port, cfg.tokeirad.metrics_port],
    aws_region: match &cfg.storage {
        Storage::Dsql(storage) => Some(storage.region.clone()),
        _ => None,
    },
    ..Service::EMPTY
}
```

The frontend asks the platform's compile-time `KindFunctions` for the default
`LocatedValue`. The same closed function set then decodes the completed value into the
serde `Service` type. Unknown fields are rejected by `#[serde(deny_unknown_fields)]`;
there is no string-keyed plugin registry or public object protocol.

When changing a defaulted kind, keep its default field set and serde type in lockstep.
Prefer no default to a misleading default that changes resource identity or security
posture.

## Use logical volumes instead of host paths

Definitions describe volume intent and container targets, not machine-specific source
paths:

```rust
volumes: vec![
    Volume::State(StateVolume {
        sub: "grafana".into(),
        at: "/var/lib/grafana".into(),
    }),
    Volume::Config(ConfigVolume {
        sub: "grafana/dashboards".into(),
        at: "/var/lib/grafana/dashboards/".into(),
    }),
],
```

Compose resolves these typed values beneath the deployment's state and configuration
directories during invocation-bound placement. `Volume::DockerSocket` is an explicit
platform-owned capability, not a general path escape. The author-visible Compose context
therefore needs only `project_name`; the deployment directory remains an execution fact.

## Bind outputs through structural references

`resource()` returns a frontend-private handle that can create checked structural output
references:

```rust
let cluster = dsql.resource("cluster", DsqlCluster { /* ... */ });
d.writeback(
    "infrastructure.dsql.endpoint",
    cluster.output("cluster_endpoint"),
);
```

The completed graph contains the logical module, resource, and declared output name.
Graph verification rejects unknown resources or outputs. Compose later maps the verified
reference to the realized resource ID and collects writeback from committed
infrastructure state. The transient graph is never serialized and never becomes a
second desired-state authority.

## Keep checking pure and realization invocation-bound

Compose uses one evaluation path for standalone checks and deployment execution:

```rust
evaluate_definition(
    &self.frontend,
    source,
    context,
    services::kind_functions(),
    config::validate,
)
```

`definition check` evaluates and verifies the completed graph, including pure
`ProviderKind::validate_input` calls, but does not fabricate a deployment identity or
realize resources. Execution verifies the same evaluated set and then realizes it once
with the real deployment ID, directory, and dependency content identities.

This boundary keeps source evaluation deterministic while leaving environment, provider
clients, state stores, and filesystem effects in the concrete platform execution path.

## Assemble and provenance-bind the provisioner

No platform owns a committed `src/bin/tkp.rs`. Cargo-metadata descriptors select one
platform package and one definition frontend. `tokeira-build` generates a disposable
composition root with exactly three dependencies: the selected platform, the selected
frontend, and the generic provisioner shell.

Its generated `main.rs` binds conventional exports:

```rust
tokeira_provisioner_cli::bound_provisioner_main!(
    expected_platform: "compose",
    platform: selected_platform::provisioner,
    expected_format: "tkd",
    frontend: selected_frontend::frontend,
);
```

The generated root contains no platform dispatch. `tkr` builds and records this
point-in-time selection, including platform, format, definition path, source closure,
lock closure, and generated-root evidence. See [`tkr` and `tkp`](tkr-and-tkp.md) for the
provenance chain.

## Review checklist

When borrowing a pattern from current source, check the contract rather than copying its
shape mechanically:

- Is config admitted through one serde type with pure validation?
- Is a structural branch better modeled as an enum variant?
- Is the logical module or resource name stable across ordinary edits?
- Is the dependency a module edge, resource edge, or service start-order edge?
- Does a content-consuming resource retain both dependency and digest coupling?
- Are platform-specific runtime facts carried by a typed context?
- Does checking validate without realization?
- Do checking and execution evaluate the same structural definition?
- Does writeback name an actual declared output and persistence owner?
- Is the kind part of a small compile-time first-party set?
- Does the generated provisioner bind exactly one platform and one frontend?

## Further reading

- [Deployment definition programming guide](deployment-definitions.md) — language and
  authoring rules.
- [Provisioning overview](README.md) — matched language and engine architecture.
- [The platform provisioner](provisioner.md) — lifecycle seam, state, and transitions.
- [`tkr` and `tkp`](tkr-and-tkp.md) — construction, identity, placement, and verification.
- [Extending the IaC framework](../iac/extending.md) — resource and provider seams.
- [Compose definition set](../../platforms/compose/deployment.tkd),
  [kind set](../../crates/tokeira-compose/src/kinds/mod.rs), and
  [provisioner](../../platforms/compose/src/lib.rs) — the complete current Compose path.
