# Deployment Definitions

A Tokeira deployment — its services, its storage, its observability stack, the wiring between
them — is described by a single file called `definition.tkd`. The file is written in a small,
closed dialect of Rust, but it is **data, not code**: the provisioner (`tkp`) parses and interprets
it at runtime, every time. Editing it and running `tkp apply` is the whole change workflow. Nothing
is compiled, and the interpreter rejects — by construction, with a named error — anything in the
file that is not a description of a deployment.

This guide is for the person who authors or operates such a file. It assumes little or no Rust
experience: the dialect is small enough to learn from this page. The reference definition it walks
through is `platforms/compose-syn/definition.tkd`, the live definition of the Docker-compose
platform. The engine underneath (resources, plan/apply, state) is documented separately in
[`docs/architecture/120-iac-framework.md`](../../architecture/120-iac-framework.md); deeper design
reading is collected at the end.

## The one idea

Everything about `.tkd` files follows from one split:

> The **engine** — the interpreter, the vocabulary of things a definition may name, and the code
> that turns those names into real infrastructure — is compiled into a versioned `tkp` binary.
> The **definition** is data that engine reads. Editing the definition is an ordinary `apply`;
> changing the engine is an `upgrade`, a deliberate, gated, reversible event.

The dialect enforces this split at the language level. A definition can *name* the vocabulary the
engine provides (`Service`, `DsqlCluster`, the `d.module(…)` calls you'll meet below) but cannot
define new resource kinds, do I/O, read the environment, or otherwise alter how applying works. So
an operator edit is *structurally guaranteed* to stay a configuration change — it can never
silently become an engine change. That guarantee is why the operator owns the file: values **and**
structure (adding a service, rewiring dependencies) are both fair game for an edit-and-apply.

Each applied edit advances an ever-increasing **config revision** and retains the file's exact
content, so any prior revision can be restored with `tkp revert`. The engine's own identity is
tracked separately (a **provenance stamp** recorded in the deployment's state), and the applying
verbs first check that the running binary matches the recorded one — the **binding gate**. More on
revisions under [The everyday loop](#the-everyday-loop-edit-plan-apply), and on the gate under
[Operating a deployment](#operating-a-deployment).

## Reading a definition

A definition has two halves, in one file:

1. **`config()` — the knobs.** A handful of type declarations (what settings exist, what shapes
   they take) and one function returning their current values. To change a setting, edit a value
   here.
2. **`deployment(cfg, cx)` — the structure.** One function that reads those settings and declares
   the deployment: which infrastructure resources exist, which services run, and how they depend
   on each other.

### The Rust you need

The table below is not a starting point — everything outside it is rejected, so it is the whole
language. (Plus a handful of value shims — `.into()`, `.clone()`, `.to_string()`, `.as_str()`,
`.as_deref()`, `.is_some()`, `.is_none()` — and the comparisons, `!`, `&&`/`||`, and `matches!`
used in constraints and conditions.)

| You'll see | It means |
|---|---|
| `struct Tokeirad { image: String, replicas: u32, … }` | A named bundle of fields: every `Tokeirad` value has an `image` (text) and a `replicas` (number). Declares the *shape* of a setting. |
| `enum Storage { InMemory, Dsql { region: String, … } }` | A choice between named alternatives, each optionally carrying its own fields. A `Storage` is *either* `InMemory` *or* `Dsql{…}`. |
| `Tokeirad { image: "tokeirad:latest".into(), replicas: 1, … }` | Constructing a value of that shape — the thing you actually edit. `.into()` and `.clone()` are conversion noise the interpreter treats as identity; read past them. |
| `X::y` | "The `y` belonging to `X`": an enum alternative (`Storage::InMemory`, `DsqlMode::Managed`), a constant (`Service::EMPTY`), or the one constructor a definition calls (`Deployment::new`). |
| `..Service::EMPTY` | "Every field I didn't write takes its default." Closes every `Service { … }`; this exact spelling is the only spread allowed, and only `Service` has defaults. |
| `#[create]` / `#[require(…)]` | Annotations attached to the field or type declared **immediately below** them. *Not* comments (comments are `//`) — never delete one as noise. Their meaning: [The guardrails](#the-guardrails). |
| `cfg.tokeirad.image` | Reading a field. |
| `let runtime = d.module("runtime", &["local_state"]);` | Naming a result so later lines can use it (`d` is the deployment being built — see the structure half). The file's `let mut d = …` opener is the same thing; the `mut` is required noise on that one line only. |
| `if let Storage::Dsql { region, .. } = &cfg.storage { … }` | "If the storage choice is `Dsql`, take its `region` and do the following." The `..` means "ignore the other fields"; the `&` is a technicality the interpreter sees through (in signatures like `cfg: &Compose` too). |
| `match &cfg.storage { Storage::Dsql { region, .. } => …, _ => … }` | The multi-way version of the same question; `_` is "anything else". |
| `vec![9009]` and `&["local_state"]` | Both are lists. The rule of thumb: lists inside a `Service { … }` or other kind literal are `vec![…]`; name lists handed to `Deployment::new`/`d.module` are `&[…]`. Copy the form you see in that position. |
| `("GF_SECURITY_ADMIN_USER".into(), "admin".into())` | A pair — how `env:` entries are written. |
| `format!("{}-dsql-rate-limiter", cx.project_name)` | Building text from parts: `{}` is filled by each argument in order. |
| `Some("x".into())` / `None` | A value that is present / absent, for optional fields (declared as `Option<String>` in the shape). |
| `fn config() -> Compose { … }` | A function returning a `Compose`. The file has exactly two that matter, and their headers are fixed scaffolding — edit what's between the braces. |

### The config half

The top of `definition.tkd` declares the settings and their defaults:

```rust
enum Storage {
    InMemory,
    Dsql {
        region: String,
        mode: DsqlMode,                 // Managed | Preexisting
        endpoint: Option<String>,
        arn: Option<String>,
    },
}

struct Compose {
    #[create]
    storage: Storage,
    tokeirad: Tokeirad,
    observability: Observability,
}

fn config() -> Compose {
    Compose {
        storage: Storage::InMemory,
        tokeirad: Tokeirad {
            image: "tokeirad:latest".into(),
            replicas: 1,
            grpc_port: 7233,
            metrics_port: 9090,
        },
        // … observability defaults elided …
    }
}
```

Two things to notice:

- **The operator's config *is* the `config()` literal.** There is no second file of overrides:
  changing a default means editing the value in place. Switching to persistent storage is
  replacing `Storage::InMemory` with
  `Storage::Dsql { region: "us-east-1".into(), mode: DsqlMode::Managed, endpoint: None, arn: None }`.
- **`#[create]` marks a create-time decision.** Fields carrying it (here: which storage backs the
  deployment) are chosen once, when the deployment is first created. Changing one later is not a
  *reconcile* (the normal path: adjust live infrastructure to match the edited file) but a
  **retarget** — the provisioner's contract is to refuse it rather than quietly rebuild the
  deployment around it. See [Create-time fields](#create-time-fields-create).

### The structure half

`deployment(cfg, cx)` receives the resolved config (`cfg`) and an engine-injected context (`cx`),
and declares the deployment top to bottom. The moving parts, in the order the file introduces
them:

```rust
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);          // the deployment + its workflow namespaces

    let local_state = d.module("local_state", &[]);     // a module: a named group of resources
    d.resource(&local_state, "dir", LocalStateDir);     // a resource: one piece of infrastructure

    if let Storage::Dsql { region, mode, endpoint, arn } = &cfg.storage {
        let dsql = d.module("dsql", &["local_state"]);  // this module needs local_state first
        let cluster = d.resource(&dsql, "cluster", DsqlCluster {
            region: region.clone(),
            mode: mode.clone(),
            endpoint: endpoint.clone(),
            arn: arn.clone(),
        });
        // … two DynamoDbTable resources (rate_limiter, conn_lease) and four more
        //   writeback lines elided …
        d.writeback("infrastructure.dsql.endpoint", cluster.output("cluster_endpoint"));
    }

    let observability = d.module("observability", &["local_state"]);
    let o = &cfg.observability;
    d.service(&observability, "mimir", Service {        // a service: a running workload
        image: o.mimir.image.clone(),
        replicas: o.mimir.replicas,
        publish: vec![9009],
        volumes: vec![
            cx.state("mimir", "/data"),
            cx.config("mimir.yaml", "/etc/mimir/mimir.yaml"),
            cx.config("mimir/rules", "/data/mimir/rules"),
        ],
        command: vec!["--config.file=/etc/mimir/mimir.yaml".into()],
        ..Service::EMPTY                                // every field not written = its default
    });
    // … the config_files resource, the loki/grafana/alloy services, and the
    //   runtime module with the tokeirad service elided …

    d                                                   // the last line is the return value
}
```

The vocabulary in play:

- A **module** groups resources and orders infrastructure work: `d.module("dsql",
  &["local_state"])` declares that everything in `dsql` is provisioned after `local_state`.
- A **resource** is one piece of infrastructure of a given **kind** (`DsqlCluster`,
  `DynamoDbTable`, `LocalStateDir`, …). The kind's fields are its configuration; the engine knows
  how to create, diff, and destroy each kind. `d.resource(…)` returns a handle — hold it in a
  `let` if something later needs this resource's outputs.
- A **service** is a running workload (a container, on this platform). Note the two distinct
  dependency ideas: the *module argument* groups a service's infrastructure, while its `needs:`
  field orders service startup (grafana `needs` mimir and loki; that says nothing about how their
  infrastructure is grouped).
- A **writeback** projects a fact the engine only learns by provisioning — here the DSQL cluster's
  endpoint — into the running server's own config file (`tokeirad.toml`). `cluster.output("cluster_endpoint")`
  is a deferred reference, resolved after apply from recorded state. Writeback lines are
  engine-plumbing rather than knobs: they exist so `tokeirad` finds the storage that was just
  provisioned. Edit them only if you know the server-config schema on the other end. (Status
  note: the file-write itself is not yet wired into `tkp apply` — see
  [From definition to running containers](#from-definition-to-running-containers).)
- **`cx`** is the deliberately tiny window onto the outside world: the deployment's name
  (`cx.project_name`), an optional recorded region (`cx.region`), and three volume helpers
  (`cx.state(…)`, `cx.config(…)`, `cx.docker_sock()`). There is no path, environment, or
  filesystem access — where a mount lands on the host is the engine's business, not the
  definition's.

The conditional DSQL block is the dialect earning its keep: structure follows configuration. When
`storage` is `InMemory` the `dsql` module simply does not exist — no resources, no writeback — and
the same file, unedited, describes both shapes.

## Before the first apply

Both tools build from this workspace: `cargo build --release -p tkp -p tkr` leaves `tkp` and
`tkr` in `target/release/`. `tkp` needs nothing but a **deployment directory** — any directory
holding the definition and the state the provisioner keeps about it.

### Where things live

(`tkr`-created deployments live under `~/Library/Application Support/tokeira/tkr/<name>/` on
macOS; a bare directory works just as well.)

| Entry | Written by | What it is |
|---|---|---|
| `definition.tkd` | **you** | the deployment definition. Its *presence* selects the compose-syn platform; without it `tkp` treats the directory as a minimal `local` deployment configured by `deployment.toml` |
| `deployment.toml` | `tkr deployment create`, then you | platform bootstrap config; `tkp init` reads its `project_name` as the deployment identity (default `tokeira`) |
| `tokeirad.toml` | `tkr deployment create`; patched by writeback (today via `tkr infra apply` — `tkp apply` does not write it yet) | the *server's* config — a different file with a different owner, reached from the definition only through `d.writeback(…)` |
| `state/envelope/` | `tkp` | the **deployment state envelope**: recorded binding, integrity manifest, `config_revision`, rollback checkpoint, operation marker |
| `state/config-revisions/<n>/` | `tkp init`/`apply`/`revert` | the retained definition source per revision — what `revert` restores |
| `state/lock/` | `tkp` | the operation lock (see [Two locks](#two-locks-two-jobs)) |
| `state/infra/`, `state/deploy/` | the engines | recorded infrastructure and workload state |

Note what's missing: `tkr deployment create` does **not** write a `definition.tkd`. Creating a
`.tkd`-backed deployment today means placing the file yourself — start from a copy of
`platforms/compose-syn/definition.tkd`:

```bash
mkdir -p ~/deployments/dev
cp platforms/compose-syn/definition.tkd ~/deployments/dev/
```

Every `tkp` verb picks it up by presence.

### Day 0: init

```bash
tkp init --deployment-dir ~/deployments/dev
```

```text
initialized deployment 'tokeira' — stamped with provisioner 0.1.0 (Dev), source_tree_hash 3fa1b2…
```

`init` is mandatory versioning before anything exists: it stamps the envelope with the running
binary's provenance (version, git SHA, source-tree hash, build mode) and an integrity checksum of
the binary itself, then retains the definition as revision 0. It refuses to run twice. From then
on, no state in this deployment is ever unattributed — which is what makes every later verdict
possible. (Driving through `tkr`? `tkr deployment apply` runs `init` for you on first contact.)

## The everyday loop: edit, plan, apply

```bash
vi ~/deployments/dev/definition.tkd          # 1. edit — say, replicas: 1 → 2 on mimir
tkp plan  --deployment-dir ~/deployments/dev # 2. preview what would change (read-only)
tkp apply --deployment-dir ~/deployments/dev # 3. make it so
```

`plan` prints the platform, whether `apply` would be allowed to proceed, and the engine's diff of
desired against live state — without touching anything (it does not even need the Docker daemon
running):

```text
platform: compose-syn
binding:  Match — apply would proceed
infra plan: 1 change(s)
  Update [compose_service] observability::mimir
```

`apply` gates, applies, and records:

```text
binding: Match (authoritative) — proceeding
[compose-syn] infra apply: 1 change(s)
envelope: config_revision now 5 (config sha256:8c1d…)
```

(The *envelope* is `tkp`'s own record about this deployment — its recorded engine, revision
counter, and history; the layout is under [Where things live](#where-things-live).)

That last line is the revision machinery at work. Every `apply` advances `config_revision` and
snapshots the exact definition it applied under `state/config-revisions/<n>/definition.tkd`.
History is append-only and nothing prunes it, which buys an undo that is itself ordinary:

```bash
tkp revert --deployment-dir ~/deployments/dev --to 4
```

```text
binding: Match (authoritative) — proceeding
restored config revision 4 → /Users/op/deployments/dev/definition.tkd
[compose-syn] revert reconcile: 1 change(s)
envelope: config_revision now 6 (content of revision 4)
```

A revert restores the retained snapshot **over the live file**, reconciles, and moves *forward* —
revision 6's content equals revision 4's. Reverts are therefore revertable, and history never
rewrites. Two refusals to know about: `--to` must name a revision *older* than the current one,
and only revisions actually retained by a prior `init`/`apply` on this platform can be restored.

Structural edits use the same loop. Adding a whole service to the observability module is one
block in the definition — accepted by the interpreter exactly as written here (the rules are in
[The guardrails](#the-guardrails), next):

```rust
d.service(&observability, "pyroscope", Service {
    image: "grafana/pyroscope:1.5.0".into(),
    replicas: 1,
    publish: vec![4040],
    ..Service::EMPTY
});
```

…followed by `tkp plan` (expect `Create [compose_service] observability::pyroscope`) and
`tkp apply`. Success is visible from the outside: `docker ps` shows the new pyroscope container
publishing 4040. Removing the block and re-applying removes the service. No plugin, no recompile,
no new binary: the *shape* of the deployment is configuration.

## The guardrails

Three layers are defined between an edit and the engine — two run on every load today; the third
is contract, pending wiring (see its note below). Each fails closed with a message naming what it
refused. In order:

### The interpreted subset

Before evaluating anything, the interpreter walks the whole file against an allow-list — the
constructs in [The Rust you need](#the-rust-you-need), the builder vocabulary, three macros
(`vec!`, `format!`, `matches!`), and comparison/boolean operators. Everything else is rejected
*by default*: the question is never "is this construct banned?" but "is it allowed?". In practice:

| If you write… | The interpreter says… |
|---|---|
| `for x in list { … }` | ``expression not allowed: `for-loop` `` |
| `std::env::var("HOME")` | ``call `std::env::var` is not allowed`` |
| `path.exists()` (or `.join()`, or any unknown method) | ``method `exists` is not allowed (no I/O, filesystem, env, or arbitrary calls in a `.tkd`)`` |
| `println!("debug")` | ``macro `println!` is not allowed`` |
| `use std::fs;` | ``item not allowed in a `.tkd`: `use` `` |
| `replicas: 1 + 1` | ``binary operator not allowed`` (there is no arithmetic — write `2`) |
| `let svc = Service { … };` | ``a kind must be used inline as a `resource`/`service` argument, not bound to a `let` `` |
| `..Service::DEFAULT` | ``struct spread must be `..<Type>::EMPTY` `` |

Subset violations are all collected in one pass and reported together, prefixed
`definition is outside the interpreted subset:`. Mistakes *within* the subset — a misspelled field
or enum variant — surface at evaluation, wrapped by `tkp` as ``invalid `.tkd`: …`` with the same
fail-closed posture: ``unknown field `imgae` ``, `` `Storage` has no variant `Dssql` ``,
`` `Compose` is missing field `storage` ``. `tkp` interprets the definition when loading it, so
every one of these refusals lands during `plan`/`apply` load — after the binding verdict is
reported, but before any engine work touches state or infrastructure.

This is the hermeticity promise: a definition cannot read the clock, the environment, or the
filesystem, cannot loop, and cannot call anything the engine didn't hand it. Interpreting the same
file always produces the same deployment. It is also the security model — reviewing a `.tkd` edit
never requires asking "what else might this line *do*?"

### Config admission: `#[require]`

Cross-field constraints are declared on the config types and checked between resolving `config()`
and building the structure — so an inconsistent config is refused before anything is planned:

```rust
#[require(replicas > 0)]
struct Backend {
    image: String,
    replicas: u32,
}
```

A violation aborts with ``config constraint failed: #[require] on `Backend` ``. The constraint is
checked against *every* value of that type in the config (each `Backend` here). The expression
language is the subset's boolean slice: comparisons, `&&`/`||`/`!`, `matches!(field, Pattern)` for
asking which alternative a choice holds, and `.is_some()`/`.is_none()` on optional fields. The
shipped compose definition currently declares no `#[require]`s — its invariants hold by type shape
alone — but the machinery runs on every load.

### Create-time fields: `#[create]`

`#[create]` marks the fields whose edit is a **retarget** — the recorded storage choice, in the
compose definition. The contract: on re-apply, the provisioner diffs each `#[create]` field
against the value recorded when the deployment was created and refuses a change with

```text
`Compose.storage` is create-time-immutable; changing it is a retarget, refused (not reconciled)
```

because "reconciling" a flipped storage backend would mean destroying and re-creating the
deployment's stateful heart — a decision that deserves a deliberate migration, not an `apply`.
Non-`#[create]` fields (images, replicas, ports) reconcile freely.

One honest status note: the diff itself (`retarget_check`) is implemented and test-covered in the
interpreter, but `tkp apply` does not yet call it against the recorded revision — the
recorded-config wiring is in flight. Until it lands, treat `#[create]` as binding contract rather
than enforced gate, and don't edit those fields casually.

## From definition to running containers

What happens on `tkp apply`, end to end:

1. **Parse and check.** The file is parsed with the same parser real Rust uses, and the subset
   allow-list walks every item, statement, and expression.
2. **Resolve config.** `config()` is evaluated to a plain value tree; `#[require]` constraints are
   checked against it.
3. **Build structure.** `deployment(cfg, cx)` runs; each builder call records a module, resource,
   service, or writeback into an in-memory deployment. Nothing external is touched.
4. **Plan.** The engine (see [`docs/architecture/120-iac-framework.md`](../../architecture/120-iac-framework.md))
   refreshes live state per resource, diffs desired against actual, and produces
   Create/Update/Delete changes ordered by module dependencies.
5. **Apply.** Changes execute in dependency order (deletes in reverse), with state saved
   incrementally for crash-safety. This is where each declared item is *realized* into its
   real-world form — a `Service { … }` literal becomes a running container, and the realize-time
   mechanics the vocabulary tables mention (the `server_config` mount, the `aws` edge, volume
   paths) are attached by the engine here, outside the hermetic file.
6. **Write back** *(wiring in flight)*. Deferred `output(…)` references are resolvable from the
   freshly recorded state, and the resolved values are destined for `tokeirad.toml` — but
   `tkp apply` does not yet perform this write; today only the classic `tkr infra apply` path
   patches `tokeirad.toml`. Until the wiring lands, a DSQL deployment driven purely by `tkp`
   needs its server config completed by hand or via `tkr infra apply`.
7. **Record.** The envelope is re-stamped: `config_revision` advances, the applied definition is
   retained, the effective config's digest is recorded.

The interpreter and vocabulary sit *inside* the engine's versioned surface — which closes the
loop on [the one idea](#the-one-idea): the definition can only combine what the recorded engine
provides, so what a revision *means* is pinned by the binding, and the same retained revision
re-applied by the same engine builds the same deployment.

## The vocabulary

The complete authoring surface, as built. If it isn't on this page or in
[The Rust you need](#the-rust-you-need), the interpreter rejects it.

### Builder verbs

| Call | Returns | Declares |
|---|---|---|
| `Deployment::new(&["ns", …])` | the deployment handle `d` | the deployment and its runtime namespaces — the tokeirad (workflow) namespaces the server serves, not container or Kubernetes namespaces |
| `d.module(name, &[needs…])` | a module handle | a resource group + which modules must be provisioned first |
| `d.resource(&module, id, Kind { … })` | a resource handle | one infrastructure resource of the given kind |
| `d.service(&module, name, Service { … })` | nothing | one workload |
| `d.writeback(key, value)` | nothing | project a constant or a resource output into the server config; `key` is a dotted path in `tokeirad.toml` |
| `r.output(name)` | an output handle | a deferred reference to a provisioned property of resource `r` (only usable as a writeback value) |

Kind literals are **take-once and inline**: a `DsqlCluster { … }` or `Service { … }` must appear
directly as the argument to `d.resource(…)`/`d.service(…)`, never parked in a `let` — the subset
enforces this so a half-built resource can't be reused or forgotten.

### Kinds (compose platform)

| Kind | Fields | It becomes |
|---|---|---|
| `LocalStateDir` | *(none — written as a bare name)* | the deployment's local state directory |
| `DsqlCluster` | `region`, `mode` (`DsqlMode::Managed` \| `Preexisting`), `endpoint: Option<String>`, `arn: Option<String>` | an Aurora DSQL cluster — created when `Managed`, adopted via `endpoint`/`arn` when `Preexisting` |
| `DynamoDbTable` | `table`, `hash_key`, `ttl: Option<String>` | an on-demand DynamoDB table (DSQL coordination) |
| `ObservabilityConfigFiles` | nine fields: `scrape_host`, `scrape_port`, `cluster`, `deployment`, `mimir_remote_write`, `loki_push`, `mimir_http_port`, `loki_http_port`, `retention_hours` | the rendered mimir/loki/grafana/alloy config tree |
| `Service` | see below | a container: an infra resource *and* a deploy-engine workload |

Only `Service` has defaults (`..Service::EMPTY`); every other kind requires all of its fields.
Output names valid in `r.output(…)` are the kind's recorded state properties — the useful ones:
`DsqlCluster` → `cluster_endpoint`, `cluster_arn`, `cluster_id`, `cluster_identity`, `mode`;
`DynamoDbTable` → `table_name`, `billing_mode`, `ttl_attribute`; `LocalStateDir` → `path`. A name
that doesn't exist in recorded state resolves to nothing and the writeback entry is silently
dropped — check spelling against this list.

### `Service` fields

| Field | Type | `EMPTY` default | Meaning |
|---|---|---|---|
| `image` | text | `""` | container image reference |
| `replicas` | number | `0` | desired replica count |
| `publish` | list of ports | `[]` | ports published as `host:container` (same number both sides) |
| `volumes` | list of `Vol` | `[]` | mounts, built with the `cx` helpers below |
| `env` | list of `("KEY", "value")` pairs | `[]` | environment variables |
| `command` | list of text | `[]` | container command arguments |
| `needs` | list of service names | `[]` | which *services* must start first (deploy ordering — distinct from module grouping) |
| `server_config` | flag | `false` | intent: "this is the server" — at realize time the engine mounts `tokeirad.toml` (if present) at `/etc/tokeira/tokeirad.toml` and sets `TOKEIRA_CONFIG` |
| `aws` | optional region text | `None` | intent: "this workload reaches AWS" — at realize time the engine mounts `~/.aws` read-only, sets `AWS_REGION`, and forwards the standard `AWS_*` credential variables from the host environment |

`server_config` and `aws` illustrate the division of labour: the definition states *intent*, and
the versioned engine performs the environment-dependent mechanics (probing for `tokeirad.toml`,
reading host credentials) at realize time — outside the hermetic file, inside the auditable
engine.

### What `cx` offers

| Expression | Yields |
|---|---|
| `cx.project_name` | the deployment's name (recorded at init; seeds derived resource names via `format!`) |
| `cx.region` | the recorded AWS region as an optional value (currently always absent under `tkp`) |
| `cx.state(sub, at)` | a volume: persistent state subdirectory `sub`, mounted at container path `at` |
| `cx.config(sub, at)` | a volume: rendered config `sub`, mounted at `at` |
| `cx.docker_sock()` | the one vetted raw mount: the Docker socket |

Anything else — including any path — is not there: ``  `Cx` has no readable field `deployment_dir` ``.

## Operating a deployment

Day to day, the [everyday loop](#the-everyday-loop-edit-plan-apply) is all you need; this section
is for the days an engine changes or something refuses. `tkp` is deliberately small: one
deployment directory, nine verbs (the ninth, `resume` — picking up an operation interrupted
mid-flight — is scaffolded but not yet implemented), and a refusal for everything it cannot
verify. `tkp describe --deployment-dir <dir> [--json]` is the one-shot view of everything this
section discusses: identity, recorded provenance, binding verdict, integrity, and the current
`config_revision`.

### The binding gate

Every applying verb (`apply`, `destroy`, `revert`, `rollback`) starts by comparing the running
binary's provenance against the recorded one. (`init` predates any recording and refuses re-init
instead; `upgrade` has its own decision table, below.) The authoritative key is the **source-tree
hash** — a digest of the engine's source — not the version string, so "same code" is a fact, not
a claim:

| Verdict | Situation | Outcome |
|---|---|---|
| `Match` | versioned binary, hashes equal | proceeds, authoritative |
| `DevIterate` | dev binary on a dev-stamped deployment | proceeds with a warning — the bring-up loop |
| `Mismatch` | versioned hashes differ (also: a versioned binary meeting a dev-stamped deployment — the promotion case) | **refuses**: resolve with the matching binary, or `tkp upgrade` |
| `Downgrade` | running binary older than recorded | **refuses** |
| `ModeRegression` | dev binary on a versioned deployment | **refuses** |
| `Unknown` | no recorded stamp | **refuses** (fail closed) |

Refusals look like ``binding gate refuses `apply` (Mismatch): the running engine does not match
the deployment's recorded engine (source_tree_hash differs) — resolve with the matching binary or
`tkp upgrade` ``. The read-only verbs `describe` and `plan` never gate — they *report* the verdict
so you can see trouble before attempting mutation.

### Changing engines: upgrade and rollback

When the refusal is intentional — a new engine should take over — `tkp upgrade` (run with the
**new** binary) is the deliberate boundary. Its first act is one atomic commit: capture a
checkpoint of the outgoing engine's world (its provenance, integrity manifest, state heads, config
reference), flip the binding to the new engine, and open an in-flight marker — all *before* any
infrastructure is touched. Then the new engine applies.

```text
upgrade: versioned advance 1.2.0 → 1.3.0
ownership transferred — [A final] checkpoint captured, operation marker open
infra apply under the new engine: 8 change(s)
upgrade complete — now bound to version 1.3.0
```

(`A` is upgrade-speak for the outgoing engine, `B` the incoming one; the checkpoint is A's final
state — what `rollback` would restore.) `upgrade` has its own decision table rather than the gate
(a hash mismatch is exactly what it exists to resolve): it accepts a versioned advance or a
dev→versioned promotion, and refuses downgrades, dev→dev (that's just `apply`), same-hash no-ops,
and a same-version-different-hash "forgotten version bump".

`tkp rollback` is the undo of an *upgrade* — re-pin to the checkpointed prior engine, which then
re-applies its retained configuration. Contrast with `revert`, the undo of a *config edit*:

|  | undoes | moves the binding | mechanism |
|---|---|---|---|
| `revert --to N` | a definition edit | no | restore retained revision N, reconcile forward |
| `rollback` | an engine upgrade | yes — back to the checkpointed engine | delete what the new engine created, re-pin, prior engine reconciles |

(Rollback is a first increment today: the re-pin is real and a reconcile runs, but the reconcile
is a same-process re-apply of the *live* definition by the running binary — restoring the
retained revision's content and the two-binary hand-off where the prior engine itself reconciles
are follow-ons, as is wiring the "delete what the new engine created" pass to the engines. Run it
with the currently bound binary; the gate refuses anything else.)

### Two locks, two jobs

- **The operation lock** (`state/lock/`) serializes mutators on one deployment: every mutating
  verb holds a leased lock (renewed continuously; holder recorded as `tkp-<verb>-pid<PID>`) for
  its whole run. A second provisioner sees ``failed to acquire the remote operation lock — another
  provisioner may be operating this deployment``.
- **The deployment lock** (`tkr deployment lock`) is the registry-level mis-apply guard: it pins
  `tkr`'s mutating commands to one named deployment and fails closed if the target changed
  identity underneath it. `tkr deployment unlock --yes` clears it.

### Teardown

```bash
tkp destroy --deployment-dir ~/deployments/dev --yes
```

Gated like `apply`, and refuses without `--yes` (``… is irreversible; re-run with `--yes` to
confirm``). It removes the infrastructure but keeps the envelope's history — the binding stays,
`config_revision` is retained — so the directory remains an auditable record:

```text
binding: Match (authoritative) — proceeding
[compose-syn] infra destroy: 8 resource(s) removed
envelope: torn down (config_revision 4 retained)
```

`tkr deployment destroy <name> --yes` is a different tool for a different job: it deletes the
registry *directory* and deliberately never touches provisioned infrastructure — run the infra
teardown first.

### Driving through tkr

`tkr` is the everyday cockpit; for lifecycle verbs it is a launcher, not an actor. `tkr deployment
describe|apply|upgrade|rollback` resolve the selected deployment, pick the right `tkp` for the
job, and forward, streaming output through. For a versioned *mutation* that means the recorded
bound binary, byte-verified against the deployment's integrity manifest before launch; a dev
deployment gets a dev build; an upgrade launches the (unverified) candidate; and read-only
`describe` deliberately launches unverified, so it can diagnose exactly the states that block the
others. A versioned deployment with no verifiable `tkp` on the PATH is refused rather than
approximated. The division of trust: through these lifecycle verbs `tkr` never mutates a
deployment's infrastructure itself (its classic `infra`/`deploy` verbs drive the compiled
platforms directly); `tkp` never manages more than the one deployment it is pointed at.

## Troubleshooting

| You see | It means | Do |
|---|---|---|
| `definition is outside the interpreted subset:` + a list | the edit used constructs outside the dialect | each line names the construct and where; rewrite within [the vocabulary](#the-vocabulary) |
| ``invalid `.tkd`: unknown field …`` / `… has no variant …` | a typo inside the subset | fix the named field/variant; the kind and config tables above list valid names |
| `config constraint failed: #[require] on …` | the config violates a declared invariant | read the `#[require(…)]` on that type; fix the values |
| `binding gate refuses … (Mismatch)` | the running `tkp` is not the recorded engine | use the matching binary, or make the change deliberate with `tkp upgrade` |
| `binding gate refuses … (Unknown)` | the deployment was never initialized | `tkp init` (or `tkr deployment apply`, which inits on first contact) |
| `compose-syn apply/destroy needs a reachable Docker daemon` | plan works without Docker; mutation doesn't | start Docker, retry |
| `config revision N was not retained …` | `revert` target predates retention or belongs to another platform's config | `ls state/config-revisions/` to see what is restorable |
| `failed to acquire the remote operation lock …` | another mutator is (or died while) holding the lease | wait for the lease to lapse; check for a live concurrent `tkp` first |
| a writeback key never appears in `tokeirad.toml` | `tkp apply` does not yet write back (only `tkr infra apply` patches `tokeirad.toml` today) — or the `output(…)` name isn't a recorded property of that resource | check the name against the [kinds table](#kinds-compose-platform); complete the server config by hand or via `tkr infra apply` until the wiring lands |

## Status and further reading

| Platform | Definition | Status |
|---|---|---|
| `platforms/compose-syn` | `definition.tkd` (interpreted) | **live** — the reference `.tkd` platform, driven end-to-end by `tkp`; proven byte-identical to the compiled compose platform by a three-way fidelity harness |
| `platforms/eks` | `.tkd` (in progress) | interpreter bridge and 15 AWS/K8s kinds landed; its `definition.tkd`, adapter, and `tkp` wiring are the next blocks |
| `platforms/compose`, `platforms/ecs` | compiled Rust | the classic pre-DSL platforms, driven by `tkr`; compose doubles as compose-syn's fidelity oracle |
| `platforms/local` | `deployment.toml` | `tkp`'s fallback when no `definition.tkd` is present |

The interpreter itself is `crates/tokeira-tkd` — platform-agnostic, one `HostBridge`
implementation per platform (`platforms/compose-syn/src/bridge.rs` is the reference). A new
platform plugs in by supplying kinds and a bridge; the dialect, subset, and admission machinery
come for free.

Deeper reading, in order of usefulness:

- [`docs/architecture/120-iac-framework.md`](../../architecture/120-iac-framework.md) — the engine the
  definitions realize into: resources, modules, plan/apply, state persistence.
- `.kiro/specs/platform-config-dsl/proposals/003-rust-via-syn-deployment-definition.md` — why the
  dialect is a Rust subset, and the capability map it projects.
- `.kiro/specs/platform-config-dsl/proposals/004-tkd-syn-interpreter.md` — the interpreter's
  design and as-built record.
- `.kiro/specs/platform-provisioner-binary/` — the provenance, binding, upgrade, and rollback
  model `tkp` implements.
