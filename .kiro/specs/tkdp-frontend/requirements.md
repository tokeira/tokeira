# Tkdp Frontend Requirements

## Introduction

Tokeira's deployment definitions are evaluated through one definition-language-neutral contract:
`DefinitionFrontend` (`crates/tokeira-platform/src/definition.rs`) evaluates recorded source bytes with
a typed platform context and the platform's kind constructors, and returns one completed transient
structure — a host-free configuration value plus a completed structural graph. `tokeira-tkd` implements
that contract for Rust-syntax `.tkd`; Compose is migrated onto it and records `Definition_Format =
"tkd"` in deployment metadata.

This feature adds the second Definition_Frontend: Python-syntax `.tkdp` definitions evaluated by
Pydantic's Monty embedded as a Rust library, packaged as the workspace crate `tokeira-tkdp` with format
identifier `tkdp`. Operators author `definition.tkdp` with typed dataclass configuration and a
`deployment(cfg, cx)` entrypoint; a deliberately restricted `match` statement — which Monty does not
implement natively — is lowered ahead of execution into Monty-supported Python, with every position in
lowered output mapped back to the operator's file. The lowering, restricted pattern subset, source
mapping, and Monty capability envelope were validated end-to-end by `spikes/monty-tkdp`; this
specification productizes that result behind the platform contract.

The frontend owns Python interpretation only. Graph semantics, kind decoding, config admission,
verification, realization, provisioner lifecycle, and catalogs are owned by their existing crates and
are unchanged by this feature. A `.tkdp` deployment differs from a `.tkd` deployment in exactly one
recorded fact: its Definition_Format and definition filename.

Compatibility authority: Python surface behaviour (dataclass semantics, `match` dispatch order, guard
evaluation, literal equality) defers to CPython as implemented by the pinned Monty revision; where
Monty and CPython diverge, the pinned Monty behaviour is the contract and the divergence is recorded.
The platform contract authority is the current `crates/tokeira-platform` source.

## Glossary

- **Tkdp_Frontend (`tokeira-tkdp`)** — The Definition_Frontend implemented by this feature: parser,
  restricted-subset preflight, match lowering, Monty execution, and structural-result conversion for
  `.tkdp` sources. It names no engine, provider, or platform types.
- **Definition_Format `tkdp`** — The validated format identifier recorded in deployment metadata and
  bound to Tkdp_Frontend, with source extension `tkdp` and default relative path `definition.tkdp`.
- **Authoring_Surface** — The Python constructs admitted in a `.tkdp` definition: module-level imports
  of the authoring facade, dataclass type definitions, the two Entrypoints, and the statement subset
  Monty executes plus the Restricted_Match_Subset.
- **Entrypoints** — `config() -> <config type>` and `deployment(cfg, cx) -> Deployment`: the two
  module-level functions the frontend invokes, mirroring the `.tkd` entrypoints.
- **Authoring_Facade** — The in-sandbox Python surface the frontend injects ahead of operator code:
  the `Deployment` builder, kind constructors, context object, and dataclass helpers. It accumulates
  plain data inside the sandbox; it performs no host calls during evaluation.
- **Restricted_Match_Subset** — The admitted `match` forms: wildcard, bare capture, literal and
  singleton patterns, keyword-only class patterns whose sub-patterns are bare captures or `_`, and
  guards on any of these. All other PEP 634 forms are rejected at preflight.
- **Definition_Lowering** — The source-to-source transform replacing each `match` statement with
  Monty-supported Python ahead of execution, leaving all other operator text byte-identical.
- **Transient_Program** — The internal program actually executed by Monty: injected facade, lowered
  operator source, and entrypoint driver. It is never persisted, surfaced, or treated as authority.
- **Source_Map** — The total mapping from Transient_Program positions to operator `.tkdp` positions
  or named internal regions, used to translate every Monty-reported position.
- **Structural_Result** — The plain located data the sandbox returns to the frontend, from which the
  frontend produces `FrontendOutput`: the config `LocatedValue` and the completed `VerifiedGraph`.
- **Monty_Pin** — The exact Monty version recorded in workspace dependencies, together with the
  capability probes that hold it to the behaviour this specification assumes.
- **Frontend_Parity** — Equality, for one logical definition authored in both formats, of the admitted
  typed config, the structural graph, and the realized desired manifests. Configuration identities
  deliberately differ (different format id and bytes).

## Target State

An operator creates a Compose deployment with `--format tkdp` and receives `definition.tkdp` seeded
from the Compose package's `.tkdp` seed. The bound provisioner for that deployment embeds Tkdp_Frontend
selected by the recorded format; `definition check`, `plan`, `apply`, revision retention, and revert
behave identically to a `.tkd` deployment. The definition below is a complete, valid authored shape:
every referenced name is defined in the file, imported from Monty's `dataclasses` module, or imported
from the Authoring_Facade's `tokeira` module (which the frontend satisfies at evaluation time). The
union annotation on `storage` is admitted because the pinned Monty stores dataclass field annotations
without evaluating them.

```python
from dataclasses import dataclass

from tokeira import Context, Deployment, DsqlCluster, LocalStateDir, ServerConfig, Service


@dataclass
class InMemory:
    pass


@dataclass
class Dsql:
    region: str
    endpoint: str
    arn: str


@dataclass
class Tokeirad:
    image: str
    replicas: int
    grpc_port: int
    metrics_port: int


@dataclass
class Compose:
    storage: InMemory | Dsql
    tokeirad: Tokeirad


def config() -> Compose:
    return Compose(
        storage=InMemory(),
        tokeirad=Tokeirad(
            image="tokeirad:latest",
            replicas=1,
            grpc_port=7233,
            metrics_port=9090,
        ),
    )


def deployment(cfg: Compose, cx: Context) -> Deployment:
    d = Deployment(["default"])
    local_state = d.module("local_state")
    local_state.resource("dir", LocalStateDir())

    match cfg.storage:
        case Dsql(region=region, endpoint=endpoint, arn=_):
            dsql = d.module("dsql", [local_state])
            cluster = dsql.resource(
                "cluster",
                DsqlCluster(
                    identity=cx.project_name + "-compose",
                    region=region,
                    endpoint=endpoint,
                ),
            )
            d.writeback("infrastructure.storage", "dsql")
            d.writeback(
                "infrastructure.dsql.endpoint",
                cluster.output("cluster_endpoint"),
            )
        case InMemory():
            pass

    runtime = d.module("runtime", [local_state])
    server_config = runtime.resource("server_config", ServerConfig())
    runtime.resource(
        "tokeirad",
        Service(
            image=cfg.tokeirad.image,
            replicas=cfg.tokeirad.replicas,
            publish=[cfg.tokeirad.grpc_port, cfg.tokeirad.metrics_port],
        ),
        [server_config],
    )

    return d
```

The Compose parity seed carries the full production topology (storage modes, observability services,
volumes, environment); the shape above is the complete grammar of the surface, not the full seed.

In scope: the `tokeira-tkdp` crate; its published definition-frontend descriptor and `frontend()`
export; preflight, lowering, source mapping, and Monty execution; conversion of the Structural_Result
into `FrontendOutput`; the Compose `.tkdp` seed and its parity evidence; admission of Monty into the
workspace dependency graph under the recorded pin; and the operator documentation for authoring
`.tkdp`.

`.tkd` and `.tkdp` are peer Definition_Formats: this feature adds the second and changes nothing
about the first.

Out of scope: any change to `crates/tokeira-platform`, `tokeira-tkd`, provisioner lifecycle, catalogs,
or `tkr` beyond publishing the new descriptor and seed — with exactly three sanctioned
accommodations this feature owns: the enum-position admission of struct-shaped
values in the `LocatedValue` deserializer (variant tagged by class name; zero fields as a unit
variant), the kind-name inventory exposed alongside membership, defaults, and decode, and the
platform-declared `default-format` seed selection (Requirement 9) that peer seeds make necessary. Also out
of scope: general Python support beyond the pinned Monty subset; PEP 634 forms outside the
Restricted_Match_Subset; ECS and EKS seeds (those platforms are not yet migrated onto the platform
contract); type-checking integration; and lowering `match` inside Monty itself (upstream
contribution is a separately decided effort).

Sanctioned deviations from CPython, stated rather than accidental:

- A `match` whose cases all fail raises an error identifying the definition position (CPython falls
  through silently). A configuration definition that matches nothing is a defect, and silent
  fall-through would let it produce an incomplete graph. Validated as the spike's strict-exhaustion
  mode; the CPython-faithful mode is not carried into the product.
- Class patterns match on exact variant identity (`type(subject) is Cls`), not `isinstance`
  subclassing. Config variants form a closed algebraic set; subclass admission is an inheritance
  surprise with no authoring use. (PEP 634 §class patterns uses `isinstance`.)

## Evidence From Current Code

- `crates/tokeira-platform/src/definition.rs` — `DefinitionFrontend::evaluate(source, &C, KindFunctions<K>)
  -> Result<FrontendOutput<K>, FrontendDiagnostic>`; `FrontendOutput { config: LocatedValue, graph:
  VerifiedGraph<K> }`; `evaluate_definition` admits typed config; `verify_definition` is pure input
  validation; `VerifiedDefinition::realize` is the one invocation-bound realization.
- `crates/tokeira-platform/src/kind.rs` — `KindFunctions<K> { contains, defaults, decode }`: the
  frontend constructs kinds by name plus `LocatedValue`; defaults back `<Kind>::EMPTY`-style
  authoring.
- `crates/tokeira-platform/src/author.rs` — `LocatedValue`/`ValueShape` (scalars, string, sequence,
  option, map, struct, externally tagged enum with unit/newtype variants) with `SourceRange`. Its
  deserializer admits enum positions from `ValueShape::Enum` (the enum name is ignored; the variant
  tag decides) and from a bare string as a unit variant — not from `Struct` — which is what makes
  the variant-spelling decision in Requirement 2.9 a real design choice rather than a detail.
- `crates/tokeira-platform/src/graph.rs` — `StructuralGraphBuilder<K>`: `add_namespace`,
  `add_module(name, deps)`, `add_resource(module, id, kind, deps)`, `add_writeback`, `finish()`
  validating names, targets, and acyclicity.
- `crates/tokeira-tkd/src/framework.rs` — the sibling frontend: UTF-8 admission, own source map,
  context serialization, evaluation entirely inside the frontend runtime, structural output. The
  pattern Tkdp_Frontend mirrors.
- `crates/tokeira-tkd/Cargo.toml` — the trusted descriptor convention
  (`[package.metadata.tokeira.definition-frontend]`, `frontend-contract = 1`) and conventional
  `frontend()` export consumed by generated composition roots.
- `platforms/compose/definition.tkd` — the live authored surface this feature mirrors: user config
  types, `config()`, `deployment(cfg, cx)`, modules with dependencies, kind construction
  (`LocalStateDir`, `DsqlCluster`, `DynamoDbTable`, `ServerConfig`, `Service` with `EMPTY`
  defaults, `ObservabilityConfiguration`), resource dependencies, `output()` references,
  writebacks, and `cx.project_name`.
- `spikes/monty-tkdp` (merged `a99e0274`) — validated ground truth for: the restricted subset and its
  preflight codes; splice lowering with CPython-faithful dispatch, guard, and binding semantics;
  exact source mapping including internal-region labelling; Monty capabilities at rev `69f8a613`
  (in-sandbox `@dataclass` from pydantic/monty#626, plain classes with methods, `type(x) is C`
  identity, `getattr`/`hasattr`, closures, f-strings) and gaps (`X | Y` union on classes fails as a
  runtime expression but is admitted unevaluated in dataclass annotation position — both verified by
  executing probes through the spike; `dataclasses.field()`/`default_factory`, decorator options,
  `__post_init__`, and `InitVar` are not assumed); the import behaviour (`from tokeira import …`
  reaches runtime and fails with `ModuleNotFoundError` — verified by probe — so the frontend, not
  Monty, satisfies the facade import); and the dependency constraints (crates.io `monty 0.0.19`
  predates dataclasses; ruff crates pinned to Monty's own `0.0.3` line; `get-size2` incompatibility
  above `0.10.1`).
- Monty upstream: `crates/monty/src/parse.rs` rejects `Stmt::Match` as not implemented at the spike's
  pinned revision — the reason lowering exists.

## Contract Policy

### Definition-frontend descriptor (published by `tokeira-tkdp`)

| Field | Value | Policy |
|---|---|---|
| `format` | `tkdp` | Canonical id recorded in metadata; resolved through the existing catalogs; never inferred from extension |
| `source-extension` | `tkdp` | Seed-materialization convention only |
| `default-relative-path` | `definition.tkdp` | Seed-materialization convention only; live path is recorded metadata |

The descriptor carries identity facts only — no contract or version field. Definition frontends are
engine components: `tokeira-tkdp` versions with the engine, and assembly compatibility is governed
entirely by the platform definition's engine-version indication. Prerequisite: the descriptor slice
replacing the `binding-contract`/`frontend-contract` counters with that indication lands before this
feature's descriptor is published; this table describes the post-slice convention.

### Entrypoint signatures

| Entrypoint | Signature policy | Error on violation |
|---|---|---|
| `config` | Module-level `def config()` with zero parameters, returning the platform config value | Preflight rejection with definition position |
| `deployment` | Module-level `def deployment(cfg, cx)` with exactly two positional parameters, returning the `Deployment` builder value | Preflight rejection with definition position |
| both | Exactly one definition of each; `config` required; `deployment` required | Preflight rejection naming the missing or duplicated entrypoint |

### Match pattern admission

| Pattern form | Example | Policy | Diagnostic |
|---|---|---|---|
| wildcard | `case _:` | admit | — |
| bare capture | `case x:` | admit; irrefutable form must be final case | irrefutable-not-last rejected |
| string/bytes/int/float literal, negated number | `case "dsql":`, `case -1:` | admit; compares by `==` | — |
| singleton | `case None:`, `case True:` | admit; compares by `is` | — |
| keyword-only class pattern | `case Dsql(region=r, arn=_):` | admit; bare class name; sub-patterns bare capture or `_`; exact variant identity | — |
| guard | `case P if expr:` | admit on any admitted pattern; evaluated after captures bind | — |
| positional class args | `case Dsql(r):` | reject | spanned, names keyword-only requirement |
| sequence / mapping / OR / `as` / star / value (dotted) / complex literal | `case [a]:`, `case A \| B:`, `case C.X:` | reject | spanned, names the rejected form |
| duplicate field or capture in one case | `case C(a=x, a=y):` | reject | spanned |
| dotted class name | `case aws.C():` | reject | spanned |
| reserved identifiers | any `__tokeira_internal_`-prefixed name anywhere | reject | spanned (lowering hygiene namespace) |
| tab indentation | any tab-indented line | reject | spanned (lowering is space-arithmetic) |

### Variant spelling (Requirement 2.10)

One dataclass per variant; zero-field dataclasses are the unit-variant spelling; the Python form is
flatter than `.tkd` (the variant class carries the payload fields directly) with identical decoded
results. The same classes serve construction in `config()` and dispatch in `match`.

```python
@dataclass
class Managed:
    pass


@dataclass
class State:
    sub: str
    at: str
```

| `.tkdp` spelling | `.tkd` spelling | Decoded result |
|---|---|---|
| `InMemory()` | `Storage::InMemory` | `Storage::InMemory` |
| `Dsql(region="eu-west-2", mode=Managed())` | `Storage::Dsql(DsqlStorage { region: …, mode: DsqlMode::Managed, endpoint: None, arn: None })` | identical |
| `Managed()` | `DsqlMode::Managed` | identical |
| `State(sub="mimir", at="/data")` | `Volume::State(StateVolume { sub: …, at: … })` | identical |

How the conversion distinguishes a variant instance from a plain struct value is a design decision;
the spelling above is the requirements-level contract.

## Requirements

### Requirement 1: Format identity, descriptor, and assembly

**User Story:** As a deployment operator, I want `tkdp` to be a recorded definition format selected
through the existing catalogs and bound-provisioner assembly, so that choosing Python authoring
changes nothing else about how a deployment is created, verified, or mutated.

#### Acceptance Criteria

1. THE `tokeira-tkdp` package SHALL publish the definition-frontend descriptor with the exact field
   values in Contract Policy and a conventional `pub fn frontend() -> TkdpFrontend` export.
2. THE TkdpFrontend SHALL implement `DefinitionFrontend` with `format()` returning the validated id
   `tkdp`.
3. WHEN the composition root is generated for a platform with `expected_format = "tkdp"`, THE
   produced Bound_Provisioner SHALL embed exactly one platform binding and the Tkdp_Frontend.
4. THE feature SHALL add no format enum, platform-name branch, or frontend match arm to `tkr`,
   `tokeira-build`, or `tokeira-provisioner-cli`.
5. WHEN a deployment records `Definition_Format = "tkdp"`, THE lifecycle SHALL resolve the frontend
   from the recorded identity and SHALL NOT infer it from the definition filename or extension.
6. WHEN `ConfigurationIdentity` is computed for a `.tkdp` source, THE identity SHALL cover the `tkdp`
   format identifier and the exact source bytes through the existing algorithm.
7. WHEN a retained revision of a different Definition_Format is selected for revert, THE existing
   same-format refusal SHALL apply unchanged to `tkdp` deployments.

### Requirement 2: The authoring surface

**User Story:** As a definition author, I want to write typed Python — dataclass configuration, a
`config()` value, and a `deployment(cfg, cx)` builder — so that a `.tkdp` definition expresses
exactly what the `.tkd` definition expresses, in Python.

#### Acceptance Criteria

1. THE Authoring_Surface SHALL admit module-level dataclass definitions (`@dataclass` with typed
   fields, default values, and unevaluated union annotations) for operator config types.
2. THE Authoring_Facade SHALL publish the `Deployment` builder, the `Context` type, and the
   platform's kind constructors as names importable from the `tokeira` module, in the form
   `from tokeira import <name>[, <name> …]` with optional `as` aliases.
3. THE frontend SHALL satisfy the `tokeira` import during evaluation so imported names are bound
   before operator code executes; the satisfaction mechanism SHALL NOT be operator-visible.
4. IF a definition imports a name the facade does not publish, or uses `import tokeira` or
   `from tokeira import *`, THEN THE preflight SHALL reject it at the import's position.
5. WHEN `config()` returns a value, THE frontend SHALL convert it to a `LocatedValue` whose shapes
   (struct, externally tagged enum with unit/newtype variants, scalars, sequences, options, maps)
   admit into the platform's typed config exactly as the `.tkd` frontend's conversion does.
6. WHEN `deployment(cfg, cx)` executes, THE facade SHALL support: `Deployment([namespaces])`;
   `d.module(name)` and `d.module(name, [module_handles])`; `module.resource(id, kind)` and
   `module.resource(id, kind, [resource_handles])`; `resource.output(name)`;
   `d.writeback(key, literal_or_output)`.
7. WHEN a kind constructor is invoked, THE facade SHALL accept keyword-only field construction,
   apply the platform's declared kind defaults for omitted fields, and reject unknown kind names and
   unknown fields with the definition position.
8. THE facade's kind constructors SHALL be synthesized from the supplied kind functions with no
   per-kind frontend code, so that a kind added to or evolved in a provider crate becomes
   authorable in `.tkdp` with no `tokeira-tkdp` change.
9. THE facade SHALL publish the complete kind set assembled into the bound provisioner: every kind
   the engine kind library admits SHALL be importable, with no curated subset — a definition
   edited within one engine version SHALL be able to adopt any kind that engine ships.
   (Prerequisite: the platform contract exposes the kind-name inventory alongside membership,
   defaults, and decode.)
10. WHEN a kind field or config field is enum-typed, THE conversion SHALL admit the operator's
    variant spelling (a dataclass instance per variant, unit variants as zero-field dataclasses)
    with the same decoded result as the `.tkd` enum spelling.
11. THE nested values passed to kind constructors (structs, sequences, options, maps, and variant
    values) SHALL be authorable as operator-defined dataclasses and literals admitted through the
    same conversion as config values.
12. THE context object SHALL expose exactly the serialized fields of the platform's typed context as
    read-only attributes, and SHALL expose no host path, credential, client, or environment access.
13. WHEN operator code mutates builder state after `deployment` returns, or reuses a handle across
    deployments, THE frontend SHALL reject the definition with the position of the misuse.
14. THE Entrypoints SHALL be validated per the Contract Policy table before any execution.

### Requirement 3: Restricted match admission

**User Story:** As a definition author, I want `match` over configuration variants with a precise,
documented boundary, so that admitted definitions behave predictably and rejected forms fail with
actionable positions instead of failing inside the interpreter.

#### Acceptance Criteria

1. THE preflight SHALL admit exactly the pattern forms marked *admit* in the Contract Policy match
   table, in any statement position where CPython admits a `match` statement.
2. THE preflight SHALL reject every pattern form marked *reject* with a diagnostic carrying the
   pattern's source range and a message naming the rejected form.
3. IF an irrefutable case (wildcard or unguarded bare capture) precedes another case, THEN THE
   preflight SHALL reject the definition at that case.
4. THE preflight SHALL reject any identifier using the reserved lowering prefix in any binding,
   reference, parameter, attribute, alias, or pattern position.
5. THE preflight SHALL reject tab-indented sources.
6. WHEN a definition contains multiple or nested `match` statements, THE preflight SHALL validate
   every one.
7. WHEN preflight rejects a definition, THE frontend SHALL report all findings from that pass, not
   only the first.

### Requirement 4: Match execution semantics

**User Story:** As a definition author, I want admitted `match` statements to behave exactly as
CPython dispatch would — except where this specification deliberately deviates — so that Python
intuition transfers to `.tkdp`.

#### Acceptance Criteria

1. WHEN a `match` statement executes, THE lowered form SHALL evaluate the subject expression exactly
   once.
2. THE lowered form SHALL take the first case whose pattern matches and whose guard (when present)
   is truthy, and SHALL NOT evaluate later patterns or guards after a case is taken.
3. WHEN a class pattern is probed, THE match SHALL succeed only when the subject's type is exactly
   the named variant class, per the sanctioned identity deviation.
4. WHEN a class pattern names a field absent on the matched variant, THE execution SHALL fail with a
   diagnostic naming the field and variant at the pattern's definition position.
5. WHEN a literal pattern is probed, THE comparison SHALL use equality; WHEN a singleton pattern is
   probed, THE comparison SHALL use identity.
6. WHEN a guard is present, THE captures of its case SHALL be bound before the guard evaluates, and a
   falsy guard SHALL fall through to the next case with the bindings persisting, as CPython binds
   them.
7. WHEN a guarded case's pattern does not match, THE guard SHALL NOT be evaluated.
8. IF no case takes, THEN THE execution SHALL fail with a diagnostic carrying the `match` statement's
   definition position and the subject's rendered value, per the sanctioned exhaustion deviation.
9. WHEN `break`, `continue`, or `return` executes inside a case body, THE statement SHALL bind to the
   enclosing loop or function exactly as it would in the un-lowered source.
10. THE lowering SHALL be deterministic: identical source bytes SHALL produce an identical
    Transient_Program.

### Requirement 5: Diagnostics and source mapping

**User Story:** As a definition author, I want every failure — preflight, lowering, Monty parse, or
runtime — reported against my `.tkdp` file, so that the transient machinery is invisible when things
go wrong.

#### Acceptance Criteria

1. THE Source_Map SHALL cover every byte of the Transient_Program, mapping operator-derived text
   linearly and generated scaffolding to the construct that motivated it.
2. WHEN Monty reports a position in operator-derived text, THE frontend SHALL translate it to the
   exact original position, correct for multi-byte characters.
3. WHEN Monty reports a position in injected facade or driver text, THE rendered diagnostic SHALL
   name the internal region and SHALL NOT print transient-program coordinates.
4. WHEN evaluation fails, THE frontend SHALL return a `FrontendDiagnostic` carrying the `tkdp`
   format, the shell-supplied source name, the mapped range when one exists, and an actionable
   message.
5. WHEN a runtime failure carries a traceback, THE rendered message SHALL present the mapped frames
   in operator terms.
6. THE Transient_Program SHALL NOT be written into the deployment directory, recorded in state, or
   presented as authority in any operator output.

### Requirement 6: Structural evaluation contract

**User Story:** As a platform maintainer, I want the `.tkdp` frontend to honour the same data
boundary as `.tkd` — evaluate, then hand over one completed structure — so that the platform
contract stays frontend-agnostic.

#### Acceptance Criteria

1. WHEN `evaluate` is invoked, THE frontend SHALL parse, preflight, lower, and execute entirely
   within the invocation and SHALL return `FrontendOutput` containing the config `LocatedValue` and
   the completed `VerifiedGraph`, retaining no runtime state afterwards.
2. THE frontend SHALL construct kinds exclusively through the supplied `KindFunctions` — membership,
   defaults, then decode — and SHALL attach the invoking definition position to decode failures.
3. THE frontend SHALL build the structural graph through `StructuralGraphBuilder` from the
   Structural_Result, preserving declaration order of namespaces, modules, resources, and
   writebacks.
4. WHILE a definition evaluates, THE frontend SHALL perform no filesystem, network, environment,
   provider, or state access, and the sandbox SHALL have no host capability beyond the injected
   facade data.
5. WHEN definition code exceeds the configured Monty resource limits (memory, stack, execution
   time), THE frontend SHALL fail evaluation with a diagnostic identifying the limit.
6. WHEN the typed context serializes, THE facade SHALL expose it to definition code without exposing
   `deployment_dir` or any host-path fact unless the platform's context explicitly serializes one.
7. WHEN `finish()` reports a structural violation (duplicate names, unknown targets, cycles), THE
   frontend SHALL surface it as a located diagnostic using the declaring construct's position.

### Requirement 7: Frontend parity

**User Story:** As a platform owner, I want proof that the same logical definition means the same
deployment in either format, so that format choice is authoring taste, not behaviour.

#### Acceptance Criteria

1. THE Compose package SHALL publish a `.tkdp` seed logically equivalent to its `.tkd` seed.
2. WHEN the Compose `.tkdp` seed and `.tkd` seed are evaluated with the same typed context, THE
   admitted typed config values SHALL be equal.
3. WHEN both seeds are evaluated with the same typed context, THE structural graphs SHALL be equal:
   namespaces, module order and dependencies, resource identities, kinds and decoded inputs,
   resource dependencies, and writeback entries.
4. WHEN both evaluated definitions are verified and realized with identical invocation facts, THE
   realized desired manifests SHALL be equal.
5. THE parity evidence SHALL cover both storage variants of the Compose seed (in-memory and DSQL).
6. THE configuration identities of the two formats SHALL differ, and THE parity tests SHALL assert
   that difference.

### Requirement 8: Monty admission and pinning

**User Story:** As the workspace owner, I want Monty admitted deliberately — exact pin, verified
capabilities, bounded blast radius — so that an experimental upstream cannot destabilize the
deployment toolchain.

#### Acceptance Criteria

1. THE workspace SHALL depend on Monty only from `tokeira-tkdp`, at an exact recorded Monty_Pin.
2. THE Monty_Pin SHALL be a crates.io release containing native in-sandbox dataclass support; IF no
   such release exists at implementation time, THEN a git-revision pin SHALL require an explicit
   `deny.toml` sources exception approved by the operator before merge.
3. THE `tokeira-tkdp` test suite SHALL include capability probes asserting the pinned Monty still
   provides: in-sandbox `@dataclass` construction with defaults and keyword arguments, unevaluated
   dataclass field annotations (the union spelling), exact class identity under `type()`,
   `getattr`/`hasattr`, and rejection of native `match` — so a pin bump that breaks an assumption
   fails loudly.
4. THE frontend SHALL NOT depend on Monty behaviours recorded as absent (`X | Y` class unions in
   expression position — annotation position is admitted; `dataclasses.field`/`default_factory`;
   decorator keyword options; `__post_init__`; `InitVar`), and the facade SHALL NOT emit them into
   the Transient_Program.
5. WHEN the Monty_Pin moves, THE lowering goldens, capability probes, semantics suite, and parity
   suite SHALL gate the bump.
6. THE dependency additions SHALL keep the workspace `--locked` discipline: lockfile movement occurs
   only in the slice that records the pin.

### Requirement 9: Deployment lifecycle on `.tkdp`

**User Story:** As a deployment operator, I want a `.tkdp` deployment's whole life — create, check,
plan, apply, revise, revert — to behave exactly like a `.tkd` deployment's, so that format selection
carries no operational cost.

#### Acceptance Criteria

1. WHEN `tkr deployment create` selects the Compose platform with format `tkdp`, THE created
   deployment SHALL contain `definition.tkdp` materialized from the Compose `.tkdp` seed, with
   metadata recording the format and relative path.
2. WHEN create-time storage or region choices are supplied, THE seed materialization SHALL encode
   them in the initial definition before validation, as the `.tkd` seed path does.
3. WHEN the deployment is created, THE creation-time validation SHALL evaluate the materialized
   `.tkdp` definition through the selected engine before publication, and creation SHALL remain
   all-or-nothing.
4. WHEN `definition check` runs against a `.tkdp` deployment, THE result SHALL reflect frontend
   evaluation plus the existing pure verification, with findings rendered through the existing
   output modes.
5. WHEN a configuration revision commits, THE retained revision SHALL preserve the `.tkdp` source
   and its format identity for explicit same-format revert.
6. WHEN `definition check --definition <path> --format tkdp` is invoked in authoring mode, THE check
   SHALL evaluate the standalone file without deployment state.
7. WHEN creation names no format, THE platform's declared `default-format` (platform package
   metadata) SHALL select the seed; a platform supplying seeds for more than one format with no
   declaration SHALL be refused with the peer formats and the `--format` remedy named.

### Requirement 10: Completeness, tests, and documentation

**User Story:** As a maintainer, I want the frontend held to the workspace bar with its behaviour
documented for operators, so that `.tkdp` ships as a product surface, not a demo.

#### Acceptance Criteria

1. THE implementation SHALL pass the workspace finishing bar (fmt, lints with zero warnings, check,
   full test suite, docs with warnings denied) with the default suite requiring no Docker and no
   live credentials.
2. THE preflight, lowering, semantics, mapping, parity, and capability suites SHALL run under
   `cargo test --workspace`.
3. Every correctness property accepted in the design SHALL have a required property-based or
   differential test tagged to it.
4. THE operator documentation SHALL cover: the authoring surface, the Restricted_Match_Subset with
   its rejection table, the two sanctioned CPython deviations, entrypoint signatures, and the seed
   workflow, in `docs/provisioning/deployment-definitions.md` or a sibling document it links.
5. THE spike (`spikes/monty-tkdp`) SHALL be removed in the slice that lands the production frontend,
   with its README's findings preserved in the frontend crate's documentation.
6. WHEN this specification's tasks complete, THE `tasks.md` ledger SHALL carry DONE records
   reflecting the landed slices.
