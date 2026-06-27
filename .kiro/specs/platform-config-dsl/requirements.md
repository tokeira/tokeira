# Requirements Document

## Introduction

A tokeira deployment's infrastructure and services are defined today in **compiled Rust**: a
fixed-arity config struct per platform (`EcsConfig`, `ComposeConfig`, `LocalConfig`), the
`modules.rs`/`services.rs` that turn that struct into resources, and the validation that guards it.
The operator-facing `deployment.toml` supplies only *values within that compiled shape*. Consequently
any **structural** change to a deployment — a new service, a re-wiring, a changed validation rule —
requires editing platform code and rebuilding the provisioner binary (`tkp`). That coupling is the
problem the `platform-provisioner-binary` spec was forced to work around (retained buildable source,
reproducible builds, an operator toolchain).

This spec removes the coupling at its root by introducing a **platform configuration DSL**: a
strongly-typed, total (terminating, side-effect-free) language in which a deployment's platform —
its modules, resources, services, their typed parameters, and their dependency wiring — is expressed
as a **program**. A **compiler embedded in `tkp`** type-checks that program and **lowers** it, at
plan/apply time, into the exact in-memory composition the IaC and runtime engines already consume
(`InfraComposition` and the runtime service set). The compiled binary carries a **fixed library of
typed resource kinds** (the executable `create`/`update`/`delete`/`describe` logic stays Rust); the
DSL describes only their **composition**, never their behaviour.

The effect on the provisioner is decisive: because both *structural* and *value* changes are now
edits to a **data artifact** (the DSL program) interpreted by a generic binary, they are an ordinary
`apply` — no rebuild, no per-deployment source snapshot, no reproducible-build binding problem. The
engine identity that the provisioner binds against collapses to **the resource-kind library + the
language/compiler version** compiled into `tkp`; the DSL program is the deployment-married
configuration the provisioner records and retains.

### Scope boundary

- **In scope:** the DSL's surface and semantics; the typed resource-kind library contract; static
  type checking; totality/determinism; validation parity with today's platform definitions;
  operator-grade diagnostics; the lowering contract to `InfraComposition` and the runtime service
  set; in-program parametrisation; language/library versioning; ECS + Compose + Local parity; and the
  artifact the provisioner records/retains.
- **Out of scope (deferred / owned elsewhere):** the choice of language *substrate* (a bespoke
  lexer/parser stack vs. embedding an existing typed-config language) — a design decision, not a
  requirement; the provisioner's binding/upgrade/rollback machinery (owned by
  `platform-provisioner-binary`, which *consumes* this spec); the running server's runtime config
  (`tokeirad.toml` / `TokeiraConfig`), which is the deployed server's own configuration, not the
  platform's infra+services definition; adding genuinely new resource *kinds* (a code change to the
  kind library, hence an engine-identity change).

### Authority for "correct"

This DSL matches no external system. Its ground-truth authority is **the current platform definitions
in code** — the set of resources/services, their parameters, their wiring, and their validations as
implemented today — which the DSL must be able to express at parity, and **the engine's consumption
contract** (`InfraComposition`, the `Resource` trait, the runtime service set) into which it must
lower without loss.

## Glossary

- **Platform DSL (the DSL)** — the strongly-typed, total language in which a deployment's platform
  (modules, resources, services, wiring) is written.
- **Program** — the compilation unit: the composed source of one **deployment definition** (one or
  more `.platform` files) that the compiler processes.
- **Deployment definition** — the rooted set of `.platform` files describing one deployment; the
  deployment-married configuration artifact the provisioner retains and digests.
- **Deployment root** — the boundary directory containing a deployment definition; imports never
  resolve outside it.
- **Import (`use`)** — a relative, downward-only include of another `.platform` file within the
  deployment root.
- **RuntimeContext** — the closed, typed record `tkp` injects at execution: an **implicit** part
  (kind-library-delivered, e.g. `deployment_dir`, `home`) plus an operator-**declared** part bound to
  canonical providers and resolved by `tkp`. Secret-bearing fields are typed `Secret<…>`. The
  composition reads only typed `ctx.<field>` values and never names a provider; there is no OS-env,
  network, clock, or arbitrary-filesystem access from the language.
- **Implicit context** — the `RuntimeContext` fields the `tkp` kind library always delivers for a
  platform (derived from the platforms: `deployment_dir`, and `home` where needed).
- **Context declaration** — the operator `context { }` block in the deployment definition binding named
  context fields to providers; the only place a provider is named.
- **Provider** — a `tkp`-resolved source of a declared context value, from a catalog **fixed by the
  `(language, kind-library)` version**. The canonical catalog derived from the current platforms is
  `{ env }`; new providers arrive only by engine upgrade.
- **Secret taint** — the typing rule that a `Secret<…>` value never appears in a diagnostic and flows
  only into resource parameters typed to accept secrets.
- **Resource kind** — a compiled Rust `Resource` implementation (executable provider lifecycle:
  `create`/`update`/`delete`/`describe`/`diff`). The DSL references kinds; it does not define them.
- **Kind library** — the fixed set of resource kinds (and service kinds) compiled into a given `tkp`,
  spanning the supported platforms (ECS, Compose, Local).
- **Compiler** — the embedded pipeline (lex → parse → resolve → type-check → validate → lower) that
  runs in-process inside `tkp` at plan/apply time.
- **Lowering** — the deterministic transformation of a type-checked program into the engine's
  in-memory composition: an `InfraComposition` (desired/known/active modules → `Resource` objects)
  plus the runtime service set.
- **Composition** — the engine input produced by lowering; the `InfraComposition` plus runtime
  services.
- **Diagnostic** — a compiler-emitted error or warning carrying a source span, a message, and where
  possible a remediation hint.
- **Totality** — the property that evaluation always terminates and performs no I/O or other side
  effects; a program is a pure function from inputs to a composition.
- **Determinism** — the property that the same program (with the same inputs) always lowers to the
  same composition, byte-for-byte in the resource set, ids, and ordering inputs.
- **Inputs** — operator-tunable *configuration values* a program reads (scaling, image refs, storage
  choice) so that *value* refinement need not edit structural code; distinct from the `RuntimeContext`
  (operator-authored config vs. host-injected execution context).
- **Language/library version** — the version pair `(language, kind-library)` a `tkp` provides; it is
  **derived from the compiling `tkp`**, never declared in a program; the provisioner's engine identity
  is derived from it.
- **Engine identity** — (from `platform-provisioner-binary`) the binding key over the
  engine/resource-implementation surface; under this spec it is the kind library + compiler/language
  compiled into `tkp`, never the DSL program.

## Target State

- A deployment's platform is authored as a **DSL program**, not a compiled Rust struct + TOML values.
- `tkp` is **generic**: engine + fixed kind library + embedded compiler. It is no longer
  deployment-specific by source.
- The compiler **type-checks and validates** a program fully before any lowering, and **lowers**
  a valid program to the engine composition with no loss of resource ids, dependencies, or module
  ownership.
- Every validation enforced today in `EcsConfig::validate` (and the compose conditionals) is enforced
  by the compiler as a typed/contract check, surfaced as a diagnostic, not deferred to post-lowering
  Rust.
- **Structural and value changes are both program edits** → ordinary `apply` on the same `tkp`.
- ECS and Compose platforms are expressible **at parity** with their current definitions; Local is
  expressible.
- The DSL **stays a configuration language**: total, side-effect-free, not Turing-complete in the
  sense of unbounded computation, and incapable of defining resource behaviour.

## Evidence From Current Code

- **Compiled platform shape + validation (the thing the DSL replaces):**
  `platforms/ecs/src/config.rs` — fixed-arity `EcsConfig`/`ServiceConfigs`/`CapacityProviderConfigs`;
  `EcsConfig::validate` with canonical-port checks (`expect_port`, `expect_metrics`), cpu/memory
  pairing (`validate_cpu_memory`), capacity-range checks (`validate_capacity`,
  `validate_runtime_capacity`), task-resource sufficiency (`validate_resource_sufficiency`), and
  conditional-required DSQL fields (`require_preexisting`). `#[serde(deny_unknown_fields)]` throughout.
- **Compiled service composition + conditionals:** `platforms/compose/src/compose.rs` —
  `compose_services()` builds a fixed service list and conditionally injects AWS env/volumes when
  `storage == Dsql`; `module_for_service()` assigns module ownership.
- **The engine consumption contract (the lowering target):**
  `crates/tokeira-iac/src/types.rs` — `InfraComposition { desired_modules, known_modules,
  active_modules }`, `Change`, `ChangeKind`; `crates/tokeira-iac/src/lib.rs` — the `Resource` trait,
  `ResourceId`, `ResourceType`, `Module`; `crates/tokeira-iac/src/engine.rs` — plan/apply over a
  composition, topological ordering, and the composition-validation invariants
  (unique ids, `desired ⊆ known`, deps present, DAG).
- **Current authoring/loading path (replaced):** `apps/tkr/src/prototypical.rs` (TOML template
  generation) and `apps/tkr/src/deployment_dir.rs` (`deployment.toml` parsed via
  `tokeira_config::load_config`).
- **Consumer of this spec:** `.kiro/specs/platform-provisioner-binary/` — engine identity, binding,
  retention, and rollback, which bind against the kind-library/language version and record/retain the
  DSL program.

## Validation Parity Policy

Every validation the compiled platform performs today MUST be enforced by the compiler before
lowering. Each is accounted for below with the target enforcement mechanism and the diagnostic class.

| Current check (source) | DSL enforcement | On violation |
|------------------------|-----------------|--------------|
| Unknown config field (`deny_unknown_fields`) | Static typing: unknown field/parameter is not in the kind's schema | Type-error diagnostic at the field span |
| Canonical service ports (`expect_port`/`expect_metrics`) | Typed constraint on the service kind's port parameters | Validation diagnostic naming service, field, expected/actual |
| CPU/memory pairing (`validate_cpu_memory`) | Constraint on the kind's `(cpu, memory)` parameters | Validation diagnostic with the allowed pairing hint |
| Capacity range `min ≤ desired ≤ max` (`validate_capacity`) | Constraint on capacity-provider parameters | Validation diagnostic naming the provider |
| Task-resource sufficiency (`validate_resource_sufficiency`) | Cross-field constraint over service + observability overhead | Validation diagnostic with computed overhead |
| Conditional-required DSQL fields (`require_preexisting`) | Typed sum: `preexisting` variant requires its fields, `managed` forbids them | Type/validation diagnostic at the variant |
| Non-empty VPC CIDR / AZs (`EmptyVpcCidr`/`EmptySubnets`) | Non-empty typed constraint | Validation diagnostic at the field |
| Compose DSQL conditional env/volumes (`compose_services`) | Conditional expression on a typed `storage` enum | Lowered deterministically; mis-typed condition is a type error |
| Module ownership (`module_for_service`) | Each resource/service declares its module; uniqueness enforced | Validation diagnostic on duplicate/missing module |

## Requirements

### Requirement 1: Platform composition defined by a DSL program

**User Story:** As an operator, I want my deployment's platform (modules, resources, services, wiring)
expressed as a DSL program rather than compiled Rust, so that I can evolve its structure without
rebuilding a binary.

#### Acceptance Criteria

1. THE platform DSL SHALL be sufficient to express a deployment's full infra+services definition —
   modules, resources, services, their typed parameters, and their inter-resource dependencies — such
   that no part of a supported platform's structure requires compiled Rust beyond the resource-kind
   library.
2. WHEN a deployment is created, THEN its platform SHALL be represented by exactly one DSL program as
   the authoritative source of its infra+services definition.
3. THE DSL program SHALL be the artifact the provisioner records and retains for the deployment
   (`platform-provisioner-binary`), not a compiled per-deployment struct.
4. THE DSL SHALL NOT carry the running server's runtime configuration (`TokeiraConfig`); that remains
   a separate concern outside this spec.

### Requirement 2: Resource kinds are a fixed, typed, compiled library

**User Story:** As a platform engineer, I want the DSL to instantiate a fixed library of compiled,
typed resource kinds rather than describe resource behaviour, so that provider correctness stays in
reviewed Rust while composition stays editable.

#### Acceptance Criteria

1. THE DSL SHALL reference resource and service **kinds** by name from a fixed library compiled into
   `tkp`; it SHALL NOT define or alter a kind's provider lifecycle behaviour.
2. WHEN a program references a kind not present in the running `tkp`'s kind library, THEN the compiler
   SHALL emit a diagnostic and SHALL NOT lower the program.
3. WHEN a program supplies a parameter to a kind that is not in that kind's declared schema, or omits
   a required one, THEN the compiler SHALL emit a type diagnostic and SHALL NOT lower the program.
4. THE kind library SHALL declare, for each kind, a typed parameter schema and the kind's
   `ResourceType`/`ResourceId` derivation, so that the compiler can type-check references and the
   lowering can construct the engine's `Resource` objects.

### Requirement 3: Static type checking precedes lowering

**User Story:** As an operator, I want a program fully type-checked before anything is provisioned, so
that a malformed platform definition is caught as a diagnostic rather than mis-provisioned.

#### Acceptance Criteria

1. WHEN the compiler processes a program, THEN it SHALL complete name resolution and static type
   checking before producing any composition, and a type error SHALL prevent lowering entirely.
2. THE type system SHALL recover the guarantees the current `#[serde(deny_unknown_fields)]` typed
   structs provide: unknown fields, wrong-typed values, and missing required values are static errors.
3. IF a program fails type checking, THEN no partial composition SHALL be passed to the engine.

### Requirement 4: Totality and deterministic evaluation

**User Story:** As an operator, I want compilation to be a pure, terminating, deterministic step, so
that the plan derived from a program is reproducible and safe to run inside the provisioner.

#### Acceptance Criteria

1. THE DSL SHALL be total: evaluation SHALL always terminate and SHALL perform no I/O, network, clock,
   filesystem, or environment access during compilation.
2. WHEN the same program is compiled with the same inputs and the same `RuntimeContext`, THEN it SHALL
   lower to the same composition — the same resource set, the same ids, dependencies, and module
   ownership — every time.
3. THE compiler SHALL run in-process within `tkp` at plan/apply time and SHALL NOT require a separate
   external build step or toolchain.

### Requirement 5: Validation parity with the current platform definitions

**User Story:** As a platform engineer, I want every validation the compiled platforms enforce today
to be enforced by the compiler, so that moving to the DSL loses no safety.

#### Acceptance Criteria

1. THE compiler SHALL enforce each check in the Validation Parity Policy table before lowering, and a
   violation SHALL be a diagnostic that prevents lowering.
2. WHERE a current validation is conditional (e.g. DSQL `preexisting` requires endpoint/role fields),
   THE DSL SHALL express the condition through the type system (typed sums/optionals) so the
   requirement is checked statically rather than after lowering.
3. THE engine SHALL NOT be relied upon to re-perform a platform validation that the compiler can
   perform; validations move forward to compile time, not backward to runtime.

### Requirement 6: Operator-grade diagnostics

**User Story:** As an operator, I want compiler errors that point at the exact place in my program and
explain the problem, so that I can fix a platform definition without reading source.

#### Acceptance Criteria

1. WHEN the compiler rejects a program at any phase (lex, parse, resolve, type, validation, or
   pre-lowering composition check), THEN each diagnostic SHALL carry a source span, a human-readable
   message, and where applicable a remediation hint.
2. WHERE multiple independent errors exist in a program, THE compiler SHALL report as many as it can
   in one pass (error recovery) rather than failing on the first.
3. THE diagnostics SHALL be emitted both human-readably and, under a machine-readable mode, as
   structured data for `--json` consumers.

### Requirement 7: Lowering contract to the engine composition

**User Story:** As the IaC engine, I want a type-checked program lowered to exactly the composition I
consume, with the composition invariants already guaranteed, so that I never receive a malformed graph.

#### Acceptance Criteria

1. WHEN a type-checked, validated program is lowered, THEN the compiler SHALL produce an
   `InfraComposition` (desired/known/active modules resolving to `Resource` objects) and the runtime
   service set, preserving every declared `ResourceId`, dependency edge, and module ownership.
2. BEFORE returning a composition, THE compiler SHALL guarantee the engine's composition invariants —
   unique module names, unique resource ids, `desired ⊆ known`, every dependency present unless
   declared external, and no dependency cycles — and SHALL emit a diagnostic (not a composition) if
   any would be violated.
3. THE lowering SHALL be the only path from a program to engine input; a program SHALL NOT be able to
   inject a `Resource` object the kind library did not construct.

### Requirement 8: In-program parametrisation and value refinement

**User Story:** As an operator, I want to vary values (scaling, image refs, region, AZs) through
program inputs or bindings without editing structural definitions, so that everyday refinement stays
low-friction and both value and structural edits are the same kind of operation.

#### Acceptance Criteria

1. THE DSL SHALL support bindings/inputs and typed expressions (including typed enums, optionals, and
   conditionals) so that values can be parameterised and reused without duplicating structure.
2. WHEN an operator changes only input values, THEN recompilation SHALL re-lower to a composition that
   differs only in those values, and the change SHALL be an ordinary `apply` on the same `tkp` (no new
   language/kind-library version).
3. WHERE an input is required and unbound, or bound to a value of the wrong type, THE compiler SHALL
   emit a diagnostic and SHALL NOT lower.

### Requirement 9: Language and kind-library versioning

**User Story:** As an operator, I want the language and kind-library version to be derived from the
`tkp` that compiles a program — never declared in the program — so that version stamping for the
provisioner is reliable and a program is never silently mis-compiled by a different binary.

#### Acceptance Criteria

1. THE running `tkp` SHALL expose the `(language, kind-library)` version it provides, and the
   provisioner's engine identity SHALL be derived from it (per `platform-provisioner-binary`), never
   from a deployment's DSL program.
2. THE program SHALL NOT declare or pin a language or kind-library version; the
   `(language, kind-library)` version SHALL be derived solely from the compiling `tkp`. A reference to
   a kind, field, or construct the running library does not provide SHALL be rejected per Requirement 2
   (not via a program-declared version).
3. WHEN the kind library or language changes (a new/changed kind, a changed schema, a language
   change), THEN it SHALL constitute an engine-identity change handled by the provisioner's
   `upgrade` path, distinct from an ordinary program edit.

### Requirement 10: Multi-platform parity

**User Story:** As a platform engineer, I want the DSL to express the existing ECS, Compose, and Local
platforms, so that the DSL can replace the compiled platform structs without regression.

#### Acceptance Criteria

1. THE kind library SHALL provide the resource and service kinds required to express the ECS and
   Compose platforms at parity with their current definitions (`platforms/ecs`, `platforms/compose`),
   and to express the Local platform.
2. WHEN an ECS or Compose deployment's current compiled definition is re-expressed as a DSL program,
   THEN lowering SHALL produce a composition equivalent to today's — the same resources, services,
   dependencies, and module ownership — for the same inputs.
3. WHERE a platform applies a conditional today (e.g. compose DSQL env/volumes, ECS optional
   endpoints), THE DSL SHALL express it as a typed conditional that lowers to the same result.

### Requirement 11: The program is the retained, deployment-married artifact

**User Story:** As an operator, I want the deployment to carry its DSL program (and a content digest)
so that the provisioner records, retains, and can roll back to it, making the deployment
self-contained without a buildable source tree.

#### Acceptance Criteria

1. WHEN the provisioner records or retains deployment configuration, THEN it SHALL record/retain the
   **deployment definition** (the set of `.platform` files) and a content digest computed over the
   sorted `(relative_path, sha256)` set, sufficient to recompile and re-lower deterministically.
2. WHEN the provisioner captures a rollback checkpoint (per `platform-provisioner-binary`), THEN the
   prior **deployment definition** (the file set) SHALL be part of that checkpoint, so a prior
   configuration is restorable.
3. THE retained deployment definition SHALL be compilable by the `tkp` whose `(language,
   kind-library)` version is recorded alongside it; a checkpoint definition SHALL be paired with the
   version that can compile it.

### Requirement 12: Closed runtime context and security posture

**User Story:** As an operator, I want the DSL confined to a closed, typed runtime context and the
compiled kind library with no ambient authority, so that a deployment definition can never read host
secrets, reach the OS environment / network / arbitrary filesystem, or execute host code.

#### Acceptance Criteria

1. THE program SHALL access no OS environment, network, clock, or arbitrary filesystem; the only
   external data a program may read at execution is the closed, typed `RuntimeContext` injected by
   `tkp`. THERE SHALL be no environment-variable or key-based lookup construct in the language.
2. WHERE a workload requires secret-bearing material (e.g. cloud credentials), THE program SHALL only
   *declare the need* through a typed field (such as `aws_auth`), AND `tkp` SHALL perform the injection
   at materialization, so secret values never enter the program, its evaluation, or its output.
3. THE compiler's diagnostics SHALL reference names and source spans only, never resolved
   `RuntimeContext` values, so no secret is echoed to logs or telemetry.
4. THE compiler SHALL enforce resource bounds (maximum file count, per-file and total bytes, import
   depth, and AST nesting/size) and SHALL refuse with a diagnostic rather than consume unbounded
   resources on adversarial input.
5. THE program SHALL exercise no authority beyond composition over the fixed kind library, the
   deployment-local files (Requirement 13), and the closed `RuntimeContext`; host-code execution from a
   program SHALL be impossible.

### Requirement 13: Modular deployment definition and import containment

**User Story:** As an operator, I want to split a deployment definition across multiple `.platform`
files contained within the deployment, so that large platforms stay maintainable while the definition
remains a bounded, auditable, tamper-evident unit that nothing outside the deployment can influence.

#### Acceptance Criteria

1. A deployment definition MAY comprise multiple `.platform` files within the deployment root, composed
   into one program via relative `use` imports.
2. WHEN resolving a `use` import, THEN the target SHALL be a relative path with no `..` component and no
   absolute prefix, AND after symlink canonicalization its real path SHALL be strictly within the
   deployment root; any violation SHALL be a diagnostic and SHALL prevent compilation (fail-closed).
3. THE deployment definition SHALL have a maximum folder depth of one below the deployment root; a file
   deeper than one directory level SHALL be a diagnostic.
4. THE import graph SHALL be acyclic (a cycle is a diagnostic), AND the composed program SHALL be
   deterministic regardless of file read order (files composed in a stable path-sorted order),
   preserving Requirement 4.2.
5. WHEN declarations are composed across files, THEN name resolution SHALL span the whole composed
   program, AND a duplicate top-level declaration across files SHALL be a diagnostic (no silent
   shadowing).
6. THE deployment definition's content digest SHALL be computed over the sorted `(relative_path,
   sha256)` set of its files and SHALL be the artifact the provisioner records, retains, and verifies
   (Requirement 11).

### Requirement 14: Runtime context definition (implicit + declared providers)

**User Story:** As an operator, I want the runtime context to combine values delivered implicitly by
`tkp` with values I declare from a canonical, version-fixed set of providers, so that a deployment can
wire the host data it needs without the DSL gaining ambient authority.

#### Acceptance Criteria

1. THE `RuntimeContext` SHALL comprise (a) an **implicit** set delivered by the `tkp` kind library — at
   minimum `deployment_dir`, and `home` where the platform requires it — and (b) an
   operator-**declared** set bound, in a `context` declaration within the deployment definition, to
   providers from the canonical catalog.
2. THE canonical provider catalog SHALL be **fixed by the `tkp` `(language, kind-library)` version** and
   derived from the existing platforms; it SHALL include the **`env`** provider (named host environment
   variables). New providers SHALL be added only by an engine upgrade (Requirement 9.3), never declared
   by an operator program.
3. WHERE a declared context value is secret-bearing (e.g. an `env` variable resolved via `env.secret`),
   THE value SHALL be typed `Secret<…>` and SHALL be subject to the secret-taint rules (Requirement
   12.3): it SHALL NOT appear in any diagnostic and SHALL flow only into resource parameters typed to
   accept secrets.
4. THE DSL composition SHALL read context only as typed `ctx.<field>` values; it SHALL NOT name or read
   a provider directly — provider resolution is performed solely by `tkp` at execution and injected as
   the closed `RuntimeContext`.
5. WHERE a workload requires the standard cloud credential chain, THE program MAY declare it via the
   `aws_auth` convenience, which `tkp` SHALL satisfy by resolving the standard credential chain (the
   `env` provider plus the platform's credential-file location from the implicit `home`) and injecting
   it at materialization — with no secret value entering the program.
6. WHEN a declared `env` variable is absent at execution, THEN `tkp` SHALL surface it per the field's
   optionality (an optional field resolves to none; a required one is a host-runtime error), and SHALL
   NOT silently substitute a value.
7. THE following SHALL NOT be modelled as context providers, being either composition or host-runtime
   concerns: AWS Secrets Manager secrets (a resource kind consumed by a container `value_from`
   reference), the provisioner's own credential chain and STS caller-identity check (host-runtime auth),
   and `region` (an operator input).

### Requirement 15: Resource output references

**User Story:** As a platform engineer, I want a resource parameter to reference another resource's
provisioned output (a cluster ARN, a bucket name, a secret ARN), so that dependent resources are wired
correctly without hard-coding values that only exist after provisioning.

#### Acceptance Criteria

1. THE DSL SHALL support an **output reference** expression `<resource>.<output>` usable in a resource
   parameter, a container secret reference, or a writeback target.
2. WHEN a program uses an output reference, THEN lowering SHALL create a dependency edge from the
   referencing resource to the referenced resource AND record a deferred binding; the value SHALL be
   resolved by the engine during apply, in dependency order, from the referenced resource's provisioned
   state — never at compile time or at execute-to-composition time.
3. WHERE an output reference names a resource absent from the composition, or an output the referenced
   kind does not declare, THE compiler SHALL emit a diagnostic and SHALL NOT lower.
4. THE referenced output's value SHALL NOT be required at compile time; a composition MAY contain
   unresolved output references whose concrete values the provider supplies during apply. Determinism
   (Requirement 4.2) applies to the composition structure and the set of references, not to
   provider-assigned output values.
5. WHERE an output reference is a secret-bearing container reference (e.g. a Secrets Manager secret
   consumed via `value_from`), THE secret value SHALL be resolved by the runtime (e.g. ECS) at
   materialization and SHALL NOT enter the program or the `RuntimeContext` (reinforcing Requirement
   14.7).
