# AGENTS.md — Tokeira

## Mission

Build a Temporal-compatible durable execution engine in Rust, specialized for Aurora DSQL. Preserve the public Temporal contract that SDKs, operators, and tooling depend on. Collapse internal correctness around a single authoritative per-run transition log.

This is a product-from-scratch. The architecture is informed by Temporal but the implementation is original. Do not port Temporal code.

### Compatibility Target

- **Temporal server compatibility: v1.31.0.** This is the release whose public API *behaviour* Tokeira claims to match. It is the authority for every API-behaviour question (see §8). Pinned as `TEMPORAL_SERVER_COMPAT` in `crates/tokeira-build-info/src/pinned.rs`.
- **Temporal API: v1.62.11.** This is the vendored proto surface Tokeira builds against (`proto/upstream/`, mirrored by `proto/UPSTREAM_VERSION`). Pinned as `TEMPORAL_PROTO_VERSION`.
- These pins are independent and tracked ahead on purpose: the vendored API `v1.62.11` is newer than the API version Temporal server `1.31.0` ships (`v1.62.8`). RPCs present only in `v1.62.11` (e.g. Nexus operation execution) are **not** part of the `1.31.0` behavioural claim and are tracked separately in the api-conformance tracker. Do not bump the server compatibility claim just because the vendored proto version moved.

---

## Non-Negotiable Rules

### 1. Rust Standards

- Edition 2024, stable toolchain pinned to 1.96.
- `cargo clippy --workspace --all-targets` must pass. No suppressed warnings without a comment explaining why.
- `cargo +nightly fmt` for formatting (some settings require nightly). Just run it — don't check first.
- Error handling: `thiserror` in library crates, `anyhow` in binary crates. No `.unwrap()` outside tests.
- All public types derive `Debug`. Serializable types derive `Serialize, Deserialize`.
- No `unsafe`. No runtime reflection (no trait-object downcasting to concrete types driven by runtime values).
- Typed extension bags (`HashMap<TypeId, Box<dyn Any + Send + Sync>>` keyed by `TypeId::of::<T>()`, accessed via a type-parameterised `extension::<T>()` helper) are the sanctioned exception. They exist in `ProvisionContext`, `ModuleContext`, `ServiceContext`, and `ImageContext` because the library crates holding these types (`tokeira-iac`, `tokeira-deploy-engine`) cannot depend on the platform crates that register handles into them. This keeps the platform boundary clean at the cost of one well-contained `Box<dyn Any>` per context type. New contexts SHALL NOT introduce additional `Box<dyn Any>` usage beyond this bounded set.
- Prefer `&str` over `String` in function signatures where ownership isn't needed.
- Use `tracing` for structured logging. No `println!` or `eprintln!` in library code.
- Comments and documentation follow the Code Documentation standard (§9). The bar is high: every module, public item, and non-obvious decision carries rationale. Comments explain WHY, never restate WHAT.
- Do NOT put `use` statements in function scope. Always at the top of the file or module.
- No explicit sleeps in test code. Use synchronization (channels, `tokio::sync::Notify`, condition variables) instead of `tokio::time::sleep` or `std::thread::sleep`.
- Rust compilation takes time. Do not interrupt builds or tests unless they are taking more than 5 minutes.

### 2. The Kernel Stays Pure

`tokeira-kernel` is a deterministic state machine. No I/O, no async, no storage, no metrics, no network. If a change would add any of these to the kernel, it belongs in `tokeira-runtime`, `tokeira-storage`, or `tokeira-edge` instead.

### 3. History is Authority

Every state-changing request becomes a per-run transition. Dispatch and projection are derived effects. If a design puts correctness weight on a queue write or a visibility update, the design is wrong.

### 4. Review Before Action

The CLI follows a `plan → confirm → apply` model. Silent mutations are a bug.

- `tkr infra plan` shows what will change before `tkr infra apply` does it.
- `tkr deploy plan` shows service manifest changes.
- Destructive operations (`infra destroy`, `deployment destroy`, `scale down`) require `--yes` or interactive confirmation.

### 5. Revert Safety (worktree integrity)

The working tree frequently contains unstaged edits that represent hours of in-flight work. Reverting files from the git index or HEAD destroys that work irreversibly.

- NEVER run `git checkout`, `git checkout-index`, `git restore`, `git reset --hard`, `git clean -f`, or any equivalent command to revert files without explicit user approval of the exact command.
- When asked to "undo your changes", produce a reverse patch (`git diff | patch -R`) that reverses ONLY the hunks you introduced. Do not restore files from the index.
- Before any revert operation, run `git status` and `git diff` and explicitly confirm with the user whether the unstaged changes belong to them.
- If you did not snapshot the pre-edit content yourself, do not attempt an automatic revert. Stop and ask.
- Treat all unstaged changes as user work unless proven otherwise.

### 6. Spec editing safety

The following requires the explicit instruction from the user. They need to explicitly indicate they want a spec snapshot.

Before editing any file under `.kiro/specs/**`, snapshot the pre-edit state:

```bash
mkdir -p /tmp/tokeira-spec-snapshots
cp .kiro/specs/**/*.md /tmp/tokeira-spec-snapshots/$(date +%Y%m%d-%H%M%S)/
```

or equivalently produce a patch:

```bash
git diff -- .kiro/specs > /tmp/spec-before-$(date +%Y%m%d-%H%M%S).patch
```

After editing, report the snapshot or patch path to the user. If asked to undo, apply a reverse patch for only the assistant-authored hunks.

If the working tree has uncommitted spec edits and the user gives a broad instruction like "undo your changes", clarify first: "Undo only the hunks I just introduced; do not restore files from git." Do not assume the broad instruction means restore-from-index.

### 7. Commit messages via `-F` file (Kiro-specific)

Kiro's embedded terminal truncates long single-line `git commit -m "..."` invocations AND heredocs that embed backticks. The failure is silent — the terminal delivers a truncated command and the commit either fails parsing or records a short prefix.

Always write commit messages to a file and pass them via `-F`. The reliable pattern:

1. Author the message file via the `fsWrite` tool (NOT via terminal heredoc). This bypasses the embedded terminal entirely. Use a path under the workspace root such as `artifacts/commit-msg.txt` — paths starting with `/tmp/` are outside the `fsWrite` sandbox.
2. `git commit -F artifacts/commit-msg.txt` from bash.
3. `rm -rf artifacts/commit-msg.txt` (or the whole `artifacts/` dir if you created it just for this) after the commit lands.

```bash
# Step 2 and 3 only — step 1 is a fsWrite call in Kiro, not bash
git commit -F artifacts/commit-msg.txt
rm artifacts/commit-msg.txt
```

Benefits:

- Supports multi-line messages (title + blank line + body paragraphs + trailers) without shell-quoting gymnastics.
- Bypasses the terminal's per-line input cap.
- Bypasses the shell's `argv` size limit (256 KB on macOS default, but the terminal cap bites first).
- Bypasses heredoc backtick issues.
- Lets you preview the message by `cat`ing the file before committing.

`-m "short"` is acceptable only for terse single-line messages (under ~60 characters, no backticks, no angle brackets). Never use `-m` with a multi-line message via `\n` escapes — those route through the terminal's input buffer and hit the same truncation. Never use `cat <<'EOF' > file.txt` heredocs for commit messages — backticks inside the heredoc body can still hit the terminal cap.

### 8. Temporal Behaviour Defers to the Targeted Release

The targeted Temporal release is pinned by `TEMPORAL_SERVER_COMPAT` (currently `1.31.0`) and the vendored API by `TEMPORAL_PROTO_VERSION` (currently `v1.62.11`), both in `crates/tokeira-build-info/src/pinned.rs`.

For any question about public API **behaviour** — field semantics, error/status mapping, defaulting, lifecycle ordering, inheritance rules — the contract is **whatever the targeted release does**, verified against ground truth in this order:

1. **Vendored protos in `proto/upstream/`** for wire shape: messages, field numbers, enums, oneofs. NEVER read generated artifacts under `target/` — they can be stale; `proto/upstream/` is the source of truth.
2. **Temporal server source at the matching tag** for runtime behaviour the proto does not specify. **Read it from the local checkout** — there is a clone at `../temporal` (sibling of this repo) with the `v1.31.0` tag available. Use `git -C <temporal-checkout> show v1.31.0:<path>` and `git grep <pattern> v1.31.0 -- <path>` to read the exact tagged source offline; this is faster, pinned, and grep-able. Do NOT use web search/fetch for Temporal source when the local checkout is available. Read the actual code (e.g. `service/history/...`, `service/worker/...`, `common/...`) — do not infer behaviour from proto doc comments, SDK docs, blog posts, or memory. In specs/PRs, cite the source by repo-relative path + tag (e.g. `service/frontend/workflow_handler.go @ v1.31.0`, optionally linked as [`github.com/temporalio/temporal` tag `v1.31.0`](https://github.com/temporalio/temporal/tree/v1.31.0)). Never hardcode an absolute developer-machine path in committed specs, code, or docs.

Rules:

- When a behaviour question arises, resolve it against the targeted release **before** writing or amending a spec. A spec or requirement that contradicts the targeted release is wrong; fix the spec.
- This is distinct from "Do not port Temporal code" in the Mission: **reading** Temporal source to determine the contract is required; **copying** its implementation is forbidden. Tokeira's implementation stays original; only the observable contract is shared.
- Where a Tokeira mechanism has no exact Temporal analog (e.g. history replay reconstruction), the test of correctness is: *does Tokeira's response match what the targeted release would return for the same execution lineage?*
- Cite the verifying source (proto path or server source path + tag) in the spec/PR when a behaviour decision is non-obvious, so reviewers can confirm against the same ground truth.

### 9. Code Documentation

Tokeira is a correctness-critical durable execution engine whose behaviour mirrors a
specific external contract (Temporal v1.31.0). Code that is merely correct is not
enough — the *reasoning* behind it must survive in the source, because the next reader
(human or agent) cannot re-derive a concurrency invariant, an ordering subtlety, or a
ground-truthed behaviour decision from the code alone. Comments are part of the
deliverable, not an afterthought. A change that adds non-obvious logic without
explaining why it is correct is incomplete.

**The WHY-not-WHAT rule.** A comment must add information the code cannot. Restating a
type signature, a variable name, or obvious control flow is noise — it is worse than no
comment because it rots, misleads, and trains readers to skip comments. Delete such
comments when you see them.

```rust
// BAD — restates the code:
// increment the revision number
info.revision_number += 1;

// GOOD — explains why this is safe/necessary:
// Bump the revision so any task dispatched against the prior routing decision
// is fenced as stale at start time (recordworkflowtaskstarted/api.go @ v1.31.0).
info.revision_number += 1;
```

**What MUST be documented:**

- **Every module** carries a `//!` doc: what it owns, where it sits in the architecture
  (which plane/crate boundary), and the key invariants it upholds. A reader landing in
  the file cold should learn its purpose and its contract in the first screen.
- **Every public item** (type, trait, function, field) carries a `///` doc stating its
  contract: what it guarantees, what it assumes of callers, and any non-obvious failure
  or edge behaviour. "Pub" means "someone else depends on this" — document the promise.
- **Correctness-critical decisions** carry an inline WHY: concurrency hazards and the
  invariant that makes the code race-free (lock ordering, TOCTOU windows, why an
  operation is serialized); ordering and idempotency assumptions; CAS/OCC and fencing
  semantics; why a value is computed live vs. stored; precedence rules; and anything a
  future editor could plausibly "simplify" into a bug.
- **Ground-truthed behaviour** cites its source. Where behaviour matches the targeted
  Temporal release, cite the proto path or server source path + tag inline (e.g.
  `service/history/workflow/util.go @ v1.31.0`), per §8. Never invent an anchor; only
  cite what you have verified. This is what lets a reviewer confirm against ground truth
  without re-investigating.
- **Deliberate deviations and non-obvious tradeoffs** are stated explicitly, so they are
  not mistaken for oversights and silently "fixed".

**What must NOT be documented:** anything already obvious from the code. Do not narrate
control flow, paraphrase the next line, or add ceremonial headers. When in doubt, ask
"does this sentence tell the reader something the code does not?" — if no, omit it.

**Tests.** Property-based tests carry a one-line statement of the invariant they prove
(and a `// Feature: <name>, Property N` tag where a spec defines one). Do not comment
obvious test scaffolding.

**This is enforced like any other standard.** A pre-commit review (and any agent doing
implementation) treats missing module docs, undocumented public items, and uncommented
non-obvious logic as defects to fix before the change is complete — the same weight as a
failing lint. Comment density is not the metric; comment *quality and coverage of the
non-obvious* is.

---

## Architecture

Three planes:

- **Compatibility edge** (`tokeira-edge`, `tokeira-proto`, `tokeira-types`) — admits and translates requests. Does not own workflow semantics.
- **Authoritative runtime and storage** (`tokeira-kernel`, `tokeira-runtime`, `tokeira-storage`) — owns correctness. Shard/bundle ownership, lane-local execution, durable transitions, derived dispatch.
- **Projection plane** (`tokeira-projection`) — owns read models. Visibility, rollups, custom sinks. Outside the correctness path.

Detailed architecture docs: `docs/architecture/000-overview.md` and linked documents.

---

## Workspace Structure

```
tokeira/
├── Cargo.toml                    # Workspace root
├── apps/
│   ├── tokeirad/                 # Server binary
│   └── tkr/                      # Operator/developer CLI
├── crates/
│   ├── tokeira-types/            # Shared identifiers and value types
│   ├── tokeira-proto/            # Wire types (public + internal)
│   ├── tokeira-kernel/           # Pure deterministic transition engine
│   ├── tokeira-storage/          # Persistence interfaces + in-memory store
│   ├── tokeira-runtime/          # Lanes, broker, sweepers, timers
│   ├── tokeira-edge/             # Compatibility shell for public APIs
│   ├── tokeira-projection/       # Projection workers + visibility API
│   ├── tokeira-observability/    # Metrics/label definitions
│   ├── tokeira-build-info/       # Compatibility pins (proto + server-compat)
│   ├── tokeira-compatibility/    # Feature/SDK compatibility matrices
│   ├── tokeira-compatibility-proto/    # Tokeira-owned compatibility wire types
│   ├── tokeira-compatibility-service/  # Compatibility metadata service
│   ├── tokeira-state/            # Deployment state (CAS store + S3 store)
│   ├── tokeira-iac/              # IaC engine (plan/apply/destroy)
│   ├── tokeira-deploy-engine/    # Service lifecycle engine
│   ├── tokeira-config/           # Server config + generic TOML loader
│   ├── tokeira-orchestrator/     # Deployment orchestration facade
│   ├── tokeira-compose/          # Docker Compose provider (bollard)
│   ├── tokeira-aws/              # AWS resource implementations
│   ├── tokeira-build/            # Dagger image build recipes
│   ├── tokeira-controller/       # Control-plane service
│   ├── tokeira-autoscaler/       # Autoscaling service
│   ├── tokeira-remote-workstation/     # Remote workstation support
│   └── dagger-client/            # Dagger GraphQL client
├── proto/
│   └── upstream/                 # Vendored Temporal protos (authoritative; API v1.62.11)
├── platforms/
│   ├── local/                    # Bare-process local platform
│   └── compose/                  # Docker Compose platform with observability + DSQL module
├── docs/
│   └── architecture/             # Design documents (000–131)
└── .kiro/specs/                  # Feature specs (requirements, design, tasks)
```

---

## Package Boundaries

- `tokeira-kernel` is pure — no I/O, no async, no storage, no metrics.
- `tokeira-edge` is thin — translates requests, does not implement workflow semantics.
- `tokeira-projection` owns visibility types and the `VisibilityApi` trait. Edge re-exports them.
- `tokeira-state` provides two store implementations: `CasStore` (backend-agnostic single-document CAS) and `S3StateStore` (manifest + immutable snapshots).
- `tokeira-iac` and `tokeira-deploy-engine` are provider-agnostic. Platform-specific resources and services live in platform crates.
- Platform crates (`platforms/local`, `platforms/compose`) follow the deploy-eks `project` pattern: `config.rs`, `modules.rs`, `services.rs`, `compose.rs`.
- `tokeira-config` owns both the server runtime config model (`TokeiraConfig`) and the generic TOML loader. These are in the same crate because there is currently one consumer.
- `proto/upstream/` holds the vendored Temporal protos (API `v1.62.11`) and is the authoritative wire shape. `tokeira-proto` generates from it. Never treat generated output under `target/` as authoritative.

---

## IaC Engine Contracts

The engine distinguishes **desired** resources (what should exist) from **known** resources (everything the deployment can manage, including resources that may need deletion). The `InfraComposition` carries both sets plus `active_modules` for scoped operations.

- Resources implement `create()`, `update()`, `delete()`, `describe()`, `diff()`, `dependencies()`.
- Modules implement `name()`, `dependencies()`, `resources()`.
- Both modules and resources are topologically sorted by dependencies before execution.
- `describe()` is called during `refresh_state` to get live provider state before diffing.
- The engine calls an optional `StateSaver` callback after each mutating operation for incremental crash-safety.
- State backends must tolerate a missing backing store on `load()` (return default) so the remote-state module can bootstrap the store during the first apply.

---

## Configuration

- Server config: `tokeirad.toml` — `TokeiraConfig` with four sections: infrastructure, policy, capacity, emergency.
- Platform config: `deployment.toml` — platform-specific (`LocalConfig` or `ComposeConfig`). Compose DSQL deployments carry `storage = "dsql"` plus `[dsql]` mode/endpoint/arn/region fields.
- Compose DSQL writeback updates `tokeirad.toml` with `infrastructure.storage = "dsql"` plus `infrastructure.dsql.endpoint` and `infrastructure.dsql.region`.
- `serde(deny_unknown_fields)` on all config structs — typos are caught at parse time.
- `RuntimeConfig` is always `Default` — not configurable from TOML. Mechanical settings are auto-tuned.
- No env vars on invocation. Defaults characterized by expected performance, not deployment environment.
- Emergency overrides (`disable_stickiness`, `freeze_projection`, `cap_poll_admission`) are logged as warnings.

---

## Testing

- Unit tests co-located in each module (`#[cfg(test)]`).
- Property-based tests using `proptest` for config validation, serialization round-trips, dependency ordering.
- `cargo test` runs all unit tests. All tests must pass before committing.
- No tests that require live AWS credentials or Docker in the default test suite.
- Some tests cause intentional panics. Only consider tests that have failed according to the test harness to be a real problem.
- Key properties to maintain:
  - Config TOML round-trips without loss.
  - Unknown config fields are rejected.
  - Module dependency graph is a DAG (no cycles).
  - Service dependency graph is a DAG (no cycles, no missing deps).
  - State CAS: two concurrent saves from the same version — at most one succeeds.

### Functional conformance harness (Tier 2)

Behavioural conformance against Temporal is validated separately from `cargo test`
by replaying Temporal's functional Go corpus (pinned at `TEMPORAL_SERVER_COMPAT`)
over the real gRPC wire against a running `tokeirad`. It is operator-invoked and
lives in the sibling Temporal fork, not the default test suite. Do not assume it
runs under `cargo test`. See `docs/testing/functional-conformance-harness.md` for
what it proves, how it works, and the conventions binding any fix derived from a
run (v1.31.0 ground truth, no kernel additions, config-as-constant, feature modes
as independent runs, raise ambiguity).

Runs pin the corpus's `go.mod` toolchain (`go1.26.2`) via `GOTOOLCHAIN`, set by the
run-all runner. Tests that cannot run against an out-of-process `tokeirad` (e.g.
those depending on `OverrideDynamicConfig`) are skipped by name through the fork's
conformance skip registry (`tests/testcore/tokeira_conformance_skip.go`) — never by
editing a corpus test body — applied via the `SetupTest`/`SetupSubTest` hooks and a
runner-derived `go test -skip` regexp for raw `t.Run` sub-tests. Each skip carries a
cited reason and still emits a classified `skip` outcome.

## Enforced Commands

The following commands are enforced for each pull request:

```bash
cargo +nightly fmt --all --check   # formatting
cargo lint                         # clippy on workspace + all targets
cargo test-lint                    # clippy on tests
cargo check --workspace            # compilation
cargo test --workspace             # unit tests
cargo doc --workspace --no-deps    # documentation (RUSTDOCFLAGS="-D warnings")
tkr ci check                       # compatibility invariants once Dagger module is available
```

Use `cargo lint` to check if everything compiles without running tests. `cargo check` alone does not build test targets.

---

## Decision Process

0. **For API-behaviour questions, resolve against the targeted Temporal release first.** See §8 — `proto/upstream/` for shape, Temporal server source at tag `v1.31.0` for behaviour. This tier sits above the spec: a spec that contradicts the targeted release is the thing that's wrong.
1. **Check the spec first.** Requirements and design docs are in `.kiro/specs/`. They're the source of truth.
2. **Check existing patterns.** Look at how similar things are done in the codebase before inventing a new approach.
3. **Prefer boring solutions.** The simplest approach that satisfies the requirement is the right one.
4. **Ask if unsure.** If a decision has architectural implications, surface it rather than guessing.

### Crate-local AGENTS.md (read before editing a high-risk crate)

Some crates carry their own `AGENTS.md` that refines this root file for that crate. When editing under one of these paths, read its `AGENTS.md` first and treat it as binding; on conflict with this root file, the crate-local (stricter) rule wins. This applies to every agent, not only Codex — do not rely on automatic nested-file loading; open it explicitly.

| Crate | Concentrates |
|-------|--------------|
| `crates/tokeira-kernel/AGENTS.md` | Purity: no I/O/async/storage/metrics, no side-effecting commands, no non-determinism. |
| `crates/tokeira-storage/AGENTS.md` | DSQL migrations (forward-only / build-phase no-ALTER / DDL subset) and the `max_idle_conns == max_conns` invariant. |
| `crates/tokeira-runtime/AGENTS.md` | History is authority; queues are disposable; durable state via fenced `commit_transition`. |
| `crates/tokeira-edge/AGENTS.md` | Thin translation only; public-API behaviour ground-truthed to the targeted release (§8). |
| `crates/tokeira-state/AGENTS.md` | CAS-not-force-overwrite; immutable snapshots; tolerate a missing store on load. |

### Change Classification

| Change Type | Examples | Required |
|-------------|----------|----------|
| **Trivial** | Fix typo, add doc comment, rename local variable | Tests pass |
| **Standard** | New resource, new service, new CLI command | Tests pass + follows existing patterns |
| **Architectural** | New crate, new dependency, change to state format | Spec update or explicit approval |
| **Destructive** | Remove crate, change config schema, break state compatibility | Spec update AND explicit approval |

---

## Working Agreements

### Temporal Compatibility Changes

1. `TEMPORAL_PROTO_VERSION` (API surface, `v1.62.11`) and `TEMPORAL_SERVER_COMPAT` (behavioural target, `1.31.0`) are independent pins in `crates/tokeira-build-info/src/pinned.rs`. `TEMPORAL_SERVER_COMPAT` is the authority for every API-behaviour question (see §8). Do not bump the server compatibility claim just because the vendored proto version changed.
2. New WorkflowService or OperatorService surfaces must be classified in `FEATURE_MATRIX` in `crates/tokeira-compatibility/src/matrix.rs`.
3. New SDK claims must update `SDK_MATRIX` in `crates/tokeira-compatibility/src/sdk.rs` with evidence and verification state.
4. Tokeira-owned compatibility metadata uses Buffa/connect-rust under `proto/tokeira/compatibility/v1/`; do not add Tokeira extension fields to upstream Temporal protos.
5. Run `tkr ci check` when the Dagger compatibility module is available. Until then, use the focused matrix, CLI, and edge tests described in `.kiro/specs/temporal-compatibility/`.

### Adding a New Platform

1. Create `platforms/{name}/` with `config.rs`, `modules.rs`, `services.rs`, `compose.rs` (or equivalent).
2. Implement `Deployment` and `Ops` traits from `tokeira-orchestrator`.
3. Add `PlatformKind` variant and `CliPlatformKind` variant.
4. Add prototypical config generation in `tkr/src/prototypical.rs`.
5. Add tests for config generation, module composition, and service ordering.

### Adding a New IaC Module

1. Create the module in the platform's `modules.rs`.
2. Implement `Module` trait with `name()`, `dependencies()`, `resources()`.
3. Register it in the platform's `infra_modules()` method.
4. Add tests for resource enumeration and dependency ordering.
5. For compose storage modules, use `DsqlModule` as the reference pattern: module-owned config, explicit dependencies, and provider handles registered through `register_infra_extensions()`.

### Adding a New CLI Command

1. Add subcommand enum variant in `tkr/src/cli.rs`.
2. Create handler in `tkr/src/commands/{group}.rs`.
3. Wire into the command tree in `main.rs`.
4. Add CLI parse tests.
5. For multi-file command groups, use `.kiro/specs/image-lifecycle/` as the reference pattern: clap variant, handler module, main wiring, and any command-specific session re-exec helper.

### Adding a New Image

1. Decide which platform(s) need the image (compose, ECS, or both).
2. In each owning platform's `src/images/` module, declare a struct implementing `tokeira_deploy_engine::image::Image`.
3. Add the struct to that submodule's `all()` function, such as `images::tokeirad::all()` or `images::observability::all()`.
4. If the image's remote ref is referenced by config, override `writeback_targets(ctx)` to list the dotted TOML keys.
5. Add property-test coverage if `desired_ref` or `writeback_targets` logic is non-trivial.
6. If the image needs a new build recipe, add a free function to `tokeira-build` with its own hardcoded Dagger pipeline.

### Adding or Changing a DSQL Migration

DSQL migrations live in `crates/tokeira-storage/migrations/` as `VNNN__snake_case.sql`, one statement per file. `build.rs` embeds them at compile time; the runner (`crates/tokeira-storage/src/dsql/migration.rs`) is forward-only, checksum-verified, and rejects version gaps and duplicates.

- **Initial build phase (now): no `ALTER TABLE`.** There is no baseline schema to preserve, so a new column/constraint MUST be folded into the table's base `CREATE TABLE` migration rather than added as a follow-up `ALTER`. Editing an already-embedded migration is fine — its checksum simply changes and the schema is recreated from scratch. Keep versions contiguous (no gaps); deleting the highest migration is acceptable.
- **After a baseline is cut, this flips to strictly forward-only.** Once any environment has applied the migrations, an embedded migration MUST NOT be edited — the runner rejects a changed checksum for an applied version. From that point every schema change is a new `VNNN` migration (including `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), never an in-place edit of an existing one. Removing the build-phase "fold into base / no ALTER" rule is itself the signal that the baseline has been cut.
- DSQL DDL constraints still apply at all times: one statement per file, secondary indexes created `ASYNC`, no `CHECK` constraints (validate in the application), no `BIGSERIAL` (generate IDs in-app). The `DdlValidator` enforces the DSQL-safe subset.
- There is no historical hand-maintained schema dump — the migrations directory is the single authoritative schema source.

---

## Observability Stack (Compose Platform)

Pinned versions:
- Mimir: `grafana/mimir:3.0.6`
- Loki: `grafana/loki:3.7.1`
- Grafana: `grafana/grafana-oss:12.4.3`
- Alloy: `grafana/alloy:v1.16.0`
- AWS CLI: `public.ecr.aws/aws-cli/aws-cli:latest`
- BusyBox: `public.ecr.aws/docker/library/busybox:latest`

Three compose IaC modules are relevant in DSQL mode: `local-state`, `dsql`, `observability`, then `runtime` by dependency order. In-memory compose deployments omit `dsql`.

Provisioned Grafana dashboards:
- `broker-runtime-health.json`
- `grpc-edge-health.json`
- `storage-projection-health.json`
- `log-exploration.json`

The six mirror images (Mimir, Loki, Grafana, Alloy, AWS CLI, BusyBox) are declared in each platform's `src/images/observability/mod.rs` via a platform-local `mirror_image!` macro. Version bumps are a one-line change in the platform's `ObservabilityConfig::default()` defaults or the `default_<field>_image()` helpers for `aws_cli_image` and `busybox_image`.

---

## Repository Values

1. **Correctness over speed.** A slow transition that commits correctly beats a fast one that corrupts state.
2. **Explicitness over magic.** Every resource, every permission, every config field — visible in code.
3. **Operator empathy.** Error messages tell the operator what happened, why, and what to do next.
4. **Minimal surface.** Every dependency, every abstraction, every config option must earn its place.

---

## Spec Reference

- `.kiro/specs/*/` — feature specs (requirements, design, tasks)
- `docs/architecture/` — architecture design documents
- `proto/upstream/` — vendored Temporal protos (API `v1.62.11`); authoritative wire shape
- Temporal server source for behaviour: [`github.com/temporalio/temporal` at tag `v1.31.0`](https://github.com/temporalio/temporal/tree/v1.31.0) (the `TEMPORAL_SERVER_COMPAT` target) — see §8
