# Spike: Tokeira CI as a Dagger module (rust.3)

**Question.** Should the interior of `tkr ci check` become a Dagger module authored with
the fork's Rust SDK (invoked via `dagger check` / `dagger call`), instead of the
in-process client session the release-process spec lands? This spike gathers the
evidence; it changes no spec and no production code.

**Verdict: defer.** Module authoring is real and well-designed at
`sdk/rust/v1.0.0-beta.11.rust.3`, and the authored surface of a CI module expresses
cleanly (see `shape/`). But module *consumption* is blocked at rust.3 by two release
gaps — findings F1 and F2 below, both fork-side, both with the enabling mechanism
already present — and the single most CI-relevant engine behaviour (F3, long calls)
cannot be probed until they clear. Client-mode `tkr ci check` and the CI-substrate
work proceed unaffected; the module question re-enters when a release carries the
F1/F2 fixes, via the rerun plan at the bottom.

## Ground truth and environment

- Fork tag: `sdk/rust/v1.0.0-beta.11.rust.3` (== fork HEAD at spike time). All code
  citations below are `<path> @ sdk/rust/v1.0.0-beta.11.rust.3` in `iw/dagger`.
- CLI: the release's native `darwin/arm64` binary, commit `c5f7dc77`, from the
  apple-silicon companion release.
- Engines: the companion release's `linux/arm64` engine (already running locally);
  the main release's `linux/amd64` engine, fetched from the release, SHA-256
  verified, run under emulation (`--platform linux/amd64`, privileged, no restart
  policy). Emulated timings carry an asterisk.
- Engine endpoint via `_EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://<name>`;
  identity enforced by the SDK/CLI exact-version target (strict pair). The darwin
  CLI ↔ linux engine handshake reports `v1.0.0-beta.11.rust.3+c5f7dc77` on both
  engines.

## What was executed

1. `dagger sdk install rust` against a spike-local `dagger.toml` (workspace scoping
   below) — installs the builtin SDK entry (~30 s cold, including packaged-content
   load). One rough edge: F4.
2. `dagger module init rust ci --path module` on the **arm64** engine → **F1**
   (amd64-only runtime helper; fails before any Rust work).
3. Same init on the **amd64** engine → runtime helper executes; fails with
   `DEPENDENCY_RESOLUTION_FAILED` at `initialization.cargo` → **F2**
   (2 m 31 s cold, 24 s warm to the same failure\*).
4. Workaround attempt: `module/.cargo/config.toml` (kept as an exhibit) patching
   both SDK crates to `git+https://github.com/iw/dagger@c5f7dc77…` — structurally
   defeated, which is itself the decisive half of F2.
5. Engine-free authoring: `shape/` compiles the honest authored surface of a
   Tokeira CI module against the vendored rust.3 SDK + macros (fmt / clippy /
   test / rustdoc `-D warnings` all clean).

A useful negative result along the way: both failed inits exported **nothing** —
initialization refuses to yield a mutation-capable Changeset on failure
(`crates/dagger-sdk-engine/src/initialization.rs`), so the tree stayed clean.

## Findings

### F1 — arm64 engine ships an amd64-only Rust SDK runtime helper (blocker)

On the arm64 engine, the module runtime's
`withExec /usr/local/bin/dagger-rust-engine execute …` dies in the dynamic loader:
`/lib64/ld-linux-x86-64.so.2` missing — an x86-64 binary inside the arm64 engine's
packaged SDK content. Consistent with `sdk/rust/docs/engine-integration.md` ("the
ordinary complete-engine build … composes that content into the standard complete
`linux/amd64` engine"): the apple-silicon companion rebuilt the *engine* for arm64
but the *packaged SDK content* still carries the amd64 helper, and companion
verification exercised client-mode operations only.

This is release assembly, not design: `sdk/rust/runtime/assets/runtime-policy.json`
already defines both `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`
targets, and the runtime builds modules for the engine's own platform. Until fixed,
module-mode is unavailable on exactly the platforms tokeira's substrate design puts
first (Apple Silicon dev machines, arm64 CI runners).

### F2 — the packaged SDK dependency cannot resolve, and consumers cannot override it (blocker)

The engine's packaged SDK content bakes the dependency every generated module
project receives — observed in the init request:
`"sdk_dependency": {"source":"registry","registry":"crates-io","exact_version":"1.0.0-beta.11.rust.3","package":"dagger-sdk"}`.
That version is not published on crates.io (the `dagger-sdk` name there is the
community 0.x SDK), so initialization fails at
`initialization.cargo — Cargo dependency resolution failed`.

The model already has the alternative —
`PublishedSdkDependency::Git { url, revision }`
(`crates/dagger-codegen/src/engine/model.rs`) — but selection happens when the SDK
content is built into the engine, and the consumer-side override is structurally
excluded *by design*: initialization's cargo runs with `current_dir` at the
operation root **above** the mounted context, so no committed `.cargo/config.toml`
is on cargo's discovery chain, and the child environment is a validated allowlist
of exactly `CARGO_HOME`/`RUSTUP_HOME`/`SSL_CERT_DIR`/`SSL_CERT_FILE`
(`crates/dagger-sdk-engine/src/post_work.rs`). Hermetic dependency policy working
as intended; the fix belongs in the release, not the consumer.

Fix options, in preference order: build the packaged content selecting the Git
source at the release revision (repo is public; mechanism exists at rust.3), or
publish the crates under a registry name the fork owns. The two `.crate` files
already ship as release assets.

### F3 — the ~30 s long-call regression is untestable until F1/F2 clear (risk, unresolved)

`sdk/rust/docs/engine-integration.md` records a "previously observed long-running
module-query failure — `Post "http://dagger/query": unexpected EOF` after roughly
30 seconds — … an unverified engine regression" that release readiness explicitly
does not claim fixed. CI checks are precisely long-running module→engine calls
(a workspace nextest run holds one query open for many minutes), so this is the
go/no-go behaviour for module-mode CI — and nothing can probe it until a module
loads. `shape/`'s `probe_long_call(seconds)` is written for that day: bisect
`seconds` across the boundary. Client-mode long execs are unaffected (the prior
spike ran a 158 s hermetic step).

### F4 — `sdk install rust` capability inspection mis-resolves the builtin (minor)

Install succeeds and writes the entry, but its "inspect SDK capabilities" step
resolves `rust` as a local path (`moduleSource(refString: "rust") → local path
"rust" does not exist`) instead of routing through the builtin loader
(`core/sdk/builtin_source.go` handles exactly this for workspace installation).
Cosmetic in this run; worth fixing so install output ends clean.

### F5 — the CLI warns on git worktrees (minor, fleet-relevant)

Every CLI invocation from this worktree logs `WARN failed to open git repository
err="core.repositoryformatversion does not support extension: worktreeconfig"` —
the CLI's go-git layer cannot read repos using the `worktreeconfig` extension,
which every Claude fleet worktree does. Workspace detection (walk up to `.git`)
still worked here; flagging because a future feature that actually reads the repo
through that library would degrade in exactly the environment tokeira's agents
work in.

### F6 — macro hygiene: fieldless `#[object]` trips `clippy::unused_unit` (nit)

`#[sdk::object]` on a fieldless struct expands code clippy warns on, and an
item-site `#[allow]` does not reach the emitted items (crate-level allow needed —
see `shape/src/lib.rs`). SDK feedback, not a blocker.

## What maps well (the positive column)

- **`dagger check` is the CI surface.** Check-annotated functions
  (`role = "check"` in the authoring grammar), pattern selection (`ci:fmt`),
  `--skip`, `--failfast`, `-l` — the verb models a CI bar directly.
- **Contextual directory defaults replace hand-built mount filters.** An ordinary
  `Directory` parameter with `#[dagger(default_path = "/", ignore = ["target", ".git"])]`
  gives every check the workspace-with-`target/`-excluded mount the release-process
  spec requires (Req 7.4) as a declared default. (Authoring lesson: `context` marks
  injected call context and is mutually exclusive with value metadata — a
  contextual directory is a normal parameter.)
- **Engine-side build caching is the right shape.** The module runtime mounts
  cargo registry/git/target cache volumes keyed by
  `toolchain + rust-target + source_digest` (`sdk/rust/runtime/runtime.go`) — the
  same stable-inputs-only keying the CI specs demand.
- **Fail-closed everywhere.** Failed operations export no Changeset; the strict
  pair refuses version drift across architectures; the runtime build is `--locked`
  with a digest-verified plan and a distroless, stripped runtime image.
- **Workspace scoping cooperates.** A spike-local `dagger.toml` (nearest-config
  from cwd, boundary at the git root) kept every workspace file under `spikes/`.

## Layout

- `dagger.toml` — spike-local Dagger workspace config; carries the builtin SDK
  entry written by `dagger sdk install rust`. Live for the rerun day.
- `module/.cargo/config.toml` — the defeated F2 workaround, kept as an exhibit.
- `shape/` — standalone crate (own lockfile, workspace-excluded like every spike):
  the compile-verified authored surface — root object `tokeiraCi`, `fmt`/`nextest`
  as `role = "check"` functions, `CiCheckOutcome` as the typed wire projection of
  client-mode's `CiCheckResult`, and `probe_long_call` for F3.

## Rerun plan (the day a release carries F1+F2 fixes)

From this directory, with the pinned CLI and that release's engine:

1. `dagger module init rust ci --path module` — expect a scaffold + committed
   generated assets; inspect the rendered Cargo project and `dagger-module.toml`.
2. Move the authored surface from `shape/src/lib.rs` into the scaffold;
   `dagger generate`; commit the regenerated assets.
3. `dagger check -l`, then `dagger check ci:fmt` — first real module-mode check;
   record cold and warm timings (module compile happens engine-side, in the cache
   volumes above).
4. `dagger call tokeira-ci probe-long-call --seconds 45` (then bisect 25/35/60/300)
   — settle F3.
5. Only after 1–4: measure a real expensive check (`ci:nextest`) and compare
   verdict + wall-clock against client-mode `tkr ci check` on the same tree.

Steps 1–4 green ⇒ reopen the module-vs-client interior decision with data;
any red ⇒ the finding updates this README and the verdict stands.
