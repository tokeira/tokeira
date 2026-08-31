# Handoff: finish the dagger-module-ci rust.4 rerun (Claude → Codex)

Working doc for the agent completing the rerun; delete it in the slice's final
commit once `README.md` carries the outcome. Authored 2026-08-31 by Claude on
branch `agent/claude/ci-spike-rust4` (base this task's worktree on that branch —
the dependency is declared here per AGENTS.md §10.2).

## Mission

Complete the rerun plan at the bottom of `README.md` against the published
`sdk/rust/v1.0.0-beta.11.rust.4` release (fork commit `620c646c`), settle
**finding F3** (the ~30 s long-call regression — the go/no-go for CI-as-module),
and update `README.md` with the rust.4 outcome (keep the rust.3 record; the
verdict section changes). Findings below feed three fork-issue drafts — filing
them on iw/dagger needs Ian's go.

## What is already proven (do not re-derive)

- **Release verified**: six assets, SHA256SUMS OK. CLI reports
  `v1.0.0-beta.11.rust.4`, commit `620c646c`, `dirty: no`, `darwin/arm64/v8`.
- **F1 (#90) FIXED**: the arm64 runtime helper executes (rust.3 died in the
  x86-64 loader before any work).
- **F2 (#91) FIXED**: the packaged SDK dependency is
  `git = "https://github.com/iw/dagger", rev = "620c646c…"` (visible in the
  engine image's content history), and a cold `module init` resolved it inside
  the hermetic exec: **init PASSED, 5m23s cold**, full scaffold + generated
  assets + amended manifest + `Cargo.lock` landed.
- **Strict pair handshake**: client `v1.0.0-beta.11.rust.4` ↔ server
  `v1.0.0-beta.11.rust.4+620c646c`.
- The authored surface in `module/src/lib.rs` **compiles clean** against the
  real rust.4 SDK from Git (with the pre-generation stub, below) — the client
  API usage (`with_exec_opts`, `ContainerWithExecOpts`, `ReturnType`,
  `ModuleError`, `cache_volume`, contextual `Directory`) is validated.

## Environment recipe (this machine)

- **CLI**: `~/Projects/dagger/artifacts/620c646ce23110c85c1a5f5866141a9c1028e4fc/dagger`
  (extracted from the release; checksums verified).
- **Engine**: Docker image `tokeira/dagger-engine:v1.0.0-beta.11.rust.4-linux-arm64`
  (loaded from the release OCI tar in the same artifacts directory), container
  `tokeira-dagger-engine-rust4-arm64` (started with `docker run -d --privileged
  --name … <image>`); currently running. The rust.3 container
  (`tokeira-dagger-engine-rust3-arm64`) also runs — leave it alone.
- **Every dagger invocation** needs BOTH:
  - `_EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://tokeira-dagger-engine-rust4-arm64`
    — without it the CLI tries to pull `registry.dagger.io/engine:v1.0.0-beta.11.rust.4`,
    which is not published (even `dagger call --help` provisions).
  - `GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1` — the operator's global
    gitconfig carries `url.git@github.com:.insteadOf = https://github.com/`;
    the module layer replicates host insteadOf rules into the session
    (observed as an `/.init git config --global …` container), and the
    hermetic exec carries no SSH credentials, so the packaged HTTPS dependency
    would be rewritten to unauthenticatable SSH. NEVER edit the operator's
    `~/.gitconfig`; neutralize per-invocation only.
- Run dagger commands **from `spikes/dagger-module-ci/`** (nearest `dagger.toml`
  = the spike workspace). The repo root also has a `dagger.toml` + a **v1
  `dagger.lock` that rust.4 refuses** (`unsupported lockfile version "1"`) —
  do not run engine commands from the repo root, and do not regenerate that
  lock in this task (it belongs to the client-mode `tkr ci` surface).
- Untouchables, as ever: `~/.cargo/config.toml`, kache/`RUSTC_WRAPPER`,
  rustup, `KACHE_*`/`CARGO_*` machine config; never `cargo clean`.

## Current state on the branch

`spikes/dagger-module-ci/` on `agent/claude/ci-spike-rust4`:

- `module/Cargo.toml` — pre-authored, `[package]` + **`[workspace]` guard**
  (the F7 workaround, see findings) + the git dagger-sdk dep and tokio, as the
  first init amended them. `[[bin]]` deliberately absent (init re-adds it).
- `module/src/lib.rs` — the real authored surface: root `tokeiraCi`,
  `CiCheckOutcome`, `role = "check"` fns `fmt`/`nextest` with bodies mirroring
  client-mode `crates/tokeira-build/src/pipelines/ci.rs` (same builder image
  line, apt set, pinned fmt nightly `nightly-2026-06-16`, fmt byte-parity
  shell, nextest 0.9.143, cache volumes), `probe_long_call(seconds)` for F3,
  `#[dagger(context)] ctx: ModuleContext` injection, fallible returns via
  `sdk::ModuleError`. Constructor returns the **concrete** root type (the
  source compiler's rule: "a constructor must return the exact module root
  object"; `-> Self` is unproven and suspect).
- `module/src/dagger_generated/{mod.rs,module_context.rs}` — **pre-generation
  stubs** (marked as such). They satisfy (1) rustc: the macros unconditionally
  emit `crate::dagger_generated::__private::…` impls, and the authored bodies
  need `ModuleContext`/Deref-to-`Query`; (2) the authoring source walker,
  which follows `mod` declarations and fails authoring on a declared module
  with no source document (`SourceModuleInvalid`).
- `module/Cargo.lock` — from the successful first init (dependency set is
  unchanged, so re-init should take the `VerifyLockedMetadata` path).
- `dagger.toml` — SDK entry only. `[modules.ci]` registration is **commented
  out on purpose** (see findings: registration must trail regeneration). The
  `[[modules.dagger-rust-sdk.as-sdk.modules]]` entry is absent on purpose
  (init refuses "already authored" while it exists).
- `f2-workaround.cargo-config.exhibit` — the rust.3 F2 exhibit, moved out of
  `module/` (F2 is fixed; fold its story into the README update and delete the
  file in the final commit unless the README keeps it as an exhibit).
- Generated state (`dagger-module.toml`, `.dagger/`, `src/bin/`, real
  `src/dagger_generated/*`) was deleted mid-investigation — that deletion is
  what the resume sequence repairs via re-init.

## Resume sequence

1. **Re-init over the authored surface** (from `spikes/dagger-module-ci/`):

   ```
   dagger -y module init rust ci --path module
   ```

   with the env above. Expected: initialization amends the manifest
   (re-adding `[[bin]]`), preserves `src/lib.rs` (starter only renders when
   `src/lib.rs` is absent), runs generate-clients + generate-modules, and
   writes the real `src/dagger_generated/` tree + `src/bin/dagger-module.rs`
   + `dagger-module.toml` + the ownership manifest.

   - The open risk: generation may refuse to overwrite the stub files as
     user-owned bytes ("preserved and diagnosed"). If it does, delete the two
     stub files AND the `pub mod dagger_generated;` declaration + the
     `use dagger_generated::ModuleContext;` import from `lib.rs` (temporarily
     spelling the context parameter type in a way authoring accepts —
     the walker only checks the final path segment `ModuleContext`, so e.g. a
     locally-stubbed inline `mod dagger_generated { … }` also works), re-init,
     then restore the declaration AFTER generation and expect the
     `GENERATED_STALE` deadlock (finding: issue draft 3) — at which point the
     only exit is this same delete-marker-and-re-init loop with the tree
     present. Prefer making the stub-overwrite work; record whichever path
     succeeded in the README.
   - The previous authoring failure
     (`GENERATION_FAILED at operation.authoring`) was caused by the mod
     declaration pointing at the deleted tree, possibly compounded by
     `-> Self`; both are fixed in the committed state. If authoring still
     fails, bisect the surface — causes are NOT reported (diagnosability
     finding) — by commenting spans of `lib.rs` (context param → role
     metadata → fallible returns → ignore list) and re-initing; each cycle is
     ~2–6 min.

2. **Host-verify the regenerated crate**:
   `cargo check --manifest-path module/Cargo.toml` — the dispatch must now
   reference `TokeiraCi` (the stale-bridge compile errors disappear).
   Expect to need `pub mod dagger_generated;` present in `lib.rs` — if the
   fresh init returns the starter-style tree without the declaration compiling
   (F8), the crate only compiles once the author adds that line, which then
   stales the manifest → the deadlock again. If trapped, document it as the
   confirmation of issue draft 3 and probe F3 with a surface that never
   changes post-init (author lib.rs fully BEFORE init, exactly as this branch
   does).

3. **Register the module**: uncomment `[modules.ci]` in `dagger.toml`.

4. **Checks**: `dagger check -l` (expect `ci:fmt`, `ci:nextest`), then
   `dagger check ci:fmt` — record cold and warm wall-clock. Note: the
   contextual default `default_path = "/"` resolves to the DAGGER workspace
   root; if that is the spike directory, `cargo +nightly fmt --all` finds no
   root manifest and the check fails — that outcome is itself a README-worthy
   semantics note, and `ci:fmt`/`ci:nextest` are then exercised with an
   explicit `--source` pointing at a tree with a workspace manifest. Beware
   upload cost (finding below): from a Claude worktree the upload swept
   ~2 GB because gitignore filtering is dead there; a fresh Codex worktree
   with no `target/` uploads ~100 MB.

5. **F3 — the deliverable**: with the module loadable,
   `dagger call ci probe-long-call --seconds 45`, then bisect 25/35/60/300.
   Distinct `seconds` values defeat the exec cache (a repeated value returns
   cached instantly — rerun with a fresh value instead). A `sleep 300` held
   open in one module call ≈ the nextest-scale hold. Then, budget permitting,
   `dagger check ci:nextest` against a real workspace tree for the
   end-to-end datum (engine-side cold toolchain + workspace build: expect
   tens of minutes and multi-GB engine state).

6. **Write it down**: update `README.md` — rust.4 rerun section (steps 1–5
   outcomes, timings, the findings ledger below), revised verdict for the
   module-vs-client question driven by F3, layout section refreshed (exhibit
   file disposition, HANDOFF file deleted). Tick nothing in `.kiro/` — this
   spike has no spec.

7. **Finish per AGENTS.md**: §10.4 bar (workspace-scoped; the spike module is
   workspace-excluded — `cargo check --manifest-path` it separately), §11
   trailers (`Co-authored-by: Claude <noreply@anthropic.com>` for the carried
   work + your own), push, PR per §10.6.

## Findings ledger for the README update (all rust.4, this machine)

- **F7 (new, blocker-class)**: `new_manifest`
  (`crates/dagger-sdk-engine/src/project/manifest.rs`) renders no
  `[workspace]` table → `module init` inside any parent cargo workspace fails
  `cargo generate-lockfile` instantly ("current package believes it's in a
  workspace when it's not", reproduced verbatim). The release Verify gate
  inits in a bare directory and structurally cannot see it. Workaround
  proven: pre-author the manifest with the guard.
- **Diagnosability (new)**: `DEPENDENCY_RESOLUTION_FAILED` /
  `GENERATION_FAILED` carry `causes: []` — post-work captures redacted
  bounded stderr but initialization drops it; three distinct root causes in
  this rerun rendered as identical one-liners. Made every failure a
  source-archaeology exercise.
- **insteadOf hazard (new)**: host SSH-rewrite gitconfig is replicated into
  the module session; hermetic exec cannot authenticate SSH. Neutralization
  per-invocation works; the packaged-dependency fetch should be immune by
  construction.
- **Edit-loop deadlock (new)**: after any authored-source edit the ownership
  manifest is stale; `dagger generate` discovers zero generators in every
  spelling/state tried; a registered stale module bricks every workspace
  command including `dagger query`; `dagger module sdk` forwarder dispatches
  a rejected command shape; init is once-only behind two gates (as-sdk entry:
  "already authored"; module content: "already exists"). Working loop:
  remove the as-sdk entry + `dagger-module.toml`, re-init.
- **Registration gap (new)**: init writes only the as-sdk bookkeeping entry;
  `[modules.<name>]` workspace registration is the author's job, and must
  trail regeneration.
- **F8 (new, minor)**: the rendered starter does not compile as shipped
  (macros need `crate::dagger_generated`, starter never declares it).
- **v1 lockfile refusal (new, tokeira-side)**: the repo-root `dagger.lock`
  (v1, written by rust.3-era client work) is refused by rust.4
  (`unsupported lockfile version "1"`); regenerating it is a migration item
  for the client-mode `tkr ci` surface when tokeira adopts rust.4.
- **F5 upgraded**: go-git cannot read `worktreeconfig` repos (every Claude
  worktree) → workspace-upload gitignore filtering is dead → a module
  operation uploaded ~2 GB (swept `target/`). Real CI cost, no longer
  cosmetic.
- **F4 still present**: `-m rust` resolves the builtin as a local path.
- **F6**: not yet re-tested at rust.4 (no `#![allow(clippy::unused_unit)]` in
  the authored surface; check whether clippy still trips on fieldless
  objects when you lint the module crate).

## Issue drafts (file on iw/dagger only with Ian's authorization)

Three drafts are ready: (1) F7 workspace-guard template defect + gate
blind-spot; (2) initialization/generation discard cargo & authoring stderr;
(3) the regeneration deadlock (no reachable `dagger generate` path, workspace
bricking, once-only init). Full texts live in the session scratchpad and are
reproducible from this ledger; keep the "gate inits in a bare directory /
under a parent workspace" framing — it is the load-bearing observation.

## Wider context

The spike's verdict feeds the CI-architecture conversation (module-vs-client
interior for `tkr ci check`; client-mode landed in `tokeira-build` and is the
comparison baseline — `crates/tokeira-build/src/pipelines/ci.rs`). F3 is the
single decisive datum; everything else is ergonomics evidence for that
decision. The rust.4 release itself is DONE (tag + GitHub release published);
issues #90/#91 can close on this rerun's evidence once Ian says so.
