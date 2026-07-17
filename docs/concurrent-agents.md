# Concurrent AI agents on one machine

*Working practice for running Claude Code, Codex (ChatGPT app), and Kiro CLI in parallel
against this repository. The non-negotiable rules live in `AGENTS.md` §10; this document
is the mechanics. Facts verified July 2026.*

## The shape of the practice

Two decisions carry the whole practice. First, **every agent works in its own git
worktree**, so each has its own `target/` and cargo never contends — on stable cargo, two
builds against one target directory fully serialize on the build- and artifact-dir locks.
Second, **kache runs as a global `RUSTC_WRAPPER`**, so all those per-worktree target
directories are backed by one content-addressed store: compiled artifacts exist once on
disk and are restored into each `target/` as APFS reflinks.

```
<repo>                          ← main checkout: the human + integration workspace
<repo>/.claude/worktrees/…      ← Claude Code native worktrees (Claude manages these)
$CODEX_HOME/worktrees/…         ← ChatGPT app managed worktrees (the app keeps an LRU of 15)
<repo>-wt/…                     ← tkw-created worktrees, branch agent/<name> (Kiro CLI, manual)
        │  each worktree has its OWN target/ → zero cargo lock contention
        ▼
   kache store (one per machine) ← one physical copy of every compiled artifact,
                                   shared via reflink, LRU-evicted at the store cap
```

Integration stays serialized where serialization is cheap and correct: agents produce
branches; the human merges them one at a time.

## Why this is the right shape (July 2026)

The "obvious" alternative — pointing every agent at one shared `CARGO_TARGET_DIR` — is
precisely wrong. On stable cargo, two builds against the same target directory take
exclusive whole-build locks. The only relaxation so far is Cargo 1.93 (2026-01-22):
*"Avoid unnecessary artifact directory locking for `check` builds."* Per-unit locking
exists only as nightly `-Zfine-grain-locking` (tracking: rust-lang/cargo#4282, still
open). This workspace pins Rust 1.96 (`rust-toolchain.toml`), so the check-lock
relaxation is already in effect; nothing further to install.

Sharing artifacts *across* worktrees is the part cargo cannot do yet: `build.build-dir`
(stable since 1.91) relocates intermediates but does not dedupe them, sccache largely
misses across worktrees because compilation paths leak into its keys, and cargo's own
cross-workspace cache is a 2026 project goal headed for nightly first, initially
excluding build scripts and proc-macros — not something to build a workflow on before
2027. kache is built for exactly this gap: keys are blake3 hashes of normalized compiler
inputs (build-script outputs and extern rlib/rmeta hashes included), so the same crate
compiled in any worktree hits the same store entry, and restores are zero-copy on APFS.

Sensible check-in for retiring pieces of this setup: early 2027, or whenever a Rust
release post mentions the cargo build cache stabilizing.

## The shared layer: kache

Two directories are deliberately shared machine-wide; everything else is per-worktree.

- **`$CARGO_HOME` (`~/.cargo`)** — the registry/git source caches (self-GC'd by cargo
  since 1.88), `config.toml` where `kache init` writes the `rustc-wrapper` hook, and the
  `.package-cache` lock every cargo invocation touches for a few milliseconds (the one
  residual cross-worktree contention; harmless).
- **The kache store** — location printed by `kache doctor`; on macOS expect it under
  `~/Library/Caches`. It must live on the same APFS volume as the worktrees, or reflinks
  silently degrade to full copies.

What kache does **not** cache matters here: binaries, test executables, dylibs, and
proc-macro outputs are skipped by default (linking and macOS code-signing make them poor
cache citizens). So a fresh worktree's first `cargo check` restores the dependency graph
in seconds — but the first `cargo test --workspace` (the conformance bar) still pays
full linking of `tokeirad` and every test binary. That is the expected cost profile:
fast check, then a real link bill once. `KACHE_CACHE_EXECUTABLES=1` exists if the link
bill proves worse than the cache overhead.

Two trade-offs, stated honestly:

- **Incremental compilation is off while kache wraps rustc** (the two strategies
  conflict). Agents barely notice — their builds are dominated by dependency and cold
  workspace compilation, which the store accelerates. If the human's inner loop on a leaf
  crate suffers, disable the wrapper in that one shell (see the escape hatch below).
- **kache is young** (open-sourced March 2026, ~weekly releases). The risk is bounded
  because the fallback is graceful: comment out `rustc-wrapper` in `~/.cargo/config.toml`
  and everything builds normally, unwrapped. The worktree half of this practice — the
  lock-contention fix — does not depend on kache at all.

### Install and verify

```bash
cargo install kache cargo-sweep
kache init      # writes rustc-wrapper into ~/.cargo/config.toml; installs the launchd daemon
kache doctor    # verify wiring; note the store path it reports
```

> **Verify at install, don't trust this doc:** several knob names circulating in kache
> write-ups (`KACHE_MAX_SIZE`, `KACHE_DISABLED`, `KACHE_VERIFY_RESTORES`) are absent from
> the public docs as of July 2026. `kache gc [--max-age <dur>]` (LRU + age eviction) and
> `KACHE_BASE_DIR` / `KACHE_CACHE_EXECUTABLES` are documented. Confirm the store-size cap
> and the per-shell disable knob against `kache doctor` / `kache --help` output when
> installing, and correct this section if the names differ.

After install, one cold `cargo check --workspace` in the main checkout populates the
store; every worktree created afterwards restores from it.

## The agents

`AGENTS.md` is the shared contract, read natively by all three agents. `.worktreeinclude`
(tracked, gitignore syntax) lists gitignored files copied into new worktrees — honored
natively by Claude Code and the ChatGPT app, and applied by `tkw` for the rest.

### Claude Code — native worktrees

```bash
claude --worktree parser-fix     # .claude/worktrees/parser-fix, branch worktree-parser-fix
```

Run `claude` once at the repo root first to accept workspace trust, or the first
`--worktree` invocation errors out. Mid-session, asking Claude to "work in a worktree"
does the same via its built-in tool; the desktop app creates a worktree per parallel
session automatically.

- `.claude/settings.json` sets `worktree.baseRef: "head"`, so worktrees branch from the
  local HEAD (unpushed work included) rather than `origin/HEAD` — right for a local-first
  solo flow.
- Hooks (same file) run `tkw hook post-edit` after each edit (single-file nightly
  rustfmt) and `tkw hook stop` on session end (`cargo check --workspace`; exit 2 blocks
  the session finishing until the tree is green, with loop protection via
  `stop_hook_active`).
- Cleanup is Claude's: clean worktrees are removed on exit, dirty ones prompt, and agent
  worktrees are `git worktree lock`ed while running so sweeps can't remove them. Keep the
  number of *simultaneously committing* agents to a handful anyway — all worktrees share
  one `.git`, and parallel commits contend on its index lock.

### Codex — ChatGPT app managed worktrees

Codex runs through the ChatGPT app's **Local | Worktree** environment picker, not the
CLI. Day to day:

- **Research / questions** → Local (the main checkout).
- **Coding task** → Worktree, based on `main`, *without* copying unstaged local changes.
  The app creates a managed worktree under `$CODEX_HOME/worktrees/` in detached-HEAD
  state; it is a real linked worktree of this repository.
- **Finishing** → have it run the Enforced Commands bar, review the diff, then **Create
  branch here** with a clear name (`codex/<task>`), commit via the app's git controls.
  The app retains the 15 most recent worktrees and cleans older ones.

Configuration is split in two, per Codex's project-config rules:

- **Tracked, in this repo** — `.codex/config.toml`: selects the permission profile and
  raises `project_doc_max_bytes` to 128 KiB (this repo's `AGENTS.md` exceeds the 32 KiB
  default, which would otherwise be silently truncated).
- **User-level** — `~/.codex/config.toml` defines the profile (profiles cannot live in
  project config). Permission profiles are beta; do **not** combine them with the older
  `sandbox_mode` / `[sandbox_workspace_write]` keys, which take precedence if present:

```toml
default_permissions = "tokeira"

[permissions.tokeira]
extends = ":workspace"

[permissions.tokeira.filesystem]
"~/.cargo" = "write"                  # cargo's package-cache lock + registry
"~/Library/Caches/kache" = "write"    # the kache store (confirm path via `kache doctor`)

[permissions.tokeira.network]
enabled = false                       # agents build --locked from the warm registry
```

> Smoke-test on first use: run one Worktree chat, have it `cargo check`, and confirm the
> profile syntax took (the docs' filesystem examples use a nested `:workspace_roots`
> form; flat path keys as above came from Codex itself) and that the wrapped rustc can
> reach the kache daemon. Then check `kache stats` for hits.

### Kiro CLI 3.0 (early access) — tkw worktrees

Kiro CLI has no managed worktrees, so `tkw` provides them:

```bash
tkw new docs-pass                # <repo>-wt/docs-pass, branch agent/docs-pass, includes copied
cd ../<repo>-wt/docs-pass && kiro-cli --v3
```

- **Context:** Kiro v3 reads `AGENTS.md` natively (always included). `.kiro/specs/`
  drives the spec workflow from the terminal (`/spec`).
- **Hooks:** `.kiro/hooks/rust-quality.json` (tracked, v3 schema) wires the same two
  `tkw hook` commands as Claude. One asymmetry to know: Kiro blocks on exit 2 only for
  `PreToolUse`/`UserPromptSubmit` — the `Stop` gate is *advisory* under Kiro (a warning,
  not a block). The finish-green bar for Kiro work is therefore enforced by `AGENTS.md`
  §10 discipline plus the human's integration check, not by the hook.
- **Permissions:** v3 replaces Supervised mode with `permissions.yaml`, deliberately
  stored *outside* the repo (user level: `~/.kiro/settings/permissions.yaml`; workspace
  overrides under `~/.kiro/workspace-roots/<hash>/`) so repositories can't inject rules.
  Suggested baseline — narrow allows, no bare `*`:

```yaml
rules:
  - capability: shell
    effect: allow
    match:
      - cargo check *
      - cargo test *
      - cargo fmt *
      - cargo +nightly fmt *
      - cargo lint *
      - cargo test-lint *
      - git *
      - tkw *
    exclude:
      - git push *
      - git reset --hard *
  - capability: fs_read
    effect: allow
```

Early-access caveat: v3 surfaces are moving; re-verify hook and permission schemas
against kiro.dev/docs/cli/v3 when upgrading.

## tkw — the fleet tool

`tools/tkw` (workspace member; install once with `cargo install --path tools/tkw`):

| Command | Does |
|---------|------|
| `tkw new <name> [--base <ref>]` | Worktree at `<repo>-wt/<name>`, branch `agent/<name>`, base = main checkout HEAD; copies `.worktreeinclude` matches |
| `tkw ls` | Every worktree of the repo, annotated `[main\|tkw\|claude\|codex\|manual]` |
| `tkw rm <name> [--force]` | Remove a tkw-owned worktree (branch kept) |
| `tkw clean [--base <ref>]` | Remove tkw worktrees whose branch is fully merged |
| `tkw tidy` | Weekly hygiene: clean + prune + `cargo sweep` + `kache gc` + report |
| `tkw hook post-edit\|stop` | The hook bodies Claude/Kiro configs invoke |

The ownership rule, enforced in code: tkw only ever **removes** worktrees it created.
Claude's and the app's worktrees have their own lifecycles; `tkw tidy` will sweep stale
build artifacts *inside* them (artifact deletion, never worktree deletion) and `tkw ls`
shows them, but removal is refused. Overrides: `TKW_DIR` (worktree location — keep it on
the same APFS volume as the store) and `CODEX_HOME` (classification only).

## Integration discipline

Merge agent branches **one at a time** in the main checkout, rebase-or-merge each onto
`main` before the next; never merge agent branches into each other. Before a batch of
agent work starts, `main` is green; after each merge, `cargo check --workspace` (cheap —
warm store) before the next. Two git settings worth having:

```bash
git config rerere.enabled true        # replays conflict resolutions you've made before
git config worktree.guessRemote true
```

Lockfile conflicts (two branches that each changed dependencies — which `AGENTS.md` §10
forbids without a task saying so): keep both sides' `Cargo.toml`, delete the conflicted
`Cargo.lock` hunks, `cargo check`, commit the regenerated lockfile.

One git rule worth remembering: a branch can be checked out in only one worktree at a
time — to open an agent's branch in the main checkout, remove its worktree first.

## Disk hygiene

Steady-state disk = the main checkout's `target/` + one thin reflink-backed `target/`
per live worktree + the LRU-capped store. `tkw tidy` weekly (or via launchd) keeps the
edges trimmed; it prints `kache stats` and a `df` so the reclaim is visible. Scorched
earth should never be needed again — but if it is, deleting any `target/` is always
safe: worst case, the next build restores from the store.

## Repo-specific notes

- **Linker:** the tracked `.cargo/config.toml` links with lld (`-fuse-ld=lld` via clang)
  on aarch64 and sets `debug = "line-tables-only"` — deliberate link-time tuning that
  every worktree inherits. Since kache skips linked outputs, link time is unaffected by
  the cache either way; keep or benchmark lld independently of this practice.
- **Conformance runs:** `run_suite.sh` launches `target/debug/tokeirad` from whichever
  checkout runs it — per-worktree targets mean parallel suites don't collide on the
  binary, but they do share the DSQL/storage layer a suite points at; keep live-suite
  runs to one at a time unless configs isolate storage.
- **Zed / rust-analyzer:** the tracked `.zed/settings.json` sets rust-analyzer's
  `cargo.targetDir: true`, giving RA its own `target/rust-analyzer` so save-triggered
  checks never serialize against terminal or agent builds in the same checkout — and
  since it's tracked, every worktree opened in Zed inherits it. The extra artifacts
  dedupe through the kache store like any other target dir. If RA's per-save check feels
  slower once kache lands (incremental is off under the wrapper), the escape hatch is
  RA's `cargo.extraEnv` / `check.extraEnv` in the same file, setting kache's disable
  knob (name confirmed by `kache doctor`) for RA's cargo invocations only.

## Sources

Cargo locking/changelog: doc.rust-lang.org/cargo/CHANGELOG.html (1.91 build-dir, 1.93
check-lock, 1.88 cache GC) · rust-lang/cargo#4282 · cross-workspace cache goal:
rust-lang.github.io/rust-project-goals/2026/cargo-cross-workspace-cache.html — kache:
github.com/kunobi-ninja/kache · kunobi.ninja/docs/kache ·
kunobi.ninja/blog/what-kache-actually-caches — Claude Code worktrees/hooks:
code.claude.com/docs/en/worktrees · code.claude.com/docs/en/hooks — Codex/ChatGPT app:
learn.chatgpt.com/docs/environments/git-worktrees · learn.chatgpt.com/docs/permissions ·
learn.chatgpt.com/docs/config-file/config-reference — Kiro CLI v3:
kiro.dev/docs/cli/v3/ · kiro.dev/docs/cli/v3/hooks/ · kiro.dev/docs/cli/v3/permissions/
