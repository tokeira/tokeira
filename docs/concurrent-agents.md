# Concurrent AI agents on one machine

*Working practice for running Claude Code, Codex (ChatGPT app), and Kiro CLI in parallel
against this repository. The non-negotiable rules live in `AGENTS.md` §10; this document
is the mechanics. Statements tagged `[observed]` are local measurements or behaviors of
the installed tool versions — re-verify them on upgrade; everything else follows upstream
documentation.*

## The shape of the practice

Two decisions carry the whole practice. First, **every agent works in its own git
worktree**, so each has its own `target/`, eliminating cross-agent cargo target-directory
build and artifact lock contention — on stable cargo, two builds against one target
directory fully serialize on those locks. Second, **kache runs as a global
`RUSTC_WRAPPER`**, so all those per-worktree target directories are backed by one
content-addressed store: compiled artifacts exist once on disk and are restored into each
`target/` as APFS reflinks.

```
<repo>                          ← main checkout: the human's workspace + integration seat
<repo>/.claude/worktrees/…      ← Claude Code native worktrees (Claude manages these)
$CODEX_HOME/worktrees/…         ← ChatGPT app managed worktrees (app-managed LRU)
<repo>-wt/…                     ← tkw-created worktrees, branch agent/<name> (Kiro CLI, manual)
        │  each worktree has its OWN target/ → no target-dir lock contention
        ▼
   kache store (one per machine) ← one physical copy of every compiled artifact,
                                   shared via reflink, LRU-evicted at the store cap
```

Isolated worktrees remove the *build* coupling between agents. What still couples them:
`$CARGO_HOME`'s brief package-cache lock, CPU/memory/disk/thermal budgets, shared
external services (a live DSQL endpoint, a running `tokeirad`), and git's shared refs
and object database. Bound the number of *simultaneously building* agents for those
reasons; per-process `CARGO_BUILD_JOBS` is available as a knob when the machine is
saturated.

Integration stays serialized: agents produce branches and open pull requests at the
cadence their goal dictates; a single integration controller merges one PR at a time.

## Why this is the right shape (July 2026)

The "obvious" alternative — pointing every agent at one shared `CARGO_TARGET_DIR` — is
precisely wrong. On stable cargo, two builds against the same target directory take
exclusive whole-build locks. The only relaxation so far is Cargo 1.93: *"Avoid
unnecessary artifact directory locking for `check` builds."* Per-unit locking exists only
as nightly `-Zfine-grain-locking` (tracking: rust-lang/cargo#4282). This workspace pins a
toolchain new enough that the check-lock relaxation is already in effect.

Sharing artifacts *across* worktrees is the part cargo cannot do yet: `build.build-dir`
(stable since 1.91) relocates intermediates but does not dedupe them; sccache can share
across worktrees when `SCCACHE_BASEDIRS` normalizes checkout roots, but it retains Rust
linker/incremental limitations and offers no reflink-oriented local artifact
deduplication; cargo's own cross-workspace cache is a 2026 project goal headed for
nightly first. kache is built for exactly this gap: keys are blake3 hashes of normalized
compiler inputs (build-script outputs and extern rlib/rmeta hashes included), so the same
crate compiled in any worktree hits the same store entry, and restores are zero-copy on
APFS.

Sensible check-in for retiring pieces of this setup: early 2027, or whenever a Rust
release post mentions the cargo build cache stabilizing.

## The shared layer: kache

Two directories are deliberately shared machine-wide; everything else is per-worktree.

- **`$CARGO_HOME` (`~/.cargo`)** — the registry/git source caches (self-GC'd by cargo),
  `config.toml` where `kache init` writes the `rustc-wrapper` hook, and the
  `.package-cache` lock every cargo invocation touches for a few milliseconds.
- **The kache store** — location printed by `kache doctor` (under `~/Library/Caches` on
  macOS). It must live on the same APFS volume as the worktrees, or reflinks silently
  degrade to full copies.

Cost profile to expect in a fresh worktree: the first `cargo check` restores the
dependency graph from the store (the sub-minute class against a warm store, versus
minutes cold `[observed]`); the first `cargo test --workspace` additionally recompiles
build scripts and proc-macros and links every binary `[observed v0.10.0]`. Upstream
documentation currently disagrees with itself on whether proc-macro and dylib outputs
are cached by default or gated behind `KACHE_CACHE_EXECUTABLES` — verify against the
installed build with `kache list` / `kache report` rather than assuming either.

Two trade-offs, stated plainly:

- **Incremental compilation is off while kache wraps rustc** (the wrapper strips cargo's
  incremental flags). Agents barely notice — their builds are dominated by dependency and
  cold workspace compilation, which the store accelerates. Note that `KACHE_DISABLED=1`
  bypasses cache reads/writes but still runs through the wrapper and still disables
  incremental; to restore true incremental compilation for a human inner loop, bypass the
  wrapper entirely for that invocation:

  ```bash
  RUSTC_WRAPPER= CARGO_INCREMENTAL=1 cargo check -p <leaf-crate>
  ```

  (an empty `RUSTC_WRAPPER` overrides the config-file wrapper with "none").
- **kache is young** and releases fast. The risk is bounded because the fallback is
  graceful: comment out `rustc-wrapper` in `~/.cargo/config.toml` and everything builds
  normally, unwrapped. The worktree half of this practice — the lock-contention fix —
  does not depend on kache at all.

### Install and verify

```bash
cargo install kache cargo-sweep
kache init      # writes rustc-wrapper into ~/.cargo/config.toml; installs the launchd daemon
kache doctor    # verify wiring; note the store path it reports
```

Knobs `[observed v0.10.0]`: `KACHE_MAX_SIZE` (store LRU cap — set it in the shell
profile; `50G` here), `KACHE_DISABLED`, `KACHE_CACHE_EXECUTABLES`,
`KACHE_VERIFY_RESTORES`, `KACHE_BASE_DIR`, `KACHE_CONFIG`, `KACHE_LOCAL_ONLY`;
`kache gc [--max-age <dur>]` adds age eviction on top of the LRU cap. The daemon is
optional for local-only caching (it serves remote sync/planner features); its checks in
`kache doctor` are informational.

Seeding protocol: the `RUSTC_WRAPPER` wiring must be active for the seed build. Run one
cold `cargo check --workspace`, then **treat `kache stats` as a mandatory smoke test** —
a store with zero entries after a full build means the wrapper or its configuration was
not active for that build; investigate before relying on the cache.

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

- `.claude/settings.json` sets `worktree.baseRef: "fresh"`: worktrees branch from the
  remote default branch, so unpushed local work never leaks into agent branches. Use
  `"head"` only for an explicitly dependent task whose prompt records the parent branch
  or commit.
- Hooks (same file) run `tkw hook post-edit` after each edit (single-file nightly
  rustfmt) and `tkw hook stop` **each time the agent finishes a turn** (`cargo check
  --workspace`; exit 2 blocks until the tree compiles, with loop protection via
  `stop_hook_active`). The per-turn gate is a compile gate, not the release bar — the
  full Enforced Commands run belongs to task completion, before commit and PR.
- Cleanup is Claude's: clean worktrees are removed on exit, dirty ones prompt, and agent
  worktrees are `git worktree lock`ed while running so sweeps can't remove them.
- Rename the branch to the fleet convention before its first push:
  `git branch -m agent/claude/<task-slug>`.

### Codex — ChatGPT app managed worktrees

Codex runs through the ChatGPT app's **Local | Worktree** environment picker, not the
CLI. Day to day:

- **Research / questions** → Local (the main checkout, read-only).
- **Coding task** → Worktree, based on `main` — only valid when local `main` equals
  `origin/main` (see the integration preflight) — *without* copying unstaged local
  changes. The app creates a managed worktree under `$CODEX_HOME/worktrees/` in
  detached-HEAD state; it is a real linked worktree of this repository.
- **Finishing** → run the completion protocol below; **Create branch here** with the
  fleet name (`agent/codex/<task-slug>`), commit via the app's git controls. The app
  retains a bounded LRU of recent worktrees and cleans older ones `[observed]`.

Configuration is split in two, per Codex's project-config rules:

- **Tracked, in this repo** — `.codex/config.toml`: selects the permission profile and
  raises `project_doc_max_bytes` (this repo's `AGENTS.md` exceeds the 32 KiB default,
  which would otherwise be silently truncated).
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

> Smoke-test a new install: run one Worktree chat, have it `cargo check`, confirm the
> profile syntax took (upstream examples use a nested `:workspace_roots` form; the flat
> path keys above are `[observed]` working), then check `kache stats` for hits.

### Kiro CLI 3.0 (early access) — tkw worktrees

Kiro CLI has no managed worktrees, so `tkw` provides them:

```bash
tkw new docs-pass                # <repo>-wt/docs-pass, branch agent/docs-pass, includes copied
cd ../<repo>-wt/docs-pass && kiro-cli --v3
```

- **Context:** Kiro v3 reads `AGENTS.md` natively (always included). `.kiro/specs/`
  drives the spec workflow from the terminal (`/spec`).
- **Hooks:** `.kiro/hooks/rust-quality.json` (tracked, v3 schema) wires the same two
  `tkw hook` commands as Claude. Kiro blocks on exit 2 only for
  `PreToolUse`/`UserPromptSubmit`; on `Stop`, a nonzero exit surfaces as a warning. A
  blocking `{"decision":"block"}` JSON response is documented for Kiro Stop hooks and is
  planned for `tkw hook stop`; until wired, the finish-green bar for Kiro work is
  enforced by `AGENTS.md` §10 discipline plus the integration gate.
- **Permissions:** v3 uses `permissions.yaml`, deliberately stored *outside* the repo
  (user level: `~/.kiro/settings/permissions.yaml`; workspace overrides under
  `~/.kiro/workspace-roots/<hash>/`) so repositories can't inject rules. Every fresh
  worktree hashes to a new workspace root, so per-workspace overrides can't serve the
  fleet — **the user-level file is where fleet rules live**. Effects resolve
  `deny > ask > allow`. The fleet baseline (note bare and wildcard forms both listed —
  a pattern with a trailing ` *` does not match the bare command):

```yaml
  # ── Tokeira concurrent-agents fleet (AGENTS.md §10) ──
  - capability: shell
    effect: deny
    match:
      - cargo clean            # §10: never the fix; defeats the shared build cache
      - cargo clean *
  - capability: shell
    effect: ask
    match:
      - git push               # agents push only at a declared PR boundary
      - git push *
      - git reset --hard *     # §5 revert safety
      - git checkout *
      - git restore *
      - git clean *
      - tkw rm *               # worktree removal is the integration seat's call
      - tkw tidy
  - capability: shell
    effect: allow
    match:
      - cargo check *
      - cargo test *
      - cargo nextest *
      - cargo fmt *
      - cargo +nightly fmt *
      - cargo lint
      - cargo lint *
      - cargo test-lint
      - cargo test-lint *
      - cargo doc *
      - cargo metadata *
      - cargo tree *
      - rustfmt *
      - tkw ls
      - tkw new *
      - tkw hook *
```

  Filesystem read/write capabilities are left to Kiro's per-agent defaults and
  prompts rather than granted globally.

Early-access caveat: v3 surfaces are moving; re-verify hook and permission schemas
against kiro.dev/docs/cli/v3 when upgrading.

## tkw — the fleet tool

`tools/tkw` (workspace member; install once with `cargo install --path tools/tkw`):

| Command | Does |
|---------|------|
| `tkw new <name> [--base <ref>]` | Worktree at `<repo>-wt/<name>`, branch `agent/<name>`, base = `origin/main` (falls back to `HEAD` without a remote); copies `.worktreeinclude` matches. `--base` declares a dependent task's parent |
| `tkw ls` | Every worktree of the repo, annotated `[main\|tkw\|claude\|codex\|manual]` |
| `tkw rm <name> [--force]` | Remove a tkw-owned worktree (branch kept) |
| `tkw clean [--base <ref>]` | Remove tkw worktrees whose branch is fully merged |
| `tkw tidy` | Periodic hygiene: clean + prune + `cargo sweep` + machete report + `kache gc` |
| `tkw hook post-edit\|stop` | The hook bodies Claude/Kiro configs invoke |

The ownership rule, enforced in code: tkw only ever **removes** worktrees it created.
Claude's and the app's worktrees have their own lifecycles; `tkw tidy` will sweep stale
build artifacts *inside* them (artifact deletion, never worktree deletion) and `tkw ls`
shows them, but removal is refused. Overrides: `TKW_DIR` (worktree location — keep it on
the same APFS volume as the store) and `CODEX_HOME` (classification only).

## Completion and integration

Every unit of work lives on a named task branch in its own worktree. **A pull request is
not an automatic per-task boundary: the goal statement given to the agent dictates the PR
cadence** (per-slice, per-milestone, one PR for a whole correction set). Until a PR
boundary is reached, work accumulates as commits on the task branch. Merges use **merge
commits**, so branch ancestry survives integration and ancestry-based cleanup
(`tkw clean`, `git branch -d`) remains valid.

### Preflight (before starting an agent batch)

The main checkout is the integration seat. Before dispatching agents:

```bash
git fetch --prune origin
test -z "$(git status --porcelain)" || echo "main checkout is not clean"
test "$(git rev-parse main)" = "$(git rev-parse origin/main)" || echo "local main != origin/main"
```

Worktrees base on `origin/main` (Claude `baseRef: "fresh"`, `tkw new`'s default, the
app's `main` picker once the preflight holds), so agent branches never inherit unpushed
local state.

### Agent completion protocol

All steps run inside the agent's own worktree.

1. **Named branch.** Attach detached HEADs and rename default-named branches to
   `agent/<provider>/<task-slug>` before the first push. Never finish agent work on
   `main`.
2. **Final validation.** The full `AGENTS.md` Enforced Commands bar — not just the
   per-turn compile gate. No PR is ready merely because `cargo check` passed.
3. **Commit all intended work.** Inspect `git status --short` and the staged diff before
   committing; all task changes committed, no ignored/machine-local files forced in, and
   a clean worktree afterwards.
4. **Rebase once, before the first push**: `git fetch --prune && git rebase origin/main`,
   re-run the final validation if the rebase changed anything. Never merge `main` into
   the task branch; never incorporate another agent's unmerged branch unless the task
   declares that dependency (a declared-dependent task bases on the parent via `--base`
   and waits for the parent to merge).
5. **Push the branch** (`git push --set-upstream origin <branch>`). After the PR exists,
   CI fixes are additional commits — no autonomous force-pushes; a history rewrite needs
   explicit approval and `--force-with-lease`.
6. **Open the PR at the dictated boundary** (`gh pr create --base main`), with a body
   covering summary, validation commands run, base and head SHAs, and dependency/lockfile
   notes.
7. **Report and stop**: branch, head SHA, PR URL, commands passed, known risks. The agent
   leaves worktree and branch intact and never merges.

### Integration controller

One controller (the human, from the main checkout) processes PRs **one at a time**:

1. Inspect: correct base, expected files only, dependency/lockfile changes explained.
2. Bring the branch up to date with `origin/main` and let the required checks run; on
   conflict, hand back to the agent or resolve in the agent's worktree.
3. Merge server-side with a merge commit, pinning the reviewed head
   (`gh pr merge --merge --match-head-commit <sha>`).
4. Fast-forward the integration seat: `git pull --ff-only`. Then the next PR.

Branches merge server-side — there is no need to check an agent branch out in the main
checkout. After a verified merge, remove the worktree through its owner (`tkw rm`, or
the managing product) and let ancestry-based cleanup collect the branch.

Two git settings worth having in the main checkout:

```bash
git config rerere.enabled true        # replays conflict resolutions you've made before
git config worktree.guessRemote true
```

Lockfile conflicts (two branches that each changed dependencies — which `AGENTS.md` §10
forbids without a task saying so): keep both sides' `Cargo.toml`, delete the conflicted
`Cargo.lock` hunks, `cargo check`, commit the regenerated lockfile.

## Disk hygiene

Steady-state disk = the main checkout's `target/` + one thin reflink-backed `target/`
per live worktree + the LRU-capped store. `tkw tidy` weekly (or via launchd) keeps the
edges trimmed; it prints `kache stats` and a `df` so the reclaim is visible. Deleting any
`target/` is always safe: worst case, the next build restores from the store.

## Repo-specific notes

- **Linker:** the tracked `.cargo/config.toml` links with lld (`-fuse-ld=lld` via clang)
  on aarch64 and sets `debug = "line-tables-only"` — deliberate link-time tuning that
  every worktree inherits. kache skips linked outputs, so link time is unaffected by the
  cache either way; keep or benchmark lld independently of this practice.
- **Conformance runs:** `run_suite.sh` launches `target/debug/tokeirad` from whichever
  checkout runs it — per-worktree targets mean parallel suites don't collide on the
  binary, but they do share the DSQL/storage layer a suite points at; keep live-suite
  runs to one at a time unless configs isolate storage.
- **Zed / rust-analyzer:** the tracked `.zed/settings.json` sets rust-analyzer's
  `cargo.targetDir: true`, giving RA its own `target/rust-analyzer` so save-triggered
  checks never serialize against terminal or agent builds in the same checkout — and
  since it's tracked, every worktree opened in Zed inherits it. The extra artifacts
  dedupe through the kache store like any other target dir. If RA's per-save check needs
  true incremental back, set `RUSTC_WRAPPER` to the empty string plus
  `CARGO_INCREMENTAL=1` in RA's `cargo.extraEnv` / `check.extraEnv` — bypassing the
  wrapper, not merely the cache.

## Sources

Cargo locking/changelog: doc.rust-lang.org/cargo/CHANGELOG.html (1.91 build-dir, 1.93
check-lock) · rust-lang/cargo#4282 · cross-workspace cache goal:
rust-lang.github.io/rust-project-goals/2026/cargo-cross-workspace-cache.html — git
worktrees (per-worktree HEAD/index): git-scm.com/docs/git-worktree — sccache:
github.com/mozilla/sccache/blob/main/docs/Rust.md — kache: github.com/kunobi-ninja/kache
· kunobi.ninja/docs/kache — Claude Code worktrees/hooks: code.claude.com/docs/en/worktrees
· code.claude.com/docs/en/hooks — Codex/ChatGPT app:
learn.chatgpt.com/docs/environments/git-worktrees · learn.chatgpt.com/docs/permissions ·
learn.chatgpt.com/docs/config-file/config-reference — Kiro CLI v3:
kiro.dev/docs/cli/v3/ · kiro.dev/docs/cli/v3/hooks/ · kiro.dev/docs/cli/v3/permissions/
— GitHub CLI merge guards: cli.github.com/manual/gh_pr_merge
