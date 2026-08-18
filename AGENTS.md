# AGENTS.md — Tokeira

## Mission

Build a Temporal-compatible durable execution engine in Rust, specialized for Aurora DSQL. Preserve the public Temporal contract that SDKs, operators, and tooling depend on. Collapse internal correctness around a single authoritative per-run transition log.

This is a product-from-scratch. The architecture is informed by Temporal but the implementation is original. Do not port Temporal code.

### Compatibility Target

- **Temporal server compatibility: v1.31.0.** This is the release whose public API *behaviour* Tokeira claims to match. It is the authority for every API-behaviour question (see §8). Pinned as `TEMPORAL_SERVER_COMPAT` in `crates/tokeira-build-info/src/pinned.rs`.
- **Temporal API: v1.62.11.** This is the vendored proto surface Tokeira builds against (`proto/upstream/`, mirrored by `proto/UPSTREAM_VERSION`). Pinned as `TEMPORAL_PROTO_VERSION`.
- These pins are independent and tracked ahead on purpose: the vendored API `v1.62.11` is newer than the API version Temporal server `1.31.0` ships (`v1.62.8`). RPCs present only in `v1.62.11` (e.g. Nexus operation execution) are **not** part of the `1.31.0` behavioural claim and are tracked separately in the api-conformance tracker. Do not bump the server compatibility claim just because the vendored proto version moved.

---

## How to read this file

- The numbered rules **§1–§12** are binding. Cite them as `AGENTS.md §8` (or `root §8`
  from a crate). Numbers are **stable**: new rules append; existing rules are never
  renumbered — code comments, crate docs, and out-of-repo agent configs cite them.
- **Precedence:** crate-local `AGENTS.md` (strictest; table under *Doing the work*) →
  this file → `docs/`. The stricter rule wins; a genuine contradiction is a defect —
  report it rather than picking silently.
- **Contracts here, mechanics elsewhere.** Fleet/worktree/kache mechanics:
  [concurrent-agents.md](docs/agents/concurrent-agents.md). Codex operator guide:
  [codex-chatgpt-worktrees.md](docs/agents/codex-chatgpt-worktrees.md). Task-specific
  reference contracts and recipes (equally binding):
  [engineering-reference.md](docs/agents/engineering-reference.md). Design:
  [docs/architecture/000-overview.md](docs/architecture/000-overview.md), `docs/adr/`.
- **Harness reality:** Kiro reads this file natively; Claude Code only via the root
  `CLAUDE.md` `@AGENTS.md` import (§12.1); Codex concatenates global → root → cwd
  `AGENTS.md` under `project_doc_max_bytes` and silently truncates overflow — the
  tracked `.codex/config.toml` raises that cap to 128 KiB.
- **Budget (CI-enforced ≤ 32 KiB):** the binding constraint is context cost — every
  byte here is paid by every agent every session. Cut before you append. Never commit
  an `AGENTS.override.md` — Codex silently prefers it over this file.

## Values

DSQL is the design centre, not a pluggable afterthought. Never trade DSQL-layer
correctness or elegance for speed of conformance progress.

In tiebreak order: 1. **Correctness over speed** — a slow transition that commits
correctly beats a fast one that corrupts state. 2. **Explicitness over magic** — every
resource, permission, config field visible in code. 3. **Operator empathy** — errors
say what happened, why, what to do next. 4. **Minimal surface** — every dependency,
abstraction, and config option must earn its place (§1).

## Architecture

Three planes — details in
[docs/architecture/000-overview.md](docs/architecture/000-overview.md), decisions in
`docs/adr/`:

- **Compatibility edge** (`tokeira-edge`, `tokeira-proto`, `tokeira-types`) — admits and
  translates requests. Owns no workflow semantics.
- **Authoritative runtime and storage** (`tokeira-kernel`, `tokeira-chasm*`,
  `tokeira-runtime`, `tokeira-storage`) — owns correctness: shard/bundle ownership,
  lane-local execution, durable transitions, derived dispatch.
- **Projection plane** (`tokeira-projection`) — owns read models: visibility, rollups,
  custom sinks. Outside the correctness path.

Per-crate boundary contracts (state stores, provider-agnosticism, config ownership):
[engineering-reference.md](docs/agents/engineering-reference.md).

## The Rules

### §1. Rust standards

- Edition 2024; stable toolchain pinned to 1.97.1 (`rust-toolchain.toml`). Formatting uses
  nightly-only options: run `cargo +nightly fmt --all` — don't check first. (CI pins a
  dated nightly for fmt — advance together.)
- **The lint wall is compiler-enforced** in `[workspace.lints]` — prose rules drift;
  lints survive refactors. Binding today: `unsafe_code = deny` (exactly four carve-outs,
  each `#[allow]` + `// SAFETY:`), `undocumented_unsafe_blocks = deny`,
  `unwrap_used = deny` (tests exempt via `clippy.toml`), `print_stdout`/`print_stderr =
  deny` (use `tracing`; CLI/bench/probe crates carry documented allows),
  `missing_debug_implementations = deny`. `expect` with an invariant message is the
  sanctioned unwrap remedy — `expect_used` deliberately not adopted. `missing_docs` is a
  staged campaign (~9,000 sites), not yet a flip.
- `cargo lint` passes with zero warnings; no suppression without a comment saying why.
- `thiserror` in library crates, `anyhow` in binaries. Serializable types derive
  `Serialize, Deserialize`.
- No runtime reflection. The typed extension bags in `ProvisionContext` /
  `ModuleContext` / `ServiceContext` / `ImageContext`
  (`HashMap<TypeId, Box<dyn Any + Send + Sync>>` behind `extension::<T>()`) are the one
  sanctioned exception — `tokeira-iac`/`tokeira-deploy-engine` cannot depend on the
  platform crates that register handles into them. New contexts SHALL NOT introduce
  additional `Box<dyn Any>`.
- Prefer `&str` over `String` in signatures where ownership isn't needed. `use` at
  module top, never in function scope.
- No explicit sleeps in tests — synchronize (channels, `tokio::sync::Notify`, condition
  variables).
- Every dependency must earn its place; adding one is architectural (see *Change
  classification*).
- Rust compilation takes time. Don't interrupt builds or tests under 5 minutes.

### §2. The kernel stays pure

`tokeira-kernel` is a deterministic state machine. No I/O, no async, no storage, no
metrics, no network. If a change would add any of these to the kernel, it belongs in
`tokeira-runtime`, `tokeira-storage`, or `tokeira-edge` instead.

### §3. History is authority

Every state-changing request becomes a per-run transition. Dispatch and projection are
derived effects. If a design puts correctness weight on a queue write or a visibility
update, the design is wrong.

### §4. Review before action

The CLI follows `plan → confirm → apply`; silent mutations are a bug. `tkr infra plan`
before `tkr infra apply`; `tkr deploy plan` for service manifests. Destructive
operations (`infra destroy`, `deployment destroy`, `scale down`) require `--yes` or
interactive confirmation.

### §5. Revert safety (worktree integrity)

A working tree may hold unstaged edits representing hours of in-flight work. Restoring
files from the index or HEAD destroys that work irreversibly.

- NEVER run `git checkout`, `git checkout-index`, `git restore`, `git reset --hard`,
  `git clean -f`, or any equivalent revert without explicit user approval of the exact
  command.
- "Undo your changes" means: produce a reverse patch (`git diff | patch -R`) covering
  ONLY the hunks you introduced. Do not restore files from the index.
- Before any revert, run `git status` and `git diff` and confirm whose changes are in
  the tree. If you did not snapshot the pre-edit content yourself, stop and ask.
- Treat all unstaged changes as user work unless proven otherwise.

### §6. Spec editing safety

Files under `.kiro/specs/**` are edited only on explicit user instruction. Before
editing, snapshot the pre-edit state and report the path:

```bash
git diff -- .kiro/specs > /tmp/spec-before-$(date +%Y%m%d-%H%M%S).patch
```

If asked to undo, reverse only the assistant-authored hunks (§5). If the tree holds
uncommitted spec edits and the instruction is broad ("undo your changes"), clarify
before touching anything — never assume it means restore-from-index.

### §7. Commit messages via `-F` file (Kiro-specific)

Kiro's embedded terminal silently truncates long single-line `git commit -m` invocations
AND heredocs containing backticks — the commit fails to parse or records a short prefix.
Always commit from a message file: author it with `fsWrite` (not a terminal heredoc)
under the workspace root, e.g. `artifacts/commit-msg.txt` (`/tmp/` is outside the
`fsWrite` sandbox); then `git commit -F artifacts/commit-msg.txt`; then remove the file.

`-m "short"` is acceptable only for one-liners under ~60 characters with no backticks or
angle brackets. Never fake multi-line via `\n` escapes and never heredoc the message —
both route through the same truncating buffer. The `-F` file is also where the §11
trailers are authored.

### §8. Temporal behaviour defers to the targeted release

Pins: *Compatibility Target* (head of this file); constants in
`crates/tokeira-build-info/src/pinned.rs`.

For any question about public API **behaviour** — field semantics, error/status mapping,
defaulting, lifecycle ordering, inheritance rules — the contract is **whatever the
targeted release does**, verified against ground truth in this order:

1. **Vendored protos in `proto/upstream/`** for wire shape: messages, field numbers,
   enums, oneofs. NEVER read generated artifacts under `target/` — they can be stale;
   `proto/upstream/` is the source of truth.
2. **Temporal server source at the matching tag** for behaviour the proto does not
   specify. **Read the local reference checkout** — a sibling of the **main** checkout;
   `../temporal` is correct from the repo root only, so resolve it from any worktree:

   ```bash
   TEMPORAL="$(git rev-parse --path-format=absolute --git-common-dir)/../../temporal"
   git -C "$TEMPORAL" show v1.31.0:<path>   # grep: git -C "$TEMPORAL" grep <pattern> v1.31.0
   ```

   Offline, pinned, grep-able. Do NOT
   web-search for Temporal source when the local checkout is available. Read the actual
   code (`service/history/...`, `service/worker/...`, `common/...`) — never infer
   behaviour from proto doc comments, SDK docs, blog posts, or memory. Cite by
   repo-relative path + tag (e.g. `service/frontend/workflow_handler.go @ v1.31.0`);
   never hardcode an absolute developer-machine path in committed specs, code, or docs.

Rules:

- Resolve behaviour questions against the targeted release **before** writing or
  amending a spec. A spec that contradicts the targeted release is wrong; fix the spec.
- Distinct from "do not port Temporal code": **reading** Temporal source is required;
  **copying** its implementation is forbidden.
- Where a Tokeira mechanism has no exact Temporal analog (e.g. history replay
  reconstruction), the correctness test is: *does Tokeira's response match what the
  targeted release would return for the same execution lineage?*
- Cite the verifying source in the spec/PR when a behaviour decision is non-obvious, so
  reviewers confirm against the same ground truth.

### §9. Code documentation

Tokeira is a correctness-critical engine mirroring an external contract. The *reasoning*
must survive in the source — the next reader (human or agent) cannot re-derive a
concurrency invariant or a ground-truthed behaviour decision from the code alone.
Comments are part of the deliverable; a change that adds non-obvious logic without
explaining why it is correct is incomplete.

**The WHY-not-WHAT rule.** A comment must add information the code cannot. Restating a
signature, a name, or control flow is noise — worse than none: it rots and trains
readers to skip comments. Delete such comments when you see them.

```rust
// BAD — restates the code:
// increment the revision number
// GOOD — explains why this is safe/necessary:
// Bump the revision so any task dispatched against the prior routing decision
// is fenced as stale at start time (recordworkflowtaskstarted/api.go @ v1.31.0).
info.revision_number += 1;
```

**MUST be documented:** every module (`//!`: what it owns, where it sits, its
invariants — the first screen tells a cold reader purpose and contract); every public
item (`///`: guarantees, caller assumptions, non-obvious failure/edge behaviour — "pub"
means someone depends on it); correctness-critical decisions (inline WHY: concurrency
hazards and the invariant making the code race-free — lock ordering, TOCTOU windows,
why an operation serializes; ordering/idempotency assumptions; CAS/OCC and fencing
semantics; why a value is computed live vs stored; anything a future editor could
"simplify" into a bug); ground-truthed behaviour (cite proto path or server path + tag
inline, per §8 — never invent an anchor); deliberate deviations and tradeoffs (stated
explicitly so they are not mistaken for oversights and silently "fixed").

**Must NOT be documented:** anything obvious from the code — no narrated control flow,
no paraphrased next line, no ceremonial headers. Test scaffolding stays uncommented;
property tests carry a one-line invariant statement (and a `// Feature: <name>,
Property N` tag where a spec defines one).

Enforced like any other standard: missing module docs, undocumented public items, and
uncommented non-obvious logic are defects to fix before the change is complete — the
same weight as a failing lint.

### §10. Concurrent agents — fleet discipline and the git protocol

Several agents and one human work this repository simultaneously. These rules are the
contract; mechanics live in [concurrent-agents.md](docs/agents/concurrent-agents.md).
Every task follows one lifecycle:

> **worktree + branch → work → finish green → rebase (or recommend) → push + PR →
> human approval, serial merge → cleanup**

#### §10.1 Fleet model

- **One agent, one worktree, one branch, one task.** Work only inside your own worktree.
  Never run git or cargo against a checkout you don't own — including the main checkout
  at the repo root, which is the human's integration seat.
- Worktree homes: `.claude/worktrees/…` (Claude, native), `$CODEX_HOME/worktrees/…`
  (ChatGPT app, managed), `<repo>-wt/…` (`tkw new` — Kiro CLI and manual).
  `.worktreeinclude` (tracked) lists the gitignored files copied into new worktrees.
- Shared machine-wide, therefore off-limits: the git object DB and refs, `~/.cargo`, and
  the kache store (the kache `RUSTC_WRAPPER` dedupes per-worktree `target/` into one
  content-addressed store). **Never
  `cargo clean`** — it defeats the shared cache and is never the fix. Never delete or
  move `target/`; never modify `~/.cargo/config.toml`, rustup toolchains,
  `RUSTC_WRAPPER`, or `KACHE_*`/`CARGO_*` configuration. If the build seems
  inconsistent, stop and report.

#### §10.2 Start: worktree + branch

- Branch naming: **`agent/<provider>/<task-slug>`** — `agent/claude/…`, `agent/codex/…`,
  `agent/kiro/…`. Rename harness-default names (Claude's `worktree-*`, tkw's
  `agent/<name>`, Codex's detached HEAD at **Create branch here**) to the convention
  before the first push. Never finish agent work on `main`.
- Base every task worktree on **`origin/main`** via the harness's fresh mechanism
  (Claude `worktree.baseRef: "fresh"`; `tkw new` default; the app's `main` picker after
  the operator preflight). Never copy unstaged main-checkout state into a task worktree.
- Building on another agent's unmerged branch is allowed **only** when the task declares
  that dependency: base on the parent (`tkw new --base <ref>`) and wait for the parent
  to merge first.

#### §10.3 Work discipline

- Stay within the crate(s) the task names. No drive-by edits to other crates, shared
  configs, CI files, or the workspace `Cargo.toml` unless the task says so.
- **Dependencies are single-writer.** No add/remove/upgrade unless the task explicitly
  calls for it — assume another agent holds the lockfile this window. Otherwise build
  `--locked` so `Cargo.lock` can never be rewritten by accident.
- Commit only your own coherent work, on your own branch (Kiro: via `-F`, §7), with §11
  trailers. If you cannot finish, leave the worktree dirty and report what remains — do
  not half-commit.
- Never read or commit secrets (`.env*`, keys, tokens) — fleet-wide, not just where
  harness deny rules enforce it.

#### §10.4 Finish green — the Enforced Commands bar

Run before any push or PR. The per-turn hook (`tkw hook stop` → `cargo check
--workspace`) is a compile gate, not this bar.

```bash
cargo +nightly fmt --all                                  # CI verifies with --check
cargo lint --locked                                       # clippy: workspace + all targets
cargo check --workspace --locked
cargo nextest run --workspace --locked                    # tests; see note
cargo test --workspace --doc --locked                     # doctests
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

- `cargo lint` builds test targets; `cargo check` alone does not.
- Tests run under nextest **by contract** — one process per test, so cross-test races on
  process-global state cannot exist (tracing-span-lifecycle-hygiene spec, Req 5). Plain
  `cargo test` is inner-loop only.
- CI (`.github/workflows/ci.yml`) additionally enforces: `cargo-deny`
  bans/licenses/sources (merge-gating; the advisories job is advisory-only, re-run
  weekly), a lychee offline link check over every `*.md` including `.kiro/` (broken
  relative links redden CI), and `git diff --exit-code` (builds must not dirty the
  tree). Everything runs `--locked`: dependency movement is a reviewed change, never a
  CI side effect.

#### §10.5 Rebase — or recommend

- At the PR boundary, rebase **once**: `git fetch --prune && git rebase origin/main`.
  If the rebase changed anything, re-run the bar.
- Conflicts within your task's scope: resolve and continue. Conflicts that are semantic
  — overlapping another agent's slice, or code your task doesn't own — **stop before
  pushing**: leave the branch pre-rebase and report a *recommendation* instead
  (conflicting paths, the overlapping branch/PR, suggested merge order, whether a
  hand-back is needed). The integration seat decides.
- Never merge `main` into a task branch. After a PR exists, updates are additional
  commits — no history rewrites without explicit approval, and then only
  `--force-with-lease`.

#### §10.6 Push + PR

- Push at the boundary the goal statement dictates — per-slice by default; a goal may
  batch slices into one PR. `git push --set-upstream origin agent/<provider>/<slug>`,
  then `gh pr create --base main`.
- PR body: what/why summary; validation commands actually run (name anything skipped,
  and why); base and head SHAs; dependency/lockfile notes; known risks. GitHub renders
  co-authors from the §11 trailers.
- Networked git and GitHub mutations use each harness's approval/escalation gate (§12).

#### §10.7 Approval + serial merge

- The human integration seat processes PRs **one at a time**: inspect, await green
  checks, and explicitly approve the exact head. On approval, the owning agent merges it
  with `gh pr merge --merge --match-head-commit <sha>` and verifies the result. The
  operator then runs `git pull --ff-only` in main. Merge commits preserve ancestry for
  cleanup.
- Agents never approve their own work. Agents merge on explicit human approval of the
  exact head. They resolve another branch's conflicts only when handed that task. Review
  feedback returns to the owning agent as commits on the same branch.

#### §10.8 Cleanup

Only after the merge is verified:

- Worktrees are removed **by their owner**: `tkw rm <name>` for tkw worktrees (tkw
  refuses to remove worktrees it didn't create), Claude removes its own on exit, the
  ChatGPT app LRU-cleans its managed ones. Never remove another agent's worktree.
- Branches are collected by ancestry once merged: `tkw clean`, or `git branch -d` —
  never `-D` an unmerged branch.
- `tkw tidy` periodically sweeps the edges: merged-worktree cleanup, prune,
  `cargo sweep`, machete report, `kache gc`.

### §11. Commit attribution — recognise agent work (required)

Kiro, Claude Code, and Codex do a large share of the daily work in this repo, and that
contribution is recognised **in the history** — never flattened into a lone human
author. The human operator stays the git `author`; the agents are credited with
**required** commit trailers:

- **`Co-authored-by: <Agent> <email>`** — one line for **every agent that authored**
  part of the change (code, docs, tests, or specs). GitHub renders these as co-authors
  on the commit and PR.
- **`Assisted-by: <Agent> <email>`** — one line for **every agent that assisted**
  without primary authorship: review, verification, ground-truthing, or pairing.

Both trailers are required whenever their role applies. Credit every agent that took
part — generously, not grudgingly. A commit with genuine agent involvement and no
attribution trailer is an incomplete change, the same as a missing test or a failing
lint.

Canonical identities (use exactly these; if an address changes, update it here so all
three agents stay consistent):

- `Kiro <kiro@kiro.dev>`
- `Claude <noreply@anthropic.com>`
- `Codex <codex@openai.com>`

Trailers go at the end of the message, after a blank line — which is why the `-F`
message file is authored to end with them. Example message file:

```text
feat(placement): fence slot leases on the monotonic token

<why-this-is-correct body>

Co-authored-by: Kiro <kiro@kiro.dev>
Assisted-by: Claude <noreply@anthropic.com>
```

### §12. Per-agent direction

What differs per harness. Everything in §10 applies to all three agents.

#### §12.1 Claude Code

- Session context arrives via the root `CLAUDE.md` (`@AGENTS.md` import) — Claude Code
  does not read `AGENTS.md` natively. Never remove the shim.
- Worktrees are native: `.claude/worktrees/<name>`, with `worktree.baseRef: "fresh"`
  (`.claude/settings.json`) so branches start from `origin/main`. Rename `worktree-*` →
  `agent/claude/<slug>` before the first push.
- Hooks: `tkw hook post-edit` after each edit (single-file nightly rustfmt);
  `tkw hook stop` at each turn end (`cargo check --workspace`, blocking). These are
  compile gates — §10.4 is the completion bar.
- Permission gates are policy, not friction: push/rebase/checkout/reset ask first;
  force-push, `git clean`, `branch -D`, `reset --hard` are denied. Work with the gates,
  never around them.
- The §8 reference checkout is `../temporal` only from the main checkout — in a
  worktree, resolve the path per §8. Ground-truth reads never modify it, and your
  harness denies edits there; the clone doubles as the conformance fork, whose branch
  is updated only in dedicated functional-conformance work (see Verification).
- The full lifecycle is yours, including `git push` and `gh pr create` (§10.5–§10.6).

#### §12.2 Codex (ChatGPT app)

- You run inside an app-managed linked worktree of this repository
  (`$CODEX_HOME/worktrees/…`, detached HEAD, own `target/`). Work only there. **Local**
  (non-worktree) chats are research-only: read and answer; never edit from Local.
- **Your sandbox restricts network by default.** Networked git and `gh` mutations
  require harness escalation and explicit operator authorization. Build from the warm
  registry with `--locked`; no dependency changes unless requested; never configure
  around kache (§10.1).
- **Your finishing move:** run the §10.4 bar; report omissions; present the diff; create
  `agent/codex/<task-slug>` via **Create branch here**; commit with §11 trailers. When
  authorized, complete rebase, push, PR, approved merge, and cleanup (§10.5–§10.8).
- If `main` advanced, report both SHAs and rebase once onto `origin/main`. Resolve only
  in-scope conflicts; return semantic or out-of-scope conflicts to the integration seat.
- Final report: branch, head SHA, bar results, files touched, known risks, merge-order
  recommendation when relevant.
- Operator guide: [codex-chatgpt-worktrees.md](docs/agents/codex-chatgpt-worktrees.md).

#### §12.3 Kiro CLI

- Worktrees via tkw: `tkw new <slug>` → `<repo>-wt/<slug>` on branch `agent/<slug>`;
  rename to `agent/kiro/<slug>` before the first push (§10.2).
- Spec-driven: `.kiro/specs/` + `/spec`, house style in
  `.agents/skills/kiro-spec-driven-development/` (EARS, property-based testing,
  ground-truth verification; auto-discovered via the `.kiro/skills/` symlink).
- Commits via the `-F` message file — §7 is non-negotiable in Kiro's terminal.
- Hooks (`.kiro/hooks/rust-quality.json`) are advisory on Stop — running the §10.4 bar
  before declaring done is on you.
- Permissions are user-level (`~/.kiro/settings/permissions.yaml`), deliberately outside
  the repo; push and worktree removal are ask-gated there.

## Doing the work

### Decision process

0. **API-behaviour questions → the targeted release first** (§8): `proto/upstream/` for
   shape, server source at the pinned tag for behaviour. This tier sits above the spec — a
   spec that contradicts the targeted release is the thing that's wrong.
1. **Check the spec.** `.kiro/specs/` is the source of truth for what to build. When
   authoring or amending one, follow the house-style skill
   `.agents/skills/kiro-spec-driven-development/` — auto-discovered by Kiro and
   Claude; Codex (no skills mechanism) opens its `SKILL.md` explicitly.
2. **Check existing patterns** before inventing a new approach.
3. **Prefer boring solutions.** The simplest approach that satisfies the requirement.
4. **Ask if unsure.** Surface architectural implications rather than guessing.

### Crate-local AGENTS.md (read before editing a high-risk crate)

Binding refinements; the stricter crate-local rule wins. Never rely on automatic
nested loading — open explicitly. Applies to every agent.

| Crate | Concentrates |
|-------|--------------|
| `crates/tokeira-kernel/AGENTS.md` | Purity: no I/O/async/storage/metrics, no side-effecting commands, no non-determinism. |
| `crates/tokeira-storage/AGENTS.md` | DSQL migrations (forward-only / build-phase no-ALTER / DDL subset) and the `max_idle_conns == max_conns` invariant. |
| `crates/tokeira-runtime/AGENTS.md` | History is authority; queues are disposable; durable state via fenced `commit_transition`. |
| `crates/tokeira-edge/AGENTS.md` | Thin translation only; public-API behaviour ground-truthed to the targeted release (§8). |
| `crates/tokeira-state/AGENTS.md` | CAS-not-force-overwrite; immutable snapshots; tolerate a missing store on load. |

### Change classification

| Change Type | Examples | Required |
|-------------|----------|----------|
| **Trivial** | Typo fix, doc comment, local rename | Tests pass |
| **Standard** | New resource, service, CLI command | Tests pass + follows existing patterns |
| **Architectural** | New crate, new dependency, state-format change | Spec update or explicit approval |
| **Destructive** | Remove crate, config-schema break, state-compat break | Spec update AND explicit approval |

## Verification

**Inner loop** (§10.4 is the finishing bar, not the per-edit loop): `cargo check -p` /
`cargo clippy -p <crate> --all-targets` for seconds-fast leaf feedback; `cargo nextest
run -p <crate> -E 'test(<name>)'` isolates one test per process and kills hangs at
180 s (`.config/nextest.toml`); doctests still need `cargo test --doc`.

### Tests

- Unit tests co-located per module (`#[cfg(test)]`); property-based tests with
  `proptest` for config validation, serialization round-trips, dependency ordering.
- `cargo test --workspace` passes before every commit. The default suite requires no
  live AWS credentials and no Docker.
- Some tests panic intentionally; only harness-reported failures are real problems.
- Standing properties: config TOML round-trips losslessly; unknown fields rejected;
  dependency graphs are DAGs; state CAS admits one of two concurrent same-version saves.

### Functional conformance harness (Tier 2)

Behavioural conformance is validated separately from `cargo test` by replaying
Temporal's functional Go corpus (pinned at `TEMPORAL_SERVER_COMPAT`) over the real gRPC
wire against a running `tokeirad`. Operator-invoked from the sibling fork's branch
`tokeira/conformance-v1.31.0` — never assume it runs under `cargo test`. The runbook (build `tokeirad`, run the full corpus or one suite, distil
outcomes) and the conventions binding any fix derived from a run (v1.31.0 ground truth,
no kernel additions, config-as-constant, feature modes as independent runs, raise
ambiguity):
[docs/testing/functional-conformance-harness.md](docs/testing/functional-conformance-harness.md).

Contract highlights: a run never excludes a test — out-of-scope cases are skipped **by
name** in the fork's skip registry (`tests/testcore/tokeira_conformance_skip.go`), each
with a cited reason and a classified `skip` outcome, never by editing a corpus test
body. Tests needing non-default dynamic config are not blanket skips: the override
bridge delivers wired keys to a `--features conformance` `tokeirad`
(`.kiro/specs/conformance-config-override/`); only unwired, kernel-excluded, or
not-enforced keys fall back to the registry.

Campaign order and per-tier ledger:
[docs/readiness/functional-test-order.md](docs/readiness/functional-test-order.md) ·
[docs/readiness/conformance.md](docs/readiness/conformance.md).

## Reference

### Workspace map

The workspace `Cargo.toml` member list is the authority; this is orientation
(`tokeira-` prefixes elided):

```
apps/       tokeirad (server) · tkr (operator CLI) · controller · autoscaler · bench
crates/     engine   types · proto · kernel · chasm{,-derive,-activity} · storage ·
                     runtime · projection · edge · observability · auth
            compat   build-info · compatibility{,-proto,-service} ·
                     conformance{,-proto,-control}
            deploy   state · iac · deploy-engine · config · orchestrator ·
                     platform-definition · k8s · aws · compose · build ·
                     deployment · tkp · autoscaler ·
                     controller · remote-workstation · dagger-client
platforms/  local · compose · ecs · eks
tools/      tkw (fleet worktrees) · proto-sync · simulation (excluded)
proto/      upstream/ — vendored Temporal protos (authoritative wire shape, §8)
.kiro/specs/  feature specs        spec/  TLA+/refinement stack
scenarios/  e2e samples (excluded)
docs/       agents · adr · architecture · conformance · operations · platforms ·
            readiness · testing · crates · diagrams
```

### Working agreements

Package boundaries, configuration contracts, IaC engine contracts, recipes
(platform / IaC-module / CLI-command / image), and observability pins:
[engineering-reference.md](docs/agents/engineering-reference.md) — equally binding,
loaded when needed. Two agreements stay here because other files cite them by name:

#### Temporal compatibility changes

Pins: see *Compatibility Target*. New WorkflowService/OperatorService surfaces are
classified in `FEATURE_MATRIX` (`crates/tokeira-compatibility/src/matrix.rs`); SDK
claims update `SDK_MATRIX` (`crates/tokeira-compatibility/src/sdk.rs`) with evidence and
verification state. Tokeira-owned compatibility metadata uses Buffa/connect-rust under
`proto/tokeira/compatibility/v1/` — never add Tokeira extension fields to upstream
Temporal protos. Run `tkr ci check` once the Dagger compatibility module is available;
until then, the focused matrix/CLI/edge tests in `.kiro/specs/temporal-compatibility/`.

#### Adding or Changing a DSQL Migration

Canonical rules: [crates/tokeira-storage/AGENTS.md](crates/tokeira-storage/AGENTS.md)
(build-phase no-`ALTER`, forward-only after baseline, DSQL DDL subset, contiguous
versions). The heading stays here because other files cite it by name; the crate file
carries the full agreement, including the baseline-cut signal.

### Pointers

- `.kiro/specs/*/` — feature specs.
- [deployment-definitions.md](docs/provisioning/deployment-definitions.md) — programming
  `.tkd` deployment definitions and understanding the `tkp` interpretation contract.
- Temporal ground truth (§8): `proto/upstream/` (API `v1.62.11`) and the server source
  at tag `v1.31.0` — the local reference checkout (sibling of the main checkout, §8), or
  [github.com/temporalio/temporal @ v1.31.0](https://github.com/temporalio/temporal/tree/v1.31.0).
