# Codex in the ChatGPT app

Direction for running Codex coding tasks against this repository through the ChatGPT
app's **Worktree** environments. The agent-facing contract is
[`AGENTS.md`](../../AGENTS.md) §12.2, the fleet lifecycle is §10, and shared mechanics
are in [concurrent-agents.md](concurrent-agents.md).

Open chats in the `tokeira` project as usual and choose **Worktree** instead of
**Local** for every coding task. Kache works invisibly underneath Cargo.

## The mental model

```text
ChatGPT project: tokeira
│
├── Local
│   └── the main checkout — research and questions only (the integration seat)
│
├── Worktree chat: fix-parser
│   └── Separate checkout + its own target/
│
├── Worktree chat: add-endpoint
│   └── Separate checkout + its own target/
│
└── Worktree chat: storage-tests
    └── Separate checkout + its own target/

All cargo commands
        │
        ▼
~/Library/Caches/kache
One shared compilation cache
```

Never create directories for ChatGPT worktrees manually — the app manages them under
`~/.codex/worktrees/`.

## Configuration

Configuration is split in two, per Codex's project-config rules.

**User-level `~/.codex/config.toml`** defines the permission profile (profiles cannot
live in project config):

```toml
[permissions.tokeira]
extends = ":workspace"

[permissions.tokeira.filesystem]
"~/.cargo" = "write"                  # cargo's package-cache lock + registry
"~/Library/Caches/kache" = "write"    # the kache store (path per `kache doctor`)

[permissions.tokeira.network]
enabled = false                       # agents build --locked from the warm registry
```

Define profiles for other repositories the same way; each repository's tracked config
selects its own.

**Tracked `.codex/config.toml` (this repo)** selects the profile and raises the
project-doc cap so `AGENTS.md` is never silently truncated:

```toml
default_permissions = "tokeira"
project_doc_max_bytes = 131072
```

In the ChatGPT permission selector choose **Custom (config.toml)**, and restart ChatGPT
after changing configuration. Do not combine permission profiles with the older
`sandbox_mode` / `[sandbox_workspace_write]` keys — when those are loaded they take
precedence and profiles are ignored. Profiles are beta:
[permissions documentation](https://learn.chatgpt.com/docs/permissions) ·
[configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).

## Starting a coding task

1. Open the `tokeira` project and start a new chat with **Codex** selected.
2. Under the composer, change the environment from **Local** to **Worktree**.
3. Select `main` as the starting branch — valid only when local `main` equals
   `origin/main` (run the preflight in [concurrent-agents.md](concurrent-agents.md)).
4. Do not select any option that copies unstaged local changes.
5. State the task, scoped to named crates:

```text
Implement activity retry handling according to the relevant Kiro spec.
Run formatting, linting, and focused tests. Work only in this worktree.
```

The app creates a managed worktree — detached HEAD at the selected commit, its own
`target/` — reads the tracked files and `AGENTS.md`, and runs Cargo with kache
automatically. The chat header's **Open** control reveals the worktree in Finder, an
IDE, or the integrated terminal.
[Worktree documentation](https://learn.chatgpt.com/docs/environments/git-worktrees).

## Concurrent tasks

Start another chat the same way: **Worktree**, based on `main`, with a
**non-overlapping** task that names the crates it owns (`AGENTS.md` §10.3). Each chat
gets its own `target/`, and all of them draw on the shared kache store, so a crate
compiled in one worktree restores into the others instead of recompiling.

Keep to **at most two simultaneous heavy Cargo builds**: worktrees remove target-dir
lock contention, but CPU, memory, disk bandwidth, `$CARGO_HOME`, and git metadata stay
shared.

## Finishing a task

1. Have Codex run the Enforced Commands bar (`AGENTS.md` §10.4) and report any check
   not run and why.
2. Review the diff in the **Changes** view.
3. Click **Create branch here** in the chat header and name the branch to the fleet
   convention: `agent/codex/<task-slug>`.
4. Commit through the app's git controls, ending with the §11 attribution trailers
   (`Co-authored-by: Codex <codex@openai.com>`).

**Create branch here** turns the detached-HEAD worktree into an ordinary local branch.
[Branch behavior](https://learn.chatgpt.com/docs/environments/git-worktrees#working-between-local-and-worktree).

## Integration

Codex's sandbox has no network, so a finished task ends at the named local branch — it
is handed off, not pushed. From the main checkout (or via Claude), push the branch and
open the PR:

```sh
git push --set-upstream origin agent/codex/<task-slug>
gh pr create --base main
```

The integration seat merges PRs serially, server-side (`AGENTS.md` §10.6–§10.8). Never
merge agent branches locally in the main checkout, and do not hand chats back to
**Local** for coding — coding stays in worktrees. After a verified merge the app
LRU-cleans its managed worktrees; branch cleanup follows §10.8.

## Day to day

- Question or research → new chat → **Local**.
- Coding task → new chat → **Worktree**, based on `main`, without copying unstaged
  changes.
- Additional concurrent task → another **Worktree** chat with a non-overlapping scope.
