# Namespace Devboxes — remote cargo offload

Long-running workspace-wide cargo operations (the [AGENTS.md](../../AGENTS.md) §10.4
bar, `cargo test --workspace`, workspace clippy, doc builds) run on a
[Namespace](https://namespace.so) Devbox instead of the development Mac. The loop:
rsync the current worktree — uncommitted state included — to the box's persistent
volume, run cargo there over plain SSH, stream output back.

**Compute moves; authority does not.** Remote runs produce verdicts and diagnostics
only. Artifacts are per-target-triple (the box is Linux amd64, the Mac is
aarch64-apple-darwin) and never flow back into a local `target/` or the kache store.
The per-turn `tkw hook stop` check still compiles locally. Verdicts transfer because
the workspace has no `target_os` conditionals — only `cfg(unix)`, which Linux
satisfies identically — and no OpenSSL (rustls-only by deliberate pin).

## Interface: `tkw devbox`

| Command | Does |
|---------|------|
| `tkw devbox sync` | rsync the current worktree to `/workspaces/<worktree-name>/` on the box |
| `tkw devbox run -- <cmd…>` | sync, then run `<cmd…>` in the remote copy, streaming output, mirroring the exit code |
| `tkw devbox bar` | sync, then the §10.4 bar remotely (fmt in `--check` form), per-step timing, stop at first failure |

Box selection: `--box <name>` or `TKW_DEVBOX`. The tool talks to the plain SSH host
`<name>.devbox.namespace` that `devbox create` writes into `~/.ssh/config`; it does
not require the `devbox` CLI.

The sync excludes are hardcoded and not configurable:

- `.env*` — local environment files are machine-local and never leave it (§10.3).
- `.git` — a linked worktree's `.git` is a pointer file into the machine-local
  common dir; meaningless remotely. `tokeira-build-info` degrades to an `unknown`
  git SHA without it.
- `target/` — artifacts are platform-local in both directions.

Each worktree syncs to its own directory under `/workspaces`, so one box serves the
whole fleet without two agents clobbering each other's tree. Remote fmt runs as
verification (`--check`); formatting mutations happen locally.

## Box lifecycle

CLIs: `nsc` (`brew install namespacelabs/namespace/nsc`) and the separate `devbox`
binary (`curl -fsSL get.namespace.so/devbox/install.sh | bash`; installs to
`~/.local/bin`). Both have their own `login`. The devbox CLI iterates quickly —
trust `--help` over any doc, including this one.

```bash
devbox site-latency          # pick the nearest site
devbox create --name <box> --size xl --volume_size_gb 100 \
    --no_checkout --site <site> --image builtin:base --purpose "<why>"
```

- `create` auto-runs the SSH-config step; `devbox configure-ssh <name>` is only for
  boxes created elsewhere.
- `--no_checkout` skips the default repo clone: rsync carries the worktree, so the
  box needs no repository credentials.
- A `create` that fails (e.g. against a workspace quota) still registers the name,
  leaving a record to remove with `devbox expire <name> --force` before the name can
  be reused. Fleet-wide vCPU concurrency is a separate per-plan limit from instance
  shape.
- Boxes pause when idle (configurable `--auto_stop_idle_timeout`), cost $0 paused,
  and resume on the next SSH connect in seconds — onto a fresh instance around the
  same volume. Persistence is whole-disk: the synced tree, remote `target/`,
  toolchains, and apt packages all survive stop/resume, so provisioning is once-ever.

### Mark long-running tasks

Namespace considers a Devbox active while any file exists under
[`/.namespace/tasks`](https://namespace.so/docs/devbox/managing#how-idleness-is-detected).
Before starting a long non-interactive build or test run, create a uniquely named
marker so the box cannot idle-stop between SSH connections:

```bash
devbox exec <box> -- touch /.namespace/tasks/<agent>-<task>
```

Remove exactly that marker as soon as the task is finished, including after a failure.
A stale marker prevents idle-stop and therefore keeps the box billable:

```bash
devbox exec <box> -- rm /.namespace/tasks/<agent>-<task>
```

One Devbox may serve several worktrees, so each active task owns its own marker; never
remove another task's file. Session creation also suppresses idleness for 15 minutes,
but a task marker is the explicit lifetime signal for a long cargo run.

Provision (Ubuntu `builtin:base` image):

```bash
ssh <box>.devbox.namespace 'bash -s' <<'EOF'
set -e
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    protobuf-compiler cmake clang pkg-config git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --quiet --default-toolchain 1.97.1 \
    --component rustfmt --component clippy
. /usr/local/cargo/env
rustup toolchain install nightly-2026-06-16 --component rustfmt --profile minimal
# The §10.4 bar's test step runs under nextest (root §10.4); prebuilt binary,
# not `cargo install`, so provisioning stays minutes not tens of minutes.
curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C /usr/local/cargo/bin
EOF
```

- The base image ships rustup system-wide: `RUSTUP_HOME=/usr/local/rustup`, env file
  at `/usr/local/cargo/env` (not `~/.cargo/env`). Non-interactive SSH does not source
  login profiles; `tkw devbox` sources the cargo env itself.
- `protoc` is a hard build requirement (prost-build and connectrpc-build shell out to
  it; nothing vendors it). cmake + clang cover aws-lc-sys, ring, zstd-sys, mimalloc.
- The fmt nightly must match CI's `NIGHTLY_FMT_TOOLCHAIN` pin exactly; `tkw devbox
  bar` uses whichever dated nightly is installed on the box, keeping the pin's home
  in CI config.

## Boundaries and rules

- **kache stays local and untouched.** Its `RUSTC_WRAPPER` wiring lives in the Mac's
  `~/.cargo/config.toml`, not the repo, so the synced tree builds unwrapped remotely
  — correct on both sides. Never copy the Mac's cargo config or `KACHE_*` env to a
  box (§10.1 applies fleet-wide).
- **No remote compilation cache products** (Namespace sccache, etc.): they would
  displace kache in the single `RUSTC_WRAPPER` slot and cache only rustc
  compilations. The offload model is warm persistent volumes, not shared caches.
- Leave `TOKEIRA_BUILD_MANIFEST_PATH` unset remotely so `tokeira-build-info` uses its
  dev fallback.
- To place a one-off file on a box, use `tkw devbox sync`, `devbox upload`, or
  `ssh <box> 'cat > path' < file` — not `scp`, which (observed 2026-08-12) writes the
  file as `root` and then errors.
- Renaming or moving a synced tree over a warm remote `target/` invalidates
  compile-time-baked paths (`env!("CARGO_MANIFEST_DIR")`) in cached build-script and
  test binaries whose sources haven't changed since — they re-run against the old
  absolute path. `touch` the affected source locally and re-sync so it recompiles;
  both observed cases (a build script, a fixture-reading test) resolved this way.

## Measured reference timings

Size `m` (8 vCPU / 16 GiB) box at a nearby site, 2026-08-12, against this workspace
(52 crates, ~346k lines, 895 locked packages):

| Measurement | Result |
|-------------|--------|
| First connect incl. box activation | 4.1 s |
| Resume from idle-stop (fresh instance, state intact) | 7.3 s |
| First full worktree sync (29.5 MB) | 4.7 s |
| Incremental sync after a 1-line edit | 1.9 s |
| Cold `cargo check --workspace --locked` incl. registry fetch | 2 m 44 s |
| Warm check after a mid-graph edit / no-op | 3.2 s / 0.5 s |
| Cold `cargo test --workspace --locked` (146 suites green) | 8 m 02 s |
| Warm full §10.4 bar (fmt 2 s · lint 47 s · check 4 s · test 3 m 07 s · doc 16 s) | 4 m 16 s |

Devbox pricing is per-minute while running (size `m` $0.008/min, `xl` $0.032/min),
$0 paused. Plan choice is driven by fleet-wide vCPU concurrency caps, not price.
