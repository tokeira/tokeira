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
| `tkw devbox sync` | rsync the current worktree to `/workspaces/<worktree-name>/` on the box (needs `--box`) |
| `tkw devbox run -- <cmd…>` | sync, then run `<cmd…>` in the remote copy, streaming output, mirroring the exit code |
| `tkw devbox bar` | sync, then the §10.4 bar remotely (fmt in `--check` form), per-step timing, stop at first failure |
| `tkw devbox markers` | list the activity markers keeping a box awake, whoever owns them |
| `tkw devbox down` | stop the box now instead of waiting out its idle timeout |

Box selection: **`--tag <tag>`** leases a box from a pool for the length of the command
(`run` and `bar` only), or **`--box <name>`** addresses one directly; `TKW_DEVBOX_TAG`
and `TKW_DEVBOX` are the respective fallbacks, and the two flags are mutually
exclusive. The data-plane path talks to the plain SSH host `<name>.devbox.namespace`
that `devbox create` writes into `~/.ssh/config` and needs no `devbox` CLI; leasing and
`down` are control-plane operations and do require it.

### Pooled boxes: `--tag`

`devbox acquire <tag>` reuses a box already carrying the tag when its lease is free and
builds one only when none is, so a pool keeps its warm `target/` across runs while each
run still has an explicit end:

```bash
tkw devbox bar --tag tokeira-bar
```

`tkw` acquires before syncing and releases on every exit path, so the box returns to the
pool whether the bar passes, fails, or panics. Pooled boxes are created at the shape
above — `m`, 100 GB, 15-minute idle timeout, `builtin:base`, no checkout — and that
shape is not configurable: a pool whose members differ gives runs whose cost and timing
depend on which box they happened to land on. A one-off that needs something else is
created by hand and addressed with `--box`.

**This is why `--ephemeral` is the wrong flag here.** An ephemeral devbox keeps no
state: Namespace deletes the instance *and its storage* when it stops, so nothing is
ever reused and every run starts cold. Measured on `m`, a warm bar is about 4m ($0.03)
while a cold one — provisioning, cold registry, cold clippy over every target — is
nearer 30–40m ($0.24–0.32). Ephemeral trades a storage line of roughly $0.05/GB-month
for repeated cold rebuilds on the Devbox-Minutes line, which is an order larger. Reserve
it for genuinely disposable one-offs.

A lease that outlives its run is the marker hazard one level up: it costs no compute
(the box idle-stops regardless), but `acquire` skips a box whose lease is still held, so
the next run builds *another* box and the pool grows a volume at a time. Each acquire
therefore first releases leases held past four hours, on the same reasoning as the
marker sweep.

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
devbox create --name <box> --size m --volume_size_gb 100 \
    --auto_stop_idle_timeout 15m \
    --no_checkout --site <site> --image builtin:base --purpose "<why>"
```

**`m` is the default size and raising it needs a reason.** A warm bar is about 4m on `m`
(measured below); `xl` bills 4× the rate and is nowhere near 4× faster on a bar whose
longest step is a test run with bounded parallelism, so `xl` costs *more per bar* for
finishing sooner. The multiplier applies to idle time too — an `xl` sitting awake
burns 8 Devbox Minutes a minute against `m`'s 2 — which is why size is the single
biggest lever on a bill dominated by boxes being awake.

- `create` auto-runs the SSH-config step; `devbox configure-ssh <name>` is only for
  boxes created elsewhere.
- `--no_checkout` skips the default repo clone: rsync carries the worktree, so the
  box needs no repository credentials.
- A `create` that fails (e.g. against a workspace quota) still registers the name,
  leaving a record to remove with `devbox expire <name> --force` before the name can
  be reused. Fleet-wide vCPU concurrency is a separate per-plan limit from instance
  shape.
- Boxes pause when idle (configurable `--auto_stop_idle_timeout`) and resume on the
  next SSH connect in seconds — onto a fresh instance around the same volume.
  Persistence is whole-disk: the synced tree, remote `target/`, toolchains, and apt
  packages all survive stop/resume, so provisioning is once-ever.
- **Paused costs compute nothing, but volumes bill continuously.** Devbox Minutes stop
  accruing when a box pauses. The persistent volume (`--volume_size_gb`) does not: it
  bills as its own **Persistent Volume Storage** line in GB-month, whether or not the
  box is ever activated. The rate is not published and is of the order of
  $0.05/GB-month — a fraction of what the Minutes come to. Volume size is therefore a
  real but secondary lever: worth right-sizing, not worth restructuring the workflow
  around.

### Idle tail — stop the box when the work is done

**Devbox Minutes are dominated by boxes being awake, not by boxes building.** A box
keeps billing from the moment the work finishes until its idle timeout fires, so every
session carries a tail. A fleet doing many short activations therefore spends most of
its Devbox Minutes on boxes that are awake rather than working, and no single leaked
marker is needed to make that the dominant line.

Two mechanisms, in order of effect:

```bash
tkw devbox down --box <box>    # stop now; Devbox Minutes stop accruing immediately
```

and `--auto_stop_idle_timeout 15m` at create time, which bounds the tail when nobody
runs `down`. Fifteen minutes is the shortest preset and is safe precisely because
`tkw devbox run`/`bar` hold an activity marker for the duration of their work — the
timeout can only fire once the work has actually finished.

`down` needs the `devbox` CLI, because stopping is a control-plane operation with no
SSH equivalent; without the CLI it prints the command to run. Stopping loses nothing:
the volume persists, and the next connect resumes in seconds onto a fresh instance
around the same disk.

### Activity markers — `tkw` owns them

Namespace considers a Devbox active while any file exists under
[`/.namespace/tasks`](https://namespace.so/docs/guides/devbox/long-running-tasks),
which is what keeps a box alive across the gaps between a bar's separate SSH
invocations. A marker that outlives its run keeps the box awake and billable
indefinitely.

**`tkw devbox run` and `bar` claim and release their own marker.** There is nothing to
do by hand. The marker is named `tkw-<worktree>-<pid>`, released on every exit path
including failure, and re-touched at each bar step so a long run is never mistaken for
a leak. Each claim first sweeps `tkw-` markers unrefreshed for over four hours, so the
next run on a box repairs a previous one's leak — covering the cases a guard cannot
see, such as a SIGKILL or a session abandoned mid-run. The `tkw-` prefix scopes both
the naming and the sweep: markers you create by hand, and markers belonging to other
agents, are never swept.

Marker upkeep is owned by the tool rather than left to convention because the failure is
silent and unbounded: a marker left behind keeps its box awake and billing indefinitely,
and a hand-executed protocol offers nothing that detects the omission.

To see what is holding a box awake, whoever owns it:

```bash
tkw devbox markers --box <box>
```

A marker you create by hand is still yours to remove — remove exactly that file, never
a glob, because one Devbox serves several worktrees:

```bash
devbox exec <box> -- rm /.namespace/tasks/<name>
```

Session creation also suppresses idleness for 15 minutes, which is why a bare
`tkw devbox sync` needs no marker.

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

Devbox Minutes accrue per minute while running: `m` $0.008/min, `xl` $0.032/min
($1.92/hr). **They are metered separately and do not draw down a plan's included unit
minutes** — the Team plan's 100,000 cover compute, Docker builds and CI runners, not
Devboxes (confirmed with Namespace support, 2026-09-16). Plan choice is driven by
fleet-wide vCPU concurrency caps; Devbox spend is governed by size and running time
alone, which is why `m` is the default (a bar is about 4m on `m` — an `xl` bills 4× the
rate and is nowhere near 4× faster on a test-dominated bar).
