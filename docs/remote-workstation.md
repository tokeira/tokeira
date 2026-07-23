# Remote Workstation

`tkr workstation` creates an AWS build workstation for Rust-heavy development
workloads. It is intentionally separate from the normal deployment/IaC flow:
workstation lifecycle uses direct AWS SDK calls, is tagged for discovery, and is
optimized for fast create/resume/stop cycles.

## Prerequisites

- AWS credentials with EC2, IAM, and SSM permissions for the target account.
- AWS CLI plus `session-manager-plugin` for `tkr workstation ssh`
  (`brew install session-manager-plugin` on macOS).
- GitHub CLI (`gh`) authenticated on the local machine if using
  `tkr workstation github-key`.
- A VPC with at least one public subnet — the workstation uses a transient
  public IP for egress; no NAT Gateway is required.
- No inbound SSH access is required. Interactive access uses SSM Session
  Manager.

## Happy Path

```bash
tkr workstation up
tkr workstation remote-exec cargo build --workspace
tkr workstation stop
```

`up` writes the selected workstation ID to
`~/.tokeira/workstations/.latest`. Later commands use that value unless
`--workstation <id>` is supplied.

## Lifecycle

| Command | What it does |
|---------|-------------|
| `tkr workstation up` | Creates a new workstation (first run) or resumes a stopped one. Runs bootstrap, waits for readiness. |
| `tkr workstation stop` | Stops the instance. EBS volumes persist; NVMe (`/work/target`, `/work/sccache`) is erased. Public IP released. |
| `tkr workstation destroy --yes` | Terminates the instance, deletes EBS volumes, removes IAM role and security group. Irreversible. |
| `tkr workstation bootstrap` | Forces a bootstrap refresh (re-installs toolchain, cargo tools) without destroying the instance. Triggered automatically on `up` when drift is detected. |
| `tkr workstation status` | Shows state, uptime, cost rate, bootstrap fingerprint, volume IDs. |
| `tkr workstation list` | Enumerates all workstations in the account with state and cost. |
| `tkr workstation idle --defer 2h` | Extends the idle-shutdown window (prevents auto-stop during long unattended builds). |

The workstation stops automatically after 30 minutes of idle (configurable). A
forgotten instance does not silently accumulate cost.

## First Run vs Resume

**What happens on first `up`:**

- Instance created, EBS volumes provisioned and attached.
- Cloud-init bootstrap runs (~5–8 minutes): installs Rust toolchain (stable +
  nightly from `rust-toolchain.toml`), cargo tools, mounts NVMe, clones the
  repository.
- `tkr workstation up` blocks until bootstrap completes (polls via SSM).

**What happens on subsequent `up` (resume):**

- Instance started (~30 seconds to reach `running`).
- Bootstrap fingerprint checked — if your `rust-toolchain.toml` changed, a
  refresh runs automatically.
- NVMe reformatted (`target/` and sccache are cold); EBS volumes intact (cargo
  registry, rustup, repo checkout preserved).

## Development Workflow

A typical day working with the remote workstation:

```bash
# 1. Bring the workstation online (creates on first run, resumes thereafter)
tkr workstation up

# 2. (First time only) Add a GitHub deploy key so you can push from the workstation
tkr workstation github-key add --repo <owner>/tokeira

# 3. Sync code to the workstation (clones on first run, pulls thereafter)
tkr workstation code sync
tkr workstation code sync --branch feature/my-work   # specific branch

# 4. Build
tkr workstation remote-exec cargo build --workspace

# 5. Test
tkr workstation remote-exec cargo test --workspace

# 6. Lint
tkr workstation remote-exec cargo clippy --workspace --all-targets

# 7. Format check
tkr workstation remote-exec cargo +nightly fmt --all --check

# 8. (Optional) Interactive debugging session
tkr workstation ssh
# You're now in a shell on the workstation at /work/tokeira
exit

# 9. Push results back to origin
tkr workstation code push
tkr workstation code push --branch feature/my-work   # specific branch

# 10. Stop when done for the day
tkr workstation stop
```

## Storage Tiering

| Tier | Mount | Survives stop? | Contents |
|------|-------|----------------|----------|
| Local NVMe | `/work` (root), `/work/target`, `/work/sccache` | No | `CARGO_TARGET_DIR`, sccache cache |
| Cache EBS (30 GiB) | `/work/cache` → `~/.cargo`, `~/.rustup` | Yes | Crate registry, toolchains, cargo tools |
| Repo EBS (40 GiB) | `/work/repo` → `/work/tokeira` | Yes | Repository checkouts, uncommitted work |

The first build after a resume is a cold build (NVMe wiped), but cargo's
incremental state on the Cache EBS gives most of the benefit. Subsequent builds
within a session hit sccache on NVMe for maximum speed.

## Cost Model

The embedded cost table is intentionally small and stale-tolerant. Unknown
rates are printed as `unknown` rather than hidden.

| Region | Instance | Embedded hourly rate | Approx active day |
|--------|----------|----------------------|-------------------|
| `eu-west-2` | `c8gd.8xlarge` | `$1.87776` | `$45.07` |
| `us-east-1` | `c8gd.8xlarge` | `$1.56768` | `$37.62` |

Stopped workstations retain the cache/repo EBS volumes. With the default
30 GiB cache and 40 GiB repo volumes, the stopped cost is roughly `$0.25/day`,
varying by region and EBS pricing.

As a monthly rule of thumb: at 20 working days × 10 active hours/day in
`eu-west-2`, expect on the order of `$385/month` (instance + EBS + transient
Elastic IP).

## Bootstrap

The bootstrap script:

- Mounts EC2 instance-store NVMe at `/work`.
- Mounts persistent cache and repo EBS volumes by explicit volume ID.
- Installs Rust toolchains, `cargo-nextest`, `cargo-deny`, `sccache`, and
  GitHub CLI.
- Pins GitHub SSH host keys.
- Clones the configured repository into `/work/repo/tokeira` when public access
  works.
- Writes `/etc/tokeira/workstation-fingerprint`.

Private repository clone failures are non-fatal. The bootstrap writes
`/etc/tokeira/repo-clone-status` and still completes, so the operator can run:

```bash
tkr workstation github-key add --repo <owner>/<repo>
tkr workstation bootstrap
```

## Operations

```bash
tkr workstation status
tkr workstation list
tkr workstation ssh
tkr workstation remote-exec -- cargo test --workspace
tkr workstation idle --defer 2h
tkr workstation bootstrap
tkr workstation destroy --yes
```

`remote-exec` uses SSM Run Command. Output is polled and emitted with bounded
near-real-time behavior; it is not a byte-stream terminal. Commands that look
like they contain secrets are blocked unless `--yes-secret-in-command` is
provided, because full command text is logged by AWS control-plane services.

## GitHub Deploy Keys

`github-key add` generates an ed25519 keypair on the workstation and adds the
public key as a GitHub deploy key from the local machine via `gh api`.

```bash
tkr workstation github-key add --repo <owner>/<repo>
tkr workstation github-key list
tkr workstation github-key remove --repo <owner>/<repo>
```

Local registry entries are stored at
`~/.tokeira/workstations/<id>/deploy-keys.jsonl`. `destroy` uses this registry
to best-effort remove orphan deploy keys before tearing down AWS resources.

## Troubleshooting

- `session-manager-plugin is not installed`: install the AWS Session Manager
  plugin locally, then retry `tkr workstation ssh`.
- `bootstrap fingerprint was not written`: inspect SSM Run Command output and
  rerun `tkr workstation bootstrap`.
- Private repo did not clone: run `tkr workstation github-key add --repo
  <owner>/<repo>`, then rerun bootstrap or clone manually over SSH.
- Idle shutdown fired too soon: run `tkr workstation idle --defer 2h` before a
  long unattended build.
- Suspected orphan resources: filter AWS resources by tags
  `tokeira-workstation=true` and `workstation-id=<id>`.

## See Also

- [Development guide](development.md) — the local build/test loop the
  workstation accelerates
- [`.kiro/specs/remote-workstation/`](../.kiro/specs/remote-workstation/) —
  full requirements, design, and task history
