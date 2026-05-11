# Remote Workstation

`tkr workstation` creates an AWS build workstation for Rust-heavy development
workloads. It is intentionally separate from the normal deployment/IaC flow:
workstation lifecycle uses direct AWS SDK calls, is tagged for discovery, and is
optimized for fast create/resume/stop cycles.

## Prerequisites

- AWS credentials with EC2, IAM, and SSM permissions for the target account.
- AWS CLI plus `session-manager-plugin` for `tkr workstation ssh`.
- GitHub CLI (`gh`) authenticated on the local machine if using
  `tkr workstation github-key`.
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
