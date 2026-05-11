# Requirements Document: Remote Workstation

## Introduction

Tokeira development on a MacBook hit a wall: cold `cargo build --workspace` cycles of 10–20 minutes collapsed the iteration loop. This spec restores productivity by moving the Rust build and validation surface to a dedicated EC2 instance under direct AWS account control (a Graviton4 `c8gd.8xlarge` with local NVMe), accessed from the MacBook via AWS Systems Manager Session Manager. The operator keeps the MacBook as the control surface and editing environment; the EC2 instance is the build and validation environment.

This spec is **scoped to the workstation itself**. The agent controller (Codex, review packs, `tkr agent *` subcommands) is a separate spec that consumes this one. The rule is: a working remote workstation must be usable on its own merits — fast `cargo build`, `cargo test`, `cargo lint` against the Tokeira workspace — without any agent-related code existing yet. Proving that discipline before expanding scope is a principal goal of this spec.

### What this spec delivers

- A new `tkr workstation` command group in `apps/tkr` that creates, inspects, and destroys one or more long-lived `c8gd.8xlarge` build instances in the operator's AWS account, each tagged so `tkr` can rediscover them idempotently.
- A new `remote-workstation` module in `crates/tokeira-aws/src/` that declares one EC2 instance, one security group with no inbound rules, persistent cache and repo EBS volumes, an IAM role (+ instance profile) granting SSM agent permissions, and a tag vocabulary that marks instances as Tokeira workstations. The module calls the AWS SDK directly; the rationale for not routing this surface through `tokeira-iac` is documented in Feature 6.3.
- A cloud-init bootstrap that installs a pinned Rust toolchain, common build tooling (`cargo-nextest`, `cargo-deny`, `sccache`, `protoc`, `buf`, `uv`, `ripgrep`, `fd`), mounts the instance's local NVMe at `/work`, mounts the cache and repo EBS volumes at stable paths, and writes a shell environment under `/etc/profile.d` that points `CARGO_TARGET_DIR`, `RUSTC_WRAPPER`, and related variables at the right tier of storage.
- A `tkr workstation remote-exec` command that streams shell command output from the instance back to the operator's terminal over an SSM Session Manager channel — zero public ingress, zero SSH keys.
- An idle-shutdown watchdog on the instance that stops the instance (preserving EBS) after a configurable idle window, so a forgotten `tkr workstation up` does not silently accumulate cost.
- Cost observability surfaced on the `tkr workstation status` command: the approximate instance-hour cost rate and the elapsed uptime for the running instance.

### What this spec explicitly defers

- **`tkr agent *` subcommands** — `agent submit`, `agent status`, `agent logs`, `agent diff`, `agent review-pack`. The agent controller is a follow-up spec (`agent-controller`); this spec ships a remote workstation, not an agent.
- **agentd daemon on the instance** — no Tokeira-authored process runs on the remote workstation in this spec. Every operator interaction is `tkr` on the MacBook talking to the instance over SSM Session Manager with a shell command. `agentd` is owned by `agent-controller`.
- **Codex installation or credentials on the instance** — `agent-controller` installs and authenticates Codex on the workstation. This spec's bootstrap is agent-free.
- **Multi-instance orchestration** — one workstation per `tkr workstation up` invocation. Multiple instances can coexist under distinct `workstation-id` tags; the CLI operates on one at a time selected by `--workstation` or the last-used sentinel. Parallel agent workloads across multiple instances are a concern for `agent-controller`, not this spec.
- **S3-backed shared sccache** — the bootstrap installs `sccache` with the local filesystem backend at `/work/sccache` on the instance's NVMe. A shared S3 backend is a cost/speed optimisation deferred to a future iteration once two or more workstations regularly coexist or the post-stop cold-cache cost becomes operationally annoying.
- **Remote debugging, LLDB forwarding, interactive IDE sync** — these are fine use cases for the SSM channel but are not requirements of this spec. The operator SSHes (via `tkr workstation ssh`) for ad-hoc interactive work; the programmatic surface is `tkr workstation remote-exec`.
- **Multi-region support** — every workstation lives in a single operator-configured default region. Multi-region is an explicit non-goal; if it becomes necessary, a future spec reshapes the configuration surface.
- **Public internet ingress to the instance** — the instance has no public IP and no inbound security-group rules. All access is via SSM Session Manager with IAM-scoped authorisation.
- **Tokeira runtime code on the workstation** — `tokeirad` does not run on the remote workstation under this spec. The workstation exists to compile, test, and lint Tokeira, not to host it.

### Cross-references

- [`agent-controller`](../agent-controller/requirements.md): explicit consumer of this spec's workstation surface. Depends on `tkr workstation up` + `tkr workstation remote-exec` + the SSM port-forwarding affordance. Does not exist yet; this spec's Feature 8 pre-declares the minimal extension point (a Unix-socket path convention) so `agent-controller` can plug in without reshaping this spec's module.
- `tokeira-iac`: explicitly NOT consumed by this spec. The workstation surface uses the AWS SDK directly; see Feature 6.3 for the rationale. The `tokeira-iac` crate is unchanged.
- `tokeira-aws`: gains one new top-level module (`remote_workstation.rs`) and one new AWS SDK client dependency (`aws-sdk-ssm`). Existing resource implementations are unchanged.

## Glossary

- **Workstation_Instance**: The single EC2 `c8gd.8xlarge` Graviton4 instance this spec provisions. Tagged `tokeira-workstation=true` plus a `workstation-id` that uniquely identifies it across the operator's AWS account.
- **Workstation_Id**: A short stable identifier chosen at `tkr workstation up` time (e.g. `ws-01HXYZ...`). Used as the tag value on the instance and its attached EBS volumes, and as the filesystem path under which the local `tkr` CLI caches its view of the remote state.
- **Cache_Volume**: A persistent EBS gp3 volume holding `~/.cargo` (crate registry and git sources) plus `~/.rustup` (toolchains). Survives instance stop. 30 GiB default — cargo registry ~5 GiB, rustup ~6 GiB for stable+nightly, headroom for multi-toolchain workflows.
- **Repo_Volume**: A persistent EBS gp3 volume holding one or more repository checkouts under `/work/`. Primary use is `/work/tokeira`; the default 40 GiB allows several additional working-tree clones without repartitioning. Separate from the Cache_Volume so a toolchain recycle does not risk uncommitted work.
- **Local_NVMe**: The instance-store NVMe disk native to the `c8gd` family (~1900 GiB on `c8gd.8xlarge`). Ephemeral — erased on instance stop. Used for `CARGO_TARGET_DIR` AND the `sccache` filesystem cache; both are latency-sensitive and recreatable, which matches NVMe's properties exactly. Losing the cache on stop is accepted in exchange for NVMe-speed hits on every build within a session.
- **SSM_Session**: An AWS Systems Manager Session Manager session. The only authorised inbound channel to the Workstation_Instance. Scoped to the operator's AWS IAM identity via an allow-list of SSM-related permissions.
- **Workstation_Profile**: A named profile declaring the instance type, AMI family, region, Cache_Volume size, Repo_Volume size, and idle-shutdown window. The default profile (`c8gd-rust`) captures the choices this spec endorses; future profiles are additive.
- **Idle_Shutdown_Watchdog**: An `systemd`-managed process on the instance that polls CPU load and SSM-session presence. After a configurable idle window elapses with both conditions below a quiescence threshold, it invokes `shutdown -h now` to stop the instance.
- **Workstation_State_Dir**: A MacBook-local directory (`~/.tokeira/workstations/<workstation-id>/`) where `tkr` caches last-known state for each workstation. Includes tag values, EBS volume IDs, bootstrap fingerprint, last-seen uptime. Reconciliation against AWS always wins on conflict; this directory is a performance cache, not a source of truth.
- **Bootstrap_Fingerprint**: A SHA-256 over the bootstrap script contents plus the pinned toolchain versions. Stored in Workstation_State_Dir and as an instance tag. Drift detection triggers a re-bootstrap on `tkr workstation up` when the fingerprints disagree.

## Requirements

---

## Feature 1: Workstation Lifecycle

### Requirement 1.1: `tkr workstation up` creates or resumes a workstation

**User Story:** As an operator, I want a single command that either provisions a new workstation or brings my existing one back online, so that I do not accumulate duplicate instances by running `up` twice.

#### Acceptance Criteria

1. WHEN the operator invokes `tkr workstation up --profile <profile>`, THE CLI SHALL query EC2 for instances in the configured region matching `tag:tokeira-workstation=true` AND owned by the current IAM principal's account.
2. IF zero matching instances are found, THEN THE CLI SHALL create a fresh Workstation_Instance per the Workstation_Profile — including the two EBS volumes, the security group, the IAM role, and the instance profile — and tag it with a newly-generated Workstation_Id.
3. IF exactly one matching instance is found and its state is `stopped`, THEN THE CLI SHALL invoke `StartInstances` and wait for the instance to reach `running` before returning. NO new instance SHALL be created in this branch.
4. IF exactly one matching instance is found and its state is `running`, THEN THE CLI SHALL return successfully without any state change; the command SHALL print the Workstation_Id and the bound SSM session-ready status.
5. IF two or more matching instances are found, THEN THE CLI SHALL fail with a clear error message enumerating the instance IDs and tags; the operator SHALL select one via `--workstation <workstation-id>` before `up` succeeds. THE CLI SHALL NOT silently pick one.
6. IF a matching instance is in a transitional state (`pending`, `stopping`, `shutting-down`), THEN THE CLI SHALL wait for the state to settle (bounded by a 5-minute timeout) and then re-evaluate.
7. WHEN `up` returns successfully, THE CLI SHALL write the Workstation_Id into `~/.tokeira/workstations/.latest` so subsequent commands default to it.

### Requirement 1.2: `tkr workstation stop` preserves EBS and NVMe-less state

**User Story:** As an operator, I want a stop command that halts the instance without destroying any persistent data, so that I can resume where I left off without paying instance-hours while I am not working.

#### Acceptance Criteria

1. WHEN the operator invokes `tkr workstation stop`, THE CLI SHALL invoke `StopInstances` on the Workstation_Instance and wait for the instance to reach `stopped`.
2. THE Cache_Volume and Repo_Volume SHALL remain attached across a stop/start cycle. NO volume detach SHALL occur during `stop`.
3. THE operator SHALL be warned, in the `stop` command's output, that the Local_NVMe contents will be erased on stop. The warning SHALL name the specific paths affected: `/work/target` (cargo target directory) AND `/work/sccache` (sccache cache).
4. WHEN the `stop` command completes, `tkr workstation status` SHALL report the instance state as `stopped` and SHALL retain the Workstation_Id mapping in `~/.tokeira/workstations/`.

### Requirement 1.3: `tkr workstation destroy` removes all resources

**User Story:** As an operator, I want a destroy command that terminates the instance, deletes the EBS volumes, and removes the supporting IAM and security-group resources, so that no Tokeira workstation resources linger in my AWS account.

#### Acceptance Criteria

1. WHEN the operator invokes `tkr workstation destroy`, THE CLI SHALL require confirmation — either via an interactive prompt or the `--yes` flag per the `tkr` convention documented in `cli.rs`.
2. WHEN confirmation is received, THE CLI SHALL invoke the `tokeira-iac` destroy path on the `remote-workstation` module, which tears down in reverse dependency order: terminate the instance, delete the EBS volumes, delete the security group, delete the instance profile, delete the IAM role.
3. IF any AWS resource is missing when the destroy sequence reaches it, THE CLI SHALL log a warning and proceed with the next resource. Missing resources SHALL NOT fail the destroy.
4. WHEN destroy completes, THE CLI SHALL remove the Workstation_State_Dir for the destroyed workstation AND clear `~/.tokeira/workstations/.latest` if the destroyed Workstation_Id was the pointer target.

### Requirement 1.4: Idempotent bootstrap

**User Story:** As an operator, I want repeated `tkr workstation up` calls to detect bootstrap drift and re-run only the parts of the bootstrap that changed, so that I can upgrade toolchain pins without destroying the instance.

#### Acceptance Criteria

1. WHEN the instance is created in Req 1.1.2, THE cloud-init user-data SHALL include the current Bootstrap_Fingerprint as an environment variable and SHALL write it to the instance at `/etc/tokeira/workstation-fingerprint` after bootstrap completion.
2. WHEN `tkr workstation up` resumes a stopped instance (Req 1.1.3), THE CLI SHALL compare the local Bootstrap_Fingerprint to the instance's stored fingerprint (retrieved via SSM) and, if they differ, SHALL execute a bootstrap refresh over SSM against the running instance.
3. THE bootstrap refresh SHALL re-install the pinned Rust toolchain, refresh the set of installed cargo-subcommands, and rewrite `/etc/profile.d/tokeira-workstation.sh`. It SHALL NOT touch the Cache_Volume or Repo_Volume contents. It SHALL NOT re-format the Local_NVMe mount.
4. THE bootstrap refresh SHALL be surfaced to the operator in the `up` command's output as "bootstrap drift detected, refreshing …"; a no-drift case SHALL output "bootstrap up to date".

### Requirement 1.5: Multiple workstations coexist

**User Story:** As an operator, I want to run two workstations in parallel when needed (for example, one per spec, or one for a clean build and one for an experiment), so that long-running operations on one do not block the other.

#### Acceptance Criteria

1. THE CLI SHALL accept a `--workstation <workstation-id>` flag on every `tkr workstation` subcommand and on `tkr workstation remote-exec`. WHEN the flag is absent, THE CLI SHALL read `~/.tokeira/workstations/.latest` as the default.
2. WHEN two or more workstations with distinct Workstation_Ids exist, `tkr workstation list` SHALL enumerate them with their state (`running`, `stopped`, `pending`, …), uptime, region, and instance type.
3. THE security group for each workstation SHALL be scoped to that workstation (distinct group per Workstation_Id). Inbound rules SHALL remain empty.
4. Workstation_Ids SHALL be globally unique across the AWS account. THE CLI SHALL reject an `up` command that would produce a colliding Workstation_Id (extremely unlikely for ULID-style IDs but rejected defensively).

---

## Feature 2: Storage Tiering

### Requirement 2.1: Three-tier storage with explicit mount points

**User Story:** As an operator, I want the workstation to have a clear storage hierarchy so that I always know what survives a stop, what does not, and why the split is drawn that way.

#### Acceptance Criteria

1. THE Workstation_Instance SHALL mount the `c8gd` Local_NVMe at `/work`. THE bootstrap SHALL format the NVMe as ext4 on first boot and re-format on every subsequent boot (since Local_NVMe is erased on stop).
2. THE Repo_Volume SHALL be mounted at `/work/repo` (directory path chosen to distinguish the mount point from any single repository clone). THE bootstrap SHALL create `/work/repo/tokeira` as the canonical Tokeira checkout path on first boot. A symlink `/work/tokeira -> /work/repo/tokeira` SHALL be created for ergonomic `cd /work/tokeira` usage.
3. THE Cache_Volume SHALL be mounted at `/work/cache` and the following subdirectories SHALL be bind-mounted into the operator's home directory: `/work/cache/cargo` → `~/.cargo`, `/work/cache/rustup` → `~/.rustup`.
4. `~/.cache/sccache` SHALL be a bind-mount to `/work/sccache`, which is a directory on the Local_NVMe (not the Cache_Volume). The rationale is latency: `sccache` hit-path reads hot artefacts on every compile, and the NVMe's native latency advantage over EBS gp3 is exactly the win `sccache` exists to capture. The cost of this choice is that sccache contents are lost on instance stop; the first build after a resume is a cold build. For a daily workflow (stop overnight, resume in the morning) this is acceptable because `cargo`'s on-disk incremental state on the Cache_Volume gives most of the benefit anyway.
5. `CARGO_TARGET_DIR` SHALL be set to `/work/target` in `/etc/profile.d/tokeira-workstation.sh`. `RUSTC_WRAPPER` SHALL be set to `sccache`. `SCCACHE_DIR` SHALL be set to `/work/sccache`. `CARGO_INCREMENTAL` SHALL be set to `1`.
6. THE cloud-init SHALL handle every mount-point bootstrap idempotently: re-running bootstrap on an already-configured instance SHALL detect existing mounts and skip their setup. The Local_NVMe is the exception — it is reformatted on every boot per Req 2.1.1.

### Requirement 2.2: Volume sizing defaults and profile control

**User Story:** As an operator, I want sensible default sizes for the cache and repo volumes with the ability to override per profile, so that small and large workspaces can both be served without paying for unused capacity.

#### Acceptance Criteria

1. THE default Workstation_Profile (`c8gd-rust`) SHALL declare: Root_Volume 20 GiB, Cache_Volume 30 GiB, Repo_Volume 40 GiB, instance type `c8gd.8xlarge`, region read from `AWS_REGION` environment or the operator's default AWS profile (typically `eu-west-2`).
2. Sizing rationale: the Cache_Volume holds `~/.cargo` (~5 GiB for the Tokeira workspace after full resolution) plus `~/.rustup` (~6 GiB for stable + nightly with rustfmt, clippy, rust-src) plus installed cargo tools (~500 MiB) with meaningful headroom for multi-toolchain workflows. The Repo_Volume holds the Tokeira checkout (~1.2 GiB including `.git`) plus headroom for several additional working-tree clones (each typically 1–3 GiB), leaving comfortable margin. The Root_Volume carries the Ubuntu 24.04 base (~4 GiB) plus installed OS packages and operator dotfiles, with ~12 GiB unused.
3. Profile fields SHALL be overridable on the `tkr workstation up` command line: `--cache-volume-gib`, `--repo-volume-gib`, `--root-volume-gib`, `--instance-type`, `--region`.
4. Profile definitions SHALL live in a Rust constant table in `crates/tokeira-aws/src/remote_workstation.rs` (see Req 6.3); configuration-file profiles are a deliberate non-goal for this spec.
5. WHERE an operator overrides the instance type to a non-`c8gd*` family, THE CLI SHALL emit a warning naming the deviation from the Tokeira-recommended baseline. The CLI SHALL NOT reject the override.

### Requirement 2.3: EBS volumes are encrypted

**User Story:** As a security-aware operator, I want the cache and repo volumes encrypted at rest, so that the Tokeira source and the build artefact cache meet baseline encryption expectations.

#### Acceptance Criteria

1. Both the Cache_Volume and Repo_Volume SHALL be provisioned with `Encrypted = true`. The KMS key SHALL be the AWS-managed default EBS key unless the profile overrides it.
2. THE root volume of the Workstation_Instance SHALL also be encrypted.
3. THE Local_NVMe is ephemeral instance-store; no EBS encryption policy applies to it. THE bootstrap SHALL NOT attempt to configure at-rest encryption on the NVMe.

---

## Feature 3: Bootstrap

### Requirement 3.1: Single-source cloud-init user-data

**User Story:** As an operator, I want the instance to be fully usable — Rust toolchain, cargo subcommands, protoc — immediately after `tkr workstation up` returns, so that I do not have to run manual setup steps.

#### Acceptance Criteria

1. THE cloud-init user-data SHALL be a single Rust-rendered script (rendered from a template in `crates/tokeira-aws/src/resources/remote_workstation_bootstrap.rs`) passed to EC2 at `RunInstances` time. The script SHALL be idempotent on re-execution.
2. THE script SHALL install, at minimum: rustup with stable + nightly toolchains pinned to the versions used by the Tokeira workspace, `cargo-nextest`, `cargo-deny`, `sccache`, `protoc`, `buf`, `uv`, `ripgrep`, `fd-find`, `jq`, `lld`, `mold`, `git`, `gh`.
3. THE bootstrap SHALL complete within 10 minutes of instance creation on a cold `c8gd.8xlarge`. THE `tkr workstation up` command SHALL poll the instance's bootstrap completion marker (`/etc/tokeira/workstation-fingerprint`) via SSM and return only when the marker is present or the 15-minute overall `up` timeout elapses.
4. IF the 15-minute timeout elapses, THE CLI SHALL surface a diagnostic pointing at the cloud-init log path on the instance and SHALL leave the instance running so the operator can debug.

### Requirement 3.2: Toolchain pin drives the bootstrap

**User Story:** As a Tokeira maintainer, I want the remote workstation to compile the exact toolchain the workspace requires, so that results on the workstation are reproducible against the MacBook-local results where relevant.

#### Acceptance Criteria

1. THE bootstrap SHALL read the MSRV and nightly-pin from `rust-toolchain.toml` at the workspace root when rendering the user-data. Both pins SHALL be applied via `rustup install` in the bootstrap.
2. WHERE `rust-toolchain.toml` declares a nightly component set (e.g. `rustfmt`, `clippy`), THE bootstrap SHALL install those components.
3. THE bootstrap SHALL NOT hardcode specific toolchain version strings inside the cloud-init script. Version strings flow from `rust-toolchain.toml` through the Bootstrap_Fingerprint computation into the user-data at render time.

### Requirement 3.3: Repository checkout is bootstrap-managed

**User Story:** As an operator, I want the remote workstation to start with a fresh clone of the Tokeira repository on the Repo_Volume so that my first `tkr workstation remote-exec cargo build` has a workspace to build.

#### Acceptance Criteria

1. WHEN the Repo_Volume is first formatted, THE bootstrap SHALL clone the Tokeira repository to `/work/tokeira` using the repository URL configured in the Workstation_Profile (default: the `origin` URL of the local checkout running `tkr workstation up`).
2. IF the Repo_Volume already contains a `/work/tokeira` directory, THE bootstrap SHALL NOT overwrite it. Keeping uncommitted work safe is a first-class bootstrap invariant.
3. THE bootstrap SHALL configure git on the instance with the local git user's name and email (read from `git config` at `tkr workstation up` time) so commits authored on the workstation carry the expected identity.
4. THE bootstrap SHALL NOT install an SSH key, a GitHub personal access token, or any other GitHub credential by default. Push-from-workstation is opt-in via `tkr workstation github-key add` per Feature 10. Read-only clone from public repositories works credential-free; private-repository clone requires the opt-in path.

---

## Feature 4: Access via SSM Session Manager

### Requirement 4.1: No public ingress, no SSH key, transient public IP

**User Story:** As a security-aware but cost-aware operator, I want zero inbound ingress and no SSH keys on the workstation, but I also want to avoid the standing cost of a dedicated NAT Gateway for a single-developer use case.

#### Acceptance Criteria

1. THE security group attached to the Workstation_Instance SHALL have ZERO ingress rules. THE egress ruleset SHALL allow all outbound (0.0.0.0/0) so `cargo` can fetch crates and `rustup` can download toolchains.
2. THE Workstation_Instance SHALL be launched into a PUBLIC subnet with `MapPublicIpOnLaunch = false` at the subnet level, so the instance receives a public IP only when explicitly associated. This keeps egress free (via the subnet's Internet Gateway route) without the $36/month standing charge of a dedicated NAT Gateway. A NAT Gateway is explicitly NOT provisioned by this spec. If the operator's VPC already has a NAT Gateway serving private subnets, the operator MAY override `--subnet-id` to select a private subnet; this is an advanced override, not the default path.
3. WHEN the instance is started (`tkr workstation up` on a stopped instance OR fresh create), THE CLI SHALL invoke `AssociateAddress` to attach a public IP — either an account-owned Elastic IP (if one exists and is tagged `tokeira-workstation-eip`) or auto-assignment via the subnet's public-IP assignment setting overridden per-request. WHEN the instance is stopped (`tkr workstation stop`), THE CLI SHALL release the public IP to avoid the stopped-EIP charge (~$3.60/month if left attached to a stopped instance).
4. THE operator SHALL reach the instance exclusively through SSM Session Manager. NO SSH key material SHALL be imported to the instance, and NO EC2 Instance Connect SSH endpoint SHALL be provisioned by this spec. The instance's public IP is used only for outbound connectivity; inbound traffic is blocked by the security group's empty ingress ruleset.

### Requirement 4.2: IAM role grants SSM agent permissions

**User Story:** As an operator, I want the instance to be managed by SSM without having to modify my account's SSM configuration.

#### Acceptance Criteria

1. THE instance profile attached to the Workstation_Instance SHALL include an IAM role with the AWS-managed policy `AmazonSSMManagedInstanceCore` attached. Additional permissions (e.g. CloudWatch Logs) SHALL be added only if justified by a specific bootstrap requirement in this spec.
2. THE instance profile SHALL be named `tokeira-workstation-<workstation-id>-profile` and SHALL be scoped to that workstation. Reusing an instance profile across workstations is explicitly rejected; it complicates least-privilege accounting.
3. THE cloud-init SHALL ensure the SSM agent (`amazon-ssm-agent`) is installed and enabled. Modern AL2023 and Ubuntu 24.04 AMIs include it by default; the bootstrap SHALL still verify its status on boot.

### Requirement 4.3: `tkr workstation ssh` opens an interactive SSM session

**User Story:** As an operator, I want an `ssh`-style command that drops me into an interactive shell on the workstation, for ad-hoc debugging that does not fit the `remote-exec` model.

#### Acceptance Criteria

1. `tkr workstation ssh` SHALL invoke `aws ssm start-session --target <instance-id>` (or the equivalent via `aws-sdk-ssm`) to open an interactive shell. The shell runs as the `ubuntu` user (for Ubuntu AMIs) or `ec2-user` (for AL2023).
2. THE CLI SHALL validate that the Session Manager plugin (`session-manager-plugin`) is installed on the operator's MacBook before attempting the session. IF missing, THE CLI SHALL print an actionable install command and exit non-zero.
3. THE session lifetime SHALL be governed by the operator's shell: `exit` or `Ctrl-D` ends the session, which SHALL bubble up to the `tkr` command as a clean exit.

### Requirement 4.4: `tkr workstation remote-exec` streams a single command

**User Story:** As an operator, I want a non-interactive command that runs one shell command on the workstation with stdout/stderr streamed back, so that I can run `cargo build` from a MacBook script without an interactive terminal in the loop.

#### Acceptance Criteria

1. `tkr workstation remote-exec <shell-command>` SHALL invoke `aws-sdk-ssm`'s `send_command` or `start_session` (choose one consistently; the design.md records the chosen API) to execute `<shell-command>` on the Workstation_Instance under `/bin/bash -lc`.
2. THE working directory on the instance SHALL default to `/work/tokeira`. Operators MAY override it with `--cwd <path>`.
3. STDOUT and STDERR from the remote command SHALL be streamed to the operator's terminal in real time as the remote command runs — not buffered to end-of-command. This is a non-negotiable usability requirement: `cargo build` emits progress over 30+ seconds on a warm cache and any noticeable lag defeats the purpose.
4. THE exit code of the remote command SHALL be the exit code of `tkr workstation remote-exec`.
5. WHEN the local operator sends SIGINT (Ctrl-C) to the `tkr` process, THE CLI SHALL cancel the SSM session and terminate the remote command (best-effort: SSM Run Command supports cancellation; Session Manager rides on the shell's own signal handling).
6. THE `remote-exec` command SHALL NOT require the operator to know the instance ID. It SHALL discover the target instance from the `--workstation` flag or `~/.tokeira/workstations/.latest`.

### Requirement 4.5: SSM port forwarding placeholder for future agentd

**User Story:** As a future `agent-controller` implementer, I want the SSM session to be capable of port-forwarding to a Unix socket on the instance, so that the future agentd can listen on `/run/tokeira-agentd.sock` and `tkr` can reach it from the MacBook without reshaping this spec.

#### Acceptance Criteria

1. THE IAM role SHALL include the SSM permissions necessary for port forwarding (`ssm:StartSession` with the document `AWS-StartPortForwardingSessionToRemoteHost` is permitted by `AmazonSSMManagedInstanceCore`). NO additional inline policy SHALL be required solely to enable port forwarding.
2. THE bootstrap SHALL create the directory `/run/tokeira-agentd/` owned by the shell user, world-executable, with group-write off. This is the directory `agent-controller` will place a Unix socket in. The directory existing at this path is the extension point this spec pre-declares; its contents are `agent-controller`'s concern.
3. `tkr` SHALL NOT implement port-forwarding wiring in this spec. `tkr workstation remote-exec` and `tkr workstation ssh` are the only SSM-driven commands this spec ships. Forwarding is the agent-controller spec's first task.

---

## Feature 5: Idle Shutdown and Cost Observability

### Requirement 5.1: Idle-shutdown watchdog

**User Story:** As a cost-conscious operator, I want the workstation to stop automatically after a period of idleness, so that forgetting to run `tkr workstation stop` does not produce a six-figure monthly surprise.

#### Acceptance Criteria

1. THE bootstrap SHALL install a `systemd` timer + service pair named `tokeira-workstation-idle.timer` / `tokeira-workstation-idle.service`. The timer SHALL fire every 5 minutes.
2. THE service SHALL evaluate two conditions: (a) 1-minute CPU load average below the configured threshold (default: 0.5), (b) no active SSM Session Manager session on the instance (checked via `/var/lib/amazon/ssm/...` session directory or equivalent).
3. IF both conditions hold for N consecutive firings (default: 6, giving a 30-minute idle window) AND the profile's `idle_shutdown_enabled = true`, THEN the service SHALL invoke `shutdown -h now`. This triggers a clean stop and preserves EBS per Requirement 1.2.
4. THE idle window and enablement SHALL be configurable per Workstation_Profile: `idle_shutdown_minutes` (default 30, range 0 to disable up to 1440) and `idle_shutdown_enabled` (default true).
5. THE operator SHALL be able to trigger an ad-hoc cancellation of the watchdog's next firing via `tkr workstation idle --defer 2h` (shorthand: writes a sentinel file the service checks). This is a convenience affordance for long-running unattended builds; the bootstrap installs the support but `tkr workstation idle` is shippable in this spec's Feature 1 CLI surface.

### Requirement 5.2: Cost observability on `tkr workstation status`

**User Story:** As a cost-conscious operator, I want `tkr workstation status` to show approximate dollar cost per hour and cumulative uptime for each workstation, so that I see at a glance whether I left something running.

#### Acceptance Criteria

1. `tkr workstation status` SHALL print the Workstation_Id, state, instance type, region, IAM role, security group ID, volume IDs, bootstrap fingerprint, current uptime (for running instances), and an approximate dollar-per-hour cost rate.
2. THE cost rate SHALL be computed from a built-in Rust constant table mapping (instance type, region) → hourly on-demand rate. THE table SHALL be last-updated at spec-implementation time; staleness is an accepted cost in exchange for not having a runtime dependency on the AWS Pricing API.
3. WHERE no rate is known for a given (type, region) pair, THE status command SHALL print `cost rate: unknown (not in local table)` rather than omit the field. The placeholder makes the stale-table case visible.
4. THE status command SHALL also print cumulative instance-hours across all historical uptimes, drawn from a local state file `~/.tokeira/workstations/<workstation-id>/uptime-log.jsonl`. Every `start` / `stop` event SHALL append one line. The running total is advisory (it ignores time between `tkr` invocations) but gives a rough operating-cost signal.

---

## Feature 6: CLI Integration and Deployment Surface

### Requirement 6.1: `tkr workstation` subcommand group

**User Story:** As an operator, I want the workstation commands organised under one clap subcommand group, consistent with the existing `tkr deployment`, `tkr infra`, `tkr dev` groups, so that the CLI surface is discoverable through `--help`.

#### Acceptance Criteria

1. `apps/tkr/src/cli.rs` SHALL gain a `Workstation { #[command(subcommand)] action: WorkstationAction }` variant on the top-level `Command` enum.
2. `WorkstationAction` SHALL declare variants: `Up`, `Stop`, `Destroy`, `Ssh`, `RemoteExec`, `Status`, `List`, `Bootstrap`, `Idle`. The `Bootstrap` variant SHALL be an explicit re-run of the bootstrap drift-detection path per Req 1.4; the `Idle` variant implements Req 5.1.5.
3. Every subcommand SHALL accept `--workstation <workstation-id>` and read `~/.tokeira/workstations/.latest` as the default, per Req 1.5.1.
4. The dispatch in `apps/tkr/src/main.rs` SHALL route each variant to a handler module under `apps/tkr/src/commands/workstation/`.

### Requirement 6.2: Handlers delegate to a workstation engine

**User Story:** As a Tokeira maintainer, I want the CLI handlers to be thin wrappers over a `Workstation` engine in `tokeira-aws`, so that non-CLI consumers (future `agent-controller`) reuse the same lifecycle surface.

#### Acceptance Criteria

1. THE `tokeira-aws` crate SHALL gain a `remote_workstation` module exposing: `WorkstationProfile`, `WorkstationHandle`, and async methods `up`, `stop`, `destroy`, `remote_exec`, `status`, `list`, `bootstrap`, `idle_control`.
2. THE CLI handlers SHALL contain only argument translation, output formatting, and confirmation logic — no direct AWS SDK calls. All SDK work lives inside the engine.
3. THE engine SHALL call the AWS SDK directly via `aws-sdk-ec2` and `aws-sdk-ssm`. Lifecycle operations (`up`, `stop`, `destroy`) sequence explicit SDK calls against the AWS control plane; no `tokeira-iac` `Engine::apply` / `Engine::destroy` machinery is involved.

### Requirement 6.3: Direct AWS SDK with tag-based state

**User Story:** As a Tokeira maintainer, I want the remote-workstation module to use the AWS SDK directly rather than plumb through the `tokeira-iac` `Module`/`Resource` engine, so that the one-instance-with-two-volumes topology does not carry the weight of multi-resource composition, state-document persistence, or project-wiring overhead that `tokeira-iac` exists to manage for larger deployments.

#### Acceptance Criteria

1. THE module SHALL live at `crates/tokeira-aws/src/remote_workstation.rs` (a single file at the crate root, NOT under `crates/tokeira-aws/src/resources/`). The file-location convention matters: `resources/` holds `tokeira_iac::Resource` impls registered with the IaC engine; top-level crate files hold direct-SDK wrappers. The remote workstation is the latter.
2. THE source of truth for workstation state SHALL be AWS tags on the live EC2 resources. The tag `tokeira-workstation=true` plus `workstation-id=<id>` identifies workstation-owned resources; discovery proceeds via `DescribeInstances` / `DescribeVolumes` / `DescribeSecurityGroups` filtered by those tags.
3. THE local `~/.tokeira/workstations/<workstation-id>/` directory SHALL be a performance cache only. Any conflict between cache and AWS state SHALL resolve to AWS (Req 7.1.1). NO project-level IaC state document (`InfraState`, `InfraStateStore`, or equivalent) SHALL be introduced by this spec.
4. THE module SHALL NOT register itself as a `tokeira_iac::Module` or appear in any `InfraComposition`. `tkr workstation` commands SHALL NOT import `tokeira_iac::Engine`.
5. Reasons for rejecting the `tokeira-iac` pattern are documented inline in the module's crate-level doc comment: (a) the topology is one instance + two EBS volumes + one security group + one IAM role + one instance profile, all with fixed wiring; (b) AWS tags already provide authoritative state; (c) `tokeira-iac`'s value (dependency ordering across heterogeneous resource families, cross-module composition, drift detection on complex state) does not apply to a single-shape developer workstation. If a subsequent spec introduces closely-related workstation-like resources that justify it, migrating onto `tokeira-iac` is a straightforward follow-up — the engine methods become `Resource::create/delete` bodies and tags remain authoritative.

---

## Feature 7: State Cache and Reconciliation

### Requirement 7.1: `~/.tokeira/workstations/` as a performance cache only

**User Story:** As a Tokeira maintainer, I want the MacBook-local state cache to never be the source of truth for workstation state, so that a stale or corrupted cache does not mislead the operator into believing stopped instances are running or vice versa.

#### Acceptance Criteria

1. Every `tkr workstation` command SHALL reconcile the local state file against AWS before returning to the operator. If AWS reports a state inconsistent with the local cache, AWS wins and the cache is overwritten.
2. THE `~/.tokeira/workstations/<workstation-id>/` directory SHALL carry: `state.json` (cached tags, volume IDs, instance type, region, AMI ID, last-seen state), `uptime-log.jsonl` (append-only Req 5.2.4), `bootstrap-fingerprint.txt`.
3. THE `~/.tokeira/workstations/.latest` sentinel SHALL be a single line containing the last Workstation_Id the operator acted on. NO other state SHALL live in `.latest`.
4. WHEN the operator deletes `~/.tokeira/workstations/` manually, `tkr workstation list` SHALL rebuild the directory from AWS tags on the next invocation without operator intervention. The local cache is always reconstructible.

### Requirement 7.2: Bootstrap fingerprint round-trip

**User Story:** As a Tokeira maintainer, I want the bootstrap fingerprint stored both locally and on the instance so that drift detection (Req 1.4) is straightforward and symmetric.

#### Acceptance Criteria

1. THE local Bootstrap_Fingerprint SHALL be the SHA-256 of: (a) the rendered bootstrap script bytes, (b) the content of `rust-toolchain.toml`, (c) a monotonic bootstrap-schema version (`BOOTSTRAP_SCHEMA = "v1"`).
2. THE instance SHALL store the fingerprint at `/etc/tokeira/workstation-fingerprint`, world-readable, written by the bootstrap.
3. THE `tkr workstation up` command SHALL retrieve the instance fingerprint via `ssm:SendCommand` running `cat /etc/tokeira/workstation-fingerprint` and compare it to the local computed fingerprint.
4. A fingerprint mismatch SHALL trigger a bootstrap refresh per Req 1.4.

---

## Feature 8: Extension Points for `agent-controller`

### Requirement 8.1: Declare the Unix socket path convention

**User Story:** As a future `agent-controller` implementer, I want the workstation to expose a known filesystem path where agentd will place its Unix socket, so that I do not need to re-open this spec to decide the convention.

#### Acceptance Criteria

1. THE path `/run/tokeira-agentd/agentd.sock` SHALL be the designated Unix-socket path for the future agentd daemon. The bootstrap SHALL create `/run/tokeira-agentd/` (but not the socket file itself) with ownership `ubuntu:ubuntu` (or `ec2-user:ec2-user` on AL2023) and mode `0750`.
2. `agent-controller` SHALL bind its agentd daemon to this path. Rebinding to a different path in a future spec SHALL be documented as a breaking change to `agent-controller`, not to this spec.

### Requirement 8.2: No agent-related code in this spec

**User Story:** As a Tokeira maintainer, I want to enforce the scope separation so that `agent-controller` is built on a clean foundation.

#### Acceptance Criteria

1. NO crate in this spec SHALL depend on Codex, the OpenAI Agents SDK, or any agent-specific AWS permission.
2. THE workstation bootstrap SHALL NOT pre-install Codex or configure agent credentials. Doing so is the first task of `agent-controller`.
3. NO `tkr agent *` subcommand SHALL be introduced by this spec. `tkr workstation` is the only new command group.

---

## Feature 9: Correctness Properties

### Requirement 9.1: `up` is idempotent

**User Story:** As a Tokeira maintainer, I want a property test asserting that repeated `up` invocations produce at most one Workstation_Instance per AWS account, so that the combination of AWS API retries and operator-driven retries never produces orphan infrastructure.

#### Acceptance Criteria

1. GIVEN an initial state of zero Workstation_Instances and a sequence of N `tkr workstation up --profile <p>` invocations (possibly interleaved with `stop`, `start`, and flaky-AWS simulations), THE final state SHALL contain exactly one Workstation_Instance with the matching profile tags.
2. THE test SHALL be implemented against a mock `aws-sdk-ec2` / `aws-sdk-ssm` client (not a live AWS account). It SHALL live under `crates/tokeira-aws/tests/remote_workstation_idempotence.rs`.
3. THE property SHALL be expressed as a `proptest` strategy over command sequences with at least 64 iterations.

### Requirement 9.2: Destroy is total

**User Story:** As a Tokeira maintainer, I want a property test asserting that after `destroy` no AWS resource tagged for that Workstation_Id remains, so that destroy never leaves orphan volumes or security groups.

#### Acceptance Criteria

1. GIVEN an initial mock AWS state with one Workstation_Instance and its associated resources, after `tkr workstation destroy`, NO mock resource with the `workstation-id` tag SHALL remain.
2. THE test SHALL tolerate intermediate AWS failures (injected mock errors on individual resource deletions); destroy SHALL log and continue per Req 1.3.3.
3. THE test SHALL live under `crates/tokeira-aws/tests/remote_workstation_destroy.rs`.

### Requirement 9.3: Bootstrap fingerprint is deterministic

**User Story:** As a Tokeira maintainer, I want a property test asserting that the Bootstrap_Fingerprint is a deterministic function of its inputs, so that drift detection never false-triggers.

#### Acceptance Criteria

1. GIVEN two computations of the Bootstrap_Fingerprint with identical inputs, the fingerprints SHALL be byte-equal.
2. GIVEN two computations with any single-byte difference in the bootstrap script, `rust-toolchain.toml`, or the schema version, the fingerprints SHALL NOT be equal.
3. THE test SHALL live under `crates/tokeira-aws/tests/remote_workstation_fingerprint.rs`.

### Requirement 9.4: CLI defaults stay sane

**User Story:** As a Tokeira maintainer, I want a property test asserting that every `tkr workstation` subcommand either resolves a workstation via `--workstation` or `.latest` or surfaces a clear error, so that the default-resolution path never crashes with a null dereference.

#### Acceptance Criteria

1. GIVEN an arbitrary state of `~/.tokeira/workstations/` (empty, stale, corrupted, missing `.latest`, `.latest` pointing at a nonexistent Workstation_Id), every subcommand SHALL return `Ok(_)` with a cleanly-formatted status output OR `Err(_)` with a message naming the resolution problem. NO subcommand SHALL panic.
2. THE test SHALL live under `apps/tkr/tests/workstation_resolution.rs`.

---

## Feature 10: GitHub Credential Policy

The workstation is a Rust build surface. It is NOT a git-push surface by default. This feature establishes the credential-free default posture and provides an opt-in path for operators who do need to commit and push from the workstation without exposing their main GitHub credentials.

Feature ordering note: Feature 10 follows the Correctness Properties in this spec's ordering because it was added after the initial feature enumeration was drafted. The in-spec cross-references in design.md and tasks.md (which cite Req 9.x) remain valid because no existing requirement was renumbered.

### Requirement 10.1: No GitHub credentials by default

**User Story:** As a security-aware operator, I want the remote workstation to carry no GitHub credentials by default, so that an instance compromise — whether through a supply-chain attack on a crate, a lateral movement from another AWS resource, or an SSM session hijack — cannot exfiltrate my GitHub access.

#### Acceptance Criteria

1. THE bootstrap SHALL NOT install any SSH key, GitHub personal access token, OAuth token, or GitHub App installation token on the instance.
2. THE `gh` CLI SHALL be installed as part of the standard bootstrap tool set (per Req 3.1.2) but SHALL NOT be pre-authenticated. Running `gh` on the instance with no prior auth SHALL produce the standard unauthenticated error (`error: You are not logged into any GitHub hosts...`).
3. IF the operator's `repo_url` points at a public repository (HTTPS URL resolvable without authentication), the initial clone in the bootstrap (per Req 3.3.1) SHALL succeed without any credential.
4. IF the operator's `repo_url` points at a private repository, the initial clone SHALL fail with a clear message that names `tkr workstation github-key add` as the next step. The bootstrap SHALL NOT retry, fall back to a different auth method, or prompt for credentials interactively — fail loudly and cleanly.
5. THE instance's IAM role SHALL NOT include any permissions beyond those required for SSM Session Manager (Req 4.2.1). In particular, it SHALL NOT include `secretsmanager:GetSecretValue`, `ssm:GetParameter` for a GitHub-related parameter, or any AWS permission that would let a compromised instance pivot to operator-owned credentials stored elsewhere in the account.

### Requirement 10.2: Opt-in workstation-scoped SSH deploy key

**User Story:** As an operator who genuinely needs to commit and push from the workstation — for example, during an interactive `tkr workstation ssh` session where I'm iterating on a fix and want to cut a PR without round-tripping to my MacBook — I want an explicit command that provisions a per-workstation SSH deploy key, so that push access is available when I need it but OFF by default, and tightly scoped to one repository.

#### Acceptance Criteria

1. `tkr workstation github-key add --repo <owner/name>` SHALL generate an ed25519 keypair on the Workstation_Instance at `~/.ssh/tokeira-workstation-<workstation-id>` (private key) and `~/.ssh/tokeira-workstation-<workstation-id>.pub` (public key). Both files SHALL live on the Cache_Volume (survives instance stop) with mode `0600` / `0644` respectively.
2. IF `--repo` is omitted, THE CLI SHALL extract the owner and name from the workstation's `repo_url` configured at creation time. IF that URL does not resolve to a single owner/name pair (e.g. a file:// URL), the command SHALL fail with a clear message requesting `--repo` explicitly.
3. THE public-key upload to GitHub SHALL run from the **operator's MacBook**, using the operator's existing MacBook-side `gh auth` state, NOT from the Workstation_Instance. The command the CLI invokes is `gh api --method POST repos/<owner>/<repo>/keys -f title=tokeira-workstation-<workstation-id> -f key="<pubkey>" -F read_only=false` executed on the MacBook. THE operator's primary GitHub token (laptop-resident) SHALL NOT travel to the workstation at any point.
4. THE deploy key's title SHALL be `tokeira-workstation-<workstation-id>`, embedding the Workstation_Id so keys are distinguishable in the GitHub UI under Settings → Deploy keys AND so a follow-up `tkr workstation destroy` can locate and remove the correct key.
5. AFTER the key is accepted by GitHub, `tkr workstation github-key add` SHALL configure `~/.ssh/config` on the instance with a `github.com` stanza routing through the generated key and SHALL rewrite any `https://github.com/<owner>/<repo>(.git)?` remotes in `/work/tokeira` to their `git@github.com:<owner>/<repo>.git` equivalents via `git remote set-url origin`. Subsequent `git push` operations on the instance SHALL succeed using the deploy key.
6. THE deploy key SHALL be added with `read_only=false`. The operator may explicitly request read-only with `tkr workstation github-key add --read-only`; the CLI SHALL respect the override and skip the remote rewrite in Req 10.2.5 (read-only access works fine with HTTPS clones too).
7. `tkr workstation github-key remove` (companion command) SHALL reverse the `add` sequence: call `gh api --method DELETE repos/<owner>/<repo>/keys/<key-id>` from the MacBook to remove the deploy key, rewrite the instance remotes back to HTTPS, and delete the keypair files from the instance.
8. `tkr workstation destroy` SHALL invoke the `github-key remove` path (best-effort, per Req 1.3.3) for every deploy key tagged with the workstation's ID across every repository the key was added to. A local registry at `~/.tokeira/workstations/<id>/deploy-keys.jsonl` records each `add` call so `destroy` knows where to look. Failure to remove a key SHALL log a warning with explicit instructions: "Remove the orphan deploy key manually at https://github.com/<owner>/<repo>/settings/keys — look for one titled `tokeira-workstation-<id>`."

### Requirement 10.3: Secret-in-command warning on `remote-exec`

**User Story:** As a security-aware operator, I want `tkr workstation remote-exec` to warn me if my command string looks like it contains a secret, so that I don't accidentally expose a token by passing it into an SSM Run Command invocation (which is logged to CloudTrail with the command text).

#### Acceptance Criteria

1. WHEN `tkr workstation remote-exec <command>` is invoked, THE CLI SHALL pattern-match the command string against a set of known secret-substring heuristics: `gh auth login --with-token`, `GITHUB_TOKEN=`, `-H "Authorization: Bearer`, `AWS_SECRET_ACCESS_KEY=`, and a small set of similar patterns. The exact list SHALL be embedded as a Rust constant and updated only by a spec-change.
2. IF a match is found, THE CLI SHALL print a warning to stderr before executing and require either `--yes-secret-in-command` confirmation OR an interactive y/N prompt (default N). The warning SHALL read: "The command looks like it contains a secret. SSM Run Command invocations are logged to CloudTrail with the full command text. Use `tkr workstation ssh` for interactive secret entry instead. Proceed anyway? [y/N]"
3. WHERE the operator genuinely needs to pass a secret for legitimate reasons (e.g. passing an ad-hoc PAT for a one-time clone), THE documented path in the operator guide SHALL be: `tkr workstation ssh`, then type the secret-bearing command interactively; SSH sessions via Session Manager are not Run Command invocations and do not embed the command text in CloudTrail.

### Requirement 10.4: SSH host-key pinning for GitHub

**User Story:** As a security-aware operator, I want the instance's first connection to `github.com` over SSH to verify against pre-pinned GitHub host keys, so that a man-in-the-middle attack on the NAT path cannot masquerade as GitHub on trust-on-first-use.

#### Acceptance Criteria

1. THE cloud-init bootstrap SHALL write GitHub's published SSH host keys to `~/.ssh/known_hosts` during Phase 2 (tool installation) for the shell user. Keys SHALL be sourced from GitHub's documented fingerprint set (`https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints`) and embedded as a Rust constant in `remote_workstation_bootstrap.rs`. The keys are published under a stable identity and rotate infrequently; a rotation event is a spec-level change.
2. THE bootstrap SHALL ensure `StrictHostKeyChecking yes` is set for `github.com` in `~/.ssh/config`, so that the first SSH connection from the instance fails hard if the host key does not match the pinned set, rather than prompting the operator with a non-interactive TOFU dialog that SSM-session users cannot answer.
3. GitHub's published host keys SHALL be updated in `remote_workstation_bootstrap.rs` whenever GitHub publishes a rotation announcement. A test in `crates/tokeira-aws/tests/remote_workstation_host_keys.rs` SHALL assert that the embedded keys parse as valid OpenSSH public keys; the test is structural, not a freshness check.

### Requirement 10.5: No GitHub credential on the instance role

**User Story:** As a security-aware operator, I want the instance's AWS IAM role to have no path to fetch GitHub credentials from AWS, so that even if the instance is compromised, there is no AWS-side credential chain to pivot through.

#### Acceptance Criteria

1. THE workstation's IAM role (per Req 4.2.1) SHALL attach only the AWS-managed policy `AmazonSSMManagedInstanceCore`. No inline policy SHALL be attached.
2. `Secrets Manager`, `Parameter Store`, `IAM`, and `sts:AssumeRole` permissions SHALL NOT be present on the instance role under this spec. IF a future spec (e.g. `agent-controller`) needs any of these permissions, that spec SHALL add them explicitly with justification in its own requirements.
3. THE bootstrap SHALL NOT call any AWS service that requires credentials beyond SSM core (e.g. `aws secretsmanager get-secret-value`, `aws ssm get-parameter`) during its execution. The only AWS permissions the bootstrap exercises are the ones SSM Session Manager itself uses.
