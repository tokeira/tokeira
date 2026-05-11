# Implementation Plan: Remote Workstation

## Overview

Implement the remote-workstation CLI and engine end to end so an operator can run `tkr workstation up && tkr workstation remote-exec "cargo build --workspace"` against a Graviton4 `c8gd.8xlarge` in `eu-west-2` with sub-2-minute cold builds and a monthly bill around $390.

The plan follows the five-step arc laid out in design.md:

1. Engine-layer skeleton in `tokeira-aws` — SDK client wiring, `WorkstationProfile`, `WorkstationHandle`, `Workstation` methods as `unimplemented!()` stubs that compile. Unblocks the CLI layer's type-checking.
2. Lifecycle methods implemented against the SDK — `up`, `stop`, `destroy`, `status`, `list`, `bootstrap`. This is the bulk of the engine work.
3. SSM surface — `ssh` (aws CLI subprocess) and `remote_exec` (`SendCommand` polling with stream).
4. CLI handlers in `apps/tkr` and clap surface extension in `cli.rs`. Thin wrappers over the engine.
5. Cloud-init bootstrap, idle-shutdown watchdog, correctness-property tests.

Task groups 1–4 are the v1 "you can build Tokeira on this" deliverable. Task group 5 closes the spec; if MVP-cut is needed, group 5 is the deferrable surface.

Target crates and files:

- `crates/tokeira-aws/Cargo.toml` — add `aws-sdk-ssm` dep.
- `crates/tokeira-aws/src/lib.rs` — add `pub mod remote_workstation`.
- `crates/tokeira-aws/src/remote_workstation.rs` — new module (engine).
- `crates/tokeira-aws/src/remote_workstation_bootstrap.rs` — new module (cloud-init renderer).
- `crates/tokeira-aws/tests/remote_workstation_*.rs` — property + example tests.
- `apps/tkr/Cargo.toml` — add `humantime`, `which` (already present).
- `apps/tkr/src/cli.rs` — add `Workstation` variant and `WorkstationAction`.
- `apps/tkr/src/commands/workstation/` — new directory with 10 handler files.
- `apps/tkr/src/main.rs` — dispatch for `Workstation` variant.
- `apps/tkr/tests/workstation_resolution.rs` — proptest on CLI defaults.

## Tasks

- [ ] 1. Engine-layer skeleton in `tokeira-aws`
  - [ ] 1.1 Add `aws-sdk-ssm = "1"` to `crates/tokeira-aws/Cargo.toml` dependencies
    - Dep is needed for SSM-based access (§4 of design) and fingerprint drift detection (§3.4). `aws-sdk-ec2` and `aws-sdk-iam` are already declared.
    - (Req 6.2.3)
  - [ ] 1.2 Declare `pub mod remote_workstation` in `crates/tokeira-aws/src/lib.rs`
    - Add the module at the top level (NOT under `resources/`) with a doc comment that cross-references `design.md` §7 for the "why not `tokeira-iac`" rationale.
    - (Req 6.3.1)
  - [ ] 1.3 Implement the public type surface in `remote_workstation.rs`
    - `WorkstationProfile` with the c8gd-rust defaults (Req 2.2.1, §"Data Models" of design.md).
    - `WorkstationHandle` carrying every AWS ID the engine needs (Req 7.1.2).
    - `InstanceState`, `UpOutcome`, `BootstrapDrift`, `WorkstationStatus`, `WorkstationSummary` enums/structs per design.md §2.
    - `WorkstationError` `thiserror` enum per design.md §2.
    - `COST_RATE_TABLE` constant with the eu-west-2 and us-east-1 `c8gd.8xlarge` rates verified against live AWS pricing (eu-west-2: $1.87776, us-east-1: $1.56768 as of 2026-05).
    - `hourly_rate(region, instance_type) -> Option<f64>` helper.
    - (Req 5.2.2, 5.2.3, 6.2.1)
  - [ ] 1.4 Implement `Workstation::new(region) -> Result<Self, WorkstationError>`
    - Loads AWS config via `aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region.into())).load().await`.
    - Builds the three SDK clients: ec2, ssm, iam.
    - Stores the configured region for later tagging and cost lookup.
    - (Req 6.2.1)
  - [ ] 1.5 Stub all lifecycle methods as `unimplemented!()`
    - Signatures for `up`, `stop`, `destroy`, `status`, `list`, `remote_exec`, `bootstrap`, `idle_defer` per design.md §2.
    - This locks the surface and lets the CLI layer proceed in parallel if needed.
    - (Req 6.2.1)

- [x] 2. Checkpoint — engine surface compiles
  - Ensure `cargo check -p tokeira-aws` is green; the CLI layer can now target the `Workstation` API even before individual methods are implemented.

- [ ] 3. Lifecycle methods implemented
  - [ ] 3.1 Implement `Workstation::list` — baseline discovery
    - Calls `ec2.describe_instances` with filter `tag:tokeira-workstation=true`.
    - Maps each matching `Instance` to `WorkstationSummary { workstation_id, instance_id, state, instance_type, uptime, hourly_cost_usd_rate }`.
    - Reads `workstation_id` from the instance's tag list.
    - Returns empty Vec if no matches.
    - (Req 1.5.2)
  - [ ] 3.2 Implement `Workstation::status` for an existing workstation
    - Accepts `workstation_id: &str`.
    - Calls `ec2.describe_instances` with `tag:workstation-id=<id>` filter.
    - Resolves the instance, then queries volume attachments via `BlockDeviceMappings` on the instance plus `DescribeVolumes` for size and state.
    - Reads `bootstrap-fingerprint` tag from the instance.
    - Reads `~/.tokeira/workstations/<id>/uptime-log.jsonl` for `cumulative_uptime_hours`.
    - Looks up `hourly_cost_usd` via `hourly_rate`. `None` if unknown.
    - Returns `WorkstationStatus`.
    - (Req 5.2.1, 5.2.3, 7.1.1, 7.1.2)
  - [ ] 3.3 Implement `Workstation::up` — the core engine method
    - Step 1: Discover — call `ec2.describe_instances` with the discovery filter from Req 1.1.1. Enumerate matches.
    - Step 2a: Zero matches → fresh create path per design.md §3.1.
      - Generate `workstation_id` via `ulid::Ulid::new()`.
      - Create IAM role `tokeira-workstation-<id>-role` with `sts:AssumeRole` trust policy for `ec2.amazonaws.com`.
      - `iam.attach_role_policy` to attach `arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore`.
      - Create instance profile and add role.
      - Create security group with zero inbound rules and all egress.
      - `ec2.create_volume` × 2 for Cache_Volume and Repo_Volume (gp3, encrypted, sized per profile, tagged `tokeira-workstation`, `workstation-id`, `Name=Cache` or `Repo`).
      - Resolve AMI ID via SSM Parameter Store (design.md §3.1 "AMI resolution").
      - Resolve public subnet via `ec2.describe_subnets` (design.md §3.1 "Subnet discovery").
      - Render bootstrap script via `remote_workstation_bootstrap::render(BootstrapContext)` (landed in task 5.1).
      - `ec2.run_instances` with AMI, subnet, security group, instance profile, root-volume block-device mapping, user-data.
      - Wait for instance state `Running` (use `ec2.describe_instances` polling up to 5-minute timeout).
      - Attach Cache_Volume to `/dev/sdf`, Repo_Volume to `/dev/sdg` via `ec2.attach_volume`.
      - Poll `/etc/tokeira/workstation-fingerprint` via `ssm.send_command` with shell `cat`. Loop until present or 15-minute bootstrap-completion timeout.
      - Write `state.json` to `~/.tokeira/workstations/<id>/`.
      - Append `{"event": "create", "at": "<iso>"}` to `uptime-log.jsonl`.
      - Return `UpOutcome::Created`.
    - Step 2b: One match, `Stopped` → resume path.
      - Compute local Bootstrap_Fingerprint via `remote_workstation_bootstrap::fingerprint(…)` (task 5.1).
      - `ec2.start_instances`; wait for Running.
      - `ec2.allocate_address` + `ec2.associate_address` for a fresh EIP (or reuse if tagged `tokeira-workstation-eip`).
      - `ssm.send_command` to `cat /etc/tokeira/workstation-fingerprint`; compare with local.
      - If mismatch, call `self.bootstrap(workstation_id, profile)` (task 3.7).
      - Append `{"event": "start", "at": "<iso>"}` to `uptime-log.jsonl`.
      - Update `state.json`.
      - Return `UpOutcome::Resumed { bootstrap_drift }`.
    - Step 2c: One match, `Running` → return `UpOutcome::AlreadyRunning`.
    - Step 2d: One match, transitional → wait 5 min re-poll, then re-evaluate.
    - Step 2e: Multiple matches → return `WorkstationError::AmbiguousMatch(ids)`.
    - (Req 1.1.1 through 1.1.7, 4.1.3, 7.1)
  - [ ] 3.4 Implement `Workstation::stop`
    - Discover workstation by ID (`tag:workstation-id` filter).
    - Print the stop-warning text to stderr listing `/work/target` and `/work/sccache` (Req 1.2.3).
    - `ec2.stop_instances` with `Hibernate=false`.
    - Wait for instance state `Stopped` (up to 2-minute timeout).
    - Look up the engine-allocated EIP by tag and `ec2.disassociate_address` + `ec2.release_address`. Skip if no engine-allocated EIP.
    - Append `{"event": "stop", "at": "<iso>"}` to `uptime-log.jsonl`.
    - Update `state.json` with `last_seen_state = "Stopped"`.
    - (Req 1.2, 4.1.3)
  - [ ] 3.5 Implement `Workstation::destroy`
    - Discover workstation by ID.
    - `ec2.terminate_instances`. Wait for `Terminated`.
    - `ec2.delete_volume` for Cache_Volume, Repo_Volume (root volume goes with the instance). Individual failures log and continue (Req 1.3.3).
    - `ec2.delete_security_group`. Tolerate `DependencyViolation`.
    - `iam.remove_role_from_instance_profile` + `iam.delete_instance_profile`.
    - `iam.detach_role_policy` + `iam.delete_role`.
    - Release any tagged Elastic IP.
    - Remove `~/.tokeira/workstations/<id>/` directory.
    - Clear `~/.tokeira/workstations/.latest` if it pointed at this workstation.
    - (Req 1.3)
  - [ ] 3.6 Implement `Workstation::bootstrap` (drift-refresh)
    - Compute local Bootstrap_Fingerprint.
    - Read remote fingerprint via SSM `cat /etc/tokeira/workstation-fingerprint || echo MISSING`.
    - If match, return `BootstrapDrift::UpToDate`.
    - If mismatch, render the bootstrap script via `remote_workstation_bootstrap::render()` and send it via `ssm.send_command` as a shell script.
    - Poll `ssm.get_command_invocation` until terminal. Error out on Failed/Cancelled/TimedOut with the command's `StandardErrorContent` attached.
    - Return `BootstrapDrift::Drift { local, remote }`.
    - (Req 1.4, 7.2)
  - [ ] 3.7 Implement `Workstation::idle_defer`
    - Computes `until_epoch = until.timestamp()`.
    - Issues `ssm.send_command` running `echo <until_epoch> > /var/lib/tokeira/idle-defer.timestamp`.
    - Waits for Success.
    - (Req 5.1.5)

- [x] 4. Checkpoint — lifecycle functional end-to-end against a mocked AWS
  - All unit tests (mocked SDK clients) pass. A live-AWS smoke test is deferred to task 6.x but `cargo check && cargo test` is green for the mocked path.

- [ ] 5. SSM surface — `ssh` and `remote_exec`
  - [ ] 5.1 Implement `Workstation::remote_exec`
    - Signature per design.md §2: accepts `workstation_id`, `cwd`, `command: &[String]`, plus `stdout` and `stderr` writers.
    - Resolve instance ID from workstation ID.
    - Build the shell command: `cd <shell-escape-cwd> && <shell-escape-joined-command>`. Use the `shell-escape` crate (or `shlex`) — do NOT naively interpolate untrusted strings per AGENTS.md safety_guardrails.
    - `ssm.send_command` with document `AWS-RunShellScript`, parameters `{"commands": ["bash -lc <escaped>"]}`, `instance_ids: [instance_id]`.
    - Poll `ssm.get_command_invocation` at 500 ms intervals. On each poll, diff `StandardOutputContent` and `StandardErrorContent` against the last-seen values and write the deltas to the caller's writers.
    - Continue until terminal status. Return the `ResponseCode` as the exit code.
    - On SIGINT (via `tokio::signal::ctrl_c`), call `ssm.cancel_command` with the outstanding command ID.
    - (Req 4.4.1 through 4.4.6)
  - [ ] 5.2 Verify `session-manager-plugin` presence
    - Add a helper `Workstation::ensure_session_manager_plugin()` that calls `which::which("session-manager-plugin")`. Return `WorkstationError::SessionManagerPluginMissing` with the install URL if missing.
    - (Req 4.3.2)
  - [ ] 5.3 Implement `Workstation::start_interactive_session` helper for `ssh`
    - Resolve instance ID.
    - Call `ensure_session_manager_plugin()` first.
    - Exec `aws ssm start-session --target <instance-id> --region <region>` via `tokio::process::Command`, inheriting stdin/stdout/stderr so it becomes the operator's interactive shell.
    - Wait for the subprocess to exit; return its exit code.
    - (Req 4.3.1, 4.3.3)

- [ ] 6. CLI layer — `tkr workstation` subcommand group
  - [ ] 6.1 Extend `apps/tkr/src/cli.rs`
    - Add `Workstation { #[command(subcommand)] action: WorkstationAction }` to the top-level `Command` enum.
    - Declare `WorkstationAction` with all 9 variants per design.md §1 (`Up`, `Stop`, `Destroy`, `Ssh`, `RemoteExec`, `Status`, `List`, `Bootstrap`, `Idle`).
    - Use `#[arg(trailing_var_arg = true)]` on `RemoteExec.command`.
    - Document each subcommand with `#[doc]` strings that surface under `tkr workstation --help`.
    - (Req 6.1.1, 6.1.2)
  - [ ] 6.2 Create `apps/tkr/src/commands/workstation/mod.rs`
    - Declare all handler submodules (one per action).
    - Implement `resolve_workstation_id(override: Option<&str>) -> Result<String, anyhow::Error>` used by every handler to read `--workstation` or fall back to `~/.tokeira/workstations/.latest`. Missing-latest + no-override is a descriptive error, not a panic.
    - Implement `load_profile(profile_name: &str) -> WorkstationProfile` that consults `WorkstationProfile::by_name` with a helpful error if the name is unknown.
    - (Req 6.1.3, 7.1.3)
  - [ ] 6.3 Implement handler `apps/tkr/src/commands/workstation/up.rs`
    - Parse the `Up` args into overrides on `WorkstationProfile`.
    - Instantiate `Workstation::new(region)`.
    - Call `workstation.up(&profile, args.workstation.as_deref())`.
    - On Ok, format the `UpOutcome` to the operator's terminal (JSON if `--json`, otherwise human-readable with the workstation ID, bound public IP, cost rate, drift status).
    - Write `~/.tokeira/workstations/.latest` with the new ID.
    - (Req 1.1, 1.7)
  - [ ] 6.4 Implement `stop.rs`
    - `resolve_workstation_id` → `workstation.stop(id)`.
    - (Req 1.2)
  - [ ] 6.5 Implement `destroy.rs`
    - `resolve_workstation_id`.
    - Unless `--yes`, print the confirmation prompt: "Destroy workstation <id>? This deletes the instance AND both EBS volumes permanently. [y/N]:" and read stdin. Abort on anything other than `y` or `yes`.
    - `workstation.destroy(id)`.
    - (Req 1.3.1, 1.3.2)
  - [ ] 6.6 Implement `ssh.rs`
    - `resolve_workstation_id` → `workstation.start_interactive_session(id)` (from task 5.3).
    - (Req 4.3)
  - [ ] 6.7 Implement `remote_exec.rs`
    - `resolve_workstation_id`.
    - Join `args.command` into a single command string. Reject empty command list with a clear error.
    - Call `workstation.remote_exec(id, &args.cwd, &args.command, tokio::io::stdout(), tokio::io::stderr())`.
    - Exit with the returned exit code.
    - (Req 4.4)
  - [ ] 6.8 Implement `status.rs`, `list.rs`, `bootstrap.rs`, `idle.rs`
    - Each is a thin argument-translation + format wrapper over the corresponding `Workstation` engine method.
    - `status.rs` formats the `WorkstationStatus` with the cost rate and uptime; handles the `hourly_cost_usd: None` case by printing "cost rate: unknown".
    - `list.rs` prints a table with columns: WorkstationId, State, Region, InstanceType, Uptime, HourlyRate.
    - `bootstrap.rs` prints the `BootstrapDrift` outcome.
    - `idle.rs --defer 2h`: parse `humantime::Duration`, compute `Utc::now() + duration`, call `workstation.idle_defer(id, until)`.
    - (Req 5.2.1, 5.2.3)
  - [ ] 6.9 Dispatch in `apps/tkr/src/main.rs`
    - Add the `Command::Workstation { action }` match arm, routing each `WorkstationAction` variant to the corresponding handler.
    - (Req 6.1.4)

- [x] 7. Checkpoint — CLI end-to-end smoke test
  - `tkr workstation up`, `status`, `stop` round-trip against mock AWS (via an in-process smoke test that drives the engine with mocked SDK clients). Interactive `ssh` and `remote-exec` require the live-AWS path from task 9.

- [ ] 8. Cloud-init bootstrap + idle-shutdown watchdog
  - [ ] 8.1 Create `crates/tokeira-aws/src/remote_workstation_bootstrap.rs`
    - `BootstrapContext` struct per design.md §5.
    - `render(context: &BootstrapContext) -> String` renders the full 7-phase bash script.
    - `fingerprint(context: &BootstrapContext) -> String` returns the SHA-256 hex.
    - Each of the 7 phases is its own Rust function returning `String`, so they compose cleanly.
    - Phase 1 (filesystem): NVMe detect via `lsblk`, format if unformatted, mount at `/work`, `fstab` entries for EBS volumes.
    - Phase 2 (toolchain): `apt-get install`, rustup install, `rustup show`, cargo tool installation.
    - Phase 3 (profile.d env): generate the shell file with the four exports.
    - Phase 4 (repo clone): if `/work/repo/tokeira/.git` missing, clone; symlink `/work/tokeira`.
    - Phase 5 (agentd socket dir): `mkdir /run/tokeira-agentd`, `tmpfiles.d` entry.
    - Phase 6 (idle watchdog): write the service + timer files (see task 8.2 for the service script).
    - Phase 7 (fingerprint): write `/etc/tokeira/workstation-fingerprint`.
    - (Req 3.1, 3.2, 3.3, 5.1, 8.1)
  - [ ] 8.2 Write the idle-shutdown watchdog script
    - 40-line bash script per design.md §6.
    - Embedded as a string literal in `remote_workstation_bootstrap.rs` and written to the instance during Phase 6.
    - (Req 5.1)
  - [ ] 8.3 Idempotency verification
    - The bootstrap script must be safe to re-run. Phase 1 (filesystem) checks for existing mounts before remounting; Phase 2 (toolchain) uses `rustup`'s idempotent install; Phase 3 overwrites the profile.d file idempotently; Phase 4 skips clone if `.git` exists; Phase 5 uses `mkdir -p`; Phase 6 handles `systemctl enable --now` being safe; Phase 7 overwrites the fingerprint.
    - Add a short `# idempotency: ...` comment at the top of each phase documenting the re-run behaviour.

- [x] 9. Checkpoint — live-AWS smoke test
  - Bring up a real c8gd.8xlarge via `tkr workstation up`, run `tkr workstation remote-exec "cargo build --workspace"` against the Tokeira repo, verify:
    - Build completes (measures cold-build time for the cost-model tradeoff discussion).
    - Stop → start cycle preserves `~/.cargo` and the repo checkout; loses `target/` and `sccache` as expected.
    - Destroy tears down cleanly, no orphan volumes in the AWS console.
  - This is the real acceptance gate. Budget: one hour of operator time plus ~$2 of AWS costs.

- [ ] 10. Correctness-property tests
  - [ ]* 10.1 Property 1 — `up` is idempotent (Req 9.1)
    - `crates/tokeira-aws/tests/remote_workstation_idempotence.rs`.
    - `proptest` generates command sequences `[Up, Up, Stop, Start, Up, …]` of length 1–10.
    - Run each sequence against a fresh mock AWS state.
    - Assert final state has exactly one workstation instance per profile tag set.
    - Min 64 iterations.
  - [ ]* 10.2 Property 2 — destroy is total (Req 9.2)
    - `crates/tokeira-aws/tests/remote_workstation_destroy.rs`.
    - Seed mock state with one full workstation. Inject failures on each resource-delete in turn. Assert post-destroy no tagged resource remains.
    - Min 32 iterations.
  - [ ]* 10.3 Property 3 — fingerprint determinism (Req 9.3)
    - `crates/tokeira-aws/tests/remote_workstation_fingerprint.rs`.
    - Compute `fingerprint(ctx)` twice; assert byte-equal.
    - Mutate each input component by one byte; assert different fingerprint each time.
  - [ ]* 10.4 Property 4 — CLI defaults stay sane (Req 9.4)
    - `apps/tkr/tests/workstation_resolution.rs`.
    - `proptest` over `~/.tokeira/workstations/` directory states: empty, stale `.latest`, corrupted JSON, dangling ID, well-formed single workstation, well-formed multi-workstation.
    - For each state, invoke every `WorkstationAction` variant (via engine entry points, not the full CLI parse) and assert `Ok` or `Err` with descriptive message. NO panics.
    - Min 64 iterations.

- [ ] 11. Documentation updates
  - [ ] 11.1 Update `README.md`
    - Short section introducing the `tkr workstation` subcommand group with the canonical 3-command workflow: `up`, `remote-exec cargo build`, `stop`.
    - Cost disclaimer: "~$19/day active, ~$0.25/day stopped in eu-west-2".
    - Cross-link to the spec at `.kiro/specs/remote-workstation/`.
  - [ ] 11.2 Add `docs/remote-workstation.md`
    - Full operator guide: prerequisites (session-manager-plugin, AWS credentials), the 3-command happy path, troubleshooting (bootstrap fingerprint drift, idle-shutdown deferral, destroy-all-workstations escape hatch).
    - Cost-model table reproduced from design.md with a note that the embedded cost-rate table is stale-tolerant per Req 5.2.3.
  - [ ] 11.3 Update `CONTRIBUTING.md` if relevant
    - Add a "Remote workstation for Rust builds" bullet with the 3-command entrypoint.

- [ ] 12. Final checkpoint — spec complete
  - `cargo +nightly fmt --all --check`, `cargo lint`, `cargo check --workspace`, `cargo test --workspace` all green.
  - `cargo test --package tokeira-aws --test remote_workstation_idempotence` (and siblings) green.
  - Live-AWS acceptance test from task 9 passes: `tkr workstation up && tkr workstation remote-exec "cargo build --workspace" && tkr workstation stop` produces a successful build with no infra orphans after stop.
  - Update `docs/CODEX_START_HERE.md` if present to mention the new workstation surface.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP. Per the workflow, these include all property test sub-tasks (10.1–10.4). The correctness properties themselves remain invariants the implementation upholds.
- Each task references specific requirements in parentheses for traceability. Every requirement number from `requirements.md` Features 1–9 appears in at least one task's parenthetical reference.
- Checkpoints (tasks 2, 4, 7, 9, 12) mark integration points where `cargo build --workspace` stays green and the spec remains bisectable. Task 9 is the first checkpoint that requires live AWS credentials; prior checkpoints are fully mocked.
- The 3-command happy path (`tkr workstation up` → `tkr workstation remote-exec "cargo build --workspace"` → `tkr workstation stop`) is the spec's north-star UX. Every task should be evaluated against "does this improve or complicate the 3-command path?"
- Follow-up work deferred to `agent-controller`: nothing in this task list installs Codex, binds agentd to `/run/tokeira-agentd/agentd.sock`, or adds `tkr agent *` subcommands. All such work is owned by the `agent-controller` spec, which will consume `tkr workstation up` and `tkr workstation remote-exec` as its foundation.
