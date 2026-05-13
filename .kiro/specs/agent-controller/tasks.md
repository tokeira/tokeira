# Implementation Plan: Agent Controller

## Overview

Implement the agent orchestration layer for Tokeira's remote workstation: an `agentd` Rust daemon that receives tasks over a Unix socket and TCP listener, manages git worktrees, spawns `codex exec` processes, and reports results — plus a `tkr agent` CLI command group that communicates with `agentd` via SSM TCP port-forwarding.

The plan follows a bottom-up arc:

1. **Crate scaffold and data models** — `apps/agentd/` binary crate with protocol types, task state machine, and policy constants.
2. **Core daemon logic** — server (socket + TCP), executor (Codex spawning, JSONL streaming), queue (SQLite-backed FIFO), graceful shutdown.
3. **CLI command group** — `tkr agent` subcommands, SSM port-forward connector, output formatting.
4. **Security and policy** — secret scanning, sandbox enforcement, branch naming, user separation.
5. **Property tests** — proptest coverage for all 10 correctness properties.

Target crates and files:

- `apps/agentd/Cargo.toml` — new binary crate
- `apps/agentd/src/` — `main.rs`, `server.rs`, `protocol.rs`, `task.rs`, `executor.rs`, `review.rs`, `budget.rs`, `policy.rs`, `secrets.rs`, `constants.rs`
- `apps/agentd/tests/` — property tests (10 files)
- `apps/agentd/PROTOCOL.md` — protocol specification
- `apps/tkr/src/commands/agent/` — CLI handler modules (18 files)
- `apps/tkr/src/cli.rs` — `Agent` variant and `AgentAction` enum

## Tasks

- [ ] 1. Scaffold `apps/agentd/` binary crate
  - [ ] 1.1 Create `apps/agentd/Cargo.toml` with workspace dependencies
    - Add `tokio` (full), `serde`, `serde_json`, `anyhow`, `tracing`, `tracing-subscriber`, `rusqlite` (with `bundled` feature), `ulid`, `chrono`, `sd-notify`, `proptest` (dev), `proptest-derive` (dev)
    - Set `edition = "2024"`, inherit workspace package metadata
    - _Requirements: 1.1.1_
  - [ ] 1.2 Add `apps/agentd` to workspace `Cargo.toml` members
    - _Requirements: 1.1.1_
  - [ ] 1.3 Create `apps/agentd/src/main.rs` entry point stub
    - Tokio runtime init, tracing subscriber setup, placeholder for server start
    - Signal handling skeleton (SIGTERM via `tokio::signal`)
    - `sd_notify` readiness signalling placeholder
    - _Requirements: 1.1.1, 1.1.4, 1.3.4_
  - [ ] 1.4 Create `apps/agentd/src/constants.rs`
    - `DEFAULT_SOCKET_PATH`, `DEFAULT_TCP_PORT` (18777), `DEFAULT_SANDBOX_MODE` ("workspace-write"), `BRANCH_PREFIX` ("agent/"), `TASK_ID_REGEX`, `MAX_LINE_SIZE` (10 MiB), `IDLE_TIMEOUT_SECS` (60), `SHUTDOWN_GRACE_SECS` (30)
    - Codex install command constant, review prompt version constant
    - _Requirements: 10.1.4, 12.1.3, 4.2.1, 6.1.8, 6.1.9_

- [ ] 2. Protocol types and data models
  - [ ] 2.1 Create `apps/agentd/src/protocol.rs` — wire types
    - `Request` struct: `id: u64`, `protocol_version: u32`, `method: Method`, `params: serde_json::Value`
    - `Response` struct: `id: u64`, `result: Option<Value>`, `error: Option<ProtocolError>`, `done: Option<bool>`, `seq: Option<u64>`
    - `Method` enum with all 16 variants (Submit, Status, Logs, Diff, ReviewPack, ReviewSpec, Usage, Cancel, Resume, Cleanup, Push, Pr, Commit, Validate, Doctor, CodexLogin)
    - `ProtocolError` struct with `ErrorCode` enum (InvalidRequest, TaskNotFound, BranchConflict, QueuePaused, NotInstalled, InternalError, ProtocolVersionMismatch, UnknownMethod, AuthRequired)
    - Derive `Serialize, Deserialize, Debug, Clone, PartialEq, Eq` on all types
    - Derive `proptest_derive::Arbitrary` behind `#[cfg(test)]` on all types
    - _Requirements: 6.1.1, 6.1.2, 6.1.3, 6.1.6, 6.1.7, 6.1.10, 6.1.11, 9.2_
  - [ ] 2.2 Create `apps/agentd/src/task.rs` — task state machine and queue
    - `TaskState` enum: Queued, Preparing, Running, Validating, AwaitingPublish, Completed, Failed, Cancelled, Interrupted, RateLimited
    - `FailureKind` enum: CodexExitNonzero, ValidationFailed, GitFailed, RateLimited, Cancelled, Interrupted
    - `PushState` enum: NotPushed, Pushed, PrCreated
    - `Task` struct with all fields per design.md
    - `TaskState::transition(self, event: Event) -> Result<TaskState, InvalidTransition>` enforcing the valid transition table
    - `Event` enum: Dequeue, Cancel, WorktreeCreated, GitFailed, CodexExit0, CodexExitNonzero, Sigterm, RateLimitDetected, ValidationPass, ValidationFail, OperatorComplete, OperatorDiscard, Resume, Retry
    - `TaskQueue` struct with FIFO ordering, `enqueue`, `dequeue_next`, `cancel`, `get`, `list_all` methods
    - Derive `proptest_derive::Arbitrary` behind `#[cfg(test)]` on state/event enums
    - _Requirements: 9.1.1, 9.1.2, 9.1.3, 2.3.1, 2.3.2, 2.3.3_

  - [ ] 2.3 Create `apps/agentd/src/budget.rs` — rate-limit tracking
    - `UsageState` struct: tasks_completed_5h_window, tasks_completed_weekly, last_rate_limit_event, estimated_reset, queue_state
    - `QueueState` enum: Active, Paused { reason: PauseReason }
    - `PauseReason` enum: RateLimited
    - Rate-limit detection logic: scan JSONL events for HTTP 429 / "rate limit" / "quota exceeded"
    - _Requirements: 5.1.1, 5.1.2, 5.2.1, 5.2.2_
  - [ ] 2.4 Create `apps/agentd/src/review.rs` — spec review types
    - `ReviewFinding` struct: severity, location, category, description
    - `Severity` enum: Error, Warning, Info
    - `FindingCategory` enum: Ambiguity, Inconsistency, MissingDetail, Testability, Contradiction
    - Review prompt constant (versioned) containing EARS rules, INCOSE rules, Tokeira conventions
    - Finding parser: extract JSON array from Codex output, fallback to raw text
    - _Requirements: 4.1.3, 4.1.4, 4.2.1, 4.2.2, 4.2.3_
  - [ ] 2.5 Create `apps/agentd/src/secrets.rs` — secret scanning
    - Pattern set: AWS access keys, GitHub tokens (classic + fine-grained), OpenAI API keys, SSH private key markers, high-entropy strings `[A-Za-z0-9+/=]{40,}`
    - `scan_diff(diff: &str) -> Vec<SecretDetection>` function
    - `redact(diff: &str, detections: &[SecretDetection]) -> String` function
    - Sensitive path exclusion list: `~/.codex/**`, `~/.aws/**`, `.env*`, `*.pem`, `*.key`, `id_rsa`, `id_ed25519`, `/etc/tokeira/agentd-env`
    - _Requirements: 12.2.1, 12.2.2, 12.2.3, 11.2.3_
  - [ ] 2.6 Create `apps/agentd/src/policy.rs` — sandbox and branch enforcement
    - Blocked mode list: `danger-full-access`, `dangerously-bypass-approvals-and-sandbox`
    - `validate_submission(params) -> Result<(), PolicyViolation>` checking blocked modes, root UID, push-to-main
    - `validate_task_id(id: &str) -> Result<(), InvalidTaskId>` against regex `[a-z0-9][a-z0-9-]{2,63}`
    - Branch name derivation: `format!("agent/{}", task_id)`
    - Commit message derivation: `format!("agent({}): {}", task_id, first_line_of_prompt)`
    - _Requirements: 12.1.1, 12.1.2, 12.1.3, 12.3.1, 12.3.2, 12.3.3, 2.1.2_

- [ ] 3. Checkpoint — data models compile
  - Ensure `cargo check -p agentd` is green. All types, enums, and validation functions compile. No runtime logic yet.

- [ ] 4. SQLite persistence layer
  - [ ] 4.1 Implement SQLite schema and migrations in `task.rs`
    - Create table `tasks` with columns matching `Task` struct fields
    - `TaskStore` struct wrapping `rusqlite::Connection`
    - `TaskStore::open(path)` — create DB file, run migrations
    - `TaskStore::insert(task)`, `TaskStore::update_state(id, state, failure_kind)`, `TaskStore::get(id)`, `TaskStore::list_all()`, `TaskStore::list_queued()`, `TaskStore::get_running()`
    - On open: transition any `running` tasks to `interrupted` (crash recovery)
    - _Requirements: 2.3.1, 9.5.1_
  - [ ] 4.2 Implement `UsageStore` persistence in `budget.rs`
    - Store rate-limit events and task completion timestamps in SQLite
    - Query methods for 5-hour window and weekly counts
    - _Requirements: 5.2.1, 5.2.2_

- [ ] 5. Server and connection handling
  - [ ] 5.1 Create `apps/agentd/src/server.rs` — socket + TCP listener
    - `Server::new(socket_path, tcp_port, auth_token)` constructor
    - Bind Unix socket at `socket_path`; remove stale socket file if exists
    - Bind TCP listener at `127.0.0.1:<tcp_port>`
    - TCP connections require auth token on first message
    - Accept loop: spawn a task per connection, read newline-delimited JSON
    - Enforce max line size (10 MiB) and idle timeout (60s)
    - Route parsed `Request` to handler based on `method` field
    - Reject unknown methods with `ErrorCode::UnknownMethod`
    - Reject unsupported `protocol_version` with `ErrorCode::ProtocolVersionMismatch`
    - _Requirements: 1.1.2, 1.1.3, 1.1.5, 1.1.6, 6.1.1, 6.1.7, 6.1.8, 6.1.9, 6.1.11_
  - [ ] 5.2 Implement request dispatch and response serialization
    - Match `Method` variants to handler functions
    - Serialize `Response` as JSON + newline, write to connection
    - For streaming responses (logs): send multiple responses with same `id`, incrementing `seq`, final message has `done: true`
    - _Requirements: 6.1.3, 6.1.4, 6.1.10_
  - [ ] 5.3 Wire server into `main.rs`
    - Read auth token from env file path
    - Start server, await shutdown signal
    - `sd_notify(READY=1)` after successful bind
    - _Requirements: 1.1.4, 1.3.4_

- [ ] 6. Executor — Codex process management
  - [ ] 6.1 Create `apps/agentd/src/executor.rs` — Codex spawning and JSONL streaming
    - `CodexRunner` trait: `async fn run(&self, config: CodexConfig) -> Result<CodexOutcome>`
    - `RealCodexRunner` implementation: spawn `codex exec` via `tokio::process::Command`
    - Command construction: `--cd <worktree>`, `--sandbox workspace-write`, `--ask-for-approval never`, `--json`, `--output-last-message <worktree>/.agentd/final.md`, `-`
    - Pipe prompt via stdin (NOT as shell argument)
    - Environment filtering: do NOT inherit `OPENAI_API_KEY`, `AWS_*`, secrets
    - Read stdout line-by-line, parse each as JSONL event via `serde_json`
    - Log malformed lines at `warn`, skip them (never panic)
    - Write full JSONL stream to `<worktree>/.agentd/codex-output.jsonl`
    - Track wall-clock duration
    - Detect rate-limit events in JSONL stream
    - _Requirements: 2.4.1, 2.4.2, 2.4.3, 2.4.4, 2.4.5, 5.1.1, 9.4_
  - [ ] 6.2 Implement worktree management in executor
    - `git fetch --prune origin` before worktree creation
    - `git worktree add /work/worktrees/<task-id> -b agent/<task-id> <base>`
    - Detect branch conflict (branch already exists) → fail with `BranchConflict`
    - After Codex exit 0: `git add -A && git commit -m "agent(<task-id>): <first line>"`
    - After Codex exit non-zero: preserve worktree, store exit code + last 50 JSONL lines
    - _Requirements: 2.2.1, 2.2.2, 2.2.3, 2.2.4, 12.3.1, 12.3.3, 12.3.4_
  - [ ] 6.3 Implement `MockCodexRunner` for testing
    - Configurable exit code, JSONL events, delay
    - Used by property tests and unit tests to exercise state machine without real processes
    - _Requirements: 9.1, 9.4, 9.5_

- [ ] 7. Task orchestration — queue + execution loop
  - [ ] 7.1 Implement the main execution loop
    - On task submission: validate task ID, check uniqueness, enqueue, persist to SQLite
    - If queue was empty: immediately begin execution
    - Execution flow: `queued → preparing → running → validating → awaiting_publish`
    - On completion/failure: dequeue next task, begin execution
    - On rate-limit: pause queue, mark task `RateLimited`
    - On cancel (queued): remove from queue, mark `Cancelled`
    - On cancel (running): SIGTERM to child, mark `Cancelled`
    - _Requirements: 2.1.6, 2.3.1, 2.3.2, 2.3.3, 2.3.5, 5.1.1, 5.1.4_
  - [ ] 7.2 Implement graceful shutdown sequence
    - On SIGTERM: `sd_notify(STOPPING=1)`, stop accepting connections
    - SIGTERM to running `codex exec` child, wait 30s
    - If child still alive: SIGKILL to process group
    - Mark in-progress task as `Interrupted`, persist to SQLite
    - Close sockets, remove socket file, exit 0
    - _Requirements: 1.3.1, 1.3.2, 1.3.3, 1.3.4_
  - [ ] 7.3 Implement `resume` handler
    - Transition queue from `Paused` to `Active`
    - Re-queue `RateLimited` task as `Queued`
    - Begin execution if queue non-empty
    - _Requirements: 5.1.4_

- [ ] 8. Checkpoint — daemon end-to-end with mock Codex
  - Wire executor + queue + server together
  - Verify: submit a task → mock Codex runs → task completes → status shows `awaiting_publish`
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Request handlers — status, logs, diff, review-pack, usage
  - [ ] 9.1 Implement `status` handler
    - No args: return all tasks (running, queued, completed, failed, cancelled, interrupted)
    - With `task_id`: return detailed status (state, prompt summary, base branch, worktree path, timestamps, exit code, queue position, codex_events_count)
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4_
  - [ ] 9.2 Implement `logs` handler
    - For completed/failed tasks: read and return full JSONL file
    - For running tasks: stream new events as they arrive (streaming response with `seq` + `done`)
    - _Requirements: 3.2.1, 3.2.2, 3.2.4, 3.2.5_
  - [ ] 9.3 Implement `diff` handler
    - Run `git diff <base>...HEAD` (committed) + `git diff` + `git status --porcelain=v1` (uncommitted/untracked)
    - Support `--stat` mode via `git diff --stat`
    - _Requirements: 3.3.1, 3.3.2, 3.3.3_
  - [ ] 9.4 Implement `review_pack` handler
    - Run `cargo test --workspace` in worktree, capture exit code + last 200 lines stdout/stderr
    - Collect diff (`git diff <base>..<branch>`) + uncommitted diff + untracked files
    - Summarise JSONL events: files changed, tools invoked, errors encountered, total events, duration
    - Run secret scan on diff, include `secrets_detected` field with redacted values
    - Exclude sensitive paths from diff/file listing
    - Return structured `ReviewPack` JSON
    - _Requirements: 3.4.1, 3.4.2, 3.4.3, 3.4.4, 12.2.1, 12.2.2, 11.2.3_
  - [ ] 9.5 Implement `usage` handler
    - Return `UsageState`: tasks in 5h window, weekly count, last rate-limit event, queue state
    - _Requirements: 5.2.1, 5.2.2, 5.2.3_
  - [ ] 9.6 Implement `review_spec` handler
    - Receive spec file contents from CLI
    - Spawn `codex exec --cd /work/tokeira --sandbox read-only --ask-for-approval never --json --output-last-message <path> -` with review prompt + spec content via stdin
    - Parse structured findings from output, fallback to raw text
    - Store payload digest (SHA-256) and prompt version in metadata
    - _Requirements: 4.1.2, 4.1.3, 4.1.4, 4.1.5, 4.1.6, 4.1.7_

- [ ] 10. Request handlers — push, pr, commit, validate, cancel, cleanup, doctor, codex_login
  - [ ] 10.1 Implement `push` handler
    - Run `git push -u origin agent/<task-id>` in the task's worktree
    - Update task's `push_state` to `Pushed`
    - _Requirements: 2.2.3 (operator-triggered push)_
  - [ ] 10.2 Implement `pr` handler
    - Run `gh pr create --head agent/<task-id> --base <base> --draft` (or non-draft)
    - Update task's `push_state` to `PrCreated`
    - Return PR URL
    - _Requirements: 8.1.2 (Pr variant)_
  - [ ] 10.3 Implement `commit` handler
    - Run `git add -A && git commit -m "agent(<task-id>): <message>"` in worktree
    - For tasks in `awaiting_publish` state only
    - _Requirements: 8.1.2 (Commit variant)_
  - [ ] 10.4 Implement `validate` handler
    - Run `cargo test --workspace` in the task's worktree
    - Return exit code + stdout/stderr (last 200 lines)
    - _Requirements: 8.1.2 (Validate variant)_
  - [ ] 10.5 Implement `cancel` handler
    - Queued task: remove from queue, mark `Cancelled`
    - Running task: SIGTERM to child process, mark `Cancelled`
    - _Requirements: 2.3.5_
  - [ ] 10.6 Implement `cleanup` handler
    - Remove worktrees for completed/failed tasks older than threshold (default 7d)
    - Run `git worktree remove <path>` for each
    - _Requirements: 2.2.5_
  - [ ] 10.7 Implement `doctor` handler
    - Check: agentd binary exists, systemd unit running, Codex CLI installed, Codex auth present, bubblewrap installed, Codex sandbox works (smoke test), repo exists, worktrees dir writable, git remote writable, IMDS blocked (iptables rule), TCP connection succeeds
    - Return structured results per check
    - _Requirements: 8.1.2 (Doctor variant)_
  - [ ] 10.8 Implement `codex_login` handler
    - Invoke `codex login` on the workstation (interactive SSM session)
    - Verify `/home/agent/.codex/auth.json` created
    - _Requirements: 7.1.6_

- [ ] 11. Checkpoint — all daemon handlers functional
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 12. CLI command group — `tkr agent`
  - [ ] 12.1 Add `Agent` variant to `Command` enum in `apps/tkr/src/cli.rs`
    - `Agent { #[command(subcommand)] action: AgentAction }`
    - `AgentAction` enum with all 18 variants per design.md (Submit, Status, Logs, Diff, ReviewPack, ReviewSpec, Usage, Install, Uninstall, Cancel, Resume, Cleanup, Push, Pr, Commit, Validate, Doctor, CodexLogin)
    - Each variant with appropriate clap args per design.md
    - All variants accept `--workstation <id>` with `.latest` file resolution
    - _Requirements: 8.1.1, 8.1.2, 8.1.3_
  - [ ] 12.2 Create `apps/tkr/src/commands/agent/mod.rs` — shared helpers
    - `resolve_workstation(ws: Option<String>)` — read `~/.tokeira/workstations/.latest` as default
    - `connect_agentd(instance_id: &str, auth_token: &str) -> Result<AgentdConnection>` — establish SSM port-forward to `127.0.0.1:18777`, send auth token on first message
    - `send_request(conn, request) -> Result<Response>` — serialize request, read response
    - `stream_responses(conn, request) -> impl Stream<Item = Response>` — for streaming handlers
    - Error handling: connectivity errors suggest `tkr workstation up` + `tkr agent install`
    - _Requirements: 6.2.1, 6.2.2, 6.2.3, 8.2.3_
  - [ ] 12.3 Implement `submit` handler in `apps/tkr/src/commands/agent/submit.rs`
    - Read spec's `tasks.md` if `--spec` provided
    - Assemble prompt (spec context + operator prompt)
    - Validate sandbox mode against policy (block unsafe modes unless `--i-accept-risk`)
    - Send `submit` request to agentd
    - Print task ID and queue position
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4, 2.1.5, 2.1.6, 12.1.1, 12.1.2_
  - [ ] 12.4 Implement `status` handler in `apps/tkr/src/commands/agent/status.rs`
    - Send `status` request, format response as table (human) or JSON
    - _Requirements: 3.1.1, 3.1.2, 8.2.1, 8.2.2_
  - [ ] 12.5 Implement `logs` handler in `apps/tkr/src/commands/agent/logs.rs`
    - For running tasks: stream events to stdout, stop on SIGINT (task continues)
    - For completed tasks: print full JSONL and exit
    - _Requirements: 3.2.1, 3.2.2, 3.2.3, 3.2.4_
  - [ ] 12.6 Implement `diff` handler in `apps/tkr/src/commands/agent/diff.rs`
    - Send `diff` request, print output to stdout
    - Support `--stat` flag
    - _Requirements: 3.3.1, 3.3.2, 3.3.3_
  - [ ] 12.7 Implement `review_pack` handler in `apps/tkr/src/commands/agent/review_pack.rs`
    - Send `review_pack` request, format as human-readable or JSON
    - Print warning if `secrets_detected` is non-empty
    - _Requirements: 3.4.1, 3.4.2, 3.4.3, 12.2.3_
  - [ ] 12.8 Implement `review_spec` handler in `apps/tkr/src/commands/agent/review_spec.rs`
    - Read all `.md` files from `.kiro/specs/<spec-name>/`
    - Send to agentd as review request
    - Print findings grouped by severity, or raw JSON with `--json`
    - Fallback: print raw Codex response with warning if structured parsing fails
    - _Requirements: 4.1.1, 4.1.5, 4.1.6_
  - [ ] 12.9 Implement `usage` handler in `apps/tkr/src/commands/agent/usage.rs`
    - Send `usage` request, format response
    - Include warning about incomplete counts if no rate-limit events observed
    - _Requirements: 5.2.1, 5.2.3, 5.2.4_
  - [ ] 12.10 Implement remaining CLI handlers
    - `install.rs` — full install flow (create agent user, install Codex, copy binary, write unit file, write env file, set up CODEX_HOME, add iptables rule, enable service)
    - `uninstall.rs` — stop/disable service, remove unit file, remove binary, remove env file
    - `cancel.rs`, `resume.rs`, `cleanup.rs` — thin wrappers over protocol
    - `push.rs`, `pr.rs`, `commit.rs`, `validate.rs` — thin wrappers over protocol
    - `doctor.rs` — format health check results
    - `codex_login.rs` — invoke interactive SSM session for `codex login`
    - _Requirements: 7.1.1, 7.1.2, 7.1.3, 7.1.4, 7.1.5, 7.2.1, 7.2.2, 7.2.3, 7.2.4, 11.1.1, 11.1.2, 11.1.3, 11.1.4, 11.1.5, 11.2.1, 11.2.4, 11.3.1, 11.3.2, 11.3.3_
  - [ ] 12.11 Wire `Agent` dispatch in `apps/tkr/src/main.rs`
    - Route each `AgentAction` variant to its handler module
    - _Requirements: 8.1.4_

- [ ] 13. Checkpoint — CLI compiles and dispatches
  - Ensure `cargo check -p tkr` is green with the new `Agent` command group
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 14. Property tests
  - [ ]* 14.1 Write property test: task state machine transitions (`tests/task_state_machine.rs`)
    - **Property 1: Task state machine only transitions through valid states**
    - Generate `Vec<Event>` sequences via proptest, apply to initial `Queued` state
    - Assert every transition is valid per the transition table; no panic, no invalid state
    - Minimum 256 iterations
    - **Validates: Requirements 9.1.1, 9.1.2, 9.1.3, 2.3.1**
  - [ ]* 14.2 Write property test: protocol round-trip (`tests/protocol_roundtrip.rs`)
    - **Property 2: Protocol messages round-trip through JSON without loss**
    - `Arbitrary` impls for `Request`, `Response`, all wire types
    - Assert `serde_json::from_str(serde_json::to_string(x)) == x` for all generated messages
    - Minimum 256 iterations
    - **Validates: Requirements 9.2.1, 9.2.2, 9.2.3, 6.1.1, 6.1.2, 6.1.3**
  - [ ]* 14.3 Write property test: worktree path uniqueness (`tests/worktree_isolation.rs`)
    - **Property 3: Worktree paths are unique for distinct task IDs**
    - Generate `HashSet<String>` of valid task IDs, derive paths, assert set size preserved
    - Minimum 256 iterations
    - **Validates: Requirements 9.3.1, 9.3.2**
  - [ ]* 14.4 Write property test: JSONL parser resilience (`tests/jsonl_resilience.rs`)
    - **Property 4: JSONL parser never panics on arbitrary input**
    - Generate `any::<Vec<u8>>()`, feed to parser, assert no panic (use `catch_unwind`)
    - Parser must return `Ok(event)` or `Err(...)` — never panic
    - Minimum 256 iterations
    - **Validates: Requirements 9.4.1, 9.4.2**
  - [ ]* 14.5 Write property test: FIFO queue ordering (`tests/queue_ordering.rs`)
    - **Property 5: Queue executes tasks in FIFO order**
    - Generate submission sequences with interleaved completions
    - Verify execution order matches submission order (modulo cancellations)
    - Minimum 256 iterations
    - **Validates: Requirements 2.3.1, 2.3.2, 2.3.3**
  - [ ]* 14.6 Write property test: secret scanning (`tests/secret_scanning.rs`)
    - **Property 6: Review pack never exposes secrets or sensitive paths**
    - Generate diffs with embedded secret patterns (AWS keys, GitHub tokens, etc.)
    - Verify all secrets are redacted in output, sensitive paths excluded
    - Minimum 256 iterations
    - **Validates: Requirements 12.2.1, 12.2.2, 11.2.3**
  - [ ]* 14.7 Write property test: policy enforcement (`tests/policy_enforcement.rs`)
    - **Property 7: Policy enforcement blocks all unsafe configurations**
    - Generate combinations of blocked configs ± `--i-accept-risk`
    - Verify: blocked without flag → rejected; blocked with flag → accepted; safe → accepted
    - Minimum 256 iterations
    - **Validates: Requirements 12.1.1, 12.1.2**
  - [ ]* 14.8 Write property test: naming conventions (`tests/naming_conventions.rs`)
    - **Property 8: Agent output follows naming conventions**
    - Generate arbitrary valid task IDs and prompts
    - Verify branch name matches `^agent/[a-z0-9][a-z0-9-]{2,63}$`
    - Verify commit message matches `agent(<task-id>): <first-line>`
    - Minimum 256 iterations
    - **Validates: Requirements 12.3.1, 12.3.2, 12.3.3**
  - [ ]* 14.9 Write property test: crash recovery (`tests/crash_recovery.rs`)
    - **Property 9: Crash recovery preserves queue and transitions running to interrupted**
    - Generate arbitrary queue states (mix of queued, running, completed, failed tasks)
    - Persist to SQLite, re-open store, verify: `running → interrupted`, `queued` stays `queued`
    - Minimum 256 iterations
    - **Validates: Requirements 9.5.1, 9.5.2**
  - [ ]* 14.10 Write property test: task ID sanitization (`tests/task_id_sanitization.rs`)
    - **Property 10: Task ID sanitization rejects path traversal and invalid formats**
    - Generate arbitrary strings via proptest
    - Verify: only strings matching `[a-z0-9][a-z0-9-]{2,63}` accepted
    - Verify: strings with `..`, `/`, `\` always rejected
    - Minimum 256 iterations
    - **Validates: Requirements 9.6.1, 9.6.2**

- [ ] 15. Protocol documentation
  - [ ] 15.1 Write `apps/agentd/PROTOCOL.md`
    - Document the JSON-over-newline protocol: message format, request/response structure
    - List all methods with their params and result schemas
    - Document streaming response model (seq, done)
    - Document error codes and their meanings
    - Document auth token requirement for TCP connections
    - Document max line size (10 MiB) and idle timeout (60s)
    - _Requirements: 6.1.5_

- [ ] 16. CI lint for Kiro dependency exclusion
  - [ ] 16.1 Add CI check that `apps/agentd/Cargo.toml` contains no Kiro-related dependencies
    - Grep for `kiro`, `claude`, `anthropic` in agentd's Cargo.toml — fail if found
    - _Requirements: 5.3.1, 5.3.4_
  - [ ] 16.2 Add CI check that `agentd` source does not contain unsafe sandbox strings outside `policy.rs`
    - Grep for `danger-full-access` and `dangerously-bypass` in agentd source — only allowed in `policy.rs`
    - _Requirements: 12.1.4_

- [ ] 17. Final checkpoint
  - Ensure `cargo check --workspace` passes
  - Ensure `cargo test -p agentd` passes (all property tests + unit tests)
  - Ensure `cargo clippy -p agentd` passes
  - Ensure `cargo check -p tkr` passes with the new agent command group
  - Ask the user if questions arise.

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1"], "description": "Scaffold agentd binary crate" },
    { "wave": 2, "tasks": ["2"], "description": "Protocol types and data models" },
    { "wave": 3, "tasks": ["3"], "description": "Checkpoint — data models compile" },
    { "wave": 4, "tasks": ["4"], "description": "SQLite persistence layer" },
    { "wave": 5, "tasks": ["5"], "description": "Server and connection handling" },
    { "wave": 6, "tasks": ["6"], "description": "Executor — Codex process management" },
    { "wave": 7, "tasks": ["7"], "description": "Task orchestration — queue + execution loop" },
    { "wave": 8, "tasks": ["8"], "description": "Checkpoint — daemon end-to-end" },
    { "wave": 9, "tasks": ["9", "10"], "description": "Request handlers (all)" },
    { "wave": 10, "tasks": ["11"], "description": "Checkpoint — all handlers functional" },
    { "wave": 11, "tasks": ["12"], "description": "CLI command group — tkr agent" },
    { "wave": 12, "tasks": ["13"], "description": "Checkpoint — CLI compiles" },
    { "wave": 13, "tasks": ["14", "15", "16"], "description": "Property tests, docs, CI lints" },
    { "wave": 14, "tasks": ["17"], "description": "Final checkpoint" }
  ]
}
```

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from design.md
- The `MockCodexRunner` trait abstraction (task 6.3) enables all property tests to run without spawning real processes
- Integration tests (requiring a live workstation) are out of scope for this task list — they are gated behind `--features integration`
