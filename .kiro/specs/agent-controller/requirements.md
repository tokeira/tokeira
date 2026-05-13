# Requirements Document: Agent Controller

## Introduction

The remote workstation spec delivers a Graviton4 `c8gd.8xlarge` EC2 instance accessible via SSM Session Manager with `tkr workstation up` / `tkr workstation remote-exec` / `tkr workstation ssh`. This spec builds the agent orchestration layer on top of that foundation: an `agentd` daemon (Rust binary) running on the workstation that orchestrates OpenAI Codex (via `codex exec`) to implement tasks from Tokeira specs, plus a `tkr agent` CLI command group on the operator's local machine that submits tasks, monitors progress, retrieves results, and manages budgets.

The integration surface is `codex exec --cd "$WORKTREE" --sandbox workspace-write --ask-for-approval never --json --output-last-message "$WORKTREE/.agentd/final.md" -` (prompt via stdin) — the non-interactive CLI mode that streams JSONL events and exits with 0/1. No abstraction layer for swapping in other agents; this controller targets OpenAI Codex exclusively.

### What this spec delivers

- An `agentd` Rust binary that listens on `/run/tokeira-agentd/agentd.sock` (Unix socket) and `127.0.0.1:18777` (TCP, for SSM port-forward access). It receives tasks over a JSON-over-newline protocol, manages git worktrees, spawns `codex exec` processes, streams progress, and reports results.
- A `tkr agent` CLI command group on the operator's local machine that communicates with `agentd` via SSM port-forwarding. Commands: `submit`, `status`, `logs`, `diff`, `review-pack`, `review-spec`, `usage`, `install`, `uninstall`, `push`, `pr`, `commit`, `validate`, `doctor`, `codex-login`.
- A spec-review workflow where `codex exec` is fed spec files with a structured output schema and returns findings (ambiguities, inconsistencies, missing detail) as structured JSON.
- Usage observability: Codex rate-limit tracking (5-hour rolling window + weekly quota) surfaced via `tkr agent usage`, plus a hard guardrail that the controller NEVER autonomously triggers Kiro sessions.

### What this spec explicitly defers

- **Approval gates and multi-turn conversations** — if needed later, the Codex App Server protocol (same binary, different invocation) is the upgrade path. This spec uses `codex exec` only.
- **Parallel task execution across multiple workstations** — one workstation runs one `agentd`; multi-workstation orchestration is a future spec.
- **Automatic integration of branches** — integration is serial and operator-controlled. The controller produces branches; the operator merges.
- **Kiro-mediated work** — spec review, integration decisions, and conflict resolution via Kiro are always operator-initiated. The controller surfaces recommendations but never auto-consumes Kiro credits.

### Cross-references

- [`remote-workstation`](../remote-workstation/requirements.md): provides `tkr workstation up`, `tkr workstation remote-exec`, SSM access, the `/run/tokeira-agentd/` socket directory (Req 8.1), and the GitHub deploy-key surface (Feature 10) for push-from-workstation.
- `tokeira-aws` crate: gains the `agentd` binary (or it lives in a new `apps/agentd/` crate).
- `apps/tkr`: gains the `tkr agent` command group.

### Agentic workflow roadmap

This spec is the execution primitive in a three-stage progression toward Tokeira-orchestrated agentic development:

| Stage | Scope | Orchestration | This spec? |
|---|---|---|---|
| **v1 — Agent Controller** | Serial task execution, operator-driven submission, branch-per-task isolation, spec review | `agentd` + FIFO queue | **Yes** |
| **v2 — Tokeira-Orchestrated Pipelines** | Durable workflow definitions modelling the spec→review→implement→test→integrate pipeline as execution steps, each dispatching to `agentd` | Tokeira workflows (durable execution) | No — follow-up spec |
| **v3 — Multi-Workstation Parallelism** | DAG-based task scheduling across multiple workstations, retry policies, timeout handling, human-in-the-loop approval gates | Tokeira workflows + multi-instance `agentd` | No — future spec |

The v1 design is shaped to be consumable by v2: the Socket_Protocol, task state machine, and review-pack format are all designed as stable primitives that a Tokeira workflow activity can invoke without reshaping this spec. The key architectural decision is that `agentd` is a policy-light, durable executor (it persists task records but does not own scheduling policy beyond FIFO); scheduling intelligence lives in the orchestrator, which in v1 is the operator and in v2 is a Tokeira workflow.

Tokeira orchestrating its own development is the "eating your own dogfood" moment — proving the durable execution model by using it to build Tokeira. This spec delivers the execution primitive; the orchestration spec consumes it.

## Glossary

- **Agentd**: The Rust daemon binary running on the Workstation_Instance. Listens on the pre-declared Unix socket and TCP port 18777, manages task lifecycle, spawns Codex processes.
- **Codex**: OpenAI Codex CLI tool (`codex exec`). The sole AI agent this controller targets.
- **Task**: A unit of work submitted to `agentd` for Codex execution. Identified by a Task_Id. Each task maps to one git worktree, one `codex exec` invocation, and one result branch.
- **Task_Id**: A short stable identifier for a submitted task (e.g. `task-01HXYZ...`). Used in CLI commands, log filenames, branch names, and worktree paths.
- **Task_Queue**: The ordered list of tasks `agentd` manages. Tasks execute serially (one `codex exec` at a time) to avoid resource contention on the workstation.
- **JSONL_Stream**: The newline-delimited JSON event stream produced by `codex exec --json`. Each line is a self-contained JSON object describing a Codex operational event (tool call, file write, command execution, error, completion).
- **Worktree**: A git worktree created by `agentd` for task isolation. Located at `/work/worktrees/<task-id>/`. Each task runs in its own worktree branched from the operator-specified base.
- **Review_Pack**: A bundle of diff, test results, and Codex event summary for a completed task. Produced by `tkr agent review-pack` for operator review before integration.
- **Spec_Review**: A structured analysis of a Tokeira spec produced by feeding spec files to `codex exec` with a review-focused prompt and structured output schema.
- **Budget**: The combination of Codex rate limits (5-hour rolling window, weekly quota) and Kiro credits (AWS-billed). The controller tracks Codex usage; Kiro budget is never autonomously consumed.
- **Socket_Protocol**: The JSON-over-newline protocol used for communication between `tkr agent` (via SSM TCP port-forward) and `agentd`.

## Requirements

---

## Feature 1: Agentd Daemon Lifecycle

### Requirement 1.1: `agentd` binary and socket binding

**User Story:** As an operator, I want a daemon on the workstation that listens for task submissions on a well-known socket, so that the `tkr agent` CLI can communicate with it without SSH or custom networking.

#### Acceptance Criteria

1. THE Agentd binary SHALL be a Rust binary crate (either `apps/agentd/` or a binary target in `crates/tokeira-aws/`). It SHALL use `anyhow` for error handling, `tracing` for structured logging, and `tokio` for async runtime.
2. WHEN started, Agentd SHALL bind to `/run/tokeira-agentd/agentd.sock` (the path pre-declared by remote-workstation Req 8.1). IF the socket file already exists from a previous unclean shutdown, Agentd SHALL remove it before binding.
3. Agentd SHALL accept connections on the Unix socket using the Socket_Protocol (JSON-over-newline). Each connection MAY send multiple requests; Agentd SHALL process them sequentially per connection.
4. Agentd SHALL log startup, shutdown, and connection events via `tracing` at `info` level. Task lifecycle events SHALL be logged at `info`; Codex JSONL forwarding SHALL be logged at `debug`.
5. IF Agentd cannot bind to the socket (permission denied, directory missing), THEN Agentd SHALL exit with a non-zero status and a message naming the path and the OS error.
6. Agentd SHALL ALSO listen on `127.0.0.1:18777` (TCP) for SSM port-forward access. The TCP listener SHALL require a client authentication token (read from `/etc/tokeira/agentd-env`) on the first message of each connection.

### Requirement 1.2: `agentd` managed by systemd

**User Story:** As an operator, I want `agentd` to start automatically when the workstation boots and restart on crash, so that I do not need to manually start it after every `tkr workstation up`.

#### Acceptance Criteria

1. THE `tkr agent install` command SHALL write a systemd unit file at `/etc/systemd/system/tokeira-agentd.service` on the Workstation_Instance. The unit SHALL declare `Type=notify`, `Restart=on-failure`, `RestartSec=5s`, `RuntimeDirectory=tokeira-agentd`, and `ExecStart` pointing at the installed `agentd` binary path. The `sd-notify` crate SHALL be used for `Type=notify` readiness signalling.
2. THE unit SHALL run as `User=agent` (the dedicated agent user from Feature 11). The unit SHALL include the following hardening directives: `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectHome=true`, `ReadWritePaths=/work/worktrees /var/lib/tokeira-agentd /home/agent/.codex`, `RestrictSUIDSGID=true`, `IPAddressDeny=169.254.169.254/32`, `IPAddressDeny=fd00:ec2::254/128`.
3. WHEN `tkr agent install` completes, THE CLI SHALL enable and start the service via `systemctl enable --now tokeira-agentd.service`.
4. `tkr agent uninstall` SHALL stop and disable the service, remove the unit file, and remove the `agentd` binary. It SHALL NOT remove task worktrees or logs.

### Requirement 1.3: Graceful shutdown

**User Story:** As an operator, I want `agentd` to finish or abort the current Codex process cleanly on shutdown, so that a `tkr workstation stop` does not leave orphan processes or corrupted worktrees.

#### Acceptance Criteria

1. WHEN Agentd receives SIGTERM (from systemd stop), IT SHALL send SIGTERM to any running `codex exec` child process and wait up to 30 seconds for it to exit.
2. IF the child process does not exit within 30 seconds, Agentd SHALL send SIGKILL to the child process group.
3. AFTER the child process exits (or is killed), Agentd SHALL mark the in-progress task as `interrupted` in its state file and close the Unix socket cleanly.
4. Agentd SHALL use `sd_notify` (systemd readiness protocol) to signal `READY=1` after successful socket bind and `STOPPING=1` on shutdown initiation.

---

## Feature 2: Task Submission and Execution

### Requirement 2.1: `tkr agent submit` sends a task to `agentd`

**User Story:** As an operator, I want to submit a task from my local machine that tells Codex what to implement, so that I can queue work without logging into the workstation.

#### Acceptance Criteria

1. `tkr agent submit --task <id> [--spec <spec-name>] [--base <branch>] [--prompt <text>]` SHALL connect to Agentd via SSM port-forwarding and send a `submit` request over the Socket_Protocol.
2. THE `--task <id>` flag SHALL be optional. IF omitted, Agentd SHALL auto-generate a ULID as the Task_Id. Manual task IDs SHALL be sanitized against regex `[a-z0-9][a-z0-9-]{2,63}` and rejected if they contain path traversal characters. It becomes the Task_Id used for the worktree, branch name, and all subsequent commands referencing this task.
3. WHERE `--spec <spec-name>` is provided, THE CLI SHALL read the spec's `tasks.md` from `.kiro/specs/<spec-name>/tasks.md` on the local machine and include the relevant task description in the prompt sent to Agentd.
4. WHERE `--prompt <text>` is provided, THE CLI SHALL use it as the Codex prompt verbatim. IF both `--spec` and `--prompt` are provided, the spec-derived context SHALL be prepended to the operator's prompt.
5. THE `--base` flag SHALL default to resolving `origin/main` (fetched ref). Agentd SHALL run `git fetch --prune origin` before worktree creation to ensure the base ref is current.
6. WHEN Agentd accepts the submission, THE CLI SHALL print the Task_Id and queue position. IF the queue is empty, execution begins immediately.

### Requirement 2.2: Agentd creates a worktree per task

**User Story:** As an operator, I want each task isolated in its own git worktree, so that concurrent submissions do not interfere and each task produces a clean branch.

#### Acceptance Criteria

1. WHEN Agentd begins executing a task, IT SHALL run `git worktree add /work/worktrees/<task-id> -b agent/<task-id> <base>` in the `/work/tokeira` repository.
2. IF the branch `agent/<task-id>` already exists, Agentd SHALL fail the task with status `branch_conflict` and surface the error to the operator via `tkr agent status`.
3. AFTER Codex completes (exit 0), Agentd SHALL commit any uncommitted changes in the worktree with message `agent(<task-id>): <first line of prompt>`. Agentd SHALL NOT push automatically. Push and PR creation are operator-triggered via `tkr agent push` and `tkr agent pr`.
4. IF Codex exits with non-zero, Agentd SHALL preserve the worktree state, mark the task as `failed`, and include the exit code and last 50 lines of JSONL output in the task status.
5. Agentd SHALL NOT delete worktrees automatically. The operator reclaims disk space via `tkr agent cleanup` (a convenience command that removes worktrees for completed/failed tasks older than a configurable threshold).

### Requirement 2.3: Serial execution with queue

**User Story:** As an operator, I want tasks to execute one at a time to avoid resource contention on the workstation, with additional submissions queued in order.

#### Acceptance Criteria

1. Agentd SHALL persist task state to a SQLite database at `/var/lib/tokeira-agentd/state.sqlite`. On daemon restart, tasks in `running` state SHALL transition to `interrupted`. Tasks in `queued` state SHALL remain queued. The queue is durable across restarts. At most one task SHALL be in `running` state at any time.
2. WHEN a task completes (success or failure), Agentd SHALL dequeue the next task and begin execution.
3. THE queue order SHALL be FIFO (first submitted, first executed).
4. `tkr agent status` SHALL show the currently running task and all queued tasks with their positions.
5. THE operator SHALL be able to cancel a queued (not yet running) task via `tkr agent cancel --task <id>`. Cancelling a running task SHALL send SIGTERM to the `codex exec` process and mark the task as `cancelled`.

### Requirement 2.4: Codex invocation via `codex exec`

**User Story:** As an operator, I want Agentd to invoke Codex in non-interactive JSON mode so that the execution is automated and produces structured output.

#### Acceptance Criteria

1. Agentd SHALL spawn Codex via `tokio::process::Command` with the following arguments: `codex exec --cd <worktree-path> --sandbox workspace-write --ask-for-approval never --json --output-last-message <worktree-path>/.agentd/final.md -`. The prompt SHALL be passed via stdin (piped to the child process), NOT as a shell argument.
2. THE child process SHALL NOT inherit `OPENAI_API_KEY`, `AWS_*`, or other secret environment variables. Agentd SHALL configure Codex via `/home/agent/.codex/config.toml` with `[shell_environment_policy] inherit = "core"` and `exclude = ["*KEY*", "*SECRET*", "*TOKEN*", "*PASSWORD*", "AWS_*", "OPENAI_API_KEY"]`. Codex authenticates via `auth.json` (ChatGPT login) or the environment file read by the Codex process itself — not by inheriting secrets into spawned commands.
3. Agentd SHALL read the child's stdout line-by-line, parsing each line as a JSONL event via `serde_json`. Malformed lines SHALL be logged at `warn` level and skipped.
4. Agentd SHALL store the full JSONL stream to `/work/worktrees/<task-id>/.agentd/codex-output.jsonl` for later retrieval via `tkr agent logs`.
5. Agentd SHALL track wall-clock duration of the `codex exec` process and include it in the task completion status.

---

## Feature 3: Task Monitoring and Results

### Requirement 3.1: `tkr agent status` shows task state

**User Story:** As an operator, I want to see the current state of all tasks at a glance, so that I know what is running, what is queued, and what has completed.

#### Acceptance Criteria

1. `tkr agent status` (no flags) SHALL display all tasks known to Agentd: running, queued, completed, failed, cancelled, interrupted.
2. `tkr agent status --task <id>` SHALL display detailed status for one task: state, prompt summary, base branch, worktree path, start time, duration (if completed), exit code (if failed), and queue position (if queued).
3. THE status response SHALL be retrieved from Agentd via the Socket_Protocol over SSM port-forward.
4. EACH task status SHALL include a `codex_events_count` field showing how many JSONL events have been emitted so far (useful for gauging progress on a running task).

### Requirement 3.2: `tkr agent logs` streams Codex output

**User Story:** As an operator, I want to see the raw Codex JSONL output for a task, so that I can understand what Codex did and debug failures.

#### Acceptance Criteria

1. `tkr agent logs --task <id>` SHALL request the JSONL stream from Agentd and print it to stdout.
2. WHERE the task is still running, THE command SHALL stream new events as they arrive (tail -f behaviour) until the operator sends SIGINT.
3. WHEN the operator sends SIGINT while a task is still running, THE command SHALL stop streaming immediately (the task itself continues running on the workstation).
4. WHERE the task is completed or failed, THE command SHALL print the full stored JSONL file and exit.
5. THE `--follow` flag SHALL be implicit for running tasks and a no-op for completed tasks.

### Requirement 3.3: `tkr agent diff` shows the branch diff

**User Story:** As an operator, I want to see what files Codex changed, so that I can review the work before integrating.

#### Acceptance Criteria

1. `tkr agent diff --task <id>` SHALL run both `git diff <base>...HEAD` (committed changes) AND `git diff` + `git status --porcelain=v1` (uncommitted/untracked) in the task's worktree and stream the combined output.
2. THE diff SHALL be retrieved via Agentd (which executes the git command locally) rather than requiring the operator to have the branch checked out locally.
3. WHERE `--stat` is provided, THE command SHALL show `git diff --stat` instead of the full patch.

### Requirement 3.4: `tkr agent review-pack` bundles results for review

**User Story:** As an operator, I want a single command that produces a review-ready bundle (diff + test results + event summary), so that I can make an integration decision without running multiple commands.

#### Acceptance Criteria

1. `tkr agent review-pack --task <id>` SHALL instruct Agentd to: (a) run `cargo test --workspace` in the task's worktree, (b) collect the diff (`git diff <base>..<branch>`), (c) summarise the JSONL events (files changed, tools invoked, errors encountered).
2. THE review pack SHALL be returned as a structured JSON object with fields: `task_id`, `diff_stat`, `full_diff`, `uncommitted_diff`, `untracked_files: Vec<String>`, `test_exit_code`, `test_stdout` (last 200 lines), `test_stderr` (last 200 lines), `event_summary`. The review pack captures the full worktree state, not just committed refs.
3. THE CLI SHALL print the review pack in a human-readable format by default, or raw JSON with `--json`.
4. IF tests fail, THE review pack SHALL still be produced (the test failure is information for the reviewer, not a blocker for producing the pack).

---

## Feature 4: Spec Review via Codex

### Requirement 4.1: `tkr agent review-spec` feeds specs to Codex

**User Story:** As an operator, I want Codex to review a completed spec for ambiguities, inconsistencies, and missing detail before I begin implementation, so that I catch spec-level issues early.

#### Acceptance Criteria

1. `tkr agent review-spec --spec <spec-name>` SHALL read all `.md` files from `.kiro/specs/<spec-name>/` on the local machine and send them to Agentd as a review request.
2. Agentd SHALL invoke `codex exec --cd /work/tokeira --sandbox read-only --ask-for-approval never --json --output-schema <schema-path> --output-last-message <output-path> -` with the spec content piped via stdin. The `read-only` sandbox ensures the review cannot modify any files.
3. THE review prompt SHALL instruct Codex to output a JSON array of findings, each with fields: `severity` (error, warning, info), `location` (file + section reference), `category` (ambiguity, inconsistency, missing_detail, testability, contradiction), and `description`.
4. Agentd SHALL parse the Codex output, extract the structured findings, and return them to the CLI.
5. THE CLI SHALL print findings grouped by severity, with location and description. THE `--json` flag SHALL output raw JSON.
6. IF Codex does not produce valid structured output, THE CLI SHALL fall back to printing the raw Codex response with a warning that structured parsing failed.
7. Agentd SHALL store the spec payload digest (SHA-256 of the concatenated spec files) and the prompt version in the review result metadata, so that findings are reproducible.

### Requirement 4.2: Review prompt is versioned

**User Story:** As a Tokeira maintainer, I want the review prompt to be a versioned constant in the codebase, so that improvements to the review quality are tracked and reproducible.

#### Acceptance Criteria

1. THE review prompt template SHALL live as a Rust constant in the `agentd` crate (or a shared module accessible to both `agentd` and `tkr agent`).
2. THE prompt SHALL include: the EARS pattern rules, the INCOSE quality rules, the Tokeira-specific conventions (from AGENTS.md), and instructions for structured JSON output.
3. THE prompt version SHALL be included in the review request metadata so that findings can be correlated with the prompt version that produced them.

---

## Feature 5: Budget Tracking and Guardrails

### Requirement 5.1: Codex rate-limit detection

**User Story:** As an operator, I want `agentd` to detect when Codex rate limits are exhausted, so that tasks fail fast with a clear message rather than hanging or producing cryptic errors.

#### Acceptance Criteria

1. WHEN `codex exec` emits a JSONL event indicating rate-limit exhaustion (HTTP 429, or an error event containing "rate limit" or "quota exceeded"), Agentd SHALL mark the current task as `rate_limited` and pause the queue.
2. WHEN the queue is paused due to rate limiting, Agentd SHALL log the pause reason and the estimated reset time (if available from the error event) at `warn` level.
3. `tkr agent status` SHALL show the queue state as `paused: rate_limited` with the estimated reset time.
4. THE operator SHALL be able to resume the queue manually via `tkr agent resume` after confirming rate limits have reset. Agentd SHALL NOT auto-resume without operator action (to prevent burn-through of a partially-restored quota).

### Requirement 5.2: `tkr agent usage` shows remaining capacity

**User Story:** As an operator, I want to see my observed Codex usage before submitting work, so that I can plan my task submissions around rate limits.

#### Acceptance Criteria

1. `tkr agent usage` SHALL query Agentd for the last-known rate-limit state and display: tasks completed in the current 5-hour window, tasks completed in the current weekly window, last rate-limit event timestamp, and queue state.
2. THE usage information SHALL be derived from Agentd's internal tracking of Codex JSONL events (specifically, completion events and rate-limit events). Agentd SHALL NOT call any external API to check quota.
3. WHERE no rate-limit event has ever been observed, THE command SHALL display "no rate-limit events observed" independently of task counts. WHERE task counts are displayed without rate-limit event correlation, THE command SHALL include a warning that counts may be incomplete.
4. NOTE: This command reports observed usage from agentd's perspective. It cannot know true remaining quota if the operator also uses Codex elsewhere.

### Requirement 5.3: Kiro budget guardrail

**User Story:** As an operator, I want a hard guarantee that the agent controller never autonomously triggers Kiro sessions, so that my AWS-billed Kiro credits are never consumed without my explicit action.

#### Acceptance Criteria

1. THE Agentd binary SHALL NOT import, link against, or invoke any Kiro API, Kiro CLI, or Claude API.
2. THE `tkr agent` CLI SHALL NOT invoke Kiro sessions as part of any automated workflow. Kiro-mediated work (spec review, integration decisions, conflict resolution) is always operator-initiated through the Kiro IDE, never through `tkr agent`.
3. THE Agentd binary SHALL NOT read or write any Kiro configuration files, tokens, or credentials.
4. THIS requirement SHALL be enforced by a compile-time check: the `agentd` crate's `Cargo.toml` SHALL NOT list any Kiro-related dependency. A CI lint SHALL verify this.

---

## Feature 6: Communication Protocol

### Requirement 6.1: JSON-over-newline Socket_Protocol

**User Story:** As a Tokeira maintainer, I want a simple, debuggable protocol between `tkr agent` and `agentd`, so that the communication layer is easy to test and extend.

#### Acceptance Criteria

1. THE Socket_Protocol SHALL be newline-delimited JSON. Each message is a single JSON object terminated by `\n`. No framing headers, no binary encoding.
2. REQUEST messages SHALL have fields: `id` (monotonic u64), `protocol_version` (integer, currently `1`), `method` (string enum: `submit`, `status`, `logs`, `diff`, `review_pack`, `review_spec`, `usage`, `cancel`, `resume`, `cleanup`, `push`, `pr`, `commit`, `validate`, `doctor`, `codex_login`), and `params` (method-specific JSON object).
3. RESPONSE messages SHALL have fields: `id` (matching the request), `result` (method-specific JSON object on success), `error` (object with `code` and `message` on failure).
4. FOR streaming responses (logs of a running task), Agentd SHALL send multiple response messages with the same `id`, each containing a `chunk` field. The final message SHALL include `done: true`.
5. THE protocol SHALL be documented in a `PROTOCOL.md` file in the `agentd` crate directory.
6. EACH request message SHALL include a `protocol_version` field (integer, currently `1`).
7. Agentd SHALL reject requests with an unsupported `protocol_version` with error code `ProtocolVersionMismatch`.
8. Agentd SHALL enforce a maximum line size of 10 MiB. Lines exceeding this limit SHALL be rejected.
9. Agentd SHALL enforce a 60-second timeout on idle connections (no complete message received). Timed-out connections SHALL be closed.
10. FOR streaming responses, messages SHALL include a `seq` (monotonic sequence number) field for ordering.
11. Agentd SHALL respond to unknown methods with error code `UnknownMethod` rather than crashing.

### Requirement 6.2: SSM port-forwarding for socket access

**User Story:** As an operator, I want `tkr agent` commands to transparently set up SSM port-forwarding to the agentd TCP listener, so that I do not need to manually configure tunnels.

#### Acceptance Criteria

1. WHEN any `tkr agent` command needs to communicate with Agentd, THE CLI SHALL establish an SSM port-forward session: `aws ssm start-session --target <instance-id> --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["18777"],"localPortNumber":["18777"]}'`. The CLI connects to `127.0.0.1:18777` locally. The first message on each connection SHALL include the client authentication token (matching the token in `/etc/tokeira/agentd-env`).
2. THE port-forward session SHALL be reused across multiple `tkr agent` commands within the same CLI invocation. For single-shot commands, the session SHALL be torn down on command exit.
3. IF the port-forward cannot be established (workstation not running, SSM agent not responding), THE CLI SHALL fail with a message suggesting `tkr workstation up` and `tkr agent install`.
4. THE port-forwarding SHALL use the SSM `StartSession` API with document `AWS-StartPortForwardingSession`.

---

## Feature 7: Installation and Codex Setup

### Requirement 7.1: `tkr agent install` provisions the workstation

**User Story:** As an operator, I want a single command that installs `agentd` and Codex on the workstation, so that I can go from a bare remote-workstation to a working agent controller in one step.

#### Acceptance Criteria

1. `tkr agent install` SHALL perform the following steps on the Workstation_Instance via SSM: (a) install the Codex CLI (via the documented installation method), (b) copy the compiled `agentd` binary to a stable path (e.g. `/usr/local/bin/agentd`), (c) write the systemd unit file per Req 1.2, (d) enable and start the service.
2. THE Codex CLI installation SHALL use the official OpenAI-published method. THE exact installation command SHALL be a Rust constant in the `agentd` crate, updatable without a spec change.
3. THE operator SHALL choose an authentication mode: `--auth chatgpt` (default, uses Codex's ChatGPT sign-in flow via `codex login`) or `--auth api-key --api-key-stdin` (reads an API key from stdin). For ChatGPT auth, the install command SHALL invoke `codex login` interactively on the workstation (via SSM session) and verify that `/home/agent/.codex/auth.json` is created. For API key auth, the key SHALL be written to `/etc/tokeira/agentd-env`.
4. IF `agentd` is already installed and running, `tkr agent install` SHALL detect the existing installation, stop the service, replace the binary, and restart. This is the upgrade path.
5. THE `agentd` binary SHALL be cross-compiled for `aarch64-unknown-linux-gnu` (Graviton4) as part of the Tokeira workspace build. The install command copies the pre-built binary; it does not compile on the workstation.
6. `tkr agent codex-login` SHALL be available as a standalone command for re-authenticating Codex without a full reinstall.

### Requirement 7.2: `tkr agent uninstall` removes agent infrastructure

**User Story:** As an operator, I want to cleanly remove the agent controller from a workstation without destroying the workstation itself, so that I can revert to a plain build surface.

#### Acceptance Criteria

1. `tkr agent uninstall` SHALL stop and disable the `tokeira-agentd` service, remove the unit file, remove the `agentd` binary, and remove `/etc/tokeira/agentd-env`.
2. THE uninstall SHALL NOT remove Codex CLI, task worktrees, or JSONL logs. Those are left for the operator to clean up manually or via `tkr agent cleanup`.
3. THE uninstall SHALL NOT modify the `/run/tokeira-agentd/` directory (owned by the remote-workstation bootstrap).
4. AFTER uninstall, `tkr agent status` SHALL fail with a clear message: "agentd is not installed on this workstation. Run `tkr agent install` to set up."

---

## Feature 8: CLI Command Group

### Requirement 8.1: `tkr agent` subcommand group

**User Story:** As an operator, I want the agent commands organised under one clap subcommand group, consistent with the existing `tkr workstation` group, so that the CLI surface is discoverable.

#### Acceptance Criteria

1. `apps/tkr/src/cli.rs` SHALL gain an `Agent { #[command(subcommand)] action: AgentAction }` variant on the top-level `Command` enum.
2. `AgentAction` SHALL declare variants: `Submit`, `Status`, `Logs`, `Diff`, `ReviewPack`, `ReviewSpec`, `Usage`, `Install`, `Uninstall`, `Cancel`, `Resume`, `Cleanup`, `Push`, `Pr`, `Commit`, `Validate`, `Doctor`, `CodexLogin`.
   - `Push` — push the task branch to origin
   - `Pr` — create a draft PR from the task branch
   - `Commit` — manually commit uncommitted changes in a task worktree
   - `Validate` — run the validation profile against a task worktree
   - `Doctor` — check agentd health, Codex auth, bubblewrap, IMDS blocking, repo state
   - `CodexLogin` — re-authenticate Codex on the workstation
3. Every subcommand SHALL accept `--workstation <workstation-id>` and read `~/.tokeira/workstations/.latest` as the default (same resolution as `tkr workstation` commands).
4. THE dispatch in `apps/tkr/src/main.rs` SHALL route each variant to a handler module under `apps/tkr/src/commands/agent/`.

### Requirement 8.2: Consistent output formatting

**User Story:** As an operator, I want `tkr agent` commands to follow the same output conventions as the rest of `tkr`, so that scripting and human reading both work.

#### Acceptance Criteria

1. ALL `tkr agent` commands SHALL support `--json` for machine-readable JSON output.
2. DEFAULT output SHALL be human-readable tables or structured text, consistent with `tkr workstation status` formatting.
3. ERROR output SHALL follow the Tokeira convention: what happened, why, and what to do next.

---

## Feature 9: Correctness Properties

### Requirement 9.1: Task state machine is well-defined

**User Story:** As a Tokeira maintainer, I want a property test asserting that the task state machine only transitions through valid states, so that no combination of events produces an illegal state.

#### Acceptance Criteria

1. THE task state machine SHALL have states: `queued`, `preparing`, `running`, `validating`, `awaiting_publish`, `completed`, `failed`, `cancelled`, `interrupted`, `rate_limited`.
2. Valid transitions SHALL be:
   - `queued → preparing` (task reaches head of queue)
   - `queued → cancelled` (operator cancels)
   - `preparing → running` (worktree created, Codex spawned)
   - `preparing → failed` (git error, branch conflict)
   - `running → validating` (Codex exits 0)
   - `running → failed` (Codex exits non-zero)
   - `running → cancelled` (operator cancels)
   - `running → interrupted` (SIGTERM)
   - `running → rate_limited` (rate-limit event)
   - `validating → awaiting_publish` (validation passes)
   - `validating → failed` (validation fails — distinguish via `failure_kind`)
   - `awaiting_publish → completed` (operator marks done without pushing)
   - `awaiting_publish → cancelled` (operator discards)
   - `rate_limited → queued` (operator resumes)
   - `interrupted → queued` (operator retries)
3. A `failure_kind` enum SHALL distinguish failure causes: `codex_exit_nonzero`, `validation_failed`, `git_failed`, `rate_limited`, `cancelled`, `interrupted`.
4. A `proptest` strategy SHALL generate arbitrary sequences of events and assert that no invalid transition occurs.
5. THE test SHALL live under `apps/agentd/tests/task_state_machine.rs` (or equivalent path for the agentd crate).

### Requirement 9.2: Socket_Protocol round-trips without loss

**User Story:** As a Tokeira maintainer, I want a property test asserting that any valid protocol message serializes and deserializes without loss, so that the communication layer is trustworthy.

#### Acceptance Criteria

1. FOR ALL valid request messages, serializing to JSON and deserializing back SHALL produce an equivalent message.
2. FOR ALL valid response messages, serializing to JSON and deserializing back SHALL produce an equivalent message.
3. THE test SHALL use `proptest` with `Arbitrary` implementations for request and response types.
4. THE test SHALL live under the agentd crate's test directory.

### Requirement 9.3: Worktree isolation is maintained

**User Story:** As a Tokeira maintainer, I want a property test asserting that no two tasks share a worktree path, so that concurrent task state never collides on disk.

#### Acceptance Criteria

1. GIVEN any sequence of task submissions with distinct Task_Ids, THE worktree paths SHALL all be distinct.
2. GIVEN a task submission with a Task_Id that collides with an existing worktree, Agentd SHALL reject the submission with `branch_conflict`.
3. THE test SHALL generate arbitrary Task_Id sequences and verify path uniqueness.

### Requirement 9.5: Crash-recovery test

**User Story:** As a Tokeira maintainer, I want a property test asserting that daemon restart correctly transitions running tasks to interrupted and preserves queued tasks.

#### Acceptance Criteria

1. THE test SHALL simulate daemon restart by persisting state to SQLite, re-loading, and verifying that tasks in `running` state transition to `interrupted` and tasks in `queued` state remain queued.
2. THE test SHALL use `proptest` specifically to generate arbitrary queue states and verify recovery invariants. THE test SHALL NOT use alternative property testing frameworks or manual test cases as a substitute for proptest-generated states.
3. THE test SHALL live under the agentd crate's test directory.

### Requirement 9.6: Task ID sanitization

**User Story:** As a Tokeira maintainer, I want a property test asserting that arbitrary strings are correctly rejected or accepted as task IDs, so that path-traversal attacks are impossible.

#### Acceptance Criteria

1. THE test SHALL use `proptest` to generate arbitrary strings and verify that only strings matching `[a-z0-9][a-z0-9-]{2,63}` are accepted.
2. THE test SHALL verify that strings containing path traversal characters (`..`, `/`, `\`) are always rejected.
3. THE test SHALL live under the agentd crate's test directory.

### Requirement 9.4: JSONL parsing is resilient

**User Story:** As a Tokeira maintainer, I want a property test asserting that the JSONL parser handles arbitrary byte sequences without panicking, so that malformed Codex output cannot crash `agentd`.

#### Acceptance Criteria

1. FOR ALL arbitrary byte sequences presented as a "JSONL line", THE parser SHALL either produce a valid event OR log a warning and skip the line. It SHALL NOT panic.
2. THE test SHALL use `proptest` with `any::<Vec<u8>>()` to generate arbitrary inputs.
3. THE test SHALL live under the agentd crate's test directory.

---

## Feature 10: Sandbox Policy and Security Posture

The workstation runs arbitrary code via `codex exec`. This feature establishes the sandbox boundaries, documents the accepted risks, and defines the hardening path.

### Requirement 10.1: Codex runs in `workspace-write` sandbox mode

**User Story:** As a security-aware operator, I want Codex confined to writing only within the task's worktree, so that a compromised or hallucinating agent cannot corrupt my main checkout, system files, or other tasks' worktrees.

#### Acceptance Criteria

1. Agentd SHALL invoke `codex exec` with `--sandbox workspace-write`. This restricts file writes to the `--cd` directory tree (the task's worktree).
2. THE `--cd` argument SHALL be set to the task's worktree path (`/work/worktrees/<task-id>/`), NOT to `/work/tokeira` (the main checkout). This ensures Codex cannot write to the operator's uncommitted work in the main checkout.
3. IF the operator explicitly requests a less restrictive sandbox (e.g. for a task that needs network writes), THE `submit` command SHALL accept `--sandbox <mode>` and pass it through. THE CLI SHALL print a warning naming the deviation from the default.
4. THE default sandbox mode SHALL be a Rust constant in the `agentd` crate, changeable only by a spec-level edit.
5. THE `tkr agent install` command SHALL verify that `bubblewrap` (bwrap) is installed on the workstation. IF missing, install SHALL install it via `apt-get install -y bubblewrap`. Bubblewrap is required for Codex's Linux sandbox to function.
6. THE Codex config at `/home/agent/.codex/config.toml` SHALL NOT add writable roots beyond the worktree. It SHOULD exclude `/tmp` and `$TMPDIR` from writable roots if practical.

### Requirement 10.2: Accepted risks documented

**User Story:** As a security-aware operator, I want the accepted risks of running `codex exec` on the workstation explicitly documented, so that I can make an informed decision about what I'm trusting.

#### Acceptance Criteria

1. THE following risks SHALL be documented in the spec's design.md and in the operator guide:
   - **Network exfiltration**: `workspace-write` sandbox does NOT restrict network access. A malicious build script or Codex-generated command can `curl` secrets to an external endpoint. Mitigation: worktree isolation + branch review before merge + the workstation's IAM role has no access to secrets beyond SSM core.
   - **Build script execution**: `cargo build` runs arbitrary `build.rs` from dependencies. A compromised crate can read the `OPENAI_API_KEY` from the environment. Mitigation: the API key is scoped to Codex usage only; the operator's primary credentials (GitHub, AWS root) are not on the workstation.
   - **Instance-profile credential exposure**: A compromised process could obtain credentials for the workstation's instance profile unless IMDS is blocked. The blast radius is whatever permissions that instance profile has: SSM managed-instance registration and any future permissions added to the role. Mitigation: IMDS blocked for agent UID; the instance profile SHALL NOT include `ssm:StartSession`, `ec2:*`, `iam:*`, `sts:AssumeRole`, `secretsmanager:*`, `ssm:GetParameter*`, or production DSQL/RDS permissions.
   - **IMDS credential theft**: A process on the instance can reach the EC2 metadata service to obtain temporary credentials for the instance role. Mitigation: the role's permissions are minimal (SSM core only); IMDSv2 with hop limit 1 is enforced by the remote-workstation bootstrap.
2. EACH accepted risk SHALL name: the threat, the existing mitigation, and the deferred hardening (if any).

### Requirement 10.3: Deferred hardening path

**User Story:** As a Tokeira maintainer, I want the future hardening options documented so that a follow-up spec can pick them up without re-analysing the threat model.

#### Acceptance Criteria

1. THE following hardening options SHALL be documented as deferred (not implemented in this spec):
   - **Network namespace isolation**: Run `codex exec` inside a network namespace that allows only `crates.io`, `github.com`, and `static.rust-lang.org`. Blocks arbitrary egress.
   - **Seccomp profile**: Restrict syscalls available to the Codex child process. Blocks `ptrace`, raw socket creation, and kernel module loading.
   - **Bubblewrap (bwrap) wrapping**: Run `codex exec` inside a Bubblewrap sandbox with a read-only root filesystem overlay, bind-mounting only the worktree as writable. Provides filesystem isolation beyond what `--sandbox workspace-write` offers.
   - **Ephemeral API key rotation**: Generate a short-lived Codex API key per task (if OpenAI supports scoped keys in the future). Limits the blast radius of key theft to one task's duration.
   - **IMDSv2 hop limit enforcement**: Already set to 1 by the remote-workstation bootstrap; verify it is not overridable by the Codex process.
2. EACH deferred option SHALL name: what it mitigates, estimated implementation effort (low/medium/high), and the trigger condition that would justify implementing it (e.g. "implement network namespace isolation if the workstation begins running untrusted third-party code or if a supply-chain incident is observed").

### Requirement 10.4: API key isolation

**User Story:** As a security-aware operator, I want the Codex API key stored with minimal exposure, so that a compromised Codex process cannot trivially exfiltrate it to a third party.

#### Acceptance Criteria

1. THE Codex API key SHALL be stored in `/etc/tokeira/agentd-env` (mode 0600, owned by the shell user) and loaded via systemd's `EnvironmentFile=` directive. It SHALL NOT be passed on the command line (visible in `/proc/<pid>/cmdline`).
2. THE API key SHALL NOT be passed to Codex-spawned child processes. Codex SHALL authenticate via `auth.json` (ChatGPT mode) or read the key from its own config — NOT from inherited environment variables. The Codex config SHALL use `[shell_environment_policy] inherit = "core"` with `exclude = ["*KEY*", "*SECRET*", "*TOKEN*", "*PASSWORD*", "AWS_*", "OPENAI_API_KEY"]` to prevent secret leakage to build scripts and test processes.
3. THE API key SHALL be scoped to Codex usage only. THE operator guide SHALL recommend using a dedicated OpenAI API key for the workstation rather than their primary account key, so that revocation does not disrupt other OpenAI usage.
4. `tkr agent install --api-key` SHALL accept the key via stdin (piped) or via a flag. IF passed via flag, THE CLI SHALL warn that the key will appear in shell history and recommend the stdin path: `echo $KEY | tkr agent install --api-key-stdin`.

---

## Feature 11: User Separation and Process Isolation

### Requirement 11.1: Dedicated `agent` user for Codex execution

**User Story:** As a security-aware operator, I want Codex to run as a dedicated non-privileged user rather than the admin shell user, so that a sandbox escape cannot escalate to system-level access.

#### Acceptance Criteria

1. `tkr agent install` SHALL create a system user `agent` (no login shell, no sudo, home directory at `/home/agent/`). The user SHALL be a member of a `tokeira` group that has read access to `/work/repo` and write access to `/work/worktrees/`.
2. THE `agentd` systemd unit SHALL run as the `agent` user (via `User=agent` in the unit file). The `agentd` binary itself runs as `agent`; it spawns `codex exec` which inherits the same UID.
3. THE `agent` user SHALL NOT have sudo access. THE user SHALL NOT be in the `sudo`, `wheel`, or `admin` groups.
4. THE `agent` user SHALL NOT have read access to the admin user's home directory (`/home/ubuntu/` or `/home/ec2-user/`). This prevents Codex from reading the operator's SSH keys, shell history, or any credentials stored in the admin home.
5. THE `/run/tokeira-agentd/` directory SHALL be owned by `agent:tokeira` with mode `0750`. The admin user can still reach the socket (via group membership) for debugging, but Codex processes cannot write outside their designated paths.

### Requirement 11.2: `CODEX_HOME` isolation

**User Story:** As a security-aware operator, I want the Codex authentication state stored in a protected location that is excluded from all export paths, so that a review pack or snapshot cannot accidentally leak my Codex credentials.

#### Acceptance Criteria

1. THE environment variable `CODEX_HOME` SHALL be set to `/home/agent/.codex` in the systemd unit's `EnvironmentFile`. The directory SHALL be mode `0700`, owned by `agent:agent`.
2. THE file `/home/agent/.codex/auth.json` SHALL be mode `0600`. Agentd SHALL NOT read, log, or include this file in any JSONL output, review pack, or task state.
3. THE review-pack generation (Req 3.4) SHALL explicitly exclude the following paths from any diff or file listing: `~/.codex/**`, `~/.aws/**`, `.env*`, `*.pem`, `*.key`, `id_rsa`, `id_ed25519`, `/etc/tokeira/agentd-env`.
4. THE `tkr agent install` command SHALL set `CODEX_HOME` in the environment file and create the directory with correct ownership before starting `agentd`.

### Requirement 11.3: IMDS blocking for the `agent` user

**User Story:** As a security-aware operator, I want the Codex process unable to reach the EC2 metadata service, so that a compromised agent cannot obtain instance role credentials.

#### Acceptance Criteria

1. `tkr agent install` SHALL add an iptables rule blocking outbound traffic from UID `agent` to `169.254.169.254` (IPv4) and `fd00:ec2::254` (IPv6). The rule SHALL be persisted via `iptables-save` / `netfilter-persistent` so it survives reboots.
2. THE `agentd` process itself (running as `agent`) SHALL NOT need IMDS access. All AWS SDK calls (if any future requirement adds them) would run from the admin user or a separate service.
3. THE iptables rule SHALL be documented in the operator guide with the rationale: "prevents Codex-spawned processes from obtaining temporary AWS credentials via the metadata service."
4. IMDS blocking for the `agent` user is invariant. Tasks that require AWS credentials are out of scope for v1. No `tkr agent` command SHALL modify the iptables IMDS rule at runtime.

---

## Feature 12: Policy Enforcement

### Requirement 12.1: `tkr agent` refuses unsafe modes by default

**User Story:** As a security-aware operator, I want the CLI to block known-dangerous Codex configurations unless I explicitly opt in, so that a typo or copy-paste from documentation cannot accidentally run Codex without sandboxing.

#### Acceptance Criteria

1. THE following configurations SHALL be blocked by default (the CLI SHALL refuse to submit a task with these settings):
   - `--sandbox danger-full-access`
   - `--dangerously-bypass-approvals-and-sandbox` (or `--yolo` alias)
   - Running as root or a sudo-capable user (detected by checking UID 0 or membership in sudo/wheel/admin groups on the remote)
   - Pushing directly to `main` or `master` (the branch name `agent/<task-id>` is enforced; any `--base main` with `--push` is rejected)
2. WHERE the operator genuinely needs an unsafe mode, THE CLI SHALL require `--i-accept-risk` as an additional flag. THE CLI SHALL print a warning to stderr naming the specific risk being accepted.
3. THE blocked-mode list SHALL be a Rust constant in the `tkr agent` CLI crate, changeable only by a spec-level edit.
4. A CI lint SHALL verify that the `agentd` crate does not contain the string `danger-full-access` or `dangerously-bypass` outside of the policy-enforcement module (prevents accidental hardcoding of unsafe defaults).

### Requirement 12.2: Review pack secret scanning

**User Story:** As a security-aware operator, I want the review pack to be scanned for secrets before it leaves the workstation, so that I never accidentally expose credentials in a review artifact.

#### Acceptance Criteria

1. BEFORE producing a review pack (Req 3.4), Agentd SHALL scan the diff output against a set of secret-detection patterns (similar to `gitleaks` rules). The pattern set SHALL cover: AWS access keys, GitHub tokens (classic and fine-grained), OpenAI API keys, SSH private key markers, and generic high-entropy strings matching `[A-Za-z0-9+/=]{40,}`.
2. IF secrets are detected, THE review pack SHALL include a `secrets_detected` field listing the file paths and line numbers where matches were found. THE actual secret values SHALL be redacted (replaced with `[REDACTED:<pattern-name>]`).
3. THE CLI SHALL print a warning when displaying a review pack that has `secrets_detected` entries, advising the operator to investigate before sharing the pack.
4. THE secret-detection pattern set SHALL be a Rust constant, updatable without a spec change (same maintenance model as the `remote-exec` secret scanner in remote-workstation Req 10.3).

### Requirement 12.3: Branch naming enforcement

**User Story:** As a Tokeira maintainer, I want all agent-produced branches to follow a predictable naming convention, so that they are immediately identifiable in `git log` and GitHub and cannot collide with human-authored branches.

#### Acceptance Criteria

1. ALL branches created by Agentd SHALL follow the pattern `agent/<task-id>`. No other branch prefix SHALL be used.
2. Agentd SHALL refuse to create a branch that does not match this pattern, even if the operator passes a custom branch name via the protocol.
3. THE commit message for agent-produced commits SHALL follow the pattern `agent(<task-id>): <first line of prompt>`. This makes agent commits immediately identifiable in `git log --oneline`.
4. Agentd SHALL NOT force-push, rebase, or amend commits on agent branches. Each task produces exactly one commit (or zero, if Codex made no changes).
