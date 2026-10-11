# Changelog

All notable user-facing changes to this project are documented here.

## 0.6.0 on 2026-10-11

### Added

* The runtime reports each lane's backlog as tokeira_runtime_lane_queue_depth and how often its runs interleave as tokeira_runtime_lane_carry_overs_total.
* tokeira_runtime_scanner_stale_deleted_total counts the due timer rows the scanner deleted because their run was gone.

### Changed

* tokeira-storage's RunRepository gains delete_due_timer_if_matches, and DueTimer carries the fire time it was read with.
* Shard recovery and worker-compute sampling read only runs that hold recoverable work, indexed by a new workflow_hot column (migrations V072 and V073).
* DSQL commits no longer copy each activity into the activity_state table, which nothing read; activities stay in run state, and run deletion still clears old rows.
* Upgrade with every node stopped.

### Fixed

* Workflow admission and task-token epochs now use execution homes, and startup repairs legacy reset placement before serving.
* A node that has lost an execution home's lease can no longer commit writes for that home; every write of a run now checks the lease in its own transaction.
* A runtime lane no longer deadlocks when its mailbox is full and another run's command waits behind the active run; lane commands keep arrival order.
* A run closed by its worker now deletes its pending timers, whose rows were rejected on every timer scan and could stop a shard's timers from firing.
* A closed run's activities no longer accept heartbeats or retry after a failure or timeout; their calls answer that the workflow execution already completed.
* The timer scanner deletes a due timer whose run has closed or no longer exists, so rows left by earlier releases stop blocking a shard's timers.
* DescribeWorkflowExecution now lists the pending activities of a terminated, timed-out or reset run, as Temporal does, instead of dropping them at the close.
* By-id activity calls on a closed workflow run now answer that the workflow execution already completed, even when the activity is gone or never started.
* Shard recovery on DSQL no longer fails binding its page sizes, elapses a delayed start instead of firing a timer, and skips timer rows the engine rejects.
* A shard takeover no longer times out an activity that is still heartbeating. Each heartbeat's time is stored with the run, and Describe reports last_heartbeat_time.
* A single node without a placement controller now runs the recovery sweep for each shard at startup, so pending workflow tasks and timeouts resume after a restart.
* On DSQL, demoting a task whose copy the durable backlog already holds no longer fails the grace scanner's whole batch on every retry; the stored copy is kept.
* A child workflow start that hits a transient error is now retried, as Temporal does, instead of being reported to its parent as failed, and so is its confirmation.
* Histories longer than 1,000 events on DSQL are no longer cut short, and GetWorkflowExecutionHistory and its reverse page at most 256 events, as Temporal does.
* Requests over Temporal's payload limits (2 MiB per payload or memo, 100 search attributes within 40 KiB) are refused, or recorded as server failures, as Temporal does.
* A workflow task's command over Temporal's payload limits now terminates the workflow, as Temporal does; too many search attributes or Nexus operations fail the task.
* A run over Temporal's history, history batch or state limits is now terminated, as Temporal does; buffered events and stored activity failures are bounded too.
* A run now refuses signals past 10,000 and updates past Temporal's in-flight, payload and total limits, answering each caller, and a signal's sender, as Temporal does.
* Workflow commits carry deployment-version projection history in run state, avoiding reads of prior projection images while preserving visibility output.
* Paused workflows no longer start queued tasks, stale offers cannot start a retried workflow-task attempt, and reset successors place their timers at the execution home immediately.
* The migration runner and `tkr schema setup` now retry a schema conflict and wait for index builds, as engine startup does.
* Sticky workflow-task schedule-to-start deadlines now survive shard recovery, even after sticky affinity is cleared, and a concurrent start keeps its timeout.
* After a restart, a client's retry of an update the run admitted is delivered and answered, no empty workflow tasks repeat, and updates a reset reapplied count as in flight.
* Building the Rust workspace no longer requires a system protoc installation. Temporal SDK and Tokeira API bindings use protox, matching the repository's protobuf generator.
* Recovery now scopes timeout and callback tracking to the current shard acquisition and preserves replacement entries during concurrent scans.
* Deletions, resets and backlog spills now stay within DSQL's transaction limits, and a reset no longer leaves two runs open when a start lands while it runs.
* Activity inputs and headers now live only in history, so a workflow with many large pending inputs keeps committing, and each start reads them from history.
* A signal to another workflow that names no namespace now reaches it in the sender's namespace, and the events resolving such signals and cancel requests name that namespace.
* A distributed tokeirad now refuses to start on a DSQL schema its release can't run on, and `tkr schema setup` now migrates Compose and ECS deployments.





























## 0.5.1 on 2026-10-02

### Fixed

* An event recorded while an update's speculative workflow task is in flight no longer reuses the task's event ids; child, Nexus, external-workflow and pause events now buffer.





## 0.5.0 on 2026-09-28

### Changed

* tokeira-proto's connect bindings regenerate on buffa 0.9; handlers take ServiceRequest or InboundStream, and stream items expose their zero-copy view through view().

### Security

* connectrpc 0.8.2 closes RUSTSEC-2026-0304 for the conformance control listener; ringbuf 0.5.2 closes RUSTSEC-2026-0293; chacha20 moves off the yanked 0.10.0 to 0.10.2.
* connectrpc 0.9.1 closes RUSTSEC-2026-0304 on the placement controller's membership stream; the workspace moves to one connectrpc 0.9 and buffa 0.9 line.

## 0.4.1 on 2026-09-19

### Added

* tokeira_engine::chasm now carries the Component derive, every contract a library names, namespace_id_for, and TypedEngine::reference to resolve a run by business id.

## 0.4.0 on 2026-09-17

### Added

* CHASM tasks carry stable names and task-owned codecs; libraries register typed pure and side-effect handlers and search-attribute definitions.
* Standalone activities persist completion callbacks and a deployment-version target and drive callback delivery through registered tasks, gated off.
* Standalone activities deliver completion callbacks over Nexus or into a staging component; versioned standalone tasks go only to scoped workers of the matching release.
* Embedders build the engine with extension CHASM libraries, executors and a clock behind the chasm-extensions feature; a config key gates standalone-activity callbacks.
* Embedded CHASM applications can list and count their component executions with scoped pagination and typed visibility attributes.

### Changed

* CHASM current-run pointers are keyed by archetype in a new table; legacy activity pointers are backfilled at engine start and the old table stays read-only.
* CHASM runs registered pure tasks, fences external outcomes, and rebuilds pending timers and effects from committed state on startup and periodic scans.
* Crate and readiness documentation describes the CHASM plane as an extension surface: registered libraries, typed handlers, the executors and rebuild scan, the archetype pointer.

### Fixed

* Standalone activity starts now commit atomically, preventing stranded runs and reporting state_transition_count as 1 immediately after start, matching Temporal v1.31.0.
* Concurrent starts fence the current-run pointer, so duplicate request IDs return one execution, and losing creates leave no orphaned nodes.
* The CHASM rebuild pass isolates an execution it cannot serve instead of stalling every other one; start refuses storage it cannot serve and names why.
* A scoped worker can claim a versioned standalone activity when the CHASM clock is a simulation clock; task-token provenance now expires on real time.
* Advance the DSQL schema contract to V071 so default-gate embedded startup installs and accepts the CHASM pointer and backfill tables.

### Security

* rustls 0.23.45 closes RUSTSEC-2026-0285 (TLS 1.3 handshake messages accepted across encryption levels); aws-lc-rs and rustls-webpki move with it.







## 0.3.1 on 2026-09-07

### Fixed

* Existing embedded DSQL now honors the configured database endpoint through wake refresh and reports the pool locator, enabling PrivateLink without new configuration fields.

## 0.3.0 on 2026-09-06

### Added

* Visibility queries resolve predicates against registered fields and validate literal types before execution; an absent or blank predicate matches every execution in scope.

### Changed

* Public storage signatures now use SQLx 0.9 types; the Aurora DSQL connector moves to 0.2.2 in the 0.3.0 release train.
* The gRPC edge, in-process endpoint, listeners, and generated bindings move to tonic 0.14, prost 0.14, and hyper 1; public message and status types change accordingly.

### Removed

* Regenerating the protobuf bindings no longer needs a system `protoc`; `proto-sync` compiles the vendored protos in pure Rust.

### Security

* Remove the legacy AWS HTTPS client and its four rustls-webpki and h2 advisory exceptions from the workspace.

## 0.2.0 on 2026-09-06

### Added

* Workflow-task started events and poll responses carry Temporal's continue-as-new advice (`suggest_continue_as_new`, its reasons, and the persisted history size).
* Embedded engines can attach a TCP listener after startup (`Engine::listen`) that serves the same Temporal gRPC services as the in-process endpoint.

### Changed

* The embedded engine's client seam moves to Temporal Rust SDK 1.0.0; embedding hosts should upgrade their SDK pins to match.
* AWS SDK clients in the published crates no longer enable the legacy rustls connector, and Postcard drops its unused heapless default; formats and behaviour are unchanged.
* Scoped worker credentials may describe their own namespace by exact name, so standard Temporal SDK Workers (Core-based Python, TypeScript, .NET, and Rust) start on the scoped credential alone; describing a namespace by ID, listing namespaces, and every namespace mutation stay denied.
* Describe reports the history size accounted at commit; state and history blobs gain versioned envelopes, so stores written by 0.1.2 must be recreated before upgrading.

## 0.1.2 on 2026-09-03

### Added

* Add a deterministic, resumable release engineering pipeline.

### Changed

* The embedded engine's client seam moves to Temporal Rust SDK 0.8.0; embedding hosts should upgrade their SDK pins to match.

### Fixed

* Bound Aurora DSQL connection-class pressure warnings: sustained permit contention warns at most once per minute per class instead of flooding the log.
* Validate a rendered configuration root standalone: artifact validators now live in the production validation module, and tkr validates an explicit rendered root without deployment admission.

