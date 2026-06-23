# Hand-over — `nexus-async-completion` implementation (Wave 1 in flight)

**Author:** Kiro · **Date:** 2026-06-22 · **For:** Claude (continuing implementation)
**Branch:** `main` (tokeira pushes directly to `main`) · **Last clean commit:** `7d4e24cc`

> The working tree is **dirty with non-compiling Wave 1 changes** (see §4). Do **not**
> `git reset`/`checkout`/`stash drop` — that work is hours of in-flight edits. Resume from it.

---

## 1. What this is

Implement **async Nexus operation completion delivery** so a started async Nexus operation's result
is delivered back to the caller workflow. This is the last engine blocker for **Odori 2.4**'s durable
path (Python `@workflow_run_operation` backed by an `AgentWorkflow` that completes asynchronously).

Today the runtime's `DispatchOp::DispatchCompletionCallback` is a **no-op stub**, so a started async
op never resolves and the caller hangs to schedule-to-close timeout. The synchronous Nexus round-trip
(handler replies `SyncSuccess` inline) is already verified end-to-end over gRPC (Odori's
`nexus-roundtrip-probe`, 2026-06-22) — only the **async** (callback-delivered) path is missing.

**Spec (complete, committed):** `.kiro/specs/nexus-async-completion/{requirements,design,tasks}.md`.
Read all three. The design is ground-truthed to Temporal **v1.31.0**; tasks are 9 dependency-ordered
waves with every property P1–P9 mapped to a required test task.

## 2. Non-negotiable conventions (this repo)

- **Ground truth = v1.31.0.** `proto/upstream/` for wire shape; Temporal source at tag `v1.31.0` via
  `git -C ../temporal show v1.31.0:<path>` and `git grep <pat> v1.31.0 -- <path>`. Never web-search
  Temporal; never infer from `target/` artifacts. Cite source path + tag in comments for non-obvious
  behaviour. A green test on a guessed contract is worse than a raised question.
- **Kernel purity** (`tokeira-kernel`): no I/O, async, storage, metrics, network, no config reads. It
  derives effects (dispatch ops, state) only. Delivery (HTTP, scanner) is **runtime**; the inbound
  endpoint is **edge**.
- **Build-phase migrations / schema:** fold new fields into base structs with `#[serde(default)]`; no
  `ALTER`. (Applies to `CompletionCallback.next_attempt_at` — already done this way.)
- **Verification gates (per touched crate):** `cargo +nightly fmt -p <pkg>`,
  `cargo lint -p <pkg>` (NOT raw clippy — `cargo lint` is the alias with `-D warnings`),
  `cargo test -p <pkg>`; for doc changes `RUSTDOCFLAGS="-D warnings" cargo doc -p <pkg> --no-deps`.
  `cargo check --workspace` to catch downstream exhaustive-match breakage.
- **Commits:** author the message via `fs_write` to `artifacts/cm-*.txt`, then
  `git commit -F artifacts/cm-*.txt`, then `rm -rf artifacts` (the embedded terminal truncates long
  `-m`). Multiple focused commits OK. Push to `main` after committing.
- **NEVER commit:** `.claude/` and
  `.kiro/specs/temporal-functional-conformance/reference/runall-results.json` (both show as untracked
  — leave them). Stage spec/code files explicitly; never `git add .`.
- **No explicit sleeps in tests** — synchronize on observable state (channels/Notify/condvar).

## 3. Locked design decisions (do not relitigate)

- **Token is NOT signed.** v1.31.0's callback token is `{v:1, d:base64(proto)}`, version-checked only
  (`common/nexus/callback_token.go @ v1.31.0` — "encryption support will come later"). Integrity
  comes from op-fencing (`StaleNexusResolution`/`UnknownNexusOperation`), tokeira's analogue of the
  `StateMachineRef` staleness check. Match this: versioned, opaque, version-checked.
- **Inbound HTTP `/nexus/callback` endpoint IS in scope**, hand-rolled on **`hyper`** (no new
  dependency; bump lockfile to `hyper 1.10.1` via `cargo update -p hyper@1.9.0 --precise 1.10.1` in
  Wave 5). Firing POSTs over the Nexus completion HTTP protocol; `temporal://system` resolves to
  tokeira's own listener (the loopback v1.31.0 does via `routeSystemCallbackRequest`). Only
  **cross-cluster routing** of the POST defers to `nexus-multi-cluster`.
- **Outcome derivation** mirrors `GetNexusCompletion @ v1.31.0` (`service/history/workflow/
  mutable_state_impl.go`): completed→result payload, failed→failure, canceled→`CanceledFailureInfo`,
  terminated→`TerminatedFailureInfo`, timed-out→`TimeoutFailureInfo`, continued-as-new→upstream
  internal-error (tokeira deliberately maps it to a `failed` completion to avoid hanging the caller).
  Failure **synthesis** (proto) happens in the runtime, not the kernel.
- **Config (Wave 0, committed `7d4e24cc`):** `PolicyConfig.nexus_completion` — `http_addr`
  (`0.0.0.0:7253`), `system_callback_url` (`http://127.0.0.1:7253`), retry policy (1s initial / 1h max
  / 2.0 coefficient; `retry_max_attempts = 0` = unbounded, matching v1.31.0 `NoInterval`). The runtime
  computes `next_attempt_at` from this and passes it into the kernel (kernel does no backoff math).

## 4. EXACT current state — Wave 1 (kernel), IN FLIGHT, does not compile

Wave 0 is **done and committed** (`7d4e24cc`). Wave 1 edits are **uncommitted** in the working tree.

### Done (uncommitted, correct):
- `crates/tokeira-kernel/src/state.rs` — `CompletionCallback` gained `next_attempt_at:
  Option<OffsetDateTime>` (`#[serde(default)]`, doc'd).
- All six kernel `CompletionCallback {…}` literals updated with `next_attempt_at: None`:
  `tests/golden_tests.rs`, `tests/property_tests.rs`, `src/translate/to_internal.rs` (edge),
  `src/translate/history_serializer.rs` (edge), `src/runtime/mod.rs` (runtime),
  `tests/runtime_lane.rs` (runtime).
- `crates/tokeira-kernel/src/transition.rs` — added `pub enum CallbackCompletionOutcome { Success {
  result: Option<Payload> }, Failed { failure: Payload }, Canceled { details: Option<Payloads> },
  Terminated, TimedOut, ContinuedAsNew }`; added `outcome: CallbackCompletionOutcome` field to
  `DispatchOp::DispatchCompletionCallback`. (`Payload` import added.)
- `crates/tokeira-kernel/src/command.rs` — added `pub enum CallbackAttemptOutcome { Succeeded,
  RetryableFailure { failure: Payload, next_attempt_at: OffsetDateTime }, NonRetryableFailure {
  failure: Payload } }`, `pub struct CompletionCallbackAttemptedRequest { callback_index: usize,
  outcome: CallbackAttemptOutcome, now: OffsetDateTime }`, and
  `Command::CompletionCallbackAttempted(CompletionCallbackAttemptedRequest)`.
- `crates/tokeira-kernel/src/kernel.rs`:
  - Added free fn `callback_completion_outcome(kind: &HistoryEventKind) ->
    Option<CallbackCompletionOutcome>` (maps terminal events → outcome; cites `GetNexusCompletion`).
  - `schedule_completion_callbacks` now derives the outcome from `self.history_events.last()` and
    attaches it to the dispatch op.
  - Added the apply dispatch arm `Command::CompletionCallbackAttempted(req) =>
    self.apply_completion_callback_attempted(loaded, req)`.
  - Added two `Reject` variants: `UnknownCompletionCallback(usize)`,
    `CompletionCallbackAlreadyTerminal(usize)`.
  - Added `CallbackCompletionOutcome` to the `transition::{…}` import.

### NOT done — the resume point (in order):
1. **Write `apply_completion_callback_attempted`** in `kernel.rs` (THE current compile error:
   `no method named apply_completion_callback_attempted`). Also add `CompletionCallbackAttemptedRequest`
   and `CallbackAttemptOutcome` to the `command::{…}` import in kernel.rs. **Semantics (exact):**
   - Load the run. **Allow a CLOSED run** (callbacks fire post-close) — do NOT use `expect_open`.
     `LoadedRun::Absent` → `Reject::MissingRun`. (Pattern: see how other methods get the state, but
     skip the open check. The state is `WorkflowState` either way.)
   - `req.callback_index` out of range of `state.completion_callbacks` → `Reject::UnknownCompletionCallback(idx)`.
   - If that callback's `state` is terminal (`Succeeded`/`Failed`) →
     `Reject::CompletionCallbackAlreadyTerminal(idx)`.
   - Build a `TransitionBuilder::new(state, req.now)`. Mutate
     `builder.state.completion_callbacks[idx]` per `req.outcome`:
     - `Succeeded` → `state = Succeeded`, `next_attempt_at = None`.
     - `RetryableFailure { failure, next_attempt_at }` → `state = BackingOff`, `attempt += 1`,
       `last_attempt_failure = Some(failure)`, `next_attempt_at = Some(next_attempt_at)`.
     - `NonRetryableFailure { failure }` → `state = Failed`, `last_attempt_failure = Some(failure)`,
       `next_attempt_at = None`.
   - **Emit no history event** (callback lifecycle is mutable-state, not history) and **no dispatch op**
     here (the re-fire is the runtime scanner's job, Wave 4). Return `builder.finish()` (or whatever the
     builder's terminal method is — check the other apply methods; some return via a `finish()` /
     building `Transition` directly). The transition still bumps `transition_seq` (a state-only commit
     on a closed run). **NOTE/RISK:** confirm storage `commit_transition` accepts a transition on a
     CLOSED run (most commands reject closed runs at the kernel; this one must be allowed). If storage
     rejects closed-run writes, that's a storage-layer change to allow callback-state transitions —
     raise it; do not hack around it.
2. **Downstream exhaustive-match fixes** (surface after the kernel compiles; run `cargo check
   --workspace`):
   - `crates/tokeira-runtime/src/publisher.rs` — the `DispatchOp::DispatchCompletionCallback { … }`
     match arm (the no-op stub, ~line 1209) must bind the new `outcome` field (`{ callback_index,
     callback, outcome }`). For Wave 1, just bind it (`let _ = outcome;`) to compile; real delivery is
     Wave 4.
   - Any exhaustive `match` on `Command` (runtime adapter / edge) must handle
     `Command::CompletionCallbackAttempted` — check `crates/tokeira-runtime` and `crates/tokeira-edge`.
   - Any exhaustive `match` on `Reject` (edge error mapping, `grpc/errors.rs`) must handle the two new
     variants (map to an internal/edge error; these are not client-facing).
   - Any exhaustive `match` on `DispatchOp` elsewhere.
3. **Golden tests** (`tests/golden_tests.rs`, task 1.5): `DispatchCompletionCallback` carries the
   correct `outcome` per close kind (completed/failed/canceled/terminated/timed-out/continued-as-new);
   `CompletionCallbackAttempted` transitions for each `CallbackAttemptOutcome`; out-of-range and
   already-terminal rejections.
4. **Property tests** (`tests/property_tests.rs`): **P4** (task 1.6 — lifecycle well-formed/bounded),
   **P2** (task 1.7 — close kind → outcome variant). Tag `// Feature: nexus-async-completion, Property N`.
5. **Wave 1 gates:** `cargo +nightly fmt`, `cargo lint`, `cargo test -p tokeira-kernel`, then
   `cargo check --workspace`. Commit (see §2) and update `tasks.md` checkboxes (1.1–1.7, checkpoint 2).

## 5. Remaining waves (after Wave 1) — see `tasks.md` for the full text

- **W3 (runtime types):** `NexusCompletionToken` (`{version, originator_run_key, operation_id,
  scheduled_event_id, request_id}`, `{v,d}` base64, version-checked) + `NexusCompletionClient` trait
  (reqwest) + Noop. Consts `TEMPORAL_CALLBACK_TOKEN_HEADER`, `SYSTEM_CALLBACK_URL`.
- **W4 (runtime delivery):** outbound attachment (token + callback_url on the Worker `NexusTask` /
  `start_operation_to_proto`), the real `DispatchCompletionCallback` handler (build outcome→HTTP body,
  POST, map status→`CompletionCallbackAttempted`), the backoff retry scanner (mirror
  `scan_nexus_timeouts_once`).
- **W5 (edge + HTTP):** emit callback fields in `start_operation_to_proto`; inbound `/nexus/callback`
  handler (decode token+state+body → `resolve_nexus_operation`, mirror `completionHandler.CompleteOperation
  @ v1.31.0`); the hyper server in `apps/tokeirad` serve path + the `hyper 1.10.1` bump; Describe surface.
- **W7 (integration):** async analogue of the sync round-trip in `apps/tokeirad/tests/` (handler
  workflow closes → loopback POST → caller observes `NexusOperationCompleted`).
- Reuses (no new work): `Command::NexusOperationResolved` → `NexusOperation*` events; the
  `RespondNexusTaskFailed` failure conversion in `crates/tokeira-edge/src/translate/nexus.rs`.

## 6. Odori coordination (downstream consumer)

- Odori keeps two throwaway, uncommitted probes in **their** repo (`tokeira-odori`):
  `crates/odori-control/src/bin/nexus-roundtrip-probe.rs` and `nexus_handler_verify.py`. Don't touch
  them; they're Odori's reference.
- Odori can stage 2.4 on a **synchronous** Nexus handler today (works on `main`). The **async**
  `@workflow_run_operation` path needs this spec. When this lands, Odori swaps sync→async.

## 7. Quick map of the engagement so far (committed on `main`)

`8ef10dc9`..`0b01931e` conflict-policy + Nexus failure-DTO + `worker_may_ignore` + UseExisting→success
(the `TestNexusWorkflowTestSuite` blockers). `dbfcd43c`/`755df85c` reconciled the Nexus spec tracking
(C4a done; C4b round-trip verified). `aae8ae24` async-completion spec. `7d4e24cc` Wave 0 config.
The big-picture conformance state is in `.kiro/specs/temporal-functional-conformance/reference/FINDINGS.md`.
