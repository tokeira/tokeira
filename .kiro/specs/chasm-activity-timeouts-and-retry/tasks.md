# Implementation Plan: CHASM Activity Timeouts and Retry

## Overview

Ground-truth every behaviour to v1.31.0 (`chasm/lib/activity/{activity,activity_tasks,statemachine}.go`)
and cite it. The kernel stays pure; activity semantics live in `tokeira-chasm-activity` (pure) +
`tokeira-edge` (bridge); the runtime adds only a clock+loop sweeper. Verify each stage with
`cargo +nightly fmt`, `cargo lint`, `cargo test` on the touched crate; the Tier-2 leaves are confirmed
via the operator-invoked live corpus (Stage 7).

## Tasks

### Stage 1 — Pure retry decision (`tokeira-chasm-activity`)

- [ ] 1.1 Fold the full retry policy into `ActivityState` (initial interval, backoff coefficient, max
  interval; `maximum_attempts` already present) and add `last_heartbeat_time_nanos`. Build-phase: edit
  the base message, no `ALTER` (root AGENTS storage rule). _Req 5, 7_
- [ ] 1.2 Add `backoff::exponential_retry_interval(policy, attempt)` mirroring
  `backoff.CalculateExponentialRetryInterval @ v1.31.0`; unit-test coefficient/cap. _Req 5_
- [ ] 1.3 Add `retry_decision(state, now, override) -> RetryOutcome` mirroring `shouldRetry` +
  `hasEnoughTimeForRetry @ v1.31.0`; unit-test the matrix. _Req 5_

### Stage 2 — Worker-failure retry (`tokeira-edge`)

- [ ] 2.1 Compute `is_retryable` from the worker `Failure` (`HandleFailed @ v1.31.0`). _Req 4_
- [ ] 2.2 Retryable + `Reschedule(d)` → `Rescheduled{failure, interval:d}`; else `Failed` (thread
  `NextRetryDelay` as override). _Req 4, 5_
- [ ] 2.3 Unit-test: non-retryable→Failed; retryable+attempts→Rescheduled; exhausted→Failed; existing
  terminal-fail tests still pass. _Req 4_

### Stage 3 — Backoff-delayed re-dispatch (`tokeira-chasm-activity` + `tokeira-edge`)

- [ ] 3.1 `Rescheduled` stages `DispatchTask` with `fire_at = now + interval`; re-arm the new attempt's
  pure timers. _Req 6_
- [ ] 3.2 Queue/`poll_activity_task` treat a not-yet-due dispatch as not pollable; release due delayed
  dispatches on the sweeper tick. _Req 6_
- [ ] 3.3 Unit-test: poll before interval→empty; after→attempt N+1; old token fenced `NotFound`
  (`StaleAttemptToken` shape). _Req 6, 8_

### Stage 4 — Timer-firing sweeper (`tokeira-edge` + `tokeira-runtime`)

- [ ] 4.1 Bridge `evaluate_timeouts(key, now)`: derive due timer(s) from `ActivityState`, apply
  `TimedOut` (s2s/s2c) or retry-or-`TimedOut` (s2c-timeout/heartbeat) under one fenced update; s2c
  precedence; validate-then-drop. _Req 1, 2, 3_
- [ ] 4.2 `ChasmTimerSweeper` (`tokeira-runtime`): interval tick (+ optional engine `Notify`), read
  armed due entries, call `evaluate_timeouts`, release due delayed dispatches; injectable clock. _Req 1_
- [ ] 4.3 Recovery scan: re-derive armed deadlines from node state so a lost entry self-heals. _Req 9_
- [ ] 4.4 Unit-test each timeout type + precedence with an injected clock. _Req 2, 3_

### Stage 5 — Heartbeat timer reset (`tokeira-chasm-activity` + `tokeira-edge`)

- [ ] 5.1 `Heartbeat` sets `last_heartbeat_time_nanos` and re-arms the heartbeat timer from it; due
  check uses `max(last_heartbeat, started)`. _Req 7_
- [ ] 5.2 Unit-test: heartbeat within timeout keeps alive; no-timeout never kills. _Req 7_

### Stage 6 — Wiring + properties (`apps/tokeirad`)

- [ ] 6.1 `spawn_chasm_timer_sweeper(...)` next to `spawn_visibility_repair`, gated on standalone
  activities; injectable clock. _Req 9_
- [ ] 6.2 Property tests P1 terminal-once, P2 fence, P3 budget, P4 derived-recovery. _Req 1, 5, 8, 9_

### Stage 7 — Tier-2 verification

- [ ] 7.1 Live SA-enabled `tokeirad` + `TestStandaloneActivityTestSuite`: confirm the ~20 timeout/retry
  leaves pass (`TestStartToCloseTimeout*`, `TestScheduleToStartTimeout`, `Test*ScheduleToClose*`,
  `ActivityRetriesOnHeartbeatTimeout`, `HeartbeatKeepsActivityAlive`,
  `HeartbeatWithNoTimeoutDoesNotKillActivity`, `StaleAttemptToken`, `HeartbeatDetailsAvailableOnRetry`,
  retry-on-fail by-token/by-id). Update FINDINGS C1 (concise).

## Task Dependency Graph

Waves (each wave's tasks may proceed once prior waves complete):

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1.1", "1.2", "1.3"], "depends_on": [] },
    { "wave": 2, "tasks": ["2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "5.1", "5.2"], "depends_on": [1] },
    { "wave": 3, "tasks": ["4.1", "4.2", "4.3", "4.4"], "depends_on": [1, 2] },
    { "wave": 4, "tasks": ["6.1", "6.2"], "depends_on": [2, 3] },
    { "wave": 5, "tasks": ["7.1"], "depends_on": [4] }
  ]
}
```

- Stage 1 (pure retry decision) underpins Stage 2 (fail-path retry) and Stage 4 (timeout retry).
- Stage 3 (backoff re-dispatch) depends on Stage 1's interval and the `Rescheduled` path that Stages 2
  and 4 both produce, so it must land for either to be pollable.
- Stage 4 (sweeper) depends on Stages 1 and 3; Stage 5 (heartbeat reset) shares Stage 1's
  `ActivityState` change. Stage 6 (wiring + properties) depends on 2–5; Stage 7 (Tier-2) on all.

## Notes

- Sweeper is runtime-only (clock+loop); semantics stay pure (chasm-activity) — kernel-purity and
  history-authority hold (root AGENTS §2/§3).
- Prefer `DispatchTask.fire_at` (option A) for backoff over a new `RetryTimer` (option B).
- Confirm the v1.31.0 `scheduleToStart` Execute path (`activity_tasks.go:100-152`) during Stage 4.
