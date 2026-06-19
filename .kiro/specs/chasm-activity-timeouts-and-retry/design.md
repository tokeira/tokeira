# Design Document

## Overview

The CHASM substrate already has every piece **except firing**:

- `tokeira-chasm-activity`: the pure timer tasks (`ScheduleToStartTimer`, `ScheduleToCloseTimer`,
  `StartToCloseTimer`, `HeartbeatTimer`) + validators, the `TimedOut`/`Rescheduled` events with
  `apply`/`legal_target`, and `schedule_attempt_timers` (re-arms timers + dispatch on
  `Scheduled`/`Rescheduled`).
- `tokeira-runtime` `ChasmEngine`: `arm_timer` records the earliest pure-timer deadline per execution
  in an in-memory map; `post_commit` re-arms it after each transition.
- `tokeira-edge` `ActivityBridge`: poll/complete/fail/cancel/heartbeat, dispatch queue, attempt/stamp
  fence.

**Missing:** (a) nothing *fires* the armed deadline; (b) the fail path always goes terminal (no retry
evaluation); (c) `Rescheduled`'s dispatch is immediate, not backoff-delayed; (d) heartbeat does not
reset the heartbeat timer. This design adds firing + the retry decision + backoff re-dispatch, keeping
all activity *semantics* pure (chasm-activity) and the runtime piece a clock+loop.

## Architecture

```
ChasmTimerSweeper (tokeira-runtime, background)        ActivityBridge (tokeira-edge)
  interval tick (+ engine Notify)  ── due (key,deadline) ──> evaluate_timeouts(key, now)
  release due delayed dispatches                              │  load ActivityState
                                                              │  derive due timer(s)
                                                              ▼
                              retry_decision (tokeira-chasm-activity, pure)
                                                              │
                              Rescheduled{interval} | TimedOut | Failed  (fenced commit)
```

The sweeper is the only new runtime component; it holds no authoritative state. Timeout derivation, the
retry decision, and the transitions live in `tokeira-chasm-activity` (pure) + the bridge, so root
AGENTS §2 (kernel pure) and §3 (history authority; queues/timers disposable) hold — the armed map and
dispatch queue are re-derivable from node state.

## Components and Interfaces

- **`tokeira-chasm-activity`**
  - `backoff::exponential_retry_interval(policy, attempt) -> Duration` — mirror
    `backoff.CalculateExponentialRetryInterval @ v1.31.0`.
  - `retry_decision(state, now, override) -> RetryOutcome { Reschedule(Duration) | Terminal }` — mirror
    `shouldRetry` + `hasEnoughTimeForRetry @ v1.31.0`.
  - `Heartbeat` event also re-arms the heartbeat timer from the heartbeat time.
- **`tokeira-edge::chasm_activity::ActivityBridge`**
  - `evaluate_timeouts(key, now) -> EdgeResult<()>` — re-derive due pure timers from `ActivityState`,
    apply `TimedOut` (schedule-to-start/close) or retry-or-`TimedOut` (start-to-close/heartbeat) under
    one fenced `update`; schedule-to-close precedence.
  - fail path: compute `is_retryable` from the worker `Failure`, then `retry_decision` →
    `Rescheduled{failure, interval}` or `Failed`.
- **`tokeira-runtime::chasm::ChasmTimerSweeper`**
  - `tokio::time::interval` (default ~200ms) + optional engine `Notify`; reads armed due entries;
    calls `evaluate_timeouts`; releases due delayed dispatches; injectable clock.
- **`apps/tokeirad`** — `spawn_chasm_timer_sweeper(engine, bridge, clock, cancel)` next to
  `spawn_visibility_repair`, gated on standalone activities.

## Data Models

- `ActivityState` (proto, `tokeira-chasm-activity`): add the full retry policy (initial interval,
  backoff coefficient, max interval — `maximum_attempts` already stored) and `last_heartbeat_time_nanos`.
  **Build-phase migration rule (root AGENTS):** fold into the base message; no `ALTER`.
- `DispatchTask` already carries `fire_at_unix_nanos`; `Rescheduled` stages it at `now + interval` and
  the queue/poll treat a not-yet-due dispatch as not pollable.
- No kernel changes; no new crate; no new external deps (reuse `tokio::time`).

## Correctness Properties

### Property 1: terminal-once
A timeout/failure that reschedules never also commits a terminal outcome for the same attempt; once
terminal, no later timer fires a second outcome.
**Validates: Requirements 1, 3, 4**

### Property 2: fence
Firing/redispatch is a no-op against a superseded attempt or a closed execution.
**Validates: Requirements 8**

### Property 3: budget
No reschedule is issued whose next interval would exceed the schedule-to-close deadline
(`hasEnoughTimeForRetry`).
**Validates: Requirements 5**

### Property 4: derived
The firing loop holds no authoritative state; due timers are re-derivable from node state, so a restart
loses no timeout.
**Validates: Requirements 9**

## Error Handling

- Firing is best-effort and idempotent: a fenced-commit conflict or a validator that no longer holds is
  dropped (validate-then-drop); the next tick re-evaluates. A load error logs and retries next tick.
- The retry decision is pure and total (`Reschedule | Terminal`); no panics, no `.unwrap()` outside
  tests (root AGENTS §1).

## Testing Strategy

- Unit (`tokeira-chasm-activity`): `retry_decision` matrix (attempts-exhausted, non-retryable,
  budget-exceeded, override vs exponential); heartbeat-timer reset.
- Unit (`tokeira-edge`): sweeper fires each timeout type with an injected clock; start-to-close/
  heartbeat retry-vs-timeout; schedule-to-close precedence; delayed dispatch not pollable before the
  interval, pollable after.
- Property: P1–P4.
- Tier-2: the ~20 C1 timeout/retry leaves pass against a live SA-enabled `tokeirad`.

## Deviations / Notes

- v1.31.0 `scheduleToStart` Execute applies `TimedOut` directly (its
  `recordScheduleToStartOrCloseTimeoutFailure` handles any S2S retry internally); mirror the handler
  shape rather than inventing a separate S2S retry path — confirm against
  `activity_tasks.go:100-152 @ v1.31.0` during impl.
- Backoff re-dispatch uses the existing `DispatchTask.fire_at` (option A) over a new `RetryTimer`
  (option B) — fewer moving parts.
