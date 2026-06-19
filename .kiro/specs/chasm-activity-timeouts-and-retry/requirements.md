# Requirements Document

## Introduction

Make standalone (first-class) activities **time out** and **retry** on the CHASM engine, matching
Temporal **v1.31.0**. This closes the last C1 conformance blocker (`TestStandaloneActivityTestSuite`
~20 leaves): the heartbeat / start-to-close / schedule-to-start / schedule-to-close timeout tests and
the retry-redispatch tests (`StaleAttemptToken`, `HeartbeatDetailsAvailableOnRetry`,
`ActivityRetriesOnHeartbeatTimeout`, `RetryWithoutScheduleToCloseTimeout`,
`Test_ScheduleToCloseTimeout_WithRetry`, `TestScheduleToStartTimeout`, `TestStartToCloseTimeout*`).

**In scope:** firing the CHASM activity pure timers; the retry decision (reschedule vs terminal) on
worker failure and on start-to-close/heartbeat timeout; backoff-delayed re-dispatch of the next
attempt; heartbeat-timer reset; wiring the firing loop into `tokeirad`.

**Out of scope:** the lane/kernel workflow-activity model (the existing `runtime-activity-timeouts` /
`runtime-activity-pump` specs target that, not the CHASM engine); kernel changes (the kernel stays
pure — root AGENTS §2); visibility.

**Ground truth (v1.31.0).** Timeout handlers (`chasm/lib/activity/activity_tasks.go`): `startToClose`
and `heartbeat` Execute call `tryReschedule(...)` then fall back to `TransitionTimedOut`;
`scheduleToStart`/`scheduleToClose` Execute apply `TransitionTimedOut` directly; every `Validate` is
validate-then-drop. Retry decision (`activity.go:509 shouldRetry`): reschedule iff `Rescheduled` legal
AND attempts remain (`MaximumAttempts == 0 || count < max`) AND the next interval fits the
schedule-to-close budget; interval = `NextRetryDelay` override if positive else
`backoff.CalculateExponentialRetryInterval(policy, attempt)`. Worker failure (`activity.go:289
HandleFailed`): retryable iff `ApplicationFailureInfo` present, `!NonRetryable`, type ∉
`nonRetryableErrorTypes`.

## Glossary

- **Pure timer:** a CHASM pure task fired at a deadline, validated against component state
  (validate-then-drop) — here the four activity timeout timers.
- **Reschedule:** `TransitionRescheduled` — bump attempt/stamp, record the failure, re-arm timers, and
  (after backoff) re-dispatch the next attempt.
- **Sweeper:** the runtime clock+loop that fires due pure timers; derived/non-authoritative.

## Requirements

### Requirement 1: Pure-timer firing

**User Story:** As an operator, I want armed activity timers to actually fire, so timeouts resolve.

#### Acceptance Criteria
1. WHEN an activity's armed pure-timer deadline elapses THEN the runtime SHALL load the execution,
   determine the due pure timer(s) from node state, and apply the corresponding transition under a
   fenced commit.
2. IF a due timer's validator no longer holds (status/attempt moved) THEN the runtime SHALL drop it
   without effect.

### Requirement 2: Schedule-to-start / schedule-to-close timeout

**User Story:** As an operator, I want unstarted or over-budget activities to time out, so they don't hang forever.

#### Acceptance Criteria
1. WHEN the schedule-to-start or schedule-to-close deadline elapses with the activity non-terminal
   THEN the runtime SHALL transition to `TIMED_OUT` with the matching `TimeoutFailureInfo`.
2. IF both fire in one tick THEN schedule-to-close SHALL take precedence (v1.31.0).

### Requirement 3: Start-to-close / heartbeat timeout with retry

**User Story:** As a workflow author, I want a stalled attempt to retry on timeout, so transient worker loss recovers.

#### Acceptance Criteria
1. WHEN the start-to-close or heartbeat deadline elapses THEN the runtime SHALL attempt a retry (Req 5).
2. IF retry is not possible THEN the runtime SHALL transition to `TIMED_OUT`.

### Requirement 4: Worker-failure retry

**User Story:** As a workflow author, I want a retryable worker failure to retry, so flaky activities recover automatically.

#### Acceptance Criteria
1. WHEN `RespondActivityTaskFailed` carries a retryable failure THEN the runtime SHALL attempt a retry
   (Req 5).
2. IF retry is not possible THEN the runtime SHALL transition to `FAILED`.

### Requirement 5: Retry decision

**User Story:** As a workflow author, I want retries bounded by my policy and the schedule-to-close budget, so they stop correctly.

#### Acceptance Criteria
1. The runtime SHALL reschedule IF AND ONLY IF `Rescheduled` is legal from the current status AND
   attempts remain AND the next interval fits within the schedule-to-close budget.
2. The interval SHALL be the failure's `NextRetryDelay` override when positive, else the policy's
   exponential backoff.

### Requirement 6: Backoff re-dispatch

**User Story:** As a worker, I want the next attempt to become pollable only after the backoff, so retries are paced.

#### Acceptance Criteria
1. WHEN an activity reschedules THEN its next attempt's dispatch SHALL become pollable only after the
   retry interval elapses; a poll before then SHALL NOT return it.

### Requirement 7: Heartbeat timer reset

**User Story:** As a worker, I want heartbeats to keep a long activity alive, so progress prevents a false timeout.

#### Acceptance Criteria
1. WHEN a heartbeat is recorded THEN the heartbeat-timeout deadline SHALL be measured from the
   heartbeat time, not the start time.

### Requirement 8: Attempt fencing

**User Story:** As an operator, I want stale timers and dispatches to be inert, so a superseded attempt never acts.

#### Acceptance Criteria
1. A fired timer or a re-dispatch SHALL act only on the attempt it was armed for; a superseded
   attempt's timer/dispatch SHALL be inert.

### Requirement 9: Wiring and durability

**User Story:** As an operator, I want timeouts to fire in the running server and survive a restart, so durability holds.

#### Acceptance Criteria
1. The firing loop SHALL run in `tokeirad` alongside the existing CHASM background workers.
2. The firing loop SHALL be non-authoritative: a missed tick SHALL self-heal on the next, and due
   timers SHALL be re-derivable from node state alone.
