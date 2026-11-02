# Activity dispatch: implementation mapping and proof boundary

The activity implementation uses the dispatch obligations in
[`40_dispatch_handoff`](../tla/40_dispatch_handoff.tla) and the bounded-slice
repair obligations in [`41_dispatch_repair`](../tla/41_dispatch_repair.tla).
This is a mapping of authority and delivery obligations, not a machine-checked
refinement of every concrete RPC and broker state.

| Abstract coordinate | Concrete activity coordinate |
|---|---|
| Task slot `r` | `(run_key, activity_id)`. Multiple slots in one run share its transaction and lease. |
| Incarnation `inc[r]` | `(schedule_event_id, attempt, stamp)`. Schedule identity distinguishes reused activity IDs; a reset may lower attempt while advancing stamp. The bounded model's integer names successive distinct incarnations, not the numeric attempt. |
| `Scheduled` and `Derived(r)` | Open running state with an unstarted, unpaused slot and a valid eligibility time; `derive_activity_dispatch` supplies its canonical row. Future eligibility still requires a row. |
| `Started`, `NoRow` | Committed start and its request receipt; desired-set maintenance removes the row in the same transaction. Transient activity history is materialized under the existing kernel rules. |
| `serving` | The captured execution-home acquisition is Active. Repair completes both recovery-candidate and complete physical-home walks before activation. |
| `held`, `inflight`, `cursor` | Acquisition-scoped broker reservations and fixed-cutoff queue-family continuation. Budget pressure pauses a pass; successful return/drop releases only its captured reservation. |
| `StartCall` | Fenced owner decision checks schedule, attempt, stamp, routing, rules and worker admission, then commits the receipt and start once. A failed/OCC attempt has no authoritative effect. |
| `Complete`, reschedule, timeout | Existing fenced kernel transition; canonical row/receipt lifecycle follows the resulting state. |

## Receipt replay is observation

A same-request replay validates the stored UUID, offered incarnation, selected
queue, routing revision and worker identity. It reads the scheduled input and
validates the observation against current state and lease in one transaction,
using hot-row `FOR UPDATE` and lease `FOR KEY SHARE` conflict checks without
data mutation. It does not increment the transition sequence, append a second
start, recreate the dispatch row or change the committed start time. Resolution,
replacement and takeover races must invalidate the old observation.

Projecting onto authoritative task status, incarnation, row and start count,
this replay is a stuttering step. The existing model's `offer` variable does not
encode a request UUID or a committed-but-unacknowledged receipt. Consequently,
the complete concrete replay response cannot be claimed to refine its full
`offer` state machine without an additional abstraction. The implementation's
receipt identity, authenticated response and no-resurrection obligations are
checked separately by activity-dispatch Properties 7, 8, 9, 12 and 16, including
controlled commit-then-drop and physical read/resolve races. The model is not
evidence that an unrelated poll may recover a prior worker's start.

## Checks retained

All positive configurations and the existing negative controls remain unchanged.
Disabling incarnation checks must violate start safety; fixed-head discovery or
a permanently advanced durable cursor must violate progress; incomplete repair
must violate wanted/existing row equality. The repair model additionally checks
retained retry descriptors and progress across bounded slices. Physical DSQL
OCC and ownership ordering remain the responsibility of the lease model and
live storage tests. These boundaries prevent a finite abstract model from being
used as evidence for unmodeled authentication, database plans or wire payloads.
