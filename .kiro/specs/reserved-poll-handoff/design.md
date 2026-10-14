# Reserved Poll Hand-off — Bugfix Design

## Overview

A reserved poll waits only for the start that reserved it:
- **The loop knows.** Each turn, the poll checks whether its waiter is still on the queue's wait list. If a start has
  taken it, the poll waits for its channel, or for a wake of its queue in case the reservation comes back, and does
  nothing else: no take, no query, no deadline.
- **Leaving can't race reserving.** A take, a query or a departure at the deadline happens only while the waiter is on
  the list, and removes it in the same critical section as the reservation's own removal.
- **A returned reservation resumes the poll.** The reservation keeps the waiter's id, and returning it re-parks the
  waiter under that id, so the poll recognizes it and carries on.

## Bug Details

### Bug Condition

A start reserves a parked poll, and before the start delivers, the poll's queue wakes with a workflow task or query
ready, or the poll's deadline passes. Workflow tasks reach the normal queue from workers without a sticky cache, from
sticky tasks past their schedule-to-start deadline, from discovery on several nodes, and from starts that found no
parked poll.

### Examples

- **A second start.** Two starts on one queue with one parked poll: the first reserves it, the second publishes its
  first task, and the poll takes the second start's task. The first start's task waits 10 s.
- **A slow commit.** A start reserves a poll near its deadline, and its commit takes longer than the time left. The
  poll returns empty and the start's task waits 10 s.

## Root Cause

`try_reserve_poller` takes the waiter's channel off the wait list, but the poll's loop runs on, unaware. Its takes and
its deadline exit check nothing about the reservation, so they can close the channel the start is about to use.

## Correctness Properties

Property 1: A reserved poll delivers its start's task

_For any_ reserved poll, whatever becomes ready on its queue and however long the start takes, the poll SHALL return
the start's task when the start delivers it, and SHALL take nothing else first.

**Validates: Requirements 2.1, 2.2**

Property 2: A returned reservation changes nothing

_For any_ reservation handed back, the poll SHALL behave as a poll that was never reserved: it takes ready work, and
returns empty once its deadline has passed.

**Validates: Requirements 2.3, 3.1**

## Fix Implementation

In `crates/tokeira-runtime/src/broker.rs`:
- **The reservation** keeps its waiter's id, and `return_reserved_poller` re-parks the waiter under it.
- **The loop** checks each turn whether its waiter is still parked. If not, it waits on its channel and on its queue's
  wake. When the channel closes without a task, the poll returns empty.
- **Takes and exits.** `try_take`, a waiter-aware `try_take_query_as`, and the deadline and denial exits act only
  while the waiter is parked, and deregister it under the same lock.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, each failing there:
- a reserved poll takes another workflow task published on its queue;
- a reserved poll takes a direct query;
- a reserved poll returns empty at its deadline;
- a poll whose reservation is handed back has already taken the other task while reserved.

### Unit Tests

The same four, with paused time, pass after the fix:
- the reserved poll returns the start's task, and the other task waits for the next poll;
- it takes no query;
- it outlasts its deadline;
- a handed-back reservation takes the ready task, and one handed back past its deadline returns empty and leaves the
  wait list.

### Preservation Checking

The broker's and the runtime's existing tests stay green, among them those for per-queue wakes, deadlines, sticky
polls and denial.
