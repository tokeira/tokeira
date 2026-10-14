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
- **The lane reserves and hands over.** A start reserves a parked poll only when its lane takes the start, so the poll
  is held through the start's commit and not while the start waits in a busy lane's queue. The lane hands the poll the
  first workflow task after the commit, whether or not the start's caller still waits, and hands a start's reservation
  back when the start doesn't commit.

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
- **A caller that gives up.** A start waits behind a busy lane past its caller's deadline. The caller's reservation is
  dropped and the poll returns empty, and the start then commits its first task as started for that poll. The task
  waits 10 s.

## Root Cause

`try_reserve_poller` takes the waiter's channel off the wait list, but the poll's loop runs on, unaware. Its takes and
its deadline exit check nothing about the reservation, so they can close the channel the start is about to use.

The start's caller takes the reservation before the start joins its lane's queue, and hands the task over after the
lane replies. Only the caller can deliver, so a caller that gives up strands the task. And once the poll waits only for
its start, a reservation taken that early would hold it out of service for as long as the start waits behind a busy
lane.

## Correctness Properties

Property 1: A reserved poll delivers its start's task

_For any_ reserved poll, whatever becomes ready on its queue and however long the start takes, the poll SHALL return
the start's task when the start delivers it, and SHALL take nothing else first.

**Validates: Requirements 2.1, 2.2**

Property 2: A returned reservation changes nothing

_For any_ reservation handed back, the poll SHALL behave as a poll that was never reserved: it takes ready work, and
returns empty once its deadline has passed.

**Validates: Requirements 2.3, 3.1**

Property 3: A start's lane decides its poll

_For any_ start that may reserve, its lane SHALL reserve a parked poll only as it takes the start, SHALL hand that poll
the start's first workflow task once the start has committed, whether or not the start's caller still waits, and SHALL
hand the reservation back when the start doesn't commit. With no poll parked, the start SHALL commit its first task
unstarted and publish it. An eager start, and a start with a start delay, SHALL ask for no poll.

**Validates: Requirements 2.5, 3.2, 3.4, 3.5**

## Fix Implementation

In `crates/tokeira-runtime/src/broker.rs`:
- **The reservation** keeps its waiter's id, and `return_reserved_poller` re-parks the waiter under it.
- **The loop** checks each turn whether its waiter is still parked. If not, it waits on its channel and on its queue's
  wake. When the channel closes without a task, the poll returns empty.
- **Takes and exits.** `try_take`, a waiter-aware `try_take_query_as`, and the deadline and denial exits act only
  while the waiter is parked, and deregister it under the same lock.

In the start's lane:
- **Asking.** `start_workflow_prepared` reserves nothing itself. A start with no eager worker and no start delay goes
  to its lane through `submit_start`, which marks its lane message as asking for a parked poll.
- **Reserving.** When the lane takes such a start, it asks its `DispatchPublisher` to reserve a poll parked at the
  start's queue, before it clones the command for its post-commit steps. With a poll, the command carries the poll's
  identity, so the kernel starts the first workflow task for it in the start's own commit, and the lane drops the
  task's broker enqueue as before.
- **Handing over.** Before replying, the lane builds the started task from the committed state, with its execution
  home's epoch, and hands it to the poll. A start that didn't commit returns the reservation through the publisher.
- **The publisher.** `DispatchPublisher` gains `reserve_start_poller` and `return_start_poller`. The runtime's
  publisher implements them on the broker; by default they reserve nothing and drop what they are given.
- **The task.** `started_workflow_task` builds a started task from a run's state and its home's epoch, for the lane and
  for an eager start's inline task.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, each failing there:
- a reserved poll takes another workflow task published on its queue;
- a reserved poll takes a direct query;
- a reserved poll returns empty at its deadline;
- a poll whose reservation is handed back has already taken the other task while reserved.

A property test of Property 1, over generated interleavings of tasks and queries published on the reserved poll's
queue and waits past its deadline, fails on the broker before the fix and passes after it.

### Unit Tests

The same four, with paused time, pass after the fix:
- the reserved poll returns the start's task, and the other task waits for the next poll;
- it takes no query;
- it outlasts its deadline;
- a handed-back reservation takes the ready task, and one handed back past its deadline returns empty and leaves the
  wait list.

Lane tests, each against the real kernel:
- a start's lane reserves a parked poll as it takes the start, and hands it the first workflow task, which is not
  published to the queue;
- it hands the task over after the start's caller has gone;
- a start that doesn't commit returns its poll, which then takes the queue's next task;
- with no poll parked, the start commits its first task unstarted and publishes it;
- a poll parked while its start waits behind a busy lane takes other work, and the start then commits its first task
  unstarted and publishes it;
- a start whose execution home is lost between its commit and the hand-off returns its poll, which then takes the
  queue's next task.

For Requirement 3.5, a unit test checks that only a start with no worker of its own and no start delay asks its lane for
a poll, and runtime tests check that an eager start and a delayed start leave a parked poll free to take the next task.
The unit test is the one that fails if either asks: the lane would return a reservation it couldn't use, and the poll
would carry on.

These use the lane's new interface, so they can't run before the change. Negative controls stand in: each part of the
lane change, removed alone, fails its tests. Without the reservation, the two hand-off tests fail; without a hand-off
once the caller has gone, that test fails; without the return, the return test fails; and with the reservation made
after the command is cloned for the post-commit steps, the task is also published to the queue, and the first test
fails. Without the return on a lost home, the lost-home test fails, and with every start asking for a poll, the unit
test of Requirement 3.5 fails.

### Preservation Checking

The broker's and the runtime's existing tests stay green, among them those for per-queue wakes, deadlines, sticky
polls and denial, and the runtime's test of a reserved start's direct delivery, timeout tracking and history.
