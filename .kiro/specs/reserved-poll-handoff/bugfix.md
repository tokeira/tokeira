# Bugfix Requirements Document

## Introduction

When a workflow starts and a poll is already parked on its task queue, the start hands the run's first workflow task
straight to that poll, a synchronous match:
- the start reserves the poll: `try_reserve_poller` takes the poll's waiter off the queue's wait list
  (`crates/tokeira-runtime/src/broker.rs:1473-1490`);
- the start's commit records the first workflow task as started, for the reserved poll's worker identity;
- after the commit, the start delivers the task on the poll's channel (`deliver_reserved_start_workflow_task`,
  `crates/tokeira-runtime/src/runtime/workflow_task.rs:758-777`, called from `start_workflow_prepared`,
  `crates/tokeira-runtime/src/runtime/lifecycle.rs:85-165`).

The reserved poll doesn't know it was reserved. Its loop keeps running (`broker.rs:1171-1215`): every wake of its
queue, such as another workflow task published there, has it look for ready work again, and at its deadline it
returns empty. If it takes another workflow task or a direct query, or reaches its deadline, before the start
delivers, its channel closes. The start then finds no poll to deliver to, and the run's first workflow task, already
started for that poll's identity, waits out its workflow-task timeout, 10 s by default, before it is scheduled again.

The start takes its reservation before it joins its run's lane queue, and only its caller can hand the task over. A
caller that gives up while the start waits behind a busy lane drops the reservation, and the poll returns empty. The
start, still in the lane's queue, then commits the first workflow task as started for that poll, which waits out its
timeout too.

A v1.31.0 poller that matching pairs with a task receives exactly that task: it is enqueued and waits for one match
result (`service/matching/pri_matcher.go:587-627 @ v1.31.0`). It can't take a second one.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a start has reserved a parked poll AND another workflow task becomes ready on the poll's queue before the
start delivers THEN the poll takes that task, and the start's first workflow task waits out its timeout.

1.2 WHEN a start has reserved a parked poll AND a direct query becomes ready on the poll's queue before the start
delivers THEN the poll takes the query, with the same result.

1.3 WHEN a start has reserved a parked poll AND the poll's deadline passes before the start delivers THEN the poll
returns empty, with the same result.

1.4 WHEN a start hands its reservation back THEN the poll returns to the wait list under a new waiter id, which its own
loop no longer recognizes.

1.5 WHEN a start's caller gives up after the start has joined its lane's queue THEN the reservation is dropped, and the
start's commit still starts the first workflow task for that poll, which waits out its timeout.

### Expected Behavior (Correct)

2.1 WHILE a start holds a poll's reservation THE poll SHALL take no workflow task and no query, and SHALL wait for the
start's delivery, or for the reservation to be handed back, whatever its deadline.

2.2 A poll SHALL take a ready workflow task or query, or leave at its deadline or because its worker is denied, only
while its waiter is still on the wait list, and SHALL leave the wait list in the same critical section, so that a
start can't reserve a poll that has taken other work or left.

2.3 WHEN a start hands a reservation back THE poll SHALL return to the front of the wait list under its own waiter id,
and SHALL carry on as before the reservation: it takes ready work, and leaves empty if its deadline has passed.

2.4 WHEN a start drops a reservation without delivering or handing it back THE poll SHALL return empty.

2.5 A start SHALL reserve a parked poll only when its lane takes the start, so that a start waiting in its lane's queue
holds no poll, and its lane SHALL hand the poll the start's first workflow task once the start has committed, whether
or not the start's caller still waits.

### Unchanged Behavior (Regression Prevention)

3.1 A poll that is never reserved SHALL CONTINUE TO take ready work, wake only for its own queue, and return empty at
its deadline.

3.2 A start SHALL CONTINUE TO reserve only a live parked poll, and to hand the reservation back when it doesn't
commit.

3.3 Sticky polls SHALL CONTINUE TO be outside starts' reservations.

3.4 WHEN no poll is parked at a start's queue THE start SHALL CONTINUE TO commit its first workflow task unstarted, and
to publish it to its queue.

3.5 An eager start, and a start with a start delay, SHALL CONTINUE TO reserve no poll.

### Out of Scope

- A poll whose client cancels while it is reserved. Its channel closes, and the start's first task still waits for its
  timeout, as a v1.31.0 task does when its poller vanishes after matching.
