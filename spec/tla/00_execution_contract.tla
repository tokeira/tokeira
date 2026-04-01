----------------------------- MODULE 00_execution_contract -----------------------------
EXTENDS Naturals, Sequences, TLC

(***************************************************************************
This is the first executable TLA+ model for Tokeira.

It is intentionally small and intentionally opinionated.

WHAT THIS MODULE MODELS
=======================

This module models the *semantic contract* of a single workflow run.
It is the specification most directly related to `tokeira-kernel`.

The Rust shape it is trying to describe is roughly:

    Kernel::apply(loaded_run, command) -> Result<Transition, Reject>

In plain English, this spec answers:

    Given the current semantic state of one workflow run,
    which commands are allowed,
    and what semantic transition do they produce?

The commands modelled here correspond to the current kernel starter shape:

- Start
- Signal
- WorkflowTaskStarted
- WorkflowTaskCompleted
- ActivityResolved
- TimerDue

WHAT THIS MODULE DOES NOT MODEL
===============================

This module deliberately does *not* model:

- multiple runs for the same workflow id,
- current_execution uniqueness,
- request dedupe persistence,
- bundle leases / ownership epochs,
- broker reservations / pollers / sticky routing,
- atomic storage commit,
- projection sinks,
- archival.

Those belong to later specs.

WHY THE MODEL IS SMALL
======================

A TLA+ beginner should be able to read this file as a state machine.
The point is not to impress anyone with formal cleverness.
The point is to make the core execution semantics crisp.

The model therefore uses a deliberately reduced state:

- `st`      : compact semantic run state
- `history` : sequence of semantic event kinds

We do not model payload contents, memo values, search-attribute values,
worker identities, or queue placement here because they are not the heart of
this first semantic contract.

HOW TO READ THIS FILE
=====================

1. Read `Init`.
2. Read the helper operator `ScheduleWorkflowTask`.
3. Read one action at a time.
4. Read `Next`.
5. Read the invariants at the end.

A good mental model is:

- each action corresponds to one allowed semantic transition,
- every action updates `st` and `history` together,
- `history` is the abstract authoritative event sequence,
- `st.lastEventId = Len(history)` is one of the core safety claims.

FINITE MODELING NOTE
====================

TLC explores finite state spaces. Real Tokeira does not have a bound like
`MaxTransitions`, but this model does. That is not a semantic claim about the
production system. It is simply how we keep the first model check finite and
fast.
***************************************************************************)

CONSTANTS
    ActivityIds,
    TimerIds,
    MaxTransitions

VARIABLES
    st,
    history

vars == <<st, history>>

(***************************************************************************
Basic domains
***************************************************************************)

Status == {"Absent", "Running", "Completed", "Failed"}

EventKinds == {
    "WorkflowExecutionStarted",
    "WorkflowExecutionSignaled",
    "WorkflowTaskScheduled",
    "WorkflowTaskStarted",
    "WorkflowTaskCompleted",
    "ActivityTaskScheduled",
    "ActivityTaskCompleted",
    "TimerStarted",
    "TimerFired",
    "WorkflowExecutionCompleted",
    "WorkflowExecutionFailed"
}

(***************************************************************************
Pending workflow task representation

The Rust kernel uses an Option-like field. In this first TLA+ model we encode
that with a record and a `present` bit. The sentinel value 0 means
"not allocated yet" for ids inside the `NoWFT` record.
***************************************************************************)

Bound == 0..(2 * MaxTransitions + 2)

NoWFT == [present |-> FALSE,
          logicalSeq |-> 0,
          scheduledEventId |-> 0,
          startedEventId |-> 0,
          attempt |-> 0]

WFTRecords ==
    { [present |-> TRUE,
       logicalSeq |-> logicalSeq,
       scheduledEventId |-> scheduledEventId,
       startedEventId |-> startedEventId,
       attempt |-> attempt] :
          logicalSeq \in Bound,
          scheduledEventId \in Bound,
          startedEventId \in Bound,
          attempt \in Bound }

PendingWFTType == {NoWFT} \cup WFTRecords

WorkflowStateType ==
    [ status          : Status,
      transitionSeq   : Bound,
      lastEventId     : Bound,
      nextWFTSeq      : Bound,
      pendingWFT      : PendingWFTType,
      activities      : SUBSET ActivityIds,
      timers          : SUBSET TimerIds ]

CanStep == st.transitionSeq < MaxTransitions

(***************************************************************************
Helper operators
***************************************************************************)

ScheduleWorkflowTask(s) ==
    LET scheduledEventId == s.lastEventId + 1
        logicalSeq      == s.nextWFTSeq
    IN [s EXCEPT
          !.lastEventId = @ + 1,
          !.nextWFTSeq = @ + 1,
          !.pendingWFT = [present |-> TRUE,
                          logicalSeq |-> logicalSeq,
                          scheduledEventId |-> scheduledEventId,
                          startedEventId |-> 0,
                          attempt |-> 0]]

ClosedKinds == {"WorkflowExecutionCompleted", "WorkflowExecutionFailed"}

ContainsCloseEvent(h) ==
    \E i \in DOMAIN h : h[i] \in ClosedKinds

(***************************************************************************
Initial state

`Absent` means the run does not yet exist.
Later specs will model current_execution and multiple runs. This first spec
only models a single run's lifecycle.
***************************************************************************)

Init ==
    /\ st = [ status        |-> "Absent",
              transitionSeq |-> 0,
              lastEventId   |-> 0,
              nextWFTSeq    |-> 1,
              pendingWFT    |-> NoWFT,
              activities    |-> {},
              timers        |-> {} ]
    /\ history = <<>>

(***************************************************************************
Actions
***************************************************************************)

Start ==
    /\ CanStep
    /\ st.status = "Absent"
    /\ st' = [ status        |-> "Running",
               transitionSeq |-> 1,
               lastEventId   |-> 2,
               nextWFTSeq    |-> 2,
               pendingWFT    |-> [present |-> TRUE,
                                   logicalSeq |-> 1,
                                   scheduledEventId |-> 2,
                                   startedEventId |-> 0,
                                   attempt |-> 0],
               activities    |-> {},
               timers        |-> {} ]
    /\ history' = <<"WorkflowExecutionStarted", "WorkflowTaskScheduled">>

Signal ==
    /\ CanStep
    /\ st.status = "Running"
    /\ IF st.pendingWFT.present
          THEN /\ st' = [st EXCEPT
                          !.transitionSeq = @ + 1,
                          !.lastEventId = @ + 1]
               /\ history' = Append(history, "WorkflowExecutionSignaled")
          ELSE LET afterSignal == [st EXCEPT
                                     !.transitionSeq = @ + 1,
                                     !.lastEventId = @ + 1]
                   afterSchedule == ScheduleWorkflowTask(afterSignal)
               IN  /\ st' = afterSchedule
                   /\ history' = Append(Append(history,
                                                "WorkflowExecutionSignaled"),
                                        "WorkflowTaskScheduled")

WorkflowTaskStarted ==
    /\ CanStep
    /\ st.status = "Running"
    /\ st.pendingWFT.present
    /\ st.pendingWFT.startedEventId = 0
    /\ LET startedEventId == st.lastEventId + 1
           nextPending == [st.pendingWFT EXCEPT
                             !.startedEventId = startedEventId,
                             !.attempt = @ + 1]
       IN /\ st' = [st EXCEPT
                     !.transitionSeq = @ + 1,
                     !.lastEventId = @ + 1,
                     !.pendingWFT = nextPending]
          /\ history' = Append(history, "WorkflowTaskStarted")

WorkflowTaskCompleteNoop ==
    /\ CanStep
    /\ st.status = "Running"
    /\ st.pendingWFT.present
    /\ st.pendingWFT.startedEventId # 0
    /\ st' = [st EXCEPT
                !.transitionSeq = @ + 1,
                !.lastEventId = @ + 1,
                !.pendingWFT = NoWFT]
    /\ history' = Append(history, "WorkflowTaskCompleted")

WorkflowTaskClose(newStatus, closeEvent) ==
    /\ CanStep
    /\ st.status = "Running"
    /\ st.pendingWFT.present
    /\ st.pendingWFT.startedEventId # 0
    /\ newStatus \in {"Completed", "Failed"}
    /\ closeEvent \in ClosedKinds
    /\ st' = [st EXCEPT
                !.status = newStatus,
                !.transitionSeq = @ + 1,
                !.lastEventId = @ + 2,
                !.pendingWFT = NoWFT,
                !.activities = {},
                !.timers = {}]
    /\ history' = Append(Append(history, "WorkflowTaskCompleted"), closeEvent)

WorkflowTaskCompleteWorkflow ==
    WorkflowTaskClose("Completed", "WorkflowExecutionCompleted")

WorkflowTaskFailWorkflow ==
    WorkflowTaskClose("Failed", "WorkflowExecutionFailed")

WorkflowTaskScheduleActivity(activity) ==
    /\ CanStep
    /\ st.status = "Running"
    /\ st.pendingWFT.present
    /\ st.pendingWFT.startedEventId # 0
    /\ activity \in ActivityIds \ st.activities
    /\ st' = [st EXCEPT
                !.transitionSeq = @ + 1,
                !.lastEventId = @ + 2,
                !.pendingWFT = NoWFT,
                !.activities = @ \cup {activity}]
    /\ history' = Append(Append(history, "WorkflowTaskCompleted"),
                         "ActivityTaskScheduled")

WorkflowTaskStartTimer(timer) ==
    /\ CanStep
    /\ st.status = "Running"
    /\ st.pendingWFT.present
    /\ st.pendingWFT.startedEventId # 0
    /\ timer \in TimerIds \ st.timers
    /\ st' = [st EXCEPT
                !.transitionSeq = @ + 1,
                !.lastEventId = @ + 2,
                !.pendingWFT = NoWFT,
                !.timers = @ \cup {timer}]
    /\ history' = Append(Append(history, "WorkflowTaskCompleted"),
                         "TimerStarted")

ActivityResolved(activity) ==
    /\ CanStep
    /\ st.status = "Running"
    /\ activity \in st.activities
    /\ LET afterResolve == [st EXCEPT
                              !.transitionSeq = @ + 1,
                              !.lastEventId = @ + 1,
                              !.activities = @ \ {activity}]
       IN /\ IF st.pendingWFT.present
                THEN /\ st' = afterResolve
                     /\ history' = Append(history, "ActivityTaskCompleted")
                ELSE LET afterSchedule == ScheduleWorkflowTask(afterResolve)
                     IN  /\ st' = afterSchedule
                         /\ history' = Append(Append(history,
                                                      "ActivityTaskCompleted"),
                                              "WorkflowTaskScheduled")

TimerDue(timer) ==
    /\ CanStep
    /\ st.status = "Running"
    /\ timer \in st.timers
    /\ LET afterFire == [st EXCEPT
                           !.transitionSeq = @ + 1,
                           !.lastEventId = @ + 1,
                           !.timers = @ \ {timer}]
       IN /\ IF st.pendingWFT.present
                THEN /\ st' = afterFire
                     /\ history' = Append(history, "TimerFired")
                ELSE LET afterSchedule == ScheduleWorkflowTask(afterFire)
                     IN  /\ st' = afterSchedule
                         /\ history' = Append(Append(history, "TimerFired"),
                                              "WorkflowTaskScheduled")

(***************************************************************************
Next-state relation

For the first spec we expand the workflow-task-completion outcomes into
separate actions because that is easier for a newcomer to read than modeling
an arbitrary command list.
***************************************************************************)

Next ==
    \/ Start
    \/ Signal
    \/ WorkflowTaskStarted
    \/ WorkflowTaskCompleteNoop
    \/ WorkflowTaskCompleteWorkflow
    \/ WorkflowTaskFailWorkflow
    \/ \E activity \in ActivityIds : WorkflowTaskScheduleActivity(activity)
    \/ \E timer \in TimerIds : WorkflowTaskStartTimer(timer)
    \/ \E activity \in ActivityIds : ActivityResolved(activity)
    \/ \E timer \in TimerIds : TimerDue(timer)

Spec == Init /\ [][Next]_vars

(***************************************************************************
Invariants

These are written as state predicates because TLC checks them against every
reachable state.
***************************************************************************)

TypeInvariant ==
    /\ st \in WorkflowStateType
    /\ history \in Seq(EventKinds)
    /\ MaxTransitions \in Nat

LastEventMatchesHistory ==
    st.lastEventId = Len(history)

PendingWorkflowTaskConsistency ==
    IF st.pendingWFT.present
        THEN /\ st.pendingWFT.logicalSeq > 0
             /\ st.pendingWFT.logicalSeq < st.nextWFTSeq
             /\ st.pendingWFT.scheduledEventId > 0
             /\ st.pendingWFT.scheduledEventId <= st.lastEventId
             /\ st.pendingWFT.startedEventId <= st.lastEventId
        ELSE TRUE

ClosedRunsHaveNoPendingWork ==
    st.status \in {"Completed", "Failed"}
        => /\ ~st.pendingWFT.present
           /\ st.activities = {}
           /\ st.timers = {}

CompletedRunsHaveCompletedEvent ==
    st.status = "Completed"
        => \E i \in DOMAIN history : history[i] = "WorkflowExecutionCompleted"

FailedRunsHaveFailedEvent ==
    st.status = "Failed"
        => \E i \in DOMAIN history : history[i] = "WorkflowExecutionFailed"

RunningRunsHaveNoCloseEvent ==
    st.status = "Running"
        => ~ContainsCloseEvent(history)

=============================================================================
