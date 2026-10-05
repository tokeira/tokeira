------------------------------ MODULE 40_dispatch_handoff ------------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
This module models how a scheduled task reaches a worker when dispatch is
durable state rather than a message.

WHAT THIS MODULE MODELS
=======================

When a run's transition schedules a workflow task, the same commit writes a
dispatch row: the durable statement that this run wants this incarnation of
its task started. The row is derived state. It exists exactly while the
run's committed state says the task is scheduled and not yet started. The
transition that starts, reschedules or closes the task removes or replaces
the row in the same commit.

Everything else is disposable and may be lost at any time:

- the notification a commit sends to the queue's home node,
- the home's memory of the rows it has found (held) and offered (in flight),
- an offer on its way to a poller, and the reply to a start.

A queue home finds rows with discovery passes that start at the head of the
queue every time. Claiming a task writes nothing. The run's start transition
is the only fence: it checks the offer's incarnation against committed
state, marks the task started and deletes the row, in one commit.

The model checks that this is safe and that it makes progress:

- Safety. At most one start commits per incarnation, never for a stale
  incarnation or one the run no longer wants. A shard that serves has
  exactly the rows its committed state derives: none missing, none stale.
- Liveness. An incarnation that stays wanted, on a queue that a compatible
  poller keeps polling, is eventually started or stops being wanted.
- Notification-only state. A sticky incarnation, which only a notification
  announces, is eventually started or converted by its schedule-to-start
  timeout into one that discovery can find.

Activity tasks have the same shape, with the attempt and stamp as their
incarnation, so one model covers both.

This is a sibling of 00_execution_contract, not a refinement of it. It keeps
only the part of a run's state that dispatch depends on (whether a task is
scheduled, its incarnation, and whether it is sticky) and models the layers
around it: storage rows, brokers, pollers and shard ownership.

HOW IT MAPS TO THE INTENDED CODE
================================

  status, inc, sticky   the run's committed task state: the kernel's pending
                        workflow task, its logical sequence and queue kind
  row                   the run's dispatch row
  serving               the run's shard admits commands (ShardState::Active)
  active, held,         queue homes and the brokers' in-memory state
  inflight, cursor
  notes                 notifications sent after a commit
  offer                 what each poller holds

WHAT THIS MODULE DOES NOT MODEL
===============================

- Fencing between shard owners. Every committing transition here happens
  while the shard serves; which node may commit is 30_bundle_lease's
  question.
- Operation rows for children, signals and Nexus, rows a close leaves
  behind, and the visibility projection.
- Real time. A timeout is an action that may happen whenever its condition
  holds.
- More than two runs. Durable order is abstracted to one head run that
  sorts first.

FINITE MODELING NOTE
====================

Real Tokeira has no bound on reschedules or failures. Here incarnations stop
at MaxInc, and every environment fault draws from one budget, MaxFaults:
lost notifications, lost offers, lost start replies, broker restarts, shard
ownership changes, queue-home changes and writes by an old release. The
budget is what makes "failures eventually stop" true in a finite model,
which liveness needs.

FAIRNESS
========

Only weak fairness is used, and only on steps the protocol itself takes:
discovery, offers, start calls, completions, lease expiry, timeouts and
reconciliation. Notifications and faults get none. So a passing check shows
that progress never depends on a notification arriving, and that no step
needs strong fairness.

NEGATIVE CONTROLS
=================

The switches CheckIncarnation, Discovery, ReconcileInserts, ReconcileDeletes
and StickyTimeouts exist so the configurations under negative/ can break
one part of the protocol each. Every one of them must fail. A property that
no broken variant violates would prove nothing.
***************************************************************************)

CONSTANTS
    Runs,             \* runs, each with one task slot
    HeadRun,          \* the run whose row sorts first in its queue
    ServiceableRuns,  \* runs whose queue and routing class some poller serves
    Homes,            \* nodes that can act as the queue's home
    FirstHome,        \* the home in the initial state
    Pollers,          \* workers polling the queue
    StickyQueue,      \* stands for a worker's sticky queue in an offer
    MaxInc,           \* incarnations per run
    MaxFaults,        \* environment faults per behavior
    Budget,           \* rows one discovery pass may admit
    StickyEnabled,    \* incarnations may be sticky
    LegacyWrites,     \* an old release may commit while this shard doesn't serve
    CheckIncarnation, \* the start transition checks the incarnation
    Discovery,        \* "paged", or the negative controls "bare" and "cursor"
    ReconcileInserts, \* acquisition inserts missing rows
    ReconcileDeletes, \* acquisition deletes stale rows
    StickyTimeouts    \* a sticky incarnation's schedule-to-start timeout fires

VARIABLES
    status,             \* [Runs -> TaskStatus]: the committed task state
    inc,                \* [Runs -> 0..MaxInc]: the current incarnation
    sticky,             \* [Runs -> BOOLEAN]: the incarnation is on a sticky queue
    row,                \* [Runs -> Rows]: the durable dispatch row
    serving,            \* the shard admits commands
    active,             \* [Homes -> BOOLEAN]: acting as the queue's home
    held,               \* [Homes -> SUBSET Entries]: found, not yet offered
    inflight,           \* [Homes -> SUBSET Entries]: offered under a lease
    cursor,             \* [Homes -> 0..2]: used only by the "cursor" control
    notes,              \* notifications in transit
    offer,              \* [Pollers -> Offers]
    starts,             \* [Runs -> [Incs -> 0..2]]: committed starts, saturating
    staleStart,         \* a start committed for a non-current incarnation
    startWithoutIntent, \* a start committed for a task that wasn't scheduled
    faults              \* environment faults so far

vars == <<status, inc, sticky, row, serving, active, held, inflight, cursor,
          notes, offer, starts, staleStart, startWithoutIntent, faults>>

(***************************************************************************
Domains
***************************************************************************)

TaskStatus == {"Idle", "Scheduled", "Started", "Closed"}

Incs == 1..MaxInc

Entries == {<<r, i>> : r \in Runs, i \in Incs}

NoRow == [inc |-> 0, sticky |-> FALSE]

Rows == {NoRow} \cup [inc : Incs, sticky : BOOLEAN]

Notes == [run : Runs, inc : Incs, sticky : BOOLEAN]

NoOffer == [stage |-> "none", run |-> HeadRun, inc |-> 0, home |-> FirstHome]

Offers ==
    {NoOffer}
        \cup [stage : {"offered", "working"},
              run : Runs,
              inc : Incs,
              home : Homes \cup {StickyQueue}]

ASSUME
    /\ HeadRun \in Runs
    /\ ServiceableRuns \subseteq Runs
    /\ Cardinality(Runs) <= 2
    /\ FirstHome \in Homes
    /\ StickyQueue \notin Homes
    /\ MaxInc \in Nat \ {0}
    /\ MaxFaults \in Nat
    /\ Budget \in Nat \ {0}
    /\ Discovery \in {"paged", "bare", "cursor"}
    /\ StickyEnabled \in BOOLEAN
    /\ LegacyWrites \in BOOLEAN
    /\ CheckIncarnation \in BOOLEAN
    /\ ReconcileInserts \in BOOLEAN
    /\ ReconcileDeletes \in BOOLEAN
    /\ StickyTimeouts \in BOOLEAN

(***************************************************************************
Helpers
***************************************************************************)

\* The run wants incarnation i of its task started.
Wanted(r, i) == status[r] = "Scheduled" /\ inc[r] = i

\* The row the run's committed state derives.
Derived(r) ==
    IF status[r] = "Scheduled"
        THEN [inc |-> inc[r], sticky |-> sticky[r]]
        ELSE NoRow

CanFault == faults < MaxFaults

\* Durable order within the queue: the head run sorts first.
Order(r) == IF r = HeadRun THEN 1 ELSE 2

\* The first Budget runs of a set, in durable order. With at most two runs,
\* a set larger than the budget holds both, so its first row is the head
\* run's.
FirstRows(S) == IF Cardinality(S) <= Budget THEN S ELSE {HeadRun}

\* A new incarnation's row is written in the same commit that creates it,
\* and a notification is sent after the commit.
NewIncarnation(r, s) ==
    /\ status' = [status EXCEPT ![r] = "Scheduled"]
    /\ inc' = [inc EXCEPT ![r] = @ + 1]
    /\ sticky' = [sticky EXCEPT ![r] = s]
    /\ row' = [row EXCEPT ![r] = [inc |-> inc[r] + 1, sticky |-> s]]
    /\ notes' = notes \cup {[run |-> r, inc |-> inc[r] + 1, sticky |-> s]}

\* An incarnation may be sticky only if there is room for its
\* schedule-to-start timeout to create the next one.
StickyChoices(r) ==
    IF StickyEnabled /\ inc[r] + 1 < MaxInc THEN BOOLEAN ELSE {FALSE}

(***************************************************************************
Initial state
***************************************************************************)

Init ==
    /\ status = [r \in Runs |-> "Idle"]
    /\ inc = [r \in Runs |-> 0]
    /\ sticky = [r \in Runs |-> FALSE]
    /\ row = [r \in Runs |-> NoRow]
    /\ serving = TRUE
    /\ active = [h \in Homes |-> h = FirstHome]
    /\ held = [h \in Homes |-> {}]
    /\ inflight = [h \in Homes |-> {}]
    /\ cursor = [h \in Homes |-> 0]
    /\ notes = {}
    /\ offer = [p \in Pollers |-> NoOffer]
    /\ starts = [r \in Runs |-> [i \in Incs |-> 0]]
    /\ staleStart = FALSE
    /\ startWithoutIntent = FALSE
    /\ faults = 0

(***************************************************************************
Run transitions

Each is one commit on the run's shard, so each requires `serving`.
***************************************************************************)

\* An event (a signal, an activity result, a timer) schedules a task.
ScheduleTask(r) ==
    /\ serving
    /\ status[r] = "Idle"
    /\ inc[r] < MaxInc
    /\ \E s \in StickyChoices(r) : NewIncarnation(r, s)
    /\ UNCHANGED <<serving, active, held, inflight, cursor, offer, starts,
                   staleStart, startWithoutIntent, faults>>

\* A started task that times out is scheduled again, on the normal queue.
StartToCloseTimeout(r) ==
    /\ serving
    /\ status[r] = "Started"
    /\ inc[r] < MaxInc
    /\ NewIncarnation(r, FALSE)
    /\ UNCHANGED <<serving, active, held, inflight, cursor, offer, starts,
                   staleStart, startWithoutIntent, faults>>

\* A sticky incarnation that no sticky worker starts in time moves to the
\* normal queue as a new incarnation.
StickyTimeout(r) ==
    /\ StickyTimeouts
    /\ serving
    /\ status[r] = "Scheduled"
    /\ sticky[r]
    /\ inc[r] < MaxInc
    /\ NewIncarnation(r, FALSE)
    /\ UNCHANGED <<serving, active, held, inflight, cursor, offer, starts,
                   staleStart, startWithoutIntent, faults>>

\* The run closes, for example by termination, and its row goes with it.
Close(r) ==
    /\ serving
    /\ status[r] \in {"Idle", "Scheduled", "Started"}
    /\ status' = [status EXCEPT ![r] = "Closed"]
    /\ row' = [row EXCEPT ![r] = NoRow]
    /\ UNCHANGED <<inc, sticky, serving, active, held, inflight, cursor, notes,
                   offer, starts, staleStart, startWithoutIntent, faults>>

(***************************************************************************
Notifications

They get no fairness: one may wait for ever, which is the same as being
lost. Sticky rows are found only through notifications.
***************************************************************************)

DeliverNote(h, n) ==
    /\ active[h]
    /\ n \in notes
    /\ ~n.sticky
    /\ notes' = notes \ {n}
    /\ held' = IF <<n.run, n.inc>> \in inflight[h]
                  THEN held
                  ELSE [held EXCEPT ![h] = @ \cup {<<n.run, n.inc>>}]
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, inflight, cursor,
                   offer, starts, staleStart, startWithoutIntent, faults>>

\* A sticky notification goes straight to the worker's sticky queue.
DeliverStickyNote(p, n) ==
    /\ n \in notes
    /\ n.sticky
    /\ offer[p].stage = "none"
    /\ n.run \in ServiceableRuns
    /\ notes' = notes \ {n}
    /\ offer' = [offer EXCEPT ![p] = [stage |-> "offered", run |-> n.run,
                                      inc |-> n.inc, home |-> StickyQueue]]
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, held, inflight,
                   cursor, starts, staleStart, startWithoutIntent, faults>>

LoseNote(n) ==
    /\ CanFault
    /\ n \in notes
    /\ notes' = notes \ {n}
    /\ faults' = faults + 1
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, held, inflight,
                   cursor, offer, starts, staleStart, startWithoutIntent>>

(***************************************************************************
Discovery

A pass reads the queue's rows in durable order. Sticky rows aren't on the
queue. A home skips a row whose incarnation it already holds or has in
flight.

- "paged": every pass starts at the head and pages past rows this home
  already knows, admitting up to Budget new ones.
- "bare": every pass reads only the first Budget rows, a LIMIT with no
  paging. A negative control: known rows at the head hide the rest.
- "cursor": a pass continues after a durable cursor and never returns to
  the head. A negative control: a row committed below the cursor is never
  read.
***************************************************************************)

QueueRows == {r \in Runs : row[r].inc # 0 /\ ~row[r].sticky}

Known(h, r) == <<r, row[r].inc>> \in held[h] \cup inflight[h]

Admit(h) ==
    CASE Discovery = "paged" ->
             FirstRows({r \in QueueRows : ~Known(h, r)})
      [] Discovery = "bare" ->
             {r \in FirstRows(QueueRows) : ~Known(h, r)}
      [] Discovery = "cursor" ->
             {r \in FirstRows({q \in QueueRows : Order(q) > cursor[h]}) :
                 ~Known(h, r)}

Discover(h) ==
    /\ active[h]
    /\ Admit(h) # {}
    /\ held' = [held EXCEPT ![h] = @ \cup {<<r, row[r].inc>> : r \in Admit(h)}]
    /\ cursor' = IF Discovery = "cursor"
                    THEN [cursor EXCEPT ![h] =
                              IF \E r \in Admit(h) : Order(r) = 2 THEN 2 ELSE 1]
                    ELSE cursor
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, inflight, notes,
                   offer, starts, staleStart, startWithoutIntent, faults>>

(***************************************************************************
Queue homes

Queue-home ownership is for efficiency, not correctness. A new home may
start while the old one still serves, and both may offer the same
incarnation. The start transition lets only one start commit.
***************************************************************************)

QueueHomeChange(h) ==
    /\ ~active[h]
    /\ CanFault
    /\ active' = [active EXCEPT ![h] = TRUE]
    /\ faults' = faults + 1
    /\ UNCHANGED <<status, inc, sticky, row, serving, held, inflight, cursor,
                   notes, offer, starts, staleStart, startWithoutIntent>>

RetireHome(h) ==
    /\ active[h]
    /\ \E g \in Homes \ {h} : active[g]
    /\ active' = [active EXCEPT ![h] = FALSE]
    /\ held' = [held EXCEPT ![h] = {}]
    /\ inflight' = [inflight EXCEPT ![h] = {}]
    /\ cursor' = [cursor EXCEPT ![h] = 0]
    /\ UNCHANGED <<status, inc, sticky, row, serving, notes, offer, starts,
                   staleStart, startWithoutIntent, faults>>

\* The broker's memory is lost. A durable cursor would survive.
BrokerRestart(h) ==
    /\ active[h]
    /\ CanFault
    /\ held' = [held EXCEPT ![h] = {}]
    /\ inflight' = [inflight EXCEPT ![h] = {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, cursor, notes,
                   offer, starts, staleStart, startWithoutIntent>>

(***************************************************************************
Offers and starts

An offer is held under a lease in memory. Claiming writes nothing.
***************************************************************************)

Offer(h, r, i, p) ==
    /\ active[h]
    /\ <<r, i>> \in held[h]
    /\ offer[p].stage = "none"
    /\ r \in ServiceableRuns
    /\ held' = [held EXCEPT ![h] = @ \ {<<r, i>>}]
    /\ inflight' = [inflight EXCEPT ![h] = @ \cup {<<r, i>>}]
    /\ offer' = [offer EXCEPT ![p] = [stage |-> "offered", run |-> r,
                                      inc |-> i, home |-> h]]
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, cursor, notes,
                   starts, staleStart, startWithoutIntent, faults>>

LoseOffer(p) ==
    /\ offer[p].stage = "offered"
    /\ CanFault
    /\ offer' = [offer EXCEPT ![p] = NoOffer]
    /\ faults' = faults + 1
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, held, inflight,
                   cursor, notes, starts, staleStart, startWithoutIntent>>

\* A lease may end while its poller still holds the offer, so the
\* incarnation can be found and offered again: a duplicate offer.
ExpireLease(h, r, i) ==
    /\ <<r, i>> \in inflight[h]
    /\ inflight' = [inflight EXCEPT ![h] = @ \ {<<r, i>>}]
    /\ UNCHANGED <<status, inc, sticky, row, serving, active, held, cursor,
                   notes, offer, starts, staleStart, startWithoutIntent, faults>>

\* The run's start transition: the only fence. It commits only on a serving
\* shard, and only for the incarnation the run currently wants. The commit
\* marks the task started and deletes the row. Its reply may be lost.
StartCall(p) ==
    /\ offer[p].stage = "offered"
    /\ LET r == offer[p].run
           i == offer[p].inc
           h == offer[p].home
           valid == IF CheckIncarnation
                       THEN status[r] = "Scheduled" /\ inc[r] = i
                       ELSE status[r] \in {"Scheduled", "Started"}
       IN /\ inflight' = IF h \in Homes
                            THEN [inflight EXCEPT ![h] = @ \ {<<r, i>>}]
                            ELSE inflight
          /\ IF serving /\ valid
                THEN /\ status' = [status EXCEPT ![r] = "Started"]
                     /\ row' = [row EXCEPT ![r] = NoRow]
                     /\ starts' = [starts EXCEPT ![r][i] = IF @ < 2 THEN @ + 1 ELSE 2]
                     /\ staleStart' = (staleStart \/ i # inc[r])
                     /\ startWithoutIntent' =
                            (startWithoutIntent \/ status[r] # "Scheduled")
                     /\ \/ /\ offer' = [offer EXCEPT ![p] =
                                            [stage |-> "working", run |-> r,
                                             inc |-> i, home |-> h]]
                           /\ UNCHANGED faults
                        \/ /\ CanFault
                           /\ offer' = [offer EXCEPT ![p] = NoOffer]
                           /\ faults' = faults + 1
                ELSE /\ offer' = [offer EXCEPT ![p] = NoOffer]
                     /\ UNCHANGED <<status, row, starts, staleStart,
                                    startWithoutIntent, faults>>
    /\ UNCHANGED <<inc, sticky, serving, active, held, cursor, notes>>

\* The worker finishes. The run accepts the result only for the incarnation
\* it started; a result for a superseded one is rejected.
Complete(p) ==
    /\ offer[p].stage = "working"
    /\ LET r == offer[p].run
           i == offer[p].inc
       IN IF serving /\ status[r] = "Started" /\ inc[r] = i
             THEN \E next \in {"Idle", "Closed"} :
                      status' = [status EXCEPT ![r] = next]
             ELSE UNCHANGED status
    /\ offer' = [offer EXCEPT ![p] = NoOffer]
    /\ UNCHANGED <<inc, sticky, row, serving, active, held, inflight, cursor,
                   notes, starts, staleStart, startWithoutIntent, faults>>

(***************************************************************************
Shard ownership

While the shard doesn't serve, another node owns it. If that node runs an
old release, it may commit without maintaining dispatch rows. Before the
shard serves again, acquisition reconciles every row with the state.
***************************************************************************)

OwnerChange ==
    /\ serving
    /\ CanFault
    /\ serving' = FALSE
    /\ faults' = faults + 1
    /\ UNCHANGED <<status, inc, sticky, row, active, held, inflight, cursor,
                   notes, offer, starts, staleStart, startWithoutIntent>>

LegacyCommit(r) ==
    /\ LegacyWrites
    /\ ~serving
    /\ CanFault
    /\ faults' = faults + 1
    /\ \/ /\ status[r] = "Idle"
          /\ inc[r] < MaxInc
          /\ status' = [status EXCEPT ![r] = "Scheduled"]
          /\ inc' = [inc EXCEPT ![r] = @ + 1]
          /\ sticky' = [sticky EXCEPT ![r] = FALSE]
          /\ UNCHANGED starts
       \/ /\ status[r] = "Scheduled"
          /\ status' = [status EXCEPT ![r] = "Started"]
          /\ starts' = [starts EXCEPT ![r][inc[r]] = IF @ < 2 THEN @ + 1 ELSE 2]
          /\ UNCHANGED <<inc, sticky>>
       \/ /\ status[r] \in {"Idle", "Scheduled", "Started"}
          /\ status' = [status EXCEPT ![r] = "Closed"]
          /\ UNCHANGED <<inc, sticky, starts>>
    /\ UNCHANGED <<row, serving, active, held, inflight, cursor, notes, offer,
                   staleStart, startWithoutIntent>>

ReconciledRow(r) ==
    LET d == Derived(r) IN
    IF row[r] = d THEN row[r]
    ELSE IF d = NoRow THEN (IF ReconcileDeletes THEN NoRow ELSE row[r])
    ELSE IF ReconcileInserts THEN d
    ELSE IF ReconcileDeletes THEN NoRow
    ELSE row[r]

Reconcile ==
    /\ ~serving
    /\ row' = [r \in Runs |-> ReconciledRow(r)]
    /\ serving' = TRUE
    /\ UNCHANGED <<status, inc, sticky, active, held, inflight, cursor, notes,
                   offer, starts, staleStart, startWithoutIntent, faults>>

(***************************************************************************
Next-state relation and fairness
***************************************************************************)

Next ==
    \/ \E r \in Runs :
          \/ ScheduleTask(r)
          \/ StartToCloseTimeout(r)
          \/ StickyTimeout(r)
          \/ Close(r)
          \/ LegacyCommit(r)
    \/ \E h \in Homes :
          \/ Discover(h)
          \/ BrokerRestart(h)
          \/ QueueHomeChange(h)
          \/ RetireHome(h)
    \/ \E h \in Homes, n \in notes : DeliverNote(h, n)
    \/ \E p \in Pollers, n \in notes : DeliverStickyNote(p, n)
    \/ \E n \in notes : LoseNote(n)
    \/ \E h \in Homes, r \in Runs, i \in Incs, p \in Pollers : Offer(h, r, i, p)
    \/ \E p \in Pollers :
          \/ StartCall(p)
          \/ LoseOffer(p)
          \/ Complete(p)
    \/ \E h \in Homes, r \in Runs, i \in Incs : ExpireLease(h, r, i)
    \/ OwnerChange
    \/ Reconcile

\* Weak fairness only, and none on notifications or faults.
Fairness ==
    /\ \A h \in Homes : WF_vars(Discover(h))
    /\ \A p \in Pollers :
          WF_vars(\E h \in Homes, r \in Runs, i \in Incs : Offer(h, r, i, p))
    /\ \A p \in Pollers : WF_vars(StartCall(p))
    /\ \A p \in Pollers : WF_vars(Complete(p))
    /\ \A h \in Homes : WF_vars(\E r \in Runs, i \in Incs : ExpireLease(h, r, i))
    /\ \A r \in Runs : WF_vars(StartToCloseTimeout(r))
    /\ \A r \in Runs : WF_vars(StickyTimeout(r))
    /\ WF_vars(Reconcile)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************
Invariants
***************************************************************************)

TypeOK ==
    /\ status \in [Runs -> TaskStatus]
    /\ inc \in [Runs -> 0..MaxInc]
    /\ sticky \in [Runs -> BOOLEAN]
    /\ row \in [Runs -> Rows]
    /\ serving \in BOOLEAN
    /\ active \in [Homes -> BOOLEAN]
    /\ \A h \in Homes : held[h] \subseteq Entries /\ inflight[h] \subseteq Entries
    /\ cursor \in [Homes -> 0..2]
    /\ notes \subseteq Notes
    /\ offer \in [Pollers -> Offers]
    /\ starts \in [Runs -> [Incs -> 0..2]]
    /\ staleStart \in BOOLEAN
    /\ startWithoutIntent \in BOOLEAN
    /\ faults \in 0..MaxFaults

AtMostOneStartPerIncarnation ==
    \A r \in Runs : \A i \in Incs : starts[r][i] <= 1

NoStaleIncarnationStart == ~staleStart

NoStartWithoutIntent == ~startWithoutIntent

\* While serving, no row the committed state wants is missing ...
WantedRowsExist ==
    serving => \A r \in Runs : status[r] = "Scheduled" => row[r] = Derived(r)

\* ... and no row is stale. A missing row can stop work for ever; a stale
\* dispatch row only costs an offer the start transition rejects. So they
\* are checked separately.
RowsAreWanted ==
    serving => \A r \in Runs : row[r] # NoRow => row[r] = Derived(r)

AtLeastOneHome == \E h \in Homes : active[h]

(***************************************************************************
Temporal properties
***************************************************************************)

\* An incarnation that stays wanted, on a queue a compatible poller serves,
\* is eventually started or no longer wanted.
EventuallyResolved ==
    \A r \in ServiceableRuns : \A i \in Incs : Wanted(r, i) ~> ~Wanted(r, i)

\* A sticky incarnation is notification-only. Rule: it must eventually be
\* started, or converted into state that discovery can find.
StickyConverts ==
    \A r \in Runs : \A i \in Incs :
        (Wanted(r, i) /\ sticky[r]) ~> ~(Wanted(r, i) /\ sticky[r])

=============================================================================
