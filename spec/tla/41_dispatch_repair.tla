-------------------------- MODULE 41_dispatch_repair --------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
Concrete companion to 40_dispatch_handoff: three permanently blocked positions
precede a serviceable task; a fifth position is an orphan. Discovery keeps its
cursor across bounded slices. A failed first task creates a second incarnation.
Acquisition traverses NULL and true recovery candidates, then every home row.
One repair step writes at most one run, even when a read page contains more.

The refinement image is <<wanted, inc, abstractRow, serving>>. While Sweeping,
abstractRow retains the image from acquisition, so individual repairs stutter;
Activate is the abstract Reconcile step. Active Start and Retry update intent
and rows together. Fault can interrupt any step, clears volatile offers, and
restarts both walks at the head with a new local generation. MaxFaults explicitly
bounds faults: no notification action or notification fairness is assumed.
This companion checks a single owner; it makes no competing-owner fence claim.
***************************************************************************)
CONSTANTS PageSize, MaxFaults, RestartSlices, RetainRetryIdentity, RepairDeletes
VARIABLES wanted, inc, row, starts, phase, repairCursor, cursor, held, failed,
          faults, epoch, acquisitionEpoch, acquisitionRows
vars == <<wanted, inc, row, starts, phase, repairCursor, cursor, held, failed,
          faults, epoch, acquisitionEpoch, acquisitionRows>>

Runs == 1..5
Incs == 1..2
NullCandidates == {1, 4}
TrueCandidates == {2, 3}
serving == phase = "active"
Derived(r) == IF wanted[r] THEN inc[r] ELSE 0
abstractRow == IF serving THEN row ELSE acquisitionRows
End(c) == IF c + PageSize > 5 THEN 5 ELSE c + PageSize
Page(c) == (c + 1)..End(c)

Init ==
    /\ wanted = [r \in Runs |-> r # 5]
    /\ inc = [r \in Runs |-> 1]
    /\ row = [r \in Runs |-> IF r = 4 THEN 0 ELSE 1]
    /\ starts = [i \in Incs |-> 0]
    /\ phase = "null"
    /\ repairCursor = 0
    /\ cursor = 0
    /\ held = {}
    /\ failed = FALSE
    /\ faults = 0
    /\ epoch = 1
    /\ acquisitionEpoch = 1
    /\ acquisitionRows = row

Repair ==
    /\ ~serving
    /\ repairCursor < 5
    /\ LET r == repairCursor + 1
           candidate == CASE phase = "null" -> r \in NullCandidates
                          [] phase = "true" -> r \in TrueCandidates
                          [] OTHER -> row[r] # 0
       IN row' = IF candidate /\ (wanted[r] \/ RepairDeletes)
                 THEN [row EXCEPT ![r] = Derived(r)] ELSE row
    /\ repairCursor' = repairCursor + 1
    /\ UNCHANGED <<wanted, inc, starts, phase, cursor, held, failed, faults,
                    epoch, acquisitionEpoch, acquisitionRows>>

NextWalk ==
    /\ phase \in {"null", "true"}
    /\ repairCursor = 5
    /\ phase' = IF phase = "null" THEN "true" ELSE "home"
    /\ repairCursor' = 0
    /\ UNCHANGED <<wanted, inc, row, starts, cursor, held, failed, faults,
                    epoch, acquisitionEpoch, acquisitionRows>>

Activate ==
    /\ phase = "home"
    /\ repairCursor = 5
    /\ epoch = acquisitionEpoch
    /\ phase' = "active"
    /\ UNCHANGED <<wanted, inc, row, starts, repairCursor, cursor, held,
                    failed, faults, epoch, acquisitionEpoch, acquisitionRows>>

Slice ==
    /\ serving
    /\ held' = held \cup {<<r, row[r]>> : r \in {p \in Page(cursor) : row[p] # 0}}
    /\ cursor' = IF RestartSlices \/ End(cursor) = 5 THEN 0 ELSE End(cursor)
    /\ UNCHANGED <<wanted, inc, row, starts, phase, repairCursor, failed,
                    faults, epoch, acquisitionEpoch, acquisitionRows>>

Start ==
    /\ serving
    /\ wanted[4]
    /\ <<4, inc[4]>> \in held
    /\ wanted' = [wanted EXCEPT ![4] = FALSE]
    /\ row' = [row EXCEPT ![4] = 0]
    /\ starts' = [starts EXCEPT ![inc[4]] = @ + 1]
    /\ UNCHANGED <<inc, phase, repairCursor, cursor, held, failed,
                    faults, epoch, acquisitionEpoch, acquisitionRows>>

Retry ==
    /\ serving
    /\ ~wanted[4]
    /\ ~failed
    /\ inc' = [inc EXCEPT ![4] = IF RetainRetryIdentity THEN @ ELSE @ + 1]
    /\ wanted' = [wanted EXCEPT ![4] = TRUE]
    /\ row' = [row EXCEPT ![4] = inc'[4]]
    /\ failed' = TRUE
    /\ UNCHANGED <<starts, phase, repairCursor, cursor, held,
                    faults, epoch, acquisitionEpoch, acquisitionRows>>

Fault ==
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ epoch' = epoch + 1
    /\ acquisitionEpoch' = epoch'
    /\ acquisitionRows' = abstractRow
    /\ phase' = "null"
    /\ repairCursor' = 0
    /\ cursor' = 0
    /\ held' = {}
    /\ UNCHANGED <<wanted, inc, row, starts, failed>>

Next == Repair \/ NextWalk \/ Activate \/ Slice \/ Start \/ Retry \/ Fault
Spec == Init /\ [][Next]_vars
    /\ WF_vars(Repair) /\ WF_vars(NextWalk) /\ WF_vars(Activate)
    /\ WF_vars(Slice) /\ WF_vars(Start) /\ WF_vars(Retry)

TypeOK ==
    /\ wanted \in [Runs -> BOOLEAN]
    /\ inc \in [Runs -> Incs]
    /\ row \in [Runs -> 0..2]
    /\ starts \in [Incs -> 0..2]
    /\ phase \in {"null", "true", "home", "active"}
    /\ repairCursor \in 0..5
    /\ cursor \in 0..5
    /\ held \subseteq (Runs \X Incs)
    /\ faults \in 0..MaxFaults
    /\ epoch = acquisitionEpoch
WantedRowsExist == serving => \A r \in Runs : wanted[r] => row[r] = inc[r]
RowsAreWanted == serving => \A r \in Runs : row[r] # 0 => (wanted[r] /\ row[r] = inc[r])
AtMostOneStartPerIncarnation == \A i \in Incs : starts[i] <= 1
EventuallyResolved == \A i \in Incs : (wanted[4] /\ inc[4] = i) ~> ~(wanted[4] /\ inc[4] = i)
EventuallyServing == <>[]serving
NonServingStutters == [][(~serving /\ ~serving') => abstractRow' = abstractRow]_vars
=============================================================================
