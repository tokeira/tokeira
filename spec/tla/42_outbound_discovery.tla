------------------------ MODULE 42_outbound_discovery ------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Outbound discovery visits a fixed set of active home/kind streams on ticks.
Each round visits at most Min(Pages, Homes * Kinds) streams and stops before a
visit when global admission is full. A visit fills available admission from
that stream's pending rows in row order; a full home is visited without a read.
Rows are reusable slots: arrival has no fairness, and each admitted attempt
weakly fairly retires its row or fails back to pending. Completion may interleave
with visits. Payloads, scan cursors, retry delays, wakes and ownership changes
are outside this scheduling model.

The reset, continue and full rules walk streams in home/kind key order. Reset
hands over after the round's first stream, continue after its last, and full
uses reset only after a complete sweep. Home instead carries a partial home
across rounds and rotates that home's kind offset after all its kinds are seen.

The leader observer follows consecutive visits, including across rounds. A new
home visit begins after another home or after all kinds of the same home were
seen. Wait counters begin on a stream's first visit and saturate one beyond
WaitBound so liveness-only checks still have a finite state space. Fair visits
and rotating leaders do not reserve admission for a pending row. The observers
leader, seenKinds and wait keep value-level IF expressions; protocol outcomes
are separate flat conjunctions. Tick captures the first stream before any visit;
arrivals and completions cannot change which stream the walk selects.
***************************************************************************)
CONSTANTS Homes, Kinds, Pages, MaxPerHome, MaxInFlight, MaxRows, Rule
VARIABLES rows, active, visits, first, previous, home, offset, within,
          lastHome, seenKinds, leader, wait
vars == <<rows, active, visits, first, previous, home, offset, within,
          lastHome, seenKinds, leader, wait>>

HomeSet == 1..Homes
KindSet == 1..Kinds
Streams == HomeSet \X KindSet
Rows == HomeSet \X KindSet \X (1..MaxRows)
N == Homes * Kinds
Min(a, b) == IF a < b THEN a ELSE b
RoundLimit == Min(Pages, N)
WaitBound == IF N <= Pages THEN 2 * N - 2 ELSE N + Kinds - 2
Unseen == WaitBound + 2
NextHome(h) == (h % Homes) + 1
NextKind(k) == (k % Kinds) + 1
After(s) == IF s[2] = Kinds THEN <<NextHome(s[1]), 1>>
           ELSE <<s[1], NextKind(s[2])>>
stream == IF Rule = "home"
          THEN <<home, ((offset[home] - 1 + within) % Kinds) + 1>>
          ELSE After(previous)
InFlight == {r \in Rows : rows[r] = "flight"}
HomeFlight(h) == {r \in InFlight : r[1] = h}
Pending(s) == {r \in Rows : <<r[1], r[2]>> = s /\ rows[r] = "pending"}
Room(s) == Min(MaxPerHome - Cardinality(HomeFlight(s[1])),
               MaxInFlight - Cardinality(InFlight))
Offered(s) == {r \in Pending(s) :
    Cardinality({q \in Pending(s) : q[3] <= r[3]}) <= Room(s)}
Begins(s) == lastHome # s[1] \/ seenKinds = KindSet

ASSUME /\ Homes \in Nat \ {0} /\ Kinds \in Nat \ {0}
       /\ Pages \in Nat \ {0} /\ MaxPerHome \in Nat \ {0}
       /\ MaxInFlight \in Nat \ {0} /\ MaxRows \in Nat \ {0}
       /\ Rule \in {"reset", "continue", "full", "home"}

Init ==
    /\ rows = [r \in Rows |-> "absent"]
    /\ active = FALSE
    /\ visits = 0
    /\ first = <<1, 1>>
    /\ previous = <<Homes, Kinds>>
    /\ home = 1
    /\ offset = [h \in HomeSet |-> 1]
    /\ within = 0
    /\ lastHome = 0
    /\ seenKinds = {}
    /\ leader = [h \in HomeSet |-> 0]
    /\ wait = [s \in Streams |-> Unseen]

Arrive(r) ==
    /\ rows[r] = "absent"
    /\ rows' = [rows EXCEPT ![r] = "pending"]
    /\ UNCHANGED <<active, visits, first, previous, home, offset, within,
                    lastHome, seenKinds, leader, wait>>

Retire(r) ==
    /\ rows[r] = "flight"
    /\ rows' = [rows EXCEPT ![r] = "absent"]
    /\ UNCHANGED <<active, visits, first, previous, home, offset, within,
                    lastHome, seenKinds, leader, wait>>

Fail(r) ==
    /\ rows[r] = "flight"
    /\ rows' = [rows EXCEPT ![r] = "pending"]
    /\ UNCHANGED <<active, visits, first, previous, home, offset, within,
                    lastHome, seenKinds, leader, wait>>

Tick ==
    /\ ~active
    /\ active' = TRUE
    /\ first' = stream
    /\ UNCHANGED <<rows, visits, previous, home, offset, within,
                    lastHome, seenKinds, leader, wait>>

VisitStream(s) ==
    /\ active
    /\ visits < RoundLimit
    /\ Cardinality(InFlight) < MaxInFlight
    /\ rows' = [r \in Offered(s) |-> "flight"] @@ rows
    /\ visits' = visits + 1
    /\ lastHome' = s[1]
    /\ seenKinds' = IF Begins(s) THEN {s[2]}
                    ELSE seenKinds \cup {s[2]}
    /\ leader' = IF Begins(s)
                 THEN [leader EXCEPT ![s[1]] = s[2]] ELSE leader
    /\ wait' = [other \in Streams |->
           \* Component comparison avoids tla-rs tuple representation differences.
           IF other[1] = s[1] /\ other[2] = s[2] THEN 0
           ELSE IF wait[other] = Unseen THEN Unseen
           ELSE Min(wait[other] + 1, WaitBound + 1)]
    /\ UNCHANGED <<active, first>>

VisitKeyOrder ==
    /\ Rule # "home"
    /\ VisitStream(After(previous))
    /\ previous' = After(previous)
    /\ UNCHANGED <<home, offset, within>>

VisitHomeWithin ==
    /\ Rule = "home"
    /\ within + 1 < Kinds
    /\ VisitStream(stream)
    /\ within' = within + 1
    /\ UNCHANGED <<previous, home, offset>>

VisitHomeLast ==
    /\ Rule = "home"
    /\ within + 1 = Kinds
    /\ VisitStream(stream)
    /\ home' = NextHome(home)
    /\ offset' = [offset EXCEPT ![home] = NextKind(@)]
    /\ within' = 0
    /\ UNCHANGED previous

Visit == VisitKeyOrder \/ VisitHomeWithin \/ VisitHomeLast

ResetPrevious == visits > 0 /\ (Rule = "reset" \/ (Rule = "full" /\ visits = N))

EndRound ==
    /\ active
    /\ visits = RoundLimit \/ Cardinality(InFlight) = MaxInFlight
    /\ active' = FALSE
    /\ visits' = 0
    /\ first' = <<1, 1>>
    /\ UNCHANGED <<rows, home, offset, within, lastHome, seenKinds, leader, wait>>

EndFirst ==
    /\ ResetPrevious
    /\ EndRound
    /\ previous' = first

EndKeep ==
    /\ ~ResetPrevious
    /\ EndRound
    /\ UNCHANGED previous

End == EndFirst \/ EndKeep

Next ==
    \/ Tick \/ Visit \/ End
    \/ (\E r \in Rows : Arrive(r) \/ Retire(r) \/ Fail(r))
Spec == Init /\ [][Next]_vars
    /\ WF_vars(Tick) /\ WF_vars(Visit) /\ WF_vars(End)
    /\ (\A r \in Rows : WF_vars(Retire(r) \/ Fail(r)))

TypeOK ==
    /\ rows \in [Rows -> {"absent", "pending", "flight"}]
    /\ active \in BOOLEAN
    /\ visits \in 0..RoundLimit
    /\ first \in Streams /\ previous \in Streams
    /\ home \in HomeSet
    /\ offset \in [HomeSet -> KindSet]
    /\ within \in 0..(Kinds - 1)
    /\ lastHome \in 0..Homes
    /\ seenKinds \subseteq KindSet
    /\ leader \in [HomeSet -> 0..Kinds]
    /\ wait \in [Streams -> 0..Unseen]
AdmissionBounded ==
    /\ Cardinality(InFlight) <= MaxInFlight
    /\ \A h \in HomeSet : Cardinality(HomeFlight(h)) <= MaxPerHome
BoundedWait == \A s \in Streams : wait[s] # WaitBound + 1
FirstPickRotates == \A h \in HomeSet, k \in KindSet : []<>(leader[h] = k)
EventuallyOffered == \A r \in Rows :
    (rows[r] = "pending") ~> (rows[r] = "flight" \/ rows[r] = "absent")
=============================================================================
