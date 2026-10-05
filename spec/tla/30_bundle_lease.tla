-------------------------------- MODULE 30_bundle_lease --------------------------------
EXTENDS Naturals

(***************************************************************************
This module models how a bundle's lease fences writes on Aurora DSQL.

WHAT THIS MODULE MODELS
=======================

A bundle has one lease row: its owner, its epoch, and whether it has
expired. The node that owns the lease commits run transitions. Every run
commit, creation and deletion is a DSQL transaction fenced by the lease.
Once the lease has expired, another node may take it over, which advances
the epoch. The question is which fence stops the old owner committing after
a takeover, on a database that takes no locks.

DSQL uses optimistic concurrency control. A transaction reads from the
snapshot taken when it begins, and conflicts are adjudicated at commit:
of two conflicting transactions, whichever commits last fails. AWS
documents which operations on the same row conflict:

                            key write or     UPDATE of        SELECT ...
                            FOR UPDATE       non-key only     FOR KEY SHARE
  key write or FOR UPDATE        X                X                 X
  UPDATE of non-key only         X                X
  SELECT ... FOR KEY SHARE       X

A key write is an INSERT, a DELETE, or an UPDATE of a key column. A key
column belongs to a unique, non-partial, non-expression index. A plain
SELECT never conflicts. (Aurora DSQL user guide, "Concurrency control in
Aurora DSQL".)

The protocol checked here, with the switches at their passing values:

- epoch is a key column, through a unique index on (shard_id, epoch);
- every run commit, creation and deletion reads the lease row
  FOR KEY SHARE in its own transaction, and checks owner and epoch;
- only an acquisition or a release writes epoch; renewal updates only the
  expiry, without FOR UPDATE;
- an owner whose lease has expired by its own clock stops starting
  commits.

What it checks:

- NoStaleOwnerCommit: a run write commits only if its node owns the lease,
  at the epoch it checked, at the moment the write commits.
- NoSpuriousAbort: the lease row never aborts a commit by its current
  owner. Renewals and the owner's other commits don't conflict with it;
  only a change of ownership does.
- LapsedLeaseResolves: an expired lease is eventually taken over, renewed
  or released, as long as the old owner stops when its lease expires.

The symmetric rule has a consequence the third property captures. A fenced
commit that lands first aborts a concurrent takeover. So an owner that
keeps committing after its lease has expired can starve every takeover.
Safety doesn't depend on the owner stopping; failover does.

HOW IT MAPS TO THE CODE
=======================

  lease           the shard_lease row (storage migration V002)
  phase,          a node's view of its ownership (ShardState in
  believed        tokeira-runtime/src/shard.rs)
  txn             a run commit, creation or deletion
                  (storage run_repository/commit.rs and delete.rs)
  ltx             a lease transaction: acquire, renew or relinquish
                  (storage run_repository/leases.rs)
  exists          which runs exist

The negative controls under negative/ reproduce today's code: the plain read
of the epoch inside the commit, the separate check transaction in
controller mode, epoch outside any unique index, and renewal that reads
FOR UPDATE.

WHAT THIS MODULE DOES NOT MODEL
===============================

- What a run transition does. A commit is a write to a run's row, and its
  effects are 40_dispatch_handoff's subject.
- Real time and clock skew. Expiry is an action, and an owner notices its
  own expiry at some later point.
- Whether an UPDATE that assigns a key column its current value counts as a
  key write. Today's acquire does that when an owner re-acquires its own
  live lease. The protocol here never assigns epoch except to change it,
  so it doesn't depend on the answer.

FINITE MODELING NOTE
====================

Epochs stop at MaxEpoch. A lease can expire, and a node can give up after a
failed sweep, only while there is still room to advance the epoch, so the
bound never strands a lapsed lease.
***************************************************************************)

CONSTANTS
    Nodes,          \* nodes that can own the bundle
    None,           \* no owner
    FirstOwner,     \* the owner in the initial state
    Runs,           \* run ids (strings)
    InitialRuns,    \* runs that exist at the start
    MaxEpoch,       \* the largest epoch
    FenceRead,      \* "keyshare", or the negative controls "forupdate",
                    \* "plain" and "separate"
    EpochIsKey,     \* epoch belongs to a unique index
    RenewForUpdate, \* renewal reads the lease row FOR UPDATE
    SelfFence       \* an owner stops starting commits once its lease expires

VARIABLES
    lease,          \* [owner, epoch, expired]
    exists,         \* [Runs -> BOOLEAN]
    phase,          \* [Nodes -> Phases]
    believed,       \* [Nodes -> 0..MaxEpoch]: the epoch a node thinks it owns
    checked,        \* [Nodes -> BOOLEAN]: the separate check passed
    txn,            \* [Nodes -> run transaction in flight, or NoTxn]
    ltx,            \* [Nodes -> lease transaction in flight, or NoLtx]
    staleCommit,    \* a run write committed without owning the lease
    spuriousAbort   \* the lease row aborted a commit by its current owner

vars == <<lease, exists, phase, believed, checked, txn, ltx, staleCommit,
          spuriousAbort>>

(***************************************************************************
Domains

A node is "idle" without the lease, "sweeping" after acquiring it,
"active" once its sweep has finished, "fenced" after noticing its lease
expired, and "releasing" after a failed sweep.
***************************************************************************)

Phases == {"idle", "sweeping", "active", "fenced", "releasing"}

Epochs == 1..MaxEpoch

RunKinds == {"update", "create", "delete"}

\* How a transaction touches a row: "K" a key write or FOR UPDATE, "N" an
\* UPDATE of non-key columns only, "S" a SELECT ... FOR KEY SHARE.
Classes == {"K", "N", "S"}

\* The matrix above: K conflicts with everything, N with N, S with K only.
Conflicts(a, b) == a = "K" \/ b = "K" \/ (a = "N" /\ b = "N")

Rows == {"lease"} \cup Runs

NoTxn == [kind |-> "none", run |-> "none", epoch |-> 0, leaseCls |-> "none",
          runCls |-> "none", doomed |-> FALSE, spurious |-> FALSE]

NoLtx == [kind |-> "none", classes |-> {}, snapEpoch |-> 0, doomed |-> FALSE]

ASSUME
    /\ None \notin Nodes
    /\ FirstOwner \in Nodes
    /\ InitialRuns \subseteq Runs
    /\ "lease" \notin Runs
    /\ MaxEpoch \in Nat \ {0}
    /\ FenceRead \in {"keyshare", "forupdate", "plain", "separate"}
    /\ EpochIsKey \in BOOLEAN
    /\ RenewForUpdate \in BOOLEAN
    /\ SelfFence \in BOOLEAN

(***************************************************************************
Access sets and conflicts
***************************************************************************)

\* How a run transaction reads the lease row.
LeaseReadClass ==
    IF FenceRead = "keyshare" THEN "S"
    ELSE IF FenceRead = "forupdate" THEN "K"
    ELSE "none"

\* Writing epoch is a key write only if epoch belongs to a unique index.
EpochClass == IF EpochIsKey THEN "K" ELSE "N"

RunAccess(t) ==
    (IF t.leaseCls = "none" THEN {} ELSE {[row |-> "lease", cls |-> t.leaseCls]})
        \cup {[row |-> t.run, cls |-> t.runCls]}

LeaseAccess(l) == {[row |-> "lease", cls |-> c] : c \in l.classes}

Clash(A, B) == \E a \in A, b \in B : a.row = b.row /\ Conflicts(a.cls, b.cls)

LeaseClash(A, B) ==
    \E a \in A, b \in B :
        a.row = "lease" /\ b.row = "lease" /\ Conflicts(a.cls, b.cls)

\* A transaction that commits with access set A dooms every concurrent
\* transaction it conflicts with: that one will commit last, so it fails.
\* A run transaction doomed through the lease row by a renewal or another
\* run commit, rather than by a change of ownership, is marked spurious.
DoomTxn(t, A, kind) ==
    IF t = NoTxn THEN t
    ELSE [t EXCEPT
            !.doomed = @ \/ Clash(RunAccess(t), A),
            !.spurious = @ \/ (LeaseClash(RunAccess(t), A)
                                 /\ kind \in RunKinds \cup {"renew"})]

DoomLtx(l, A) ==
    IF l = NoLtx THEN l ELSE [l EXCEPT !.doomed = @ \/ Clash(LeaseAccess(l), A)]

(***************************************************************************
Initial state
***************************************************************************)

Init ==
    /\ lease = [owner |-> FirstOwner, epoch |-> 1, expired |-> FALSE]
    /\ exists = [r \in Runs |-> r \in InitialRuns]
    /\ phase = [n \in Nodes |-> IF n = FirstOwner THEN "active" ELSE "idle"]
    /\ believed = [n \in Nodes |-> IF n = FirstOwner THEN 1 ELSE 0]
    /\ checked = [n \in Nodes |-> FALSE]
    /\ txn = [n \in Nodes |-> NoTxn]
    /\ ltx = [n \in Nodes |-> NoLtx]
    /\ staleCommit = FALSE
    /\ spuriousAbort = FALSE

(***************************************************************************
Run transactions

A transaction's reads come from the snapshot taken when it begins, so the
fence check and the choice of write happen at Begin. Commit adjudicates.
***************************************************************************)

Owns(n) == lease.owner = n /\ lease.epoch = believed[n]

\* Today's controller mode checks the lease in its own transaction first.
SeparateCheck(n) ==
    /\ FenceRead = "separate"
    /\ phase[n] \in {"sweeping", "active"}
    /\ txn[n] = NoTxn
    /\ ~checked[n]
    /\ Owns(n)
    /\ checked' = [checked EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<lease, exists, phase, believed, txn, ltx, staleCommit,
                   spuriousAbort>>

BeginRun(n, kind, r) ==
    /\ phase[n] \in {"sweeping", "active"}
    /\ txn[n] = NoTxn
    /\ IF kind = "create" THEN ~exists[r] ELSE exists[r]
    /\ IF FenceRead = "separate" THEN checked[n] ELSE Owns(n)
    /\ txn' = [txn EXCEPT ![n] =
                  [kind |-> kind, run |-> r, epoch |-> believed[n],
                   leaseCls |-> LeaseReadClass,
                   runCls |-> IF kind = "update" THEN "N" ELSE "K",
                   doomed |-> FALSE, spurious |-> FALSE]]
    /\ checked' = [checked EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<lease, exists, phase, believed, ltx, staleCommit,
                   spuriousAbort>>

\* The write a run transaction makes when it commits.
AfterRun(t) ==
    IF t.kind = "create" THEN [exists EXCEPT ![t.run] = TRUE]
    ELSE IF t.kind = "delete" THEN [exists EXCEPT ![t.run] = FALSE]
    ELSE exists

\* The transaction commits: nothing that committed since it began conflicts
\* with it. It dooms every concurrent transaction it conflicts with.
CommitRun(n) ==
    /\ txn[n] # NoTxn
    /\ ~txn[n].doomed
    /\ exists' = AfterRun(txn[n])
    /\ staleCommit' =
           (staleCommit \/ ~(lease.owner = n /\ lease.epoch = txn[n].epoch))
    /\ txn' = [m \in Nodes |->
                  IF m = n THEN NoTxn
                  ELSE DoomTxn(txn[m], RunAccess(txn[n]), txn[n].kind)]
    /\ ltx' = [m \in Nodes |-> DoomLtx(ltx[m], RunAccess(txn[n]))]
    /\ UNCHANGED <<lease, phase, believed, checked, spuriousAbort>>

\* The transaction fails at commit, because something it conflicts with
\* committed first. The abort is spurious if the node still owns the lease at
\* the epoch it checked.
AbortRun(n) ==
    /\ txn[n] # NoTxn
    /\ txn[n].doomed
    /\ spuriousAbort' =
           (spuriousAbort \/ (txn[n].spurious /\ lease.owner = n
                                             /\ lease.epoch = txn[n].epoch))
    /\ txn' = [txn EXCEPT ![n] = NoTxn]
    /\ UNCHANGED <<lease, exists, phase, believed, checked, ltx, staleCommit>>

FinishRun(n) == CommitRun(n) \/ AbortRun(n)

(***************************************************************************
Lease transactions
***************************************************************************)

\* Acquire an unowned or expired lease, advancing the epoch.
BeginAcquire(c) ==
    /\ ltx[c] = NoLtx
    /\ phase[c] \in {"idle", "fenced"}
    /\ lease.owner = None \/ lease.expired
    /\ lease.epoch < MaxEpoch
    /\ ltx' = [ltx EXCEPT ![c] = [kind |-> "acquire", classes |-> {EpochClass, "N"},
                                  snapEpoch |-> lease.epoch, doomed |-> FALSE]]
    /\ UNCHANGED <<lease, exists, phase, believed, checked, txn, staleCommit,
                   spuriousAbort>>

\* Renew: update the expiry only.
BeginRenew(n) ==
    /\ ltx[n] = NoLtx
    /\ phase[n] \in {"sweeping", "active"}
    /\ Owns(n)
    /\ ltx' = [ltx EXCEPT ![n] =
                  [kind |-> "renew",
                   classes |-> IF RenewForUpdate THEN {"K", "N"} ELSE {"N"},
                   snapEpoch |-> lease.epoch, doomed |-> FALSE]]
    /\ UNCHANGED <<lease, exists, phase, believed, checked, txn, staleCommit,
                   spuriousAbort>>

\* Give up the lease after a failed sweep, advancing the epoch.
BeginRelease(n) ==
    /\ ltx[n] = NoLtx
    /\ phase[n] = "releasing"
    /\ Owns(n)
    /\ ltx' = [ltx EXCEPT ![n] = [kind |-> "release", classes |-> {EpochClass, "N"},
                                  snapEpoch |-> lease.epoch, doomed |-> FALSE]]
    /\ UNCHANGED <<lease, exists, phase, believed, checked, txn, staleCommit,
                   spuriousAbort>>

\* A committed lease transaction dooms every concurrent transaction it
\* conflicts with, the committing node's own run transaction included.
CommitAcquire(n) ==
    /\ ltx[n].kind = "acquire"
    /\ ~ltx[n].doomed
    /\ lease' = [owner |-> n, epoch |-> ltx[n].snapEpoch + 1, expired |-> FALSE]
    /\ believed' = [believed EXCEPT ![n] = ltx[n].snapEpoch + 1]
    /\ phase' = [phase EXCEPT ![n] = "sweeping"]
    /\ txn' = [m \in Nodes |-> DoomTxn(txn[m], LeaseAccess(ltx[n]), "acquire")]
    /\ ltx' = [m \in Nodes |->
                  IF m = n THEN NoLtx ELSE DoomLtx(ltx[m], LeaseAccess(ltx[n]))]
    /\ UNCHANGED <<exists, checked, staleCommit, spuriousAbort>>

CommitRenew(n) ==
    /\ ltx[n].kind = "renew"
    /\ ~ltx[n].doomed
    /\ lease' = [lease EXCEPT !.expired = FALSE]
    /\ txn' = [m \in Nodes |-> DoomTxn(txn[m], LeaseAccess(ltx[n]), "renew")]
    /\ ltx' = [m \in Nodes |->
                  IF m = n THEN NoLtx ELSE DoomLtx(ltx[m], LeaseAccess(ltx[n]))]
    /\ UNCHANGED <<exists, phase, believed, checked, staleCommit, spuriousAbort>>

CommitRelease(n) ==
    /\ ltx[n].kind = "release"
    /\ ~ltx[n].doomed
    /\ lease' = [owner |-> None, epoch |-> ltx[n].snapEpoch + 1, expired |-> FALSE]
    /\ believed' = [believed EXCEPT ![n] = 0]
    /\ phase' = [phase EXCEPT ![n] = "idle"]
    /\ txn' = [m \in Nodes |-> DoomTxn(txn[m], LeaseAccess(ltx[n]), "release")]
    /\ ltx' = [m \in Nodes |->
                  IF m = n THEN NoLtx ELSE DoomLtx(ltx[m], LeaseAccess(ltx[n]))]
    /\ UNCHANGED <<exists, checked, staleCommit, spuriousAbort>>

\* A lease transaction fails at commit: a failed takeover, renewal or release.
AbortLease(n) ==
    /\ ltx[n] # NoLtx
    /\ ltx[n].doomed
    /\ ltx' = [ltx EXCEPT ![n] = NoLtx]
    /\ UNCHANGED <<lease, exists, phase, believed, checked, txn, staleCommit,
                   spuriousAbort>>

FinishLease(n) ==
    CommitAcquire(n) \/ CommitRenew(n) \/ CommitRelease(n) \/ AbortLease(n)

(***************************************************************************
Time, sweeps and noticing

Expiry is an environment step: the owner failed to renew in time. It
happens only while an acquisition would still fit under MaxEpoch.
***************************************************************************)

LeaseExpires ==
    /\ lease.owner # None
    /\ ~lease.expired
    /\ lease.epoch < MaxEpoch
    /\ lease' = [lease EXCEPT !.expired = TRUE]
    /\ UNCHANGED <<exists, phase, believed, checked, txn, ltx, staleCommit,
                   spuriousAbort>>

\* The owner's own clock passes its lease's expiry, and it stops starting
\* commits. Transactions already in flight still run to their end.
NoticeExpiry(n) ==
    /\ SelfFence
    /\ phase[n] \in {"sweeping", "active"}
    /\ Owns(n)
    /\ lease.expired
    /\ phase' = [phase EXCEPT ![n] = "fenced"]
    /\ UNCHANGED <<lease, exists, believed, checked, txn, ltx, staleCommit,
                   spuriousAbort>>

\* A node learns it no longer owns the lease, from a failed fence check or
\* renewal, and gives up its claim.
NoticeLoss(n) ==
    /\ phase[n] # "idle"
    /\ ~Owns(n)
    /\ phase' = [phase EXCEPT ![n] = "idle"]
    /\ believed' = [believed EXCEPT ![n] = 0]
    /\ checked' = [checked EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<lease, exists, txn, ltx, staleCommit, spuriousAbort>>

SweepDone(n) ==
    /\ phase[n] = "sweeping"
    /\ phase' = [phase EXCEPT ![n] = "active"]
    /\ UNCHANGED <<lease, exists, believed, checked, txn, ltx, staleCommit,
                   spuriousAbort>>

SweepFails(n) ==
    /\ phase[n] = "sweeping"
    /\ lease.epoch < MaxEpoch
    /\ phase' = [phase EXCEPT ![n] = "releasing"]
    /\ UNCHANGED <<lease, exists, believed, checked, txn, ltx, staleCommit,
                   spuriousAbort>>

(***************************************************************************
Next-state relation and fairness
***************************************************************************)

Next ==
    \/ \E n \in Nodes :
          \/ SeparateCheck(n)
          \/ \E kind \in RunKinds, r \in Runs : BeginRun(n, kind, r)
          \/ FinishRun(n)
          \/ BeginAcquire(n)
          \/ BeginRenew(n)
          \/ BeginRelease(n)
          \/ FinishLease(n)
          \/ NoticeExpiry(n)
          \/ NoticeLoss(n)
          \/ SweepDone(n)
          \/ SweepFails(n)
    \/ LeaseExpires

\* Weak fairness only, on the protocol's own steps:
\* - a node keeps trying to acquire a lapsed lease;
\* - transactions in flight reach their commit;
\* - an owner eventually notices its own expiry;
\* - a node that has lost the lease eventually notices, as its renewer would;
\* - a node that decided to release does so.
\* Starting commits, renewals, expiry and sweep outcomes are left to the
\* environment.
Fairness ==
    /\ \A n \in Nodes : WF_vars(BeginAcquire(n))
    /\ \A n \in Nodes : WF_vars(FinishLease(n))
    /\ \A n \in Nodes : WF_vars(FinishRun(n))
    /\ \A n \in Nodes : WF_vars(NoticeExpiry(n))
    /\ \A n \in Nodes : WF_vars(NoticeLoss(n))
    /\ \A n \in Nodes : WF_vars(BeginRelease(n))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************
Invariants and properties
***************************************************************************)

TxnRecords ==
    [kind : RunKinds, run : Runs, epoch : 0..MaxEpoch,
     leaseCls : {"none", "K", "S"}, runCls : {"K", "N"},
     doomed : BOOLEAN, spurious : BOOLEAN]

LtxRecords ==
    [kind : {"acquire", "renew", "release"}, classes : SUBSET {"K", "N"},
     snapEpoch : Epochs, doomed : BOOLEAN]

TypeOK ==
    /\ lease \in [owner : Nodes \cup {None}, epoch : Epochs, expired : BOOLEAN]
    /\ exists \in [Runs -> BOOLEAN]
    /\ phase \in [Nodes -> Phases]
    /\ believed \in [Nodes -> 0..MaxEpoch]
    /\ checked \in [Nodes -> BOOLEAN]
    /\ \A n \in Nodes : txn[n] = NoTxn \/ txn[n] \in TxnRecords
    /\ \A n \in Nodes : ltx[n] = NoLtx \/ ltx[n] \in LtxRecords
    /\ staleCommit \in BOOLEAN
    /\ spuriousAbort \in BOOLEAN

NoStaleOwnerCommit == ~staleCommit

NoSpuriousAbort == ~spuriousAbort

\* No two nodes believe they hold the same epoch.
OneOwnerPerEpoch ==
    \A m, n \in Nodes : (m # n /\ believed[m] # 0) => believed[m] # believed[n]

Lapsed == lease.owner # None /\ lease.expired

LapsedLeaseResolves == Lapsed ~> ~Lapsed

=============================================================================
