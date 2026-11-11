------------------------- MODULE 43_activity_timeouts -------------------------
EXTENDS Naturals

(***************************************************************************
This module models how an activity's timeout deadlines are tracked in memory
and how the timeout scanner turns a due deadline into a commit.

WHAT THIS MODULE MODELS
=======================

One activity of one open run, with a retry policy. Its committed state is
the authority: the current attempt, whether and when that attempt started,
the last heartbeat time, and when the attempt was scheduled. Every commit
advances the run's transition sequence and is fenced by it, so a writer
whose read is no longer current conflicts and reloads.

The tracking cache is derived in memory and holds no authority. It keeps the
anchors the deadlines are computed from, the sequence of the committed image
they were taken from, a revision that every applied update advances, and two
flags: a deadline is pending, and a reload was requested. Its writers are:

- the start, heartbeat and retry paths, each after its commit. Each update
  carries the image its commit wrote, and applies unless the entry already
  holds a later image. It then takes that image's anchors, keeping a cached
  heartbeat at or after the image's start.
- the scanner, which marks an entry attempted before it reads the run,
  reloads the cache from that read by the same rule, and requests a reload
  after a stale rejection.
- the full pass, which marks every entry for reload.

Only the scanner's mark clears a requested reload; an update never does.

The scanner evaluates its read at its own clock reading. A start-to-close or
heartbeat timeout that the retry policy still allows becomes a retry commit;
any other timeout is submitted to the run's lane as a terminal resolution.
Both commit paths carry the observation (attempt, timeout and clock reading)
and recheck it against every image they load, including after a conflict.

Workers race the scanner. A start commits the current attempt's start time.
A worker heartbeats or fails the attempt it holds; a failure commits the
next attempt when the retry policy allows. Each update can land long after
its commit. With Replays, a worker may act on a start it learned from a
replayed start request while the original call's update is still pending,
and a start's caller may lose the outcome of a commit that applied. That
start then makes no update, and the cache misses it until the full pass
reloads the entry.

The model checks four invariants:

- StartedAttemptTracked: an open activity with a started attempt keeps a
  tracking entry, so every full pass reloads it.
- StartedDeadlineTracked: a started attempt always has a tracked deadline.
  The entry becomes due no later than the attempt's earliest deadline in
  committed state, unless that start's update is still pending, a reload is
  already requested, the scanner is examining the entry, or a start's
  update was lost and no full pass has run since.
- TimeoutForObservedAttempt: a timeout commit applies only to the attempt it
  was computed for.
- TimeoutStillDue: a timeout commit applies only while the timeout it
  observed still holds for the committed anchors at the observed time.

Temporal evaluates an activity's timers from its current mutable state under
the workflow lock and skips a timer of an earlier attempt
(service/history/timer_queue_active_task_executor.go @ v1.31.0). Here the
fenced commit and its recheck stand in for that lock.

HOW IT MAPS TO THE CODE
=======================

  run          the activity in the run's committed state; seq is the run's
               transition sequence
  cache        its ActivityTrackingState entry (activity_timeout.rs over
               acquisition_tracking.rs); seq is the image it holds
  scan         one pass of scan_activity_timeouts_at for this entry, and the
               terminal submission the run's lane validates and commits
  st, hb, wf   a start, a heartbeat and a worker's retryable failure, each
               from its load to its tracking update
  img          the image an update carries, or the scanner's read
  cur          the captured revision still equals the entry's revision
  fresh        no commit has landed since this writer's load
  lost,        history flags for StartedDeadlineTracked and the two timeout
  misapplied,  invariants
  undue

The order is the run's transition sequence rather than the attempt number:
a reset can set a started activity back to attempt 1, and an activity ID can
be reused by a later schedule, but every commit advances the sequence.

WHAT THIS MODULE DOES NOT MODEL
===============================

- Shard ownership, acquisitions and their fences. Every scoped update here
  passes its acquisition check.
- Pause, options updates, cancel requests, resets and eligibility tracking.
- Writers that carry no image: a published dispatch's schedule record, the
  lane's option updates, and recovery. They never change the entry's image
  sequence, so a later image applies over them, and the full pass reloads
  what they leave stale.
- Worker completions and terminal failures. They go through the run's lane,
  as the terminal timeout does, and are what validation's Resolved outcome
  reports; this model never reaches that outcome.
- Load and commit errors other than conflicts and a start's lost outcome,
  the lane's cached image (any image it validates is fenced by its
  sequence), reads shared within a pass, and the per-pass submission cap.
- An activity that starts before its schedule is tracked. The entry exists
  from the first schedule.
- Real time. The clock is a counter, and deadlines are strict, as in the
  code: a timeout of d anchored at a fires once now > a + d.

NEGATIVE CONTROLS
=================

  CheckAttempt = FALSE          validation ignores the attempt
  CheckDeadline = FALSE         validation ignores the timeout's re-evaluation
  RecheckAfterConflict = FALSE  validation runs only on the first load
  StaleKeepsTracking = FALSE    a stale rejection drops the entry
  OrderedUpdates = FALSE        the start's update applies only under the
                                revision it read after its load, the
                                heartbeat's update sets only the heartbeat
                                time, and the scanner's retry update applies
                                only under its captured revision
  OrderedWorkerRetry = FALSE    a worker retry's update applies whenever the
                                entry exists
  OrderedReload = FALSE         the scanner's reload applies only under its
                                captured revision
  UpdatesKeepRefresh = FALSE    every applied update clears a requested
                                reload

The last four are the code's rules before updates were ordered by image.
Replays and WorkerFailures select which workers act.
***************************************************************************)

CONSTANTS MaxTime, MaxAttempt, MaxBeats, Backoff,
          ScheduleToStart, StartToClose, Heartbeat, ScheduleToClose,
          Replays, WorkerFailures,
          CheckAttempt, CheckDeadline, RecheckAfterConflict,
          StaleKeepsTracking,
          OrderedUpdates, OrderedWorkerRetry, OrderedReload,
          UpdatesKeepRefresh

VARIABLES now, run, cache, scan, st, hb, wf, beats, lost, misapplied, undue
vars == <<now, run, cache, scan, st, hb, wf, beats, lost, misapplied, undue>>

ASSUME /\ MaxTime \in Nat \ {0} /\ MaxAttempt \in Nat \ {0}
       /\ MaxBeats \in Nat /\ Backoff \in Nat
       /\ ScheduleToStart \in Nat /\ StartToClose \in Nat
       /\ Heartbeat \in Nat /\ ScheduleToClose \in Nat
       /\ Replays \in BOOLEAN /\ WorkerFailures \in BOOLEAN
       /\ CheckAttempt \in BOOLEAN /\ CheckDeadline \in BOOLEAN
       /\ RecheckAfterConflict \in BOOLEAN /\ StaleKeepsTracking \in BOOLEAN
       /\ OrderedUpdates \in BOOLEAN /\ OrderedWorkerRetry \in BOOLEAN
       /\ OrderedReload \in BOOLEAN /\ UpdatesKeepRefresh \in BOOLEAN

\* The clock starts at 1, so 0 can stand for "no start" and "no heartbeat".
None == 0
Scheduled == 1
Time == 1..MaxTime
Dispatch == 1..(MaxTime + Backoff)
Never == MaxTime + Backoff + ScheduleToStart + StartToClose + Heartbeat
         + ScheduleToClose + 2
\* Commits: a start per attempt, the heartbeats, a retry per later attempt and
\* the terminal resolution, after the schedule's sequence 1.
MaxSeq == 2 * MaxAttempt + MaxBeats + 1
Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
Violations == {"none", "close", "start", "heartbeat", "attempt"}

(***************************************************************************
Timeout rules. A zero timeout is unset. Schedule-to-close is anchored at the
first schedule and applies to every attempt. A started attempt adds the
heartbeat timeout, anchored at the later of its start and last heartbeat,
then start-to-close; an unstarted attempt adds schedule-to-start, anchored
at its scheduled time. Violation names: "close" schedule-to-close, "start"
schedule-to-start, "heartbeat" heartbeat, "attempt" start-to-close.
***************************************************************************)
Violation(d, beat, t) ==
    IF ScheduleToClose > 0 /\ t > Scheduled + ScheduleToClose THEN "close"
    ELSE IF d.start # None
    THEN IF Heartbeat > 0 /\ t > Max(beat, d.start) + Heartbeat THEN "heartbeat"
         ELSE IF StartToClose > 0 /\ t > d.start + StartToClose THEN "attempt"
         ELSE "none"
    ELSE IF ScheduleToStart > 0 /\ t > d.disp + ScheduleToStart THEN "start"
    ELSE "none"

DueAfter(anchor, timeout) == IF timeout > 0 THEN anchor + timeout + 1 ELSE Never

\* When an image's earliest deadline becomes due: one tick after the deadline,
\* as timeouts are strict (activity_timeout_due_at). Applied to the cache, it
\* is when the scanner selects the entry; applied to committed state, it is
\* when a timeout first holds.
Due(c) ==
    Min(DueAfter(Scheduled, ScheduleToClose),
        IF c.start # None
        THEN Min(DueAfter(c.start, StartToClose),
                 DueAfter(Max(c.beat, c.start), Heartbeat))
        ELSE DueAfter(c.disp, ScheduleToStart))

\* The retry policy allows another attempt after a failure decided at t.
RetryAllowed(att, t) ==
    /\ att < MaxAttempt
    /\ (ScheduleToClose = 0 \/ t + Backoff < Scheduled + ScheduleToClose)

Retries(v, att, t) == v \in {"heartbeat", "attempt"} /\ RetryAllowed(att, t)

\* The activity's part of a committed image, and an empty one.
ImageOf(r) == [att |-> r.att, start |-> r.start, beat |-> r.beat,
               disp |-> r.disp, seq |-> r.seq]
NoImage == [att |-> 1, start |-> None, beat |-> None, disp |-> Scheduled,
            seq |-> 0]

Untracked == [present |-> FALSE, seq |-> 0, start |-> None, beat |-> None,
              disp |-> Scheduled, refresh |-> FALSE, pend |-> FALSE]

ScanIdle == [phase |-> "idle", cur |-> FALSE, fresh |-> FALSE,
             retried |-> FALSE, at |-> 1, beat |-> None, att |-> 1,
             v |-> "none", done |-> 1, img |-> NoImage]
StartIdle == [phase |-> "idle", cur |-> FALSE, fresh |-> FALSE, at |-> 1,
              img |-> NoImage]
BeatIdle == [phase |-> "idle", fresh |-> FALSE, at |-> 1, img |-> NoImage]
FailIdle == [phase |-> "idle", fresh |-> FALSE, at |-> 1, att |-> 1,
             img |-> NoImage]

\* A commit leaves every other writer's read behind.
Behind(w) == [w EXCEPT !.fresh = FALSE]
\* An applied update advances the revision every capture holds.
Superseded(w) == [w EXCEPT !.cur = FALSE]

(***************************************************************************
Cache updates. An image applies unless the entry holds a later one. Taking
an image keeps a cached heartbeat at or after the image's start, so a later
heartbeat survives an earlier image of the same start.
***************************************************************************)
NotBehind(i) == i.seq >= cache.seq
BeatFor(b, i) == IF i.start # None /\ Max(b, i.beat) >= i.start
                 THEN Max(b, i.beat) ELSE None
Adopt(c, i) == [c EXCEPT !.seq = i.seq, !.start = i.start,
                         !.beat = BeatFor(c.beat, i), !.disp = i.disp]
\* An applied update rearms the deadline. It keeps a requested reload unless
\* the negative control clears it.
Touched(c) == [c EXCEPT !.pend = TRUE, !.refresh = c.refresh /\ UpdatesKeepRefresh]

StartApplies == cache.present /\ IF OrderedUpdates THEN NotBehind(st.img)
                                 ELSE st.cur
BeatApplies == cache.present /\ (OrderedUpdates => NotBehind(hb.img))
BeatUpdate == IF OrderedUpdates THEN Adopt(cache, hb.img)
              ELSE [cache EXCEPT !.beat = hb.at]
FailApplies == cache.present /\ (OrderedWorkerRetry => NotBehind(wf.img))
RetryApplies == cache.present /\ IF OrderedUpdates THEN NotBehind(scan.img)
                                 ELSE scan.cur

\* The scanner's reload (refresh_scanned) keeps the revision and rearms the
\* deadline only if it is still in the future at the scan's clock reading.
ReloadApplies == cache.present /\ IF OrderedReload THEN NotBehind(scan.img)
                                  ELSE scan.cur
Pending(c) == [c EXCEPT !.pend = Due(c) > scan.at]
Refreshed == IF ReloadApplies THEN Pending(Adopt(cache, scan.img)) ELSE cache
ScanView == Violation(scan.img, Max(scan.beat, scan.img.beat), scan.at)

\* A stale rejection keeps the entry and requests an immediate reload.
RefreshRequested == IF cache.present THEN [cache EXCEPT !.refresh = TRUE]
                    ELSE cache
\* Negative control: a stale rejection removes the captured entry instead.
StaleRemoved == IF cache.present /\ scan.cur THEN Untracked ELSE cache

\* ActivityTimeoutCheck::validate on an open activity.
Holds(d) ==
    /\ (CheckAttempt => d.att = scan.att)
    /\ (CheckDeadline => Violation(d, d.beat, scan.at) = scan.v)
\* Whether this load is validated: always, unless the negative control skips
\* validation after a conflict.
Checked == ~scan.retried \/ RecheckAfterConflict

\* A worker holds the current started attempt.
WorkerHolds == run.open /\ run.start # None /\ (Replays \/ st.phase # "reply")

Init ==
    /\ now = 1
    /\ run = [open |-> TRUE, att |-> 1, start |-> None, beat |-> None,
              disp |-> Scheduled, seq |-> 1]
    /\ cache = [Untracked EXCEPT !.present = TRUE, !.seq = 1, !.pend = TRUE]
    /\ scan = ScanIdle
    /\ st = StartIdle
    /\ hb = BeatIdle
    /\ wf = FailIdle
    /\ beats = 0
    /\ lost = FALSE
    /\ misapplied = FALSE
    /\ undue = FALSE

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<run, cache, scan, st, hb, wf, beats, lost, misapplied,
                   undue>>

FullPass ==
    /\ cache.present
    /\ (lost \/ ~(cache.refresh /\ cache.pend))
    /\ cache' = [cache EXCEPT !.refresh = TRUE, !.pend = TRUE]
    /\ lost' = FALSE
    /\ UNCHANGED <<now, run, scan, st, hb, wf, beats, misapplied, undue>>

(***************************************************************************
The scanner (scan_activity_timeouts_at). It selects a due or refresh-marked
entry and marks it attempted, then reads the run. After the read it reloads
the cache from that image and evaluates the image at its own clock reading.
One outcome per action: nothing due, a retry, or a terminal timeout for the
lane.
***************************************************************************)
ScanSelect ==
    /\ scan.phase = "idle"
    /\ cache.present
    /\ (cache.refresh \/ (cache.pend /\ Due(cache) <= now))
    /\ cache' = [cache EXCEPT !.refresh = FALSE, !.pend = FALSE]
    /\ scan' = [ScanIdle EXCEPT !.phase = "load", !.cur = TRUE, !.at = now,
                                !.beat = cache.beat]
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

ScanLoad ==
    /\ scan.phase = "load"
    /\ run.open
    /\ scan' = [scan EXCEPT !.phase = "read", !.img = ImageOf(run)]
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

ScanReadNone ==
    /\ scan.phase = "read"
    /\ ScanView = "none"
    /\ cache' = Refreshed
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

ScanReadRetry ==
    /\ scan.phase = "read"
    /\ Retries(ScanView, scan.img.att, scan.at)
    /\ cache' = Refreshed
    /\ scan' = [scan EXCEPT !.phase = "retry", !.att = scan.img.att,
                            !.v = ScanView]
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

ScanReadTerminal ==
    /\ scan.phase = "read"
    /\ ScanView # "none"
    /\ ~Retries(ScanView, scan.img.att, scan.at)
    /\ cache' = Refreshed
    /\ scan' = [scan EXCEPT !.phase = "lane", !.att = scan.img.att,
                            !.v = ScanView, !.img = NoImage]
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

\* The run's lane validates the terminal timeout against each image it loads
\* (ActivityTimeoutCheck::validate), commits under that image's sequence, and
\* reloads after a conflict.
LaneStale ==
    /\ scan.phase = "lane"
    /\ run.open
    /\ Checked /\ ~Holds(run)
    /\ StaleKeepsTracking
    /\ cache' = RefreshRequested
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

LaneStaleDrops ==
    /\ scan.phase = "lane"
    /\ run.open
    /\ Checked /\ ~Holds(run)
    /\ ~StaleKeepsTracking
    /\ cache' = StaleRemoved
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

LaneLoad ==
    /\ scan.phase = "lane"
    /\ run.open
    /\ (~Checked \/ Holds(run))
    /\ scan' = [scan EXCEPT !.phase = "laneCommit", !.fresh = TRUE]
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

\* The resolution closes the activity; the lane then stops tracking it.
LaneCommit ==
    /\ scan.phase = "laneCommit"
    /\ scan.fresh
    /\ misapplied' = (misapplied \/ run.att # scan.att)
    /\ undue' = (undue \/ Violation(run, run.beat, scan.at) # scan.v)
    /\ run' = [run EXCEPT !.open = FALSE, !.seq = run.seq + 1]
    /\ cache' = Untracked
    /\ scan' = ScanIdle
    /\ st' = Behind(st)
    /\ hb' = Behind(hb)
    /\ wf' = Behind(wf)
    /\ UNCHANGED <<now, beats, lost>>

LaneConflict ==
    /\ scan.phase = "laneCommit"
    /\ ~scan.fresh
    /\ scan' = [scan EXCEPT !.phase = "lane", !.retried = TRUE]
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

\* The scanner's retry (commit_activity_retry) validates the same observation
\* on each load.
RetryStale ==
    /\ scan.phase = "retry"
    /\ run.open
    /\ Checked /\ ~Holds(run)
    /\ StaleKeepsTracking
    /\ cache' = RefreshRequested
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

RetryStaleDrops ==
    /\ scan.phase = "retry"
    /\ run.open
    /\ Checked /\ ~Holds(run)
    /\ ~StaleKeepsTracking
    /\ cache' = StaleRemoved
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, st, hb, wf, beats, lost, misapplied, undue>>

\* The retry's own expected-attempt check: an error the scanner only logs.
RetryWrongAttempt ==
    /\ scan.phase = "retry"
    /\ run.open
    /\ (~Checked \/ Holds(run))
    /\ run.att # scan.att
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

RetryLoad ==
    /\ scan.phase = "retry"
    /\ run.open
    /\ (~Checked \/ Holds(run))
    /\ run.att = scan.att
    /\ scan' = [scan EXCEPT !.phase = "retryCommit", !.fresh = TRUE,
                            !.done = now]
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

RetryCommit ==
    /\ scan.phase = "retryCommit"
    /\ scan.fresh
    /\ misapplied' = (misapplied \/ run.att # scan.att)
    /\ undue' = (undue \/ Violation(run, run.beat, scan.at) # scan.v)
    /\ run' = [run EXCEPT !.att = run.att + 1, !.start = None,
                          !.disp = scan.done + Backoff, !.seq = run.seq + 1]
    /\ scan' = [scan EXCEPT !.phase = "retryReply", !.fresh = FALSE,
                            !.img = ImageOf(run')]
    /\ st' = Behind(st)
    /\ hb' = Behind(hb)
    /\ wf' = Behind(wf)
    /\ UNCHANGED <<now, cache, beats, lost>>

RetryConflict ==
    /\ scan.phase = "retryCommit"
    /\ ~scan.fresh
    /\ scan' = [scan EXCEPT !.phase = "retry", !.retried = TRUE]
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

\* The retry's update carries the image the retry committed.
RetryReplyApplies ==
    /\ scan.phase = "retryReply"
    /\ RetryApplies
    /\ cache' = Touched(Adopt(cache, scan.img))
    /\ scan' = ScanIdle
    /\ st' = Superseded(st)
    /\ UNCHANGED <<now, run, hb, wf, beats, lost, misapplied, undue>>

RetryReplyDropped ==
    /\ scan.phase = "retryReply"
    /\ ~RetryApplies
    /\ scan' = ScanIdle
    /\ UNCHANGED <<now, run, cache, st, hb, wf, beats, lost, misapplied,
                   undue>>

(***************************************************************************
A start (start_activity_task_inner): load, commit, then its tracking update
with the image it committed. Its revision capture after the load matters
only to the negative control.
***************************************************************************)
StartLoad ==
    /\ st.phase = "idle"
    /\ run.open
    /\ run.start = None
    /\ run.disp <= now
    /\ st' = [st EXCEPT !.phase = "loaded", !.cur = cache.present,
                        !.fresh = TRUE, !.at = now]
    /\ UNCHANGED <<now, run, cache, scan, hb, wf, beats, lost, misapplied,
                   undue>>

StartCommit ==
    /\ st.phase = "loaded"
    /\ st.fresh
    /\ run' = [run EXCEPT !.start = st.at, !.seq = run.seq + 1]
    /\ st' = [st EXCEPT !.phase = "reply", !.fresh = FALSE,
                        !.img = ImageOf(run')]
    /\ scan' = Behind(scan)
    /\ hb' = Behind(hb)
    /\ wf' = Behind(wf)
    /\ UNCHANGED <<now, cache, beats, lost, misapplied, undue>>

StartConflict ==
    /\ st.phase = "loaded"
    /\ ~st.fresh
    /\ st' = StartIdle
    /\ UNCHANGED <<now, run, cache, scan, hb, wf, beats, lost, misapplied,
                   undue>>

StartReplyTracks ==
    /\ st.phase = "reply"
    /\ StartApplies
    /\ cache' = Touched(Adopt(cache, st.img))
    /\ st' = StartIdle
    /\ scan' = Superseded(scan)
    /\ UNCHANGED <<now, run, hb, wf, beats, lost, misapplied, undue>>

StartReplyDropped ==
    /\ st.phase = "reply"
    /\ ~StartApplies
    /\ st' = StartIdle
    /\ UNCHANGED <<now, run, cache, scan, hb, wf, beats, lost, misapplied,
                   undue>>

\* The caller lost the outcome of a commit that applied, so no update
\* follows; it learns the start from a replayed request.
StartReplyLost ==
    /\ st.phase = "reply"
    /\ Replays
    /\ st' = StartIdle
    /\ lost' = TRUE
    /\ UNCHANGED <<now, run, cache, scan, hb, wf, beats, misapplied, undue>>

(***************************************************************************
A heartbeat (record_activity_heartbeat_inner): load, commit the heartbeat
time, then its tracking update with the image it committed.
***************************************************************************)
BeatLoad ==
    /\ hb.phase = "idle"
    /\ beats < MaxBeats
    /\ WorkerHolds
    /\ hb' = [hb EXCEPT !.phase = "loaded", !.fresh = TRUE, !.at = now]
    /\ UNCHANGED <<now, run, cache, scan, st, wf, beats, lost, misapplied,
                   undue>>

BeatCommit ==
    /\ hb.phase = "loaded"
    /\ hb.fresh
    /\ run' = [run EXCEPT !.beat = hb.at, !.seq = run.seq + 1]
    /\ beats' = beats + 1
    /\ hb' = [hb EXCEPT !.phase = "reply", !.fresh = FALSE,
                        !.img = ImageOf(run')]
    /\ scan' = Behind(scan)
    /\ st' = Behind(st)
    /\ wf' = Behind(wf)
    /\ UNCHANGED <<now, cache, lost, misapplied, undue>>

\* A conflicted heartbeat starts again; a changed attempt rejects its token.
BeatConflict ==
    /\ hb.phase = "loaded"
    /\ ~hb.fresh
    /\ hb' = BeatIdle
    /\ UNCHANGED <<now, run, cache, scan, st, wf, beats, lost, misapplied,
                   undue>>

BeatReplyLands ==
    /\ hb.phase = "reply"
    /\ BeatApplies
    /\ cache' = Touched(BeatUpdate)
    /\ hb' = BeatIdle
    /\ scan' = Superseded(scan)
    /\ st' = Superseded(st)
    /\ UNCHANGED <<now, run, wf, beats, lost, misapplied, undue>>

BeatReplyIgnored ==
    /\ hb.phase = "reply"
    /\ ~BeatApplies
    /\ hb' = BeatIdle
    /\ UNCHANGED <<now, run, cache, scan, st, wf, beats, lost, misapplied,
                   undue>>

(***************************************************************************
A worker's retryable failure (fail_activity_task_inner, then
commit_activity_retry): the retry decision and load, the commit of the next
attempt, then its tracking update with the image it committed.
***************************************************************************)
FailLoad ==
    /\ wf.phase = "idle"
    /\ WorkerFailures
    /\ WorkerHolds
    /\ RetryAllowed(run.att, now)
    /\ wf' = [wf EXCEPT !.phase = "loaded", !.fresh = TRUE, !.at = now,
                        !.att = run.att]
    /\ UNCHANGED <<now, run, cache, scan, st, hb, beats, lost, misapplied,
                   undue>>

FailCommit ==
    /\ wf.phase = "loaded"
    /\ wf.fresh
    /\ run' = [run EXCEPT !.att = run.att + 1, !.start = None,
                          !.disp = wf.at + Backoff, !.seq = run.seq + 1]
    /\ wf' = [wf EXCEPT !.phase = "reply", !.fresh = FALSE,
                        !.img = ImageOf(run')]
    /\ scan' = Behind(scan)
    /\ st' = Behind(st)
    /\ hb' = Behind(hb)
    /\ UNCHANGED <<now, cache, beats, lost, misapplied, undue>>

\* A conflicted retry reloads; it continues only while its attempt is current.
FailReload ==
    /\ wf.phase = "loaded"
    /\ ~wf.fresh
    /\ run.open /\ run.att = wf.att
    /\ wf' = [wf EXCEPT !.fresh = TRUE, !.at = now]
    /\ UNCHANGED <<now, run, cache, scan, st, hb, beats, lost, misapplied,
                   undue>>

FailRejected ==
    /\ wf.phase = "loaded"
    /\ ~wf.fresh
    /\ ~(run.open /\ run.att = wf.att)
    /\ wf' = FailIdle
    /\ UNCHANGED <<now, run, cache, scan, st, hb, beats, lost, misapplied,
                   undue>>

FailReplyLands ==
    /\ wf.phase = "reply"
    /\ FailApplies
    /\ cache' = Touched(Adopt(cache, wf.img))
    /\ wf' = FailIdle
    /\ scan' = Superseded(scan)
    /\ st' = Superseded(st)
    /\ UNCHANGED <<now, run, hb, beats, lost, misapplied, undue>>

FailReplyIgnored ==
    /\ wf.phase = "reply"
    /\ ~FailApplies
    /\ wf' = FailIdle
    /\ UNCHANGED <<now, run, cache, scan, st, hb, beats, lost, misapplied,
                   undue>>

Next ==
    \/ Tick \/ FullPass
    \/ ScanSelect \/ ScanLoad \/ ScanReadNone \/ ScanReadRetry
    \/ ScanReadTerminal
    \/ LaneStale \/ LaneStaleDrops \/ LaneLoad \/ LaneCommit \/ LaneConflict
    \/ RetryStale \/ RetryStaleDrops \/ RetryWrongAttempt \/ RetryLoad
    \/ RetryCommit \/ RetryConflict \/ RetryReplyApplies \/ RetryReplyDropped
    \/ StartLoad \/ StartCommit \/ StartConflict
    \/ StartReplyTracks \/ StartReplyDropped \/ StartReplyLost
    \/ BeatLoad \/ BeatCommit \/ BeatConflict \/ BeatReplyLands
    \/ BeatReplyIgnored
    \/ FailLoad \/ FailCommit \/ FailReload \/ FailRejected
    \/ FailReplyLands \/ FailReplyIgnored

Spec == Init /\ [][Next]_vars

Seqs == 0..MaxSeq
Images == [att: 1..MaxAttempt, start: {None} \cup Time,
           beat: {None} \cup Time, disp: Dispatch, seq: Seqs]

TypeOK ==
    /\ now \in Time
    /\ run \in [open: BOOLEAN, att: 1..MaxAttempt, start: {None} \cup Time,
                beat: {None} \cup Time, disp: Dispatch, seq: Seqs]
    /\ cache \in [present: BOOLEAN, seq: Seqs, start: {None} \cup Time,
                  beat: {None} \cup Time, disp: Dispatch,
                  refresh: BOOLEAN, pend: BOOLEAN]
    /\ scan \in [phase: {"idle", "load", "read", "lane", "laneCommit",
                         "retry", "retryCommit", "retryReply"},
                 cur: BOOLEAN, fresh: BOOLEAN, retried: BOOLEAN, at: Time,
                 beat: {None} \cup Time, att: 1..MaxAttempt, v: Violations,
                 done: Time, img: Images]
    /\ st \in [phase: {"idle", "loaded", "reply"}, cur: BOOLEAN,
               fresh: BOOLEAN, at: Time, img: Images]
    /\ hb \in [phase: {"idle", "loaded", "reply"}, fresh: BOOLEAN, at: Time,
               img: Images]
    /\ wf \in [phase: {"idle", "loaded", "reply"}, fresh: BOOLEAN, at: Time,
               att: 1..MaxAttempt, img: Images]
    /\ beats \in 0..MaxBeats
    /\ lost \in BOOLEAN
    /\ misapplied \in BOOLEAN
    /\ undue \in BOOLEAN

StartedAttemptTracked == run.open /\ run.start # None => cache.present

\* Due(run) is the attempt's earliest deadline from committed state. The
\* entry must become due no later, unless that start's update is still
\* pending, a reload is already requested, the scanner is examining it, or a
\* start's update was lost and no full pass has run since.
StartedDeadlineTracked ==
    run.open /\ run.start # None /\ st.phase # "reply" /\ scan.phase = "idle"
    /\ ~lost
        => /\ cache.present
           /\ \/ cache.refresh
              \/ cache.pend /\ Due(cache) <= Due(run)

TimeoutForObservedAttempt == ~misapplied

TimeoutStillDue == ~undue
=============================================================================
