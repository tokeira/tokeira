//! The lane's per-run queues and their admission accounting.
//!
//! A lane keeps one FIFO per run that has work, and a ready order of runs
//! waiting for an activation. This module owns only that bookkeeping: it does
//! no I/O and never awaits, so the lane can call it under a plain mutex and the
//! accounting can be property-tested without a runtime.
//!
//! Invariants the lane relies on:
//!
//! - **Exact claims.** A claim is taken by [`RunQueues::try_reserve`] and given
//!   back exactly once: by [`RunQueues::release`] when the caller drops it
//!   unused, or by [`RunQueues::take_next`] when its command starts. Claims
//!   therefore count every command that is accepted but not started, queued or
//!   still being built, and never exceed the lane budget or the run share.
//! - **One activation per run.** A run is `Idle`, `Ready` (in the ready order,
//!   exactly once) or `Active`. Only [`RunQueues::pop_ready`] makes a run
//!   active and only [`RunQueues::finish`] ends that, so the lane never runs two
//!   activations for one run and commands leave a run's queue in arrival order.
//! - **Fair re-entry.** A run finished with work left goes to the back of the
//!   ready order, behind every run already waiting.
//! - **Bounded map.** An entry is removed once it is idle with no claims and an
//!   empty queue, so the map holds at most the claimed runs plus the active ones.

use std::collections::{HashMap, VecDeque};

use tokeira_types::RunKey;

/// The most commands one lane holds accepted but not started, summed over its
/// runs: the lane's mailbox capacity. It bounds the memory a lane's backlog can
/// pin and how long the last accepted command waits: at the default drain of
/// 16 commands per activation, a full lane drains in 64 activations. Past it,
/// a submit is refused rather than queued behind work that cannot finish
/// within a caller's deadline.
pub(crate) const LANE_BUDGET: usize = 1024;

/// Why a lane refused a submit reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LaneRefusal {
    /// The run already holds its share of the lane.
    RunShareFull,
    /// The lane holds its whole budget across its runs.
    LaneBudgetFull,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunState {
    Idle,
    Ready,
    Active,
}

#[derive(Debug)]
struct RunEntry<T> {
    queue: VecDeque<T>,
    /// This run's claims, queued or still being built.
    reserved: usize,
    state: RunState,
}

impl<T> RunEntry<T> {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            reserved: 0,
            state: RunState::Idle,
        }
    }

    fn is_unused(&self) -> bool {
        self.state == RunState::Idle && self.reserved == 0 && self.queue.is_empty()
    }
}

/// Per-run FIFOs, the ready order, and the lane's claim accounting.
#[derive(Debug)]
pub(crate) struct RunQueues<T> {
    budget: usize,
    share: usize,
    reserved: usize,
    queued: usize,
    active: usize,
    runs: HashMap<RunKey, RunEntry<T>>,
    ready: VecDeque<RunKey>,
}

impl<T> RunQueues<T> {
    /// Queues admitting `budget` unstarted commands per lane and half that per
    /// run. Half is the largest share that still leaves every other run on the
    /// lane at least as much room as one hot run takes.
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            budget,
            share: (budget / 2).max(1),
            reserved: 0,
            queued: 0,
            active: 0,
            runs: HashMap::new(),
            ready: VecDeque::new(),
        }
    }

    /// Take one claim for `run`, or refuse at once.
    ///
    /// The run share is checked before the lane budget, so a hot run's own
    /// submits see the busy-workflow refusal rather than the lane-wide one.
    /// This is the single place an admission policy would decide who is
    /// refused first; today every caller faces the same two bounds.
    pub(crate) fn try_reserve(&mut self, run: RunKey) -> Result<(), LaneRefusal> {
        let run_reserved = self.runs.get(&run).map_or(0, |entry| entry.reserved);
        if run_reserved >= self.share {
            return Err(LaneRefusal::RunShareFull);
        }
        if self.reserved >= self.budget {
            return Err(LaneRefusal::LaneBudgetFull);
        }
        self.runs.entry(run).or_insert_with(RunEntry::new).reserved += 1;
        self.reserved += 1;
        Ok(())
    }

    /// Give back a claim whose command was never enqueued.
    pub(crate) fn release(&mut self, run: RunKey) {
        let Some(entry) = self.runs.get_mut(&run) else {
            debug_assert!(false, "released a claim the run does not hold");
            return;
        };
        debug_assert!(entry.reserved > entry.queue.len(), "no unqueued claim");
        entry.reserved = entry.reserved.saturating_sub(1);
        self.reserved = self.reserved.saturating_sub(1);
        self.prune(run);
    }

    /// Append `item` to `run`'s queue under a claim the caller already holds.
    /// Returns `true` when the run became ready, so the caller wakes the
    /// dispatcher.
    pub(crate) fn enqueue(&mut self, run: RunKey, item: T) -> bool {
        let entry = self.runs.entry(run).or_insert_with(RunEntry::new);
        debug_assert!(
            entry.reserved > entry.queue.len(),
            "enqueue without a claim"
        );
        entry.queue.push_back(item);
        self.queued += 1;
        if entry.state == RunState::Idle {
            entry.state = RunState::Ready;
            self.ready.push_back(run);
            return true;
        }
        false
    }

    /// Hand the first ready run to an activation.
    pub(crate) fn pop_ready(&mut self) -> Option<RunKey> {
        let run = self.ready.pop_front()?;
        let entry = self
            .runs
            .get_mut(&run)
            .expect("a ready run keeps its entry");
        debug_assert_eq!(entry.state, RunState::Ready);
        entry.state = RunState::Active;
        self.active += 1;
        Some(run)
    }

    /// Start the next command of an active run, releasing its claim.
    pub(crate) fn take_next(&mut self, run: RunKey) -> Option<T> {
        let entry = self.runs.get_mut(&run)?;
        debug_assert_eq!(entry.state, RunState::Active, "only an activation takes");
        let item = entry.queue.pop_front()?;
        entry.reserved -= 1;
        self.reserved -= 1;
        self.queued -= 1;
        Some(item)
    }

    /// End `run`'s activation. A run with work left re-enters the ready order
    /// at the back; returns whether it did.
    pub(crate) fn finish(&mut self, run: RunKey) -> bool {
        let Some(entry) = self.runs.get_mut(&run) else {
            debug_assert!(false, "finished a run with no entry");
            return false;
        };
        debug_assert_eq!(entry.state, RunState::Active);
        self.active -= 1;
        if entry.queue.is_empty() {
            entry.state = RunState::Idle;
            self.prune(run);
            return false;
        }
        entry.state = RunState::Ready;
        self.ready.push_back(run);
        true
    }

    /// Whether an activation for `run` is in flight.
    pub(crate) fn is_active(&self, run: RunKey) -> bool {
        self.runs
            .get(&run)
            .is_some_and(|entry| entry.state == RunState::Active)
    }

    /// Whether any run waits for an activation.
    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Commands accepted and enqueued but not started.
    pub(crate) fn queued(&self) -> usize {
        self.queued
    }

    /// Claims held on the lane, queued or not.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        self.reserved
    }

    /// Activations in flight.
    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.active
    }

    /// Remove every queued item, for a lane whose dispatcher has gone: the
    /// items' reply senders drop with them, so their callers see the lane
    /// closed instead of waiting forever.
    pub(crate) fn drain_all(&mut self) -> Vec<T> {
        let mut items = Vec::with_capacity(self.queued);
        for entry in self.runs.values_mut() {
            entry.reserved -= entry.queue.len();
            self.reserved -= entry.queue.len();
            items.extend(entry.queue.drain(..));
        }
        self.queued = 0;
        self.ready.clear();
        for entry in self.runs.values_mut() {
            if entry.state == RunState::Ready {
                entry.state = RunState::Idle;
            }
        }
        self.runs.retain(|_, entry| !entry.is_unused());
        items
    }

    fn prune(&mut self, run: RunKey) {
        if self.runs.get(&run).is_some_and(RunEntry::is_unused) {
            self.runs.remove(&run);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};

    use proptest::prelude::*;
    use tokeira_types::RunKey;
    use uuid::Uuid;

    use super::{LaneRefusal, RunQueues};

    const RUNS: u8 = 4;

    #[derive(Clone, Debug)]
    enum Op {
        Reserve(u8),
        Release(u8),
        Enqueue(u8),
        PopReady,
        TakeNext(u8),
        Finish(u8),
        /// Pop a ready run, take up to `n` commands bounded by the drain limit,
        /// and finish it, as one activation does.
        Activate(u8),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (0..RUNS).prop_map(Op::Reserve),
            1 => (0..RUNS).prop_map(Op::Release),
            3 => (0..RUNS).prop_map(Op::Enqueue),
            1 => Just(Op::PopReady),
            2 => (0..RUNS).prop_map(Op::TakeNext),
            1 => (0..RUNS).prop_map(Op::Finish),
            2 => (0u8..8).prop_map(Op::Activate),
        ]
    }

    fn key(index: u8) -> RunKey {
        RunKey(Uuid::from_u128(u128::from(index) + 1))
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Idle,
        Ready,
        Active,
    }

    /// Per-run FIFOs and counters, written as plainly as possible.
    #[derive(Default)]
    struct Model {
        claims: HashMap<RunKey, usize>,
        queues: HashMap<RunKey, VecDeque<u64>>,
        states: HashMap<RunKey, State>,
        ready: VecDeque<RunKey>,
    }

    impl Model {
        fn claims(&self, run: RunKey) -> usize {
            self.claims.get(&run).copied().unwrap_or(0)
        }
        fn queued(&self, run: RunKey) -> usize {
            self.queues.get(&run).map_or(0, VecDeque::len)
        }
        fn state(&self, run: RunKey) -> State {
            self.states.get(&run).copied().unwrap_or(State::Idle)
        }
        fn total_claims(&self) -> usize {
            self.claims.values().sum()
        }
    }

    struct Harness {
        queues: RunQueues<u64>,
        model: Model,
        next_item: u64,
        budget: usize,
        drain: usize,
        last_taken: HashMap<RunKey, u64>,
    }

    impl Harness {
        fn new(budget: usize, drain: usize) -> Self {
            Self {
                queues: RunQueues::new(budget),
                model: Model::default(),
                next_item: 0,
                budget,
                drain,
                last_taken: HashMap::new(),
            }
        }

        fn take(&mut self, run: RunKey) -> Result<Option<u64>, TestCaseError> {
            let taken = self.queues.take_next(run);
            let expected = self.model.queues.entry(run).or_default().pop_front();
            prop_assert_eq!(taken, expected);
            if let Some(item) = taken {
                *self.model.claims.entry(run).or_default() -= 1;
                // Items are numbered in arrival order, so per-run FIFO means
                // each run's taken items increase.
                if let Some(previous) = self.last_taken.insert(run, item) {
                    prop_assert!(previous < item);
                }
            }
            Ok(taken)
        }

        fn finish(&mut self, run: RunKey) -> Result<(), TestCaseError> {
            let waiting_before: Vec<RunKey> = self.model.ready.iter().copied().collect();
            let requeued = self.queues.finish(run);
            let has_work = self.model.queued(run) > 0;
            prop_assert_eq!(requeued, has_work);
            if has_work {
                self.model.states.insert(run, State::Ready);
                self.model.ready.push_back(run);
                // Re-entry is behind every run that was already waiting.
                let after: Vec<RunKey> = self.model.ready.iter().copied().collect();
                prop_assert_eq!(&after[..waiting_before.len()], &waiting_before[..]);
            } else {
                self.model.states.insert(run, State::Idle);
            }
            Ok(())
        }

        fn pop(&mut self) -> Result<Option<RunKey>, TestCaseError> {
            let popped = self.queues.pop_ready();
            let expected = self.model.ready.pop_front();
            prop_assert_eq!(popped, expected);
            if let Some(run) = popped {
                self.model.states.insert(run, State::Active);
            }
            Ok(popped)
        }

        fn apply(&mut self, op: &Op) -> Result<(), TestCaseError> {
            match *op {
                Op::Reserve(index) => {
                    let run = key(index);
                    let expected = if self.model.claims(run) >= self.budget / 2 {
                        Err(LaneRefusal::RunShareFull)
                    } else if self.model.total_claims() >= self.budget {
                        Err(LaneRefusal::LaneBudgetFull)
                    } else {
                        Ok(())
                    };
                    prop_assert_eq!(self.queues.try_reserve(run), expected);
                    if expected.is_ok() {
                        *self.model.claims.entry(run).or_default() += 1;
                    }
                }
                Op::Release(index) => {
                    let run = key(index);
                    if self.model.claims(run) > self.model.queued(run) {
                        self.queues.release(run);
                        *self.model.claims.entry(run).or_default() -= 1;
                    }
                }
                Op::Enqueue(index) => {
                    let run = key(index);
                    if self.model.claims(run) > self.model.queued(run) {
                        let item = self.next_item;
                        self.next_item += 1;
                        let became_ready = self.queues.enqueue(run, item);
                        let idle = self.model.state(run) == State::Idle;
                        prop_assert_eq!(became_ready, idle);
                        self.model.queues.entry(run).or_default().push_back(item);
                        if idle {
                            self.model.states.insert(run, State::Ready);
                            self.model.ready.push_back(run);
                        }
                    }
                }
                Op::PopReady => {
                    self.pop()?;
                }
                Op::TakeNext(index) => {
                    let run = key(index);
                    if self.model.state(run) == State::Active {
                        self.take(run)?;
                    }
                }
                Op::Finish(index) => {
                    let run = key(index);
                    if self.model.state(run) == State::Active {
                        self.finish(run)?;
                    }
                }
                Op::Activate(wanted) => {
                    if let Some(run) = self.pop()? {
                        let mut taken = 0;
                        while taken < usize::from(wanted).min(self.drain) {
                            if self.take(run)?.is_none() {
                                break;
                            }
                            taken += 1;
                        }
                        prop_assert!(taken <= self.drain);
                        self.finish(run)?;
                    }
                }
            }
            Ok(())
        }

        fn check_accounting(&self) -> Result<(), TestCaseError> {
            prop_assert!(self.queues.reserved() <= self.budget);
            prop_assert_eq!(self.queues.reserved(), self.model.total_claims());
            let queued: usize = self.model.queues.values().map(VecDeque::len).sum();
            prop_assert_eq!(self.queues.queued(), queued);
            for index in 0..RUNS {
                let run = key(index);
                prop_assert!(self.model.claims(run) <= self.budget / 2);
                let entry_claims = self.queues.runs.get(&run).map_or(0, |entry| entry.reserved);
                prop_assert_eq!(entry_claims, self.model.claims(run));
                prop_assert_eq!(
                    self.queues.is_active(run),
                    self.model.state(run) == State::Active
                );
            }
            let active = self
                .model
                .states
                .values()
                .filter(|state| **state == State::Active)
                .count();
            prop_assert_eq!(self.queues.active(), active);
            prop_assert!(self.queues.runs.values().all(|entry| !entry.is_unused()));
            Ok(())
        }

        fn check_ready_order(&self) -> Result<(), TestCaseError> {
            let ready: Vec<RunKey> = self.queues.ready.iter().copied().collect();
            let expected: Vec<RunKey> = self.model.ready.iter().copied().collect();
            prop_assert_eq!(&ready, &expected);
            let distinct: HashSet<RunKey> = ready.iter().copied().collect();
            prop_assert_eq!(distinct.len(), ready.len());
            prop_assert!(ready.iter().all(|run| !self.queues.is_active(*run)));
            Ok(())
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: lane-run-concurrency, Property 5: Reservation accounting is exact and refusals don't wait
        // Claims never pass L or R, equal claims taken minus those released or
        // started, and each refusal names the bound that was hit.
        #[test]
        fn reservation_accounting_is_exact(
            budget in 2usize..12,
            ops in prop::collection::vec(op(), 0..200),
        ) {
            let mut harness = Harness::new(budget, 3);
            for op in &ops {
                harness.apply(op)?;
                harness.check_accounting()?;
            }
        }

        // Feature: lane-run-concurrency, Property 6: Ready order and the drain bound
        // A run is ready at most once and never while active, runs leave the
        // ready order in arrival order, an activation takes at most the drain
        // bound, and a finished run with work re-enters at the back.
        #[test]
        fn ready_order_and_drain_bound(
            budget in 2usize..12,
            drain in 1usize..4,
            ops in prop::collection::vec(op(), 0..200),
        ) {
            let mut harness = Harness::new(budget, drain);
            for op in &ops {
                harness.apply(op)?;
                harness.check_ready_order()?;
            }
        }
    }

    #[test]
    fn drain_all_returns_every_queued_item_and_its_claims() {
        let mut queues = RunQueues::new(8);
        let (a, b) = (key(0), key(1));
        for (run, item) in [(a, 1), (a, 2), (b, 3)] {
            queues.try_reserve(run).unwrap();
            queues.enqueue(run, item);
        }
        // A claim still being built survives the drain; its holder releases it.
        queues.try_reserve(b).unwrap();
        assert_eq!(queues.pop_ready(), Some(a));

        let mut drained = queues.drain_all();
        drained.sort_unstable();
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(queues.queued(), 0);
        assert_eq!(queues.reserved(), 1);
        assert!(!queues.has_ready());
        assert!(queues.is_active(a));
        assert!(!queues.finish(a));
        queues.release(b);
        assert!(queues.runs.is_empty());
    }
}
