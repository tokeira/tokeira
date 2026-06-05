//! Deterministic discrete-event queue and the context handed to models.
//!
//! Simulated time advances only by draining [`EventQueue`]; no wall clock is
//! ever read. Events are ordered by `(at_ms, seq)` where `seq` is a monotonic
//! insertion counter, so two events scheduled for the same simulated timestamp
//! always drain in a deterministic order. This `(time, seq)` total order is the
//! foundation of the reproducibility contract: identical seed + model + faults
//! produce an identical event sequence.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::rng::Rng;

/// One scheduled event: the model's event payload plus its dispatch ordering
/// key. `seq` breaks ties between events sharing `at_ms`.
#[derive(Clone, Debug)]
pub struct Scheduled<E> {
    /// Simulated dispatch time, in milliseconds from the start of the run.
    pub at_ms: u64,
    /// Monotonic insertion order, the deterministic tie-breaker for equal `at_ms`.
    pub seq: u64,
    /// The model-defined event to apply when this entry is drained.
    pub event: E,
}

// The queue is a max-heap, so we invert the comparison to pop the *earliest*
// `(at_ms, seq)` first. Equality/order are defined purely on the ordering key,
// never on the event payload (which need not be comparable).
impl<E> PartialEq for Scheduled<E> {
    fn eq(&self, other: &Self) -> bool {
        self.at_ms == other.at_ms && self.seq == other.seq
    }
}
impl<E> Eq for Scheduled<E> {}
impl<E> PartialOrd for Scheduled<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<E> Ord for Scheduled<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so the BinaryHeap (a max-heap) yields the smallest key first.
        other
            .at_ms
            .cmp(&self.at_ms)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// A deterministic min-ordered priority queue of scheduled events.
///
/// `next_seq` is the source of the tie-breaker and is never reset within a run,
/// guaranteeing a stable total order even when many events share a timestamp.
#[derive(Debug)]
pub struct EventQueue<E> {
    heap: BinaryHeap<Scheduled<E>>,
    next_seq: u64,
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }
}

impl<E> EventQueue<E> {
    /// Create an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue `event` to fire at absolute simulated time `at_ms`, assigning the
    /// next insertion `seq` as the tie-breaker.
    pub fn schedule_at(&mut self, at_ms: u64, event: E) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Scheduled { at_ms, seq, event });
    }

    /// Pop the earliest `(at_ms, seq)` entry, or `None` when the queue is empty.
    pub fn pop(&mut self) -> Option<Scheduled<E>> {
        self.heap.pop()
    }

    /// Number of events still queued.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// True when no events remain.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

/// The mutation surface handed to a model during `handle`.
///
/// It owns the current simulated time, the seeded [`Rng`], and the queue, so a
/// model can only advance the simulation through this context — it cannot read
/// wall-clock time or schedule events out of band. `schedule` enqueues relative
/// to `now_ms`, which is the natural way models express "in N ms from now".
pub struct SimCtx<'a, E> {
    now_ms: u64,
    rng: &'a mut Rng,
    queue: &'a mut EventQueue<E>,
}

impl<'a, E> SimCtx<'a, E> {
    /// Construct a context for the event currently being handled.
    pub fn new(now_ms: u64, rng: &'a mut Rng, queue: &'a mut EventQueue<E>) -> Self {
        Self { now_ms, rng, queue }
    }

    /// Current simulated timestamp (the `at_ms` of the event being handled).
    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// Mutable access to the seeded RNG. All model randomness MUST go through here.
    pub fn rng(&mut self) -> &mut Rng {
        self.rng
    }

    /// Schedule `event` to fire `delay_ms` after the current simulated time.
    ///
    /// `saturating_add` guards the (practically unreachable) overflow at the end
    /// of the `u64` time domain rather than panicking mid-run.
    pub fn schedule(&mut self, delay_ms: u64, event: E) {
        let at = self.now_ms.saturating_add(delay_ms);
        self.queue.schedule_at(at, event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drains_in_time_then_seq_order() {
        let mut q: EventQueue<&str> = EventQueue::new();
        q.schedule_at(10, "b");
        q.schedule_at(5, "a");
        q.schedule_at(10, "c"); // same time as "b", later seq

        let order: Vec<(u64, &str)> =
            std::iter::from_fn(|| q.pop().map(|s| (s.at_ms, s.event))).collect();
        assert_eq!(order, vec![(5, "a"), (10, "b"), (10, "c")]);
    }

    #[test]
    fn schedule_is_relative_to_now() {
        let mut rng = Rng::new(1);
        let mut q: EventQueue<u64> = EventQueue::new();
        {
            let mut ctx = SimCtx::new(100, &mut rng, &mut q);
            ctx.schedule(5, 999);
        }
        let s = q.pop().unwrap();
        assert_eq!(s.at_ms, 105);
        assert_eq!(s.event, 999);
    }

    #[test]
    fn equal_time_events_preserve_insertion_order_across_interleaving() {
        // Even when scheduled at different wall positions, equal at_ms ties
        // resolve by insertion seq, which is monotonic and never reset.
        let mut q: EventQueue<u32> = EventQueue::new();
        q.schedule_at(1, 1);
        q.schedule_at(2, 2);
        q.schedule_at(1, 3); // seq 2, same time as event 1 (seq 0)
        let order: Vec<u32> = std::iter::from_fn(|| q.pop().map(|s| s.event)).collect();
        assert_eq!(order, vec![1, 3, 2]);
    }
}
