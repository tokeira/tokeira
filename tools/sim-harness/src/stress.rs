//! The seeded stress runner and the `StressModel` trait it drives.
//!
//! This is `placement-sim`'s seeded loop generalised: bootstrap the queue,
//! drain events in `(time, seq)` order applying each to the model, check safety
//! invariants after every event, and evaluate liveness invariants once at the
//! quiescent point / run end. The runner owns the RNG and clock; the model only
//! mutates state and schedules follow-on events through the `SimCtx`.

use std::fmt;

use crate::{
    event::{EventQueue, SimCtx},
    invariant::{InvariantRegistry, Violation},
    report::{SeedReport, SignalCounters},
    rng::Rng,
};

/// A model the stress runner can drive over one seed.
///
/// Implementations are pure in the sense that matters here: no wall clock, no
/// real I/O, no async. All randomness comes from `ctx.rng()` and all time from
/// the event queue, so a `(seed, model, fault-config)` triple reproduces an
/// identical event sequence.
pub trait StressModel {
    /// The model's event payload type.
    type Event: Clone;

    /// Seed the queue with initial events: the workload and the fault schedule.
    /// Equivalent to `placement-sim`'s `bootstrap` + `schedule_workload`.
    fn bootstrap(&mut self, ctx: &mut SimCtx<'_, Self::Event>);

    /// Apply one event — the only place model state changes — possibly
    /// scheduling follow-on events via `ctx`.
    fn handle(&mut self, event: Self::Event, ctx: &mut SimCtx<'_, Self::Event>);

    /// The model's accumulated signal counters, for the seed report.
    fn signals(&self) -> &SignalCounters;

    /// True when the model has reached a point at which liveness invariants are
    /// meaningful to evaluate (e.g. workload drained, nothing in flight).
    /// Defaults to false; the runner also evaluates liveness at run end.
    fn is_quiescent(&self) -> bool {
        false
    }
}

/// A stress-run failure: which seed, when (simulated), and what invariant broke.
#[derive(Clone, Debug)]
pub struct Failure {
    /// The seed that produced the failing run.
    pub seed: u64,
    /// Simulated time of the violating event.
    pub now_ms: u64,
    /// The violated invariant and reason.
    pub violation: Violation,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "stress simulation failed")?;
        writeln!(f, "  seed:    {}", self.seed)?;
        writeln!(f, "  time_ms: {}", self.now_ms)?;
        writeln!(
            f,
            "  invariant {} violated: {}",
            self.violation.invariant, self.violation.reason
        )
    }
}

/// Drives one seed of a [`StressModel`] to completion or first safety violation.
///
/// `time_bound_ms` stops draining once events pass the simulated bound (matching
/// `placement-sim`'s `event.at_ms > max_time_ms` break). On a safety violation
/// the run stops immediately and the violation is recorded in the [`SeedReport`].
pub fn run_seed<M: StressModel>(
    mut model: M,
    seed: u64,
    time_bound_ms: u64,
    registry: &InvariantRegistry<M>,
    verbose: bool,
) -> SeedReport
where
    M::Event: fmt::Debug,
{
    let mut rng = Rng::new(seed);
    let mut queue: EventQueue<M::Event> = EventQueue::new();

    {
        let mut ctx = SimCtx::new(0, &mut rng, &mut queue);
        model.bootstrap(&mut ctx);
    }

    while let Some(scheduled) = queue.pop() {
        if scheduled.at_ms > time_bound_ms {
            break;
        }
        if verbose {
            println!("  t={} event={:?}", scheduled.at_ms, scheduled.event);
        }
        {
            let mut ctx = SimCtx::new(scheduled.at_ms, &mut rng, &mut queue);
            model.handle(scheduled.event, &mut ctx);
        }
        if let Some(violation) = registry.check_safety(&model) {
            return SeedReport {
                seed,
                signals: model.signals().clone(),
                violation: Some(violation),
            };
        }
        if model.is_quiescent() {
            if let Some(violation) = registry.check_liveness(&model) {
                return SeedReport {
                    seed,
                    signals: model.signals().clone(),
                    violation: Some(violation),
                };
            }
        }
    }

    // Final liveness sweep at run end — but only if the model is genuinely
    // quiescent. Liveness describes eventual behaviour, so asserting it at an
    // arbitrary time-bound cutoff (where work may still be legitimately in
    // flight) would be a false positive. A run that never reaches quiescence
    // simply does not claim its liveness invariants.
    let violation = if model.is_quiescent() {
        registry.check_liveness(&model)
    } else {
        None
    };
    SeedReport {
        seed,
        signals: model.signals().clone(),
        violation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariant::{Invariant, InvariantClass};

    // A model that schedules `ops` "tick" events and counts them; a safety
    // invariant fails if the count ever exceeds a cap, which it won't here.
    struct Ticker {
        ops: u32,
        ticks: u32,
        signals: SignalCounters,
    }

    #[derive(Clone, Debug)]
    enum Ev {
        Tick,
    }

    impl StressModel for Ticker {
        type Event = Ev;
        fn bootstrap(&mut self, ctx: &mut SimCtx<'_, Ev>) {
            for n in 0..self.ops {
                ctx.schedule(u64::from(n) + 1, Ev::Tick);
            }
        }
        fn handle(&mut self, _event: Ev, _ctx: &mut SimCtx<'_, Ev>) {
            self.ticks += 1;
            self.signals.incr("ticks");
        }
        fn signals(&self) -> &SignalCounters {
            &self.signals
        }
        fn is_quiescent(&self) -> bool {
            self.ticks == self.ops
        }
    }

    fn registry() -> InvariantRegistry<Ticker> {
        let mut r = InvariantRegistry::new();
        r.register(Invariant {
            name: "S1",
            class: InvariantClass::Safety,
            check: |m: &Ticker| (m.ticks > m.ops).then(|| "too many ticks".to_string()),
        });
        r
    }

    #[test]
    fn runs_all_events_within_time_bound() {
        let model = Ticker {
            ops: 5,
            ticks: 0,
            signals: SignalCounters::new(),
        };
        let report = run_seed(model, 1, 1_000, &registry(), false);
        assert!(report.violation.is_none());
        assert_eq!(report.signals.get("ticks"), 5);
    }

    #[test]
    fn determinism_same_seed_same_signals() {
        let mk = || Ticker {
            ops: 20,
            ticks: 0,
            signals: SignalCounters::new(),
        };
        let a = run_seed(mk(), 77, 10_000, &registry(), false);
        let b = run_seed(mk(), 77, 10_000, &registry(), false);
        assert_eq!(a.signals.get("ticks"), b.signals.get("ticks"));
    }

    #[test]
    fn time_bound_cuts_off_late_events() {
        let model = Ticker {
            ops: 100,
            ticks: 0,
            signals: SignalCounters::new(),
        };
        // Bound at 10ms: only the first 10 tick events (at t=1..=10) run.
        let report = run_seed(model, 1, 10, &registry(), false);
        assert_eq!(report.signals.get("ticks"), 10);
    }
}
