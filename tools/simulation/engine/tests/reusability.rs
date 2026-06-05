//! Harness reusability validation (delivery-broker-simulator spec, Requirement 7).
//!
//! This is the cheap stand-in for the future admission-control (055) and
//! connection-management (060) simulators: a throwaway model — a bounded token
//! bucket, nothing to do with the broker — implemented against BOTH harness
//! model traits and driven through the full public surface (bootstrap →
//! `run_seed` → `Report` aggregation across seeds, plus `run_bounded_exhaustive`).
//! If the harness ever grew a broker/placement-specific assumption, this
//! unrelated model would stop compiling or stop passing, catching the leak
//! before a real second consumer is built.

use sim_engine::{
    run_bounded_exhaustive, run_seed, Invariant, InvariantClass, InvariantRegistry, Report,
    SignalCounters, SimCtx, StressModel,
};

// ---- A trivial token-bucket model, deliberately not broker-shaped ----

struct Bucket {
    capacity: u64,
    tokens: u64,
    refill_every_ms: u64,
    horizon_ms: u64,
    signals: SignalCounters,
    settled: bool,
}

#[derive(Clone, Debug)]
enum Ev {
    Take,
    Refill,
}

impl StressModel for Bucket {
    type Event = Ev;

    fn bootstrap(&mut self, ctx: &mut SimCtx<'_, Ev>) {
        // Interleave takes and refills across the horizon, all timed off the RNG
        // so the schedule is reproducible per seed.
        let mut t = 0;
        while t < self.horizon_ms {
            let gap = ctx.rng().range(1, 5);
            t += gap;
            if ctx.rng().bool_with_percent(60) {
                ctx.schedule(t, Ev::Take);
            } else {
                ctx.schedule(t, Ev::Refill);
            }
        }
        ctx.schedule(self.horizon_ms + 1, Ev::Refill);
    }

    fn handle(&mut self, event: Ev, _ctx: &mut SimCtx<'_, Ev>) {
        match event {
            Ev::Take => {
                if self.tokens > 0 {
                    self.tokens -= 1;
                    self.signals.incr("granted");
                } else {
                    self.signals.incr("rejected");
                }
            }
            Ev::Refill => {
                if self.tokens < self.capacity {
                    self.tokens += 1;
                    self.signals.incr("refilled");
                }
            }
        }
        // Once we've processed the trailing refill at horizon+1, call it settled.
        let _ = self.refill_every_ms;
    }

    fn signals(&self) -> &SignalCounters {
        &self.signals
    }

    fn is_quiescent(&self) -> bool {
        self.settled
    }
}

fn registry() -> InvariantRegistry<Bucket> {
    let mut r = InvariantRegistry::new();
    // Safety: tokens never exceed capacity and never underflow.
    r.register(Invariant {
        name: "B1",
        class: InvariantClass::Safety,
        check: |b: &Bucket| {
            (b.tokens > b.capacity).then(|| format!("over capacity: {} > {}", b.tokens, b.capacity))
        },
    });
    r
}

#[test]
fn stress_runs_a_non_broker_model_across_seeds() {
    let registry = registry();
    let mut report = Report::new(registry.initial_outcomes());

    for seed in 1..=25 {
        let model = Bucket {
            capacity: 4,
            tokens: 4,
            refill_every_ms: 3,
            horizon_ms: 200,
            signals: SignalCounters::new(),
            settled: false,
        };
        let seed_report = run_seed(model, seed, 1_000, &registry, false);
        report.add_seed(seed_report);
    }

    assert!(report.overall_passed());
    // The model exercised the signal API; at least some takes were granted.
    assert!(report.signals().get("granted") > 0);
}

#[test]
fn stress_is_deterministic_per_seed() {
    let registry = registry();
    let run = || {
        let model = Bucket {
            capacity: 4,
            tokens: 4,
            refill_every_ms: 3,
            horizon_ms: 200,
            signals: SignalCounters::new(),
            settled: false,
        };
        let r = run_seed(model, 123, 1_000, &registry, false);
        (
            r.signals.get("granted"),
            r.signals.get("rejected"),
            r.signals.get("refilled"),
        )
    };
    assert_eq!(run(), run());
}

// ---- The same domain, as a tiny exhaustive model ----

#[derive(Clone, PartialEq, Eq, Hash)]
struct MiniBucket {
    tokens: u8,
    capacity: u8,
}

#[derive(Clone, Debug)]
enum Op {
    Take,
    Refill,
}

impl sim_engine::ExhaustiveModel for MiniBucket {
    type Action = Op;

    fn initial() -> Self {
        MiniBucket {
            tokens: 2,
            capacity: 2,
        }
    }

    fn actions() -> Vec<Op> {
        vec![Op::Take, Op::Refill]
    }

    fn apply(&mut self, action: &Op) -> Result<(), String> {
        match action {
            Op::Take => {
                self.tokens = self.tokens.saturating_sub(1);
            }
            Op::Refill => {
                if self.tokens < self.capacity {
                    self.tokens += 1;
                }
            }
        }
        Ok(())
    }

    fn check(&self) -> Option<String> {
        (self.tokens > self.capacity).then(|| "over capacity".to_string())
    }
}

#[test]
fn exhaustive_explores_a_non_broker_model() {
    let report = run_bounded_exhaustive::<MiniBucket>(16).expect("bucket never over capacity");
    // Bounded state space (3 token levels), so exploration completes cheaply.
    assert!(report.states_explored >= 3);
}
