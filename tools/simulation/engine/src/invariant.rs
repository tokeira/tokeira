//! Named-invariant registry and per-event check machinery.
//!
//! An invariant is a pure predicate over model state, returning `Some(reason)`
//! when its falsification condition holds. Safety invariants are evaluated after
//! every event (the falsifying-schedule discipline); liveness invariants are
//! evaluated only at a model-signalled quiescent point, since they describe
//! eventual behaviour that need not hold mid-flight. The registry is generic
//! over the model type so it carries no domain logic.

use std::collections::BTreeMap;

/// Whether an invariant must hold after every event (safety) or only once the
/// model is quiescent (liveness).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvariantClass {
    /// Must hold under every adversarial schedule, checked after each event.
    Safety,
    /// Holds under healthy/bounded-adversary runs, checked at quiescence.
    Liveness,
}

/// A named correctness property over model state `M`.
///
/// `check` is a pure function: it returns `None` when the property holds and
/// `Some(reason)` describing the violation otherwise. Keeping it a plain `fn`
/// pointer (not a closure) keeps invariants cheap to register and free of
/// captured state, mirroring how `placement-sim` writes its checks.
pub struct Invariant<M> {
    /// Short stable name, e.g. `"S2"` or `"L4"`.
    pub name: &'static str,
    /// Safety vs liveness — decides when the registry evaluates it.
    pub class: InvariantClass,
    /// Falsification predicate: `Some(reason)` means the invariant is violated.
    pub check: fn(&M) -> Option<String>,
}

/// A recorded invariant violation: which invariant failed and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// The violated invariant's name.
    pub invariant: &'static str,
    /// Human-readable falsification detail for the report.
    pub reason: String,
}

/// Per-invariant pass/fail accumulated across a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvariantOutcome {
    /// No violation observed for this invariant in the run.
    Pass,
    /// At least one violation observed.
    Fail,
}

/// Holds the registered invariants for a model type and evaluates them.
///
/// The registry is intentionally append-only and order-preserving so the first
/// registered safety violation reported is deterministic for a given model.
pub struct InvariantRegistry<M> {
    invariants: Vec<Invariant<M>>,
}

impl<M> Default for InvariantRegistry<M> {
    fn default() -> Self {
        Self {
            invariants: Vec::new(),
        }
    }
}

impl<M> InvariantRegistry<M> {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one invariant. Names are expected unique; duplicate names are
    /// allowed structurally but would report independently.
    pub fn register(&mut self, invariant: Invariant<M>) {
        self.invariants.push(invariant);
    }

    /// The names of every registered invariant, in registration order.
    pub fn names(&self) -> Vec<&'static str> {
        self.invariants.iter().map(|i| i.name).collect()
    }

    /// Evaluate all `Safety` invariants against `model`, returning the first
    /// violation found (registration order makes this deterministic). Called
    /// after every event.
    pub fn check_safety(&self, model: &M) -> Option<Violation> {
        self.check_class(model, InvariantClass::Safety)
    }

    /// Evaluate all `Liveness` invariants against `model`, returning the first
    /// violation found. Called at the quiescent point / run end.
    pub fn check_liveness(&self, model: &M) -> Option<Violation> {
        self.check_class(model, InvariantClass::Liveness)
    }

    fn check_class(&self, model: &M, class: InvariantClass) -> Option<Violation> {
        for inv in self.invariants.iter().filter(|i| i.class == class) {
            if let Some(reason) = (inv.check)(model) {
                return Some(Violation {
                    invariant: inv.name,
                    reason,
                });
            }
        }
        None
    }

    /// Seed a pass/fail map with every registered invariant set to `Pass`.
    ///
    /// The runner flips entries to `Fail` as violations occur. Pre-seeding means
    /// the report lists every invariant even when none failed.
    pub fn initial_outcomes(&self) -> BTreeMap<&'static str, InvariantOutcome> {
        self.invariants
            .iter()
            .map(|i| (i.name, InvariantOutcome::Pass))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Model {
        counter: i32,
    }

    fn s_non_negative(m: &Model) -> Option<String> {
        (m.counter < 0).then(|| format!("counter went negative: {}", m.counter))
    }

    fn l_settles_at_zero(m: &Model) -> Option<String> {
        (m.counter != 0).then(|| format!("counter not drained: {}", m.counter))
    }

    fn registry() -> InvariantRegistry<Model> {
        let mut r = InvariantRegistry::new();
        r.register(Invariant {
            name: "S1",
            class: InvariantClass::Safety,
            check: s_non_negative,
        });
        r.register(Invariant {
            name: "L1",
            class: InvariantClass::Liveness,
            check: l_settles_at_zero,
        });
        r
    }

    #[test]
    fn safety_check_detects_violation() {
        let r = registry();
        assert!(r.check_safety(&Model { counter: 1 }).is_none());
        let v = r.check_safety(&Model { counter: -2 }).unwrap();
        assert_eq!(v.invariant, "S1");
    }

    #[test]
    fn liveness_is_not_evaluated_by_safety_check() {
        let r = registry();
        // counter != 0 violates L1 but L1 is liveness, so check_safety ignores it.
        assert!(r.check_safety(&Model { counter: 5 }).is_none());
        assert_eq!(
            r.check_liveness(&Model { counter: 5 }).unwrap().invariant,
            "L1"
        );
        assert!(r.check_liveness(&Model { counter: 0 }).is_none());
    }

    #[test]
    fn initial_outcomes_lists_all_as_pass() {
        let r = registry();
        let outcomes = r.initial_outcomes();
        assert_eq!(outcomes.get("S1"), Some(&InvariantOutcome::Pass));
        assert_eq!(outcomes.get("L1"), Some(&InvariantOutcome::Pass));
    }
}
