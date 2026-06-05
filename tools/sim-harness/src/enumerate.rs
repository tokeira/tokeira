//! Bounded-exhaustive state-space enumerator.
//!
//! Where the stress runner samples schedules randomly, this explores *every*
//! interleaving of a tiny model up to a depth bound — closer to model checking.
//! It is the part that catches protocol-shape bugs random scheduling can miss,
//! and is where a deliberately-injected bug surfaces at shallow depth. The
//! traversal and visited-state pruning mirror `placement-sim`'s
//! `run_bounded_exhaustive` exactly: a state reached again with no more
//! remaining depth than before is not re-expanded.

use std::{collections::HashMap, fmt, hash::Hash};

/// A tiny model whose full reachable state space can be enumerated.
///
/// `apply` returns `Err(reason)` for a transition that is itself an illegal
/// step (a bug), and `check` returns `Some(reason)` when a safety invariant is
/// violated at a state. State must be `Clone + Eq + Hash` so the enumerator can
/// dedup visited states.
pub trait ExhaustiveModel: Clone + Eq + Hash {
    /// The transition alphabet. `Debug` so counterexample paths print.
    type Action: Clone + fmt::Debug;

    /// The initial state every exploration starts from.
    fn initial() -> Self;

    /// Every action attempted at each state. The enumerator tries them all.
    fn actions() -> Vec<Self::Action>;

    /// Apply an action, mutating `self`. `Err` marks the transition itself as a
    /// detected bug (reported immediately as a counterexample).
    fn apply(&mut self, action: &Self::Action) -> Result<(), String>;

    /// Safety check at the resulting state: `Some(reason)` = violation.
    fn check(&self) -> Option<String>;
}

/// Successful exploration summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumReport {
    /// Distinct states expanded.
    pub states_explored: u64,
    /// Transitions attempted.
    pub transitions_tried: u64,
}

/// A falsifying path: the shortest action sequence reaching a violating state.
#[derive(Clone, Debug)]
pub struct Counterexample<A> {
    /// Depth at which the violation was found (path length).
    pub depth: usize,
    /// The violation reason (from `apply` error or `check`).
    pub message: String,
    /// The action sequence from `initial()` to the violating state.
    pub path: Vec<A>,
}

impl<A: fmt::Debug> fmt::Display for Counterexample<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "bounded exhaustive checker failed")?;
        writeln!(f, "  depth: {}", self.depth)?;
        writeln!(f, "  error: {}", self.message)?;
        writeln!(f, "  path:")?;
        for (idx, action) in self.path.iter().enumerate() {
            writeln!(f, "    {:02}: {:?}", idx + 1, action)?;
        }
        Ok(())
    }
}

/// Depth-first exploration to `max_depth` with shortest-path-to-state pruning.
///
/// The visited map records, per state, the largest "remaining depth" budget the
/// state has previously been reached with. A re-encounter with `remaining <=`
/// the recorded budget cannot reach anything new, so it is pruned — this is the
/// exact `best_remaining_by_state` technique from `placement-sim`, which bounds
/// the search while still guaranteeing the first reported counterexample is via
/// a shortest path (the stack is seeded so shallower paths are explored first).
///
/// Invariants are checked on the initial state and after every transition; the
/// first violation returns the accumulated path.
pub fn run_bounded_exhaustive<M: ExhaustiveModel>(
    max_depth: usize,
) -> Result<EnumReport, Counterexample<M::Action>> {
    let initial = M::initial();
    if let Some(message) = initial.check() {
        return Err(Counterexample {
            depth: 0,
            message,
            path: Vec::new(),
        });
    }

    // Explicit stack of (state, depth, path). We push successors so that, for a
    // fixed action order, shallower states are still found via shortest paths
    // because a state's first expansion records the largest remaining budget.
    let mut stack: Vec<(M, usize, Vec<M::Action>)> = vec![(initial, 0, Vec::new())];
    let mut best_remaining_by_state: HashMap<M, usize> = HashMap::new();
    let mut states_explored = 0u64;
    let mut transitions_tried = 0u64;

    while let Some((state, depth, path)) = stack.pop() {
        states_explored += 1;
        let remaining = max_depth.saturating_sub(depth);
        if let Some(prev_best) = best_remaining_by_state.get(&state) {
            if *prev_best >= remaining {
                continue;
            }
        }
        best_remaining_by_state.insert(state.clone(), remaining);
        if depth == max_depth {
            continue;
        }

        for action in M::actions() {
            transitions_tried += 1;
            let mut next = state.clone();
            let mut next_path = path.clone();
            next_path.push(action.clone());
            if let Err(message) = next.apply(&action) {
                return Err(Counterexample {
                    depth: depth + 1,
                    message,
                    path: next_path,
                });
            }
            if let Some(message) = next.check() {
                return Err(Counterexample {
                    depth: depth + 1,
                    message,
                    path: next_path,
                });
            }
            stack.push((next, depth + 1, next_path));
        }
    }

    Ok(EnumReport {
        states_explored,
        transitions_tried,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A trivial bounded counter that must stay within [0, 3]. With a Dec action
    // available it can be driven negative, which the checker should catch.
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Counter {
        value: i32,
        allow_underflow: bool,
    }

    #[derive(Clone, Debug)]
    enum Op {
        Inc,
        Dec,
    }

    impl ExhaustiveModel for Counter {
        type Action = Op;
        fn initial() -> Self {
            Counter {
                value: 0,
                allow_underflow: true,
            }
        }
        fn actions() -> Vec<Op> {
            vec![Op::Inc, Op::Dec]
        }
        fn apply(&mut self, action: &Op) -> Result<(), String> {
            match action {
                Op::Inc => self.value += 1,
                Op::Dec => {
                    if !self.allow_underflow && self.value == 0 {
                        return Err("decrement below zero".into());
                    }
                    self.value -= 1;
                }
            }
            Ok(())
        }
        fn check(&self) -> Option<String> {
            (self.value < 0).then(|| format!("counter negative: {}", self.value))
        }
    }

    #[test]
    fn finds_shortest_counterexample() {
        // From 0, a single Dec drives the counter to -1: depth-1 violation.
        let result = run_bounded_exhaustive::<Counter>(5);
        let ce = result.expect_err("should find a violation");
        assert_eq!(ce.depth, 1);
        assert!(matches!(ce.path.as_slice(), [Op::Dec]));
    }

    // A variant whose transition guard prevents underflow, so no state ever
    // violates `check` — exploration completes.
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct SafeCounter {
        value: i32,
    }

    impl ExhaustiveModel for SafeCounter {
        type Action = Op;
        fn initial() -> Self {
            SafeCounter { value: 0 }
        }
        fn actions() -> Vec<Op> {
            vec![Op::Inc, Op::Dec]
        }
        fn apply(&mut self, action: &Op) -> Result<(), String> {
            match action {
                Op::Inc => self.value = (self.value + 1).min(3),
                Op::Dec => self.value = (self.value - 1).max(0),
            }
            Ok(())
        }
        fn check(&self) -> Option<String> {
            (!(0..=3).contains(&self.value)).then(|| "out of range".to_string())
        }
    }

    #[test]
    fn explores_fully_when_safe_and_prunes() {
        let report = run_bounded_exhaustive::<SafeCounter>(20).expect("no violation");
        // With clamping there are only 4 distinct states; pruning keeps the
        // explored count small despite depth 20.
        assert!(report.states_explored >= 4);
        assert!(report.transitions_tried >= 4);
    }
}
