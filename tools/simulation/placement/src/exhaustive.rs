//! Bounded-exhaustive checker for the placement safety kernel.
//!
//! Where the seeded stress model samples schedules randomly, this enumerates
//! *every* short interleaving of a tiny two-bundle / two-runtime / one-workflow
//! model — closer to model checking. It is where a protocol-shape bug (notably
//! routing `Start` by queue-home instead of execution-home) surfaces at shallow
//! depth with a shortest-path counterexample. The safety kernel it explores:
//! lease acquire/expire/relinquish, stale edge observation, stale local
//! ownership, Start/Signal routing with the OCC fence, request dedupe, and
//! runtime crash.
//!
//! The model implements the engine's [`ExhaustiveModel`]; `run_with_bug` selects
//! between the correct model and the buggy variant via the [`buggy_model!`]
//! macro, exactly as the broker simulator does, so the bug is threaded into both
//! verification modes from a single [`InjectedBug`] source.

use sim_engine::ExhaustiveModel;

use crate::bug::InjectedBug;

/// Bundles in the mini topology. Index 1 is the workflow's execution-home;
/// index 0 is its (distinct) queue-home — see [`MINI_EXECUTION_HOME`] /
/// [`MINI_QUEUE_HOME`].
const MINI_BUNDLES: usize = 2;
/// Runtimes in the mini topology.
const MINI_RUNTIMES: usize = 2;
/// The execution-home bundle for the single mini workflow — the correctness
/// boundary every Start/Signal must resolve to.
const MINI_EXECUTION_HOME: u8 = 1;
/// The advisory queue-home bundle — deliberately different from the
/// execution-home, so routing `Start` here is observably wrong.
const MINI_QUEUE_HOME: u8 = 0;

/// A mini lease row: owner, fencing epoch, and whether it is currently active.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MiniLease {
    owner: Option<u8>,
    epoch: u8,
    active: bool,
}

/// A mini advisory edge route to a believed owner at a believed epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MiniRoute {
    owner: u8,
    epoch: u8,
}

/// The full enumerable state of the mini model. Everything is small and `Hash`
/// so the engine can dedup visited states and prune the search.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MiniState {
    leases: [MiniLease; MINI_BUNDLES],
    local_owned: [[Option<u8>; MINI_BUNDLES]; MINI_RUNTIMES],
    edge_routes: [Option<MiniRoute>; MINI_BUNDLES],
    runtime_alive: [bool; MINI_RUNTIMES],
    /// The committed workflow's home bundle, once started.
    workflow_home: Option<u8>,
    start_request_applied: bool,
    signal_request_applied: bool,
    signal_count: u8,
    /// Whether `Start` routes by queue-home (the injected bug) vs execution-home.
    buggy_start_routing: bool,
}

impl MiniState {
    fn new(buggy_start_routing: bool) -> Self {
        Self {
            leases: [MiniLease {
                owner: None,
                epoch: 0,
                active: false,
            }; MINI_BUNDLES],
            local_owned: [[None; MINI_BUNDLES]; MINI_RUNTIMES],
            edge_routes: [None; MINI_BUNDLES],
            runtime_alive: [true; MINI_RUNTIMES],
            workflow_home: None,
            start_request_applied: false,
            signal_request_applied: false,
            signal_count: 0,
            buggy_start_routing,
        }
    }

    /// The currently-valid route for a bundle (owner + epoch iff active).
    fn current_route(&self, bundle: u8) -> Option<MiniRoute> {
        let lease = self.leases[bundle as usize];
        match (lease.owner, lease.active) {
            (Some(owner), true) => Some(MiniRoute {
                owner,
                epoch: lease.epoch,
            }),
            _ => None,
        }
    }

    /// Point-repair the edge route for a bundle from live lease state, never
    /// regressing to an older epoch (mirrors the full model's monotone repair).
    fn repair_edge_route(&mut self, bundle: u8) {
        let current = self.current_route(bundle);
        let slot = &mut self.edge_routes[bundle as usize];
        match (*slot, current) {
            (Some(old), Some(new)) if old.epoch > new.epoch => {}
            (_, new_route) => *slot = new_route,
        }
    }

    /// Resolve and (if the fence passes) apply one edge operation.
    ///
    /// Returns `Err` only for the buggy-routing case where `Start` resolved to
    /// queue-home rather than execution-home — the protocol-shape bug the
    /// checker exists to catch. All other paths (stale route, dead owner, fence
    /// miss, dedupe) are legitimate no-ops/repairs, exactly as the full model.
    fn edge_operation(&mut self, is_start: bool) -> Result<(), String> {
        let bundle = if is_start && self.buggy_start_routing {
            MINI_QUEUE_HOME
        } else {
            MINI_EXECUTION_HOME
        };
        if is_start && bundle != MINI_EXECUTION_HOME {
            return Err(format!(
                "StartWorkflow resolved to queue-home bundle {bundle} instead of execution-home bundle {MINI_EXECUTION_HOME}"
            ));
        }
        let Some(route) = self.edge_routes[bundle as usize] else {
            self.repair_edge_route(bundle);
            return Ok(());
        };
        let runtime = route.owner as usize;
        let bundle_idx = bundle as usize;
        if runtime >= MINI_RUNTIMES || !self.runtime_alive[runtime] {
            self.repair_edge_route(bundle);
            return Ok(());
        }
        // Local-ownership belief must match the routed epoch (NotShardOwner else).
        if self.local_owned[runtime][bundle_idx] != Some(route.epoch) {
            self.repair_edge_route(bundle);
            return Ok(());
        }
        // The DSQL fence: owner + epoch + active must all line up at commit.
        let lease = self.leases[bundle_idx];
        let fence_ok =
            lease.active && lease.owner == Some(route.owner) && lease.epoch == route.epoch;
        if !fence_ok {
            self.local_owned[runtime][bundle_idx] = None;
            self.repair_edge_route(bundle);
            return Ok(());
        }
        if is_start {
            if self.start_request_applied {
                return Ok(());
            }
            if self.workflow_home.is_none() {
                self.workflow_home = Some(bundle);
                self.start_request_applied = true;
            }
        } else {
            if self.signal_request_applied {
                return Ok(());
            }
            match self.workflow_home {
                Some(home) if home == bundle => {
                    self.signal_request_applied = true;
                    self.signal_count = self.signal_count.saturating_add(1);
                }
                Some(home) => {
                    return Err(format!(
                        "SignalWorkflow routed to bundle {bundle} but workflow home is {home}"
                    ));
                }
                None => {}
            }
        }
        Ok(())
    }
}

/// The mini transition alphabet over the safety kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MiniAction {
    /// Edge observes a bundle's live route.
    ObserveBundle(u8),
    /// A runtime acquires a free/expired bundle lease.
    Acquire { runtime: u8, bundle: u8 },
    /// A bundle's lease expires.
    ExpireBundle(u8),
    /// A runtime relinquishes a lease it holds.
    Relinquish { runtime: u8, bundle: u8 },
    /// A runtime crashes (drops all local ownership).
    CrashRuntime(u8),
    /// A client start operation.
    StartWorkflow,
    /// A client signal operation.
    SignalWorkflow,
}

/// The correct (bug-free) mini model.
///
/// Implements [`ExhaustiveModel`] for both the correct (`BUGGY = false`) and
/// buggy (`BUGGY = true`) variants from a single body; the const generic selects
/// the routing behaviour in `initial`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MiniModel<const BUGGY: bool> {
    state: MiniState,
}

impl<const BUGGY: bool> ExhaustiveModel for MiniModel<BUGGY> {
    type Action = MiniAction;

    fn initial() -> Self {
        Self {
            state: MiniState::new(BUGGY),
        }
    }

    fn actions() -> Vec<MiniAction> {
        let mut actions = Vec::new();
        for bundle in 0..MINI_BUNDLES as u8 {
            actions.push(MiniAction::ObserveBundle(bundle));
            actions.push(MiniAction::ExpireBundle(bundle));
            for runtime in 0..MINI_RUNTIMES as u8 {
                actions.push(MiniAction::Acquire { runtime, bundle });
                actions.push(MiniAction::Relinquish { runtime, bundle });
            }
        }
        for runtime in 0..MINI_RUNTIMES as u8 {
            actions.push(MiniAction::CrashRuntime(runtime));
        }
        actions.push(MiniAction::StartWorkflow);
        actions.push(MiniAction::SignalWorkflow);
        actions
    }

    fn apply(&mut self, action: &MiniAction) -> Result<(), String> {
        let state = &mut self.state;
        match *action {
            MiniAction::ObserveBundle(bundle) => state.repair_edge_route(bundle),
            MiniAction::Acquire { runtime, bundle } => {
                if !state.runtime_alive[runtime as usize] {
                    return Ok(());
                }
                let lease = &mut state.leases[bundle as usize];
                if lease.owner.is_none() || !lease.active {
                    lease.owner = Some(runtime);
                    lease.epoch = lease.epoch.saturating_add(1);
                    lease.active = true;
                    state.local_owned[runtime as usize][bundle as usize] = Some(lease.epoch);
                }
            }
            MiniAction::ExpireBundle(bundle) => {
                state.leases[bundle as usize].active = false;
            }
            MiniAction::Relinquish { runtime, bundle } => {
                let local_epoch = state.local_owned[runtime as usize][bundle as usize];
                let lease = &mut state.leases[bundle as usize];
                if state.runtime_alive[runtime as usize]
                    && lease.active
                    && lease.owner == Some(runtime)
                    && local_epoch == Some(lease.epoch)
                {
                    lease.owner = None;
                    lease.epoch = lease.epoch.saturating_add(1);
                    lease.active = false;
                    state.local_owned[runtime as usize][bundle as usize] = None;
                }
            }
            MiniAction::CrashRuntime(runtime) => {
                state.runtime_alive[runtime as usize] = false;
                state.local_owned[runtime as usize] = [None; MINI_BUNDLES];
            }
            MiniAction::StartWorkflow => state.edge_operation(true)?,
            MiniAction::SignalWorkflow => state.edge_operation(false)?,
        }
        Ok(())
    }

    fn check(&self) -> Option<String> {
        let state = &self.state;
        // I1: the workflow, once committed, lives on its execution-home.
        if let Some(home) = state.workflow_home {
            if home != MINI_EXECUTION_HOME {
                return Some(format!(
                    "I1: workflow committed on bundle {home}, expected execution-home {MINI_EXECUTION_HOME}"
                ));
            }
        }
        // I2: a signal request applies at most once.
        if state.signal_count > 1 {
            return Some("I2: signal request applied more than once".to_string());
        }
        // I4: the edge never holds a future epoch.
        for bundle in 0..MINI_BUNDLES {
            if let Some(route) = state.edge_routes[bundle] {
                let lease = state.leases[bundle];
                if route.epoch > lease.epoch {
                    return Some(format!(
                        "I4: edge has future epoch {} for bundle {bundle}, DSQL epoch is {}",
                        route.epoch, lease.epoch
                    ));
                }
            }
        }
        None
    }
}

/// Run the bounded-exhaustive checker, selecting the correct or buggy model from
/// the optional [`InjectedBug`]. The buggy variant is expected to fail; a clean
/// run is expected to pass.
pub fn run_with_bug(
    bug: Option<InjectedBug>,
    max_depth: usize,
) -> Result<sim_engine::EnumReport, sim_engine::Counterexample<MiniAction>> {
    match bug {
        Some(InjectedBug::BuggyStartRouting) => {
            sim_engine::run_bounded_exhaustive::<MiniModel<true>>(max_depth)
        }
        None => sim_engine::run_bounded_exhaustive::<MiniModel<false>>(max_depth),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_model_passes() {
        // The correct safety kernel has no reachable violation within the bound.
        assert!(run_with_bug(None, 12).is_ok());
    }

    #[test]
    fn buggy_routing_is_caught() {
        // Routing Start by queue-home must produce a shortest-path counterexample.
        let ce = run_with_bug(Some(InjectedBug::BuggyStartRouting), 12)
            .expect_err("buggy routing must be falsified");
        assert!(ce.message.contains("queue-home") || ce.message.contains("I1"));
    }
}
