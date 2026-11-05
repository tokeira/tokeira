//! What goes wrong in a run, and when.
//!
//! A [`Fault`] pairs an [`Action`] on turmoil's network, its hosts or one
//! node's store calls with a [`Trigger`]. [`topology::Run::drive`](crate::topology::Run::drive) checks
//! the triggers between simulation steps and applies each fault once, so a
//! fault lands at the same step on every run of one seed, and the trace
//! records it.
//!
//! A trigger on something the worker has seen beats one on a time: placement
//! and timing shift when the code changes, and a fault meant for "while the
//! activity is running" should still land there.

use std::{collections::BTreeMap, time::Duration};

use tokeira_storage::InMemoryStore;
use turmoil::Sim;

/// One fault: what happens, and when.
#[derive(Clone, Debug)]
pub struct Fault {
    trigger: Trigger,
    action: Action,
}

/// When a fault is applied.
#[derive(Clone, Debug)]
pub enum Trigger {
    /// At this much simulated time into the run.
    At(Duration),
    /// `delay` after the first step that contains `text` was recorded.
    Seen {
        /// Text a recorded step must contain.
        text: &'static str,
        /// How long after that step to wait.
        delay: Duration,
    },
}

/// What a fault does. Hosts are named as the topology names them.
#[derive(Clone, Debug)]
pub enum Action {
    /// Drop every message between two hosts, in both directions. A TCP
    /// segment dropped this way is lost, so an open connection between them
    /// stops making progress.
    Partition(&'static str, &'static str),
    /// Undo a [`Action::Partition`].
    Repair(&'static str, &'static str),
    /// Hold every message between two hosts, in both directions, until
    /// released. Nothing is lost: open connections resume where they were.
    Hold(&'static str, &'static str),
    /// Deliver what a [`Action::Hold`] held, and stop holding.
    Release(&'static str, &'static str),
    /// Stop a host. Whatever it kept in memory is gone.
    Crash(&'static str),
    /// Restart a host from scratch: a crash followed by a fresh start.
    Bounce(&'static str),
    /// Make a node's lease renewals wait, as if it couldn't reach the store,
    /// while the rest of its store calls and all of its network traffic go on.
    /// Its leases lapse unless it is resumed first.
    StallRenewals(&'static str),
    /// Let a node's stalled renewals go on, each as if it arrived now.
    ResumeRenewals(&'static str),
}

impl Fault {
    /// Apply `action` at `time` into the run.
    pub fn at(time: Duration, action: Action) -> Self {
        Self {
            trigger: Trigger::At(time),
            action,
        }
    }

    /// Apply `action` on the step after the first step containing `text` is
    /// recorded.
    pub fn when_seen(text: &'static str, action: Action) -> Self {
        Self::after_seen(text, Duration::ZERO, action)
    }

    /// Apply `action` `delay` after the first step containing `text` is
    /// recorded.
    pub fn after_seen(text: &'static str, delay: Duration, action: Action) -> Self {
        Self {
            trigger: Trigger::Seen { text, delay },
            action,
        }
    }
}

impl Action {
    fn apply(&self, sim: &mut Sim<'_>, node_stores: &BTreeMap<&'static str, InMemoryStore>) {
        let node_store = |host: &str| {
            node_stores
                .get(host)
                .unwrap_or_else(|| panic!("the topology has no store handle for {host}"))
        };
        match *self {
            Self::Partition(a, b) => sim.partition(a, b),
            Self::Repair(a, b) => sim.repair(a, b),
            Self::Hold(a, b) => sim.hold(a, b),
            Self::Release(a, b) => sim.release(a, b),
            Self::Crash(host) => sim.crash(host),
            Self::Bounce(host) => sim.bounce(host),
            Self::StallRenewals(host) => node_store(host).stall_lease_renewals(),
            Self::ResumeRenewals(host) => node_store(host).resume_lease_renewals(),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Partition(a, b) => format!("fault: partition {a} from {b}"),
            Self::Repair(a, b) => format!("fault: repair {a} to {b}"),
            Self::Hold(a, b) => format!("fault: hold traffic between {a} and {b}"),
            Self::Release(a, b) => format!("fault: release traffic between {a} and {b}"),
            Self::Crash(host) => format!("fault: crash {host}"),
            Self::Bounce(host) => format!("fault: bounce {host}"),
            Self::StallRenewals(host) => format!("fault: stall {host}'s lease renewals"),
            Self::ResumeRenewals(host) => format!("fault: resume {host}'s lease renewals"),
        }
    }
}

/// The faults of one run and how far each has got.
#[derive(Debug)]
pub(crate) struct Schedule {
    pending: Vec<Pending>,
    /// Steps before this index have been searched for `Seen` triggers.
    searched: usize,
}

#[derive(Debug)]
struct Pending {
    fault: Fault,
    /// When the trigger's text was first seen, for `Seen` triggers.
    seen_at: Option<Duration>,
    applied: bool,
}

impl Schedule {
    pub(crate) fn new(faults: impl IntoIterator<Item = Fault>) -> Self {
        Self {
            pending: faults
                .into_iter()
                .map(|fault| Pending {
                    fault,
                    seen_at: None,
                    applied: false,
                })
                .collect(),
            searched: 0,
        }
    }

    /// Note the `Seen` triggers that `steps`, the trace so far, satisfies
    /// at `now`. Only steps recorded since the last call can newly satisfy
    /// one, so each step is searched once.
    pub(crate) fn notice(&mut self, now: Duration, steps: &[String]) {
        let new_steps = &steps[self.searched.min(steps.len())..];
        for pending in &mut self.pending {
            if let Trigger::Seen { text, .. } = pending.fault.trigger
                && pending.seen_at.is_none()
                && new_steps.iter().any(|step| step.contains(text))
            {
                pending.seen_at = Some(now);
            }
        }
        self.searched = steps.len();
    }

    /// Apply every fault due at `now`, in schedule order, and return what
    /// each did, for the trace.
    ///
    /// The caller must not hold the trace's lock: crashing or bouncing a
    /// host runs host code, which records into the trace.
    pub(crate) fn apply_due(
        &mut self,
        sim: &mut Sim<'_>,
        node_stores: &BTreeMap<&'static str, InMemoryStore>,
        now: Duration,
    ) -> Vec<String> {
        let mut applied = Vec::new();
        for pending in self.pending.iter_mut().filter(|pending| !pending.applied) {
            let due_at = match pending.fault.trigger {
                Trigger::At(time) => Some(time),
                Trigger::Seen { delay, .. } => pending.seen_at.map(|seen| seen + delay),
            };
            if due_at.is_some_and(|due_at| now >= due_at) {
                pending.fault.action.apply(sim, node_stores);
                pending.applied = true;
                applied.push(pending.fault.action.describe());
            }
        }
        applied
    }
}
