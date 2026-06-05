//! The broker safety (S1–S7) and liveness (L1–L4) invariants.
//!
//! Each is a pure predicate over [`BrokerModel`] state returning `Some(reason)`
//! on violation. Safety invariants are checked after every event; liveness at
//! the quiescent point. The falsification conditions match design.md Properties
//! 1–11.

use sim_harness::{Invariant, InvariantClass, InvariantRegistry};

use crate::model_machine::BrokerModel;

/// Build the registry with all eleven broker invariants.
pub fn registry() -> InvariantRegistry<BrokerModel> {
    let mut r = InvariantRegistry::new();

    // ---- Safety ----

    // S1 — at most one in-flight workflow task per run.
    r.register(Invariant {
        name: "S1",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            for (id, count) in &m.auth.live_deliveries {
                if id.is_wft() && *count > 1 {
                    return Some(format!("run {} has {} in-flight WFTs", id.run(), count));
                }
            }
            None
        },
    });

    // S2 — no double-start: at most one live delivery per logical task.
    r.register(Invariant {
        name: "S2",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            for (id, count) in &m.auth.live_deliveries {
                if *count > 1 {
                    return Some(format!("{id:?} started {count} times concurrently"));
                }
            }
            None
        },
    });

    // S3 — reservation⇄commit coupling: a held token implies a committed start.
    r.register(Invariant {
        name: "S3",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            for (id, delivery) in &m.broker.inflight {
                if !delivery.committed {
                    return Some(format!(
                        "{id:?} holds a token (delivery {}) with no committed start transaction",
                        delivery.delivery_id
                    ));
                }
            }
            None
        },
    });

    // S4 — stale completion rejection: a completed task is never also in flight
    // under the same id without a fresh delivery (modelled: a completed id must
    // not be marked completed twice, and a stale completion must not have
    // mutated authoritative state). We assert completed ⇒ not currently
    // double-counted as live.
    r.register(Invariant {
        name: "S4",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            for id in &m.auth.completed {
                if m.auth.live_deliveries.get(id).copied().unwrap_or(0) > 1 {
                    return Some(format!(
                        "{id:?} completed but still has multiple live deliveries (stale not fenced)"
                    ));
                }
            }
            None
        },
    });

    // S5 — broker restart disposable: after a crash the authoritative pending
    // set is never reduced by the crash itself. We assert that every pending
    // task is either deliverable (enqueued / inflight) or reconstructable. Since
    // the sweeper re-enqueues on crash, a pending task that is neither enqueued
    // nor in flight nor completed indicates a crash dropped it.
    r.register(Invariant {
        name: "S5",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            for (id, _) in m.auth.all_pending() {
                if !m.is_accounted_for(id) {
                    return Some(format!(
                        "{id:?} is authoritative-pending but lost: not deliverable, in flight, reserved, backlogged, nor completed"
                    ));
                }
            }
            None
        },
    });

    // S6 — duplicate publication safety: a logical task appears at most once
    // across all ready tiers and backlog.
    r.register(Invariant {
        name: "S6",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            use std::collections::BTreeMap;
            let mut counts: BTreeMap<crate::model::LogicalTaskId, u32> = BTreeMap::new();
            for dq in m.broker.sticky_ready.values() {
                for t in dq {
                    *counts.entry(t.id).or_insert(0) += 1;
                }
            }
            for dq in m.broker.general_ready.values() {
                for t in dq {
                    *counts.entry(t.id).or_insert(0) += 1;
                }
            }
            for items in m.broker.backlog.values() {
                for it in items {
                    *counts.entry(it.id).or_insert(0) += 1;
                }
            }
            for (id, c) in counts {
                if c > 1 {
                    return Some(format!("{id:?} appears {c} times across ready/backlog"));
                }
            }
            None
        },
    });

    // S7 — sticky safety: a run with an expired sticky claim must not have its
    // task lost. We assert: if a run's sticky expired, the task is either
    // deliverable, in flight, or completed (never silently dropped).
    r.register(Invariant {
        name: "S7",
        class: InvariantClass::Safety,
        check: |m: &BrokerModel| {
            for run in &m.auth.expired_sticky {
                // Find any pending task for this run.
                if let Some((id, _)) = m.auth.pending_wft.get(run) {
                    if !m.is_accounted_for(*id) {
                        return Some(format!(
                            "run {run} sticky expired but its WFT {id:?} was dropped, not promoted"
                        ));
                    }
                }
            }
            None
        },
    });

    // ---- Liveness (checked at quiescence) ----

    // L1 — eventual delivery / no loss: at quiescence every authoritative task
    // is completed (nothing pending or stuck).
    r.register(Invariant {
        name: "L1",
        class: InvariantClass::Liveness,
        check: |m: &BrokerModel| {
            if !m.auth.pending_wft.is_empty() || !m.auth.pending_activities.is_empty() {
                return Some(format!(
                    "at quiescence {} WFTs and {} activities remain undelivered",
                    m.auth.pending_wft.len(),
                    m.auth.pending_activities.len()
                ));
            }
            None
        },
    });

    // L2 — bounded poller memory: waiters per queue never exceed the cap.
    r.register(Invariant {
        name: "L2",
        class: InvariantClass::Liveness,
        check: |m: &BrokerModel| {
            for (queue, dq) in &m.broker.waiters {
                if dq.len() > m.cfg.max_waiters {
                    return Some(format!(
                        "queue {queue:?} has {} waiters, cap {}",
                        dq.len(),
                        m.cfg.max_waiters
                    ));
                }
            }
            None
        },
    });

    // L3 — long polls resolve cleanly: at quiescence no waiter remains parked.
    r.register(Invariant {
        name: "L3",
        class: InvariantClass::Liveness,
        check: |m: &BrokerModel| {
            for (queue, dq) in &m.broker.waiters {
                if !dq.is_empty() {
                    return Some(format!(
                        "queue {queue:?} still has {} unresolved waiters at quiescence",
                        dq.len()
                    ));
                }
            }
            None
        },
    });

    // L4 — backlog fairness, no starvation: at quiescence no backlog remains
    // (everything dispatched), and the control loop never set a starving split.
    r.register(Invariant {
        name: "L4",
        class: InvariantClass::Liveness,
        check: |m: &BrokerModel| {
            let backlog_remaining: usize = m.broker.backlog.values().map(|v| v.len()).sum();
            if backlog_remaining > 0 {
                return Some(format!(
                    "{backlog_remaining} backlog items undispatched at quiescence (possible starvation)"
                ));
            }
            // Backlog share must never dominate live-ready (would starve fresh
            // sync-matchable work).
            if m.broker.budget.backlog > m.broker.budget.live_ready {
                return Some(format!(
                    "control-loop backlog share {} exceeds live-ready {} (starves fresh work)",
                    m.broker.budget.backlog, m.broker.budget.live_ready
                ));
            }
            None
        },
    });

    r
}
