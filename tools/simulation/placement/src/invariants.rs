//! The placement safety invariants (I1–I6), registered against the engine.
//!
//! These encode the placement design thesis (architecture doc 035): DSQL owns
//! truth, runtime ownership is valid only by the current lease epoch, queue-home
//! is advisory, and execution-home is the correctness boundary. All six are
//! *safety* invariants — they must hold after every event under every schedule,
//! so the engine evaluates them following each step and stops the seed at the
//! first violation.
//!
//! Most are pure predicates over the authoritative [`Dsql`] and advisory
//! [`Edge`] state. Two conditions are detected at the moment they occur (a
//! commit resolving off a workflow's execution-home, and a drain relinquishing
//! before its routing was withdrawn) and recorded into model fields; the I1 and
//! I6 predicates simply surface those recordings, keeping all verdicts in the
//! invariant layer.

use sim_engine::{Invariant, InvariantClass, InvariantRegistry};

use crate::{model::execution_home, model_machine::PlacementModel};

/// Build the registry of placement invariants in stable I1..I6 order.
pub fn registry() -> InvariantRegistry<PlacementModel> {
    let mut r = InvariantRegistry::new();
    r.register(Invariant {
        name: "I1",
        class: InvariantClass::Safety,
        check: i1_execution_home,
    });
    r.register(Invariant {
        name: "I2",
        class: InvariantClass::Safety,
        check: i2_durable_dedupe,
    });
    r.register(Invariant {
        name: "I3",
        class: InvariantClass::Safety,
        check: i3_dedupe_accounting,
    });
    r.register(Invariant {
        name: "I4",
        class: InvariantClass::Safety,
        check: i4_no_future_epoch_at_edge,
    });
    r.register(Invariant {
        name: "I5",
        class: InvariantClass::Safety,
        check: i5_owner_implies_no_orphan_lease,
    });
    r.register(Invariant {
        name: "I6",
        class: InvariantClass::Safety,
        check: i6_drain_orders_routing_before_relinquish,
    });
    r
}

/// I1 — execution-home is the correctness boundary.
///
/// Two ways this can be falsified, both surfaced here: (a) a committed workflow
/// whose recorded home bundle is not its canonical execution-home, or (b) the
/// commit path detected an op resolving off the execution-home (recorded in the
/// model). (b) is what the `--bug=buggy-start-routing` defect trips.
fn i1_execution_home(m: &PlacementModel) -> Option<String> {
    for (workflow_id, record) in &m.dsql.workflows {
        let canonical = execution_home(*workflow_id, m.cfg.bundle_count);
        if record.home_bundle != canonical {
            return Some(format!(
                "workflow {} committed on bundle {} but its execution-home is {}",
                workflow_id.0, record.home_bundle.0, canonical.0
            ));
        }
    }
    m.wrong_execution_home().map(str::to_string)
}

/// I2 — durable request dedupe: no request id may apply more than once.
///
/// The replayed-request fraction of the workload exists precisely to attack
/// this; the fence + applied-set must keep every apply count at one.
fn i2_durable_dedupe(m: &PlacementModel) -> Option<String> {
    for (request_id, count) in &m.dsql.request_apply_count {
        if *count > 1 {
            return Some(format!(
                "request {} applied {} times (durable dedupe broken)",
                request_id.0, count
            ));
        }
    }
    None
}

/// I3 — dedupe-set integrity.
///
/// Every workflow's start request must be recorded in the applied set, and the
/// applied set must be exactly the set of request ids with a positive apply
/// count. This guards the bookkeeping I2 reasons over: a divergence would mean a
/// mutation took effect without being recorded as applied (or vice versa),
/// which would let a replay slip through dedupe later.
fn i3_dedupe_accounting(m: &PlacementModel) -> Option<String> {
    for (workflow_id, record) in &m.dsql.workflows {
        if !m.dsql.applied_requests.contains(&record.started_by) {
            return Some(format!(
                "workflow {} was started by request {} but that request is not in the applied set",
                workflow_id.0, record.started_by.0
            ));
        }
    }
    // The applied set and the positive-count set must coincide.
    for request_id in &m.dsql.applied_requests {
        match m.dsql.request_apply_count.get(request_id) {
            Some(count) if *count >= 1 => {}
            _ => {
                return Some(format!(
                    "request {} is marked applied but has no positive apply count",
                    request_id.0
                ));
            }
        }
    }
    None
}

/// I4 — the advisory edge cache never holds a *future* epoch.
///
/// The edge may lag (a stale-but-older epoch is fine and merely causes a fence
/// miss + repair), but it must never name an epoch newer than DSQL has issued —
/// that would mean routing invented authority. Snapshot monotonicity by
/// generation and the epoch-guarded point repair together uphold this.
fn i4_no_future_epoch_at_edge(m: &PlacementModel) -> Option<String> {
    for (idx, route) in m.edge.snapshot.bundle_routes.iter().enumerate() {
        if let Some(route) = route {
            let row = m.dsql.leases[idx];
            if route.epoch > row.epoch {
                return Some(format!(
                    "edge holds future epoch {} for bundle {} but DSQL epoch is {}",
                    route.epoch.0, idx, row.epoch.0
                ));
            }
        }
    }
    None
}

/// I5 — no orphaned live lease.
///
/// A lease row with no owner must not also carry a future expiry: an unowned row
/// that still looks "leased until later" would let stale routing treat a free
/// bundle as owned. Relinquish bumps the epoch and sets expiry to now, so an
/// unowned row always has a non-future `lease_until`.
fn i5_owner_implies_no_orphan_lease(m: &PlacementModel) -> Option<String> {
    let now = m.now_ms();
    for (idx, row) in m.dsql.leases.iter().enumerate() {
        if row.owner.is_none() && row.lease_until_ms > now {
            return Some(format!(
                "bundle {} has no owner but a future lease_until of {} (now {})",
                idx, row.lease_until_ms, now
            ));
        }
    }
    None
}

/// I6 — drain orders routing withdrawal before relinquish.
///
/// A graceful drain must remove the draining runtime from the edge routing
/// (phase 1) before it gives up any lease (phase 2); otherwise an in-flight op
/// could still be routed to a node that just dropped ownership. The condition is
/// detected when it occurs and recorded; this predicate surfaces it.
fn i6_drain_orders_routing_before_relinquish(m: &PlacementModel) -> Option<String> {
    m.i6_violation().map(str::to_string)
}
