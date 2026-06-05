//! Reproducible workload + fault schedule for the placement model.
//!
//! Mirrors the broker simulator's workload module: every timing and branch
//! decision is drawn from the engine `SimCtx` RNG, so a given seed reproduces an
//! identical event sequence. The mix is the original `placement-sim`'s: mostly
//! `Start`/`Signal` client ops (with a small fraction of `Signal`s deliberately
//! replaying an earlier request id to exercise dedupe), interleaved with the
//! three membership faults — renewal suppression, crash/restart, and graceful
//! drain.

use sim_engine::SimCtx;

use crate::{
    events::{PlacementEvent, PlacementEventKind},
    model::{ClientOp, OpKind, RequestId, WorkflowId},
    model_machine::PlacementModel,
};

/// Size of the workflow-id pool the workload samples from. A bounded pool means
/// `Start` and `Signal` ops collide on the same workflows, exercising
/// already-exists and same-home signal paths.
const WORKFLOW_POOL: u64 = 80;

/// Lay down one seed's worth of client operations and faults onto the queue.
///
/// Request ids minted for `Start`/`Signal` ops are remembered so a small
/// fraction of later signals can replay an existing id — the only way to drive
/// the durable-dedupe invariant (I2) under contention.
pub fn schedule(model: &mut PlacementModel, ctx: &mut SimCtx<'_, PlacementEvent>) {
    let ops = model.cfg.ops_per_seed;
    let max_time = model.cfg.max_time_ms;
    let mut known_request_ids: Vec<RequestId> = Vec::new();

    for _ in 0..ops {
        let at = ctx.rng().range(5, max_time);
        let choice = ctx.rng().range(0, 100);
        match choice {
            // Start a workflow.
            0..=34 => {
                let workflow_id = WorkflowId(ctx.rng().range(1, WORKFLOW_POOL));
                let request_id = model.next_request_id();
                known_request_ids.push(request_id);
                ctx.schedule(
                    at,
                    PlacementEvent::new(PlacementEventKind::EdgeOp {
                        op: ClientOp {
                            kind: OpKind::Start,
                            workflow_id,
                            request_id,
                        },
                        attempt: 0,
                    }),
                );
            }
            // Signal a workflow — occasionally replaying a known request id
            // (~8%) so dedupe is exercised, otherwise a fresh id.
            35..=77 => {
                let workflow_id = WorkflowId(ctx.rng().range(1, WORKFLOW_POOL));
                let duplicate = !known_request_ids.is_empty() && ctx.rng().range(0, 100) < 8;
                let request_id = if duplicate {
                    let idx = ctx.rng().range(0, known_request_ids.len() as u64) as usize;
                    known_request_ids[idx]
                } else {
                    let rid = model.next_request_id();
                    known_request_ids.push(rid);
                    rid
                };
                ctx.schedule(
                    at,
                    PlacementEvent::new(PlacementEventKind::EdgeOp {
                        op: ClientOp {
                            kind: OpKind::Signal,
                            workflow_id,
                            request_id,
                        },
                        attempt: 0,
                    }),
                );
            }
            // Fault: suppress a runtime's renewals so its lease lapses while it
            // may still believe it owns the bundle.
            78..=87 => {
                if let Some(runtime) = model.random_runtime_id(ctx.rng()) {
                    let d = ctx.rng().range(80, 260);
                    ctx.schedule(
                        at,
                        PlacementEvent::new(PlacementEventKind::DisableRenewals {
                            runtime,
                            duration_ms: d,
                        }),
                    );
                }
            }
            // Fault: crash a runtime; it restarts as a fresh incarnation.
            88..=94 => {
                if let Some(runtime) = model.random_runtime_id(ctx.rng()) {
                    let d = ctx.rng().range(50, 240);
                    ctx.schedule(
                        at,
                        PlacementEvent::new(PlacementEventKind::CrashRuntime {
                            runtime,
                            restart_delay_ms: d,
                        }),
                    );
                }
            }
            // Fault: begin a graceful drain (routing-then-relinquish ordering).
            _ => {
                if let Some(runtime) = model.random_runtime_id(ctx.rng()) {
                    ctx.schedule(
                        at,
                        PlacementEvent::new(PlacementEventKind::BeginDrain { runtime }),
                    );
                }
            }
        }
    }
}
