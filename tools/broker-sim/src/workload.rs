//! Reproducible workload + fault schedule generation for the broker model.
//!
//! All timing and choices come from the harness `SimCtx` RNG, so a given seed
//! produces an identical event sequence. The workload publishes workflow and
//! activity tasks across a small set of runs/queues/partitions and interleaves
//! polls, direct claims, queries, and the adversarial faults the spec requires.

use sim_harness::SimCtx;

use crate::{
    events::{act_id, activity_queue, wft_id, wft_queue, BrokerEvent, BrokerEventKind},
    model::{PartitionIx, QueueKey},
    model_machine::BrokerModel,
};

const NAMESPACE: u32 = 0;
const TASK_QUEUE: u32 = 0;
const RUN_POOL: u64 = 24;
const WORKER_POOL: u32 = 8;

fn pick_queue_wft(partition: PartitionIx) -> QueueKey {
    wft_queue(NAMESPACE, TASK_QUEUE, None, None, partition)
}

fn pick_queue_activity(partition: PartitionIx) -> QueueKey {
    activity_queue(NAMESPACE, TASK_QUEUE, partition)
}

/// Schedule the full workload (and faults) for one seed onto the event queue.
pub fn schedule(model: &mut BrokerModel, ctx: &mut SimCtx<'_, BrokerEvent>) {
    let ops = model.ops as u64;
    let horizon = model.horizon_ms.max(2);
    let partitions = model.cfg.partitions_per_queue.max(1);

    for _ in 0..ops {
        let at = ctx.rng().range(1, horizon);
        let choice = ctx.rng().range(0, 100);
        let run = ctx.rng().range(1, RUN_POOL);
        let partition = ctx.rng().range(0, u64::from(partitions)) as PartitionIx;
        let worker = ctx.rng().range(0, u64::from(WORKER_POOL)) as u32;
        // A run's workflow task has a single execution-home partition; all
        // publishes (and duplicates) of that run's WFT must target it, exactly
        // as a real run has one home. Activities key by full id (incl. attempt)
        // so they may use any partition without aliasing.
        let wft_home = (run % u64::from(partitions)) as PartitionIx;

        match choice {
            // Publish a workflow task, sometimes sticky-preferred.
            0..=29 => {
                let sticky = if ctx.rng().bool_with_percent(40) {
                    Some(ctx.rng().range(0, u64::from(WORKER_POOL)) as u32)
                } else {
                    None
                };
                let priority = ctx.rng().range(0, 3) as u8;
                let queue = pick_queue_wft(wft_home);
                ctx.schedule(
                    at,
                    BrokerEvent::new(BrokerEventKind::PublishWft {
                        id: wft_id(run, 0),
                        queue,
                        sticky_target: sticky,
                        priority,
                    }),
                );
            }
            // Publish an activity task.
            30..=49 => {
                let activity = ctx.rng().range(0, 4) as u32;
                let attempt = ctx.rng().range(1, 3) as u32;
                let priority = ctx.rng().range(0, 3) as u8;
                let queue = pick_queue_activity(partition);
                ctx.schedule(
                    at,
                    BrokerEvent::new(BrokerEventKind::PublishActivity {
                        id: act_id(run, activity, attempt),
                        queue,
                        priority,
                    }),
                );
            }
            // Poll a workflow or activity queue.
            50..=84 => {
                let queue = if ctx.rng().bool_with_percent(60) {
                    pick_queue_wft(partition)
                } else {
                    pick_queue_activity(partition)
                };
                ctx.schedule(
                    at,
                    BrokerEvent::new(BrokerEventKind::Poll {
                        queue,
                        worker,
                        attempt: 0,
                    }),
                );
            }
            // Direct claim (targets the run's home partition where its WFT lives).
            85..=88 => {
                let queue = pick_queue_wft(wft_home);
                ctx.schedule(
                    at,
                    BrokerEvent::new(BrokerEventKind::DirectClaim {
                        queue,
                        run_key: run,
                    }),
                );
            }
            // Query.
            89..=92 => {
                let queue = QueueKey {
                    namespace: NAMESPACE,
                    task_queue: TASK_QUEUE,
                    kind: crate::model::TaskKind::Query,
                    deployment: None,
                    build: None,
                    partition,
                };
                ctx.schedule(
                    at,
                    BrokerEvent::new(BrokerEventKind::PublishQuery {
                        queue,
                        sticky_target: None,
                    }),
                );
            }
            // Faults (the required set, all RNG-timed).
            93 => ctx.schedule(at, BrokerEvent::new(BrokerEventKind::BrokerCrash)),
            94 => ctx.schedule(
                at,
                BrokerEvent::new(BrokerEventKind::WorkerCrash { worker }),
            ),
            95 => ctx.schedule(
                at,
                BrokerEvent::new(BrokerEventKind::DenyWorker {
                    namespace: NAMESPACE,
                    task_queue: TASK_QUEUE,
                    worker,
                }),
            ),
            96 => ctx.schedule(
                at,
                BrokerEvent::new(BrokerEventKind::PartitionBacklogPressure {
                    queue: pick_queue_wft(partition),
                }),
            ),
            97 => ctx.schedule(
                at,
                BrokerEvent::new(BrokerEventKind::SustainedBacklogAge {
                    queue: pick_queue_wft(partition),
                }),
            ),
            // Duplicate publish to stress dedup (same run's WFT, its home partition).
            _ => {
                let queue = pick_queue_wft(wft_home);
                ctx.schedule(
                    at,
                    BrokerEvent::new(BrokerEventKind::DuplicatePublish {
                        id: wft_id(run, 0),
                        queue,
                        priority: 0,
                    }),
                );
            }
        }
    }
}
