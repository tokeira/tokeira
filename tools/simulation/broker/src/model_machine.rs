//! The `BrokerModel`: a pure deterministic state machine over [`BrokerEvent`]s.
//!
//! This is the `StressModel` the harness drives. It re-models the delivery
//! broker's semantics — three-tier delivery, reservation/commit coupling,
//! sticky promotion, dedup, the grace scanner, denied workers, memory-only
//! pollers, the sweeper, WFT-vs-AT separation, partitioned queues, the control
//! loop, and delivery-quality signals — entirely in terms of [`BrokerState`]
//! and [`AuthoritativePendingState`]. The injectable bug is threaded in so a
//! single flag deliberately violates a named safety invariant.

use std::collections::VecDeque;

use sim_engine::{SignalCounters, SimCtx, StressModel};

use crate::{
    bug::InjectedBug,
    events::{BrokerEvent, BrokerEventKind},
    model::{
        AuthoritativePendingState, BacklogItem, BrokerCfg, BrokerState, BudgetSplit, Delivery,
        LogicalTaskId, QueueKey, ReadyTask, WorkerIdentity,
    },
};

/// The simulator's model. Owns both state halves, the config, the RNG-driven
/// workload shape, signal counters, and the optional injected bug.
pub struct BrokerModel {
    /// Tunable parameters (meaning fixed by the spec; values are defaults).
    pub cfg: BrokerCfg,
    /// Ephemeral broker state (discarded on crash).
    pub broker: BrokerState,
    /// Authoritative per-run truth (survives crash).
    pub auth: AuthoritativePendingState,
    /// Accumulated named signals for the report.
    pub signals: SignalCounters,
    /// The deliberately-injected bug, if any.
    pub bug: Option<InjectedBug>,
    /// Monotonic counter minting fresh `DeliveryId`s.
    next_delivery_id: u64,
    /// Monotonic counter for backlog FIFO ordering.
    next_backlog_seq: u64,
    /// Monotonic counter for waiter ids.
    next_waiter_id: u64,
    /// Number of workload ops to schedule at bootstrap.
    pub ops: usize,
    /// Simulated time horizon for workload scheduling.
    pub horizon_ms: u64,
    /// In-flight reservations awaiting their `StartTxnCommit`:
    /// `(id, delivery_id, worker, queue, reserved_at_ms)`.
    pending_reservations: Vec<(LogicalTaskId, u64, WorkerIdentity, QueueKey, u64)>,
    /// Set once the workload + in-flight work have drained (liveness point).
    settled: bool,
}

impl BrokerModel {
    /// Construct a model with the given config, workload size, and optional bug.
    pub fn new(cfg: BrokerCfg, ops: usize, horizon_ms: u64, bug: Option<InjectedBug>) -> Self {
        BrokerModel {
            cfg,
            broker: BrokerState::default(),
            auth: AuthoritativePendingState::default(),
            signals: SignalCounters::new(),
            bug,
            next_delivery_id: 1,
            next_backlog_seq: 0,
            next_waiter_id: 0,
            ops,
            horizon_ms,
            pending_reservations: Vec::new(),
            settled: false,
        }
    }

    fn mint_delivery_id(&mut self) -> u64 {
        let id = self.next_delivery_id;
        self.next_delivery_id += 1;
        id
    }

    fn quality(&mut self, queue: QueueKey) -> &mut crate::model::QueueQuality {
        self.broker.quality.entry(queue).or_default()
    }

    // ---- Publish + dedup + tier placement (task 6.2) ----

    fn publish(
        &mut self,
        id: LogicalTaskId,
        queue: QueueKey,
        sticky_target: Option<WorkerIdentity>,
        priority: u8,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        // Dedup: a logical task already present ANYWHERE in the system — a live
        // tier, durable backlog, an in-flight reservation/delivery, or already
        // completed — is not enqueued again, UNLESS the no-dedup bug is active.
        // Checking the full accounted-for union (not just the live-tier marker)
        // closes the windows where a task is transiently absent from `enqueued`
        // (reserved mid-commit, spilled to backlog, in flight) but a duplicate
        // publish would otherwise create a second copy → a double start.
        let skip_dedup = matches!(self.bug, Some(InjectedBug::NoDedupOnRepublish));
        if !skip_dedup && self.is_accounted_for(id) {
            self.signals.incr("duplicates_suppressed");
            return;
        }

        // Record authoritative pending state (the truth the broker optimises over).
        match id {
            LogicalTaskId::Wft(run, _) => {
                self.auth.pending_wft.entry(run).or_insert((id, now_ms));
            }
            LogicalTaskId::Activity(run, activity, _) => {
                // A new attempt supersedes any prior pending attempt for the same
                // (run, activity): only one attempt is current at a time. Evict
                // the stale attempt's id from the dedup set and the live tiers so
                // authoritative pending always points at a deliverable task.
                if let Some((prev_id, _)) = self.auth.pending_activities.get(&(run, activity)) {
                    if *prev_id != id {
                        let stale = *prev_id;
                        self.evict_ready(stale, queue);
                        self.broker.enqueued.remove(&stale);
                    }
                }
                self.auth
                    .pending_activities
                    .insert((run, activity), (id, now_ms));
            }
        }

        self.broker.enqueued.insert(id);

        // Sync-match quality: did a compatible waiter already exist at publish?
        let has_waiter = self
            .broker
            .waiters
            .get(&queue)
            .is_some_and(|w| !w.is_empty());
        {
            let q = self.quality(queue);
            q.published += 1;
            if has_waiter {
                q.published_with_waiter += 1;
            }
        }

        let task = ReadyTask {
            id,
            queue,
            sticky_target,
            entered_at_ms: now_ms,
            sticky_deadline_ms: sticky_target.map(|_| now_ms + self.cfg.sticky_ttl_ms),
            priority,
        };

        if let Some(target) = sticky_target {
            // Schedule the sticky TTL expiry so an un-served sticky claim gets
            // promoted to general (S7).
            ctx.schedule(
                self.cfg.sticky_ttl_ms,
                BrokerEvent::new(BrokerEventKind::StickyTtlExpire { id, queue }),
            );
            let _ = target;
            self.broker
                .sticky_ready
                .entry(queue)
                .or_default()
                .push_back(task);
        } else {
            self.broker
                .general_ready
                .entry(queue)
                .or_default()
                .push_back(task);
        }

        // Tier A inline vs Tier B: if a waiter was already present we could match
        // immediately. We model the match by waking a poll attempt now.
        if has_waiter {
            self.signals.incr("tier_a_inline");
            // Pull a waiter and attempt a synchronous match this instant.
            if let Some(waiter) = self.pop_eligible_waiter(queue, now_ms) {
                self.try_match_for_worker(queue, waiter.worker, now_ms, ctx);
            }
        } else {
            self.signals.incr("tier_b_live_ready");
            // Schedule a grace scan to spill if it ages out.
            ctx.schedule(
                self.cfg.grace_window_ms + 1,
                BrokerEvent::new(BrokerEventKind::GraceScan { queue }),
            );
        }
    }

    // ---- Poll tier-ladder + reservation/commit (task 6.3) ----

    fn poll(
        &mut self,
        queue: QueueKey,
        worker: WorkerIdentity,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        // Denied workers never receive a task on the queue (task 6.4 / R14).
        if self.is_denied(queue, worker) {
            self.register_waiter(queue, worker, now_ms, ctx);
            return;
        }
        if self.try_match_for_worker(queue, worker, now_ms, ctx) {
            return;
        }
        // No match: register a memory-only waiter and a poll deadline (R15).
        self.register_waiter(queue, worker, now_ms, ctx);
    }

    /// Attempt to deliver a task to `worker` following the doc-040 preference
    /// order: sticky-exact -> general live -> backlog (budget-permitting).
    /// Returns true if a reservation was started.
    fn try_match_for_worker(
        &mut self,
        queue: QueueKey,
        worker: WorkerIdentity,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) -> bool {
        if self.is_denied(queue, worker) {
            return false;
        }

        // 1. Sticky-exact match for this worker.
        if let Some(task) = self.take_sticky_for_worker(queue, worker) {
            self.begin_reservation(task.id, queue, worker, now_ms, ctx);
            return true;
        }

        // 2. General live waiter / ready.
        if let Some(task) = self.take_general(queue) {
            self.begin_reservation(task.id, queue, worker, now_ms, ctx);
            return true;
        }

        // 3. Backlog offer, only if the control-loop budget permits and it does
        //    not starve fresh sync-matchable work. We model "fresh work present"
        //    as: do not serve backlog if live-ready still has a task for a
        //    compatible worker. Backlog is FIFO within priority.
        if self.broker.budget.backlog > 0 {
            if let Some(item) = self.take_backlog(queue) {
                self.broker.enqueued.insert(item.id);
                self.begin_reservation(item.id, queue, worker, now_ms, ctx);
                self.signals.incr("tier_c_backlog_spill_redeliver");
                return true;
            }
        }
        false
    }

    fn begin_reservation(
        &mut self,
        id: LogicalTaskId,
        queue: QueueKey,
        worker: WorkerIdentity,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        let delivery_id = self.mint_delivery_id();
        // Record schedule-to-start latency sample using the authoritative
        // schedule time when known.
        let scheduled_at = match id {
            LogicalTaskId::Wft(run, _) => self.auth.pending_wft.get(&run).map(|(_, t)| *t),
            LogicalTaskId::Activity(run, activity, _) => self
                .auth
                .pending_activities
                .get(&(run, activity))
                .map(|(_, t)| *t),
        };
        if let Some(scheduled_at) = scheduled_at {
            let q = self.quality(queue);
            q.sched_to_start_total_ms += now_ms.saturating_sub(scheduled_at);
            q.sched_to_start_samples += 1;
        }

        // The BUG: hand out the token before the start transaction commits.
        if matches!(self.bug, Some(InjectedBug::TokenBeforeCommit)) {
            self.broker.inflight.insert(
                id,
                Delivery {
                    delivery_id,
                    worker,
                    lease_until_ms: now_ms + self.cfg.lease_ms,
                    committed: false, // token held without commit -> violates S3
                },
            );
            *self.auth.live_deliveries.entry(id).or_insert(0) += 1;
            self.signals.incr("tokens_delivered");
        }

        // Schedule the commit resolution (1-5ms later), mostly committing.
        let will_commit = ctx.rng().bool_with_percent(85);
        let delay = ctx.rng().range(1, 5);
        ctx.schedule(
            delay,
            BrokerEvent::new(BrokerEventKind::StartTxnCommit {
                id,
                delivery_id,
                will_commit,
            }),
        );
        // Stash the pending reservation so commit can find the worker.
        self.pending_reservations
            .push((id, delivery_id, worker, queue, now_ms));
    }

    fn start_txn_commit(
        &mut self,
        id: LogicalTaskId,
        delivery_id: u64,
        will_commit: bool,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        let Some(pos) = self
            .pending_reservations
            .iter()
            .position(|(rid, did, _, _, _)| *rid == id && *did == delivery_id)
        else {
            return;
        };
        let (_, _, worker, queue, _) = self.pending_reservations.remove(pos);

        if !will_commit {
            // Start transaction did not commit: return the reserved poller and
            // leave the task deliverable. If the buggy path pre-delivered a
            // token, that is the S3 violation the checker catches; the correct
            // path delivered nothing yet.
            if matches!(self.bug, Some(InjectedBug::TokenBeforeCommit)) {
                // The token was wrongly handed out; it stays uncommitted in
                // inflight, which the S3 check flags.
                self.signals.incr("reservation_aborts");
            } else {
                self.signals.incr("reservation_returns");
                // Make the task deliverable again.
                self.broker.enqueued.insert(id);
                self.requeue_general(id, queue, now_ms, ctx);
            }
            return;
        }

        // Commit succeeded: token legitimately delivered now.
        if !matches!(self.bug, Some(InjectedBug::TokenBeforeCommit)) {
            self.broker.inflight.insert(
                id,
                Delivery {
                    delivery_id,
                    worker,
                    lease_until_ms: now_ms + self.cfg.lease_ms,
                    committed: true,
                },
            );
            *self.auth.live_deliveries.entry(id).or_insert(0) += 1;
            self.signals.incr("tokens_delivered");
        } else if let Some(d) = self.broker.inflight.get_mut(&id) {
            // Buggy path already inserted uncommitted; mark committed now.
            d.committed = true;
        }

        // Schedule lease expiry and a likely completion.
        ctx.schedule(
            self.cfg.lease_ms,
            BrokerEvent::new(BrokerEventKind::LeaseExpire { id, delivery_id }),
        );
        let complete_delay = ctx.rng().range(1, self.cfg.lease_ms.max(2));
        ctx.schedule(
            complete_delay,
            BrokerEvent::new(BrokerEventKind::CompleteTask { id, delivery_id }),
        );
    }

    fn complete_task(&mut self, id: LogicalTaskId, delivery_id: u64) {
        // Stale completion fence (S4): only the current delivery may complete.
        let current = self
            .broker
            .inflight
            .get(&id)
            .map(|d| d.delivery_id)
            .unwrap_or(0);
        if current != delivery_id {
            self.signals.incr("stale_completions");
            return;
        }
        // Terminal: clear inflight, decrement live count, mark completed, clear
        // authoritative pending + dedup.
        self.broker.inflight.remove(&id);
        if let Some(c) = self.auth.live_deliveries.get_mut(&id) {
            *c = c.saturating_sub(1);
        }
        self.auth.completed.insert(id);
        self.broker.enqueued.remove(&id);
        match id {
            LogicalTaskId::Wft(run, _) => {
                self.auth.pending_wft.remove(&run);
            }
            LogicalTaskId::Activity(run, activity, _) => {
                self.auth.pending_activities.remove(&(run, activity));
            }
        }
        self.signals.incr("completions");
    }

    // ---- Sticky promotion, grace scan, direct claim, query (task 6.4) ----

    fn sticky_ttl_expire(
        &mut self,
        id: LogicalTaskId,
        queue: QueueKey,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        // Find the task still in the sticky tier; if gone, nothing to do.
        let Some(dq) = self.broker.sticky_ready.get_mut(&queue) else {
            return;
        };
        let Some(pos) = dq.iter().position(|t| t.id == id) else {
            return;
        };
        let mut task = dq.remove(pos).expect("position valid");

        // The BUG: drop the expired sticky claim instead of promoting it.
        if matches!(self.bug, Some(InjectedBug::DropExpiredSticky)) {
            self.broker.enqueued.remove(&id);
            self.auth.expired_sticky.insert(id.run());
            // Note: we intentionally do NOT re-enqueue, and we leave the task
            // lost — the S7 / L1 checks catch the resulting loss.
            return;
        }

        // Promote to general: any compatible poller may now take it.
        task.sticky_target = None;
        task.sticky_deadline_ms = None;
        task.entered_at_ms = now_ms;
        self.auth.expired_sticky.insert(id.run());
        self.broker
            .general_ready
            .entry(queue)
            .or_default()
            .push_back(task);
        self.signals.incr("sticky_promotions");
        // A grace scan should still be able to spill it later.
        ctx.schedule(
            self.cfg.grace_window_ms + 1,
            BrokerEvent::new(BrokerEventKind::GraceScan { queue }),
        );
    }

    fn grace_scan(&mut self, queue: QueueKey, now_ms: u64) {
        // Spill live-ready tasks older than the grace window to durable backlog,
        // clearing their dedup key so the same logical task can be re-published.
        let grace = self.cfg.grace_window_ms;
        let mut spilled: Vec<ReadyTask> = Vec::new();
        for tier in [
            &mut self.broker.general_ready,
            &mut self.broker.sticky_ready,
        ] {
            if let Some(dq) = tier.get_mut(&queue) {
                let mut kept: VecDeque<ReadyTask> = VecDeque::new();
                while let Some(t) = dq.pop_front() {
                    if now_ms.saturating_sub(t.entered_at_ms) > grace {
                        spilled.push(t);
                    } else {
                        kept.push_back(t);
                    }
                }
                *dq = kept;
            }
        }
        for t in spilled {
            self.broker.enqueued.remove(&t.id);
            let seq = self.next_backlog_seq;
            self.next_backlog_seq += 1;
            self.broker
                .backlog
                .entry(queue)
                .or_default()
                .push(BacklogItem {
                    id: t.id,
                    queue,
                    priority: t.priority,
                    enqueue_seq: seq,
                });
            self.signals.incr("tier_c_backlog_spill");
        }
    }

    fn direct_claim(
        &mut self,
        queue: QueueKey,
        run_key: crate::model::RunKey,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        // Pull a run's task out of the GENERAL tier only (never sticky) for eager
        // dispatch. In the real system the eager dispatcher then starts the task
        // directly, so the model hands it straight into a committed in-flight
        // delivery (and schedules its completion) — it is NOT dropped from
        // accounting, and it cannot also be delivered via the normal poll path.
        let claimed = self
            .broker
            .general_ready
            .get_mut(&queue)
            .and_then(|dq| {
                dq.iter()
                    .position(|t| t.id.run() == run_key)
                    .map(|pos| dq.remove(pos))
            })
            .flatten();
        if let Some(task) = claimed {
            self.broker.enqueued.remove(&task.id);
            let delivery_id = self.mint_delivery_id();
            self.broker.inflight.insert(
                task.id,
                Delivery {
                    delivery_id,
                    worker: 0, // eager dispatcher
                    lease_until_ms: now_ms + self.cfg.lease_ms,
                    committed: true,
                },
            );
            *self.auth.live_deliveries.entry(task.id).or_insert(0) += 1;
            self.signals.incr("direct_claims");
            let complete_delay = ctx.rng().range(1, self.cfg.lease_ms.max(2));
            ctx.schedule(
                complete_delay,
                BrokerEvent::new(BrokerEventKind::CompleteTask {
                    id: task.id,
                    delivery_id,
                }),
            );
        }
    }

    fn publish_query(&mut self, queue: QueueKey, sticky_target: Option<WorkerIdentity>) {
        // Query path bypasses dedup + backlog; deliver-all to a matching worker.
        self.broker
            .query_ready
            .entry(queue)
            .or_default()
            .push_back((0, sticky_target));
        self.signals.incr("query_published");
    }

    // ---- Control loop + quality (task 6.5) ----

    fn control_loop_tick(&mut self, now_ms: u64, ctx: &mut SimCtx<'_, BrokerEvent>) {
        // Recompute the budget split from the oldest backlog age. Low age biases
        // toward sticky/live; once backlog has aged past the high-water mark we
        // raise the backlog share — but keep it strictly below the live-ready
        // share so fresh sync-matchable work is never starved (L4).
        let oldest_backlog_age_ms = self
            .broker
            .backlog
            .values()
            .flatten()
            .map(|item| now_ms.saturating_sub(item.enqueue_seq.min(now_ms)))
            .max()
            .unwrap_or(0);
        let backlog_present = self.broker.backlog.values().any(|v| !v.is_empty());
        self.broker.budget = if !backlog_present {
            BudgetSplit {
                sticky: 50,
                live_ready: 40,
                backlog: 10,
            }
        } else if oldest_backlog_age_ms >= self.cfg.backlog_age_high_ms {
            BudgetSplit {
                sticky: 25,
                live_ready: 45,
                backlog: 30,
            }
        } else {
            BudgetSplit {
                sticky: 40,
                live_ready: 45,
                backlog: 15,
            }
        };
        self.signals.add("control_loop_ticks", 1);
        // Re-arm the tick.
        ctx.schedule(20, BrokerEvent::new(BrokerEventKind::ControlLoopTick));
    }

    // ---- Sweeper, crash, lease, partition pressure (task 6.6) ----

    fn broker_crash(&mut self, now_ms: u64, ctx: &mut SimCtx<'_, BrokerEvent>) {
        // Discard ALL ephemeral broker state. Authoritative pending state is
        // kept. Mark nothing complete. The in-flight deliveries vanish with the
        // broker, so their live-delivery counts must drop to zero — otherwise a
        // post-crash redelivery would read as a concurrent (double) start.
        // Pending reservations are likewise lost.
        for (id, _) in std::mem::take(&mut self.broker.inflight) {
            if let Some(c) = self.auth.live_deliveries.get_mut(&id) {
                *c = 0;
            }
        }
        self.pending_reservations.clear();
        let preserved_quality = std::mem::take(&mut self.broker.quality);
        let preserved_denied = std::mem::take(&mut self.broker.denied_workers);
        self.broker = BrokerState {
            quality: preserved_quality,
            denied_workers: preserved_denied,
            ..BrokerState::default()
        };
        self.signals.incr("broker_crashes");
        self.sweeper_rebuild(now_ms, ctx);
    }

    fn sweeper_rebuild(&mut self, now_ms: u64, ctx: &mut SimCtx<'_, BrokerEvent>) {
        // Reconstruct delivery candidates from authoritative pending state.
        // Republish each pending task to the general tier (expired sticky claims
        // become general-deliverable, never re-bound to the lost worker).
        let pending = self.auth.all_pending();
        for (id, _scheduled_at) in pending {
            if self.broker.enqueued.contains(&id) {
                continue;
            }
            self.broker.enqueued.insert(id);
            let queue = self.sweeper_queue_for(id);
            self.broker
                .general_ready
                .entry(queue)
                .or_default()
                .push_back(ReadyTask {
                    id,
                    queue,
                    sticky_target: None, // expired/lost sticky -> general
                    entered_at_ms: now_ms,
                    sticky_deadline_ms: None,
                    priority: 0,
                });
            ctx.schedule(
                self.cfg.grace_window_ms + 1,
                BrokerEvent::new(BrokerEventKind::GraceScan { queue }),
            );
        }
        self.signals.incr("sweeper_rebuilds");
    }

    /// The queue a swept task is republished to. The model keys swept tasks onto
    /// a canonical partition-0 queue for the run's namespace/task-queue; this is
    /// sufficient for the no-loss invariant which only requires the task to be
    /// deliverable somewhere.
    fn sweeper_queue_for(&self, id: LogicalTaskId) -> QueueKey {
        let kind = if id.is_wft() {
            crate::model::TaskKind::Workflow
        } else {
            crate::model::TaskKind::Activity
        };
        QueueKey {
            namespace: 0,
            task_queue: 0,
            kind,
            deployment: None,
            build: None,
            partition: 0,
        }
    }

    fn lease_expire(
        &mut self,
        id: LogicalTaskId,
        delivery_id: u64,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        // If this delivery is still the current in-flight one and uncompleted,
        // the lease lapses: redeliver. The old delivery_id becomes stale, so a
        // late completion under it is rejected (S4).
        let still_current = self
            .broker
            .inflight
            .get(&id)
            .map(|d| d.delivery_id == delivery_id)
            .unwrap_or(false);
        if !still_current {
            return; // already completed or superseded
        }
        if self.auth.completed.contains(&id) {
            return;
        }
        // Drop the lapsed delivery and make the task deliverable again.
        self.broker.inflight.remove(&id);
        if let Some(c) = self.auth.live_deliveries.get_mut(&id) {
            *c = c.saturating_sub(1);
        }
        self.broker.enqueued.insert(id);
        let queue = self.sweeper_queue_for(id);
        self.broker
            .general_ready
            .entry(queue)
            .or_default()
            .push_back(ReadyTask {
                id,
                queue,
                sticky_target: None,
                entered_at_ms: now_ms,
                sticky_deadline_ms: None,
                priority: 0,
            });
        self.signals.incr("redeliveries");
        let _ = ctx;
    }

    // ---- Small helpers ----

    fn is_denied(&self, queue: QueueKey, worker: WorkerIdentity) -> bool {
        self.broker
            .denied_workers
            .contains(&(queue.namespace, queue.task_queue, worker))
    }

    /// Remove a logical task from both live-ready tiers of a queue (used when a
    /// new activity attempt supersedes a stale one).
    fn evict_ready(&mut self, id: LogicalTaskId, queue: QueueKey) {
        for tier in [
            &mut self.broker.general_ready,
            &mut self.broker.sticky_ready,
        ] {
            if let Some(dq) = tier.get_mut(&queue) {
                dq.retain(|t| t.id != id);
            }
        }
    }

    fn register_waiter(
        &mut self,
        queue: QueueKey,
        worker: WorkerIdentity,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        let dq = self.broker.waiters.entry(queue).or_default();
        if dq.len() >= self.cfg.max_waiters {
            // Reject the excess poll (L2) — memory bound enforced.
            self.signals.incr("poll_rejections");
            return;
        }
        let waiter_id = self.next_waiter_id;
        self.next_waiter_id += 1;
        let deadline = now_ms + 50;
        dq.push_back(crate::model::Waiter {
            waiter_id,
            worker,
            deadline_ms: deadline,
        });
        ctx.schedule(
            50,
            BrokerEvent::new(BrokerEventKind::PollDeadline { queue, waiter_id }),
        );
    }

    fn poll_deadline(&mut self, queue: QueueKey, waiter_id: u64) {
        if let Some(dq) = self.broker.waiters.get_mut(&queue) {
            if let Some(pos) = dq.iter().position(|w| w.waiter_id == waiter_id) {
                dq.remove(pos);
                let q = self.quality(queue);
                q.polls_resolved += 1; // resolved as timeout
                self.signals.incr("poll_timeouts");
            }
        }
    }

    fn pop_eligible_waiter(
        &mut self,
        queue: QueueKey,
        now_ms: u64,
    ) -> Option<crate::model::Waiter> {
        let dq = self.broker.waiters.get_mut(&queue)?;
        let _ = now_ms;
        dq.pop_front()
    }

    fn take_sticky_for_worker(
        &mut self,
        queue: QueueKey,
        worker: WorkerIdentity,
    ) -> Option<ReadyTask> {
        let dq = self.broker.sticky_ready.get_mut(&queue)?;
        let pos = dq.iter().position(|t| t.sticky_target == Some(worker))?;
        let task = dq.remove(pos);
        if let Some(t) = &task {
            self.broker.enqueued.remove(&t.id);
            let q = self.broker.quality.entry(queue).or_default();
            q.polls_resolved += 1;
            q.polls_with_work += 1;
        }
        task
    }

    fn take_general(&mut self, queue: QueueKey) -> Option<ReadyTask> {
        let dq = self.broker.general_ready.get_mut(&queue)?;
        let task = dq.pop_front();
        if let Some(t) = &task {
            self.broker.enqueued.remove(&t.id);
            let q = self.broker.quality.entry(queue).or_default();
            q.polls_resolved += 1;
            q.polls_with_work += 1;
        }
        task
    }

    fn take_backlog(&mut self, queue: QueueKey) -> Option<BacklogItem> {
        let items = self.broker.backlog.get_mut(&queue)?;
        if items.is_empty() {
            return None;
        }
        // FIFO within the lowest priority band: pick min (priority, enqueue_seq).
        let mut best = 0usize;
        for i in 1..items.len() {
            let a = (items[i].priority, items[i].enqueue_seq);
            let b = (items[best].priority, items[best].enqueue_seq);
            if a < b {
                best = i;
            }
        }
        Some(items.remove(best))
    }

    fn requeue_general(
        &mut self,
        id: LogicalTaskId,
        queue: QueueKey,
        now_ms: u64,
        ctx: &mut SimCtx<'_, BrokerEvent>,
    ) {
        self.broker
            .general_ready
            .entry(queue)
            .or_default()
            .push_back(ReadyTask {
                id,
                queue,
                sticky_target: None,
                entered_at_ms: now_ms,
                sticky_deadline_ms: None,
                priority: 0,
            });
        ctx.schedule(
            self.cfg.grace_window_ms + 1,
            BrokerEvent::new(BrokerEventKind::GraceScan { queue }),
        );
    }

    /// Whether all workload has drained and no deliveries are in flight.
    fn is_drained(&self) -> bool {
        self.auth.pending_wft.is_empty()
            && self.auth.pending_activities.is_empty()
            && self.broker.inflight.is_empty()
            && self.pending_reservations.is_empty()
            && self.broker.backlog.values().all(|v| v.is_empty())
            && self.broker.waiters.values().all(|w| w.is_empty())
    }

    /// Whether `id` is currently held in a reservation awaiting its start-txn
    /// commit. Such a task is legitimately accounted for (not lost) even though
    /// it is transiently absent from both `enqueued` and `inflight`.
    pub fn is_reserved(&self, id: LogicalTaskId) -> bool {
        self.pending_reservations.iter().any(|(rid, ..)| *rid == id)
    }

    /// Whether `id` sits in durable backlog (Tier C). A backlogged task is
    /// deliverable — it is redelivered when a poll consumes the backlog — so it
    /// counts as accounted-for for the no-loss invariants.
    pub fn is_in_backlog(&self, id: LogicalTaskId) -> bool {
        self.broker
            .backlog
            .values()
            .any(|items| items.iter().any(|it| it.id == id))
    }

    /// Whether `id` is deliverable, in flight, reserved, backlogged, or already
    /// completed — i.e. accounted for somewhere and not lost. This is the union
    /// the no-loss safety invariants (S5/S7) check against.
    pub fn is_accounted_for(&self, id: LogicalTaskId) -> bool {
        self.broker.enqueued.contains(&id)
            || self.broker.inflight.contains_key(&id)
            || self.auth.completed.contains(&id)
            || self.is_reserved(id)
            || self.is_in_backlog(id)
    }
}

// The pending reservations awaiting their commit event are stored in the
// `pending_reservations` field on the model.

impl StressModel for BrokerModel {
    type Event = BrokerEvent;

    fn bootstrap(&mut self, ctx: &mut SimCtx<'_, BrokerEvent>) {
        // Arm the control loop.
        ctx.schedule(20, BrokerEvent::new(BrokerEventKind::ControlLoopTick));
        // Generate a reproducible workload of publishes and polls.
        crate::workload::schedule(self, ctx);
    }

    fn handle(&mut self, event: BrokerEvent, ctx: &mut SimCtx<'_, BrokerEvent>) {
        let now = ctx.now_ms();
        match event.kind {
            BrokerEventKind::PublishWft {
                id,
                queue,
                sticky_target,
                priority,
            } => self.publish(id, queue, sticky_target, priority, now, ctx),
            BrokerEventKind::PublishActivity {
                id,
                queue,
                priority,
            } => self.publish(id, queue, None, priority, now, ctx),
            BrokerEventKind::PublishQuery {
                queue,
                sticky_target,
            } => self.publish_query(queue, sticky_target),
            BrokerEventKind::Poll { queue, worker, .. } => self.poll(queue, worker, now, ctx),
            BrokerEventKind::DirectClaim { queue, run_key } => {
                self.direct_claim(queue, run_key, now, ctx)
            }
            BrokerEventKind::ReserveAndStart { .. } => {
                // Reservations are begun inline from matches; this variant is
                // reserved for future external drivers and is a no-op here.
            }
            BrokerEventKind::StartTxnCommit {
                id,
                delivery_id,
                will_commit,
            } => self.start_txn_commit(id, delivery_id, will_commit, now, ctx),
            BrokerEventKind::CompleteTask { id, delivery_id } => {
                self.complete_task(id, delivery_id)
            }
            BrokerEventKind::GraceScan { queue } => self.grace_scan(queue, now),
            BrokerEventKind::StickyTtlExpire { id, queue } => {
                self.sticky_ttl_expire(id, queue, now, ctx)
            }
            BrokerEventKind::PollDeadline { queue, waiter_id } => {
                self.poll_deadline(queue, waiter_id)
            }
            BrokerEventKind::ControlLoopTick => self.control_loop_tick(now, ctx),
            BrokerEventKind::BrokerCrash => self.broker_crash(now, ctx),
            BrokerEventKind::LeaseExpire { id, delivery_id } => {
                self.lease_expire(id, delivery_id, now, ctx)
            }
            BrokerEventKind::WorkerCrash { worker } => {
                // Free the worker's in-flight deliveries: they lapse like a lease.
                let lapsed: Vec<(LogicalTaskId, u64)> = self
                    .broker
                    .inflight
                    .iter()
                    .filter(|(_, d)| d.worker == worker)
                    .map(|(id, d)| (*id, d.delivery_id))
                    .collect();
                for (id, did) in lapsed {
                    self.lease_expire(id, did, now, ctx);
                }
                self.signals.incr("worker_crashes");
            }
            BrokerEventKind::DenyWorker {
                namespace,
                task_queue,
                worker,
            } => {
                self.broker
                    .denied_workers
                    .insert((namespace, task_queue, worker));
                self.signals.incr("worker_denials");
            }
            BrokerEventKind::PartitionBacklogPressure { queue } => {
                // Publish a real WFT to this partition with no waiting poller, so
                // it ages into backlog — modelling cross-partition collapse with
                // genuine (deliverable, accountable) work rather than a phantom.
                let run = u64::from(u32::MAX) + self.next_backlog_seq + 1;
                self.next_backlog_seq += 1;
                self.publish(LogicalTaskId::Wft(run, 0), queue, None, 5, now, ctx);
                self.signals.incr("partition_pressure_events");
            }
            BrokerEventKind::SustainedBacklogAge { queue } => {
                let _ = queue;
                self.signals.incr("sustained_backlog_events");
            }
            BrokerEventKind::DuplicatePublish {
                id,
                queue,
                priority,
            } => self.publish(id, queue, None, priority, now, ctx),
        }

        if !self.settled && self.is_drained() {
            self.settled = true;
        } else if self.settled && !self.is_drained() {
            self.settled = false;
        }
    }

    fn signals(&self) -> &SignalCounters {
        &self.signals
    }

    fn is_quiescent(&self) -> bool {
        self.settled
    }
}
