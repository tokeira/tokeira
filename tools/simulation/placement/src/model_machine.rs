//! The placement state machine: [`PlacementModel`] implementing the engine's
//! [`StressModel`].
//!
//! This is the seeded-stress counterpart to the `Sim` the original
//! `placement-sim` ran inline. It owns the authoritative [`Dsql`], the advisory
//! [`Edge`], the runtimes and controllers, and drives them through the event
//! taxonomy in [`crate::events`]. Two design points carry the whole simulator:
//!
//! 1. **The fence is checked at commit, not at routing.** An `EdgeOp` is routed
//!    optimistically off the advisory snapshot; the runtime only *begins* a
//!    transaction at `RuntimeHandle`; the OCC fence is evaluated later at
//!    `CommitAttempt`. A lease that changes hands in that window fences the
//!    commit out — so a stale route causes a retry, never a wrong write.
//!
//! 2. **Detected violations are recorded, not thrown.** The engine checks
//!    invariants as pure predicates over model state after every event, so the
//!    two conditions the original detected inline (a commit that resolved off a
//!    workflow's execution-home, and a drain that relinquished before routing
//!    was updated) are recorded into [`PlacementModel`] fields that the I1 and
//!    I6 invariant predicates read. This keeps detection in the invariant layer
//!    where the engine expects it.

use std::collections::{HashMap, HashSet};

use sim_engine::{SignalCounters, SimCtx, StressModel};

use crate::{
    events::{PlacementEvent, PlacementEventKind},
    model::{
        execution_home, queue_partition_for, BundleId, ClientOp, CommitOutcome, Controller,
        ControllerId, Dsql, Edge, OpKind, PlacementCfg, RequestId, Runtime, RuntimeId,
        MAX_EDGE_RETRIES,
    },
    workload,
};

/// The placement model: authoritative store, advisory caches, and the cluster
/// of runtimes/controllers, plus the signal counters and the recorded-violation
/// fields the invariants read.
#[derive(Clone, Debug)]
pub struct PlacementModel {
    /// Topology and timing knobs (and the buggy-routing toggle).
    pub cfg: PlacementCfg,
    /// The authoritative store — the only source of truth.
    pub dsql: Dsql,
    /// The advisory edge routing cache.
    pub edge: Edge,
    /// Runtimes keyed by incarnation id.
    pub runtimes: HashMap<RuntimeId, Runtime>,
    /// Controllers keyed by id.
    pub controllers: HashMap<ControllerId, Controller>,
    /// Accumulated named signals for the report.
    pub signals: SignalCounters,
    /// Next runtime incarnation id to mint on restart.
    next_runtime_id: u64,
    /// Next client request id to mint.
    next_request_id: u64,
    /// Draining runtimes whose routing has already been updated (drain phase 1),
    /// so the I6 predicate can tell a correctly-ordered relinquish from an
    /// out-of-order one.
    drain_routing_updated: HashSet<RuntimeId>,
    /// Set when a commit resolved off a workflow's execution-home (the
    /// buggy-routing detector). Read by invariant I1.
    wrong_execution_home: Option<String>,
    /// Set when a draining runtime relinquished a bundle before its routing was
    /// updated. Read by invariant I6.
    i6_violation: Option<String>,
    /// The current simulated time, refreshed at the top of every `handle`. The
    /// engine owns the clock; the model mirrors it here so the pure invariant
    /// predicates (notably I5's lease-expiry check) can compare against "now".
    now_ms: u64,
}

impl PlacementModel {
    /// Construct the initial cluster: `runtime_count` live runtimes and
    /// `controller_count` live controllers, an empty DSQL and edge. No RNG is
    /// consumed here — all randomness happens in `bootstrap`, so the engine's
    /// seed fully determines the run.
    pub fn new(cfg: PlacementCfg) -> Self {
        let mut runtimes = HashMap::new();
        for n in 0..cfg.runtime_count {
            let id = RuntimeId(n as u64 + 1);
            runtimes.insert(id, Runtime::new(id));
        }
        let mut controllers = HashMap::new();
        for n in 0..cfg.controller_count {
            let id = ControllerId(n as u64 + 1);
            controllers.insert(id, Controller { id, alive: true });
        }
        let dsql = Dsql::new(cfg.bundle_count);
        let edge = Edge::new(cfg.bundle_count, cfg.queue_partitions);
        Self {
            next_runtime_id: cfg.runtime_count as u64 + 1,
            next_request_id: 1,
            cfg,
            dsql,
            edge,
            runtimes,
            controllers,
            signals: SignalCounters::new(),
            drain_routing_updated: HashSet::new(),
            wrong_execution_home: None,
            i6_violation: None,
            now_ms: 0,
        }
    }

    /// The model's mirror of the engine's current simulated time.
    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// The first recorded execution-home violation, if any (read by I1).
    pub fn wrong_execution_home(&self) -> Option<&str> {
        self.wrong_execution_home.as_deref()
    }

    /// The first recorded drain-ordering violation, if any (read by I6).
    pub fn i6_violation(&self) -> Option<&str> {
        self.i6_violation.as_deref()
    }

    /// Mint the next client request id.
    pub fn next_request_id(&mut self) -> RequestId {
        let id = RequestId(self.next_request_id);
        self.next_request_id += 1;
        id
    }

    /// Live runtime ids, sorted for deterministic selection.
    pub fn live_runtime_ids(&self) -> Vec<RuntimeId> {
        let mut ids: Vec<_> = self
            .runtimes
            .values()
            .filter(|rt| rt.alive)
            .map(|rt| rt.id)
            .collect();
        ids.sort();
        ids
    }

    /// Pick a random live runtime via the supplied RNG (drawn from `SimCtx`).
    pub fn random_runtime_id(&self, rng: &mut sim_engine::Rng) -> Option<RuntimeId> {
        let ids = self.live_runtime_ids();
        if ids.is_empty() {
            None
        } else {
            Some(ids[rng.range(0, ids.len() as u64) as usize])
        }
    }

    /// Resolve which bundle an operation routes to.
    ///
    /// Correct routing always uses the workflow's execution-home. The buggy
    /// variant routes `Start` by *queue-home* instead — the single deliberate
    /// defect that must trip invariant I1, since queue-home and execution-home
    /// are independent hashes that (almost always) disagree.
    pub fn resolve_operation_bundle(&self, op: ClientOp) -> BundleId {
        match op.kind {
            OpKind::Start if self.cfg.buggy_start_routing => {
                let partition = queue_partition_for(op.workflow_id, self.cfg.queue_partitions);
                BundleId(partition.0 % self.cfg.bundle_count)
            }
            _ => execution_home(op.workflow_id, self.cfg.bundle_count),
        }
    }

    /// Format the most recent commit-log entries newest-first, for embedding in
    /// a violation reason so a failing seed is diagnosable from the report alone.
    pub fn recent_commits_tail(&self) -> String {
        let mut out = String::from(" recent commits (newest first):");
        for entry in self.dsql.commit_log.iter().rev().take(8) {
            out.push_str(&format!(
                " [t={} rt={} bundle={} epoch={} op={:?} -> {:?}]",
                entry.time_ms,
                entry.runtime_id.0,
                entry.bundle_id.0,
                entry.epoch.0,
                entry.op.kind,
                entry.outcome
            ));
        }
        out
    }
}

impl StressModel for PlacementModel {
    type Event = PlacementEvent;

    /// Seed the queue: start each controller observing at t=0, schedule an
    /// initial acquisition per bundle, then lay down the full randomised
    /// workload + fault schedule (all via the seeded RNG in `ctx`).
    fn bootstrap(&mut self, ctx: &mut SimCtx<'_, Self::Event>) {
        let controllers: Vec<_> = self.controllers.keys().copied().collect();
        let mut controllers = controllers;
        controllers.sort();
        for c in controllers {
            ctx.schedule(
                0,
                PlacementEvent::new(PlacementEventKind::ControllerObserve { controller: c }),
            );
        }
        let runtime_ids = self.live_runtime_ids();
        for bundle_idx in 0..self.cfg.bundle_count {
            let runtime = runtime_ids[bundle_idx % runtime_ids.len()];
            let d = ctx.rng().range(0, 10);
            ctx.schedule(
                d,
                PlacementEvent::new(PlacementEventKind::RuntimeAcquire {
                    runtime,
                    bundle: BundleId(bundle_idx),
                }),
            );
        }
        workload::schedule(self, ctx);
    }

    /// Apply one event. The only place model state changes; follow-on events are
    /// scheduled back through `ctx`.
    fn handle(&mut self, event: Self::Event, ctx: &mut SimCtx<'_, Self::Event>) {
        // Mirror the engine clock so pure invariant predicates can read "now".
        self.now_ms = ctx.now_ms();
        match event.kind {
            PlacementEventKind::ControllerObserve { controller } => {
                let Some(c) = self.controllers.get(&controller) else {
                    return;
                };
                if !c.alive {
                    return;
                }
                let live = self.live_runtime_ids();
                let prev_gen = self.dsql.routing_generation;
                let snapshot = self.dsql.build_snapshot(
                    ctx.now_ms(),
                    self.cfg.bundle_count,
                    self.cfg.queue_partitions,
                    &live,
                );
                if self.dsql.routing_generation != prev_gen {
                    self.signals.incr("routing_publications");
                }
                let delivery_delay = ctx.rng().range(1, 60);
                ctx.schedule(
                    delivery_delay,
                    PlacementEvent::new(PlacementEventKind::EdgeApplySnapshot { snapshot }),
                );
                // Cover any unowned bundle by nudging a deterministic live
                // runtime to acquire it (offset by controller id so active-active
                // controllers don't all pick the same target).
                if !live.is_empty() {
                    for bundle_idx in 0..self.cfg.bundle_count {
                        let bundle = BundleId(bundle_idx);
                        if self.dsql.current_route(ctx.now_ms(), bundle).is_none() {
                            let runtime = live[(bundle_idx + controller.0 as usize) % live.len()];
                            let d = ctx.rng().range(1, 20);
                            ctx.schedule(
                                d,
                                PlacementEvent::new(PlacementEventKind::RuntimeAcquire {
                                    runtime,
                                    bundle,
                                }),
                            );
                        }
                    }
                }
                let d = self.cfg.controller_observe_ms + ctx.rng().range(0, 15);
                ctx.schedule(
                    d,
                    PlacementEvent::new(PlacementEventKind::ControllerObserve { controller }),
                );
            }
            PlacementEventKind::EdgeApplySnapshot { snapshot } => {
                self.edge.snapshot.apply_full_snapshot(snapshot);
            }
            PlacementEventKind::RuntimeAcquire { runtime, bundle } => {
                let Some(rt) = self.runtimes.get(&runtime) else {
                    return;
                };
                if !rt.alive {
                    return;
                }
                if let Some(epoch) =
                    self.dsql
                        .acquire_bundle(ctx.now_ms(), bundle, runtime, self.cfg.lease_ms)
                {
                    let rt = self
                        .runtimes
                        .get_mut(&runtime)
                        .expect("runtime disappeared");
                    rt.local_owned.insert(bundle, epoch);
                    ctx.schedule(
                        self.cfg.renew_ms,
                        PlacementEvent::new(PlacementEventKind::RuntimeRenew {
                            runtime,
                            bundle,
                            epoch,
                        }),
                    );
                }
            }
            PlacementEventKind::RuntimeRenew {
                runtime,
                bundle,
                epoch,
            } => {
                let Some(rt) = self.runtimes.get(&runtime) else {
                    return;
                };
                if !rt.alive {
                    return;
                }
                // Renewals suppressed (fault): keep trying to renew on schedule,
                // but the lease will lapse meanwhile — the adversarial case the
                // fence must survive.
                if !rt.renewals_enabled {
                    ctx.schedule(
                        self.cfg.renew_ms,
                        PlacementEvent::new(PlacementEventKind::RuntimeRenew {
                            runtime,
                            bundle,
                            epoch,
                        }),
                    );
                    return;
                }
                let renewed =
                    self.dsql
                        .renew_bundle(ctx.now_ms(), bundle, runtime, epoch, self.cfg.lease_ms);
                if renewed {
                    ctx.schedule(
                        self.cfg.renew_ms,
                        PlacementEvent::new(PlacementEventKind::RuntimeRenew {
                            runtime,
                            bundle,
                            epoch,
                        }),
                    );
                } else if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    // Lost the lease: drop the stale local belief so the runtime
                    // stops attempting commits it can no longer fence.
                    rt.local_owned.remove(&bundle);
                }
            }
            PlacementEventKind::RuntimeRelinquish {
                runtime,
                bundle,
                epoch,
            } => {
                // I6: a draining runtime must not relinquish a bundle until its
                // routing has been withdrawn (phase 1), or in-flight ops could be
                // sent to a node that just dropped the lease. Record the breach
                // for the invariant rather than throwing.
                if let Some(rt) = self.runtimes.get(&runtime) {
                    if rt.draining && !self.drain_routing_updated.contains(&runtime) {
                        if self.i6_violation.is_none() {
                            self.i6_violation = Some(format!(
                                "runtime {} relinquished bundle {} before its drain routing update.{}",
                                runtime.0,
                                bundle.0,
                                self.recent_commits_tail()
                            ));
                        }
                        return;
                    }
                }
                let _ = self
                    .dsql
                    .relinquish_bundle(ctx.now_ms(), bundle, runtime, epoch);
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    rt.local_owned.remove(&bundle);
                }
            }
            PlacementEventKind::EdgeOp { op, attempt } => {
                let bundle = self.resolve_operation_bundle(op);
                match self.edge.snapshot.route_for_bundle(bundle) {
                    Some(route) => {
                        let d = ctx.rng().range(1, 12);
                        ctx.schedule(
                            d,
                            PlacementEvent::new(PlacementEventKind::RuntimeHandle {
                                runtime: route.owner,
                                op,
                                bundle,
                                observed_epoch: route.epoch,
                                attempt,
                            }),
                        );
                    }
                    None => {
                        ctx.schedule(
                            1,
                            PlacementEvent::new(PlacementEventKind::EdgeRepairAndRetry {
                                op,
                                bundle,
                                attempt,
                            }),
                        );
                    }
                }
            }
            PlacementEventKind::RuntimeHandle {
                runtime,
                op,
                bundle,
                observed_epoch,
                attempt,
            } => {
                let local_ok = self
                    .runtimes
                    .get(&runtime)
                    .map(|rt| rt.locally_owns(bundle, observed_epoch))
                    .unwrap_or(false);
                if !local_ok {
                    // The runtime no longer believes it owns this at the routed
                    // epoch (NotShardOwner): repair the edge and retry.
                    self.signals.incr("not_shard_owner");
                    ctx.schedule(
                        1,
                        PlacementEvent::new(PlacementEventKind::EdgeRepairAndRetry {
                            op,
                            bundle,
                            attempt,
                        }),
                    );
                    return;
                }
                // Begin the OCC transaction now; the fence is validated at the
                // delayed CommitAttempt, opening the lease-change race window.
                let tx_id = self.dsql.begin_transaction(bundle);
                let d = ctx.rng().range(1, 5);
                ctx.schedule(
                    d,
                    PlacementEvent::new(PlacementEventKind::CommitAttempt {
                        runtime,
                        op,
                        bundle,
                        observed_epoch,
                        attempt,
                        tx_id,
                    }),
                );
            }
            PlacementEventKind::CommitAttempt {
                runtime,
                op,
                bundle,
                observed_epoch,
                attempt,
                tx_id,
            } => {
                // The runtime may have crashed during the transaction delay; if
                // so, abandon the read-set and the commit.
                let still_alive = self
                    .runtimes
                    .get(&runtime)
                    .map(|rt| rt.alive)
                    .unwrap_or(false);
                if !still_alive {
                    self.dsql.abort_transaction(bundle, tx_id);
                    return;
                }
                self.signals.incr("commit_attempts");
                let outcome = self.dsql.commit(
                    ctx.now_ms(),
                    runtime,
                    bundle,
                    observed_epoch,
                    op,
                    self.cfg.bundle_count,
                    Some(tx_id),
                );
                match outcome {
                    CommitOutcome::Applied => {
                        self.signals.incr("successful_mutations");
                        match op.kind {
                            OpKind::Start => self.signals.incr("workflows_started"),
                            OpKind::Signal => self.signals.incr("signals_applied"),
                        }
                    }
                    CommitOutcome::AlreadyExists | CommitOutcome::Duplicate => {
                        self.signals.incr("idempotent_noops");
                    }
                    CommitOutcome::FenceRejected => {
                        // The lease moved under the transaction: drop stale local
                        // belief, count it, and repair+retry.
                        self.signals.incr("fence_rejections");
                        if let Some(rt) = self.runtimes.get_mut(&runtime) {
                            rt.local_owned.remove(&bundle);
                        }
                        self.signals.incr("not_shard_owner");
                        ctx.schedule(
                            1,
                            PlacementEvent::new(PlacementEventKind::EdgeRepairAndRetry {
                                op,
                                bundle,
                                attempt,
                            }),
                        );
                    }
                    CommitOutcome::NotFound => {
                        self.signals.incr("signal_not_found");
                    }
                    CommitOutcome::WrongExecutionHome => {
                        // The op committed against a bundle that is not the
                        // workflow's execution-home. This is the I1 falsification
                        // (and the buggy-start-routing detector). Record it.
                        if self.wrong_execution_home.is_none() {
                            self.wrong_execution_home = Some(format!(
                                "op {:?} for workflow {} routed to bundle {} but its execution-home is {}.{}",
                                op.kind,
                                op.workflow_id.0,
                                bundle.0,
                                execution_home(op.workflow_id, self.cfg.bundle_count).0,
                                self.recent_commits_tail()
                            ));
                        }
                    }
                }
            }
            PlacementEventKind::EdgeRepairAndRetry {
                op,
                bundle,
                attempt,
            } => {
                self.signals.incr("edge_repairs");
                // Model the controller RefreshBundle RPC latency before the
                // repair resolves against live DSQL.
                let d = ctx.rng().range(5, 30);
                ctx.schedule(
                    d,
                    PlacementEvent::new(PlacementEventKind::EdgeRepairResolve {
                        op,
                        bundle,
                        attempt,
                    }),
                );
            }
            PlacementEventKind::EdgeRepairResolve {
                op,
                bundle,
                attempt,
            } => {
                let route = self.dsql.current_route(ctx.now_ms(), bundle);
                self.edge.snapshot.repair_bundle(bundle, route);
                if attempt < MAX_EDGE_RETRIES {
                    let d = ctx.rng().range(1, 10);
                    ctx.schedule(
                        d,
                        PlacementEvent::new(PlacementEventKind::EdgeOp {
                            op,
                            attempt: attempt + 1,
                        }),
                    );
                }
            }
            PlacementEventKind::DisableRenewals {
                runtime,
                duration_ms,
            } => {
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    if rt.alive {
                        rt.renewals_enabled = false;
                        self.signals.incr("renewal_suspensions");
                        ctx.schedule(
                            duration_ms,
                            PlacementEvent::new(PlacementEventKind::EnableRenewals { runtime }),
                        );
                    }
                }
            }
            PlacementEventKind::EnableRenewals { runtime } => {
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    if rt.alive {
                        rt.renewals_enabled = true;
                    }
                }
            }
            PlacementEventKind::CrashRuntime {
                runtime,
                restart_delay_ms,
            } => {
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    rt.alive = false;
                    rt.renewals_enabled = false;
                    rt.local_owned.clear();
                    self.signals.incr("crashes");
                    ctx.schedule(
                        restart_delay_ms,
                        PlacementEvent::new(PlacementEventKind::RestartRuntime {
                            old_runtime: runtime,
                        }),
                    );
                }
            }
            PlacementEventKind::RestartRuntime { old_runtime: _ } => {
                // A crash/restart is a *new incarnation* with a fresh id, so no
                // stale reference to the dead runtime can ever alias it.
                let new_id = RuntimeId(self.next_runtime_id);
                self.next_runtime_id += 1;
                self.runtimes.insert(new_id, Runtime::new(new_id));
            }
            PlacementEventKind::BeginDrain { runtime } => {
                let Some(rt) = self.runtimes.get(&runtime).cloned() else {
                    return;
                };
                if !rt.alive || rt.draining {
                    return;
                }
                if let Some(rt_mut) = self.runtimes.get_mut(&runtime) {
                    rt_mut.renewals_enabled = false;
                    rt_mut.draining = true;
                }
                self.signals.incr("drains");
                // Phase 1 first: update routing, then relinquish (I6 ordering).
                let d = ctx.rng().range(2, 8);
                ctx.schedule(
                    d,
                    PlacementEvent::new(PlacementEventKind::DrainRoutingUpdate { runtime }),
                );
            }
            PlacementEventKind::DrainRoutingUpdate { runtime } => {
                let Some(rt) = self.runtimes.get(&runtime) else {
                    return;
                };
                if !rt.alive || !rt.draining {
                    return;
                }
                // Withdraw the draining runtime from the edge routing so no new
                // op is sent to it, *before* it gives up its leases.
                self.edge
                    .snapshot
                    .bundle_routes
                    .iter_mut()
                    .for_each(|slot| {
                        if let Some(route) = slot {
                            if route.owner == runtime {
                                *slot = None;
                            }
                        }
                    });
                self.drain_routing_updated.insert(runtime);
                let d = ctx.rng().range(1, 5);
                ctx.schedule(
                    d,
                    PlacementEvent::new(PlacementEventKind::DrainRelinquishPhase { runtime }),
                );
            }
            PlacementEventKind::DrainRelinquishPhase { runtime } => {
                let Some(rt) = self.runtimes.get(&runtime).cloned() else {
                    return;
                };
                if !rt.alive || !rt.draining {
                    return;
                }
                // Iterate owned bundles in a deterministic (sorted) order:
                // `local_owned` is a HashMap, and scheduling draws an RNG delay
                // per bundle, so an unsorted walk would pair delays with bundles
                // nondeterministically and break seed reproducibility.
                let mut owned: Vec<_> = rt.local_owned.into_iter().collect();
                owned.sort_by_key(|(bundle, _)| bundle.0);
                for (bundle, epoch) in owned {
                    let d = ctx.rng().range(1, 20);
                    ctx.schedule(
                        d,
                        PlacementEvent::new(PlacementEventKind::RuntimeRelinquish {
                            runtime,
                            bundle,
                            epoch,
                        }),
                    );
                }
            }
        }
    }

    fn signals(&self) -> &SignalCounters {
        &self.signals
    }
}
