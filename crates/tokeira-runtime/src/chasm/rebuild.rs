//! Reconstruct disposable timers and dispatch from committed root outboxes.
//! The current-pointer scan is paged and ordered; validation uses the same
//! registered handlers as transition close. Re-delivery is intentional and every
//! sink/executor must fence duplicate effects using the durable task identity.

use std::sync::Arc;

use tokeira_chasm::{DispatchableTask, LifecycleState, TaskValidity};
use tokeira_storage::{ChasmNodeRepository, CurrentExecutionCursor};

use super::{ChasmEngine, DispatchSink, ROOT_PATH, engine::TransitionContext};

/// Work attempted by one rebuild pass, including repeated derived delivery.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RebuildStats {
    /// Current executions visited in storage's deterministic order.
    pub scanned: usize,
    /// Roots with a pending pure deadline installed in the armed map.
    pub timers_armed: usize,
    /// Due, valid effects handed to the sink, including idempotent repeats.
    pub effects_dispatched: usize,
}

/// Startup and periodic recovery of derived CHASM delivery state.
pub struct OutboxRebuildScanner {
    nodes: Arc<dyn ChasmNodeRepository>,
    engine: Arc<ChasmEngine>,
    sink: Arc<dyn DispatchSink>,
    page: usize,
}

impl std::fmt::Debug for OutboxRebuildScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxRebuildScanner")
            .field("page", &self.page)
            .finish_non_exhaustive()
    }
}

impl OutboxRebuildScanner {
    /// Use bounded pages of 500 pointers. The repository and engine must refer
    /// to the same durable store, and the sink must tolerate unchanged repeats.
    pub fn new(
        nodes: Arc<dyn ChasmNodeRepository>,
        engine: Arc<ChasmEngine>,
        sink: Arc<dyn DispatchSink>,
    ) -> Self {
        Self {
            nodes,
            engine,
            sink,
            page: 500,
        }
    }

    /// Rebuild from one clock reading. No component state changes here: a raced
    /// snapshot can only produce a stale delivery, which executors/transitions
    /// must reject under their own fence. Storage or validation errors propagate.
    pub async fn rebuild_once(&self) -> anyhow::Result<RebuildStats> {
        let now = self.engine.now();
        let mut cursor = None;
        let mut stats = RebuildStats::default();
        loop {
            let page = self
                .nodes
                .scan_current_executions(LifecycleState::Running, cursor, self.page)
                .await?;
            let Some(last) = page.last() else { break };
            cursor = Some(CurrentExecutionCursor {
                namespace_id: last.key.namespace_id.clone(),
                archetype_id: last.archetype_id,
                business_id: last.key.business_id.clone(),
            });
            for execution in page {
                stats.scanned += 1;
                let key = execution.key;
                let nodes = self.nodes.load_execution(&key).await?;
                let Some((_, root)) = nodes.into_iter().find(|(path, _)| path == ROOT_PATH) else {
                    self.engine.set_armed_timer(&key, None);
                    continue;
                };
                // The live root is authoritative even if a pointer became stale
                // between the page read and this load.
                if root
                    .metadata
                    .lifecycle_state
                    .is_some_and(LifecycleState::is_closed)
                {
                    self.engine.set_armed_timer(&key, None);
                    continue;
                }
                let deadline = root.metadata.outbox.earliest_pure_deadline();
                self.engine.set_armed_timer(&key, deadline);
                stats.timers_armed += usize::from(deadline.is_some());
                let data = root
                    .data
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("CHASM root has no data: {key:?}"))?;
                let ctx =
                    TransitionContext::new(key.clone(), root.metadata.versioned_transition, now);
                let mut tasks = Vec::new();
                for task in root.metadata.outbox.side_effect_tasks {
                    if task.fire_at_unix_nanos.is_none_or(|at| at <= now)
                        && self.engine.registry().validate_task(
                            root.metadata.component_type_id,
                            data,
                            &task,
                            &ctx,
                        )? == TaskValidity::Valid
                    {
                        tasks.push(DispatchableTask {
                            node_path: ROOT_PATH.to_vec(),
                            task,
                        });
                    }
                }
                stats.effects_dispatched += tasks.len();
                if !tasks.is_empty()
                    && let Err(error) = self.sink.dispatch(&key, tasks).await
                {
                    tracing::warn!(
                        ?error,
                        ?key,
                        "CHASM rebuild dispatch failed; tasks remain pending"
                    );
                }
            }
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chasm::{
        ChasmTimerSweeper, SideEffectExecutor,
        test_support::{self as ts, Work},
    };
    use proptest::prelude::*;
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    use tokeira_chasm::Task;
    use tokeira_storage::InMemoryChasmNodeStore;

    // Feature: chasm-extension-archetypes, Property 4: timer rehydration round-trip
    // Restart re-arms exactly the unconsumed timers; committed executions never repeat.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn timers_survive_generated_crashes(deadlines in prop::collection::vec(1i64..100, 0..25), crashes in prop::collection::vec((0i64..25, any::<bool>()), 1..12)) {
            ts::runtime().block_on(async {
                let repo = Arc::new(InMemoryChasmNodeStore::new());
                let now = Arc::new(AtomicI64::new(0));
                let mut engine = ts::engine(repo.clone(), now.clone(), ts::sink());
                let key = ts::key(0);
                let reference = ts::start(&engine, key.clone()).await;
                let tasks: Vec<_> = deadlines.iter().enumerate().map(|(i, &deadline)| Work::<false> { token: i as u32, deadline, ..Default::default() }).collect();
                ts::stage(&engine, &reference, &tasks).await;
                let mut pending = tasks.clone();
                let mut expected = Vec::new();
                let mut time = 0;
                for (delta, crash_before_sweep) in crashes.into_iter().chain([(200, true)]) {
                    time += delta;
                    now.store(time, Ordering::SeqCst);
                    if !crash_before_sweep {
                        ChasmTimerSweeper::new(engine.clone()).sweep_once().await;
                        let mut due: Vec<_> = pending.iter().filter(|t| t.deadline <= time).collect();
                        due.sort_by_key(|t| (t.deadline, t.token));
                        expected.extend(due.into_iter().map(|t| t.token));
                        pending.retain(|t| t.deadline > time);
                    }
                    drop(engine);
                    engine = ts::engine(repo.clone(), now.clone(), ts::sink());
                    prop_assert_eq!(engine.armed_timer(&key), None);
                    let scanner = OutboxRebuildScanner::new(repo.clone(), engine.clone(), ts::sink());
                    scanner.rebuild_once().await.unwrap();
                    prop_assert_eq!(engine.armed_timer(&key), pending.iter().map(|t| t.deadline).min());
                    ChasmTimerSweeper::new(engine.clone()).sweep_once().await;
                    let mut due: Vec<_> = pending.iter().filter(|t| t.deadline <= time).collect();
                    due.sort_by_key(|t| (t.deadline, t.token));
                    expected.extend(due.into_iter().map(|t| t.token));
                    pending.retain(|t| t.deadline > time);
                    prop_assert_eq!(ts::data(&engine, &key).await.pure, expected.clone());
                }
                Ok(())
            })?;
        }
    }

    // Feature: chasm-extension-archetypes, Property 5: dispatch derived from state
    // Each scan derives the same ordered pending effects; executor repeats are inert.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn dispatch_rebuild_matches_committed_model(groups in prop::collection::vec(prop::collection::vec((0i64..80, 0i64..100), 0..12), 1..8), time in 0i64..100) {
            ts::runtime().block_on(async {
                let repo = Arc::new(InMemoryChasmNodeStore::new());
                let now = Arc::new(AtomicI64::new(0));
                let engine = ts::engine(repo.clone(), now.clone(), ts::sink());
                for (index, tasks) in groups.iter().enumerate().rev() {
                    let reference = ts::start(&engine, ts::key(index)).await;
                    let tasks: Vec<_> = tasks.iter().enumerate().map(|(i, &(deadline, drop_at))| Work::<true> { token: i as u32, deadline, drop_at, ..Default::default() }).collect();
                    ts::stage(&engine, &reference, &tasks).await;
                }
                drop(engine);
                now.store(time, Ordering::SeqCst);
                let sink = ts::sink();
                let engine = ts::engine(repo.clone(), now, sink.clone());
                let scanner = OutboxRebuildScanner { nodes: repo, engine, sink: sink.clone(), page: 2 };
                let expected: Vec<_> = groups.iter().enumerate().flat_map(|(index, tasks)| tasks.iter().enumerate().filter_map(move |(token, &(deadline, drop_at))| {
                    ((deadline == 0 || deadline <= time) && ts::valid(drop_at, time)).then_some((ts::key(index), token as u32))
                })).collect();
                let executor = ts::IdempotentExecutor::default();
                for _ in 0..2 {
                    let stats = scanner.rebuild_once().await.unwrap();
                    prop_assert_eq!(stats.scanned, groups.len());
                    prop_assert_eq!(stats.effects_dispatched, expected.len());
                    let dispatched = std::mem::take(&mut *sink.dispatched.lock().unwrap());
                    let actual: Vec<_> = dispatched.iter().map(|(key, task)| (key.clone(), <Work<true> as Task>::decode(&task.task.payload).unwrap().token)).collect();
                    prop_assert_eq!(actual, expected.clone());
                    for (key, task) in dispatched { executor.execute(&key, &task.task).await.unwrap(); }
                    prop_assert_eq!(executor.effects.lock().unwrap().len(), expected.len());
                }
                Ok(())
            })?;
        }
    }
}
