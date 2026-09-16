//! Reconstruct disposable timers and dispatch from committed root outboxes.
//! The current-pointer scan is paged and ordered; validation uses the same
//! registered handlers as transition close. Re-delivery is intentional and every
//! sink/executor must fence duplicate effects using the durable task identity.

use std::sync::Arc;

use tokeira_chasm::{DispatchableTask, LifecycleState, TaskValidity};
use tokeira_storage::{ChasmNodeRepository, CurrentExecutionCursor};

use super::{ChasmEngine, DispatchSink, ROOT_PATH, engine::TransitionContext};

/// Why a persisted execution cannot be reconstructed by the loaded library.
/// Each cause carries a task identity only when one exists in stored work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildFailure {
    /// The root exists but has no component bytes.
    MissingRootData,
    /// The registry has no handler for this persisted task type.
    UnknownTaskType {
        /// Type id persisted in the unserviceable task.
        task_type_id: u32,
    },
    /// The registered handler cannot decode the persisted component or task bytes.
    UndecodablePayload {
        /// Type id whose handler rejected the persisted bytes.
        task_type_id: u32,
    },
}

impl std::fmt::Display for RebuildFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRootData => formatter.write_str("root component data is missing"),
            Self::UnknownTaskType { task_type_id } => write!(
                formatter,
                "persisted task type {task_type_id} has no registered handler"
            ),
            Self::UndecodablePayload { task_type_id } => write!(
                formatter,
                "the handler for task type {task_type_id} cannot decode its persisted payload"
            ),
        }
    }
}

/// Work attempted by one rebuild pass, including repeated derived delivery.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RebuildStats {
    /// Current executions visited in storage's deterministic order.
    pub scanned: usize,
    /// Roots with a pending pure deadline installed in the armed map.
    pub timers_armed: usize,
    /// Due, valid effects handed to the sink, including idempotent repeats.
    pub effects_dispatched: usize,
    /// Executions whose root data or task handlers could not be validated.
    pub unserviceable: usize,
    /// First failing archetype and its explicit cause in deterministic scan order.
    pub first_unserviceable: Option<(u32, RebuildFailure)>,
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
    /// must reject under their own fence. Storage errors propagate; malformed
    /// root data and task-validation errors isolate one execution and are counted
    /// so startup can fail closed while periodic passes continue healing others.
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
                let archetype_id = root.metadata.component_type_id;
                let Some(data) = root.data.as_deref() else {
                    self.engine.set_armed_timer(&key, None);
                    stats.unserviceable += 1;
                    stats
                        .first_unserviceable
                        .get_or_insert((archetype_id, RebuildFailure::MissingRootData));
                    tracing::error!(
                        ?key,
                        archetype_id,
                        "CHASM rebuild cannot serve root with missing data"
                    );
                    continue;
                };
                let ctx =
                    TransitionContext::new(key.clone(), root.metadata.versioned_transition, now);
                let mut tasks = Vec::new();
                let mut failure = None;
                // Validate future and pure tasks too: startup must not silently
                // admit storage whose missing handler is hidden by a later deadline.
                // Publish no derived work from an execution until every task checks.
                for task in root
                    .metadata
                    .outbox
                    .pure_tasks
                    .iter()
                    .chain(&root.metadata.outbox.side_effect_tasks)
                {
                    match self
                        .engine
                        .registry()
                        .validate_task(archetype_id, data, task, &ctx)
                    {
                        Ok(TaskValidity::Valid)
                            if task.kind == tokeira_chasm::TaskKind::SideEffect
                                && task.fire_at_unix_nanos.is_none_or(|at| at <= now) =>
                        {
                            tasks.push(DispatchableTask {
                                node_path: ROOT_PATH.to_vec(),
                                task: task.clone(),
                            });
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let cause = match error {
                                tokeira_chasm::ChasmError::UnknownTaskType { .. } => {
                                    RebuildFailure::UnknownTaskType {
                                        task_type_id: task.task_type_id,
                                    }
                                }
                                _ => RebuildFailure::UndecodablePayload {
                                    task_type_id: task.task_type_id,
                                },
                            };
                            failure = Some((cause, error));
                            break;
                        }
                    }
                }
                if let Some((cause, error)) = failure {
                    self.engine.set_armed_timer(&key, None);
                    stats.unserviceable += 1;
                    stats
                        .first_unserviceable
                        .get_or_insert((archetype_id, cause));
                    tracing::error!(?key, archetype_id, %cause, ?error, "CHASM rebuild cannot serve execution; continuing pass");
                    continue;
                }
                let deadline = root.metadata.outbox.earliest_pure_deadline();
                self.engine.set_armed_timer(&key, deadline);
                stats.timers_armed += usize::from(deadline.is_some());
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

    #[tokio::test]
    async fn unserviceable_execution_does_not_block_healthy_rebuild() {
        for failure_kind in 0..3 {
            let repo = Arc::new(InMemoryChasmNodeStore::new());
            let engine = ts::engine(repo.clone(), Arc::new(AtomicI64::new(2)), ts::sink());
            for index in 0..2 {
                let reference = ts::start(&engine, ts::key(index)).await;
                ts::stage(
                    &engine,
                    &reference,
                    &[Work::<false> {
                        deadline: 10,
                        ..Default::default()
                    }],
                )
                .await;
                ts::stage(&engine, &reference, &[Work::<true>::default()]).await;
            }
            let mut root = engine.root_node(&ts::key(0)).await.unwrap().unwrap();
            let archetype = root.metadata.component_type_id;
            let expected = tokeira_storage::ExpectedVersion::Vt(root.metadata.versioned_transition);
            let unknown = u32::MAX;
            if failure_kind == 0 {
                root.data = None;
                root.metadata.outbox.pure_tasks.clear();
                root.metadata.outbox.side_effect_tasks.clear();
            } else if failure_kind == 1 {
                root.metadata.outbox.pure_tasks[0].task_type_id = unknown;
            } else {
                root.metadata.outbox.pure_tasks[0].payload = vec![0xff];
            }
            let cause = match failure_kind {
                0 => RebuildFailure::MissingRootData,
                1 => RebuildFailure::UnknownTaskType {
                    task_type_id: unknown,
                },
                _ => RebuildFailure::UndecodablePayload {
                    task_type_id: root.metadata.outbox.pure_tasks[0].task_type_id,
                },
            };
            repo.persist_dirty(
                &ts::key(0),
                vec![tokeira_storage::NodeWrite {
                    encoded_path: ROOT_PATH.to_vec(),
                    node: root,
                    expected,
                }],
            )
            .await
            .unwrap();
            let sink = ts::sink();
            let engine = ts::engine(repo.clone(), Arc::new(AtomicI64::new(2)), sink.clone());
            let scanner = OutboxRebuildScanner::new(repo, engine.clone(), sink.clone());
            let stats = scanner.rebuild_once().await.unwrap();
            assert_eq!(stats.scanned, 2);
            assert_eq!(stats.unserviceable, 1);
            assert_eq!(stats.first_unserviceable, Some((archetype, cause)));
            assert_eq!(stats.timers_armed, 1);
            assert_eq!(stats.effects_dispatched, 1);
            assert_eq!(engine.armed_timer(&ts::key(0)), None);
            assert_eq!(engine.armed_timer(&ts::key(1)), Some(10));
            assert_eq!(sink.dispatched.lock().unwrap()[0].0, ts::key(1));
        }
    }

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
