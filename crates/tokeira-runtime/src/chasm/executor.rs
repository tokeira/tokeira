//! Post-commit side-effect routing. Executors perform I/O while the committed
//! outbox remains the authority for pending work; a rebuild may deliver it again.
//! Failures are logged per task and never turn a successful commit into a failure.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokeira_chasm::{ChasmError, DispatchableTask, ExecutionKey, ScheduledTask};

use super::DispatchSink;

/// Runtime I/O for one durable task type, outside the component transition.
/// Implementations must be idempotent: repeated delivery of an unchanged task
/// must produce no second effect. Outcome mutation re-enters a fenced transition.
#[async_trait]
pub trait SideEffectExecutor: Send + Sync {
    /// Durable task type routed to this executor.
    fn task_type_id(&self) -> u32;

    /// Perform the effect, retaining its durable identity as the idempotence key.
    async fn execute(&self, key: &ExecutionKey, task: &ScheduledTask) -> anyhow::Result<()>;
}

/// Immutable-after-bootstrap task routing. Registration rejects ambiguous ids;
/// dispatch preserves the caller's order and isolates failures between tasks.
#[derive(Default)]
pub struct DispatchMultiplexer {
    executors: HashMap<u32, Arc<dyn SideEffectExecutor>>,
}

impl std::fmt::Debug for DispatchMultiplexer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchMultiplexer")
            .field("executors", &self.executors.len())
            .finish_non_exhaustive()
    }
}

impl DispatchMultiplexer {
    /// Register one executor, rejecting a duplicate id without replacing it.
    pub fn register(&mut self, executor: Arc<dyn SideEffectExecutor>) -> Result<(), ChasmError> {
        let id = executor.task_type_id();
        if self.executors.contains_key(&id) {
            return Err(ChasmError::Validation(format!(
                "executor for task type {id} is already registered"
            )));
        }
        self.executors.insert(id, executor);
        Ok(())
    }

    /// Executor for a task type, or `None` when this runtime cannot deliver it.
    pub fn executor(&self, task_type_id: u32) -> Option<&Arc<dyn SideEffectExecutor>> {
        self.executors.get(&task_type_id)
    }
}

#[async_trait]
impl DispatchSink for DispatchMultiplexer {
    async fn dispatch(
        &self,
        key: &ExecutionKey,
        tasks: Vec<DispatchableTask>,
    ) -> anyhow::Result<()> {
        for dispatch in tasks {
            let task = dispatch.task;
            match self.executor(task.task_type_id) {
                Some(executor) => {
                    if let Err(error) = executor.execute(key, &task).await {
                        tracing::warn!(?error, ?key, task_id = ?task.id, task_type_id = task.task_type_id,
                            "CHASM effect failed; committed task remains pending");
                    }
                }
                None => tracing::warn!(?key, task_id = ?task.id, task_type_id = task.task_type_id,
                    "CHASM executor missing; committed task remains pending"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokeira_chasm::{TaskId, TaskKind, VersionedTransition};

    struct RecordingExecutor {
        id: u32,
        fail: bool,
        seen: Arc<Mutex<Vec<u32>>>,
    }
    #[async_trait]
    impl SideEffectExecutor for RecordingExecutor {
        fn task_type_id(&self) -> u32 {
            self.id
        }
        async fn execute(&self, _: &ExecutionKey, task: &ScheduledTask) -> anyhow::Result<()> {
            self.seen.lock().unwrap().push(task.task_type_id);
            anyhow::ensure!(!self.fail, "injected executor failure");
            Ok(())
        }
    }
    #[tokio::test]
    async fn routes_in_order_and_isolates_unknown_and_failed_effects() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut mux = DispatchMultiplexer::default();
        for (id, fail) in [(10, true), (20, false)] {
            mux.register(Arc::new(RecordingExecutor {
                id,
                fail,
                seen: seen.clone(),
            }))
            .unwrap();
        }
        assert!(
            mux.register(Arc::new(RecordingExecutor {
                id: 10,
                fail: false,
                seen: seen.clone()
            }))
            .is_err()
        );
        assert!(mux.executor(30).is_none());
        let tasks = [10, 30, 20, 10]
            .into_iter()
            .enumerate()
            .map(|(offset, task_type_id)| DispatchableTask {
                node_path: vec![],
                task: ScheduledTask {
                    id: TaskId::new(VersionedTransition::new(0, 1), offset as u32),
                    task_type_id,
                    kind: TaskKind::SideEffect,
                    payload: vec![],
                    fire_at_unix_nanos: None,
                },
            })
            .collect();
        mux.dispatch(&ExecutionKey::new("ns", "id", "run"), tasks)
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![10, 20, 10]);
    }
}
