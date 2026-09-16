//! Independent root and task handlers for runtime fencing and restart properties.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokeira_chasm::{
    BusinessIdPolicy, ChasmError, ChasmNode, Component, ComponentRef, Context, ContextMetadata,
    EngineComponent, ExecutionKey, FieldRegistry, Lifecycle, LifecycleState, MutableContext,
    PureTaskHandler, Registry, RootComponent, SearchAttributeProvider, SearchAttributes,
    SideEffectTaskHandler, Task, TaskKind, TaskOutcome, TaskValidity, TerminateReason,
    VisibilityContributor, VisibilitySnapshot, task_type_id_for_fqn,
};
use tokeira_storage::{
    ChasmNodeRepository, CurrentExecution, CurrentExecutionCursor, CurrentRun,
    InMemoryChasmNodeStore, NodePersistOutcome, NodeWrite,
};

use super::{
    ChasmEngine, CollectingDispatchSink, CollectingVisibilitySink, DispatchSink, Engine,
    SideEffectExecutor, TypedEngine,
};

#[derive(Clone, PartialEq, Message)]
pub(super) struct Data {
    #[prost(uint32, repeated, tag = "1")]
    pub pure: Vec<u32>,
    #[prost(uint32, repeated, tag = "2")]
    pub outcomes: Vec<u32>,
    #[prost(bool, tag = "3")]
    pub closed: bool,
    #[prost(bool, tag = "4")]
    pub hidden: bool,
}

pub(super) struct Root {
    pub data: Data,
    meta: ContextMetadata,
}
impl Component for Root {
    type Data = Data;
    const FQN: &'static str = "runtime_test.root";
    fn fields(&self) -> FieldRegistry<'_> {
        FieldRegistry::new(&[])
    }
}
impl Lifecycle for Root {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        if self.data.closed {
            LifecycleState::Completed
        } else {
            LifecycleState::Running
        }
    }
}
impl RootComponent for Root {
    fn terminate(
        &mut self,
        _: &mut dyn MutableContext,
        _: &TerminateReason,
    ) -> Result<(), ChasmError> {
        self.data.closed = true;
        Ok(())
    }
    fn context_metadata(&self) -> &ContextMetadata {
        &self.meta
    }
}
impl EngineComponent for Root {
    fn from_data(data: Data) -> Self {
        Self {
            data,
            meta: ContextMetadata::default(),
        }
    }
    fn into_data(self) -> Data {
        self.data
    }
}
impl SearchAttributeProvider for Root {
    fn search_attributes(&self) -> SearchAttributes {
        vec![("Count".into(), self.data.pure.len().to_string())]
    }
}
impl VisibilityContributor for Root {
    fn visibility_snapshot(&self) -> Option<VisibilitySnapshot> {
        (!self.data.hidden).then(|| VisibilitySnapshot {
            status_keyword: if self.data.closed {
                "Completed"
            } else {
                "Running"
            }
            .into(),
            lifecycle_state: if self.data.closed {
                LifecycleState::Completed
            } else {
                LifecycleState::Running
            },
            execution_type: Some(Self::FQN.into()),
            task_queue: None,
            start_time_unix_nanos: Some(0),
            close_time_unix_nanos: None,
            search_attributes: Default::default(),
            memo: Default::default(),
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Message)]
pub(super) struct Work<const EFFECT: bool> {
    #[prost(uint32, tag = "1")]
    pub token: u32,
    #[prost(int64, tag = "2")]
    pub deadline: i64,
    #[prost(int64, tag = "3")]
    pub drop_at: i64,
    #[prost(bool, tag = "4")]
    pub close: bool,
    #[prost(bool, tag = "5")]
    pub fail: bool,
}
impl<const E: bool> Task for Work<E> {
    const FQN: &'static str = if E {
        "runtime_test.effect"
    } else {
        "runtime_test.timer"
    };
    const KIND: TaskKind = if E {
        TaskKind::SideEffect
    } else {
        TaskKind::Pure
    };
    fn fire_at(&self) -> Option<i64> {
        (self.deadline != 0).then_some(self.deadline)
    }
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        Ok(self.encode_to_vec())
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        <Self as Message>::decode(bytes).map_err(|e| ChasmError::Validation(e.to_string()))
    }
}
pub(super) fn valid(drop_at: i64, now: i64) -> bool {
    drop_at == 0 || now < drop_at
}
pub(super) struct PureHandler;
impl PureTaskHandler for PureHandler {
    type Component = Root;
    type Task = Work<false>;
    fn validate(&self, c: &Root, t: &Work<false>, ctx: &dyn Context) -> TaskValidity {
        if !c.data.closed && valid(t.drop_at, ctx.now_unix_nanos()) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
    fn execute(
        &self,
        c: &mut Root,
        t: &Work<false>,
        _: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        c.data.pure.push(t.token);
        c.data.closed |= t.close;
        if t.fail {
            return Err(ChasmError::Validation("pure handler rejected".into()));
        }
        Ok(())
    }
}
pub(super) struct EffectHandler;
impl SideEffectTaskHandler for EffectHandler {
    type Component = Root;
    type Task = Work<true>;
    fn validate(&self, c: &Root, t: &Work<true>, ctx: &dyn Context) -> TaskValidity {
        if !c.data.closed && valid(t.drop_at, ctx.now_unix_nanos()) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
    fn on_outcome(
        &self,
        c: &mut Root,
        t: &Work<true>,
        _: &TaskOutcome,
        _: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        c.data.outcomes.push(t.token);
        c.data.closed |= t.close;
        if t.fail {
            return Err(ChasmError::Validation("outcome handler rejected".into()));
        }
        Ok(())
    }
}
pub(super) fn registry() -> Arc<Registry> {
    let mut b = Registry::builder();
    b.register_root::<Root>("runtime_test")
        .unwrap()
        .register_pure_task("runtime_test", PureHandler)
        .unwrap()
        .register_side_effect_task("runtime_test", EffectHandler)
        .unwrap();
    Arc::new(b.build())
}
pub(super) fn engine(
    repo: Arc<dyn ChasmNodeRepository>,
    now: Arc<AtomicI64>,
    sink: Arc<dyn DispatchSink>,
) -> Arc<ChasmEngine> {
    Arc::new(
        ChasmEngine::new(
            repo,
            registry(),
            sink,
            Arc::new(CollectingVisibilitySink::default()),
        )
        .with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
    )
}
pub(super) fn key(index: usize) -> ExecutionKey {
    ExecutionKey::new("ns", format!("root-{index:03}"), "run")
}
pub(super) async fn start(engine: &Arc<ChasmEngine>, key: ExecutionKey) -> ComponentRef {
    TypedEngine::<Root>::new(engine.clone())
        .start(key, Data::default(), None, BusinessIdPolicy::default())
        .await
        .unwrap()
        .reference
}
pub(super) async fn stage<const E: bool>(
    engine: &Arc<ChasmEngine>,
    reference: &ComponentRef,
    tasks: &[Work<E>],
) {
    TypedEngine::<Root>::new(engine.clone())
        .update(reference, |_, ctx| {
            for task in tasks {
                ctx.add_task(
                    Work::<E>::KIND,
                    task_type_id_for_fqn(Work::<E>::FQN),
                    Task::encode(task)?,
                    task.fire_at(),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
}
pub(super) async fn data(engine: &ChasmEngine, key: &ExecutionKey) -> Data {
    let bytes = engine.read_component(key).await.unwrap().data.unwrap();
    Data::decode(bytes.as_slice()).unwrap()
}
pub(super) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
pub(super) fn sink() -> Arc<CollectingDispatchSink> {
    Arc::new(CollectingDispatchSink::default())
}

#[derive(Default)]
pub(super) struct IdempotentExecutor {
    pub effects: Mutex<Vec<(ExecutionKey, tokeira_chasm::TaskId)>>,
}
#[async_trait]
impl SideEffectExecutor for IdempotentExecutor {
    fn task_type_id(&self) -> u32 {
        task_type_id_for_fqn(Work::<true>::FQN)
    }
    async fn execute(
        &self,
        key: &ExecutionKey,
        task: &tokeira_chasm::ScheduledTask,
    ) -> anyhow::Result<()> {
        let mut effects = self.effects.lock().unwrap();
        let identity = (key.clone(), task.id);
        if !effects.contains(&identity) {
            effects.push(identity);
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ConflictingStore {
    pub inner: InMemoryChasmNodeStore,
    pub conflicts: AtomicUsize,
    pub create_conflicts: AtomicUsize,
}
#[async_trait]
impl ChasmNodeRepository for ConflictingStore {
    async fn persist_dirty(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
    ) -> anyhow::Result<NodePersistOutcome> {
        if self
            .conflicts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok()
        {
            return Ok(NodePersistOutcome::Conflict {
                reason: "injected contention".into(),
            });
        }
        self.inner.persist_dirty(key, batch).await
    }
    async fn persist_new_execution(
        &self,
        key: &ExecutionKey,
        archetype_id: u32,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
        expected_current: Option<CurrentRun>,
    ) -> anyhow::Result<NodePersistOutcome> {
        if self
            .create_conflicts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok()
        {
            return Ok(NodePersistOutcome::Conflict {
                reason: "injected start contention".into(),
            });
        }
        self.inner
            .persist_new_execution(key, archetype_id, batch, current, expected_current)
            .await
    }
    async fn current_run(
        &self,
        namespace_id: &str,
        archetype_id: u32,
        business_id: &str,
    ) -> anyhow::Result<Option<CurrentRun>> {
        self.inner
            .current_run(namespace_id, archetype_id, business_id)
            .await
    }
    async fn scan_current_executions(
        &self,
        status: LifecycleState,
        after: Option<CurrentExecutionCursor>,
        limit: usize,
    ) -> anyhow::Result<Vec<CurrentExecution>> {
        self.inner
            .scan_current_executions(status, after, limit)
            .await
    }
    async fn backfill_current_executions(
        &self,
        archetype_id: u32,
        batch: usize,
    ) -> anyhow::Result<usize> {
        self.inner
            .backfill_current_executions(archetype_id, batch)
            .await
    }
    async fn distinct_archetypes(&self) -> anyhow::Result<Vec<(u32, u64)>> {
        self.inner.distinct_archetypes().await
    }
    async fn backfill_marker_set(&self, name: &str) -> anyhow::Result<bool> {
        self.inner.backfill_marker_set(name).await
    }
    async fn set_backfill_marker(&self, name: &str) -> anyhow::Result<()> {
        self.inner.set_backfill_marker(name).await
    }
    async fn load_execution(
        &self,
        key: &ExecutionKey,
    ) -> anyhow::Result<Vec<(Vec<u8>, ChasmNode)>> {
        self.inner.load_execution(key).await
    }
    async fn load_subtree(
        &self,
        key: &ExecutionKey,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<(Vec<u8>, ChasmNode)>> {
        self.inner.load_subtree(key, prefix).await
    }
    async fn delete_execution(&self, key: &ExecutionKey) -> anyhow::Result<()> {
        self.inner.delete_execution(key).await
    }
    async fn scan_executions(&self) -> anyhow::Result<Vec<(ExecutionKey, ChasmNode)>> {
        self.inner.scan_executions().await
    }
}
