//! Minimal independent root, tasks and contexts for substrate contract tests.

use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Data {
    #[prost(int64, tag = "1")]
    pub value: i64,
}

pub(crate) struct Root<const N: usize = 0> {
    pub data: Data,
    meta: ContextMetadata,
}

impl<const N: usize> Lifecycle for Root<N> {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        LifecycleState::Running
    }
}

impl<const N: usize> Component for Root<N> {
    type Data = Data;
    const FQN: &'static str = match N {
        0 | 2 => "test.root",
        _ => "test.other",
    };
    fn fields(&self) -> FieldRegistry<'_> {
        FieldRegistry::new(&[])
    }
}

impl<const N: usize> EngineComponent for Root<N> {
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

impl<const N: usize> RootComponent for Root<N> {
    fn terminate(
        &mut self,
        _: &mut dyn MutableContext,
        _: &TerminateReason,
    ) -> Result<(), ChasmError> {
        Ok(())
    }
    fn context_metadata(&self) -> &ContextMetadata {
        &self.meta
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, prost::Message)]
pub(crate) struct Tick {
    #[prost(int64, tag = "1")]
    pub delta: i64,
}

impl Task for Tick {
    const KIND: TaskKind = TaskKind::Pure;
    const FQN: &'static str = "test.tick";
    fn fire_at(&self) -> Option<i64> {
        Some(10)
    }
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        Ok(prost::Message::encode_to_vec(self))
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        <Self as prost::Message>::decode(bytes).map_err(|e| ChasmError::Validation(e.to_string()))
    }
}

pub(crate) struct TickHandler;

impl PureTaskHandler for TickHandler {
    type Component = Root;
    type Task = Tick;
    fn validate(&self, c: &Root, t: &Tick, ctx: &dyn Context) -> TaskValidity {
        if c.data.value + t.delta <= ctx.now_unix_nanos() {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
    fn execute(
        &self,
        c: &mut Root,
        t: &Tick,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        if t.delta < 0 {
            return Err(ChasmError::Validation("negative delta".into()));
        }
        c.data.value += t.delta;
        ctx.mark_dirty()
    }
}

pub(crate) struct OutcomeHandler;

impl SideEffectTaskHandler for OutcomeHandler {
    type Component = Root;
    type Task = StartActivityTask;
    fn validate(&self, _: &Root, t: &StartActivityTask, _: &dyn Context) -> TaskValidity {
        if t.activity_id.is_empty() {
            TaskValidity::Drop
        } else {
            TaskValidity::Valid
        }
    }
    fn on_outcome(
        &self,
        c: &mut Root,
        t: &StartActivityTask,
        outcome: &TaskOutcome,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        c.data.value += t.heartbeat_nanos;
        if let TaskOutcome::Completed { payload } = outcome {
            c.data.value += payload.len() as i64;
        }
        ctx.resolve_task(TaskId::new(VersionedTransition::new(1, 1), 7));
        ctx.mark_dirty()
    }
}

pub(crate) struct TestContext {
    key: ExecutionKey,
    pub dirty: bool,
    pub resolved: Vec<TaskId>,
}

impl Default for TestContext {
    fn default() -> Self {
        Self {
            key: ExecutionKey::new("ns", "business", "run"),
            dirty: false,
            resolved: Vec::new(),
        }
    }
}

impl Context for TestContext {
    fn execution_key(&self) -> &ExecutionKey {
        &self.key
    }
    fn execution_info(&self) -> ExecutionInfo {
        ExecutionInfo::default()
    }
    fn now_unix_nanos(&self) -> i64 {
        100
    }
}

impl MutableContext for TestContext {
    fn add_task(
        &mut self,
        _: TaskKind,
        _: u32,
        _: Vec<u8>,
        _: Option<i64>,
    ) -> Result<(), ChasmError> {
        Ok(())
    }
    fn resolve_task(&mut self, id: TaskId) {
        self.resolved.push(id);
    }
    fn mark_dirty(&mut self) -> Result<(), ChasmError> {
        self.dirty = true;
        Ok(())
    }
}

pub(crate) fn scheduled<T: Task>(task: &T, offset: u32) -> ScheduledTask {
    ScheduledTask {
        kind: T::KIND,
        task_type_id: task_type_id_for_fqn(T::FQN),
        payload: task.encode().unwrap(),
        fire_at_unix_nanos: task.fire_at(),
        id: TaskId::new(VersionedTransition::new(1, 1), offset),
    }
}
