//! The embedder's compile check: a root component, its library, one pure task,
//! one side-effect task with its executor, and the control-plane calls, written
//! against `tokeira_engine::chasm` alone. The `Component` derive runs through the
//! re-export. What this file proves is that the published surface is sufficient;
//! the acceptance crate proves the behaviour behind it.
#![cfg(feature = "chasm-extensions")]

// Every internal crate is shadowed by an empty module: a `use tokeira_chasm::…`
// or `tokeira_runtime::…` path below no longer resolves to the crate, so an
// accidental direct import is a compile error rather than a silent escape. Only
// a leading `::` bypasses the shadow, and nothing in this file writes one.
mod tokeira_chasm {}
mod tokeira_chasm_activity {}
mod tokeira_chasm_derive {}
mod tokeira_edge {}
mod tokeira_projection {}
mod tokeira_proto {}
mod tokeira_runtime {}
mod tokeira_storage {}
mod tokeira_types {}

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokeira_engine::{
    EmbeddedEngineConfig, Engine,
    chasm::{
        BusinessIdPolicy, ChasmError, Component, Context, ContextMetadata, EngineComponent,
        ExecutionKey, Field, Library, Lifecycle, LifecycleState, Memo, MutableContext,
        PureTaskHandler, RegistryBuilder, RootComponent, ScheduledTask, SearchAttrKind,
        SearchAttrValue, SearchAttributeDef, SearchAttributeProvider, SearchAttributes,
        SideEffectExecutor, SideEffectTaskHandler, Task, TaskId, TaskKind, TaskOutcome,
        TaskValidity, TerminateReason, VisibilityContributor, VisibilitySnapshot, namespace_id_for,
        task_type_id_for_fqn,
    },
};

#[derive(Clone, PartialEq, Message)]
struct CounterState {
    #[prost(uint32, tag = "1")]
    ticks: u32,
    #[prost(uint32, tag = "2")]
    effects: u32,
    #[prost(bool, tag = "3")]
    closed: bool,
}

#[derive(Debug, Component)]
#[chasm(fqn = "embedder.counter", crate = "::tokeira_engine::chasm")]
struct Counter {
    #[chasm(data)]
    state: Field<CounterState>,
    #[chasm(transient)]
    meta: ContextMetadata,
}

impl Counter {
    fn state(&self) -> Result<&CounterState, ChasmError> {
        self.state
            .value()
            .ok_or_else(|| ChasmError::Internal("counter state is not materialized".into()))
    }

    fn state_mut(&mut self) -> Result<&mut CounterState, ChasmError> {
        self.state
            .value_mut()
            .ok_or_else(|| ChasmError::Internal("counter state is not materialized".into()))
    }
}

impl Lifecycle for Counter {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        if self.state.value().is_some_and(|state| state.closed) {
            LifecycleState::Completed
        } else {
            LifecycleState::Running
        }
    }
}

impl RootComponent for Counter {
    fn terminate(
        &mut self,
        _: &mut dyn MutableContext,
        _: &TerminateReason,
    ) -> Result<(), ChasmError> {
        self.state_mut()?.closed = true;
        Ok(())
    }

    fn context_metadata(&self) -> &ContextMetadata {
        &self.meta
    }
}

impl EngineComponent for Counter {
    fn from_data(data: CounterState) -> Self {
        Self {
            state: Field::with_value(data),
            meta: ContextMetadata::default(),
        }
    }

    fn into_data(self) -> CounterState {
        self.state.into_value().unwrap_or_default()
    }
}

impl SearchAttributeProvider for Counter {
    fn search_attributes(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

impl VisibilityContributor for Counter {
    fn visibility_snapshot(&self) -> Option<VisibilitySnapshot> {
        let state = self.state.value()?;
        let lifecycle_state = if state.closed {
            LifecycleState::Completed
        } else {
            LifecycleState::Running
        };
        Some(VisibilitySnapshot {
            status_keyword: if state.closed { "Closed" } else { "Counting" }.to_owned(),
            lifecycle_state,
            execution_type: Some(Self::FQN.to_owned()),
            task_queue: None,
            start_time_unix_nanos: None,
            close_time_unix_nanos: None,
            search_attributes: SearchAttributes(
                [(
                    "Ticks".to_owned(),
                    SearchAttrValue::Int(i64::from(state.ticks)),
                )]
                .into(),
            ),
            memo: Memo::default(),
        })
    }
}

/// The library's own timer: a pure task the engine fires on the CHASM clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Tick {
    due_nanos: i64,
}

impl Task for Tick {
    const KIND: TaskKind = TaskKind::Pure;
    const FQN: &'static str = "embedder.tick";

    fn fire_at(&self) -> Option<i64> {
        Some(self.due_nanos)
    }

    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        serde_json::to_vec(self).map_err(|error| ChasmError::Internal(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        serde_json::from_slice(bytes).map_err(|error| ChasmError::Validation(error.to_string()))
    }
}

/// The library's own effect: a side-effect task the embedder's executor performs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Notify {
    ticks: u32,
}

impl Task for Notify {
    const KIND: TaskKind = TaskKind::SideEffect;
    const FQN: &'static str = "embedder.notify";

    fn fire_at(&self) -> Option<i64> {
        None
    }

    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        serde_json::to_vec(self).map_err(|error| ChasmError::Internal(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        serde_json::from_slice(bytes).map_err(|error| ChasmError::Validation(error.to_string()))
    }
}

#[derive(Debug)]
struct TickHandler;

impl PureTaskHandler for TickHandler {
    type Component = Counter;
    type Task = Tick;

    fn validate(&self, _: &Counter, _: &Tick, _: &dyn Context) -> TaskValidity {
        TaskValidity::Valid
    }

    fn execute(
        &self,
        c: &mut Counter,
        _: &Tick,
        _: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        c.state_mut()?.ticks += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct NotifyHandler;

impl SideEffectTaskHandler for NotifyHandler {
    type Component = Counter;
    type Task = Notify;

    fn validate(&self, _: &Counter, _: &Notify, _: &dyn Context) -> TaskValidity {
        TaskValidity::Valid
    }

    fn on_outcome(
        &self,
        c: &mut Counter,
        _: &Notify,
        _: &TaskOutcome,
        _: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        c.state_mut()?.effects += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct EmbedderLibrary;

impl Library for EmbedderLibrary {
    const NAME: &'static str = "embedder";

    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
        builder
            .register_root::<Counter>(Self::NAME)?
            .register_search_attributes::<Counter>(&[SearchAttributeDef {
                name: "Ticks",
                kind: SearchAttrKind::Int,
            }])?
            .register_pure_task(Self::NAME, TickHandler)?
            .register_side_effect_task(Self::NAME, NotifyHandler)?;
        Ok(())
    }
}

/// The embedder's executor for its own side-effect task; here it only records
/// what the engine delivered once the staging commit landed.
#[derive(Debug, Default)]
struct Recorder {
    delivered: Mutex<Vec<(ExecutionKey, TaskId, Notify)>>,
}

#[async_trait]
impl SideEffectExecutor for Recorder {
    fn task_type_id(&self) -> u32 {
        task_type_id_for_fqn(Notify::FQN)
    }

    async fn execute(&self, key: &ExecutionKey, task: &ScheduledTask) -> anyhow::Result<()> {
        let notify = Notify::decode(&task.payload)?;
        self.delivered
            .lock()
            .expect("recorder lock poisoned")
            .push((key.clone(), task.id, notify));
        Ok(())
    }
}

#[tokio::test]
async fn a_library_builds_and_runs_against_the_engine_alone() {
    let recorder = Arc::new(Recorder::default());
    let now = 1_000_000_000_i64;
    let engine = Engine::builder(EmbeddedEngineConfig::default())
        .library::<EmbedderLibrary>()
        .side_effect_executor(recorder.clone())
        .clock(Arc::new(move || now))
        .build()
        .await
        .unwrap();
    let counter = engine.chasm::<Counter>().unwrap();
    let namespace = namespace_id_for("default");
    let key = ExecutionKey::new(
        namespace.0.to_string(),
        "counter-1",
        "00000000-0000-0000-0000-000000000021",
    );
    assert_eq!(
        counter
            .reference(&key.namespace_id, "counter-1")
            .await
            .unwrap(),
        None
    );

    let started = counter
        .start_with(
            key.clone(),
            CounterState::default(),
            Some("create-1".into()),
            BusinessIdPolicy::default(),
            |_, ctx| {
                ctx.add_task(
                    TaskKind::Pure,
                    task_type_id_for_fqn(Tick::FQN),
                    Tick {
                        due_nanos: now + 60,
                    }
                    .encode()?,
                    Some(now + 60),
                )?;
                ctx.add_task(
                    TaskKind::SideEffect,
                    task_type_id_for_fqn(Notify::FQN),
                    Notify { ticks: 0 }.encode()?,
                    None,
                )
            },
        )
        .await
        .unwrap();
    assert!(started.created);

    // The executor ran once the creating commit landed, with the durable task.
    let delivered = recorder.delivered.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, key);
    assert_eq!(delivered[0].2, Notify { ticks: 0 });

    // A caller holding only the business id gets the reference the start minted.
    let reference = counter
        .reference(&key.namespace_id, "counter-1")
        .await
        .unwrap()
        .expect("current run");
    assert_eq!(reference, started.reference);

    let (ticks, outcome) = counter
        .update(&reference, |c, _| {
            let state = c.state_mut()?;
            state.ticks += 1;
            Ok(state.ticks)
        })
        .await
        .unwrap();
    assert_eq!(ticks, 1);
    assert!(!outcome.closed);
    assert_eq!(
        counter
            .read(&outcome.reference, |c, ctx| Ok((
                c.state()?.ticks,
                ctx.now_unix_nanos()
            )))
            .await
            .unwrap(),
        (1, now)
    );
    assert_eq!(
        engine
            .chasm_visibility::<Counter>(namespace)
            .unwrap()
            .count(None)
            .await
            .unwrap(),
        1
    );
    engine.shutdown().await.unwrap();
}
