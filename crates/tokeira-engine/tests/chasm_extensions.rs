//! Public builder and typed-handle wiring; task consumers are covered by the
//! stage-13 acceptance archetype, which owns independent task payload codecs.
#![cfg(feature = "chasm-extensions")]

#[path = "support/chasm_root.rs"]
mod test_root;

use async_trait::async_trait;
use prost::Message;
use std::sync::Arc;
use test_root::{Data, Root, TestLibrary};
use tokeira_chasm::{ExecutionKey, ScheduledTask};
use tokeira_chasm_activity::{
    ActivityExecution, ActivityState, statemachine::ActivityEvent, tasks::DISPATCH_TASK_ID,
};
use tokeira_edge::translate::to_internal::namespace_id_for;
use tokeira_engine::{
    EmbeddedEngineConfig, EmbeddedEngineStartError, Engine, InProcessGrpcRequest,
    chasm::{ChasmError, Library, RegistryBuilder, SideEffectExecutor},
};
use tokeira_proto::workflowservice::{
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
};

#[derive(Debug)]
struct ReservedLibrary;
impl Library for ReservedLibrary {
    const NAME: &'static str = "activity";
    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
        builder.register_root::<Root<0>>(Self::NAME)?;
        Ok(())
    }
}

#[derive(Debug)]
struct DuplicateDispatch;
#[async_trait]
impl SideEffectExecutor for DuplicateDispatch {
    fn task_type_id(&self) -> u32 {
        DISPATCH_TASK_ID
    }
    async fn execute(&self, _: &ExecutionKey, _: &ScheduledTask) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn builder_exposes_roots_seeds_attributes_and_uses_chasm_clock() {
    let now = 1_234_567_890;
    let engine = Engine::builder(EmbeddedEngineConfig::default())
        .library::<TestLibrary<0>>()
        .clock(Arc::new(move || now))
        .build()
        .await
        .unwrap();
    let root = engine.chasm::<Root<0>>().unwrap();
    let key = ExecutionKey::new(
        namespace_id_for("default").0.to_string(),
        "builder-root",
        "00000000-0000-0000-0000-000000000011",
    );
    let started = root
        .start(
            key,
            Data {
                value: 17,
                closed: false,
            },
            None,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        root.read(&started.reference, |c, ctx| Ok((
            c.data.value,
            ctx.now_unix_nanos()
        )))
        .await
        .unwrap(),
        (17, now)
    );
    assert!(
        engine
            .chasm::<Root<3>>()
            .unwrap_err()
            .to_string()
            .contains("absent.root")
    );

    let activity = engine.chasm::<ActivityExecution>().unwrap();
    let key = ExecutionKey::new(
        namespace_id_for("default").0.to_string(),
        "clock-activity",
        "00000000-0000-0000-0000-000000000012",
    );
    let state = ActivityState {
        activity_id: key.business_id.clone(),
        activity_type: "clock".into(),
        task_queue: "clock-queue".into(),
        ..Default::default()
    };
    let started = activity
        .start_with(key, state, None, Default::default(), |c, ctx| {
            c.apply(ActivityEvent::Scheduled, ctx)
        })
        .await
        .unwrap();
    assert_eq!(
        activity
            .read(&started.reference, |c, _| Ok(c
                .activity_state()
                .unwrap()
                .scheduled_time_nanos))
            .await
            .unwrap(),
        now
    );

    let response = engine
        .endpoint()
        .call(InProcessGrpcRequest {
            service: "temporal.api.workflowservice.v1.WorkflowService".into(),
            rpc: "ListWorkflowExecutions".into(),
            headers: Default::default(),
            proto: ListWorkflowExecutionsRequest {
                namespace: "default".into(),
                query: "BuilderValue = 17".into(),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        })
        .await
        .unwrap();
    let _ = ListWorkflowExecutionsResponse::decode(response.proto.as_slice()).unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_keeps_builtins_and_rejects_registration_collisions() {
    let engine = Engine::builder(EmbeddedEngineConfig::default())
        .build()
        .await
        .unwrap();
    assert!(engine.chasm::<ActivityExecution>().is_ok());
    engine.shutdown().await.unwrap();
    let error = Engine::builder(EmbeddedEngineConfig::default())
        .library::<ReservedLibrary>()
        .build()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EmbeddedEngineStartError::Registry(ChasmError::ReservedLibraryName { .. })
    ));
    assert!(error.to_string().contains("activity"));
    let error = Engine::builder(EmbeddedEngineConfig::default())
        .side_effect_executor(Arc::new(DuplicateDispatch))
        .build()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EmbeddedEngineStartError::Registry(ChasmError::Validation(_))
    ));
    assert!(
        error
            .to_string()
            .contains(&format!("task type {DISPATCH_TASK_ID}"))
    );
}
