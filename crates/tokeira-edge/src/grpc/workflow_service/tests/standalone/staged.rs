//! Component-staged starts through the public activity query and describe handlers.

use super::*;
use crate::chasm_executors::StartActivityExecutor;
use tokeira_chasm::{
    ChasmError, Context, MutableContext, SideEffectTaskHandler, StartActivityTask, Task,
    TaskOutcome, TaskValidity, task_type_id_for_fqn,
};
use tokeira_projection::{
    InMemoryVisibilityStore, VisibilityQueryService, VisibilitySink, store::VisibilityStore,
    types::SearchAttrType,
};
use tokeira_proto::{
    common::{Payload, SearchAttributes},
    public::temporal::api::sdk::v1::UserMetadata,
};
use tokeira_runtime::chasm::ProjectionVisibilitySink;

pub(super) struct StagedActivityHandler;

impl SideEffectTaskHandler for StagedActivityHandler {
    type Component = ActivityExecution;
    type Task = StartActivityTask;

    fn validate(
        &self,
        _: &ActivityExecution,
        _: &StartActivityTask,
        _: &dyn Context,
    ) -> TaskValidity {
        TaskValidity::Valid
    }

    fn on_outcome(
        &self,
        _: &mut ActivityExecution,
        _: &StartActivityTask,
        _: &TaskOutcome,
        _: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        Ok(())
    }
}

#[tokio::test]
async fn component_started_activity_is_listed_by_attribute_and_describes_metadata() {
    let store = InMemoryVisibilityStore::default();
    let namespace = crate::translate::to_internal::namespace_id_for("default");
    store
        .register_attr(namespace, "ComponentId".into(), SearchAttrType::Keyword)
        .await
        .unwrap();
    let visibility = Arc::new(VisibilityQueryService::new(store.clone()));
    let config = ActivityConfig {
        enable_standalone: true,
        ..Default::default()
    };
    let h = Harness::with_runtime_store_sink(
        config.clone(),
        visibility.clone(),
        Arc::new(PollNoneRuntime),
        Arc::new(InMemoryChasmNodeStore::new()),
        Arc::new(ProjectionVisibilitySink::new(
            Arc::new(VisibilitySink::new(store)),
            1,
        )),
    );
    h.mux
        .register(Arc::new(
            StartActivityExecutor::new(Arc::downgrade(&h.engine), config, 1000)
                .with_visibility(visibility),
        ))
        .unwrap();
    let attributes = SearchAttributes {
        indexed_fields: [(
            "ComponentId".into(),
            Payload {
                metadata: [
                    ("encoding".into(), b"json/plain".to_vec()),
                    ("type".into(), b"Keyword".to_vec()),
                ]
                .into(),
                data: b"\"parent\"".to_vec(),
                ..Default::default()
            },
        )]
        .into(),
    };
    let metadata = UserMetadata {
        summary: Some(Payload {
            data: b"component activity".to_vec(),
            ..Default::default()
        }),
        // One byte over `limit.userMetadataDetailsSize`'s default, which v1.31.0
        // defines but never reads (`common/dynamicconfig/constants.go:3171-3175`).
        details: Some(Payload {
            data: vec![b'x'; 20_001],
            ..Default::default()
        }),
    };
    let request = StartActivityTask {
        activity_id: "child".into(),
        activity_type: "ChildActivity".into(),
        task_queue: QUEUE.into(),
        start_to_close_nanos: 10 * SEC,
        search_attributes: attributes.encode_to_vec(),
        user_metadata: metadata.encode_to_vec(),
        ..Default::default()
    };
    TypedEngine::<ActivityExecution>::new(h.engine.clone())
        .start_with(
            ExecutionKey::new(h.namespace.clone(), "parent", Uuid::new_v4().to_string()),
            ActivityState {
                activity_id: "parent".into(),
                activity_type: "ParentActivity".into(),
                task_queue: QUEUE.into(),
                start_to_close_nanos: 10 * SEC,
                ..Default::default()
            },
            None,
            BusinessIdPolicy::default(),
            |_, ctx| {
                ctx.add_task(
                    StartActivityTask::KIND,
                    task_type_id_for_fqn(StartActivityTask::FQN),
                    Task::encode(&request)?,
                    None,
                )
            },
        )
        .await
        .unwrap();
    let listed = h
        .grpc
        .list_activity_executions(Request::new(
            workflowservice::ListActivityExecutionsRequest {
                namespace: "default".into(),
                query: "ComponentId = 'parent'".into(),
                page_size: 10,
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.executions.len(), 1);
    assert_eq!(listed.executions[0].activity_id, "child");
    assert_eq!(
        listed.executions[0].search_attributes.as_ref(),
        Some(&attributes)
    );
    let described = h
        .grpc
        .describe_activity_execution(Request::new(
            workflowservice::DescribeActivityExecutionRequest {
                namespace: "default".into(),
                activity_id: "child".into(),
                run_id: listed.executions[0].run_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(described.info.unwrap().user_metadata, Some(metadata));
}
