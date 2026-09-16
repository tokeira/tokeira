//! Executor integration tests with durable in-memory roots and a recording client.

use super::*;
use crate::{chasm_activity::ActivityBridge, namespace_cache::ResolvedNamespace};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    },
};
use tokeira_chasm::{
    Component, Context, ContextMetadata, EngineComponent, FieldRegistry, Library, Lifecycle,
    LifecycleState, MutableContext, Registry, RootComponent, SearchAttributeProvider,
    SearchAttributes, SideEffectTaskHandler, TaskKind, TaskValidity, TerminateReason,
    VisibilityContributor, VisibilitySnapshot,
};
use tokeira_chasm_activity::{ActivityEvent, ActivityExecution, ActivityLibrary};
use tokeira_runtime::chasm::{CollectingVisibilitySink, DispatchMultiplexer, TypedEngine};
use tokeira_storage::InMemoryChasmNodeStore;

const SEC: i64 = 1_000_000_000;

#[derive(Clone, PartialEq, prost::Message)]
struct ParentData {
    #[prost(bytes = "vec", repeated, tag = "1")]
    outcomes: Vec<Vec<u8>>,
    #[prost(bool, tag = "2")]
    reject: bool,
}
struct Parent {
    data: ParentData,
    metadata: ContextMetadata,
}
impl Component for Parent {
    type Data = ParentData;
    const FQN: &'static str = "executor_test.parent";
    fn fields(&self) -> FieldRegistry<'_> {
        FieldRegistry::new(&[])
    }
}
impl Lifecycle for Parent {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        LifecycleState::Running
    }
}
impl RootComponent for Parent {
    fn terminate(
        &mut self,
        _: &mut dyn MutableContext,
        _: &TerminateReason,
    ) -> Result<(), ChasmError> {
        Ok(())
    }
    fn context_metadata(&self) -> &ContextMetadata {
        &self.metadata
    }
}
impl EngineComponent for Parent {
    fn from_data(data: ParentData) -> Self {
        Self {
            data,
            metadata: ContextMetadata::default(),
        }
    }
    fn into_data(self) -> ParentData {
        self.data
    }
}
impl SearchAttributeProvider for Parent {
    fn search_attributes(&self) -> SearchAttributes {
        Vec::new()
    }
}
impl VisibilityContributor for Parent {
    fn visibility_snapshot(&self) -> Option<VisibilitySnapshot> {
        None
    }
}
struct ParentHandler;
impl SideEffectTaskHandler for ParentHandler {
    type Component = Parent;
    type Task = StartActivityTask;
    fn validate(&self, _: &Parent, _: &StartActivityTask, _: &dyn Context) -> TaskValidity {
        TaskValidity::Valid
    }
    fn on_outcome(
        &self,
        component: &mut Parent,
        _: &StartActivityTask,
        outcome: &TaskOutcome,
        _: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        if component.data.reject {
            return Err(ChasmError::Validation("target rejected outcome".into()));
        }
        component
            .data
            .outcomes
            .push(serde_json::to_vec(outcome).unwrap());
        Ok(())
    }
}

struct NamespaceFixture {
    result: Option<ResolvedNamespace>,
    error: bool,
}
#[async_trait]
impl NamespaceCache for NamespaceFixture {
    async fn get(&self, _: &str) -> anyhow::Result<Option<ResolvedNamespace>> {
        Ok(self.result.clone())
    }
    async fn get_by_id(&self, _: &str) -> anyhow::Result<Option<ResolvedNamespace>> {
        anyhow::ensure!(!self.error, "transient namespace cache failure");
        Ok(self.result.clone())
    }
    async fn list_all(&self) -> anyhow::Result<Vec<ResolvedNamespace>> {
        Ok(self.result.iter().cloned().collect())
    }
    async fn insert(&self, _: ResolvedNamespace) -> anyhow::Result<()> {
        unreachable!()
    }
}

#[derive(Clone, Debug)]
struct Call {
    url: String,
    token: String,
    completion: NexusCompletion,
    links: Vec<tokeira_kernel::Link>,
}
#[derive(Default)]
struct Client {
    calls: Mutex<Vec<Call>>,
    outcome: Mutex<Option<CompletionDeliveryOutcome>>,
    preflight_error: bool,
}
#[async_trait]
impl NexusCompletionClient for Client {
    async fn complete_operation(
        &self,
        url: &str,
        token: &str,
        _: &str,
        completion: NexusCompletion,
        links: &[tokeira_kernel::Link],
    ) -> anyhow::Result<CompletionDeliveryOutcome> {
        self.calls.lock().unwrap().push(Call {
            url: url.into(),
            token: token.into(),
            completion,
            links: links.into(),
        });
        anyhow::ensure!(!self.preflight_error, "preflight failure");
        Ok(self
            .outcome
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(CompletionDeliveryOutcome::Delivered))
    }
}

struct Fixture {
    engine: Arc<ChasmEngine>,
    mux: Arc<DispatchMultiplexer>,
    bridge: ActivityBridge,
    clock: Arc<AtomicI64>,
    client: Arc<Client>,
    deliver: DeliverCallbackExecutor,
}
impl Fixture {
    fn new(
        namespace: Option<ResolvedNamespace>,
        cache_error: bool,
        config: NexusCompletionRuntimeConfig,
    ) -> Self {
        let mut registry = Registry::builder();
        ActivityLibrary::register(&mut registry).unwrap();
        registry
            .register_root::<Parent>("executor_test")
            .unwrap()
            .register_side_effect_task("executor_test", ParentHandler)
            .unwrap();
        let mux = Arc::new(DispatchMultiplexer::default());
        let clock = Arc::new(AtomicI64::new(100 * SEC));
        let now = clock.clone();
        let engine = Arc::new(
            ChasmEngine::new(
                Arc::new(InMemoryChasmNodeStore::new()),
                Arc::new(registry.build()),
                mux.clone(),
                Arc::new(CollectingVisibilitySink::default()),
            )
            .with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
        );
        let dispatch = Arc::new(ActivityDispatchExecutor::new(
            Arc::downgrade(&engine),
            Arc::new(ActivityDispatchQueue::new()),
        ));
        mux.register(dispatch.clone()).unwrap();
        let bridge = ActivityBridge::new(
            engine.clone(),
            ActivityConfig {
                enable_standalone: true,
                enable_callbacks: true,
                ..Default::default()
            },
            1000,
        )
        .with_dispatch_executor(dispatch);
        let client = Arc::new(Client::default());
        let deliver = DeliverCallbackExecutor::new(
            Arc::downgrade(&engine),
            client.clone(),
            config,
            Arc::new(NamespaceFixture {
                result: namespace,
                error: cache_error,
            }),
        );
        Self {
            engine,
            mux,
            bridge,
            clock,
            client,
            deliver,
        }
    }
    async fn parent(
        &self,
        request: StartActivityTask,
        reject: bool,
    ) -> (ExecutionKey, ScheduledTask) {
        let key = ExecutionKey::new("ns-id", "parent", "parent-run");
        TypedEngine::<Parent>::new(self.engine.clone())
            .start_with(
                key.clone(),
                ParentData {
                    reject,
                    ..Default::default()
                },
                None,
                BusinessIdPolicy::default(),
                |_, ctx| {
                    ctx.add_task(
                        TaskKind::SideEffect,
                        task_type_id_for_fqn(StartActivityTask::FQN),
                        Task::encode(&request)?,
                        None,
                    )
                },
            )
            .await
            .unwrap();
        let task = self
            .engine
            .root_node(&key)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .outbox
            .side_effect_tasks[0]
            .clone();
        (key, task)
    }
    async fn state(&self, key: &ExecutionKey) -> ActivityState {
        ActivityState::decode(
            self.engine
                .read_component(key)
                .await
                .unwrap()
                .data
                .unwrap()
                .as_slice(),
        )
        .unwrap()
    }
    async fn ready_callback(&self) -> (ExecutionKey, ScheduledTask) {
        let key = ExecutionKey::new("ns-id", "activity", "run");
        let data = ActivityState {
            activity_id: "activity".into(),
            task_queue: "queue".into(),
            start_to_close_nanos: SEC,
            ..Default::default()
        };
        let reference = TypedEngine::<ActivityExecution>::new(self.engine.clone())
            .start_with(
                key.clone(),
                data,
                None,
                BusinessIdPolicy::default(),
                |component, ctx| {
                    component.apply(
                        ActivityEvent::CallbacksAttached {
                            request_id: "attach".into(),
                            callbacks: vec![CallbackSpec {
                                target: CallbackTarget::Nexus {
                                    url: "temporal://system".into(),
                                    header: BTreeMap::from([(
                                        "TeMpOrAl-CaLlBaCk-ToKeN".into(),
                                        "TOKEN".into(),
                                    )]),
                                },
                                links: vec![
                                    tokeira_proto::common::Link {
                                        variant: Some(
                                            tokeira_proto::common::link::Variant::BatchJob(
                                                tokeira_proto::common::link::BatchJob {
                                                    job_id: "job".into(),
                                                },
                                            ),
                                        ),
                                    }
                                    .encode_to_vec(),
                                ],
                            }],
                            max_callbacks: 2,
                        },
                        ctx,
                    )?;
                    component.apply(ActivityEvent::Scheduled, ctx)
                },
            )
            .await
            .unwrap()
            .reference;
        self.bridge
            .record_started(key.clone(), 100 * SEC, "worker".into())
            .await
            .unwrap();
        let results = tokeira_proto::common::Payloads {
            payloads: vec![
                tokeira_proto::common::Payload {
                    data: vec![1],
                    ..Default::default()
                },
                tokeira_proto::common::Payload {
                    data: vec![2],
                    ..Default::default()
                },
            ],
        }
        .encode_to_vec();
        self.bridge
            .record_completed(reference.execution_key, results, "worker".into())
            .await
            .unwrap();
        let task = self
            .engine
            .root_node(&key)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .outbox
            .side_effect_tasks
            .into_iter()
            .find(|task| task.task_type_id == DELIVER_CALLBACK_TASK_ID)
            .unwrap();
        (key, task)
    }
}

#[tokio::test]
async fn nested_callback_delivery_keeps_the_committed_retry_timer() {
    let fixture = Fixture::new(None, false, Default::default());
    let (key, task) = fixture.ready_callback().await;
    *fixture.client.outcome.lock().unwrap() = Some(CompletionDeliveryOutcome::RetryableError {
        detail: "later".into(),
    });
    fixture.deliver.execute(&key, &task).await.unwrap();
    let due = fixture.state(&key).await.callbacks[0].next_attempt_time_nanos;
    fixture.clock.store(due, Ordering::SeqCst);
    fixture
        .mux
        .register(Arc::new(DeliverCallbackExecutor::new(
            Arc::downgrade(&fixture.engine),
            fixture.client.clone(),
            Default::default(),
            Arc::new(NamespaceFixture {
                result: None,
                error: false,
            }),
        )))
        .unwrap();
    fixture.bridge.evaluate_timeouts(&key, due).await.unwrap();
    let state = fixture.state(&key).await;
    assert_eq!(state.callbacks[0].state(), CallbackState::BackingOff);
    assert_eq!(
        fixture.engine.armed_timer(&key),
        Some(state.callbacks[0].next_attempt_time_nanos)
    );
    assert_eq!(fixture.client.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn nexus_resolved_namespace_preserves_backlink_headers_and_first_payload() {
    let fixture = Fixture::new(
        Some(ResolvedNamespace::active("public-name")),
        false,
        Default::default(),
    );
    let (key, task) = fixture.ready_callback().await;
    fixture.deliver.execute(&key, &task).await.unwrap();
    fixture.deliver.execute(&key, &task).await.unwrap();
    {
        let calls = fixture.client.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].token, "TOKEN");
        assert!(calls[0].url.starts_with("http://127.0.0.1:7253"));
        assert_eq!(
            calls[0].links[0],
            tokeira_kernel::Link::Activity {
                namespace: "public-name".into(),
                activity_id: "activity".into(),
                run_id: "run".into()
            }
        );
        assert_eq!(
            calls[0].links[1],
            tokeira_kernel::Link::BatchJob {
                job_id: "job".into()
            }
        );
        assert_eq!(
            calls[0].completion,
            NexusCompletion::Succeeded(tokeira_types::Payloads(vec![payload_to_domain(
                &tokeira_proto::common::Payload {
                    data: vec![1],
                    ..Default::default()
                }
            )]))
        );
    }
    assert_eq!(
        fixture.state(&key).await.callbacks[0].state(),
        CallbackState::Succeeded
    );
}

#[tokio::test]
async fn nexus_missing_or_tombstoned_namespace_delivers_without_backlink() {
    let mut deleted = ResolvedNamespace::active("deleted");
    deleted.deleted = true;
    for namespace in [None, Some(deleted)] {
        let fixture = Fixture::new(namespace, false, Default::default());
        let (key, task) = fixture.ready_callback().await;
        fixture.deliver.execute(&key, &task).await.unwrap();
        assert_eq!(
            fixture.client.calls.lock().unwrap()[0].links,
            vec![tokeira_kernel::Link::BatchJob {
                job_id: "job".into()
            }]
        );
        assert_eq!(
            fixture.state(&key).await.callbacks[0].state(),
            CallbackState::Succeeded
        );
    }
}

#[tokio::test]
async fn namespace_cache_error_retries_at_shared_backoff_and_evaluator_rearms() {
    let fixture = Fixture::new(None, true, Default::default());
    let (key, task) = fixture.ready_callback().await;
    fixture.deliver.execute(&key, &task).await.unwrap();
    assert!(fixture.client.calls.lock().unwrap().is_empty());
    let state = fixture.state(&key).await;
    let callback = &state.callbacks[0];
    let expected = fixture.engine.now()
        + nexus_completion_backoff(
            &fixture.deliver.config,
            1,
            callback_seed(&key, &callback.id),
        )
        .whole_nanoseconds() as i64;
    assert_eq!(callback.state(), CallbackState::BackingOff);
    assert_eq!(callback.next_attempt_time_nanos, expected);
    assert_eq!(
        fixture
            .bridge
            .evaluate_timeouts(&key, expected - 1)
            .await
            .unwrap(),
        Some(expected)
    );
    fixture.clock.store(expected, Ordering::SeqCst);
    assert_eq!(
        fixture
            .bridge
            .evaluate_timeouts(&key, expected)
            .await
            .unwrap(),
        None
    );
    let state = fixture.state(&key).await;
    assert_eq!(state.callbacks[0].state(), CallbackState::Scheduled);
    let root = fixture.engine.root_node(&key).await.unwrap().unwrap();
    assert_eq!(root.metadata.outbox.side_effect_tasks.len(), 1);
    assert!(root.metadata.outbox.pure_tasks.is_empty());
}

#[tokio::test]
async fn client_preflight_error_and_internal_conflict_use_shared_backoff() {
    for internal in [false, true] {
        let mut fixture = Fixture::new(None, false, Default::default());
        let (key, task) = fixture.ready_callback().await;
        let before = fixture.state(&key).await;
        if internal {
            let outcome = fixture.deliver.internal_outcome(
                &fixture.engine,
                &key,
                &before.callbacks[0],
                Err(ChasmError::RetriesExhausted { attempts: 8 }),
            );
            fixture
                .engine
                .apply_side_effect_outcome(&key, DELIVER_CALLBACK_TASK_ID, task.id, outcome)
                .await
                .unwrap();
        } else {
            fixture.deliver.client = Arc::new(Client {
                preflight_error: true,
                ..Default::default()
            });
            fixture.deliver.execute(&key, &task).await.unwrap();
        }
        let state = fixture.state(&key).await;
        let callback = &state.callbacks[0];
        assert_eq!(callback.state(), CallbackState::BackingOff);
        assert_eq!(
            callback.next_attempt_time_nanos,
            fixture.engine.now()
                + nexus_completion_backoff(
                    &fixture.deliver.config,
                    1,
                    callback_seed(&key, &callback.id)
                )
                .whole_nanoseconds() as i64
        );
        let failure = Failure::decode(callback.last_attempt_failure.as_slice()).unwrap();
        assert!(
            matches!(failure.failure_info, Some(FailureInfo::ApplicationFailureInfo(info)) if !info.non_retryable)
        );
    }
}

#[tokio::test]
async fn nexus_retry_cap_and_nonretryable_responses_set_failure_flags() {
    for (outcome, cap, expected) in [
        (
            CompletionDeliveryOutcome::RetryableError {
                detail: "retry".into(),
            },
            0,
            CallbackState::BackingOff,
        ),
        (
            CompletionDeliveryOutcome::RetryableError {
                detail: "retry".into(),
            },
            1,
            CallbackState::Failed,
        ),
        (
            CompletionDeliveryOutcome::NonRetryableError {
                detail: "reject".into(),
            },
            0,
            CallbackState::Failed,
        ),
    ] {
        let fixture = Fixture::new(
            None,
            false,
            NexusCompletionRuntimeConfig {
                retry_max_attempts: cap,
                ..Default::default()
            },
        );
        *fixture.client.outcome.lock().unwrap() = Some(outcome);
        let (key, task) = fixture.ready_callback().await;
        fixture.deliver.execute(&key, &task).await.unwrap();
        let state = fixture.state(&key).await;
        assert_eq!(state.callbacks[0].state(), expected);
        let failure = Failure::decode(state.callbacks[0].last_attempt_failure.as_slice()).unwrap();
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("application failure")
        };
        assert_eq!(info.non_retryable, expected == CallbackState::Failed);
    }
}

fn staged_request() -> StartActivityTask {
    StartActivityTask {
        activity_id: "child".into(),
        activity_type: "TaskType".into(),
        task_queue: "queue".into(),
        input: vec![1],
        header: vec![2],
        retry_policy: tokeira_proto::common::RetryPolicy {
            maximum_attempts: 3,
            ..Default::default()
        }
        .encode_to_vec(),
        schedule_to_start_nanos: 2 * SEC,
        schedule_to_close_nanos: 30 * SEC,
        start_to_close_nanos: 10 * SEC,
        heartbeat_nanos: SEC,
        version_target: Some(tokeira_chasm::DeploymentVersionTarget {
            deployment_name: "deployment".into(),
            build_id: "build".into(),
        }),
    }
}

#[tokio::test]
async fn staged_start_is_idempotent_and_internal_delivery_resolves_parent_once() {
    let fixture = Fixture::new(None, false, Default::default());
    let request = staged_request();
    let (parent, task) = fixture.parent(request.clone(), false).await;
    let executor = StartActivityExecutor::new(
        Arc::downgrade(&fixture.engine),
        ActivityConfig::default(),
        1000,
    );
    executor.execute(&parent, &task).await.unwrap();
    let run = fixture
        .bridge
        .current_run("ns-id", "child")
        .await
        .unwrap()
        .unwrap();
    let key = ExecutionKey::new("ns-id", "child", run);
    let before = fixture.state(&key).await;
    assert_eq!(before.activity_type, request.activity_type);
    assert_eq!(before.task_queue, request.task_queue);
    assert_eq!(before.input, request.input);
    assert_eq!(before.header, request.header);
    assert_eq!(before.retry_policy, request.retry_policy);
    assert_eq!(
        before.schedule_to_start_nanos,
        request.schedule_to_start_nanos
    );
    assert_eq!(
        before.schedule_to_close_nanos,
        request.schedule_to_close_nanos
    );
    assert_eq!(before.start_to_close_nanos, request.start_to_close_nanos);
    assert_eq!(before.heartbeat_nanos, request.heartbeat_nanos);
    assert_eq!(before.version_target, request.version_target);
    assert_eq!(before.maximum_attempts, 3);
    assert_eq!(before.retry_initial_interval_nanos, SEC);
    assert!(
        before.priority.is_empty()
            && before.search_attributes.is_empty()
            && before.user_metadata.is_empty()
    );
    executor.execute(&parent, &task).await.unwrap();
    assert_eq!(fixture.state(&key).await, before);
    assert_eq!(before.callbacks.len(), 1);
    let Some(activity_callback::Target::Internal(target)) = &before.callbacks[0].target else {
        panic!("internal callback")
    };
    assert_eq!(
        ComponentRef::decode(&target.component_ref)
            .unwrap()
            .execution_key,
        parent
    );
    assert_eq!(decode_task_id(&target.task_id).unwrap(), task.id);
    fixture
        .bridge
        .record_started(key.clone(), fixture.engine.now(), "worker".into())
        .await
        .unwrap();
    fixture
        .bridge
        .record_completed(key.clone(), vec![9], "worker".into())
        .await
        .unwrap();
    let delivery = fixture
        .engine
        .root_node(&key)
        .await
        .unwrap()
        .unwrap()
        .metadata
        .outbox
        .side_effect_tasks[0]
        .clone();
    fixture.deliver.execute(&key, &delivery).await.unwrap();
    fixture.deliver.execute(&key, &delivery).await.unwrap();
    let data = ParentData::decode(
        fixture
            .engine
            .read_component(&parent)
            .await
            .unwrap()
            .data
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    assert_eq!(data.outcomes.len(), 1);
    assert_eq!(
        serde_json::from_slice::<TaskOutcome>(&data.outcomes[0]).unwrap(),
        TaskOutcome::Completed { payload: vec![9] }
    );
    assert!(
        fixture
            .engine
            .root_node(&parent)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .outbox
            .side_effect_tasks
            .is_empty()
    );
    assert_eq!(
        fixture.state(&key).await.callbacks[0].state(),
        CallbackState::Succeeded
    );
    assert!(fixture.client.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn internal_delivery_handles_already_applied_missing_and_rejected_targets() {
    for mode in 0..3 {
        let fixture = Fixture::new(None, false, Default::default());
        let (parent, task) = fixture.parent(staged_request(), mode == 2).await;
        StartActivityExecutor::new(
            Arc::downgrade(&fixture.engine),
            ActivityConfig::default(),
            1000,
        )
        .execute(&parent, &task)
        .await
        .unwrap();
        let key = ExecutionKey::new(
            "ns-id",
            "child",
            fixture
                .bridge
                .current_run("ns-id", "child")
                .await
                .unwrap()
                .unwrap(),
        );
        fixture
            .bridge
            .terminate(
                key.clone(),
                "stop".into(),
                "terminate".into(),
                "client".into(),
            )
            .await
            .unwrap();
        let delivery = fixture
            .engine
            .root_node(&key)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .outbox
            .side_effect_tasks[0]
            .clone();
        if mode == 0 {
            fixture
                .engine
                .apply_side_effect_outcome(
                    &parent,
                    task.task_type_id,
                    task.id,
                    TaskOutcome::Terminated,
                )
                .await
                .unwrap();
        }
        if mode == 1 {
            fixture.engine.delete_execution(&parent).await.unwrap();
        }
        fixture.deliver.execute(&key, &delivery).await.unwrap();
        let state = fixture.state(&key).await;
        assert_eq!(
            state.callbacks[0].state(),
            if mode == 0 {
                CallbackState::Succeeded
            } else {
                CallbackState::Failed
            }
        );
        if mode == 1 {
            assert!(
                Failure::decode(state.callbacks[0].last_attempt_failure.as_slice())
                    .unwrap()
                    .message
                    .contains("parent-run")
            );
        }
        assert!(fixture.client.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn invalid_start_reports_nonretryable_outcome_and_drops_staging_task() {
    let fixture = Fixture::new(None, false, Default::default());
    let mut request = staged_request();
    request.activity_type.clear();
    let (parent, task) = fixture.parent(request, false).await;
    StartActivityExecutor::new(
        Arc::downgrade(&fixture.engine),
        ActivityConfig::default(),
        1000,
    )
    .execute(&parent, &task)
    .await
    .unwrap();
    let root = fixture.engine.root_node(&parent).await.unwrap().unwrap();
    assert!(root.metadata.outbox.side_effect_tasks.is_empty());
    let data = ParentData::decode(root.data.unwrap().as_slice()).unwrap();
    let TaskOutcome::Failed { failure, retryable } =
        serde_json::from_slice(&data.outcomes[0]).unwrap()
    else {
        panic!("rejection")
    };
    assert!(!retryable);
    assert!(
        Failure::decode(failure.as_slice())
            .unwrap()
            .message
            .contains("child")
    );
    assert!(
        fixture
            .bridge
            .current_run("ns-id", "child")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn start_result_classes_and_cross_parent_identity() {
    for error in [
        EdgeError::ActivityExecutionAlreadyStarted {
            message: "exists".into(),
            run_id: "existing-run".into(),
            start_request_id: "request".into(),
        },
        EdgeError::AlreadyExists("pointer conflict".into()),
        EdgeError::BadRequest("invalid payload".into()),
    ] {
        assert!(matches!(
            start_rejection("child", &error),
            Some(TaskOutcome::Failed {
                retryable: false,
                ..
            })
        ));
    }
    assert!(start_rejection("child", &EdgeError::Internal("storage unavailable".into())).is_none());
    let id = TaskId::new(VersionedTransition::new(11, 22), 33);
    let first = start_request_id(&ExecutionKey::new("ns", "one", "run"), id);
    assert!(first.starts_with("chasm.start_activity:11:22:33:"));
    assert_ne!(
        first,
        start_request_id(&ExecutionKey::new("ns", "two", "run"), id)
    );
}
