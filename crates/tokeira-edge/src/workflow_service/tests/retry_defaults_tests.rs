//! Workflow retry admission through command history, persisted successors and schedule actions.

use super::*;
use std::sync::atomic::{AtomicI64, Ordering};
use tokeira_clock::Clock;
use tokeira_kernel::{LoadedRun, WorkflowState, WorkflowTaskCompletedRequest};
use tokeira_proto::{
    common::RetryPolicy,
    history::history_event::Attributes as HistoryAttributes,
    public::temporal::api::{
        command::v1::{self as command, command::Attributes},
        schedule::v1 as schedule,
        workflow::v1 as workflow,
    },
    workflowservice,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, NexusCompletionDeps, NexusEndpointRegistry,
    NexusTimeoutScannerConfig, NoopNexusHttpClient, StartedWorkflowTask,
};
use tokeira_types::QueueKey;

use crate::{
    grpc::translate::{proto_command_to_workflow_command, start_request_to_edge},
    translate::{history_serializer::history_event_to_proto, schedule as schedule_translate},
};

#[derive(Debug, Default)]
struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(self.0.load(Ordering::SeqCst))
    }
}

impl TestClock {
    fn advance(&self, milliseconds: i64) {
        self.0.fetch_add(milliseconds, Ordering::SeqCst);
    }
}

async fn fixture() -> (
    WorkflowService,
    Arc<TokeiraRuntime<InMemoryStore>>,
    Arc<TestClock>,
) {
    let clock = Arc::new(TestClock::default());
    let store = Arc::new(InMemoryStore::default().with_clock(clock.clone()));
    let mut runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        1,
        "owner".into(),
        "node".into(),
        true,
        None,
        clock.clone(),
    );
    runtime.shutdown_wft_timeout_scanner().await.unwrap();
    runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    runtime.shutdown_activity_timeout_scanner().await.unwrap();
    runtime.shutdown_nexus_timeout_scanner().await.unwrap();
    let runtime = Arc::new(runtime);
    let cache = Arc::new(InMemoryNamespaceCache::new());
    cache
        .insert(ResolvedNamespace::active("default"))
        .await
        .unwrap();
    let service = WorkflowService::new(
        Arc::new(RuntimeAdapter::new(runtime.clone())),
        Arc::new(NoopResolver),
        Arc::new(EmptyVisibilityApi),
        store,
        Arc::new(InMemoryOperatorApi::new("local", "0.1.0+test0001")),
        cache.clone(),
        Arc::new(EdgeInterceptors::permissive(cache)),
        PollerRegistry::default(),
        crate::PendingQueryStore::default(),
        InMemoryBroker::default(),
        LongPollGate::new(LongPollConfig::default()),
        Arc::new(LocalOnlyRouter),
    );
    (service, runtime, clock)
}

fn wire_start() -> workflowservice::StartWorkflowExecutionRequest {
    workflowservice::StartWorkflowExecutionRequest {
        namespace: "default".into(),
        workflow_id: "parent".into(),
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: "workflow".into(),
        }),
        task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
            name: "queue".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn command(child: bool, retry_policy: Option<RetryPolicy>) -> WorkflowCommand {
    let start = wire_start();
    let attributes = if child {
        Attributes::StartChildWorkflowExecutionCommandAttributes(
            command::StartChildWorkflowExecutionCommandAttributes {
                workflow_id: "child".into(),
                workflow_type: start.workflow_type,
                task_queue: start.task_queue,
                retry_policy,
                ..Default::default()
            },
        )
    } else {
        Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
            command::ContinueAsNewWorkflowExecutionCommandAttributes {
                workflow_type: start.workflow_type,
                task_queue: start.task_queue,
                retry_policy,
                ..Default::default()
            },
        )
    };
    proto_command_to_workflow_command(
        command::Command {
            attributes: Some(attributes),
            ..Default::default()
        },
        "default",
    )
    .unwrap()
}

fn partial_policy() -> RetryPolicy {
    RetryPolicy {
        maximum_attempts: 3,
        ..Default::default()
    }
}

fn defaulted_policy() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        backoff_coefficient: 2.0,
        maximum_interval: Some(prost_types::Duration {
            seconds: 100,
            nanos: 0,
        }),
        maximum_attempts: 3,
        non_retryable_error_types: Vec::new(),
    }
}

fn queue() -> QueueKey {
    QueueKey {
        namespace_id: namespace_id_for("default"),
        task_queue: TaskQueueName("queue".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

async fn poll(runtime: &TokeiraRuntime<InMemoryStore>) -> Option<StartedWorkflowTask> {
    runtime.republish_queue(queue(), 64).await.unwrap();
    runtime
        .poll_workflow_task(
            queue(),
            WorkerIdentity("worker".into()),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
}

async fn complete(
    runtime: &TokeiraRuntime<InMemoryStore>,
    task: StartedWorkflowTask,
    commands: Vec<WorkflowCommand>,
    now: OffsetDateTime,
) {
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker".into()),
            commands,
            client_discards_speculative_with_events: false,
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: Default::default(),
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            eager_activities: Default::default(),
            inline_workflow_task: None,
            heartbeat: Default::default(),
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(now),
            now,
            command_sizes: Vec::new(),
        })
        .await
        .unwrap();
}

async fn state(runtime: &TokeiraRuntime<InMemoryStore>, key: RunKey) -> WorkflowState {
    let LoadedRun::Existing(state) = runtime.repo().load_run(key).await.unwrap() else {
        panic!("persisted run")
    };
    state
}

async fn started_policy(
    runtime: &TokeiraRuntime<InMemoryStore>,
    key: RunKey,
) -> Option<RetryPolicy> {
    let event = runtime
        .repo()
        .read_history_event(key, 1)
        .await
        .unwrap()
        .unwrap();
    let Some(HistoryAttributes::WorkflowExecutionStartedEventAttributes(attrs)) =
        history_event_to_proto(&event).attributes
    else {
        panic!("started event")
    };
    attrs.retry_policy
}

async fn command_run(child: bool, policy: Option<RetryPolicy>, check_history: bool) {
    let (_service, runtime, clock) = fixture().await;
    let context = EdgeContext {
        request_id: crate::request_id::RequestId::new("start"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: clock.now(),
        is_long_poll: false,
    };
    let start = crate::translate::to_internal::start_request(
        start_request_to_edge(wire_start()).unwrap(),
        &context,
        clock.now(),
    );
    let parent = start.run_key;
    runtime.start_workflow(start).await.unwrap();
    let task = poll(&runtime).await.unwrap();
    clock.advance(2_000);
    complete(
        &runtime,
        task,
        vec![command(child, policy.clone())],
        clock.now(),
    )
    .await;
    let history = runtime.repo().read_history(parent, 0, 64).await.unwrap();
    let mut recorded = None;
    let mut successor = None;
    for event in &history {
        match history_event_to_proto(event).attributes {
            Some(HistoryAttributes::StartChildWorkflowExecutionInitiatedEventAttributes(attrs)) => {
                recorded = Some(attrs.retry_policy)
            }
            Some(HistoryAttributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs)) => {
                recorded = Some(match &event.kind {
                    tokeira_kernel::HistoryEventKind::WorkflowExecutionContinuedAsNew {
                        retry_policy,
                        ..
                    } => retry_policy.as_ref().map(|policy| RetryPolicy {
                        initial_interval: Some(
                            tokeira_proto::conversions::common::to_proto_duration(
                                policy.initial_interval,
                            ),
                        ),
                        backoff_coefficient: policy.backoff_coefficient,
                        maximum_interval: policy
                            .maximum_interval
                            .map(tokeira_proto::conversions::common::to_proto_duration),
                        maximum_attempts: policy.maximum_attempts as i32,
                        non_retryable_error_types: policy.non_retryable_error_types.clone(),
                    }),
                    _ => unreachable!(),
                });
                successor = Some(RunId(
                    uuid::Uuid::parse_str(&attrs.new_execution_run_id).unwrap(),
                ));
            }
            _ => {}
        }
    }
    let task = loop {
        let task = poll(&runtime).await.unwrap();
        if task.run_key != parent {
            break task;
        }
        complete(&runtime, task, Vec::new(), clock.now()).await;
    };
    let key = task.run_key;
    let stored = state(&runtime, key).await;
    if let Some(successor) = successor {
        assert_eq!(stored.run_id, successor);
    }
    let expected = policy.as_ref().map(|_| defaulted_policy());
    if check_history {
        assert_eq!(recorded.unwrap(), expected);
        assert_eq!(started_policy(&runtime, key).await, expected);
        assert_eq!(
            stored.retry_policy.as_ref().map(|p| p.initial_interval),
            policy.as_ref().map(|_| Duration::seconds(1))
        );
    }
    clock.advance(2_000);
    complete(
        &runtime,
        task,
        vec![WorkflowCommand::FailWorkflow {
            failure: Payload::new(b"failure"),
        }],
        clock.now(),
    )
    .await;
    let history = runtime.repo().read_history(key, 0, 64).await.unwrap();
    let failed = history
        .iter()
        .find_map(|event| match history_event_to_proto(event).attributes {
            Some(HistoryAttributes::WorkflowExecutionFailedEventAttributes(attrs)) => Some(attrs),
            _ => None,
        })
        .unwrap();
    if policy.is_none() {
        assert!(failed.new_execution_run_id.is_empty());
        assert_eq!(
            failed.retry_state,
            tokeira_proto::enums::RetryState::RetryPolicyNotSet as i32
        );
        return;
    }
    let retry_id = RunId(uuid::Uuid::parse_str(&failed.new_execution_run_id).unwrap());
    let retry_key = RunKey::derive(stored.namespace_id, &stored.workflow_id, retry_id);
    let retry = state(&runtime, retry_key).await;
    assert_eq!(retry.workflow_start_delay, Some(Duration::seconds(1)));
    assert_eq!(
        started_policy(&runtime, retry_key).await,
        Some(defaulted_policy())
    );
    assert!(poll(&runtime).await.is_none());
    clock.advance(999);
    assert!(poll(&runtime).await.is_none());
    clock.advance(1);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let mut dispatched = None;
    for _ in 0..1000 {
        if let Some(task) = poll(&runtime).await {
            dispatched = Some(task.run_key);
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(dispatched, Some(retry_key));
}

// Defaults precede both persisted events: service/history/api/command_attr_validator.go:516,566-578;
// service/history/historybuilder/event_factory.go:839-855;
// service/history/transfer_queue_active_task_executor.go:1630-1639 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn child_retry_defaults_reach_history() {
    command_run(true, Some(partial_policy()), true).await;
}

// The first retry uses the default initial interval: common/retrypolicy/retry_policy.go:32-56;
// common/backoff/retry.go:180-185 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn child_retry_defaults_wait_one_second() {
    command_run(true, Some(partial_policy()), false).await;
}

// The normalized command is the successor's start request:
// service/history/api/command_attr_validator.go:442,566-578;
// service/history/workflow/mutable_state_impl.go:2554-2565 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn continue_as_new_retry_defaults_reach_history() {
    command_run(false, Some(partial_policy()), true).await;
}

// common/retrypolicy/retry_policy.go:32-56; common/backoff/retry.go:180-185 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn continue_as_new_retry_defaults_wait_one_second() {
    command_run(false, Some(partial_policy()), false).await;
}

// An absent command policy disables retries: service/history/api/command_attr_validator.go:570-572;
// service/history/workflow/mutable_state_impl.go:2554-2565 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn commands_without_retry_policy_remain_non_retrying() {
    command_run(true, None, true).await;
    command_run(false, None, true).await;
}

// EnsureDefaults changes only zero fields: common/retrypolicy/retry_policy.go:40-56 @ v1.31.0.
#[test]
fn workflow_retry_defaults_preserve_explicit_and_unlimited_policies() {
    for child in [false, true] {
        for attempts in [0, 1, 7] {
            let policy = RetryPolicy {
                initial_interval: Some(prost_types::Duration {
                    seconds: 3,
                    nanos: 4,
                }),
                maximum_interval: Some(prost_types::Duration {
                    seconds: 27,
                    nanos: 8,
                }),
                backoff_coefficient: 1.5,
                maximum_attempts: attempts,
                non_retryable_error_types: vec!["Permanent".into()],
            };
            let converted = command(child, Some(policy.clone()));
            let wire = crate::grpc::translate::workflow_command_to_proto(&converted).unwrap();
            let actual = match wire.attributes.unwrap() {
                Attributes::StartChildWorkflowExecutionCommandAttributes(attrs) => {
                    attrs.retry_policy
                }
                Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(attrs) => {
                    attrs.retry_policy
                }
                _ => panic!("workflow command"),
            };
            assert_eq!(actual, Some(policy));
        }
    }
}

fn schedule(policy: Option<RetryPolicy>) -> schedule::Schedule {
    let start = wire_start();
    schedule::Schedule {
        spec: Some(schedule::ScheduleSpec {
            interval: vec![schedule::IntervalSpec {
                interval: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
                phase: None,
            }],
            ..Default::default()
        }),
        action: Some(schedule::ScheduleAction {
            action: Some(schedule::schedule_action::Action::StartWorkflow(
                workflow::NewWorkflowExecutionInfo {
                    workflow_id: "scheduled".into(),
                    workflow_type: start.workflow_type,
                    task_queue: start.task_queue,
                    retry_policy: policy,
                    ..Default::default()
                },
            )),
        }),
        ..Default::default()
    }
}

async fn scheduled_retry_defaults(updated: bool, engine: bool) {
    let explicit = RetryPolicy {
        initial_interval: Some(prost_types::Duration {
            seconds: 3,
            nanos: 5,
        }),
        backoff_coefficient: 1.5,
        maximum_interval: Some(prost_types::Duration {
            seconds: 20,
            nanos: 7,
        }),
        maximum_attempts: 0,
        non_retryable_error_types: vec!["Permanent".into()],
    };
    for policy in [Some(partial_policy()), Some(explicit), None] {
        let (service, runtime, clock) = fixture().await;
        let (_, id, entry, _) = schedule_translate::create_schedule_request_to_edge(
            workflowservice::CreateScheduleRequest {
                namespace: "default".into(),
                schedule_id: "schedule".into(),
                schedule: Some(schedule(if updated { None } else { policy.clone() })),
                ..Default::default()
            },
            clock.now(),
        )
        .unwrap();
        service.schedule_store.create(entry).unwrap();
        if updated {
            let (namespace_id, _, _, replacement) =
                schedule_translate::update_schedule_request_to_edge(
                    workflowservice::UpdateScheduleRequest {
                        namespace: "default".into(),
                        schedule_id: id.0.clone(),
                        schedule: Some(schedule(policy.clone())),
                        ..Default::default()
                    },
                    clock.now(),
                )
                .unwrap();
            service
                .schedule_store
                .update(namespace_id, &id, &[], |entry| {
                    entry.action = replacement.action
                })
                .unwrap();
        }
        if engine {
            tokeira_runtime::trigger_schedule_action(
                &service.schedule_store,
                &runtime,
                namespace_id_for("default"),
                &id,
                clock.now(),
                clock.now(),
            )
            .await
            .unwrap();
        } else {
            service
                .trigger_scheduled_workflow(
                    namespace_id_for("default"),
                    &id,
                    clock.now(),
                    clock.now(),
                )
                .await
                .unwrap();
        }
        let task = poll(&runtime).await.unwrap();
        assert_eq!(
            started_policy(&runtime, task.run_key).await,
            policy
                .as_ref()
                .map(|policy| if policy.initial_interval.is_none() {
                    defaulted_policy()
                } else {
                    policy.clone()
                })
        );
        let entry = service
            .schedule_store
            .describe(namespace_id_for("default"), &id)
            .unwrap();
        let stored = entry.action.start_workflow.retry_policy;
        assert_eq!(
            stored.as_ref().map(|p| p.backoff_coefficient),
            policy.as_ref().map(|p| p.backoff_coefficient)
        );
    }
}

// Schedule authoring retains unset fields; firing uses frontend start admission:
// service/frontend/workflow_handler.go:3761,3812-3861,4528-4532,6594-6602;
// service/worker/scheduler/activities.go:67-74 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn created_schedule_retry_defaults_apply_when_fired() {
    scheduled_retry_defaults(false, false).await;
}

// The updated action follows the same firing path: service/frontend/workflow_handler.go:4528-4532;
// service/worker/scheduler/activities.go:67-74 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn updated_schedule_retry_defaults_apply_when_fired() {
    scheduled_retry_defaults(true, false).await;
}

// Both scheduler implementations start through frontend admission:
// service/worker/scheduler/activities.go:67-74;
// chasm/lib/scheduler/invoker_tasks.go:565-590 @ v1.31.0.
#[tokio::test(start_paused = true)]
async fn schedule_engine_retry_defaults_apply_when_fired() {
    scheduled_retry_defaults(false, true).await;
    scheduled_retry_defaults(true, true).await;
}
