//! `external-command-namespace-default` through the runtime on the in-memory store.
//!
//! A signal or cancel command whose namespace is empty targets the workflow's own
//! namespace, as v1.31.0's workflow task completion handler defaults it, and the
//! event that resolves the request records that namespace's name and id.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusNamespaceResolver, NexusTimeoutScannerConfig, NoopNexusHttpClient,
    TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};
use uuid::Uuid;

const NAMESPACE: &str = "orders";
const QUEUE: &str = "namespace-default-q";

/// The namespace resolver the engine wires, for the one namespace these tests use.
#[derive(Debug)]
struct Names(BTreeMap<NamespaceId, String>);

#[async_trait]
impl NexusNamespaceResolver for Names {
    async fn name_for_id(&self, namespace_id: NamespaceId) -> Option<String> {
        self.0.get(&namespace_id).cloned()
    }
}

/// A runtime whose namespace resolver names `namespace_id`, or none.
fn runtime(
    store: Arc<InMemoryStore>,
    namespace_id: Option<NamespaceId>,
) -> TokeiraRuntime<InMemoryStore> {
    let resolver = namespace_id.map(|namespace_id| {
        Arc::new(Names(BTreeMap::from([(
            namespace_id,
            NAMESPACE.to_string(),
        )]))) as Arc<dyn NexusNamespaceResolver>
    });
    TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        1,
        "namespace-default".to_string(),
        "127.0.0.1:0".to_string(),
        true,
        resolver,
    )
}

fn start_request(namespace_id: NamespaceId, workflow_id: &WorkflowId) -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::derive(namespace_id, workflow_id, run_id),
        namespace_id,
        workflow_id: workflow_id.clone(),
        run_id,
        workflow_type: WorkflowType("example".into()),
        task_queue: TaskQueueName(QUEUE.into()),
        input: Payloads::default(),
        header: None,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        client_cron_schedule: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: RequestId(format!("start-{}", workflow_id.0)),
            caller_identity: None,
            principal: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

async fn start(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: &WorkflowId,
) -> Result<RunKey> {
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(namespace_id, workflow_id))
        .await?
    else {
        anyhow::bail!("start not applied");
    };
    Ok(new_state.run_key)
}

/// Complete the next workflow task of `namespace_id`'s queue with `commands`.
async fn complete(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    commands: Vec<WorkflowCommand>,
) -> Result<RunKey> {
    let task = runtime
        .poll_workflow_task(
            QueueKey {
                namespace_id,
                task_queue: TaskQueueName(QUEUE.into()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            WorkerIdentity("worker".into()),
            tokio::time::Duration::from_secs(5),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("a workflow task"))?;
    let run_key = task.run_key;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands,
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            now: OffsetDateTime::now_utc(),
            command_sizes: Vec::new(),
        })
        .await?;
    Ok(run_key)
}

/// A signal command that names no namespace: the edge maps an empty namespace to
/// the nil id.
fn signal(target: &WorkflowId, signal_name: &str) -> WorkflowCommand {
    WorkflowCommand::SignalExternalWorkflowExecution {
        target_namespace_id: NamespaceId(Uuid::nil()),
        target_namespace: None,
        target_workflow_id: target.clone(),
        target_run_id: None,
        signal_name: signal_name.to_string(),
        input: Payloads(vec![Payload::new(b"input".to_vec())]),
        header: None,
        control: String::new(),
    }
}

/// A cancel command that names no namespace.
fn cancel(target: &WorkflowId) -> WorkflowCommand {
    WorkflowCommand::RequestCancelExternalWorkflowExecution {
        target_namespace_id: NamespaceId(Uuid::nil()),
        target_namespace: None,
        target_workflow_id: target.clone(),
        target_run_id: None,
        control: String::new(),
    }
}

/// The run's history once `done` accepts it. The runtime delivers and resolves a
/// request in tasks of its own, so this yields to them until then; the ceiling
/// only bounds the suite.
async fn history_until(
    store: &InMemoryStore,
    run_key: RunKey,
    done: impl Fn(&[HistoryEvent]) -> bool,
) -> Result<Vec<HistoryEvent>> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let history = store.read_history_to_end(run_key, 0).await?;
        if done(&history) {
            return Ok(history);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the history never got there: {history:?}"
        );
        tokio::task::yield_now().await;
    }
}

/// The run's history once it holds an event `found` accepts.
async fn history_with(
    store: &InMemoryStore,
    run_key: RunKey,
    found: impl Fn(&HistoryEvent) -> bool,
) -> Result<Vec<HistoryEvent>> {
    history_until(store, run_key, |history| history.iter().any(&found)).await
}

fn signaled(target: &WorkflowId) -> impl Fn(&HistoryEvent) -> bool + '_ {
    move |event| {
        matches!(
            &event.kind,
            HistoryEventKind::ExternalWorkflowExecutionSignaled { target_workflow_id, .. }
                | HistoryEventKind::SignalExternalWorkflowExecutionFailed { target_workflow_id, .. }
                if target_workflow_id == target
        )
    }
}

/// A signal to a workflow in the same namespace, naming no namespace, reaches it.
/// The initiated event records the command's empty namespace and the workflow's
/// own namespace id; the resolving event records the namespace's name and id
/// (bugfix 2.1, 2.2, 2.3).
#[tokio::test]
async fn a_signal_naming_no_namespace_reaches_its_workflow() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let runtime = runtime(store.clone(), Some(namespace_id));
    let sender = WorkflowId("sender".into());
    let receiver = WorkflowId("receiver".into());
    let receiver_run = start(&runtime, namespace_id, &receiver).await?;
    complete(&runtime, namespace_id, Vec::new()).await?;
    let sender_run = start(&runtime, namespace_id, &sender).await?;
    complete(&runtime, namespace_id, vec![signal(&receiver, "poke")]).await?;

    let history = history_with(&store, sender_run, signaled(&receiver)).await?;
    let initiated = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                namespace_id,
                namespace,
                ..
            } => Some((*namespace_id, namespace.clone())),
            _ => None,
        })
        .expect("the initiated event");
    assert_eq!(initiated, (namespace_id, None));
    let resolved = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::ExternalWorkflowExecutionSignaled {
                namespace_id,
                namespace,
                ..
            } => Some((*namespace_id, namespace.clone())),
            _ => None,
        })
        .expect("the signal reached its workflow");
    assert_eq!(resolved, (namespace_id, Some(NAMESPACE.to_string())));
    history_with(&store, receiver_run, |event| {
        matches!(
            &event.kind,
            HistoryEventKind::WorkflowExecutionSignaled { signal_name, .. } if signal_name == "poke"
        )
    })
    .await?;
    Ok(())
}

/// A cancel request to a workflow in the same namespace, naming no namespace,
/// records the namespace's name and id in the event that resolves it (bugfix 2.3,
/// 3.2).
#[tokio::test]
async fn a_cancel_request_naming_no_namespace_names_its_namespace() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let runtime = runtime(store.clone(), Some(namespace_id));
    let sender = WorkflowId("canceller".into());
    let target = WorkflowId("cancelled".into());
    start(&runtime, namespace_id, &target).await?;
    complete(&runtime, namespace_id, Vec::new()).await?;
    let sender_run = start(&runtime, namespace_id, &sender).await?;
    complete(&runtime, namespace_id, vec![cancel(&target)]).await?;

    let history = history_with(&store, sender_run, |event| {
        matches!(
            &event.kind,
            HistoryEventKind::ExternalWorkflowExecutionCancelRequested { .. }
                | HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed { .. }
        )
    })
    .await?;
    let initiated = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                namespace_id,
                namespace,
                ..
            } => Some((*namespace_id, namespace.clone())),
            _ => None,
        })
        .expect("the initiated event");
    assert_eq!(initiated, (namespace_id, None));
    let resolved = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::ExternalWorkflowExecutionCancelRequested {
                namespace_id,
                namespace,
                ..
            } => Some((*namespace_id, namespace.clone())),
            _ => None,
        })
        .expect("the cancel request reached its workflow");
    assert_eq!(resolved, (namespace_id, Some(NAMESPACE.to_string())));
    Ok(())
}

/// Without a namespace resolver, a resolving event whose command named no
/// namespace keeps an empty name, and the signal still reaches its workflow.
#[tokio::test]
async fn without_a_resolver_the_resolving_event_has_no_name() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let runtime = runtime(store.clone(), None);
    let sender = WorkflowId("sender".into());
    let receiver = WorkflowId("receiver".into());
    start(&runtime, namespace_id, &receiver).await?;
    complete(&runtime, namespace_id, Vec::new()).await?;
    let sender_run = start(&runtime, namespace_id, &sender).await?;
    complete(&runtime, namespace_id, vec![signal(&receiver, "poke")]).await?;

    let history = history_with(&store, sender_run, signaled(&receiver)).await?;
    assert!(
        history.iter().any(|event| matches!(
            &event.kind,
            HistoryEventKind::ExternalWorkflowExecutionSignaled { namespace_id: id, namespace: None, .. }
                if *id == namespace_id
        )),
        "{history:?}"
    );
    Ok(())
}

proptest! {
    // A failing case waits out the suite's ceiling, so shrinking stays short.
    #![proptest_config(ProptestConfig { cases: 16, max_shrink_iters: 4, ..ProptestConfig::default() })]

    // Feature: external-command-namespace-default, Property 3: A signal within a
    // namespace reaches its workflow
    #[test]
    fn property_a_signal_within_a_namespace_reaches_its_workflow(
        sender in "[a-z]{1,12}",
        receiver in "[a-z]{1,12}",
        signal_name in "[a-z]{1,12}",
    ) {
        prop_assume!(sender != receiver);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = Arc::new(InMemoryStore::default());
            let namespace_id = NamespaceId::new();
            let runtime = runtime(store.clone(), Some(namespace_id));
            let sender = WorkflowId(sender);
            let receiver = WorkflowId(receiver);
            let receiver_run = start(&runtime, namespace_id, &receiver).await?;
            complete(&runtime, namespace_id, Vec::new()).await?;
            let sender_run = start(&runtime, namespace_id, &sender).await?;
            complete(
                &runtime,
                namespace_id,
                vec![signal(&receiver, &signal_name), signal(&sender, &signal_name)],
            )
            .await?;

            history_with(&store, receiver_run, |event| matches!(
                &event.kind,
                HistoryEventKind::WorkflowExecutionSignaled { signal_name: name, .. }
                    if *name == signal_name
            ))
            .await?;
            let history = history_until(&store, sender_run, |history| {
                history.iter().any(signaled(&sender)) && history.iter().any(signaled(&receiver))
            })
            .await?;
            // A workflow can't signal itself (`transfer_queue_active_task_executor.go:683-697 @ v1.31.0`).
            anyhow::ensure!(history.iter().any(|event| matches!(
                &event.kind,
                HistoryEventKind::SignalExternalWorkflowExecutionFailed {
                    target_workflow_id, namespace_id: id, namespace, cause, ..
                } if *target_workflow_id == sender
                    && *id == namespace_id
                    && namespace.as_deref() == Some(NAMESPACE)
                    && cause == "EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND"
            )), "the signal to itself: {history:?}");
            anyhow::ensure!(history.iter().any(|event| matches!(
                &event.kind,
                HistoryEventKind::ExternalWorkflowExecutionSignaled {
                    target_workflow_id, namespace_id: id, namespace, ..
                } if *target_workflow_id == receiver
                    && *id == namespace_id
                    && namespace.as_deref() == Some(NAMESPACE)
            )), "the signal to the other: {history:?}");
            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }
}
