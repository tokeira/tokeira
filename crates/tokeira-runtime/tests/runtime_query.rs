use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};

use tokeira_kernel::{StartRequest, TerminateRequest, WorkflowTaskCompletedRequest};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, QueryResult, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payloads, RequestContext, RequestId, SearchAttributes,
    WorkerIdentity, WorkflowId, WorkflowType,
};

#[tokio::test]
async fn query_roundtrip_returns_worker_result() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("query-success".into());
    let run_key = start_workflow(&runtime, namespace_id, workflow_id.clone()).await?;
    quiesce_workflow(&runtime, namespace_id).await?;

    let broker = runtime.broker();
    let worker = tokio::spawn(async move {
        let query = broker
            .poll_query_task(
                &workflow_queue(namespace_id),
                &WorkerIdentity("worker-a".into()),
                std::time::Duration::from_millis(50),
            )
            .await
            .expect("query should be delivered");
        assert_eq!(query.run_key, run_key);
        assert_eq!(query.query_type, "describe");
        let _ = query.response_tx.send(QueryResult::Completed {
            result: payloads("ok"),
        });
    });

    let result = runtime
        .query_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: None,
            },
            "describe".into(),
            Payloads::default(),
            Duration::milliseconds(100),
        )
        .await?;

    worker.await.unwrap();
    assert_eq!(
        result,
        QueryResult::Completed {
            result: payloads("ok")
        }
    );
    Ok(())
}

#[tokio::test]
async fn query_times_out_when_no_worker_responds() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = make_runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("query-timeout".into());
    let _ = start_workflow(&runtime, namespace_id, workflow_id.clone()).await?;
    quiesce_workflow(&runtime, namespace_id).await?;

    let error = runtime
        .query_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: None,
            },
            "describe".into(),
            Payloads::default(),
            Duration::milliseconds(10),
        )
        .await
        .expect_err("query should time out");
    assert!(error.to_string().contains("timed out"));
    Ok(())
}

#[tokio::test]
async fn closed_run_query_with_explicit_run_id_still_dispatches() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("query-closed".into());
    let run_key = start_workflow(&runtime, namespace_id, workflow_id.clone()).await?;
    quiesce_workflow(&runtime, namespace_id).await?;
    let run_id = match store.load_run(run_key).await? {
        tokeira_kernel::LoadedRun::Existing(state) => state.run_id,
        tokeira_kernel::LoadedRun::Absent => panic!("run missing"),
    };

    let _ = runtime
        .terminate_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: workflow_id.clone(),
                run_id: Some(run_id),
            },
            TerminateRequest {
                reason: "done".into(),
                details: None,
                identity: "tester".into(),
                request: RequestContext {
                    request_id: RequestId("term-1".into()),
                    caller_identity: Some("tester".into()),
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;

    let broker = runtime.broker();
    let worker = tokio::spawn(async move {
        let query = broker
            .poll_query_task(
                &workflow_queue(namespace_id),
                &WorkerIdentity("worker-a".into()),
                std::time::Duration::from_millis(50),
            )
            .await
            .expect("query should still dispatch");
        let _ = query.response_tx.send(QueryResult::Failed {
            message: "closed cache".into(),
        });
    });

    let result = runtime
        .query_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: Some(run_id),
            },
            "closed-check".into(),
            Payloads::default(),
            Duration::milliseconds(100),
        )
        .await?;

    worker.await.unwrap();
    assert_eq!(
        result,
        QueryResult::Failed {
            message: "closed cache".into()
        }
    );
    Ok(())
}

fn make_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

async fn start_workflow(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
) -> Result<tokeira_types::RunKey> {
    let run_id = tokeira_types::RunId::new();
    let result = runtime
        .start_workflow(StartRequest {
            run_key: tokeira_types::RunKey::new(),
            namespace_id,
            workflow_id,
            run_id,
            workflow_type: WorkflowType("query-workflow".into()),
            task_queue: tokeira_types::TaskQueueName("queue-a".into()),
            deployment: None,
            build_id: None,
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
            continued_execution_run_id: None,
            attempt: 1,
            first_execution_run_id: None,
            first_run_started_at: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            request: RequestContext {
                request_id: RequestId("start-1".into()),
                caller_identity: Some("tester".into()),
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
            cron_schedule: None,
            reserved_poller_identity: None,
        })
        .await?;
    Ok(match result {
        CommitResult::Applied { new_state } => new_state.run_key,
        other => panic!("unexpected start result: {other:?}"),
    })
}

async fn quiesce_workflow(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
) -> Result<()> {
    let started = runtime
        .poll_workflow_task(
            workflow_queue(namespace_id),
            WorkerIdentity("worker-a".into()),
            std::time::Duration::from_millis(50),
        )
        .await?
        .expect("initial workflow task should be available");
    let _ = runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: started.token,
            identity: WorkerIdentity("worker-a".into()),
            sdk_metadata: None,
            worker_version: None,
            commands: Vec::new(),
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(())
}

fn workflow_queue(namespace_id: NamespaceId) -> tokeira_types::QueueKey {
    tokeira_types::QueueKey {
        namespace_id,
        task_queue: tokeira_types::TaskQueueName("queue-a".into()),
        task_kind: tokeira_types::TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload {
        data: value.as_bytes().to_vec(),
        metadata: Default::default(),
    }])
}
