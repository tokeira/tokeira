use tokio::sync::oneshot;
use tokeira_types::{Payloads, QueueKey, RunKey, WorkerIdentity};

/// A transient read-only query task delivered to a worker.
pub struct QueryTask {
    /// Durable storage key for the target run.
    pub run_key: RunKey,
    /// Query handler name.
    pub query_type: String,
    /// Serialized query arguments.
    pub query_args: Payloads,
    /// Versioned workflow queue to route the query to.
    pub queue: QueueKey,
    /// Sticky worker hint when one is currently active.
    pub sticky_preferred: Option<WorkerIdentity>,
    /// One-shot response channel back to the caller.
    pub response_tx: oneshot::Sender<QueryResult>,
}

/// Result returned by a worker query handler.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryResult {
    /// Query completed successfully.
    Completed { result: Payloads },
    /// Query evaluation failed at the worker.
    Failed { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;
    use time::{Duration, OffsetDateTime};
    use tokio::sync::oneshot;
    use tokeira_kernel::{LoadedRun, StartRequest};
    use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
    use tokeira_types::{
        ExecutionRef, Memo, NamespaceId,
        QueueKey, RequestContext, RequestId, RunId, RunKey,
        SearchAttributes, TaskKind,
        TaskQueueName, WorkerIdentity,
        WorkflowId, WorkflowType,
    };

    use crate::{
        BacklogConfig, InMemoryBroker, LaneConfig,
        TimerScannerConfig, TokeiraRuntime,
        WorkflowTimeoutScannerConfig,
    };

    fn make_runtime(
        store: Arc<InMemoryStore>,
    ) -> TokeiraRuntime<InMemoryStore> {
        TokeiraRuntime::new(
            store,
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        )
    }

    fn start_request(
        ns: NamespaceId,
        wf_id: &str,
    ) -> StartRequest {
        StartRequest {
            run_key: RunKey::new(),
            namespace_id: ns,
            workflow_id: WorkflowId(wf_id.into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId(
                    format!("req-{wf_id}"),
                ),
                caller_identity: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        }
    }

    fn exec_ref(
        ns: NamespaceId,
        wf_id: &str,
    ) -> ExecutionRef {
        ExecutionRef {
            namespace_id: ns,
            workflow_id: WorkflowId(wf_id.into()),
            run_id: None,
        }
    }

    fn queue_for(ns: NamespaceId) -> QueueKey {
        QueueKey {
            namespace_id: ns,
            task_queue: TaskQueueName("q".into()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        }
    }

    // ── Property 1: Query dispatch produces no transitions
    // Feature: runtime-query-dispatch
    // Validates: Requirements 1.4, 1.5, 5.3, 7.1
    #[tokio::test]
    async fn property_query_produces_no_transitions() {
        let store = Arc::new(InMemoryStore::default());
        let runtime = make_runtime(store.clone());
        let ns = NamespaceId::new();
        let req = start_request(ns, "p1");
        let run_key = match runtime
            .start_workflow(req)
            .await
            .unwrap()
        {
            CommitResult::Applied { new_state } => {
                new_state.run_key
            }
            other => panic!("unexpected: {other:?}"),
        };

        let before = match store.load_run(run_key).await.unwrap()
        {
            LoadedRun::Existing(s) => s,
            _ => panic!("missing"),
        };

        // Dispatch query — answer it immediately so it
        // doesn't timeout.
        let broker = runtime.broker();
        let q = queue_for(ns);
        let worker = tokio::spawn(async move {
            let task = broker
                .poll_query_task(
                    &q,
                    &WorkerIdentity("w".into()),
                    std::time::Duration::from_millis(50),
                )
                .await
                .unwrap();
            let _ = task.response_tx.send(
                QueryResult::Completed {
                    result: Payloads::default(),
                },
            );
        });

        let _ = runtime
            .query_workflow(
                exec_ref(ns, "p1"),
                "check".into(),
                Payloads::default(),
                Duration::milliseconds(100),
            )
            .await
            .unwrap();
        worker.await.unwrap();

        let after = match store.load_run(run_key).await.unwrap()
        {
            LoadedRun::Existing(s) => s,
            _ => panic!("missing"),
        };

        assert_eq!(
            before.transition_seq,
            after.transition_seq
        );
        assert_eq!(
            before.last_event_id, after.last_event_id
        );
    }

    // ── Property 4: Query result round-trip
    // Feature: runtime-query-dispatch
    // Validates: Requirements 4.3, 4.4, 4.5
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_query_result_round_trip(
            is_completed in any::<bool>(),
            data in "[a-z]{1,8}",
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let (tx, rx) = oneshot::channel();
                let sent = if is_completed {
                    QueryResult::Completed {
                        result: Payloads(vec![
                            tokeira_types::Payload {
                                data: data.as_bytes().to_vec(),
                                metadata: Default::default(),
                            },
                        ]),
                    }
                } else {
                    QueryResult::Failed {
                        message: data.clone(),
                    }
                };
                tx.send(sent.clone()).unwrap();
                let received = rx.await.unwrap();
                prop_assert_eq!(received, sent);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 2: QueryTask carries correct metadata
    // Feature: runtime-query-dispatch
    // Validates: Requirements 2.1, 2.2
    #[tokio::test]
    async fn property_query_task_carries_correct_metadata()
    {
        let store = Arc::new(InMemoryStore::default());
        let runtime = make_runtime(store.clone());
        let ns = NamespaceId::new();
        let req = start_request(ns, "p2");
        let run_key = match runtime
            .start_workflow(req)
            .await
            .unwrap()
        {
            CommitResult::Applied { new_state } => {
                new_state.run_key
            }
            other => panic!("unexpected: {other:?}"),
        };

        let broker = runtime.broker();
        let q = queue_for(ns);
        let worker = tokio::spawn(async move {
            let task = broker
                .poll_query_task(
                    &q,
                    &WorkerIdentity("w".into()),
                    std::time::Duration::from_millis(50),
                )
                .await
                .unwrap();
            assert_eq!(task.run_key, run_key);
            assert_eq!(task.query_type, "my-query");
            assert_eq!(task.queue.namespace_id, ns);
            assert_eq!(
                task.queue.task_kind,
                TaskKind::Workflow
            );
            let _ = task.response_tx.send(
                QueryResult::Completed {
                    result: Payloads::default(),
                },
            );
        });

        let _ = runtime
            .query_workflow(
                exec_ref(ns, "p2"),
                "my-query".into(),
                Payloads::default(),
                Duration::milliseconds(100),
            )
            .await
            .unwrap();
        worker.await.unwrap();
    }

    // ── Property 3: Sticky affinity correctly reflected
    // Feature: runtime-query-dispatch
    // Validates: Requirements 3.1, 3.2, 3.3
    #[tokio::test]
    async fn property_sticky_affinity_reflected_on_query()
    {
        let store = Arc::new(InMemoryStore::default());
        let runtime = make_runtime(store.clone());
        let ns = NamespaceId::new();
        let req = start_request(ns, "p3");
        let _ = runtime.start_workflow(req).await.unwrap();

        // No sticky set → sticky_preferred should be None.
        let broker = runtime.broker();
        let q = queue_for(ns);
        let b1 = broker.clone();
        let q1 = q.clone();
        let worker = tokio::spawn(async move {
            let task = b1
                .poll_query_task(
                    &q1,
                    &WorkerIdentity("w".into()),
                    std::time::Duration::from_millis(50),
                )
                .await
                .unwrap();
            assert_eq!(task.sticky_preferred, None);
            let _ = task.response_tx.send(
                QueryResult::Completed {
                    result: Payloads::default(),
                },
            );
        });

        let _ = runtime
            .query_workflow(
                exec_ref(ns, "p3"),
                "check".into(),
                Payloads::default(),
                Duration::milliseconds(100),
            )
            .await
            .unwrap();
        worker.await.unwrap();
    }

    // ── Property 5: Timeout enforcement
    // Feature: runtime-query-dispatch
    // Validates: Requirements 5.1, 5.2, 8.2
    #[tokio::test]
    async fn property_query_timeout_enforcement() {
        let store = Arc::new(InMemoryStore::default());
        let runtime = make_runtime(store.clone());
        let ns = NamespaceId::new();
        let req = start_request(ns, "p5");
        let _ = runtime.start_workflow(req).await.unwrap();

        let err = runtime
            .query_workflow(
                exec_ref(ns, "p5"),
                "slow".into(),
                Payloads::default(),
                Duration::milliseconds(10),
            )
            .await
            .expect_err("should timeout");
        assert!(err.to_string().contains("timed out"));
    }

    // ── Property 6: Concurrent queries independent
    // Feature: runtime-query-dispatch
    // Validates: Requirements 6.1, 6.2, 6.3
    #[tokio::test]
    async fn property_concurrent_queries_independent() {
        let store = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(make_runtime(store.clone()));
        let ns = NamespaceId::new();
        let req = start_request(ns, "p6");
        let _ = runtime.start_workflow(req).await.unwrap();

        let broker = runtime.broker();
        let q = queue_for(ns);

        // Spawn a worker that answers 2 queries.
        let worker = tokio::spawn(async move {
            for _ in 0..2 {
                let task = broker
                    .poll_query_task(
                        &q,
                        &WorkerIdentity("w".into()),
                        std::time::Duration::from_millis(100),
                    )
                    .await
                    .unwrap();
                let _ = task.response_tx.send(
                    QueryResult::Completed {
                        result: Payloads::default(),
                    },
                );
            }
        });

        let r1 = runtime.clone();
        let h1 = tokio::spawn(async move {
            r1.query_workflow(
                exec_ref(ns, "p6"),
                "q1".into(),
                Payloads::default(),
                Duration::milliseconds(200),
            )
            .await
        });

        let r2 = runtime.clone();
        let h2 = tokio::spawn(async move {
            r2.query_workflow(
                exec_ref(ns, "p6"),
                "q2".into(),
                Payloads::default(),
                Duration::milliseconds(200),
            )
            .await
        });

        let res1 = h1.await.unwrap().unwrap();
        let res2 = h2.await.unwrap().unwrap();
        worker.await.unwrap();

        assert!(matches!(
            res1,
            QueryResult::Completed { .. }
        ));
        assert!(matches!(
            res2,
            QueryResult::Completed { .. }
        ));
    }

    // ── Property 7: Query tasks bypass dedup
    // Feature: runtime-query-dispatch
    // Validates: Requirements 6.4, 7.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_query_tasks_bypass_dedup(
            n in 2usize..6,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let broker = InMemoryBroker::default();
                let queue = queue_for(NamespaceId::new());
                let run_key = RunKey::new();

                for i in 0..n {
                    let (tx, _rx) = oneshot::channel();
                    broker
                        .publish_query_task(QueryTask {
                            run_key,
                            query_type: format!("q{i}"),
                            query_args: Payloads::default(),
                            queue: queue.clone(),
                            sticky_preferred: None,
                            response_tx: tx,
                        })
                        .await;
                }

                let mut delivered = 0;
                for _ in 0..n {
                    if broker
                        .poll_query_task(
                            &queue,
                            &WorkerIdentity("w".into()),
                            std::time::Duration::from_millis(5),
                        )
                        .await
                        .is_some()
                    {
                        delivered += 1;
                    }
                }
                prop_assert_eq!(delivered, n);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 8: Queries to closed executions not
    //    rejected at dispatch
    // Feature: runtime-query-dispatch
    // Validates: Requirements 8.1, 8.3, 8.4
    #[tokio::test]
    async fn property_closed_execution_query_dispatches()
    {
        let store = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(make_runtime(store.clone()));
        let ns = NamespaceId::new();
        let req = start_request(ns, "p8");
        let run_key = match runtime
            .start_workflow(req)
            .await
            .unwrap()
        {
            CommitResult::Applied { new_state } => {
                new_state.run_key
            }
            other => panic!("unexpected: {other:?}"),
        };

        let run_id = match store
            .load_run(run_key)
            .await
            .unwrap()
        {
            LoadedRun::Existing(s) => s.run_id,
            _ => panic!("missing"),
        };

        // Terminate the workflow.
        let _ = runtime
            .terminate_workflow(
                ExecutionRef {
                    namespace_id: ns,
                    workflow_id: WorkflowId("p8".into()),
                    run_id: Some(run_id),
                },
                tokeira_kernel::TerminateRequest {
                    reason: "done".into(),
                    details: None,
                    identity: "test".into(),
                    request: RequestContext {
                        request_id: RequestId(
                            "term".into(),
                        ),
                        caller_identity: None,
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .unwrap();

        // Query with explicit run_id should still dispatch.
        let broker = runtime.broker();
        let q = queue_for(ns);
        let worker = tokio::spawn(async move {
            let task = broker
                .poll_query_task(
                    &q,
                    &WorkerIdentity("w".into()),
                    std::time::Duration::from_millis(50),
                )
                .await
                .unwrap();
            let _ = task.response_tx.send(
                QueryResult::Failed {
                    message: "closed".into(),
                },
            );
        });

        let result = runtime
            .query_workflow(
                ExecutionRef {
                    namespace_id: ns,
                    workflow_id: WorkflowId("p8".into()),
                    run_id: Some(run_id),
                },
                "check".into(),
                Payloads::default(),
                Duration::milliseconds(100),
            )
            .await
            .unwrap();
        worker.await.unwrap();

        assert_eq!(
            result,
            QueryResult::Failed {
                message: "closed".into()
            }
        );
    }
}
