//! Provider-neutral delivery over the existing Nexus endpoint transports.
//!
//! Endpoint metadata is resolved for every attempt. External targets reuse the
//! outbound Nexus HTTP client; Worker targets reuse the Nexus task broker and its
//! ordinary public poll/respond surface. Retry policy and durable action state remain
//! outside this adapter.

use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::KeyValue;
use time::OffsetDateTime;
use tokeira_storage::WorkerComputeProviderAction;

use super::{
    WorkerComputeProviderCompletion, WorkerComputeProviderOutcome, provider_nexus_invocation,
    validate_provider_completion,
};
use crate::nexus::{
    EndpointTarget, NexusEndpointRegistry, NexusHttpClient, NexusHttpTaskRequest,
    NexusHttpTaskRequestVariant, NexusStartResult, NexusTaskBroker, NexusTaskRequest,
};

/// Bounded Nexus target classification used by delivery telemetry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerComputeProviderTargetKind {
    /// The endpoint was absent when this attempt resolved it.
    Unresolved,
    /// The endpoint dispatched through the existing outbound HTTP client.
    External,
    /// The endpoint dispatched through the existing Nexus task broker.
    Worker,
}

/// Provider-neutral result of one transport attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerComputeProviderAttempt {
    /// Nexus transport selected from the endpoint record current at attempt time.
    pub target_kind: WorkerComputeProviderTargetKind,
    /// Policy-neutral completion classification consumed by the durable outbox.
    pub outcome: WorkerComputeProviderOutcome,
}

/// Provider-neutral attempt adapter consumed by durable outbox delivery.
#[async_trait]
pub trait WorkerComputeProvider: Send + Sync {
    /// Deliver immutable action bytes once under the current claim fence.
    async fn deliver(
        &self,
        action: &WorkerComputeProviderAction,
        claim_epoch: u64,
        now: OffsetDateTime,
    ) -> WorkerComputeProviderAttempt;
}

/// Existing-Nexus transport adapter for worker-compute provider actions.
#[derive(Clone)]
pub struct NexusWorkerComputeProvider {
    endpoints: NexusEndpointRegistry,
    http_client: Arc<dyn NexusHttpClient>,
    task_broker: NexusTaskBroker,
    attempt_timeout: std::time::Duration,
}

impl std::fmt::Debug for NexusWorkerComputeProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NexusWorkerComputeProvider")
            .finish_non_exhaustive()
    }
}

impl NexusWorkerComputeProvider {
    /// Construct an adapter from the same endpoint registry and transports used by
    /// workflow-originated Nexus operations.
    #[must_use]
    pub fn new(
        endpoints: NexusEndpointRegistry,
        http_client: Arc<dyn NexusHttpClient>,
        task_broker: NexusTaskBroker,
    ) -> Self {
        Self {
            endpoints,
            http_client,
            task_broker,
            attempt_timeout: super::PROVIDER_ATTEMPT_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_attempt_timeout(mut self, attempt_timeout: std::time::Duration) -> Self {
        self.attempt_timeout = attempt_timeout;
        self
    }

    /// Deliver one already-claimed action without applying retry or persistence.
    pub async fn deliver(
        &self,
        action: &WorkerComputeProviderAction,
        claim_epoch: u64,
        now: OffsetDateTime,
    ) -> WorkerComputeProviderAttempt {
        let invocation = provider_nexus_invocation(action);
        let Some(endpoint) = self.endpoints.resolve(&action.endpoint_name) else {
            return WorkerComputeProviderAttempt {
                target_kind: WorkerComputeProviderTargetKind::Unresolved,
                outcome: WorkerComputeProviderOutcome::RetryableFailure(
                    tokeira_types::WorkerComputeFailureCategory::EndpointNotFound,
                ),
            };
        };
        let (target_kind, completion) = match endpoint.target {
            EndpointTarget::External { address } => {
                let timeout = time::Duration::try_from(self.attempt_timeout)
                    .expect("fixed provider timeout fits time::Duration");
                let completion = match self
                    .http_client
                    .start_operation(
                        &address,
                        &invocation.request_id,
                        invocation.service,
                        invocation.operation,
                        &invocation.input,
                        Some(timeout),
                        &[] as &[KeyValue],
                    )
                    .await
                {
                    Ok(NexusStartResult::SyncCompleted { result, .. }) => {
                        WorkerComputeProviderCompletion::Synchronous(result)
                    }
                    Ok(NexusStartResult::SyncFailed { .. }) => {
                        WorkerComputeProviderCompletion::OperationUnsuccessful
                    }
                    Ok(NexusStartResult::AsyncAccepted { .. }) => {
                        WorkerComputeProviderCompletion::Asynchronous
                    }
                    Ok(NexusStartResult::HandlerError { retryable, .. }) => {
                        WorkerComputeProviderCompletion::HandlerError { retryable }
                    }
                    Err(_) => WorkerComputeProviderCompletion::TransportFailure,
                };
                (WorkerComputeProviderTargetKind::External, completion)
            }
            EndpointTarget::Worker {
                namespace_id,
                task_queue,
            } => {
                let Some(receiver) = self
                    .task_broker
                    .register_worker_compute_waiter(action.action_id, claim_epoch)
                else {
                    return WorkerComputeProviderAttempt {
                        target_kind: WorkerComputeProviderTargetKind::Worker,
                        outcome: WorkerComputeProviderOutcome::RetryableFailure(
                            tokeira_types::WorkerComputeFailureCategory::Transport,
                        ),
                    };
                };
                let [payload] = invocation.input.0.as_slice() else {
                    return WorkerComputeProviderAttempt {
                        target_kind: WorkerComputeProviderTargetKind::Worker,
                        outcome: WorkerComputeProviderOutcome::TerminalFailure(
                            tokeira_types::WorkerComputeFailureCategory::InvalidResponsePayload,
                        ),
                    };
                };
                let timeout = time::Duration::try_from(self.attempt_timeout)
                    .expect("fixed provider timeout fits time::Duration");
                let request = NexusTaskRequest::Http(NexusHttpTaskRequest {
                    header: Default::default(),
                    scheduled_time: now,
                    temporal_failure_responses: true,
                    endpoint: action.endpoint_name.clone(),
                    dispatch_deadline: now + timeout,
                    variant: NexusHttpTaskRequestVariant::StartOperation {
                        service: invocation.service.to_owned(),
                        operation: invocation.operation.to_owned(),
                        request_id: invocation.request_id.clone(),
                        callback: String::new(),
                        payload: Some(payload.clone()),
                        callback_header: Default::default(),
                        links: Vec::new(),
                    },
                });
                let lease = self
                    .task_broker
                    .publish_worker_compute(
                        namespace_id,
                        task_queue,
                        action.action_id,
                        claim_epoch,
                        request,
                    )
                    .await;
                let completion = tokio::time::timeout(self.attempt_timeout, receiver).await;
                lease.cancel().await;
                let completion = match completion {
                    Ok(Ok(completion)) => completion,
                    Ok(Err(_)) | Err(_) => WorkerComputeProviderCompletion::TransportFailure,
                };
                (WorkerComputeProviderTargetKind::Worker, completion)
            }
        };
        WorkerComputeProviderAttempt {
            target_kind,
            outcome: validate_provider_completion(action.action_id, &completion),
        }
    }
}

#[async_trait]
impl WorkerComputeProvider for NexusWorkerComputeProvider {
    async fn deliver(
        &self,
        action: &WorkerComputeProviderAction,
        claim_epoch: u64,
        now: OffsetDateTime,
    ) -> WorkerComputeProviderAttempt {
        Self::deliver(self, action, claim_epoch, now).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use proptest::prelude::*;
    use prost::Message;
    use time::OffsetDateTime;
    use tokeira_proto::compute::v1::InvokeWorkerResponse;
    use tokeira_types::{
        NamespaceId, Payload, Payloads, TaskQueueName, WorkerComputeFailureCategory,
    };

    use super::*;
    use crate::{
        InMemoryNexusEndpointStore, NexusCancelResult, NexusEndpointSpec, NexusEndpointSpecTarget,
        NexusEndpointStore, WorkerComputeProviderCompletion,
    };

    #[derive(Clone, Copy, Debug)]
    enum TestHttpOutcome {
        MatchingSuccess,
        MismatchedSuccess,
        Async,
        OperationUnsuccessful,
        RetryableHandler,
        TerminalHandler,
        TransportFailure,
    }

    #[derive(Debug)]
    struct RecordingHttpClient {
        addresses: Mutex<Vec<String>>,
        outcomes: Mutex<VecDeque<TestHttpOutcome>>,
    }

    impl Default for RecordingHttpClient {
        fn default() -> Self {
            Self::with_outcomes(std::iter::once(TestHttpOutcome::MatchingSuccess))
        }
    }

    impl RecordingHttpClient {
        fn with_outcomes(outcomes: impl IntoIterator<Item = TestHttpOutcome>) -> Self {
            Self {
                addresses: Mutex::new(Vec::new()),
                outcomes: Mutex::new(outcomes.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl NexusHttpClient for RecordingHttpClient {
        async fn start_operation(
            &self,
            address: &str,
            operation_id: &str,
            _service: &str,
            _operation: &str,
            _input: &Payloads,
            _schedule_to_close_timeout: Option<time::Duration>,
            _trace_headers: &[KeyValue],
        ) -> Result<NexusStartResult> {
            self.addresses
                .lock()
                .expect("address recorder lock poisoned")
                .push(address.to_owned());
            match self
                .outcomes
                .lock()
                .expect("outcome recorder lock poisoned")
                .pop_front()
                .unwrap_or(TestHttpOutcome::MatchingSuccess)
            {
                TestHttpOutcome::MatchingSuccess => Ok(NexusStartResult::SyncCompleted {
                    result: response_payload(operation_id),
                    links: Vec::new(),
                }),
                TestHttpOutcome::MismatchedSuccess => Ok(NexusStartResult::SyncCompleted {
                    result: response_payload(&uuid::Uuid::new_v4().to_string()),
                    links: Vec::new(),
                }),
                TestHttpOutcome::Async => Ok(NexusStartResult::AsyncAccepted {
                    operation_token: "not-supported".to_owned(),
                    links: Vec::new(),
                }),
                TestHttpOutcome::OperationUnsuccessful => Ok(NexusStartResult::SyncFailed {
                    message: "provider rejected request".to_owned(),
                }),
                TestHttpOutcome::RetryableHandler => Ok(NexusStartResult::HandlerError {
                    error_type: "TEST".to_owned(),
                    failure: None,
                    retryable: true,
                }),
                TestHttpOutcome::TerminalHandler => Ok(NexusStartResult::HandlerError {
                    error_type: "TEST".to_owned(),
                    failure: None,
                    retryable: false,
                }),
                TestHttpOutcome::TransportFailure => Err(anyhow!("transport unavailable")),
            }
        }

        async fn cancel_operation(
            &self,
            _address: &str,
            _service: &str,
            _operation: &str,
            _operation_token: &str,
            _trace_headers: &[KeyValue],
        ) -> Result<NexusCancelResult> {
            Ok(NexusCancelResult::Succeeded)
        }
    }

    fn response_payload(request_id: &str) -> Payloads {
        Payloads(vec![Payload {
            data: InvokeWorkerResponse {
                request_id: request_id.to_owned(),
            }
            .encode_to_vec(),
            metadata: std::collections::BTreeMap::from([
                ("encoding".to_owned(), "binary/protobuf".to_owned()),
                (
                    "messageType".to_owned(),
                    tokeira_proto::compute::INVOKE_WORKER_RESPONSE_MESSAGE_TYPE.to_owned(),
                ),
            ]),
            external_payloads: Vec::new(),
        }])
    }

    fn action(endpoint_name: &str) -> WorkerComputeProviderAction {
        super::super::build_provider_action(super::super::ProviderActionInput {
            action_id: uuid::Uuid::new_v4(),
            controller_key: tokeira_types::ControllerInstanceKey {
                namespace_id: NamespaceId::new(),
                deployment_name: tokeira_types::DeploymentId("deployment".to_owned()),
                build_id: tokeira_types::BuildId("build".to_owned()),
            },
            namespace_name: "namespace".to_owned(),
            scaling_group: tokeira_types::ScalingGroupId("group".to_owned()),
            fingerprint: tokeira_types::ConfigurationFingerprint::from_bytes([1; 32]),
            provider: super::super::RemoteNexusProvider {
                provider_type: "remote".to_owned(),
                details: None,
                nexus_endpoint: endpoint_name.to_owned(),
            },
            reason: tokeira_types::WorkerComputeInvokeReason::NoSyncMatch,
            task_queues: Vec::new(),
            now: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("provider action")
    }

    fn external_spec(name: &str, url: &str) -> NexusEndpointSpec {
        NexusEndpointSpec {
            name: name.to_owned(),
            description: Vec::new(),
            target: NexusEndpointSpecTarget::External {
                url: url.to_owned(),
            },
        }
    }

    #[derive(Clone, Debug)]
    enum EndpointMutation {
        External(String),
        Worker,
        Missing,
    }

    fn endpoint_mutation_strategy() -> impl Strategy<Value = EndpointMutation> {
        prop_oneof![
            "https://[a-z]{1,8}\\.example".prop_map(EndpointMutation::External),
            Just(EndpointMutation::Worker),
            Just(EndpointMutation::Missing),
        ]
    }

    #[tokio::test]
    async fn external_target_maps_transport_and_handler_outcomes_without_owning_retry() {
        let cases = [
            (
                TestHttpOutcome::MatchingSuccess,
                WorkerComputeProviderOutcome::Delivered,
            ),
            (
                TestHttpOutcome::MismatchedSuccess,
                WorkerComputeProviderOutcome::TerminalFailure(
                    WorkerComputeFailureCategory::ResponseIdMismatch,
                ),
            ),
            (
                TestHttpOutcome::Async,
                WorkerComputeProviderOutcome::TerminalFailure(
                    WorkerComputeFailureCategory::AsyncResponse,
                ),
            ),
            (
                TestHttpOutcome::OperationUnsuccessful,
                WorkerComputeProviderOutcome::TerminalFailure(
                    WorkerComputeFailureCategory::OperationUnsuccessful,
                ),
            ),
            (
                TestHttpOutcome::RetryableHandler,
                WorkerComputeProviderOutcome::RetryableFailure(
                    WorkerComputeFailureCategory::RetryableHandler,
                ),
            ),
            (
                TestHttpOutcome::TerminalHandler,
                WorkerComputeProviderOutcome::TerminalFailure(
                    WorkerComputeFailureCategory::NonRetryableHandler,
                ),
            ),
            (
                TestHttpOutcome::TransportFailure,
                WorkerComputeProviderOutcome::RetryableFailure(
                    WorkerComputeFailureCategory::Transport,
                ),
            ),
        ];
        let store = Arc::new(InMemoryNexusEndpointStore::new());
        store
            .create(external_spec("provider", "https://provider.example"), 0)
            .expect("external endpoint");
        let client = Arc::new(RecordingHttpClient::with_outcomes(
            cases.iter().map(|(outcome, _)| *outcome),
        ));
        let provider = NexusWorkerComputeProvider::new(
            NexusEndpointRegistry::new(store),
            client,
            NexusTaskBroker::default(),
        );
        let action = action("provider");

        for (claim_epoch, (_, expected)) in cases.into_iter().enumerate() {
            assert_eq!(
                provider
                    .deliver(
                        &action,
                        u64::try_from(claim_epoch).expect("small case index"),
                        OffsetDateTime::UNIX_EPOCH,
                    )
                    .await
                    .outcome,
                expected,
            );
        }
    }

    #[tokio::test]
    async fn endpoint_delete_and_recreate_are_observed_without_mutating_action_bytes() {
        let store = Arc::new(InMemoryNexusEndpointStore::new());
        let first = store
            .create(external_spec("provider", "https://first.example"), 0)
            .expect("first endpoint");
        let client = Arc::new(RecordingHttpClient::default());
        let provider = NexusWorkerComputeProvider::new(
            NexusEndpointRegistry::new(store.clone()),
            client.clone(),
            NexusTaskBroker::default(),
        );
        let action = action("provider");
        let request_data = action.request_data.clone();
        assert_eq!(
            provider
                .deliver(&action, 1, OffsetDateTime::UNIX_EPOCH)
                .await
                .outcome,
            WorkerComputeProviderOutcome::Delivered,
        );

        store.delete(&first.id).expect("delete endpoint");
        assert_eq!(
            provider
                .deliver(&action, 2, OffsetDateTime::UNIX_EPOCH)
                .await
                .outcome,
            WorkerComputeProviderOutcome::RetryableFailure(
                WorkerComputeFailureCategory::EndpointNotFound,
            ),
        );
        store
            .create(external_spec("provider", "https://second.example"), 1)
            .expect("recreated endpoint");
        assert_eq!(
            provider
                .deliver(&action, 3, OffsetDateTime::UNIX_EPOCH)
                .await
                .outcome,
            WorkerComputeProviderOutcome::Delivered,
        );
        assert_eq!(action.request_data, request_data);
        assert_eq!(
            *client
                .addresses
                .lock()
                .expect("address recorder lock poisoned"),
            ["https://first.example", "https://second.example"],
        );
    }

    #[tokio::test]
    async fn worker_target_timeout_removes_task_route_and_waiter() {
        let store = Arc::new(InMemoryNexusEndpointStore::new());
        let worker_namespace_id = NamespaceId::new();
        store
            .create(
                NexusEndpointSpec {
                    name: "provider".to_owned(),
                    description: Vec::new(),
                    target: NexusEndpointSpecTarget::Worker {
                        namespace_name: "workers".to_owned(),
                        namespace_id: worker_namespace_id.0.to_string(),
                        task_queue: "provider-tasks".to_owned(),
                    },
                },
                0,
            )
            .expect("worker endpoint");
        let broker = NexusTaskBroker::default();
        let provider = NexusWorkerComputeProvider::new(
            NexusEndpointRegistry::new(store),
            Arc::new(RecordingHttpClient::default()),
            broker.clone(),
        )
        .with_attempt_timeout(std::time::Duration::ZERO);
        let action = action("provider");
        assert_eq!(
            provider
                .deliver(&action, 9, OffsetDateTime::UNIX_EPOCH)
                .await
                .outcome,
            WorkerComputeProviderOutcome::RetryableFailure(WorkerComputeFailureCategory::Transport,),
        );
        assert!(
            broker
                .poll(
                    worker_namespace_id,
                    TaskQueueName("provider-tasks".to_owned()),
                    std::time::Duration::ZERO,
                )
                .await
                .is_none()
        );
        assert!(!broker.complete_worker_compute(
            action.action_id,
            9,
            WorkerComputeProviderCompletion::TransportFailure,
        ));
    }

    #[tokio::test]
    async fn worker_target_uses_existing_broker_and_attempt_correlation() {
        let store = Arc::new(InMemoryNexusEndpointStore::new());
        let worker_namespace_id = NamespaceId::new();
        store
            .create(
                NexusEndpointSpec {
                    name: "provider".to_owned(),
                    description: Vec::new(),
                    target: NexusEndpointSpecTarget::Worker {
                        namespace_name: "workers".to_owned(),
                        namespace_id: worker_namespace_id.0.to_string(),
                        task_queue: "provider-tasks".to_owned(),
                    },
                },
                0,
            )
            .expect("worker endpoint");
        let broker = NexusTaskBroker::default();
        let provider = NexusWorkerComputeProvider::new(
            NexusEndpointRegistry::new(store),
            Arc::new(RecordingHttpClient::default()),
            broker.clone(),
        );
        let action = action("provider");
        let action_id = action.action_id;
        let delivery = tokio::spawn(async move {
            provider
                .deliver(&action, 7, OffsetDateTime::now_utc())
                .await
        });
        let task = broker
            .poll(
                worker_namespace_id,
                TaskQueueName("provider-tasks".to_owned()),
                std::time::Duration::from_secs(1),
            )
            .await
            .expect("worker-target provider task");
        let expected_origin = task.origin.clone();
        let NexusTaskRequest::Http(request) = task.request else {
            panic!("provider delivery must use the neutral HTTP request envelope");
        };
        assert_eq!(request.endpoint, "provider");
        let NexusHttpTaskRequestVariant::StartOperation {
            service,
            operation,
            request_id,
            callback,
            payload,
            ..
        } = request.variant
        else {
            panic!("provider delivery must start an operation");
        };
        assert_eq!(service, tokeira_proto::compute::NEXUS_SERVICE_NAME);
        assert_eq!(operation, tokeira_proto::compute::INVOKE_WORKER_OPERATION);
        assert_eq!(request_id, action_id.to_string());
        assert!(callback.is_empty());
        assert!(payload.is_some());
        assert_eq!(
            broker.consume(&task.token.task_id).await,
            Some(crate::NexusTaskCorrelation::WorkerCompute {
                action_id,
                claim_epoch: 7,
                origin: expected_origin,
            })
        );
        assert!(broker.complete_worker_compute(
            action_id,
            7,
            WorkerComputeProviderCompletion::Synchronous(response_payload(&action_id.to_string(),)),
        ));
        assert_eq!(
            delivery.await.expect("delivery task").outcome,
            WorkerComputeProviderOutcome::Delivered,
        );
        assert!(broker.consume(&task.token.task_id).await.is_none());
        assert!(
            !broker.complete_worker_compute(
                action_id,
                7,
                WorkerComputeProviderCompletion::TransportFailure,
            ),
            "late responses must not find an attempt waiter"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 15: endpoint re-resolution changes only transport
        #[test]
        fn property_endpoint_re_resolution_changes_only_transport(
            mutations in proptest::collection::vec(endpoint_mutation_strategy(), 1..12),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let store = Arc::new(InMemoryNexusEndpointStore::new());
                let worker_namespace_id = NamespaceId::new();
                let client = Arc::new(RecordingHttpClient::default());
                let broker = NexusTaskBroker::default();
                let provider = NexusWorkerComputeProvider::new(
                    NexusEndpointRegistry::new(store.clone()),
                    client.clone(),
                    broker.clone(),
                );
                let action = action("provider");
                let original_bytes = action.request_data.clone();
                let mut record: Option<crate::NexusEndpointRecord> = None;
                let mut expected_addresses = Vec::new();
                for (index, mutation) in mutations.iter().enumerate() {
                    let now = i64::try_from(index).expect("small generated index");
                    let spec = match mutation {
                        EndpointMutation::External(url) => {
                            Some(external_spec("provider", url))
                        }
                        EndpointMutation::Worker => Some(NexusEndpointSpec {
                            name: "provider".to_owned(),
                            description: Vec::new(),
                            target: NexusEndpointSpecTarget::Worker {
                                namespace_name: "workers".to_owned(),
                                namespace_id: worker_namespace_id.0.to_string(),
                                task_queue: "provider-tasks".to_owned(),
                            },
                        }),
                        EndpointMutation::Missing => None,
                    };
                    if let Some(spec) = spec {
                        record = Some(match record {
                            Some(ref current) => store
                                .update(&current.id, current.version, spec, now)
                                .expect("endpoint update"),
                            None => store.create(spec, now).expect("endpoint create"),
                        });
                    } else if let Some(current) = record.take() {
                        store.delete(&current.id).expect("endpoint delete");
                    }

                    let claim_epoch = u64::try_from(index).expect("small generated index");
                    match mutation {
                        EndpointMutation::External(url) => {
                            expected_addresses.push(url.clone());
                            assert_eq!(
                                provider
                                    .deliver(
                                        &action,
                                        claim_epoch,
                                        OffsetDateTime::UNIX_EPOCH,
                                    )
                                    .await
                                    .outcome,
                                WorkerComputeProviderOutcome::Delivered,
                            );
                        }
                        EndpointMutation::Worker => {
                            let provider = provider.clone();
                            let delivery_action = action.clone();
                            let action_id = action.action_id;
                            let delivery = tokio::spawn(async move {
                                provider
                                    .deliver(
                                        &delivery_action,
                                        claim_epoch,
                                        OffsetDateTime::UNIX_EPOCH,
                                    )
                                    .await
                            });
                            let task = broker
                                .poll(
                                    worker_namespace_id,
                                    TaskQueueName("provider-tasks".to_owned()),
                                    std::time::Duration::from_secs(1),
                                )
                                .await
                                .expect("worker task");
                            assert_eq!(
                                broker.consume(&task.token.task_id).await,
                                Some(crate::NexusTaskCorrelation::WorkerCompute {
                                    action_id,
                                    claim_epoch,
                                    origin: task.origin.clone(),
                                }),
                            );
                            assert!(broker.complete_worker_compute(
                                action_id,
                                claim_epoch,
                                WorkerComputeProviderCompletion::Synchronous(response_payload(
                                    &action_id.to_string(),
                                )),
                            ));
                            assert_eq!(
                                delivery.await.expect("worker delivery").outcome,
                                WorkerComputeProviderOutcome::Delivered,
                            );
                        }
                        EndpointMutation::Missing => {
                            assert_eq!(
                                provider
                                    .deliver(
                                        &action,
                                        claim_epoch,
                                        OffsetDateTime::UNIX_EPOCH,
                                    )
                                    .await
                                    .outcome,
                                WorkerComputeProviderOutcome::RetryableFailure(
                                    WorkerComputeFailureCategory::EndpointNotFound,
                                ),
                            );
                        }
                    }
                    assert_eq!(action.request_data, original_bytes);
                }
                assert_eq!(
                    *client.addresses.lock().expect("address recorder lock poisoned"),
                    expected_addresses,
                );
            });
        }
    }
}
