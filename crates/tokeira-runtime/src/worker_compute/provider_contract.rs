//! Canonical provider action encoding.
//!
//! This module constructs the immutable protobuf bytes committed with a controller
//! decision. It contains no transport, endpoint lookup, credentials, or provider I/O.

use std::{collections::BTreeMap, time::Duration};

use prost::Message;
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_proto::{
    compute::v1::{
        InvokeReason, InvokeWorkerRequest, InvokeWorkerResponse,
        TaskQueueBinding as ProtoTaskQueueBinding, TaskQueueType,
    },
    conversions::common::payload_from_domain,
};
use tokeira_storage::WorkerComputeProviderAction;
use tokeira_types::{
    ConfigurationFingerprint, ControllerInstanceKey, Payload, Payloads, ScalingGroupId,
    WorkerComputeFailureCategory, WorkerComputeInvokeReason, WorkerComputeProviderActionStatus,
    WorkerComputeTaskQueueBinding, WorkerComputeTaskType,
};
use uuid::Uuid;

use super::RemoteNexusProvider;
use crate::nexus_http::NEXUS_PAYLOAD_SIZE_LIMIT;

/// Complete provider-neutral input for one immutable action.
#[derive(Clone, Debug)]
pub struct ProviderActionInput {
    /// Stable idempotency and Nexus request identifier.
    pub action_id: Uuid,
    /// Exact controller identity.
    pub controller_key: ControllerInstanceKey,
    /// Public namespace name.
    pub namespace_name: String,
    /// Effective scaling group.
    pub scaling_group: ScalingGroupId,
    /// Current configuration fence.
    pub fingerprint: ConfigurationFingerprint,
    /// Current remote provider shape.
    pub provider: RemoteNexusProvider,
    /// Why one unit of capacity was requested.
    pub reason: WorkerComputeInvokeReason,
    /// Unique queue bindings observed for the group.
    pub task_queues: Vec<WorkerComputeTaskQueueBinding>,
    /// Durable decision time.
    pub now: OffsetDateTime,
}

/// Canonical provider request construction failure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkerComputeProviderContractError {
    /// The encoded request exceeds the established Nexus payload ceiling.
    #[error("worker-compute provider request exceeds the Nexus payload-size limit")]
    RequestTooLarge,
}

/// Fixed Nexus invocation derived from immutable durable action bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerComputeNexusInvocation {
    /// Stable action idempotency key.
    pub request_id: String,
    /// Fixed provider service.
    pub service: &'static str,
    /// Fixed provider operation.
    pub operation: &'static str,
    /// Exactly one protobuf payload.
    pub input: Payloads,
    /// Fixed per-attempt deadline.
    pub attempt_timeout: Duration,
}

/// Provider-neutral completion shape consumed by exact contract validation.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkerComputeProviderCompletion {
    /// Nexus completed synchronously with its result payloads.
    Synchronous(Payloads),
    /// Nexus accepted an asynchronous operation, which this contract forbids.
    Asynchronous,
    /// The operation itself completed unsuccessfully.
    OperationUnsuccessful,
    /// A Nexus handler rejected the request.
    HandlerError {
        /// Transport-provided retry classification.
        retryable: bool,
    },
    /// Endpoint invocation failed before any handler completion.
    TransportFailure,
}

/// Bounded delivery result suitable for durable outbox finalization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerComputeProviderOutcome {
    /// Exact synchronous acknowledgement with the matching action ID.
    Delivered,
    /// Retry under the outbox's bounded backoff policy.
    RetryableFailure(WorkerComputeFailureCategory),
    /// Retain an audit record but do not retry this action.
    TerminalFailure(WorkerComputeFailureCategory),
}

/// Build the exact immutable action and canonical request bytes.
pub fn build_provider_action(
    input: ProviderActionInput,
) -> Result<WorkerComputeProviderAction, WorkerComputeProviderContractError> {
    let mut task_queues = input.task_queues;
    task_queues.sort();
    task_queues.dedup();
    let request = InvokeWorkerRequest {
        request_id: input.action_id.to_string(),
        namespace: input.namespace_name,
        deployment_name: input.controller_key.deployment_name.0.clone(),
        build_id: input.controller_key.build_id.0.clone(),
        scaling_group: input.scaling_group.0.clone(),
        count: 1,
        task_queues: task_queues
            .into_iter()
            .map(|binding| ProtoTaskQueueBinding {
                name: binding.name.0,
                r#type: match binding.task_type {
                    WorkerComputeTaskType::Workflow => TaskQueueType::Workflow as i32,
                    WorkerComputeTaskType::Activity => TaskQueueType::Activity as i32,
                    WorkerComputeTaskType::Nexus => TaskQueueType::Nexus as i32,
                },
            })
            .collect(),
        provider_type: input.provider.provider_type,
        provider_details: input.provider.details.as_ref().map(payload_from_domain),
        configuration_fingerprint: input.fingerprint.as_bytes().to_vec(),
        reason: match input.reason {
            WorkerComputeInvokeReason::ConfigurationActivation => {
                InvokeReason::ConfigurationActivation as i32
            }
            WorkerComputeInvokeReason::NoSyncMatch => InvokeReason::NoSyncMatch as i32,
            WorkerComputeInvokeReason::Backlog => InvokeReason::Backlog as i32,
            WorkerComputeInvokeReason::WorkerRefresh => InvokeReason::WorkerRefresh as i32,
        },
    };
    let request_data = request.encode_to_vec();
    if request_data.len() > NEXUS_PAYLOAD_SIZE_LIMIT {
        return Err(WorkerComputeProviderContractError::RequestTooLarge);
    }

    Ok(WorkerComputeProviderAction {
        action_id: input.action_id,
        due_bucket: WorkerComputeProviderAction::due_bucket(input.action_id),
        controller_key: input.controller_key,
        scaling_group: input.scaling_group,
        configuration_fingerprint: input.fingerprint,
        endpoint_name: input.provider.nexus_endpoint,
        reason: input.reason,
        request_data,
        status: WorkerComputeProviderActionStatus::Pending,
        attempts: 0,
        attempt_started_at: None,
        claim_epoch: 0,
        next_attempt_at: input.now,
        claim: None,
        superseded_at: None,
        last_error_category: None,
        created_at: input.now,
        updated_at: input.now,
    })
}

/// Build the fixed Nexus envelope from bytes frozen in one durable action.
#[must_use]
pub fn provider_nexus_invocation(
    action: &WorkerComputeProviderAction,
) -> WorkerComputeNexusInvocation {
    WorkerComputeNexusInvocation {
        request_id: action.action_id.to_string(),
        service: tokeira_proto::compute::NEXUS_SERVICE_NAME,
        operation: tokeira_proto::compute::INVOKE_WORKER_OPERATION,
        input: Payloads(vec![Payload {
            data: action.request_data.clone(),
            metadata: BTreeMap::from([
                ("encoding".to_owned(), "binary/protobuf".to_owned()),
                (
                    "messageType".to_owned(),
                    tokeira_proto::compute::INVOKE_WORKER_REQUEST_MESSAGE_TYPE.to_owned(),
                ),
            ]),
            external_payloads: Vec::new(),
        }]),
        attempt_timeout: super::PROVIDER_ATTEMPT_TIMEOUT,
    }
}

/// Validate one provider completion and reduce it to a bounded durable outcome.
#[must_use]
pub fn validate_provider_completion(
    action_id: Uuid,
    completion: &WorkerComputeProviderCompletion,
) -> WorkerComputeProviderOutcome {
    match completion {
        WorkerComputeProviderCompletion::Synchronous(payloads) => {
            validate_sync_payload(action_id, payloads)
        }
        WorkerComputeProviderCompletion::Asynchronous => {
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::AsyncResponse,
            )
        }
        WorkerComputeProviderCompletion::OperationUnsuccessful => {
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::OperationUnsuccessful,
            )
        }
        WorkerComputeProviderCompletion::HandlerError { retryable: true } => {
            WorkerComputeProviderOutcome::RetryableFailure(
                WorkerComputeFailureCategory::RetryableHandler,
            )
        }
        WorkerComputeProviderCompletion::TransportFailure => {
            WorkerComputeProviderOutcome::RetryableFailure(WorkerComputeFailureCategory::Transport)
        }
        WorkerComputeProviderCompletion::HandlerError { retryable: false } => {
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::NonRetryableHandler,
            )
        }
    }
}

fn validate_sync_payload(action_id: Uuid, payloads: &Payloads) -> WorkerComputeProviderOutcome {
    let [payload] = payloads.0.as_slice() else {
        return WorkerComputeProviderOutcome::TerminalFailure(
            WorkerComputeFailureCategory::InvalidResponsePayload,
        );
    };
    if payload.external_payloads.is_empty()
        && payload.metadata
            == BTreeMap::from([
                ("encoding".to_owned(), "binary/protobuf".to_owned()),
                (
                    "messageType".to_owned(),
                    tokeira_proto::compute::INVOKE_WORKER_RESPONSE_MESSAGE_TYPE.to_owned(),
                ),
            ])
        && let Ok(response) = InvokeWorkerResponse::decode(payload.data.as_slice())
    {
        if response.request_id == action_id.to_string() {
            return WorkerComputeProviderOutcome::Delivered;
        }
        return WorkerComputeProviderOutcome::TerminalFailure(
            WorkerComputeFailureCategory::ResponseIdMismatch,
        );
    }
    WorkerComputeProviderOutcome::TerminalFailure(
        WorkerComputeFailureCategory::InvalidResponsePayload,
    )
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_proto::compute::v1::{InvokeWorkerRequest, InvokeWorkerResponse};
    use tokeira_types::{BuildId, DeploymentId, NamespaceId, TaskQueueName};

    use super::*;

    fn action_input(
        action_id: Uuid,
        queues: Vec<WorkerComputeTaskQueueBinding>,
    ) -> ProviderActionInput {
        ProviderActionInput {
            action_id,
            controller_key: ControllerInstanceKey {
                namespace_id: NamespaceId::new(),
                deployment_name: DeploymentId("payments".to_owned()),
                build_id: BuildId("build-a".to_owned()),
            },
            namespace_name: "payments-prod".to_owned(),
            scaling_group: ScalingGroupId("primary".to_owned()),
            fingerprint: ConfigurationFingerprint::from_bytes([7; 32]),
            provider: RemoteNexusProvider {
                provider_type: "test-remote".to_owned(),
                details: Some(Payload::new("provider-config")),
                nexus_endpoint: "worker-compute".to_owned(),
            },
            reason: WorkerComputeInvokeReason::NoSyncMatch,
            task_queues: queues,
            now: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn response_payload(request_id: impl Into<String>) -> Payloads {
        Payloads(vec![Payload {
            data: InvokeWorkerResponse {
                request_id: request_id.into(),
            }
            .encode_to_vec(),
            metadata: BTreeMap::from([
                ("encoding".to_owned(), "binary/protobuf".to_owned()),
                (
                    "messageType".to_owned(),
                    tokeira_proto::compute::INVOKE_WORKER_RESPONSE_MESSAGE_TYPE.to_owned(),
                ),
            ]),
            external_payloads: Vec::new(),
        }])
    }

    #[test]
    fn nexus_invocation_uses_fixed_identity_timeout_and_one_protobuf_payload() {
        let action_id = Uuid::new_v4();
        let action =
            build_provider_action(action_input(action_id, Vec::new())).expect("provider action");
        let invocation = provider_nexus_invocation(&action);

        assert_eq!(invocation.request_id, action_id.to_string());
        assert_eq!(
            invocation.service,
            tokeira_proto::compute::NEXUS_SERVICE_NAME
        );
        assert_eq!(
            invocation.operation,
            tokeira_proto::compute::INVOKE_WORKER_OPERATION
        );
        assert_eq!(
            invocation.attempt_timeout,
            super::super::PROVIDER_ATTEMPT_TIMEOUT
        );
        let [payload] = invocation.input.0.as_slice() else {
            panic!("provider input must contain exactly one payload");
        };
        assert_eq!(payload.data, action.request_data);
        assert_eq!(
            payload.metadata.get("encoding").map(String::as_str),
            Some("binary/protobuf")
        );
        assert_eq!(
            payload.metadata.get("messageType").map(String::as_str),
            Some(tokeira_proto::compute::INVOKE_WORKER_REQUEST_MESSAGE_TYPE)
        );
    }

    #[test]
    fn malformed_and_non_synchronous_completions_are_bounded() {
        let action_id = Uuid::new_v4();
        assert_eq!(
            validate_provider_completion(
                action_id,
                &WorkerComputeProviderCompletion::Synchronous(response_payload(
                    action_id.to_string()
                ))
            ),
            WorkerComputeProviderOutcome::Delivered
        );
        assert_eq!(
            validate_provider_completion(action_id, &WorkerComputeProviderCompletion::Asynchronous),
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::AsyncResponse
            )
        );
        assert_eq!(
            validate_provider_completion(
                action_id,
                &WorkerComputeProviderCompletion::Synchronous(Payloads::default())
            ),
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::InvalidResponsePayload
            )
        );
        assert_eq!(
            validate_provider_completion(
                action_id,
                &WorkerComputeProviderCompletion::Synchronous(response_payload(
                    Uuid::new_v4().to_string()
                ))
            ),
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::ResponseIdMismatch
            )
        );
    }

    #[test]
    fn yadori_contract_fixture_is_the_canonical_nexus_envelope() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../proto/tokeira/compute/v1/fixtures/invoke-worker.json"
        )))
        .expect("worker-compute contract fixture is valid JSON");
        let action_id =
            Uuid::parse_str("018f4f62-87c5-7b5c-a56e-1b3d9a7c4210").expect("fixed UUID");
        let action = build_provider_action(ProviderActionInput {
            action_id,
            controller_key: ControllerInstanceKey {
                namespace_id: NamespaceId::new(),
                deployment_name: DeploymentId("payments".to_owned()),
                build_id: BuildId("build-a".to_owned()),
            },
            namespace_name: "payments-prod".to_owned(),
            scaling_group: ScalingGroupId("primary".to_owned()),
            fingerprint: ConfigurationFingerprint::from_bytes([7; 32]),
            provider: RemoteNexusProvider {
                provider_type: "yadori".to_owned(),
                details: Some(Payload {
                    data: br#"{"pool":"default"}"#.to_vec(),
                    metadata: BTreeMap::from([("encoding".to_owned(), "json/plain".to_owned())]),
                    external_payloads: Vec::new(),
                }),
                nexus_endpoint: "yadori".to_owned(),
            },
            reason: WorkerComputeInvokeReason::NoSyncMatch,
            task_queues: vec![
                WorkerComputeTaskQueueBinding {
                    name: TaskQueueName("payments-workflow".to_owned()),
                    task_type: WorkerComputeTaskType::Workflow,
                },
                WorkerComputeTaskQueueBinding {
                    name: TaskQueueName("payments-activity".to_owned()),
                    task_type: WorkerComputeTaskType::Activity,
                },
                WorkerComputeTaskQueueBinding {
                    name: TaskQueueName("payments-nexus".to_owned()),
                    task_type: WorkerComputeTaskType::Nexus,
                },
            ],
            now: OffsetDateTime::UNIX_EPOCH,
        })
        .expect("fixture action is bounded");
        let invocation = provider_nexus_invocation(&action);

        assert_eq!(
            fixture["nexus"]["service"].as_str(),
            Some(invocation.service)
        );
        assert_eq!(
            fixture["nexus"]["operation"].as_str(),
            Some(invocation.operation)
        );
        assert_eq!(
            fixture["nexus"]["request_id"].as_str(),
            Some(invocation.request_id.as_str())
        );
        assert_eq!(
            fixture["request_payload"]["encoding"].as_str(),
            invocation.input.0[0]
                .metadata
                .get("encoding")
                .map(String::as_str)
        );
        assert_eq!(
            fixture["request_payload"]["message_type"].as_str(),
            invocation.input.0[0]
                .metadata
                .get("messageType")
                .map(String::as_str)
        );
        assert_eq!(
            STANDARD
                .decode(
                    fixture["request_payload"]["data_base64"]
                        .as_str()
                        .expect("fixture request bytes")
                )
                .expect("fixture request is base64"),
            action.request_data
        );

        let response_bytes = STANDARD
            .decode(
                fixture["response_payload"]["data_base64"]
                    .as_str()
                    .expect("fixture response bytes"),
            )
            .expect("fixture response is base64");
        let response =
            InvokeWorkerResponse::decode(response_bytes.as_slice()).expect("response protobuf");
        assert_eq!(response.request_id, action_id.to_string());
        assert_eq!(
            fixture["response_payload"]["encoding"].as_str(),
            Some("binary/protobuf")
        );
        assert_eq!(
            fixture["response_payload"]["message_type"].as_str(),
            Some(tokeira_proto::compute::INVOKE_WORKER_RESPONSE_MESSAGE_TYPE)
        );

        // The provider receives launch scope, not a worker polling grant. Issue #29 and
        // `scoped-worker-authorization` own the separate credential handoff.
        let fixture_text = fixture.to_string().to_ascii_lowercase();
        for forbidden in ["authorization", "bearer", "credential", "task_token"] {
            assert!(
                !fixture_text.contains(forbidden),
                "provider contract fixture must not contain {forbidden}"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 12: provider request encoding is canonical and secret-free
        #[test]
        fn property_provider_request_encoding_is_canonical_and_secret_free(
            uuid_bytes in any::<[u8; 16]>(),
            queue_values in proptest::collection::vec((0u8..3, "[a-z]{1,12}"), 0..30),
        ) {
            let action_id = Uuid::from_bytes(uuid_bytes);
            let task_type = |value| match value {
                0 => WorkerComputeTaskType::Workflow,
                1 => WorkerComputeTaskType::Activity,
                _ => WorkerComputeTaskType::Nexus,
            };
            let queues = queue_values
                .iter()
                .map(|(kind, name)| WorkerComputeTaskQueueBinding {
                    name: TaskQueueName(name.clone()),
                    task_type: task_type(*kind),
                })
                .collect::<Vec<_>>();
            let first = build_provider_action(action_input(action_id, queues.clone()))
                .expect("bounded generated action");
            let second = build_provider_action(action_input(action_id, queues))
                .expect("same action input");
            prop_assert_eq!(&first.request_data, &second.request_data);

            let decoded = InvokeWorkerRequest::decode(first.request_data.as_slice())
                .expect("canonical request decodes");
            prop_assert_eq!(&decoded.request_id, &action_id.to_string());
            prop_assert_eq!(decoded.count, 1);
            prop_assert_eq!(&decoded.namespace, "payments-prod");
            prop_assert_eq!(&decoded.deployment_name, "payments");
            prop_assert_eq!(&decoded.build_id, "build-a");
            prop_assert_eq!(&decoded.scaling_group, "primary");
            prop_assert_eq!(&decoded.provider_type, "test-remote");
            prop_assert_eq!(decoded.configuration_fingerprint, vec![7; 32]);
            let ordered = decoded.task_queues.windows(2).all(|pair| {
                (pair[0].r#type, pair[0].name.as_str())
                    < (pair[1].r#type, pair[1].name.as_str())
            });
            prop_assert!(ordered);
            let unique = decoded
                .task_queues
                .iter()
                .map(|queue| (queue.r#type, queue.name.as_str()))
                .collect::<std::collections::BTreeSet<_>>();
            prop_assert_eq!(unique.len(), decoded.task_queues.len());
            let encoded_text = String::from_utf8_lossy(&first.request_data);
            prop_assert!(!encoded_text.contains("workflow-id-must-not-leak"));
            prop_assert!(!encoded_text.contains("bearer-token-must-not-leak"));
            prop_assert!(!encoded_text.contains("authorization-grant-must-not-leak"));
        }

        // Feature: worker-compute-controller, Property 13: provider completion validation is exact
        #[test]
        fn property_provider_completion_validation_is_exact(
            uuid_bytes in any::<[u8; 16]>(),
            other_bytes in any::<[u8; 16]>(),
            shape in 0u8..10,
        ) {
            let action_id = Uuid::from_bytes(uuid_bytes);
            let other_id = Uuid::from_bytes(other_bytes);
            let (completion, expected) = match shape {
                0 => (
                    WorkerComputeProviderCompletion::Synchronous(response_payload(action_id.to_string())),
                    WorkerComputeProviderOutcome::Delivered,
                ),
                1 => (
                    WorkerComputeProviderCompletion::Synchronous(Payloads::default()),
                    WorkerComputeProviderOutcome::TerminalFailure(
                        WorkerComputeFailureCategory::InvalidResponsePayload,
                    ),
                ),
                2 => {
                    let payload = response_payload(action_id.to_string()).0.remove(0);
                    (
                        WorkerComputeProviderCompletion::Synchronous(Payloads(vec![
                            payload.clone(),
                            payload,
                        ])),
                        WorkerComputeProviderOutcome::TerminalFailure(
                            WorkerComputeFailureCategory::InvalidResponsePayload,
                        ),
                    )
                }
                3 => (
                    WorkerComputeProviderCompletion::Synchronous(Payloads(vec![Payload {
                        data: vec![0xff],
                        metadata: response_payload(action_id.to_string()).0[0].metadata.clone(),
                        external_payloads: Vec::new(),
                    }])),
                    WorkerComputeProviderOutcome::TerminalFailure(
                        WorkerComputeFailureCategory::InvalidResponsePayload,
                    ),
                ),
                4 => (
                    WorkerComputeProviderCompletion::Synchronous(response_payload(
                        other_id.to_string()
                    )),
                    if other_id == action_id {
                        WorkerComputeProviderOutcome::Delivered
                    } else {
                        WorkerComputeProviderOutcome::TerminalFailure(
                            WorkerComputeFailureCategory::ResponseIdMismatch,
                        )
                    },
                ),
                5 => (
                    WorkerComputeProviderCompletion::Asynchronous,
                    WorkerComputeProviderOutcome::TerminalFailure(
                        WorkerComputeFailureCategory::AsyncResponse,
                    ),
                ),
                6 => (
                    WorkerComputeProviderCompletion::OperationUnsuccessful,
                    WorkerComputeProviderOutcome::TerminalFailure(
                        WorkerComputeFailureCategory::OperationUnsuccessful,
                    ),
                ),
                7 => (
                    WorkerComputeProviderCompletion::HandlerError { retryable: true },
                    WorkerComputeProviderOutcome::RetryableFailure(
                        WorkerComputeFailureCategory::RetryableHandler,
                    ),
                ),
                8 => (
                    WorkerComputeProviderCompletion::HandlerError { retryable: false },
                    WorkerComputeProviderOutcome::TerminalFailure(
                        WorkerComputeFailureCategory::NonRetryableHandler,
                    ),
                ),
                _ => (
                    WorkerComputeProviderCompletion::TransportFailure,
                    WorkerComputeProviderOutcome::RetryableFailure(
                        WorkerComputeFailureCategory::Transport,
                    ),
                ),
            };
            prop_assert_eq!(
                validate_provider_completion(action_id, &completion),
                expected
            );
        }
    }
}
