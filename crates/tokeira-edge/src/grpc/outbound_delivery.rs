//! Bounded, purpose-authenticated transport of committed outbound references.
//! Wire validation lives here; runtime alone hydrates and validates workflow
//! relationships. Responses contain no payloads, credentials or redirect URLs.

use std::{
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_kernel::{OperationIncarnation, OperationKey, OperationKind};
use tokeira_proto::internal::runtime::{self as wire, outbound_delivery_request::Reference};
use tokeira_runtime::{
    DeliveryPhase, OutboundDeliveryOutcome, OutboundDeliveryReference, OutboundDeliveryTransport,
    OutboundTerminalCause, TokeiraRuntime,
};
use tokeira_storage::{
    DeliveryGuard, ExecutionHomeFence, IntentKey, IntentKind, RunRepository, WriteFence,
};
use tokeira_types::{RunId, RunKey, ShardEpoch, ShardId, TransitionSeq};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::{
    peer_transport::PeerChannels,
    workflow_offer::{OUTBOUND_TIMEOUT, OfferGate, OfferResponseLimit, PeerOfferEndpoint},
};
use crate::RoutingCache;

const MAX_REFERENCE_BYTES: usize = 4096;

#[cfg(test)]
#[path = "outbound_delivery_tests.rs"]
mod tests;

/// Installed only on network stacks; the outer gate removes its route when peer
/// bootstrap material is absent, before authentication or protobuf decoding.
pub struct OutboundDeliveryGrpc<R> {
    runtime: Arc<TokeiraRuntime<R>>,
    peer: Option<Arc<PeerOfferEndpoint>>,
}

impl<R> fmt::Debug for OutboundDeliveryGrpc<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundDeliveryGrpc")
            .finish_non_exhaustive()
    }
}

impl<R: RunRepository + 'static> OutboundDeliveryGrpc<R> {
    /// Reuse already-validated bootstrap material; this constructor reads no file.
    pub fn new(runtime: Arc<TokeiraRuntime<R>>, peer: Option<Arc<PeerOfferEndpoint>>) -> Self {
        Self { runtime, peer }
    }

    /// Enforce purpose authentication, concurrency, deadline and pre-decode size.
    pub fn into_service(
        self,
    ) -> OfferGate<wire::outbound_delivery_server::OutboundDeliveryServer<Self>> {
        let peer = self.peer.clone();
        OfferGate::new(
            wire::outbound_delivery_server::OutboundDeliveryServer::new(self)
                .max_decoding_message_size(MAX_REFERENCE_BYTES)
                .max_encoding_message_size(MAX_REFERENCE_BYTES),
            peer,
        )
    }
}

#[tonic::async_trait]
impl<R: RunRepository + 'static> wire::outbound_delivery_server::OutboundDelivery
    for OutboundDeliveryGrpc<R>
{
    async fn deliver(
        &self,
        request: Request<wire::OutboundDeliveryRequest>,
    ) -> Result<Response<wire::OutboundDeliveryResponse>, Status> {
        // The service is exposed only through into_service's pre-decode gate.
        if self.peer.is_none() {
            return Err(Status::unimplemented("unknown method"));
        }
        let reference = decode_reference(request.into_inner())?;
        Ok(Response::new(encode_outcome(
            self.runtime.deliver_outbound_reference(reference).await,
        )))
    }
}

/// One bounded attempt against the current placement snapshot. Owner changes
/// yield to durable discovery, which resolves placement afresh on its next try.
#[derive(Debug)]
pub struct RemoteOutboundDelivery {
    routing: Arc<RoutingCache>,
    peer: Arc<PeerOfferEndpoint>,
    channels: PeerChannels,
}

impl RemoteOutboundDelivery {
    /// Use the existing routing subscription and validated peer signer.
    pub fn new(routing: Arc<RoutingCache>, peer: Arc<PeerOfferEndpoint>) -> Self {
        Self {
            routing,
            peer,
            channels: PeerChannels::default(),
        }
    }
}

#[async_trait]
impl OutboundDeliveryTransport for RemoteOutboundDelivery {
    async fn deliver(
        &self,
        reference: OutboundDeliveryReference,
    ) -> Result<OutboundDeliveryOutcome> {
        let snapshot = self.routing.snapshot();
        let owner = snapshot
            .lookup_bundle_owner(reference.destination_home)
            .ok_or_else(|| anyhow!("outbound destination has no placement"))?;
        let endpoint = snapshot
            .lookup_node_endpoint(owner.node_id)
            .ok_or_else(|| anyhow!("outbound destination has no endpoint"))?;
        let channel = self.channels.get(&endpoint.as_authority())?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let token = self.peer.signer().outbound_token_at(now)?;
        let wire = encode_reference(&reference)?;
        let mut request = Request::new(wire);
        request.set_timeout(OUTBOUND_TIMEOUT);
        request
            .metadata_mut()
            .insert("authorization", token.authorization().parse()?);
        let mut client = wire::outbound_delivery_client::OutboundDeliveryClient::new(
            OfferResponseLimit::new(channel),
        )
        .max_decoding_message_size(MAX_REFERENCE_BYTES)
        .max_encoding_message_size(MAX_REFERENCE_BYTES);
        let response = tokio::time::timeout(OUTBOUND_TIMEOUT, client.deliver(request))
            .await??
            .into_inner();
        decode_outcome(response, &reference)
    }
}

fn invalid() -> Status {
    Status::invalid_argument("malformed outbound reference")
}

fn uuid(bytes: &[u8]) -> Result<Uuid, Status> {
    let value = Uuid::from_slice(bytes).map_err(|_| invalid())?;
    if value.is_nil() {
        return Err(invalid());
    }
    Ok(value)
}

fn decode_reference(
    value: wire::OutboundDeliveryRequest,
) -> Result<OutboundDeliveryReference, Status> {
    let source = RunKey(uuid(&value.source_run_key)?);
    let destination = RunKey(uuid(&value.destination_run_key)?);
    if value.source_owner.is_empty()
        || value.source_owner.len() > 256
        || !value.source_owner.is_ascii()
        || value.source_epoch == 0
    {
        return Err(invalid());
    }
    let phase = match wire::OutboundDeliveryPhase::try_from(value.phase).map_err(|_| invalid())? {
        wire::OutboundDeliveryPhase::ChildStart => DeliveryPhase::ChildStart,
        wire::OutboundDeliveryPhase::ChildReplace => DeliveryPhase::ChildReplace,
        wire::OutboundDeliveryPhase::ChildRelease => DeliveryPhase::ChildRelease,
        wire::OutboundDeliveryPhase::External => DeliveryPhase::External,
        wire::OutboundDeliveryPhase::ParentClose => DeliveryPhase::ParentClose,
        wire::OutboundDeliveryPhase::ChildResult => DeliveryPhase::ChildResult,
        _ => return Err(invalid()),
    };
    let current = value
        .expected_current_run_key
        .as_deref()
        .map(uuid)
        .transpose()?
        .map(RunKey);
    let authority = match value.reference.ok_or_else(invalid)? {
        Reference::Operation(operation) => {
            if operation.initiating_event_id <= 0
                || operation.requested_event_id < 0
                || operation.source_transition_seq == 0
            {
                return Err(invalid());
            }
            let kind = match (operation.kind, phase) {
                (1, DeliveryPhase::ChildStart | DeliveryPhase::ChildReplace) => {
                    OperationKind::ChildStart
                }
                (2, DeliveryPhase::ChildRelease) => OperationKind::ChildRelease,
                (3, DeliveryPhase::External) => OperationKind::ExternalSignal,
                (4, DeliveryPhase::External) => OperationKind::ExternalCancel,
                _ => return Err(invalid()),
            };
            let incarnation = OperationIncarnation {
                key: OperationKey {
                    source_run: source,
                    kind,
                    initiating_event: operation.initiating_event_id,
                },
                requested_event: operation.requested_event_id,
                attempt: operation.attempt,
            };
            let source_seq = TransitionSeq(operation.source_transition_seq);
            if phase == DeliveryPhase::ChildStart {
                DeliveryGuard::ChildStart {
                    incarnation,
                    source_seq,
                    expected_current: current,
                }
            } else {
                if current.is_some() {
                    return Err(invalid());
                }
                DeliveryGuard::Operation {
                    incarnation,
                    source_seq,
                }
            }
        }
        Reference::Intent(intent) => {
            if intent.closing_transition_seq == 0 {
                return Err(invalid());
            }
            let kind = match (intent.kind, phase) {
                (1, DeliveryPhase::ParentClose) => IntentKind::ParentClose,
                (2, DeliveryPhase::ChildResult) => IntentKind::ChildResult,
                _ => return Err(invalid()),
            };
            let key = IntentKey {
                source_run: source,
                close_seq: TransitionSeq(intent.closing_transition_seq),
                kind,
                ordinal: intent.ordinal,
            };
            if phase == DeliveryPhase::ParentClose {
                if current != Some(destination) {
                    return Err(invalid());
                }
                DeliveryGuard::CurrentIntent {
                    key,
                    expected_current: current,
                }
            } else {
                if current.is_some() {
                    return Err(invalid());
                }
                DeliveryGuard::Intent(key)
            }
        }
    };
    Ok(OutboundDeliveryReference {
        authority,
        phase,
        destination,
        destination_home: ShardId(value.destination_home),
        source_fence: WriteFence::Lease(ExecutionHomeFence {
            home: ShardId(value.source_home),
            owner: value.source_owner,
            epoch: ShardEpoch(value.source_epoch),
        }),
    })
}

fn encode_reference(
    reference: &OutboundDeliveryReference,
) -> Result<wire::OutboundDeliveryRequest> {
    let WriteFence::Lease(fence) = &reference.source_fence else {
        anyhow::bail!("unfenced execution cannot use peer delivery");
    };
    let (source, wire_reference, current) = match reference.authority {
        DeliveryGuard::Operation {
            incarnation,
            source_seq,
        }
        | DeliveryGuard::ChildStart {
            incarnation,
            source_seq,
            ..
        } => {
            let current = match reference.authority {
                DeliveryGuard::ChildStart {
                    expected_current, ..
                } => expected_current,
                _ => None,
            };
            (
                incarnation.key.source_run,
                Reference::Operation(wire::OutboundOperationReference {
                    kind: incarnation.key.kind as u32,
                    initiating_event_id: incarnation.key.initiating_event,
                    requested_event_id: incarnation.requested_event,
                    attempt: incarnation.attempt,
                    source_transition_seq: source_seq.0,
                }),
                current,
            )
        }
        DeliveryGuard::Intent(key) | DeliveryGuard::CurrentIntent { key, .. } => {
            let current = match reference.authority {
                DeliveryGuard::CurrentIntent {
                    expected_current, ..
                } => expected_current,
                _ => None,
            };
            (
                key.source_run,
                Reference::Intent(wire::OutboundIntentReference {
                    kind: key.kind as u32,
                    closing_transition_seq: key.close_seq.0,
                    ordinal: key.ordinal,
                }),
                current,
            )
        }
    };
    let phase = match reference.phase {
        DeliveryPhase::ChildStart => wire::OutboundDeliveryPhase::ChildStart,
        DeliveryPhase::ChildReplace => wire::OutboundDeliveryPhase::ChildReplace,
        DeliveryPhase::ChildRelease => wire::OutboundDeliveryPhase::ChildRelease,
        DeliveryPhase::External => wire::OutboundDeliveryPhase::External,
        DeliveryPhase::ParentClose => wire::OutboundDeliveryPhase::ParentClose,
        DeliveryPhase::ChildResult => wire::OutboundDeliveryPhase::ChildResult,
    };
    Ok(wire::OutboundDeliveryRequest {
        source_run_key: source.0.as_bytes().to_vec(),
        reference: Some(wire_reference),
        destination_run_key: reference.destination.0.as_bytes().to_vec(),
        destination_home: reference.destination_home.0,
        phase: phase as i32,
        expected_current_run_key: current.map(|key| key.0.as_bytes().to_vec()),
        source_home: fence.home.0,
        source_owner: fence.owner.clone(),
        source_epoch: fence.epoch.0,
    })
}

fn encode_outcome(outcome: OutboundDeliveryOutcome) -> wire::OutboundDeliveryResponse {
    let mut response = wire::OutboundDeliveryResponse::default();
    response.outcome = match outcome {
        OutboundDeliveryOutcome::Applied { run_id, seq } => {
            response.target_run_id = Some(run_id.0.as_bytes().to_vec());
            response.target_transition_seq = Some(seq.0);
            wire::OutboundDeliveryOutcome::Applied
        }
        OutboundDeliveryOutcome::Terminal(cause) => {
            response.terminal_cause = Some(match cause {
                OutboundTerminalCause::MissingExecution => {
                    wire::OutboundTerminalCause::MissingExecution
                }
                OutboundTerminalCause::ClosedExecution => {
                    wire::OutboundTerminalCause::ClosedExecution
                }
                OutboundTerminalCause::SignalLimit => wire::OutboundTerminalCause::SignalLimit,
            } as i32);
            wire::OutboundDeliveryOutcome::Terminal
        }
        OutboundDeliveryOutcome::Obsolete => wire::OutboundDeliveryOutcome::Obsolete,
        OutboundDeliveryOutcome::NotOwner => wire::OutboundDeliveryOutcome::NotOwner,
        OutboundDeliveryOutcome::Retryable => wire::OutboundDeliveryOutcome::Retryable,
    } as i32;
    response
}

fn decode_outcome(
    value: wire::OutboundDeliveryResponse,
    reference: &OutboundDeliveryReference,
) -> Result<OutboundDeliveryOutcome> {
    anyhow::ensure!(
        value.reset_redirect_run_id.is_none(),
        "unexpected outbound redirect"
    );
    let outcome = wire::OutboundDeliveryOutcome::try_from(value.outcome)?;
    if outcome == wire::OutboundDeliveryOutcome::Applied {
        anyhow::ensure!(value.terminal_cause.is_none(), "ambiguous outbound outcome");
        let run_id = RunId(uuid(
            value
                .target_run_id
                .as_deref()
                .ok_or_else(|| anyhow!("missing target identity"))?,
        )?);
        let seq = value
            .target_transition_seq
            .filter(|seq| *seq > 0)
            .ok_or_else(|| anyhow!("missing target sequence"))?;
        return Ok(OutboundDeliveryOutcome::Applied {
            run_id,
            seq: TransitionSeq(seq),
        });
    }
    anyhow::ensure!(
        value.target_run_id.is_none() && value.target_transition_seq.is_none(),
        "unexpected target identity"
    );
    if outcome == wire::OutboundDeliveryOutcome::Terminal {
        anyhow::ensure!(
            reference.phase == DeliveryPhase::External,
            "unexpected terminal refusal"
        );
        let cause = wire::OutboundTerminalCause::try_from(
            value
                .terminal_cause
                .ok_or_else(|| anyhow!("missing terminal cause"))?,
        )?;
        let signal = reference
            .authority
            .operation()
            .is_some_and(|op| op.key.kind == OperationKind::ExternalSignal);
        return Ok(OutboundDeliveryOutcome::Terminal(match cause {
            wire::OutboundTerminalCause::MissingExecution => {
                OutboundTerminalCause::MissingExecution
            }
            wire::OutboundTerminalCause::ClosedExecution if signal => {
                OutboundTerminalCause::ClosedExecution
            }
            wire::OutboundTerminalCause::SignalLimit if signal => {
                OutboundTerminalCause::SignalLimit
            }
            _ => anyhow::bail!("invalid terminal refusal for outbound kind"),
        }));
    }
    anyhow::ensure!(value.terminal_cause.is_none(), "unexpected terminal cause");
    Ok(match outcome {
        wire::OutboundDeliveryOutcome::Obsolete => OutboundDeliveryOutcome::Obsolete,
        wire::OutboundDeliveryOutcome::NotOwner => OutboundDeliveryOutcome::NotOwner,
        wire::OutboundDeliveryOutcome::Retryable => OutboundDeliveryOutcome::Retryable,
        _ => anyhow::bail!("unsupported outbound response"),
    })
}
