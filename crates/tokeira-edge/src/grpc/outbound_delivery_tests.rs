//! Bounded reference round-trips and rejection before recipe hydration.

use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use proptest::prelude::*;
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokeira_auth::peer::{PeerMaterial, PeerVerifier};
use tonic::{body::Body, codegen::Service};

#[derive(Debug)]
struct WireFixture(Arc<AtomicUsize>);

#[tonic::async_trait]
impl wire::outbound_delivery_server::OutboundDelivery for WireFixture {
    async fn deliver(
        &self,
        request: tonic::Request<wire::OutboundDeliveryRequest>,
    ) -> Result<tonic::Response<wire::OutboundDeliveryResponse>, Status> {
        decode_reference(request.into_inner())?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(encode_outcome(
            OutboundDeliveryOutcome::Retryable,
        )))
    }
}

fn endpoint() -> Arc<PeerOfferEndpoint> {
    let material = PeerMaterial::generate().unwrap();
    Arc::new(PeerOfferEndpoint::new(
        PeerVerifier::new(material.profile().clone()).unwrap(),
        material.signer(material.profile()).unwrap(),
    ))
}

fn framed(payload: Vec<u8>) -> Body {
    let mut bytes = vec![0];
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend(payload);
    Body::new(Full::new(Bytes::from(bytes)))
}

fn reference(seed: u128, phase: u8) -> OutboundDeliveryReference {
    let source = RunKey(Uuid::from_u128(seed.saturating_add(1)));
    let destination = RunKey(Uuid::from_u128(7));
    let incarnation = OperationIncarnation {
        key: OperationKey {
            source_run: source,
            kind: match phase {
                0 | 1 => OperationKind::ChildStart,
                2 => OperationKind::ChildRelease,
                3 => OperationKind::ExternalSignal,
                _ => OperationKind::ExternalCancel,
            },
            initiating_event: 4,
        },
        requested_event: 0,
        attempt: 0,
    };
    let source_seq = TransitionSeq(8);
    let authority = match phase {
        0 => DeliveryGuard::ChildStart {
            incarnation,
            source_seq,
            expected_current: Some(destination),
        },
        5 => DeliveryGuard::CurrentIntent {
            key: IntentKey {
                source_run: source,
                close_seq: source_seq,
                kind: IntentKind::ParentClose,
                ordinal: 2,
            },
            expected_current: Some(destination),
        },
        6 => DeliveryGuard::Intent(IntentKey {
            source_run: source,
            close_seq: source_seq,
            kind: IntentKind::ChildResult,
            ordinal: 0,
        }),
        _ => DeliveryGuard::Operation {
            incarnation,
            source_seq,
        },
    };
    OutboundDeliveryReference {
        authority,
        destination,
        destination_home: ShardId(2),
        source_fence: WriteFence::Lease(ExecutionHomeFence {
            home: ShardId(1),
            owner: "source-node".into(),
            epoch: ShardEpoch(3),
        }),
        phase: match phase {
            0 => DeliveryPhase::ChildStart,
            1 => DeliveryPhase::ChildReplace,
            2 => DeliveryPhase::ChildRelease,
            3 | 4 => DeliveryPhase::External,
            5 => DeliveryPhase::ParentClose,
            _ => DeliveryPhase::ChildResult,
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 18: Remote delivery grants no extra authority.
    #[test]
    fn grpc_frame_and_credentials_are_checked_before_reference_hydration(seed in 0u128..u128::MAX, auth in 0u8..5, oversized in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let peer = endpoint();
            let other = endpoint();
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let calls = Arc::new(AtomicUsize::new(0));
            let server = wire::outbound_delivery_server::OutboundDeliveryServer::new(WireFixture(calls.clone()))
                .max_decoding_message_size(MAX_REFERENCE_BYTES);
            let mut gate = OfferGate::new(server, (auth != 4).then_some(peer.clone()));
            let size = MAX_REFERENCE_BYTES + usize::from(oversized);
            let mut payload = encode_reference(&reference(seed, 3)).unwrap().encode_to_vec();
            let padding = size - payload.len() - 4;
            payload.extend_from_slice(&[0x9a, 0x06]);
            prost::encoding::encode_varint(padding as u64, &mut payload);
            payload.resize(size, 0);
            let mut request = http::Request::builder()
                .uri("/tokeira.internal.runtime.v1.OutboundDelivery/Deliver")
                .header("content-type", "application/grpc");
            let token = match auth {
                0 | 4 => Some(peer.signer().outbound_token_at(now).unwrap()),
                1 => Some(peer.signer().token_at(now).unwrap()),
                2 => Some(other.signer().outbound_token_at(now).unwrap()),
                _ => None,
            };
            if let Some(token) = token { request = request.header("authorization", token.authorization()); }
            let response = gate.call(request.body(framed(payload)).unwrap()).await.unwrap();
            let initial = response.headers().get("grpc-status").cloned();
            let body = response.into_body().collect().await.unwrap();
            let status = initial.or_else(|| body.trailers().and_then(|headers| headers.get("grpc-status")).cloned()).unwrap();
            prop_assert_eq!(calls.load(Ordering::SeqCst), usize::from(auth == 0 && !oversized));
            match auth {
                0 => prop_assert_eq!(status, if oversized { "8" } else { "0" }),
                1 => prop_assert_eq!(status, "7"),
                2 | 3 => prop_assert_eq!(status, "16"),
                _ => prop_assert_eq!(status, "12"),
            }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 18: Remote delivery grants no extra authority.
    #[test]
    fn references_round_trip_and_reject_unknown_authority(seed in any::<u128>(), phase in 0u8..7, mutation in 0u8..7) {
        let reference = reference(seed, phase);
        let mut encoded = encode_reference(&reference).unwrap();
        prop_assert_eq!(decode_reference(encoded.clone()).unwrap(), reference.clone());
        match mutation {
            0 => encoded.source_run_key.clear(),
            1 => encoded.destination_run_key = vec![0; 16],
            2 => encoded.reference = None,
            3 => encoded.phase = 99,
            4 => encoded.source_owner = "x".repeat(257),
            5 => encoded.source_epoch = 0,
            _ => match encoded.reference.as_mut().unwrap() {
                Reference::Operation(op) => op.kind = 99,
                Reference::Intent(intent) => intent.kind = 99,
            },
        }
        prop_assert_eq!(decode_reference(encoded).unwrap_err().code(), tonic::Code::InvalidArgument);
        let outcome = OutboundDeliveryOutcome::Applied { run_id: RunId(Uuid::from_u128(8)), seq: TransitionSeq(12) };
        prop_assert_eq!(decode_outcome(encode_outcome(outcome.clone()), &reference).unwrap(), outcome);
        let mut response = encode_outcome(OutboundDeliveryOutcome::Retryable);
        response.terminal_cause = Some(1);
        prop_assert!(decode_outcome(response, &reference).is_err());
        let mut response = encode_outcome(OutboundDeliveryOutcome::Retryable);
        response.outcome = 99;
        prop_assert!(decode_outcome(response, &reference).is_err());
    }
}
