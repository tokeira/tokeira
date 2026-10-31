//! Offer-only wire admission and transport limits.
//!
//! The gate runs before decoding and shares its semaphore across connections.
//! Public start forwarding retains its own authorization and response budget.
//! Valid offers remain unavailable until acquisition-scoped broker admission can
//! back an acknowledgement; this module must never call the legacy publisher.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use http::{HeaderMap, Request, Response};
use http_body_util::Limited;
use prost::Message as _;
use tokeira_auth::peer::{MAX_AUTHORIZATION_BYTES, PeerError, PeerSigner, PeerVerifier};
use tokeira_proto::internal::runtime::{
    OfferOutcome, OfferQueue, OfferQueueKind, OfferRoutingMode, OfferStickyUnavailableReason,
    OfferVersionBehavior, OfferWorkflowTaskRequest, OfferWorkflowTaskResponse,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Status, body::Body, server::NamedService};
use tower::Service;
use uuid::Uuid;

/// Fixed pre-decode request budget, equal to the unchanged start request budget.
pub const MAX_OFFER_REQUEST_BYTES: usize = 64 * 1024;
/// Offer replies contain only bounded typed outcomes, never start attachments.
pub const MAX_OFFER_RESPONSE_BYTES: usize = 4 * 1024;
/// Node-wide cap; overload never queues a waiter.
pub const MAX_OFFER_HANDLERS: usize = 64;
/// Includes credentials, decode and handling; no caller can enlarge this budget.
pub const OFFER_TIMEOUT: Duration = Duration::from_millis(500);
/// Exact internal route, absent from no-peer deployments.
pub const OFFER_PATH: &str = "/tokeira.internal.runtime.v1.WorkflowTaskDispatch/OfferWorkflowTask";
/// Reference-only delivery has a separate credential purpose and handler budget.
pub const OUTBOUND_PATH: &str = "/tokeira.internal.runtime.v1.OutboundDelivery/Deliver";
/// Fixed end-to-end budget, including bounded authoritative recipe reads.
pub const OUTBOUND_TIMEOUT: Duration = Duration::from_secs(5);

/// Validated bootstrap capability. Construction is restricted to peer startup.
pub struct PeerOfferEndpoint {
    verifier: PeerVerifier,
    // Retain validated signing material for the later bounded sender. No sender
    // or renewal task is activated by installing the wire contract.
    signer: PeerSigner,
    handlers: Arc<Semaphore>,
    outbound_handlers: Arc<Semaphore>,
}

impl fmt::Debug for PeerOfferEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerOfferEndpoint").finish_non_exhaustive()
    }
}

impl PeerOfferEndpoint {
    /// Both adapters must have been validated against the same protected-file profile.
    pub fn new(verifier: PeerVerifier, signer: PeerSigner) -> Self {
        Self {
            verifier,
            signer,
            handlers: Arc::new(Semaphore::new(MAX_OFFER_HANDLERS)),
            outbound_handlers: Arc::new(Semaphore::new(32)),
        }
    }

    /// Validated local signer; each internal purpose uses its distinct audience.
    pub fn signer(&self) -> &PeerSigner {
        &self.signer
    }

    fn enter(&self, headers: &HeaderMap, now: u64) -> Result<OwnedSemaphorePermit, Status> {
        self.enter_purpose(headers, now, false)
    }

    fn enter_purpose(
        &self,
        headers: &HeaderMap,
        now: u64,
        outbound: bool,
    ) -> Result<OwnedSemaphorePermit, Status> {
        let permit = self
            .handlers_for(outbound)
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("offer handler limit exceeded"))?;
        // Count every header value, not only the first authorization value. This
        // bounds attacker-supplied metadata before JWT parsing/allocation.
        let bytes = headers.iter().try_fold(0usize, |total, (name, value)| {
            total
                .checked_add(name.as_str().len())?
                .checked_add(value.len())
        });
        if bytes.is_none_or(|bytes| bytes > MAX_AUTHORIZATION_BYTES) {
            return Err(Status::resource_exhausted("offer metadata limit exceeded"));
        }
        let mut values = headers.get_all(http::header::AUTHORIZATION).iter();
        let value = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("peer authentication required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated("ambiguous peer authentication"));
        }
        if outbound {
            self.verifier.verify_outbound_at(value, now)
        } else {
            self.verifier.verify_at(value, now)
        }
        .map_err(peer_status)?;
        Ok(permit)
    }

    fn handlers_for(&self, outbound: bool) -> &Arc<Semaphore> {
        if outbound {
            &self.outbound_handlers
        } else {
            &self.handlers
        }
    }
}

fn peer_status(error: PeerError) -> Status {
    match error {
        PeerError::TooLarge => Status::resource_exhausted("offer authentication limit exceeded"),
        PeerError::PermissionDenied => {
            Status::permission_denied("identity has no grant for this peer purpose")
        }
        _ => Status::unauthenticated("invalid peer authentication"),
    }
}

/// A service wrapper that removes the new route in no-peer modes and bounds
/// active offer decode/verification work across all cloned connections.
#[derive(Clone, Debug)]
pub struct OfferGate<S> {
    inner: S,
    peer: Option<Arc<PeerOfferEndpoint>>,
}

impl<S> OfferGate<S> {
    pub(crate) fn new(inner: S, peer: Option<Arc<PeerOfferEndpoint>>) -> Self {
        Self { inner, peer }
    }
}

impl<S: NamedService> NamedService for OfferGate<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<Request<B>> for OfferGate<S>
where
    S: Service<Request<B>, Response = Response<Body>> + Send,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let outbound = request.uri().path() == OUTBOUND_PATH;
        if request.uri().path() != OFFER_PATH && !outbound {
            return Box::pin(self.inner.call(request));
        }
        let deadline = tokio::time::Instant::now()
            + if outbound {
                OUTBOUND_TIMEOUT
            } else {
                OFFER_TIMEOUT
            };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|v| v.as_secs());
        let entered = match (&self.peer, now) {
            (None, _) => Err(Status::unimplemented("unknown method")),
            (Some(peer), Ok(now)) => {
                if outbound {
                    peer.enter_purpose(request.headers(), now, true)
                } else {
                    peer.enter(request.headers(), now)
                }
            }
            _ => Err(Status::unavailable("peer clock unavailable")),
        };
        let permit = match entered {
            Ok(permit) => permit,
            Err(status) => return Box::pin(async move { Ok(status.into_http()) }),
        };
        let response = self.inner.call(request);
        Box::pin(async move {
            let _permit = permit;
            match tokio::time::timeout_at(deadline, response).await {
                Ok(Ok(result))
                    if result
                        .headers()
                        .get("grpc-status")
                        .is_some_and(|value| value == "11") =>
                {
                    // Tonic's pre-decode message cap reports OutOfRange. Offer
                    // overload uses ResourceExhausted; Start keeps its mapping.
                    Ok(Status::resource_exhausted("offer request limit exceeded").into_http())
                }
                Ok(result) => result,
                Err(_) => {
                    Ok(Status::deadline_exceeded("offer attempt deadline exceeded").into_http())
                }
            }
        })
    }
}

/// Build the dormant transport adapter with offer-specific receive/send limits.
/// Startup modes that prohibit peers must never call this constructor.
pub fn offer_client(
    channel: tonic::transport::Channel,
) -> tokeira_proto::internal::runtime::workflow_task_dispatch_client::WorkflowTaskDispatchClient<
    OfferResponseLimit<tonic::transport::Channel>,
> {
    tokeira_proto::internal::runtime::workflow_task_dispatch_client::WorkflowTaskDispatchClient::new(OfferResponseLimit(channel))
        .max_decoding_message_size(MAX_OFFER_RESPONSE_BYTES)
        .max_encoding_message_size(MAX_OFFER_REQUEST_BYTES)
}

/// Bound the unary offer body independently of the peer's response headers.
/// Existing Start clients and their larger response budget are unchanged.
#[derive(Clone, Debug)]
pub struct OfferResponseLimit<S>(S);

impl<S> OfferResponseLimit<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self(inner)
    }
}

impl<S> Service<Request<Body>> for OfferResponseLimit<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let offer = matches!(request.uri().path(), OFFER_PATH | OUTBOUND_PATH);
        let response = self.0.call(request);
        Box::pin(async move {
            let response = response.await?;
            if !offer {
                return Ok(response);
            }
            // Tonic's trailers-only path uses Streaming::new_empty without the
            // configured decode limit. A successful unary offer must carry a
            // message and trailers, so reject initial grpc-status=0 before that
            // path can allocate from an untrusted frame's declared length.
            if response
                .headers()
                .get("grpc-status")
                .is_some_and(|value| value == "0")
            {
                return Ok(Status::internal("malformed workflow offer response").into_http());
            }
            // Allow the five-byte gRPC header in addition to one uncompressed
            // message; compression is disabled. Extra frames share this budget.
            Ok(response.map(|body| Body::new(Limited::new(body, MAX_OFFER_RESPONSE_BYTES + 5))))
        })
    }
}

fn invalid() -> Status {
    Status::invalid_argument("malformed workflow offer")
}

fn timestamp(value: Option<&prost_types::Timestamp>) -> Result<(i64, i32), Status> {
    let value = value.ok_or_else(invalid)?;
    if !(-62_135_596_800..=253_402_300_799).contains(&value.seconds)
        || !(0..1_000_000_000).contains(&value.nanos)
    {
        return Err(invalid());
    }
    Ok((value.seconds, value.nanos))
}

fn queue(value: Option<&OfferQueue>) -> Result<&OfferQueue, Status> {
    let value = value.ok_or_else(invalid)?;
    if value.name.is_empty() || value.build_id.is_some() && value.deployment.is_none() {
        return Err(invalid());
    }
    Ok(value)
}

/// Implement the spec's field policy without reading or mutating a run.
/// Placement agreement and current incarnation are checked by later admission
/// and authoritative start respectively; neither can be asserted from wire shape.
pub fn validate_request(value: &OfferWorkflowTaskRequest) -> Result<(), Status> {
    if value.encoded_len() > MAX_OFFER_REQUEST_BYTES {
        return Err(Status::resource_exhausted("offer request limit exceeded"));
    }
    for id in [&value.run_key, &value.namespace_id] {
        if Uuid::from_slice(id).map_or(true, |id| id.is_nil()) {
            return Err(invalid());
        }
    }
    if value.logical_seq == 0 || value.logical_seq > i64::MAX as u64 {
        return Err(invalid());
    }
    let selected = queue(value.selected_queue.as_ref())?;
    let kind = OfferQueueKind::try_from(value.queue_kind).map_err(|_| invalid())?;
    if kind == OfferQueueKind::Unspecified {
        return Err(invalid());
    }
    let routing = value.routing.as_ref().ok_or_else(invalid)?;
    match OfferRoutingMode::try_from(routing.mode).map_err(|_| invalid())? {
        OfferRoutingMode::Live if routing.deployment.is_none() && routing.build_id.is_none() => {}
        OfferRoutingMode::Exact if routing.deployment.is_some() => {}
        _ => return Err(invalid()),
    }
    let directive = value.version_directive.as_ref().ok_or_else(invalid)?;
    let behavior = OfferVersionBehavior::try_from(directive.behavior).map_err(|_| invalid())?;
    if behavior == OfferVersionBehavior::Unspecified
        || directive.build_id.is_some() && directive.deployment.is_none()
        || behavior == OfferVersionBehavior::Pinned
            && (directive.deployment.is_none() || directive.build_id.is_none())
    {
        return Err(invalid());
    }
    let scheduled = timestamp(value.scheduled_at.as_ref())?;
    if let Some(priority) = &value.priority
        && (!priority.fairness_weight.is_finite() || priority.fairness_weight < 0.0)
    {
        return Err(invalid());
    }
    match &value.sticky_origin {
        None if kind == OfferQueueKind::Sticky => return Err(invalid()),
        None => {}
        Some(origin) => {
            let sticky = queue(origin.sticky_queue.as_ref())?;
            let normal = queue(origin.normal_queue.as_ref())?;
            if origin.worker_identity.is_empty()
                || timestamp(origin.deadline.as_ref())? < scheduled
                || (kind == OfferQueueKind::Sticky && selected != sticky)
                || (kind == OfferQueueKind::Normal && selected.name != normal.name)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

/// Check typed outcomes and the offer-specific response bound, independent of
/// the dispatch service's larger start-response limit.
pub fn validate_response(value: &OfferWorkflowTaskResponse) -> Result<(), Status> {
    if value.encoded_len() > MAX_OFFER_RESPONSE_BYTES {
        return Err(Status::resource_exhausted("offer response limit exceeded"));
    }
    let outcome = OfferOutcome::try_from(value.outcome).map_err(|_| invalid())?;
    if outcome == OfferOutcome::Unspecified {
        return Err(invalid());
    }
    match (outcome, value.sticky_unavailable_reason) {
        (OfferOutcome::StickyUnavailable, Some(reason)) => {
            if OfferStickyUnavailableReason::try_from(reason).map_err(|_| invalid())?
                == OfferStickyUnavailableReason::Unspecified
            {
                return Err(invalid());
            }
        }
        (OfferOutcome::StickyUnavailable, None) | (_, Some(_)) => return Err(invalid()),
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use proptest::prelude::*;
    use tokeira_auth::peer::PeerMaterial;
    use tokeira_proto::internal::runtime::{
        OfferRouting, OfferVersionDirective, StartWorkflowTaskRequest, StartWorkflowTaskResponse,
        workflow_task_dispatch_server::{WorkflowTaskDispatch, WorkflowTaskDispatchServer},
    };

    struct WireFixture;

    #[tonic::async_trait]
    impl WorkflowTaskDispatch for WireFixture {
        async fn offer_workflow_task(
            &self,
            request: tonic::Request<OfferWorkflowTaskRequest>,
        ) -> Result<tonic::Response<OfferWorkflowTaskResponse>, Status> {
            validate_request(request.get_ref())?;
            Err(Status::unavailable("admission unavailable"))
        }

        async fn start_workflow_task(
            &self,
            _: tonic::Request<StartWorkflowTaskRequest>,
        ) -> Result<tonic::Response<StartWorkflowTaskResponse>, Status> {
            Ok(tonic::Response::new(StartWorkflowTaskResponse {
                started_task_json: vec![0; MAX_OFFER_RESPONSE_BYTES + 1],
                decorations_proto: vec![],
            }))
        }
    }

    fn framed(payload: Vec<u8>) -> Body {
        let mut frame = vec![0];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        Body::new(Full::new(Bytes::from(frame)))
    }

    fn padded_request(size: usize) -> Vec<u8> {
        let mut bytes = request().encode_to_vec();
        // Field 99 is an unknown length-delimited field. Its tag and three-byte
        // length prefix consume five bytes at these request sizes.
        let padding = size - bytes.len() - 5;
        bytes.extend_from_slice(&[0x9a, 0x06]);
        prost::encoding::encode_varint(padding as u64, &mut bytes);
        bytes.resize(size, 0);
        bytes
    }

    struct ReplyFixture(usize, bool);

    impl Service<Request<Body>> for ReplyFixture {
        type Response = Response<Body>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: Request<Body>) -> Self::Future {
            let mut payload = vec![8, OfferOutcome::Admitted as u8, 0x9a, 0x06];
            prost::encoding::encode_varint((self.0 - 6) as u64, &mut payload);
            payload.resize(self.0, 0);
            let mut response = Response::builder().header("content-type", "application/grpc");
            let body = if self.1 {
                response = response.header("grpc-status", "0");
                framed(payload)
            } else {
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                Body::new(framed(payload).with_trailers(std::future::ready(Some(Ok(trailers)))))
            };
            std::future::ready(Ok(response.body(body).unwrap()))
        }
    }

    #[tokio::test]
    async fn outbound_route_rejects_offer_credentials_and_absent_peer_before_decode() {
        let peer = Arc::new(endpoint());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let offer = peer.signer().token_at(now).unwrap();
        for (configured, authorization, expected) in [
            (None, None, "12"),
            (Some(peer.clone()), None, "16"),
            (Some(peer.clone()), Some(offer.authorization()), "7"),
        ] {
            let mut gate = OfferGate::new(ReplyFixture(0, false), configured);
            let mut request = Request::builder().uri(OUTBOUND_PATH);
            if let Some(value) = authorization {
                request = request.header("authorization", value);
            }
            let response = gate
                .call(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.headers()["grpc-status"], expected);
        }
    }

    // Feature: queue-home-delivery, Property 7
    // The receive budget includes unknown wire fields, even when the decoded
    // response would fit in a few bytes. This fixture is not an offer handler.
    #[tokio::test]
    async fn exact_and_one_over_response_frames() {
        for (size, initial_status) in [
            (MAX_OFFER_RESPONSE_BYTES, false),
            (MAX_OFFER_RESPONSE_BYTES + 1, false),
            (MAX_OFFER_RESPONSE_BYTES, true),
            (MAX_OFFER_RESPONSE_BYTES + 1, true),
        ] {
            let mut client = tokeira_proto::internal::runtime::workflow_task_dispatch_client::WorkflowTaskDispatchClient::new(OfferResponseLimit(ReplyFixture(size, initial_status)))
                .max_decoding_message_size(MAX_OFFER_RESPONSE_BYTES)
                .max_encoding_message_size(MAX_OFFER_REQUEST_BYTES);
            let result = client.offer_workflow_task(request()).await;
            assert_eq!(
                result.is_ok(),
                size == MAX_OFFER_RESPONSE_BYTES && !initial_status
            );
            if let Ok(response) = result {
                validate_response(response.get_ref()).unwrap();
            }
        }
    }

    // Feature: queue-home-delivery, Property 7
    // The frame cap precedes decoding (including discarded unknown fields), and
    // installing the offer gate cannot shrink the existing start response.
    #[tokio::test]
    async fn predecode_limits_and_start_response_are_independent() {
        let peer = Arc::new(endpoint());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let authorization = peer.signer().token_at(now).unwrap();
        let server = WorkflowTaskDispatchServer::new(WireFixture)
            .max_decoding_message_size(MAX_OFFER_REQUEST_BYTES)
            .max_encoding_message_size(128 * 1024 * 1024);
        let mut gate = OfferGate::new(server, Some(peer));
        for (size, status) in [
            (MAX_OFFER_REQUEST_BYTES, "14"),
            (MAX_OFFER_REQUEST_BYTES + 1, "8"),
        ] {
            let request = Request::builder()
                .uri(OFFER_PATH)
                .header("content-type", "application/grpc")
                .header("authorization", authorization.authorization())
                .body(framed(padded_request(size)))
                .unwrap();
            let response = gate.call(request).await.unwrap();
            assert_eq!(response.headers()["grpc-status"], status);
        }
        let request = Request::builder()
            .uri("/tokeira.internal.runtime.v1.WorkflowTaskDispatch/StartWorkflowTask")
            .header("content-type", "application/grpc")
            .body(framed(StartWorkflowTaskRequest::default().encode_to_vec()))
            .unwrap();
        let response = gate.call(request).await.unwrap();
        let body = response.into_body().collect().await.unwrap();
        assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
        assert!(body.to_bytes().len() > MAX_OFFER_RESPONSE_BYTES);
        let mut absent = OfferGate::new(WorkflowTaskDispatchServer::new(WireFixture), None);
        let response = absent
            .call(
                Request::builder()
                    .uri(OFFER_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["grpc-status"], "12");
    }

    fn request() -> OfferWorkflowTaskRequest {
        OfferWorkflowTaskRequest {
            run_key: Uuid::new_v4().as_bytes().to_vec(),
            namespace_id: Uuid::new_v4().as_bytes().to_vec(),
            logical_seq: 1,
            selected_queue: Some(OfferQueue {
                name: "queue".into(),
                ..Default::default()
            }),
            queue_kind: OfferQueueKind::Normal as i32,
            routing: Some(OfferRouting {
                mode: OfferRoutingMode::Live as i32,
                ..Default::default()
            }),
            version_directive: Some(OfferVersionDirective {
                behavior: OfferVersionBehavior::Unversioned as i32,
                ..Default::default()
            }),
            scheduled_at: Some(prost_types::Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
            ..Default::default()
        }
    }

    fn endpoint() -> PeerOfferEndpoint {
        let material = PeerMaterial::generate().unwrap();
        PeerOfferEndpoint::new(
            PeerVerifier::new(material.profile().clone()).unwrap(),
            material.signer(material.profile()).unwrap(),
        )
    }

    // Feature: queue-home-delivery, Property 7
    // Unknown enums and malformed combinations never reach volatile admission.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: outbound-dispatch-recovery, Property 18: Remote delivery grants no extra authority.
        #[test]
        fn outbound_admission_has_separate_purpose_and_capacity(release in 1usize..=32) {
            let endpoint = endpoint();
            let mut outbound = HeaderMap::new();
            outbound.insert(http::header::AUTHORIZATION, endpoint.signer().outbound_token_at(10_000).unwrap().authorization().parse().unwrap());
            prop_assert!(endpoint.enter(&outbound, 10_000).is_err());
            let mut permits = (0..32).map(|_| endpoint.enter_purpose(&outbound, 10_000, true).unwrap()).collect::<Vec<_>>();
            prop_assert_eq!(endpoint.enter_purpose(&outbound, 10_000, true).unwrap_err().code(), tonic::Code::ResourceExhausted);
            let mut offer = HeaderMap::new();
            offer.insert(http::header::AUTHORIZATION, endpoint.signer().token_at(10_000).unwrap().authorization().parse().unwrap());
            prop_assert!(endpoint.enter(&offer, 10_000).is_ok());
            permits.truncate(32 - release);
            for _ in 0..release { permits.push(endpoint.enter_purpose(&outbound, 10_000, true).unwrap()); }
            drop(permits);
            let used: usize = outbound.iter().map(|(name, value)| name.as_str().len() + value.len()).sum();
            outbound.insert("padding", "x".repeat(MAX_AUTHORIZATION_BYTES - used - 7).parse().unwrap());
            prop_assert!(endpoint.enter_purpose(&outbound, 10_000, true).is_ok());
            outbound.insert("padding", "x".repeat(MAX_AUTHORIZATION_BYTES - used - 6).parse().unwrap());
            prop_assert_eq!(endpoint.enter_purpose(&outbound, 10_000, true).unwrap_err().code(), tonic::Code::ResourceExhausted);
        }
        #[test]
        fn generated_wire_shapes(unknown in 4i32..i32::MAX, mutation in 0u8..9) {
            let mut value = request();
            prop_assert!(validate_request(&value).is_ok());
            match mutation {
                0 => value.queue_kind = unknown,
                1 => value.routing.as_mut().unwrap().mode = unknown,
                2 => value.version_directive.as_mut().unwrap().behavior = unknown,
                3 => value.logical_seq = 0,
                4 => value.run_key.pop().map(|_| ()).unwrap(),
                5 => value.queue_kind = OfferQueueKind::Sticky as i32,
                6 => value.routing.as_mut().unwrap().build_id = Some("present".into()),
                7 => value.scheduled_at.as_mut().unwrap().nanos = 1_000_000_000,
                _ => value.selected_queue.as_mut().unwrap().name.clear(),
            }
            prop_assert_eq!(validate_request(&value).unwrap_err().code(), tonic::Code::InvalidArgument);
            let unknown_outcome = OfferWorkflowTaskResponse { outcome: 7 + unknown % 10000, sticky_unavailable_reason: None };
            let misplaced_reason = OfferWorkflowTaskResponse { outcome: OfferOutcome::Known as i32, sticky_unavailable_reason: Some(1) };
            let unknown_reason = OfferWorkflowTaskResponse { outcome: OfferOutcome::StickyUnavailable as i32, sticky_unavailable_reason: Some(5 + unknown % 10000) };
            prop_assert!(validate_response(&unknown_outcome).is_err());
            prop_assert!(validate_response(&misplaced_reason).is_err());
            prop_assert!(validate_response(&unknown_reason).is_err());
        }

        // Feature: queue-home-delivery, Property 7
        // Exact limits succeed, one-over limits reject without waiting; dropping
        // an attempt immediately returns its handler reservation.
        #[test]
        fn generated_bounds_and_handler_saturation(release in 1usize..=MAX_OFFER_HANDLERS) {
            let endpoint = endpoint();
            let mut headers = HeaderMap::new();
            headers.insert(http::header::AUTHORIZATION, endpoint.signer().token_at(10_000).unwrap().authorization().parse().unwrap());
            let mut permits = (0..MAX_OFFER_HANDLERS).map(|_| endpoint.enter(&headers, 10_000).unwrap()).collect::<Vec<_>>();
            prop_assert_eq!(endpoint.enter(&headers, 10_000).unwrap_err().code(), tonic::Code::ResourceExhausted);
            permits.truncate(MAX_OFFER_HANDLERS - release);
            for _ in 0..release { permits.push(endpoint.enter(&headers, 10_000).unwrap()); }
            drop(permits);
            let used: usize = headers.iter().map(|(name,value)| name.as_str().len() + value.len()).sum();
            headers.insert("padding", "x".repeat(MAX_AUTHORIZATION_BYTES - used - "padding".len()).parse().unwrap());
            prop_assert!(endpoint.enter(&headers, 10_000).is_ok());
            headers.insert("padding", "x".repeat(MAX_AUTHORIZATION_BYTES - used - "padding".len() + 1).parse().unwrap());
            prop_assert_eq!(endpoint.enter(&headers, 10_000).unwrap_err().code(), tonic::Code::ResourceExhausted);
            let mut value = request();
            value.selected_queue.as_mut().unwrap().name = "x".repeat(MAX_OFFER_REQUEST_BYTES - value.encoded_len() - 8);
            while value.encoded_len() < MAX_OFFER_REQUEST_BYTES { value.selected_queue.as_mut().unwrap().name.push('x'); }
            prop_assert_eq!(value.encoded_len(), MAX_OFFER_REQUEST_BYTES);
            prop_assert!(validate_request(&value).is_ok());
            value.selected_queue.as_mut().unwrap().name.push('x');
            prop_assert_eq!(validate_request(&value).unwrap_err().code(), tonic::Code::ResourceExhausted);
        }
    }
}
