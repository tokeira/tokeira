//! Raw response fixtures exercise limits before protobuf discards unknown fields.

use std::{
    convert::Infallible,
    future::Ready,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use tonic::body::Body;
use tower::Service;

use super::*;

struct Reply {
    size: usize,
    initial_status: bool,
    declared_only: bool,
}

impl Service<http::Request<Body>> for Reply {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<Body>) -> Self::Future {
        let mut frame = vec![0];
        frame.extend_from_slice(&(self.size as u32).to_be_bytes());
        if !self.declared_only {
            frame.extend_from_slice(&[0x9a, 0x06]);
            prost::encoding::encode_varint((self.size - 6) as u64, &mut frame);
            frame.resize(self.size + 5, 0);
        }
        let body = Body::new(Full::new(Bytes::from(frame)));
        let mut response = http::Response::builder().header("content-type", "application/grpc");
        let body = if self.initial_status {
            response = response.header("grpc-status", "0");
            body
        } else {
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            Body::new(body.with_trailers(std::future::ready(Some(Ok(trailers)))))
        };
        std::future::ready(Ok(response.body(body).unwrap()))
    }
}

async fn receive(
    reply: Reply,
) -> std::result::Result<Response<wire::StartActivityTaskResponse>, Status> {
    ActivityTaskDispatchClient::new(
        super::super::super::workflow_offer::OfferResponseLimit::new(reply),
    )
    .max_decoding_message_size(MAX_RESPONSE)
    .start_activity_task(wire::StartActivityTaskRequest::default())
    .await
}

#[tokio::test]
async fn exact_activity_response_budget_includes_unknown_wire_fields() {
    assert!(
        receive(Reply {
            size: MAX_RESPONSE,
            initial_status: false,
            declared_only: false
        })
        .await
        .is_ok()
    );
    assert!(
        receive(Reply {
            size: MAX_RESPONSE + 1,
            initial_status: false,
            declared_only: false
        })
        .await
        .is_err()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 9: an oversized declared frame is rejected before allocating its promised body.
    #[test]
    fn oversized_activity_response_headers_are_bounded(excess in 1usize..4096, initial_status in any::<bool>()) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let result = receive(Reply { size: MAX_RESPONSE + excess, initial_status, declared_only: true }).await;
            prop_assert!(result.is_err());
            Ok(())
        })?;
    }
}
