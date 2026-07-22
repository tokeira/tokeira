//! Dynamic protobuf success and `google.rpc.Status` HTTP rendering.

use prost::Message as _;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::{Map, Value};

use super::{HttpApiDispatch, HttpApiError, JsonRepresentation, encode_json_message};

/// Fully rendered HTTP response payload returned to the app transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpApiRenderedResponse {
    /// HTTP status selected by gRPC-Gateway's v1.31.0 mapping.
    pub status: u16,
    /// Temporal JSON media type selected for this request.
    pub content_type: &'static str,
    /// JSON response bytes.
    pub body: Vec<u8>,
    /// `WWW-Authenticate` value for unauthenticated statuses.
    pub www_authenticate: Option<String>,
}

/// Decode and render one successful unary protobuf message.
pub fn render_success(
    dispatch: &HttpApiDispatch,
    protobuf: &[u8],
) -> Result<HttpApiRenderedResponse, HttpApiError> {
    let message = DynamicMessage::decode(dispatch.output.clone(), protobuf)
        .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
    Ok(HttpApiRenderedResponse {
        status: 200,
        content_type: dispatch.representation.content_type(),
        body: encode_json_message(&message, dispatch.representation)?,
        www_authenticate: None,
    })
}

/// Render one gRPC status, preferring its serialized `google.rpc.Status` envelope.
pub fn render_error(
    descriptor_source: &MessageDescriptor,
    representation: JsonRepresentation,
    code: i32,
    message: &str,
    status_details: Option<&[u8]>,
) -> Result<HttpApiRenderedResponse, HttpApiError> {
    let pool = descriptor_source.parent_pool();
    let status = if let Some(details) = status_details.filter(|details| !details.is_empty()) {
        RpcStatus::decode(details)
            .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?
    } else {
        RpcStatus {
            code,
            message: message.to_owned(),
            details: Vec::new(),
        }
    };
    let body = status_json(pool, &status)?;
    let body = if representation.pretty {
        serde_json::to_vec_pretty(&body)
    } else {
        serde_json::to_vec(&body)
    }
    .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
    Ok(HttpApiRenderedResponse {
        status: grpc_code_to_http(code),
        content_type: representation.content_type(),
        body,
        www_authenticate: (code == 16).then(|| message.to_owned()),
    })
}

#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

fn status_json(
    pool: &prost_reflect::DescriptorPool,
    status: &RpcStatus,
) -> Result<Value, HttpApiError> {
    let mut object = Map::new();
    object.insert("code".to_owned(), Value::from(status.code));
    object.insert("message".to_owned(), Value::String(status.message.clone()));
    if !status.details.is_empty() {
        let details = status
            .details
            .iter()
            .map(|detail| any_json(pool, detail))
            .collect::<Result<Vec<_>, _>>()?;
        object.insert("details".to_owned(), Value::Array(details));
    }
    Ok(Value::Object(object))
}

fn any_json(
    pool: &prost_reflect::DescriptorPool,
    any: &prost_types::Any,
) -> Result<Value, HttpApiError> {
    let type_name = any.type_url.rsplit('/').next().unwrap_or_default();
    let descriptor = pool.get_message_by_name(type_name).ok_or_else(|| {
        HttpApiError::Descriptor(format!("unknown status detail type `{}`", any.type_url))
    })?;
    let message = DynamicMessage::decode(descriptor, any.value.as_slice())
        .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
    let mut value = serde_json::to_value(&message)
        .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
    let object = value.as_object_mut().ok_or_else(|| {
        HttpApiError::InvalidGrpcResponse("status detail did not encode as an object".to_owned())
    })?;
    // ProtoJSON renders Any by merging the unpacked message with its type URL.
    // The response descriptor pool contains all Temporal error-detail messages,
    // so no runtime registry or reflection service is required.
    object.insert("@type".to_owned(), Value::String(any.type_url.clone()));
    Ok(value)
}

fn grpc_code_to_http(code: i32) -> u16 {
    match code {
        0 => 200,
        1 => 499,
        3 | 9 | 11 => 400,
        16 => 401,
        7 => 403,
        5 => 404,
        6 | 10 => 409,
        8 => 429,
        12 => 501,
        14 => 503,
        4 => 504,
        _ => 500,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost_reflect::DescriptorPool;

    use super::*;

    #[test]
    fn routing_unimplemented_maps_to_temporal_501_shape() {
        let pool =
            DescriptorPool::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET).expect("descriptor");
        let source = pool
            .get_message_by_name("temporal.api.workflowservice.v1.GetSystemInfoResponse")
            .expect("response");
        let rendered = render_error(
            &source,
            JsonRepresentation::default(),
            12,
            "Method Not Allowed",
            None,
        )
        .expect("response");
        assert_eq!(rendered.status, 501);
        assert_eq!(
            std::str::from_utf8(&rendered.body).expect("utf8"),
            r#"{"code":12,"message":"Method Not Allowed"}"#
        );
    }

    #[test]
    fn status_details_render_already_started_run_id() {
        let pool =
            DescriptorPool::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET).expect("descriptor");
        let source = pool
            .get_message_by_name("temporal.api.workflowservice.v1.GetSystemInfoResponse")
            .expect("response");
        let detail_descriptor = pool
            .get_message_by_name(
                "temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure",
            )
            .expect("detail");
        let run_id = detail_descriptor
            .get_field_by_name("run_id")
            .expect("field");
        let mut detail = DynamicMessage::new(detail_descriptor);
        detail.set_field(&run_id, prost_reflect::Value::String("run-123".to_owned()));
        let status = RpcStatus {
            code: 6,
            message: "already started".to_owned(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure".to_owned(),
                value: detail.encode_to_vec(),
            }],
        };
        let rendered = render_error(
            &source,
            JsonRepresentation::default(),
            6,
            "already started",
            Some(&status.encode_to_vec()),
        )
        .expect("response");
        let body = serde_json::from_slice::<Value>(&rendered.body).expect("JSON");
        assert_eq!(body["details"][0]["runId"], "run-123");
    }

    #[test]
    fn pretty_error_uses_two_spaces_without_a_trailing_newline() {
        let pool =
            DescriptorPool::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET).expect("descriptor");
        let source = pool
            .get_message_by_name("temporal.api.workflowservice.v1.GetSystemInfoResponse")
            .expect("response");
        let rendered = render_error(
            &source,
            JsonRepresentation {
                payload_shorthand: true,
                pretty: true,
            },
            5,
            "missing",
            None,
        )
        .expect("response");
        assert_eq!(
            rendered.body,
            b"{\n  \"code\": 5,\n  \"message\": \"missing\"\n}"
        );
    }

    proptest! {
        // Feature: edge-http-api-gateway, Property 9
        #[test]
        fn status_code_and_message_survive_http_mapping(
            code in 0_i32..=16,
            message in "[A-Za-z0-9 _.-]{0,64}",
        ) {
            let pool = DescriptorPool::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET)
                .expect("descriptor");
            let source = pool
                .get_message_by_name("temporal.api.workflowservice.v1.GetSystemInfoResponse")
                .expect("response");
            let rendered = render_error(
                &source,
                JsonRepresentation::default(),
                code,
                &message,
                None,
            ).expect("response");
            let body = serde_json::from_slice::<Value>(&rendered.body).expect("JSON");
            prop_assert_eq!(body["code"].as_i64(), Some(i64::from(code)));
            prop_assert_eq!(body["message"].as_str(), Some(message.as_str()));
            prop_assert_eq!(rendered.status, grpc_code_to_http(code));
            prop_assert_eq!(rendered.www_authenticate.is_some(), code == 16);
        }
    }
}
