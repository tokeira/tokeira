//! Canonical protobuf JSON and Temporal payload-shorthand transforms.

use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost_reflect::{DynamicMessage, Kind, MessageDescriptor, ReflectMessage};
use serde_json::{Map, Value};

use super::HttpApiError;

const PAYLOAD: &str = "temporal.api.common.v1.Payload";
const PAYLOADS: &str = "temporal.api.common.v1.Payloads";
const ENCODING: &str = "encoding";
const MESSAGE_TYPE: &str = "messageType";
const PROTO_TYPE_KEY: &str = "_protoMessageType";

/// Selected Temporal JSON representation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JsonRepresentation {
    /// Whether eligible Temporal Payload values use shorthand JSON.
    pub payload_shorthand: bool,
    /// Whether JSON uses two-space indentation.
    pub pretty: bool,
}

impl JsonRepresentation {
    /// Resolve the outbound representation from Accept plus Temporal's query overrides.
    pub fn outbound(accept: Option<&str>, query: Option<&str>) -> Self {
        let media = accept.unwrap_or("application/json");
        let mut result = match media.split(',').next().unwrap_or(media).trim() {
            "application/json+pretty" => Self {
                payload_shorthand: true,
                pretty: true,
            },
            "application/json+no-payload-shorthand" => Self {
                payload_shorthand: false,
                pretty: false,
            },
            "application/json+pretty+no-payload-shorthand" => Self {
                payload_shorthand: false,
                pretty: true,
            },
            _ => Self {
                payload_shorthand: true,
                pretty: false,
            },
        };
        if let Some(query) = query {
            for (name, _) in url::form_urlencoded::parse(query.as_bytes()) {
                match name.as_ref() {
                    "pretty" => result.pretty = true,
                    "noPayloadShorthand" => result.payload_shorthand = false,
                    _ => {}
                }
            }
        }
        result
    }

    /// Resolve the inbound representation from Content-Type.
    pub fn inbound(content_type: Option<&str>) -> Self {
        let media = content_type
            .and_then(|value| value.split(';').next())
            .unwrap_or("application/json")
            .trim();
        match media {
            "application/json+pretty" => Self {
                payload_shorthand: true,
                pretty: true,
            },
            "application/json+no-payload-shorthand" => Self {
                payload_shorthand: false,
                pretty: false,
            },
            "application/json+pretty+no-payload-shorthand" => Self {
                payload_shorthand: false,
                pretty: true,
            },
            _ => Self {
                payload_shorthand: true,
                pretty: false,
            },
        }
    }

    /// Temporal media type corresponding to this representation.
    pub fn content_type(self) -> &'static str {
        match (self.payload_shorthand, self.pretty) {
            (true, false) => "application/json",
            (true, true) => "application/json+pretty",
            (false, false) => "application/json+no-payload-shorthand",
            (false, true) => "application/json+pretty+no-payload-shorthand",
        }
    }
}

/// Decode canonical protobuf JSON, optionally expanding Temporal payload shorthand first.
pub fn decode_json_message(
    descriptor: MessageDescriptor,
    bytes: &[u8],
    shorthand: bool,
) -> Result<DynamicMessage, HttpApiError> {
    let mut value: Value = serde_json::from_slice(bytes)
        .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
    if shorthand {
        expand_message(&descriptor, &mut value)?;
    }
    dynamic_from_value(descriptor, &value)
}

/// Encode canonical protobuf JSON, optionally contracting eligible Temporal payloads.
pub fn encode_json_message(
    message: &DynamicMessage,
    representation: JsonRepresentation,
) -> Result<Vec<u8>, HttpApiError> {
    let mut value = serde_json::to_value(message)
        .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
    if representation.payload_shorthand {
        contract_message(&message.descriptor(), &mut value)?;
    }
    if representation.pretty {
        serde_json::to_vec_pretty(&value)
    } else {
        serde_json::to_vec(&value)
    }
    .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))
}

fn dynamic_from_value(
    descriptor: MessageDescriptor,
    value: &Value,
) -> Result<DynamicMessage, HttpApiError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let message = DynamicMessage::deserialize(descriptor, &mut deserializer)
        .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
    deserializer
        .end()
        .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
    Ok(message)
}

fn expand_message(descriptor: &MessageDescriptor, value: &mut Value) -> Result<(), HttpApiError> {
    match descriptor.full_name() {
        PAYLOAD => return expand_payload(value),
        PAYLOADS => return expand_payloads(value),
        _ => {}
    }
    let Some(object) = value.as_object_mut() else {
        return Ok(());
    };
    for field in descriptor.fields() {
        let key = if object.contains_key(field.json_name()) {
            field.json_name()
        } else if object.contains_key(field.name()) {
            field.name()
        } else {
            continue;
        };
        let Some(field_value) = object.get_mut(key) else {
            continue;
        };
        transform_field(
            field.kind(),
            field.is_list(),
            field.is_map(),
            field_value,
            true,
        )?;
    }
    Ok(())
}

fn contract_message(descriptor: &MessageDescriptor, value: &mut Value) -> Result<(), HttpApiError> {
    match descriptor.full_name() {
        PAYLOAD => return contract_payload_in_place(value).map(|_| ()),
        PAYLOADS => return contract_payloads(value),
        _ => {}
    }
    let Some(object) = value.as_object_mut() else {
        return Ok(());
    };
    for field in descriptor.fields() {
        let key = field.json_name();
        let Some(field_value) = object.get_mut(key) else {
            continue;
        };
        transform_field(
            field.kind(),
            field.is_list(),
            field.is_map(),
            field_value,
            false,
        )?;
    }
    Ok(())
}

fn transform_field(
    kind: Kind,
    is_list: bool,
    is_map: bool,
    value: &mut Value,
    expanding: bool,
) -> Result<(), HttpApiError> {
    let Kind::Message(message) = kind else {
        return Ok(());
    };
    if is_map {
        if let Some(values) = value.as_object_mut() {
            for value in values.values_mut() {
                if expanding {
                    expand_message(&message, value)?;
                } else {
                    contract_message(&message, value)?;
                }
            }
        }
    } else if is_list {
        if let Some(values) = value.as_array_mut() {
            for value in values {
                if expanding {
                    expand_message(&message, value)?;
                } else {
                    contract_message(&message, value)?;
                }
            }
        }
    } else if expanding {
        expand_message(&message, value)?;
    } else {
        contract_message(&message, value)?;
    }
    Ok(())
}

fn expand_payloads(value: &mut Value) -> Result<(), HttpApiError> {
    if value.is_null() {
        *value = Value::Object(Map::new());
        return Ok(());
    }
    let Some(values) = value.as_array_mut() else {
        // A canonical Payloads object remains canonical, but its payload members
        // still need the strict canonical decoder rather than shorthand wrapping.
        return Ok(());
    };
    for value in values.iter_mut() {
        expand_payload(value)?;
    }
    let payloads = std::mem::take(values);
    *value = serde_json::json!({ "payloads": payloads });
    Ok(())
}

fn expand_payload(value: &mut Value) -> Result<(), HttpApiError> {
    if is_canonical_payload(value) {
        return Ok(());
    }
    let (encoding, message_type, data) = match value {
        Value::Null => ("binary/null", None, Vec::new()),
        Value::Object(object) if object.contains_key(PROTO_TYPE_KEY) => {
            let message_type = object
                .remove(PROTO_TYPE_KEY)
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .ok_or_else(|| {
                    HttpApiError::InvalidRequest(format!(
                        "internal key `{PROTO_TYPE_KEY}` should have type string"
                    ))
                })?;
            let data = serde_json::to_vec(object)
                .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
            ("json/protobuf", Some(message_type), data)
        }
        _ => {
            let data = serde_json::to_vec(value)
                .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
            ("json/plain", None, data)
        }
    };
    let mut metadata = Map::new();
    metadata.insert(
        ENCODING.to_owned(),
        Value::String(STANDARD.encode(encoding.as_bytes())),
    );
    if let Some(message_type) = message_type {
        metadata.insert(
            MESSAGE_TYPE.to_owned(),
            Value::String(STANDARD.encode(message_type.as_bytes())),
        );
    }
    let mut payload = Map::new();
    payload.insert("metadata".to_owned(), Value::Object(metadata));
    if !data.is_empty() {
        payload.insert("data".to_owned(), Value::String(STANDARD.encode(data)));
    }
    *value = Value::Object(payload);
    Ok(())
}

fn is_canonical_payload(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let Some(metadata) = object.get("metadata").and_then(Value::as_object) else {
        return false;
    };
    let Some(encoding) = metadata.get(ENCODING).and_then(Value::as_str) else {
        return false;
    };
    let Ok(encoding) = STANDARD.decode(encoding) else {
        return false;
    };
    if encoding == b"binary/null" && !object.contains_key("data") {
        return true;
    }
    if object.len() != 2 || !object.get("data").is_some_and(Value::is_string) {
        return false;
    }
    object
        .get("data")
        .and_then(Value::as_str)
        .is_some_and(|data| STANDARD.decode(data).is_ok())
        && metadata.values().all(|value| {
            value
                .as_str()
                .is_some_and(|value| STANDARD.decode(value).is_ok())
        })
}

fn contract_payloads(value: &mut Value) -> Result<(), HttpApiError> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    let Some(payloads) = object.get("payloads").and_then(Value::as_array) else {
        *value = Value::Array(Vec::new());
        return Ok(());
    };
    let mut shorthand = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let mut payload = payload.clone();
        if !contract_payload_in_place(&mut payload)? {
            return Ok(());
        }
        shorthand.push(payload);
    }
    *value = Value::Array(shorthand);
    Ok(())
}

fn contract_payload_in_place(value: &mut Value) -> Result<bool, HttpApiError> {
    let Some(object) = value.as_object() else {
        return Ok(false);
    };
    let Some(metadata_object) = object.get("metadata").and_then(Value::as_object) else {
        return Ok(false);
    };
    let metadata = decode_metadata(metadata_object)?;
    let encoding = metadata
        .get(ENCODING)
        .map(Vec::as_slice)
        .unwrap_or_default();
    match encoding {
        b"binary/null" => {
            *value = Value::Null;
            Ok(true)
        }
        b"json/plain" if metadata.len() == 1 => {
            let data = decode_payload_data(object)?;
            *value = serde_json::from_slice(&data)
                .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
            Ok(true)
        }
        b"json/protobuf" if metadata.len() == 2 => {
            let Some(message_type) = metadata.get(MESSAGE_TYPE).filter(|value| !value.is_empty())
            else {
                return Ok(false);
            };
            let data = decode_payload_data(object)?;
            let mut decoded: Value = serde_json::from_slice(&data)
                .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))?;
            let Some(decoded) = decoded.as_object_mut() else {
                return Err(HttpApiError::InvalidGrpcResponse(
                    "json/protobuf payload data is not an object".to_owned(),
                ));
            };
            decoded.insert(
                PROTO_TYPE_KEY.to_owned(),
                Value::String(String::from_utf8_lossy(message_type).into_owned()),
            );
            *value = Value::Object(std::mem::take(decoded));
            Ok(true)
        }
        b"json/plain" | b"json/protobuf" => Ok(false),
        _ => Err(HttpApiError::InvalidGrpcResponse(format!(
            "unsupported encoding {}",
            String::from_utf8_lossy(encoding)
        ))),
    }
}

fn decode_metadata(object: &Map<String, Value>) -> Result<HashMap<String, Vec<u8>>, HttpApiError> {
    object
        .iter()
        .map(|(key, value)| {
            let encoded = value.as_str().ok_or_else(|| {
                HttpApiError::InvalidGrpcResponse("payload metadata is not base64 text".to_owned())
            })?;
            STANDARD
                .decode(encoded)
                .map(|value| (key.clone(), value))
                .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))
        })
        .collect()
}

fn decode_payload_data(object: &Map<String, Value>) -> Result<Vec<u8>, HttpApiError> {
    let encoded = object
        .get("data")
        .and_then(Value::as_str)
        .unwrap_or_default();
    STANDARD
        .decode(encoded)
        .map_err(|error| HttpApiError::InvalidGrpcResponse(error.to_string()))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost::Message as _;
    use prost_reflect::DescriptorPool;

    use super::*;

    fn pool() -> DescriptorPool {
        DescriptorPool::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET).expect("descriptor")
    }

    #[test]
    fn payloads_shorthand_round_trips_through_canonical_wire_value() {
        let descriptor = pool()
            .get_message_by_name("temporal.api.common.v1.Payloads")
            .expect("Payloads");
        let shorthand =
            br#"[{"someField":"value"},null,{"name":"x","_protoMessageType":"pkg.Msg"}]"#;
        let message = decode_json_message(descriptor, shorthand, true).expect("decode");
        let encoded = encode_json_message(
            &message,
            JsonRepresentation {
                payload_shorthand: true,
                pretty: false,
            },
        )
        .expect("encode");
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).expect("JSON"),
            serde_json::from_slice::<Value>(shorthand).expect("JSON")
        );
    }

    #[test]
    fn representation_query_flags_override_accept() {
        assert_eq!(
            JsonRepresentation::outbound(
                Some("application/json"),
                Some("pretty&noPayloadShorthand")
            ),
            JsonRepresentation {
                payload_shorthand: false,
                pretty: true,
            }
        );
    }

    #[test]
    fn compact_and_pretty_success_json_have_temporal_formatting() {
        let descriptor = pool()
            .get_message_by_name("temporal.api.workflowservice.v1.DescribeNamespaceRequest")
            .expect("request");
        let field = descriptor.get_field_by_name("namespace").expect("field");
        let mut message = DynamicMessage::new(descriptor);
        message.set_field(&field, prost_reflect::Value::String("default".to_owned()));
        let compact = encode_json_message(
            &message,
            JsonRepresentation {
                payload_shorthand: false,
                pretty: false,
            },
        )
        .expect("compact");
        let pretty = encode_json_message(
            &message,
            JsonRepresentation {
                payload_shorthand: false,
                pretty: true,
            },
        )
        .expect("pretty");
        assert_eq!(compact, br#"{"namespace":"default"}"#);
        assert_eq!(pretty, b"{\n  \"namespace\": \"default\"\n}");
    }

    #[test]
    fn ineligible_payload_metadata_preserves_canonical_shape() {
        let descriptor = pool()
            .get_message_by_name("temporal.api.common.v1.Payload")
            .expect("Payload");
        let canonical = serde_json::json!({
            "metadata": {
                "encoding": STANDARD.encode("json/plain"),
                "codec": STANDARD.encode("operator-owned")
            },
            "data": STANDARD.encode("{\"value\":1}")
        });
        let message = decode_json_message(
            descriptor,
            &serde_json::to_vec(&canonical).expect("JSON"),
            true,
        )
        .expect("decode");
        let encoded = encode_json_message(&message, JsonRepresentation::default()).expect("encode");
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).expect("JSON"),
            canonical
        );
    }

    proptest! {
        // Feature: edge-http-api-gateway, Property 4
        #[test]
        fn dynamic_proto_json_preserves_wire_value(namespace in "[A-Za-z0-9_.-]{0,64}") {
            let descriptor = pool()
                .get_message_by_name("temporal.api.workflowservice.v1.DescribeNamespaceRequest")
                .expect("request");
            let field = descriptor.get_field_by_name("namespace").expect("field");
            let mut message = DynamicMessage::new(descriptor.clone());
            message.set_field(&field, prost_reflect::Value::String(namespace));
            let json = encode_json_message(
                &message,
                JsonRepresentation { payload_shorthand: false, pretty: false },
            ).expect("encode");
            let decoded = decode_json_message(descriptor, &json, false).expect("decode");
            prop_assert_eq!(decoded.encode_to_vec(), message.encode_to_vec());
        }

        // Feature: edge-http-api-gateway, Property 5
        #[test]
        fn eligible_json_plain_payload_shorthand_round_trips(
            value in prop_oneof![
                any::<bool>().prop_map(Value::Bool),
                any::<i32>().prop_map(|value| Value::from(i64::from(value))),
                "[A-Za-z0-9 _.-]{0,64}".prop_map(Value::String),
                Just(Value::Null),
            ]
        ) {
            let descriptor = pool()
                .get_message_by_name("temporal.api.common.v1.Payload")
                .expect("Payload");
            let bytes = serde_json::to_vec(&value).expect("JSON");
            let message = decode_json_message(descriptor, &bytes, true).expect("decode");
            let encoded = encode_json_message(
                &message,
                JsonRepresentation { payload_shorthand: true, pretty: false },
            ).expect("encode");
            prop_assert_eq!(serde_json::from_slice::<Value>(&encoded).expect("JSON"), value);
        }
    }
}
