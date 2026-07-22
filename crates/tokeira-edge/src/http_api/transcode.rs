//! HTTP body/path/query to dynamic protobuf request transcoding.

use std::collections::{HashMap, HashSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message;
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, ReflectMessage, Value,
};

use super::{HttpApiError, HttpApiMatch, JsonRepresentation, decode_json_message};

/// Protobuf dispatch produced from one annotated HTTP request.
#[derive(Clone, Debug)]
pub struct HttpApiDispatch {
    /// Full gRPC method path.
    pub grpc_path: String,
    /// Unframed protobuf request bytes.
    pub protobuf: Vec<u8>,
    /// Descriptor used to decode the unary response.
    pub output: MessageDescriptor,
    /// Selected outbound JSON representation.
    pub representation: JsonRepresentation,
    /// Top-level namespace label, if present after transcoding.
    pub namespace: Option<String>,
}

/// Stateless descriptor-guided HTTP request transcoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct HttpApiTranscoder;

impl HttpApiTranscoder {
    /// Translate body, captures, and query into the binding's protobuf input.
    pub fn transcode(
        matched: &HttpApiMatch,
        body: &[u8],
        query: Option<&str>,
        inbound: JsonRepresentation,
        outbound: JsonRepresentation,
    ) -> Result<HttpApiDispatch, HttpApiError> {
        let descriptor = matched.binding.input.clone();
        let mut request = match matched.binding.body.as_str() {
            "" => DynamicMessage::new(descriptor.clone()),
            "*" => decode_json_message(descriptor.clone(), body, inbound.payload_shorthand)?,
            selector => {
                let json: serde_json::Value = serde_json::from_slice(body)
                    .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
                let mut root = serde_json::Value::Object(serde_json::Map::new());
                insert_json_path(&mut root, selector, json)?;
                let bytes = serde_json::to_vec(&root)
                    .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
                decode_json_message(descriptor.clone(), &bytes, inbound.payload_shorthand)?
            }
        };

        let mut exclusions = HashSet::new();
        if matched.binding.body == "*" {
            exclusions.insert("*".to_owned());
        } else if !matched.binding.body.is_empty() {
            exclusions.insert(matched.binding.body.clone());
        }
        for capture in &matched.captures {
            set_field_path(&mut request, &capture.field_path, &[capture.value.as_str()])?;
            exclusions.insert(capture.field_path.clone());
        }
        if !exclusions.contains("*") {
            populate_query(&mut request, query, &exclusions)?;
        }
        let namespace = request
            .get_field_by_name("namespace")
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .filter(|value| !value.is_empty());
        Ok(HttpApiDispatch {
            grpc_path: matched.binding.grpc_path.clone(),
            protobuf: request.encode_to_vec(),
            output: matched.binding.output.clone(),
            representation: outbound,
            namespace,
        })
    }
}

fn insert_json_path(
    root: &mut serde_json::Value,
    selector: &str,
    value: serde_json::Value,
) -> Result<(), HttpApiError> {
    let mut current = root;
    let mut parts = selector.split('.').peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            let object = current.as_object_mut().ok_or_else(|| {
                HttpApiError::InvalidRequest(format!(
                    "body selector `{selector}` crosses a non-object"
                ))
            })?;
            object.insert(part.to_owned(), value);
            return Ok(());
        }
        let object = current.as_object_mut().ok_or_else(|| {
            HttpApiError::InvalidRequest(format!("body selector `{selector}` crosses a non-object"))
        })?;
        current = object
            .entry(part.to_owned())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    }
    Err(HttpApiError::InvalidRequest(
        "empty body selector".to_owned(),
    ))
}

fn populate_query(
    message: &mut DynamicMessage,
    query: Option<&str>,
    exclusions: &HashSet<String>,
) -> Result<(), HttpApiError> {
    let Some(query) = query else {
        return Ok(());
    };
    let mut values: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if matches!(key.as_ref(), "pretty" | "noPayloadShorthand") {
            continue;
        }
        values
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    for (key, values) in values {
        let (selector, map_key) = split_map_selector(&key);
        if exclusions.iter().any(|excluded| {
            selector == *excluded
                || selector.starts_with(&format!("{excluded}."))
                || excluded.starts_with(&format!("{selector}."))
        }) {
            continue;
        }
        if field_path_exists(&message.descriptor(), selector) {
            let values = values.iter().map(String::as_str).collect::<Vec<_>>();
            if let Some(map_key) = map_key {
                set_map_field_path(message, selector, map_key, &values)?;
            } else {
                set_field_path(message, selector, &values)?;
            }
        }
    }
    Ok(())
}

fn split_map_selector(selector: &str) -> (&str, Option<&str>) {
    let Some(prefix) = selector.strip_suffix(']') else {
        return (selector, None);
    };
    let Some(open) = prefix.rfind('[') else {
        return (selector, None);
    };
    if open == 0 || prefix[open + 1..].is_empty() {
        return (selector, None);
    }
    (&prefix[..open], Some(&prefix[open + 1..]))
}

fn field_path_exists(descriptor: &MessageDescriptor, selector: &str) -> bool {
    let mut descriptor = descriptor.clone();
    let parts = selector.split('.').collect::<Vec<_>>();
    for (index, part) in parts.iter().enumerate() {
        let Some(field) = find_field(&descriptor, part) else {
            return false;
        };
        if index + 1 < parts.len() {
            let Kind::Message(next) = field.kind() else {
                return false;
            };
            if field.is_list() || field.is_map() {
                return false;
            }
            descriptor = next;
        }
    }
    true
}

fn set_field_path(
    message: &mut DynamicMessage,
    selector: &str,
    values: &[&str],
) -> Result<(), HttpApiError> {
    let Some((head, tail)) = selector.split_once('.') else {
        let descriptor = message.descriptor();
        let field = find_field(&descriptor, selector).ok_or_else(|| {
            HttpApiError::InvalidRequest(format!("unknown field selector `{selector}`"))
        })?;
        let value = parse_field_values(&field, values)?;
        message
            .try_set_field(&field, value)
            .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
        return Ok(());
    };
    let descriptor = message.descriptor();
    let field = find_field(&descriptor, head).ok_or_else(|| {
        HttpApiError::InvalidRequest(format!("unknown field selector `{selector}`"))
    })?;
    if field.is_list() || field.is_map() {
        return Err(HttpApiError::InvalidRequest(format!(
            "non-singular message in field selector `{selector}`"
        )));
    }
    let Kind::Message(nested_descriptor) = field.kind() else {
        return Err(HttpApiError::InvalidRequest(format!(
            "non-message component `{head}` in field selector `{selector}`"
        )));
    };
    if !message.has_field(&field) {
        message.set_field(
            &field,
            Value::Message(DynamicMessage::new(nested_descriptor)),
        );
    }
    let nested = message
        .get_field_mut(&field)
        .as_message_mut()
        .ok_or_else(|| HttpApiError::InvalidRequest(format!("invalid selector `{selector}`")))?;
    set_field_path(nested, tail, values)
}

fn set_map_field_path(
    message: &mut DynamicMessage,
    selector: &str,
    key: &str,
    values: &[&str],
) -> Result<(), HttpApiError> {
    let Some((head, tail)) = selector.split_once('.') else {
        let descriptor = message.descriptor();
        let field = find_field(&descriptor, selector).ok_or_else(|| {
            HttpApiError::InvalidRequest(format!("unknown map field selector `{selector}`"))
        })?;
        let Kind::Message(entry) = field.kind() else {
            return Err(HttpApiError::InvalidRequest(format!(
                "field `{selector}` is not a map"
            )));
        };
        if !field.is_map() || values.len() != 1 {
            return Err(HttpApiError::InvalidRequest(format!(
                "map field `{selector}` requires one bracket-keyed value"
            )));
        }
        let map_key = parse_map_key(entry.map_entry_key_field().kind(), key)?;
        let map_value = parse_scalar(entry.map_entry_value_field().kind(), values[0])?;
        let map = message.get_field_mut(&field).as_map_mut().ok_or_else(|| {
            HttpApiError::InvalidRequest(format!("field `{selector}` is not a map"))
        })?;
        map.insert(map_key, map_value);
        return Ok(());
    };
    let descriptor = message.descriptor();
    let field = find_field(&descriptor, head).ok_or_else(|| {
        HttpApiError::InvalidRequest(format!("unknown map field selector `{selector}`"))
    })?;
    if field.is_list() || field.is_map() {
        return Err(HttpApiError::InvalidRequest(format!(
            "non-singular message in map field selector `{selector}`"
        )));
    }
    let Kind::Message(nested_descriptor) = field.kind() else {
        return Err(HttpApiError::InvalidRequest(format!(
            "non-message component `{head}` in map field selector `{selector}`"
        )));
    };
    if !message.has_field(&field) {
        message.set_field(
            &field,
            Value::Message(DynamicMessage::new(nested_descriptor)),
        );
    }
    let nested = message
        .get_field_mut(&field)
        .as_message_mut()
        .ok_or_else(|| HttpApiError::InvalidRequest(format!("invalid selector `{selector}`")))?;
    set_map_field_path(nested, tail, key, values)
}

fn find_field(descriptor: &MessageDescriptor, name: &str) -> Option<FieldDescriptor> {
    descriptor
        .get_field_by_name(name)
        .or_else(|| descriptor.fields().find(|field| field.json_name() == name))
}

fn parse_field_values(field: &FieldDescriptor, values: &[&str]) -> Result<Value, HttpApiError> {
    if field.is_map() {
        return Err(HttpApiError::InvalidRequest(format!(
            "map query syntax for `{}` requires bracket notation",
            field.name()
        )));
    }
    if field.is_list() {
        return values
            .iter()
            .map(|value| parse_scalar(field.kind(), value))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List);
    }
    if values.len() != 1 {
        return Err(HttpApiError::InvalidRequest(format!(
            "too many values for field `{}`: {}",
            field.name(),
            values.join(", ")
        )));
    }
    parse_scalar(field.kind(), values[0])
}

fn parse_map_key(kind: Kind, value: &str) -> Result<MapKey, HttpApiError> {
    let invalid = |error: &dyn std::fmt::Display| HttpApiError::InvalidRequest(error.to_string());
    match kind {
        Kind::Bool => value
            .parse()
            .map(MapKey::Bool)
            .map_err(|error| invalid(&error)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => value
            .parse()
            .map(MapKey::I32)
            .map_err(|error| invalid(&error)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => value
            .parse()
            .map(MapKey::I64)
            .map_err(|error| invalid(&error)),
        Kind::Uint32 | Kind::Fixed32 => value
            .parse()
            .map(MapKey::U32)
            .map_err(|error| invalid(&error)),
        Kind::Uint64 | Kind::Fixed64 => value
            .parse()
            .map(MapKey::U64)
            .map_err(|error| invalid(&error)),
        Kind::String => Ok(MapKey::String(value.to_owned())),
        _ => Err(HttpApiError::InvalidRequest(
            "protobuf map keys must be scalar integral, boolean, or string values".to_owned(),
        )),
    }
}

fn parse_scalar(kind: Kind, value: &str) -> Result<Value, HttpApiError> {
    let invalid = |error: &dyn std::fmt::Display| HttpApiError::InvalidRequest(error.to_string());
    match kind {
        Kind::Bool => value
            .parse()
            .map(Value::Bool)
            .map_err(|error| invalid(&error)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => value
            .parse()
            .map(Value::I32)
            .map_err(|error| invalid(&error)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => value
            .parse()
            .map(Value::I64)
            .map_err(|error| invalid(&error)),
        Kind::Uint32 | Kind::Fixed32 => value
            .parse()
            .map(Value::U32)
            .map_err(|error| invalid(&error)),
        Kind::Uint64 | Kind::Fixed64 => value
            .parse()
            .map(Value::U64)
            .map_err(|error| invalid(&error)),
        Kind::Float => value
            .parse()
            .map(Value::F32)
            .map_err(|error| invalid(&error)),
        Kind::Double => value
            .parse()
            .map(Value::F64)
            .map_err(|error| invalid(&error)),
        Kind::String => Ok(Value::String(value.to_owned())),
        Kind::Bytes => STANDARD
            .decode(value)
            .map(|bytes| Value::Bytes(bytes.into()))
            .map_err(|error| invalid(&error)),
        Kind::Enum(descriptor) => descriptor
            .get_value_by_name(value)
            .map(|value| Value::EnumNumber(value.number()))
            .or_else(|| {
                value.parse::<i32>().ok().and_then(|number| {
                    descriptor
                        .get_value(number)
                        .map(|value| Value::EnumNumber(value.number()))
                })
            })
            .ok_or_else(|| {
                HttpApiError::InvalidRequest(format!("`{value}` is not a valid enum value"))
            }),
        Kind::Message(descriptor) => {
            let json = serde_json::Value::String(value.to_owned());
            let bytes = serde_json::to_vec(&json)
                .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
            decode_json_message(descriptor, &bytes, false).map(Value::Message)
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost_reflect::DynamicMessage;

    use super::*;
    use crate::http_api::HttpApiCatalog;

    #[test]
    fn body_then_path_then_query_matches_generated_handler_order() {
        let catalog = HttpApiCatalog::pinned().expect("catalog");
        let matched = catalog
            .recognize("POST", "/namespaces/path-ns/workflows/path-wf")
            .expect("path")
            .expect("binding");
        let body = br#"{
            "namespace":"body-ns",
            "workflowId":"body-wf",
            "workflowType":{"name":"type"},
            "taskQueue":{"name":"queue"}
        }"#;
        let dispatch = HttpApiTranscoder::transcode(
            &matched,
            body,
            Some("namespace=query-ns&requestId=req"),
            JsonRepresentation {
                payload_shorthand: false,
                pretty: false,
            },
            JsonRepresentation::default(),
        )
        .expect("dispatch");
        let request =
            DynamicMessage::decode(matched.binding.input.clone(), dispatch.protobuf.as_slice())
                .expect("protobuf");
        assert_eq!(
            request
                .get_field_by_name("namespace")
                .and_then(|v| v.as_str().map(ToOwned::to_owned)),
            Some("path-ns".to_owned())
        );
        assert_eq!(
            request
                .get_field_by_name("workflow_id")
                .and_then(|v| v.as_str().map(ToOwned::to_owned)),
            Some("path-wf".to_owned())
        );
        assert_eq!(
            request
                .get_field_by_name("request_id")
                .and_then(|v| v.as_str().map(ToOwned::to_owned)),
            Some(String::new()),
            "body `*` consumes all ordinary request fields, so query is filtered"
        );
    }

    #[test]
    fn history_filter_accepts_numeric_enum_query() {
        let catalog = HttpApiCatalog::pinned().expect("catalog");
        let matched = catalog
            .recognize("GET", "/namespaces/ns/workflows/wf/history")
            .expect("path")
            .expect("binding");
        let dispatch = HttpApiTranscoder::transcode(
            &matched,
            &[],
            Some("historyEventFilterType=2"),
            JsonRepresentation::default(),
            JsonRepresentation::default(),
        )
        .expect("dispatch");
        let request =
            DynamicMessage::decode(matched.binding.input.clone(), dispatch.protobuf.as_slice())
                .expect("protobuf");
        assert_eq!(
            request
                .get_field_by_name("history_event_filter_type")
                .and_then(|value| match value.as_ref() {
                    Value::EnumNumber(number) => Some(*number),
                    _ => None,
                }),
            Some(2)
        );
    }

    #[test]
    fn map_query_uses_bracket_key_syntax() {
        let catalog = HttpApiCatalog::pinned().expect("catalog");
        let descriptor = catalog
            .pool()
            .get_message_by_name("temporal.api.operatorservice.v1.AddSearchAttributesRequest")
            .expect("request descriptor");
        let field = descriptor
            .get_field_by_name("search_attributes")
            .expect("map field");
        let mut request = DynamicMessage::new(descriptor);
        populate_query(
            &mut request,
            Some("searchAttributes[CustomKeywordField]=INDEXED_VALUE_TYPE_KEYWORD"),
            &HashSet::new(),
        )
        .expect("query");
        let value = request.get_field(&field);
        let map = value.as_map().expect("map value");
        assert_eq!(
            map.get(&MapKey::String("CustomKeywordField".to_owned())),
            Some(&Value::EnumNumber(2))
        );
    }

    proptest! {
        // Feature: edge-http-api-gateway, Property 3
        #[test]
        fn path_fields_override_body_and_complete_body_excludes_query(
            path_namespace in "[a-z]{1,16}",
            path_workflow in "[a-z]{1,16}",
            body_namespace in "[a-z]{1,16}",
            query_request_id in "[a-z]{1,16}",
        ) {
            let catalog = HttpApiCatalog::pinned().expect("catalog");
            let path = format!("/namespaces/{path_namespace}/workflows/{path_workflow}");
            let matched = catalog.recognize("POST", &path).expect("path").expect("binding");
            let body = serde_json::to_vec(&serde_json::json!({
                "namespace": body_namespace,
                "workflowId": "body-workflow",
                "workflowType": {"name": "type"},
                "taskQueue": {"name": "queue"}
            })).expect("body");
            let query = format!("requestId={query_request_id}");
            let dispatch = HttpApiTranscoder::transcode(
                &matched,
                &body,
                Some(&query),
                JsonRepresentation::default(),
                JsonRepresentation::default(),
            ).expect("dispatch");
            let request = DynamicMessage::decode(
                matched.binding.input.clone(),
                dispatch.protobuf.as_slice(),
            ).expect("protobuf");
            prop_assert_eq!(request.get_field_by_name("namespace").and_then(|v| v.as_str().map(ToOwned::to_owned)), Some(path_namespace));
            prop_assert_eq!(request.get_field_by_name("workflow_id").and_then(|v| v.as_str().map(ToOwned::to_owned)), Some(path_workflow));
            prop_assert_eq!(request.get_field_by_name("request_id").and_then(|v| v.as_str().map(ToOwned::to_owned)), Some(String::new()));
        }
    }
}
