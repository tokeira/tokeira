//! Small shared conversions for public protobuf common types.

use crate::{
    conversions::ProtoConversionError,
    public::{
        common,
        temporal::api::{failure::v1 as failure_proto, taskqueue::v1 as taskqueue},
    },
};
use prost::Message as _;
use prost_types::{Duration as ProtoDuration, Timestamp};
use time::OffsetDateTime;
use tokeira_types::{
    Headers, Memo, Payload as DomainPayload, Payloads as DomainPayloads, RunId, SearchAttrValue,
    SearchAttributes as DomainSearchAttributes, TaskQueueName, TaskToken, WorkflowId,
};

pub fn payload_from_domain(value: &DomainPayload) -> common::Payload {
    common::Payload {
        metadata: value
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
            .collect(),
        data: value.data.clone(),
        external_payloads: value
            .external_payloads
            .iter()
            .map(|detail| common::payload::ExternalPayloadDetails {
                size_bytes: detail.size_bytes,
            })
            .collect(),
    }
}

pub fn payload_to_domain(value: &common::Payload) -> DomainPayload {
    DomainPayload {
        metadata: value
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
            .collect(),
        data: value.data.clone(),
        external_payloads: value
            .external_payloads
            .iter()
            .map(|detail| tokeira_types::ExternalPayloadDetail {
                size_bytes: detail.size_bytes,
            })
            .collect(),
    }
}

pub fn failure_to_payload(value: &failure_proto::Failure) -> DomainPayload {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("encoding".to_string(), "temporal/failure+proto".to_string());
    DomainPayload {
        data: value.encode_to_vec(),
        metadata,
        external_payloads: Vec::new(),
    }
}

pub fn payload_to_failure(value: &DomainPayload) -> failure_proto::Failure {
    failure_proto::Failure::decode(value.data.as_slice()).unwrap_or_else(|_| {
        failure_proto::Failure {
            message: String::from_utf8_lossy(&value.data).into_owned(),
            ..Default::default()
        }
    })
}

pub fn payloads_from_domain(values: &DomainPayloads) -> common::Payloads {
    common::Payloads {
        payloads: values.0.iter().map(payload_from_domain).collect(),
    }
}

pub fn payloads_to_domain(values: &common::Payloads) -> DomainPayloads {
    DomainPayloads(values.payloads.iter().map(payload_to_domain).collect())
}

pub fn headers_from_domain(value: &Headers) -> common::Header {
    common::Header {
        fields: value
            .0
            .iter()
            .map(|(k, v)| (k.clone(), payload_from_domain(v)))
            .collect(),
    }
}

pub fn headers_to_domain(value: &common::Header) -> Headers {
    Headers(
        value
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), payload_to_domain(v)))
            .collect(),
    )
}

pub fn memo_from_domain(value: &Memo) -> common::Memo {
    common::Memo {
        fields: value
            .0
            .iter()
            .map(|(k, v)| (k.clone(), payload_from_domain(v)))
            .collect(),
    }
}

pub fn memo_to_domain(value: &common::Memo) -> Memo {
    Memo(
        value
            .fields
            .iter()
            .filter(|(_name, payload)| !is_temporal_nil_payload(payload))
            .map(|(k, v)| (k.clone(), payload_to_domain(v)))
            .collect(),
    )
}

pub fn search_attributes_from_domain(value: &DomainSearchAttributes) -> common::SearchAttributes {
    common::SearchAttributes {
        indexed_fields: value
            .0
            .iter()
            .map(|(name, val)| (name.clone(), search_attr_value_to_payload(val)))
            .collect(),
    }
}

pub fn search_attributes_to_domain(
    value: &common::SearchAttributes,
) -> Result<DomainSearchAttributes, ProtoConversionError> {
    Ok(DomainSearchAttributes(
        value
            .indexed_fields
            .iter()
            .map(|(name, val)| {
                // Internal-only predefined attributes are rejected before any
                // payload handling, with the verbatim v1.31.0 admission text
                // (`searchattribute/validator.go:118-120 @ v1.31.0`); the
                // whitelisted predefined set (BatcherUser etc.) stays settable.
                if tokeira_types::is_banned_predefined_search_attribute(name) {
                    return Err(ProtoConversionError::InvalidArgument(format!(
                        "{name} attribute can't be set in SearchAttributes"
                    )));
                }
                Ok((name, val))
            })
            .filter(
                |entry| !matches!(entry, Ok((_name, payload)) if is_temporal_nil_payload(payload)),
            )
            .map(|entry| {
                let (name, val) = entry?;
                Ok((name.clone(), search_attr_payload_to_domain(val)?))
            })
            .collect::<Result<_, ProtoConversionError>>()?,
    ))
}

/// Return whether a payload carries Temporal's memo/search-attribute deletion
/// sentinel (`json/plain` null or empty-list).
///
/// Starts filter these values; workflow-task upserts retain the distinction as
/// a per-key clear operation (`common/payload/payload.go @ v1.31.0`).
pub fn is_temporal_nil_payload(value: &common::Payload) -> bool {
    let encoding = value.metadata.get("encoding").map(Vec::as_slice);
    // Temporal filters JSON null and empty-list payloads from memo/search
    // attributes before writing start history, so client-side nil values do not
    // become validation errors (`common/payload/payload.go:94 @ v1.31.0`).
    matches!(encoding, Some(b"binary/null"))
        || (matches!(encoding, Some(b"json/plain"))
            && matches!(value.data.as_slice(), b"null" | b"[]"))
}

/// Encode a search-attribute value in the standard Temporal wire format: the
/// bare JSON value with `encoding=json/plain` plus a `type` metadata key naming
/// the `IndexedValueType` (`searchattribute/encode_value.go:14-22` +
/// `sadefs/util.go:22-33 @ v1.31.0`). Datetimes are RFC 3339 strings.
pub fn search_attr_value_to_payload(value: &SearchAttrValue) -> common::Payload {
    let (type_name, json) = match value {
        SearchAttrValue::Keyword(v) => ("Keyword", serde_json::json!(v)),
        SearchAttrValue::KeywordList(v) => ("KeywordList", serde_json::json!(v)),
        SearchAttrValue::Int(v) => ("Int", serde_json::json!(v)),
        SearchAttrValue::Double(v) => ("Double", serde_json::json!(v)),
        SearchAttrValue::Bool(v) => ("Bool", serde_json::json!(v)),
        SearchAttrValue::Datetime(v) => (
            "Datetime",
            serde_json::json!(
                v.format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            ),
        ),
        SearchAttrValue::Text(v) => ("Text", serde_json::json!(v)),
    };
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("encoding".to_string(), b"json/plain".to_vec());
    metadata.insert("type".to_string(), type_name.as_bytes().to_vec());
    common::Payload {
        metadata,
        data: serde_json::to_vec(&json).unwrap_or_default(),
        external_payloads: Vec::new(),
    }
}

/// Decode the standard Temporal search-attribute wire payload: bare JSON data
/// (`payload.EncodeString` writes `encoding=json/plain` + the JSON value with
/// NO type metadata, `common/payload/payload.go:19-23 @ v1.31.0`), with the
/// optional `type` metadata key naming the `IndexedValueType` when the writer
/// knew it (`searchattribute/decode_value.go` reads it). Without a `type`
/// key the variant is inferred from the JSON shape — strings map to `Keyword`
/// (Temporal's default for unregistered string attributes; `Text`/`Datetime`
/// require an explicit type).
pub fn search_attr_payload_to_domain(
    value: &common::Payload,
) -> Result<SearchAttrValue, ProtoConversionError> {
    let invalid = |_| ProtoConversionError::MissingField("SearchAttributes: invalid payload data");
    let json: serde_json::Value = serde_json::from_slice(&value.data).map_err(invalid)?;
    let type_name = value
        .metadata
        .get("type")
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned());

    let type_mismatch =
        || ProtoConversionError::MissingField("SearchAttributes: invalid payload data");
    match type_name.as_deref() {
        Some("Keyword") => Ok(SearchAttrValue::Keyword(
            json.as_str().ok_or_else(type_mismatch)?.to_string(),
        )),
        Some("Text") => Ok(SearchAttrValue::Text(
            json.as_str().ok_or_else(type_mismatch)?.to_string(),
        )),
        Some("Int") => Ok(SearchAttrValue::Int(
            json.as_i64().ok_or_else(type_mismatch)?,
        )),
        Some("Double") => Ok(SearchAttrValue::Double(
            json.as_f64().ok_or_else(type_mismatch)?,
        )),
        Some("Bool") => Ok(SearchAttrValue::Bool(
            json.as_bool().ok_or_else(type_mismatch)?,
        )),
        Some("Datetime") => {
            let text = json.as_str().ok_or_else(type_mismatch)?;
            let parsed =
                OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
                    .map_err(|_| type_mismatch())?;
            Ok(SearchAttrValue::Datetime(parsed))
        }
        Some("KeywordList") => keyword_list_from_json(&json).ok_or_else(type_mismatch),
        // No (or unrecognised) type metadata: infer from the JSON shape.
        _ => match &json {
            serde_json::Value::String(text) => Ok(SearchAttrValue::Keyword(text.clone())),
            serde_json::Value::Bool(flag) => Ok(SearchAttrValue::Bool(*flag)),
            serde_json::Value::Number(number) => Ok(number
                .as_i64()
                .map(SearchAttrValue::Int)
                .or_else(|| number.as_f64().map(SearchAttrValue::Double))
                .ok_or_else(type_mismatch)?),
            serde_json::Value::Array(_) => keyword_list_from_json(&json).ok_or_else(type_mismatch),
            _ => Err(type_mismatch()),
        },
    }
}

fn keyword_list_from_json(json: &serde_json::Value) -> Option<SearchAttrValue> {
    let items = json.as_array()?;
    let values = items
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()?;
    Some(SearchAttrValue::KeywordList(values))
}

pub fn workflow_execution_from_ids(
    workflow_id: &WorkflowId,
    run_id: RunId,
) -> common::WorkflowExecution {
    common::WorkflowExecution {
        workflow_id: workflow_id.0.clone(),
        run_id: run_id.0.to_string(),
    }
}

pub fn task_queue_from_domain(value: &TaskQueueName) -> taskqueue::TaskQueue {
    taskqueue::TaskQueue {
        name: value.0.clone(),
        ..Default::default()
    }
}

pub fn task_queue_to_domain(value: &taskqueue::TaskQueue) -> TaskQueueName {
    TaskQueueName(value.name.clone())
}

pub fn encode_task_token(value: &TaskToken) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

pub fn decode_task_token(value: &[u8]) -> Result<TaskToken, ProtoConversionError> {
    serde_json::from_slice(value)
        .map_err(|err| ProtoConversionError::InvalidTaskToken(err.to_string()))
}

pub fn to_proto_timestamp(value: OffsetDateTime) -> Timestamp {
    let nanos = value.nanosecond();
    Timestamp {
        seconds: value.unix_timestamp(),
        nanos: nanos as i32,
    }
}

pub fn to_opt_proto_timestamp(value: Option<OffsetDateTime>) -> Option<Timestamp> {
    value.map(to_proto_timestamp)
}

pub fn to_proto_duration(value: time::Duration) -> ProtoDuration {
    let seconds = value.whole_seconds();
    let nanos = (value - time::Duration::seconds(seconds)).whole_nanoseconds();
    ProtoDuration {
        seconds,
        nanos: nanos as i32,
    }
}

pub fn to_opt_proto_duration(value: Option<time::Duration>) -> Option<ProtoDuration> {
    value.map(to_proto_duration)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn json_payload(data: &[u8]) -> common::Payload {
        let mut metadata = BTreeMap::new();
        metadata.insert("encoding".to_string(), b"json/plain".to_vec());
        common::Payload {
            metadata,
            data: data.to_vec(),
            external_payloads: Vec::new(),
        }
    }

    #[test]
    fn memo_conversion_filters_temporal_nil_payloads() {
        let memo = common::Memo {
            fields: BTreeMap::from([
                ("nil".to_string(), json_payload(b"null")),
                ("empty".to_string(), json_payload(b"[]")),
                ("kept".to_string(), json_payload(br#""value""#)),
            ]),
        };

        let converted = memo_to_domain(&memo);

        assert!(!converted.0.contains_key("nil"));
        assert!(!converted.0.contains_key("empty"));
        assert!(converted.0.contains_key("kept"));
    }

    // Feature: search-attribute wire codec — the BatcherUser corpus case:
    // payload.EncodeString("1.0.0") = {encoding=json/plain, data=b"\"1.0.0\"",
    // NO type metadata} must decode as Keyword (payload.go:19-23 @ v1.31.0).
    #[test]
    fn search_attribute_decodes_standard_wire_string() {
        let decoded = search_attr_payload_to_domain(&json_payload(br#""1.0.0""#))
            .expect("bare JSON string decodes");
        assert_eq!(decoded, SearchAttrValue::Keyword("1.0.0".to_string()));
    }

    #[test]
    fn search_attribute_round_trips_every_variant() {
        let values = vec![
            SearchAttrValue::Keyword("k".to_string()),
            SearchAttrValue::KeywordList(vec!["a".to_string(), "b".to_string()]),
            SearchAttrValue::Int(42),
            SearchAttrValue::Double(2.5),
            SearchAttrValue::Bool(true),
            SearchAttrValue::Datetime(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
            SearchAttrValue::Text("full text".to_string()),
        ];
        for value in values {
            let payload = search_attr_value_to_payload(&value);
            assert_eq!(
                payload.metadata.get("encoding").map(Vec::as_slice),
                Some(b"json/plain".as_slice())
            );
            assert!(payload.metadata.contains_key("type"));
            let decoded = search_attr_payload_to_domain(&payload).expect("round trip");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn search_attribute_rejects_banned_predefined_names() {
        let attributes = common::SearchAttributes {
            indexed_fields: BTreeMap::from([(
                "TemporalWorkerDeploymentVersion".to_string(),
                json_payload(br#""1.0.0""#),
            )]),
        };
        let error = search_attributes_to_domain(&attributes).unwrap_err();
        assert_eq!(
            error.to_string(),
            "TemporalWorkerDeploymentVersion attribute can't be set in SearchAttributes"
        );
    }

    #[test]
    fn search_attribute_allows_whitelisted_predefined_names() {
        let attributes = common::SearchAttributes {
            indexed_fields: BTreeMap::from([(
                "BatcherUser".to_string(),
                json_payload(br#""1.0.0""#),
            )]),
        };
        let converted = search_attributes_to_domain(&attributes).expect("whitelisted");
        assert_eq!(
            converted.0.get("BatcherUser"),
            Some(&SearchAttrValue::Keyword("1.0.0".to_string()))
        );
    }

    #[test]
    fn search_attribute_conversion_filters_temporal_nil_payloads() {
        let attributes = common::SearchAttributes {
            indexed_fields: BTreeMap::from([
                ("nil".to_string(), json_payload(b"null")),
                ("empty".to_string(), json_payload(b"[]")),
                (
                    "kept".to_string(),
                    search_attr_value_to_payload(&SearchAttrValue::Keyword("value".to_string())),
                ),
            ]),
        };

        let converted = search_attributes_to_domain(&attributes).expect("search attributes");

        assert!(!converted.0.contains_key("nil"));
        assert!(!converted.0.contains_key("empty"));
        assert!(converted.0.contains_key("kept"));
    }
}
