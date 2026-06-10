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
        external_payloads: Vec::new(),
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
    }
}

pub fn failure_to_payload(value: &failure_proto::Failure) -> DomainPayload {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("encoding".to_string(), "temporal/failure+proto".to_string());
    DomainPayload {
        data: value.encode_to_vec(),
        metadata,
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
            .filter(|(_name, payload)| !is_temporal_nil_payload(payload))
            .map(|(name, val)| Ok((name.clone(), search_attr_payload_to_domain(val)?)))
            .collect::<Result<_, ProtoConversionError>>()?,
    ))
}

fn is_temporal_nil_payload(value: &common::Payload) -> bool {
    let encoding = value.metadata.get("encoding").map(Vec::as_slice);
    // Temporal filters JSON null and empty-list payloads from memo/search
    // attributes before writing start history, so client-side nil values do not
    // become validation errors (`common/payload/payload.go:94 @ v1.31.0`).
    matches!(encoding, Some(b"json/plain")) && matches!(value.data.as_slice(), b"null" | b"[]")
}

fn search_attr_value_to_payload(value: &SearchAttrValue) -> common::Payload {
    let data = serde_json::to_vec(value).unwrap_or_default();
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("encoding".to_string(), b"json/plain".to_vec());
    common::Payload {
        metadata,
        data,
        external_payloads: Vec::new(),
    }
}

fn search_attr_payload_to_domain(
    value: &common::Payload,
) -> Result<SearchAttrValue, ProtoConversionError> {
    serde_json::from_slice(&value.data).map_err(|_err| {
        ProtoConversionError::MissingField("SearchAttributes: invalid payload data")
    })
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
