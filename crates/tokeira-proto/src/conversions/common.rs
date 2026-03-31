//! Small shared conversions for public protobuf common types.

use crate::conversions::ProtoConversionError;
use crate::public::common;
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
            .map(|(k, v)| (k.clone(), payload_to_domain(v)))
            .collect(),
    )
}

pub fn search_attributes_from_domain(value: &DomainSearchAttributes) -> common::SearchAttributes {
    common::SearchAttributes {
        indexed_fields: value
            .0
            .iter()
            .map(|(name, val)| (name.clone(), search_attr_value_from_domain(val)))
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
            .map(|(name, val)| Ok((name.clone(), search_attr_value_to_domain(val)?)))
            .collect::<Result<_, ProtoConversionError>>()?,
    ))
}

pub fn search_attr_value_from_domain(value: &SearchAttrValue) -> common::SearchAttributeValue {
    use common::search_attribute_value::Kind;

    let kind = match value {
        SearchAttrValue::Keyword(v) => Kind::Keyword(v.clone()),
        SearchAttrValue::KeywordList(v) => Kind::KeywordList(common::KeywordList {
            values: v.clone(),
        }),
        SearchAttrValue::Int(v) => Kind::IntValue(*v),
        SearchAttrValue::Bool(v) => Kind::BoolValue(*v),
        SearchAttrValue::Double(v) => Kind::DoubleValue(*v),
        SearchAttrValue::Datetime(v) => Kind::DatetimeUnixNanos(to_unix_nanos(*v)),
        SearchAttrValue::Text(v) => Kind::Text(v.clone()),
    };

    common::SearchAttributeValue { kind: Some(kind) }
}

pub fn search_attr_value_to_domain(
    value: &common::SearchAttributeValue,
) -> Result<SearchAttrValue, ProtoConversionError> {
    use common::search_attribute_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::Keyword(v)) => Ok(SearchAttrValue::Keyword(v.clone())),
        Some(Kind::KeywordList(v)) => Ok(SearchAttrValue::KeywordList(v.values.clone())),
        Some(Kind::IntValue(v)) => Ok(SearchAttrValue::Int(*v)),
        Some(Kind::BoolValue(v)) => Ok(SearchAttrValue::Bool(*v)),
        Some(Kind::DoubleValue(v)) => Ok(SearchAttrValue::Double(*v)),
        Some(Kind::DatetimeUnixNanos(v)) => from_unix_nanos(*v).map(SearchAttrValue::Datetime),
        Some(Kind::Text(v)) => Ok(SearchAttrValue::Text(v.clone())),
        None => Err(ProtoConversionError::MissingField("SearchAttributeValue.kind")),
    }
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

pub fn task_queue_from_domain(value: &TaskQueueName) -> common::TaskQueue {
    common::TaskQueue {
        name: value.0.clone(),
    }
}

pub fn task_queue_to_domain(value: &common::TaskQueue) -> TaskQueueName {
    TaskQueueName(value.name.clone())
}

pub fn encode_task_token(value: &TaskToken) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

pub fn decode_task_token(value: &[u8]) -> Result<TaskToken, ProtoConversionError> {
    serde_json::from_slice(value)
        .map_err(|err| ProtoConversionError::InvalidTaskToken(err.to_string()))
}

fn to_unix_nanos(value: OffsetDateTime) -> i64 {
    let nanos = value.unix_timestamp_nanos();
    if nanos > i64::MAX as i128 {
        i64::MAX
    } else if nanos < i64::MIN as i128 {
        i64::MIN
    } else {
        nanos as i64
    }
}

fn from_unix_nanos(value: i64) -> Result<OffsetDateTime, ProtoConversionError> {
    OffsetDateTime::from_unix_timestamp_nanos(value as i128)
        .map_err(|err| ProtoConversionError::InvalidTimestamp(err.to_string()))
}
