//! Temporal v1.31.0's limits on what a request may carry, checked when the
//! request arrives (`payload-admission-limits`).
//!
//! A client call over a limit fails with `InvalidArgument`; a worker response
//! over a limit is recorded as a non-retryable server failure instead. Sizes
//! are protobuf-encoded sizes, `prost::Message::encoded_len`, which is what Go's
//! `Size()` returns (`common/util.go:578-608 @ v1.31.0`).
//!
//! The limits are v1.31.0's values and Tokeira offers no setting to change
//! them. Only the Temporal functional harness's build can override the
//! accessors below, so that its tests that shrink a limit can run
//! (`conformance-config-override`).

use prost::Message as _;
// v1.31.0's values, defined once in the kernel crate, which the runtime also
// hands to the kernel for a workflow task's commands.
use tokeira_kernel::limits::{
    BLOB_SIZE_LIMIT_ERROR, BLOB_SIZE_LIMIT_WARN, MEMO_SIZE_LIMIT_ERROR, MEMO_SIZE_LIMIT_WARN,
    SEARCH_ATTRIBUTES_NUMBER_OF_KEYS_LIMIT, SEARCH_ATTRIBUTES_SIZE_OF_VALUE_LIMIT,
    SEARCH_ATTRIBUTES_TOTAL_SIZE_LIMIT,
};
use tokeira_proto::{
    conversions::ProtoConversionError,
    public::temporal::api::{common::v1 as proto_common, failure::v1 as failure_proto},
};
use tracing::warn;

/// `common.ErrBlobSizeExceedsLimit` (`common/util.go:118 @ v1.31.0`).
pub(crate) const BLOB_SIZE_EXCEEDS_LIMIT: &str = "Blob data size exceeds limit.";
/// `common.ErrMemoSizeExceedsLimit` (`common/util.go:120 @ v1.31.0`).
pub(crate) const MEMO_SIZE_EXCEEDS_LIMIT: &str = "Memo size exceeds limit.";
/// `common.FailureReasonCompleteResultExceedsLimit` (`common/util.go:97 @ v1.31.0`).
pub(crate) const COMPLETE_RESULT_EXCEEDS_LIMIT: &str = "Complete result exceeds size limit.";
/// `common.FailureReasonFailureExceedsLimit` (`common/util.go:99 @ v1.31.0`).
pub(crate) const FAILURE_EXCEEDS_LIMIT: &str = "Failure exceeds size limit.";
/// `common.FailureReasonCancelDetailsExceedsLimit` (`common/util.go:101 @ v1.31.0`).
pub(crate) const CANCEL_DETAILS_EXCEED_LIMIT: &str = "Cancel details exceed size limit.";
/// `common.FailureReasonHeartbeatExceedsLimit` (`common/util.go:103 @ v1.31.0`).
pub(crate) const HEARTBEAT_DETAILS_EXCEED_LIMIT: &str = "Heartbeat details exceed size limit.";

/// The depth `failure.Truncate` follows causes to
/// (`common/failure/failure.go:48-50 @ v1.31.0`).
const FAILURE_TRUNCATION_DEPTH: u32 = 20;

#[cfg(feature = "conformance")]
fn harness_override(key: &str, default: usize) -> usize {
    crate::conformance::overrides::reads()
        .get_i64(key)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default)
}

macro_rules! limit_accessor {
    ($name:ident, $default:ident, $key:literal) => {
        #[cfg(not(feature = "conformance"))]
        pub(crate) fn $name() -> usize {
            $default
        }

        #[cfg(feature = "conformance")]
        pub(crate) fn $name() -> usize {
            harness_override($key, $default)
        }
    };
}

limit_accessor!(
    blob_size_limit_error,
    BLOB_SIZE_LIMIT_ERROR,
    "limit.blobSize.error"
);
limit_accessor!(
    blob_size_limit_warn,
    BLOB_SIZE_LIMIT_WARN,
    "limit.blobSize.warn"
);
limit_accessor!(
    memo_size_limit_error,
    MEMO_SIZE_LIMIT_ERROR,
    "limit.memoSize.error"
);
limit_accessor!(
    memo_size_limit_warn,
    MEMO_SIZE_LIMIT_WARN,
    "limit.memoSize.warn"
);
limit_accessor!(
    search_attributes_number_of_keys_limit,
    SEARCH_ATTRIBUTES_NUMBER_OF_KEYS_LIMIT,
    "frontend.searchAttributesNumberOfKeysLimit"
);
limit_accessor!(
    search_attributes_size_of_value_limit,
    SEARCH_ATTRIBUTES_SIZE_OF_VALUE_LIMIT,
    "frontend.searchAttributesSizeOfValueLimit"
);
limit_accessor!(
    search_attributes_total_size_limit,
    SEARCH_ATTRIBUTES_TOTAL_SIZE_LIMIT,
    "frontend.searchAttributesTotalSizeLimit"
);

/// Whether `size` exceeds a limit: only when it is greater
/// (`common.CheckEventBlobSizeLimit`, `common/util.go:578-608 @ v1.31.0`).
/// Above the warn limit it logs, as v1.31.0 does.
fn exceeds(size: usize, warn_limit: usize, error_limit: usize, operation: &str) -> bool {
    if size > warn_limit {
        warn!(
            operation,
            size, warn_limit, "Blob data size exceeds the warning limit."
        );
        size > error_limit
    } else {
        false
    }
}

/// Whether a payload field of `size` bytes exceeds the blob size limit.
pub(crate) fn blob_exceeds_limit(size: usize, operation: &str) -> bool {
    exceeds(
        size,
        blob_size_limit_warn(),
        blob_size_limit_error(),
        operation,
    )
}

/// Whether a standalone activity's payload or reason of `size` bytes exceeds
/// the blob size limit. The standalone validator compares the error limit on
/// its own, where `CheckEventBlobSizeLimit` errors only above the warn limit
/// too; the two agree unless the error limit is set below the warn limit
/// (`chasm/lib/activity/validator.go:222-247 @ v1.31.0`).
pub(crate) fn standalone_blob_exceeds_limit(size: usize, operation: &str) -> bool {
    let warn_limit = blob_size_limit_warn();
    if size > warn_limit {
        warn!(
            operation,
            size, warn_limit, "Activity blob size exceeds the warning limit."
        );
    }
    size > blob_size_limit_error()
}

/// Whether a memo of `size` bytes exceeds the memo size limit.
pub(crate) fn memo_exceeds_limit(size: usize, operation: &str) -> bool {
    exceeds(
        size,
        memo_size_limit_warn(),
        memo_size_limit_error(),
        operation,
    )
}

/// Logs a workflow task command's payload above the blob warn limit, as
/// v1.31.0 does; the kernel decides whether it is over the limit
/// (`workflow-task-command-limits` criterion 2.13).
pub(crate) fn note_blob_size(size: usize, operation: &str) {
    let _ = blob_exceeds_limit(size, operation);
}

/// Logs a workflow task command's memo above the memo warn limit.
pub(crate) fn note_memo_size(size: usize, operation: &str) {
    let _ = memo_exceeds_limit(size, operation);
}

/// The encoded size of an optional message; an absent field has size 0, as
/// Go's `Size()` on a nil message does.
pub(crate) fn encoded_size<M: prost::Message>(message: Option<&M>) -> usize {
    message.map_or(0, prost::Message::encoded_len)
}

/// The key count limit (`validator.Validate`,
/// `common/searchattribute/validator.go:60-75 @ v1.31.0`).
pub(crate) fn check_search_attribute_count(
    search_attributes: Option<&proto_common::SearchAttributes>,
) -> Result<(), String> {
    let count = search_attributes.map_or(0, |attributes| attributes.indexed_fields.len());
    let limit = search_attributes_number_of_keys_limit();
    if count > limit {
        return Err(format!(
            "number of search attributes {count} exceeds limit {limit}"
        ));
    }
    Ok(())
}

/// Each value's data length, then the map's encoded size
/// (`validator.ValidateSize`, `common/searchattribute/validator.go:145-175 @
/// v1.31.0`). The map iterates in key order, so the error names the same
/// attribute on every call.
pub(crate) fn check_search_attribute_sizes(
    search_attributes: Option<&proto_common::SearchAttributes>,
) -> Result<(), String> {
    let Some(attributes) = search_attributes else {
        return Ok(());
    };
    let value_limit = search_attributes_size_of_value_limit();
    for (name, value) in &attributes.indexed_fields {
        let size = value.data.len();
        if size > value_limit {
            return Err(format!(
                "search attribute {name} value size {size} exceeds size limit {value_limit}"
            ));
        }
    }
    let total = attributes.encoded_len();
    let total_limit = search_attributes_total_size_limit();
    if total > total_limit {
        return Err(format!(
            "total size of search attributes {total} exceeds size limit {total_limit}"
        ));
    }
    Ok(())
}

/// A start's search attributes, input and memo, in v1.31.0's order: the
/// frontend's search attribute validation, then history's input and memo
/// checks (`service/frontend/workflow_handler.go:6148-6153`;
/// `service/history/api/create_workflow_util.go:230-256 @ v1.31.0`).
pub(crate) fn check_start_payloads(
    search_attributes: Option<&proto_common::SearchAttributes>,
    input: Option<&proto_common::Payloads>,
    memo: Option<&proto_common::Memo>,
    operation: &str,
) -> Result<(), ProtoConversionError> {
    check_search_attribute_count(search_attributes)
        .map_err(ProtoConversionError::InvalidArgument)?;
    check_search_attribute_sizes(search_attributes)
        .map_err(ProtoConversionError::InvalidArgument)?;
    check_blob(encoded_size(input), operation)?;
    if memo_exceeds_limit(encoded_size(memo), operation) {
        return Err(ProtoConversionError::InvalidArgument(
            MEMO_SIZE_EXCEEDS_LIMIT.to_owned(),
        ));
    }
    Ok(())
}

/// A payload field against the blob size limit, as a client call's error.
pub(crate) fn check_blob(size: usize, operation: &str) -> Result<(), ProtoConversionError> {
    if blob_exceeds_limit(size, operation) {
        return Err(ProtoConversionError::InvalidArgument(
            BLOB_SIZE_EXCEEDS_LIMIT.to_owned(),
        ));
    }
    Ok(())
}

/// RespondActivityTaskFailed's limits, by task token or by id: oversized last
/// heartbeat details are dropped, then an oversized failure is replaced. The
/// server failures for both are returned, for the response to list
/// (`service/frontend/workflow_handler.go:1804-1838, 1927-1962 @ v1.31.0`).
pub(crate) fn limit_activity_failure(
    last_heartbeat_details: &mut Option<proto_common::Payloads>,
    failure: &mut Option<failure_proto::Failure>,
    operation: &str,
) -> Vec<failure_proto::Failure> {
    let mut failures = Vec::new();
    if last_heartbeat_details.is_some()
        && blob_exceeds_limit(encoded_size(last_heartbeat_details.as_ref()), operation)
    {
        failures.push(server_failure(HEARTBEAT_DETAILS_EXCEED_LIMIT));
        *last_heartbeat_details = None;
    }
    if let Some(replacement) = oversized_replacement(failure.as_ref(), operation) {
        failures.push(replacement.clone());
        *failure = Some(replacement);
    }
    failures
}

/// What to record in place of `failure` when it is over the blob size limit.
pub(crate) fn oversized_replacement(
    failure: Option<&failure_proto::Failure>,
    operation: &str,
) -> Option<failure_proto::Failure> {
    failure
        .filter(|failure| blob_exceeds_limit(failure.encoded_len(), operation))
        .map(oversized_failure)
}

/// A non-retryable server failure (`failure.NewServerFailure`,
/// `common/failure/failure.go:14-23 @ v1.31.0`).
pub(crate) fn server_failure(message: &str) -> failure_proto::Failure {
    failure_proto::Failure {
        message: message.to_owned(),
        failure_info: Some(failure_proto::failure::FailureInfo::ServerFailureInfo(
            failure_proto::ServerFailureInfo {
                non_retryable: true,
            },
        )),
        ..Default::default()
    }
}

/// What v1.31.0 records in place of a failure over the blob size limit: a
/// server failure whose cause is the original truncated to the warn limit
/// (`service/frontend/workflow_handler.go:1231-1245, 1824-1838 @ v1.31.0`).
pub(crate) fn oversized_failure(original: &failure_proto::Failure) -> failure_proto::Failure {
    let mut failure = server_failure(FAILURE_EXCEEDS_LIMIT);
    let max_size = i64::try_from(blob_size_limit_warn()).unwrap_or(i64::MAX);
    failure.cause = Some(Box::new(truncate_failure(original, max_size)));
    failure
}

/// A port of `failure.TruncateWithDepth` with a depth of 20
/// (`common/failure/failure.go:48-95 @ v1.31.0`).
pub(crate) fn truncate_failure(
    failure: &failure_proto::Failure,
    max_size: i64,
) -> failure_proto::Failure {
    truncate_failure_with_depth(failure, max_size, FAILURE_TRUNCATION_DEPTH)
}

/// One field's share of the budget: bytes go to earlier calls first, so fields
/// are cut in order of importance, each charged 4 bytes of proto overhead
/// when non-empty.
fn truncate_field(text: &str, max_size: &mut i64) -> String {
    let kept = truncate_utf8(text, *max_size).to_owned();
    *max_size -= i64::try_from(kept.len()).unwrap_or(i64::MAX);
    if !kept.is_empty() {
        *max_size -= 4;
    }
    kept
}

fn truncate_failure_with_depth(
    failure: &failure_proto::Failure,
    mut max_size: i64,
    max_depth: u32,
) -> failure_proto::Failure {
    let mut truncated = failure_proto::Failure::default();
    // Application and server failure info keep their non-retryable flag.
    match &failure.failure_info {
        Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(info)) => {
            let r#type = truncate_field(&info.r#type, &mut max_size);
            truncated.failure_info =
                Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                    failure_proto::ApplicationFailureInfo {
                        non_retryable: info.non_retryable,
                        r#type,
                        ..Default::default()
                    },
                ));
            max_size -= 8;
        }
        Some(failure_proto::failure::FailureInfo::ServerFailureInfo(info)) => {
            truncated.failure_info = Some(failure_proto::failure::FailureInfo::ServerFailureInfo(
                failure_proto::ServerFailureInfo {
                    non_retryable: info.non_retryable,
                },
            ));
            max_size -= 4;
        }
        _ => {}
    }
    truncated.source = truncate_field(&failure.source, &mut max_size);
    truncated.message = truncate_field(&failure.message, &mut max_size);
    truncated.stack_trace = truncate_field(&failure.stack_trace, &mut max_size);
    if let Some(cause) = &failure.cause
        && max_size > 4
        && max_depth > 0
    {
        truncated.cause = Some(Box::new(truncate_failure_with_depth(
            cause,
            max_size - 4,
            max_depth - 1,
        )));
    }
    truncated
}

/// `util.TruncateUTF8`: at most `max_bytes` bytes, cut back to a character
/// boundary (`common/util/strings.go:9-19 @ v1.31.0`).
fn truncate_utf8(text: &str, max_bytes: i64) -> &str {
    let Ok(max_bytes) = usize::try_from(max_bytes) else {
        return "";
    };
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    use super::*;
    use crate::grpc::translate;
    use tokeira_proto::workflowservice;

    fn payload(data_len: usize) -> proto_common::Payload {
        proto_common::Payload {
            metadata: BTreeMap::from([("encoding".to_owned(), b"binary/plain".to_vec())]),
            data: vec![b'x'; data_len],
            ..Default::default()
        }
    }

    /// The message `build` makes from a data length, at the data length whose
    /// encoding is exactly `size` bytes.
    fn of_size<M: prost::Message>(size: usize, build: impl Fn(usize) -> M) -> M {
        let overhead = build(0).encoded_len();
        assert!(
            size >= overhead,
            "the message needs at least {overhead} bytes"
        );
        // Length prefixes grow at varint boundaries, so step towards the size
        // and refuse a size the encoding skips rather than oscillate.
        let mut data_len = size - overhead;
        let mut stepped_down = false;
        loop {
            let candidate = build(data_len);
            match candidate.encoded_len().cmp(&size) {
                std::cmp::Ordering::Equal => return candidate,
                std::cmp::Ordering::Greater => {
                    data_len -= 1;
                    stepped_down = true;
                }
                std::cmp::Ordering::Less => {
                    assert!(!stepped_down, "no message encodes to {size} bytes");
                    data_len += 1;
                }
            }
        }
    }

    /// One payload whose `Payloads` message encodes to exactly `size` bytes.
    fn payloads_of_size(size: usize) -> proto_common::Payloads {
        of_size(size, |data_len| proto_common::Payloads {
            payloads: vec![payload(data_len)],
        })
    }

    /// A one-field memo that encodes to exactly `size` bytes.
    fn memo_of_size(size: usize) -> proto_common::Memo {
        of_size(size, |data_len| proto_common::Memo {
            fields: BTreeMap::from([("m".to_owned(), payload(data_len))]),
        })
    }

    fn search_attributes(values: &[(String, usize)]) -> proto_common::SearchAttributes {
        proto_common::SearchAttributes {
            indexed_fields: values
                .iter()
                .map(|(name, len)| (name.clone(), payload(*len)))
                .collect(),
        }
    }

    // Feature: payload-admission-limits, Property 1: The limits match v1.31.0's
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn property_limits_match_v1_31(
            size in prop_oneof![
                0usize..4 * 1024,
                (512usize * 1024 - 4)..(512 * 1024 + 4),
                (2usize * 1024 * 1024 - 4)..(2 * 1024 * 1024 + 4),
            ],
            values in prop::collection::vec(("[a-z]{1,12}", 0usize..3 * 1024), 0..120),
        ) {
            // `CheckEventBlobSizeLimit` with v1.31.0's defaults: greater than 2 MiB.
            prop_assert_eq!(blob_exceeds_limit(size, "test"), size > 2 * 1024 * 1024);
            prop_assert_eq!(memo_exceeds_limit(size, "test"), size > 2 * 1024 * 1024);
            prop_assert_eq!(standalone_blob_exceeds_limit(size, "test"), size > 2 * 1024 * 1024);

            // The search attribute checks measure what v1.31.0 measures.
            let mut deduped: Vec<(String, usize)> = Vec::new();
            for (name, len) in values {
                if !deduped.iter().any(|(existing, _)| *existing == name) {
                    deduped.push((name, len));
                }
            }
            let attributes = search_attributes(&deduped);
            let count = check_search_attribute_count(Some(&attributes));
            prop_assert_eq!(count.is_err(), deduped.len() > 100);
            let sizes = check_search_attribute_sizes(Some(&attributes));
            let oversized_value = deduped.iter().any(|(_, len)| *len > 2 * 1024);
            let oversized_total = attributes.encoded_len() > 40 * 1024;
            prop_assert_eq!(sizes.is_err(), oversized_value || oversized_total);
            if oversized_value {
                prop_assert!(sizes.unwrap_err().contains("value size"));
            }
        }
    }

    #[test]
    fn encoded_size_is_the_proto_size_and_absent_is_zero() {
        let payloads = payloads_of_size(1000);
        assert_eq!(encoded_size(Some(&payloads)), 1000);
        assert_eq!(encoded_size::<proto_common::Payloads>(None), 0);
    }

    #[test]
    fn search_attribute_messages_match_v1_31() {
        let many: Vec<(String, usize)> = (0..101).map(|i| (format!("Key{i}"), 1)).collect();
        assert_eq!(
            check_search_attribute_count(Some(&search_attributes(&many))).unwrap_err(),
            "number of search attributes 101 exceeds limit 100"
        );
        let large = vec![("CustomerId".to_owned(), 2049)];
        assert_eq!(
            check_search_attribute_sizes(Some(&search_attributes(&large))).unwrap_err(),
            "search attribute CustomerId value size 2049 exceeds size limit 2048"
        );
        let wide: Vec<(String, usize)> = (0..30).map(|i| (format!("Key{i:02}"), 2000)).collect();
        let total = search_attributes(&wide).encoded_len();
        assert_eq!(
            check_search_attribute_sizes(Some(&search_attributes(&wide))).unwrap_err(),
            format!("total size of search attributes {total} exceeds size limit 40960")
        );
    }

    /// A transcription of `failure.TruncateWithDepth`, written apart from the
    /// port above so a slip in one shows as a disagreement
    /// (`common/failure/failure.go:52-95 @ v1.31.0`).
    fn reference_truncate(
        f: &failure_proto::Failure,
        max_size: i64,
        max_depth: i64,
    ) -> failure_proto::Failure {
        fn cut(s: &str, n: i64) -> String {
            if (s.len() as i64) <= n {
                return s.to_owned();
            }
            if n <= 0 {
                return String::new();
            }
            let mut end = n as usize;
            while end > 0 && (s.as_bytes()[end] & 0xC0) == 0x80 {
                end -= 1;
            }
            s[..end].to_owned()
        }
        let mut budget = max_size;
        let take = |s: &str, budget: &mut i64| {
            let kept = cut(s, *budget);
            *budget -= kept.len() as i64;
            if !kept.is_empty() {
                *budget -= 4;
            }
            kept
        };
        let mut out = failure_proto::Failure::default();
        if let Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(info)) =
            &f.failure_info
        {
            let kept_type = take(&info.r#type, &mut budget);
            out.failure_info = Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    non_retryable: info.non_retryable,
                    r#type: kept_type,
                    ..Default::default()
                },
            ));
            budget -= 8;
        } else if let Some(failure_proto::failure::FailureInfo::ServerFailureInfo(info)) =
            &f.failure_info
        {
            out.failure_info = Some(failure_proto::failure::FailureInfo::ServerFailureInfo(
                failure_proto::ServerFailureInfo {
                    non_retryable: info.non_retryable,
                },
            ));
            budget -= 4;
        }
        out.source = take(&f.source, &mut budget);
        out.message = take(&f.message, &mut budget);
        out.stack_trace = take(&f.stack_trace, &mut budget);
        if let Some(cause) = &f.cause
            && budget > 4
            && max_depth > 0
        {
            out.cause = Some(Box::new(reference_truncate(
                cause,
                budget - 4,
                max_depth - 1,
            )));
        }
        out
    }

    fn arb_failure() -> impl Strategy<Value = failure_proto::Failure> {
        let text = "[a-zé€😀 ]{0,40}";
        let info = prop_oneof![
            Just(None),
            (any::<bool>(), "[A-Za-z]{0,12}").prop_map(|(non_retryable, r#type)| Some(
                failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                    failure_proto::ApplicationFailureInfo {
                        non_retryable,
                        r#type,
                        ..Default::default()
                    }
                )
            )),
            any::<bool>().prop_map(|non_retryable| Some(
                failure_proto::failure::FailureInfo::ServerFailureInfo(
                    failure_proto::ServerFailureInfo { non_retryable }
                )
            )),
            Just(Some(
                failure_proto::failure::FailureInfo::CanceledFailureInfo(
                    failure_proto::CanceledFailureInfo::default()
                )
            )),
        ];
        let leaf = (text, text, text, info.clone()).prop_map(
            |(source, message, stack_trace, failure_info)| failure_proto::Failure {
                source,
                message,
                stack_trace,
                failure_info,
                ..Default::default()
            },
        );
        leaf.prop_recursive(4, 8, 1, move |inner| {
            (text, text, text, info.clone(), inner).prop_map(
                |(source, message, stack_trace, failure_info, cause)| failure_proto::Failure {
                    source,
                    message,
                    stack_trace,
                    failure_info,
                    cause: Some(Box::new(cause)),
                    ..Default::default()
                },
            )
        })
    }

    // Feature: payload-admission-limits, Property 2: Truncation matches v1.31.0's
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn property_truncation_matches_v1_31(failure in arb_failure(), max_size in -8i64..400) {
            prop_assert_eq!(
                truncate_failure(&failure, max_size),
                reference_truncate(&failure, max_size, 20)
            );
        }
    }

    #[test]
    fn truncation_cuts_at_character_boundaries_and_follows_causes_twenty_deep() {
        let accented = failure_proto::Failure {
            message: "héllo".to_owned(),
            ..Default::default()
        };
        // Two bytes would split the é, so only the h fits.
        assert_eq!(truncate_failure(&accented, 2).message, "h");

        let mut chain = failure_proto::Failure {
            message: "level-25".to_owned(),
            ..Default::default()
        };
        for level in (0..25).rev() {
            chain = failure_proto::Failure {
                message: format!("level-{level}"),
                cause: Some(Box::new(chain)),
                ..Default::default()
            };
        }
        let truncated = truncate_failure(&chain, 1_000_000);
        let mut depth = 0;
        let mut node = &truncated;
        while let Some(cause) = &node.cause {
            depth += 1;
            node = cause;
        }
        assert_eq!(depth, 20, "the root and twenty causes are kept");
    }

    #[test]
    fn an_oversized_failure_becomes_a_server_failure_with_a_truncated_cause() {
        let original = failure_proto::Failure {
            message: "x".repeat(3 * 1024 * 1024),
            failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: "Boom".to_owned(),
                    non_retryable: false,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let mut failure = Some(original.clone());
        let mut details = Some(payloads_of_size(2 * 1024 * 1024 + 1));
        let failures = limit_activity_failure(&mut details, &mut failure, "test");
        assert!(details.is_none(), "oversized heartbeat details are dropped");
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0], server_failure(HEARTBEAT_DETAILS_EXCEED_LIMIT));
        let recorded = failure.expect("a failure is recorded");
        assert_eq!(failures[1], recorded);
        assert_eq!(recorded.message, FAILURE_EXCEEDS_LIMIT);
        assert!(matches!(
            recorded.failure_info,
            Some(failure_proto::failure::FailureInfo::ServerFailureInfo(
                failure_proto::ServerFailureInfo {
                    non_retryable: true
                }
            ))
        ));
        let cause = recorded.cause.expect("the original is kept as the cause");
        assert_eq!(*cause, truncate_failure(&original, 512 * 1024));
        assert!(cause.encoded_len() <= 512 * 1024);

        // Within the limits nothing changes.
        let mut small = Some(original.clone());
        small.as_mut().unwrap().message = "small".to_owned();
        let mut small_details = Some(payloads_of_size(2 * 1024 * 1024));
        assert!(limit_activity_failure(&mut small_details, &mut small, "test").is_empty());
        assert!(small_details.is_some());
        assert_eq!(small.unwrap().message, "small");
    }

    #[test]
    fn client_calls_over_a_limit_are_refused_with_v1_31_messages() {
        let over = payloads_of_size(2 * 1024 * 1024 + 1);
        let at = payloads_of_size(2 * 1024 * 1024);
        let start =
            |input: proto_common::Payloads| workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".to_owned(),
                workflow_id: "wf".to_owned(),
                workflow_type: Some(proto_common::WorkflowType {
                    name: "type".to_owned(),
                }),
                task_queue: Some(
                    tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: "queue".to_owned(),
                        ..Default::default()
                    },
                ),
                input: Some(input),
                ..Default::default()
            };
        let error = translate::start_request_to_edge(start(over.clone())).unwrap_err();
        assert_eq!(error.to_string(), BLOB_SIZE_EXCEEDS_LIMIT);
        assert!(translate::start_request_to_edge(start(at.clone())).is_ok());

        let mut memo_start = start(payloads_of_size(1000));
        memo_start.memo = Some(memo_of_size(2 * 1024 * 1024 + 1));
        assert_eq!(
            translate::start_request_to_edge(memo_start.clone())
                .unwrap_err()
                .to_string(),
            MEMO_SIZE_EXCEEDS_LIMIT
        );
        memo_start.memo = Some(memo_of_size(2 * 1024 * 1024));
        assert!(translate::start_request_to_edge(memo_start).is_ok());

        // The start in ExecuteMultiOperation is refused for that operation.
        let multi_operation = |start: workflowservice::StartWorkflowExecutionRequest| {
            use tokeira_proto::public::temporal::api::update::v1 as update;
            use workflowservice::execute_multi_operation_request::{Operation, operation};
            workflowservice::ExecuteMultiOperationRequest {
                namespace: "default".to_owned(),
                operations: vec![
                    Operation {
                        operation: Some(operation::Operation::StartWorkflow(start)),
                    },
                    Operation {
                        operation: Some(operation::Operation::UpdateWorkflow(
                            workflowservice::UpdateWorkflowExecutionRequest {
                                namespace: "default".to_owned(),
                                workflow_execution: Some(proto_common::WorkflowExecution {
                                    workflow_id: "wf".to_owned(),
                                    run_id: String::new(),
                                }),
                                request: Some(update::Request {
                                    meta: Some(update::Meta {
                                        update_id: String::new(),
                                        identity: "client".to_owned(),
                                    }),
                                    input: Some(update::Input {
                                        header: None,
                                        name: "update".to_owned(),
                                        args: None,
                                    }),
                                }),
                                ..Default::default()
                            },
                        )),
                    },
                ],
                ..Default::default()
            }
        };
        assert_eq!(
            translate::multi_operation_request_to_edge(multi_operation(start(over.clone())))
                .unwrap_err(),
            translate::MultiOperationRequestError::PerOperation {
                start: Some(BLOB_SIZE_EXCEEDS_LIMIT.to_owned()),
                update: None,
            }
        );
        assert!(
            translate::multi_operation_request_to_edge(multi_operation(start(at.clone()))).is_ok()
        );

        // The search attribute limits come before the input.
        let mut both = start(over.clone());
        both.search_attributes = Some(search_attributes(
            &(0..101).map(|i| (format!("Key{i}"), 1)).collect::<Vec<_>>(),
        ));
        assert_eq!(
            translate::start_request_to_edge(both)
                .unwrap_err()
                .to_string(),
            "number of search attributes 101 exceeds limit 100"
        );

        let signal =
            |input: proto_common::Payloads| workflowservice::SignalWorkflowExecutionRequest {
                namespace: "default".to_owned(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: "wf".to_owned(),
                    run_id: String::new(),
                }),
                signal_name: "s".to_owned(),
                input: Some(input),
                ..Default::default()
            };
        assert_eq!(
            translate::signal_request_to_edge(signal(over.clone()))
                .unwrap_err()
                .to_string(),
            BLOB_SIZE_EXCEEDS_LIMIT
        );
        assert!(translate::signal_request_to_edge(signal(at.clone())).is_ok());

        let query = |args: proto_common::Payloads| workflowservice::QueryWorkflowRequest {
            namespace: "default".to_owned(),
            execution: Some(proto_common::WorkflowExecution {
                workflow_id: "wf".to_owned(),
                run_id: String::new(),
            }),
            query: Some(
                tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                    query_type: "q".to_owned(),
                    query_args: Some(args),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        assert_eq!(
            translate::query_request_to_edge(query(over.clone()))
                .unwrap_err()
                .to_string(),
            BLOB_SIZE_EXCEEDS_LIMIT
        );
        assert!(translate::query_request_to_edge(query(at.clone())).is_ok());

        // Signal-with-start: the start's limits, then the signal input.
        let sws = |signal_input: proto_common::Payloads| {
            workflowservice::SignalWithStartWorkflowExecutionRequest {
                namespace: "default".to_owned(),
                workflow_id: "wf".to_owned(),
                workflow_type: Some(proto_common::WorkflowType {
                    name: "type".to_owned(),
                }),
                task_queue: Some(
                    tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: "queue".to_owned(),
                        ..Default::default()
                    },
                ),
                signal_name: "s".to_owned(),
                signal_input: Some(signal_input),
                ..Default::default()
            }
        };
        assert_eq!(
            translate::signal_with_start_request_to_edge(sws(over.clone()))
                .unwrap_err()
                .to_string(),
            BLOB_SIZE_EXCEEDS_LIMIT
        );
        assert!(translate::signal_with_start_request_to_edge(sws(at)).is_ok());
        let mut memo_first = sws(over);
        memo_first.memo = Some(memo_of_size(2 * 1024 * 1024 + 1));
        assert_eq!(
            translate::signal_with_start_request_to_edge(memo_first)
                .unwrap_err()
                .to_string(),
            MEMO_SIZE_EXCEEDS_LIMIT
        );
    }

    // ── workflow-task-command-limits: what the edge measures on a command ──

    use tokeira_proto::public::temporal::api::command::v1 as command;

    fn arb_wire_payload() -> impl Strategy<Value = proto_common::Payload> {
        (
            prop::collection::btree_map(
                "[a-z]{1,8}",
                prop::collection::vec(any::<u8>(), 0..12),
                0..3,
            ),
            prop::collection::vec(any::<u8>(), 0..400),
        )
            .prop_map(|(metadata, data)| proto_common::Payload {
                metadata,
                data,
                ..Default::default()
            })
    }

    fn arb_wire_payloads() -> impl Strategy<Value = proto_common::Payloads> {
        prop::collection::vec(arb_wire_payload(), 0..3)
            .prop_map(|payloads| proto_common::Payloads { payloads })
    }

    /// A payload map in which some fields remove their key (JSON `null`).
    fn arb_upserted_fields() -> impl Strategy<Value = BTreeMap<String, proto_common::Payload>> {
        prop::collection::btree_map(
            "[A-Za-z]{1,8}",
            prop_oneof![
                arb_wire_payload(),
                Just(proto_common::Payload {
                    metadata: BTreeMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
                    data: b"null".to_vec(),
                    ..Default::default()
                }),
            ],
            0..5,
        )
    }

    fn sent_command(attributes: command::command::Attributes) -> command::Command {
        command::Command {
            attributes: Some(attributes),
            ..Default::default()
        }
    }

    // Feature: workflow-task-command-limits, Property 3: Sizes match v1.31.0's measurements
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn property_command_sizes_match_v1_31(
            input in arb_wire_payloads(),
            single in arb_wire_payload(),
            details in prop::collection::btree_map("[a-z]{1,8}", arb_wire_payloads(), 0..3),
            memo in prop::collection::btree_map("[a-z]{1,8}", arb_wire_payload(), 0..3),
            attributes in prop::collection::btree_map("[A-Za-z]{1,8}", arb_wire_payload(), 0..4),
            upserted in arb_upserted_fields(),
        ) {
            use command::command::Attributes;
            let memo = proto_common::Memo { fields: memo };
            let search_attributes = proto_common::SearchAttributes { indexed_fields: attributes };
            let expected_attributes = tokeira_kernel::SearchAttributeSizes {
                keys: search_attributes.indexed_fields.len(),
                value_sizes: search_attributes
                    .indexed_fields
                    .iter()
                    .map(|(key, payload)| (key.clone(), payload.data.len()))
                    .collect(),
                total: search_attributes.encoded_len(),
            };

            let schedule = translate::command_payload_sizes(&sent_command(
                Attributes::ScheduleActivityTaskCommandAttributes(command::ScheduleActivityTaskCommandAttributes {
                    input: Some(input.clone()),
                    ..Default::default()
                }),
            ));
            prop_assert_eq!(schedule.payload, Some(input.encoded_len()));

            let marker = translate::command_payload_sizes(&sent_command(
                Attributes::RecordMarkerCommandAttributes(command::RecordMarkerCommandAttributes {
                    details: details.clone(),
                    ..Default::default()
                }),
            ));
            let marker_size: usize = details.iter().map(|(key, payloads)| key.len() + payloads.encoded_len()).sum();
            prop_assert_eq!(marker.payload, Some(marker_size));

            let child = translate::command_payload_sizes(&sent_command(
                Attributes::StartChildWorkflowExecutionCommandAttributes(
                    command::StartChildWorkflowExecutionCommandAttributes {
                        input: Some(input.clone()),
                        memo: Some(memo.clone()),
                        search_attributes: Some(search_attributes.clone()),
                        ..Default::default()
                    },
                ),
            ));
            prop_assert_eq!(child.payload, Some(input.encoded_len()));
            prop_assert_eq!(child.memo, Some(memo.encoded_len()));
            prop_assert_eq!(child.search_attributes.as_ref(), Some(&expected_attributes));

            let continued = translate::command_payload_sizes(&sent_command(
                Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
                    command::ContinueAsNewWorkflowExecutionCommandAttributes {
                        input: Some(input.clone()),
                        memo: Some(memo.clone()),
                        search_attributes: Some(search_attributes.clone()),
                        ..Default::default()
                    },
                ),
            ));
            prop_assert_eq!(continued, child);

            let upsert = translate::command_payload_sizes(&sent_command(
                Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                    command::UpsertWorkflowSearchAttributesCommandAttributes {
                        search_attributes: Some(proto_common::SearchAttributes {
                            indexed_fields: upserted.clone(),
                        }),
                    },
                ),
            ));
            let fields = upsert.upserted_fields.expect("an upsert's fields are measured");
            let fields_size: usize = upserted.iter().map(|(key, payload)| key.len() + payload.data.len()).sum();
            prop_assert_eq!(fields.fields_size, fields_size);
            let set: BTreeMap<String, tokeira_kernel::PayloadSize> = upserted
                .iter()
                .filter(|(_, payload)| payload.data != b"null")
                .map(|(key, payload)| (key.clone(), tokeira_kernel::PayloadSize {
                    data: payload.data.len(),
                    encoded: payload.encoded_len(),
                }))
                .collect();
            prop_assert_eq!(fields.set_fields, set);

            let nexus = |endpoint: &str| translate::command_payload_sizes(&sent_command(
                Attributes::ScheduleNexusOperationCommandAttributes(
                    command::ScheduleNexusOperationCommandAttributes {
                        endpoint: endpoint.to_owned(),
                        input: Some(single.clone()),
                        ..Default::default()
                    },
                ),
            ));
            prop_assert_eq!(nexus("endpoint").payload, Some(single.encoded_len()));
            prop_assert_eq!(nexus("__temporal_system").payload, None);
        }
    }

    #[test]
    fn leftover_messages_keep_their_sizes_through_the_splice() {
        use command::command::Attributes;
        use tokeira_proto::public::temporal::api::{
            protocol::v1 as protocol, update::v1 as update,
        };
        let input = payloads_of_size(1000);
        let result = payloads_of_size(2000);
        let acceptance = update::Acceptance {
            accepted_request_message_id: "request".to_owned(),
            accepted_request_sequencing_event_id: 3,
            accepted_request: Some(update::Request {
                meta: Some(update::Meta {
                    update_id: "update".to_owned(),
                    identity: "client".to_owned(),
                }),
                input: Some(update::Input {
                    header: None,
                    name: "update".to_owned(),
                    args: None,
                }),
            }),
        };
        let body = prost_types::Any {
            type_url: "type.googleapis.com/temporal.api.update.v1.Acceptance".to_owned(),
            value: acceptance.encode_to_vec(),
        };
        let request = workflowservice::RespondWorkflowTaskCompletedRequest {
            commands: vec![
                sent_command(Attributes::ScheduleActivityTaskCommandAttributes(
                    command::ScheduleActivityTaskCommandAttributes {
                        activity_id: "activity".to_owned(),
                        input: Some(input.clone()),
                        ..Default::default()
                    },
                )),
                sent_command(Attributes::CompleteWorkflowExecutionCommandAttributes(
                    command::CompleteWorkflowExecutionCommandAttributes {
                        result: Some(result.clone()),
                    },
                )),
            ],
            // Not referenced by a command: it is spliced in before the close.
            messages: vec![protocol::Message {
                id: "message".to_owned(),
                protocol_instance_id: "update".to_owned(),
                body: Some(body.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let translated = translate::respond_completed_request_to_edge(request).unwrap();
        assert_eq!(translated.commands.len(), 3);
        assert!(matches!(
            translated.commands[1],
            tokeira_kernel::WorkflowCommand::ProtocolMessage { .. }
        ));
        assert_eq!(translated.command_sizes.len(), 3);
        assert_eq!(translated.command_sizes[0].payload, Some(1000));
        assert_eq!(
            translated.command_sizes[1].protocol_message,
            Some(tokeira_kernel::ProtocolMessageSize {
                body: body.encoded_len(),
                type_name: "temporal.api.update.v1.Acceptance".to_owned(),
            })
        );
        assert_eq!(translated.command_sizes[2].payload, Some(2000));
    }

    #[test]
    fn an_oversized_query_answer_in_a_completion_fails_that_query() {
        use tokeira_proto::public::temporal::api::query::v1 as query;
        let answered = |size: usize| query::WorkflowQueryResult {
            result_type: tokeira_proto::enums::QueryResultType::Answered as i32,
            answer: Some(payloads_of_size(size)),
            ..Default::default()
        };
        let request = workflowservice::RespondWorkflowTaskCompletedRequest {
            query_results: BTreeMap::from([
                ("over".to_owned(), answered(2 * 1024 * 1024 + 1)),
                ("at".to_owned(), answered(2 * 1024 * 1024)),
            ]),
            ..Default::default()
        };
        let translated = translate::respond_completed_request_to_edge(request).unwrap();
        assert_eq!(
            translated.query_results["over"],
            crate::translate::QueryResultDto::ResultTooLarge
        );
        assert!(matches!(
            translated.query_results["at"],
            crate::translate::QueryResultDto::Answered { .. }
        ));
    }
}
