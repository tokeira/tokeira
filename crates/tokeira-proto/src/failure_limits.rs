//! Temporal v1.31.0's replacement for a failure over a size limit: a server
//! failure whose cause is the original cut down by `failure.TruncateWithDepth`
//! (`common/failure/failure.go:14-23, 48-91 @ v1.31.0`).
//!
//! The edge records it for a failure a request carries
//! (`payload-admission-limits`), and the runtime stores it for a retried
//! activity's failure (`run-growth-limits`).

use crate::public::temporal::api::failure::v1 as failure_proto;

/// `common.FailureReasonFailureExceedsLimit` (`common/util.go:99 @ v1.31.0`).
pub const FAILURE_EXCEEDS_LIMIT: &str = "Failure exceeds size limit.";

/// The depth `failure.Truncate` follows causes to
/// (`common/failure/failure.go:48-50 @ v1.31.0`).
const FAILURE_TRUNCATION_DEPTH: u32 = 20;

/// A server failure (`failure.NewServerFailure`,
/// `common/failure/failure.go:14-23 @ v1.31.0`).
pub fn server_failure(message: &str, non_retryable: bool) -> failure_proto::Failure {
    failure_proto::Failure {
        message: message.to_owned(),
        failure_info: Some(failure_proto::failure::FailureInfo::ServerFailureInfo(
            failure_proto::ServerFailureInfo { non_retryable },
        )),
        ..Default::default()
    }
}

/// A port of `failure.TruncateWithDepth` with a depth of 20
/// (`common/failure/failure.go:48-91 @ v1.31.0`).
pub fn truncate_failure(failure: &failure_proto::Failure, max_size: i64) -> failure_proto::Failure {
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
