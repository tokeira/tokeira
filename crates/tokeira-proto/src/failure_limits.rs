//! Temporal v1.31.0's replacement for a failure over a size limit: a server
//! failure `Failure exceeds size limit.` whose cause is the original failure
//! cut down (`service/frontend/workflow_handler.go:1231-1245`;
//! `service/history/workflow/mutable_state_impl.go:6587-6608 @ v1.31.0`).
//!
//! The edge records it for a failure a request carries
//! (`payload-admission-limits`), and the runtime stores it for a retried
//! activity's failure (`run-growth-limits`). Failures are cut by their
//! protobuf-encoded size, so a cut-down failure always fits its limit.

use prost::{Message as _, encoding::encoded_len_varint};

use crate::public::temporal::api::failure::v1::{
    ApplicationFailureInfo, Failure, ServerFailureInfo, failure::FailureInfo,
};

/// `common.FailureReasonFailureExceedsLimit` (`common/util.go:99 @ v1.31.0`).
pub const FAILURE_EXCEEDS_LIMIT: &str = "Failure exceeds size limit.";

/// How many causes a cut-down failure keeps at most, as many as v1.31.0's.
const MAX_CAUSES: usize = 20;

/// A server failure (`failure.NewServerFailure`,
/// `common/failure/failure.go:14-23 @ v1.31.0`).
pub fn server_failure(message: &str, non_retryable: bool) -> Failure {
    Failure {
        message: message.to_owned(),
        failure_info: Some(FailureInfo::ServerFailureInfo(ServerFailureInfo {
            non_retryable,
        })),
        ..Default::default()
    }
}

/// The server failure `Failure exceeds size limit.` that stands in for
/// `original`, with `original` cut down as its cause so that the whole encodes
/// in at most `limit` bytes, when the server failure alone does.
pub fn oversized_failure(original: &Failure, limit: usize, non_retryable: bool) -> Failure {
    let mut replacement = server_failure(FAILURE_EXCEEDS_LIMIT, non_retryable);
    attach_cause(&mut replacement, original, limit, MAX_CAUSES);
    replacement
}

/// `failure` cut down to encode in at most `limit` bytes.
///
/// An application or server failure keeps its kind and whether it is
/// retryable. Then an application failure's type, the source, the message and
/// the stack trace are kept in that order, each whole or, for the first that
/// doesn't fit, as much of it as fits, cut at a character boundary; nothing
/// after a cut field is kept. When every field is whole, the cause is cut
/// down the same way in the room left, up to twenty causes deep. Details and
/// every other field are dropped. A limit too small for even the failure's
/// kind gives an empty failure.
pub fn truncate_failure(failure: &Failure, limit: usize) -> Failure {
    cut_down(failure, limit, MAX_CAUSES)
}

fn cut_down(failure: &Failure, limit: usize, causes: usize) -> Failure {
    let mut kept = Failure {
        failure_info: kept_kind(failure),
        ..Default::default()
    };
    if kept.encoded_len() > limit {
        return Failure::default();
    }
    if fill_text(&mut kept, limit, failure)
        && causes > 0
        && let Some(cause) = &failure.cause
    {
        attach_cause(&mut kept, cause, limit, causes - 1);
    }
    kept
}

/// The kind an application or server failure keeps, with whether it is
/// retryable; other kinds are dropped.
fn kept_kind(failure: &Failure) -> Option<FailureInfo> {
    match &failure.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(info)) => Some(
            FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo {
                non_retryable: info.non_retryable,
                ..Default::default()
            }),
        ),
        Some(FailureInfo::ServerFailureInfo(info)) => {
            Some(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                non_retryable: info.non_retryable,
            }))
        }
        _ => None,
    }
}

/// Fill `kept`'s text fields from `failure` in order, within `limit`,
/// stopping at the first that has to be cut. Returns whether every field was
/// kept whole.
fn fill_text(kept: &mut Failure, limit: usize, failure: &Failure) -> bool {
    if let Some(FailureInfo::ApplicationFailureInfo(info)) = &failure.failure_info
        && !fill(kept, limit, &info.r#type, |kept, text| {
            if let Some(FailureInfo::ApplicationFailureInfo(info)) = &mut kept.failure_info {
                info.r#type = text.to_owned();
            }
        })
    {
        return false;
    }
    fill(kept, limit, &failure.source, |kept, text| {
        kept.source = text.to_owned();
    }) && fill(kept, limit, &failure.message, |kept, text| {
        kept.message = text.to_owned();
    }) && fill(kept, limit, &failure.stack_trace, |kept, text| {
        kept.stack_trace = text.to_owned();
    })
}

/// Set one field of `kept` to `text`, or to its longest prefix that keeps
/// `kept` within `limit`. Returns whether the whole text fit.
fn fill(kept: &mut Failure, limit: usize, text: &str, set: impl Fn(&mut Failure, &str)) -> bool {
    set(kept, text);
    let mut over = kept.encoded_len().saturating_sub(limit);
    if over == 0 {
        return true;
    }
    // Each byte taken off saves at least a byte, so shrink by the excess until
    // it fits. With the field empty, `kept` fits as it did before.
    let mut len = text.len();
    while over > 0 {
        len = char_boundary_at_most(text, len.saturating_sub(over));
        set(kept, &text[..len]);
        over = kept.encoded_len().saturating_sub(limit);
    }
    // A shorter length prefix can free a byte: take back whole characters
    // while they fit.
    while let Some(next) = text[len..].chars().next() {
        let longer = len + next.len_utf8();
        set(kept, &text[..longer]);
        if kept.encoded_len() > limit {
            set(kept, &text[..len]);
            break;
        }
        len = longer;
    }
    false
}

/// Attach `cause`, cut down to the room `kept` leaves within `limit`, unless
/// nothing of it fits.
fn attach_cause(kept: &mut Failure, cause: &Failure, limit: usize, causes: usize) {
    let room = limit.saturating_sub(kept.encoded_len());
    let cut = cut_down(cause, cause_budget(room), causes);
    if cut != Failure::default() {
        kept.cause = Some(Box::new(cut));
    }
}

/// The largest cause that fits in `room` with its field's tag and length
/// prefix.
fn cause_budget(room: usize) -> usize {
    let field = |len: usize| 1 + encoded_len_varint(len as u64) + len;
    let mut budget = room.saturating_sub(1 + encoded_len_varint(room as u64));
    while field(budget + 1) <= room {
        budget += 1;
    }
    budget
}

/// The largest character boundary of `text` at or below `index`.
fn char_boundary_at_most(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}
