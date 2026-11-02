//! Stable bounded lookup keys shared by derived dispatch indexes.
//!
//! The original workflow digest domain is deliberately preserved: sharing its
//! encoding must not strand rows written before activity discovery adopted it.

use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};

pub(crate) fn lookup_digest(domain: &[u8], value: Option<&str>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tokeira-workflow-dispatch-v1\0");
    digest.update((domain.len() as u64).to_be_bytes());
    digest.update(domain);
    digest.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

pub(crate) fn dispatch_timestamp(value: OffsetDateTime) -> OffsetDateTime {
    // Match SQLx's truncation toward zero from the PostgreSQL epoch, including
    // dates before that epoch. State keeps its original sub-microsecond time.
    let epoch = OffsetDateTime::UNIX_EPOCH + Duration::seconds(946_684_800);
    epoch
        + Duration::microseconds(
            i64::try_from((value - epoch).whole_microseconds())
                .expect("time's supported date range fits SQL timestamp microseconds"),
        )
}
