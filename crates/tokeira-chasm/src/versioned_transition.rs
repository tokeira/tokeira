//! The `VersionedTransition` logical clock.
//!
//! A `VersionedTransition` (VT) is the monotonic per-execution logical clock
//! `{ namespace_failover_version, transition_count }` that stamps every dirty node
//! on transition close (`hsm.proto:114 @ v1.31.0`; Requirement 5.4). It is the
//! single primitive underpinning long-poll change detection and
//! [`ComponentRef`](crate::component_ref) staleness.
//!
//! Invariants this module upholds:
//! - [`VersionedTransition::staleness_check`] yields **exactly one** of
//!   `Advanced`/`Same`/`Behind`, comparing `namespace_failover_version` first and
//!   then `transition_count` (`transition_history.go:9 @ v1.31.0`; Requirement 5.5).
//! - The VT is logical, not a wall clock, and **round-trips through its protobuf
//!   wire encoding without loss** to preserve SDK long-poll compatibility
//!   (`hsm.proto:114 @ v1.31.0`; Requirement 5.6).

use prost::Message as _;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::error::ChasmError;

/// Logical per-execution clock; monotonic, advancing by one `transition_count`
/// on each committed transition (and tracking `namespace_failover_version` so
/// post-failover transitions order after pre-failover ones regardless of count).
///
/// This is the value stamped onto every node a transition dirties, and the token
/// long-poll and [`ComponentRef`](crate::component_ref) staleness compare against.
/// It is a logical clock, **not** a wall clock: only its order carries meaning.
///
/// The wire shape mirrors `persistence.v1.VersionedTransition` (`hsm.proto:114 @
/// v1.31.0`) — two `int64` fields at tags 1 and 2 — so [`encode`](Self::encode)
/// output is byte-compatible with the targeted release and round-trips without
/// loss (Requirement 5.6).
///
/// [`Default`] is the **zero clock** `{ 0, 0 }`: the logical time before any
/// transition has committed. A fresh [`NodeTree`](crate::node::NodeTree) starts
/// here, and the first committed transition advances past it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VersionedTransition {
    /// The namespace failover version at transition time. Compared first by
    /// [`staleness_check`](Self::staleness_check): a higher failover version is
    /// unconditionally newer, so transitions written after a namespace failover
    /// order after earlier ones even if their `transition_count` is lower.
    pub namespace_failover_version: i64,
    /// The transition count perceived during the current `namespace_failover_version`.
    /// Compared second, only when the failover versions are equal.
    pub transition_count: i64,
}

/// The result of comparing one [`VersionedTransition`] against another: the
/// receiver is `Advanced` past, at the `Same` point as, or `Behind` the argument.
///
/// Exactly one variant is returned for any pair (Requirement 5.5). This is the
/// `-1 / 0 / 1` contract of `transition_history.go:9 @ v1.31.0` named for the
/// reader: `Behind` corresponds to `-1`, `Same` to `0`, `Advanced` to `1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Staleness {
    /// The receiver's clock is strictly newer than the argument's: the execution
    /// has advanced past the compared token (long-poll resolves; a ref is live).
    Advanced,
    /// The two clocks are identical: no advance since the compared token.
    Same,
    /// The receiver's clock is strictly older than the argument's: the receiver
    /// references state that no longer matches the live clock (a stale ref/token).
    Behind,
}

impl VersionedTransition {
    /// Construct a `VersionedTransition` from its two components.
    pub const fn new(namespace_failover_version: i64, transition_count: i64) -> Self {
        Self {
            namespace_failover_version,
            transition_count,
        }
    }

    /// Compare `self` against `other`, returning exactly one [`Staleness`].
    ///
    /// `namespace_failover_version` is compared first and `transition_count` only
    /// when the failover versions are equal, reproducing
    /// `transitionhistory.Compare` (`transition_history.go:9 @ v1.31.0`): there,
    /// `Compare(a, b)` returns `-1`/`0`/`1` ordering by failover version then
    /// transition count. Here `self` plays the role of the live clock and `other`
    /// the compared token, so `self > other` is reported as [`Staleness::Advanced`]
    /// (the live state has moved past the token) and `self < other` as
    /// [`Staleness::Behind`] (the token references newer state than `self`) —
    /// matching how `ExecutionStateChanged` reads the `-1`/`1` result to decide
    /// "advanced" vs. "stale" in the same file.
    pub fn staleness_check(&self, other: &VersionedTransition) -> Staleness {
        match self
            .namespace_failover_version
            .cmp(&other.namespace_failover_version)
            .then(self.transition_count.cmp(&other.transition_count))
        {
            Ordering::Greater => Staleness::Advanced,
            Ordering::Equal => Staleness::Same,
            Ordering::Less => Staleness::Behind,
        }
    }

    /// Encode to the `persistence.v1.VersionedTransition` protobuf wire form
    /// (`hsm.proto:114 @ v1.31.0`). Infallible — protobuf encoding into an owned
    /// buffer cannot fail.
    pub fn encode(&self) -> Vec<u8> {
        VersionedTransitionProto {
            namespace_failover_version: self.namespace_failover_version,
            transition_count: self.transition_count,
        }
        .encode_to_vec()
    }

    /// Decode from the `persistence.v1.VersionedTransition` protobuf wire form
    /// (`hsm.proto:114 @ v1.31.0`).
    ///
    /// Returns [`ChasmError::Validation`] when `bytes` is not a well-formed
    /// encoding — a malformed long-poll/ref token is bad input, not a substrate
    /// bug. Combined with [`encode`](Self::encode) this is a lossless round-trip
    /// (Requirement 5.6).
    pub fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        let proto = VersionedTransitionProto::decode(bytes).map_err(|e| {
            ChasmError::Validation(format!("invalid VersionedTransition wire encoding: {e}"))
        })?;
        Ok(Self {
            namespace_failover_version: proto.namespace_failover_version,
            transition_count: proto.transition_count,
        })
    }
}

/// Wire mirror of `persistence.v1.VersionedTransition` (`hsm.proto:114 @ v1.31.0`).
///
/// Kept private so the protobuf field tags are an implementation detail of
/// [`VersionedTransition::encode`]/[`VersionedTransition::decode`] rather than
/// part of the substrate's public surface. The tag numbers (1, 2) and `int64`
/// types are fixed by the targeted release and must not change.
#[derive(Clone, PartialEq, ::prost::Message)]
struct VersionedTransitionProto {
    #[prost(int64, tag = "1")]
    namespace_failover_version: i64,
    #[prost(int64, tag = "2")]
    transition_count: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staleness_advances_on_higher_transition_count() {
        let earlier = VersionedTransition::new(1, 5);
        let later = VersionedTransition::new(1, 6);
        assert_eq!(later.staleness_check(&earlier), Staleness::Advanced);
        assert_eq!(earlier.staleness_check(&later), Staleness::Behind);
    }

    #[test]
    fn staleness_same_when_identical() {
        let vt = VersionedTransition::new(3, 9);
        assert_eq!(vt.staleness_check(&vt), Staleness::Same);
    }

    #[test]
    fn failover_version_dominates_transition_count() {
        // A higher failover version is newer even when its transition_count is
        // lower (transition_history.go:9 compares failover first).
        let pre_failover = VersionedTransition::new(1, 100);
        let post_failover = VersionedTransition::new(2, 0);
        assert_eq!(
            post_failover.staleness_check(&pre_failover),
            Staleness::Advanced
        );
        assert_eq!(
            pre_failover.staleness_check(&post_failover),
            Staleness::Behind
        );
    }

    #[test]
    fn behind_on_lower_failover_version() {
        let lower = VersionedTransition::new(1, 7);
        let higher = VersionedTransition::new(4, 7);
        assert_eq!(lower.staleness_check(&higher), Staleness::Behind);
        assert_eq!(higher.staleness_check(&lower), Staleness::Advanced);
    }

    #[test]
    fn wire_round_trip_is_lossless() {
        for vt in [
            VersionedTransition::new(0, 0),
            VersionedTransition::new(1, 1),
            VersionedTransition::new(i64::MAX, i64::MAX),
            VersionedTransition::new(i64::MIN, i64::MIN),
            VersionedTransition::new(7, -3),
        ] {
            let decoded =
                VersionedTransition::decode(&vt.encode()).expect("well-formed encoding decodes");
            assert_eq!(decoded, vt);
        }
    }

    #[test]
    fn decode_rejects_malformed_bytes() {
        // A truncated varint is not a valid protobuf message.
        let err = VersionedTransition::decode(&[0x08]).unwrap_err();
        assert!(matches!(err, ChasmError::Validation(_)));
    }
}
