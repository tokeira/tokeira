//! The addressable, staleness-checked node return-address.
//!
//! A [`ComponentRef`] is a serialized "return address" to a specific component
//! node, captured as of a specific point in the execution's logical time. It is
//! how a side-effect task, a callback, or an external caller names *which node*
//! to deliver a result to and *as of when*, so a result for a node that has since
//! moved or closed is detected rather than misapplied. It mirrors upstream CHASM's
//! `ComponentRef` (`chasm/ref.go:16 @ v1.31.0`).
//!
//! It carries five things (Requirement 8.4):
//! - the [`ExecutionKey`] naming the execution,
//! - the `archetype_id` of the root component (`0` reserved for legacy Workflow),
//! - the **execution** [`VersionedTransition`] the ref was issued at — the
//!   staleness token for the whole execution,
//! - the `component_path` to the node within the tree, and
//! - the **component-initial** [`VersionedTransition`] — the VT at which *this*
//!   node was created.
//!
//! Two invariants this module upholds:
//! - **Node identity is `(component_path, component_initial_versioned_transition)`**
//!   (Requirement 8.5): the path alone is not enough, because a node can be
//!   deleted and a new node created at the same path; the initial VT
//!   distinguishes the instances.
//! - **Staleness is decided by the execution VT** (Requirement 8.6): if the ref's
//!   execution VT is [`Behind`](Staleness::Behind) the live execution clock, the
//!   ref is reported stale ([`ChasmError::StaleReference`]) rather than followed.
//!
//! The wire form round-trips losslessly (Requirement 8.5). It is a tokeira-owned
//! encoding (it reuses [`VersionedTransition`] and [`path`](crate::path) for its
//! sub-encodings); byte-compatibility with Temporal's `ChasmComponentRef` is not
//! required because the token is internal to tokeira, but the round-trip and
//! staleness semantics match `ref.go @ v1.31.0`.

use prost::Message as _;
use serde::{Deserialize, Serialize};

use crate::{
    ChasmError,
    node::ExecutionKey,
    path::{self, PathSegment},
    versioned_transition::{Staleness, VersionedTransition},
};

/// A serialized return-address to a specific component node, as of a specific
/// execution [`VersionedTransition`].
///
/// See the module documentation for the field contract and the two invariants
/// (node identity and execution-VT staleness).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentRef {
    /// The execution this reference points into.
    pub execution_key: ExecutionKey,
    /// The root component's archetype id; `0` is reserved for legacy Workflow.
    pub archetype_id: u32,
    /// The execution VersionedTransition the reference was issued at. Compared
    /// against the live execution clock to decide staleness (Requirement 8.6).
    pub execution_versioned_transition: VersionedTransition,
    /// The path from the root to the referenced node within the tree.
    pub component_path: Vec<PathSegment>,
    /// The VersionedTransition at which the referenced node was created; together
    /// with `component_path` this is the node's identity (Requirement 8.5).
    pub component_initial_versioned_transition: VersionedTransition,
}

impl ComponentRef {
    /// Construct a component reference from its parts.
    pub fn new(
        execution_key: ExecutionKey,
        archetype_id: u32,
        execution_versioned_transition: VersionedTransition,
        component_path: Vec<PathSegment>,
        component_initial_versioned_transition: VersionedTransition,
    ) -> Self {
        Self {
            execution_key,
            archetype_id,
            execution_versioned_transition,
            component_path,
            component_initial_versioned_transition,
        }
    }

    /// The node's identity: `(component_path, component_initial_versioned_transition)`
    /// (Requirement 8.5). Two refs naming the same path but different initial VTs
    /// address *different* node instances (a delete-and-recreate at the same path).
    pub fn node_identity(&self) -> (&[PathSegment], VersionedTransition) {
        (
            &self.component_path,
            self.component_initial_versioned_transition,
        )
    }

    /// Whether this reference is stale relative to `live_execution_vt`.
    ///
    /// The ref is stale iff the live execution clock has [`Advanced`](Staleness::Advanced)
    /// past the execution VT the ref was issued at — i.e. the execution has
    /// committed transitions since the ref was minted, so the referenced state may
    /// have moved (Requirement 8.6). A ref issued at the current clock
    /// ([`Same`](Staleness::Same)) is fresh.
    pub fn is_stale(&self, live_execution_vt: &VersionedTransition) -> bool {
        matches!(
            live_execution_vt.staleness_check(&self.execution_versioned_transition),
            Staleness::Advanced
        )
    }

    /// Return `Ok(())` if the reference is fresh against `live_execution_vt`, or
    /// [`ChasmError::StaleReference`] if it is stale. This is the check the engine
    /// performs before following a reference to a node (Requirement 8.6).
    pub fn ensure_fresh(&self, live_execution_vt: &VersionedTransition) -> Result<(), ChasmError> {
        if self.is_stale(live_execution_vt) {
            Err(ChasmError::StaleReference)
        } else {
            Ok(())
        }
    }

    /// Encode to the tokeira-owned `ComponentRef` wire form.
    ///
    /// The two [`VersionedTransition`] fields and the `component_path` are encoded
    /// with their own canonical encoders ([`VersionedTransition::encode`] and
    /// [`path::encode`]) so this stays decoupled from their internal shapes and
    /// round-trips losslessly (Requirement 8.5).
    ///
    /// # Errors
    ///
    /// Returns [`ChasmError::Internal`] if the `component_path` contains an empty
    /// segment name (propagated from [`path::encode`]).
    pub fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        let proto = ComponentRefProto {
            namespace_id: self.execution_key.namespace_id.clone(),
            business_id: self.execution_key.business_id.clone(),
            run_id: self.execution_key.run_id.clone(),
            archetype_id: self.archetype_id,
            execution_versioned_transition: self.execution_versioned_transition.encode(),
            component_path: path::encode(&self.component_path)?,
            component_initial_versioned_transition: self
                .component_initial_versioned_transition
                .encode(),
        };
        Ok(proto.encode_to_vec())
    }

    /// Decode from the tokeira-owned `ComponentRef` wire form.
    ///
    /// # Errors
    ///
    /// Returns [`ChasmError::Validation`] if `bytes` is not a well-formed encoding
    /// (a malformed token is bad input, not a substrate bug), propagating the
    /// sub-decoder errors for the embedded VTs and path.
    pub fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        let proto = ComponentRefProto::decode(bytes).map_err(|e| {
            ChasmError::Validation(format!("invalid ComponentRef wire encoding: {e}"))
        })?;
        Ok(Self {
            execution_key: ExecutionKey::new(proto.namespace_id, proto.business_id, proto.run_id),
            archetype_id: proto.archetype_id,
            execution_versioned_transition: VersionedTransition::decode(
                &proto.execution_versioned_transition,
            )?,
            component_path: path::decode(&proto.component_path)?,
            component_initial_versioned_transition: VersionedTransition::decode(
                &proto.component_initial_versioned_transition,
            )?,
        })
    }
}

/// Wire mirror of the component reference. Private so the field tags are an
/// implementation detail of [`ComponentRef::encode`]/[`ComponentRef::decode`].
/// The embedded VTs and path are themselves canonically-encoded byte blobs (see
/// [`ComponentRef::encode`]).
#[derive(Clone, PartialEq, ::prost::Message)]
struct ComponentRefProto {
    #[prost(string, tag = "1")]
    namespace_id: String,
    #[prost(string, tag = "2")]
    business_id: String,
    #[prost(string, tag = "3")]
    run_id: String,
    #[prost(uint32, tag = "4")]
    archetype_id: u32,
    #[prost(bytes = "vec", tag = "5")]
    execution_versioned_transition: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    component_path: Vec<u8>,
    #[prost(bytes = "vec", tag = "7")]
    component_initial_versioned_transition: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ref() -> ComponentRef {
        ComponentRef::new(
            ExecutionKey::new("ns", "wf-1", "run-1"),
            7,
            VersionedTransition::new(1, 10),
            vec![
                PathSegment::field("attempts"),
                PathSegment::collection("0001"),
            ],
            VersionedTransition::new(1, 4),
        )
    }

    #[test]
    fn wire_round_trip_is_lossless() {
        let reference = sample_ref();
        let decoded = ComponentRef::decode(&reference.encode().expect("encode"))
            .expect("well-formed encoding decodes");
        assert_eq!(decoded, reference);
    }

    #[test]
    fn node_identity_is_path_plus_initial_vt() {
        let reference = sample_ref();
        let (path, initial_vt) = reference.node_identity();
        assert_eq!(path, reference.component_path.as_slice());
        assert_eq!(initial_vt, VersionedTransition::new(1, 4));
    }

    #[test]
    fn staleness_tracks_execution_clock() {
        let reference = sample_ref(); // issued at exec VT (1, 10)
        // Live clock at the same point: fresh.
        assert!(!reference.is_stale(&VersionedTransition::new(1, 10)));
        reference
            .ensure_fresh(&VersionedTransition::new(1, 10))
            .expect("fresh ref passes");
        // Live clock advanced past the ref: stale.
        assert!(reference.is_stale(&VersionedTransition::new(1, 11)));
        assert!(matches!(
            reference.ensure_fresh(&VersionedTransition::new(1, 11)),
            Err(ChasmError::StaleReference)
        ));
        // A higher failover version is also "advanced" and therefore stale.
        assert!(reference.is_stale(&VersionedTransition::new(2, 0)));
    }

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = ComponentRef::decode(&[0xFF, 0xFF, 0xFF]).unwrap_err();
        assert!(matches!(err, ChasmError::Validation(_)));
    }
}
