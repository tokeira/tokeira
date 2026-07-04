//! Opaque data envelope for all user-facing values (inputs, results, memo entries).
//!
//! The server never inspects payload contents — encoding and decoding are the
//! SDK's responsibility. This keeps the platform codec-neutral: JSON, Protobuf,
//! encrypted blobs, or any future format can flow through the system without
//! changes to the core types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Opaque application payload.
///
/// We model payloads as bytes plus string metadata so that
/// this crate remains neutral about codecs. The platform
/// should be able to carry JSON, Protobuf, encrypted blobs,
/// or future codecs without changing its semantic types.
///
/// The `metadata` map typically holds keys like
/// `"encoding"` (`"json/plain"`, `"binary/protobuf"`, …) so
/// that the SDK data-converter can round-trip values without
/// the server understanding their schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    /// Raw serialised bytes. The server treats this as opaque.
    pub data: Vec<u8>,
    /// Codec hints (e.g. `"encoding" → "json/plain"`).
    ///
    /// A `BTreeMap` is used instead of `HashMap` so that
    /// serialisation order is deterministic, which simplifies
    /// snapshot testing and history comparison.
    pub metadata: BTreeMap<String, String>,
    /// References to payload content stored OUTSIDE the event blob
    /// (`Payload.external_payloads` on the wire). The server never
    /// dereferences these; they round-trip opaquely and feed the
    /// execution's external-payload statistics
    /// (`CalculateExternalPayloadSize`,
    /// service/history/workflow/external_payload_size.go @ v1.31.0).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_payloads: Vec<ExternalPayloadDetail>,
}

/// A single external-payload reference: the wire proto's
/// `Payload.ExternalPayloadDetails` (size only — the server counts and
/// sizes external payloads for execution statistics without ever
/// dereferencing them).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalPayloadDetail {
    pub size_bytes: i64,
}

impl Payload {
    /// Create a payload with the given data and no metadata.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            metadata: BTreeMap::new(),
            external_payloads: Vec::new(),
        }
    }
}

/// Ordered collection of [`Payload`] values.
///
/// Used for workflow/activity inputs and outputs where the
/// SDK passes a positional argument list.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payloads(pub Vec<Payload>);

/// Transport-level headers carried alongside a task.
///
/// Headers are distinct from [`Memo`] — they are intended for
/// cross-cutting concerns (tracing context, auth tokens) that
/// interceptors may read or mutate, whereas memos are
/// user-visible metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Headers(pub BTreeMap<String, Payload>);

/// User-attached key-value metadata stored with a workflow.
///
/// Memos are indexed by the projection plane and surfaced in
/// list queries. Unlike [`SearchAttributes`], memo values are
/// opaque [`Payload`] blobs — the visibility store does not
/// interpret their contents.
///
/// [`SearchAttributes`]: crate::SearchAttributes
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memo(pub BTreeMap<String, Payload>);
