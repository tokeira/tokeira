use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Opaque application payload.
///
/// We model payloads as bytes plus string metadata so that this crate remains
/// neutral about codecs. The platform should be able to carry JSON, Protobuf,
/// encrypted blobs, or future codecs without changing its semantic types.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    pub data: Vec<u8>,
    pub metadata: BTreeMap<String, String>,
}

impl Payload {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payloads(pub Vec<Payload>);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Headers(pub BTreeMap<String, Payload>);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memo(pub BTreeMap<String, Payload>);
