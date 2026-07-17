//! Public task-token envelopes owned by the compatibility edge.
//!
//! Runtime task tokens fence authoritative workflow and activity transitions.
//! Namespace identity instead belongs to public request admission, so the edge
//! adds it only to the self-describing JSON bytes returned to workers. This
//! mirrors the stable namespace identity in Temporal task tokens without
//! coupling kernel token equality or runtime retry reconstruction to edge policy.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokeira_types::NamespaceId;

/// Edge wire envelope around an internal workflow or activity fencing token.
#[derive(Debug, Serialize, Deserialize)]
struct NamespacedTaskToken<T> {
    /// Stable namespace identity used for pre-authentication back-fill and
    /// post-authorization mismatch validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace_id: Option<NamespaceId>,
    /// Runtime fencing fields retain their historical top-level JSON shape so
    /// pre-change in-flight tokens remain decodable.
    #[serde(flatten)]
    token: T,
}

/// Serialize an internal task token with its stable namespace identity.
pub(crate) fn encode<T: Serialize>(
    token: T,
    namespace_id: NamespaceId,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&NamespacedTaskToken {
        namespace_id: Some(namespace_id),
        token,
    })
}

/// Decode both current namespaced bytes and legacy namespace-less JSON bytes.
pub(crate) fn decode<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<(T, Option<NamespaceId>), serde_json::Error> {
    let envelope: NamespacedTaskToken<T> = serde_json::from_slice(bytes)?;
    Ok((envelope.token, envelope.namespace_id))
}

#[cfg(test)]
mod tests {
    use tokeira_types::{LogicalTaskSeq, RunKey, ShardEpoch, WorkflowTaskToken};

    use super::*;

    fn token() -> WorkflowTaskToken {
        WorkflowTaskToken {
            run_key: RunKey::new(),
            logical_seq: LogicalTaskSeq(7),
            started_event_id: 11,
            attempt: 2,
            shard_epoch: ShardEpoch(3),
        }
    }

    #[test]
    fn current_envelope_round_trips_namespace_and_fencing_fields() {
        let token = token();
        let namespace_id = NamespaceId::new();
        let bytes = encode(token.clone(), namespace_id).expect("encode");
        let (decoded, decoded_namespace) = decode::<WorkflowTaskToken>(&bytes).expect("decode");
        assert_eq!(decoded, token);
        assert_eq!(decoded_namespace, Some(namespace_id));
    }

    #[test]
    fn legacy_flat_json_decodes_without_namespace_identity() {
        let token = token();
        let legacy = serde_json::to_vec(&token).expect("legacy token");
        let (decoded, decoded_namespace) =
            decode::<WorkflowTaskToken>(&legacy).expect("legacy decode");
        assert_eq!(decoded, token);
        assert_eq!(decoded_namespace, None);
    }
}
