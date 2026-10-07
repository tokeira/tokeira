//! Pre-accumulator state codec from Tokeira `689e89a8622d114de1dd80232a22bb63705d50d1`.
//! The mixed-writer contracts execute this reader (which skips tag 3) and writer
//! (which drops it), including the baseline heartbeat extension.

use crate::codec::{
    ACTIVITY_HEARTBEAT_SECTION, BlobFormatError, ExtensionSection, StateExtensionError,
    WORKFLOW_STATE_ENVELOPE_VERSION, WORKFLOW_STATE_EXTENSION_MAGIC, encode,
};
use anyhow::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeSet;
use time::OffsetDateTime;
use tokeira_kernel::WorkflowState;
use tokeira_types::RunKey;
const WORKFLOW_STATE_KIND: &str = "workflow_hot.state_data";
#[derive(Serialize, Deserialize)]
struct ActivityHeartbeat {
    activity_id: String,
    last_heartbeat_at: OffsetDateTime,
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn encode_workflow_state(state: &WorkflowState) -> Result<Vec<u8>> {
    let mut bytes = encode_enveloped(WORKFLOW_STATE_ENVELOPE_VERSION, state)?;
    bytes.extend(encode_state_extension(state)?);
    Ok(bytes)
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn decode_workflow_state(run_key: RunKey, bytes: &[u8]) -> Result<WorkflowState> {
    let payload = envelope_payload(
        WORKFLOW_STATE_KIND,
        WORKFLOW_STATE_ENVELOPE_VERSION,
        run_key,
        bytes,
    )?;
    let (mut state, extension) = postcard::take_from_bytes::<WorkflowState>(payload)?;
    if !extension.is_empty() {
        apply_state_extension(&mut state, extension).map_err(|defect| StateExtensionError {
            kind: WORKFLOW_STATE_KIND,
            run_key,
            defect,
        })?;
    }
    Ok(state)
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn encode_state_extension(state: &WorkflowState) -> postcard::Result<Vec<u8>> {
    // `activities` is a `BTreeMap`, so entries come out in activity-id order.
    let heartbeats = state
        .activities
        .iter()
        .filter_map(|(activity_id, activity)| {
            activity
                .last_heartbeat_at
                .map(|last_heartbeat_at| ActivityHeartbeat {
                    activity_id: activity_id.clone(),
                    last_heartbeat_at,
                })
        })
        .collect::<Vec<_>>();
    let mut sections = Vec::new();
    if !heartbeats.is_empty() {
        sections.push(ExtensionSection {
            tag: ACTIVITY_HEARTBEAT_SECTION,
            payload: postcard::to_allocvec(&heartbeats)?,
        });
    }
    encode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, &sections)
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn apply_state_extension(
    state: &mut WorkflowState,
    bytes: &[u8],
) -> std::result::Result<(), &'static str> {
    for section in decode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, bytes)? {
        if section.tag != ACTIVITY_HEARTBEAT_SECTION {
            // A later release's section: its data is safe to drop by rule.
            continue;
        }
        let heartbeats = decode_exact::<Vec<ActivityHeartbeat>>(&section.payload)
            .ok_or("undecodable heartbeat section")?;
        let mut listed = BTreeSet::new();
        if !heartbeats
            .iter()
            .all(|heartbeat| listed.insert(heartbeat.activity_id.as_str()))
        {
            return Err("activity listed twice in the heartbeat section");
        }
        for heartbeat in heartbeats {
            // An activity the state does not hold is skipped; the writer never
            // lists one, and dropping it is safe.
            if let Some(activity) = state.activities.get_mut(&heartbeat.activity_id) {
                activity.last_heartbeat_at = Some(heartbeat.last_heartbeat_at);
            }
        }
    }
    Ok(())
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn encode_extension(
    magic: u32,
    sections: &[ExtensionSection],
) -> postcard::Result<Vec<u8>> {
    if sections.is_empty() {
        return Ok(Vec::new());
    }
    postcard::to_allocvec(&(magic, sections))
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn decode_extension(
    magic: u32,
    bytes: &[u8],
) -> std::result::Result<Vec<ExtensionSection>, &'static str> {
    let (found, rest) =
        postcard::take_from_bytes::<u32>(bytes).map_err(|_| "truncated extension magic")?;
    if found != magic {
        return Err("unknown extension magic");
    }
    let (sections, rest) = postcard::take_from_bytes::<Vec<ExtensionSection>>(rest)
        .map_err(|_| "undecodable extension sections")?;
    if !rest.is_empty() {
        return Err("bytes after the extension sections");
    }
    let mut tags = BTreeSet::new();
    if !sections.iter().all(|section| tags.insert(section.tag)) {
        return Err("repeated extension section tag");
    }
    Ok(sections)
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
pub(crate) fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    match postcard::take_from_bytes::<T>(bytes) {
        Ok((value, [])) => Some(value),
        _ => None,
    }
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
fn encode_enveloped<T: Serialize>(version: u32, payload: &T) -> Result<Vec<u8>> {
    // A `(version, payload)` pair encodes as the version varint followed by the
    // payload bytes, which is exactly what `decode_enveloped` peels apart.
    encode(&(version, payload))
}

/// Frozen baseline codec path; keep independent of accumulator-aware decoding.
fn envelope_payload<'a>(
    kind: &'static str,
    version: u32,
    run_key: RunKey,
    bytes: &'a [u8],
) -> Result<&'a [u8]> {
    // The version is checked before the payload is touched, so a pre-envelope blob
    // is rejected on its first varint and never partially decoded.
    let observed = postcard::take_from_bytes::<u32>(bytes).ok();
    match observed {
        Some((found, rest)) if found == version => Ok(rest),
        Some((found, _)) => Err(BlobFormatError {
            kind,
            run_key,
            observed: found,
        }
        .into()),
        None => Err(BlobFormatError {
            kind,
            run_key,
            observed: 0,
        }
        .into()),
    }
}
