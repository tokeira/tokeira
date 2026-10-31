//! Immutable publication manifests shared by reset and deferred successors.
//!
//! A manifest describes the complete semantic write before staging begins. State
//! and history use their required envelopes: postcard alone intentionally omits
//! state extensions and would silently discard the child handshake metadata.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokeira_kernel::{ActivityOp, DispatchOp, RequestDedupeOp, TimerOp, Transition, WorkflowState};
use tokeira_types::{RequestId, RunId, RunKey, ShardId, TransitionSeq};
use uuid::Uuid;

use super::{IntentKey, RecoveryError};
use crate::codec;

const MANIFEST_VERSION: u32 = 1;
const DIGEST_DOMAIN: &[u8] = b"tokeira-run-publication-v1\0";

/// Complete reset or successor publication, constructed before any visible write.
#[derive(Clone, Debug, PartialEq)]
pub struct RunPreparation {
    /// Request identity including the current execution observed at admission.
    /// It must not memoize a reset across unrelated later current executions.
    pub request_key: String,
    /// Current pointer required by the final publication transaction.
    pub expected_current: Option<RunKey>,
    /// Existing base/current mutations, each with its own sequence fence.
    pub participants: Vec<Transition>,
    /// Complete new run, including its copied history and aligned principals.
    pub new_run: Box<Transition>,
    /// Deferred-successor intent to settle with publication, if applicable.
    pub successor_intent: Option<IntentKey>,
}

impl RunPreparation {
    /// Reject malformed publication sets before reserving any staging storage.
    pub fn validate(&self) -> Result<()> {
        let new = &self.new_run.next_state;
        ensure!(
            !self.request_key.is_empty() && self.request_key.len() <= 256,
            "preparation request key must contain 1..=256 bytes"
        );
        ensure!(
            self.new_run.expected_seq == TransitionSeq::ZERO && new.status.is_open(),
            "preparation requires an absent, open successor"
        );
        ensure!(
            self.participants.len() <= 2,
            "a reset has at most base and current participants"
        );
        let mut seen = std::collections::BTreeSet::from([new.run_key]);
        for transition in self
            .participants
            .iter()
            .chain(std::iter::once(self.new_run.as_ref()))
        {
            let state = &transition.next_state;
            ensure!(
                state.namespace_id == new.namespace_id && state.workflow_id == new.workflow_id,
                "publication participants must share an execution home"
            );
            ensure!(
                state.transition_seq > transition.expected_seq,
                "publication transitions must advance their sequence"
            );
            ensure!(
                transition.history_events.len() == transition.event_principals.len(),
                "publication history/principal sidecar length mismatch"
            );
            ensure!(
                RunKey::derive(state.namespace_id, &state.workflow_id, state.run_id)
                    == state.run_key,
                "publication run identity does not match its key"
            );
            codec::workflow_state_encoded_len(state)?;
        }
        for transition in &self.participants {
            ensure!(
                transition.expected_seq != TransitionSeq::ZERO
                    && seen.insert(transition.next_state.run_key),
                "publication contains an absent or repeated existing participant"
            );
        }
        if let Some(intent) = self.successor_intent {
            ensure!(
                self.participants.is_empty() && self.expected_current == Some(intent.source_run),
                "deferred publication must preserve its predecessor reservation"
            );
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(postcard::to_allocvec(&StoredPreparation {
            version: MANIFEST_VERSION,
            request_key: self.request_key.clone(),
            expected_current: self.expected_current,
            participants: self
                .participants
                .iter()
                .map(StoredTransition::encode)
                .collect::<Result<_>>()?,
            new_run: StoredTransition::encode(&self.new_run)?,
            successor_intent: self.successor_intent,
        })?)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let (stored, rest): (StoredPreparation, _) = postcard::take_from_bytes(bytes)?;
        ensure!(
            stored.version == MANIFEST_VERSION && rest.is_empty(),
            "unsupported or trailing publication manifest data"
        );
        let value = Self {
            request_key: stored.request_key,
            expected_current: stored.expected_current,
            participants: stored
                .participants
                .into_iter()
                .map(StoredTransition::decode)
                .collect::<Result<_>>()?,
            new_run: Box::new(stored.new_run.decode()?),
            successor_intent: stored.successor_intent,
        };
        value.validate()?;
        Ok(value)
    }
}

/// Exact preparation generation; stale pages and purgers cannot borrow a new one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparationToken {
    /// Opaque preparation row identity.
    pub id: Uuid,
    /// Generation checked in every stage, publish and purge transaction.
    pub generation: Uuid,
    /// Run whose storage remains invisible until publication.
    pub target_run: RunKey,
    /// Selected public run identity, available even before all manifest parts
    /// arrive. A reset retry rebuilds with this identity instead of drawing again.
    pub run_id: RunId,
    /// Selected start clock for rebuilding an interrupted manifest upload.
    pub started_at: OffsetDateTime,
    /// Execution home whose original ownership fence authorizes publication.
    pub home: ShardId,
    /// Digest of the exact versioned manifest bytes.
    pub manifest_digest: [u8; 32],
}

impl PreparationToken {
    pub(crate) fn new(state: &WorkflowState, home: ShardId, manifest: &[u8]) -> Self {
        Self {
            id: Uuid::new_v4(),
            generation: Uuid::new_v4(),
            target_run: state.run_key,
            run_id: state.run_id,
            started_at: state.started_at,
            home,
            manifest_digest: manifest_digest(manifest),
        }
    }

    pub(crate) fn decode_manifest(&self, bytes: &[u8]) -> Result<RunPreparation> {
        if manifest_digest(bytes) != self.manifest_digest {
            return Err(RecoveryError::InvalidPreparation("manifest digest mismatch").into());
        }
        let manifest = RunPreparation::decode(bytes)?;
        ensure!(
            manifest.new_run.next_state.run_key == self.target_run
                && manifest.new_run.next_state.run_id == self.run_id
                && manifest.new_run.next_state.started_at == self.started_at,
            "manifest target differs from preparation token"
        );
        Ok(manifest)
    }
}

pub(crate) fn manifest_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update(bytes);
    hasher.finalize().into()
}

/// Publication never reports success for staging alone.
#[derive(Clone, Debug, PartialEq)]
pub enum PublicationResult {
    /// All participants and the new current execution committed together.
    Applied {
        /// Authoritative new state for each existing participant.
        participants: Vec<WorkflowState>,
        /// Authoritative published successor.
        new_run: Box<WorkflowState>,
    },
    /// A lost reply was resolved from this exact published generation.
    AlreadyPublished {
        /// Current authoritative state of the published run.
        new_run: Box<WorkflowState>,
    },
    /// A participant or current pointer changed; nothing became visible.
    Conflict {
        /// Stable diagnostic for recomputing from current authority.
        reason: String,
    },
}

/// A phase change participates in the same OCC domain as every staging page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PreparationPhase {
    Materializing,
    Ready,
    Purging,
    Published,
    /// Local deadline/dispatch reconstruction completed; manifest may be reclaimed.
    Reconciled,
}

/// In-memory durable counterpart of the DSQL manifest and its parts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PreparationRecord {
    pub token: PreparationToken,
    pub request_key: String,
    pub manifest: Vec<u8>,
    pub phase: PreparationPhase,
}

#[derive(Serialize, Deserialize)]
struct StoredPreparation {
    version: u32,
    request_key: String,
    expected_current: Option<RunKey>,
    participants: Vec<StoredTransition>,
    new_run: StoredTransition,
    successor_intent: Option<IntentKey>,
}

#[derive(Serialize, Deserialize)]
struct StoredTransition {
    run: RunKey,
    expected_seq: TransitionSeq,
    state: Vec<u8>,
    events: Vec<u8>,
    principals: Vec<u8>,
    requests: Vec<RequestId>,
    activities: Vec<ActivityOp>,
    timers: Vec<TimerOp>,
    dispatches: Vec<DispatchOp>,
    numbered_at_close: u32,
    growth_limits: Option<[usize; 7]>,
}

impl StoredTransition {
    fn encode(transition: &Transition) -> Result<Self> {
        Ok(Self {
            run: transition.next_state.run_key,
            expected_seq: transition.expected_seq,
            state: codec::encode_workflow_state(&transition.next_state)?,
            events: codec::encode_history_events(&transition.history_events)?,
            principals: codec::encode_history_principals(&transition.event_principals)?,
            requests: transition
                .request_dedupe_ops
                .iter()
                .map(|op| op.request_id.clone())
                .collect(),
            activities: transition.activity_ops.to_vec(),
            timers: transition.timer_ops.to_vec(),
            dispatches: transition.dispatch_ops.to_vec(),
            numbered_at_close: transition.events_numbered_at_close,
            growth_limits: transition.growth_limits.map(|limits| {
                [
                    limits.history_size_error,
                    limits.history_size_warn,
                    limits.history_count_error,
                    limits.history_count_warn,
                    limits.state_size_error,
                    limits.state_size_warn,
                    limits.transaction_size,
                ]
            }),
        })
    }

    fn decode(self) -> Result<Transition> {
        Ok(Transition {
            expected_seq: self.expected_seq,
            next_state: codec::decode_workflow_state(self.run, &self.state)?,
            history_events: codec::decode_history_events(self.run, &self.events)?.into(),
            event_principals: codec::decode_history_principals(&self.principals)?.into(),
            request_dedupe_ops: self
                .requests
                .into_iter()
                .map(|request_id| RequestDedupeOp { request_id })
                .collect(),
            activity_ops: self.activities.into(),
            timer_ops: self.timers.into(),
            dispatch_ops: self.dispatches.into(),
            events_numbered_at_close: self.numbered_at_close,
            growth_limits: self
                .growth_limits
                .map(|v| tokeira_kernel::limits::RunGrowthLimits {
                    history_size_error: v[0],
                    history_size_warn: v[1],
                    history_count_error: v[2],
                    history_count_warn: v[3],
                    state_size_error: v[4],
                    state_size_warn: v[5],
                    transaction_size: v[6],
                }),
        })
    }
}
