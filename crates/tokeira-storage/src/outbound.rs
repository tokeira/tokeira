//! Durable outbound descriptors and mechanical derivation from committed causes.
//!
//! Operation rows are replaceable indexes over run state. Close intents are
//! immutable obligations authored once with a terminal transition; repair must
//! never recreate a retired intent. Neither table grants permission to mutate a
//! destination: delivery revalidates the source and destination in the effect's
//! transaction, and source resolution retains its original acquisition fence.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_kernel::{
    ChildOutcomeIdentity, ChildWorkflowState, HistoryEventKind, NexusOperationCancellationState,
    OperationIncarnation, OperationKey, OperationKind, ParentClosePolicy, StartRequest, Transition,
    WorkflowState,
};
use tokeira_types::{NamespaceId, RunId, RunKey, ShardId, TransitionSeq, WorkflowId};

pub(crate) mod preparation;
pub(crate) mod successor;
pub use preparation::{PreparationToken, PublicationResult, RunPreparation};

/// Maximum descriptors returned by one discovery page.
pub const OUTBOUND_PAGE_LIMIT: usize = 100;

pub(crate) fn validate_page_limit(limit: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=OUTBOUND_PAGE_LIMIT).contains(&limit),
        "outbound page limit must be 1..={OUTBOUND_PAGE_LIMIT}"
    );
    Ok(())
}

/// Fail-closed interpretation errors, distinct from transient storage failures.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// Ordinary writers must not guess metadata absent from legacy state.
    #[error("run {0:?} requires checked outbound-recovery preparation before writing")]
    LegacyState(RunKey),
    /// A future required layout cannot be treated as an empty old layout.
    #[error("run {run:?} has unsupported outbound-state version {version}")]
    UnsupportedVersion {
        /// Run whose format is unsupported.
        run: RunKey,
        /// Observed format version.
        version: u32,
    },
    /// Invalid committed identity needs operator diagnosis, never guessed delivery.
    #[error("invalid outbound identity for {run:?}: {detail}")]
    InvalidIdentity {
        /// Source run.
        run: RunKey,
        /// Stable diagnostic describing the violated contract.
        detail: &'static str,
    },
    /// A reference no longer names the pending source incarnation.
    #[error("outbound source incarnation is obsolete")]
    Obsolete,
    /// Immutable preparation bytes or part inventory do not match their manifest.
    #[error("outbound preparation is incomplete or corrupt: {0}")]
    InvalidPreparation(&'static str),
    /// An exceptional successor owns the workflow pointer until publication.
    #[error("workflow is reserved for pending successor {successor:?}; retry after recovery")]
    SuccessorReserved {
        /// Frozen target; callers must not substitute a newly selected run.
        successor: RunKey,
    },
}

/// A payload-free, reconstructible source-operation index entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRow {
    /// Authoritative initiation and attempt generation.
    pub incarnation: OperationIncarnation,
    /// Execution home owning source resolution and discovery.
    pub source_home: ShardId,
    /// Earliest eligible attempt; persisted Nexus backoff is authoritative.
    pub due_at: OffsetDateTime,
    /// Target identity for routing/retention, absent for external Nexus endpoints.
    pub target: Option<(NamespaceId, WorkflowId)>,
}

/// Immutable close-obligation category; values are persisted in SQL rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i16)]
pub enum IntentKind {
    /// Parent close policy for one child initiation.
    ParentClose = 1,
    /// Final child outcome for its parent.
    ChildResult = 2,
    /// A frozen successor that did not fit the ordinary atomic pair.
    Successor = 3,
}

/// Collision-free identity of one obligation authored by one close transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IntentKey {
    /// Run that authored the close.
    pub source_run: RunKey,
    /// Source transition sequence at close.
    pub close_seq: TransitionSeq,
    /// Obligation category.
    pub kind: IntentKind,
    /// Stable position after sorting the affected initiation identities.
    pub ordinal: u32,
}

impl IntentKey {
    /// Fixed-width SQL key whose lexical order is the Rust key's field order.
    #[must_use]
    pub fn storage_id(self) -> String {
        format!(
            "{:016x}-{:04x}-{:08x}",
            self.close_seq.0, self.kind as i16, self.ordinal
        )
    }
}

/// Immutable effect recipe; large payloads use the repository's bounded parts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IntentPayload {
    /// Apply policy only to this parent's child chain, never an unrelated reuse.
    ParentClose {
        /// Destination namespace.
        namespace_id: NamespaceId,
        /// Destination workflow.
        workflow_id: WorkflowId,
        /// Parent initiation; required even when no child start was confirmed.
        initiating_event: i64,
        /// Confirmed or prepared first child run, when known.
        first_child_run: Option<RunId>,
        /// ABANDON is never an intent.
        policy: ParentClosePolicy,
    },
    /// Hydrate the exact terminal event; no payload or retry-state truncation.
    ChildResult {
        /// Original parent run; reset redirection is checked at delivery time.
        parent_run: RunKey,
        /// Child initiation and chain fence.
        identity: ChildOutcomeIdentity,
        /// Source terminal event carrying the full result.
        terminal_event: i64,
    },
    /// Exceptional continuation, frozen before closing the predecessor.
    Successor {
        /// Selected successor and all accepted/inherited start values.
        start: Box<StartRequest>,
    },
}

/// Immutable obligation committed with its source close.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntentRow {
    /// Exact retirement identity; an executor cannot delete a broader source range.
    pub key: IntentKey,
    /// Source execution home.
    pub source_home: ShardId,
    /// Source close clock, used for diagnostics, never for target start clocks.
    pub created_at: OffsetDateTime,
    /// Workflow lineage whose state/history must survive until this obligation
    /// settles, including reset redirects through logically deleted runs.
    pub target: Option<(NamespaceId, WorkflowId)>,
    /// Versioned immutable recipe.
    pub payload: IntentPayload,
}

/// Discovery carries only this bounded descriptor; hydrate after admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntentDescriptor {
    /// Immutable source identity.
    pub key: IntentKey,
    /// Execution home owning retirement.
    pub source_home: ShardId,
    /// Original close clock.
    pub created_at: OffsetDateTime,
}

impl From<&IntentRow> for IntentDescriptor {
    fn from(row: &IntentRow) -> Self {
        Self {
            key: row.key,
            source_home: row.source_home,
            created_at: row.created_at,
        }
    }
}

/// A finite keyset scan. A round's upper bound never grows under new insertions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanBound<K> {
    /// Capture the greatest key in this read's snapshot.
    Capture,
    /// Continue the same finite round, including its greatest key.
    Through(K),
}

/// A bounded page and its captured round boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboundPage<K, V> {
    /// Greatest key at round start; absent when the round contains no rows.
    pub through: Option<K>,
    /// Ordered descriptors, at most the requested limit.
    pub rows: Vec<V>,
}

/// Source authority read in the same transaction as the destination effect.
///
/// A hydrate-then-submit check alone has a TOCTOU window: parent close could retire
/// policy against an absent child while an old start subsequently creates it. The
/// source read in the destination transaction conflicts with that close on DSQL;
/// memory uses the same store lock for both. A source lease is not a target fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryGuard {
    /// Child creation also fences the current execution used to evaluate its
    /// reuse policy; a newer closed execution must not bypass that evaluation.
    ChildStart {
        /// Exact pending child initiation.
        incarnation: OperationIncarnation,
        /// Source sequence used for the recipe.
        source_seq: TransitionSeq,
        /// Current pointer observed before applying the accepted reuse policy.
        expected_current: Option<RunKey>,
    },
    /// Pending operation and captured source sequence.
    Operation {
        /// Expected initiation/attempt.
        incarnation: OperationIncarnation,
        /// State whose validated recipe was hydrated.
        source_seq: TransitionSeq,
    },
    /// Exact immutable intent, still present at the target commit.
    Intent(IntentKey),
    /// Parent-close delivery also fences the current child generation selected
    /// by its lineage lookup, so a concurrent continuation forces rediscovery.
    CurrentIntent {
        /// Immutable committed authority.
        key: IntentKey,
        /// Current target observed by the executor.
        expected_current: Option<RunKey>,
    },
}

impl DeliveryGuard {
    /// Provenance recorded with a destination request, absent for close intents.
    #[must_use]
    pub fn operation(self) -> Option<OperationIncarnation> {
        match self {
            Self::Operation { incarnation, .. } | Self::ChildStart { incarnation, .. } => {
                Some(incarnation)
            }
            Self::Intent(_) | Self::CurrentIntent { .. } => None,
        }
    }
}

/// Verified retirement proof supplied by the runtime's typed outcome path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntentRetirement {
    /// Exact obligation whose outcome was verified.
    pub key: IntentKey,
    /// Target outcome, distinguished from generic transport success.
    pub outcome: DefinitiveOutcome,
}

/// Only outcomes that make another delivery unnecessary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefinitiveOutcome {
    /// Verified destination commit, including a proven duplicate.
    Applied {
        /// Destination execution verified against the obligation's lineage.
        run: RunKey,
        /// Destination's committed sequence carrying the result.
        seq: TransitionSeq,
    },
    /// Exact target/relationship no longer exists; not a transient lookup failure.
    Absent,
    /// The parent/child initiation or chain has authoritatively changed.
    Obsolete,
}

/// Derive every operation from state, independent of notifications/dispatch ops.
pub fn derive_operations(
    state: &WorkflowState,
    home: ShardId,
) -> Result<Vec<OperationRow>, RecoveryError> {
    let outbound = state
        .outbound
        .as_ref()
        .ok_or(RecoveryError::LegacyState(state.run_key))?;
    if outbound.version != tokeira_kernel::OUTBOUND_STATE_VERSION {
        return Err(RecoveryError::UnsupportedVersion {
            run: state.run_key,
            version: outbound.version,
        });
    }
    let mut rows = Vec::new();
    let mut push = |kind, initiating_event, requested_event, attempt, due_at, target| {
        rows.push(OperationRow {
            incarnation: OperationIncarnation {
                key: OperationKey {
                    source_run: state.run_key,
                    kind,
                    initiating_event,
                },
                requested_event,
                attempt,
            },
            source_home: home,
            due_at,
            target,
        });
    };
    if state.is_open() {
        for child in state
            .children
            .values()
            .filter(|child| child.started_event_id.is_none())
        {
            push(
                OperationKind::ChildStart,
                child.initiated_event_id,
                0,
                0,
                state.started_at,
                Some((child.namespace_id, child.child_workflow_id.clone())),
            );
        }
        for signal in state.pending_external_signals.values() {
            push(
                OperationKind::ExternalSignal,
                signal.initiated_event_id,
                0,
                0,
                state.started_at,
                Some((
                    signal.target_namespace_id,
                    signal.target_workflow_id.clone(),
                )),
            );
        }
        for cancel in state.pending_external_cancels.values() {
            push(
                OperationKind::ExternalCancel,
                cancel.initiated_event_id,
                0,
                0,
                state.started_at,
                Some((
                    cancel.target_namespace_id,
                    cancel.target_workflow_id.clone(),
                )),
            );
        }
        for operation in state.pending_nexus_operations.values() {
            if !operation.started {
                push(
                    OperationKind::NexusStart,
                    operation.scheduled_event_id,
                    0,
                    operation.attempt,
                    operation.next_attempt_at.unwrap_or(operation.scheduled_at),
                    None,
                );
            }
            if let Some(cancel) = &operation.cancellation
                && matches!(
                    cancel.state,
                    NexusOperationCancellationState::Scheduled
                        | NexusOperationCancellationState::BackingOff
                )
            {
                push(
                    OperationKind::NexusCancel,
                    operation.scheduled_event_id,
                    cancel.requested_event_id,
                    cancel.attempt,
                    cancel
                        .next_attempt_at
                        .or(cancel.requested_time)
                        .unwrap_or(operation.scheduled_at),
                    None,
                );
            }
        }
    }
    for release in outbound.child_releases.values() {
        if state.is_open() || release.parent_close_policy == ParentClosePolicy::Abandon {
            push(
                OperationKind::ChildRelease,
                release.initiating_event,
                0,
                0,
                state.started_at,
                Some((release.namespace_id, release.workflow_id.clone())),
            );
        }
    }
    rows.sort_by_key(|row| row.incarnation.key);
    if rows
        .windows(2)
        .any(|pair| pair[0].incarnation.key == pair[1].incarnation.key)
        || rows
            .iter()
            .any(|row| row.incarnation.key.initiating_event <= 0)
    {
        return Err(RecoveryError::InvalidIdentity {
            run: state.run_key,
            detail: "duplicate or non-positive initiating event",
        });
    }
    Ok(rows)
}

/// Construct close obligations once, while the source transition is committing.
/// Repair never calls this: absence after retirement is authoritative.
pub fn derive_close_intents(
    before: Option<&WorkflowState>,
    transition: &Transition,
    home: ShardId,
) -> Result<Vec<IntentRow>, RecoveryError> {
    let state = &transition.next_state;
    let terminal = transition.history_events.iter().rev().find(|event| {
        matches!(
            event.kind,
            HistoryEventKind::WorkflowExecutionCompleted { .. }
                | HistoryEventKind::WorkflowExecutionFailed { .. }
                | HistoryEventKind::WorkflowExecutionCanceled { .. }
                | HistoryEventKind::WorkflowExecutionTerminated { .. }
                | HistoryEventKind::WorkflowExecutionTimedOut { .. }
                | HistoryEventKind::WorkflowExecutionContinuedAsNew { .. }
        )
    });
    let Some(terminal) = terminal else {
        return Ok(Vec::new());
    };
    if before.is_some_and(|state| !state.is_open()) || state.is_open() {
        return Err(RecoveryError::InvalidIdentity {
            run: state.run_key,
            detail: "close event is not an open-to-closed transition",
        });
    }
    let mut children = before
        .map(|state| state.children.clone())
        .unwrap_or_default();
    // Fold only child identity records preceding this close. This includes a
    // child initiated and closed in one WFT, which is absent from both hot states
    // after the kernel clears its open-child set. History, not dispatch, is proof.
    for event in &transition.history_events {
        match &event.kind {
            HistoryEventKind::StartChildWorkflowExecutionInitiated {
                child_workflow_id,
                namespace_id,
                namespace,
                workflow_type,
                header,
                parent_close_policy,
                ..
            }
            | HistoryEventKind::StartChildWorkflowExecutionInitiatedV2 {
                child_workflow_id,
                namespace_id,
                namespace,
                workflow_type,
                header,
                parent_close_policy,
                ..
            } => {
                children.insert(
                    child_workflow_id.clone(),
                    ChildWorkflowState {
                        child_workflow_id: child_workflow_id.clone(),
                        namespace_id: *namespace_id,
                        namespace: namespace.clone(),
                        workflow_type: workflow_type.clone(),
                        header: header.clone(),
                        child_run_id: None,
                        initiated_event_id: event.event_id,
                        started_event_id: None,
                        parent_close_policy: *parent_close_policy,
                    },
                );
            }
            HistoryEventKind::ChildWorkflowExecutionStarted {
                child_workflow_id,
                child_run_id,
                ..
            } => {
                if let Some(child) = children.get_mut(child_workflow_id) {
                    child.child_run_id = Some(*child_run_id);
                }
            }
            HistoryEventKind::StartChildWorkflowExecutionFailed {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionCompleted {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionFailed {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionCanceled {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionTerminated {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionTerminatedV2 {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionTimedOut {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionTimedOutV2 {
                child_workflow_id, ..
            } => {
                children.remove(child_workflow_id);
            }
            _ => {}
        }
    }
    let mut rows = Vec::new();
    let mut push = |kind, ordinal, payload: IntentPayload| {
        let target = match &payload {
            IntentPayload::ParentClose {
                namespace_id,
                workflow_id,
                ..
            } => Some((*namespace_id, workflow_id.clone())),
            IntentPayload::ChildResult { .. } => {
                state.parent_workflow_id.as_ref().map(|workflow| {
                    (
                        state.parent_namespace_id.unwrap_or(state.namespace_id),
                        workflow.clone(),
                    )
                })
            }
            IntentPayload::Successor { start } => {
                Some((start.namespace_id, start.workflow_id.clone()))
            }
        };
        rows.push(IntentRow {
            key: IntentKey {
                source_run: state.run_key,
                close_seq: state.transition_seq,
                kind,
                ordinal,
            },
            source_home: home,
            created_at: terminal.happened_at,
            target,
            payload,
        })
    };
    // A historical reset link alone does not suppress a later ordinary close.
    // Temporal checks the terminating identity as well as the link; the pinned
    // AllowResetWithPendingChildren default is true (transfer_queue_active_task_executor.go,
    // ndc/workflow_resetter.go, common/dynamicconfig/constants.go @ v1.31.0).
    let reset_termination = matches!(&terminal.kind,
        HistoryEventKind::WorkflowExecutionTerminated { identity, .. } if identity == "history-resetter");
    if !(reset_termination && state.reset_run_id.is_some()) {
        let mut children: Vec<_> = children.into_values().collect();
        children.sort_by_key(|child| child.initiated_event_id);
        for (ordinal, child) in children.into_iter().enumerate() {
            if child.parent_close_policy == ParentClosePolicy::Abandon {
                continue;
            }
            let first_child_run = child.child_run_id.or_else(|| {
                state
                    .outbound
                    .as_ref()
                    .and_then(|outbound| outbound.child_seeds.get(&child.initiated_event_id))
                    .map(|seed| seed.child_run_id)
            });
            let ordinal = u32::try_from(ordinal).map_err(|_| RecoveryError::InvalidIdentity {
                run: state.run_key,
                detail: "too many child close obligations",
            })?;
            push(
                IntentKind::ParentClose,
                ordinal,
                IntentPayload::ParentClose {
                    namespace_id: child.namespace_id,
                    workflow_id: child.child_workflow_id,
                    initiating_event: child.initiated_event_id,
                    first_child_run,
                    policy: child.parent_close_policy,
                },
            );
        }
    }
    if !reset_termination {
        let continues = match &terminal.kind {
            HistoryEventKind::WorkflowExecutionContinuedAsNew { .. } => true,
            HistoryEventKind::WorkflowExecutionCompleted {
                new_execution_run_id,
                ..
            }
            | HistoryEventKind::WorkflowExecutionFailed {
                new_execution_run_id,
                ..
            }
            | HistoryEventKind::WorkflowExecutionTimedOut {
                new_execution_run_id,
                ..
            } => new_execution_run_id.is_some(),
            _ => false,
        };
        // transfer_queue_active_task_executor.go @ v1.31.0: a retry/cron/CAN
        // intermediate close must not resolve the parent's pending child.
        if let Some(parent_run) = state.parent_run_key
            && !continues
        {
            push(
                IntentKind::ChildResult,
                0,
                IntentPayload::ChildResult {
                    parent_run,
                    identity: ChildOutcomeIdentity {
                        parent_initiated_event: state.parent_initiated_event_id,
                        child_first_run: state.first_execution_run_id.unwrap_or(state.run_id),
                        child_final_run: state.run_id,
                    },
                    terminal_event: terminal.event_id,
                },
            );
        }
    }
    Ok(rows)
}
