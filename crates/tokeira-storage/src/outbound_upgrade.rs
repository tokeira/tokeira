//! Reviewed, stopped-cluster preparation of legacy outbound state.
//!
//! The operator's stopped-writer assertion is indispensable: an expired lease
//! cannot prove that an old unfenced process has stopped. Each bounded write
//! shares the exclusive marker revision and source digest; restart resumes that
//! marker without inventing missing policy or historical delivery receipts.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokeira_kernel::{
    ChildReleaseState, ChildStartGate, HistoryEventKind, LoadedRun, WorkflowIdReusePolicy,
    WorkflowState,
};
use tokeira_types::{RunId, RunKey, TransitionSeq};

use crate::{RunRepository, codec, schema_contract::sha256_hex};

/// Singleton and algorithm identity; changing the algorithm requires a new name.
pub const OUTBOUND_UPGRADE: &str = "outbound-recovery-v1";
/// Bounded diagnostic output; counters still include omitted records.
pub const INSPECTION_LIMIT: usize = 100;

/// Durable continuation, committed atomically with each prepared source.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundUpgradeProgress {
    /// Exclusive revision checked by every page and completion write.
    pub revision: u64,
    /// Original reviewed inventory digest, retained across interruption.
    pub plan_digest: String,
    /// Exact operator repair evidence used by this preparation.
    pub repairs_digest: String,
    /// Last fully prepared run in UUID order.
    pub after: Option<RunKey>,
    /// Number of source records committed by this preparation.
    pub examined: u64,
    /// Closed legacy records whose old deliveries cannot be established.
    pub historical_unknown: u64,
    /// Only this final marker permits runtime construction.
    pub complete: bool,
}

/// Payload-free stopped-cluster status. No lease credentials or endpoints appear.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundUpgradeStatus {
    /// Existing marker, absent before the first preparation.
    pub progress: Option<OutboundUpgradeProgress>,
    /// Current live lease count, supplementary to stopped-writer confirmation.
    pub live_leases: u64,
    /// Durable close obligations, including sources already logically deleted.
    pub pending_intents: u64,
    /// Oldest outstanding close obligation, without exposing its payload.
    pub oldest_intent_at: Option<time::OffsetDateTime>,
}

/// Operator-supplied original evidence, bound to the reviewed source state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundRepair {
    /// Source whose accepted initiation needs missing evidence.
    pub run: RunKey,
    /// Exact source sequence in the review.
    pub source_seq: TransitionSeq,
    /// Payload-free fingerprint printed by planning for this source.
    pub source_digest: String,
    /// Child initiation, never a child workflow ID alone.
    pub initiation: i64,
    /// Original accepted policy, if confirmed by the operator.
    pub original_policy: Option<WorkflowIdReusePolicy>,
    /// Existing first child to verify and adopt; never permission to create it.
    pub existing_child: Option<RunId>,
}

/// One unresolved source; fields identify missing evidence without payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedRecovery {
    /// Authoritative source identity.
    pub run: RunKey,
    /// Reviewed sequence for a subsequent repair.
    pub source_seq: TransitionSeq,
    /// Reviewed fingerprint for a subsequent repair.
    pub source_digest: String,
    /// Low-cardinality work category.
    pub kind: String,
    /// Initiation requiring evidence, or zero for a run-level defect.
    pub initiation: i64,
    /// Field names only; diagnostics never print the history payload.
    pub missing_fields: Vec<String>,
}

/// Read-only review of all sources with bounded diagnostic detail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundUpgradePlan {
    /// Exact inventory, repair input and marker revision digest to approve.
    pub digest: String,
    /// Binary's schema identity; the CLI also verifies the database contract.
    pub schema_digest: String,
    /// Required outbound writer layout.
    pub writer_version: u32,
    /// Revision and live leases observed before the scan.
    pub status: OutboundUpgradeStatus,
    /// All examined sources, including already-current state.
    pub examined: u64,
    /// Sources still written with a legacy envelope.
    pub legacy: u64,
    /// Reconstructible pending operation count.
    pub pending: u64,
    /// Earliest initiation time among pending work and retained close obligations.
    pub oldest_pending_at: Option<time::OffsetDateTime>,
    /// Closed legacy sources with unknowable past delivery outcomes.
    pub historical_unknown: u64,
    /// Total unresolved records, including those omitted from the bounded list.
    pub unresolved_count: u64,
    /// First unresolved records in deterministic source/initiation order.
    pub unresolved: Vec<UnresolvedRecovery>,
}

/// One metadata-only transition. Public history is never rewritten by repair.
#[derive(Clone, Debug)]
pub struct OutboundUpgradeWrite {
    /// Exact pre-edit source fingerprint checked inside the page transaction.
    pub expected_digest: String,
    /// Complete state, with a single sequence advance only for legacy metadata.
    pub state: WorkflowState,
    /// Whether this source contributes to the recorded historical boundary.
    pub historical_unknown: bool,
}

/// Canonical fingerprint including extensions and the distinction between legacy
/// and empty current metadata. Hash-set iteration cannot change a review digest.
pub fn outbound_source_digest(state: &WorkflowState) -> Result<String> {
    let mut normalized = state.clone();
    let legacy = normalized.outbound.is_none();
    normalized.outbound.get_or_insert_with(Default::default);
    let mut admitted = normalized.admitted_updates.drain().collect::<Vec<_>>();
    admitted.sort();
    Ok(sha256_hex(&postcard::to_allocvec(&(
        legacy,
        codec::encode_workflow_state(&normalized)?,
        admitted,
    ))?))
}

fn repairs_digest(repairs: &[OutboundRepair]) -> Result<String> {
    ensure!(
        repairs.len() <= 1_000,
        "at most 1000 repairs may be reviewed together"
    );
    let mut keys = std::collections::BTreeSet::new();
    for repair in repairs {
        ensure!(
            keys.insert((repair.run, repair.initiation)),
            "duplicate child repair"
        );
        ensure!(
            repair.initiation > 0
                && (repair.original_policy.is_some() || repair.existing_child.is_some()),
            "repair needs an initiation and original evidence"
        );
    }
    Ok(sha256_hex(&postcard::to_allocvec(repairs)?))
}

fn unresolved(
    state: &WorkflowState,
    digest: &str,
    initiation: i64,
    field: &str,
) -> UnresolvedRecovery {
    UnresolvedRecovery {
        run: state.run_key,
        source_seq: state.transition_seq,
        source_digest: digest.into(),
        kind: if initiation == 0 {
            "run"
        } else {
            "child-start"
        }
        .into(),
        initiation,
        missing_fields: vec![field.into()],
    }
}

pub(crate) fn validate_page(
    expected: Option<&OutboundUpgradeProgress>,
    next: &OutboundUpgradeProgress,
    first: Option<RunKey>,
    write: Option<&OutboundUpgradeWrite>,
) -> Result<()> {
    ensure!(
        next.revision == expected.map_or(1, |progress| progress.revision + 1),
        "maintenance revision changed"
    );
    ensure!(
        next.plan_digest.len() == 64 && next.repairs_digest.len() == 64,
        "invalid maintenance digest"
    );
    match expected {
        None => ensure!(
            write.is_none() && !next.complete && next.after.is_none() && next.examined == 0,
            "invalid initial page"
        ),
        Some(previous) if previous.complete => ensure!(
            write.is_none()
                && !next.complete
                && next.after.is_none()
                && next.examined == 0
                && next.historical_unknown == previous.historical_unknown,
            "invalid replacement plan"
        ),
        Some(previous) => {
            if next.plan_digest != previous.plan_digest {
                // A newly reviewed inventory may repair an interrupted cutover.
                // Keep its completed prefix and historical-unknown boundary;
                // changing the digest alone never advances a source cursor.
                ensure!(
                    write.is_none()
                        && !next.complete
                        && next.after == previous.after
                        && next.examined == previous.examined
                        && next.historical_unknown == previous.historical_unknown,
                    "revised plan cannot change committed progress"
                );
                return Ok(());
            }
            ensure!(
                next.plan_digest == previous.plan_digest
                    && next.repairs_digest == previous.repairs_digest,
                "active preparation identity changed"
            );
            if let Some(write) = write {
                ensure!(
                    !next.complete
                        && first == Some(write.state.run_key)
                        && next.after == first
                        && next.examined == previous.examined + 1
                        && next.historical_unknown
                            == previous.historical_unknown + u64::from(write.historical_unknown),
                    "invalid source continuation"
                );
            } else {
                ensure!(
                    next.complete
                        && first.is_none()
                        && next.after == previous.after
                        && next.examined == previous.examined
                        && next.historical_unknown == previous.historical_unknown,
                    "preparation cannot complete before its scan"
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_write(prior: &WorkflowState, write: &OutboundUpgradeWrite) -> Result<()> {
    ensure!(
        outbound_source_digest(prior)? == write.expected_digest,
        "source changed since review; preparation page refused"
    );
    let mut unchanged = write.state.clone();
    unchanged.outbound = prior.outbound.clone();
    unchanged.transition_seq = prior.transition_seq;
    ensure!(
        &unchanged == prior,
        "maintenance may change outbound metadata only"
    );
    ensure!(
        write.state.transition_seq.0
            == prior.transition_seq.0 + u64::from(prior.outbound.is_none()),
        "invalid metadata transition sequence"
    );
    ensure!(
        write.state.outbound.is_some(),
        "preparation left a legacy envelope"
    );
    if prior.outbound.is_some() {
        ensure!(
            &write.state == prior,
            "current metadata cannot be replaced by legacy preparation"
        );
    }
    Ok(())
}

/// Matching every parent coordinate prevents an unrelated workflow-ID reuse from
/// being adopted. The original first run must exist; a later successor alone is
/// insufficient evidence of how the original child was created.
pub fn verified_legacy_child(
    parent: &WorkflowState,
    initiation: i64,
    child: &WorkflowState,
) -> bool {
    parent.children.values().any(|pending| {
        pending.initiated_event_id == initiation
            && pending.namespace_id == child.namespace_id
            && pending.child_workflow_id == child.workflow_id
    }) && child.parent_run_key == Some(parent.run_key)
        && child.parent_namespace_id == Some(parent.namespace_id)
        && child.parent_workflow_id.as_ref() == Some(&parent.workflow_id)
        && child.parent_run_id == Some(parent.run_id)
        && child.parent_initiated_event_id == initiation
        && child.first_execution_run_id.unwrap_or(child.run_id) == child.run_id
}

pub(crate) async fn classify<R: RunRepository + ?Sized>(
    repo: &R,
    state: WorkflowState,
    repairs: &[OutboundRepair],
) -> Result<(OutboundUpgradeWrite, Vec<UnresolvedRecovery>)> {
    let digest = outbound_source_digest(&state)?;
    let mut next = state.clone();
    let mut issues = Vec::new();
    for repair in repairs.iter().filter(|repair| repair.run == state.run_key) {
        ensure!(
            state.outbound.is_none()
                && state.is_open()
                && state
                    .children
                    .values()
                    .any(|child| child.initiated_event_id == repair.initiation
                        && child.child_run_id.is_none()),
            "repair must name an unresolved legacy child initiation"
        );
        ensure!(
            repair.source_seq == state.transition_seq && repair.source_digest == digest,
            "repair source changed; review a new plan for {:?}",
            state.run_key
        );
    }
    // Inspection also diagnoses damaged current-format records. It never
    // repairs them implicitly or replaces accepted metadata with a default.
    if let Some(metadata) = &state.outbound
        && state.is_open()
    {
        for row in crate::derive_operations(&state, tokeira_types::ShardId(0))? {
            let initiation = row.incarnation.key.initiating_event;
            let event = repo.read_history_event(state.run_key, initiation).await?;
            let valid = match row.incarnation.key.kind {
                tokeira_kernel::OperationKind::ChildStart => event.as_ref().is_some_and(|event| {
                    matches!(
                        event.kind,
                        HistoryEventKind::StartChildWorkflowExecutionInitiated { .. }
                            | HistoryEventKind::StartChildWorkflowExecutionInitiatedV2 { .. }
                    )
                }),
                tokeira_kernel::OperationKind::ExternalSignal => {
                    event.as_ref().is_some_and(|event| {
                        matches!(
                            event.kind,
                            HistoryEventKind::SignalExternalWorkflowExecutionInitiated { .. }
                        )
                    })
                }
                tokeira_kernel::OperationKind::ExternalCancel => {
                    event.as_ref().is_some_and(|event| {
                        matches!(
                            event.kind,
                            HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated { .. }
                        )
                    })
                }
                tokeira_kernel::OperationKind::NexusStart
                | tokeira_kernel::OperationKind::NexusCancel => {
                    event.as_ref().is_some_and(|event| {
                        matches!(event.kind, HistoryEventKind::NexusOperationScheduled { .. })
                    })
                }
                tokeira_kernel::OperationKind::ChildRelease => true,
            };
            if !valid {
                let mut issue = unresolved(&state, &digest, initiation, "initiation_history");
                issue.kind = format!("{:?}", row.incarnation.key.kind);
                issues.push(issue);
            } else if row.incarnation.key.kind == tokeira_kernel::OperationKind::ChildStart
                && event
                    .as_ref()
                    .and_then(|event| event.kind.child_reuse_policy())
                    .is_none()
                && !metadata.legacy_child_policies.contains_key(&initiation)
                && !metadata.legacy_adopted_children.contains_key(&initiation)
            {
                issues.push(unresolved(
                    &state,
                    &digest,
                    initiation,
                    "original_reuse_policy_or_verified_existing_child",
                ));
            }
        }
    }
    if state.outbound.is_none() {
        next.outbound = Some(Default::default());
        next.transition_seq = TransitionSeq(
            state
                .transition_seq
                .0
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("source sequence exhausted"))?,
        );
        if state.is_open() {
            for (initiation, kind) in state
                .pending_external_signals
                .keys()
                .map(|event| (*event, "external-signal"))
                .chain(
                    state
                        .pending_external_cancels
                        .keys()
                        .map(|event| (*event, "external-cancel")),
                )
                .chain(
                    state
                        .pending_nexus_operations
                        .values()
                        .map(|operation| (operation.scheduled_event_id, "nexus")),
                )
            {
                let event = repo.read_history_event(state.run_key, initiation).await?;
                if !event.as_ref().is_some_and(|event| event.event_id == initiation && match kind {
                    "external-signal" => matches!(event.kind, HistoryEventKind::SignalExternalWorkflowExecutionInitiated { .. }),
                    "external-cancel" => matches!(event.kind, HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated { .. }),
                    _ => matches!(event.kind, HistoryEventKind::NexusOperationScheduled { .. }),
                }) {
                    let mut issue = unresolved(&state, &digest, initiation, "initiation_history"); issue.kind = kind.into(); issues.push(issue);
                }
            }
            for pending in state
                .children
                .values()
                .filter(|child| child.child_run_id.is_none())
            {
                let initiation = pending.initiated_event_id;
                let repair = repairs
                    .iter()
                    .find(|repair| repair.run == state.run_key && repair.initiation == initiation);
                if let Some(repair) = repair {
                    ensure!(
                        repair.source_seq == state.transition_seq && repair.source_digest == digest,
                        "repair source changed; review a new plan for {:?}",
                        state.run_key
                    );
                }
                let event = repo.read_history_event(state.run_key, initiation).await?;
                let event = event.as_ref().filter(|event| {
                    event.event_id == initiation
                        && matches!(
                            event.kind,
                            HistoryEventKind::StartChildWorkflowExecutionInitiated { .. }
                                | HistoryEventKind::StartChildWorkflowExecutionInitiatedV2 { .. }
                        )
                });
                if event.is_none() {
                    issues.push(unresolved(
                        &state,
                        &digest,
                        initiation,
                        "initiation_history",
                    ));
                    continue;
                }
                if let Some(supplied) = repair.and_then(|repair| repair.original_policy)
                    && let Some(accepted) = event.and_then(|event| event.kind.child_reuse_policy())
                {
                    ensure!(
                        supplied == accepted,
                        "repair contradicts accepted reuse policy"
                    );
                }
                let existing = match repair.and_then(|repair| repair.existing_child) {
                    Some(run) => Some(RunKey::derive(
                        pending.namespace_id,
                        &pending.child_workflow_id,
                        run,
                    )),
                    None => {
                        repo.find_latest_run(pending.namespace_id, &pending.child_workflow_id)
                            .await?
                    }
                };
                let mut adopted = false;
                if let Some(key) = existing
                    && let LoadedRun::Existing(candidate) = repo.load_outbound_source(key).await?
                {
                    let first = candidate.first_execution_run_id.unwrap_or(candidate.run_id);
                    let first_key =
                        RunKey::derive(pending.namespace_id, &pending.child_workflow_id, first);
                    if let LoadedRun::Existing(child) = repo.load_outbound_source(first_key).await?
                        && verified_legacy_child(&state, initiation, &child)
                    {
                        next.outbound
                            .as_mut()
                            .expect("metadata installed")
                            .legacy_adopted_children
                            .insert(initiation, child.run_id);
                        adopted = true;
                    }
                }
                if repair.is_some_and(|repair| repair.existing_child.is_some()) && !adopted {
                    issues.push(unresolved(
                        &state,
                        &digest,
                        initiation,
                        "verified_existing_child_lineage",
                    ));
                }
                if let Some(policy) = event
                    .and_then(|event| event.kind.child_reuse_policy())
                    .or_else(|| repair.and_then(|repair| repair.original_policy))
                {
                    next.outbound
                        .as_mut()
                        .expect("metadata installed")
                        .legacy_child_policies
                        .insert(initiation, policy);
                } else if !adopted {
                    issues.push(unresolved(
                        &state,
                        &digest,
                        initiation,
                        "original_reuse_policy_or_verified_existing_child",
                    ));
                }
            }
            for pending in state
                .children
                .values()
                .filter(|child| child.child_run_id.is_some())
            {
                let first = pending.child_run_id.expect("filtered confirmed child");
                let key = RunKey::derive(pending.namespace_id, &pending.child_workflow_id, first);
                if let LoadedRun::Existing(child) = repo.load_outbound_source(key).await? {
                    if !verified_legacy_child(&state, pending.initiated_event_id, &child) {
                        issues.push(unresolved(
                            &state,
                            &digest,
                            pending.initiated_event_id,
                            "confirmed_child_lineage",
                        ));
                    } else if child.is_open() {
                        next.outbound
                            .as_mut()
                            .expect("metadata installed")
                            .child_releases
                            .insert(
                                pending.initiated_event_id,
                                ChildReleaseState {
                                    namespace_id: pending.namespace_id,
                                    workflow_id: pending.child_workflow_id.clone(),
                                    initiating_event: pending.initiated_event_id,
                                    child_run_id: first,
                                    parent_started_event_id: pending.started_event_id,
                                    parent_close_policy: pending.parent_close_policy,
                                },
                            );
                    }
                } else {
                    issues.push(unresolved(
                        &state,
                        &digest,
                        pending.initiated_event_id,
                        "confirmed_child_state",
                    ));
                }
            }
            // Old first child runs may have been committed before their task was
            // scheduled. Recover that gate from history, never from elapsed time.
            if let Some(parent) = state.parent_run_key
                && state.first_execution_run_id.unwrap_or(state.run_id) == state.run_id
            {
                let mut after = 0;
                let mut released = false;
                while after < state.last_event_id && !released {
                    let page = repo.read_history(state.run_key, after, 100).await?;
                    ensure!(
                        !page.is_empty(),
                        "child history is incomplete for {:?}",
                        state.run_key
                    );
                    let last = page.last().expect("nonempty page").event_id;
                    ensure!(last > after, "child history cursor did not advance");
                    released = page.iter().any(|event| {
                        matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. })
                    });
                    after = last;
                }
                next.outbound
                    .as_mut()
                    .expect("metadata installed")
                    .child_gate = Some(ChildStartGate {
                    parent_run_key: parent,
                    initiating_event: state.parent_initiated_event_id,
                    first_child_run_id: state.run_id,
                    released,
                });
            }
        }
    }
    Ok((
        OutboundUpgradeWrite {
            expected_digest: digest,
            state: next,
            historical_unknown: state.outbound.is_none() && !state.is_open(),
        },
        issues,
    ))
}

/// Read every hot source one at a time. The returned report is bounded, while its
/// digest covers the complete inventory and repair evidence. No writes occur.
pub async fn plan_outbound_recovery<R: RunRepository + ?Sized>(
    repo: &R,
    repairs: &[OutboundRepair],
) -> Result<OutboundUpgradePlan> {
    let repair_digest = repairs_digest(repairs)?;
    let status = repo.outbound_upgrade_status().await?;
    let schema_digest = sha256_hex(include_bytes!("../schema-contract.toml"));
    let mut hasher = Sha256::new();
    hasher.update(postcard::to_allocvec(&(
        OUTBOUND_UPGRADE,
        &schema_digest,
        &status,
        &repair_digest,
    ))?);
    let mut plan = OutboundUpgradePlan {
        digest: String::new(),
        schema_digest,
        writer_version: tokeira_kernel::OUTBOUND_STATE_VERSION,
        status: status.clone(),
        examined: 0,
        legacy: 0,
        pending: 0,
        oldest_pending_at: status.oldest_intent_at,
        historical_unknown: status
            .progress
            .as_ref()
            .map_or(0, |progress| progress.historical_unknown),
        unresolved_count: 0,
        unresolved: Vec::new(),
    };
    let mut after = None;
    let mut seen_repairs = std::collections::BTreeSet::new();
    while let Some(state) = repo.read_outbound_upgrade_run(after).await? {
        after = Some(state.run_key);
        for repair in repairs.iter().filter(|repair| repair.run == state.run_key) {
            seen_repairs.insert((repair.run, repair.initiation));
        }
        plan.examined += 1;
        plan.legacy += u64::from(state.outbound.is_none());
        let (write, issues) = classify(repo, state, repairs).await?;
        hasher.update(postcard::to_allocvec(&(
            write.state.run_key,
            &write.expected_digest,
            outbound_source_digest(&write.state)?,
            &issues,
        ))?);
        plan.historical_unknown += u64::from(write.historical_unknown);
        let operations = crate::derive_operations(&write.state, tokeira_types::ShardId(0))?;
        plan.pending += operations.len() as u64;
        for operation in operations {
            let initiation = operation.incarnation.key.initiating_event;
            if let Some(event) = repo
                .read_history_event(write.state.run_key, initiation)
                .await?
                && event.event_id == initiation
            {
                plan.oldest_pending_at = Some(
                    plan.oldest_pending_at
                        .map_or(event.happened_at, |old| old.min(event.happened_at)),
                );
            }
        }
        plan.unresolved_count += issues.len() as u64;
        plan.unresolved.extend(
            issues
                .into_iter()
                .take(INSPECTION_LIMIT.saturating_sub(plan.unresolved.len())),
        );
    }
    ensure!(
        seen_repairs.len() == repairs.len(),
        "repair names an absent source"
    );
    ensure!(
        repo.outbound_upgrade_status().await? == status,
        "maintenance status changed; review a new plan"
    );
    plan.digest = format!("{:x}", hasher.finalize());
    Ok(plan)
}

/// Apply an exact reviewed plan, or resume its durable continuation. A new plan is
/// mandatory after a source/repair/revision mismatch; this does not auto-approve it.
pub async fn apply_outbound_recovery<R: RunRepository + ?Sized>(
    repo: &R,
    digest: &str,
    stopped_writers: bool,
    repairs: &[OutboundRepair],
) -> Result<OutboundUpgradeProgress> {
    ensure!(
        stopped_writers,
        "confirm every writer is stopped; absence of live leases is insufficient"
    );
    let repair_digest = repairs_digest(repairs)?;
    let status = repo.outbound_upgrade_status().await?;
    ensure!(
        status.live_leases == 0,
        "outbound preparation requires all live leases to expire"
    );
    let mut progress = match status.progress.as_ref() {
        Some(progress)
            if progress.plan_digest == digest && progress.repairs_digest == repair_digest =>
        {
            progress.clone()
        }
        _ => {
            let plan = plan_outbound_recovery(repo, repairs).await?;
            ensure!(
                plan.digest == digest,
                "plan digest changed; review a new plan"
            );
            ensure!(
                plan.unresolved_count == 0,
                "unresolved pending work prevents preparation; supply verified repair evidence and review a new plan"
            );
            let mut next = status
                .progress
                .as_ref()
                .filter(|progress| !progress.complete)
                .cloned()
                .unwrap_or_default();
            // A subsequent inspection cannot turn unknowable historical close
            // delivery into an asserted success. Retain the cutover boundary.
            next.historical_unknown = status
                .progress
                .as_ref()
                .map_or(0, |progress| progress.historical_unknown);
            next.revision = status
                .progress
                .as_ref()
                .map_or(1, |progress| progress.revision + 1);
            next.plan_digest = digest.into();
            next.repairs_digest = repair_digest;
            repo.commit_outbound_upgrade_page(status.progress, next.clone(), None)
                .await?;
            next
        }
    };
    while !progress.complete {
        let candidate = repo.read_outbound_upgrade_run(progress.after).await?;
        let mut next = progress.clone();
        next.revision += 1;
        let write = if let Some(state) = candidate {
            let (write, issues) = classify(repo, state, repairs).await?;
            ensure!(
                issues.is_empty(),
                "source became unresolved; preparation remains incomplete for {:?}",
                write.state.run_key
            );
            next.after = Some(write.state.run_key);
            next.examined += 1;
            next.historical_unknown += u64::from(write.historical_unknown);
            Some(write)
        } else {
            next.complete = true;
            None
        };
        repo.commit_outbound_upgrade_page(Some(progress), next.clone(), write)
            .await?;
        progress = next;
        crate::metrics::record_outbound_upgrade_page();
        tracing::info!(
            examined = progress.examined,
            historical_unknown = progress.historical_unknown,
            complete = progress.complete,
            "outbound preparation page committed"
        );
    }
    Ok(progress)
}
