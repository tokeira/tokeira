//! Frozen Tokeira projection oracle for the accumulator storage refactor.
//!
//! Copied from `crates/tokeira-storage/src/api.rs` at Tokeira
//! `689e89a8622d114de1dd80232a22bb63705d50d1`. Both the previous-image fold and
//! raw derivation are independent of the new preparation helper. Preserve this
//! baseline, including its documented Temporal compatibility gaps.

use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use time::OffsetDateTime;
use tokeira_kernel::{
    WorkflowState,
    state::{VersioningBehavior, VersioningOverride},
};
use tokeira_types::{
    ArchetypeId, Memo, SearchAttrValue, SearchAttributes, VisibilityLifecycleState,
};

use crate::api::ProjectionContext;

#[path = "projection_accumulator_legacy_codec.rs"]
pub(crate) mod legacy_codec;

/// Reproduce the complete pre-accumulator image, including its legacy merge.
pub(crate) fn workflow_projection_context_with_previous(
    state: &WorkflowState,
    previous: Option<&ProjectionContext>,
    history_size_bytes: i64,
) -> Result<ProjectionContext> {
    let mut context = projection_context(
        state,
        if state.status.is_open() {
            VisibilityLifecycleState::Open
        } else {
            VisibilityLifecycleState::Closed
        },
        state.closed_at.unwrap_or(state.started_at),
        false,
        history_size_bytes,
    )?;

    let mut used_versions = previous
        .and_then(|previous| {
            previous
                .search_attributes
                .0
                .get("TemporalUsedWorkerDeploymentVersions")
        })
        .and_then(|value| match value {
            SearchAttrValue::KeywordList(values) => Some(values.clone()),
            _ => None,
        })
        .unwrap_or_default();
    if let Some(SearchAttrValue::KeywordList(current)) = context
        .search_attributes
        .0
        .get("TemporalUsedWorkerDeploymentVersions")
    {
        for version in current {
            if !used_versions.contains(version) {
                used_versions.push(version.clone());
            }
        }
    }
    if !used_versions.is_empty() {
        context.search_attributes.0.insert(
            "TemporalUsedWorkerDeploymentVersions".to_owned(),
            SearchAttrValue::KeywordList(used_versions),
        );
    }

    Ok(context)
}

fn projection_context(
    state: &WorkflowState,
    lifecycle_state: VisibilityLifecycleState,
    update_time: OffsetDateTime,
    redact_user_data: bool,
    history_size_bytes: i64,
) -> Result<ProjectionContext> {
    let transition_count = i64::try_from(state.transition_seq.0).map_err(|_| {
        anyhow!(
            "workflow transition sequence {} exceeds visibility i64 range",
            state.transition_seq.0
        )
    })?;
    let execution_duration = state
        .closed_at
        .map(|closed_at| (closed_at - state.started_at).whole_nanoseconds() as i64);
    let search_attributes = if redact_user_data {
        SearchAttributes::default()
    } else {
        let mut search_attributes = state.search_attributes.clone();
        if let Some(info) = state.versioning_info.as_ref() {
            let mut build_ids = info.build_id_search_attributes.clone();
            build_ids.retain(|value| !value.starts_with("pinned:"));
            if state.effective_behavior() == VersioningBehavior::Pinned
                && let Some(version) = state.effective_deployment()
                && !version.deployment_name.is_empty()
                && !version.build_id.is_empty()
            {
                // v1.31.0 replaces any prior pinned reachability tag with the
                // effective pinned version and puts it first
                // (`addBuildIdToLoadedSearchAttribute`,
                // mutable_state_impl.go @ v1.31.0). This is visibility state,
                // so deriving it here avoids mutating authoritative history.
                build_ids.insert(
                    0,
                    format!("pinned:{}:{}", version.deployment_name, version.build_id),
                );
            }
            if !build_ids.is_empty() {
                // BuildIds is server-managed visibility state, not a user SA and
                // not part of continue-as-new inheritance. Project it from the
                // history-derived per-run summary (`updateBuildIdsAndDeploymentSearchAttributes`,
                // mutable_state_impl.go @ v1.31.0).
                search_attributes.0.insert(
                    "BuildIds".to_owned(),
                    SearchAttrValue::KeywordList(build_ids),
                );
            }
        }
        if let Some(info) = state.versioning_info.as_ref() {
            // These are mutable-state-derived visibility attributes, not client-authored
            // WorkflowExecutionStarted attributes. Deriving them in the complete projection
            // image mirrors `addBuildIDAndDeploymentInfoToSearchAttributesWithNoVisibilityTask`
            // without leaking server-managed values into history
            // (`service/history/workflow/mutable_state_impl.go:2870-2990,3767-3835 @ v1.31.0`).
            let (deployment, version, behavior) = match info.versioning_override.as_ref() {
                Some(VersioningOverride::Pinned { version }) => (
                    Some(version.deployment_name.as_str()),
                    Some(version),
                    Some("Pinned"),
                ),
                Some(VersioningOverride::AutoUpgrade) => (
                    info.deployment_version
                        .as_ref()
                        .map(|version| version.deployment_name.as_str())
                        .or(state.worker_deployment_name.as_deref()),
                    info.deployment_version.as_ref(),
                    Some("AutoUpgrade"),
                ),
                None => (
                    info.deployment_version
                        .as_ref()
                        .map(|version| version.deployment_name.as_str())
                        .or(state.worker_deployment_name.as_deref()),
                    info.deployment_version.as_ref(),
                    match info.behavior {
                        VersioningBehavior::Pinned => Some("Pinned"),
                        VersioningBehavior::AutoUpgrade => Some("AutoUpgrade"),
                        VersioningBehavior::Unspecified => None,
                    },
                ),
            };
            if let Some(deployment) = deployment.filter(|value| !value.is_empty()) {
                search_attributes.0.insert(
                    "TemporalWorkerDeployment".to_owned(),
                    SearchAttrValue::Keyword(deployment.to_owned()),
                );
            }
            if let Some(version) = version.filter(|version| {
                !version.deployment_name.is_empty() && !version.build_id.is_empty()
            }) {
                search_attributes.0.insert(
                    "TemporalWorkerDeploymentVersion".to_owned(),
                    SearchAttrValue::Keyword(format!(
                        "{}:{}",
                        version.deployment_name, version.build_id
                    )),
                );
            }
            if let Some(behavior) = behavior {
                search_attributes.0.insert(
                    "TemporalWorkflowVersioningBehavior".to_owned(),
                    SearchAttrValue::Keyword(behavior.to_owned()),
                );
            }
            if let Some(version) = info.deployment_version.as_ref().filter(|version| {
                !version.deployment_name.is_empty() && !version.build_id.is_empty()
            }) {
                // Only a successfully completed WFT populates
                // `deployment_version`; a start-time pinned override therefore
                // cannot appear in the used-version index prematurely. Storage
                // folds this observation into the preceding projection image
                // above, matching v1.31.0's completion-side update.
                search_attributes.0.insert(
                    "TemporalUsedWorkerDeploymentVersions".to_owned(),
                    SearchAttrValue::KeywordList(vec![format!(
                        "{}:{}",
                        version.deployment_name, version.build_id
                    )]),
                );
            }
        }
        let mut pause_entries = Vec::new();
        if let Some(pause) = state.pause_info.as_ref() {
            pause_entries.push(format!("Workflow:{}", state.workflow_id.0));
            if !pause.reason.is_empty() {
                pause_entries.push(format!("Reason:{}", pause.reason));
            }
        }
        let paused_activity_types = state
            .activities
            .values()
            .filter(|activity| activity.pause_info.is_some())
            .map(|activity| activity.activity_type.as_str())
            .collect::<BTreeSet<_>>();
        pause_entries.extend(
            paused_activity_types
                .into_iter()
                .map(|activity_type| format!("property:activityType={activity_type}")),
        );
        if !pause_entries.is_empty() {
            // Temporal regenerates this server-managed KeywordList from the
            // current workflow/activity pause state on every mutation; batch
            // activity operations discover targets through the same visibility
            // attribute (`buildTemporalPauseInfoEntries`,
            // mutable_state_impl.go:6431-6475 @ v1.31.0).
            search_attributes.0.insert(
                "TemporalPauseInfo".to_owned(),
                SearchAttrValue::KeywordList(pause_entries),
            );
        } else {
            search_attributes.0.remove("TemporalPauseInfo");
        }
        if state.external_payload_count > 0 {
            search_attributes.0.insert(
                "TemporalExternalPayloadCount".to_owned(),
                SearchAttrValue::Int(state.external_payload_count),
            );
            search_attributes.0.insert(
                "TemporalExternalPayloadSizeBytes".to_owned(),
                SearchAttrValue::Int(state.external_payload_size_bytes),
            );
        }
        search_attributes
    };

    Ok(ProjectionContext {
        archetype_id: ArchetypeId::WORKFLOW,
        namespace_id: state.namespace_id,
        business_id: state.workflow_id.0.clone(),
        // The workflow producer does not yet carry a namespace failover
        // version. Transition sequence therefore remains its monotonic fence.
        authority_epoch: 0,
        status_keyword: format!("{:?}", state.status),
        lifecycle_state,
        workflow_id: state.workflow_id.clone(),
        run_id: state.run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        execution_status: state.status,
        start_time: state.started_at,
        update_time,
        // v1.31.0 derives ExecutionTime from start plus first-WFT backoff
        // (`mutable_state_impl.go:2859 @ v1.31.0`).
        execution_time: Some(state.started_at + state.workflow_start_delay.unwrap_or_default()),
        close_time: state.closed_at,
        history_length: state.last_event_id,
        execution_duration,
        state_transition_count: transition_count,
        transition_count,
        // The persisted History Size at this transition, so the visibility
        // attribute equals what the workflow was told and what Describe reports.
        history_size_bytes,
        parent_workflow_id: state.parent_workflow_id.clone(),
        parent_run_id: state.parent_run_id,
        root_workflow_id: state
            .root_workflow_id
            .clone()
            .or_else(|| Some(state.workflow_id.clone())),
        root_run_id: state.root_run_id.or(Some(state.run_id)),
        search_attr_generation: state.transition_seq.0,
        memo: if redact_user_data {
            Memo::default()
        } else {
            state.memo.clone()
        },
        search_attributes,
    })
}
