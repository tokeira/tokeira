//! Persisted child creation, confirmation and first-task release. The frozen seed
//! selects identity/inheritance once; only actual target creation chooses a clock.
//! Every ambiguous reply is checked against the exact relationship before advancing.

use std::sync::{Arc, RwLock};

use anyhow::{Result, ensure};
use time::OffsetDateTime;
use tokeira_kernel::{
    ChildStartConfirmedRequest, ChildStartResult, ChildStartSeed, Command, HistoryEventKind,
    LoadedRun, StartRequest, WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowState,
};
use tokeira_storage::{CommitResult, OperationRow, RecoveryError, RunRepository};
use tokeira_types::{ExecutionRef, ExecutionStatus, RequestContext, RequestId, RunId, RunKey};

use super::{RuntimeDispatchPublisher, authority, metadata_transition, require_applied};
use crate::{outbound::driver::Attempt, shard::ShardOwner};

impl<R: RunRepository + 'static> RuntimeDispatchPublisher<R> {
    pub(super) async fn drive_child_start(
        &self,
        row: OperationRow,
        attempt: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        let mut source = self.operation_source(&row).await?;
        let initiating_event = row.incarnation.key.initiating_event;
        let history = self
            .repo
            .read_history(source.run_key, initiating_event - 1, 1)
            .await?;
        let event = history
            .first()
            .filter(|event| event.event_id == initiating_event)
            .ok_or(RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "child initiation history missing",
            })?;
        let reuse_policy = event.kind.child_reuse_policy().or_else(|| {
            source.outbound.as_ref().and_then(|outbound| {
                outbound
                    .legacy_child_policies
                    .get(&initiating_event)
                    .copied()
            })
        });
        let (
            child_workflow_id,
            namespace_id,
            workflow_type,
            task_queue,
            _input,
            _header,
            _memo,
            _search_attributes,
            _workflow_execution_timeout,
            _workflow_run_timeout,
            _workflow_task_timeout,
            _retry_policy,
            _cron_schedule,
            priority,
        ) = match &event.kind {
            HistoryEventKind::StartChildWorkflowExecutionInitiatedV2 {
                child_workflow_id,
                namespace_id,
                workflow_type,
                task_queue,
                input,
                header,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                retry_policy,
                cron_schedule,
                priority,
                ..
            }
            | HistoryEventKind::StartChildWorkflowExecutionInitiated {
                child_workflow_id,
                namespace_id,
                workflow_type,
                task_queue,
                input,
                header,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                retry_policy,
                cron_schedule,
                priority,
                ..
            } => (
                child_workflow_id,
                *namespace_id,
                workflow_type,
                task_queue,
                input,
                header,
                memo,
                search_attributes,
                *workflow_execution_timeout,
                *workflow_run_timeout,
                *workflow_task_timeout,
                retry_policy,
                cron_schedule,
                priority,
            ),
            _ => {
                return Err(RecoveryError::InvalidIdentity {
                    run: source.run_key,
                    detail: "child initiation event kind mismatch",
                }
                .into());
            }
        };
        if source
            .outbound
            .as_ref()
            .is_some_and(|outbound| outbound.reset_request_id.is_some())
            && let Some(target) = self
                .repo
                .find_latest_run(namespace_id, child_workflow_id)
                .await?
            && let LoadedRun::Existing(child) = self.repo.load_outbound_source(target).await?
            && self
                .reset_child_relationship(&source, &child, initiating_event)
                .await?
        {
            // A reset reuses a verified pre-fork child, including a child whose
            // original start reply was lost (verifyChildWorkflow in
            // service/history/transfer_queue_active_task_executor.go @ v1.31.0).
            return self
                .confirm_child(
                    &row,
                    &attempt,
                    owner,
                    child_workflow_id.clone(),
                    ChildStartResult::Started {
                        child_run_id: child.first_execution_run_id.unwrap_or(child.run_id),
                        workflow_type: workflow_type.clone(),
                    },
                )
                .await;
        }
        if let Some(first) = source
            .outbound
            .as_ref()
            .and_then(|metadata| metadata.legacy_adopted_children.get(&initiating_event))
            .copied()
        {
            let key = RunKey::derive(namespace_id, child_workflow_id, first);
            let LoadedRun::Existing(child) = self.repo.load_outbound_source(key).await? else {
                anyhow::bail!(
                    "verified legacy child is temporarily unavailable; adoption cannot create a replacement"
                );
            };
            ensure!(
                tokeira_storage::outbound_upgrade::verified_legacy_child(
                    &source,
                    initiating_event,
                    &child
                ),
                "verified legacy child relationship changed"
            );
            // Stopped-cluster repair records verified lineage, not a fabricated
            // request receipt. It grants only adoption of this existing run.
            return self
                .confirm_child(
                    &row,
                    &attempt,
                    owner,
                    child_workflow_id.clone(),
                    ChildStartResult::Started {
                        child_run_id: first,
                        workflow_type: workflow_type.clone(),
                    },
                )
                .await;
        }
        let reuse_policy = reuse_policy.ok_or(RecoveryError::InvalidIdentity {
            run: source.run_key,
            detail: "child initiation reuse policy requires verified repair",
        })?;
        let seed = match source
            .outbound
            .as_ref()
            .and_then(|outbound| outbound.child_seeds.get(&initiating_event))
            .cloned()
        {
            Some(seed) => seed,
            None => {
                // Registry observations are selected against this exact source
                // sequence. An OCC loser never sends its uncommitted candidate.
                let candidate = ChildStartSeed {
                    child_run_id: RunId::new(),
                    request_id: RequestId(format!(
                        "child-start:{}:{initiating_event}",
                        source.run_key.0
                    )),
                    prepared_at: OffsetDateTime::now_utc(),
                    parent_namespace_name: match &self.namespace_resolver {
                        Some(resolver) => {
                            Some(resolver.name_for_id(source.namespace_id).await?.ok_or(
                                RecoveryError::InvalidIdentity {
                                    run: source.run_key,
                                    detail: "child parent namespace no longer resolves",
                                },
                            )?)
                        }
                        None => None,
                    },
                    root_workflow_id: Some(
                        source
                            .root_workflow_id
                            .clone()
                            .unwrap_or_else(|| source.workflow_id.clone()),
                    ),
                    root_run_id: Some(source.root_run_id.unwrap_or(source.run_id)),
                    effective_priority: tokeira_kernel::merge_priority(
                        source.priority.as_ref(),
                        priority.as_ref(),
                    ),
                    versioning_info: self
                        .child_versioning_from_state(&source, namespace_id, task_queue)
                        .await?,
                };
                source
                    .outbound
                    .as_mut()
                    .ok_or(RecoveryError::LegacyState(source.run_key))?
                    .child_seeds
                    .insert(initiating_event, candidate.clone());
                source = require_applied(
                    self.pick_lane(source.run_key)
                        .commit_outbound_metadata(
                            Box::new(metadata_transition(source)),
                            attempt.clone(),
                        )
                        .await?,
                )?;
                candidate
            }
        };
        attempt.validate(owner)?;
        if let Some(resolver) = &self.namespace_resolver
            && resolver.name_for_id(namespace_id).await?.is_none()
        {
            return self
                .confirm_child(
                    &row,
                    &attempt,
                    owner,
                    child_workflow_id.clone(),
                    ChildStartResult::Failed {
                        cause: "NAMESPACE_NOT_FOUND".into(),
                    },
                )
                .await;
        }
        let target = RunKey::derive(namespace_id, child_workflow_id, seed.child_run_id);
        if let LoadedRun::Existing(child) = self.repo.load_outbound_source(target).await? {
            self.verify_child_start(&source, initiating_event, &seed, &child, &row)
                .await?;
            return self
                .confirm_child(
                    &row,
                    &attempt,
                    owner,
                    child_workflow_id.clone(),
                    ChildStartResult::Started {
                        child_run_id: seed.child_run_id,
                        workflow_type: workflow_type.clone(),
                    },
                )
                .await;
        }
        // Closed workflow-ID reuse is distinct from a retry of this exact run.
        // Only these documented statuses admit ALLOW_DUPLICATE_FAILED_ONLY
        // (resolveWorkflowIDReusePolicy, service/history/api/workflow_id_dedup.go @ v1.31.0).
        let expected_current = self
            .repo
            .find_latest_run(namespace_id, child_workflow_id)
            .await?;
        if let Some(current) = expected_current {
            let LoadedRun::Existing(existing) = self.repo.load_outbound_source(current).await?
            else {
                anyhow::bail!("current child execution is temporarily unavailable");
            };
            if existing.is_open() && reuse_policy == WorkflowIdReusePolicy::TerminateIfRunning {
                self.destination(
                    current,
                    (namespace_id, child_workflow_id),
                    crate::DeliveryPhase::ChildReplace,
                    authority(&row, &source),
                    &attempt,
                    owner,
                )
                .await?;
                // Yield to discovery after one side effect. A retry revalidates
                // both the original source and the current execution pointer.
                return Ok(());
            }
            let denied = existing.is_open()
                || match reuse_policy {
                    WorkflowIdReusePolicy::RejectDuplicate => true,
                    WorkflowIdReusePolicy::AllowDuplicateFailedOnly => !matches!(
                        existing.status,
                        ExecutionStatus::Failed
                            | ExecutionStatus::Cancelled
                            | ExecutionStatus::Terminated
                            | ExecutionStatus::TimedOut
                    ),
                    _ => false,
                };
            if denied {
                return self
                    .confirm_child(
                        &row,
                        &attempt,
                        owner,
                        child_workflow_id.clone(),
                        ChildStartResult::Failed {
                            cause: "WORKFLOW_ALREADY_EXISTS".into(),
                        },
                    )
                    .await;
            }
        }
        let result = self
            .destination(
                target,
                (namespace_id, child_workflow_id),
                crate::DeliveryPhase::ChildStart,
                tokeira_storage::DeliveryGuard::ChildStart {
                    incarnation: row.incarnation,
                    source_seq: source.transition_seq,
                    expected_current,
                },
                &attempt,
                owner,
            )
            .await;
        // Verify even Applied/Duplicate: a request-ID collision or lost transport
        // reply is not evidence that this parent's chosen child was created.
        if let LoadedRun::Existing(child) = self.repo.load_outbound_source(target).await? {
            self.verify_child_start(&source, initiating_event, &seed, &child, &row)
                .await?;
            self.confirm_child(
                &row,
                &attempt,
                owner,
                child_workflow_id.clone(),
                ChildStartResult::Started {
                    child_run_id: seed.child_run_id,
                    workflow_type: workflow_type.clone(),
                },
            )
            .await
        } else {
            match result? {
                CommitResult::CurrentExecutionConflict { .. } => Ok(()),
                _ => anyhow::bail!("child start remains unverified"),
            }
        }
    }

    async fn confirm_child(
        &self,
        row: &OperationRow,
        attempt: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
        child_workflow_id: tokeira_types::WorkflowId,
        result: ChildStartResult,
    ) -> Result<()> {
        require_applied(
            self.source_outcome(
                row,
                Command::ChildStartConfirmed(ChildStartConfirmedRequest {
                    child_workflow_id,
                    initiated_event_id: row.incarnation.key.initiating_event,
                    result,
                    now: OffsetDateTime::now_utc(),
                }),
                attempt,
                owner,
            )
            .await?,
        )?;
        Ok(())
    }

    async fn verify_child_start(
        &self,
        source: &WorkflowState,
        initiation: i64,
        seed: &ChildStartSeed,
        child: &WorkflowState,
        row: &OperationRow,
    ) -> Result<()> {
        ensure!(
            child.run_id == seed.child_run_id
                && child.parent_run_key == Some(source.run_key)
                && child.parent_initiated_event_id == initiation
                && child.parent_namespace_id == Some(source.namespace_id)
                && child.parent_run_id == Some(source.run_id)
                && child.parent_workflow_id.as_ref() == Some(&source.workflow_id),
            RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "chosen child relationship mismatch"
            }
        );
        let record = self
            .repo
            .lookup_request_dedupe(
                &ExecutionRef {
                    namespace_id: child.namespace_id,
                    workflow_id: child.workflow_id.clone(),
                    run_id: Some(child.run_id),
                },
                &seed.request_id,
            )
            .await?;
        ensure!(
            record.is_some_and(|record| record.run_key == child.run_key
                && record
                    .outbound_origin
                    .is_some_and(|origin| origin.incarnation == row.incarnation)),
            RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "chosen child start provenance mismatch"
            }
        );
        Ok(())
    }

    /// Verify the reset family before reusing an existing child. Initiation and
    /// namespace checks additionally prevent unrelated workflow-ID reuse from
    /// borrowing a child first-task gate.
    pub(super) async fn reset_child_relationship(
        &self,
        source: &WorkflowState,
        child: &WorkflowState,
        initiation: i64,
    ) -> Result<bool> {
        if !source
            .outbound
            .as_ref()
            .is_some_and(|outbound| outbound.reset_request_id.is_some())
            || child.parent_namespace_id != Some(source.namespace_id)
            || child.parent_workflow_id.as_ref() != Some(&source.workflow_id)
            || child.parent_initiated_event_id != initiation
        {
            return Ok(false);
        }
        let Some(parent_key) = child.parent_run_key else {
            return Ok(false);
        };
        let original = source.original_execution_run_id.unwrap_or(source.run_id);
        if child.parent_run_id == Some(original) {
            return Ok(true);
        }
        let LoadedRun::Existing(parent) = self.repo.load_outbound_source(parent_key).await? else {
            anyhow::bail!("reset child parent lineage is temporarily unavailable");
        };
        Ok(parent.namespace_id == source.namespace_id
            && parent.workflow_id == source.workflow_id
            && parent.original_execution_run_id.unwrap_or(parent.run_id) == original)
    }

    /// Restore release obligations omitted by history replay only after checking
    /// the real child. The original gate remains the authority on the child run.
    pub(crate) async fn prepare_reset_child_releases(
        &self,
        state: &mut WorkflowState,
    ) -> Result<()> {
        let children = state.children.values().cloned().collect::<Vec<_>>();
        for child in children {
            let (Some(first), Some(started)) = (child.child_run_id, child.started_event_id) else {
                continue;
            };
            let key = RunKey::derive(child.namespace_id, &child.child_workflow_id, first);
            let LoadedRun::Existing(target) = self.repo.load_outbound_source(key).await? else {
                anyhow::bail!("reset confirmed child is temporarily unavailable");
            };
            ensure!(
                self.reset_child_relationship(state, &target, child.initiated_event_id)
                    .await?,
                RecoveryError::InvalidIdentity {
                    run: state.run_key,
                    detail: "reset confirmed child lineage mismatch"
                }
            );
            if target
                .outbound
                .as_ref()
                .and_then(|metadata| metadata.child_gate.as_ref())
                .is_some_and(|gate| !gate.released)
            {
                state
                    .outbound
                    .as_mut()
                    .ok_or(RecoveryError::LegacyState(state.run_key))?
                    .child_releases
                    .insert(
                        child.initiated_event_id,
                        tokeira_kernel::ChildReleaseState {
                            namespace_id: child.namespace_id,
                            workflow_id: child.child_workflow_id,
                            initiating_event: child.initiated_event_id,
                            child_run_id: first,
                            parent_started_event_id: Some(started),
                            parent_close_policy: child.parent_close_policy,
                        },
                    );
            }
        }
        Ok(())
    }

    pub(super) async fn drive_child_release(
        &self,
        row: OperationRow,
        attempt: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        let source = self.operation_source(&row).await?;
        let initiation = row.incarnation.key.initiating_event;
        let release = source
            .outbound
            .as_ref()
            .and_then(|outbound| outbound.child_releases.get(&initiation))
            .ok_or(RecoveryError::Obsolete)?
            .clone();
        let started = release
            .parent_started_event_id
            .ok_or(RecoveryError::Obsolete)?;
        let history = self
            .repo
            .read_history(source.run_key, started - 1, 1)
            .await?;
        ensure!(history.first().is_some_and(|event| event.event_id == started && matches!(&event.kind,
            HistoryEventKind::ChildWorkflowExecutionStarted { child_workflow_id, child_run_id, initiated_event_id, .. }
                if child_workflow_id == &release.workflow_id && *child_run_id == release.child_run_id && *initiated_event_id == initiation)),
            RecoveryError::InvalidIdentity { run: source.run_key, detail: "child release confirmation history mismatch" });
        let target = RunKey::derive(
            release.namespace_id,
            &release.workflow_id,
            release.child_run_id,
        );
        let LoadedRun::Existing(before) = self.repo.load_outbound_source(target).await? else {
            anyhow::bail!("confirmed child is temporarily unavailable");
        };
        let parent_run_key = if before.parent_run_key == Some(source.run_key) {
            source.run_key
        } else {
            ensure!(
                self.reset_child_relationship(&source, &before, initiation)
                    .await?,
                RecoveryError::InvalidIdentity {
                    run: source.run_key,
                    detail: "reset child release lineage mismatch"
                }
            );
            before
                .parent_run_key
                .ok_or(RecoveryError::InvalidIdentity {
                    run: source.run_key,
                    detail: "reset child has no parent",
                })?
        };
        attempt.validate(owner)?;
        if before.is_open() {
            self.destination(
                target,
                (release.namespace_id, &release.workflow_id),
                crate::DeliveryPhase::ChildRelease,
                authority(&row, &source),
                &attempt,
                owner,
            )
            .await?;
        }
        let LoadedRun::Existing(child) = self.repo.load_outbound_source(target).await? else {
            anyhow::bail!("released child is temporarily unavailable");
        };
        ensure!(
            !child.is_open()
                || child
                    .outbound
                    .as_ref()
                    .and_then(|outbound| outbound.child_gate.as_ref())
                    .is_some_and(|gate| gate.released
                        && gate.parent_run_key == parent_run_key
                        && gate.initiating_event == initiation
                        && gate.first_child_run_id == release.child_run_id),
            "child release remains unverified"
        );
        let mut source = self.operation_source(&row).await?;
        source
            .outbound
            .as_mut()
            .ok_or(RecoveryError::LegacyState(source.run_key))?
            .child_releases
            .remove(&initiation);
        require_applied(
            self.pick_lane(source.run_key)
                .commit_outbound_metadata(Box::new(metadata_transition(source)), attempt)
                .await?,
        )?;
        Ok(())
    }
}

/// Hydrate the accepted child recipe using only its persisted winning seed.
pub(super) fn child_start_request(
    source: &WorkflowState,
    event: &tokeira_kernel::HistoryEvent,
    now: OffsetDateTime,
) -> Result<StartRequest> {
    let initiating_event = event.event_id;
    let reuse_policy = event
        .kind
        .child_reuse_policy()
        .or_else(|| {
            source.outbound.as_ref().and_then(|outbound| {
                outbound
                    .legacy_child_policies
                    .get(&initiating_event)
                    .copied()
            })
        })
        .ok_or(RecoveryError::InvalidIdentity {
            run: source.run_key,
            detail: "child initiation reuse policy requires verified repair",
        })?;
    let (
        child_workflow_id,
        namespace_id,
        workflow_type,
        task_queue,
        input,
        header,
        memo,
        search_attributes,
        workflow_execution_timeout,
        workflow_run_timeout,
        workflow_task_timeout,
        retry_policy,
        cron_schedule,
        _priority,
    ) = match &event.kind {
        HistoryEventKind::StartChildWorkflowExecutionInitiatedV2 {
            child_workflow_id,
            namespace_id,
            workflow_type,
            task_queue,
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            priority,
            ..
        }
        | HistoryEventKind::StartChildWorkflowExecutionInitiated {
            child_workflow_id,
            namespace_id,
            workflow_type,
            task_queue,
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            priority,
            ..
        } => (
            child_workflow_id,
            *namespace_id,
            workflow_type,
            task_queue,
            input,
            header,
            memo,
            search_attributes,
            *workflow_execution_timeout,
            *workflow_run_timeout,
            *workflow_task_timeout,
            retry_policy,
            cron_schedule,
            priority,
        ),
        _ => {
            return Err(RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "child initiation event kind mismatch",
            }
            .into());
        }
    };
    let seed = source
        .outbound
        .as_ref()
        .and_then(|metadata| metadata.child_seeds.get(&initiating_event))
        .ok_or(RecoveryError::InvalidIdentity {
            run: source.run_key,
            detail: "child seed is not committed",
        })?;
    let target = RunKey::derive(namespace_id, child_workflow_id, seed.child_run_id);
    Ok(StartRequest {
        advice_policy: crate::runtime::continue_as_new_advice_policy(),
        run_key: target,
        namespace_id,
        workflow_id: child_workflow_id.clone(),
        run_id: seed.child_run_id,
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: cron_schedule
            .as_ref()
            .filter(|schedule| !schedule.is_empty())
            .map(|schedule| crate::schedule::cron_initial_backoff(schedule, now))
            .transpose()?,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: seed.effective_priority.clone(),
        input: input.clone(),
        header: header.clone(),
        memo: memo.clone(),
        search_attributes: search_attributes.clone(),
        workflow_execution_timeout,
        workflow_run_timeout,
        workflow_task_timeout,
        retry_policy: retry_policy.clone(),
        conflict_policy: WorkflowIdConflictPolicy::Fail,
        reuse_policy: if reuse_policy == WorkflowIdReusePolicy::TerminateIfRunning {
            WorkflowIdReusePolicy::AllowDuplicate
        } else {
            reuse_policy
        },
        initiator: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: None,
        parent_run_key: Some(source.run_key),
        parent_workflow_id: Some(source.workflow_id.clone()),
        parent_run_id: Some(source.run_id),
        parent_namespace_id: Some(source.namespace_id),
        parent_namespace_name: seed.parent_namespace_name.clone(),
        parent_initiated_event_id: initiating_event,
        root_workflow_id: seed.root_workflow_id.clone(),
        root_run_id: seed.root_run_id,
        original_execution_run_id: None,
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: seed.request_id.clone(),
            caller_identity: Some("runtime-child-orchestrator".into()),
            principal: None,
            received_at: now,
        },
        now,
        client_cron_schedule: None,
        cron_schedule: cron_schedule.clone(),
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: seed.versioning_info.clone(),
    })
}
