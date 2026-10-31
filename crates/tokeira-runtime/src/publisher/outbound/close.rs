//! Committed close obligations survive source closure and delivery loss. Target
//! lineage and current-pointer guards prevent an old parent from affecting a
//! reused workflow ID; only a definitive relationship outcome retires an intent.

use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
};

use anyhow::{Result, ensure};
use tokeira_kernel::{LoadedRun, WorkflowState};
use tokeira_storage::{
    CommitResult, DefinitiveOutcome, DeliveryGuard, IntentDescriptor, IntentPayload,
    IntentRetirement, IntentRow, RecoveryError, RunRepository,
};
use tokeira_types::RunKey;

use super::RuntimeDispatchPublisher;
use crate::{outbound::driver::Attempt, shard::ShardOwner};

impl<R: RunRepository + 'static> RuntimeDispatchPublisher<R> {
    pub(crate) async fn drive_outbound_intent(
        &self,
        descriptor: IntentDescriptor,
        attempt: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        attempt.validate(owner)?;
        let Some(intent) = self.repo.load_intent(descriptor.key).await? else {
            return Ok(());
        };
        ensure!(
            intent.source_home == attempt.acquisition.shard_id,
            "intent source home changed"
        );
        let outcome = match &intent.payload {
            IntentPayload::ParentClose { .. } => {
                self.deliver_parent_close(&intent, &attempt, owner).await?
            }
            IntentPayload::ChildResult { .. } => {
                self.deliver_child_result(&intent, &attempt, owner).await?
            }
            IntentPayload::Successor { .. } => {
                anyhow::bail!("successor requires atomic publication")
            }
        };
        attempt.validate(owner)?;
        self.repo
            .retire_intent(
                IntentRetirement {
                    key: intent.key,
                    outcome,
                },
                attempt.fence,
            )
            .await?;
        Ok(())
    }

    async fn deliver_parent_close(
        &self,
        intent: &IntentRow,
        attempt: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<DefinitiveOutcome> {
        let IntentPayload::ParentClose {
            namespace_id,
            workflow_id,
            initiating_event,
            first_child_run,
            policy: _,
        } = &intent.payload
        else {
            unreachable!("kind checked");
        };
        let Some(first) = first_child_run else {
            // No seed/confirmation means no authorized child could be created:
            // source closure and guarded child creation conflict in storage.
            return Ok(DefinitiveOutcome::Absent);
        };
        let Some(target) = self
            .repo
            .find_latest_run(*namespace_id, workflow_id)
            .await?
        else {
            return Ok(DefinitiveOutcome::Absent);
        };
        let LoadedRun::Existing(child) = self.repo.load_outbound_source(target).await? else {
            return Ok(DefinitiveOutcome::Absent);
        };
        if child.first_execution_run_id.unwrap_or(child.run_id) != *first
            || child.parent_run_key != Some(intent.key.source_run)
            || child.parent_initiated_event_id != *initiating_event
        {
            return Ok(DefinitiveOutcome::Obsolete);
        }
        if !child.is_open() {
            return Ok(DefinitiveOutcome::Absent);
        }
        let LoadedRun::Existing(_source) = self
            .repo
            .load_outbound_source(intent.key.source_run)
            .await?
        else {
            return Err(RecoveryError::InvalidIdentity {
                run: intent.key.source_run,
                detail: "parent-close source unavailable while intent remains",
            }
            .into());
        };
        attempt.validate(owner)?;
        let result = self
            .destination(
                target,
                (*namespace_id, workflow_id),
                crate::DeliveryPhase::ParentClose,
                DeliveryGuard::CurrentIntent {
                    key: intent.key,
                    expected_current: Some(target),
                },
                attempt,
                owner,
            )
            .await?;
        match result {
            CommitResult::Applied { new_state }
                if !new_state.is_open() || new_state.cancel_requested =>
            {
                Ok(applied(&new_state))
            }
            _ => anyhow::bail!("parent-close target outcome requires rediscovery"),
        }
    }

    async fn deliver_child_result(
        &self,
        intent: &IntentRow,
        attempt: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<DefinitiveOutcome> {
        let IntentPayload::ChildResult {
            parent_run,
            identity,
            terminal_event: _,
        } = &intent.payload
        else {
            unreachable!("kind checked");
        };
        let LoadedRun::Existing(source) = self
            .repo
            .load_outbound_source(intent.key.source_run)
            .await?
        else {
            return Err(RecoveryError::InvalidIdentity {
                run: intent.key.source_run,
                detail: "child-result source unavailable while intent remains",
            }
            .into());
        };
        ensure!(
            !source.is_open()
                && source.parent_run_key == Some(*parent_run)
                && source.parent_initiated_event_id == identity.parent_initiated_event
                && source.first_execution_run_id.unwrap_or(source.run_id)
                    == identity.child_first_run
                && source.run_id == identity.child_final_run,
            RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "child terminal lineage mismatch"
            }
        );
        let mut target = *parent_run;
        let mut visited = BTreeSet::new();
        // A reset redirect is a relationship in committed state, never a URL or
        // transport instruction (recordchildworkflowcompleted/api.go @ v1.31.0).
        for _ in 0..100 {
            ensure!(visited.insert(target), "child result reset redirect cycle");
            let LoadedRun::Existing(parent) = self.repo.load_outbound_source(target).await? else {
                return Ok(DefinitiveOutcome::Absent);
            };
            if !parent.is_open() {
                if let Some(reset) = parent.reset_run_id {
                    target = RunKey::derive(parent.namespace_id, &parent.workflow_id, reset);
                    continue;
                }
                return Ok(DefinitiveOutcome::Absent);
            }
            let Some(child) = parent.children.get(&source.workflow_id) else {
                return Ok(DefinitiveOutcome::Obsolete);
            };
            if child.initiated_event_id != identity.parent_initiated_event
                || child.namespace_id != source.namespace_id
                || child
                    .child_run_id
                    .is_some_and(|first| first != identity.child_first_run)
            {
                return Ok(DefinitiveOutcome::Obsolete);
            }
            attempt.validate(owner)?;
            let result = self
                .destination(
                    target,
                    (parent.namespace_id, &parent.workflow_id),
                    crate::DeliveryPhase::ChildResult,
                    DeliveryGuard::Intent(intent.key),
                    attempt,
                    owner,
                )
                .await?;
            return match result {
                CommitResult::Applied { new_state }
                    if !new_state.children.contains_key(&source.workflow_id) =>
                {
                    Ok(applied(&new_state))
                }
                _ => anyhow::bail!("child result target outcome requires rediscovery"),
            };
        }
        anyhow::bail!("child result reset redirect limit exceeded")
    }
}

fn applied(state: &WorkflowState) -> DefinitiveOutcome {
    DefinitiveOutcome::Applied {
        run: state.run_key,
        seq: state.transition_seq,
    }
}
