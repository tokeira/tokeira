//! Atomic publication of a fully staged reset or successor under one store lock.
//! Preparation records survive snapshots but never appear in run/history indexes.

use super::*;
use crate::{
    PreparationToken, PublicationResult, RecoveryError, RunPreparation,
    outbound::preparation::{PreparationPhase, PreparationRecord},
};

impl InMemoryStore {
    pub(super) async fn published_preparations(
        &self,
        home: ShardId,
        bound: crate::ScanBound<uuid::Uuid>,
        after: Option<uuid::Uuid>,
        limit: usize,
    ) -> Result<crate::OutboundPage<uuid::Uuid, PreparationToken>> {
        crate::outbound::validate_page_limit(limit)?;
        let store = self.inner.lock().await;
        let matching = || {
            store
                .preparations
                .values()
                .filter(|r| r.token.home == home && r.phase == PreparationPhase::Published)
        };
        let through = match bound {
            crate::ScanBound::Capture => matching().next_back().map(|r| r.token.id),
            crate::ScanBound::Through(id) => Some(id),
        };
        let rows = matching()
            .filter(|r| {
                after.is_none_or(|id| r.token.id > id) && through.is_some_and(|id| r.token.id <= id)
            })
            .take(limit)
            .map(|r| r.token.clone())
            .collect();
        Ok(crate::OutboundPage { through, rows })
    }

    pub(super) async fn complete_preparation(
        &self,
        token: PreparationToken,
        fence: WriteFence,
    ) -> Result<bool> {
        fence.ensure_home(token.home)?;
        let mut store = self.inner.lock().await;
        store.check_fence(&fence)?;
        let Some(record) = store
            .preparations
            .get_mut(&token.id)
            .filter(|r| r.token == token)
        else {
            return Ok(false);
        };
        if record.phase == PreparationPhase::Reconciled {
            return Ok(true);
        }
        if record.phase != PreparationPhase::Published {
            return Err(RecoveryError::InvalidPreparation(
                "cannot complete an unpublished preparation",
            )
            .into());
        }
        record.manifest.clear();
        record.phase = PreparationPhase::Reconciled;
        Ok(true)
    }

    pub(super) async fn prepare_publication(
        &self,
        preparation: Box<RunPreparation>,
        fence: WriteFence,
    ) -> Result<PreparationToken> {
        let manifest = preparation.encode()?;
        let mut store = self.inner.lock().await;
        let state = &preparation.new_run.next_state;
        let home = tokeira_types::execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            Self::effective_shard_count(&store),
        );
        fence.ensure_home(home)?;
        store.check_fence(&fence)?;
        if let Some(existing) = store
            .preparations
            .values()
            .find(|row| row.request_key == preparation.request_key)
        {
            if existing.phase == PreparationPhase::Purging {
                return Err(RecoveryError::InvalidPreparation(
                    "previous generation is still purging",
                )
                .into());
            }
            // A retry may propose a fresh random run ID. The committed request
            // selection wins, and publication reads that original manifest.
            return Ok(existing.token.clone());
        }
        if store.runs.contains_key(&state.run_key)
            || store.bulk_writes.contains_key(&state.run_key)
            || store
                .preparations
                .values()
                .any(|row| row.token.target_run == state.run_key)
        {
            return Err(
                RecoveryError::InvalidPreparation("target run storage is already claimed").into(),
            );
        }
        let token = PreparationToken::new(state, home, &manifest);
        store.preparations.insert(
            token.id,
            PreparationRecord {
                token: token.clone(),
                request_key: preparation.request_key,
                manifest,
                phase: PreparationPhase::Ready,
            },
        );
        Ok(token)
    }

    pub(super) async fn publish_preparation(
        &self,
        token: PreparationToken,
        fence: WriteFence,
    ) -> Result<PublicationResult> {
        let mut store = self.inner.lock().await;
        fence.ensure_home(token.home)?;
        store.check_fence(&fence)?;
        let record = store
            .preparations
            .get(&token.id)
            .filter(|record| record.token == token)
            .ok_or(RecoveryError::InvalidPreparation(
                "preparation generation is absent",
            ))?;
        if matches!(
            record.phase,
            PreparationPhase::Published | PreparationPhase::Reconciled
        ) {
            let new_run = store
                .runs
                .get(&token.target_run)
                .ok_or(RecoveryError::InvalidPreparation("published run is absent"))?
                .clone();
            return Ok(PublicationResult::AlreadyPublished {
                new_run: Box::new(new_run),
            });
        }
        if record.phase != PreparationPhase::Ready {
            return Err(RecoveryError::InvalidPreparation("preparation is not ready").into());
        }
        let publication = token.decode_manifest(&record.manifest)?;
        let state = &publication.new_run.next_state;
        let workflow = (state.namespace_id, state.workflow_id.0.clone());
        if store.current_execution.get(&workflow).copied() != publication.expected_current {
            return Ok(PublicationResult::Conflict {
                reason: "current execution changed during preparation".into(),
            });
        }
        if store.runs.contains_key(&token.target_run) {
            return Ok(PublicationResult::Conflict {
                reason: "target run already exists".into(),
            });
        }
        for transition in &publication.participants {
            if store
                .runs
                .get(&transition.next_state.run_key)
                .map(|s| s.transition_seq)
                != Some(transition.expected_seq)
            {
                return Ok(PublicationResult::Conflict {
                    reason: "publication participant sequence changed".into(),
                });
            }
        }
        super::successor::validate_reserved_publication(&store, &publication)?;
        let mut prepared = Vec::new();
        for mutation in publication.participants {
            let run = mutation.next_state.run_key;
            prepared.push((run, transition::prepare(&store, run, mutation)?));
        }
        let new_prepared = transition::prepare(&store, token.target_run, *publication.new_run)?;
        // All encoding, projection and growth checks precede the first mutation.
        // No observer can see the base link without the current termination and
        // complete new run, even if one participant's preparation fails.
        let participants = prepared
            .into_iter()
            .map(|(run, prepared)| transition::write(&mut store, run, prepared))
            .collect();
        let new_run = transition::write(&mut store, token.target_run, new_prepared);
        if let Some(key) = publication.successor_intent {
            store.outbound_intents.remove(&key);
            store.successor_reservations.remove(&workflow);
        }
        store
            .preparations
            .get_mut(&token.id)
            .expect("preparation remains under store lock")
            .phase = PreparationPhase::Published;
        Ok(PublicationResult::Applied {
            participants,
            new_run: Box::new(new_run),
        })
    }

    pub(super) async fn abandon_preparation(
        &self,
        token: PreparationToken,
        fence: WriteFence,
    ) -> Result<bool> {
        let mut store = self.inner.lock().await;
        fence.ensure_home(token.home)?;
        store.check_fence(&fence)?;
        let matches = store.preparations.get(&token.id).is_some_and(|row| {
            row.token == token
                && !matches!(
                    row.phase,
                    PreparationPhase::Published | PreparationPhase::Reconciled
                )
        });
        if matches {
            store.preparations.remove(&token.id);
        }
        Ok(matches)
    }
}

pub(super) fn target_is_preparing(store: &StoreState, run: RunKey) -> bool {
    store.preparations.values().any(|row| {
        row.token.target_run == run
            && !matches!(
                row.phase,
                PreparationPhase::Published | PreparationPhase::Reconciled
            )
    })
}
