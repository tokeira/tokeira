//! Physical memory placement model, with page changes staged before publication.
//!
//! The store lock is the memory equivalent of the DSQL marker write conflict.
//! Validation and budget checks finish before any map changes, so a failed page
//! leaves both locations and its durable cursor unchanged.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tokeira_kernel::TimerState;
use tokeira_types::{NamespaceId, RunKey, ShardId, WorkflowId, execution_home_bundle};

use super::{InMemoryStore, PLACEMENT_SECTION, SnapshotError, StoreState};
use crate::{
    PlacementPage, PlacementPhase, PlacementProgress, RunRepository, TimerPosition,
    codec::{ExtensionSection, decode_exact, encode_timer_state, encode_workflow_state},
    placement_upgrade::{MOVE_KEYS, SCREEN_KEYS, shard_uuid, validate_identity, validate_source},
    write_budget::{MAX_BYTES_PER_TRANSACTION, MAX_ROWS_PER_TRANSACTION, WriteCost},
};

#[derive(Serialize, Deserialize)]
struct PlacementSnapshot {
    progress: Option<PlacementProgress>,
    columns: BTreeMap<RunKey, (NamespaceId, WorkflowId)>,
    timers: Vec<(TimerPosition, TimerState)>,
}

/// Preserve locations the frozen logical snapshot cannot express.
pub(super) fn capture_extension(
    state: &StoreState,
    sections: &mut Vec<ExtensionSection>,
) -> Result<(), SnapshotError> {
    let exceptional = state.timer_bucket.keys().any(|key| {
        state
            .run_shard_map
            .get(&key.run_key)
            .is_none_or(|shard| key.shard != shard_uuid(*shard))
    });
    let repeated_identity = state
        .timer_bucket
        .keys()
        .map(|key| (key.run_key, &key.timer_id))
        .collect::<BTreeSet<_>>()
        .len()
        != state.timer_bucket.len();
    if repeated_identity
        || exceptional
        || state.placement_progress.is_some()
        || !state.placement_columns.is_empty()
    {
        sections.push(ExtensionSection {
            tag: PLACEMENT_SECTION,
            payload: postcard::to_allocvec(&PlacementSnapshot {
                progress: state.placement_progress.clone(),
                columns: state.placement_columns.clone(),
                timers: state
                    .timer_bucket
                    .iter()
                    .map(|(key, timer)| (key.clone(), timer.clone()))
                    .collect(),
            })
            .map_err(SnapshotError::Encode)?,
        });
    }
    Ok(())
}

/// Restore physical keys without normalizing them before preparation runs.
pub(super) fn apply_extension(state: &mut StoreState, bytes: &[u8]) -> Result<(), &'static str> {
    let decoded: PlacementSnapshot = decode_exact(bytes).ok_or("invalid placement section")?;
    let mut timers = BTreeMap::new();
    for (key, timer) in decoded.timers {
        if timers.insert(key, timer).is_some() {
            return Err("physical timer key repeated in placement section");
        }
    }
    if decoded
        .columns
        .keys()
        .any(|key| !state.runs.contains_key(key))
    {
        return Err("placement columns name a missing hot row");
    }
    state.placement_progress = decoded.progress;
    state.placement_columns = decoded.columns;
    state.timer_bucket = timers;
    Ok(())
}

fn identity(store: &StoreState, key: RunKey) -> Option<(NamespaceId, &WorkflowId)> {
    store
        .placement_columns
        .get(&key)
        .map(|(ns, wf)| (*ns, wf))
        .or_else(|| {
            store
                .runs
                .get(&key)
                .map(|state| (state.namespace_id, &state.workflow_id))
        })
}

fn home(store: &StoreState, key: RunKey) -> Option<ShardId> {
    identity(store, key).map(|(ns, wf)| {
        execution_home_bundle(
            ns.0.as_bytes(),
            wf.0.as_bytes(),
            InMemoryStore::effective_shard_count(store),
        )
    })
}

fn validate(store: &StoreState, key: RunKey, stored: uuid::Uuid, target: ShardId) -> Result<()> {
    validate_source(
        key,
        stored,
        target,
        InMemoryStore::effective_shard_count(store),
    )?;
    let (ns, wf) = identity(store, key).expect("moving rows have a hot identity");
    validate_identity(key, stored, target, ns, wf, &store.runs[&key])
}

fn prefix(first_move: Option<usize>, length: usize) -> usize {
    match first_move {
        None => length,
        Some(index) if index >= MOVE_KEYS => index,
        Some(_) => length.min(MOVE_KEYS),
    }
}

fn fits(total: WriteCost, extra: WriteCost) -> bool {
    total.rows + extra.rows <= MAX_ROWS_PER_TRANSACTION
        && total.bytes + extra.bytes <= MAX_BYTES_PER_TRANSACTION
}

impl InMemoryStore {
    /// Publish a validated page and its cursor together under the store lock.
    pub(super) async fn prepare_memory_placement_page(&self) -> Result<PlacementPage> {
        let mut store = self.inner.lock().await;
        if self.placement_ready() && store.placement_progress.is_none() {
            return Ok(PlacementPage::Committed(PlacementProgress {
                phase: PlacementPhase::Complete,
                ..PlacementProgress::default()
            }));
        }
        let mut progress = store.placement_progress.clone().unwrap_or_default();
        if progress.complete() {
            self.placement_ready
                .store(true, super::AtomicOrdering::Release);
            return Ok(PlacementPage::Committed(progress));
        }
        let mut cost = WriteCost::write(8_192);
        let mut hot_moves = Vec::new();
        let mut timer_moves = Vec::new();
        match progress.phase.clone() {
            PlacementPhase::Hot(after) => {
                let mut keys: Vec<_> = store
                    .runs
                    .keys()
                    .copied()
                    .filter(|key| after.is_none_or(|after| *key > after))
                    .collect();
                keys.sort_unstable();
                keys.truncate(SCREEN_KEYS);
                let first_move = keys
                    .iter()
                    .position(|key| store.run_shard_map.get(key).copied() != home(&store, *key));
                for key in keys.iter().take(prefix(first_move, keys.len())) {
                    let target = home(&store, *key).expect("hot row has identity");
                    let stored = store.run_shard_map.get(key).copied().ok_or_else(||
                        anyhow::anyhow!("placement upgrade run={key:?} stored_shard=missing computed_home={target:?}: missing physical hot placement"))?;
                    if stored != target {
                        validate(&store, *key, shard_uuid(stored), target)?;
                        let (_, wf) = identity(&store, *key).expect("hot row has identity");
                        let extra = WriteCost::write(
                            encode_workflow_state(&store.runs[key])?.len() + wf.0.len() + 128,
                        );
                        ensure!(
                            extra.bytes + 8_192 <= MAX_BYTES_PER_TRANSACTION,
                            "placement upgrade run={key:?} stored_shard={stored:?} computed_home={target:?}: hot row exceeds relocation budget"
                        );
                        if !fits(cost, extra) {
                            break;
                        }
                        cost.rows += extra.rows;
                        cost.bytes += extra.bytes;
                        hot_moves.push((*key, target));
                        progress.hot_moved += 1;
                    }
                    progress.hot_examined += 1;
                    progress.phase = PlacementPhase::Hot(Some(*key));
                }
                if keys.is_empty() {
                    progress.phase = PlacementPhase::Timers(None);
                }
            }
            PlacementPhase::Timers(after) => {
                let keys: Vec<_> = store
                    .timer_bucket
                    .keys()
                    .filter(|key| after.as_ref().is_none_or(|after| *key > after))
                    .take(SCREEN_KEYS)
                    .cloned()
                    .collect();
                cost.bytes = keys
                    .iter()
                    .map(|key| key.timer_id.len() + 512)
                    .max()
                    .unwrap_or(0)
                    .max(cost.bytes);
                // A large timer identity also enlarges the durable cursor. A
                // move must fit with that reserve on an otherwise empty page,
                // or retries would commit the same position forever.
                let marker_cost = cost;
                let first_move = keys.iter().position(|key| {
                    home(&store, key.run_key).is_some_and(|home| key.shard != shard_uuid(home))
                });
                for key in keys.iter().take(prefix(first_move, keys.len())) {
                    if let Some(target) =
                        home(&store, key.run_key).filter(|home| key.shard != shard_uuid(*home))
                    {
                        validate(&store, key.run_key, key.shard, target)?;
                        let timer = &store.timer_bucket[key];
                        let destination = TimerPosition {
                            shard: shard_uuid(target),
                            ..key.clone()
                        };
                        let payload = encode_timer_state(timer)?;
                        if let Some(existing) = store.timer_bucket.get(&destination) {
                            ensure!(
                                encode_timer_state(existing)? == payload,
                                "placement upgrade run={:?} stored_shard={} computed_home={target:?} timer={}: conflicting destination timer payload",
                                key.run_key,
                                key.shard,
                                key.timer_id
                            );
                        }
                        let extra = WriteCost {
                            rows: 2,
                            bytes: payload.len() + key.timer_id.len() + 128,
                        };
                        ensure!(
                            fits(marker_cost, extra),
                            "placement upgrade run={:?} stored_shard={} computed_home={target:?}: timer and marker exceed relocation budget",
                            key.run_key,
                            key.shard
                        );
                        if !fits(cost, extra) {
                            break;
                        }
                        cost.rows += extra.rows;
                        cost.bytes += extra.bytes;
                        timer_moves.push((key.clone(), destination, timer.clone()));
                        progress.timers_moved += 1;
                    }
                    progress.timers_examined += 1;
                    progress.phase = PlacementPhase::Timers(Some(key.clone()));
                }
                if keys.is_empty() {
                    progress.phase = PlacementPhase::Complete;
                }
            }
            PlacementPhase::Complete => unreachable!("completed progress returned above"),
        }
        let mut model = store.begin_bulk_transaction()?;
        // The model enforces the shared service budget too; no mutation precedes
        // its acceptance, including a failure injected between pages.
        let cursor_bytes = match &progress.phase {
            PlacementPhase::Timers(Some(cursor)) => (cursor.timer_id.len() + 512).max(8_192),
            _ => 8_192,
        };
        model.write(&[cursor_bytes])?;
        for (key, _) in &hot_moves {
            let (_, wf) = identity(&store, *key).expect("hot row has identity");
            model.write(&[
                encode_workflow_state(&store.runs[key])?.len(),
                wf.0.len(),
                128,
            ])?;
        }
        for (source, _, timer) in &timer_moves {
            model.write(&[encode_timer_state(timer)?.len(), source.timer_id.len(), 128])?;
            model.delete(1)?;
        }
        store.commit_bulk_transaction(&model);
        for (key, home) in hot_moves {
            store.run_shard_map.insert(key, home);
        }
        for (source, destination, timer) in timer_moves {
            store.timer_bucket.entry(destination).or_insert(timer);
            store.timer_bucket.remove(&source);
        }
        store.placement_progress = Some(progress.clone());
        if progress.complete() {
            self.placement_ready
                .store(true, super::AtomicOrdering::Release);
        }
        Ok(PlacementPage::Committed(progress))
    }
}
