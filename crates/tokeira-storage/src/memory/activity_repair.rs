//! Bounded activity repair from authority under the memory store's writer lock.
//! Every fallible decision precedes mutation, matching DSQL transaction rollback.

use super::*;
use crate::{
    ActivityRepairCursor, ActivityRepairPage, ActivityRepairPhase,
    activity_dispatch::{activity_repair_continuation, activity_repair_fits},
};

impl InMemoryStore {
    pub(super) async fn repair_activities(
        &self,
        home: ShardId,
        run: RunKey,
        after: Option<ActivityRepairCursor>,
        fence: WriteFence,
    ) -> Result<ActivityRepairPage> {
        fence.ensure_home(home)?;
        let mut store = self.inner.lock().await;
        store.check_fence(&fence)?;
        let state = store.runs.get(&run);
        if let Some(state) = state {
            let actual = tokeira_types::execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                Self::effective_shard_count(&store),
            );
            anyhow::ensure!(
                actual == home && store.run_shard_map.get(&run) == Some(&home),
                "activity repair authoritative home mismatch for {run:?}"
            );
        }
        let mut cursor =
            ActivityRepairCursor::resume(after, state.map(|state| state.transition_seq));
        let wanted = state
            .map(|state| crate::derive_activity_dispatch(state, home))
            .transpose()?
            .unwrap_or_default();
        let limit = crate::ACTIVITY_DISPATCH_PAGE_LIMIT as usize;
        let keys = match cursor.phase {
            ActivityRepairPhase::Wanted => wanted
                .keys()
                .copied()
                .filter(|key| cursor.after.is_none_or(|after| *key > after))
                .take(limit)
                .collect::<Vec<_>>(),
            ActivityRepairPhase::Existing => {
                let mut keys = store
                    .activity_dispatch
                    .iter()
                    .filter(|((key, _), _)| *key == run)
                    .map(|(_, row)| row.key)
                    .filter(|key| cursor.after.is_none_or(|after| *key > after))
                    .collect::<Vec<_>>();
                keys.sort_unstable();
                keys.truncate(limit);
                keys
            }
        };
        let mut exhausted = keys.len() < limit;
        let mut bytes = 0;
        let mut examined = 0;
        let mut changes = Vec::new();
        for key in keys {
            // Find by the physical slot so a colliding foreign identity is not
            // hidden by memory's raw-identity map key.
            let observed = store
                .activity_dispatch
                .iter()
                .find(|(_, row)| row.key == key);
            if let Some(((stored_run, id), row)) = observed {
                anyhow::ensure!(
                    *stored_run == run
                        && row.task.run_key == run
                        && row.task.activity_id == *id
                        && row.key == crate::activity_dispatch_key(run, id),
                    "activity repair slot disagrees with raw identity for {run:?}"
                );
                if state.is_none() {
                    anyhow::ensure!(
                        row.execution_home == home,
                        "orphan activity repair home mismatch for {run:?}"
                    );
                }
                if let Some(desired) = wanted.get(&key) {
                    anyhow::ensure!(
                        desired.incarnation.activity_id == *id,
                        "activity repair slot hash collision for {run:?}"
                    );
                }
            }
            let replacement = wanted.get(&key).map(|row| {
                let activity = &state
                    .expect("wanted rows require authoritative state")
                    .activities[&row.incarnation.activity_id];
                ActivityDispatchEntry::from_row(row, activity)
            });
            if observed.map(|(_, row)| row) != replacement.as_ref() {
                let cost = wanted
                    .get(&key)
                    .map(crate::ActivityDispatchRow::write_cost)
                    .transpose()?
                    .unwrap_or(crate::write_budget::WriteCost::DELETE);
                if !activity_repair_fits(bytes, cost)? {
                    exhausted = false;
                    break;
                }
                bytes += cost.bytes;
                let raw_key = observed.map(|(raw, _)| raw.clone()).unwrap_or_else(|| {
                    (
                        run,
                        replacement
                            .as_ref()
                            .expect("missing observed slot has a wanted replacement")
                            .task
                            .activity_id
                            .clone(),
                    )
                });
                changes.push((raw_key, replacement));
            }
            cursor.after = Some(key);
            examined += 1;
        }
        let changed = changes.len();
        for (raw, replacement) in changes {
            if let Some(row) = replacement {
                store.activity_dispatch.insert(raw, row);
            } else {
                store.activity_dispatch.remove(&raw);
            }
        }
        Ok(ActivityRepairPage {
            continuation: activity_repair_continuation(cursor, exhausted),
            examined,
            changed,
        })
    }
}
