//! In-memory model of the exclusive stopped-writer preparation revision.
//!
//! Validate the whole page before changing maps; the lock publishes source
//! metadata, reconstructed indexes and continuation as one durable snapshot cut.

use super::{InMemoryStore, StoreState};
use crate::outbound_upgrade::{
    OutboundUpgradeProgress, OutboundUpgradeStatus, OutboundUpgradeWrite, validate_page,
    validate_write,
};
use anyhow::{Result, ensure};

pub(super) fn ready(store: &StoreState) -> bool {
    store
        .outbound_upgrade
        .as_ref()
        .is_none_or(|progress| progress.complete)
        && store.runs.values().all(|state| state.outbound.is_some())
}

fn live_leases(store: &StoreState) -> u64 {
    store
        .bundle_leases
        .iter()
        .filter(|(home, (owner, _, _))| {
            owner.is_some()
                && store
                    .bundle_lease_expiries
                    .get(home)
                    .is_none_or(|expiry| *expiry > tokio::time::Instant::now())
        })
        .count() as u64
}

impl InMemoryStore {
    pub(super) async fn memory_outbound_upgrade_status(&self) -> Result<OutboundUpgradeStatus> {
        let store = self.inner.lock().await;
        Ok(OutboundUpgradeStatus {
            progress: store.outbound_upgrade.clone(),
            live_leases: live_leases(&store),
            pending_intents: store.outbound_intents.len() as u64,
            oldest_intent_at: store
                .outbound_intents
                .values()
                .map(|row| row.created_at)
                .min(),
        })
    }

    pub(super) async fn memory_outbound_upgrade_page(
        &self,
        expected: Option<OutboundUpgradeProgress>,
        next: OutboundUpgradeProgress,
        write: Option<OutboundUpgradeWrite>,
    ) -> Result<()> {
        let mut store = self.inner.lock().await;
        ensure!(
            live_leases(&store) == 0,
            "live leases prevent stopped-cluster preparation"
        );
        ensure!(
            store.outbound_upgrade == expected,
            "maintenance revision changed; review or resume the owning plan"
        );
        let after = expected.as_ref().and_then(|progress| progress.after);
        let first = store
            .runs
            .keys()
            .filter(|key| after.is_none_or(|after| **key > after))
            .min()
            .copied();
        validate_page(expected.as_ref(), &next, first, write.as_ref())?;
        let mut rows = Vec::new();
        let mut canonical_tails = 0usize;
        if let Some(write) = &write {
            let prior = store
                .runs
                .get(&write.state.run_key)
                .ok_or_else(|| anyhow::anyhow!("reviewed source disappeared"))?;
            validate_write(prior, write)?;
            let home = tokeira_types::execution_home_bundle(
                write.state.namespace_id.0.as_bytes(),
                write.state.workflow_id.0.as_bytes(),
                Self::effective_shard_count(&store),
            );
            rows = crate::derive_operations(&write.state, home)?;
            if prior.outbound.is_none() {
                let bytes = crate::codec::encode_workflow_state(&write.state)?;
                canonical_tails = bytes.len().div_ceil(983_040).saturating_sub(1);
                ensure!(
                    canonical_tails <= 900,
                    "legacy canonical state exceeds final part-retirement budget"
                );
                for page in bytes.chunks(4 * 983_040) {
                    let mut model = store.begin_bulk_transaction()?;
                    for part in page.chunks(983_040) {
                        model.write(&[part.len()]);
                    }
                    store.commit_bulk_transaction(&model);
                }
            }
            let existing = store
                .outbound_operations
                .keys()
                .filter(|key| key.source_run == write.state.run_key)
                .count();
            let mut remaining = existing;
            while remaining != 0 {
                let mut model = store.begin_bulk_transaction()?;
                let deleted = remaining.min(900);
                model.delete(deleted);
                remaining -= deleted;
                store.commit_bulk_transaction(&model);
            }
            for page in rows.chunks(100) {
                let mut model = store.begin_bulk_transaction()?;
                for row in page {
                    model.write(&[postcard::to_allocvec(row)?.len()]);
                }
                store.commit_bulk_transaction(&model);
            }
        }
        let mut final_page = store.begin_bulk_transaction()?;
        final_page.write(&[8192]);
        if write.is_some() {
            final_page.write(&[256]);
            final_page.write(&[2048]);
            final_page.delete(canonical_tails);
        }
        store.commit_bulk_transaction(&final_page);
        if let Some(write) = write {
            store
                .outbound_operations
                .retain(|key, _| key.source_run != write.state.run_key);
            store
                .outbound_operations
                .extend(rows.into_iter().map(|row| (row.incarnation.key, row)));
            store.runs.insert(write.state.run_key, write.state);
        }
        store.outbound_upgrade = Some(next);
        Ok(())
    }
}
