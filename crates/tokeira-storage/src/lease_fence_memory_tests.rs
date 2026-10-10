//! Memory-side runs of the shared bundle-lease-fence contracts.

use proptest::prelude::*;

use super::*;
use crate::{
    bulk_write_tests::{commit_base, signal},
    lease_fence_tests::{
        FENCED_WRITES, LOSSES, LeaseBackend, OWNERS, a_lost_fence_changes_nothing, acquire, fence,
        lease_op, lose, only_a_change_of_ownership_writes_the_epoch,
        the_owner_keeps_writing_across_renewals_and_refreshes,
    },
};

/// The memory store, granting leases for a duration as DSQL does, with every
/// run at one home.
struct MemoryLeases(InMemoryStore);

impl MemoryLeases {
    fn new() -> Self {
        Self(InMemoryStore::with_shard_count(1).with_lease_duration(time::Duration::seconds(30)))
    }
}

const HOME: ShardId = ShardId(0);

#[async_trait]
impl LeaseBackend for MemoryLeases {
    fn repo(&self) -> &dyn RunRepository {
        &self.0
    }

    fn leases(&self) -> &dyn LeaseRepository {
        &self.0
    }

    fn shard_count(&self) -> u32 {
        1
    }

    fn home(&self) -> ShardId {
        HOME
    }

    async fn lease(&self) -> Result<Option<(Option<String>, ShardEpoch)>> {
        let store = self.0.inner.lock().await;
        Ok(store
            .bundle_leases
            .get(&HOME)
            .map(|(owner, epoch, _endpoint)| (owner.clone(), *epoch)))
    }

    async fn expire_lease(&self) -> Result<()> {
        let mut store = self.0.inner.lock().await;
        if store.bundle_leases.contains_key(&HOME) {
            store
                .bundle_lease_expiries
                .insert(HOME, tokio::time::Instant::now());
        }
        Ok(())
    }

    async fn clear_lease(&self) -> Result<()> {
        let mut store = self.0.inner.lock().await;
        store.bundle_leases.remove(&HOME);
        store.bundle_lease_expiries.remove(&HOME);
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: bundle-lease-fence, Property 5: Only a change of ownership writes the epoch
    #[test]
    fn property_5_only_a_change_of_ownership_writes_the_epoch(
        ops in prop::collection::vec(lease_op(), 1..24),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(only_a_change_of_ownership_writes_the_epoch(&MemoryLeases::new(), &ops));
    }
}

// Feature: bundle-lease-fence, Property 4: A lost fence is final
#[tokio::test]
async fn property_4_a_lost_fence_changes_nothing() {
    for loss in LOSSES {
        for write in FENCED_WRITES {
            a_lost_fence_changes_nothing(&MemoryLeases::new(), loss, write).await;
        }
    }
}

#[tokio::test]
async fn the_owner_keeps_writing_across_lease_renewals() {
    the_owner_keeps_writing_across_renewals_and_refreshes(&MemoryLeases::new()).await;
}

/// A reset's materialization under a lost fence writes nothing: no record, and
/// no successor.
#[tokio::test]
async fn a_materialization_under_a_lost_fence_writes_nothing() {
    for loss in LOSSES {
        let backend = MemoryLeases::new();
        let store = &backend.0;
        let (base, fork) = commit_base(store, vec![vec![signal(0, 16)]]).await;
        let epoch = acquire(&backend, OWNERS[0]).await;
        let found = lose(&backend, loss, epoch).await;
        let successor_run_id = RunId::new();
        let error = store
            .materialize_reset_successor(
                base.run_key,
                fork,
                successor_run_id,
                Some(base.run_key),
                fence(HOME, OWNERS[0], epoch),
            )
            .await
            .expect_err("a materialization under a lost fence must fail");
        assert_eq!(
            error.downcast_ref::<LeaseFenceLost>(),
            Some(&LeaseFenceLost {
                home: HOME,
                epoch,
                found
            }),
            "{loss:?}"
        );
        let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
        assert!(matches!(
            store.load_run(successor).await.unwrap(),
            LoadedRun::Absent
        ));
        assert!(
            store
                .list_run_bulk_writes(HOME, None, 10)
                .await
                .unwrap()
                .is_empty(),
            "{loss:?}"
        );
    }
}

/// With a lease duration, the memory store's acquisition takes over an expired
/// lease and refuses an unexpired one, by the tokio clock.
#[tokio::test(start_paused = true)]
async fn an_unrenewed_lease_lapses_after_its_duration() {
    let backend = MemoryLeases::new();
    let epoch = acquire(&backend, OWNERS[0]).await;
    let refused = backend
        .0
        .try_acquire_bundle(HOME, OWNERS[1].to_owned(), "127.0.0.1:7234".to_owned())
        .await
        .unwrap();
    assert_eq!(
        refused,
        LeaseOutcome::Rejected {
            current_owner: OWNERS[0].to_owned(),
            current_epoch: epoch,
        }
    );
    tokio::time::advance(std::time::Duration::from_secs(29)).await;
    assert!(matches!(
        backend
            .0
            .renew_bundle(
                HOME,
                OWNERS[0].to_owned(),
                epoch,
                "127.0.0.1:7233".to_owned()
            )
            .await
            .unwrap(),
        LeaseOutcome::Renewed { .. }
    ));
    tokio::time::advance(std::time::Duration::from_secs(29)).await;
    assert!(matches!(
        backend
            .0
            .try_acquire_bundle(HOME, OWNERS[1].to_owned(), "127.0.0.1:7234".to_owned())
            .await
            .unwrap(),
        LeaseOutcome::Rejected { .. }
    ));
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        acquire(&backend, OWNERS[1]).await,
        epoch.next(),
        "a takeover advances the epoch"
    );
}
