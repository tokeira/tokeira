//! Live checks of the bundle lease fence (`bundle-lease-fence`) against an
//! operator-owned ephemeral Aurora DSQL cluster. No connection occurs unless the
//! dedicated test URL is provided.
//!
//! Each case holds a write at its pause point, after its fence read and before
//! its commit, lets another node take the lease over, then lets the write go on.
//! A node that has lost its lease must not commit: DSQL aborts the write, since
//! the takeover wrote the epoch, a key column, after the write read it
//! `FOR KEY SHARE`, and the write's retry finds the new owner.

use std::path::Path;

use proptest::{
    prelude::*,
    test_runner::{TestCaseResult, TestRunner},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use time::OffsetDateTime;
use tokeira_kernel::{LoadedRun, Transition, WorkflowState};
use tokeira_types::{ExecutionStatus, RunId, RunKey, ShardEpoch, ShardId, WorkflowId};
use tokio::{runtime::Runtime, sync::oneshot};

use super::{
    DsqlRunRepository,
    pause_point::{self, PauseKey},
};
use crate::{
    BulkWritePhase, CommitResult, CurrentExecutionConflictPolicy, DeleteRunRequest,
    DeleteRunResult, ExecutionHomeFence, LeaseFenceLost, LeaseOutcome, LeaseRepository,
    RunRepository, WriteFence,
    bulk_write_tests::{Backend, OwnedRows, StoredBatch, bundle, commit_base, signal},
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
    lease_fence_tests::{
        FENCED_WRITES, LOSSES, LeaseBackend, a_lost_fence_changes_nothing, lease_op,
        only_a_change_of_ownership_writes_the_epoch,
        the_owner_keeps_writing_across_renewals_and_refreshes,
    },
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
};

/// Enough homes that these cases have one to themselves.
const SHARD_COUNT: u32 = 256;
/// The execution home of every run these cases write.
const HOME: ShardId = ShardId(241);
const OLD_OWNER: &str = "lease-fence-old-owner";
const NEW_OWNER: &str = "lease-fence-new-owner";
const ENDPOINT: &str = "127.0.0.1:7233";

struct Fixture {
    pool: PgPool,
    store: DsqlStore,
}

impl Fixture {
    /// The cluster's store, or none when the live suite isn't configured.
    async fn connect() -> anyhow::Result<Option<Self>> {
        let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .min_connections(2)
            .idle_timeout(None)
            .connect(&url)
            .await?;
        let store = DsqlStore::from_database_url_for_tests(
            url,
            DsqlPoolConfig {
                migration: MigrationConfig {
                    migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
                },
                // The interleavings hold up to three writes at their pause
                // points, each with an open transaction on a commit-class
                // connection, beside their retries: twelve ready connections
                // give the commit class four.
                reservoir: ReservoirConfig {
                    target_ready: 12,
                    inflight_limit: 2,
                    ..ReservoirConfig::default()
                },
                shard_count: SHARD_COUNT,
                conflict_policy: CurrentExecutionConflictPolicy::Reject,
                ..DsqlPoolConfig::default()
            },
        )
        .await?;
        store.migration_runner().apply(&pool).await?;
        let fixture = Self { pool, store };
        fixture.clear_lease().await?;
        Ok(Some(fixture))
    }

    fn repo(&self) -> &DsqlRunRepository {
        self.store.run_repository()
    }

    async fn clear_lease(&self) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM shard_lease WHERE shard_id = $1")
            .bind(DsqlRunRepository::shard_id_to_uuid(HOME))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// The old owner's lease, at the epoch it was granted.
    async fn acquire_old(&self) -> anyhow::Result<ShardEpoch> {
        match self
            .repo()
            .try_acquire_bundle(HOME, OLD_OWNER.to_owned(), ENDPOINT.to_owned())
            .await?
        {
            LeaseOutcome::Acquired { epoch } => Ok(epoch),
            other => anyhow::bail!("the old owner's acquisition answered {other:?}"),
        }
    }

    /// Another node takes the lease over once it has expired. The expiry is
    /// forced, so the case doesn't wait out a lease.
    async fn take_over(&self) -> anyhow::Result<ShardEpoch> {
        sqlx::query(
            "UPDATE shard_lease SET lease_expiry = now() - interval '1 second' WHERE shard_id = $1",
        )
        .bind(DsqlRunRepository::shard_id_to_uuid(HOME))
        .execute(&self.pool)
        .await?;
        match self
            .repo()
            .try_acquire_bundle(HOME, NEW_OWNER.to_owned(), ENDPOINT.to_owned())
            .await?
        {
            LeaseOutcome::Acquired { epoch } => Ok(epoch),
            other => anyhow::bail!("the takeover answered {other:?}"),
        }
    }

    /// The takeover, run while a write waits at its pause point.
    async fn take_over_while_paused(
        &self,
        reached: tokio::sync::oneshot::Receiver<()>,
        resume: tokio::sync::oneshot::Sender<()>,
    ) -> anyhow::Result<ShardEpoch> {
        reached
            .await
            .map_err(|_| anyhow::anyhow!("the write never reached its pause point"))?;
        let epoch = self.take_over().await;
        let _ = resume.send(());
        epoch
    }
}

/// The old owner's fence at `epoch`.
fn old_fence(epoch: ShardEpoch) -> WriteFence {
    WriteFence::Lease(ExecutionHomeFence {
        home: HOME,
        owner: OLD_OWNER.to_owned(),
        epoch,
    })
}

/// Whether a write failed with a lost fence.
fn is_lease_lost<T>(result: &anyhow::Result<T>) -> bool {
    result
        .as_ref()
        .is_err_and(|error| error.downcast_ref::<LeaseFenceLost>().is_some())
}

/// A run's start whose execution home is `HOME`.
fn start_at_home() -> Transition {
    let mut transition = fresh_transition(RunKey::new());
    let namespace_id = transition.next_state.namespace_id;
    let workflow_id = (0..)
        .map(|index| {
            WorkflowId(format!(
                "lease-fence-{}-{index}",
                transition.next_state.run_key.0
            ))
        })
        .find(|workflow_id| {
            tokeira_types::execution_home_bundle(
                namespace_id.0.as_bytes(),
                workflow_id.0.as_bytes(),
                SHARD_COUNT,
            ) == HOME
        })
        .expect("some workflow id lands on the home");
    transition.next_state.workflow_id = workflow_id;
    transition
}

/// Start a run at `HOME` under the old owner's lease.
async fn started(fixture: &Fixture, epoch: ShardEpoch) -> anyhow::Result<WorkflowState> {
    let start = start_at_home();
    let run_key = start.next_state.run_key;
    Ok(applied(
        fixture
            .repo()
            .commit_transition_for_bundle(run_key, HOME, start, old_fence(epoch))
            .await?,
    ))
}

/// A transition commit through `commit_transition_for_bundle` that the old owner
/// admitted before a takeover doesn't commit after it.
#[tokio::test]
async fn dsql_lease_fence_stale_commit_for_bundle_is_refused() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let epoch = fixture.acquire_old().await?;
    let state = started(&fixture, epoch).await?;

    let (reached, resume) = pause_point::arm(PauseKey::Write(state.run_key));
    let (written, taken_over) = tokio::join!(
        fixture.repo().commit_transition_for_bundle(
            state.run_key,
            HOME,
            following(&state),
            old_fence(epoch)
        ),
        fixture.take_over_while_paused(reached, resume),
    );
    let new_epoch = taken_over?;
    let retried = fixture
        .repo()
        .commit_transition_for_bundle(state.run_key, HOME, following(&state), old_fence(epoch))
        .await;
    fixture.clear_lease().await?;
    assert_ne!(new_epoch, epoch);
    assert!(
        matches!(written, Ok(CommitResult::Conflict { .. })),
        "the old owner's commit wasn't aborted after the takeover: {written:?}"
    );
    assert!(is_lease_lost(&retried), "{retried:?}");
    Ok(())
}

/// A transition commit through `commit_transition` with the old owner's epoch
/// doesn't commit after a takeover.
#[tokio::test]
async fn dsql_lease_fence_stale_commit_is_refused() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let epoch = fixture.acquire_old().await?;
    let state = started(&fixture, epoch).await?;

    let (reached, resume) = pause_point::arm(PauseKey::Write(state.run_key));
    let (written, taken_over) = tokio::join!(
        fixture
            .repo()
            .commit_transition(state.run_key, following(&state), old_fence(epoch)),
        fixture.take_over_while_paused(reached, resume),
    );
    let new_epoch = taken_over?;
    let retried = fixture
        .repo()
        .commit_transition(state.run_key, following(&state), old_fence(epoch))
        .await;
    fixture.clear_lease().await?;
    assert_ne!(new_epoch, epoch);
    assert!(
        matches!(written, Ok(CommitResult::Conflict { .. })),
        "the old owner's commit wasn't aborted after the takeover: {written:?}"
    );
    assert!(is_lease_lost(&retried), "{retried:?}");
    Ok(())
}

/// A deletion's first transaction under the old owner's epoch doesn't commit
/// after a takeover.
#[tokio::test]
async fn dsql_lease_fence_stale_deletion_is_refused() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let epoch = fixture.acquire_old().await?;
    let state = started(&fixture, epoch).await?;
    let mut close = following(&state);
    close.next_state.status = ExecutionStatus::Completed;
    close.next_state.closed_at = Some(close.next_state.started_at);
    close.next_state.pending_workflow_task = None;
    let closed = applied(
        fixture
            .repo()
            .commit_transition_for_bundle(state.run_key, HOME, close, old_fence(epoch))
            .await?,
    );
    let request = DeleteRunRequest {
        expected_seq: closed.transition_seq,
        deleted_at: OffsetDateTime::now_utc(),
    };

    let (reached, resume) = pause_point::arm(PauseKey::Write(closed.run_key));
    let (deleted, taken_over) = tokio::join!(
        fixture
            .repo()
            .delete_run_for_bundle(closed.run_key, HOME, request, old_fence(epoch)),
        fixture.take_over_while_paused(reached, resume),
    );
    let new_epoch = taken_over?;
    let retried = fixture
        .repo()
        .delete_run_for_bundle(closed.run_key, HOME, request, old_fence(epoch))
        .await;
    fixture.clear_lease().await?;
    assert_ne!(new_epoch, epoch);
    assert!(
        matches!(deleted, Ok(DeleteRunResult::Conflict { .. })),
        "the old owner's deletion wasn't aborted after the takeover: {deleted:?}"
    );
    assert!(is_lease_lost(&retried), "{retried:?}");
    Ok(())
}

// The shared contracts, live, and the rest of the live suite.

impl Fixture {
    async fn clear_lease_at(&self, home: ShardId) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM shard_lease WHERE shard_id = $1")
            .bind(DsqlRunRepository::shard_id_to_uuid(home))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Make the home's lease expire now. The expiry isn't a key column, so the
    /// update conflicts with no fence read.
    async fn expire_lease_at(&self, home: ShardId) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE shard_lease SET lease_expiry = now() - interval '1 second' WHERE shard_id = $1",
        )
        .bind(DsqlRunRepository::shard_id_to_uuid(home))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn acquire_at(&self, home: ShardId, owner: &str) -> anyhow::Result<ShardEpoch> {
        match self
            .repo()
            .try_acquire_bundle(home, owner.to_owned(), ENDPOINT.to_owned())
            .await?
        {
            LeaseOutcome::Acquired { epoch } => Ok(epoch),
            other => anyhow::bail!("{owner}'s acquisition answered {other:?}"),
        }
    }

    async fn take_over_at(&self, home: ShardId, owner: &str) -> anyhow::Result<ShardEpoch> {
        self.expire_lease_at(home).await?;
        self.acquire_at(home, owner).await
    }

    async fn lease_at(
        &self,
        home: ShardId,
    ) -> anyhow::Result<Option<(Option<String>, ShardEpoch)>> {
        let row = sqlx::query_as::<_, (Option<String>, i64)>(
            "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
        )
        .bind(DsqlRunRepository::shard_id_to_uuid(home))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(owner, epoch)| Ok((owner, super::epoch_from_sql(epoch)?)))
            .transpose()
    }
}

#[async_trait::async_trait]
impl LeaseBackend for Fixture {
    fn repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }

    fn leases(&self) -> &dyn LeaseRepository {
        self.store.run_repository()
    }

    fn shard_count(&self) -> u32 {
        SHARD_COUNT
    }

    fn home(&self) -> ShardId {
        HOME
    }

    async fn lease(&self) -> anyhow::Result<Option<(Option<String>, ShardEpoch)>> {
        self.lease_at(HOME).await
    }

    async fn expire_lease(&self) -> anyhow::Result<()> {
        self.expire_lease_at(HOME).await
    }

    async fn clear_lease(&self) -> anyhow::Result<()> {
        self.clear_lease_at(HOME).await
    }
}

/// The reset fixtures of `bounded-bulk-writes` build their bases through this.
#[async_trait::async_trait]
impl Backend for Fixture {
    fn repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }

    fn shard_count(&self) -> u32 {
        SHARD_COUNT
    }

    async fn owned_rows(&self, _run_key: RunKey) -> anyhow::Result<OwnedRows> {
        anyhow::bail!("the lease fence cases don't count a run's rows")
    }

    async fn history_batches(&self, _run_key: RunKey) -> anyhow::Result<Vec<StoredBatch>> {
        anyhow::bail!("the lease fence cases don't read a run's batches")
    }
}

/// Run `case` for every value `strategy` generates against one fixture, as a
/// property's live counterpart. Nothing runs unless the live suite is
/// configured.
fn live_property<S: Strategy>(
    strategy: S,
    cases: u32,
    case: impl Fn(&Runtime, &Fixture, S::Value) -> TestCaseResult,
) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let Some(fixture) = runtime
        .block_on(Fixture::connect())
        .expect("the live fixture connects")
    else {
        return;
    };
    let result = TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..ProptestConfig::with_cases(cases)
    })
    .run(&strategy, |value| case(&runtime, &fixture, value));
    runtime
        .block_on(fixture.clear_lease())
        .expect("the lease clears");
    result.expect("the property holds on every generated case");
}

// Feature: bundle-lease-fence, Property 5: Only a change of ownership writes the epoch
#[test]
fn dsql_lease_fence_only_a_change_of_ownership_writes_the_epoch() {
    live_property(
        prop::collection::vec(lease_op(), 1..16),
        100,
        |runtime, fixture, ops| {
            runtime.block_on(only_a_change_of_ownership_writes_the_epoch(fixture, &ops));
            Ok(())
        },
    );
}

// Feature: bundle-lease-fence, Property 4: A lost fence is final
#[tokio::test]
async fn dsql_lease_fence_a_lost_fence_changes_nothing() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    for loss in LOSSES {
        for write in FENCED_WRITES {
            a_lost_fence_changes_nothing(&fixture, loss, write).await;
        }
    }
    Ok(())
}

#[tokio::test]
async fn dsql_lease_fence_the_owner_keeps_writing_across_renewals() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    the_owner_keeps_writing_across_renewals_and_refreshes(&fixture).await;
    Ok(())
}

/// A reset's materialization stopped between its transactions, before its
/// first copy page, while another node takes the home over: the page finds the
/// new owner and copies nothing, and the successor never becomes visible. Its
/// record stays for a purge.
#[tokio::test]
async fn dsql_lease_fence_stopped_materialization_is_refused_after_a_takeover() -> anyhow::Result<()>
{
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let (base, fork) = commit_base(&fixture, vec![vec![signal(0, 16)]]).await;
    let home = bundle(&fixture, &base);
    fixture.clear_lease_at(home).await?;
    let epoch = fixture.acquire_at(home, OLD_OWNER).await?;
    let fence = WriteFence::Lease(ExecutionHomeFence {
        home,
        owner: OLD_OWNER.to_owned(),
        epoch,
    });
    let successor_run_id = RunId::new();
    let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);

    // The record's transaction commits; the first copy page waits to begin.
    let (reached, resume) = pause_point::arm(PauseKey::Page(successor));
    let (materialized, taken_over) = tokio::join!(
        fixture.repo().materialize_reset_successor(
            base.run_key,
            fork,
            successor_run_id,
            Some(base.run_key),
            fence,
        ),
        async {
            reached
                .await
                .map_err(|_| anyhow::anyhow!("the copy page never reached its pause point"))?;
            let taken = fixture.take_over_at(home, NEW_OWNER).await;
            let _ = resume.send(());
            taken
        },
    );
    let taken = taken_over?;
    let error = materialized.expect_err("a materialization under a lost lease must fail");
    assert_eq!(
        error.downcast_ref::<LeaseFenceLost>(),
        Some(&LeaseFenceLost {
            home,
            epoch,
            found: Some((NEW_OWNER.to_owned(), taken)),
        })
    );
    assert!(matches!(
        fixture.repo().load_run(successor).await?,
        LoadedRun::Absent
    ));
    assert!(
        fixture
            .repo()
            .read_history(successor, 0, 10_000)
            .await?
            .is_empty(),
        "a copy page wrote after the takeover"
    );
    let records = fixture
        .repo()
        .list_run_bulk_writes(home, None, 1_000)
        .await?;
    assert!(
        records
            .iter()
            .any(|record| record.run_key == successor
                && record.phase == BulkWritePhase::Materializing),
        "{records:?}"
    );
    fixture.repo().purge_run(successor).await?;
    fixture.clear_lease_at(home).await?;
    Ok(())
}

/// A takeover's recovery switch, held after its fence read while yet another
/// node takes the home over, switches nothing: the record still says the run
/// is being materialized.
#[tokio::test]
async fn dsql_lease_fence_recovery_switch_under_a_lost_lease_switches_nothing() -> anyhow::Result<()>
{
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let epoch = fixture.acquire_old().await?;
    let successor = RunKey::new();
    fixture
        .repo()
        .record_materialization(successor, HOME, &WriteFence::Unfenced)
        .await?;

    let (reached, resume) = pause_point::arm(PauseKey::Write(successor));
    let (switched, taken_over) = tokio::join!(
        fixture
            .repo()
            .abandon_materialization(successor, old_fence(epoch)),
        fixture.take_over_while_paused(reached, resume),
    );
    let taken = taken_over?;
    assert_eq!(
        switched
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<LeaseFenceLost>()),
        Some(&LeaseFenceLost {
            home: HOME,
            epoch,
            found: Some((NEW_OWNER.to_owned(), taken)),
        }),
        "{switched:?}"
    );
    let records = fixture
        .repo()
        .list_run_bulk_writes(HOME, None, 1_000)
        .await?;
    assert!(
        records
            .iter()
            .any(|record| record.run_key == successor
                && record.phase == BulkWritePhase::Materializing),
        "{records:?}"
    );
    fixture.repo().purge_run(successor).await?;
    fixture.clear_lease().await?;
    Ok(())
}

/// What happens to the home's lease while the owner's writes wait after their
/// fence reads.
#[derive(Clone, Copy, Debug)]
enum LeaseEvent {
    Nothing,
    /// The owner renews.
    Renew,
    /// The owner acquires its unexpired lease again.
    Refresh,
    /// Another node takes the lapsed lease over.
    Takeover,
    /// The owner takes its own lapsed lease over, at a new epoch.
    Reacquire,
    /// The owner releases the lease.
    Release,
    /// Another node's takeover, held before its commit while the writes commit.
    TakeoverLandingLast,
}

impl LeaseEvent {
    /// Whether ownership changes before the writes commit.
    fn changes_ownership_first(self) -> bool {
        matches!(self, Self::Takeover | Self::Reacquire | Self::Release)
    }
}

fn lease_event() -> impl Strategy<Value = LeaseEvent> {
    prop_oneof![
        Just(LeaseEvent::Nothing),
        Just(LeaseEvent::Renew),
        Just(LeaseEvent::Refresh),
        Just(LeaseEvent::Takeover),
        Just(LeaseEvent::Reacquire),
        Just(LeaseEvent::Release),
        Just(LeaseEvent::TakeoverLandingLast),
    ]
}

/// Hold `writers` fenced writes of the owner, each of its own run, after their
/// fence reads; let `event` happen; then let the writes commit one at a time in
/// `order`. A write commits exactly when the lease still names its owner at its
/// epoch at its commit, and the owner's writes, renewals and refreshes never
/// abort one another. A takeover that would commit after the writes is aborted.
async fn interleaving(fixture: &Fixture, writers: usize, event: LeaseEvent, order: &[usize]) {
    fixture
        .clear_lease()
        .await
        .expect("the interleaving's step succeeds");
    let epoch = fixture
        .acquire_old()
        .await
        .expect("the interleaving's step succeeds");
    let mut states = Vec::new();
    for _ in 0..writers {
        states.push(
            started(fixture, epoch)
                .await
                .expect("the interleaving's step succeeds"),
        );
    }
    let mut reached = Vec::new();
    let mut resumes = Vec::new();
    let mut done_senders = Vec::new();
    let mut done = Vec::new();
    for state in &states {
        let (at_pause, resume) = pause_point::arm(PauseKey::Write(state.run_key));
        reached.push(at_pause);
        resumes.push(Some(resume));
        let (sender, receiver) = oneshot::channel::<()>();
        done_senders.push(Some(sender));
        done.push(Some(receiver));
    }
    let mut done_senders = done_senders.into_iter();
    let mut write = |index: usize| {
        let state = states.get(index).cloned();
        let finished = done_senders.next().flatten();
        async move {
            let state = state?;
            let result = fixture
                .repo()
                .commit_transition_for_bundle(
                    state.run_key,
                    HOME,
                    following(&state),
                    old_fence(epoch),
                )
                .await;
            if let Some(finished) = finished {
                let _ = finished.send(());
            }
            Some(result)
        }
    };
    let (writes0, writes1, writes2) = (write(0), write(1), write(2));
    let (go, went) = oneshot::channel::<()>();
    let takeover = async {
        if !matches!(event, LeaseEvent::TakeoverLandingLast) {
            return None;
        }
        went.await.ok()?;
        Some(fixture.take_over().await)
    };
    let driver = async {
        for at_pause in reached {
            at_pause.await.expect("every write reaches its pause point");
        }
        let mut held_takeover = None;
        match event {
            LeaseEvent::Nothing => {}
            LeaseEvent::Renew => {
                let renewed = fixture
                    .repo()
                    .renew_bundle(HOME, OLD_OWNER.to_owned(), epoch, ENDPOINT.to_owned())
                    .await
                    .expect("the interleaving's step succeeds");
                assert_eq!(renewed, LeaseOutcome::Renewed { epoch });
            }
            LeaseEvent::Refresh => {
                assert_eq!(
                    fixture
                        .acquire_old()
                        .await
                        .expect("the interleaving's step succeeds"),
                    epoch
                );
            }
            LeaseEvent::Takeover => {
                fixture
                    .take_over()
                    .await
                    .expect("the interleaving's step succeeds");
            }
            LeaseEvent::Reacquire => {
                fixture
                    .take_over_at(HOME, OLD_OWNER)
                    .await
                    .expect("the interleaving's step succeeds");
            }
            LeaseEvent::Release => {
                let released = fixture
                    .repo()
                    .relinquish_bundle(HOME, OLD_OWNER.to_owned(), epoch)
                    .await
                    .expect("the interleaving's step succeeds");
                assert!(
                    matches!(released, LeaseOutcome::Acquired { .. }),
                    "{released:?}"
                );
            }
            LeaseEvent::TakeoverLandingLast => {
                let (takeover_reached, takeover_resume) =
                    pause_point::arm(PauseKey::Acquisition(HOME));
                go.send(()).expect("the interleaving's step succeeds");
                takeover_reached
                    .await
                    .expect("the takeover reaches its pause point");
                held_takeover = Some(takeover_resume);
            }
        }
        for &index in order.iter().filter(|index| **index < writers) {
            let _ = resumes[index]
                .take()
                .expect("the interleaving's step succeeds")
                .send(());
            done[index]
                .take()
                .expect("the interleaving's step succeeds")
                .await
                .expect("the interleaving's step succeeds");
        }
        if let Some(resume) = held_takeover {
            let _ = resume.send(());
        }
    };
    let (result0, result1, result2, taken, ()) =
        tokio::join!(writes0, writes1, writes2, takeover, driver);
    let results = [result0, result1, result2]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(results.len(), writers);
    if event.changes_ownership_first() {
        for (state, result) in states.iter().zip(&results) {
            assert!(
                matches!(result, Ok(CommitResult::Conflict { .. })),
                "{event:?}: a write committed after a change of ownership: {result:?}"
            );
            let retried = fixture
                .repo()
                .commit_transition_for_bundle(
                    state.run_key,
                    HOME,
                    following(state),
                    old_fence(epoch),
                )
                .await;
            assert!(is_lease_lost(&retried), "{event:?}: {retried:?}");
        }
    } else {
        for result in &results {
            assert!(
                matches!(result, Ok(CommitResult::Applied { .. })),
                "{event:?}: the owner's write was refused: {result:?}"
            );
        }
        if matches!(event, LeaseEvent::TakeoverLandingLast) {
            let taken = taken.expect("the takeover ran");
            assert!(
                taken.is_err(),
                "a takeover that lands last committed: {taken:?}"
            );
        }
        assert_eq!(
            fixture
                .lease_at(HOME)
                .await
                .expect("the interleaving's step succeeds"),
            Some((Some(OLD_OWNER.to_owned()), epoch)),
            "{event:?}"
        );
    }
    fixture
        .clear_lease()
        .await
        .expect("the interleaving's step succeeds");
}

// Feature: bundle-lease-fence, Property 1: No stale owner commits
// Feature: bundle-lease-fence, Property 2: The owner's writes never abort each other
#[test]
fn dsql_lease_fence_generated_interleavings() {
    let strategy = (
        1..=3usize,
        lease_event(),
        Just(vec![0usize, 1, 2]).prop_shuffle(),
    );
    live_property(
        strategy,
        100,
        |runtime, fixture, (writers, event, order)| {
            runtime.block_on(interleaving(fixture, writers, event, &order));
            Ok(())
        },
    );
}

/// The owner's renewals and refreshes beside its writes in flight abort none of
/// them (`bundle-lease-fence` criterion 3.2).
#[tokio::test]
async fn dsql_lease_fence_renewals_and_refreshes_beside_writes() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    interleaving(&fixture, 3, LeaseEvent::Renew, &[2, 0, 1]).await;
    interleaving(&fixture, 3, LeaseEvent::Refresh, &[1, 2, 0]).await;
    Ok(())
}

/// A fenced write that commits before a takeover aborts the takeover.
#[tokio::test]
async fn dsql_lease_fence_a_write_landing_first_aborts_a_takeover() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    interleaving(&fixture, 1, LeaseEvent::TakeoverLandingLast, &[0, 1, 2]).await;
    Ok(())
}
