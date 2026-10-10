//! Shared contracts for the bundle lease fence (`bundle-lease-fence`), run
//! against the in-memory store in the default suite and against an ephemeral
//! Aurora DSQL cluster in the live suite.

use anyhow::Result;
use async_trait::async_trait;
use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_kernel::{LoadedRun, Transition, WorkflowState};
use tokeira_types::{ExecutionStatus, RunKey, ShardEpoch, ShardId, WorkflowId};

use crate::{
    CommitResult, DeleteRunRequest, ExecutionHomeFence, LeaseFenceLost, LeaseOutcome,
    LeaseRepository, RunBulkWrite, RunRepository, WriteFence,
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
};

/// The owners the contracts' leases name.
pub(crate) const OWNERS: [&str; 3] = ["lease-owner-a", "lease-owner-b", "lease-owner-c"];
const ENDPOINT: &str = "127.0.0.1:7233";

/// A store whose execution-home leases the contracts drive.
#[async_trait]
pub(crate) trait LeaseBackend: Sync {
    fn repo(&self) -> &dyn RunRepository;
    fn leases(&self) -> &dyn LeaseRepository;
    fn shard_count(&self) -> u32;
    /// The execution home the contracts' leases and runs use.
    fn home(&self) -> ShardId;
    /// The home's lease: its owner, if any, and its epoch.
    async fn lease(&self) -> Result<Option<(Option<String>, ShardEpoch)>>;
    /// Make the home's lease expire now, as if its owner had stopped renewing.
    async fn expire_lease(&self) -> Result<()>;
    /// Remove the home's lease, so the next acquisition inserts it.
    async fn clear_lease(&self) -> Result<()>;
}

/// A lease fence for `owner` at `epoch` on `home`.
pub(crate) fn fence(home: ShardId, owner: &str, epoch: ShardEpoch) -> WriteFence {
    WriteFence::Lease(ExecutionHomeFence {
        home,
        owner: owner.to_owned(),
        epoch,
    })
}

/// Acquire the home's lease for `owner`, and return its epoch.
pub(crate) async fn acquire(backend: &impl LeaseBackend, owner: &str) -> ShardEpoch {
    match backend
        .leases()
        .try_acquire_bundle(backend.home(), owner.to_owned(), ENDPOINT.to_owned())
        .await
        .expect("an acquisition answers")
    {
        LeaseOutcome::Acquired { epoch } => epoch,
        other => panic!("{owner}'s acquisition answered {other:?}"),
    }
}

/// A run's start whose execution home is the backend's home.
pub(crate) fn start_at_home(backend: &impl LeaseBackend) -> Transition {
    let mut transition = fresh_transition(RunKey::new());
    let namespace_id = transition.next_state.namespace_id;
    let run_key = transition.next_state.run_key;
    transition.next_state.workflow_id = (0..)
        .map(|index| WorkflowId(format!("lease-fence-{}-{index}", run_key.0)))
        .find(|workflow_id| {
            tokeira_types::execution_home_bundle(
                namespace_id.0.as_bytes(),
                workflow_id.0.as_bytes(),
                backend.shard_count().max(1),
            ) == backend.home()
        })
        .expect("some workflow id lands on the home");
    transition
}

// Property 5: only a change of ownership writes the epoch.

/// One step of a lease's life.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LeaseOp {
    /// An acquisition by `OWNERS[i]`: an insert, a refresh, a takeover or a
    /// refusal, by the lease's state.
    Acquire(usize),
    /// A renewal by `OWNERS[i]`, at the lease's epoch or at another.
    Renew(usize, bool),
    /// A release by `OWNERS[i]`, at the lease's epoch or at another.
    Release(usize, bool),
    /// The lease expires, as if its owner had stopped renewing it.
    Expire,
}

pub(crate) fn lease_op() -> impl Strategy<Value = LeaseOp> {
    prop_oneof![
        3 => (0..OWNERS.len()).prop_map(LeaseOp::Acquire),
        2 => (0..OWNERS.len(), any::<bool>()).prop_map(|(owner, at)| LeaseOp::Renew(owner, at)),
        1 => (0..OWNERS.len(), any::<bool>()).prop_map(|(owner, at)| LeaseOp::Release(owner, at)),
        1 => Just(LeaseOp::Expire),
    ]
}

/// The reference model of a lease: its owner and epoch, if it exists, and
/// whether it has lapsed.
#[derive(Debug, Default)]
struct LeaseModel {
    row: Option<(Option<usize>, u64)>,
    lapsed: bool,
}

impl LeaseModel {
    /// The epoch a renewal or release by `at` names.
    fn named_epoch(&self, at: bool) -> u64 {
        let epoch = self.row.map_or(0, |(_, epoch)| epoch);
        if at { epoch } else { epoch + 7 }
    }

    /// Apply `op`, and return whether ownership changed.
    fn apply(&mut self, op: LeaseOp) -> bool {
        match op {
            LeaseOp::Acquire(owner) => match &mut self.row {
                None => {
                    self.row = Some((Some(owner), 1));
                    self.lapsed = false;
                    true
                }
                Some((current, _)) if *current == Some(owner) && !self.lapsed => false,
                Some((current, epoch)) if current.is_none() || self.lapsed => {
                    *current = Some(owner);
                    *epoch += 1;
                    self.lapsed = false;
                    true
                }
                Some(_) => false,
            },
            LeaseOp::Renew(owner, at) => {
                let named = self.named_epoch(at);
                if self.row == Some((Some(owner), named)) {
                    self.lapsed = false;
                }
                false
            }
            LeaseOp::Release(owner, at) => {
                let named = self.named_epoch(at);
                match &mut self.row {
                    Some((current, epoch)) if *current == Some(owner) && *epoch == named => {
                        *current = None;
                        *epoch += 1;
                        self.lapsed = false;
                        true
                    }
                    _ => false,
                }
            }
            LeaseOp::Expire => {
                self.lapsed = self.row.is_some();
                false
            }
        }
    }

    fn expected(&self) -> Option<(Option<String>, ShardEpoch)> {
        self.row.map(|(owner, epoch)| {
            (
                owner.map(|owner| OWNERS[owner].to_owned()),
                ShardEpoch(epoch),
            )
        })
    }
}

/// Run `ops` against the home's lease. After every step the lease holds what
/// the model does, and its epoch has changed exactly when ownership changed.
// Feature: bundle-lease-fence, Property 5: Only a change of ownership writes the epoch
pub(crate) async fn only_a_change_of_ownership_writes_the_epoch(
    backend: &impl LeaseBackend,
    ops: &[LeaseOp],
) {
    backend.clear_lease().await.unwrap();
    let mut model = LeaseModel::default();
    let mut before = backend.lease().await.unwrap();
    assert_eq!(before, None);
    for (step, op) in ops.iter().copied().enumerate() {
        let leases = backend.leases();
        let home = backend.home();
        match op {
            LeaseOp::Acquire(owner) => {
                leases
                    .try_acquire_bundle(home, OWNERS[owner].to_owned(), ENDPOINT.to_owned())
                    .await
                    .unwrap();
            }
            LeaseOp::Renew(owner, at) => {
                leases
                    .renew_bundle(
                        home,
                        OWNERS[owner].to_owned(),
                        ShardEpoch(model.named_epoch(at)),
                        ENDPOINT.to_owned(),
                    )
                    .await
                    .unwrap();
            }
            LeaseOp::Release(owner, at) => {
                leases
                    .relinquish_bundle(
                        home,
                        OWNERS[owner].to_owned(),
                        ShardEpoch(model.named_epoch(at)),
                    )
                    .await
                    .unwrap();
            }
            LeaseOp::Expire => backend.expire_lease().await.unwrap(),
        }
        let changed = model.apply(op);
        let after = backend.lease().await.unwrap();
        assert_eq!(after, model.expected(), "step {step}: {op:?} after {ops:?}");
        let epoch = |lease: &Option<(Option<String>, ShardEpoch)>| lease.as_ref().map(|row| row.1);
        assert_eq!(
            epoch(&before) != epoch(&after),
            changed,
            "step {step}: {op:?} changed the epoch {} a change of ownership",
            if changed { "without" } else { "with" },
        );
        before = after;
    }
    backend.clear_lease().await.unwrap();
}

// Property 4, storage: a lost fence changes nothing.

/// How a fence is lost.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Loss {
    /// The home has no lease.
    NoLease,
    /// Another owner took the lapsed lease over.
    OtherOwner,
    /// The same owner took its own lapsed lease over, at a new epoch.
    Reacquired,
    /// The owner released the lease.
    Released,
}

/// A write of a run's authoritative state that carries a fence.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FencedWrite {
    Commit,
    CommitForBundle,
    Delete,
    Abandon,
}

/// Lose `OWNERS[0]`'s lease at `epoch` as `loss` says, and return what the
/// lease then holds: its owner (empty once released) and epoch, if any.
pub(crate) async fn lose(
    backend: &impl LeaseBackend,
    loss: Loss,
    epoch: ShardEpoch,
) -> Option<(String, ShardEpoch)> {
    match loss {
        Loss::NoLease => {
            backend.clear_lease().await.unwrap();
            None
        }
        Loss::OtherOwner => {
            backend.expire_lease().await.unwrap();
            Some((OWNERS[1].to_owned(), acquire(backend, OWNERS[1]).await))
        }
        Loss::Reacquired => {
            backend.expire_lease().await.unwrap();
            Some((OWNERS[0].to_owned(), acquire(backend, OWNERS[0]).await))
        }
        Loss::Released => {
            let outcome = backend
                .leases()
                .relinquish_bundle(backend.home(), OWNERS[0].to_owned(), epoch)
                .await
                .unwrap();
            let LeaseOutcome::Acquired { epoch } = outcome else {
                panic!("the release answered {outcome:?}");
            };
            Some((String::new(), epoch))
        }
    }
}

/// Every way of losing a fence.
pub(crate) const LOSSES: [Loss; 4] = [
    Loss::NoLease,
    Loss::OtherOwner,
    Loss::Reacquired,
    Loss::Released,
];

/// Every fenced write.
pub(crate) const FENCED_WRITES: [FencedWrite; 4] = [
    FencedWrite::Commit,
    FencedWrite::CommitForBundle,
    FencedWrite::Delete,
    FencedWrite::Abandon,
];

/// What a fenced write of a run could change.
#[derive(Debug, PartialEq)]
struct Observed {
    run: Option<WorkflowState>,
    history: usize,
    lease: Option<(Option<String>, ShardEpoch)>,
    records: Vec<RunBulkWrite>,
}

async fn observe(backend: &impl LeaseBackend, run_key: RunKey) -> Observed {
    let repo = backend.repo();
    Observed {
        run: match repo.load_run(run_key).await.unwrap() {
            LoadedRun::Existing(state) => Some(state),
            LoadedRun::Absent => None,
        },
        history: repo.read_history(run_key, 0, 10_000).await.unwrap().len(),
        lease: backend.lease().await.unwrap(),
        records: repo
            .list_run_bulk_writes(backend.home(), None, 1_000)
            .await
            .unwrap(),
    }
}

/// A fenced write whose fence `loss` has lost changes nothing, and fails with
/// the lease's loss, naming what the lease holds instead.
// Feature: bundle-lease-fence, Property 4: A lost fence is final
pub(crate) async fn a_lost_fence_changes_nothing(
    backend: &impl LeaseBackend,
    loss: Loss,
    write: FencedWrite,
) {
    let (repo, home) = (backend.repo(), backend.home());
    backend.clear_lease().await.unwrap();
    let epoch = acquire(backend, OWNERS[0]).await;
    let held = fence(home, OWNERS[0], epoch);
    let start = start_at_home(backend);
    let run_key = start.next_state.run_key;
    let mut state = applied(
        repo.commit_transition_for_bundle(run_key, home, start, held.clone())
            .await
            .unwrap(),
    );
    if matches!(write, FencedWrite::Delete) {
        let mut close = following(&state);
        close.next_state.status = ExecutionStatus::Completed;
        close.next_state.closed_at = Some(close.next_state.started_at);
        close.next_state.pending_workflow_task = None;
        state = applied(
            repo.commit_transition_for_bundle(run_key, home, close, held.clone())
                .await
                .unwrap(),
        );
    }

    let found = lose(backend, loss, epoch).await;

    let before = observe(backend, run_key).await;
    let result = match write {
        FencedWrite::Commit => repo
            .commit_transition(run_key, following(&state), held.clone())
            .await
            .map(drop),
        FencedWrite::CommitForBundle => repo
            .commit_transition_for_bundle(run_key, home, following(&state), held.clone())
            .await
            .map(drop),
        FencedWrite::Delete => repo
            .delete_run_for_bundle(
                run_key,
                home,
                DeleteRunRequest {
                    expected_seq: state.transition_seq,
                    deleted_at: OffsetDateTime::now_utc(),
                },
                held.clone(),
            )
            .await
            .map(drop),
        FencedWrite::Abandon => repo.abandon_materialization(run_key, held.clone()).await,
    };
    let error = result.expect_err("a write under a lost fence must fail");
    let lost = error
        .downcast_ref::<LeaseFenceLost>()
        .unwrap_or_else(|| panic!("{write:?} under {loss:?} failed otherwise: {error:?}"));
    assert_eq!(lost, &LeaseFenceLost { home, epoch, found });
    assert_eq!(
        observe(backend, run_key).await,
        before,
        "{write:?} under {loss:?}"
    );
    backend.clear_lease().await.unwrap();
}

/// Writes under the lease that admitted them commit, and so does the next
/// write once the owner has renewed or refreshed its lease: neither writes the
/// epoch (`bundle-lease-fence` criterion 3.2).
pub(crate) async fn the_owner_keeps_writing_across_renewals_and_refreshes(
    backend: &impl LeaseBackend,
) {
    let (repo, home) = (backend.repo(), backend.home());
    backend.clear_lease().await.unwrap();
    let epoch = acquire(backend, OWNERS[0]).await;
    let held = fence(home, OWNERS[0], epoch);
    let start = start_at_home(backend);
    let run_key = start.next_state.run_key;
    let mut state = applied(
        repo.commit_transition_for_bundle(run_key, home, start, held.clone())
            .await
            .unwrap(),
    );
    for round in 0..3 {
        let renewed = backend
            .leases()
            .renew_bundle(home, OWNERS[0].to_owned(), epoch, ENDPOINT.to_owned())
            .await
            .unwrap();
        assert_eq!(renewed, LeaseOutcome::Renewed { epoch }, "round {round}");
        state = applied(
            repo.commit_transition(run_key, following(&state), held.clone())
                .await
                .unwrap(),
        );
        assert_eq!(
            acquire(backend, OWNERS[0]).await,
            epoch,
            "a refresh keeps the epoch"
        );
        let committed = repo
            .commit_transition_for_bundle(run_key, home, following(&state), held.clone())
            .await
            .unwrap();
        assert!(
            matches!(committed, CommitResult::Applied { .. }),
            "{committed:?}"
        );
        state = applied(committed);
    }
    backend.clear_lease().await.unwrap();
}
