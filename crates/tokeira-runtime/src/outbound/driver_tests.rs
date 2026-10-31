//! Discovery properties drive the actual bounded worker against committed rows.

use super::*;
use proptest::prelude::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokeira_kernel::{
    BasicKernel, Command, Kernel, LoadedRun, PendingExternalCancel, PendingExternalSignal,
    Transition,
};
use tokeira_storage::{CommitResult, InMemoryStore};
use tokeira_types::{RunKey, ShardEpoch, WorkflowId};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}

fn owner() -> Arc<RwLock<ShardOwner>> {
    let mut owner = ShardOwner::new(1);
    owner.record_acquired(ShardId(0), ShardEpoch(1));
    owner.mark_active(ShardId(0));
    Arc::new(RwLock::new(owner))
}

fn insert(state: &mut tokeira_kernel::WorkflowState, id: i64) {
    let target = WorkflowId(format!("target-{id}"));
    state.pending_external_signals.insert(
        id,
        PendingExternalSignal {
            initiated_event_id: id,
            target_namespace_id: state.namespace_id,
            target_namespace: None,
            target_workflow_id: target.clone(),
            target_run_id: None,
            signal_name: "test".into(),
        },
    );
    state.pending_external_cancels.insert(
        id,
        PendingExternalCancel {
            initiated_event_id: id,
            target_namespace_id: state.namespace_id,
            target_namespace: None,
            target_workflow_id: target,
            target_run_id: None,
        },
    );
}

async fn fixture(count: i64) -> (Arc<InMemoryStore>, tokeira_kernel::WorkflowState) {
    let repo = Arc::new(InMemoryStore::default());
    let mut start = crate::runtime::tests::sample_start_request(None, None);
    start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
    let mut transition = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(start))
        .unwrap();
    for event in 100..100 + count {
        insert(&mut transition.next_state, event);
    }
    let CommitResult::Applied { new_state } = repo
        .commit_transition(
            transition.next_state.run_key,
            transition,
            WriteFence::Unfenced,
        )
        .await
        .unwrap()
    else {
        panic!("source commit");
    };
    (repo, new_state)
}

struct RecordingExecutor {
    seen: Mutex<BTreeSet<(OperationKind, i64)>>,
    active: AtomicUsize,
    homes: Mutex<BTreeSet<ShardId>>,
    peak: AtomicUsize,
    hold: bool,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl RecordingExecutor {
    fn new(hold: bool) -> Self {
        Self {
            seen: Mutex::default(),
            active: AtomicUsize::new(0),
            homes: Mutex::default(),
            peak: AtomicUsize::new(0),
            hold,
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Executor for RecordingExecutor {
    fn operation_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::ExternalSignal, OperationKind::ExternalCancel]
    }
    fn accepts_intent(&self, _: IntentKind) -> bool {
        false
    }
    async fn deliver(&self, work: Work, _: Attempt) -> Result<()> {
        let Work::Operation(row) = work else {
            panic!("fixture has operations only");
        };
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = Active(&self.active);
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.homes.lock().unwrap().insert(row.source_home);
        if active.is_multiple_of(MAX_PER_HOME) {
            self.reached.notify_one();
        }
        if self.hold {
            self.release.notified().await;
        }
        self.seen.lock().unwrap().insert((
            row.incarnation.key.kind,
            row.incarnation.key.initiating_event,
        ));
        // The first row keeps failing; cursor advancement must not depend on its
        // success or on retiring anything from the authoritative pending map.
        if row.incarnation.key.initiating_event == 100 {
            anyhow::bail!("injected first-row transport failure");
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 3: Bounded discovery makes progress after volatile loss
    #[test]
    fn finite_rounds_find_lower_insertions_and_pass_failing_first_rows(count in 33i64..70, lower in 1i64..12) {
        runtime().block_on(async {
            let (repo, mut state) = fixture(count).await;
            let mut cursor = Cursor::new(StreamKind::Operation(OperationKind::ExternalSignal));
            let stream = (ShardId(0), StreamKind::Operation(OperationKind::ExternalSignal));
            let first = cursor.page(repo.as_ref(), stream, 7).await.unwrap();
            prop_assert_eq!(first.len(), 7);
            let expected = state.transition_seq;
            state.transition_seq = expected.next();
            for id in 1..=lower { insert(&mut state, id); }
            let transition = Transition { expected_seq: expected, next_state: state.clone(), history_events: Default::default(), event_principals: Default::default(), request_dedupe_ops: Default::default(), activity_ops: Default::default(), timer_ops: Default::default(), dispatch_ops: Default::default(), events_numbered_at_close: 0, growth_limits: None };
            repo.commit_transition(state.run_key, transition, WriteFence::Unfenced).await.unwrap();
            let mut remaining = Vec::new();
            loop {
                let rows = cursor.page(repo.as_ref(), stream, 7).await.unwrap();
                if rows.is_empty() { break; }
                remaining.extend(rows);
            }
            prop_assert_eq!(remaining.len(), usize::try_from(count).unwrap() - 7);
            let next = cursor.page(repo.as_ref(), stream, 100).await.unwrap();
            prop_assert!(matches!(next.first(), Some(Work::Operation(row)) if row.incarnation.key.initiating_event == 1));

            let executor = Arc::new(RecordingExecutor::new(false));
            let cancel = CancellationToken::new();
            let worker = tokio::spawn(run(repo, executor.clone(), owner(), cancel.clone()));
            for _ in 0..40 {
                tokio::time::advance(Duration::from_millis(200)).await;
                for _ in 0..16 { tokio::task::yield_now().await; }
            }
            cancel.cancel(); worker.await.unwrap();
            prop_assert_eq!(executor.seen.lock().unwrap().len(), usize::try_from(2 * (count + lower)).unwrap());
            prop_assert!(executor.peak.load(Ordering::SeqCst) <= MAX_IN_FLIGHT);
            prop_assert_eq!(executor.active.load(Ordering::SeqCst), 0);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 4: Ownership changes cannot authorize stale commits
    #[test]
    fn attempt_cannot_borrow_a_replacement_acquisition(same_epoch in any::<bool>(), epoch in 1u64..100) {
        let owner = owner();
        let original = Attempt::capture(&owner, ShardId(0)).unwrap();
        owner.write().unwrap().record_acquired(ShardId(0), if same_epoch { original.acquisition.epoch } else { ShardEpoch(epoch + 1) });
        owner.write().unwrap().mark_active(ShardId(0));
        prop_assert!(original.validate(&owner).is_err());
        prop_assert!(original.acquisition.cancel.is_cancelled());
        prop_assert!(Attempt::capture(&owner, ShardId(0)).unwrap().validate(&owner).is_ok());
    }
}

#[tokio::test(start_paused = true)]
async fn admission_is_bounded_and_shutdown_drops_blocked_attempts() {
    let (repo, _) = fixture(100).await;
    let executor = Arc::new(RecordingExecutor::new(true));
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run(repo, executor.clone(), owner(), cancel.clone()));
    tokio::time::timeout(Duration::from_secs(10), executor.reached.notified())
        .await
        .unwrap();
    assert_eq!(executor.active.load(Ordering::SeqCst), MAX_PER_HOME);
    cancel.cancel();
    worker.await.unwrap();
    assert_eq!(executor.peak.load(Ordering::SeqCst), MAX_PER_HOME);
    assert_eq!(executor.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn blocked_home_cannot_take_another_homes_admission() {
    let repo = Arc::new(InMemoryStore::with_shard_count(2));
    let mut ownership = ShardOwner::new(2);
    for home in [ShardId(0), ShardId(1)] {
        let mut start = crate::runtime::tests::sample_start_request(None, None);
        start.workflow_id = (0..10_000)
            .map(|n| WorkflowId(format!("home-{n}")))
            .find(|id| {
                tokeira_types::execution_home_bundle(
                    start.namespace_id.0.as_bytes(),
                    id.0.as_bytes(),
                    2,
                ) == home
            })
            .unwrap();
        start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
        let mut transition = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start))
            .unwrap();
        for event in 100..200 {
            insert(&mut transition.next_state, event);
        }
        repo.commit_transition(
            transition.next_state.run_key,
            transition,
            WriteFence::Unfenced,
        )
        .await
        .unwrap();
        ownership.record_acquired(home, ShardEpoch(1));
        ownership.mark_active(home);
    }
    let executor = Arc::new(RecordingExecutor::new(true));
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run(
        repo,
        executor.clone(),
        Arc::new(RwLock::new(ownership)),
        cancel.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while executor.active.load(Ordering::SeqCst) < 2 * MAX_PER_HOME {
            executor.reached.notified().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *executor.homes.lock().unwrap(),
        BTreeSet::from([ShardId(0), ShardId(1)])
    );
    assert_eq!(executor.active.load(Ordering::SeqCst), 2 * MAX_PER_HOME);
    cancel.cancel();
    worker.await.unwrap();
    assert_eq!(executor.peak.load(Ordering::SeqCst), 2 * MAX_PER_HOME);
}
