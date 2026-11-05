//! Discovery properties drive the actual bounded worker against committed rows.

use super::*;
use crate::test_clock::TestClock;
use proptest::prelude::*;
use std::{
    future::{Future, poll_fn},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
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

async fn fixture(
    count: i64,
    clock: Arc<dyn Clock>,
) -> (Arc<InMemoryStore>, tokeira_kernel::WorkflowState) {
    let repo = Arc::new(InMemoryStore::default().with_clock(clock.clone()));
    let mut start = crate::runtime::tests::sample_start_request(None, None);
    start.now = clock.now();
    start.request.received_at = start.now;
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

    // Ordered home visits bound waiting and give every kind a turn at first admission.
    // Feature: outbound-dispatch-recovery, Property 23: Discovery rotates each home's starting kind.
    #[test]
    fn discovery_rotates_each_homes_starting_kind(
        homes in 1usize..=64,
        kinds in 2usize..=8,
        rounds in prop::collection::vec(1usize..=16, 1..100),
    ) {
        let all_kinds = [
            OperationKind::ChildStart,
            OperationKind::ChildRelease,
            OperationKind::ExternalSignal,
            OperationKind::ExternalCancel,
            OperationKind::NexusStart,
            OperationKind::NexusCancel,
        ];
        let operation_kinds = &all_kinds[..kinds - 2];
        let mut ownership = ShardOwner::new(u32::try_from(homes).unwrap());
        for home in (0..homes).map(|home| ShardId(u32::try_from(home).unwrap())) {
            ownership.record_acquired(home, ShardEpoch(1));
            ownership.mark_active(home);
        }
        let owner = Arc::new(RwLock::new(ownership));
        let ordered: Vec<_> = (0..homes)
            .flat_map(|home| operation_kinds.iter().copied()
                .map(StreamKind::Operation)
                .chain([StreamKind::Intent, StreamKind::Publication])
                .map(move |kind| (ShardId(u32::try_from(home).unwrap()), kind)))
            .collect();
        let count = ordered.len();
        let mut discovery = Discovery::default();
        discovery.refresh(&owner, operation_kinds);
        let mut last_read = vec![None; count];
        let mut leaders = vec![Vec::new(); homes];
        let mut reads = 0;
        for budget in rounds.iter().cycle() {
            discovery.refresh(&owner, operation_kinds);
            for _ in 0..(*budget).min(count) {
                let stream = discovery.next().unwrap();
                let index = ordered.binary_search(&stream).unwrap();
                let home = index / kinds;
                let kind = index % kinds;
                let cycle = reads / count;
                let position = reads % kinds;
                prop_assert_eq!(home, (reads % count) / kinds, "homes stay in key order");
                if position == 0 {
                    let visits = &mut leaders[home];
                    if let Some(&previous) = visits.last() {
                        prop_assert_eq!(kind, (previous + 1) % kinds, "offset advances once per home visit");
                    }
                    visits.push(kind);
                    if visits.len() >= kinds {
                        let firsts: BTreeSet<_> = visits[visits.len() - kinds..].iter().collect();
                        prop_assert_eq!(firsts.len(), kinds, "each kind reads first in every K visits");
                    }
                }
                prop_assert_eq!(kind, (cycle + position) % kinds, "kinds wrap in key order from the offset");
                if let Some(last) = last_read[index] {
                    prop_assert!(reads - last - 1 <= count + kinds - 2, "stream exceeded N + K - 2 intervening reads");
                }
                last_read[index] = Some(reads);
                for last in last_read.iter().flatten() {
                    prop_assert!(reads - last <= count + kinds - 2, "waiting stream exceeded N + K - 2 reads");
                }
                reads += 1;
            }
            if reads >= count * (2 * kinds + 1) { break; }
        }
        prop_assert!(last_read.iter().all(Option::is_some));
        prop_assert!(leaders.iter().all(|visits| visits.len() >= 2 * kinds));
    }

    // Feature: outbound-dispatch-recovery, Property 3: Bounded discovery makes progress after volatile loss
    #[test]
    fn finite_rounds_find_lower_insertions_and_pass_failing_first_rows(count in 33i64..70, lower in 1i64..12) {
        runtime().block_on(async {
            let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
            let (repo, mut state) = fixture(count, clock.clone()).await;
            let mut cursor = Cursor::new(StreamKind::Operation(OperationKind::ExternalSignal));
            let stream = (ShardId(0), StreamKind::Operation(OperationKind::ExternalSignal));
            let first = cursor.page(repo.as_ref(), stream, 7).await.unwrap();
            prop_assert_eq!(first.len(), 7);
            let expected = state.transition_seq;
            state.transition_seq = expected.next();
            for id in 1..=lower { insert(&mut state, id); }
            let transition = Transition { expected_seq: expected, next_state: state.clone(), history_events: Default::default(), event_principals: Default::default(), request_dedupe_ops: Default::default(), activity_ops: Default::default(), timer_ops: Default::default(), dispatch_ops: Default::default(), events_numbered_at_close: 0, workflow_task_heartbeat_timed_out: false, growth_limits: None };
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
            let worker = tokio::spawn(run(repo, executor.clone(), owner(), cancel.clone(), clock.clone()));
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

#[test]
fn refresh_preserves_partial_visits_and_forgets_inactive_offsets() {
    let owner = Arc::new(RwLock::new(ShardOwner::new(5)));
    for home in [ShardId(0), ShardId(2), ShardId(4)] {
        owner.write().unwrap().record_acquired(home, ShardEpoch(1));
        owner.write().unwrap().mark_active(home);
    }
    let mut discovery = Discovery::default();
    discovery.refresh(&owner, &[]);
    assert_eq!(discovery.next(), Some((ShardId(0), StreamKind::Intent)));
    discovery.refresh(&owner, &[]);
    assert_eq!(
        discovery.next(),
        Some((ShardId(0), StreamKind::Publication))
    );
    assert_eq!(discovery.next(), Some((ShardId(2), StreamKind::Intent)));
    {
        let mut owner = owner.write().unwrap();
        owner.mark_draining(ShardId(2));
        owner.record_acquired(ShardId(3), ShardEpoch(1));
        owner.mark_active(ShardId(3));
    }
    discovery.refresh(&owner, &[]);
    assert_eq!(discovery.offsets.get(&ShardId(0)), Some(&1));
    assert!(!discovery.offsets.contains_key(&ShardId(2)));
    assert_eq!(discovery.offsets.get(&ShardId(3)), Some(&0));
    assert_eq!(discovery.next(), Some((ShardId(3), StreamKind::Intent)));
    discovery.refresh(&owner, &[]);
    assert_eq!(
        discovery.next(),
        Some((ShardId(3), StreamKind::Publication))
    );
    for home in [ShardId(0), ShardId(3), ShardId(4)] {
        owner.write().unwrap().mark_draining(home);
    }
    discovery.refresh(&owner, &[]);
    assert!(discovery.next().is_none());
    assert!(discovery.offsets.is_empty());
    owner
        .write()
        .unwrap()
        .record_acquired(ShardId(0), ShardEpoch(2));
    owner.write().unwrap().mark_active(ShardId(0));
    discovery.refresh(&owner, &[]);
    assert_eq!(discovery.next(), Some((ShardId(0), StreamKind::Intent)));
}

#[tokio::test(start_paused = true)]
async fn admission_is_bounded_and_shutdown_drops_blocked_attempts() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let (repo, _) = fixture(100, clock.clone()).await;
    let executor = Arc::new(RecordingExecutor::new(true));
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run(
        repo,
        executor.clone(),
        owner(),
        cancel.clone(),
        clock.clone(),
    ));
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
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(InMemoryStore::with_shard_count(2).with_clock(clock.clone()));
    let mut ownership = ShardOwner::new(2);
    for home in [ShardId(0), ShardId(1)] {
        let mut start = crate::runtime::tests::sample_start_request(None, None);
        start.now = clock.now();
        start.request.received_at = start.now;
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
        clock.clone(),
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

#[derive(Default)]
struct ManyHomesExecutor {
    round_started: tokio::sync::Notify,
    delivered: tokio::sync::Notify,
}

#[async_trait]
impl Executor for ManyHomesExecutor {
    fn operation_kinds(&self) -> &'static [OperationKind] {
        // The driver asks for kinds once per round during stream refresh, so this marks a round.
        self.round_started.notify_one();
        &[
            OperationKind::ChildStart,
            OperationKind::ChildRelease,
            OperationKind::ExternalSignal,
            OperationKind::ExternalCancel,
            OperationKind::NexusStart,
            OperationKind::NexusCancel,
        ]
    }

    fn accepts_intent(&self, _: IntentKind) -> bool {
        false
    }

    async fn deliver(&self, work: Work, _: Attempt) -> Result<()> {
        let Work::Operation(row) = work else {
            panic!("fixture has one operation");
        };
        assert_eq!(row.source_home, ShardId(31));
        assert_eq!(row.incarnation.key.kind, OperationKind::ExternalSignal);
        self.delivered.notify_one();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn many_homes_discover_new_row_within_one_cycle() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(InMemoryStore::with_shard_count(32).with_clock(clock.clone()));
    let mut ownership = ShardOwner::new(32);
    for home in (0..32).map(ShardId) {
        ownership.record_acquired(home, ShardEpoch(1));
        ownership.mark_active(home);
    }
    let executor = Arc::new(ManyHomesExecutor::default());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run(
        repo.clone(),
        executor.clone(),
        Arc::new(RwLock::new(ownership)),
        cancel.clone(),
        clock.clone(),
    ));
    for _ in 0..17 {
        executor.round_started.notified().await;
    }

    let mut start = crate::runtime::tests::sample_start_request(None, None);
    start.now = clock.now();
    start.request.received_at = start.now;
    start.workflow_id = (0..10_000)
        .map(|n| WorkflowId(format!("home-{n}")))
        .find(|id| {
            tokeira_types::execution_home_bundle(
                start.namespace_id.0.as_bytes(),
                id.0.as_bytes(),
                32,
            ) == ShardId(31)
        })
        .unwrap();
    start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
    let mut transition = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(start))
        .unwrap();
    insert(&mut transition.next_state, 101);
    transition.next_state.pending_external_cancels.clear();
    assert!(matches!(
        repo.commit_transition(
            transition.next_state.run_key,
            transition,
            WriteFence::Unfenced,
        )
        .await
        .unwrap(),
        CommitResult::Applied { .. }
    ));

    // N + K - 2 = 262 other reads can intervene. Include the delivery read
    // and round up at 16 reads per tick: 17 ticks, even across an offset wrap.
    let deadline_rounds = (256u32 + 8 - 2 + 1).div_ceil(16);
    let delivered =
        tokio::time::timeout(TICK * deadline_rounds, executor.delivered.notified()).await;
    cancel.cancel();
    worker.await.unwrap();
    assert!(delivered.is_ok(), "row was not discovered within 17 ticks");
}

#[derive(Default)]
struct StarvationExecutor {
    delivered: tokio::sync::Notify,
    first_kind_attempts: AtomicUsize,
}

#[async_trait]
impl Executor for StarvationExecutor {
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
        assert_eq!(row.source_home, ShardId(0));
        match row.incarnation.key.kind {
            OperationKind::ExternalSignal => {
                self.first_kind_attempts.fetch_add(1, Ordering::SeqCst);
            }
            OperationKind::ExternalCancel => self.delivered.notify_one(),
            kind => panic!("unexpected kind: {kind:?}"),
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn backlogged_first_kind_cannot_starve_another_kind() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    const HOMES: u32 = 32;
    const KINDS: u32 = 4;
    let repo = Arc::new(InMemoryStore::with_shard_count(HOMES).with_clock(clock.clone()));
    let mut ownership = ShardOwner::new(HOMES);
    for home in (0..HOMES).map(ShardId) {
        ownership.record_acquired(home, ShardEpoch(1));
        ownership.mark_active(home);
    }
    let mut start = crate::runtime::tests::sample_start_request(None, None);
    start.now = clock.now();
    start.request.received_at = start.now;
    start.workflow_id = (0..10_000)
        .map(|n| WorkflowId(format!("home-{n}")))
        .find(|id| {
            tokeira_types::execution_home_bundle(
                start.namespace_id.0.as_bytes(),
                id.0.as_bytes(),
                HOMES,
            ) == ShardId(0)
        })
        .unwrap();
    start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
    let mut transition = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(start))
        .unwrap();
    // Sixteen visits worth of first-kind rows outlast the four-visit deadline.
    // A walk that always starts a home at its first kind lets those rows take
    // all eight of the home's slots on every visit.
    for event in 100..100 + i64::try_from(MAX_PER_HOME * 16).unwrap() {
        insert(&mut transition.next_state, event);
    }
    transition
        .next_state
        .pending_external_cancels
        .retain(|&event, _| event == 101);
    repo.commit_transition(
        transition.next_state.run_key,
        transition,
        WriteFence::Unfenced,
    )
    .await
    .unwrap();
    let executor = Arc::new(StarvationExecutor::default());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run(
        repo,
        executor.clone(),
        Arc::new(RwLock::new(ownership)),
        cancel.clone(),
        clock.clone(),
    ));
    // Home zero is visited in rounds 1, 9, 17 and 25. Stop after that fourth
    // visit, before round 33 could give a broken walk a fifth opportunity.
    let cycle_rounds = (HOMES * KINDS).div_ceil(u32::try_from(PAGES_PER_TICK).unwrap());
    let delivered = tokio::time::timeout(
        TICK * ((KINDS - 1) * cycle_rounds + 1),
        executor.delivered.notified(),
    )
    .await;
    cancel.cancel();
    worker.await.unwrap();
    assert!(executor.first_kind_attempts.load(Ordering::SeqCst) >= MAX_PER_HOME);
    assert!(
        delivered.is_ok(),
        "other kind was not delivered within K home visits"
    );
}

struct DeadlineExecutor(
    tokio::sync::mpsc::UnboundedSender<(OperationKey, OffsetDateTime)>,
    bool,
    Arc<dyn Clock>,
);

#[async_trait]
impl Executor for DeadlineExecutor {
    fn operation_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::NexusStart, OperationKind::NexusCancel]
    }
    fn accepts_intent(&self, _: IntentKind) -> bool {
        false
    }
    async fn deliver(&self, work: Work, _: Attempt) -> Result<()> {
        let Work::Operation(row) = work else {
            panic!("operation");
        };
        self.0.send((row.incarnation.key, self.2.now())).unwrap();
        if self.1 {
            anyhow::bail!("volatile retry");
        }
        Ok(())
    }
}

fn deadline_operation(due: OffsetDateTime) -> tokeira_kernel::PendingNexusOperation {
    tokeira_kernel::PendingNexusOperation {
        operation_id: "retry".into(),
        scheduled_event_id: 100,
        endpoint: "endpoint".into(),
        service: "service".into(),
        operation: "operation".into(),
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: None,
        scheduled_at: due - time::Duration::seconds(1),
        started: false,
        started_at: None,
        attempt: 1,
        last_attempt_failure: None,
        next_attempt_at: Some(due),
        operation_token: String::new(),
        input: Default::default(),
        cancellation: None,
    }
}

async fn nexus_driver_deadline(cancellation: bool, volatile_retry: bool, pages: bool) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let (repo, mut state) = fixture(0, clock.clone()).await;
    let due = clock.now() + time::Duration::milliseconds(200);
    state
        .pending_nexus_operations
        .insert("retry".into(), deadline_operation(due));
    if cancellation {
        let op = state.pending_nexus_operations.get_mut("retry").unwrap();
        op.started = true;
        op.started_at = Some(due - time::Duration::seconds(1));
        op.operation_token = "token".into();
        op.next_attempt_at = None;
        op.cancellation = Some(tokeira_kernel::NexusOperationCancellation {
            requested_event_id: 101,
            requested_time: Some(due - time::Duration::seconds(1)),
            state: tokeira_kernel::NexusOperationCancellationState::BackingOff,
            attempt: 1,
            last_attempt_complete_time: None,
            last_attempt_failure: None,
            next_attempt_at: Some(due),
        });
    }
    if pages {
        for id in 1..8 {
            let mut op = state.pending_nexus_operations["retry"].clone();
            op.operation_id = format!("retry-{id}");
            op.scheduled_event_id += id;
            op.next_attempt_at =
                Some(due + time::Duration::milliseconds(if id >= 6 { 200 } else { 0 }));
            state
                .pending_nexus_operations
                .insert(op.operation_id.clone(), op);
        }
    }
    let expected_seq = state.transition_seq;
    state.transition_seq = expected_seq.next();
    let transition = Transition {
        expected_seq,
        next_state: state.clone(),
        history_events: Default::default(),
        event_principals: Default::default(),
        request_dedupe_ops: Default::default(),
        activity_ops: Default::default(),
        timer_ops: Default::default(),
        dispatch_ops: Default::default(),
        events_numbered_at_close: 0,
        workflow_task_heartbeat_timed_out: false,
        growth_limits: None,
    };
    repo.commit_transition(state.run_key, transition, WriteFence::Unfenced)
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run_with_interval(
        repo,
        Arc::new(DeadlineExecutor(tx, volatile_retry, clock.clone())),
        owner(),
        cancel.clone(),
        clock.clone(),
        Duration::from_secs(3600),
    ));
    let submitted = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    if pages && let Ok(Some((key, _))) = submitted.as_ref() {
        let mut seen = BTreeSet::from([*key]);
        tokio::time::timeout(Duration::from_secs(3), async {
            while seen.len() < 8 {
                let (key, at) = rx.recv().await.expect("all pages delivered");
                assert!(
                    at < due + time::Duration::milliseconds(700),
                    "later row delayed: {key:?} at {at:?}, due {due:?}"
                );
                seen.insert(key);
            }
        })
        .await
        .expect("later deadlines in the targeted page stay armed");
    }
    if volatile_retry && submitted.is_ok() {
        let second = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        assert!(
            second.expect("volatile delay must wake driver").unwrap().1
                >= due + time::Duration::seconds(2)
        );
    }
    cancel.cancel();
    worker.await.unwrap();
    assert!(
        submitted
            .expect("backoff deadline must wake driver")
            .unwrap()
            .1
            >= due
    );
}

#[tokio::test]
async fn nexus_backoff_wakes_driver_before_one_hour_tick() {
    nexus_driver_deadline(false, false, false).await;
}

#[tokio::test]
async fn nexus_cancel_backoff_wakes_driver_before_one_hour_tick() {
    nexus_driver_deadline(true, false, false).await;
}

#[tokio::test]
async fn volatile_retry_delay_wakes_driver_before_one_hour_tick() {
    nexus_driver_deadline(false, true, false).await;
}

#[tokio::test]
async fn later_deadline_in_targeted_page_stays_armed() {
    nexus_driver_deadline(false, false, true).await;
}

struct WakingDeadlineExecutor {
    inner: DeadlineExecutor,
    wake: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl Executor for WakingDeadlineExecutor {
    fn wakeup(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(self.wake.clone())
    }
    fn operation_kinds(&self) -> &'static [OperationKind] {
        self.inner.operation_kinds()
    }
    fn accepts_intent(&self, kind: IntentKind) -> bool {
        self.inner.accepts_intent(kind)
    }
    async fn deliver(&self, work: Work, attempt: Attempt) -> Result<()> {
        self.inner.deliver(work, attempt).await
    }
}

async fn targeted_page_read(target: i64, volatile: bool) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let (store, mut state) = fixture(0, clock.clone()).await;
    let repo = Arc::new(crate::runtime::repair_tests::Repo::with_inner(
        Arc::try_unwrap(store).unwrap(),
    ));
    let due = clock.now() + time::Duration::milliseconds(500);
    for id in 100..300 {
        let mut op = deadline_operation(if id == target {
            due
        } else {
            due + time::Duration::hours(1)
        });
        op.scheduled_event_id = id;
        op.operation_id = id.to_string();
        state
            .pending_nexus_operations
            .insert(op.operation_id.clone(), op);
    }
    let expected_seq = state.transition_seq;
    state.transition_seq = expected_seq.next();
    repo.commit_transition(
        state.run_key,
        Transition {
            expected_seq,
            next_state: state,
            history_events: Default::default(),
            event_principals: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        },
        WriteFence::Unfenced,
    )
    .await
    .unwrap();
    let wake = Arc::new(tokio::sync::Notify::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run_with_interval(
        repo.clone(),
        Arc::new(WakingDeadlineExecutor {
            inner: DeadlineExecutor(tx, volatile, clock.clone()),
            wake: wake.clone(),
        }),
        owner(),
        cancel.clone(),
        clock.clone(),
        Duration::from_secs(3600),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        for round in 1..=4 {
            loop {
                let read = repo.outbound_read.notified();
                if repo.outbound_reads.lock().unwrap().len() >= round * 4 {
                    break;
                }
                read.await;
            }
            if round != 4 {
                wake.notify_one();
            }
        }
    })
    .await
    .unwrap();
    let before = std::mem::take(&mut *repo.outbound_reads.lock().unwrap());
    assert!(before.iter().any(|read| matches!(read,
        crate::runtime::repair_tests::OutboundRead::Operation { kind: OperationKind::NexusStart, after: Some(key), .. }
        if key.initiating_event > target)));
    let (key, at) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key.initiating_event, target);
    assert!(at >= due);
    let expected = crate::runtime::repair_tests::OutboundRead::Operation {
        home: ShardId(0),
        kind: OperationKind::NexusStart,
        after: Some(OperationKey {
            initiating_event: target - 1,
            ..key
        }),
        fresh_bound: true,
        limit: MAX_PER_HOME,
    };
    assert_eq!(*repo.outbound_reads.lock().unwrap(), vec![expected.clone()]);
    if volatile {
        repo.outbound_reads.lock().unwrap().clear();
        let (retried, retried_at) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried, key);
        assert!(retried_at >= at + time::Duration::seconds(2));
        assert_eq!(*repo.outbound_reads.lock().unwrap(), vec![expected]);
    }
    repo.outbound_reads.lock().unwrap().clear();
    wake.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let read = repo.outbound_read.notified();
            if repo.outbound_reads.lock().unwrap().len() >= 4 {
                break;
            }
            read.await;
        }
    })
    .await
    .unwrap();
    assert!(repo.outbound_reads.lock().unwrap().iter().any(|read| matches!(read,
        crate::runtime::repair_tests::OutboundRead::Operation { kind: OperationKind::NexusStart, after: Some(key), fresh_bound: false, .. }
        if key.initiating_event == 131)));
    cancel.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn deadline_reads_one_page_behind_cursor_without_other_streams() {
    targeted_page_read(103, false).await;
    targeted_page_read(108, false).await;
}

#[tokio::test]
async fn volatile_retry_reads_one_page_from_saved_predecessor() {
    targeted_page_read(103, true).await;
}

#[tokio::test(start_paused = true)]
async fn advanced_attempt_settle_delay_does_not_read_a_page() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let (store, mut state) = fixture(0, clock.clone()).await;
    let repo = Arc::new(crate::runtime::repair_tests::Repo::with_inner(
        Arc::try_unwrap(store).unwrap(),
    ));
    state.pending_nexus_operations.insert(
        "retry".into(),
        deadline_operation(clock.now() - time::Duration::seconds(1)),
    );
    let expected_seq = state.transition_seq;
    state.transition_seq = expected_seq.next();
    repo.commit_transition(
        state.run_key,
        Transition {
            expected_seq,
            next_state: state,
            history_events: Default::default(),
            event_principals: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        },
        WriteFence::Unfenced,
    )
    .await
    .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let driver = run_with_interval(
        repo.clone(),
        Arc::new(DeadlineExecutor(tx, false, clock.clone())),
        owner(),
        cancel.clone(),
        clock.clone(),
        Duration::from_secs(3600),
    );
    tokio::pin!(driver);
    assert!(
        poll_fn(|cx| Poll::Ready(driver.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    rx.recv().await.unwrap();
    assert!(
        poll_fn(|cx| Poll::Ready(driver.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    repo.outbound_reads.lock().unwrap().clear();

    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        poll_fn(|cx| Poll::Ready(driver.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(
        repo.outbound_reads.lock().unwrap().is_empty(),
        "an advanced attempt must leave its settle-delay recheck to the tick"
    );
    cancel.cancel();
    driver.await;
}
