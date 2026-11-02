//! Storage-boundary races for starts, lost replies and fenced replay observations.

use proptest::prelude::*;
use smallvec::SmallVec;
use uuid::Uuid;

use super::*;
use crate::{ActivityStartOutcome, ActivityTaskStartRequest, NotShardOwner};

#[derive(Debug)]
struct DroppingOwnerReply {
    owner: Arc<TokeiraRuntime<Repo>>,
    home: ShardId,
    lose_reply: bool,
    requests: Mutex<Vec<Uuid>>,
}

#[async_trait]
impl crate::ActivityTaskStarter for DroppingOwnerReply {
    async fn start(
        &self,
        home: ShardId,
        request: ActivityTaskStartRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<ActivityStartOutcome> {
        assert_eq!(home, self.home);
        let first = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.request_id);
            requests.len() == 1
        };
        let outcome = self.owner.start_forwarded_activity_task(request).await?;
        if first && self.lose_reply {
            return Err(
                crate::RetryableActivityTaskStart("injected lost committed reply".into()).into(),
            );
        }
        Ok(outcome)
    }
}

// Feature: activity-dispatch, Property 18
// No notification or legacy delivery survives: every placement must rediscover, start once and resolve.
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: None, ..Default::default() })]
    #[test]
    fn activity_delivery_survives_legacy_retirement(
        execution in 0u32..8, queue_home in 0u32..8, dropped in 0usize..4,
        restart in any::<bool>(), lost_reply in any::<bool>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let repo = Arc::new(Repo { inner: InMemoryStore::with_shard_count(8), ..Default::default() });
            let (mut state, _) = seed_start(&repo).await;
            state.workflow_id = (0..).map(|index| WorkflowId(format!("retirement-{index}"))).find(|id| execution_home_bundle(state.namespace_id.0.as_bytes(), id.0.as_bytes(), 8) == ShardId(execution)).unwrap();
            let queue_name = (0..).map(|index| TaskQueueName(format!("retirement-q-{index}"))).find(|name| {
                bundle_for_partition(workflow_queue_partition(state.namespace_id, &name.0, 8), 8) == ShardId(queue_home)
            }).unwrap();
            state.activities.get_mut("activity-0").unwrap().task_queue = queue_name.clone();
            state = write_state(&repo, state, Vec::new()).await;
            let queue = QueueKey { namespace_id: state.namespace_id, task_queue: queue_name, task_kind: TaskKind::Activity, deployment: None, build_id: None };
            let owner = home_runtime(repo.clone(), ShardId(execution), true);
            owner.ready().await.unwrap();
            let mut matching = if execution == queue_home { owner.clone() } else { home_runtime(repo.clone(), ShardId(queue_home), true) };
            matching.ready().await.unwrap();
            for _ in 0..dropped {
                let offer = matching.poll_activity_task_offer(queue.clone(), WorkerIdentity("lost-poller".into()), std::time::Duration::from_secs(10)).await.unwrap().unwrap();
                drop(offer);
                matching.activity_broker.remove_run(state.run_key).await;
            }
            if restart && execution != queue_home {
                matching.shutdown_handle().begin_shutdown();
                matching.shutdown_handle().wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
                matching = home_runtime(repo.clone(), ShardId(queue_home), true);
                matching.ready().await.unwrap();
            }
            let transport = Arc::new(DroppingOwnerReply { owner: owner.clone(), home: ShardId(execution), lose_reply: lost_reply, requests: Mutex::new(Vec::new()) });
            if execution != queue_home { matching.set_activity_task_starter(transport.clone()); }
            let started = matching.poll_activity_task(queue.clone(), WorkerIdentity("retirement-worker".into()), std::time::Duration::from_secs(10)).await.unwrap().unwrap();
            assert_eq!(started.input.0[0].data, vec![0, 255, 128]);
            let LoadedRun::Existing(after) = repo.inner.load_run(state.run_key).await.unwrap() else { panic!("run") };
            assert_eq!(after.transition_seq, state.transition_seq.next());
            assert_eq!(after.activity_start_receipts.len(), 1);
            let requests = transport.requests.lock().unwrap().clone();
            if execution != queue_home {
                assert_eq!(requests.len(), if lost_reply { 2 } else { 1 });
                assert!(requests.iter().all(|id| *id == requests[0]));
            }
            owner.complete_activity_task(started.token, Payloads::default(), None, RequestContext::unattributed(OffsetDateTime::now_utc())).await.unwrap();
            let LoadedRun::Existing(resolved) = repo.inner.load_run(state.run_key).await.unwrap() else { panic!("run") };
            assert!(!resolved.activities.contains_key("activity-0"));
            assert!(repo.inner.list_all_dispatchable_activity_tasks(&queue, 10).await.unwrap().is_empty());
            assert!(repo.inner.backlog_stats_by_priority(&queue).await.unwrap().is_empty());
            matching.shutdown_handle().begin_shutdown();
            owner.shutdown_handle().begin_shutdown();
            matching.shutdown_handle().wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
            owner.shutdown_handle().wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
            assert_eq!(repo.legacy_delivery_calls.load(Ordering::SeqCst), 0);
        });
    }
}

async fn write_state(
    repo: &Repo,
    mut state: WorkflowState,
    events: Vec<HistoryEvent>,
) -> WorkflowState {
    let expected_seq = state.transition_seq;
    state.transition_seq = expected_seq.next();
    let result = repo
        .inner
        .commit_transition(
            state.run_key,
            Transition {
                expected_seq,
                next_state: state,
                event_principals: events.iter().map(|_| None).collect(),
                history_events: events.into_iter().collect(),
                request_dedupe_ops: SmallVec::new(),
                activity_ops: SmallVec::new(),
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                events_numbered_at_close: 0,
                growth_limits: None,
            },
            WriteFence::Unfenced,
        )
        .await
        .unwrap();
    let CommitResult::Applied { new_state } = result else {
        panic!("fixture commit");
    };
    new_state
}

async fn seed_start(repo: &Arc<Repo>) -> (WorkflowState, ActivityTaskStartRequest) {
    let mut state = seed_activity_repair(repo, 1).await;
    let activity = state.activities.get_mut("activity-0").unwrap();
    activity.attempt = 1;
    activity.stamp = 0;
    activity.scheduled_at = OffsetDateTime::now_utc();
    activity.current_attempt_scheduled_at = Some(activity.scheduled_at);
    activity.start_to_close_timeout = Some(Duration::minutes(1));
    let event = HistoryEvent {
        event_id: activity.schedule_event_id,
        happened_at: activity.scheduled_at,
        kind: HistoryEventKind::ActivityTaskScheduled {
            workflow_task_completed_event_id: activity.schedule_event_id - 1,
            activity_id: activity.activity_id.clone(),
            activity_type: activity.activity_type.clone(),
            task_queue: activity.task_queue.clone(),
            input: Payloads(vec![Payload {
                data: vec![0, 255, 128],
                metadata: BTreeMap::new(),
                external_payloads: Vec::new(),
            }]),
            header: None,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: activity.start_to_close_timeout,
            heartbeat_timeout: None,
            priority: None,
        },
    };
    state.last_event_id = event.event_id;
    let state = write_state(repo, state, vec![event]).await;
    let row = derive_activity_dispatch(&state, ShardId(0))
        .unwrap()
        .into_values()
        .next()
        .unwrap();
    let request = ActivityTaskStartRequest {
        offered: row.task(),
        request_id: Uuid::new_v4(),
        worker: WorkerIdentity("race-worker".into()),
    };
    (state, request)
}

async fn fixture(
    repo: &Arc<Repo>,
) -> (
    Arc<TokeiraRuntime<Repo>>,
    WorkflowState,
    ActivityTaskStartRequest,
) {
    let (state, request) = seed_start(repo).await;
    let runtime = runtime(repo.clone());
    runtime.ready().await.unwrap();
    (runtime, state, request)
}

async fn fence(runtime: &TokeiraRuntime<Repo>, identity: &str) -> ShardEpoch {
    let LeaseOutcome::Acquired { epoch } = runtime
        .repo
        .inner
        .try_acquire_bundle(ShardId(0), identity.into(), "127.0.0.1:0".into())
        .await
        .unwrap()
    else {
        panic!("lease acquisition");
    };
    let mut owner = runtime.shard_owner.write().unwrap();
    owner.fence_writes(identity.into());
    owner.record_acquired(ShardId(0), epoch);
    owner.mark_active(ShardId(0));
    epoch
}

async fn transfer(repo: &Repo, epoch: ShardEpoch) -> ShardEpoch {
    repo.inner
        .relinquish_bundle(ShardId(0), "race-a".into(), epoch)
        .await
        .unwrap();
    let LeaseOutcome::Acquired { epoch } = repo
        .inner
        .try_acquire_bundle(ShardId(0), "race-b".into(), "127.0.0.1:1".into())
        .await
        .unwrap()
    else {
        panic!("replacement lease");
    };
    epoch
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 7: an observed start cannot overwrite a competing start, replacement or fence.
    #[test]
    fn activity_start_controlled_commit_races(race in 0u8..5, replacement_delta in 1i64..100) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let repo = Arc::new(Repo::default());
            let (runtime, state, request) = fixture(&repo).await;
            let epoch = fence(&runtime, "race-a").await;
            let (entered, resume) = repo.pause(Point::Commit);
            let pending = tokio::spawn({ let runtime = runtime.clone(); let request = request.clone(); async move {
                runtime.start_forwarded_activity_task(request).await
            }});
            entered.await.unwrap();
            match race {
                0 => {
                    let mut competing = request.clone();
                    competing.request_id = Uuid::new_v4();
                    assert!(matches!(runtime.start_forwarded_activity_task(competing).await.unwrap(), ActivityStartOutcome::Started(_)));
                }
                1 => {
                    let task = &request.offered;
                    let eager = runtime.try_claim_activity_task(task.queue.clone(), task.run_key, task.activity_id.clone(), task.schedule_event_id,
                        WorkerIdentity("eager".into()), Payloads::default(), None).await.unwrap();
                    assert!(eager.is_some());
                }
                2 => {
                    let mut replacement = state.clone();
                    replacement.activities.get_mut("activity-0").unwrap().schedule_event_id += replacement_delta;
                    write_state(&repo, replacement, Vec::new()).await;
                    let task = &request.offered;
                    assert!(runtime.try_claim_activity_task(task.queue.clone(), task.run_key, task.activity_id.clone(), task.schedule_event_id,
                        WorkerIdentity("late-eager".into()), Payloads::default(), None).await.unwrap().is_none());
                }
                3 => { transfer(&repo, epoch).await; }
                _ => { write_state(&repo, state.clone(), Vec::new()).await; }
            }
            let before_resume = repo.load_run(state.run_key).await.unwrap();
            if race == 4 {
                let (conflicted, retry) = repo.pause(Point::Committed);
                resume.send(()).unwrap();
                conflicted.await.unwrap();
                let next = transfer(&repo, epoch).await;
                {
                    let mut owner = runtime.shard_owner.write().unwrap();
                    owner.fence_writes("race-b".into());
                    owner.record_acquired(ShardId(0), next);
                    owner.mark_active(ShardId(0));
                }
                retry.send(()).unwrap();
            } else { resume.send(()).unwrap(); }
            let outcome = pending.await.unwrap();
            if race >= 3 { prop_assert!(outcome.unwrap_err().is::<NotShardOwner>()); }
            else { prop_assert!(matches!(outcome.unwrap(), ActivityStartOutcome::Stale)); }
            prop_assert_eq!(&repo.load_run(state.run_key).await.unwrap(), &before_resume);
            let LoadedRun::Existing(after) = before_resume else { panic!("run"); };
            prop_assert_eq!(after.activity_start_receipts.len(), usize::from(race < 2));
            prop_assert_eq!(after.transition_seq, if race == 3 { state.transition_seq } else { state.transition_seq.next() });
            stop(&runtime).await;
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 8: cancellation after commit leaves a replayable receipt and restored timeout.
    #[test]
    fn activity_start_lost_reply_recovers_without_write(takeover in any::<bool>(), expire in any::<bool>()) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let repo = Arc::new(Repo::default());
            let (runtime, state, request) = fixture(&repo).await;
            let epoch = fence(&runtime, "race-a").await;
            let (entered, resume) = repo.pause(Point::Committed);
            let pending = tokio::spawn({ let runtime = runtime.clone(); let request = request.clone(); async move {
                runtime.start_forwarded_activity_task(request).await
            }});
            entered.await.unwrap();
            pending.abort();
            prop_assert!(pending.await.unwrap_err().is_cancelled());
            drop(resume);
            let LoadedRun::Existing(committed) = repo.load_run(state.run_key).await.unwrap() else { panic!("run"); };
            prop_assert_eq!(committed.transition_seq, state.transition_seq.next());
            let current_epoch = if takeover {
                let epoch = transfer(&repo, epoch).await;
                runtime.shard_owner.write().unwrap().fence_writes("race-b".into());
                epoch
            } else { epoch };
            runtime.recover_self_assigned_shard(ShardId(0), current_epoch).await.unwrap();
            let tracked = runtime.activity_tracking.snapshot();
            prop_assert!(tracked.iter().any(|entry| entry.run_key == state.run_key && entry.started_at == committed.activities["activity-0"].started_at));
            let ActivityStartOutcome::Replayed(replay) = runtime.start_forwarded_activity_task(request.clone()).await.unwrap() else { panic!("receipt replay"); };
            prop_assert_eq!(replay.started_time, committed.activities["activity-0"].started_at.unwrap());
            prop_assert_eq!(replay.token.shard_epoch, current_epoch);
            prop_assert_eq!(replay.token.attempt, 1);
            prop_assert_eq!(repo.load_run(state.run_key).await.unwrap(), LoadedRun::Existing(committed.clone()));
            if expire {
                let mut expiring = committed.clone();
                expiring.activities.get_mut("activity-0").unwrap().start_to_close_timeout = Some(Duration::nanoseconds(1));
                write_state(&repo, expiring, Vec::new()).await;
                crate::activity_timeout::scan_activity_timeouts_once(
                    &runtime.activity_retry_deps(), Some(ShardId(0)), &runtime.lanes, runtime.lanes.len(),
                    &ActivityTimeoutScannerConfig::default(),
                ).await;
                let after = repo.load_run(state.run_key).await.unwrap();
                prop_assert!(matches!(runtime.start_forwarded_activity_task(request).await.unwrap(), ActivityStartOutcome::Stale));
                prop_assert_eq!(repo.load_run(state.run_key).await.unwrap(), after);
            }
            stop(&runtime).await;
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 8: replay cannot return a receipt invalidated while its observation is in flight.
    #[test]
    fn activity_replay_races_resolution_or_lease_loss(takeover in any::<bool>()) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let repo = Arc::new(Repo::default());
            let (runtime, state, request) = fixture(&repo).await;
            let epoch = fence(&runtime, "race-a").await;
            assert!(matches!(runtime.start_forwarded_activity_task(request.clone()).await.unwrap(), ActivityStartOutcome::Started(_)));
            let (entered, resume) = repo.pause(Point::ActivityObservation);
            let pending = tokio::spawn({ let runtime = runtime.clone(); async move {
                runtime.start_forwarded_activity_task(request).await
            }});
            entered.await.unwrap();
            if takeover { transfer(&repo, epoch).await; }
            else {
                let LoadedRun::Existing(mut state) = repo.load_run(state.run_key).await.unwrap() else { panic!("run"); };
                state.activities.clear();
                state.activity_start_receipts.clear();
                write_state(&repo, state, Vec::new()).await;
            }
            let before_resume = repo.load_run(state.run_key).await.unwrap();
            resume.send(()).unwrap();
            let outcome = pending.await.unwrap();
            if takeover { prop_assert!(outcome.unwrap_err().is::<NotShardOwner>()); }
            else { prop_assert!(matches!(outcome.unwrap(), ActivityStartOutcome::Stale)); }
            prop_assert_eq!(repo.load_run(state.run_key).await.unwrap(), before_resume);
            stop(&runtime).await;
            Ok(())
        })?;
    }
}
