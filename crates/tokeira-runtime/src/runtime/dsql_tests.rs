//! Opt-in runtime acquisition checks; the storage live suite bootstraps schema first.

use super::*;
use crate::{
    activity_timeout::ActivityTrackingEntry,
    discovery::tests::transition,
    nexus::{CompletionCallbackTrackingEntry, NexusTimeoutEntry},
};
use anyhow::Context;
use std::path::Path;
use tokeira_kernel::{BasicKernel, Kernel, TerminateRequest};
use tokeira_storage::{
    DeleteRunRequest, LeaseOutcome, ProjectionImageKey, ProjectionLog, WriteFence,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
};
use tokeira_types::WorkflowId;

fn finish_cleanup(result: Result<()>, cleanup: Result<()>, step: &str) -> Result<()> {
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("{step} also failed: {cleanup:#}")))
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}

async fn release_contract_lease(
    repo: &dyn LeaseRepository,
    home: ShardId,
    owner: &str,
) -> Result<()> {
    // Relinquishing the runtime only cancels local ownership. Release the
    // durable lease after renewal tasks stop, fencing any intervening takeover.
    if let Some(lease) = repo
        .list_bundle_leases()
        .await?
        .into_iter()
        .find(|lease| lease.bundle_id == home && lease.owner_node_id.as_deref() == Some(owner))
    {
        let outcome = repo
            .relinquish_bundle(home, owner.to_owned(), lease.epoch)
            .await?;
        anyhow::ensure!(
            matches!(outcome, LeaseOutcome::Acquired { .. }),
            "owned fixture lease release was rejected: {outcome:?}"
        );
    }
    anyhow::ensure!(
        repo.list_bundle_leases().await?.iter().all(|lease| {
            lease.bundle_id != home || lease.owner_node_id.as_deref() != Some(owner)
        }),
        "owned fixture lease survived shutdown"
    );
    Ok(())
}

/// Remove only this fixture's executions, including deletion projection images.
async fn purge_contract_runs(
    repo: &dyn RunRepository,
    log: &dyn ProjectionLog,
    keys: &[RunKey],
    home: ShardId,
) -> Result<()> {
    for &key in keys {
        if let LoadedRun::Existing(mut state) = repo.load_run(key).await? {
            if state.status.is_open() {
                let closed = BasicKernel.apply(
                    LoadedRun::Existing(state.clone()),
                    Command::Terminate(TerminateRequest {
                        reason: "storage contract fixture cleanup".into(),
                        details: None,
                        identity: "fixture".into(),
                        links: Vec::new(),
                        request: tokeira_types::RequestContext::unattributed(
                            OffsetDateTime::now_utc(),
                        ),
                        now: OffsetDateTime::now_utc(),
                    }),
                )?;
                let CommitResult::Applied { new_state } = repo
                    .commit_transition_for_bundle(key, home, closed, WriteFence::Unfenced)
                    .await?
                else {
                    anyhow::bail!("fixture close did not apply");
                };
                state = new_state;
            }
            repo.delete_run_for_bundle(
                key,
                home,
                DeleteRunRequest {
                    expected_seq: state.transition_seq,
                    deleted_at: OffsetDateTime::now_utc(),
                },
                WriteFence::Unfenced,
            )
            .await?;
        }
        repo.purge_run(key).await?;
        let partition = (key.0.as_u128() as u32) % 16;
        let mut cursor = Some(ProjectionImageKey {
            run_key: key,
            transition_seq: tokeira_types::TransitionSeq::ZERO,
        });
        loop {
            let page = log.read_page(partition, 1, cursor, 100).await?;
            let owned: Vec<_> = page
                .images
                .iter()
                .take_while(|image| image.run_key == key)
                .map(|image| image.key())
                .collect();
            if owned.is_empty() {
                break;
            }
            cursor = owned.last().copied();
            log.delete_images(partition, 1, &owned).await?;
        }
        anyhow::ensure!(
            repo.load_run(key).await? == LoadedRun::Absent,
            "fixture state survived cleanup"
        );
        anyhow::ensure!(
            repo.read_history(key, 0, 1).await?.is_empty(),
            "fixture history survived cleanup"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
async fn storage_contract_live_acquisition_spans_recovery_pages() -> Result<()> {
    acquisition_spans_recovery_pages(false).await
}

#[tokio::test]
#[ignore = "requires an isolated ephemeral Aurora DSQL database with 4096 homes"]
async fn activity_dispatch_live_acquisition_spans_recovery_pages() -> Result<()> {
    acquisition_spans_recovery_pages(true).await
}

async fn acquisition_spans_recovery_pages(activities: bool) -> Result<()> {
    let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .context("explicit live acquisition test requires TOKEIRA_DSQL_TEST_DATABASE_URL")?;
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..Default::default()
            },
            shard_count: 4096,
            projection_partition_count: 16,
            ..Default::default()
        },
    )
    .await?;
    let log = store.projection_log().clone();
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    let home = ShardId(4094);
    // This includes outbound preparation. It must finish with no lease held,
    // before these current-format runs become candidates for a real acquisition.
    tokeira_storage::prepare_execution_placement(repo.as_ref()).await?;
    anyhow::ensure!(
        repo.list_recovery_candidates_for_shard(home, None, 1)
            .await?
            .states
            .is_empty(),
        "acquisition home contains foreign candidates"
    );
    let queue = QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("storage-contract-acquisition".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let clock = (OffsetDateTime::now_utc() + time::Duration::hours(1)).replace_nanosecond(0)?;
    let mut transitions = Vec::new();
    for index in 0..=crate::recovery::RECOVERY_PAGE {
        let mut item = transition(&queue, 0);
        item.next_state.workflow_id = (0..)
            .map(|attempt| tokeira_types::WorkflowId(format!("contract-{index}-{attempt}")))
            .find(|id| {
                execution_home_bundle(queue.namespace_id.0.as_bytes(), id.0.as_bytes(), 4096)
                    == home
            })
            .expect("acquisition fixture invariant");
        item.next_state.started_at = clock;
        item.next_state.workflow_run_timeout = Some(time::Duration::hours(1));
        item.next_state.workflow_execution_timeout = None;
        item.next_state.workflow_task_timeout = time::Duration::hours(1);
        let task = item
            .next_state
            .pending_workflow_task
            .as_mut()
            .expect("acquisition fixture invariant");
        task.started_at = Some(clock);
        task.started_event_id = Some(2);
        if activities {
            // The first run crosses both the 64-slot repair bound and the
            // inline state threshold; all 101 runs remain acquisition candidates.
            for activity in 0..if index == 0 { 129 } else { 1 } {
                let pending = acquisition_activity(activity, &queue, clock);
                item.next_state
                    .activities
                    .insert(pending.activity_id.clone(), pending);
            }
            if index == 0 {
                item.next_state.memo.0.insert(
                    "split-state".into(),
                    Payload::new(vec![0x53; 2 * 1024 * 1024]),
                );
            }
        }
        transitions.push(item);
    }
    let keys: Vec<_> = transitions
        .iter()
        .map(|item| item.next_state.run_key)
        .collect();
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        4096,
        "storage-contract-acquisition".into(),
        "127.0.0.1:0".into(),
        false,
        None,
    ));
    let result: Result<()> = async {
        let mut expected = Vec::new();
        for item in transitions {
            let CommitResult::Applied { new_state } = repo
                .commit_transition_for_bundle(
                    item.next_state.run_key,
                    home,
                    item,
                    WriteFence::Unfenced,
                )
                .await?
            else {
                anyhow::bail!("acquisition seed rejected");
            };
            if activities {
                for row in
                    tokeira_storage::derive_activity_dispatch(&new_state, home)?.into_values()
                {
                    anyhow::ensure!(
                        repo.delete_activity_dispatch_if_matches(
                            &tokeira_storage::ActivityDispatchIdentity {
                                run_key: row.incarnation.run_key,
                                activity_id: row.incarnation.activity_id,
                                schedule_event_id: row.incarnation.schedule_event_id,
                                attempt: row.incarnation.attempt,
                                stamp: row.incarnation.stamp,
                                dispatch_revision: row.dispatch_revision,
                                dispatch_at: row.dispatch_at,
                            }
                        )
                        .await?,
                        "fixture failed to remove a canonical activity row"
                    );
                }
            }
            expected.push(new_state);
        }
        expected.sort_by_key(|state| state.run_key);
        let (mut entered, mut resume) = runtime.wft_timeout_tracking.pause_next_recovery();
        let acquiring = runtime.clone();
        let acquisition = runtime
            .runtime_shutdown
            .spawn(async move { acquiring.acquire_shard(home).await });
        for index in 0..expected.len() {
            entered
                .await
                .context("recovery ended before all seeded candidates")?;
            let current = runtime
                .shard_owner
                .read()
                .expect("acquisition fixture invariant")
                .acquisition(home)
                .context("missing acquiring generation")?;
            anyhow::ensure!(
                !runtime
                    .shard_owner
                    .read()
                    .expect("acquisition fixture invariant")
                    .acquisition_active(&current),
                "home became active before recovery completed"
            );
            // Re-arm before releasing this row, so the next page cannot race
            // past the test's barrier. The last pause is in a different SQL page.
            if index + 1 < expected.len() {
                let (next_entered, next_resume) =
                    runtime.wft_timeout_tracking.pause_next_recovery();
                resume
                    .send(())
                    .map_err(|_| anyhow!("recovery lost its resume receiver"))?;
                entered = next_entered;
                resume = next_resume;
            } else {
                resume
                    .send(())
                    .map_err(|_| anyhow!("recovery lost its final resume receiver"))?;
                break;
            }
        }
        acquisition.await??;
        let current = runtime
            .shard_owner
            .read()
            .expect("acquisition fixture invariant")
            .acquisition(home)
            .context("missing active generation")?;
        anyhow::ensure!(
            runtime
                .shard_owner
                .read()
                .expect("acquisition fixture invariant")
                .acquisition_active(&current),
            "completed acquisition is not active"
        );
        if activities {
            let mut expected_rows = Vec::new();
            for state in &expected {
                anyhow::ensure!(
                    repo.load_run(state.run_key).await? == LoadedRun::Existing(state.clone()),
                    "acquisition changed authority or failed split-state hydration"
                );
                expected_rows.extend(
                    tokeira_storage::derive_activity_dispatch(state, home)?
                        .into_values()
                        .map(|row| tokeira_storage::ActivityHomePosition {
                            run_key: row.incarnation.run_key,
                            key: row.key,
                        }),
                );
            }
            expected_rows.sort();
            let mut actual = Vec::new();
            let mut after = None;
            loop {
                let page = repo
                    .list_activity_dispatch_home_page(
                        home,
                        after,
                        std::num::NonZeroU32::new(64).expect("positive limit"),
                    )
                    .await?;
                actual.extend(page.candidates);
                after = page.last_examined;
                if page.exhausted {
                    break;
                }
            }
            anyhow::ensure!(
                actual == expected_rows && actual.len() == 229,
                "activity acquisition lost slots across run/repair pages: {}",
                actual.len()
            );
        }
        let mut workflows = runtime.workflow_timeout_tracking.snapshot_for_shard(home);
        workflows.sort_by_key(|entry| entry.run_key);
        let mut tasks = runtime.wft_timeout_tracking.snapshot_for_shard(home);
        tasks.sort_by_key(|entry| entry.run_key);
        anyhow::ensure!(
            workflows.len() == expected.len() && tasks.len() == expected.len(),
            "acquisition lost recovered deadlines"
        );
        for ((workflow, task), state) in workflows.iter().zip(&tasks).zip(&expected) {
            let pending = state
                .pending_workflow_task
                .as_ref()
                .expect("acquisition fixture invariant");
            anyhow::ensure!(
                runtime
                    .workflow_timeout_tracking
                    .inner
                    .generation(&state.run_key)
                    == Some(current.generation)
                    && runtime.wft_timeout_tracking.generation(state.run_key)
                        == Some(current.generation),
                "recovered deadline is not installed under the active acquisition"
            );
            anyhow::ensure!(
                workflow.run_key == state.run_key
                    && workflow.started_at == clock
                    && workflow.workflow_run_timeout == Some(time::Duration::hours(1)),
                "workflow deadline differs from seed"
            );
            anyhow::ensure!(
                task.run_key == state.run_key
                    && task.started_at == clock
                    && task.started_event_id == 2
                    && task.logical_seq == pending.logical_seq
                    && task.workflow_task_timeout == time::Duration::hours(1),
                "workflow-task deadline differs from seed"
            );
        }
        Ok(())
    }
    .await;
    runtime.runtime_shutdown.begin_shutdown();
    runtime.relinquish_shard(home).await;
    let stopped = runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await;
    let released = if stopped.is_ok() {
        release_contract_lease(repo.as_ref(), home, &runtime.owner_identity).await
    } else {
        Err(anyhow!("lease release deferred until renewal tasks stop"))
    };
    let cleaned = if stopped.is_ok() {
        purge_contract_runs(repo.as_ref(), &log, &keys, home).await
    } else {
        Err(anyhow!(
            "data cleanup deferred until the failed runtime is stopped"
        ))
    };
    let closed = director.shutdown().await;
    let result = finish_cleanup(result, stopped.map_err(Into::into), "runtime shutdown");
    let result = finish_cleanup(result, released, "owned lease release");
    let result = finish_cleanup(result, cleaned, "owned data cleanup");
    finish_cleanup(result, closed, "connection shutdown")
}

#[tokio::test]
async fn workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs() -> Result<()> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(());
    };
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..Default::default()
            },
            shard_count: 8,
            ..Default::default()
        },
    )
    .await?;
    let log = store.projection_log().clone();
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    tokeira_storage::prepare_execution_placement(repo.as_ref()).await?;
    let queue = QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("live-acquisition".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let mut item = transition(&queue, 0);
    item.next_state.started_at = OffsetDateTime::now_utc();
    item.next_state.workflow_run_timeout = Some(time::Duration::hours(1));
    let run_key = item.next_state.run_key;
    let execution_home = execution_home_bundle(
        queue.namespace_id.0.as_bytes(),
        item.next_state.workflow_id.0.as_bytes(),
        8,
    );
    let expected = repo
        .commit_transition(run_key, item, tokeira_storage::WriteFence::Unfenced)
        .await?;
    anyhow::ensure!(
        matches!(expected, CommitResult::Applied { .. }),
        "live fixture commit did not apply"
    );
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        "live-dispatch-acquisition".into(),
        "127.0.0.1:0".into(),
        false,
        None,
    ));
    let checking_runtime = runtime.clone();
    let checking_repo = repo.clone();
    // A failed assertion becomes a join error so fixture shutdown still runs.
    let result = tokio::spawn(async move {
    let runtime = checking_runtime;
    let repo = checking_repo;
    let (entered, resume) = runtime.wft_timeout_tracking.pause_next_recovery();
    let acquiring = runtime.clone();
    let first = runtime.runtime_shutdown.spawn(async move {
        acquiring.acquire_shard(execution_home).await
    });
    entered.await?;
    let old = runtime
        .shard_owner
        .read()
        .unwrap()
        .acquisition(execution_home)
        .unwrap();
    assert!(!runtime.shard_owner.read().unwrap().acquisition_active(&old));
    let workflow = runtime
        .workflow_timeout_tracking
        .for_acquisition(old.clone());
    let activity = runtime.activity_tracking.for_acquisition(old.clone());
    let nexus = runtime.nexus_timeout_tracking.for_acquisition(old.clone());
    let callbacks = runtime
        .completion_callback_tracking
        .for_acquisition(old.clone());
    runtime.acquire_shard(execution_home).await?;
    let _ = resume.send(());
    assert!(first.await?.is_err());
    let current = runtime
        .shard_owner
        .read()
        .unwrap()
        .acquisition(execution_home)
        .unwrap();
    assert!(
        runtime
            .shard_owner
            .read()
            .unwrap()
            .acquisition_active(&current)
    );
    assert!(old.cancel.is_cancelled());
    let before = runtime.workflow_timeout_tracking.snapshot();
    let mut stale = before
        .iter()
        .find(|entry| entry.run_key == run_key)
        .unwrap()
        .clone();
    stale.started_at -= time::Duration::hours(2);
    workflow.insert(stale);
    assert_eq!(runtime.workflow_timeout_tracking.snapshot(), before);
    activity.insert(ActivityTrackingEntry {
        run_key,
        shard_id: execution_home,
        activity_id: "old".into(),
        original_scheduled_at: OffsetDateTime::now_utc(),
        last_dispatched_at: OffsetDateTime::now_utc(),
        started_at: None,
        last_heartbeat_at: None,
        cancel_requested: false,
    });
    nexus.insert(NexusTimeoutEntry {
        run_key,
        shard_id: execution_home,
        operation_id: "old".into(),
        scheduled_event_id: 1,
        scheduled_at: OffsetDateTime::now_utc(),
    });
    callbacks.insert(CompletionCallbackTrackingEntry {
        run_key,
        shard_id: execution_home,
        callback_index: 0,
    });
    assert!(runtime.activity_tracking.snapshot().is_empty());
    assert!(runtime.nexus_timeout_tracking.snapshot().is_empty());
    assert!(runtime.completion_callback_tracking.snapshot().is_empty());
    assert!(
        matches!(expected, CommitResult::Applied { new_state } if repo.load_run(run_key).await? == LoadedRun::Existing(new_state.clone()))
    );
    Ok::<(), anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(|result| result);
    runtime.runtime_shutdown.begin_shutdown();
    runtime.relinquish_shard(execution_home).await;
    let stopped = runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await;
    let released = if stopped.is_ok() {
        release_contract_lease(repo.as_ref(), execution_home, &runtime.owner_identity).await
    } else {
        Err(anyhow!("lease release deferred until renewal tasks stop"))
    };
    let cleaned = if stopped.is_ok() {
        purge_contract_runs(repo.as_ref(), &log, &[run_key], execution_home).await
    } else {
        Err(anyhow!(
            "data cleanup deferred until the failed runtime is stopped"
        ))
    };
    let closed = director.shutdown().await;
    let result = finish_cleanup(result, stopped.map_err(Into::into), "runtime shutdown");
    let result = finish_cleanup(result, released, "owned lease release");
    let result = finish_cleanup(result, cleaned, "owned data cleanup");
    finish_cleanup(result, closed, "connection shutdown")
}

// Feature: workflow-dispatch, Property 9: Ownership loss fences repair
#[test]
fn workflow_dispatch_live_fenced_repair_final_gate() {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return;
    };
    let executor = tokio::runtime::Runtime::new().unwrap();
    let store = executor
        .block_on(DsqlStore::from_database_url_for_tests(
            url,
            DsqlPoolConfig {
                migration: MigrationConfig {
                    migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../tokeira-storage/migrations"),
                },
                reservoir: ReservoirConfig {
                    target_ready: 4,
                    inflight_limit: 2,
                    ..Default::default()
                },
                shard_count: 256,
                ..Default::default()
            },
        ))
        .unwrap();
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    let home = ShardId(242);
    let cases = std::sync::atomic::AtomicUsize::new(0);
    let result = proptest::test_runner::TestRunner::new(proptest::test_runner::Config::with_cases(
        100,
    ))
    .run(&(0u8..5, proptest::bool::ANY), |(loss, first)| {
        executor.block_on(async {
            let LeaseOutcome::Acquired { epoch } = repo
                .try_acquire_bundle(home, "repair-gate-old".into(), "endpoint".into())
                .await
                .unwrap()
            else {
                panic!("acquire isolated home")
            };
            let fence = WriteFence::Lease(tokeira_storage::ExecutionHomeFence {
                home,
                owner: "repair-gate-old".into(),
                epoch,
            });
            let lease_change = async {
                if loss == 0 || loss == 3 {
                    assert_eq!(
                        repo.renew_bundle(home, "repair-gate-old".into(), epoch, "endpoint".into())
                            .await
                            .unwrap(),
                        LeaseOutcome::Renewed { epoch }
                    );
                } else {
                    repo.relinquish_bundle(home, "repair-gate-old".into(), epoch)
                        .await
                        .unwrap();
                    if loss == 4 {
                        assert!(matches!(
                            repo.try_acquire_bundle(
                                home,
                                "repair-gate-new".into(),
                                "endpoint".into()
                            )
                            .await
                            .unwrap(),
                            LeaseOutcome::Acquired { .. }
                        ));
                    }
                }
            };
            // Empty-home completion performs no repair writes. Durable loss
            // is paired with cancellation/deadline/replacement at the local
            // gate, rather than mistaking a database read for atomic activation.
            super::repair_tests::final_gate_case(
                repo.clone(),
                home,
                epoch,
                fence,
                loss,
                first,
                lease_change,
            )
            .await;
            for lease in repo
                .list_bundle_leases()
                .await
                .unwrap()
                .into_iter()
                .filter(|lease| lease.bundle_id == home)
            {
                if let Some(owner) = lease.owner_node_id {
                    repo.relinquish_bundle(home, owner, lease.epoch)
                        .await
                        .unwrap();
                }
            }
        });
        cases.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    });
    executor.block_on(director.shutdown()).unwrap();
    result.unwrap();
    assert_eq!(cases.load(std::sync::atomic::Ordering::SeqCst), 100);
}

fn acquisition_activity(
    index: usize,
    queue: &QueueKey,
    scheduled_at: OffsetDateTime,
) -> tokeira_kernel::ActivityState {
    tokeira_kernel::ActivityState {
        activity_id: format!("activity-{index}"),
        activity_type: "work".into(),
        schedule_event_id: index as i64 + 3,
        task_queue: queue.task_queue.clone(),
        deployment: None,
        build_id: None,
        retired_input: Default::default(),
        retired_header: Default::default(),
        attempt: 2,
        retry_policy: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        scheduled_at,
        current_attempt_scheduled_at: Some(scheduled_at + time::Duration::hours(1)),
        started_at: None,
        started_event_id: None,
        last_failure: None,
        started_identity: None,
        retry_last_worker_identity: None,
        heartbeat_details: None,
        cancel_requested: false,
        pause_info: None,
        stamp: 1,
        activity_reset: false,
        reset_heartbeats: false,
        priority: None,
        last_attempt_complete_time: Some(scheduled_at),
        last_heartbeat_at: None,
    }
}

#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database with 4096 homes"]
async fn workflow_dispatch_live_lost_publication_offered_by_next_pass() -> Result<()> {
    live_dispatch_observation(Some(TaskKind::Workflow)).await
}

#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database with 4096 homes"]
async fn activity_dispatch_live_lost_publication_offered_by_next_pass() -> Result<()> {
    live_dispatch_observation(Some(TaskKind::Activity)).await
}

#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database with 4096 homes"]
async fn activity_dispatch_live_backlog_counts_rows_without_pollers() -> Result<()> {
    live_dispatch_observation(None).await
}

async fn live_dispatch_observation(lost: Option<TaskKind>) -> Result<()> {
    let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .context("explicit live dispatch test requires TOKEIRA_DSQL_TEST_DATABASE_URL")?;
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..Default::default()
            },
            shard_count: 4096,
            projection_partition_count: 16,
            ..Default::default()
        },
    )
    .await?;
    let log = store.projection_log().clone();
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    tokeira_storage::prepare_execution_placement(repo.as_ref()).await?;
    let request = super::lost_publication_tests::request();
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
    let item = super::activity_backlog_tests::fixture(&queue, &[1, 1, 0, 5], true);
    let (run_key, workflow_id) = if lost.is_some() {
        (request.run_key, &request.workflow_id)
    } else {
        (item.next_state.run_key, &item.next_state.workflow_id)
    };
    let home = execution_home_bundle(
        queue.namespace_id.0.as_bytes(),
        workflow_id.0.as_bytes(),
        4096,
    );
    let queue_home = tokeira_types::bundle_for_partition(
        tokeira_types::workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 4096),
        4096,
    );
    let mut homes = vec![home, queue_home];
    homes.sort();
    homes.dedup();
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        4096,
        format!("live-dispatch-{}", Uuid::new_v4()),
        "127.0.0.1:0".into(),
        false,
        None,
    ));
    let checking = runtime.clone();
    let checking_homes = homes.clone();
    // Turn assertion panics into errors so every live failure still cleans up.
    let result = tokio::spawn(async move {
        for id in checking_homes {
            checking.acquire_shard(id).await?;
        }
        checking.ready().await?;
        if let Some(kind) = lost {
            super::lost_publication_tests::lost_publication(&checking, request, kind).await?;
        } else {
            let fence = checking
                .shard_owner
                .read()
                .expect("fixture shard owner lock poisoned")
                .write_fence(home)
                .context("fixture execution home is not serving")?;
            let committed = checking
                .repo
                .commit_transition_for_bundle(run_key, home, item, fence)
                .await?;
            anyhow::ensure!(
                matches!(committed, CommitResult::Applied { .. }),
                "seed rejected"
            );
            super::activity_backlog_tests::check_no_poller_count(&checking, &queue).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|result| result);
    runtime.runtime_shutdown.begin_shutdown();
    for id in &homes {
        runtime.relinquish_shard(*id).await;
    }
    let stopped = runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await;
    let mut cleanup = Ok(());
    if stopped.is_ok() {
        for id in homes {
            cleanup = finish_cleanup(
                cleanup,
                release_contract_lease(repo.as_ref(), id, &runtime.owner_identity).await,
                "owned lease release",
            );
        }
        cleanup = finish_cleanup(
            cleanup,
            purge_contract_runs(repo.as_ref(), &log, &[run_key], home).await,
            "owned data cleanup",
        );
    } else {
        cleanup = Err(anyhow!("cleanup deferred until runtime renewal tasks stop"));
    }
    let closed = director.shutdown().await;
    let result = finish_cleanup(result, stopped.map_err(Into::into), "runtime shutdown");
    let result = finish_cleanup(result, cleanup, "fixture cleanup");
    finish_cleanup(result, closed, "connection shutdown")
}

async fn child_release_fixture_apply(
    repo: &dyn RunRepository,
    key: RunKey,
    command: Command,
) -> Result<WorkflowState> {
    let next = BasicKernel.apply(repo.load_run(key).await?, command)?;
    match repo
        .commit_transition(key, next, WriteFence::Unfenced)
        .await?
    {
        CommitResult::Applied { new_state } => Ok(new_state),
        other => anyhow::bail!("child release fixture rejected: {other:?}"),
    }
}

async fn child_release_fixture_complete(
    repo: &dyn RunRepository,
    start: &StartRequest,
    commands: Vec<WorkflowCommand>,
) -> Result<WorkflowState> {
    let LoadedRun::Existing(state) = repo.load_run(start.run_key).await? else {
        anyhow::bail!("parent fixture disappeared before task completion");
    };
    let task = state.pending_workflow_task.context("parent pending task")?;
    let state = child_release_fixture_apply(
        repo,
        start.run_key,
        Command::WorkflowTaskStarted(tokeira_kernel::StartWorkflowTaskRequest {
            logical_seq: task.logical_seq,
            worker_identity: WorkerIdentity("release-fixture".into()),
            request_id: format!("parent-task:{}", task.logical_seq.0),
            history_size_bytes: 0,
            advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
            deployment_transition: None,
            deployment_transition_revision_number: None,
            polled_task_queue: start.task_queue.clone(),
            now: OffsetDateTime::now_utc(),
            target_version_changed_enabled: false,
            target_deployment_version: None,
        }),
    )
    .await?;
    let task = state.pending_workflow_task.context("started parent task")?;
    let state = child_release_fixture_apply(
        repo,
        start.run_key,
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key: start.run_key,
                logical_seq: task.logical_seq,
                started_event_id: task.started_event_id.context("started event")?,
                attempt: task.attempt,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("release-fixture".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: Default::default(),
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands,
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            command_sizes: Vec::new(),
            request: RequestContext::unattributed(OffsetDateTime::now_utc()),
            now: OffsetDateTime::now_utc(),
        }),
    )
    .await?;
    Ok(state)
}

async fn child_release_parent_intents(
    repo: &dyn RunRepository,
    home: ShardId,
    parent: RunKey,
) -> Result<Vec<tokeira_storage::IntentKey>> {
    let mut bound = tokeira_storage::ScanBound::Capture;
    let mut after = None;
    let mut keys = Vec::new();
    loop {
        let page = repo.read_intent_page(home, bound, after, 100).await?;
        if page.rows.is_empty() {
            return Ok(keys);
        }
        after = page.rows.last().map(|row| row.key);
        if let Some(through) = page.through {
            bound = tokeira_storage::ScanBound::Through(through);
        }
        keys.extend(
            page.rows
                .into_iter()
                .filter(|row| row.key.source_run == parent)
                .map(|row| row.key),
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum ChildReleaseCase {
    ParentDeletion,
    AfterCancel,
}

// Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement.
#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
async fn child_release_live_survives_parent_deletion() -> Result<()> {
    child_release_live_case(ChildReleaseCase::ParentDeletion).await
}

// Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement.
#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
async fn child_release_live_intent_after_cancel_retires() -> Result<()> {
    child_release_live_case(ChildReleaseCase::AfterCancel).await
}

async fn child_release_live_case(case: ChildReleaseCase) -> Result<()> {
    let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .context("explicit child release test requires TOKEIRA_DSQL_TEST_DATABASE_URL")?;
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..Default::default()
            },
            shard_count: 8,
            projection_partition_count: 16,
            ..Default::default()
        },
    )
    .await?;
    let log = store.projection_log().clone();
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    tokeira_storage::prepare_execution_placement(repo.as_ref()).await?;
    let mut parent = tests::sample_start_request(None, None);
    parent.run_key = RunKey::derive(parent.namespace_id, &parent.workflow_id, parent.run_id);
    parent.workflow_task_timeout = Duration::minutes(5);
    let home = execution_home_bundle(
        parent.namespace_id.0.as_bytes(),
        parent.workflow_id.0.as_bytes(),
        8,
    );
    let mut child = tests::sample_start_request(None, None);
    child.namespace_id = parent.namespace_id;
    child.workflow_id = (0..)
        .map(|index| WorkflowId(format!("release-child-{index}")))
        .find(|id| {
            execution_home_bundle(child.namespace_id.0.as_bytes(), id.0.as_bytes(), 8) == home
        })
        .expect("fixture child home");
    child.run_key = RunKey::derive(child.namespace_id, &child.workflow_id, child.run_id);
    child.task_queue = TaskQueueName(format!("child-release-{}", child.run_id.0));
    child.workflow_task_timeout = Duration::minutes(5);
    child.parent_run_key = Some(parent.run_key);
    child.parent_run_id = Some(parent.run_id);
    child.parent_namespace_id = Some(parent.namespace_id);
    child.parent_workflow_id = Some(parent.workflow_id.clone());
    let queue = QueueKey {
        namespace_id: child.namespace_id,
        task_queue: child.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let queue_home = tokeira_types::bundle_for_partition(
        tokeira_types::workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    );
    let mut homes = vec![home, queue_home];
    homes.sort();
    homes.dedup();
    let keys = [child.run_key, parent.run_key];
    let setup: Result<()> = async {
        child_release_fixture_apply(
            repo.as_ref(),
            parent.run_key,
            Command::Start(parent.clone()),
        )
        .await?;
        let state = child_release_fixture_complete(
            repo.as_ref(),
            &parent,
            vec![WorkflowCommand::StartChildWorkflow {
                child_workflow_id: child.workflow_id.clone(),
                namespace_id: child.namespace_id,
                namespace: None,
                workflow_type: child.workflow_type.clone(),
                task_queue: child.task_queue.clone(),
                input: Payloads::default(),
                header: None,
                memo: Default::default(),
                search_attributes: Default::default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: child.workflow_task_timeout,
                retry_policy: None,
                cron_schedule: None,
                parent_close_policy: tokeira_kernel::ParentClosePolicy::Abandon,
                reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::RejectDuplicate,
                priority: None,
            }],
        )
        .await?;
        child.parent_initiated_event_id = state.children[&child.workflow_id].initiated_event_id;
        let state = child_release_fixture_apply(
            repo.as_ref(),
            child.run_key,
            Command::Start(child.clone()),
        )
        .await?;
        anyhow::ensure!(
            state.pending_workflow_task.is_none(),
            "child bypassed its gate"
        );
        child_release_fixture_apply(
            repo.as_ref(),
            parent.run_key,
            Command::ChildStartConfirmed(tokeira_kernel::ChildStartConfirmedRequest {
                child_workflow_id: child.workflow_id.clone(),
                initiated_event_id: child.parent_initiated_event_id,
                result: tokeira_kernel::ChildStartResult::Started {
                    child_run_id: child.run_id,
                    workflow_type: child.workflow_type.clone(),
                },
                now: OffsetDateTime::now_utc(),
            }),
        )
        .await?;
        if matches!(case, ChildReleaseCase::AfterCancel) {
            let canceled = child_release_fixture_apply(
                repo.as_ref(),
                child.run_key,
                Command::Cancel(tokeira_kernel::CancelRequest {
                    reason: "client cancels before parent close".into(),
                    external_initiator: None,
                    external_initiated_event_id: 0,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId(format!("child-release-cancel:{}", child.run_key.0)),
                        ..RequestContext::unattributed(OffsetDateTime::now_utc())
                    },
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .await?;
            anyhow::ensure!(
                canceled.cancel_requested
                    && canceled.pending_workflow_task.is_some()
                    && canceled
                        .outbound
                        .as_ref()
                        .and_then(|outbound| outbound.child_gate.as_ref())
                        .is_some_and(|gate| gate.released),
                "cancel did not open the child gate"
            );
        }
        let closed = if matches!(case, ChildReleaseCase::AfterCancel) {
            child_release_fixture_complete(
                repo.as_ref(),
                &parent,
                vec![WorkflowCommand::CompleteWorkflow {
                    result: Payloads::default(),
                }],
            )
            .await?
        } else {
            child_release_fixture_apply(
                repo.as_ref(),
                parent.run_key,
                Command::Terminate(TerminateRequest {
                    reason: "close before release".into(),
                    details: None,
                    identity: "fixture".into(),
                    links: Vec::new(),
                    now: OffsetDateTime::now_utc(),
                    request: RequestContext::unattributed(OffsetDateTime::now_utc()),
                }),
            )
            .await?
        };
        let intents = child_release_parent_intents(repo.as_ref(), home, parent.run_key).await?;
        anyhow::ensure!(
            intents.len() == 1 && intents[0].kind == tokeira_storage::IntentKind::ChildRelease,
            "parent close did not author exactly one release intent"
        );
        if matches!(case, ChildReleaseCase::ParentDeletion) {
            repo.delete_run_for_bundle(
                parent.run_key,
                home,
                DeleteRunRequest {
                    expected_seq: closed.transition_seq,
                    deleted_at: OffsetDateTime::now_utc(),
                },
                WriteFence::Unfenced,
            )
            .await?;
            repo.purge_run(parent.run_key).await?;
            anyhow::ensure!(
                repo.load_outbound_source(parent.run_key).await? == LoadedRun::Existing(closed),
                "release source was purged"
            );
        }
        Ok(())
    }
    .await;
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        format!("live-child-release-{}", Uuid::new_v4()),
        "127.0.0.1:0".into(),
        false,
        None,
    ));
    let result: Result<()> = async {
        setup?;
        for id in &homes {
            runtime.acquire_shard(*id).await?;
        }
        runtime.ready().await?;
        if matches!(case, ChildReleaseCase::ParentDeletion) {
            let activation = runtime
                .poll_workflow_activation(
                    queue,
                    None,
                    WorkerIdentity("child-release-worker".into()),
                    std::time::Duration::from_secs(30),
                )
                .await?;
            let Some(WorkflowActivation::WorkflowTask(task)) = activation else {
                anyhow::bail!("released child's first task did not reach a poll");
            };
            anyhow::ensure!(
                task.token.run_key == child.run_key,
                "wrong child reached poll"
            );
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                while !child_release_parent_intents(repo.as_ref(), home, parent.run_key)
                    .await?
                    .is_empty()
                {
                    tokio::task::yield_now().await;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await
            .context("release intent did not retire before the deadline")??;
            let history = repo.read_history(child.run_key, 0, 100).await?;
            anyhow::ensure!(
                history
                    .iter()
                    .filter(|event| matches!(
                        event.kind,
                        tokeira_kernel::HistoryEventKind::WorkflowTaskScheduled { .. }
                    ))
                    .count()
                    == 1,
                "no-op release scheduled a second workflow task"
            );
        }
        let LoadedRun::Existing(state) = repo.load_run(child.run_key).await? else {
            anyhow::bail!("released child disappeared");
        };
        anyhow::ensure!(
            state
                .outbound
                .as_ref()
                .and_then(|outbound| outbound.child_gate.as_ref())
                .is_some_and(|gate| gate.released),
            "gate still closed"
        );
        Ok(())
    }
    .await;
    runtime.runtime_shutdown.begin_shutdown();
    for id in &homes {
        runtime.relinquish_shard(*id).await;
    }
    let stopped = runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await;
    let cleanup: Result<()> = async {
        anyhow::ensure!(stopped.is_ok(), "cleanup requires stopped renewal tasks");
        for id in homes {
            release_contract_lease(repo.as_ref(), id, &runtime.owner_identity).await?;
        }
        // Close every fixture run before retiring intents, including partial
        // setup failures. The child's future result otherwise pins the parent.
        for key in keys {
            if let LoadedRun::Existing(state) = repo.load_run(key).await?
                && state.is_open()
            {
                child_release_fixture_apply(
                    repo.as_ref(),
                    key,
                    Command::Terminate(TerminateRequest {
                        reason: "fixture cleanup".into(),
                        details: None,
                        identity: "fixture".into(),
                        links: Vec::new(),
                        now: OffsetDateTime::now_utc(),
                        request: RequestContext {
                            request_id: RequestId(format!("child-release-cleanup:{}", key.0)),
                            ..RequestContext::unattributed(OffsetDateTime::now_utc())
                        },
                    }),
                )
                .await?;
            }
        }
        let mut after = None;
        loop {
            let page = repo
                .read_intent_page(home, tokeira_storage::ScanBound::Capture, after, 100)
                .await?;
            if page.rows.is_empty() {
                break;
            }
            after = page.rows.last().map(|row| row.key);
            for row in page
                .rows
                .into_iter()
                .filter(|row| keys.contains(&row.key.source_run))
            {
                repo.retire_intent(
                    tokeira_storage::IntentRetirement {
                        key: row.key,
                        outcome: tokeira_storage::DefinitiveOutcome::Absent,
                    },
                    WriteFence::Unfenced,
                )
                .await?;
            }
        }
        purge_contract_runs(repo.as_ref(), &log, &keys, home).await
    }
    .await;
    let closed = director.shutdown().await;
    let result = finish_cleanup(result, stopped.map_err(Into::into), "runtime shutdown");
    let result = finish_cleanup(result, cleanup, "fixture cleanup");
    finish_cleanup(result, closed, "connection shutdown")
}
