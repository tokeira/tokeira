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
            .unwrap();
        item.next_state.started_at = clock;
        item.next_state.workflow_run_timeout = Some(time::Duration::hours(1));
        item.next_state.workflow_execution_timeout = None;
        item.next_state.workflow_task_timeout = time::Duration::hours(1);
        let task = item.next_state.pending_workflow_task.as_mut().unwrap();
        task.started_at = Some(clock);
        task.started_event_id = Some(2);
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
        BacklogConfig::default(),
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
                .unwrap()
                .acquisition(home)
                .context("missing acquiring generation")?;
            anyhow::ensure!(
                !runtime
                    .shard_owner
                    .read()
                    .unwrap()
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
            .unwrap()
            .acquisition(home)
            .context("missing active generation")?;
        anyhow::ensure!(
            runtime
                .shard_owner
                .read()
                .unwrap()
                .acquisition_active(&current),
            "completed acquisition is not active"
        );
        let mut workflows = runtime.workflow_timeout_tracking.snapshot_for_shard(home);
        workflows.sort_by_key(|entry| entry.run_key);
        let mut tasks = runtime.wft_timeout_tracking.snapshot_for_shard(home);
        tasks.sort_by_key(|entry| entry.run_key);
        anyhow::ensure!(
            workflows.len() == expected.len() && tasks.len() == expected.len(),
            "acquisition lost recovered deadlines"
        );
        for ((workflow, task), state) in workflows.iter().zip(&tasks).zip(&expected) {
            let pending = state.pending_workflow_task.as_ref().unwrap();
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
        BacklogConfig::default(),
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
