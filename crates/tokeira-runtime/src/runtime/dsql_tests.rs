//! Opt-in runtime acquisition checks; the storage live suite bootstraps schema first.

use super::*;
use crate::{
    activity_timeout::ActivityTrackingEntry,
    discovery::tests::transition,
    nexus::{CompletionCallbackTrackingEntry, NexusTimeoutEntry},
};
use std::path::Path;
use tokeira_storage::{
    WriteFence,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
};

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
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
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
    tokeira_storage::prepare_execution_placement(repo.as_ref()).await?;
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
    let (entered, resume) = runtime.wft_timeout_tracking.pause_next_recovery();
    let acquiring = runtime.clone();
    let first = tokio::spawn(async move { acquiring.acquire_shard(execution_home).await });
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
    runtime.relinquish_shard(execution_home).await;
    runtime.runtime_shutdown.begin_shutdown();
    runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await?;
    director.shutdown().await?;
    Ok(())
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
