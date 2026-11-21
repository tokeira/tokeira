//! Generated DSQL schedules use the real authenticated transport and shared oracles.
//! Each case retires its runtimes, purges its run and releases only its own leases.

use std::{cell::Cell, collections::BTreeSet, num::NonZeroU32};

use anyhow::{Context, Result};
use proptest::test_runner::{Config, FileFailurePersistence, TestRunner};
use tokeira_kernel::TerminateRequest;
use tokeira_storage::{
    ActivityDiscoveryRange, ActivityDispatchRow, LeaseOutcome,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
};
use tokio::sync::Mutex;

use super::*;

#[derive(Clone, Copy, Debug)]
pub(in crate::peer) enum Suite {
    Recovery,
    Retirement,
    Competing,
    Takeover,
}

async fn rows<R: RunRepository + LeaseRepository + 'static>(
    c: &Cluster<R>,
) -> Vec<ActivityDispatchRow> {
    let page = c
        .store
        .list_activity_dispatch_page(
            &ActivityDiscoveryRange {
                namespace_id: c.queue.namespace_id,
                queue_name: c.queue.task_queue.clone(),
            },
            c.clock.now(),
            None,
            NonZeroU32::new(64).unwrap(),
        )
        .await
        .expect("read canonical activity intent");
    assert!(page.exhausted, "isolated fixture must fit one page");
    page.candidates
        .into_iter()
        .map(|candidate| candidate.row.expect("canonical row"))
        .collect()
}

async fn cleanup<R: RunRepository + LeaseRepository + 'static>(c: Cluster<R>) {
    // A shrunk failure can stop at a transport barrier. Release only this
    // fixture's waiters before joining servers and relinquishing leases.
    for node in &c.nodes {
        for faults in [&node.activity_faults, &node.start_faults, &node.faults] {
            faults.mode.store(0, Ordering::SeqCst);
            faults.release.notify_waiters();
        }
    }
    let home = execution(&c.request);
    let homes = c
        .nodes
        .iter()
        .flat_map(|node| node.runtime.active_shards())
        .chain([home, queue_home(&c.queue)])
        .collect::<BTreeSet<_>>();
    let leases = c.store.list_bundle_leases().await.expect("cleanup leases");
    if let LoadedRun::Existing(state) = c
        .store
        .load_run(c.request.run_key)
        .await
        .expect("cleanup state")
    {
        if state.is_open() {
            let lease = leases
                .iter()
                .find(|lease| lease.bundle_id == home)
                .expect("execution lease");
            let owner = c
                .nodes
                .iter()
                .enumerate()
                .find(|(index, _)| {
                    lease.owner_node_id.as_deref()
                        == Some(format!("activity-node-{index}").as_str())
                })
                .expect("fixture owns execution lease")
                .1;
            let now = c.clock.now();
            owner
                .runtime
                .submit(
                    c.request.run_key,
                    Command::Terminate(TerminateRequest {
                        reason: "fixture cleanup".into(),
                        details: None,
                        identity: "fixture".into(),
                        links: Vec::new(),
                        request: fresh_request(now),
                        now,
                    }),
                )
                .await
                .expect("terminate fixture run");
        }
        let LoadedRun::Existing(closed) = c.store.load_run(c.request.run_key).await.unwrap() else {
            panic!("closed fixture")
        };
        let lease = leases.iter().find(|lease| lease.bundle_id == home).unwrap();
        assert!(matches!(
            c.store
                .delete_run_for_bundle(
                    c.request.run_key,
                    home,
                    tokeira_storage::DeleteRunRequest {
                        expected_seq: closed.transition_seq,
                        deleted_at: c.clock.now()
                    },
                    tokeira_storage::WriteFence::Lease(tokeira_storage::ExecutionHomeFence {
                        home,
                        owner: lease.owner_node_id.clone().unwrap(),
                        epoch: lease.epoch
                    }),
                )
                .await
                .unwrap(),
            tokeira_storage::DeleteRunResult::Deleted { .. }
        ));
        c.store
            .purge_run(c.request.run_key)
            .await
            .expect("purge fixture run");
        assert!(matches!(
            c.store.load_run(c.request.run_key).await.unwrap(),
            LoadedRun::Absent
        ));
    }
    for node in &c.nodes {
        for home in node.runtime.active_shards() {
            node.runtime.relinquish_shard(home).await;
        }
    }
    for lease in leases
        .into_iter()
        .filter(|lease| homes.contains(&lease.bundle_id))
    {
        if let Some(owner) = lease.owner_node_id {
            assert!(matches!(
                c.store
                    .relinquish_bundle(lease.bundle_id, owner, lease.epoch)
                    .await
                    .expect("release fixture lease"),
                LeaseOutcome::Acquired { .. }
            ));
        }
    }
    assert!(
        c.store
            .list_bundle_leases()
            .await
            .unwrap()
            .iter()
            .filter(|lease| homes.contains(&lease.bundle_id))
            .all(|lease| lease.owner_node_id.is_none())
    );
    c.stop().await;
}

// Due hints never consume canonical intent; a finite fault prefix can be
// followed by either prompt recovery publication or compatible discovery.
async fn recovery<R: RunRepository + LeaseRepository + 'static>(
    c: &mut Cluster<R>,
    fault: u8,
    path: u8,
    duplicates: usize,
) {
    if path != 0 {
        c.schedule(path == 1).await;
        c.admitted(0).await;
        if path == 1 {
            let response = poll(c.nodes[1].address, &c.queue).await;
            c.assert_started(0, &response).await;
            let token = serde_json::from_slice(&response.task_token).unwrap();
            c.nodes[1]
                .activity_faults
                .mode
                .store(fault, Ordering::SeqCst);
            c.nodes[0]
                .runtime
                .fail_activity_task(
                    token,
                    Payload::new(Vec::new()),
                    None,
                    false,
                    None,
                    None,
                    fresh_request(c.clock.now()),
                )
                .await
                .unwrap();
        } else {
            c.nodes[1]
                .runtime
                .activity_broker()
                .remove_run(c.request.run_key)
                .await;
            c.nodes[1]
                .activity_faults
                .mode
                .store(fault, Ordering::SeqCst);
            if path == 2 {
                let now = c.clock.now();
                c.nodes[0]
                    .runtime
                    .pause_activities(
                        c.request.run_key,
                        PauseActivityRequest {
                            target: ActivityControlTarget::All,
                            identity: "control".into(),
                            reason: "fixture".into(),
                            request: fresh_request(now),
                            now,
                            rule_id: None,
                        },
                    )
                    .await
                    .unwrap();
                c.nodes[0]
                    .runtime
                    .unpause_activities(
                        c.request.run_key,
                        UnpauseActivitiesRequest {
                            target: ActivityControlTarget::All,
                            reset_attempts: false,
                            reset_heartbeat: false,
                            jitter: None,
                            request: fresh_request(now),
                            now,
                        },
                    )
                    .await
                    .unwrap();
            } else {
                restart_owner(c).await;
            }
        }
    } else {
        c.nodes[1]
            .activity_faults
            .mode
            .store(fault, Ordering::SeqCst);
        c.schedule(false).await;
    }
    let receipt = if fault == 0 {
        Some(c.admitted(0).await)
    } else {
        for _ in 0..if fault == 5 { 1 } else { 3 } {
            let receipt = c.nodes[0].activity_receipt().await;
            assert_eq!(
                receipt.outcome,
                Err(if fault == 5 {
                    ActivityOfferError::Unimplemented
                } else {
                    ActivityOfferError::Transport
                })
            );
        }
        None
    };
    let before = rows(c).await;
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].incarnation.run_key, c.request.run_key);
    let state = c.state().await;
    assert!(state.activities["activity"].started_at.is_none());
    c.discovery_paused();
    if let Some(receipt) = receipt {
        for _ in 0..duplicates {
            assert_eq!(
                client(&c.nodes[1])
                    .await
                    .offer_activity_task(authorized(&c.nodes[0], offer_wire(&receipt)))
                    .await
                    .unwrap()
                    .get_ref()
                    .outcome,
                wire::ActivityOfferOutcome::Known as i32
            );
        }
        assert_eq!(rows(c).await, before);
    } else {
        c.nodes[1]
            .runtime
            .activity_broker()
            .remove_run(c.request.run_key)
            .await;
        assert_eq!(rows(c).await, before);
        c.nodes[1].activity_faults.mode.store(0, Ordering::SeqCst);
        c.nodes[1].repo.activity.release();
    }
    c.assert_started(0, &poll(c.nodes[1].address, &c.queue).await)
        .await;
    c.committed_starts(1 + usize::from(path == 1));
    assert!(rows(c).await.is_empty());
    if fault == 0 {
        c.discovery_paused();
    } else {
        assert!(c.nodes[1].repo.activity.reads.load(Ordering::SeqCst) > 0);
    }
}

fn start_request<R: RunRepository + 'static>(
    node: &Node<R>,
    body: wire::StartActivityTaskRequest,
) -> Request<wire::StartActivityTaskRequest> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "x-tokeira-peer-authorization",
        node.peer
            .signer()
            .activity_start_token_at(node.peer.unix_seconds().unwrap())
            .unwrap()
            .authorization()
            .parse()
            .unwrap(),
    );
    request.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    request
}

// Holding the first loaded start before commit forces a real stale sequence
// against DSQL; only the winning request may replay its committed receipt.
async fn competing<R: RunRepository + LeaseRepository + 'static>(
    c: &mut Cluster<R>,
    warm: bool,
    conflict: bool,
    obsolete: u8,
    duplicates: usize,
) {
    c.schedule(false).await;
    let receipt = c.admitted(0).await;
    if warm {
        let now = c.clock.now();
        c.nodes[0]
            .runtime
            .submit(
                c.request.run_key,
                Command::Signal(SignalRequest {
                    signal_name: "prime-cache".into(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request: fresh_request(now),
                    now,
                }),
            )
            .await
            .unwrap();
    } else {
        restart_owner(c).await;
    }
    let task = receipt.offer.task;
    let first = wire::StartActivityTaskRequest {
        run_key: task.run_key.0.as_bytes().to_vec(),
        activity_id: task.activity_id,
        schedule_event_id: task.schedule_event_id,
        attempt: task.attempt,
        stamp: task.stamp,
        request_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace: "default".into(),
        task_queue: task.queue.task_queue.0,
        deployment: None,
        build_id: None,
        dispatch_revision: task.dispatch_revision,
        worker_identity: "integration-worker".into(),
        scoped_versioned: false,
    };
    let mut obsolete_request = first.clone();
    match obsolete {
        0 => obsolete_request.schedule_event_id -= 1,
        1 => obsolete_request.attempt += 1,
        _ => obsolete_request.stamp += 1,
    }
    let mut rpc = client(&c.nodes[0]).await;
    assert!(
        rpc.start_activity_task(start_request(&c.nodes[1], obsolete_request))
            .await
            .unwrap()
            .get_ref()
            .task
            .is_none()
    );
    let before = c.state().await;
    let reads = Reads::at(c);
    let (entered, resume) = c.nodes[0].repo.pause_commit(c.request.run_key);
    let mut first_client = rpc.clone();
    let request = start_request(&c.nodes[1], first.clone());
    let held = tokio::spawn(async move { first_client.start_activity_task(request).await });
    tokio::time::timeout(BOUND, entered).await.unwrap().unwrap();
    c.nodes[0]
        .repo
        .conflicts
        .store(usize::from(conflict), Ordering::SeqCst);
    let mut winning = first.clone();
    winning.request_id = uuid::Uuid::new_v4().as_bytes().to_vec();
    let winner = rpc
        .start_activity_task(start_request(&c.nodes[1], winning.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(winner.task.is_some());
    resume.send(()).unwrap();
    assert!(held.await.unwrap().unwrap().get_ref().task.is_none());
    let start = Reads::at(c).since(&reads);
    assert_eq!(start.starts, vec![3 + usize::from(conflict), 0, 0]);
    assert_eq!(start.observations, vec![0, 0, 0]);
    assert_eq!(
        c.nodes[0].repo.rejected.load(Ordering::SeqCst),
        usize::from(conflict)
    );
    let after = c.state().await;
    assert_eq!(after.transition_seq.0, before.transition_seq.0 + 1);
    assert_eq!(after.activity_start_receipts.len(), 1);
    assert_eq!(
        after.activity_start_receipts["activity"]
            .request_id
            .as_bytes(),
        winning.request_id.as_slice()
    );
    // Request identity governs repeated success and competing refusal:
    // service/history/api/recordactivitytaskstarted/api.go @ v1.31.0.
    for _ in 0..duplicates {
        let reads = Reads::at(c);
        let replay = rpc
            .start_activity_task(start_request(&c.nodes[1], winning.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(replay.replayed);
        assert_eq!(replay.task, winner.task);
        let replay_reads = Reads::at(c).since(&reads);
        assert_eq!(replay_reads.starts, vec![1, 0, 0]);
        assert_eq!(replay_reads.observations, vec![1, 0, 0]);
    }
    assert_eq!(c.state().await.transition_seq, after.transition_seq);
    assert!(
        rpc.start_activity_task(start_request(&c.nodes[1], first))
            .await
            .unwrap()
            .get_ref()
            .task
            .is_none()
    );
    c.committed_starts(1);
    c.discovery_paused();
}

pub(in crate::peer) fn run(suite: Suite) -> Result<()> {
    let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .context("live activity schedules require an explicitly selected isolated DSQL database")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // Socket refusal is infrastructure, not a persistable schedule witness.
    drop(
        runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .context("live activity schedules require local gRPC sockets")?,
    );
    let store = runtime
        .block_on(DsqlStore::from_database_url_for_tests(
            url,
            DsqlPoolConfig {
                migration: MigrationConfig {
                    migrations_dir: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../tokeira-storage/migrations"),
                },
                reservoir: ReservoirConfig {
                    target_ready: 5,
                    inflight_limit: 2,
                    ..Default::default()
                },
                shard_count: 8,
                ..Default::default()
            },
        ))
        .map_err(|_| anyhow::anyhow!("live activity database connection failed"))?;
    // The operator bootstraps the schema first. Placement preparation must
    // finish before any case acquires a live lease, even on repeated selections.
    runtime.block_on(tokeira_storage::prepare_execution_placement(
        store.run_repository(),
    ))?;
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    let mut runner = TestRunner::new(Config {
        cases: 100,
        source_file: Some(file!()),
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    });
    let completed = Cell::new(0);
    let result = runner.run(
        &(
            100u32..10000,
            0u8..4,
            0u8..4,
            any::<bool>(),
            any::<bool>(),
            1usize..5,
        ),
        |(index, fault, path, warm, conflict, duplicates)| {
            runtime.block_on(async {
                let c = Arc::new(Mutex::new(
                    Cluster::with_store(
                        repo.clone(),
                        TestClock::at(time::OffsetDateTime::now_utc()),
                        index,
                        3,
                    )
                    .await,
                ));
                let exercised = c.clone();
                let result = tokio::spawn(async move {
                    let mut c = exercised.lock().await;
                    match suite {
                        Suite::Recovery => {
                            recovery(&mut c, [0, 1, 2, 5][usize::from(fault)], path, duplicates)
                                .await
                        }
                        Suite::Retirement if warm => retirement_oracle(&mut c).await,
                        Suite::Retirement => queue_movement_oracle(&mut c, duplicates).await,
                        Suite::Competing => {
                            competing(&mut c, warm, conflict, path % 3, duplicates).await
                        }
                        Suite::Takeover => {
                            execution_movement_oracle(&mut c, warm, duplicates).await
                        }
                    }
                })
                .await;
                let c = Arc::try_unwrap(c)
                    .ok()
                    .expect("case released its cluster")
                    .into_inner();
                cleanup(c).await;
                result.map_err(|error| {
                    TestCaseError::fail(format!("{suite:?} schedule failed: {error}"))
                })
            })?;
            completed.set(completed.get() + 1);
            Ok(())
        },
    );
    runtime.block_on(director.shutdown())?;
    result.map_err(|error| anyhow::anyhow!("{error}"))?;
    assert!(completed.get() >= 100);
    Ok(())
}
