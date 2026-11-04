//! Disposable-cluster recovery checks through real DSQL transactions. The full
//! generated case counts run in memory; these exercise required envelopes,
//! interrupted staging, live fences and physical retention on DSQL itself.

use super::{bulk_write_tests::Fixture, *};
use crate::{
    DefinitiveOutcome, DeliveryGuard, ExecutionHomeFence, IntentKind, IntentRetirement,
    PublicationResult, RecoveryError, RunPreparation, ScanBound,
    bulk_write_tests::{Backend, OwnedRows},
    dsql::value_parts,
    memory::projection_accumulator_tests::{applied, fresh_transition},
    outbound_upgrade::{
        OUTBOUND_UPGRADE, OutboundUpgradeProgress, apply_outbound_recovery, classify,
        plan_outbound_recovery,
    },
};
use tokeira_kernel::{
    BasicKernel, Command, HistoryEventKind, Kernel, OperationKind, PendingExternalSignal,
    TerminateRequest,
};
use tokeira_types::{Payload, Payloads, RequestContext};

fn close(state: &WorkflowState) -> Transition {
    BasicKernel
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::Terminate(TerminateRequest {
                reason: "live fixture".into(),
                details: None,
                identity: "live-test".into(),
                links: Vec::new(),
                request: RequestContext::unattributed(state.started_at),
                now: state.started_at,
            }),
        )
        .expect("live fixture terminates an open run")
}

async fn delete_and_purge(repo: &dyn RunRepository, state: &WorkflowState) -> Result<()> {
    repo.delete_run_for_bundle(
        state.run_key,
        ShardId(0),
        DeleteRunRequest {
            expected_seq: state.transition_seq,
            deleted_at: state.started_at,
        },
        WriteFence::Unfenced,
    )
    .await?;
    repo.purge_run(state.run_key).await?;
    assert_eq!(repo.load_run(state.run_key).await?, LoadedRun::Absent);
    Ok(())
}

#[tokio::test]
async fn outbound_live_original_fences_and_retained_destination_proof() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    for (source_epoch, target_epoch) in [(1, 2), (2, 1), (2, 2)] {
        let mut initial = fresh_transition(RunKey::new());
        let target_id = WorkflowId(format!("target-{}", initial.next_state.run_key.0));
        let initiation = initial.next_state.last_event_id + 1;
        initial.next_state.pending_external_signals.insert(
            initiation,
            PendingExternalSignal {
                initiated_event_id: initiation,
                target_namespace_id: initial.next_state.namespace_id,
                target_namespace: None,
                target_workflow_id: target_id.clone(),
                target_run_id: None,
                signal_name: "signal".into(),
            },
        );
        initial.history_events.push(HistoryEvent {
            event_id: initiation,
            happened_at: initial.next_state.started_at,
            kind: HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                workflow_task_completed_event_id: 0,
                namespace_id: initial.next_state.namespace_id,
                namespace: None,
                target_workflow_id: target_id.clone(),
                target_run_id: None,
                signal_name: "signal".into(),
                input: Payloads::default(),
                header: None,
                control: String::new(),
            },
        });
        initial.event_principals.push(None);
        initial.next_state.last_event_id = initiation;
        let source = applied(
            repo.commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                .await?,
        );
        let row = crate::derive_operations(&source, ShardId(0))?.remove(0);
        assert_eq!(row.incarnation.key.kind, OperationKind::ExternalSignal);
        let mut target = fresh_transition(RunKey::new());
        target.next_state.namespace_id = source.namespace_id;
        target.next_state.workflow_id = target_id;
        let target = applied(
            repo.commit_transition(target.next_state.run_key, target, WriteFence::Unfenced)
                .await?,
        );
        sqlx::query("INSERT INTO shard_lease (shard_id,owner,epoch,lease_expiry,node_endpoint) VALUES ($1,'outbound-live',2,now()+interval '1 hour','http://localhost') ON CONFLICT (shard_id) DO UPDATE SET owner=EXCLUDED.owner,epoch=EXCLUDED.epoch,lease_expiry=EXCLUDED.lease_expiry")
            .bind(DsqlRunRepository::shard_id_to_uuid(ShardId(0))).execute(&fixture.pool).await?;
        let fence = |epoch| {
            WriteFence::Lease(ExecutionHomeFence {
                home: ShardId(0),
                owner: "outbound-live".into(),
                epoch: ShardEpoch(epoch),
            })
        };
        let request_id = RequestId(format!("signal-{}", source.run_key.0));
        let mut request = RequestContext::unattributed(target.started_at);
        request.request_id = request_id.clone();
        let signal = BasicKernel.apply(
            LoadedRun::Existing(target.clone()),
            Command::Signal(tokeira_kernel::SignalRequest {
                signal_name: "signal".into(),
                input: Payloads(vec![Payload::new(vec![9; 1100 * 1024])]),
                header: None,
                links: Vec::new(),
                request,
                now: target.started_at,
            }),
        )?;
        let guard = DeliveryGuard::Operation {
            incarnation: row.incarnation,
            source_seq: source.transition_seq,
        };
        let before = repo.read_history(target.run_key, 0, 100).await?;
        let delivered = repo
            .commit_outbound_transition(
                target.run_key,
                signal,
                guard,
                fence(source_epoch),
                fence(target_epoch),
            )
            .await;
        if source_epoch == 1 || target_epoch == 1 {
            assert!(
                delivered
                    .unwrap_err()
                    .downcast_ref::<LeaseFenceLost>()
                    .is_some()
            );
            assert_eq!(
                repo.load_run(target.run_key).await?,
                LoadedRun::Existing(target.clone())
            );
            assert_eq!(repo.read_history(target.run_key, 0, 100).await?, before);
            applied(
                repo.commit_transition(source.run_key, close(&source), WriteFence::Unfenced)
                    .await?,
            );
        } else {
            let target = applied(delivered?);
            let target = applied(
                repo.commit_transition(target.run_key, close(&target), WriteFence::Unfenced)
                    .await?,
            );
            delete_and_purge(repo, &target).await?;
            let execution = ExecutionRef {
                namespace_id: target.namespace_id,
                workflow_id: target.workflow_id.clone(),
                run_id: Some(target.run_id),
            };
            assert_eq!(
                repo.lookup_request_dedupe(&execution, &request_id)
                    .await?
                    .unwrap()
                    .outbound_origin
                    .unwrap()
                    .incarnation,
                row.incarnation
            );
            let source = applied(
                repo.commit_transition(source.run_key, close(&source), WriteFence::Unfenced)
                    .await?,
            );
            repo.purge_run(target.run_key).await?;
            assert!(
                repo.lookup_request_dedupe(&execution, &request_id)
                    .await?
                    .is_none()
            );
            assert_eq!(
                repo.load_outbound_source(target.run_key).await?,
                LoadedRun::Absent
            );
            let error = repo
                .commit_outbound_transition(
                    target.run_key,
                    fresh_transition(target.run_key),
                    guard,
                    WriteFence::Unfenced,
                    WriteFence::Unfenced,
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<RecoveryError>(),
                Some(RecoveryError::Obsolete)
            ));
            assert!(!source.is_open());
        }
    }
    sqlx::query("DELETE FROM shard_lease WHERE shard_id=$1 AND owner='outbound-live'")
        .bind(DsqlRunRepository::shard_id_to_uuid(ShardId(0)))
        .execute(&fixture.pool)
        .await?;
    fixture.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn outbound_live_future_child_result_retains_deleted_parent() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let initial = fresh_transition(RunKey::new());
    let parent = applied(
        repo.commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
            .await?,
    );
    let mut child = fresh_transition(RunKey::new());
    child.next_state.parent_run_key = Some(parent.run_key);
    child.next_state.parent_namespace_id = Some(parent.namespace_id);
    child.next_state.parent_workflow_id = Some(parent.workflow_id.clone());
    child.next_state.parent_run_id = Some(parent.run_id);
    child.next_state.parent_initiated_event_id = 4;
    let child = applied(
        repo.commit_transition(child.next_state.run_key, child, WriteFence::Unfenced)
            .await?,
    );
    let parent = applied(
        repo.commit_transition(parent.run_key, close(&parent), WriteFence::Unfenced)
            .await?,
    );
    delete_and_purge(repo, &parent).await?;
    assert_eq!(
        repo.load_outbound_source(parent.run_key).await?,
        LoadedRun::Existing(parent.clone())
    );
    let child = applied(
        repo.commit_transition(child.run_key, close(&child), WriteFence::Unfenced)
            .await?,
    );
    repo.purge_run(parent.run_key).await?;
    assert_eq!(
        repo.load_outbound_source(parent.run_key).await?,
        LoadedRun::Existing(parent.clone())
    );
    let key = repo
        .read_intent_page(ShardId(0), ScanBound::Capture, None, 100)
        .await?
        .rows
        .into_iter()
        .find(|row| row.key.source_run == child.run_key && row.key.kind == IntentKind::ChildResult)
        .unwrap()
        .key;
    assert!(
        repo.retire_intent(
            IntentRetirement {
                key,
                outcome: DefinitiveOutcome::Absent
            },
            WriteFence::Unfenced
        )
        .await?
    );
    repo.purge_run(parent.run_key).await?;
    assert_eq!(
        repo.load_outbound_source(parent.run_key).await?,
        LoadedRun::Absent
    );
    fixture.store.shutdown().await?;
    Ok(())
}

// Feature: outbound-dispatch-recovery, Property 19: Retention cannot erase outstanding proof.
#[tokio::test]
async fn outbound_live_owed_release_survives_parent_deletion() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let mut initial = fresh_transition(RunKey::new());
    let release = tokeira_kernel::ChildReleaseState {
        namespace_id: initial.next_state.namespace_id,
        workflow_id: WorkflowId(format!("owed-child-{}", initial.next_state.run_key.0)),
        initiating_event: 4,
        child_run_id: RunId::new(),
        parent_started_event_id: Some(5),
        parent_close_policy: tokeira_kernel::ParentClosePolicy::Abandon,
    };
    initial
        .next_state
        .outbound
        .as_mut()
        .unwrap()
        .child_releases
        .insert(release.initiating_event, release.clone());
    let parent = applied(
        repo.commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
            .await?,
    );
    let parent = applied(
        repo.commit_transition(parent.run_key, close(&parent), WriteFence::Unfenced)
            .await?,
    );
    let intents: Vec<_> = repo
        .read_intent_page(ShardId(0), ScanBound::Capture, None, 100)
        .await?
        .rows
        .into_iter()
        .filter(|row| row.key.source_run == parent.run_key)
        .collect();
    assert_eq!(intents.len(), 1);
    let key = intents[0].key;
    assert_eq!(key.kind, IntentKind::ChildRelease);
    assert_eq!(
        repo.load_intent(key).await?.unwrap().payload,
        crate::IntentPayload::ChildRelease { release }
    );
    assert!(
        repo.read_operation_page(
            ShardId(0),
            OperationKind::ChildRelease,
            ScanBound::Capture,
            None,
            100
        )
        .await?
        .rows
        .iter()
        .all(|row| row.incarnation.key.source_run != parent.run_key)
    );
    delete_and_purge(repo, &parent).await?;
    assert_eq!(
        repo.load_outbound_source(parent.run_key).await?,
        LoadedRun::Existing(parent.clone())
    );
    assert!(
        repo.read_intent_page(ShardId(0), ScanBound::Capture, None, 100)
            .await?
            .rows
            .iter()
            .any(|row| row.key == key)
    );
    assert!(
        repo.retire_intent(
            IntentRetirement {
                key,
                outcome: DefinitiveOutcome::Absent,
            },
            WriteFence::Unfenced
        )
        .await?
    );
    repo.purge_run(parent.run_key).await?;
    assert_eq!(
        repo.load_outbound_source(parent.run_key).await?,
        LoadedRun::Absent
    );
    fixture.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn outbound_live_successor_reservation_and_interrupted_publication() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let mut initial = crate::atomic_successors_test_support::start();
    initial.next_state.run_key = RunKey::derive(
        initial.next_state.namespace_id,
        &initial.next_state.workflow_id,
        initial.next_state.run_id,
    );
    let source = applied(
        repo.commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
            .await?,
    );
    let (closing, mut successor) = crate::atomic_successors_test_support::pair(
        &source,
        0,
        Payloads::default(),
        Default::default(),
        None,
    );
    successor.next_state.run_key = RunKey::derive(
        source.namespace_id,
        &source.workflow_id,
        successor.next_state.run_id,
    );
    let request =
        crate::memory::successor_tests::start_request(&successor.next_state, &source, 1100 * 1024);
    let successor = BasicKernel.apply(LoadedRun::Absent, Command::Start(request.clone()))?;
    let closed = applied(
        repo.commit_successor_fallback(
            source.run_key,
            closing,
            Box::new(request.clone()),
            WriteFence::Unfenced,
        )
        .await?,
    );
    let key = repo
        .read_intent_page(ShardId(0), ScanBound::Capture, None, 100)
        .await?
        .rows
        .into_iter()
        .find(|row| row.key.source_run == source.run_key && row.key.kind == IntentKind::Successor)
        .unwrap()
        .key;
    assert!(
        repo.retire_intent(
            IntentRetirement {
                key,
                outcome: DefinitiveOutcome::Absent
            },
            WriteFence::Unfenced
        )
        .await
        .is_err()
    );
    let mut competing = request.clone();
    competing.run_id = RunId::new();
    competing.run_key = RunKey::derive(
        competing.namespace_id,
        &competing.workflow_id,
        competing.run_id,
    );
    let transition = BasicKernel.apply(LoadedRun::Absent, Command::Start(competing.clone()))?;
    let error = repo
        .commit_transition(competing.run_key, transition, WriteFence::Unfenced)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RecoveryError>(),
        Some(RecoveryError::SuccessorReserved { .. })
    ));
    let manifest = RunPreparation {
        request_key: key.storage_id(),
        expected_current: Some(source.run_key),
        participants: Vec::new(),
        new_run: Box::new(successor),
        successor_intent: Some(key),
    };
    let token = repo
        .prepare_run_publication(Box::new(manifest.clone()), WriteFence::Unfenced)
        .await?;
    assert_eq!(repo.load_run(request.run_key).await?, LoadedRun::Absent);
    assert_eq!(
        repo.load_run(source.run_key).await?,
        LoadedRun::Existing(closed)
    );
    assert!(repo.read_history(request.run_key, 0, 100).await?.is_empty());
    assert_eq!(
        repo.prepare_run_publication(Box::new(manifest), WriteFence::Unfenced)
            .await?,
        token
    );
    let (kind, value_key, part_no, data): (i16, i64, i32, Vec<u8>) = sqlx::query_as(
        "SELECT value_kind,value_key,part_no,data FROM run_value_part WHERE run_key=$1 AND value_kind=2 ORDER BY value_key,part_no LIMIT 1"
    ).bind(request.run_key.0).fetch_one(&fixture.pool).await?;
    assert!(data.len() <= value_parts::PART_BYTES);
    sqlx::query("DELETE FROM run_value_part WHERE run_key=$1 AND value_kind=$2 AND value_key=$3 AND part_no=$4")
        .bind(request.run_key.0).bind(kind).bind(value_key).bind(part_no).execute(&fixture.pool).await?;
    assert!(
        repo.publish_prepared_run(token.clone(), WriteFence::Unfenced)
            .await
            .is_err()
    );
    assert_eq!(repo.load_run(request.run_key).await?, LoadedRun::Absent);
    assert!(repo.load_intent(key).await?.is_some());
    sqlx::query("INSERT INTO run_value_part (run_key,value_kind,value_key,part_no,data) VALUES ($1,$2,$3,$4,$5)")
        .bind(request.run_key.0).bind(kind).bind(value_key).bind(part_no).bind(data).execute(&fixture.pool).await?;
    assert!(matches!(
        repo.publish_prepared_run(token.clone(), WriteFence::Unfenced)
            .await?,
        PublicationResult::Applied { .. }
    ));
    assert!(matches!(
        repo.publish_prepared_run(token.clone(), WriteFence::Unfenced)
            .await?,
        PublicationResult::AlreadyPublished { .. }
    ));
    assert!(repo.load_intent(key).await?.is_none());
    assert_eq!(
        repo.find_latest_run(source.namespace_id, &source.workflow_id)
            .await?,
        Some(request.run_key)
    );
    assert!(
        repo.complete_run_preparation(token.clone(), WriteFence::Unfenced)
            .await?
    );
    assert!(matches!(
        repo.publish_prepared_run(token, WriteFence::Unfenced)
            .await?,
        PublicationResult::AlreadyPublished { .. }
    ));
    fixture.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn outbound_live_reset_conflict_preserves_all_participants() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let mut initial = crate::atomic_successors_test_support::start();
    initial.next_state.run_key = RunKey::derive(
        initial.next_state.namespace_id,
        &initial.next_state.workflow_id,
        initial.next_state.run_id,
    );
    let source = applied(
        repo.commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
            .await?,
    );
    let (closing, mut successor) = crate::atomic_successors_test_support::pair(
        &source,
        0,
        Payloads(vec![Payload::new(vec![5; 1100 * 1024])]),
        Default::default(),
        None,
    );
    successor.next_state.run_key = RunKey::derive(
        source.namespace_id,
        &source.workflow_id,
        successor.next_state.run_id,
    );
    let target = successor.next_state.run_key;
    let token = repo
        .prepare_run_publication(
            Box::new(RunPreparation {
                request_key: format!("reset-{}", source.run_key.0),
                expected_current: Some(source.run_key),
                participants: vec![closing],
                new_run: Box::new(successor),
                successor_intent: None,
            }),
            WriteFence::Unfenced,
        )
        .await?;
    assert_eq!(repo.load_run(target).await?, LoadedRun::Absent);
    let signal = BasicKernel.apply(
        LoadedRun::Existing(source.clone()),
        Command::Signal(tokeira_kernel::SignalRequest {
            signal_name: "racing write".into(),
            input: Payloads::default(),
            header: None,
            links: Vec::new(),
            request: RequestContext::unattributed(source.started_at),
            now: source.started_at,
        }),
    )?;
    let changed = applied(
        repo.commit_transition(source.run_key, signal, WriteFence::Unfenced)
            .await?,
    );
    let before = repo.read_history(source.run_key, 0, 100).await?;
    assert!(matches!(
        repo.publish_prepared_run(token.clone(), WriteFence::Unfenced)
            .await?,
        PublicationResult::Conflict { .. }
    ));
    assert_eq!(
        repo.load_run(source.run_key).await?,
        LoadedRun::Existing(changed)
    );
    assert_eq!(repo.load_run(target).await?, LoadedRun::Absent);
    assert_eq!(repo.read_history(source.run_key, 0, 100).await?, before);
    assert_eq!(
        repo.find_latest_run(source.namespace_id, &source.workflow_id)
            .await?,
        Some(source.run_key)
    );
    // Publication stages one history header today. Extend the abandoned
    // fixture directly so cleanup must also handle keys across decimal widths.
    let mut tx = fixture.pool.begin().await?;
    for event_id in 3..=122 {
        let events = [tokeira_kernel::HistoryEvent {
            event_id,
            happened_at: source.started_at,
            kind: crate::bulk_write_tests::signal(event_id as usize, 16),
        }];
        sqlx::query("INSERT INTO history_batch (run_key,first_event_id,last_event_id,transition_seq,events_data,created_at) VALUES ($1,$2,$2,1,$3,now())")
            .bind(target.0).bind(event_id).bind(codec::encode_history_events(&events)?)
            .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let keys: Vec<i64> = sqlx::query_scalar(
        "SELECT first_event_id FROM history_batch WHERE run_key=$1 ORDER BY first_event_id",
    )
    .bind(target.0)
    .fetch_all(&fixture.pool)
    .await?;
    assert!(keys.contains(&9) && keys.contains(&10) && keys.contains(&99) && keys.contains(&100));
    assert!(
        repo.abandon_prepared_run(token, WriteFence::Unfenced)
            .await?
    );
    assert_eq!(fixture.owned_rows(target).await?, OwnedRows::default());
    let records: i64 = sqlx::query_scalar("SELECT count(*) FROM run_bulk_write WHERE run_key=$1")
        .bind(target.0)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(records, 0);
    fixture.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn outbound_live_interrupted_large_legacy_cutover() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let start = fresh_transition(RunKey::new());
    let mut source = applied(
        repo.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced)
            .await?,
    );
    source.outbound = None;
    source.last_completion_result =
        Some(Payloads(vec![Payload::new(vec![7; 4 * 1024 * 1024 + 17])]));
    let first = source.last_event_id + 1;
    let mut events = Vec::new();
    for index in 0..33 {
        let event_id = first + index;
        source.pending_external_signals.insert(
            event_id,
            PendingExternalSignal {
                initiated_event_id: event_id,
                target_namespace_id: source.namespace_id,
                target_namespace: None,
                target_workflow_id: WorkflowId("destination".into()),
                target_run_id: None,
                signal_name: "signal".into(),
            },
        );
        events.push(HistoryEvent {
            event_id,
            happened_at: source.started_at,
            kind: HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                workflow_task_completed_event_id: 0,
                namespace_id: source.namespace_id,
                namespace: None,
                target_workflow_id: WorkflowId("destination".into()),
                target_run_id: None,
                signal_name: "signal".into(),
                input: Payloads::default(),
                header: None,
                control: "original".into(),
            },
        });
    }
    source.last_event_id = events.last().expect("fixture events").event_id;
    let bytes = crate::codec::encode_workflow_state_as_previous_release(&source)?;
    let encoded_events = codec::encode_history_events(&events)?;
    let split = value_parts::split(&bytes);
    let mut tx = fixture.pool.begin().await?;
    run_value_parts::write_parts(
        &mut tx,
        source.run_key,
        run_value_parts::RunValueKind::State,
        run_value_parts::STATE_VALUE_KEY,
        &split,
        run_value_parts::OnConflict::Replace,
    )
    .await?;
    sqlx::query("UPDATE workflow_hot SET state_data=$2,state_parts=$3,history_size_bytes=coalesce(history_size_bytes,0)+$4 WHERE run_key=$1")
        .bind(source.run_key.0)
        .bind(split.first)
        .bind(split.count)
        .bind(i64::try_from(encoded_events.len())?)
        .execute(&mut *tx)
        .await?;
    commit::write_history_batch(
        &mut tx,
        source.run_key,
        first,
        source.last_event_id,
        source.transition_seq,
        &encoded_events,
        None,
        run_value_parts::OnConflict::Replace,
    )
    .await?;
    tx.commit().await?;
    // This fixture is the only writer on the explicitly selected disposable DB.
    sqlx::query("DELETE FROM outbound_recovery_upgrade WHERE name=$1")
        .bind(OUTBOUND_UPGRADE)
        .execute(&fixture.pool)
        .await?;
    assert!(repo.ensure_outbound_prepared().await.is_err());
    let plan = plan_outbound_recovery(repo, &[]).await?;
    assert_eq!(plan.unresolved_count, 0);
    assert!(plan.legacy >= 1 && plan.pending >= 33);
    assert!(
        apply_outbound_recovery(repo, &"0".repeat(64), true, &[])
            .await
            .is_err()
    );
    let mut progress = OutboundUpgradeProgress {
        revision: 1,
        plan_digest: plan.digest.clone(),
        repairs_digest: crate::schema_contract::sha256_hex(&postcard::to_allocvec(&Vec::<
            crate::outbound_upgrade::OutboundRepair,
        >::new(
        ))?),
        ..Default::default()
    };
    repo.commit_outbound_upgrade_page(None, progress.clone(), None)
        .await?;
    while let Some(candidate) = repo.read_outbound_upgrade_run(progress.after).await? {
        let (write, issues) = classify(repo, candidate, &[]).await?;
        assert!(issues.is_empty());
        let mut next = progress.clone();
        next.revision += 1;
        next.examined += 1;
        next.after = Some(write.state.run_key);
        next.historical_unknown += u64::from(write.historical_unknown);
        if write.state.run_key == source.run_key {
            let mut interrupted = next.clone();
            interrupted.revision += 1;
            assert!(
                repo.commit_outbound_upgrade_page(Some(progress.clone()), interrupted, Some(write))
                    .await
                    .is_err()
            );
            assert!(
                matches!(repo.load_run(source.run_key).await?, LoadedRun::Existing(state) if state.outbound.is_none())
            );
            let staged: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM outbound_upgrade_state_part WHERE run_key=$1",
            )
            .bind(source.run_key.0)
            .fetch_one(&fixture.pool)
            .await?;
            assert!(staged > 4);
            break;
        }
        repo.commit_outbound_upgrade_page(Some(progress), next.clone(), Some(write))
            .await?;
        progress = next;
    }
    assert!(repo.ensure_outbound_prepared().await.is_err());
    assert!(
        apply_outbound_recovery(repo, &plan.digest, true, &[])
            .await?
            .complete
    );
    repo.ensure_outbound_prepared().await?;
    let LoadedRun::Existing(prepared) = repo.load_run(source.run_key).await? else {
        panic!("prepared source missing");
    };
    assert_eq!(
        prepared.last_completion_result,
        source.last_completion_result
    );
    assert_eq!(
        prepared.pending_external_signals,
        source.pending_external_signals
    );
    let mut cursor = None;
    let mut rediscovered = false;
    loop {
        let page = repo
            .list_recovery_candidates_for_shard(ShardId(0), cursor.as_ref(), 32)
            .await?;
        for state in page.states {
            if state.run_key == source.run_key {
                assert_eq!(state.last_completion_result, source.last_completion_result);
                assert_eq!(
                    state.pending_external_signals,
                    source.pending_external_signals
                );
                rediscovered = true;
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        rediscovered,
        "prepared large state must remain a recovery candidate"
    );
    assert_eq!(
        repo.read_history(source.run_key, first - 1, 100).await?,
        events
    );
    let pointer: Vec<u8> =
        sqlx::query_scalar("SELECT state_data FROM workflow_hot WHERE run_key=$1")
            .bind(source.run_key.0)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(pointer.starts_with(b"TKUP\x01"));
    let mut malformed = pointer.clone();
    malformed[4] = 2;
    sqlx::query("UPDATE workflow_hot SET state_data=$2 WHERE run_key=$1")
        .bind(source.run_key.0)
        .bind(malformed)
        .execute(&fixture.pool)
        .await?;
    assert!(repo.load_run(source.run_key).await.is_err());
    sqlx::query("UPDATE workflow_hot SET state_data=$2 WHERE run_key=$1")
        .bind(source.run_key.0)
        .bind(pointer)
        .execute(&fixture.pool)
        .await?;
    let ending = BasicKernel.apply(
        LoadedRun::Existing(prepared.clone()),
        Command::Terminate(TerminateRequest {
            reason: "fixture cleanup".into(),
            details: None,
            identity: "live-test".into(),
            links: Vec::new(),
            request: RequestContext::unattributed(prepared.started_at),
            now: prepared.started_at,
        }),
    )?;
    let closed = applied(
        repo.commit_transition(prepared.run_key, ending, WriteFence::Unfenced)
            .await?,
    );
    let home = tokeira_types::execution_home_bundle(
        closed.namespace_id.0.as_bytes(),
        closed.workflow_id.0.as_bytes(),
        1,
    );
    repo.delete_run_for_bundle(
        closed.run_key,
        home,
        DeleteRunRequest {
            expected_seq: closed.transition_seq,
            deleted_at: closed.started_at,
        },
        WriteFence::Unfenced,
    )
    .await?;
    repo.purge_run(closed.run_key).await?;
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbound_upgrade_state_part WHERE run_key=$1")
            .bind(closed.run_key.0)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(remaining, 0);
    fixture.store.shutdown().await?;
    Ok(())
}

async fn pending_child_limit_parent(repo: &dyn RunRepository) -> Result<WorkflowState> {
    let mut initial = fresh_transition(RunKey::new());
    for i in 0..2_000 {
        let workflow_id = WorkflowId(format!("pending-{}-{i}", initial.next_state.run_key.0));
        initial.next_state.children.insert(
            workflow_id.clone(),
            tokeira_kernel::ChildWorkflowState {
                child_workflow_id: workflow_id,
                namespace_id: initial.next_state.namespace_id,
                namespace: None,
                workflow_type: tokeira_types::WorkflowType("child".into()),
                header: None,
                child_run_id: None,
                initiated_event_id: i + 10,
                started_event_id: None,
                parent_close_policy: tokeira_kernel::ParentClosePolicy::Terminate,
            },
        );
    }
    let parent = applied(
        repo.commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
            .await?,
    );
    assert_eq!(crate::derive_operations(&parent, ShardId(0))?.len(), 2_000);
    Ok(parent)
}

async fn retire_pending_child_limit_intents(
    repo: &dyn RunRepository,
    parent: &WorkflowState,
) -> Result<()> {
    let mut bound = ScanBound::Capture;
    let mut after = None;
    loop {
        let page = repo.read_intent_page(ShardId(0), bound, after, 100).await?;
        let Some(through) = page.through else {
            break;
        };
        bound = ScanBound::Through(through);
        if page.rows.is_empty() {
            break;
        }
        for descriptor in page.rows {
            after = Some(descriptor.key);
            if descriptor.key.source_run != parent.run_key {
                continue;
            }
            // This fixture never creates a child, so all its obligations have a
            // definitive absent destination, including an unexpanded batch.
            repo.retire_intent(
                IntentRetirement {
                    key: descriptor.key,
                    outcome: DefinitiveOutcome::Absent,
                },
                WriteFence::Unfenced,
            )
            .await?;
        }
    }
    delete_and_purge(repo, parent).await
}

// Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
#[tokio::test]
async fn outbound_live_close_at_pending_child_limit_commits() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let parent = pending_child_limit_parent(repo).await?;
    // Keep this regression on the baseline's APIs so it can demonstrate SQLSTATE
    // 54000 before the fix, with the same 2,000 ChildStart rows and close.
    let closed = applied(
        repo.commit_transition(parent.run_key, close(&parent), WriteFence::Unfenced)
            .await?,
    );
    assert!(!closed.is_open());
    retire_pending_child_limit_intents(repo, &closed).await?;
    fixture.store.shutdown().await?;
    Ok(())
}

// Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
#[tokio::test]
async fn outbound_live_close_batch_expands_within_budget() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let parent = pending_child_limit_parent(repo).await?;
    let closing = close(&parent);
    let mut expected = crate::derive_close_intents(Some(&parent), &closing, ShardId(0))?;
    expected.sort_by_key(|row| row.key);
    let closed = applied(
        repo.commit_transition(parent.run_key, closing, WriteFence::Unfenced)
            .await?,
    );
    let count_operations = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_operation WHERE source_run_key=$1")
            .bind(parent.run_key.0)
            .fetch_one(&fixture.pool)
            .await
    };
    let mut prior = count_operations().await?;
    assert!(prior > 0);
    while prior > 0 {
        repo.reconcile_outbound_run(ShardId(0), parent.run_key, WriteFence::Unfenced)
            .await?;
        let left = count_operations().await?;
        assert!(left < prior);
        assert!(prior - left <= i64::try_from(crate::write_budget::WriteBudget::TRANSACTION.rows)?);
        prior = left;
    }
    let mut batch = crate::IntentKey {
        source_run: parent.run_key,
        close_seq: closed.transition_seq,
        kind: IntentKind::CloseBatch,
        ordinal: 0,
    };
    assert!(repo.load_intent(batch).await?.is_some());
    while repo.load_intent(batch).await?.is_some() {
        assert!(repo.expand_close_batch(batch, WriteFence::Unfenced).await?);
        assert!(!repo.expand_close_batch(batch, WriteFence::Unfenced).await?);
        batch.ordinal += 1;
    }
    let mut actual = Vec::new();
    let mut bound = ScanBound::Capture;
    let mut after = None;
    loop {
        let page = repo.read_intent_page(ShardId(0), bound, after, 100).await?;
        let Some(through) = page.through else {
            break;
        };
        bound = ScanBound::Through(through);
        if page.rows.is_empty() {
            break;
        }
        for descriptor in page.rows {
            after = Some(descriptor.key);
            if descriptor.key.source_run == parent.run_key {
                actual.push(
                    repo.load_intent(descriptor.key)
                        .await?
                        .expect("discovered intent"),
                );
            }
        }
    }
    assert_eq!(actual.len(), 2_000);
    assert!(actual.iter().all(|row| row.target.is_some()));
    assert_eq!(actual, expected);
    retire_pending_child_limit_intents(repo, &closed).await?;
    fixture.store.shutdown().await?;
    Ok(())
}
