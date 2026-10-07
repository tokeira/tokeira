#![cfg(feature = "dsql-integration")]
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

//! Live checks of `on-conflict-row-counts` against an explicitly selected
//! disposable Aurora DSQL database.
//!
//! Storage operations built on an `INSERT … ON CONFLICT` that can leave its row
//! unwritten, with `DO NOTHING` or with a `DO UPDATE … WHERE`, decide what
//! happened from the statement's row count. The probe pins the count DSQL
//! returns through sqlx for both clauses. Each property checks one operation's
//! answers for every input in a small, finite set, conflicting rows among them,
//! so every run covers every conflict path. The suite connects only when
//! `TOKEIRA_DSQL_TEST_DATABASE_URL` (or `DATABASE_URL`) is set; run it serially
//! (`docs/testing/dsql-live-suites.md`).

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{Context as _, Result, bail, ensure};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use sqlx::{PgPool, postgres::PgPoolOptions};
use time::{Duration, OffsetDateTime};
use tokeira_storage::{
    BacklogEntry, BacklogPayload, DSQL_ROWS_WRITTEN, DeliveryOrder, ProvenancePut, RunRepository,
    StoredTaskQueueConfig, StoredTaskQueueConfigKind, TaskQueueConfigCasResult,
    TaskQueueConfigRepository, WORKER_COMPUTE_RECORD_FORMAT_VERSION,
    WorkerComputeControllerAdmission, WorkerComputeControllerCommitResult,
    WorkerComputeControllerRecord, WorkerComputeProviderAction, WorkerComputeRepository,
    WorkerComputeScalingGroupState, WorkerTaskProvenance, WorkerTaskProvenanceError,
    WorkerTaskProvenanceStore,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
};
use tokeira_types::{
    BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, IncarnationId,
    LogicalTaskSeq, NamespaceId, QueueKey, RunKey, ScalingGroupId, TaskKind, TaskQueueName,
    WorkerComputeControllerLifecycle, WorkerComputeGroupEligibility, WorkerComputeHealth,
    WorkerComputeInvokeReason, WorkerComputeProviderActionStatus, WorkerComputeTaskType,
    WorkerTaskClass, WorkerTaskOrigin,
};
use uuid::Uuid;

/// The selected database with Tokeira's schema, and a runtime to drive it.
struct Live {
    pool: PgPool,
    store: DsqlStore,
    // Declared last so it outlives the store's and pool's background work.
    runtime: tokio::runtime::Runtime,
}

impl Live {
    fn connect() -> Result<Option<Self>> {
        let Some(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
        else {
            return Ok(None);
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (pool, store) = runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await?;
            let config = DsqlPoolConfig {
                // nextest starts this test in its crate directory, not the workspace root.
                migration: MigrationConfig {
                    migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
                },
                reservoir: ReservoirConfig {
                    target_ready: 4,
                    inflight_limit: 2,
                    ..ReservoirConfig::default()
                },
                ..DsqlPoolConfig::default()
            };
            let store = DsqlStore::from_database_url_for_tests(url.clone(), config).await?;
            store.migration_runner().apply(&pool).await?;
            Ok::<_, anyhow::Error>((pool, store))
        })?;
        Ok(Some(Self {
            pool,
            store,
            runtime,
        }))
    }
}

/// `now` to the second, which every column the fixtures write keeps exactly.
fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp()).unwrap()
}

/// The shard lease's own insert, which leaves a lease that exists unwritten.
const LEASE_INSERT: &str =
    "INSERT INTO shard_lease (shard_id, owner, epoch, lease_expiry, node_endpoint)
     VALUES ($1, $2, 1, $3, 'probe')
     ON CONFLICT (shard_id) DO NOTHING";

/// The CHASM current-run pointer's guarded upsert, reduced to its guard: it
/// replaces the pointer only when the pointer names the run in `$3`.
const POINTER_UPSERT: &str = "INSERT INTO chasm_current_execution
       (namespace_id, archetype_id, business_id, run_id, request_id, status,
        failover_version, transition_count, updated_at)
     VALUES ($1, 7, 'probe', $2, 'request', 1, 1, 1, now())
     ON CONFLICT (namespace_id, archetype_id, business_id) DO UPDATE SET
        run_id = EXCLUDED.run_id
     WHERE chasm_current_execution.run_id = $3";

/// The row count sqlx reads from DSQL's command tag is the number of rows the
/// statement wrote, for both kinds of conflict clause that can leave a row
/// unwritten: the shard lease's `DO NOTHING` insert, and the CHASM pointer's
/// guarded upsert. The operations the properties below cover decide from it.
#[test]
fn the_row_count_is_the_rows_written() -> Result<()> {
    let Some(live) = Live::connect()? else {
        return Ok(());
    };
    live.runtime.block_on(async {
        let pool = &live.pool;
        let shard = Uuid::new_v4();
        let expiry = now() + Duration::minutes(5);
        let inserted = sqlx::query(LEASE_INSERT)
            .bind(shard)
            .bind("owner-a")
            .bind(expiry)
            .execute(pool)
            .await?
            .rows_affected();
        let skipped = sqlx::query(LEASE_INSERT)
            .bind(shard)
            .bind("owner-b")
            .bind(expiry)
            .execute(pool)
            .await?
            .rows_affected();
        let owner: String = sqlx::query_scalar("SELECT owner FROM shard_lease WHERE shard_id = $1")
            .bind(shard)
            .fetch_one(pool)
            .await?;
        sqlx::query("DELETE FROM shard_lease WHERE shard_id = $1")
            .bind(shard)
            .execute(pool)
            .await?;
        ensure!(
            (inserted, skipped, owner.as_str()) == (1, 0, "owner-a"),
            "the insert counted {inserted}, the skipped insert {skipped}, and the lease names {owner}"
        );

        let namespace = Uuid::new_v4();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        sqlx::query(POINTER_UPSERT)
            .bind(namespace)
            .bind(first)
            .bind(Uuid::new_v4())
            .execute(pool)
            .await?;
        let skipped = sqlx::query(POINTER_UPSERT)
            .bind(namespace)
            .bind(second)
            .bind(Uuid::new_v4())
            .execute(pool)
            .await?
            .rows_affected();
        let unchanged: Uuid = sqlx::query_scalar(
            "SELECT run_id FROM chasm_current_execution
             WHERE namespace_id = $1 AND archetype_id = 7 AND business_id = 'probe'",
        )
        .bind(namespace)
        .fetch_one(pool)
        .await?;
        let replaced = sqlx::query(POINTER_UPSERT)
            .bind(namespace)
            .bind(second)
            .bind(first)
            .execute(pool)
            .await?
            .rows_affected();
        let current: Uuid = sqlx::query_scalar(
            "SELECT run_id FROM chasm_current_execution
             WHERE namespace_id = $1 AND archetype_id = 7 AND business_id = 'probe'",
        )
        .bind(namespace)
        .fetch_one(pool)
        .await?;
        sqlx::query("DELETE FROM chasm_current_execution WHERE namespace_id = $1")
            .bind(namespace)
            .execute(pool)
            .await?;
        ensure!(
            (skipped, unchanged, replaced, current) == (0, first, 1, second),
            "the guarded upsert counted {skipped} when its guard failed and {replaced} when \
             it held; the pointer named {unchanged} and then {current}"
        );
        Ok(())
    })
}

fn controller(
    namespace_id: NamespaceId,
    deployment: &str,
    now: OffsetDateTime,
) -> WorkerComputeControllerRecord {
    WorkerComputeControllerRecord {
        format_version: WORKER_COMPUTE_RECORD_FORMAT_VERSION,
        key: ControllerInstanceKey {
            namespace_id,
            deployment_name: DeploymentId(deployment.to_owned()),
            build_id: BuildId("build".to_owned()),
        },
        namespace_name: "namespace".to_owned(),
        revision: 0,
        lifecycle: WorkerComputeControllerLifecycle::Inactive,
        slot: None,
        owner: None,
        owner_epoch: 0,
        lease_until: None,
        groups: BTreeMap::from([(
            ScalingGroupId("primary".to_owned()),
            WorkerComputeScalingGroupState {
                fingerprint: ConfigurationFingerprint::from_canonical_bytes(&[1]),
                effective_task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
                eligibility: WorkerComputeGroupEligibility::Eligible,
                health: WorkerComputeHealth::Active,
                activation_fingerprint: None,
                activation_status: None,
                last_scale_up_at: None,
                prior_dispatch_rates: BTreeMap::new(),
                last_action_id: None,
                last_failure_category: None,
            },
        )]),
        next_metrics_poll_at: Some(now),
        reconciled_at: now,
    }
}

fn action(
    controller: &WorkerComputeControllerRecord,
    action_id: Uuid,
    now: OffsetDateTime,
) -> WorkerComputeProviderAction {
    WorkerComputeProviderAction {
        action_id,
        due_bucket: WorkerComputeProviderAction::due_bucket(action_id),
        controller_key: controller.key.clone(),
        scaling_group: ScalingGroupId("primary".to_owned()),
        configuration_fingerprint: ConfigurationFingerprint::from_canonical_bytes(&[1]),
        endpoint_name: "worker-compute".to_owned(),
        reason: WorkerComputeInvokeReason::NoSyncMatch,
        request_data: vec![1, 2, 3],
        status: WorkerComputeProviderActionStatus::Pending,
        attempts: 0,
        attempt_started_at: None,
        claim_epoch: 0,
        next_attempt_at: now,
        claim: None,
        superseded_at: None,
        last_error_category: None,
        created_at: now,
        updated_at: now,
    }
}

// Feature: on-conflict-row-counts, Property 2: A controller takes only a slot it wrote
#[test]
fn a_controller_takes_only_a_slot_it_wrote() -> Result<()> {
    let Some(live) = Live::connect()? else {
        return Ok(());
    };
    // Every slot limit up to 3, with every set of the slots below 3 held by other
    // controllers.
    for limit in 1usize..=3 {
        for mask in 0u8..8 {
            let held = (0u8..3)
                .filter(|slot| mask & (1 << slot) != 0)
                .collect::<BTreeSet<_>>();
            live.runtime
                .block_on(slot_case(&live, limit, &held))
                .with_context(|| format!("limit {limit}, other controllers in slots {held:?}"))?;
        }
    }
    Ok(())
}

async fn slot_case(live: &Live, limit: usize, held: &BTreeSet<u8>) -> Result<()> {
    let now = now();
    let namespace_id = NamespaceId::new();
    for slot in held {
        sqlx::query(
            "INSERT INTO worker_compute_controller_slot
             (namespace_id, slot, deployment_name, build_id, updated_at)
             VALUES ($1, $2, $3, 'build', $4)",
        )
        .bind(namespace_id.0)
        .bind(i16::from(*slot))
        .bind(format!("other-{slot}"))
        .bind(now)
        .execute(&live.pool)
        .await?;
    }
    let admission = live
        .store
        .worker_compute_repository()
        .admit_controller(controller(namespace_id, "candidate", now), limit, now)
        .await?;
    let free = (0..limit)
        .map(|slot| u8::try_from(slot).unwrap())
        .find(|slot| !held.contains(slot));
    match (free, admission) {
        (Some(slot), WorkerComputeControllerAdmission::Admitted(record)) => {
            ensure!(
                record.slot == Some(slot),
                "admitted to slot {:?}; the lowest free slot is {slot}",
                record.slot
            );
            let holder: String = sqlx::query_scalar(
                "SELECT deployment_name FROM worker_compute_controller_slot
                 WHERE namespace_id = $1 AND slot = $2",
            )
            .bind(namespace_id.0)
            .bind(i16::from(slot))
            .fetch_one(&live.pool)
            .await?;
            ensure!(holder == "candidate", "slot {slot} is held by {holder}");
        }
        (None, WorkerComputeControllerAdmission::CapacityLimited(record)) => {
            ensure!(
                record.slot.is_none(),
                "capacity-limited with slot {:?}",
                record.slot
            );
        }
        (free, admission) => bail!("lowest free slot {free:?}, but admission {admission:?}"),
    }
    Ok(())
}

// Feature: on-conflict-row-counts, Property 3: A stored action id is a conflict
#[test]
fn a_stored_action_id_is_a_conflict() -> Result<()> {
    let Some(live) = Live::connect()? else {
        return Ok(());
    };
    for duplicate in [false, true] {
        live.runtime
            .block_on(action_case(&live, duplicate))
            .with_context(|| format!("the second action repeats the first's id: {duplicate}"))?;
    }
    Ok(())
}

async fn action_case(live: &Live, duplicate: bool) -> Result<()> {
    let now = now();
    let namespace_id = NamespaceId::new();
    let repository = live.store.worker_compute_repository();
    let candidate = controller(namespace_id, "deciding", now);
    let key = candidate.key.clone();
    let admission = repository.admit_controller(candidate, 1, now).await?;
    ensure!(
        matches!(admission, WorkerComputeControllerAdmission::Admitted(_)),
        "admission {admission:?}"
    );
    let claimed = repository
        .claim_controller(&key, IncarnationId::new(), now, now + Duration::minutes(5))
        .await?
        .context("the controller is claimable")?;
    let first_action = Uuid::new_v4();
    let mut next = claimed.record.clone();
    next.revision += 1;
    let first = repository
        .commit_decision(
            &claimed.claim,
            claimed.record.revision,
            next.clone(),
            Some(action(&next, first_action, now)),
        )
        .await?;
    ensure!(
        first == WorkerComputeControllerCommitResult::Applied,
        "first decision {first:?}"
    );
    let mut after = next.clone();
    after.revision += 1;
    let action_id = if duplicate {
        first_action
    } else {
        Uuid::new_v4()
    };
    let second = repository
        .commit_decision(
            &claimed.claim,
            next.revision,
            after,
            Some(action(&next, action_id, now)),
        )
        .await?;
    let revision = repository
        .list_controllers(namespace_id)
        .await?
        .first()
        .context("the controller is stored")?
        .revision;
    let expected = if duplicate {
        (WorkerComputeControllerCommitResult::Conflict, next.revision)
    } else {
        (
            WorkerComputeControllerCommitResult::Applied,
            next.revision + 1,
        )
    };
    ensure!(
        (second, revision) == expected,
        "second decision {second:?} at revision {revision}, expected {expected:?}"
    );
    Ok(())
}

/// A token digest no other case uses.
fn digest() -> [u8; 32] {
    let mut digest = [0; 32];
    digest[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    digest[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    digest
}

fn provenance(digest: [u8; 32], task_queue: &str, now: OffsetDateTime) -> WorkerTaskProvenance {
    WorkerTaskProvenance {
        token_digest: digest,
        origin: WorkerTaskOrigin {
            namespace_id: NamespaceId::new(),
            normal_task_queue: TaskQueueName(task_queue.to_owned()),
            task_class: WorkerTaskClass::Workflow,
            deployment: DeploymentId("deployment".to_owned()),
            build_id: BuildId("build".to_owned()),
        },
        expires_at: now + Duration::hours(1),
        created_at: now,
    }
}

// Feature: on-conflict-row-counts, Property 4: Provenance answers what is stored
#[test]
fn provenance_answers_what_is_stored() -> Result<()> {
    let Some(live) = Live::connect()? else {
        return Ok(());
    };
    // 0: no row; 1: an equal row; 2: a different row for the same digest.
    for stored in 0u8..3 {
        live.runtime
            .block_on(provenance_case(&live, stored))
            .with_context(|| format!("stored row {stored}"))?;
    }
    Ok(())
}

async fn provenance_case(live: &Live, stored: u8) -> Result<()> {
    let store = live.store.worker_task_provenance_store();
    let record = provenance(digest(), "queue-a", now());
    let existing = match stored {
        1 => Some(record.clone()),
        2 => Some(WorkerTaskProvenance {
            origin: WorkerTaskOrigin {
                normal_task_queue: TaskQueueName("queue-b".to_owned()),
                ..record.origin.clone()
            },
            ..record.clone()
        }),
        _ => None,
    };
    if let Some(existing) = &existing {
        let put = store.put(existing.clone()).await?;
        ensure!(put == ProvenancePut::Inserted, "the fixture's row: {put:?}");
    }
    let answer = store.put(record.clone()).await;
    let kept = store
        .get(record.token_digest)
        .await?
        .context("a row is stored")?;
    store.delete(record.token_digest).await?;
    match (stored, &answer) {
        (0, Ok(ProvenancePut::Inserted))
        | (1, Ok(ProvenancePut::AlreadyPresent))
        | (2, Err(WorkerTaskProvenanceError::DigestConflict)) => {}
        _ => bail!("answered {answer:?}"),
    }
    ensure!(
        kept == existing.unwrap_or(record),
        "the stored row changed: {kept:?}"
    );
    Ok(())
}

fn task_queue_config(namespace_id: NamespaceId, rate: f32) -> StoredTaskQueueConfig {
    StoredTaskQueueConfig {
        namespace_id,
        task_queue: TaskQueueName("queue".to_owned()),
        kind: StoredTaskQueueConfigKind::Workflow,
        revision: 0,
        queue_rate_limit: Some(rate),
        queue_rate_limit_metadata: None,
        fairness_key_rate_limit_default: None,
        fairness_key_rate_limit_metadata: None,
        fairness_weight_overrides: BTreeMap::new(),
    }
}

// Feature: on-conflict-row-counts, Property 5: Only the writer of a new configuration is told it applied
#[test]
fn only_the_writer_of_a_new_configuration_is_told_it_applied() -> Result<()> {
    let Some(live) = Live::connect()? else {
        return Ok(());
    };
    for existing in [false, true] {
        live.runtime
            .block_on(configuration_case(&live, existing))
            .with_context(|| format!("a configuration already exists: {existing}"))?;
    }
    Ok(())
}

async fn configuration_case(live: &Live, existing: bool) -> Result<()> {
    let repository = live.store.task_queue_config_repository();
    let namespace_id = NamespaceId::new();
    let first = task_queue_config(namespace_id, 1.0);
    if existing {
        let created = repository
            .compare_and_swap_task_queue_config(first.clone(), None)
            .await?;
        ensure!(
            created == TaskQueueConfigCasResult::Applied { revision: 1 },
            "the fixture's create: {created:?}"
        );
    }
    let answer = repository
        .compare_and_swap_task_queue_config(task_queue_config(namespace_id, 2.0), None)
        .await?;
    let stored = repository
        .load_task_queue_config(&first.key())
        .await?
        .context("a configuration is stored")?;
    let expected = if existing {
        (TaskQueueConfigCasResult::Conflict, Some(1.0))
    } else {
        (TaskQueueConfigCasResult::Applied { revision: 1 }, Some(2.0))
    };
    ensure!(
        (answer, stored.queue_rate_limit) == expected,
        "answered {answer:?} with rate {:?} stored, expected {expected:?}",
        stored.queue_rate_limit
    );
    Ok(())
}

fn backlog_entry(queue: &QueueKey, run_key: RunKey, seq: usize) -> BacklogEntry {
    BacklogEntry {
        run_key,
        queue: queue.clone(),
        payload: BacklogPayload::Workflow {
            logical_seq: LogicalTaskSeq(u64::try_from(seq).unwrap()),
        },
        priority: None,
        scheduled_at: now(),
        order: DeliveryOrder::default(),
    }
}

/// The last value the backlog recorded as written.
fn backlog_rows_written(snapshotter: &Snapshotter) -> Option<f64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| {
            let backlog = key.key().name() == DSQL_ROWS_WRITTEN
                && key.key().labels().any(|label| {
                    label.key() == "operation" && label.value() == "persist_to_backlog"
                });
            match value {
                DebugValue::Histogram(values) if backlog => {
                    values.last().map(|value| value.into_inner())
                }
                _ => None,
            }
        })
}

// Feature: on-conflict-row-counts, Property 9: The backlog metric counts what was written
#[test]
fn the_backlog_metric_counts_what_was_written() -> Result<()> {
    let Some(live) = Live::connect()? else {
        return Ok(());
    };
    // Up to two entries already stored, and up to two new ones, in one batch.
    for (stored, new) in (0usize..3).flat_map(|stored| (0usize..3).map(move |new| (stored, new))) {
        let queue = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".to_owned()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let run_key = RunKey::new();
        let entries = (0..stored + new)
            .map(|seq| backlog_entry(&queue, run_key, seq))
            .collect::<Vec<_>>();
        let repository = live.store.run_repository();
        live.runtime
            .block_on(repository.persist_to_backlog(entries[..stored].to_vec()))?;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            live.runtime
                .block_on(repository.persist_to_backlog(entries.clone()))
        })?;
        let drained = live
            .runtime
            .block_on(repository.drain_backlog(&queue, 16))?;
        let written = backlog_rows_written(&snapshotter);
        ensure!(
            written == Some(new as f64) && drained.len() == stored + new,
            "{stored} stored and {new} new entries: recorded {written:?} written, drained {}",
            drained.len()
        );
    }
    Ok(())
}
