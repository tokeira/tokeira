//! Physical plans for the exact production activity page builders, including
//! deep equal-time and same-run ties that a residual filter could rescan.

use std::num::NonZeroU32;

use anyhow::Result;
use sqlx::{Execute, PgPool, Postgres, QueryBuilder};
use time::{Duration, OffsetDateTime};
use tokeira_types::{NamespaceId, QueueKey, RunKey, ShardId, TaskKind, TaskQueueName};

use super::{Backend, fixture};
use crate::{
    ActivityDiscoveryRange, ActivityDispatchPosition, ActivityDispatchRow, ActivityHomePosition,
    ActivityIncarnation, activity_dispatch_key,
    dsql::run_repository::activity_dispatch::{due_query, home_query},
};

async fn explain(pool: &PgPool, builder: &mut QueryBuilder<Postgres>) -> Result<Vec<String>> {
    let mut query = builder.build();
    let arguments = query
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .expect("page builder binds arguments");
    let mut explain = QueryBuilder::<Postgres>::with_arguments("EXPLAIN ", arguments);
    explain.push(query.sql().as_str());
    Ok(explain.build_query_scalar().fetch_all(pool).await?)
}

fn require_seek(plan: &[String], parts: &[&str]) {
    assert!(
        plan.iter().any(
            |line| line.contains("Index Cond:") && parts.iter().all(|part| line.contains(part))
        ),
        "missing complete index seek {parts:?}: {plan:#?}"
    );
}

#[test]
fn activity_dispatch_live_query_plans() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    runtime.block_on(async {
        let namespace_id = NamespaceId::new();
        let base = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
        let run = RunKey::new();
        // A failed prior run retains its evidence rows. Pick an unused fixture
        // home so rerunning this check cannot mix its complete-home oracle with
        // another run's slots; never delete earlier evidence to make it pass.
        let mut home = None;
        for candidate in 2_345_679..2_346_703 {
            let occupied: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM activity_dispatch WHERE shard_id=$1)",
            )
            .bind(crate::dsql::DsqlRunRepository::shard_id_to_uuid(ShardId(candidate)))
            .fetch_one(&fixture.pool)
            .await?;
            if !occupied {
                home = Some(ShardId(candidate));
                break;
            }
        }
        let home = home.expect("an unused plan-fixture home must be available");
        let limit = NonZeroU32::new(64).unwrap();
        let range = ActivityDiscoveryRange {
            namespace_id, queue_name: TaskQueueName("activity-plan".into()),
        };
        let mut due = Vec::new();
        let mut homes = Vec::new();
        for batch in 0..64 {
            let mut rows = Vec::new();
            for offset in 0..256 {
                let index = batch * 256 + offset;
                let target = index < 8192;
                let row_run = if target { run } else { RunKey::new() };
                let activity_id = format!("plan-{index:05}");
                let queue_name = if target { range.queue_name.clone() }
                    else { TaskQueueName(format!("unrelated-{}", index % 128)) };
                let queue_range = ActivityDiscoveryRange { namespace_id, queue_name: queue_name.clone() };
                let row = ActivityDispatchRow {
                    key: activity_dispatch_key(row_run, &activity_id),
                    incarnation: ActivityIncarnation { run_key: row_run, activity_id,
                        schedule_event_id: index + 3, attempt: 1, stamp: 0 },
                    execution_home: if target { home } else { ShardId(index as u32 % 64) },
                    queue: QueueKey { namespace_id, task_queue: queue_name, task_kind: TaskKind::Activity,
                        deployment: None, build_id: None },
                    queue_key: queue_range.lookup_key(), priority: None, dispatch_revision: 0,
                    dispatch_at: base + Duration::seconds(index % 8192 / 2048),
                };
                if target {
                    due.push(row.position());
                    homes.push(ActivityHomePosition { run_key: row_run, key: row.key });
                }
                rows.push((row, false));
            }
            fixture.seed_activity_rows(&rows).await?;
        }
        due.sort();
        homes.sort();
        let cutoff = base + Duration::seconds(3);
        let before_cursor = fixture.activity_repo().list_activity_dispatch_page(
            &range, base - Duration::seconds(1), Some(due[0]), limit,
        ).await?;
        assert!(before_cursor.candidates.is_empty());
        assert!(before_cursor.exhausted);
        let mut report = String::from("16384 seeded rows: 8192 in one queue family, 2048 per timestamp tie; 8192 in 128 unrelated families. The target home contains 8192 slots in one run. EXPLAIN does not measure storage-engine rows examined.\n");
        for offset in [0, 1792, 4096, 7936] {
            let after = (offset > 0).then(|| due[offset - 1]);
            let page = fixture.activity_repo().list_activity_dispatch_page(&range, cutoff, after, limit).await?;
            let actual = page.candidates.into_iter().map(|candidate| candidate.row.map(|row| row.position())).collect::<Result<Vec<ActivityDispatchPosition>>>()?;
            assert_eq!(actual, due[offset..offset + 64]);
            let plan = explain(&fixture.pool, &mut due_query(&range, cutoff, after, limit)).await?;
            require_seek(&plan, &["(key ="]);
            require_seek(&plan, &["queue_namespace =", "queue_key =", "dispatch_at <="]);
            if after.is_some() {
                require_seek(&plan, &["dispatch_at =", "key >"]);
                require_seek(&plan, &["dispatch_at >"]);
            }
            report.push_str(&format!("\nQueue offset={offset}:\n{}\n", plan.join("\n")));
            let after = (offset > 0).then(|| homes[offset - 1]);
            let page = fixture.activity_repo().list_activity_dispatch_home_page(home, after, limit).await?;
            assert_eq!(page.candidates, homes[offset..offset + 64]);
            let plan = explain(&fixture.pool, &mut home_query(home, after, limit)).await?;
            require_seek(&plan, &["shard_id ="]);
            if let Some(after) = after {
                require_seek(&plan, &["shard_id =", "run_key =", "key >"]);
                require_seek(&plan, &["shard_id =", "run_key >"]);
                // The pre-correction tuple predicate is an executable negative
                // control for covered continuation, not an alternative oracle.
                let mut old = QueryBuilder::<Postgres>::new("SELECT run_key,key FROM activity_dispatch WHERE shard_id=");
                old.push_bind(crate::dsql::run_repository::DsqlRunRepository::shard_id_to_uuid(home))
                    .push(" AND (run_key,key)>(").push_bind(after.run_key.0)
                    .push(",").push_bind(after.key).push(") ORDER BY run_key,key LIMIT 64");
                let old_plan = explain(&fixture.pool, &mut old).await?;
                assert!(!old_plan.iter().any(|line| line.contains("Index Cond:") && line.contains("key >")),
                    "negative control became a full seek; reassess the retained fixture: {old_plan:#?}");
                report.push_str(&format!("\nPre-correction home offset={offset}:\n{}\n", old_plan.join("\n")));
            }
            report.push_str(&format!("\nHome offset={offset}:\n{}\n", plan.join("\n")));
        }
        let mut cursor = None;
        let mut traversed = Vec::new();
        let mut statements = 0;
        loop {
            let page = fixture.activity_repo().list_activity_dispatch_page(&range, cutoff, cursor, limit).await?;
            statements += 1;
            for candidate in page.candidates { traversed.push(candidate.row?.position()); }
            cursor = page.last_examined;
            if page.exhausted { break; }
        }
        assert_eq!(traversed, due);
        assert_eq!(statements, 129);
        report.push_str("\nComplete queue pass: 8192 returned rows, exactly 129 page SQL statements.\n");
        if let Some(path) = std::env::var_os("TOKEIRA_ACTIVITY_DISPATCH_PLAN_OUTPUT") {
            std::fs::write(path, report)?;
        }
        fixture.store.shutdown().await
    })
}
