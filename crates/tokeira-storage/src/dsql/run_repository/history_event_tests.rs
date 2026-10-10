//! The shared history-event read contracts (`activity-inputs-from-history`)
//! against an operator-owned ephemeral Aurora DSQL cluster; the lookup's plan on
//! a run of thousands of batches; and the dispatch rows and backlog entries an
//! earlier release wrote with an activity's input. No connection occurs unless
//! the dedicated test URL is provided.

use sqlx::QueryBuilder;
use tokeira_kernel::{ActivityOp, DispatchOp, HistoryEvent, HistoryEventKind};
use tokeira_types::{Payload, Payloads};

use super::{bulk_write_tests::Fixture, load::READ_HISTORY_EVENT_BATCH_SQL, *};
use crate::{
    BacklogEntry, BacklogPayload, DeliveryOrder,
    bulk_write_tests::{commit, noise, timer, with_events},
    codec::{PreviousBacklogPayload, encode_backlog_payload_as_previous_release},
    history_event_tests::{
        read_from_a_reset_successors_copied_batches, read_from_batches_of_any_size,
    },
    memory::projection_accumulator_tests::{
        activity_fixture, applied, following, fresh_transition,
    },
};

#[tokio::test]
async fn dsql_history_event_read_from_batches_of_any_size() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        read_from_batches_of_any_size(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_history_event_read_from_a_reset_successors_copied_batches() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        read_from_a_reset_successors_copied_batches(&fixture).await;
    }
    Ok(())
}

/// Batches the plan's run holds, and events in each.
const PLAN_BATCHES: i64 = 5_000;
const PLAN_EVENTS_PER_BATCH: i64 = 3;

/// On a run of 5,000 batches, the lookup reads the batch that holds an event near
/// the end, and one near the start, with a backward scan of the primary key that
/// starts at the event's id: both key columns in its index condition, no sort,
/// and the limit pushed down to storage, which returns one row.
#[tokio::test]
async fn dsql_history_event_lookup_plan() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let run_key = RunKey::new();
    // The rows are written directly: the plan depends on the primary key alone.
    for page in 0..PLAN_BATCHES / 500 {
        let rows = (page * 500..(page + 1) * 500)
            .map(|batch| {
                let first = batch * PLAN_EVENTS_PER_BATCH + 1;
                let events = (first..first + PLAN_EVENTS_PER_BATCH)
                    .map(|event_id| HistoryEvent {
                        event_id,
                        happened_at: OffsetDateTime::UNIX_EPOCH,
                        kind: timer(usize::try_from(event_id).expect("positive")),
                    })
                    .collect::<Vec<_>>();
                Ok((first, codec::encode_history_events(&events)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut insert = QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO history_batch \
             (run_key, first_event_id, last_event_id, transition_seq, events_data) ",
        );
        insert.push_values(rows, |mut row, (first, events_data)| {
            row.push_bind(run_key.0)
                .push_bind(first)
                .push_bind(first + PLAN_EVENTS_PER_BATCH - 1)
                .push_bind(first)
                .push_bind(events_data);
        });
        insert.build().execute(&fixture.pool).await?;
    }
    sqlx::query("ANALYZE history_batch")
        .execute(&fixture.pool)
        .await?;

    let mut report = String::new();
    let repo = fixture.store.run_repository();
    for event_id in [PLAN_BATCHES * PLAN_EVENTS_PER_BATCH - 1, 5] {
        let event = repo
            .read_history_event(run_key, event_id)
            .await?
            .expect("the run holds the event");
        assert_eq!(event.event_id, event_id);

        let mut explain = QueryBuilder::<sqlx::Postgres>::new("EXPLAIN ANALYZE ");
        explain.push(READ_HISTORY_EVENT_BATCH_SQL);
        let plan = explain
            .build_query_as::<(String,)>()
            .bind(run_key.0)
            .bind(event_id)
            .fetch_all(&fixture.pool)
            .await?;
        let lines = plan.iter().map(|(line,)| line.as_str()).collect::<Vec<_>>();
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Scan Backward using history_batch_pkey")),
            "a backward scan of the primary key: {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("Index Cond:")
                && line.contains("run_key =")
                && line.contains("first_event_id <=")),
            "both key columns in the index condition: {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("Sort")),
            "no sort: {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.trim() == "Limit: 1"),
            "the limit pushed down to storage: {lines:#?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Storage Scan") && line.contains("actual rows=1 ")),
            "storage returns one row: {lines:#?}"
        );
        report.push_str(&format!(
            "Event {event_id} of a run of {PLAN_BATCHES} batches:\n"
        ));
        for line in lines {
            report.push_str(line);
            report.push('\n');
        }
    }
    if let Some(path) = std::env::var_os("TOKEIRA_HISTORY_EVENT_PLAN_OUTPUT") {
        std::fs::write(path, report)?;
    }
    Ok(())
}

impl Fixture {
    /// The stored size of the run's activity dispatch rows' `input_data`.
    async fn input_data_lens(&self, run_key: RunKey) -> Result<Vec<i32>> {
        Ok(sqlx::query_scalar::<_, i32>(
            "SELECT octet_length(input_data) FROM activity_dispatch WHERE run_key = $1",
        )
        .bind(run_key.0)
        .fetch_all(&self.pool)
        .await?)
    }
}

/// A dispatch row and a backlog entry that an earlier release wrote with an
/// activity's input read without it, and the input is read from the activity's
/// scheduled event. This release writes a row's input empty, and clears an
/// earlier input when it updates the row.
#[tokio::test]
async fn dsql_previous_release_dispatch_rows_and_backlog_entries() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let input = Payloads(vec![Payload::new(noise(200_000, 7))]);

    // A run whose second commit schedules an activity: its scheduled event, its
    // state and its dispatch row.
    let state = applied(commit(&fixture, fresh_transition(RunKey::new())).await);
    let run_key = state.run_key;
    let first = state.last_event_id + 1;
    let mut activity = activity_fixture();
    activity.activity_id = "previous-release".to_owned();
    activity.schedule_event_id = first;
    let queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: activity.task_queue.clone(),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
    let mut schedule = with_events(
        following(&state),
        first,
        vec![HistoryEventKind::ActivityTaskScheduled {
            workflow_task_completed_event_id: first - 1,
            activity_id: activity.activity_id.clone(),
            activity_type: activity.activity_type.clone(),
            task_queue: activity.task_queue.clone(),
            input: input.clone(),
            header: None,
            retry_policy: None,
            schedule_to_close_timeout: activity.schedule_to_close_timeout,
            schedule_to_start_timeout: activity.schedule_to_start_timeout,
            start_to_close_timeout: activity.start_to_close_timeout,
            heartbeat_timeout: activity.heartbeat_timeout,
            priority: None,
        }],
    );
    schedule
        .next_state
        .activities
        .insert(activity.activity_id.clone(), activity.clone());
    schedule.dispatch_ops = vec![DispatchOp::EnqueueActivityTask {
        queue: queue.clone(),
        activity_id: activity.activity_id.clone(),
        schedule_event_id: first,
        attempt: activity.attempt,
        dispatch_revision: 0,
        stamp: activity.stamp,
        dispatch_at: OffsetDateTime::UNIX_EPOCH,
        schedule_to_close_timeout: activity.schedule_to_close_timeout,
        schedule_to_start_timeout: activity.schedule_to_start_timeout,
        start_to_close_timeout: activity.start_to_close_timeout,
        heartbeat_timeout: activity.heartbeat_timeout,
        priority: None,
    }]
    .into();
    let state = applied(commit(&fixture, schedule).await);
    assert_eq!(fixture.input_data_lens(run_key).await?, [1]);

    let scheduled_input = |event: Option<HistoryEvent>| match event {
        Some(HistoryEvent {
            kind: HistoryEventKind::ActivityTaskScheduled { input, .. },
            ..
        }) => input,
        other => panic!("not a scheduled event: {other:?}"),
    };

    // The row as an earlier release wrote it, input and all.
    sqlx::query("UPDATE activity_dispatch SET input_data = $2 WHERE run_key = $1")
        .bind(run_key.0)
        .bind(codec::encode(&input)?)
        .execute(&fixture.pool)
        .await?;
    let rows = repo
        .list_all_dispatchable_activity_tasks(&queue, 10)
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].run_key, rows[0].schedule_event_id),
        (run_key, first)
    );
    assert_eq!(
        scheduled_input(repo.read_history_event(run_key, first).await?),
        input
    );
    // An update of the row clears the earlier input.
    let mut update = following(&state);
    update.activity_ops = vec![ActivityOp::Upsert(activity.clone())].into();
    applied(commit(&fixture, update).await);
    assert_eq!(fixture.input_data_lens(run_key).await?, [1]);

    // A backlog entry as an earlier release wrote it, input and all.
    let entry = BacklogEntry {
        run_key,
        queue: queue.clone(),
        payload: BacklogPayload::Activity {
            activity_id: activity.activity_id.clone(),
            retired_input: Default::default(),
            schedule_event_id: first,
            attempt: activity.attempt,
            dispatch_revision: 0,
            stamp: activity.stamp,
        },
        priority: None,
        scheduled_at: OffsetDateTime::UNIX_EPOCH,
        order: DeliveryOrder {
            priority_key: 3,
            fair_pass: 0,
            insertion_tie: 0,
        },
    };
    repo.persist_to_backlog(vec![entry.clone()]).await?;
    sqlx::query("UPDATE dispatch_backlog SET payload_data = $2 WHERE run_key = $1")
        .bind(run_key.0)
        .bind(encode_backlog_payload_as_previous_release(
            PreviousBacklogPayload::Activity {
                activity_id: activity.activity_id.clone(),
                input: input.clone(),
                schedule_event_id: first,
                attempt: activity.attempt,
                dispatch_revision: 0,
                stamp: activity.stamp,
            },
            None,
        )?)
        .execute(&fixture.pool)
        .await?;
    let drained = repo.drain_backlog(&queue, 10).await?;
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].payload, entry.payload);
    let BacklogPayload::Activity {
        schedule_event_id, ..
    } = drained[0].payload
    else {
        panic!("an activity entry");
    };
    assert_eq!(
        scheduled_input(repo.read_history_event(run_key, schedule_event_id).await?),
        input
    );
    Ok(())
}
