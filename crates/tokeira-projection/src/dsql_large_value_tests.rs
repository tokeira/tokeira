//! Live memo replacement and snapshot contracts for `large-value-parts`.
//! Each case owns its namespace; absent the test URL, it performs no database I/O.

use anyhow::{Result, ensure};
use proptest::{
    prelude::*,
    test_runner::{TestCaseError, TestRunner},
};
use tokeira_storage::{
    ProjectionRecord, codec,
    dsql::value_parts::{self, PART_BYTES},
};
use tokeira_types::{Memo, Payload, RunKey, VisibilityLifecycleState};
use tokio::runtime::Runtime;
use uuid::Uuid;

use super::bounded_apply_tests::{Live, noise, short_tokens, text_value};
use crate::{
    ApplyOutcome, CompiledFilter, ExecutionRow, PageBounds, ProjectionSink, SortOrder,
    VisibilityStore,
};

fn image(live: &Live, run: RunKey, seq: u64, len: usize) -> ProjectionRecord {
    let mut image = live.image(run, seq, Vec::new());
    image.context.memo = Memo([("large".to_owned(), Payload::new(noise(len, seq)))].into());
    image
}

async fn exact(live: &Live, image: &ProjectionRecord) -> Result<()> {
    let (first, count) = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>(
        "SELECT memo_blob, memo_parts FROM execution_visibility_current
         WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3",
    )
    .bind(live.namespace_id.0)
    .bind(i64::from(image.context.archetype_id.0))
    .bind(image.run_key.0)
    .fetch_one(&live.pool)
    .await?;
    let rest = sqlx::query_as::<_, (i32, Vec<u8>)>(
        "SELECT part_no, data FROM visibility_memo_part
         WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3 ORDER BY part_no",
    )
    .bind(live.namespace_id.0)
    .bind(i64::from(image.context.archetype_id.0))
    .bind(image.run_key.0)
    .fetch_all(&live.pool)
    .await?;
    let expected = codec::encode(&image.context.memo)?;
    ensure!(
        (count.is_none() && rest.is_empty()) == (expected.len() <= PART_BYTES),
        "whole memos have no count or parts"
    );
    ensure!(
        value_parts::join(|| "the test memo".to_owned(), first, count, rest)? == expected,
        "only this memo's parts remain"
    );
    let mut connection = live.pool.acquire().await?;
    let read = super::get_execution_row(&mut connection, image.run_key)
        .await?
        .expect("the row exists");
    ensure!(
        read.memo == image.context.memo,
        "the single-row read joins the memo"
    );
    Ok(())
}

async fn listed(live: &Live) -> Result<Vec<ExecutionRow>> {
    Ok(live
        .visibility
        .list_executions(
            live.namespace_id,
            &CompiledFilter {
                archetype: None,
                expr: None,
            },
            SortOrder::Default,
            &PageBounds {
                limit: 10,
                after: None,
            },
        )
        .await?
        .rows)
}

// Feature: large-value-parts, Property 4: A replaced value leaves only its own parts
#[test]
fn dsql_memo_replacements_leave_only_their_own_parts() {
    let runtime = Runtime::new().unwrap();
    let Some(live) = runtime.block_on(Live::connect()).unwrap() else {
        return;
    };
    let strategy = prop::collection::vec(
        prop_oneof![0usize..1024, (PART_BYTES - 1024)..=(2 * PART_BYTES)],
        1..5,
    );
    let result = TestRunner::new(ProptestConfig::with_cases(8)).run(&strategy, |sizes| {
        runtime
            .block_on(async {
                let run = RunKey(Uuid::new_v4());
                let mut last = image(&live, run, 1, 2 * PART_BYTES);
                live.visibility.apply(&last, 0).await?;
                exact(&live, &last).await?;
                let older = last.clone();
                for len in sizes.into_iter().chain([0, 2 * PART_BYTES]) {
                    last = image(&live, run, last.transition_seq.0 + 1, len);
                    ensure!(
                        live.visibility.apply(&last, 0).await? == ApplyOutcome::Advanced,
                        "the next image advances"
                    );
                    exact(&live, &last).await?;
                }
                ensure!(
                    live.visibility.apply(&older, 0).await? == ApplyOutcome::Current,
                    "the older image is refused"
                );
                exact(&live, &last).await?;
                last.transition_seq = last.transition_seq.next();
                last.context.lifecycle_state = VisibilityLifecycleState::Deleted;
                last.context.memo = Memo::default();
                live.visibility.apply_deletion(&last).await?;
                exact(&live, &last).await?;
                Ok(())
            })
            .map_err(|error: anyhow::Error| TestCaseError::fail(format!("{error:?}")))
    });
    runtime.block_on(live.finish()).unwrap();
    result.unwrap();
}

// Feature: large-value-parts, Property 3: A stored value reads back whole
#[tokio::test]
async fn dsql_a_list_held_before_memo_parts_reads_one_version() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let run = RunKey(Uuid::new_v4());
    let old = image(&live, run, 1, 2 * PART_BYTES);
    let new = image(&live, run, 2, 2 * PART_BYTES);
    live.visibility.apply(&old, 0).await?;
    let rows = {
        let (reached, resume) = super::memo_read_pause::arm(live.namespace_id);
        let read = listed(&live);
        tokio::pin!(read);
        tokio::select! {
            result = &mut read => panic!("the split page did not pause: {result:?}"),
            result = reached => { result?; }
        }
        live.visibility.apply(&new, 0).await?;
        drop(resume);
        read.await?
    };
    let after = listed(&live).await?;
    live.finish().await?;
    ensure!(
        rows.len() == 1 && rows[0].memo == old.context.memo,
        "the held snapshot returns the old memo whole"
    );
    ensure!(
        after.len() == 1 && after[0].memo == new.context.memo,
        "the next read returns the new memo whole"
    );
    Ok(())
}

// Feature: large-value-parts, Property 4: A replaced value leaves only its own parts
#[tokio::test]
async fn dsql_staged_flips_replace_memo_parts() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let attrs = live.register_text("LargeMemoText", 5).await?;
    let value = text_value(&short_tokens(694))?;
    let run = RunKey(Uuid::new_v4());
    for (seq, len) in [(1, 2 * PART_BYTES), (2, 0)] {
        let mut image = image(&live, run, seq, len);
        image.context.search_attributes.0 = attrs
            .iter()
            .map(|(name, _)| (name.clone(), value.clone()))
            .collect();
        live.visibility.apply(&image, 0).await?;
        exact(&live, &image).await?;
        let page = live
            .visibility
            .list_component_executions(
                live.namespace_id,
                &CompiledFilter {
                    archetype: None,
                    expr: None,
                },
                SortOrder::Default,
                &PageBounds {
                    limit: 1,
                    after: None,
                },
            )
            .await?;
        ensure!(
            page.rows.len() == 1 && page.rows[0].memo == image.context.memo,
            "component lists join split memos too"
        );
    }
    live.finish().await
}

// Feature: large-value-parts, Property 4: A replaced value leaves only its own parts
#[tokio::test]
async fn dsql_upsert_execution_replaces_memo_parts() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let run = RunKey(Uuid::new_v4());
    let old = image(&live, run, 1, 2 * PART_BYTES);
    for (seq, len) in [(1, 2 * PART_BYTES), (2, PART_BYTES + 1), (3, 0)] {
        let next = image(&live, run, seq, len);
        live.visibility
            .upsert_execution(&ExecutionRow::from_projection_record(&next))
            .await?;
        exact(&live, &next).await?;
    }
    let last = image(&live, run, 3, 0);
    live.visibility
        .upsert_execution(&ExecutionRow::from_projection_record(&old))
        .await?;
    exact(&live, &last).await?;
    live.finish().await
}
