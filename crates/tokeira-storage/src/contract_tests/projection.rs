//! Projection contracts compare a fixture-authored ordered image map. Decoy
//! scopes are seeded explicitly; images in the scope under test come from commits.

use proptest::prelude::*;

use super::*;
use crate::{
    ProjectionImageKey, ProjectionRecord, memory::projection_accumulator_tests::following,
    projection_accumulator_oracle::workflow_projection_context,
};

pub(crate) fn strategy() -> impl Strategy<Value = (Vec<u8>, usize)> {
    (prop::collection::vec(any::<u8>(), 3..8), 1usize..5)
}

fn image(state: &WorkflowState) -> Result<ProjectionRecord> {
    Ok(ProjectionRecord {
        partition_id: 3,
        fanout: 1,
        run_key: state.run_key,
        transition_seq: state.transition_seq,
        context: workflow_projection_context(state, 0)?,
    })
}

async fn seed(
    backend: &impl ContractBackend,
    scope: &FixtureScope,
    ranks: &[u8],
) -> Result<BTreeMap<ProjectionImageKey, ProjectionRecord>> {
    ensure!(
        backend
            .log()
            .read_page(3, 1, None, 1)
            .await?
            .images
            .is_empty(),
        "projection scope contains foreign images; run on a clean disposable database"
    );
    let mut order: Vec<_> = (1..scope.states.len()).collect();
    order.sort_by_key(|&i| (ranks[i], std::cmp::Reverse(i)));
    let mut expected = BTreeMap::new();
    for index in order {
        let state = apply(backend, start(&scope.states[index])).await?;
        let record = image(&state)?;
        expected.insert(record.key(), record);
    }
    let state = apply(backend, following(scope.states.last().unwrap())).await?;
    let record = image(&state)?;
    expected.insert(record.key(), record);
    Ok(expected)
}

async fn walk(
    backend: &impl ContractBackend,
    mut cursor: Option<ProjectionImageKey>,
    limit: usize,
) -> Result<Vec<ProjectionRecord>> {
    let mut images = Vec::new();
    for _ in 0..100 {
        let page = backend.log().read_page(3, 1, cursor, limit).await?;
        ensure!(page.images.len() <= limit, "projection page exceeds limit");
        if page.images.is_empty() {
            return Ok(images);
        }
        for row in &page.images {
            ensure!(
                cursor.is_none_or(|cursor| row.key() > cursor),
                "projection cursor did not advance"
            );
            cursor = Some(row.key());
        }
        images.extend(page.images);
    }
    anyhow::bail!("projection pass never terminated")
}

// Feature: storage-contract-tests, Property 3: Projection passes follow keys and discover late images
pub(crate) async fn pages(
    backend: &impl ContractBackend,
    (ranks, limit): (Vec<u8>, usize),
) -> Result<()> {
    let scope = FixtureScope::new(ranks.len());
    scoped(backend, &scope, async {
        let mut expected = seed(backend, &scope, &ranks).await?;
        let record = expected.values().next().unwrap().clone();
        for (partition_id, fanout) in [(4, 1), (3, 2)] {
            let decoy = ProjectionRecord {
                partition_id,
                fanout,
                ..record.clone()
            };
            backend.seed_image(&decoy).await?;
            let page = backend
                .log()
                .read_page(partition_id, fanout, None, 10)
                .await?;
            equal(&page.images, &vec![decoy], "decoy scope")?;
            equal(
                &page.oldest_written_at,
                &backend.seeded_image_time(),
                "seeded diagnostic timestamp",
            )?;
        }
        equal(
            &walk(backend, None, limit).await?,
            &expected.values().cloned().collect(),
            "key ordered pass",
        )?;
        let first = backend.log().read_page(3, 1, None, limit).await?;
        let cursor = first.images.last().unwrap().key();
        let late = image(&apply(backend, start(&scope.states[0])).await?)?;
        expected.insert(late.key(), late);
        equal(
            &walk(backend, Some(cursor), limit).await?,
            &expected
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .map(|(_, image)| image.clone())
                .collect(),
            "exclusive image suffix",
        )?;
        equal(
            &walk(backend, None, limit).await?,
            &expected.values().cloned().collect(),
            "next pass sees late insertion",
        )?;
        let before = observe(backend, &scope).await?;
        let zero = backend.log().read_page(3, 1, None, 0).await?;
        ensure!(
            zero.images.is_empty() && zero.oldest_written_at.is_none(),
            "nonempty zero projection read"
        );
        equal(
            &observe(backend, &scope).await?,
            &before,
            "zero projection read changes nothing",
        )
    })
    .await
}

// Feature: storage-contract-tests, Property 4: Image retirement cannot erase an unobserved version
pub(crate) async fn deletion(
    backend: &impl ContractBackend,
    (ranks, limit): (Vec<u8>, usize),
) -> Result<()> {
    let scope = FixtureScope::new(ranks.len());
    scoped(backend, &scope, async {
        let mut expected = seed(backend, &scope, &ranks).await?;
        let page = backend.log().read_page(3, 1, None, limit).await?;
        let newer = image(&apply(backend, following(&scope.states[1])).await?)?;
        expected.insert(newer.key(), newer);
        let mut requested: Vec<_> = page
            .images
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, row)| row.key())
            .collect();
        let removed = requested.len() as u64;
        requested.push(requested[0]);
        requested.push(ProjectionImageKey {
            run_key: scope.key(0),
            transition_seq: scope.states[0].transition_seq,
        });
        equal(
            &backend.log().delete_images(4, 1, &requested).await?,
            &0,
            "wrong partition deletes nothing",
        )?;
        equal(
            &backend.log().delete_images(3, 2, &requested).await?,
            &0,
            "wrong fanout deletes nothing",
        )?;
        equal(
            &backend.log().delete_images(3, 1, &requested).await?,
            &removed,
            "exact deletion count",
        )?;
        for key in &requested {
            expected.remove(key);
        }
        equal(
            &backend.log().delete_images(3, 1, &requested).await?,
            &0,
            "idempotent deletion",
        )?;
        equal(
            &walk(backend, None, limit).await?,
            &expected.values().cloned().collect(),
            "unobserved images survive",
        )
    })
    .await
}
