//! History slices are expected from authored vectors, never from another reader.

use proptest::prelude::*;
use tokeira_kernel::HistoryEvent;
use tokeira_types::EventPrincipal;

use super::*;
use crate::{
    HISTORY_READ_PAGE, bulk_write_tests::signal, memory::projection_accumulator_tests::following,
};

#[derive(Clone, Debug)]
pub(crate) struct HistoryCase {
    pub(crate) cuts: Vec<usize>,
    pub(crate) cursor: usize,
    pub(crate) limit: usize,
    pub(crate) attributed: bool,
    pub(crate) payload_bytes: usize,
}

pub(crate) fn strategy() -> impl Strategy<Value = HistoryCase> {
    (
        prop::collection::vec(1usize..9, 1..6),
        0usize..45,
        0usize..12,
        any::<bool>(),
    )
        .prop_map(|(cuts, cursor, limit, attributed)| HistoryCase {
            cuts,
            cursor,
            limit,
            attributed,
            payload_bytes: 8,
        })
}

// Feature: storage-contract-tests, Property 1: History pages preserve the authored sequence
pub(crate) async fn case(backend: &impl ContractBackend, case: HistoryCase) -> Result<()> {
    let scope = FixtureScope::new(2);
    scoped(backend, &scope, async {
        let mut state = scope.states[0].clone();
        let mut expected = Vec::new();
        let mut boundaries = vec![0, case.cursor as i64];
        for (batch, count) in case.cuts.iter().copied().enumerate() {
            let mut transition = if batch == 0 {
                start(&state)
            } else {
                following(&state)
            };
            for _ in 0..count {
                let id = expected.len() as i64 + 1;
                let kind = signal(id as usize, case.payload_bytes);
                let event = HistoryEvent {
                    event_id: id,
                    happened_at: now(),
                    kind,
                };
                let principal = (case.attributed && id % 2 == 0).then(|| EventPrincipal {
                    principal_type: "fixture".into(),
                    name: format!("principal-{id}"),
                });
                transition.history_events.push(event.clone());
                transition.event_principals.push(principal.clone());
                expected.push(AttributedHistoryEvent { event, principal });
            }
            transition.next_state.last_event_id = expected.len() as i64;
            state = apply(backend, transition).await?;
            boundaries.push(expected.len() as i64);
        }
        let before = observe(backend, &scope).await?;
        boundaries.extend([expected.len() as i64 + 1, -1]);
        boundaries.sort_unstable();
        boundaries.dedup();
        for cursor in boundaries {
            let suffix: Vec<_> = expected
                .iter()
                .filter(|row| row.event.event_id > cursor)
                .cloned()
                .collect();
            for limit in [0, 1, case.limit, expected.len() + 1] {
                let page: Vec<_> = suffix.iter().take(limit).cloned().collect();
                equal(
                    &backend
                        .repo()
                        .read_attributed_history(state.run_key, cursor, limit)
                        .await?,
                    &page,
                    "attributed page",
                )?;
                equal(
                    &backend
                        .repo()
                        .read_history(state.run_key, cursor, limit)
                        .await?,
                    &page.iter().map(|row| row.event.clone()).collect(),
                    "history page",
                )?;
            }
            equal(
                &backend
                    .repo()
                    .read_attributed_history_to_end(state.run_key, cursor)
                    .await?,
                &suffix,
                "attributed suffix",
            )?;
            equal(
                &backend
                    .repo()
                    .read_history_to_end(state.run_key, cursor)
                    .await?,
                &suffix.iter().map(|row| row.event.clone()).collect(),
                "history suffix",
            )?;
            for id in [cursor, cursor + 1] {
                let event = expected
                    .iter()
                    .find(|row| row.event.event_id == id)
                    .map(|row| row.event.clone());
                equal(
                    &backend.repo().read_history_event(state.run_key, id).await?,
                    &event,
                    "exact event",
                )?;
            }
        }
        equal(
            &backend.repo().read_history(scope.key(1), 0, 10).await?,
            &Vec::new(),
            "absent history",
        )?;
        equal(
            &backend.repo().read_history_event(scope.key(1), 1).await?,
            &None,
            "absent event",
        )?;
        equal(
            &observe(backend, &scope).await?,
            &before,
            "read-only history",
        )
    })
    .await
}

pub(crate) async fn boundaries(backend: &impl ContractBackend) -> Result<()> {
    for cuts in [
        vec![0],
        vec![HISTORY_READ_PAGE, HISTORY_READ_PAGE],
        vec![513, 1025],
    ] {
        case(
            backend,
            HistoryCase {
                cuts,
                cursor: 1,
                limit: 1001,
                attributed: true,
                payload_bytes: 8,
            },
        )
        .await?;
    }
    // Incompressible bytes exercise real split-value hydration on DSQL, with
    // identical semantic reads on memory and no physical limit imposed there.
    case(
        backend,
        HistoryCase {
            cuts: vec![2, 1],
            cursor: 1,
            limit: 2,
            attributed: true,
            payload_bytes: 1_100_000,
        },
    )
    .await
}
