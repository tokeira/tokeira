//! Generated query invariants over the real projection store and sink.

use std::collections::BTreeSet;

use proptest::prelude::*;
use tokeira_storage::{ProjectionContext, ProjectionRecord};
use tokeira_types::{
    ExecutionStatus, Payload, RunKey, SearchAttrValue, TaskQueueName, WorkflowId, WorkflowType,
};
use uuid::Uuid;

use super::*;
use crate::{InMemoryVisibilityStore, ProjectionSink, SearchAttrType, VisibilitySink};

fn record(
    id: u128,
    namespace: u128,
    archetype: u32,
    generation: i64,
    deleted: bool,
) -> ProjectionRecord {
    let time = OffsetDateTime::from_unix_timestamp(id as i64 % 3).unwrap();
    let run_id = RunId(Uuid::from_u128(id));
    ProjectionRecord {
        partition_id: 0,
        fanout: 1,
        run_key: RunKey(run_id.0),
        transition_seq: TransitionSeq(1),
        context: ProjectionContext {
            namespace_id: NamespaceId(Uuid::from_u128(namespace)),
            archetype_id: ArchetypeId(archetype),
            business_id: format!("deployment-{id}"),
            workflow_id: WorkflowId(format!("deployment-{id}")),
            run_id,
            authority_epoch: 0,
            status_keyword: "Degraded".into(),
            lifecycle_state: if deleted {
                VisibilityLifecycleState::Deleted
            } else {
                VisibilityLifecycleState::Open
            },
            workflow_type: WorkflowType("resource".into()),
            task_queue: TaskQueueName(String::new()),
            execution_status: ExecutionStatus::Running,
            start_time: time,
            update_time: time,
            close_time: None,
            execution_time: None,
            execution_duration: None,
            history_length: 1,
            state_transition_count: 1,
            transition_count: 1,
            history_size_bytes: 0,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: None,
            root_run_id: None,
            search_attr_generation: 1,
            memo: Memo::default(),
            search_attributes: SearchAttributes(
                [("Generation".into(), SearchAttrValue::Int(generation))].into(),
            ),
        },
    }
}

fn handle(store: &InMemoryVisibilityStore, namespace: u128, archetype: u32) -> ComponentVisibility {
    ComponentVisibility::new(
        Arc::new(store.clone()),
        NamespaceId(Uuid::from_u128(namespace)),
        ArchetypeId(archetype),
    )
}

proptest! {
    // Feature: chasm-extension-visibility, Properties 1 and 2: scoped pages enumerate exactly the matching rows.
    #[test]
    fn scoped_pages_and_counts_agree(
        rows in prop::collection::vec((1u128..3, 0u32..4, -5i64..6, any::<bool>()), 0..30),
        size in 1usize..8, threshold in -5i64..6,
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = InMemoryVisibilityStore::default();
            let sink = VisibilitySink::new(store.clone());
            for ns in [1, 2] { store.register_attr(NamespaceId(Uuid::from_u128(ns)), "Generation".into(), SearchAttrType::Int).await.unwrap(); }
            let mut expected = BTreeSet::new();
            for (i, &(ns, arch, generation, deleted)) in rows.iter().enumerate() {
                let id = i as u128 + 1;
                let row = record(id, ns, arch, generation, deleted);
                sink.apply(&row, 0).await.unwrap();
                if ns == 1 && arch == 1 && generation >= threshold && !deleted { expected.insert(row.context.run_id.0); }
            }
            let handle = handle(&store, 1, 1);
            let query = format!("Generation >= {threshold} AND ExecutionStatus = 'Degraded'");
            prop_assert_eq!(handle.count(Some(&query)).await.unwrap(), expected.len() as i64);
            let mut token = None;
            let mut actual = BTreeSet::new();
            for _ in 0..=rows.len() + 1 {
                let page = handle.list(ComponentQuery { query: Some(query.clone()), page_size: size, next_page_token: token }).await.unwrap();
                prop_assert!(page.executions.len() <= size);
                for row in page.executions { prop_assert!(actual.insert(row.run_id.0)); prop_assert_eq!(row.lifecycle_state, VisibilityLifecycleState::Open); }
                token = page.next_page_token;
                if token.is_none() { break; }
            }
            prop_assert!(token.is_none());
            prop_assert_eq!(actual, expected);
            // An OR that asks for another archetype cannot escape the handle scope.
            let escaped = handle.list(ComponentQuery { query: Some("archetype = 0 OR ExecutionStatus = 'Degraded'".into()), ..Default::default() }).await.unwrap();
            prop_assert!(escaped.executions.iter().all(|row| row.archetype_id == ArchetypeId(1) && row.namespace_id == NamespaceId(Uuid::from_u128(1))));
            Ok(())
        })?;
    }

    // Feature: chasm-extension-visibility, Property 3: generic values survive projection and older images cannot replace them.
    #[test]
    fn summaries_preserve_latest_typed_image(value in any::<i64>(), status in "[a-z]{1,20}", closed in any::<bool>()) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = InMemoryVisibilityStore::default();
            let sink = VisibilitySink::new(store.clone());
            store.register_attr(NamespaceId(Uuid::from_u128(1)), "Generation".into(), SearchAttrType::Int).await.unwrap();
            let mut older = record(1, 1, 1, 0, false);
            older.context.memo.0.insert("detail".into(), Payload::new(vec![4,5]));
            sink.apply(&older, 0).await.unwrap();
            let mut current = older.clone();
            current.transition_seq = TransitionSeq(7);
            current.context.status_keyword = status.clone();
            current.context.lifecycle_state = if closed { VisibilityLifecycleState::Closed } else { VisibilityLifecycleState::Open };
            current.context.close_time = closed.then_some(OffsetDateTime::UNIX_EPOCH);
            current.context.search_attributes.0.insert("Generation".into(), SearchAttrValue::Int(value));
            sink.apply(&current, 0).await.unwrap();
            sink.apply(&older, 0).await.unwrap();
            let page = handle(&store, 1, 1).list(Default::default()).await.unwrap();
            prop_assert_eq!(page.executions.len(), 1);
            let row = &page.executions[0];
            prop_assert_eq!(&row.status_keyword, &status);
            prop_assert_eq!(row.lifecycle_state, current.context.lifecycle_state);
            prop_assert_eq!(row.source_transition_seq, TransitionSeq(7));
            prop_assert_eq!(&row.search_attributes, &current.context.search_attributes);
            prop_assert_eq!(&row.memo, &current.context.memo);
            Ok(())
        })?;
    }

    // Feature: chasm-extension-visibility, Property 2: continuation positions are confined to their original query scope.
    #[test]
    fn cursors_are_scope_bound(namespace in 3u128..1000, archetype in 3u32..1000, version in 2u8..255) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = InMemoryVisibilityStore::default();
            let sink = VisibilitySink::new(store.clone());
            store.register_attr(NamespaceId(Uuid::from_u128(1)), "Generation".into(), SearchAttrType::Int).await.unwrap();
            for id in 1..=3 { sink.apply(&record(id, 1, 1, 1, false), 0).await.unwrap(); }
            let scoped = handle(&store, 1, 1);
            let token = scoped.list(ComponentQuery { page_size: 1, ..Default::default() }).await.unwrap().next_page_token.unwrap();
            for other in [handle(&store, namespace, 1), handle(&store, 1, archetype)] {
                prop_assert!(matches!(other.list(ComponentQuery { next_page_token: Some(token.clone()), ..Default::default() }).await, Err(ComponentQueryError::InvalidPageToken(_))), "cursor must be rejected");
            }
            prop_assert!(matches!(scoped.list(ComponentQuery { query: Some("Generation = 1".into()), next_page_token: Some(token.clone()), ..Default::default() }).await, Err(ComponentQueryError::InvalidPageToken(_))), "cursor must be rejected");
            let mut cursor: Cursor = serde_json::from_slice(&STANDARD.decode(&token).unwrap()).unwrap();
            cursor.version = version;
            let corrupt = STANDARD.encode(serde_json::to_vec(&cursor).unwrap());
            prop_assert!(matches!(scoped.list(ComponentQuery { next_page_token: Some(corrupt), ..Default::default() }).await, Err(ComponentQueryError::InvalidPageToken(_))), "cursor must be rejected");
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn invalid_inputs_are_named_and_do_not_write() {
    let store = InMemoryVisibilityStore::default();
    store
        .register_attr(
            NamespaceId(Uuid::from_u128(1)),
            "Generation".into(),
            SearchAttrType::Int,
        )
        .await
        .unwrap();
    let view = handle(&store, 1, 1);
    for query in ["Unknown = 1", "Generation = 'wrong'", "("] {
        assert!(matches!(
            view.count(Some(query)).await,
            Err(ComponentQueryError::InvalidQuery(_))
        ));
    }
    for size in [0, MAX_PAGE_SIZE + 1] {
        assert!(matches!(
            view.list(ComponentQuery {
                page_size: size,
                ..Default::default()
            })
            .await,
            Err(ComponentQueryError::InvalidPageSize(_))
        ));
    }
    assert!(matches!(
        view.list(ComponentQuery {
            next_page_token: Some("not a cursor".into()),
            ..Default::default()
        })
        .await,
        Err(ComponentQueryError::InvalidPageToken(_))
    ));
    assert_eq!(view.count(None).await.unwrap(), 0);
}
