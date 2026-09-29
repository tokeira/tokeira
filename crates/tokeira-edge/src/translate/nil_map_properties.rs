//! Snapshot omission across every history event carrying memo/search maps.
use super::*;
use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_kernel::{ContinueAsNewInitiator, ParentClosePolicy};
use tokeira_types::{
    Memo, NamespaceId, Payload, Payloads, RunId, SearchAttrValue, SearchAttributes, TaskQueueName,
    WorkflowId, WorkflowType,
};
use uuid::Uuid;

fn snapshot_events() -> Vec<HistoryEventKind> {
    vec![
        HistoryEventKind::WorkflowExecutionStartedV2 {
            initiator: None,
            workflow_type: WorkflowType("MyWorkflow".to_string()),
            task_queue: TaskQueueName("default".to_string()),
            input: Payloads::default(),
            header: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            request_id: "req-1".to_string(),
            identity: "client".to_string(),
            continued_execution_run_id: None,
            first_execution_run_id: None,
            retry_policy: None,
            attempt: 1,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::seconds(10),
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: None,
            continued_failure: None,
            last_completion_result: None,
            cron_schedule: None,
            versioning_info: None,
            worker_deployment_name: None,
            priority: None,
            eager_execution_accepted: true,
        },
        HistoryEventKind::WorkflowExecutionStarted {
            initiator: None,
            workflow_type: WorkflowType("MyWorkflow".to_string()),
            task_queue: TaskQueueName("default".to_string()),
            input: Payloads::default(),
            header: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            request_id: "req-1".to_string(),
            identity: "client".to_string(),
            continued_execution_run_id: None,
            first_execution_run_id: None,
            retry_policy: None,
            attempt: 1,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::seconds(10),
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: None,
            continued_failure: None,
            last_completion_result: None,
            cron_schedule: None,
            versioning_info: None,
            worker_deployment_name: None,
            priority: None,
        },
        HistoryEventKind::WorkflowExecutionContinuedAsNew {
            workflow_task_completed_event_id: 4,
            new_run_id: RunId(Uuid::from_u128(42)),
            workflow_type: WorkflowType("W".to_string()),
            task_queue: TaskQueueName("q".to_string()),
            input: Payloads::default(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::seconds(10),
            retry_policy: None,
            initiator: ContinueAsNewInitiator::Workflow,
            failure: None,
            last_completion_result: None,
            backoff_start_interval: None,
            cron_schedule: None,
            header: None,
            initial_versioning_behavior: ContinueAsNewVersioningBehavior::Unspecified,
            successor_versioning_info: None,
        },
        HistoryEventKind::StartChildWorkflowExecutionInitiated {
            workflow_task_completed_event_id: 4,
            child_workflow_id: WorkflowId("child-1".to_string()),
            workflow_type: WorkflowType("ChildWf".to_string()),
            task_queue: TaskQueueName("child-q".to_string()),
            input: Payloads::default(),
            namespace_id: NamespaceId(uuid::Uuid::nil()),
            namespace: None,
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::seconds(10),
            retry_policy: None,
            cron_schedule: None,
            parent_close_policy: ParentClosePolicy::Terminate,
            priority: None,
        },
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Feature: v132-lifecycle-fidelity, Property 10: nil-map omission
    // Nil is data-only, and an empty filtered map is absent on every snapshot
    // event (common/payload/payload.go:84-126 @ v1.32.0).
    #[test]
    fn nil_maps_are_absent_on_every_snapshot_event(entries in prop::collection::vec((0u8..5, "[a-z/]{0,20}"), 0..15)) {
        let mut memo = Memo::default();
        let mut search = SearchAttributes::default();
        let mut kept = Vec::new();
        for (index, (kind, encoding)) in entries.into_iter().enumerate() {
            let name = format!("key-{index}");
            let data = match kind { 0 => b"".as_slice(), 1 => b"null", 2 => b"[]", _ => b"kept" };
            let mut payload = Payload::new(data.to_vec());
            payload.metadata.insert("encoding".into(), encoding);
            memo.0.insert(name.clone(), payload);
            search.0.insert(name.clone(), SearchAttrValue::KeywordList(if kind < 3 { Vec::new() } else { vec!["kept".into()] }));
            if kind >= 3 { kept.push(name); }
        }
        kept.sort();
        for mut kind in snapshot_events() {
            match &mut kind {
                HistoryEventKind::WorkflowExecutionStarted { memo: m, search_attributes: a, .. }
                | HistoryEventKind::WorkflowExecutionStartedV2 { memo: m, search_attributes: a, .. }
                | HistoryEventKind::WorkflowExecutionContinuedAsNew { memo: m, search_attributes: a, .. }
                | HistoryEventKind::StartChildWorkflowExecutionInitiated { memo: m, search_attributes: a, .. } => { *m = memo.clone(); *a = search.clone(); },
                _ => unreachable!(),
            }
            let response = history_event_to_proto(&HistoryEvent { event_id: 1, happened_at: OffsetDateTime::UNIX_EPOCH, kind });
            let (memo, search) = match response.attributes.unwrap() {
                history::history_event::Attributes::WorkflowExecutionStartedEventAttributes(a) => (a.memo, a.search_attributes),
                history::history_event::Attributes::WorkflowExecutionContinuedAsNewEventAttributes(a) => (a.memo, a.search_attributes),
                history::history_event::Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(a) => (a.memo, a.search_attributes),
                _ => unreachable!(),
            };
            prop_assert_eq!(memo.is_none(), kept.is_empty());
            prop_assert_eq!(search.is_none(), kept.is_empty());
            if let (Some(memo), Some(search)) = (memo, search) {
                let mut memo_keys = memo.fields.keys().cloned().collect::<Vec<_>>(); memo_keys.sort();
                let mut search_keys = search.indexed_fields.keys().cloned().collect::<Vec<_>>(); search_keys.sort();
                prop_assert_eq!(&memo_keys, &kept);
                prop_assert_eq!(&search_keys, &kept);
            }
        }
    }
}
