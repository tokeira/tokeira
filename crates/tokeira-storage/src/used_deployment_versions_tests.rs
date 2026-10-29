//! A run's `TemporalUsedWorkerDeploymentVersions` list on the in-memory store, as
//! v1.31.0 keeps it: only a recorded workflow task completion adds to it, a live one
//! trims it to the search-attribute value limit, and a reset replays the copied
//! completions (`used-deployment-versions`).

use proptest::prelude::*;
use time::Duration;
use tokeira_kernel::{
    BasicKernel, Command, ContinueAsNewVersioningBehavior, HistoryEvent, HistoryEventKind, Kernel,
    PendingWorkflowTask, VersioningBehavior, WorkflowCommand, WorkflowTaskCompletedRequest,
    WorkflowTaskCompletionLimits, WorkflowTaskType,
    state::{WorkerDeploymentVersionRef, WorkflowVersioningInfo},
};
use tokeira_types::{
    Memo, Payloads, RequestContext, SearchAttrValue, SearchAttributes, ShardEpoch, TaskQueueName,
    WorkerIdentity, WorkflowType,
};

use super::*;
use crate::memory::projection_accumulator_tests::{
    applied, commit, following, fresh_transition, reset_history,
};

const LIST: &str = "TemporalUsedWorkerDeploymentVersions";

pub(crate) fn version(build_id: &str) -> WorkerDeploymentVersionRef {
    WorkerDeploymentVersionRef {
        deployment_name: "d".into(),
        build_id: build_id.into(),
    }
}

/// The list the run's latest image carries, or `None` when it leaves it out.
pub(crate) async fn image_list(store: &InMemoryStore, key: RunKey) -> Option<Vec<String>> {
    let durable = store.inner.lock().await;
    match durable
        .latest_image(key)
        .expect("the run has an image")
        .context
        .search_attributes
        .0
        .get(LIST)
    {
        Some(SearchAttrValue::KeywordList(values)) => Some(values.clone()),
        None => None,
        Some(other) => panic!("unexpected {LIST} value: {other:?}"),
    }
}

/// The length of Go's `json.Marshal` of a list of strings that need no escapes.
fn plain_json_len(list: &[String]) -> usize {
    if list.is_empty() {
        2
    } else {
        1 + 3 * list.len() + list.iter().map(String::len).sum::<usize>()
    }
}

/// `state` with a workflow task scheduled and started, as a schedule and a poll leave
/// it, and the two events that record them.
pub(crate) fn with_started_task(mut state: WorkflowState) -> (WorkflowState, [HistoryEvent; 2]) {
    let logical_seq = state.next_workflow_task_seq;
    let scheduled_event_id = state.last_event_id + 1;
    state.pending_workflow_task = Some(PendingWorkflowTask {
        advice: Default::default(),
        task_type: WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq,
        scheduled_event_id,
        scheduled_at: state.started_at,
        started_event_id: Some(scheduled_event_id + 1),
        started_at: Some(state.started_at),
        attempt: 1,
        target_worker_deployment_version_changed: false,
        target_version_changed_enabled: false,
        target_deployment_version: None,
    });
    state.last_event_id = scheduled_event_id + 1;
    state.next_workflow_task_seq = logical_seq.next();
    let events = [
        HistoryEvent {
            event_id: scheduled_event_id,
            happened_at: state.started_at,
            kind: HistoryEventKind::WorkflowTaskScheduled {
                logical_seq,
                task_queue: state.task_queue.clone(),
                workflow_task_timeout: state.workflow_task_timeout,
                attempt: 1,
            },
        },
        HistoryEvent {
            event_id: scheduled_event_id + 1,
            happened_at: state.started_at,
            kind: HistoryEventKind::WorkflowTaskStarted {
                logical_seq,
                scheduled_event_id,
                attempt: 1,
                identity: WorkerIdentity("worker".into()),
                request_id: format!("poll-{}", logical_seq.0),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                suggest_continue_as_new_reasons: Vec::new(),
                target_worker_deployment_version_changed: false,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            },
        },
    ];
    (state, events)
}

/// A completion of a workflow task started on `state`, reporting `version` with
/// `behavior`, built as the edge builds one.
pub(crate) fn completion(
    state: &WorkflowState,
    version: Option<WorkerDeploymentVersionRef>,
    behavior: VersioningBehavior,
    commands: Vec<WorkflowCommand>,
) -> WorkflowTaskCompletedRequest {
    let pending = state
        .pending_workflow_task
        .as_ref()
        .expect("a started workflow task");
    WorkflowTaskCompletedRequest {
        client_discards_speculative_with_events: false,
        token: tokeira_types::WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.expect("a started workflow task"),
            attempt: pending.attempt,
            shard_epoch: ShardEpoch::ZERO,
        },
        identity: WorkerIdentity("worker".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: behavior,
        worker_deployment_name: version
            .as_ref()
            .map(|version| version.deployment_name.clone()),
        deployment_version: version,
        sticky: None,
        commands,
        force_new_workflow_task: false,
        limits: WorkflowTaskCompletionLimits::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(state.started_at),
        now: state.started_at,
        command_sizes: Vec::new(),
    }
}

/// Start a workflow task on the committed `state` and complete it through the
/// kernel with no commands, with the events that schedule and start it.
pub(crate) fn complete(
    state: &WorkflowState,
    version: Option<WorkerDeploymentVersionRef>,
    behavior: VersioningBehavior,
) -> Transition {
    complete_with(state, version, behavior, Vec::new())
}

/// [`complete`], with `commands`.
pub(crate) fn complete_with(
    state: &WorkflowState,
    version: Option<WorkerDeploymentVersionRef>,
    behavior: VersioningBehavior,
    commands: Vec<WorkflowCommand>,
) -> Transition {
    let (started, events) = with_started_task(state.clone());
    let request = completion(&started, version, behavior, commands);
    let mut transition = BasicKernel
        .apply(
            LoadedRun::Existing(started),
            Command::WorkflowTaskCompleted(request),
        )
        .expect("the completion applies");
    for (index, event) in events.into_iter().enumerate() {
        transition.history_events.insert(index, event);
        transition.event_principals.insert(index, None);
    }
    transition
}

async fn started_run(store: &InMemoryStore, key: RunKey) -> WorkflowState {
    applied(
        commit(store, fresh_transition(key), false, WriteFence::Unfenced)
            .await
            .unwrap(),
    )
}

// Exploration 1.1: a reset run lists each version its copied completions reported.
#[tokio::test]
async fn reset_run_lists_each_copied_completions_version() {
    let store = InMemoryStore::default();
    let base = RunKey::new();
    let mut start = fresh_transition(base);
    start.next_state.status = ExecutionStatus::Completed;
    start.next_state.closed_at = Some(start.next_state.started_at);
    start.history_events = reset_history(&start.next_state).into();
    start.event_principals = vec![None; start.history_events.len()].into();
    start.next_state.last_event_id = 10;
    let base_state = applied(
        commit(&store, start, false, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    let run_id = RunId::new();
    let key = RunKey::derive(base_state.namespace_id, &base_state.workflow_id, run_id);
    let expected = store
        .find_latest_run(base_state.namespace_id, &base_state.workflow_id)
        .await
        .unwrap();
    // The reset point is the third completion, event 10, so the run copies the
    // completions on v1 and v2 and not the one on v3.
    store
        .materialize_reset_successor(base, 10, run_id, expected, WriteFence::Unfenced)
        .await
        .unwrap();
    let LoadedRun::Existing(state) = store.load_run(key).await.unwrap() else {
        panic!("the reset run")
    };
    applied(
        commit(&store, following(&state), false, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    assert_eq!(
        image_list(&store, key).await,
        Some(vec!["deployment:v1".into(), "deployment:v2".into()])
    );
}

// Exploration 1.2: a start that inherits a version lists nothing before a completion.
#[tokio::test]
async fn inherited_version_is_not_listed_before_a_completion() {
    let store = InMemoryStore::default();
    let key = RunKey::new();
    let mut start = fresh_transition(key);
    // A continue-as-new successor of an auto-upgrade run, as `apply_start` stores it.
    start.next_state.versioning_info = Some(WorkflowVersioningInfo {
        behavior: VersioningBehavior::AutoUpgrade,
        deployment_version: Some(version("v1")),
        ..Default::default()
    });
    applied(
        commit(&store, start, false, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    assert_eq!(image_list(&store, key).await, None);
}

// Exploration 1.3: a completion with unspecified versioning behaviour lists its version.
#[tokio::test]
async fn unspecified_behaviour_completion_lists_its_version() {
    let store = InMemoryStore::default();
    let key = RunKey::new();
    let state = started_run(&store, key).await;
    let transition = complete(&state, Some(version("v3")), VersioningBehavior::Unspecified);
    applied(
        commit(&store, transition, false, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    assert_eq!(image_list(&store, key).await, Some(vec!["d:v3".into()]));
}

// Exploration 1.4: a list whose encoding passes the value limit loses its oldest versions.
#[tokio::test]
async fn long_list_is_trimmed_to_the_value_limit() {
    let store = InMemoryStore::default();
    let key = RunKey::new();
    let mut state = started_run(&store, key).await;
    let builds = (0..100)
        .map(|index| format!("build-{index:03}-{}", "x".repeat(20)))
        .collect::<Vec<_>>();
    for build in &builds {
        let transition = complete(
            &state,
            Some(version(build)),
            VersioningBehavior::AutoUpgrade,
        );
        state = applied(
            commit(&store, transition, false, WriteFence::Unfenced)
                .await
                .unwrap(),
        );
    }
    let all = builds
        .iter()
        .map(|build| format!("d:{build}"))
        .collect::<Vec<_>>();
    let listed = image_list(&store, key).await.expect("a list");
    let limit = WorkflowTaskCompletionLimits::default().search_attribute_value_size_limit;
    assert!(plain_json_len(&listed) <= limit, "{} entries", listed.len());
    assert!(all.ends_with(&listed));
    // The longest such suffix: one more version would pass the limit.
    assert!(plain_json_len(&all[all.len() - listed.len() - 1..]) > limit);
}

// The image of a completion that closes the run, or continues it as new, lists the
// completion's version. A successor that inherits the version lists nothing until
// its own completion (exploration 1.2).
#[tokio::test]
async fn a_closing_completion_lists_its_version_in_the_closing_image() {
    let continue_as_new = WorkflowCommand::ContinueAsNew {
        new_run_id: RunId::new(),
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName("queue".into()),
        input: Payloads::default(),
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        header: None,
        initial_versioning_behavior: ContinueAsNewVersioningBehavior::default(),
        successor_versioning_info: None,
    };
    let complete_workflow = WorkflowCommand::CompleteWorkflow {
        result: Payloads::default(),
    };
    for command in [complete_workflow, continue_as_new] {
        let store = InMemoryStore::default();
        let key = RunKey::new();
        let state = started_run(&store, key).await;
        let transition = complete_with(
            &state,
            Some(version("v1")),
            VersioningBehavior::AutoUpgrade,
            vec![command],
        );
        let closed = applied(
            commit(&store, transition, false, WriteFence::Unfenced)
                .await
                .unwrap(),
        );
        assert!(!closed.status.is_open());
        assert_eq!(image_list(&store, key).await, Some(vec!["d:v1".into()]));
    }
}

/// A write to a run: a workflow task completion reporting a version from the pool
/// with a behaviour, a change of the stored deployment version alone, or a write
/// that changes neither.
#[derive(Clone, Debug)]
enum Write {
    Complete(Option<usize>, VersioningBehavior),
    StoredVersion(Option<usize>),
    Other,
}

fn writes() -> impl Strategy<Value = Vec<Write>> {
    let behaviour = prop_oneof![
        Just(VersioningBehavior::Unspecified),
        Just(VersioningBehavior::Pinned),
        Just(VersioningBehavior::AutoUpgrade),
    ];
    prop::collection::vec(
        prop_oneof![
            3 => (prop::option::of(0usize..4), behaviour).prop_map(|(index, behaviour)| {
                // An unversioned worker reports no behaviour.
                Write::Complete(index, if index.is_some() { behaviour } else { VersioningBehavior::Unspecified })
            }),
            1 => prop::option::of(0usize..4).prop_map(Write::StoredVersion),
            1 => Just(Write::Other),
        ],
        1..16,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    // Feature: used-deployment-versions, Property 4: Images carry the list
    #[test]
    fn images_carry_the_list(writes in writes(), inherited in prop::option::of(0usize..4)) {
        let pool = ["v0", "v1", "v2", "v3"].map(version);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = InMemoryStore::default();
            let key = RunKey::new();
            let mut start = fresh_transition(key);
            start.next_state.versioning_info = inherited.map(|index| WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(pool[index].clone()),
                ..Default::default()
            });
            let mut state = applied(commit(&store, start, false, WriteFence::Unfenced).await.unwrap());
            prop_assert_eq!(image_list(&store, key).await, None);
            for write in writes {
                let transition = match write {
                    Write::Complete(index, behaviour) => {
                        complete(&state, index.map(|index| pool[index].clone()), behaviour)
                    }
                    Write::StoredVersion(index) => {
                        let mut transition = following(&state);
                        transition.next_state.apply_wft_versioning(
                            VersioningBehavior::AutoUpgrade,
                            index.map(|index| pool[index].clone()),
                            None,
                        );
                        transition
                    }
                    Write::Other => following(&state),
                };
                state = applied(commit(&store, transition, false, WriteFence::Unfenced).await.unwrap());
                let list = state.used_worker_deployment_versions.clone().expect("a ready list");
                prop_assert_eq!(image_list(&store, key).await, (!list.is_empty()).then_some(list));
            }
            Ok(())
        })?;
    }
}
