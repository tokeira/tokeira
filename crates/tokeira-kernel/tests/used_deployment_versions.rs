//! `used-deployment-versions`: only a recorded workflow task completion adds to a
//! run's `TemporalUsedWorkerDeploymentVersions` list, whatever the run's versioning
//! behaviour; a live completion trims the list to its search-attribute value limit,
//! and a replayed one doesn't.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, ContinueAsNewAdvicePolicy, ContinueAsNewVersioningBehavior, FieldChange,
    HistoryEvent, LoadedRun, PendingWorkflowTask, Reject, ReplayContext, SignalRequest,
    StartDeploymentTransitionRequest, StartRequest, StartWorkflowTaskRequest,
    UpdateExecutionOptionsRequest, VersioningBehavior, VersioningOverride,
    VersioningOverrideChange, WorkflowCommand, WorkflowState, WorkflowTaskCompletedRequest,
    WorkflowTaskCompletionLimits, WorkflowTaskFailedCause, WorkflowTaskFailedRequest,
    WorkflowTaskType, WorkflowVersioningInfo, event::HistoryEventKind, kernel::Kernel,
    payload_size::used_deployment_versions_len, state::WorkerDeploymentVersionRef,
};
use tokeira_types::{
    Memo, NamespaceId, Payloads, RequestContext, RequestId, RunId, RunKey, SearchAttributes,
    ShardEpoch, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
};

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn request(id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.into()),
        caller_identity: Some("tester".into()),
        principal: None,
        received_at: now(),
    }
}

fn version(build_id: &str) -> WorkerDeploymentVersionRef {
    WorkerDeploymentVersionRef {
        deployment_name: "d".into(),
        build_id: build_id.into(),
    }
}

fn entry(version: &WorkerDeploymentVersionRef) -> String {
    format!("{}:{}", version.deployment_name, version.build_id)
}

/// The length of Go's `json.Marshal` of a list of strings that need no escapes.
fn plain_json_len(list: &[String]) -> usize {
    if list.is_empty() {
        2
    } else {
        1 + 3 * list.len() + list.iter().map(String::len).sum::<usize>()
    }
}

/// The model of the list after a live completion reports `version`: append it when
/// absent, then drop the oldest until the list fits `limit` or is empty.
fn live_fold(list: &mut Vec<String>, version: Option<&WorkerDeploymentVersionRef>, limit: usize) {
    if let Some(version) = version
        && !list.contains(&entry(version))
    {
        list.push(entry(version));
        while !list.is_empty() && plain_json_len(list) > limit {
            list.remove(0);
        }
    }
}

/// A run driven through the kernel, with the history its transitions recorded.
struct Run {
    state: WorkflowState,
    history: Vec<HistoryEvent>,
    requests: usize,
}

impl Run {
    /// Start a run, inheriting `inherited` as a continue-as-new successor of an
    /// auto-upgrade run does.
    fn start(inherited: Option<WorkerDeploymentVersionRef>) -> Self {
        let run_id = RunId::new();
        let namespace_id = NamespaceId::new();
        let workflow_id = WorkflowId("used-versions".into());
        let start = StartRequest {
            advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
            initiator: None,
            run_key: RunKey::derive(namespace_id, &workflow_id, run_id),
            namespace_id,
            workflow_id,
            run_id,
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            deployment: None,
            build_id: None,
            versioning_override: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            input: Payloads::default(),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: Some(run_id),
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: request("start"),
            now: now(),
            client_cron_schedule: None,
            cron_schedule: None,
            eager_execution_accepted: false,
            reserved_poller_identity: None,
            inherited_versioning_info: inherited.map(|version| WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(version),
                ..Default::default()
            }),
        };
        let transition = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start))
            .unwrap();
        Self {
            history: transition.history_events.to_vec(),
            state: transition.next_state,
            requests: 0,
        }
    }

    fn list(&self) -> Option<&Vec<String>> {
        self.state.used_worker_deployment_versions.as_ref()
    }

    fn apply(&mut self, command: Command) -> Result<(), Reject> {
        let transition = BasicKernel.apply(LoadedRun::Existing(self.state.clone()), command)?;
        self.history
            .extend(transition.history_events.iter().cloned());
        self.state = transition.next_state;
        Ok(())
    }

    fn next_request(&mut self, kind: &str) -> RequestContext {
        self.requests += 1;
        request(&format!("{kind}-{}", self.requests))
    }

    fn signal(&mut self) {
        let request = self.next_request("signal");
        self.apply(Command::Signal(SignalRequest {
            signal_name: "signal".into(),
            input: Payloads::default(),
            header: None,
            links: Vec::new(),
            request,
            now: now(),
        }))
        .unwrap();
    }

    /// Schedule a workflow task if none is pending, then start it as a poll does.
    fn start_task(&mut self) {
        if self.state.pending_workflow_task.is_none() {
            self.signal();
        }
        let logical_seq = self
            .state
            .pending_workflow_task
            .as_ref()
            .unwrap()
            .logical_seq;
        let request_id = self.next_request("poll").request_id.0;
        self.apply(Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
            advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
            logical_seq,
            worker_identity: WorkerIdentity("worker".into()),
            request_id,
            history_size_bytes: 0,
            deployment_transition: None,
            deployment_transition_revision_number: None,
            target_version_changed_enabled: false,
            target_deployment_version: None,
            polled_task_queue: TaskQueueName("queue".into()),
            now: now(),
        }))
        .unwrap();
    }

    /// Complete the started workflow task, reporting `version` with `behavior`.
    fn complete(
        &mut self,
        version: Option<WorkerDeploymentVersionRef>,
        behavior: VersioningBehavior,
        limit: usize,
        commands: Vec<WorkflowCommand>,
    ) -> Result<(), Reject> {
        let pending = self.state.pending_workflow_task.clone().unwrap();
        self.apply(Command::WorkflowTaskCompleted(
            WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: self.state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                client_discards_speculative_with_events: false,
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
                command_sizes: Vec::new(),
                force_new_workflow_task: false,
                limits: WorkflowTaskCompletionLimits {
                    search_attribute_value_size_limit: limit,
                    ..WorkflowTaskCompletionLimits::default()
                },
                delivered_update_ids: Vec::new(),
                request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                now: now(),
            },
        ))
    }

    /// Start and complete a workflow task with no commands.
    fn completion(
        &mut self,
        version: Option<WorkerDeploymentVersionRef>,
        behavior: VersioningBehavior,
        limit: usize,
    ) {
        self.start_task();
        self.complete(version, behavior, limit, Vec::new()).unwrap();
    }

    /// Fail the started workflow task, as the runtime does when the kernel refuses
    /// a completion.
    fn fail_task(&mut self) {
        let pending = self.state.pending_workflow_task.clone().unwrap();
        let request = self.next_request("fail");
        self.apply(Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            request,
            now: now(),
            reset_reapply: Vec::new(),
            history_size_bytes: 0,
            advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
            terminate_reason: None,
        }))
        .unwrap();
    }

    /// The run a reset materializes from `history`.
    fn replay(&self, history: &[HistoryEvent]) -> WorkflowState {
        BasicKernel
            .replay_history_prefix(
                ReplayContext {
                    run_key: RunKey::new(),
                    namespace_id: self.state.namespace_id,
                    workflow_id: self.state.workflow_id.clone(),
                    run_id: RunId::new(),
                    deployment: None,
                    build_id: None,
                    parent_run_key: None,
                    parent_workflow_id: None,
                    first_run_started_at: self.state.first_run_started_at,
                },
                history,
            )
            .unwrap()
    }
}

fn behaviour() -> impl Strategy<Value = VersioningBehavior> {
    prop_oneof![
        Just(VersioningBehavior::Unspecified),
        Just(VersioningBehavior::Pinned),
        Just(VersioningBehavior::AutoUpgrade),
    ]
}

/// A reported version from a pool of `pool` and the behaviour it is reported with.
/// An unversioned worker reports no behaviour.
fn report(pool: usize) -> impl Strategy<Value = (Option<usize>, VersioningBehavior)> {
    (prop::option::of(0..pool), behaviour()).prop_map(|(index, behaviour)| match index {
        Some(index) => (Some(index), behaviour),
        None => (None, VersioningBehavior::Unspecified),
    })
}

#[derive(Clone, Debug)]
enum Step {
    Complete(Option<usize>, VersioningBehavior),
    Signal,
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(
        prop_oneof![
            3 => report(6).prop_map(|(index, behaviour)| Step::Complete(index, behaviour)),
            1 => Just(Step::Signal),
        ],
        1..20,
    )
}

/// The default value limit, or one a few entries fill.
fn limits() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(WorkflowTaskCompletionLimits::default().search_attribute_value_size_limit),
        2usize..40,
    ]
}

/// Strings that need Go's JSON escapes, and multibyte text.
const ESCAPED: &str = "[ab<>&\"\\\\\u{7}\u{8}\u{2028}é]{0,10}";

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: used-deployment-versions, Property 1: The list is the fold of the completions
    #[test]
    fn the_list_is_the_fold_of_the_completions(
        steps in steps(),
        limit in limits(),
        inherited in prop::option::of(0usize..6),
    ) {
        let pool = (0..6).map(|index| version(&format!("v{index}"))).collect::<Vec<_>>();
        let mut run = Run::start(inherited.map(|index| pool[index].clone()));
        let mut expected = Vec::new();
        prop_assert_eq!(run.list(), Some(&expected));
        for step in steps {
            match step {
                Step::Signal => run.signal(),
                Step::Complete(index, behaviour) => {
                    let version = index.map(|index| pool[index].clone());
                    run.completion(version.clone(), behaviour, limit);
                    live_fold(&mut expected, version.as_ref(), limit);
                }
            }
            prop_assert_eq!(run.list(), Some(&expected));
        }
    }

    // Feature: used-deployment-versions, Property 2: A changed list fits
    #[test]
    fn a_changed_list_fits(
        stored in prop::collection::vec(ESCAPED, 0..40),
        name in ESCAPED,
        build in ESCAPED,
        limit in 2usize..400,
    ) {
        let mut run = Run::start(None);
        run.state.used_worker_deployment_versions = Some(stored.clone());
        let reported = WorkerDeploymentVersionRef { deployment_name: name, build_id: build };
        run.completion(Some(reported.clone()), VersioningBehavior::AutoUpgrade, limit);
        let listed = run.list().unwrap().clone();
        if stored.contains(&entry(&reported)) {
            // An unchanged list isn't trimmed, even over the limit.
            prop_assert_eq!(listed, stored);
        } else {
            let mut untrimmed = stored;
            untrimmed.push(entry(&reported));
            prop_assert!(untrimmed.ends_with(&listed));
            prop_assert!(listed.is_empty() || used_deployment_versions_len(&listed) <= limit);
            // The longest suffix that fits: one more entry would pass the limit.
            if listed.len() < untrimmed.len() {
                let longer = &untrimmed[untrimmed.len() - listed.len() - 1..];
                prop_assert!(used_deployment_versions_len(longer) > limit);
            }
        }
    }

    // Feature: used-deployment-versions, Property 3: Replay doesn't trim
    #[test]
    fn replay_does_not_trim(
        completions in prop::collection::vec(report(40), 1..24),
        cut in any::<prop::sample::Index>(),
    ) {
        // Entries of about 400 bytes, so five or more distinct versions pass the value
        // limit: the live list trims, and the replayed one mustn't.
        let pool = (0..40)
            .map(|index| version(&format!("build-{index:02}-{}", "x".repeat(400))))
            .collect::<Vec<_>>();
        let limit = WorkflowTaskCompletionLimits::default().search_attribute_value_size_limit;
        let mut run = Run::start(None);
        for (index, behaviour) in &completions {
            run.completion(index.map(|index| pool[index].clone()), *behaviour, limit);
        }
        let prefix = &run.history[..cut.index(run.history.len()) + 1];
        let mut expected = Vec::new();
        for event in prefix {
            if let HistoryEventKind::WorkflowTaskCompleted { deployment_version: Some(version), .. } = &event.kind
                && !expected.contains(&entry(version))
            {
                expected.push(entry(version));
            }
        }
        prop_assert_eq!(run.replay(prefix).used_worker_deployment_versions, Some(expected));
    }
}

#[test]
fn a_present_version_adds_nothing_and_does_not_trim() {
    let mut state = Run::start(None).state;
    let stored = vec!["d:v0".to_owned(), "d:v1".to_owned()];
    state.used_worker_deployment_versions = Some(stored.clone());
    state.add_used_deployment_version(Some(&version("v0")), Some(2));
    assert_eq!(state.used_worker_deployment_versions, Some(stored));
}

#[test]
fn an_appended_version_trims_the_oldest_and_keeps_the_newest() {
    let mut state = Run::start(None).state;
    state.used_worker_deployment_versions = Some(vec![
        "d:v0".to_owned(),
        "d:v1".to_owned(),
        "d:v2".to_owned(),
    ]);
    // `["d:v2","d:v3"]` is 15 bytes.
    state.add_used_deployment_version(Some(&version("v3")), Some(15));
    assert_eq!(
        state.used_worker_deployment_versions,
        Some(vec!["d:v2".to_owned(), "d:v3".to_owned()])
    );
}

#[test]
fn a_limit_no_entry_fits_under_empties_the_list() {
    let mut state = Run::start(None).state;
    state.used_worker_deployment_versions = Some(vec!["d:v0".to_owned()]);
    // `["d:v1"]` is 8 bytes.
    state.add_used_deployment_version(Some(&version("v1")), Some(7));
    assert_eq!(state.used_worker_deployment_versions, Some(Vec::new()));
}

#[test]
fn an_unseeded_list_and_an_unversioned_report_add_nothing() {
    let mut state = Run::start(None).state;
    state.used_worker_deployment_versions = None;
    state.add_used_deployment_version(Some(&version("v0")), Some(2048));
    assert_eq!(state.used_worker_deployment_versions, None);
    state.used_worker_deployment_versions = Some(vec!["d:v0".to_owned()]);
    state.add_used_deployment_version(None, Some(2048));
    assert_eq!(
        state.used_worker_deployment_versions,
        Some(vec!["d:v0".to_owned()])
    );
}

#[test]
fn the_live_completion_trims_to_its_limit_and_replay_does_not() {
    let mut run = Run::start(None);
    // `["d:vN"]` is 8 bytes, so a limit of 8 keeps only the newest.
    run.completion(Some(version("v0")), VersioningBehavior::AutoUpgrade, 8);
    run.completion(Some(version("v1")), VersioningBehavior::AutoUpgrade, 8);
    assert_eq!(run.list(), Some(&vec!["d:v1".to_owned()]));
    assert_eq!(
        run.replay(&run.history).used_worker_deployment_versions,
        Some(vec!["d:v0".to_owned(), "d:v1".to_owned()])
    );
}

#[test]
fn an_unspecified_behaviour_completion_appends_and_an_unversioned_one_does_not() {
    let mut run = Run::start(None);
    run.completion(Some(version("v3")), VersioningBehavior::Unspecified, 2048);
    assert_eq!(run.list(), Some(&vec!["d:v3".to_owned()]));
    run.completion(None, VersioningBehavior::Unspecified, 2048);
    assert_eq!(run.list(), Some(&vec!["d:v3".to_owned()]));
}

#[test]
fn a_closing_completion_lists_its_version_and_a_successor_starts_with_none() {
    let mut completed = Run::start(None);
    completed.start_task();
    completed
        .complete(
            Some(version("v1")),
            VersioningBehavior::AutoUpgrade,
            2048,
            vec![WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            }],
        )
        .unwrap();
    assert!(!completed.state.status.is_open());
    assert_eq!(completed.list(), Some(&vec!["d:v1".to_owned()]));

    let mut continued = Run::start(None);
    continued.start_task();
    continued
        .complete(
            Some(version("v1")),
            VersioningBehavior::AutoUpgrade,
            2048,
            vec![WorkflowCommand::ContinueAsNew {
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
            }],
        )
        .unwrap();
    assert!(!continued.state.status.is_open());
    assert_eq!(continued.list(), Some(&vec!["d:v1".to_owned()]));
    // The successor of an auto-upgrade run inherits its version and lists nothing.
    assert_eq!(Run::start(Some(version("v1"))).list(), Some(&Vec::new()));
}

#[test]
fn a_completion_that_fails_its_task_leaves_the_list() {
    let mut run = Run::start(None);
    run.completion(Some(version("v0")), VersioningBehavior::AutoUpgrade, 2048);
    run.start_task();
    let refused = run.complete(
        Some(version("v1")),
        VersioningBehavior::AutoUpgrade,
        2048,
        vec![
            WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            },
            WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            },
        ],
    );
    assert!(matches!(refused, Err(Reject::CommandsAfterClose { .. })));
    run.fail_task();
    assert_eq!(run.list(), Some(&vec!["d:v0".to_owned()]));
}

#[test]
fn a_dropped_speculative_completion_leaves_the_list() {
    let mut run = Run::start(None);
    run.completion(Some(version("v0")), VersioningBehavior::AutoUpgrade, 2048);
    // A speculative task records no events until it completes.
    let last_event_id = run.state.last_event_id;
    let logical_seq = run.state.next_workflow_task_seq;
    run.state.pending_workflow_task = Some(PendingWorkflowTask {
        advice: Default::default(),
        task_type: WorkflowTaskType::Speculative,
        schedule_to_start_deadline: None,
        logical_seq,
        scheduled_event_id: last_event_id + 1,
        scheduled_at: now(),
        started_event_id: Some(last_event_id + 2),
        started_at: Some(now()),
        attempt: 1,
        target_worker_deployment_version_changed: false,
        target_version_changed_enabled: false,
        target_deployment_version: None,
    });
    run.state.next_workflow_task_seq = logical_seq.next();
    let events = run.history.len();
    run.complete(
        Some(version("v1")),
        VersioningBehavior::AutoUpgrade,
        2048,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(run.history.len(), events, "the completion is dropped");
    assert!(run.state.pending_workflow_task.is_none());
    assert_eq!(run.list(), Some(&vec!["d:v0".to_owned()]));
}

#[test]
fn an_override_and_a_transition_leave_the_list() {
    let mut run = Run::start(None);
    run.completion(Some(version("v0")), VersioningBehavior::AutoUpgrade, 2048);
    let request = run.next_request("options");
    run.apply(Command::UpdateExecutionOptions(
        UpdateExecutionOptionsRequest {
            versioning_override: VersioningOverrideChange::Set(VersioningOverride::Pinned {
                version: version("v7"),
            }),
            completion_callbacks: FieldChange::Unchanged,
            attached_completion_callbacks: Vec::new(),
            attached_links: Vec::new(),
            attached_request_id: None,
            request,
            now: now(),
            priority: FieldChange::Unchanged,
            completion_callback_limit: None,
        },
    ))
    .unwrap();
    assert_eq!(run.list(), Some(&vec!["d:v0".to_owned()]));

    let mut run = Run::start(None);
    run.completion(Some(version("v0")), VersioningBehavior::AutoUpgrade, 2048);
    run.apply(Command::StartDeploymentTransition(
        StartDeploymentTransitionRequest {
            target: version("v8"),
            revision_number: 1,
            now: now(),
        },
    ))
    .unwrap();
    assert_eq!(
        run.state
            .versioning_info
            .as_ref()
            .and_then(|info| info.version_transition.clone()),
        Some(version("v8"))
    );
    assert_eq!(run.list(), Some(&vec!["d:v0".to_owned()]));
}
