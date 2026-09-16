//! Edge cases for the acceptance-owned state machine and typed visibility.
#![cfg(test)]

use prost::Message;
use tokeira_chasm::{
    ExecutionKey, Lifecycle, LifecycleState, Registry, TerminateReason, VisibilityContributor,
};
use tokeira_chasm_acceptance::{
    AcceptanceLibrary, OperationOutcome, ReconcileInput, Resource, ResourceState,
    reconcile::{ReconcileHandler, RetryHandler, stage_start},
    tasks::{RetryTimer, backoff},
};
use tokeira_engine::chasm::{
    ChasmError, Component, ComponentRef, Context, DeploymentVersionTarget, EngineComponent,
    Library, MutableContext, PureTaskHandler, RegistryBuilder, RootComponent, SearchAttrKind,
    SearchAttributeDef, SideEffectExecutor, SideEffectTaskHandler, StartActivityTask, Task, TaskId,
    TaskKind, TaskOutcome, TaskValidity, TypedEngine,
};
use tokeira_proto::{
    common::{Payloads, RetryPolicy},
    failure::Failure,
};

use tokeira_types::SearchAttrValue;

fn state() -> ResourceState {
    ResourceState {
        create_request_id: "r1".into(),
        create_digest: "first".into(),
        desired_digest: "latest".into(),
        desired_generation: 2,
        observed_generation: 1,
        target: DeploymentVersionTarget {
            deployment_name: "acceptance".into(),
            build_id: "v1".into(),
        },
        task_queue: "queue".into(),
        ..Default::default()
    }
}

struct TestContext {
    key: ExecutionKey,
    now: i64,
    tasks: Vec<Vec<u8>>,
}
impl Context for TestContext {
    fn execution_key(&self) -> &ExecutionKey {
        &self.key
    }
    fn execution_info(&self) -> tokeira_chasm::ExecutionInfo {
        Default::default()
    }
    fn now_unix_nanos(&self) -> i64 {
        self.now
    }
}
impl MutableContext for TestContext {
    fn resolve_task(&mut self, _: TaskId) {}
    fn add_task(
        &mut self,
        _: TaskKind,
        _: u32,
        payload: Vec<u8>,
        _: Option<i64>,
    ) -> Result<(), ChasmError> {
        self.tasks.push(payload);
        Ok(())
    }
    fn mark_dirty(&mut self) -> Result<(), ChasmError> {
        Ok(())
    }
}
impl TestContext {
    fn take_staged_tasks(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.tasks)
    }
}
fn context(now: i64) -> TestContext {
    TestContext {
        key: ExecutionKey::new("ns", "resource", "run"),
        now,
        tasks: vec![],
    }
}

#[test]
fn engine_reexports_accept_the_library_handlers_and_executor_types() {
    fn root<C: Component + EngineComponent + RootComponent>() {}
    fn pure<H: PureTaskHandler>() {}
    fn effect<H: SideEffectTaskHandler>() {}
    fn executor<E: SideEffectExecutor>() {}
    root::<Resource>();
    pure::<RetryHandler>();
    effect::<ReconcileHandler>();
    executor::<tokeira_edge::chasm_executors::StartActivityExecutor>();
    let mut builder: RegistryBuilder = Registry::builder();
    let registered: Result<(), ChasmError> = <AcceptanceLibrary as Library>::register(&mut builder);
    registered.unwrap();
    let expected = SearchAttributeDef {
        name: "DesiredGeneration",
        kind: SearchAttrKind::Int,
    };
    assert_eq!(expected.kind, SearchAttrKind::Int);
    let _: Option<TypedEngine<Resource>> = None;
    let reference: ComponentRef = ComponentRef::new(
        ExecutionKey::new("ns", "id", "run"),
        1,
        Default::default(),
        vec![],
        Default::default(),
    );
    assert_eq!(
        ComponentRef::decode(&reference.encode().unwrap()).unwrap(),
        reference
    );
    let mut ctx = context(123);
    let write: &mut dyn MutableContext = &mut ctx;
    write.resolve_task(TaskId::new(Default::default(), 0));
    let read: &dyn Context = write;
    assert_eq!(read.now_unix_nanos(), 123);
    assert_eq!(RetryTimer::KIND, TaskKind::Pure);
}

#[test]
fn start_payload_and_validator_capture_one_operation() {
    let mut state = state();
    let mut ctx = context(123);
    stage_start(&mut state, &mut ctx).unwrap();
    let task = ctx.take_staged_tasks().pop().unwrap();
    let task = <StartActivityTask as Task>::decode(&task).unwrap();
    assert_eq!(task.activity_id, "resource/gen-2/try-1");
    assert_eq!(task.version_target, Some(state.target.clone()));
    assert_eq!(
        RetryPolicy::decode(task.retry_policy.as_slice())
            .unwrap()
            .maximum_attempts,
        1
    );
    let input = Payloads::decode(task.input.as_slice()).unwrap();
    assert_eq!(input.payloads.len(), 1);
    assert_eq!(
        ReconcileInput::decode(input.payloads[0].data.as_slice())
            .unwrap()
            .generation,
        2
    );
    assert_eq!(task.start_to_close_nanos, 30_000_000_000);
    assert_eq!(
        (
            task.schedule_to_start_nanos,
            task.schedule_to_close_nanos,
            task.heartbeat_nanos
        ),
        (0, 0, 0)
    );
    let component = Resource::from_data(state);
    assert_eq!(
        ReconcileHandler.validate(&component, &task, &ctx),
        TaskValidity::Valid
    );
    let stale = StartActivityTask {
        activity_id: "old".into(),
        ..task
    };
    assert_eq!(
        ReconcileHandler.validate(&component, &stale, &ctx),
        TaskValidity::Drop
    );
    let snapshot = component.visibility_snapshot().unwrap();
    assert_eq!(
        snapshot.search_attributes.0["DesiredGeneration"],
        SearchAttrValue::Int(2)
    );
    assert_eq!(
        snapshot.search_attributes.0["DeploymentStatus"],
        SearchAttrValue::Keyword("Reconciling".into())
    );
}

#[test]
fn completing_an_older_generation_stages_the_latest_without_claiming_it_observed() {
    let mut state = state();
    let mut ctx = context(100);
    stage_start(&mut state, &mut ctx).unwrap();
    state.desired_generation = 4;
    let mut component = Resource::from_data(state);
    let outcome = TaskOutcome::Completed { payload: vec![] };
    ReconcileHandler
        .on_outcome(
            &mut component,
            &StartActivityTask::default(),
            &outcome,
            &mut context(200),
        )
        .unwrap();
    let state = component.state().unwrap();
    assert_eq!(state.observed_generation, 2);
    assert_eq!(state.active_operation.as_ref().unwrap().generation, 4);
    assert_eq!(state.history[0].finished_at_nanos, 200);
}

#[test]
fn every_unsuccessful_outcome_is_structured_and_retry_timers_are_fenced() {
    for outcome in [
        TaskOutcome::Failed {
            failure: Failure {
                message: "failed".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            retryable: false,
        },
        TaskOutcome::Canceled { details: vec![] },
        TaskOutcome::TimedOut { timeout_type: 1 },
        TaskOutcome::Terminated,
    ] {
        let mut state = state();
        stage_start(&mut state, &mut context(100)).unwrap();
        state.desired_generation = 3;
        let mut component = Resource::from_data(state);
        let mut ctx = context(200);
        ReconcileHandler
            .on_outcome(
                &mut component,
                &StartActivityTask::default(),
                &outcome,
                &mut ctx,
            )
            .unwrap();
        let state = component.state().unwrap();
        assert_eq!(state.observed_generation, 1);
        assert_eq!(state.history[0].outcome(), OperationOutcome::Failed);
        assert_eq!(state.last_failure, state.history[0].failure);
        assert!(
            !Failure::decode(state.last_failure.as_slice())
                .unwrap()
                .message
                .is_empty()
        );
        let timer = RetryTimer::decode(&ctx.take_staged_tasks().pop().unwrap()).unwrap();
        assert_eq!(
            timer,
            RetryTimer {
                generation: 3,
                attempt: 1,
                fire_at_nanos: 200 + backoff(1)
            }
        );
        assert_eq!(
            RetryHandler.validate(&component, &timer, &ctx),
            TaskValidity::Valid
        );
        assert_eq!(
            RetryHandler.validate(
                &component,
                &RetryTimer {
                    generation: 2,
                    ..timer.clone()
                },
                &ctx
            ),
            TaskValidity::Drop
        );
        assert_eq!(
            RetryHandler.validate(
                &component,
                &RetryTimer {
                    attempt: 0,
                    ..timer.clone()
                },
                &ctx
            ),
            TaskValidity::Drop
        );
        RetryHandler
            .execute(&mut component, &timer, &mut ctx)
            .unwrap();
        assert_eq!(
            component
                .state()
                .unwrap()
                .active_operation
                .as_ref()
                .unwrap()
                .activity_id,
            "resource/gen-3/try-2"
        );
        assert_eq!(
            RetryHandler.validate(&component, &timer, &ctx),
            TaskValidity::Drop
        );
    }
}

#[test]
fn terminal_history_is_bounded_and_termination_keeps_the_resource_live() {
    let mut state = state();
    for generation in 2..=12 {
        state.desired_generation = generation;
        stage_start(&mut state, &mut context(100)).unwrap();
        let mut component = Resource::from_data(state);
        ReconcileHandler
            .on_outcome(
                &mut component,
                &StartActivityTask::default(),
                &TaskOutcome::Completed { payload: vec![] },
                &mut context(200),
            )
            .unwrap();
        state = component.into_data();
    }
    assert_eq!(
        state
            .history
            .iter()
            .map(|op| op.generation)
            .collect::<Vec<_>>(),
        (5..=12).collect::<Vec<_>>()
    );
    state.desired_generation = 13;
    stage_start(&mut state, &mut context(300)).unwrap();
    let mut component = Resource::from_data(state);
    component
        .terminate(
            &mut context(400),
            &TerminateReason {
                reason: "operator stop".into(),
                identity: None,
                details: None,
            },
        )
        .unwrap();
    assert_eq!(
        component.lifecycle_state(&context(400)),
        LifecycleState::Running
    );
    let state = component.state().unwrap();
    assert_eq!(state.history.len(), 8);
    assert_eq!(
        state.history.last().unwrap().outcome(),
        OperationOutcome::Failed
    );
    assert!(state.active_operation.is_none());
    assert_eq!(
        Failure::decode(state.last_failure.as_slice())
            .unwrap()
            .message,
        "operator stop"
    );
}
