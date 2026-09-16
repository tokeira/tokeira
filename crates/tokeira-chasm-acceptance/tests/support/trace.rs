//! Exact clock traces with one permitted normalization: executor-minted run UUIDs.
#![cfg(test)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use prost::Message;
use tokeira_chasm::{ChasmNode, Component, ExecutionKey, Task, archetype_id_for_fqn};
use tokeira_chasm_acceptance::{
    Resource, ResourceState,
    tasks::{RetryTimer, backoff},
};
use tokeira_chasm_activity::{ActivityEvent, ActivityExecution, ActivityState};
use tokeira_edge::chasm_activity::ActivityDispatchSnapshot;
use tokeira_engine::chasm::TypedEngine;
use tokeira_proto::common::RetryPolicy;
use tokeira_storage::{ChasmNodeRepository, InMemoryChasmNodeStore};
use uuid::Uuid;

use crate::{
    commands,
    fixture::{Clock, QUEUE, SECOND, Stack, failure, key, reference, target},
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Trace {
    roots: Vec<(ExecutionKey, ChasmNode)>,
    armed: Vec<(ExecutionKey, i64)>,
    queued: Vec<ActivityDispatchSnapshot>,
}

#[derive(Default)]
struct Normalizer {
    runs: BTreeMap<String, String>,
    staged: BTreeSet<String>,
}

fn delayed_key() -> ExecutionKey {
    ExecutionKey::new(
        key().namespace_id,
        "clock-delayed",
        "00000000-0000-0000-0000-000000000003",
    )
}

impl Normalizer {
    fn key(&self, key: &ExecutionKey) -> ExecutionKey {
        if key == &crate::fixture::key() || key == &delayed_key() {
            return key.clone();
        }
        let activity = self
            .runs
            .get(&key.run_id)
            .expect("every minted run id must map to a staged activity");
        assert_eq!(&key.business_id, activity);
        ExecutionKey {
            run_id: format!("staged:{activity}"),
            ..key.clone()
        }
    }

    async fn capture(
        &mut self,
        stack: &Stack,
        store: &InMemoryChasmNodeStore,
        clock: &Clock,
    ) -> Trace {
        let roots = store.scan_executions().await.unwrap();
        for (_, node) in &roots {
            if node.metadata.component_type_id == archetype_id_for_fqn(Resource::FQN) {
                let state = ResourceState::decode(node.data.as_ref().unwrap().as_slice()).unwrap();
                self.staged.extend(
                    state
                        .active_operation
                        .iter()
                        .chain(state.history.iter())
                        .map(|operation| operation.activity_id.clone()),
                );
            }
        }
        for (key, node) in &roots {
            if node.metadata.component_type_id == archetype_id_for_fqn(ActivityExecution::FQN)
                && key != &delayed_key()
            {
                let state = ActivityState::decode(node.data.as_ref().unwrap().as_slice()).unwrap();
                assert!(
                    self.staged.contains(&state.activity_id),
                    "normalizer encountered an unstaged activity"
                );
                assert_eq!(key.business_id, state.activity_id);
                Uuid::parse_str(&key.run_id).unwrap();
                if let Some(previous) = self
                    .runs
                    .insert(key.run_id.clone(), state.activity_id.clone())
                {
                    assert_eq!(previous, state.activity_id);
                }
            }
        }
        let readings = clock.readings();
        let observed = |time: i64| {
            if time != 0 {
                assert!(
                    readings.contains(&time),
                    "timestamp {time} was not read from the injected clock"
                );
            }
        };
        for (_, node) in &roots {
            if node.metadata.component_type_id == archetype_id_for_fqn(Resource::FQN) {
                let state = ResourceState::decode(node.data.as_ref().unwrap().as_slice()).unwrap();
                for operation in state.active_operation.iter().chain(state.history.iter()) {
                    observed(operation.started_at_nanos);
                    observed(operation.finished_at_nanos);
                }
                for task in &node.metadata.outbox.pure_tasks {
                    let timer = RetryTimer::decode(&task.payload).unwrap();
                    let completed = state.history.last().unwrap().finished_at_nanos;
                    assert_eq!(timer.fire_at_nanos, completed + backoff(timer.attempt));
                    assert_eq!(task.fire_at_unix_nanos, Some(timer.fire_at_nanos));
                }
            } else {
                let state = ActivityState::decode(node.data.as_ref().unwrap().as_slice()).unwrap();
                for time in [
                    state.scheduled_time_nanos,
                    state.started_time_nanos,
                    state.close_time_nanos,
                    state.last_attempt_complete_time_nanos,
                ] {
                    observed(time);
                }
                observed(state.attempt_scheduled_time_nanos - state.current_retry_interval_nanos);
                for callback in &state.callbacks {
                    observed(callback.registration_time_nanos);
                    observed(callback.last_attempt_complete_time_nanos);
                    assert_eq!(callback.next_attempt_time_nanos, 0);
                }
                for task in &node.metadata.outbox.pure_tasks {
                    assert_eq!(
                        task.fire_at_unix_nanos,
                        Some(state.started_time_nanos + state.start_to_close_nanos)
                    );
                }
                for task in &node.metadata.outbox.side_effect_tasks {
                    if let Some(deadline) = task.fire_at_unix_nanos {
                        assert_eq!(deadline, state.attempt_scheduled_time_nanos);
                        observed(deadline - state.current_retry_interval_nanos);
                    }
                }
            }
        }
        let mut armed = stack.engine.armed_timers_snapshot();
        for (key, deadline) in &mut armed {
            let node = &roots.iter().find(|(root, _)| root == key).unwrap().1;
            assert_eq!(
                Some(*deadline),
                node.metadata
                    .outbox
                    .pure_tasks
                    .iter()
                    .filter_map(|task| task.fire_at_unix_nanos)
                    .min()
            );
            *key = self.key(key);
        }
        let mut queued = stack.queue.snapshot();
        for entry in &mut queued {
            let node = &roots.iter().find(|(root, _)| root == &entry.key).unwrap().1;
            let state = ActivityState::decode(node.data.as_ref().unwrap().as_slice()).unwrap();
            assert_eq!(entry.stamp, state.stamp);
            assert_eq!(entry.target, state.version_target);
            if let Some(deadline) = entry.fire_at {
                assert_eq!(deadline, state.attempt_scheduled_time_nanos);
                observed(deadline - state.current_retry_interval_nanos);
            }
            entry.key = self.key(&entry.key);
        }
        // Node data and metadata are deliberately untouched, including outbox
        // payloads, timestamps, transition counts and lifecycle. Unknown identities
        // above panic; normalization cannot hide arbitrary nondeterministic fields.
        let mut roots: Vec<_> = roots
            .into_iter()
            .map(|(key, node)| (self.key(&key), node))
            .collect();
        roots.sort_by(|(a, _), (b, _)| {
            (&a.namespace_id, &a.business_id, &a.run_id).cmp(&(
                &b.namespace_id,
                &b.business_id,
                &b.run_id,
            ))
        });
        Trace {
            roots,
            armed,
            queued,
        }
    }
}

async fn delayed_dispatch_probe(stack: &Stack) {
    let data = ActivityState {
        activity_id: delayed_key().business_id,
        activity_type: "clock-probe".into(),
        task_queue: "delayed-queue".into(),
        start_to_close_nanos: 30 * SECOND,
        maximum_attempts: 2,
        retry_initial_interval_nanos: 2 * SECOND,
        retry_maximum_interval_nanos: 2 * SECOND,
        retry_backoff_coefficient: 2.0,
        retry_policy: RetryPolicy {
            maximum_attempts: 2,
            ..Default::default()
        }
        .encode_to_vec(),
        ..Default::default()
    };
    TypedEngine::<ActivityExecution>::new(stack.engine.clone())
        .start_with(
            delayed_key(),
            data,
            Some("clock-probe".into()),
            Default::default(),
            |activity, ctx| activity.apply(ActivityEvent::Scheduled, ctx),
        )
        .await
        .unwrap();
    let task = stack
        .bridge
        .poll_activity_task("delayed-queue", "clock-probe", None)
        .await
        .unwrap()
        .unwrap();
    let failure = failure();
    stack
        .bridge
        .respond_activity_task_failed(
            &task.task_token,
            &key().namespace_id,
            failure.message.clone(),
            failure.encode_to_vec(),
            vec![],
            "clock-probe".into(),
        )
        .await
        .unwrap();
    assert!(
        stack
            .queue
            .snapshot()
            .iter()
            .any(|entry| entry.key == delayed_key()
                && entry.fire_at == Some(stack.engine.now() + 2 * SECOND))
    );
}

pub(crate) async fn run(now: i64, script: &[(u8, u8)]) -> Vec<Trace> {
    let store = Arc::new(InMemoryChasmNodeStore::new());
    let clock = Clock::new(now);
    let mut stack = Stack::new(store.clone(), &clock, true);
    let mut normalizer = Normalizer::default();
    let mut trace = vec![];
    delayed_dispatch_probe(&stack).await;
    trace.push(normalizer.capture(&stack, &store, &clock).await);
    // Every generated case traverses a callback and a pure retry deadline before
    // exploring arbitrary interleavings, so clock assertions cannot pass vacuously.
    let prefix = [(0, 0), (1, 0), (3, 0), (4, 1), (5, 0), (2, 0)];
    for &(action, argument) in prefix.iter().chain(script) {
        match action {
            0 => {
                commands::create(&stack.handle(), key(), "r1", "initial", target(), QUEUE)
                    .await
                    .unwrap();
            }
            1 => {
                if let Ok(view) = commands::read(&stack.handle(), &reference(key())).await {
                    commands::update(
                        &stack.handle(),
                        &reference(key()),
                        view.desired_generation,
                        &format!("digest-{argument}"),
                    )
                    .await
                    .unwrap();
                }
            }
            2 | 3 => {
                if let Some(task) = stack.poll().await {
                    trace.push(normalizer.capture(&stack, &store, &clock).await);
                    stack.complete(&task, action == 3).await;
                }
            }
            4 => clock.set(clock.now() + i64::from(argument) * SECOND),
            5 => {
                stack.sweeper.sweep_once().await;
            }
            6 => {
                let weak = Arc::downgrade(&stack.engine);
                drop(stack);
                assert!(weak.upgrade().is_none());
                stack = Stack::new(store.clone(), &clock, true);
                stack.rebuild.rebuild_once().await.unwrap();
            }
            7 => {
                stack.rebuild.rebuild_once().await.unwrap();
            }
            _ => unreachable!(),
        }
        trace.push(normalizer.capture(&stack, &store, &clock).await);
    }
    trace
}

#[test]
#[should_panic(expected = "every minted run id must map to a staged activity")]
fn normalizer_rejects_unmapped_run_ids() {
    Normalizer::default().key(&ExecutionKey::new("ns", "unknown", "unmapped"));
}
