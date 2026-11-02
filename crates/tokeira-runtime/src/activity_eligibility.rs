//! Execution-owner retry/rule timers reconstructed from authoritative activities.
//!
//! This index never scans dispatch rows or publishes offers. A successful rule
//! evaluation consumes only its captured revision. A small completed descriptor
//! remains until the incarnation changes, preventing unrelated transitions from
//! rearming an already evaluated timer; acquisition rebuilds every obligation.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use time::OffsetDateTime;
use tokeira_kernel::WorkflowState;
use tokeira_storage::{ActivityIncarnation, DispatchableActivityTask, RunRepository};
use tokeira_types::{ExecutionStatus, RunKey, ShardId};

use crate::{
    acquisition_tracking::AcquisitionEntries,
    activity_offers::incarnation,
    outbound::driver::Attempt,
    runtime::{ActivityRetryDeps, activity_dispatch_task, evaluate_activity_eligibility},
    shard::{ShardAcquisition, ShardOwner},
};

const ELIGIBILITY_QUANTUM: usize = 64;
type Position = (OffsetDateTime, ActivityIncarnation);

#[derive(Clone, Debug)]
struct Eligibility {
    task: DispatchableActivityTask,
    home: ShardId,
    due: OffsetDateTime,
    evaluated: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ActivityEligibility {
    entries: AcquisitionEntries<(RunKey, String), Eligibility>,
    cursors: Arc<Mutex<HashMap<(ShardId, u64), Position>>>,
}

impl ActivityEligibility {
    pub(crate) fn set_owner(&self, owner: Arc<RwLock<ShardOwner>>) {
        self.entries.set_owner(owner);
    }

    pub(crate) fn for_acquisition(&self, acquisition: ShardAcquisition) -> Self {
        Self {
            entries: self.entries.for_acquisition(acquisition),
            ..self.clone()
        }
    }

    pub(crate) fn for_activity(&self, run: RunKey, activity: &str) -> Self {
        Self {
            entries: self.entries.for_key(&(run, activity.into())),
            ..self.clone()
        }
    }

    pub(crate) fn refresh(&self, state: &WorkflowState, home: ShardId) {
        self.entries.retain(|(run, id), _| {
            *run != state.run_key
                || (state.status == ExecutionStatus::Running
                    && state.activities.get(id).is_some_and(|activity| {
                        activity.started_at.is_none()
                            && activity.started_event_id.is_none()
                            && activity.pause_info.is_none()
                            && activity.current_attempt_scheduled_at.is_some()
                    }))
        });
        if state.status != ExecutionStatus::Running {
            return;
        }
        for activity in state.activities.values() {
            if activity.started_at.is_some()
                || activity.started_event_id.is_some()
                || activity.pause_info.is_some()
            {
                continue;
            }
            if let Some(due) = activity.current_attempt_scheduled_at {
                self.record(
                    activity_dispatch_task(state.run_key, state, activity),
                    home,
                    due,
                );
            }
        }
    }

    pub(crate) fn record(
        &self,
        task: DispatchableActivityTask,
        home: ShardId,
        due: OffsetDateTime,
    ) {
        self.entries
            .upsert_if((task.run_key, task.activity_id.clone()), home, |previous| {
                if previous.is_some_and(|entry| {
                    // Reset can lower the attempt, but bumps the control stamp;
                    // activity-ID reuse has a later schedule event. Compare in
                    // that order so a late commit reply cannot rearm an older
                    // incarnation over the replacement's due timer.
                    (
                        entry.task.schedule_event_id,
                        entry.task.stamp,
                        entry.task.attempt,
                    ) > (task.schedule_event_id, task.stamp, task.attempt)
                        || (incarnation(&entry.task) == incarnation(&task) && entry.due == due)
                }) {
                    None
                } else {
                    Some(Eligibility {
                        task,
                        home,
                        due,
                        evaluated: false,
                    })
                }
            });
    }

    pub(crate) fn remove(&self, run: RunKey, activity: &str) {
        self.entries.remove(&(run, activity.into()));
    }
    pub(crate) fn remove_run(&self, run: RunKey) {
        self.entries.retain(|(key, _), _| *key != run);
    }
    pub(crate) fn remove_home(&self, home: ShardId) {
        // AcquisitionEntries applies the same captured-generation restriction to
        // retain as to insert; a late handoff cannot erase replacement entries.
        self.entries.retain(|_, entry| entry.home != home);
    }
}

/// Process one independent due slice after timeout submissions. Failures advance
/// the continuation but leave their obligations pending for the next full pass.
pub(crate) async fn scan_activity_eligibility_once<R: RunRepository + 'static>(
    deps: &ActivityRetryDeps<R>,
    home: ShardId,
    now: OffsetDateTime,
) {
    let Some(attempt) = Attempt::capture(&deps.shard_owner, home) else {
        return;
    };
    let tracking = &deps.tracking.eligibility;
    let generation = attempt.acquisition.generation;
    let after = {
        let mut cursors = tracking
            .cursors
            .lock()
            .expect("activity eligibility cursor lock poisoned");
        cursors.retain(|(id, old), _| *id != home || *old == generation);
        cursors.get(&(home, generation)).cloned()
    };
    let mut entries: Vec<_> = tracking
        .entries
        .snapshot(Some(home))
        .into_iter()
        .filter(|entry| {
            !entry.value.evaluated && entry.value.due <= now && tracking.entries.active(entry)
        })
        .collect();
    entries.sort_by_key(|entry| (entry.value.due, incarnation(&entry.value.task)));
    let mut examined = 0;
    let mut last = None;
    for entry in entries
        .into_iter()
        .filter(|entry| {
            after
                .as_ref()
                .is_none_or(|after| (entry.value.due, incarnation(&entry.value.task)) > *after)
        })
        .take(ELIGIBILITY_QUANTUM)
    {
        if attempt.validate(&deps.shard_owner).is_err() {
            return;
        }
        examined += 1;
        last = Some((entry.value.due, incarnation(&entry.value.task)));
        let mut scoped = deps.clone();
        scoped.tracking = deps.tracking.for_acquisition(attempt.acquisition.clone());
        match evaluate_activity_eligibility(&scoped, &entry.value.task, now, &attempt).await {
            Ok(true) => {
                tracking.entries.for_entry(&entry).update(
                    &(
                        entry.value.task.run_key,
                        entry.value.task.activity_id.clone(),
                    ),
                    |current| current.evaluated = true,
                );
            }
            Ok(false) => {}
            Err(error) => tracing::warn!(
                ?error,
                "activity eligibility evaluation failed; retry on next pass"
            ),
        }
    }
    if attempt.validate(&deps.shard_owner).is_err() {
        return;
    }
    let mut cursors = tracking
        .cursors
        .lock()
        .expect("activity eligibility cursor lock poisoned");
    if examined == ELIGIBILITY_QUANTUM {
        cursors.insert(
            (home, generation),
            last.expect("nonempty eligibility quantum has position"),
        );
    } else {
        cursors.remove(&(home, generation));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_types::{NamespaceId, QueueKey, ShardEpoch, TaskKind, TaskQueueName};

    fn task(stamp: u64, attempt: u32) -> DispatchableActivityTask {
        DispatchableActivityTask {
            run_key: RunKey(uuid::Uuid::nil()),
            activity_id: "activity".into(),
            schedule_event_id: 7,
            attempt,
            stamp,
            queue: QueueKey {
                namespace_id: NamespaceId(uuid::Uuid::nil()),
                task_queue: TaskQueueName("queue".into()),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            },
            dispatch_revision: 0,
            priority: None,
            order: None,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: activity-dispatch, Property 6
        // Late timer completions, refreshes and cleanup cannot alter a replacement revision or acquisition.
        #[test]
        fn activity_eligibility_late_callbacks_preserve_replacements(stamp in 1u64..10000, attempt in 2u32..100, takeover in any::<bool>()) {
            let owner = Arc::new(RwLock::new(ShardOwner::new(1)));
            owner.write().unwrap().record_acquired(ShardId(0), ShardEpoch(1));
            owner.write().unwrap().mark_active(ShardId(0));
            let first = owner.read().unwrap().acquisition(ShardId(0)).unwrap();
            let tracking = ActivityEligibility::default();
            tracking.set_owner(owner.clone());
            let old = tracking.for_acquisition(first);
            old.record(task(stamp, attempt), ShardId(0), OffsetDateTime::UNIX_EPOCH);
            let submitted = tracking.entries.snapshot(None).pop().unwrap();
            if takeover {
                owner.write().unwrap().record_acquired(ShardId(0), ShardEpoch(1));
                owner.write().unwrap().mark_active(ShardId(0));
            }
            let replacement = owner.read().unwrap().acquisition(ShardId(0)).unwrap();
            tracking.for_acquisition(replacement).record(task(stamp + 1, 1), ShardId(0), OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1));
            old.record(task(stamp, attempt), ShardId(0), OffsetDateTime::UNIX_EPOCH);
            tracking.entries.for_entry(&submitted).update(&(task(stamp, attempt).run_key, "activity".into()), |entry| entry.evaluated = true);
            if takeover {
                old.remove_run(task(stamp, attempt).run_key);
                old.remove_home(ShardId(0));
            }
            let current = tracking.entries.snapshot(None).pop().unwrap().value;
            assert_eq!((current.task.stamp, current.task.attempt), (stamp + 1, 1));
            assert_eq!(current.due, OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1));
            assert!(!current.evaluated);
        }
    }
}
