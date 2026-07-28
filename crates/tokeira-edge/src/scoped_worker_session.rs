//! Volatile association between a scoped credential and one SDK Worker process.
//!
//! Task completion authority comes only from durable token provenance plus the
//! runtime's existing task fence. This registry protects caller-authored
//! lifecycle coordinates (`worker_instance_key`, sticky/control queues, and
//! identity) from targeting another process. Loss therefore denies shutdown
//! rather than widening access.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokeira_auth::WorkerScope;
use tokeira_types::{NamespaceId, TaskQueueName, WorkerIdentity, WorkerInstanceKey};

const SESSION_TTL: Duration = Duration::minutes(5);

/// Stable identity of one scoped SDK Worker process.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScopedWorkerSessionKey {
    /// Namespace admitted by the edge.
    pub namespace_id: NamespaceId,
    /// Verified JWT subject or IAM ARN.
    pub subject: String,
    /// SDK process-unique Worker key.
    pub worker_instance_key: WorkerInstanceKey,
}

/// Fully authorized poll observation used to establish or refresh a session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedWorkerPollSession {
    /// Exact attenuation attached to the verified credential.
    pub scope: WorkerScope,
    /// Caller-authored SDK identity, fixed after first observation.
    pub worker_identity: WorkerIdentity,
    /// Stable application task-queue family.
    pub normal_task_queue: TaskQueueName,
    /// SDK-generated sticky queue used by this process, when polling sticky.
    pub sticky_task_queue: Option<TaskQueueName>,
    /// Dedicated control queue, when supplied.
    pub worker_control_task_queue: Option<TaskQueueName>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScopedWorkerSession {
    scope: WorkerScope,
    worker_identity: WorkerIdentity,
    normal_task_queue: TaskQueueName,
    worker_control_task_queue: Option<TaskQueueName>,
    sticky_task_queues: HashSet<TaskQueueName>,
    expires_at: OffsetDateTime,
}

/// Fixed reason a scoped session observation was rejected.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ScopedWorkerSessionError {
    /// A required stable Worker-process key was absent.
    #[error("worker session key is missing")]
    MissingKey,
    /// No live poll-established session exists.
    #[error("worker session is missing")]
    MissingSession,
    /// The credential scope differs from the established session.
    #[error("worker session scope differs")]
    Scope,
    /// SDK identity differs from the established session.
    #[error("worker session identity differs")]
    Identity,
    /// Stable normal task queue differs.
    #[error("worker session task queue differs")]
    Queue,
    /// Dedicated control queue conflicts.
    #[error("worker session control queue differs")]
    ControlQueue,
    /// Shutdown named a sticky queue never established by an authorized poll.
    #[error("worker session sticky queue differs")]
    StickyQueue,
}

/// Process-local scoped Worker-session registry.
#[derive(Clone, Debug, Default)]
pub struct ScopedWorkerSessionRegistry {
    sessions: Arc<RwLock<HashMap<ScopedWorkerSessionKey, ScopedWorkerSession>>>,
}

impl ScopedWorkerSessionRegistry {
    /// Establish or monotonically refresh one fully authorized poll session.
    pub fn record_poll(
        &self,
        key: ScopedWorkerSessionKey,
        observation: ScopedWorkerPollSession,
        now: OffsetDateTime,
    ) -> Result<(), ScopedWorkerSessionError> {
        if key.worker_instance_key.0.trim().is_empty() {
            return Err(ScopedWorkerSessionError::MissingKey);
        }
        let mut sessions = self
            .sessions
            .write()
            .expect("worker session registry poisoned");
        prune_expired(&mut sessions, now);
        let expires_at = now + SESSION_TTL;
        match sessions.get_mut(&key) {
            Some(session) => {
                if session.scope != observation.scope {
                    return Err(ScopedWorkerSessionError::Scope);
                }
                if session.worker_identity != observation.worker_identity {
                    return Err(ScopedWorkerSessionError::Identity);
                }
                if session.normal_task_queue != observation.normal_task_queue {
                    return Err(ScopedWorkerSessionError::Queue);
                }
                if let (Some(existing), Some(observed)) = (
                    session.worker_control_task_queue.as_ref(),
                    observation.worker_control_task_queue.as_ref(),
                ) && existing != observed
                {
                    return Err(ScopedWorkerSessionError::ControlQueue);
                }
                if session.worker_control_task_queue.is_none() {
                    session.worker_control_task_queue = observation.worker_control_task_queue;
                }
                if let Some(sticky) = observation.sticky_task_queue {
                    session.sticky_task_queues.insert(sticky);
                }
                session.expires_at = expires_at;
            }
            None => {
                let sticky_task_queues = observation
                    .sticky_task_queue
                    .into_iter()
                    .collect::<HashSet<_>>();
                sessions.insert(
                    key,
                    ScopedWorkerSession {
                        scope: observation.scope,
                        worker_identity: observation.worker_identity,
                        normal_task_queue: observation.normal_task_queue,
                        worker_control_task_queue: observation.worker_control_task_queue,
                        sticky_task_queues,
                        expires_at,
                    },
                );
            }
        }
        Ok(())
    }

    /// Validate one shutdown request against the live poll-established session.
    pub fn authorize_shutdown(
        &self,
        key: &ScopedWorkerSessionKey,
        scope: &WorkerScope,
        worker_identity: &WorkerIdentity,
        normal_task_queue: &TaskQueueName,
        sticky_task_queue: Option<&TaskQueueName>,
        worker_control_task_queue: Option<&TaskQueueName>,
        now: OffsetDateTime,
    ) -> Result<(), ScopedWorkerSessionError> {
        if key.worker_instance_key.0.trim().is_empty() {
            return Err(ScopedWorkerSessionError::MissingKey);
        }
        let mut sessions = self
            .sessions
            .write()
            .expect("worker session registry poisoned");
        prune_expired(&mut sessions, now);
        let session = sessions
            .get(key)
            .ok_or(ScopedWorkerSessionError::MissingSession)?;
        if &session.scope != scope {
            return Err(ScopedWorkerSessionError::Scope);
        }
        if &session.worker_identity != worker_identity {
            return Err(ScopedWorkerSessionError::Identity);
        }
        if &session.normal_task_queue != normal_task_queue {
            return Err(ScopedWorkerSessionError::Queue);
        }
        if let Some(control) = worker_control_task_queue
            && session.worker_control_task_queue.as_ref() != Some(control)
        {
            return Err(ScopedWorkerSessionError::ControlQueue);
        }
        if let Some(sticky) = sticky_task_queue
            && !session.sticky_task_queues.contains(sticky)
        {
            return Err(ScopedWorkerSessionError::StickyQueue);
        }
        Ok(())
    }
}

fn prune_expired(
    sessions: &mut HashMap<ScopedWorkerSessionKey, ScopedWorkerSession>,
    now: OffsetDateTime,
) {
    sessions.retain(|_, session| session.expires_at > now);
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;

    fn scope(queue: &str) -> WorkerScope {
        WorkerScope::try_new(
            "payments".to_owned(),
            vec![queue.to_owned()],
            "payments-deployment".to_owned(),
            "build-a".to_owned(),
        )
        .expect("scope")
    }

    fn key() -> ScopedWorkerSessionKey {
        ScopedWorkerSessionKey {
            namespace_id: NamespaceId(Uuid::nil()),
            subject: "worker-subject".to_owned(),
            worker_instance_key: WorkerInstanceKey("instance-a".to_owned()),
        }
    }

    fn observation(queue: &str, sticky: Option<&str>) -> ScopedWorkerPollSession {
        ScopedWorkerPollSession {
            scope: scope(queue),
            worker_identity: WorkerIdentity("worker-a".to_owned()),
            normal_task_queue: TaskQueueName(queue.to_owned()),
            sticky_task_queue: sticky.map(|value| TaskQueueName(value.to_owned())),
            worker_control_task_queue: Some(TaskQueueName("control-a".to_owned())),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_scoped_worker_session_monotonicity(
            sticky_queues in proptest::collection::btree_set("[a-z]{1,8}", 0..8),
            conflicting_queue in any::<bool>(),
            expires in any::<bool>(),
        ) {
            // Feature: scoped-worker-authorization, Property 9: Scoped Worker-session monotonicity
            let registry = ScopedWorkerSessionRegistry::default();
            let now = OffsetDateTime::UNIX_EPOCH;
            registry
                .record_poll(key(), observation("queue-a", None), now)
                .expect("initial normal-queue poll establishes the session");
            for sticky in &sticky_queues {
                registry
                    .record_poll(key(), observation("queue-a", Some(sticky)), now)
                    .expect("monotonic sticky addition");
            }
            let requested_queue = if conflicting_queue { "queue-b" } else { "queue-a" };
            let shutdown_at = if expires {
                now + SESSION_TTL + Duration::nanoseconds(1)
            } else {
                now
            };
            let sticky = sticky_queues
                .iter()
                .next()
                .map(|value| TaskQueueName(value.clone()));
            let result = registry.authorize_shutdown(
                &key(),
                &scope("queue-a"),
                &WorkerIdentity("worker-a".to_owned()),
                &TaskQueueName(requested_queue.to_owned()),
                sticky.as_ref(),
                Some(&TaskQueueName("control-a".to_owned())),
                shutdown_at,
            );
            let expected = !expires && !conflicting_queue;
            prop_assert_eq!(result.is_ok(), expected);
        }
    }
}
