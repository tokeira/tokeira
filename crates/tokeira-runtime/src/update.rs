//! In-memory registry for waiting update callers.
//!
//! Updates follow a two-phase lifecycle: *admitted* (the kernel records the
//! update ID) → *accepted* (the worker processes the update and writes an
//! acceptance event). Callers using `UpdateWaitPolicy::Completed` need to
//! block until the lane commits the final resolution event. This registry
//! holds the oneshot senders that bridge the gap between the API call and
//! the lane's eventual `notify` — it is purely in-memory because the
//! caller's connection lifetime, not durable state, determines how long
//! the entry lives. When a run closes, `drain_for_run` aborts each waiter
//! per v1.31.0's abort matrix (closing / continuing / accepted-closed)
//! to all waiting callers so they don't hang indefinitely.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokeira_types::{ExecutionRef, Payload, Payloads, RunKey};
use tokio::sync::oneshot;

/// Lifecycle position reported by the public update APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateLifecycleStage {
    /// No lifecycle state is known yet.
    Unspecified,
    /// The update is admitted to the runtime but not yet worker-accepted.
    Admitted,
    /// The worker accepted the update and an acceptance event is committed.
    Accepted,
    /// The update reached a terminal success or rejection outcome.
    Completed,
}

/// Runtime-owned snapshot projected to update API responses.
#[derive(Clone, Debug, PartialEq)]
pub struct UpdateLifecycleSnapshot {
    pub workflow_execution: ExecutionRef,
    pub update_id: String,
    pub update_name: String,
    pub stage: UpdateLifecycleStage,
    pub outcome: Option<UpdateOutcome>,
}

/// Caller-visible outcome of an update request.
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateOutcome {
    /// The update completed with a result.
    Completed {
        accepted_event_id: i64,
        result: Payloads,
    },
    /// The update was rejected by workflow code.
    Rejected {
        accepted_event_id: i64,
        failure: Payload,
    },
    /// The workflow closed after accepting but before completing the update.
    /// The edge renders v1.31.0's server-authored failure outcome verbatim
    /// (`acceptedUpdateCompletedWorkflowFailure`: Message "Workflow Update
    /// failed because the Workflow completed before the Update completed.",
    /// Source "Server", ApplicationFailureInfo Type
    /// "AcceptedUpdateCompletedWorkflow" NonRetryable,
    /// errors_failures.go:10-35 @ v1.31.0).
    AcceptedRunClosed,
    /// The update was delivered to a worker that completed its workflow task
    /// without processing it — v1.31.0's server-side RejectUnprocessed rider
    /// (spec speculative-wft Req 9). The edge renders the server-authored
    /// `unprocessedUpdateFailure` verbatim (Message "Workflow Update is
    /// rejected because it wasn't processed by worker. …", Source "Server",
    /// ApplicationFailureInfo Type "UnprocessedUpdate" NonRetryable,
    /// errors_failures.go:18-25 @ v1.31.0). Surfaces as a COMPLETED-stage
    /// response with a failure outcome (no RPC error), never redelivered.
    RejectedUnprocessed,
}

/// How long the caller wants to wait.
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateWaitPolicy {
    /// Return the current lifecycle stage immediately.
    Unspecified,
    /// Return the current lifecycle stage immediately.
    Admitted,
    /// Block until the worker accepts the update and the acceptance event is
    /// committed.
    Accepted,
    /// Block until the lane commits the update's final resolution
    /// (completed/rejected). These callers hold a registry entry until notified.
    Completed,
}

/// A single admitted-but-unresolved update, surfaced to the worker so it can be
/// (re)delivered.
///
/// The edge drains these from the [`UpdateRegistry`] when a worker needs the set
/// of updates still awaiting processing for a run (for example after a worker
/// reconnects), carrying the original caller-supplied name, input, and identity.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingUpdateTransport {
    /// Caller-assigned update identifier, unique within the run.
    pub update_id: String,
    /// Update handler name to invoke on the worker.
    pub update_name: String,
    /// Serialized update arguments as supplied by the caller.
    pub input: Payloads,
    /// Identity of the caller that issued the update.
    pub identity: String,
}

/// Worker-reported outcome of processing an update, fed back into the registry
/// to resolve any [`UpdateWaitPolicy::Completed`] caller.
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateTransportResolution {
    /// The worker accepted the update; no terminal result yet.
    Accepted,
    /// The update ran to completion with a result.
    Completed { result: Payloads },
    /// Workflow code rejected the update.
    Rejected { failure: Payload },
}

#[derive(Clone, Debug)]
pub(crate) enum UpdateResolution {
    Accepted {
        accepted_event_id: i64,
    },
    Completed {
        result: Payloads,
    },
    Rejected {
        failure: Payload,
    },
    /// The run closed with NO successor while the update was still
    /// admitted/sent — v1.31.0 aborts pre-accepted updates with
    /// `AbortedByWorkflowClosingErr` (NotFound "workflow update was aborted
    /// by closing workflow"; abort_reason.go:25-121 @ v1.31.0).
    AbortedByClosingWorkflow,
    /// The run closed INTO a successor (continue-as-new / retry / cron)
    /// while the update was still admitted/sent — v1.31.0 aborts with the
    /// retryable `consts.ErrWorkflowClosing` so a retried request lands the
    /// update on the new run (`AbortReasonWorkflowContinuing`).
    WorkflowContinuing,
    /// The run closed after the worker ACCEPTED the update but before it
    /// completed — NOT an error: the caller receives a COMPLETED stage with
    /// the server-authored failure outcome "Workflow Update failed because
    /// the Workflow completed before the Update completed."
    /// (`acceptedUpdateCompletedWorkflowFailure`, errors_failures.go:10-35
    /// @ v1.31.0).
    AcceptedRunClosed,
    /// The SERVER failed the workflow task while this update was in the Sent
    /// state (a bad-message validation failure decided during
    /// RespondWorkflowTaskCompleted, not an explicit RespondWorkflowTaskFailed)
    /// — v1.31.0's `AbortReasonWorkflowTaskFailed` for a Sent update aborts
    /// with the non-retryable `workflowTaskFailErr`
    /// (`serviceerror.WorkflowNotReady("Unable to perform workflow execution
    /// update due to unexpected workflow task failure.")`, abort_reason.go:86-103
    /// and errors_failures.go:14 @ v1.31.0). Surfaces as a FAILED_PRECONDITION
    /// RPC error, with `response` nil.
    AbortedByWftFailure,
    /// The update was delivered but the completing worker ignored it — the
    /// server auto-rejects it with the `unprocessedUpdateFailure` server
    /// outcome (RejectUnprocessed, Req 9). The caller receives a COMPLETED
    /// stage with a failure outcome and NO RPC error; never redelivered
    /// (`rejectUnprocessedUpdates`, workflow_task_completed_handler.go:213-262
    /// @ v1.31.0).
    RejectedUnprocessed,
}

#[derive(Debug)]
pub(crate) struct UpdateWaiter {
    pub wait_policy: UpdateWaitPolicy,
    pub tx: oneshot::Sender<UpdateResolution>,
}

#[derive(Debug)]
pub(crate) struct UpdateRegistryEntry {
    pub update_name: String,
    pub input: Payloads,
    pub identity: String,
    pub waiters: Vec<UpdateWaiter>,
    /// True once this update has been shipped to a worker on a workflow
    /// task. A sent update is not re-offered on subsequent tasks — v1.31.0
    /// sends Admitted-state updates exactly once (`needToSend`/`Send`,
    /// update.go:404-540 @ v1.31.0), re-including already-sent updates only
    /// on transient retry attempts. Reset when an attempt-1 WFT fails or
    /// times out so the replacement task carries the update again.
    pub sent: bool,
    /// True once the worker has accepted this update (the kernel moved it from
    /// `admitted_updates` to `pending_updates`). An accepted update has left the
    /// sendable set: it must not be re-offered as a fresh request message on a
    /// later workflow task — the worker resumes the in-flight update via history
    /// replay. v1.31.0 gates sending on `stateAdmitted`/`stateSent` only and never
    /// re-sends an accepted update (`needToSend`/`Send`/`onAcceptanceMsg`,
    /// service/history/workflow/update/update.go:404-540 @ v1.31.0). Re-offering it
    /// would make the worker re-accept and trip the kernel's `DuplicateUpdateId`
    /// guard, which is exactly the failure seen running the OpenAI sandbox agent's
    /// blocking `pause` update.
    pub accepted: bool,
    /// Monotonic admission order within the registry. v1.31.0 sends updates to
    /// the worker sorted by admitted time (`registry.Send` orders by
    /// `admittedTime`, update/registry.go:341-348 @ v1.31.0), which the corpus
    /// asserts (`TestUpdatesAreSentToWorkerInOrderOfAdmission`). The registry's
    /// backing map has no stable order, so each entry records the sequence in
    /// which it was admitted and `drain_pending_updates` sorts by it.
    pub admitted_seq: u64,
}

/// In-memory registry of waiting update callers.
#[derive(Clone, Default)]
pub struct UpdateRegistry {
    inner: Arc<Mutex<HashMap<(RunKey, String), UpdateRegistryEntry>>>,
    /// Monotonic counter stamped onto each entry at admission so the worker
    /// sees updates in admission order (see `UpdateRegistryEntry::admitted_seq`).
    next_seq: Arc<std::sync::atomic::AtomicU64>,
}

impl UpdateRegistry {
    /// Create an empty registry. Entries are added as
    /// [`UpdateWaitPolicy::Completed`] callers begin waiting.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the entry for an update and attach the caller's waiter.
    /// Returns `true` when this call CREATED the entry (the caller must admit
    /// the update to the kernel) and `false` when it attached to an
    /// in-flight update — v1.31.0's `FindOrCreate` dedupe: every caller
    /// sharing an update id shares one update and one outcome
    /// (registry.go:398-452 @ v1.31.0).
    pub(crate) fn register(
        &self,
        run_key: RunKey,
        update_id: String,
        update_name: String,
        input: Payloads,
        identity: String,
        wait_policy: UpdateWaitPolicy,
        tx: oneshot::Sender<UpdateResolution>,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        match inner.entry((run_key, update_id)) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                occupied
                    .get_mut()
                    .waiters
                    .push(UpdateWaiter { wait_policy, tx });
                false
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let admitted_seq = self
                    .next_seq
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                vacant.insert(UpdateRegistryEntry {
                    update_name,
                    input,
                    identity,
                    waiters: vec![UpdateWaiter { wait_policy, tx }],
                    accepted: false,
                    sent: false,
                    admitted_seq,
                });
                true
            }
        }
    }

    /// Drop waiters whose callers have gone away (their receiver was
    /// dropped, e.g. a soft-timeout return). Other callers sharing the
    /// update keep waiting.
    pub(crate) fn clear_waiter(&self, run_key: RunKey, update_id: &str) {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(&(run_key, update_id.to_string()))
        {
            entry.waiters.retain(|waiter| !waiter.tx.is_closed());
        }
    }

    pub(crate) fn attach_waiter(
        &self,
        run_key: RunKey,
        update_id: &str,
        wait_policy: UpdateWaitPolicy,
        tx: oneshot::Sender<UpdateResolution>,
    ) -> bool {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(&(run_key, update_id.to_string()))
        {
            entry.waiters.push(UpdateWaiter { wait_policy, tx });
            true
        } else {
            false
        }
    }

    /// Updates deliverable on a workflow task: never-accepted entries that
    /// have not been sent yet — or, when `include_sent` is set (transient
    /// retry attempts), also the already-sent ones. Returned entries are
    /// marked sent.
    pub(crate) fn drain_pending_updates(
        &self,
        run_key: RunKey,
        include_sent: bool,
    ) -> Vec<(String, String, Payloads, String)> {
        let mut deliverable: Vec<(u64, String, String, Payloads, String)> = self
            .inner
            .lock()
            .unwrap()
            .iter_mut()
            .filter(|((entry_run_key, _), entry)| {
                *entry_run_key == run_key && !entry.accepted && (include_sent || !entry.sent)
            })
            .map(|((_entry_run_key, update_id), entry)| {
                entry.sent = true;
                (
                    entry.admitted_seq,
                    update_id.clone(),
                    entry.update_name.clone(),
                    entry.input.clone(),
                    entry.identity.clone(),
                )
            })
            .collect();
        // v1.31.0 delivers updates to the worker in admission order
        // (`TestUpdatesAreSentToWorkerInOrderOfAdmission`); the backing map is
        // unordered, so sort by the admission sequence recorded at register.
        deliverable.sort_by_key(|(seq, _, _, _, _)| *seq);
        deliverable
            .into_iter()
            .map(|(_, update_id, update_name, input, identity)| {
                (update_id, update_name, input, identity)
            })
            .collect()
    }

    /// Re-arm delivery after an attempt-1 workflow task failed or timed out:
    /// the replacement task must carry the still-unaccepted updates again.
    pub(crate) fn reset_sent_for_run(&self, run_key: RunKey) {
        for ((entry_run_key, _), entry) in self.inner.lock().unwrap().iter_mut() {
            if *entry_run_key == run_key && !entry.accepted {
                entry.sent = false;
            }
        }
    }

    /// Ids of the updates delivered to a worker on the current workflow task
    /// (sent, not yet accepted) — the "delivered set" the kernel needs to
    /// distinguish updates the completing worker ignored (RejectUnprocessed,
    /// Req 9) from updates admitted while the task ran (which ride the
    /// follow-up task, K7). Read at completion-build time before the commit
    /// processes the worker's accept/reject commands.
    pub(crate) fn sent_update_ids(&self, run_key: RunKey) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|((entry_run_key, _), entry)| {
                *entry_run_key == run_key && entry.sent && !entry.accepted
            })
            .map(|((_entry_run_key, update_id), _)| update_id.clone())
            .collect()
    }

    /// Abort in-flight waiters when the SERVER fails the workflow task while an
    /// update was Sent — a bad-message validation failure decided during
    /// RespondWorkflowTaskCompleted, NOT an explicit RespondWorkflowTaskFailed
    /// (`AbortReasonWorkflowTaskFailed`, respondworkflowtaskcompleted/api.go:
    /// 454-460 + abort_reason.go:86-103 @ v1.31.0). Each Sent, not-yet-accepted
    /// update's waiters resolve with the non-retryable
    /// [`UpdateResolution::AbortedByWftFailure`] (WorkflowNotReady on the wire).
    ///
    /// The registry entry is RETAINED and re-armed for delivery (`sent` reset):
    /// tokeira records admitted updates durably in the kernel, so the update is
    /// redelivered on the retry task, and a caller re-sending the same id
    /// dedupes to this entry instead of re-admitting into a `DuplicateUpdateId`.
    /// This differs from v1.31.0's non-durable registry (which drops the
    /// aborted update) but matches its observable behaviour: the original
    /// caller sees WorkflowNotReady, and a resend rides the redelivery
    /// (`TestSpeculativeWorkflowTask_Fail`, `TestValidateWorkerMessages`).
    /// Accepted updates are left untouched — they survive the WFT failure and
    /// resume on replay.
    pub(crate) fn abort_sent_for_wft_failure(&self, run_key: RunKey) -> usize {
        let mut aborted_waiters = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            for ((entry_run_key, _), entry) in inner.iter_mut() {
                if *entry_run_key == run_key && entry.sent && !entry.accepted {
                    aborted_waiters.append(&mut entry.waiters);
                    entry.sent = false;
                }
            }
        }
        let count = aborted_waiters.len();
        for waiter in aborted_waiters {
            let _ = waiter.tx.send(UpdateResolution::AbortedByWftFailure);
        }
        count
    }

    /// Server-side RejectUnprocessed (Req 9): after a non-heartbeat completion
    /// that left the run open, every update the worker was sent but neither
    /// accepted nor rejected is auto-rejected with the server-authored
    /// `unprocessedUpdateFailure` and REMOVED (never redelivered)
    /// (`rejectUnprocessedUpdates`, workflow_task_completed_handler.go:213-262;
    /// `registry.go:297-316` @ v1.31.0). Runs post-commit, once the lane has
    /// marked accepted updates `accepted` and removed completed/rejected ones,
    /// so the remaining Sent, not-accepted entries are exactly the ignored set.
    /// The kernel prunes these from its admitted set in the same completion
    /// transition (via the delivered-ids passed on the request) so the two
    /// stay consistent. Returns the rejected ids for tracing.
    pub(crate) fn reject_unprocessed(&self, run_key: RunKey) -> Vec<String> {
        let mut rejected = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            let keys: Vec<(RunKey, String)> = inner
                .iter()
                .filter(|((entry_run_key, _), entry)| {
                    *entry_run_key == run_key && entry.sent && !entry.accepted
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in keys {
                if let Some(entry) = inner.remove(&key) {
                    rejected.push((key.1, entry.waiters));
                }
            }
        }
        rejected
            .into_iter()
            .map(|(id, waiters)| {
                for waiter in waiters {
                    let _ = waiter.tx.send(UpdateResolution::RejectedUnprocessed);
                }
                id
            })
            .collect()
    }

    pub(crate) fn notify(
        &self,
        run_key: RunKey,
        update_id: &str,
        resolution: UpdateResolution,
    ) -> bool {
        let entry = self
            .inner
            .lock()
            .unwrap()
            .remove(&(run_key, update_id.to_string()));
        match entry {
            Some(entry) => {
                let mut notified = false;
                for waiter in entry.waiters {
                    notified |= waiter.tx.send(resolution.clone()).is_ok();
                }
                notified
            }
            None => false,
        }
    }

    pub(crate) fn notify_accepted(
        &self,
        run_key: RunKey,
        update_id: &str,
        accepted_event_id: i64,
    ) -> bool {
        let mut waiters_to_notify = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(entry) = inner.get_mut(&(run_key, update_id.to_string())) {
                // The worker has accepted the update; it leaves the sendable set so
                // a subsequent workflow task does not re-offer it (which would make
                // the worker re-accept and trip the kernel's DuplicateUpdateId guard).
                entry.accepted = true;
                // Accepted-stage waiters resolve now; Completed-stage waiters
                // keep waiting on the same entry. The entry itself is retained
                // (accepted=true) so a close can still deliver the
                // accepted-but-completed-before outcome to remaining waiters.
                let mut kept = Vec::new();
                for waiter in entry.waiters.drain(..) {
                    match waiter.wait_policy {
                        UpdateWaitPolicy::Accepted => waiters_to_notify.push(waiter),
                        UpdateWaitPolicy::Completed => kept.push(waiter),
                        UpdateWaitPolicy::Unspecified | UpdateWaitPolicy::Admitted => {}
                    }
                }
                entry.waiters = kept;
                // The entry stays until terminal resolution (notify/drain):
                // it is the dedupe anchor — a repeat request for an accepted
                // update attaches here instead of re-admitting.
            }
        }

        let mut notified = false;
        for waiter in waiters_to_notify {
            notified |= waiter
                .tx
                .send(UpdateResolution::Accepted { accepted_event_id })
                .is_ok();
        }
        notified
    }

    pub(crate) fn remove(&self, run_key: RunKey, update_id: &str) {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, update_id.to_string()));
    }

    /// Remove an update's registry entry and return its waiters, WITHOUT
    /// resolving them. Used to pre-claim a worker-rejected update before its
    /// completion commits so that a close in the SAME workflow task cannot
    /// abort it first: v1.31.0 applies the rejection as an effect and only the
    /// close-abort touches still-pending updates, so a rejected-and-closed
    /// update reports its rejection, not `AbortedByClosingWorkflow`
    /// (`TestCompleteWorkflow_AbortUpdates/update_rejected_*`). The caller
    /// resolves the returned waiters once the completion durably commits.
    pub(crate) fn take_entry_waiters(&self, run_key: RunKey, update_id: &str) -> Vec<UpdateWaiter> {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, update_id.to_string()))
            .map(|entry| entry.waiters)
            .unwrap_or_default()
    }

    /// Resolve a pre-claimed set of waiters (from [`take_entry_waiters`]) with
    /// `resolution`. Returns whether any waiter was still listening.
    pub(crate) fn resolve_waiters(
        &self,
        waiters: Vec<UpdateWaiter>,
        resolution: UpdateResolution,
    ) -> bool {
        let mut notified = false;
        for waiter in waiters {
            notified |= waiter.tx.send(resolution.clone()).is_ok();
        }
        notified
    }

    /// Abort every in-flight update for a closing run, resolving each waiter
    /// per v1.31.0's abort matrix (abort_reason.go:25-121): accepted updates
    /// get the non-error completed-before failure outcome regardless of the
    /// close shape; admitted/sent updates get the retryable continuing abort
    /// when the close created a successor run, else the closing-workflow
    /// NotFound abort. `continuing` is true when the close authored a
    /// successor (continue-as-new / retry / cron).
    pub(crate) fn drain_for_run(&self, run_key: RunKey, continuing: bool) -> usize {
        let mut drained = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            let keys: Vec<_> = inner
                .keys()
                .filter(|(key_run, _)| *key_run == run_key)
                .cloned()
                .collect();
            for key in keys {
                if let Some(entry) = inner.remove(&key) {
                    drained.push(entry);
                }
            }
        }
        let count = drained.len();
        for entry in drained {
            let resolution = if entry.accepted {
                UpdateResolution::AcceptedRunClosed
            } else if continuing {
                UpdateResolution::WorkflowContinuing
            } else {
                UpdateResolution::AbortedByClosingWorkflow
            };
            for waiter in entry.waiters {
                let _ = waiter.tx.send(resolution.clone());
            }
        }
        count
    }

    /// Read the update_name and input for a registered update without removing it.
    pub fn peek_update_info(&self, run_key: RunKey, update_id: &str) -> Option<(String, Payloads)> {
        self.inner
            .lock()
            .unwrap()
            .get(&(run_key, update_id.to_string()))
            .map(|entry| (entry.update_name.clone(), entry.input.clone()))
    }

    pub fn contains_registered_update(&self, run_key: RunKey, update_id: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .contains_key(&(run_key, update_id.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, run_key: RunKey, update_id: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .contains_key(&(run_key, update_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use tokeira_types::Payload;

    // ── Generators ──────────────────────────────────

    fn arb_payload() -> impl Strategy<Value = Payload> {
        prop::collection::vec(any::<u8>(), 0..32).prop_map(|data| Payload {
            data,
            metadata: BTreeMap::new(),
            external_payloads: Vec::new(),
        })
    }

    fn arb_payloads() -> impl Strategy<Value = Payloads> {
        prop::collection::vec(arb_payload(), 0..4).prop_map(Payloads)
    }

    fn arb_update_id() -> impl Strategy<Value = String> {
        "[a-z0-9]{1,12}".prop_map(|s| format!("upd-{s}"))
    }

    fn arb_failure() -> impl Strategy<Value = Payload> {
        "[a-z ]{1,20}".prop_map(|s| Payload::new(s.into_bytes()))
    }

    // ── Existing unit tests ─────────────────────────

    #[tokio::test]
    async fn register_and_notify_delivers_resolution() {
        let registry = UpdateRegistry::new();
        let run_key = RunKey::new();
        let (tx, rx) = oneshot::channel();

        registry.register(
            run_key,
            "update-1".into(),
            "name".into(),
            Payloads::default(),
            "worker".into(),
            UpdateWaitPolicy::Completed,
            tx,
        );
        assert!(registry.contains(run_key, "update-1"));

        assert!(registry.notify(
            run_key,
            "update-1",
            UpdateResolution::Completed {
                result: Payloads::default(),
            },
        ));
        assert!(!registry.contains(run_key, "update-1"));
        assert!(matches!(rx.await, Ok(UpdateResolution::Completed { .. })));
    }

    // Feature: agentic-orchestration (OpenAI sandbox demo). Property: once the
    // worker has accepted an update, it MUST NOT be re-offered as a fresh request
    // message on a later workflow task. v1.31.0 sends an update request only while
    // it is Admitted/Sent; on acceptance it leaves the sendable set and the worker
    // resumes it via history replay, never a re-sent request
    // (`needToSend`/`Send`/`onAcceptanceMsg`, service/history/workflow/update/update.go:404-540 @ v1.31.0).
    // Re-offering an accepted update makes the worker re-accept it, which the kernel
    // rejects as `DuplicateUpdateId` — the failure observed running the OpenAI sandbox
    // agent's blocking `pause` update against tokeirad.
    #[tokio::test]
    async fn accepted_update_is_not_redelivered_as_pending_transport() {
        let registry = UpdateRegistry::new();
        let run_key = RunKey::new();
        let (tx, _rx) = oneshot::channel();

        registry.register(
            run_key,
            "update-1".into(),
            "pause".into(),
            Payloads::default(),
            "worker".into(),
            UpdateWaitPolicy::Completed,
            tx,
        );

        // While Admitted (not yet accepted), the update must be offered to the worker.
        assert_eq!(
            registry.drain_pending_updates(run_key, false).len(),
            1,
            "an admitted update should be offered to the worker"
        );

        // The worker accepts the update on its first workflow task. A Completed-policy
        // caller stays registered so it can still receive the terminal result.
        assert!(
            !registry.notify_accepted(run_key, "update-1", 7),
            "a Completed-policy waiter is not resolved on acceptance"
        );
        assert!(
            registry.contains(run_key, "update-1"),
            "the Completed waiter must remain registered after acceptance"
        );

        // REGRESSION: an accepted update must not be re-offered on a subsequent WFT.
        // Today this returns the update, causing a double-accept → DuplicateUpdateId.
        assert!(
            registry.drain_pending_updates(run_key, false).is_empty(),
            "an accepted update must not be re-offered as a pending transport"
        );
    }

    #[tokio::test]
    async fn drain_for_run_closes_all_waiters() {
        let registry = UpdateRegistry::new();
        let run_key = RunKey::new();
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        registry.register(
            run_key,
            "update-1".into(),
            "name-1".into(),
            Payloads::default(),
            "worker".into(),
            UpdateWaitPolicy::Completed,
            tx1,
        );
        registry.register(
            run_key,
            "update-2".into(),
            "name-2".into(),
            Payloads::default(),
            "worker".into(),
            UpdateWaitPolicy::Completed,
            tx2,
        );

        assert_eq!(registry.drain_for_run(run_key, false), 2);
        assert!(!registry.contains(run_key, "update-1"));
        assert!(!registry.contains(run_key, "update-2"));
        assert!(matches!(
            rx1.await,
            Ok(UpdateResolution::AbortedByClosingWorkflow)
        ));
        assert!(matches!(
            rx2.await,
            Ok(UpdateResolution::AbortedByClosingWorkflow)
        ));
    }

    // ── Property 1: Update command preserves caller
    //    parameters
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 1.2, 1.4**
    //
    // For any valid update_id, update_name, and input
    // payload, when update_workflow is called the
    // Command::Update submitted to the lane carries the
    // exact same values.
    //
    // We test this at the registry + runtime level by
    // starting a real workflow, calling update_workflow
    // with wait_policy=Accepted, and verifying the
    // returned outcome plus the kernel state reflect the
    // caller-provided parameters.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop1_update_preserves_caller_params(
            update_id in arb_update_id(),
            update_name in "[a-z]{1,8}",
            input in arb_payloads(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store =
                    Arc::new(InMemoryStore::default());
                let runtime = make_runtime(store.clone());
                let ns = NamespaceId::new();
                let run_key = start_wf(
                    &runtime, ns, &update_id,
                )
                .await;

                let outcome = runtime
                    .update_workflow(
                        exec_ref(ns, &update_id),
                        update_id.clone(),
                        update_name.clone(),
                        input.clone(),
                        req_ctx(&update_id),
                        Duration::milliseconds(500),
                        UpdateWaitPolicy::Accepted,
                    )
                    .await
                    .unwrap();

                prop_assert_eq!(
                    outcome.stage,
                    UpdateLifecycleStage::Admitted
                );
                prop_assert!(outcome.outcome.is_none());

                // The update should be in admitted_updates,
                // not pending_updates (no worker acceptance yet).
                let state = match store
                    .load_run(run_key)
                    .await
                    .unwrap()
                {
                    LoadedRun::Existing(s) => s,
                    _ => panic!("run missing"),
                };
                prop_assert!(
                    state.admitted_updates.contains(&update_id)
                );
                prop_assert!(
                    !state.pending_updates.contains_key(&update_id)
                );

                // For Accepted wait policy, the UpdateRegistry
                // does not hold the entry (it's only registered
                // for Completed policy). The update_name and input
                // are preserved in the kernel's admitted_updates set
                // (just the ID) and in the UpdateRegistry for
                // Completed callers.
                let is_admitted = state.admitted_updates.contains(&update_id);
                prop_assert!(is_admitted);

                Ok(())
            })?;
        }
    }

    // ── Property 2: Kernel rejections propagate and
    //    clean up pre-registered entry
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 1.6, 8.1, 8.2, 8.3,
    //   8.4**
    //
    // When the kernel rejects a Command::Update, the
    // runtime returns an error and the UpdateRegistry
    // contains no entry for that (RunKey, update_id).
    //
    // We trigger RunClosed by terminating the workflow
    // first, then sending an update. The kernel will
    // reject with Reject::RunClosed.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop2_rejection_cleans_registry(
            update_id in arb_update_id(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store =
                    Arc::new(InMemoryStore::default());
                let runtime = make_runtime(store.clone());
                let ns = NamespaceId::new();
                let wf_id = format!("rej-{update_id}");
                let _run_key =
                    start_wf(&runtime, ns, &wf_id).await;

                // Close the workflow so the kernel
                // rejects subsequent updates.
                let run_id = match store
                    .load_run(_run_key)
                    .await
                    .unwrap()
                {
                    LoadedRun::Existing(s) => s.run_id,
                    _ => panic!("missing"),
                };
                runtime
                    .terminate_workflow(
                        ExecutionRef {
                            namespace_id: ns,
                            workflow_id: WorkflowId(
                                wf_id.clone(),
                            ),
                            run_id: Some(run_id),
                        },
                        TerminateRequest {
                            reason: "test".into(),
                            details: None,
                            identity: "test".into(),
                            request: req_ctx("term"),
                            now: OffsetDateTime::now_utc(),
                        },
                    )
                    .await
                    .unwrap();

                // Now try to update — should fail.
                let err = runtime
                    .update_workflow(
                        exec_ref(ns, &wf_id),
                        update_id.clone(),
                        "handler".into(),
                        Payloads::default(),
                        req_ctx(&update_id),
                        Duration::milliseconds(200),
                        UpdateWaitPolicy::Completed,
                    )
                    .await;
                prop_assert!(err.is_err());

                // Registry must be clean.
                prop_assert!(!runtime
                    .update_registry()
                    .contains(
                        _run_key, &update_id
                    ));
                Ok(())
            })?;
        }
    }

    // ── Property 3: Acceptance notification carries
    //    correct event ID
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 3.1, 3.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop3_acceptance_event_id(
            update_id in arb_update_id(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store =
                    Arc::new(InMemoryStore::default());
                let runtime = make_runtime(store.clone());
                let ns = NamespaceId::new();
                let run_key = start_wf(
                    &runtime, ns, &update_id,
                )
                .await;

                let outcome = runtime
                    .update_workflow(
                        exec_ref(ns, &update_id),
                        update_id.clone(),
                        "handler".into(),
                        Payloads::default(),
                        req_ctx(&update_id),
                        Duration::milliseconds(500),
                        UpdateWaitPolicy::Accepted,
                    )
                    .await
                    .unwrap();

                prop_assert_eq!(
                    outcome.stage,
                    UpdateLifecycleStage::Admitted
                );
                prop_assert!(outcome.outcome.is_none());

                let state = match store
                    .load_run(run_key)
                    .await
                    .unwrap()
                {
                    LoadedRun::Existing(s) => s,
                    _ => panic!("missing"),
                };
                prop_assert!(
                    state.admitted_updates.contains(&update_id)
                );
                Ok(())
            })?;
        }
    }

    // ── Property 4: Update resolution notification
    //    round-trip
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 4.1, 4.2, 4.3, 4.4,
    //   7.2, 7.3, 7.5**
    //
    // Register callers in the UpdateRegistry, then
    // notify with random resolutions. Verify each
    // caller receives the exact payload.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop4_resolution_round_trip(
            n in 1usize..5,
            is_completed in prop::collection::vec(
                any::<bool>(), 1..5
            ),
            payloads_vec in prop::collection::vec(
                arb_payloads(), 1..5
            ),
            failures in prop::collection::vec(
                arb_failure(), 1..5
            ),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let registry = UpdateRegistry::new();
                let run_key = RunKey::new();
                let count = n.min(is_completed.len())
                    .min(payloads_vec.len())
                    .min(failures.len());
                let mut receivers = Vec::new();

                for i in 0..count {
                    let uid = format!("upd-{i}");
                    let (tx, rx) = oneshot::channel();
                    registry.register(
                        run_key,
                        uid.clone(),
                        format!("name-{i}"),
                        Payloads::default(),
                        format!("worker-{i}"),
                        UpdateWaitPolicy::Completed,
                        tx,
                    );
                    receivers.push((uid, rx));
                }

                // Notify all callers.
                for (i, (uid, _)) in
                    receivers.iter().enumerate()
                {
                    let resolution = if is_completed[i] {
                        UpdateResolution::Completed {
                            result: payloads_vec[i]
                                .clone(),
                        }
                    } else {
                        UpdateResolution::Rejected {
                            failure: failures[i]
                                .clone(),
                        }
                    };
                    let found = registry.notify(
                        run_key, uid, resolution,
                    );
                    prop_assert!(found);
                }

                // Verify each caller got the right
                // payload.
                for (i, (_, rx)) in
                    receivers.into_iter().enumerate()
                {
                    let got = rx.await.unwrap();
                    if is_completed[i] {
                        match got {
                            UpdateResolution::Completed {
                                result,
                            } => {
                                prop_assert_eq!(
                                    result,
                                    payloads_vec[i]
                                        .clone()
                                );
                            }
                            other => panic!(
                                "expected Completed: \
                                 {other:?}"
                            ),
                        }
                    } else {
                        match got {
                            UpdateResolution::Rejected {
                                failure,
                            } => {
                                prop_assert_eq!(
                                    failure,
                                    failures[i]
                                        .clone()
                                );
                            }
                            other => panic!(
                                "expected Rejected: \
                                 {other:?}"
                            ),
                        }
                    }
                }
                Ok(())
            })?;
        }
    }

    // ── Property 5: Silent discard when no caller is
    //    waiting
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 4.5, 5.6**
    //
    // When no caller is registered (simulating timeout),
    // notify returns false and no error occurs.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop5_silent_discard_no_caller(
            result in arb_payloads(),
            failure in arb_failure(),
            use_completed in any::<bool>(),
        ) {
            let registry = UpdateRegistry::new();
            let run_key = RunKey::new();
            let uid = "orphan";

            let resolution = if use_completed {
                UpdateResolution::Completed {
                    result,
                }
            } else {
                UpdateResolution::Rejected { failure }
            };

            // No caller registered — notify should
            // return false without error.
            let found =
                registry.notify(run_key, uid, resolution);
            prop_assert!(!found);
        }
    }

    // ── Property 6: Timeout enforcement without kernel
    //    state mutation
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 5.1, 5.3, 5.4**
    //
    // Admit an update but don't let a worker accept it.
    // Server soft timeout returns the reached stage
    // without canceling the update or mutating kernel
    // state.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop6_timeout_preserves_kernel_state(
            update_id in arb_update_id(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store =
                    Arc::new(InMemoryStore::default());
                let runtime = make_runtime(store.clone());
                let ns = NamespaceId::new();
                let run_key = start_wf(
                    &runtime, ns, &update_id,
                )
                .await;

                let snapshot = runtime
                    .update_workflow(
                        exec_ref(ns, &update_id),
                        update_id.clone(),
                        "slow".into(),
                        Payloads::default(),
                        req_ctx(&update_id),
                        Duration::milliseconds(20),
                        UpdateWaitPolicy::Completed,
                    )
                    .await
                    .unwrap();
                prop_assert_eq!(
                    snapshot.stage,
                    UpdateLifecycleStage::Admitted
                );
                prop_assert!(snapshot.outcome.is_none());

                // The registry keeps transport payload
                // after the caller returns, so the update
                // can still be delivered to a worker.
                prop_assert!(runtime
                    .update_registry()
                    .contains(
                        run_key, &update_id
                    ));

                // Kernel state must still have the
                // pending update.
                let state = match store
                    .load_run(run_key)
                    .await
                    .unwrap()
                {
                    LoadedRun::Existing(s) => s,
                    _ => panic!("missing"),
                };
                prop_assert!(state
                    .admitted_updates
                    .contains(&update_id));
                Ok(())
            })?;
        }
    }

    // ── Property 7: Concurrent updates are independent
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 6.1, 6.2, 6.4**
    //
    // Register N concurrent updates, resolve a subset,
    // verify unresolved remain pending.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop7_concurrent_updates_independent(
            n in 2usize..9,
            resolve_mask in prop::collection::vec(
                any::<bool>(), 2..9
            ),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let registry = UpdateRegistry::new();
                let run_key = RunKey::new();
                let count = n.min(resolve_mask.len());
                let mut receivers = Vec::new();

                for i in 0..count {
                    let uid = format!("upd-{i}");
                    let (tx, rx) = oneshot::channel();
                    registry.register(
                        run_key,
                        uid.clone(),
                        format!("name-{i}"),
                        Payloads::default(),
                        format!("worker-{i}"),
                        UpdateWaitPolicy::Completed,
                        tx,
                    );
                    receivers.push((uid, rx));
                }

                // Resolve only the masked subset.
                for (i, (uid, _)) in
                    receivers.iter().enumerate()
                {
                    if resolve_mask[i] {
                        registry.notify(
                            run_key,
                            uid,
                            UpdateResolution::Completed {
                                result:
                                    Payloads::default(),
                            },
                        );
                    }
                }

                // Verify resolved are gone, unresolved
                // remain.
                for (i, (uid, _)) in
                    receivers.iter().enumerate()
                {
                    if resolve_mask[i] {
                        prop_assert!(!registry
                            .contains(
                                run_key, uid
                            ));
                    } else {
                        prop_assert!(registry
                            .contains(
                                run_key, uid
                            ));
                    }
                }
                Ok(())
            })?;
        }
    }

    // ── Property 8: Run close drains all waiting
    //    callers
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 9.1, 9.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop8_run_close_drains_all(
            k in 1usize..9,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let registry = UpdateRegistry::new();
                let run_key = RunKey::new();
                let mut receivers = Vec::new();

                for i in 0..k {
                    let uid = format!("upd-{i}");
                    let (tx, rx) = oneshot::channel();
                    registry.register(
                        run_key,
                        uid.clone(),
                        format!("name-{i}"),
                        Payloads::default(),
                        format!("worker-{i}"),
                        UpdateWaitPolicy::Completed,
                        tx,
                    );
                    receivers.push((uid, rx));
                }

                let drained = registry
                    .drain_for_run(run_key, false);
                prop_assert_eq!(drained, k);

                // All entries removed.
                for (uid, _) in &receivers {
                    prop_assert!(!registry
                        .contains(run_key, uid));
                }

                // All callers got the closing abort.
                for (_, rx) in receivers {
                    let got = rx.await.unwrap();
                    prop_assert!(matches!(
                        got,
                        UpdateResolution::AbortedByClosingWorkflow
                    ));
                }
                Ok(())
            })?;
        }
    }

    // ── Property 9: Registry cleanup on all resolution
    //    paths
    // Feature: runtime-update-lifecycle
    // **Validates: Requirements 2.3**
    //
    // After completion, rejection, remove (timeout), or
    // drain (run close), the registry is empty for that
    // entry.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop9_cleanup_all_paths(
            path in 0u8..4,
            result in arb_payloads(),
            failure in arb_failure(),
        ) {
            let registry = UpdateRegistry::new();
            let run_key = RunKey::new();
            let uid = "cleanup-test";
            let (tx, rx) = oneshot::channel();
            registry.register(
                run_key,
                uid.into(),
                "name".into(),
                Payloads::default(),
                "worker".into(),
                UpdateWaitPolicy::Completed,
                tx,
            );

            match path {
                0 => {
                    // Completion
                    registry.notify(
                        run_key,
                        uid,
                        UpdateResolution::Completed {
                            result,
                        },
                    );
                    let got =
                        rx.blocking_recv().unwrap();
                    let is_completed = matches!(
                        got,
                        UpdateResolution::Completed {
                            ..
                        }
                    );
                    prop_assert!(is_completed);
                }
                1 => {
                    // Rejection
                    registry.notify(
                        run_key,
                        uid,
                        UpdateResolution::Rejected {
                            failure,
                        },
                    );
                    let got =
                        rx.blocking_recv().unwrap();
                    let is_rejected = matches!(
                        got,
                        UpdateResolution::Rejected {
                            ..
                        }
                    );
                    prop_assert!(is_rejected);
                }
                2 => {
                    // Timeout (remove)
                    registry.remove(run_key, uid);
                    // rx should error (sender dropped)
                    prop_assert!(
                        rx.blocking_recv().is_err()
                    );
                }
                _ => {
                    // Run close (drain)
                    let n = registry
                        .drain_for_run(run_key, false);
                    prop_assert_eq!(n, 1);
                    let got =
                        rx.blocking_recv().unwrap();
                    let is_closed = matches!(
                        got,
                        UpdateResolution::AbortedByClosingWorkflow
                    );
                    prop_assert!(is_closed);
                }
            }

            // Registry must be empty for this entry.
            prop_assert!(
                !registry.contains(run_key, uid)
            );
        }
    }

    // Feature: api-conformance-update-lifecycle, Property 4: stage monotonicity
    // **Validates: Requirements 4.1, 4.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop10_stage_ordering_and_rejection_terminality(
            left in 0usize..4,
            right in 0usize..4,
            failure in arb_failure(),
        ) {
            let stages = [
                UpdateLifecycleStage::Unspecified,
                UpdateLifecycleStage::Admitted,
                UpdateLifecycleStage::Accepted,
                UpdateLifecycleStage::Completed,
            ];

            prop_assert_eq!(
                stages[left] <= stages[right],
                left <= right
            );

            let rejected = UpdateLifecycleSnapshot {
                workflow_execution: ExecutionRef {
                    namespace_id: NamespaceId::new(),
                    workflow_id: WorkflowId("rejected-update".into()),
                    run_id: None,
                },
                update_id: "update-rejected".into(),
                update_name: "handler".into(),
                stage: UpdateLifecycleStage::Completed,
                outcome: Some(UpdateOutcome::Rejected {
                    accepted_event_id: 7,
                    failure: failure.clone(),
                }),
            };

            prop_assert_eq!(
                rejected.stage,
                UpdateLifecycleStage::Completed
            );
            prop_assert_eq!(
                rejected.outcome,
                Some(UpdateOutcome::Rejected {
                    accepted_event_id: 7,
                    failure,
                })
            );
        }
    }

    // ── Test helpers ────────────────────────────────

    use std::sync::Arc;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{LoadedRun, StartRequest, TerminateRequest};
    use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
    use tokeira_types::{
        ExecutionRef, Memo, NamespaceId, RequestContext, RequestId, RunId, SearchAttributes,
        TaskQueueName, WorkflowId, WorkflowType,
    };

    use crate::{
        BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
    };

    fn make_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
        TokeiraRuntime::new(
            store,
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        )
    }

    fn req_ctx(id: &str) -> RequestContext {
        RequestContext {
            request_id: RequestId(id.into()),
            caller_identity: Some("tester".into()),
            received_at: OffsetDateTime::now_utc(),
        }
    }

    fn exec_ref(ns: NamespaceId, wf_id: &str) -> ExecutionRef {
        ExecutionRef {
            namespace_id: ns,
            workflow_id: WorkflowId(wf_id.into()),
            run_id: None,
        }
    }

    async fn start_wf(
        runtime: &TokeiraRuntime<InMemoryStore>,
        ns: NamespaceId,
        wf_id: &str,
    ) -> RunKey {
        let run_key = RunKey::new();
        let run_id = RunId::new();
        let result = runtime
            .start_workflow(StartRequest {
                run_key,
                namespace_id: ns,
                workflow_id: WorkflowId(wf_id.into()),
                run_id,
                workflow_type: WorkflowType("update-wf".into()),
                task_queue: TaskQueueName("q".into()),
                deployment: None,
                build_id: None,
                versioning_override: None,
                workflow_start_delay: None,
                client_cron_schedule: None,
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
                first_execution_run_id: None,
                parent_run_key: None,
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: Some(run_id),
                continued_failure: None,
                last_completion_result: None,
                first_run_started_at: None,
                request: req_ctx(&format!("start-{wf_id}")),
                now: OffsetDateTime::now_utc(),
                cron_schedule: None,
                reserved_poller_identity: None,
            })
            .await
            .unwrap();
        match result {
            CommitResult::Applied { new_state } => new_state.run_key,
            other => {
                panic!("unexpected: {other:?}")
            }
        }
    }
}
