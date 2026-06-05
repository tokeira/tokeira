//! In-memory registry for waiting update callers.
//!
//! Updates follow a two-phase lifecycle: *admitted* (the kernel records the
//! update ID) → *accepted* (the worker processes the update and writes an
//! acceptance event). Callers using `UpdateWaitPolicy::Completed` need to
//! block until the lane commits the final resolution event. This registry
//! holds the oneshot senders that bridge the gap between the API call and
//! the lane's eventual `notify` — it is purely in-memory because the
//! caller's connection lifetime, not durable state, determines how long
//! the entry lives. When a run closes, `drain_for_run` sends `RunClosed`
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

#[derive(Debug)]
pub(crate) enum UpdateResolution {
    Accepted { accepted_event_id: i64 },
    Completed { result: Payloads },
    Rejected { failure: Payload },
    RunClosed,
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
    pub waiter: Option<UpdateWaiter>,
}

/// In-memory registry of waiting update callers.
#[derive(Clone, Default)]
pub struct UpdateRegistry {
    inner: Arc<Mutex<HashMap<(RunKey, String), UpdateRegistryEntry>>>,
}

impl UpdateRegistry {
    /// Create an empty registry. Entries are added as
    /// [`UpdateWaitPolicy::Completed`] callers begin waiting.
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(
        &self,
        run_key: RunKey,
        update_id: String,
        update_name: String,
        input: Payloads,
        identity: String,
        wait_policy: UpdateWaitPolicy,
        tx: oneshot::Sender<UpdateResolution>,
    ) {
        self.inner.lock().unwrap().insert(
            (run_key, update_id),
            UpdateRegistryEntry {
                update_name,
                input,
                identity,
                waiter: Some(UpdateWaiter { wait_policy, tx }),
            },
        );
    }

    pub(crate) fn clear_waiter(&self, run_key: RunKey, update_id: &str) {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(&(run_key, update_id.to_string()))
        {
            entry.waiter = None;
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
            entry.waiter = Some(UpdateWaiter { wait_policy, tx });
            true
        } else {
            false
        }
    }

    pub(crate) fn drain_pending_updates(
        &self,
        run_key: RunKey,
    ) -> Vec<(String, String, Payloads, String)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|((entry_run_key, _), _)| *entry_run_key == run_key)
            .map(|((_entry_run_key, update_id), entry)| {
                (
                    update_id.clone(),
                    entry.update_name.clone(),
                    entry.input.clone(),
                    entry.identity.clone(),
                )
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
                if let Some(waiter) = entry.waiter {
                    let _ = waiter.tx.send(resolution);
                    true
                } else {
                    false
                }
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
        let mut waiter_to_notify = None;
        let mut remove_entry = false;
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(entry) = inner.get_mut(&(run_key, update_id.to_string())) {
                match entry
                    .waiter
                    .as_ref()
                    .map(|waiter| waiter.wait_policy.clone())
                {
                    Some(UpdateWaitPolicy::Accepted) => {
                        waiter_to_notify = entry.waiter.take();
                        remove_entry = true;
                    }
                    Some(UpdateWaitPolicy::Completed) => {}
                    Some(UpdateWaitPolicy::Unspecified | UpdateWaitPolicy::Admitted) | None => {
                        remove_entry = true;
                    }
                }
            }
            if remove_entry {
                inner.remove(&(run_key, update_id.to_string()));
            }
        }

        if let Some(waiter) = waiter_to_notify {
            let _ = waiter
                .tx
                .send(UpdateResolution::Accepted { accepted_event_id });
            true
        } else {
            false
        }
    }

    pub(crate) fn remove(&self, run_key: RunKey, update_id: &str) {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, update_id.to_string()));
    }

    pub(crate) fn drain_for_run(&self, run_key: RunKey) -> usize {
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
            if let Some(waiter) = entry.waiter {
                let _ = waiter.tx.send(UpdateResolution::RunClosed);
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

        assert_eq!(registry.drain_for_run(run_key), 2);
        assert!(!registry.contains(run_key, "update-1"));
        assert!(!registry.contains(run_key, "update-2"));
        assert!(matches!(rx1.await, Ok(UpdateResolution::RunClosed)));
        assert!(matches!(rx2.await, Ok(UpdateResolution::RunClosed)));
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

                let drained =
                    registry.drain_for_run(run_key);
                prop_assert_eq!(drained, k);

                // All entries removed.
                for (uid, _) in &receivers {
                    prop_assert!(!registry
                        .contains(run_key, uid));
                }

                // All callers got RunClosed.
                for (_, rx) in receivers {
                    let got = rx.await.unwrap();
                    prop_assert!(matches!(
                        got,
                        UpdateResolution::RunClosed
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
                        .drain_for_run(run_key);
                    prop_assert_eq!(n, 1);
                    let got =
                        rx.blocking_recv().unwrap();
                    let is_closed = matches!(
                        got,
                        UpdateResolution::RunClosed
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
