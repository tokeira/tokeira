//! A signal takes its lane claim before any run-state read.

use std::sync::atomic::Ordering;

use proptest::prelude::*;
use tokeira_storage::InMemoryStore;
use tokeira_types::{RequestContext, RequestId, WorkflowId};

use super::{
    repair_tests::Repo,
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::test_clock::TestClock;

#[derive(Clone, Copy, Debug)]
enum Fill {
    RunShare,
    LaneBudget,
}

#[derive(Clone, Copy, Debug)]
enum EarlyReturn {
    MissingRun,
    Closing,
    Duplicate,
}

/// Every read the signal path could make, by kind.
fn reads(repo: &Repo) -> [usize; 5] {
    [
        repo.resolutions.load(Ordering::SeqCst),
        repo.loads.load(Ordering::SeqCst),
        repo.stats_loads.load(Ordering::SeqCst),
        repo.history_reads.load(Ordering::SeqCst),
        repo.dedupe_lookups.load(Ordering::SeqCst),
    ]
}

fn signal(runtime: &TokeiraRuntime<Repo>, request_id: &str) -> SignalRequest {
    let now = runtime.clock.now();
    SignalRequest {
        signal_name: "signal".into(),
        input: Payloads::default(),
        header: None,
        links: Vec::new(),
        request: RequestContext {
            request_id: RequestId(request_id.into()),
            caller_identity: None,
            principal: None,
            received_at: now,
        },
        now,
    }
}

/// A running workflow whose first workflow task has started.
async fn started_run() -> (Arc<Repo>, TokeiraRuntime<Repo>, ExecutionRef, RunKey) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::with_inner(
        InMemoryStore::default().with_clock(clock.clone()),
    ));
    let runtime = fixture_on(repo.clone(), clock).await;
    let request = start(&runtime, false);
    let run_key = request.run_key;
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let execution = ExecutionRef {
        namespace_id: request.namespace_id,
        workflow_id: request.workflow_id.clone(),
        run_id: Some(request.run_id),
    };
    runtime.start_workflow(request).await.unwrap();
    runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    (repo, runtime, execution, run_key)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: lane-run-concurrency, Property 8: A refused signal reads only its run key
    // A signal to a run at its share, or on a lane at its budget, is refused
    // with the matching refusal having read nothing but its run-key
    // resolution; an early return after a successful claim gives it back.
    #[test]
    fn a_refused_signal_reads_only_its_run_key(
        fill in prop_oneof![Just(Fill::RunShare), Just(Fill::LaneBudget)],
        with_run_id in any::<bool>(),
        early in prop_oneof![
            Just(EarlyReturn::MissingRun),
            Just(EarlyReturn::Closing),
            Just(EarlyReturn::Duplicate),
        ],
    ) {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (repo, runtime, mut execution, run_key) = started_run().await;
                if !with_run_id {
                    execution.run_id = None;
                }
                let lane = runtime.pick_lane(run_key);

                // Early returns after a successful claim give the claim back.
                let before = lane.reserved_claims();
                match early {
                    EarlyReturn::MissingRun => {
                        let reservation = runtime.reserve_submit(RunKey::new()).unwrap();
                        let missing = ExecutionRef {
                            workflow_id: WorkflowId("missing".into()),
                            run_id: None,
                            ..execution.clone()
                        };
                        let result = runtime
                            .signal_workflow_reserved(missing, signal(&runtime, "a"), reservation)
                            .await;
                        prop_assert!(result.is_err());
                    }
                    EarlyReturn::Closing | EarlyReturn::Duplicate => {
                        let applied = runtime
                            .signal_workflow(execution.clone(), signal(&runtime, "applied"))
                            .await
                            .unwrap();
                        let applied = matches!(applied, CommitResult::Applied { .. });
                        prop_assert!(applied);
                        runtime
                            .close_attempt_tracking
                            .lock()
                            .unwrap()
                            .insert(run_key);
                        let request_id = match early {
                            EarlyReturn::Duplicate => "applied",
                            _ => "fresh",
                        };
                        let reservation = runtime.reserve_submit(run_key).unwrap();
                        let result = runtime
                            .signal_workflow_reserved(
                                execution.clone(),
                                signal(&runtime, request_id),
                                reservation,
                            )
                            .await;
                        match early {
                            EarlyReturn::Duplicate => {
                                prop_assert!(matches!(result, Ok(CommitResult::Duplicate)));
                            }
                            _ => {
                                let error = result.unwrap_err();
                                prop_assert!(error.is::<crate::WorkflowClosing>(), "{error:?}");
                            }
                        }
                        runtime
                            .close_attempt_tracking
                            .lock()
                            .unwrap()
                            .remove(&run_key);
                    }
                }
                prop_assert_eq!(lane.reserved_claims(), before);

                // Fill the run's share, or the lane's budget with other runs'
                // claims, then signal.
                let mut claims = Vec::new();
                match fill {
                    Fill::RunShare => {
                        while let Ok(claim) = lane.reserve(run_key) {
                            claims.push(claim);
                        }
                    }
                    Fill::LaneBudget => {
                        let mut other = RunKey::new();
                        loop {
                            match lane.reserve(other) {
                                Ok(claim) => claims.push(claim),
                                Err(crate::lane::LaneRefusal::RunShareFull) => {
                                    other = RunKey::new();
                                }
                                Err(crate::lane::LaneRefusal::LaneBudgetFull) => break,
                            }
                        }
                    }
                }
                let reads_before = reads(&repo);
                let error = runtime
                    .signal_workflow(execution.clone(), signal(&runtime, "refused"))
                    .await
                    .unwrap_err();
                match fill {
                    Fill::RunShare => prop_assert!(error.is::<crate::WorkflowBusy>(), "{error:?}"),
                    Fill::LaneBudget => {
                        prop_assert!(error.is::<crate::LaneOverloaded>(), "{error:?}");
                    }
                }
                let mut expected = reads_before;
                expected[0] += 1;
                prop_assert_eq!(reads(&repo), expected);
                prop_assert_eq!(lane.reserved_claims(), claims.len());
                Ok(())
            })?;
    }
}
