//! Generated owner-handler checks for the shared preparation image and envelope.
//! Socket integration remains in the baseline; these use the same runtime lanes
//! and authenticated handler without a listener so the sandbox can execute them.

use super::*;
use crate::{EdgeContext, EdgeError, EdgeResult, interceptors::Authenticator};
use proptest::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokeira_auth::{AuthPrincipal, Claims, WorkerCallTarget, WorkerTarget};
use tokeira_kernel::{BasicKernel, Command, Kernel, LoadedRun, SignalRequest};
use tokeira_storage::{InMemoryStore, WriteFence, read_probe::ReadProbe};
use tokeira_types::{RequestContext, StickyAffinity, execution_home_bundle};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct Policy {
    targets: Mutex<Vec<String>>,
    target_calls: AtomicUsize,
    pause_second: AtomicBool,
    deny: AtomicBool,
    reached: Notify,
    resume: Notify,
}

#[async_trait]
impl Authenticator for Policy {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        if headers.get("authorization").and_then(|v| v.to_str().ok())
            != Some("Bearer synthetic-worker")
        {
            return Err(EdgeError::Unauthorized("worker credential required".into()));
        }
        Ok(None)
    }
    async fn authorize(
        &self,
        _: Option<&Claims>,
        _: Action,
        _: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        Ok(None)
    }
    async fn authorize_worker(
        &self,
        _: Option<&Claims>,
        _: Action,
        _: Option<&str>,
        worker: WorkerCallTarget<'_>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        let normal = match worker.target {
            WorkerTarget::TaskQueue { normal_task_queue }
            | WorkerTarget::VersionedTask {
                normal_task_queue, ..
            } => normal_task_queue,
            _ => return Ok(None),
        };
        self.targets.lock().unwrap().push(normal.into());
        if self.target_calls.fetch_add(1, Ordering::SeqCst) == 1
            && self.pause_second.load(Ordering::SeqCst)
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        if self.deny.load(Ordering::SeqCst) {
            return Err(EdgeError::Unauthorized("worker policy changed".into()));
        }
        Ok(None)
    }
}

struct Fixture {
    repo: Arc<InMemoryStore>,
    runtime: Arc<TokeiraRuntime<InMemoryStore>>,
    service: WorkflowTaskDispatchGrpc<InMemoryStore>,
    request: StartWorkflowTaskRequest,
    key: RunKey,
    policy: Arc<Policy>,
}

async fn fixture(index: u32, sticky: bool, warm: bool) -> Fixture {
    let repo = Arc::new(InMemoryStore::with_shard_count(8));
    let context = EdgeContext {
        request_id: crate::request_id::RequestId::new("prepared-property"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: time::OffsetDateTime::now_utc(),
        is_long_poll: false,
    };
    let mut dto = tests::start_dto();
    dto.workflow_id = format!("prepared-{index}");
    dto.task_queue = format!("normal-{index}");
    let request = crate::translate::to_internal::start_request(
        dto,
        &context,
        time::OffsetDateTime::now_utc(),
    );
    let home = execution_home_bundle(
        request.namespace_id.0.as_bytes(),
        request.workflow_id.0.as_bytes(),
        8,
    );
    let key = request.run_key;
    let normal = request.task_queue.0.clone();
    let selected = if sticky {
        format!("sticky-{index}")
    } else {
        normal.clone()
    };
    let mut initial = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(request))
        .unwrap();
    if sticky {
        initial.next_state.sticky = Some(StickyAffinity {
            worker_identity: WorkerIdentity("worker".into()),
            sticky_queue: TaskQueueName(selected.clone()),
            schedule_to_start_timeout: time::Duration::minutes(5),
        });
        initial
            .next_state
            .pending_workflow_task
            .as_mut()
            .unwrap()
            .schedule_to_start_deadline =
            Some(time::OffsetDateTime::now_utc() + time::Duration::minutes(5));
    }
    repo.commit_transition(key, initial, WriteFence::Unfenced)
        .await
        .unwrap();
    let runtime = read_baseline::runtime(repo.clone(), "prepared-owner");
    runtime.acquire_shard(home).await.unwrap();
    if warm {
        signal(&runtime, key).await;
    }
    let policy = Arc::new(Policy::default());
    let service = read_baseline::owner_service(repo.clone(), runtime.clone(), policy.clone()).await;
    Fixture {
        repo,
        runtime,
        service,
        key,
        policy,
        request: StartWorkflowTaskRequest {
            run_key: key.0.as_bytes().to_vec(),
            logical_seq: 1,
            namespace: "default".into(),
            task_queue: selected,
            deployment: None,
            build_id: None,
            worker_identity: "worker".into(),
            sticky,
            normal_task_queue: sticky.then_some(normal),
            scoped_versioned: false,
        },
    }
}

async fn signal(runtime: &TokeiraRuntime<InMemoryStore>, key: RunKey) {
    let now = time::OffsetDateTime::now_utc();
    runtime
        .submit(
            key,
            Command::Signal(SignalRequest {
                signal_name: "concurrent".into(),
                input: Default::default(),
                header: None,
                links: Vec::new(),
                request: RequestContext {
                    request_id: tokeira_types::RequestId(uuid::Uuid::new_v4().to_string()),
                    ..RequestContext::unattributed(now)
                },
                now,
            }),
        )
        .await
        .unwrap();
}

fn offered(fixture: &Fixture) -> DispatchableWorkflowTask {
    DispatchableWorkflowTask {
        run_key: fixture.key,
        logical_seq: LogicalTaskSeq::ONE,
        queue: QueueKey {
            namespace_id: tokeira_types::NamespaceId::new(),
            task_queue: TaskQueueName(fixture.request.task_queue.clone()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        },
        sticky_preferred: fixture
            .request
            .sticky
            .then(|| WorkerIdentity("worker".into())),
        normal_queue: None,
        sticky_deadline: None,
        priority: None,
        order: None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 6. Forged bindings and over-limit attachments never commit.
    #[test]
    fn property_6_prepared_bindings_and_envelope(index in any::<u32>(), bytes in 1usize..4096, warm in any::<bool>(), conflict in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut f = fixture(index, true, warm).await;
            let before = f.repo.load_run(f.key).await.unwrap();
            let LoadedRun::Existing(state) = &before else { panic!("fixture missing") };
            let real_home = execution_home_bundle(state.namespace_id.0.as_bytes(), state.workflow_id.0.as_bytes(), 8);
            let misplaced = read_baseline::runtime(f.repo.clone(), "misplaced-owner");
            misplaced.acquire_shard(ShardId((real_home.0 + 1) % 8)).await.unwrap();
            let misplaced_service = read_baseline::owner_service(f.repo.clone(), misplaced.clone(), Arc::new(Policy::default())).await;
            let error = misplaced_service.start_workflow_task(tests::authorized_reply(f.request.clone())).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable);
            assert_eq!(f.repo.load_run(f.key).await.unwrap(), before);
            read_baseline::stop(&misplaced).await;
            for field in 1..9 {
                let mut request = f.request.clone();
                match field {
                    1 => request.namespace = "other".into(),
                    2 => { request.sticky = false; request.task_queue = format!("forged-normal-{index}"); },
                    3 => request.task_queue = format!("forged-sticky-{index}"),
                    4 => request.worker_identity = format!("forged-worker-{index}"),
                    5 => request.deployment = Some(format!("forged-deployment-{index}")),
                    6 => request.build_id = Some(format!("forged-build-{index}")),
                    7 => request.normal_task_queue = Some(format!("forged-origin-{index}")),
                    _ => request.logical_seq += 1,
                }
                let result = f.service.start_workflow_task(tests::authorized_reply(request)).await;
                assert!(result.is_err(), "forged field {field} committed");
                assert_eq!(f.repo.load_run(f.key).await.unwrap(), before);
            }
            let LoadedRun::Existing(state) = f.repo.load_run(f.key).await.unwrap() else { panic!("fixture missing") };
            f.runtime.update_workflow(
                tokeira_types::ExecutionRef { namespace_id: state.namespace_id, workflow_id: state.workflow_id, run_id: Some(state.run_id) },
                "prepared-update".into(), "update-handler".into(),
                tokeira_types::Payloads(vec![tokeira_types::Payload::new(vec![42; bytes])]),
                RequestContext::unattributed(time::OffsetDateTime::now_utc()),
                time::Duration::seconds(30), tokeira_runtime::UpdateWaitPolicy::Admitted, 64,
            ).await.unwrap();
            let before = f.repo.load_run(f.key).await.unwrap();
            let updates = f.runtime.update_registry().snapshot_pending_updates(f.key);
            assert_eq!(updates.len(), 1);
            let mut waiter = tests::buffer_query(&f.runtime, f.key, 0, vec![255; bytes]);
            let queries = f.runtime.buffered_queries().snapshot(f.key);
            let decoration = WorkflowTaskDecorations {
                queries: queries.iter().map(|q| (q.query_id.clone(), WorkflowQueryDto { query_type: q.query_type.clone(), query_args: q.query_args.clone() })).collect(),
                messages: updates.iter().map(|update| update_message(update, i64::MAX)).collect(),
            };
            let exact = decoration.encode_proto().len();
            f.service.attachment_limit = exact - 1;
            assert_eq!(f.service.start_workflow_task(tests::authorized_reply(f.request.clone())).await.unwrap_err().code(), Code::ResourceExhausted);
            assert_eq!(f.repo.load_run(f.key).await.unwrap(), before);
            assert!(matches!(waiter.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
            assert_eq!(f.runtime.buffered_queries().snapshot(f.key).len(), 1);
            assert_eq!(f.runtime.update_registry().snapshot_pending_updates(f.key).len(), 1);
            f.service.attachment_limit = exact;
            let probe = ReadProbe::start(f.key, "generated-envelope");
            if conflict { probe.inject_conflicts(1); }
            let response = f.service.start_workflow_task(tests::authorized_reply(f.request)).await.unwrap().into_inner();
            let delivered = WorkflowTaskDecorations::decode_proto(&response.decorations_proto).unwrap();
            assert_eq!(delivered.queries, decoration.queries);
            assert_eq!(delivered.messages.len(), 1);
            assert_eq!(delivered.messages[0].protocol_instance_id, "prepared-update");
            assert!(f.runtime.update_registry().mark_prepared_updates_sent(f.key, &updates, false).is_empty());
            assert!(response.encoded_len() <= MAX_START_RESPONSE_BYTES);
            assert!(matches!(waiter.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
            let report = probe.report();
            assert_eq!(report.counts.get(&("execution-owner/preparation", "logical")), Some(&1));
            assert_eq!(report.counts.get(&("execution-owner/lane-cache-or-retry", "logical")).copied().unwrap_or(0), usize::from(conflict));
            drop(probe);
            read_baseline::stop(&f.runtime).await;
        });
    }

    // Feature: queue-home-delivery, Property 6. Unrelated transitions preserve starts; sticky policy is checked twice.
    #[test]
    fn property_6_prepared_state_and_policy_changes(index in any::<u32>(), warm in any::<bool>(), external in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let f = fixture(index, true, warm).await;
            let (_, stats) = f.repo.load_run_with_stats(f.key).await.unwrap();
            let preparation = ReadProbe::start(f.key, "concurrent-preparation");
            let prepared = f.runtime.prepare_workflow_start(f.key).await.unwrap();
            assert_eq!(preparation.report().counts.get(&("execution-owner/preparation", "logical")), Some(&1));
            drop(preparation);
            let mut task = offered(&f);
            task.queue.namespace_id = prepared.state().namespace_id;
            if external {
                let now = time::OffsetDateTime::now_utc();
                let transition = BasicKernel.apply(f.repo.load_run(f.key).await.unwrap(), Command::Signal(SignalRequest {
                    signal_name: "competing".into(), input: Default::default(), header: None, links: Vec::new(),
                    request: RequestContext { request_id: tokeira_types::RequestId(uuid::Uuid::new_v4().to_string()), ..RequestContext::unattributed(now) }, now,
                })).unwrap();
                f.repo.commit_transition(f.key, transition, WriteFence::Unfenced).await.unwrap();
            } else {
                signal(&f.runtime, f.key).await;
            }
            let changed = f.repo.load_run(f.key).await.unwrap();
            f.policy.target_calls.store(0, Ordering::SeqCst);
            f.policy.pause_second.store(true, Ordering::SeqCst);
            let (result, ()) = tokio::join!(
                f.service.start_workflow_task(tests::authorized_reply(f.request.clone())),
                async {
                    f.policy.reached.notified().await;
                    f.policy.deny.store(true, Ordering::SeqCst);
                    f.policy.resume.notify_one();
                }
            );
            assert!(result.is_err());
            assert_eq!(f.repo.load_run(f.key).await.unwrap(), changed);
            assert_eq!(f.policy.targets.lock().unwrap().as_slice(), &[format!("normal-{index}"), format!("normal-{index}")]);
            f.policy.pause_second.store(false, Ordering::SeqCst);
            f.policy.deny.store(false, Ordering::SeqCst);
            let probe = ReadProbe::start(f.key, "concurrent-image-change");
            f.runtime.start_prepared_workflow_task(prepared, task, WorkerIdentity("worker".into())).await.unwrap();
            let report = probe.report();
            assert_eq!(report.counts.get(&("execution-owner/lane-cache-or-retry", "logical")).copied().unwrap_or(0), usize::from(external && warm));
            assert_eq!(report.counts.get(&("execution-owner/admission-cache", "logical")).copied().unwrap_or(0), usize::from(external && !warm));
            assert!(report.counts.keys().all(|(phase, _)| matches!(*phase, "execution-owner/lane-cache-or-retry" | "execution-owner/admission-cache")));
            drop(probe);
            let LoadedRun::Existing(started) = f.repo.load_run(f.key).await.unwrap() else { panic!("run missing") };
            let LoadedRun::Existing(changed) = changed else { panic!("run missing") };
            assert_eq!(started.transition_seq, changed.transition_seq.next());
            assert!(started.pending_workflow_task.as_ref().unwrap().started_event_id.is_some());
            assert_eq!(started.pending_workflow_task.as_ref().unwrap().advice.history_size_bytes, stats.history_size_bytes);
            let history = f.repo.read_history_to_end(f.key, 0).await.unwrap();
            assert!(history.iter().any(|event| matches!(event.kind, tokeira_kernel::HistoryEventKind::WorkflowTaskStarted { history_size_bytes, .. } if history_size_bytes == stats.history_size_bytes)));
            read_baseline::stop(&f.runtime).await;
        });
    }
}
