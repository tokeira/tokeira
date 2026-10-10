//! The runtime's side of the bundle lease fence (`bundle-lease-fence`): every
//! write's fence comes from the ownership that admitted it, a lost fence ends
//! the write and the home's acquisition, and an owner stops admitting writes at
//! its own deadline.

use std::{
    path::{Path, PathBuf},
    time::Duration as StdDuration,
};

use tokeira_storage::{InMemoryStore, LeaseOutcome, LeaseRepository};
use tokeira_types::{Memo, RequestId, SearchAttributes, WorkflowId, WorkflowType};

use super::*;

/// The execution home of every run in these tests: the store has one shard.
pub(super) const HOME: ShardId = ShardId(0);
/// The lease owner of the runtime under test.
pub(super) const NODE_A: &str = "lease-fence-node-a";
/// Another node, which takes the home over.
pub(super) const NODE_B: &str = "lease-fence-node-b";

/// A controller-managed runtime over `store` that fences its writes with the
/// leases it acquires as `NODE_A`.
pub(super) fn fenced_runtime(store: Arc<InMemoryStore>) -> Arc<TokeiraRuntime<InMemoryStore>> {
    Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        store,
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        1,
        NODE_A.to_owned(),
        "127.0.0.1:7233".to_owned(),
        false,
        None,
    ))
}

/// Take the home over as `NODE_B` behind the runtime's back: release the lease
/// the runtime holds at `epoch`, then acquire it. Returns the new epoch.
pub(super) async fn take_over_behind_its_back(
    store: &InMemoryStore,
    epoch: ShardEpoch,
) -> ShardEpoch {
    let released = store
        .relinquish_bundle(HOME, NODE_A.to_owned(), epoch)
        .await
        .unwrap();
    assert!(
        matches!(released, LeaseOutcome::Acquired { .. }),
        "{released:?}"
    );
    match store
        .try_acquire_bundle(HOME, NODE_B.to_owned(), "127.0.0.1:7234".to_owned())
        .await
        .unwrap()
    {
        LeaseOutcome::Acquired { epoch } => epoch,
        other => panic!("the takeover answered {other:?}"),
    }
}

fn start_request() -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::new(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("lease-fence".to_owned()),
        run_id,
        workflow_type: WorkflowType("test".to_owned()),
        task_queue: TaskQueueName("test".to_owned()),
        input: Payloads::default(),
        header: None,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: WorkflowIdConflictPolicy::Fail,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: None,
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
        request: request_context("lease-fence-start"),
        now: OffsetDateTime::now_utc(),
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

fn request_context(id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.to_owned()),
        caller_identity: None,
        principal: None,
        received_at: OffsetDateTime::now_utc(),
    }
}

fn signal(index: usize) -> Command {
    Command::Signal(SignalRequest {
        signal_name: "lease-fence".to_owned(),
        input: Payloads::default(),
        header: None,
        links: Vec::new(),
        request: request_context(&format!("lease-fence-signal-{index}")),
        now: OffsetDateTime::now_utc(),
    })
}

fn is_not_owner(result: &Result<CommitResult>) -> Option<&NotShardOwner> {
    result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<NotShardOwner>())
}

/// A write whose fence another node's takeover has lost ends as
/// `NotShardOwner`, which no lane retries, carrying the lease's new epoch. It
/// changes nothing, and the home's acquisition is cancelled, so the home admits
/// nothing more.
// Feature: bundle-lease-fence, Property 4: A lost fence is final
#[tokio::test]
async fn a_lost_fence_ends_the_write_and_the_homes_acquisition() {
    let store = Arc::new(InMemoryStore::with_shard_count(1));
    let runtime = fenced_runtime(store.clone());
    let epoch = runtime.acquire_shard(HOME).await.unwrap();
    let request = start_request();
    let run_key = request.run_key;
    assert!(matches!(
        runtime.start_workflow(request).await.unwrap(),
        CommitResult::Applied { .. }
    ));
    let taken = take_over_behind_its_back(&store, epoch).await;
    let acquisition = runtime
        .shard_owner
        .read()
        .unwrap()
        .acquisition(HOME)
        .unwrap();
    assert!(
        runtime.shard_owner.read().unwrap().is_active(HOME),
        "the runtime still believes it holds the home"
    );

    let before = store.snapshot().await.unwrap();
    let written = runtime.submit(run_key, signal(0)).await;
    let not_owner = is_not_owner(&written).unwrap_or_else(|| panic!("{written:?}"));
    assert_eq!(not_owner.bundle_id, HOME);
    assert_eq!(not_owner.current_epoch, taken);
    assert_eq!(
        store.snapshot().await.unwrap(),
        before,
        "the write changed nothing"
    );
    assert!(
        acquisition.cancel.is_cancelled(),
        "the home's acquisition is cancelled"
    );
    assert!(!runtime.shard_owner.read().unwrap().is_active(HOME));
    let refused = runtime.submit(run_key, signal(1)).await;
    assert!(is_not_owner(&refused).is_some(), "{refused:?}");
}

/// A deletion's first transaction under a lost fence also ends as
/// `NotShardOwner`, leaving the run in place.
// Feature: bundle-lease-fence, Property 4: A lost fence is final
#[tokio::test]
async fn a_lost_fence_ends_a_deletion() {
    let store = Arc::new(InMemoryStore::with_shard_count(1));
    let runtime = fenced_runtime(store.clone());
    let epoch = runtime.acquire_shard(HOME).await.unwrap();
    let request = start_request();
    let run_key = request.run_key;
    runtime.start_workflow(request).await.unwrap();
    let terminated = runtime
        .submit(
            run_key,
            Command::Terminate(tokeira_kernel::TerminateRequest {
                reason: "lease-fence".to_owned(),
                details: None,
                identity: "lease-fence".to_owned(),
                links: Vec::new(),
                request: request_context("lease-fence-terminate"),
                now: OffsetDateTime::now_utc(),
            }),
        )
        .await
        .unwrap();
    assert!(matches!(terminated, CommitResult::Applied { .. }));
    let taken = take_over_behind_its_back(&store, epoch).await;

    let before = store.snapshot().await.unwrap();
    let deleted = runtime
        .delete_workflow(
            run_key,
            DeleteWorkflowRequest {
                request: request_context("lease-fence-delete"),
                now: OffsetDateTime::now_utc(),
                purge_inline: true,
            },
        )
        .await;
    let error = deleted.expect_err("a deletion under a lost fence must fail");
    let not_owner = error
        .downcast_ref::<NotShardOwner>()
        .unwrap_or_else(|| panic!("{error:?}"));
    assert_eq!(not_owner.current_epoch, taken);
    assert_eq!(
        store.snapshot().await.unwrap(),
        before,
        "the run is still there"
    );
}

/// An owner whose lease lapses without renewal stops admitting writes at its
/// local deadline, without dispatch reconciliation, under a steady write load.
/// Another node then takes the home over.
// Feature: bundle-lease-fence, Property 3: A lapsed lease is taken over
#[tokio::test(start_paused = true)]
async fn a_lapsed_owner_admits_nothing_and_is_taken_over() {
    const LEASE: StdDuration = StdDuration::from_secs(10);
    let store =
        Arc::new(InMemoryStore::with_shard_count(1).with_lease_duration(Duration::seconds(10)));
    let runtime = fenced_runtime(store.clone());
    assert!(!runtime.shard_owner.read().unwrap().reconciliation_enabled());
    let acquired_at = tokio::time::Instant::now();
    let epoch = runtime.acquire_shard(HOME).await.unwrap();
    let request = start_request();
    let run_key = request.run_key;
    runtime.start_workflow(request).await.unwrap();
    store.stall_lease_renewals().await;

    let deadline = acquired_at + LEASE;
    let mut admitted_late = 0;
    let mut admitted_early = 0;
    for index in 0..30 {
        tokio::time::advance(StdDuration::from_millis(500)).await;
        let now = tokio::time::Instant::now();
        let written = runtime.submit(run_key, signal(index)).await;
        if now + StdDuration::from_secs(1) < deadline {
            assert!(
                matches!(written, Ok(CommitResult::Applied { .. })),
                "a write before the deadline: {written:?}"
            );
            admitted_early += 1;
        } else if now >= deadline && is_not_owner(&written).is_none() {
            admitted_late += 1;
        }
    }
    assert!(admitted_early > 0);
    assert_eq!(
        admitted_late, 0,
        "the owner admitted writes after its deadline"
    );
    assert!(!runtime.shard_owner.read().unwrap().is_active(HOME));

    let taken = store
        .try_acquire_bundle(HOME, NODE_B.to_owned(), "127.0.0.1:7234".to_owned())
        .await
        .unwrap();
    assert_eq!(
        taken,
        LeaseOutcome::Acquired {
            epoch: epoch.next()
        },
        "the takeover of the lapsed lease"
    );
}

/// The `src` files whose code is production code: not test modules.
fn production_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if path.is_dir() {
            production_sources(&path, files);
        } else if name.ends_with(".rs") && !name.ends_with("tests.rs") && name != "test_support.rs"
        {
            files.push(path);
        }
    }
}

/// A file's text before its first inline test module.
fn before_tests(text: &str) -> &str {
    let mut end = text.len();
    for marker in [
        "\n#[cfg(test)]\nmod ",
        "\n#[cfg(test)]\npub(crate) mod ",
        "\n#[cfg(test)]\npub mod ",
    ] {
        let mut from = 0;
        while let Some(found) = text[from..].find(marker) {
            let at = from + found;
            // An inline module, not a declaration of a module file.
            let line_end = text[at + 1..].find('\n').map_or(text.len(), |n| at + 1 + n);
            let next_line_end = text[line_end + 1..]
                .find('\n')
                .map_or(text.len(), |n| line_end + 1 + n);
            if text[line_end..next_line_end].trim_end().ends_with('{') {
                end = end.min(at);
                break;
            }
            from = at + marker.len();
        }
    }
    &text[..end]
}

/// The runtime's writes of a run's authoritative state, by file: the lane's
/// commit and reset materialization, the activity paths that commit directly,
/// a deletion's first transaction, and a takeover's recovery switch. A new
/// writer fails the audit until its fence is checked and it is listed here.
const FENCED_WRITERS: [(&str, usize); 4] = [
    ("lane.rs", 2),
    ("runtime/activity.rs", 5),
    ("runtime/lifecycle.rs", 1),
    ("purge.rs", 1),
];

/// No runtime writer passes `WriteFence::Unfenced` itself: every fence comes
/// from `ShardOwner`, which builds the home's lease fence under
/// controller-managed placement, so no write is unfenced there
/// (`bundle-lease-fence` criterion 2.7).
#[test]
fn every_runtime_write_takes_its_fence_from_the_ownership_state() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    production_sources(&src, &mut files);
    let mut writers = std::collections::BTreeMap::new();
    let mut offenders = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let code = before_tests(&text);
        let relative = file
            .strip_prefix(&src)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let count = [
            ".commit_transition(",
            ".commit_transition_for_bundle(",
            ".delete_run_for_bundle(",
            ".materialize_reset_successor(",
            ".abandon_materialization(",
        ]
        .iter()
        .map(|method| code.matches(method).count())
        .sum::<usize>();
        if count > 0 {
            writers.insert(relative.clone(), count);
        }
        for (index, line) in code.lines().enumerate() {
            if line.contains("WriteFence::Unfenced") && relative != "shard.rs" {
                offenders.push(format!("{relative}:{}: {}", index + 1, line.trim()));
            }
        }
    }
    let expected = FENCED_WRITERS
        .iter()
        .map(|(file, count)| ((*file).to_owned(), *count))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(writers, expected, "the runtime's fenced writers changed");
    assert!(
        offenders.is_empty(),
        "unfenced runtime writes:\n{}",
        offenders.join("\n")
    );
}
