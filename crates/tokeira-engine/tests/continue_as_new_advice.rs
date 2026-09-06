//! Continue-as-new advice through real commits on an embedded engine.
//!
//! The pieces are exercised together rather than in isolation: storage
//! accounts the History Size when a batch commits, the runtime hands the
//! operands to the kernel at every workflow-task start, the kernel records the
//! Advice on the started event, and the edge delivers that one record on every
//! path a worker or operator reads it from — the persisted event, the poll
//! response's synthesized suffix, the history read's synthesized suffix, and
//! the materialized event of a transient attempt. The delivery-path scenario
//! runs over the in-process endpoint and over a bound listener; the boundary
//! scenarios drive thousands of commits and stay in-process.
//!
//! Thresholds are the release defaults the runtime pins
//! (`common/dynamicconfig/constants.go:370-375, 412-417 @ v1.31.0`); the
//! engine crate has no override seam, so the mid-run threshold change is
//! covered by the runtime's policy tests instead.

mod listener_support;

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context as _, Result, bail, ensure};
use listener_support::{
    STEP, Transport, execution, runtime, start_engine_with_listener, task_queue,
};
use proptest::prelude::*;
use tokeira_engine::Engine;
use tokeira_proto::{
    common::{Payload, Payloads, WorkflowType},
    enums::{CommandType, SuggestContinueAsNewReason, WorkflowTaskFailedCause},
    failure::Failure,
    public::temporal::api::{
        command::v1::{
            Command, CompleteWorkflowExecutionCommandAttributes,
            command::Attributes as CommandAttributes,
        },
        history::v1::{HistoryEvent, history_event::Attributes as EventAttributes},
    },
    workflowservice::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
        GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
        PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
        RespondWorkflowTaskFailedRequest, RespondWorkflowTaskFailedResponse,
        SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    },
};

const NAMESPACE: &str = "default";
const IDENTITY: &str = "advice-worker";
/// `limit.historyCount.suggestContinueAsNew` (`constants.go:412-417 @ v1.31.0`).
const COUNT_THRESHOLD: i64 = 4 * 1024;
/// `limit.historySize.suggestContinueAsNew` (`constants.go:370-375 @ v1.31.0`).
const SIZE_THRESHOLD: i64 = 4 * 1024 * 1024;

static REQUESTS: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// The Advice as a worker reads it
// ---------------------------------------------------------------------------

/// The three Advice fields of one started event, compared as a unit.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Advice {
    history_size_bytes: i64,
    suggest_continue_as_new: bool,
    reasons: Vec<i32>,
}

impl Advice {
    /// The v1.31.0 rule at the pinned defaults: `>=` on both operands, reasons
    /// in enum order, the flag set iff a reason is present
    /// (`workflow_task_state_machine.go:1440-1466 @ v1.31.0`).
    fn expected(history_size_bytes: i64, next_event_id: i64) -> Self {
        let mut reasons = Vec::new();
        if history_size_bytes >= SIZE_THRESHOLD {
            reasons.push(SuggestContinueAsNewReason::HistorySizeTooLarge as i32);
        }
        if next_event_id >= COUNT_THRESHOLD {
            reasons.push(SuggestContinueAsNewReason::TooManyHistoryEvents as i32);
        }
        Self {
            history_size_bytes,
            suggest_continue_as_new: !reasons.is_empty(),
            reasons,
        }
    }
}

fn started_advice(event: &HistoryEvent) -> Option<Advice> {
    match &event.attributes {
        Some(EventAttributes::WorkflowTaskStartedEventAttributes(attrs)) => Some(Advice {
            history_size_bytes: attrs.history_size_bytes,
            suggest_continue_as_new: attrs.suggest_continue_as_new,
            reasons: attrs.suggest_continue_as_new_reasons.clone(),
        }),
        _ => None,
    }
}

/// The Advice on the `WorkflowTaskStarted` event with `event_id`.
fn advice_of(events: &[HistoryEvent], event_id: i64) -> Result<Advice> {
    events
        .iter()
        .find(|event| event.event_id == event_id)
        .and_then(started_advice)
        .with_context(|| format!("no WorkflowTaskStarted event with id {event_id}"))
}

fn poll_history(task: &PollWorkflowTaskQueueResponse) -> &[HistoryEvent] {
    task.history
        .as_ref()
        .map(|history| history.events.as_slice())
        .unwrap_or(&[])
}

// ---------------------------------------------------------------------------
// Raw-proto worker and operator calls over a transport
// ---------------------------------------------------------------------------

/// Start `workflow_id` on a task queue of the same name, so polls never cross
/// workflows; returns the run id.
async fn start(worker: &Transport, workflow_id: &str) -> Result<String> {
    let response: StartWorkflowExecutionResponse = worker
        .unary(
            "StartWorkflowExecution",
            StartWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_id: workflow_id.to_owned(),
                workflow_type: Some(WorkflowType {
                    name: "advice".to_owned(),
                }),
                task_queue: Some(task_queue(workflow_id)),
                request_id: format!("start-{workflow_id}"),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await
        .with_context(|| format!("{}: start failed", worker.label()))?;
    Ok(response.run_id)
}

async fn poll(worker: &Transport, queue: &str) -> Result<PollWorkflowTaskQueueResponse> {
    let task: PollWorkflowTaskQueueResponse = tokio::time::timeout(
        STEP,
        worker.unary(
            "PollWorkflowTaskQueue",
            PollWorkflowTaskQueueRequest {
                namespace: NAMESPACE.to_owned(),
                task_queue: Some(task_queue(queue)),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
            &[],
        ),
    )
    .await
    .with_context(|| format!("{}: poll did not return", worker.label()))?
    .with_context(|| format!("{}: poll failed", worker.label()))?;
    ensure!(
        !task.task_token.is_empty(),
        "{}: poll returned no task",
        worker.label()
    );
    Ok(task)
}

async fn complete(worker: &Transport, task_token: Vec<u8>, commands: Vec<Command>) -> Result<()> {
    let _: RespondWorkflowTaskCompletedResponse = worker
        .unary(
            "RespondWorkflowTaskCompleted",
            RespondWorkflowTaskCompletedRequest {
                task_token,
                identity: IDENTITY.to_owned(),
                namespace: NAMESPACE.to_owned(),
                commands,
                ..Default::default()
            },
            &[],
        )
        .await
        .with_context(|| format!("{}: completion failed", worker.label()))?;
    Ok(())
}

/// Fail the attempt the way an unhandled worker error does; the next attempt
/// is transient, so its start persists nothing until it completes.
async fn fail(worker: &Transport, task_token: Vec<u8>) -> Result<()> {
    let _: RespondWorkflowTaskFailedResponse = worker
        .unary(
            "RespondWorkflowTaskFailed",
            RespondWorkflowTaskFailedRequest {
                task_token,
                cause: WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure as i32,
                failure: Some(Failure {
                    message: "boom".to_owned(),
                    ..Default::default()
                }),
                identity: IDENTITY.to_owned(),
                namespace: NAMESPACE.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await
        .with_context(|| format!("{}: failure report failed", worker.label()))?;
    Ok(())
}

/// One signal carrying `payload_len` bytes. Everything but the payload has a
/// fixed width so two signals differ in encoded size only by their payloads
/// and by their timestamps.
async fn signal(
    worker: &Transport,
    workflow_id: &str,
    run_id: &str,
    payload_len: usize,
) -> Result<()> {
    let sequence = REQUESTS.fetch_add(1, Ordering::Relaxed);
    let _: SignalWorkflowExecutionResponse = worker
        .unary(
            "SignalWorkflowExecution",
            SignalWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_execution: Some(execution(workflow_id, run_id)),
                signal_name: "bump".to_owned(),
                input: Some(Payloads {
                    payloads: vec![Payload {
                        metadata: BTreeMap::from([(
                            "encoding".to_owned(),
                            b"binary/plain".to_vec(),
                        )]),
                        data: vec![b'x'; payload_len],
                        ..Default::default()
                    }],
                }),
                identity: IDENTITY.to_owned(),
                request_id: format!("signal-{sequence:012}"),
                ..Default::default()
            },
            &[],
        )
        .await
        .with_context(|| format!("{}: signal failed", worker.label()))?;
    Ok(())
}

async fn describe_size(worker: &Transport, workflow_id: &str, run_id: &str) -> Result<i64> {
    let response: DescribeWorkflowExecutionResponse = worker
        .unary(
            "DescribeWorkflowExecution",
            DescribeWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                execution: Some(execution(workflow_id, run_id)),
            },
            &[],
        )
        .await
        .with_context(|| format!("{}: describe failed", worker.label()))?;
    Ok(response
        .workflow_execution_info
        .context("describe carried no execution info")?
        .history_size_bytes)
}

async fn full_history(
    worker: &Transport,
    workflow_id: &str,
    run_id: &str,
) -> Result<Vec<HistoryEvent>> {
    let mut events = Vec::new();
    let mut next_page_token = Vec::new();
    loop {
        let page: GetWorkflowExecutionHistoryResponse = worker
            .unary(
                "GetWorkflowExecutionHistory",
                GetWorkflowExecutionHistoryRequest {
                    namespace: NAMESPACE.to_owned(),
                    execution: Some(execution(workflow_id, run_id)),
                    maximum_page_size: 1000,
                    next_page_token,
                    ..Default::default()
                },
                &[],
            )
            .await
            .with_context(|| format!("{}: history read failed", worker.label()))?;
        if let Some(history) = page.history {
            events.extend(history.events);
        }
        if page.next_page_token.is_empty() {
            return Ok(events);
        }
        next_page_token = page.next_page_token;
    }
}

/// Wait until visibility answers a `HistorySizeBytes` equality query with the
/// run. The projection applies after the commit returns, so this synchronises
/// on the observable read instead of sleeping; a rejected query fails at once.
async fn wait_for_visibility_size(worker: &Transport, workflow_id: &str, size: i64) -> Result<()> {
    tokio::time::timeout(STEP, async {
        loop {
            let response: ListWorkflowExecutionsResponse = worker
                .unary(
                    "ListWorkflowExecutions",
                    ListWorkflowExecutionsRequest {
                        namespace: NAMESPACE.to_owned(),
                        page_size: 10,
                        query: format!(
                            "WorkflowId = '{workflow_id}' AND HistorySizeBytes = {size}"
                        ),
                        ..Default::default()
                    },
                    &[],
                )
                .await
                .context("visibility query failed")?;
            if !response.executions.is_empty() {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .with_context(|| {
        format!("visibility never reported HistorySizeBytes = {size} for {workflow_id}")
    })?
}

fn complete_workflow() -> Command {
    Command {
        command_type: CommandType::CompleteWorkflowExecution as i32,
        user_metadata: None,
        attributes: Some(
            CommandAttributes::CompleteWorkflowExecutionCommandAttributes(
                CompleteWorkflowExecutionCommandAttributes { result: None },
            ),
        ),
    }
}

// ---------------------------------------------------------------------------
// Property 3: recorded Advice is identical on every delivery path
// ---------------------------------------------------------------------------

/// One transport's run: a persisted attempt-1 start, a failure, and a
/// transient attempt-2 start, each read on every path that can deliver it.
/// Returns both attempts' Advice for the cross-transport comparison.
async fn delivery_paths(worker: &Transport, workflow_id: &str) -> Result<(Advice, Advice)> {
    let label = worker.label();
    let run_id = start(worker, workflow_id).await?;
    let size_before_first = describe_size(worker, workflow_id, &run_id).await?;
    ensure!(
        size_before_first > 0,
        "{label}: Describe must report a positive size once the start batch committed"
    );

    // Attempt 1 persists its started event; the poll, a history read during
    // the attempt, and the closed history all carry the same record, derived
    // from the size Describe reported before the start and the started id.
    let first = poll(worker, workflow_id).await?;
    ensure!(first.attempt == 1, "{label}: first poll must be attempt 1");
    let first_advice = advice_of(poll_history(&first), first.started_event_id)?;
    let expected_first = Advice::expected(size_before_first, first.started_event_id);
    ensure!(
        first_advice == expected_first,
        "{label}: attempt 1 poll advice {first_advice:?} != {expected_first:?}"
    );
    let during_first = full_history(worker, workflow_id, &run_id).await?;
    ensure!(
        advice_of(&during_first, first.started_event_id)? == first_advice,
        "{label}: history read during attempt 1 disagrees with the poll"
    );

    fail(worker, first.task_token).await?;

    // Attempt 2 is transient: its scheduled and started events live only in
    // the pending record until the attempt completes. The operand is the
    // virtual scheduled id, one past the persisted failed event.
    let size_before_second = describe_size(worker, workflow_id, &run_id).await?;
    ensure!(
        size_before_second > size_before_first,
        "{label}: the persisted failed event must grow the statistic"
    );
    let second = poll(worker, workflow_id).await?;
    ensure!(second.attempt == 2, "{label}: retry poll must be attempt 2");
    ensure!(
        second.started_event_id == first.started_event_id + 3,
        "{label}: transient start id {} must sit past the failed event",
        second.started_event_id
    );
    let second_advice = advice_of(poll_history(&second), second.started_event_id)?;
    let expected_second = Advice::expected(size_before_second, second.started_event_id - 1);
    ensure!(
        second_advice == expected_second,
        "{label}: attempt 2 poll advice {second_advice:?} != {expected_second:?}"
    );
    let during_second = full_history(worker, workflow_id, &run_id).await?;
    ensure!(
        advice_of(&during_second, second.started_event_id)? == second_advice,
        "{label}: history read during the transient attempt disagrees with the poll"
    );

    complete(worker, second.task_token, vec![complete_workflow()]).await?;
    let closed = full_history(worker, workflow_id, &run_id).await?;
    ensure!(
        advice_of(&closed, first.started_event_id)? == first_advice,
        "{label}: the persisted attempt-1 record changed after the run closed"
    );
    ensure!(
        advice_of(&closed, second.started_event_id)? == second_advice,
        "{label}: the materialized transient start disagrees with what the worker was told"
    );
    Ok((first_advice, second_advice))
}

// Feature: continue-as-new-advice, Property 3: recorded Advice is identical on every delivery path
#[tokio::test]
async fn advice_is_identical_on_every_delivery_path_over_both_transports() -> Result<()> {
    let (engine, listener, in_process, network) = start_engine_with_listener().await?;
    let over_in_process = delivery_paths(&in_process, "advice-paths-a").await?;
    let over_network = delivery_paths(&network, "advice-paths-b").await?;
    // The two runs are byte-for-byte the same sequence except for run ids and
    // event timestamps, whose varint widths can differ by a byte, so the
    // sizes are compared within each run above; the decisions must agree.
    for (path, in_process_advice, network_advice) in [
        ("attempt 1", &over_in_process.0, &over_network.0),
        ("attempt 2", &over_in_process.1, &over_network.1),
    ] {
        ensure!(
            in_process_advice.suggest_continue_as_new == network_advice.suggest_continue_as_new
                && in_process_advice.reasons == network_advice.reasons,
            "{path}: transports disagree: {in_process_advice:?} vs {network_advice:?}"
        );
        ensure!(in_process_advice.history_size_bytes > 0 && network_advice.history_size_bytes > 0);
    }
    drop(listener);
    engine.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Boundary examples through real commits
// ---------------------------------------------------------------------------

/// Complete the first task, then signal until the run's last persisted event
/// id is `last_event_id` with the next workflow task scheduled and unpolled.
/// The first signal appends the signaled event and the scheduled event; every
/// later one appends exactly one event while that task stays scheduled.
async fn ramp_to(
    worker: &Transport,
    workflow_id: &str,
    run_id: &str,
    last_event_id: i64,
) -> Result<()> {
    let first = poll(worker, workflow_id).await?;
    complete(worker, first.task_token, Vec::new()).await?;
    // Events 1-4 are the start, and the first task's scheduled, started, and
    // completed events; the first signal lands events 5 and 6.
    let signals = last_event_id - 5;
    ensure!(
        signals >= 1,
        "ramp target {last_event_id} is below the first signal"
    );
    for _ in 0..signals {
        signal(worker, workflow_id, run_id, 0).await?;
    }
    Ok(())
}

// Feature: continue-as-new-advice, Property 3 at the count boundary — 4095 below,
// 4096 at, 4097 above (Requirements 2.1, 2.2, 2.8)
#[tokio::test]
async fn event_count_boundaries_through_real_commits() -> Result<()> {
    let engine = Engine::start().await?;
    let worker = Transport::InProcess(engine.endpoint());
    let count_reason = vec![SuggestContinueAsNewReason::TooManyHistoryEvents as i32];

    // Started id 4095: one below the threshold, no advice. Failing that attempt
    // persists event 4096, and the transient retry's virtual scheduled id 4097
    // is the operand — one above the threshold.
    let below = "count-below";
    let run_id = start(&worker, below).await?;
    ramp_to(&worker, below, &run_id, COUNT_THRESHOLD - 2).await?;
    let size = describe_size(&worker, below, &run_id).await?;
    let task = poll(&worker, below).await?;
    ensure!(task.started_event_id == COUNT_THRESHOLD - 1);
    let below_advice = advice_of(poll_history(&task), task.started_event_id)?;
    ensure!(below_advice == Advice::expected(size, task.started_event_id));
    ensure!(!below_advice.suggest_continue_as_new && below_advice.reasons.is_empty());

    fail(&worker, task.task_token).await?;
    let size = describe_size(&worker, below, &run_id).await?;
    let retry = poll(&worker, below).await?;
    ensure!(retry.attempt == 2 && retry.started_event_id == COUNT_THRESHOLD + 2);
    let above_advice = advice_of(poll_history(&retry), retry.started_event_id)?;
    ensure!(above_advice == Advice::expected(size, COUNT_THRESHOLD + 1));
    ensure!(above_advice.suggest_continue_as_new && above_advice.reasons == count_reason);
    complete(&worker, retry.task_token, vec![complete_workflow()]).await?;
    let closed = full_history(&worker, below, &run_id).await?;
    ensure!(advice_of(&closed, task.started_event_id)? == below_advice);
    ensure!(
        advice_of(&closed, retry.started_event_id)? == above_advice,
        "the materialized transient start must keep the recorded advice"
    );

    // Started id 4096: exactly the threshold, advised.
    let at = "count-at";
    let run_id = start(&worker, at).await?;
    ramp_to(&worker, at, &run_id, COUNT_THRESHOLD - 1).await?;
    let size = describe_size(&worker, at, &run_id).await?;
    let task = poll(&worker, at).await?;
    ensure!(task.started_event_id == COUNT_THRESHOLD);
    let at_advice = advice_of(poll_history(&task), task.started_event_id)?;
    ensure!(at_advice == Advice::expected(size, COUNT_THRESHOLD));
    ensure!(at_advice.suggest_continue_as_new && at_advice.reasons == count_reason);
    complete(&worker, task.task_token, vec![complete_workflow()]).await?;
    let closed = full_history(&worker, at, &run_id).await?;
    ensure!(advice_of(&closed, task.started_event_id)? == at_advice);

    engine.shutdown().await?;
    Ok(())
}

/// Land the run's History Size exactly on `target` with a task scheduled and
/// unpolled: fill with 1 MiB signals, measure one signal's fixed overhead with
/// a probe, then close the gap with a payload in the probe's varint-width
/// band. Returns `false` when the closing event's timestamp encoded a byte or
/// two narrower or wider than the probe's did — the one width the caller
/// cannot control — so the caller retries on a fresh run.
async fn land_size_on(
    worker: &Transport,
    workflow_id: &str,
    run_id: &str,
    target: i64,
) -> Result<bool> {
    const CHUNK: usize = 1024 * 1024;
    const PROBE: usize = 20_000;
    let first = poll(worker, workflow_id).await?;
    complete(worker, first.task_token, Vec::new()).await?;
    // The opener carries the scheduled event with it; every later signal
    // appends exactly one event.
    signal(worker, workflow_id, run_id, CHUNK).await?;
    let mut size = describe_size(worker, workflow_id, run_id).await?;
    while target - size > (CHUNK + 4 * PROBE) as i64 {
        signal(worker, workflow_id, run_id, CHUNK).await?;
        size = describe_size(worker, workflow_id, run_id).await?;
    }
    signal(worker, workflow_id, run_id, PROBE).await?;
    let after_probe = describe_size(worker, workflow_id, run_id).await?;
    let overhead = (after_probe - size) - PROBE as i64;
    size = after_probe;
    let closing = target - size - overhead;
    ensure!(
        (16_384..2_097_152).contains(&closing),
        "closing payload {closing} left the probe's varint-width band"
    );
    signal(worker, workflow_id, run_id, closing as usize).await?;
    Ok(describe_size(worker, workflow_id, run_id).await? == target)
}

async fn size_boundary(worker: &Transport, target: i64) -> Result<()> {
    for attempt in 0..20 {
        let workflow_id = format!("size-{target}-{attempt}");
        let run_id = start(worker, &workflow_id).await?;
        if !land_size_on(worker, &workflow_id, &run_id, target).await? {
            continue;
        }
        let task = poll(worker, &workflow_id).await?;
        let advice = advice_of(poll_history(&task), task.started_event_id)?;
        let expected = Advice::expected(target, task.started_event_id);
        ensure!(
            advice == expected,
            "size {target}: poll advice {advice:?} != {expected:?}"
        );
        ensure!(advice.suggest_continue_as_new == (target >= SIZE_THRESHOLD));
        complete(worker, task.task_token, vec![complete_workflow()]).await?;
        let closed = full_history(worker, &workflow_id, &run_id).await?;
        ensure!(advice_of(&closed, task.started_event_id)? == advice);
        return Ok(());
    }
    bail!("could not land the History Size exactly on {target} in twenty runs")
}

// Feature: continue-as-new-advice, Property 3 at the size boundary — one byte
// below, at, and one byte above 4 MiB (Requirements 2.1, 2.8)
#[tokio::test]
async fn history_size_boundaries_through_real_commits() -> Result<()> {
    let engine = Engine::start().await?;
    let worker = Transport::InProcess(engine.endpoint());
    size_boundary(&worker, SIZE_THRESHOLD - 1).await?;
    size_boundary(&worker, SIZE_THRESHOLD).await?;
    size_boundary(&worker, SIZE_THRESHOLD + 1).await?;
    engine.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Property 8: one statistic, three readers
// ---------------------------------------------------------------------------

async fn one_statistic(payload_lens: Vec<usize>) -> Result<()> {
    let engine = Engine::start().await.context("engine starts")?;
    let worker = Transport::InProcess(engine.endpoint());
    let workflow_id = "one-statistic";
    let run_id = start(&worker, workflow_id).await?;
    let first = poll(&worker, workflow_id).await?;
    complete(&worker, first.task_token, Vec::new()).await?;
    let mut previous = describe_size(&worker, workflow_id, &run_id).await?;
    ensure!(
        previous > 0,
        "Describe must be positive after the first commits"
    );
    for len in payload_lens {
        signal(&worker, workflow_id, &run_id, len).await?;
        let size = describe_size(&worker, workflow_id, &run_id).await?;
        ensure!(size > previous, "every committed batch grows the statistic");
        wait_for_visibility_size(&worker, workflow_id, size).await?;
        previous = size;
    }
    let task = poll(&worker, workflow_id).await?;
    let advice = advice_of(poll_history(&task), task.started_event_id)?;
    ensure!(
        advice.history_size_bytes == previous,
        "the next start's operand {} is not the Describe number {previous}",
        advice.history_size_bytes
    );
    complete(&worker, task.task_token, vec![complete_workflow()]).await?;
    engine.shutdown().await?;
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 6,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    // Feature: continue-as-new-advice, Property 8: one statistic, three readers
    #[test]
    fn describe_visibility_and_the_next_start_read_one_statistic(
        payload_lens in prop::collection::vec(0usize..4096, 1..5),
    ) {
        runtime()
            .block_on(one_statistic(payload_lens))
            .map_err(|error| TestCaseError::fail(format!("{error:#}")))?;
    }
}
