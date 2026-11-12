//! Read-only sticky recovery, owned by one repaired execution acquisition.
//!
//! FIFO permits share node capacity fairly between homes. A home retains its
//! cursor only in this task, and releases its permit before its next period.
//! Completed rows advance the cursor even within a page; an incomplete read
//! retries only the unhandled suffix. Nothing here writes durable state.

use std::{collections::BTreeMap, num::NonZeroU32, sync::Mutex};

use super::*;
use crate::discovery::QueueHome;
use tokeira_storage::WorkflowDispatchRow;
use tokeira_types::RunKey;
use tokio::{
    sync::Semaphore,
    time::{Duration, Instant},
};

const PERIOD: Duration = Duration::from_secs(1);
const CONCURRENCY: usize = 8;
const PAGE_SIZE: u32 = 64;

#[cfg(test)]
#[path = "sweep_tests.rs"]
mod tests;

/// Node-wide fair page capacity and per-home registration/rate limits.
#[derive(Debug)]
pub(super) struct Sweeps {
    started: Mutex<BTreeMap<ShardId, SweepHome>>,
    capacity: Semaphore,
}

#[derive(Debug)]
struct SweepHome {
    generation: u64,
    last_query: Option<Instant>,
}

impl Default for Sweeps {
    fn default() -> Self {
        Self {
            started: Mutex::new(BTreeMap::new()),
            capacity: Semaphore::new(CONCURRENCY),
        }
    }
}

impl<R: RunRepository + 'static> WorkflowDelivery<R> {
    /// Install once after repair and activation, including exclusive startup.
    /// Remember only the latest generation per home, never a durable cursor.
    pub(crate) fn start_sticky_sweep(self: &Arc<Self>, acquisition: ShardAcquisition) {
        let owner = self.homes.owner.read().expect("shard owner lock poisoned");
        if !owner.acquisition_active(&acquisition)
            || self.shutdown.cancellation_token().is_cancelled()
        {
            return;
        }
        // Same lock order as acquisition replacement: a delayed old activation
        // cannot register a sweep for the replacement's generation.
        let mut started = self
            .sweeps
            .started
            .lock()
            .expect("sticky sweep lock poisoned");
        if started
            .get(&acquisition.shard_id)
            .is_some_and(|home| home.generation == acquisition.generation)
        {
            return;
        }
        started
            .entry(acquisition.shard_id)
            .and_modify(|home| home.generation = acquisition.generation)
            .or_insert(SweepHome {
                generation: acquisition.generation,
                last_query: None,
            });
        let _task = self.shutdown.spawn(run(self.clone(), acquisition));
    }

    fn active(&self, acquisition: &ShardAcquisition) -> bool {
        self.homes
            .owner
            .read()
            .expect("shard owner lock poisoned")
            .acquisition_active(acquisition)
    }

    async fn recover(
        &self,
        offer: WorkflowOffer,
        acquisition: &ShardAcquisition,
        state: &WorkflowState,
    ) -> Result<bool, WorkflowOfferError> {
        if !self.active(acquisition) {
            return Ok(false);
        }
        let sender = self
            .sender
            .read()
            .expect("offer sender lock poisoned")
            .clone();
        let destination = self.destination(&offer.hint.task.queue);
        if let Some(sender) = sender {
            return Ok(sender.try_enqueue(
                offer,
                acquisition.clone(),
                PublicationSource::Recovery,
                destination,
                Instant::now(),
                false,
                self.clock.now(),
            ) != sender::EnqueueOutcome::Refused);
        }
        // Exclusive modes have no remote scheduler. Reuse receiver policy and
        // normal fallback under the same total budget, outside every run lane.
        let result = tokio::time::timeout(sender::TOTAL, async {
            let mut result = tokio::time::timeout(
                sender::ATTEMPT,
                self.receive(&offer, destination, OfferSource::Local),
            )
            .await
            .unwrap_or(Err(WorkflowOfferError::Deadline));
            sender::record_result(PublicationSource::Recovery, result);
            if let Ok(WorkflowOfferOutcome::StickyUnavailable(reason)) = result {
                sender::record_sticky(reason);
                // This pass already validated the run. Local fallback needs a
                // current routing lookup, not another hydration of that same run.
                let origin = offer
                    .hint
                    .sticky_origin
                    .as_ref()
                    .expect("sticky fallback has origin");
                let registry = self
                    .registry
                    .read()
                    .expect("deployment registry lock poisoned")
                    .clone();
                let queue = tokio::time::timeout(
                    sender::ATTEMPT,
                    fallback_target(registry.as_ref(), state, origin.normal_queue.clone()),
                )
                .await
                .map_err(|_| WorkflowOfferError::Deadline)?
                .map_err(|_| WorkflowOfferError::Routing)?;
                if !self.active(acquisition) {
                    return Ok(false);
                }
                let mut fallback = offer;
                fallback.hint.task.queue = queue;
                fallback.hint.task.sticky_preferred = None;
                fallback.hint.task.normal_queue = None;
                // The origin and absolute deadline survive the volatile retarget.
                result = tokio::time::timeout(
                    sender::ATTEMPT,
                    self.receive(
                        &fallback,
                        self.destination(&fallback.hint.task.queue),
                        OfferSource::Local,
                    ),
                )
                .await
                .unwrap_or(Err(WorkflowOfferError::Deadline));
                sender::record_result(PublicationSource::Recovery, result);
            }
            result.map(|reply| {
                matches!(
                    reply,
                    WorkflowOfferOutcome::Admitted | WorkflowOfferOutcome::Known
                )
            })
        })
        .await;
        result.unwrap_or(Err(WorkflowOfferError::Deadline))
    }
}

fn skipped(reason: &'static str) {
    metrics::counter!(crate::metrics::WORKFLOW_OFFER_SWEEP_SKIPPED_TOTAL, "reason" => reason)
        .increment(1);
}

async fn examine<R: RunRepository + 'static>(
    delivery: &WorkflowDelivery<R>,
    acquisition: &ShardAcquisition,
    candidate: &WorkflowDispatchRow,
) -> Result<()> {
    metrics::counter!(crate::metrics::WORKFLOW_OFFER_SWEEP_EXAMINED_TOTAL).increment(1);
    if !delivery.active(acquisition) {
        skipped("ownership");
        anyhow::bail!("sticky recovery acquisition ended");
    }
    let now = delivery.clock.now();
    if candidate
        .schedule_to_start_deadline
        .is_some_and(|at| at <= now)
    {
        skipped("expired");
        return Ok(());
    }
    // Let publication use its entire retry budget before duplicating its read
    // and delivery work. Wrapping will revisit this row without a durable bit.
    if now - candidate.scheduled_at
        < time::Duration::try_from(sender::TOTAL).expect("bounded sender budget")
    {
        skipped("recent");
        return Ok(());
    }
    let LoadedRun::Existing(state) = tokio::time::timeout(
        PERIOD,
        tokeira_storage::read_probe::scope(
            "sticky-recovery",
            delivery.repo.load_run(candidate.incarnation.run_key),
        ),
    )
    .await??
    else {
        skipped("missing");
        return Ok(());
    };
    // Compare all durable intent, not just the task number: affinity or
    // routing can change while retaining the same logical incarnation.
    if derive_workflow_dispatch(&state, acquisition.shard_id).as_ref() != Some(candidate) {
        skipped("stale");
        return Ok(());
    }
    if !delivery.active(acquisition) {
        skipped("ownership");
        anyhow::bail!("sticky recovery acquisition ended");
    }
    let Some(offer) = WorkflowOffer::from_state(&state, acquisition.shard_id)? else {
        skipped("stale");
        return Ok(());
    };
    if offer
        .hint
        .task
        .sticky_deadline
        .is_some_and(|at| at <= delivery.clock.now())
    {
        skipped("expired");
        return Ok(());
    }
    match delivery.recover(offer, acquisition, &state).await {
        Ok(true) => {
            metrics::counter!(crate::metrics::WORKFLOW_OFFER_SWEEP_ENQUEUED_TOTAL).increment(1)
        }
        Ok(false) => skipped("refused"),
        Err(error) => skipped(error.label()),
    }
    Ok(())
}

async fn page<R: RunRepository + 'static>(
    delivery: &WorkflowDelivery<R>,
    acquisition: &ShardAcquisition,
    cursor: &mut Option<RunKey>,
) -> Result<()> {
    if !delivery.active(acquisition) {
        anyhow::bail!("sticky recovery acquisition ended");
    }
    let page = tokio::time::timeout(
        PERIOD,
        delivery.repo.list_sticky_workflow_dispatch(
            acquisition.shard_id,
            *cursor,
            NonZeroU32::new(PAGE_SIZE).expect("positive sticky page size"),
        ),
    )
    .await??;
    for candidate in page.candidates {
        examine(delivery, acquisition, &candidate).await?;
        // The repository's continuation is exclusive. Leave it before a failed
        // row so retry includes that row, while retaining all completed progress.
        *cursor = Some(candidate.incarnation.run_key);
    }
    *cursor = if page.exhausted {
        None
    } else {
        page.last_examined
    };
    Ok(())
}

async fn run<R: RunRepository + 'static>(
    delivery: Arc<WorkflowDelivery<R>>,
    acquisition: ShardAcquisition,
) {
    let shutdown = delivery.shutdown.cancellation_token();
    let home = QueueHome {
        id: acquisition.shard_id,
        generation: acquisition.generation,
        cancel: acquisition.cancel.clone(),
        deadline: acquisition.deadline.clone(),
    };
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {},
        _ = home.retired() => {},
        _ = async {
            let mut cursor = None;
            let mut failures = 0u32;
            loop {
                // Replacement restarts traversal, but does not reset the home's
                // query rate. Keep pacing even across rapid reacquisitions.
                let last_query = delivery.sweeps.started.lock().expect("sticky sweep lock poisoned")
                    .get(&acquisition.shard_id).and_then(|home| home.last_query);
                if let Some(at) = last_query { tokio::time::sleep_until(at + PERIOD).await; }
                let permit = delivery.sweeps.capacity.acquire().await.expect("sticky capacity stays open");
                if let Some(home) = delivery.sweeps.started.lock().expect("sticky sweep lock poisoned").get_mut(&acquisition.shard_id) {
                    if home.generation != acquisition.generation { return; }
                    home.last_query = Some(Instant::now());
                }
                // Individual reads are bounded, but a whole page may take longer
                // than one second. Timing out the whole page could starve its tail.
                let result = page(&delivery, &acquisition, &mut cursor).await;
                drop(permit);
                match result {
                    Ok(()) => { failures = 0; },
                    _ => { failures = (failures + 1).min(5); skipped("incomplete"); },
                }
                // Completion-based pacing is conservative: never two page
                // queries within a second, even after a slow or refused page.
                tokio::time::sleep(PERIOD * (1 << failures)).await;
            }
        } => {},
    }
}
