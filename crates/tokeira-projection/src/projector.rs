//! The projector: passes over one partition of the projection log.
//!
//! Each pass reads the partition from its head, a page at a time in
//! `(run_key, transition_seq)` order, applies each run's newest image in the page
//! through the strictly-newer guard, and then deletes every image of the page whose
//! run's apply succeeded (`projection-passes` criteria 2.1-2.3). The runs of different
//! rollup stripes are applied concurrently, one run of a stripe at a time (criterion
//! 2.10). The position lives
//! inside one pass: run keys are hashes, so commits land anywhere in key order, and an
//! image committed behind the position is at or after the head of the next pass.
//! Nothing durable records progress, so there is no checkpoint to conflict on.
//!
//! A failure ends neither the pass loop nor the projector (criterion 2.6). A failed
//! apply keeps that run's images for a later pass and lets the rest of the page go
//! on; a failed read or delete ends the pass early. The next pass starts after a
//! backoff that doubles from 100 ms to 1 s. A pass that neither applies nor deletes
//! anything waits 1 s before the next (criterion 2.7).
//!
//! [`ProjectorSupervisor`] gives each partition one projector in a deployment: the
//! node that holds the partition's home bundle runs it, while that home's
//! acquisition lasts (criterion 2.5).

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::Poll,
    time::{Duration, Instant},
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_observability::{
    ErrorBiasedSamplingReason, ProjectionFailureKindLabel, ProjectionOutcomeLabel,
    mark_error_biased_sample,
};
use tokeira_storage::{ProjectionImageKey, ProjectionLog, ProjectionRecord};
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    metrics as projection_metrics,
    rollup::stripe_for,
    sink::{ApplyOutcome, ProjectionSink},
};

/// Images read in one page of a pass.
pub const PAGE_SIZE: usize = 256;

/// The wait after a pass that applied and deleted nothing. An image committed during
/// the wait is applied within about a second, inside the 4 s that v1.31.0's
/// functional tests allow visibility to settle (`WaitForESToSettle`,
/// `tests/testcore/constants.go:11 @ v1.31.0`), and an idle partition costs one read
/// a second.
pub const IDLE_PAUSE: Duration = Duration::from_secs(1);

/// The first wait after a pass with a failure. Each further failing pass doubles it,
/// up to [`MAX_BACKOFF`]; a pass without a failure resets it.
pub const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// The longest wait after a failing pass.
pub const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// How often the supervisor looks at each partition's home.
pub const HOME_CHECK_PERIOD: Duration = Duration::from_secs(1);

/// Runs passes over one partition of the projection log.
///
/// Two projectors of one partition stay correct, as during a move of its home:
/// each apply reads the stored row inside its own transaction, so of two applies of
/// one run the later fails with a write conflict and retries from a fresh read, and a
/// delete names keys, so deleting an image twice deletes nothing the second time.
/// Ownership saves work; it is not a fence.
#[derive(Debug)]
pub struct Projector<L, S> {
    pub log: L,
    pub sink: S,
    pub partition_id: u32,
    pub fanout: u16,
    pub page_size: usize,
}

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Images read from the partition.
    pub read: usize,
    /// Runs whose newest image became their stored row.
    pub applied: usize,
    /// Runs whose stored row was already at least as new as their newest image.
    pub current: usize,
    /// Images the log deleted.
    pub deleted: u64,
    /// Reads that failed; each ends its pass.
    pub failed_reads: usize,
    /// Runs whose newest image failed to apply; their images wait for a later pass.
    pub failed_applies: usize,
    /// Deletes that failed; each ends its pass.
    pub failed_deletes: usize,
    /// When the oldest image the pass read was written, where the log records it.
    pub oldest_written_at: Option<OffsetDateTime>,
}

impl PassReport {
    /// Whether any read, apply or delete of the pass failed.
    pub fn failed(&self) -> bool {
        self.failed_reads + self.failed_applies + self.failed_deletes > 0
    }

    /// Whether the pass changed nothing: it applied no image and deleted none.
    ///
    /// A run found current doesn't count. The in-memory store keeps the latest image
    /// of an unseeded run whatever it is asked to delete, so every pass finds that
    /// image current again; counting it would run passes back to back for as long as
    /// the run stays unseeded.
    pub fn idle(&self) -> bool {
        self.applied == 0 && self.deleted == 0
    }
}

/// The wait before the next pass, given the last pass's report and the current
/// backoff, and the backoff to use after that.
pub fn next_pause(report: &PassReport, backoff: Duration) -> (Duration, Duration) {
    if report.failed() {
        (backoff, backoff.saturating_mul(2).min(MAX_BACKOFF))
    } else if report.idle() {
        (IDLE_PAUSE, FIRST_BACKOFF)
    } else {
        (Duration::ZERO, FIRST_BACKOFF)
    }
}

/// A run's newest image by `(authority_epoch, transition_seq)`.
fn newest_image(run: &[ProjectionRecord]) -> &ProjectionRecord {
    run.iter()
        .max_by_key(|image| (image.context.authority_epoch, image.transition_seq))
        .expect("chunk_by yields no empty run")
}

/// Drive `futures` to completion together on the calling task, and answer their
/// outputs in order. A pass has at most one future a rollup stripe, so polling each
/// unfinished one on every wake costs little.
async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *output = Some(value),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    outputs
        .into_iter()
        .map(|output| output.expect("every future finished"))
        .collect()
}

impl<L, S> Projector<L, S>
where
    L: ProjectionLog,
    S: ProjectionSink,
{
    /// Run passes until `cancel` fires. Nothing ends the loop but cancellation.
    pub async fn run(&self, cancel: CancellationToken) {
        let mut backoff = FIRST_BACKOFF;
        while !cancel.is_cancelled() {
            let report = self.pass(&cancel).await;
            let (pause, next_backoff) = next_pause(&report, backoff);
            backoff = next_backoff;
            if pause.is_zero() {
                // A busy partition runs passes back to back. Yield so that a store
                // whose reads never wait, like the in-memory one, can't starve the
                // tasks that share this thread.
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(pause) => {}
            }
        }
        debug!(
            partition = self.partition_id,
            fanout = self.fanout,
            "projector stopped"
        );
    }

    /// Run one pass over the partition, from its head to its end, and report it.
    pub async fn pass(&self, cancel: &CancellationToken) -> PassReport {
        let started = Instant::now();
        let mut report = PassReport::default();
        let mut after = None;
        while !cancel.is_cancelled() {
            let page = match self
                .log
                .read_page(self.partition_id, self.fanout, after, self.page_size)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    report.failed_reads += 1;
                    warn!(
                        ?error,
                        partition = self.partition_id,
                        "projection log read failed"
                    );
                    break;
                }
            };
            report.read += page.images.len();
            report.oldest_written_at = match (report.oldest_written_at, page.oldest_written_at) {
                (Some(oldest), Some(page_oldest)) => Some(oldest.min(page_oldest)),
                (oldest, page_oldest) => oldest.or(page_oldest),
            };
            let Some(last) = page.images.last() else {
                break;
            };
            after = Some(last.key());
            let finished = self.apply_page(&page.images, &mut report, cancel).await;
            if !finished.is_empty() {
                match self
                    .log
                    .delete_images(self.partition_id, self.fanout, &finished)
                    .await
                {
                    Ok(deleted) => report.deleted += deleted,
                    Err(error) => {
                        report.failed_deletes += 1;
                        warn!(
                            ?error,
                            partition = self.partition_id,
                            "projection log delete failed"
                        );
                        break;
                    }
                }
            }
            if page.images.len() < self.page_size {
                break;
            }
        }
        self.record(&report, started.elapsed());
        report
    }

    /// Apply each run's newest image in `images`, and return the keys of every image
    /// of each run whose apply succeeded.
    ///
    /// Within a run, the newest image by `(authority_epoch, transition_seq)` is at
    /// least as new as every other image of the run in the page. After it applies,
    /// or is found current, the stored row is at least as new as all of them, so all
    /// of them can go. Superseded images are never applied, and they go with it:
    /// deleting only applied images would keep every dominated image for ever.
    ///
    /// The runs of different rollup stripes are applied concurrently, at most one run
    /// of a stripe at a time (criterion 2.10). Their rows and counter rows are
    /// disjoint, so their applies never conflict, while two runs of one stripe would
    /// contend on its counter rows. Each stripe's runs go in page order, on this task,
    /// and the store bounds how many applies hold a connection at once: on DSQL, the
    /// projection class's.
    async fn apply_page(
        &self,
        images: &[ProjectionRecord],
        report: &mut PassReport,
        cancel: &CancellationToken,
    ) -> Vec<ProjectionImageKey> {
        // The log returns a run's images together, in key order.
        let runs: Vec<&[ProjectionRecord]> = images
            .chunk_by(|left, right| left.run_key == right.run_key)
            .collect();
        // One lane per stripe in the page, in the order of its first run.
        let mut lanes: Vec<Vec<usize>> = Vec::new();
        let mut lane_of_stripe = HashMap::new();
        for (index, run) in runs.iter().enumerate() {
            let lane = *lane_of_stripe
                .entry(stripe_for(run[0].run_key))
                .or_insert_with(|| {
                    lanes.push(Vec::new());
                    lanes.len() - 1
                });
            lanes[lane].push(index);
        }
        let mut outcomes: Vec<(usize, Result<ApplyOutcome>)> = join_all(
            lanes
                .into_iter()
                .map(|lane| self.apply_lane(&runs, lane, cancel))
                .collect(),
        )
        .await
        .into_iter()
        .flatten()
        .collect();
        outcomes.sort_unstable_by_key(|(index, _)| *index);

        let mut finished = Vec::with_capacity(images.len());
        for (index, outcome) in outcomes {
            let run = runs[index];
            match outcome {
                Ok(outcome) => {
                    match outcome {
                        ApplyOutcome::Advanced => report.applied += 1,
                        ApplyOutcome::Current => report.current += 1,
                    }
                    finished.extend(run.iter().map(ProjectionRecord::key));
                }
                Err(error) => {
                    report.failed_applies += 1;
                    mark_error_biased_sample(ErrorBiasedSamplingReason::ProjectionSinkFailure);
                    let newest = newest_image(run);
                    warn!(
                        ?error,
                        partition = self.partition_id,
                        run_key = %newest.run_key.0,
                        transition_seq = newest.transition_seq.0,
                        "projection apply failed"
                    );
                }
            }
        }
        finished
    }

    /// Apply the newest image of each of `lane`'s runs, one run at a time, and answer
    /// each run's index in `runs` with its outcome.
    async fn apply_lane(
        &self,
        runs: &[&[ProjectionRecord]],
        lane: Vec<usize>,
        cancel: &CancellationToken,
    ) -> Vec<(usize, Result<ApplyOutcome>)> {
        let mut outcomes = Vec::with_capacity(lane.len());
        for index in lane {
            if cancel.is_cancelled() {
                break;
            }
            let outcome = self
                .sink
                .apply(newest_image(runs[index]), self.partition_id)
                .await;
            outcomes.push((index, outcome));
        }
        outcomes
    }

    fn record(&self, report: &PassReport, duration: Duration) {
        let partition = self.partition_id;
        projection_metrics::record_images_read(partition, report.read);
        projection_metrics::record_records_processed_with_outcome(
            partition,
            report.applied,
            ProjectionOutcomeLabel::Success,
        );
        projection_metrics::record_records_processed_with_outcome(
            partition,
            report.current,
            ProjectionOutcomeLabel::Skipped,
        );
        projection_metrics::record_records_processed_with_outcome(
            partition,
            report.failed_applies,
            ProjectionOutcomeLabel::Failure,
        );
        projection_metrics::record_images_deleted(partition, report.deleted);
        for (kind, count) in [
            (ProjectionFailureKindLabel::Read, report.failed_reads),
            (ProjectionFailureKindLabel::Apply, report.failed_applies),
            (ProjectionFailureKindLabel::Delete, report.failed_deletes),
        ] {
            if count > 0 {
                projection_metrics::record_pass_failures(partition, kind, count);
            }
        }
        projection_metrics::record_pass_duration(partition, duration);
        match report.oldest_written_at {
            Some(written_at) => {
                let age = OffsetDateTime::now_utc() - written_at;
                projection_metrics::set_oldest_image_age(
                    partition,
                    age.try_into().unwrap_or(Duration::ZERO),
                );
            }
            // A complete pass that read nothing leaves nothing waiting.
            None if report.read == 0 && !report.failed() => {
                projection_metrics::set_oldest_image_age(partition, Duration::ZERO);
            }
            None => {}
        }
        debug!(
            partition,
            read = report.read,
            applied = report.applied,
            current = report.current,
            deleted = report.deleted,
            failed = report.failed(),
            "projection pass"
        );
    }
}

/// A partition's home, as this node holds it.
#[derive(Clone, Debug)]
pub struct ProjectorHome {
    /// The acquisition's generation. A home lost and regained between two looks has
    /// a new one.
    pub generation: u64,
    /// Cancelled when the acquisition ends.
    pub cancel: CancellationToken,
}

/// Runs the projector of each partition whose home bundle this node holds
/// (`projection-passes` criterion 2.5).
///
/// It looks up every partition's home once a [`HOME_CHECK_PERIOD`] through `homes`.
/// When the node holds a home it doesn't yet run a projector for, it starts the
/// partition's projector under a child of the acquisition's token, so the projector
/// stops when the acquisition ends. A home regained under a new acquisition gets a
/// new projector. A node that assigns itself every home runs every partition's
/// projector. Another node may still be running a partition's projector while its
/// home moves; [`Projector`] says why that stays correct.
#[derive(Debug)]
pub struct ProjectorSupervisor<L, F, H> {
    pub log: L,
    /// Builds each projector's sink.
    pub sink: F,
    /// The partitions to look after, `0..partitions`.
    pub partitions: u32,
    pub fanout: u16,
    pub page_size: usize,
    /// This node's active acquisition of a partition's home, if it holds one.
    pub homes: H,
}

/// A projector the supervisor started.
struct RunningProjector {
    generation: u64,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl<L, F, S, H> ProjectorSupervisor<L, F, H>
where
    L: ProjectionLog + Clone + 'static,
    F: Fn() -> S + Send + Sync,
    S: ProjectionSink + 'static,
    H: Fn(u32) -> Option<ProjectorHome> + Send + Sync,
{
    /// Look after the partitions until `cancel` fires, then stop every projector
    /// started here and wait for them.
    pub async fn run(self, cancel: CancellationToken) {
        let mut running = HashMap::new();
        let mut checks = tokio::time::interval(HOME_CHECK_PERIOD);
        checks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = checks.tick() => {}
            }
            for partition in 0..self.partitions {
                self.check(partition, &mut running);
            }
        }
        for (_, projector) in running.drain() {
            projector.cancel.cancel();
            let _ = projector.task.await;
        }
    }

    fn check(&self, partition: u32, running: &mut HashMap<u32, RunningProjector>) {
        let home = (self.homes)(partition);
        if let Some(current) = running.get(&partition) {
            let held = home
                .as_ref()
                .is_some_and(|home| home.generation == current.generation);
            // A projector ends only when its token is cancelled, so a finished one
            // under a held home panicked; start it again.
            if held && !current.task.is_finished() {
                return;
            }
            // The acquisition it ran under is over, even if the node holds the home
            // again under a new one.
            current.cancel.cancel();
            running.remove(&partition);
        }
        let Some(home) = home else {
            return;
        };
        let cancel = home.cancel.child_token();
        let projector = Projector {
            log: self.log.clone(),
            sink: (self.sink)(),
            partition_id: partition,
            fanout: self.fanout,
            page_size: self.page_size,
        };
        let task = tokio::spawn({
            let cancel = cancel.clone();
            async move { projector.run(cancel).await }
        });
        running.insert(
            partition,
            RunningProjector {
                generation: home.generation,
                cancel,
                task,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    };

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokeira_storage::{ProjectionContext, ProjectionPage};
    use tokeira_types::{
        ArchetypeId, ExecutionStatus, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
        TaskQueueName, TransitionSeq, VisibilityLifecycleState, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        memory::InMemoryVisibilityStore, store::VisibilityStore, visibility_sink::VisibilitySink,
    };

    /// A projection log in DSQL's key order, with failures to inject.
    #[derive(Clone, Default)]
    struct TestLog {
        images: Arc<Mutex<BTreeMap<(u32, ProjectionImageKey), ProjectionRecord>>>,
        failing_reads: Arc<Mutex<usize>>,
        failing_deletes: Arc<Mutex<usize>>,
        deletes: Arc<Mutex<Vec<Vec<ProjectionImageKey>>>>,
        /// Each run's stored version, as the test sink last saw it; a delete of an
        /// image newer than its run's stored row is recorded in `unsafe_deletes`.
        stored: Arc<Mutex<BTreeMap<RunKey, TransitionSeq>>>,
        unsafe_deletes: Arc<Mutex<Vec<ProjectionImageKey>>>,
        /// Images committed right after the next read, behind its position.
        late: Arc<Mutex<Vec<ProjectionRecord>>>,
    }

    impl TestLog {
        fn commit(&self, image: ProjectionRecord) {
            self.images
                .lock()
                .unwrap()
                .insert((image.partition_id, image.key()), image);
        }

        fn keys(&self) -> Vec<ProjectionImageKey> {
            self.images
                .lock()
                .unwrap()
                .keys()
                .map(|(_, key)| *key)
                .collect()
        }
    }

    #[async_trait]
    impl ProjectionLog for TestLog {
        async fn read_page(
            &self,
            partition_id: u32,
            _fanout: u16,
            after: Option<ProjectionImageKey>,
            limit: usize,
        ) -> Result<ProjectionPage> {
            {
                let mut failing = self.failing_reads.lock().unwrap();
                if *failing > 0 {
                    *failing -= 1;
                    return Err(anyhow!("injected read failure"));
                }
            }
            let page = {
                let images = self.images.lock().unwrap();
                ProjectionPage {
                    images: images
                        .values()
                        .filter(|image| image.partition_id == partition_id)
                        .filter(|image| after.is_none_or(|after| image.key() > after))
                        .take(limit)
                        .cloned()
                        .collect(),
                    oldest_written_at: None,
                }
            };
            let late = std::mem::take(&mut *self.late.lock().unwrap());
            for image in late {
                self.commit(image);
            }
            Ok(page)
        }

        async fn delete_images(
            &self,
            partition_id: u32,
            _fanout: u16,
            keys: &[ProjectionImageKey],
        ) -> Result<u64> {
            {
                let mut failing = self.failing_deletes.lock().unwrap();
                if *failing > 0 {
                    *failing -= 1;
                    return Err(anyhow!("injected delete failure"));
                }
            }
            self.deletes.lock().unwrap().push(keys.to_vec());
            let stored = self.stored.lock().unwrap();
            self.unsafe_deletes
                .lock()
                .unwrap()
                .extend(keys.iter().filter(|key| {
                    stored
                        .get(&key.run_key)
                        .is_none_or(|stored| *stored < key.transition_seq)
                }));
            let mut images = self.images.lock().unwrap();
            Ok(keys
                .iter()
                .filter(|key| images.remove(&(partition_id, **key)).is_some())
                .count() as u64)
        }
    }

    /// A sink over the in-memory store whose applies of some runs always fail.
    #[derive(Clone)]
    struct TestSink {
        sink: Arc<VisibilitySink<InMemoryVisibilityStore>>,
        store: InMemoryVisibilityStore,
        poisoned: Arc<Mutex<BTreeSet<RunKey>>>,
        applied: Arc<Mutex<Vec<ProjectionImageKey>>>,
        stored: Arc<Mutex<BTreeMap<RunKey, TransitionSeq>>>,
    }

    impl TestSink {
        fn new() -> Self {
            let store = InMemoryVisibilityStore::default();
            Self {
                sink: Arc::new(VisibilitySink::new(store.clone())),
                store,
                poisoned: Default::default(),
                applied: Default::default(),
                stored: Default::default(),
            }
        }

        /// A sink and a log that checks each delete against the sink's rows.
        fn with_log() -> (Self, TestLog) {
            let sink = Self::new();
            let log = TestLog {
                stored: sink.stored.clone(),
                ..TestLog::default()
            };
            (sink, log)
        }

        async fn stored_seq(&self, run_key: RunKey) -> Option<TransitionSeq> {
            self.store
                .get_row(run_key)
                .await
                .map(|row| row.source_transition_seq)
        }
    }

    #[async_trait]
    impl ProjectionSink for TestSink {
        async fn apply(
            &self,
            record: &ProjectionRecord,
            partition_id: u32,
        ) -> Result<ApplyOutcome> {
            if self.poisoned.lock().unwrap().contains(&record.run_key) {
                return Err(anyhow!("this run's images never apply"));
            }
            self.applied.lock().unwrap().push(record.key());
            let outcome = self.sink.apply(record, partition_id).await?;
            if let Some(stored) = self.stored_seq(record.run_key).await {
                self.stored.lock().unwrap().insert(record.run_key, stored);
            }
            Ok(outcome)
        }
    }

    /// The most applies a sink had in flight at once: in all, of one rollup stripe,
    /// and of one run.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct Overlap {
        all: usize,
        stripe: usize,
        run: usize,
    }

    /// A test sink whose every apply yields once before it reaches the store, so the
    /// applies a projector runs together are all in flight before any finishes. It
    /// records the most it saw in flight at once.
    #[derive(Clone)]
    struct OverlapSink {
        inner: TestSink,
        in_flight: Arc<Mutex<Vec<RunKey>>>,
        most: Arc<Mutex<Overlap>>,
    }

    impl OverlapSink {
        fn new(inner: TestSink) -> Self {
            Self {
                inner,
                in_flight: Default::default(),
                most: Default::default(),
            }
        }

        fn most(&self) -> Overlap {
            *self.most.lock().unwrap()
        }
    }

    #[async_trait]
    impl ProjectionSink for OverlapSink {
        async fn apply(
            &self,
            record: &ProjectionRecord,
            partition_id: u32,
        ) -> Result<ApplyOutcome> {
            {
                let mut in_flight = self.in_flight.lock().unwrap();
                in_flight.push(record.run_key);
                let stripe = stripe_for(record.run_key);
                let mut most = self.most.lock().unwrap();
                most.all = most.all.max(in_flight.len());
                most.stripe = most.stripe.max(
                    in_flight
                        .iter()
                        .filter(|run_key| stripe_for(**run_key) == stripe)
                        .count(),
                );
                most.run = most.run.max(
                    in_flight
                        .iter()
                        .filter(|run_key| **run_key == record.run_key)
                        .count(),
                );
            }
            tokio::task::yield_now().await;
            let outcome = self.inner.apply(record, partition_id).await;
            let mut in_flight = self.in_flight.lock().unwrap();
            let position = in_flight
                .iter()
                .position(|run_key| *run_key == record.run_key)
                .expect("this apply is in flight");
            in_flight.swap_remove(position);
            outcome
        }
    }

    fn overlap_projector(log: &TestLog, sink: &OverlapSink) -> Projector<TestLog, OverlapSink> {
        Projector {
            log: log.clone(),
            sink: sink.clone(),
            partition_id: 0,
            fanout: 1,
            page_size: PAGE_SIZE,
        }
    }

    async fn status_counts(store: &InMemoryVisibilityStore) -> BTreeMap<String, i64> {
        store
            .count_from_rollup(
                NamespaceId(Uuid::from_u128(1)),
                ArchetypeId::WORKFLOW,
                crate::RollupDimension::ExecutionStatus,
            )
            .await
            .unwrap()
            .groups
            .into_iter()
            .map(|group| (group.value, group.count))
            .filter(|(_, count)| *count != 0)
            .collect()
    }

    fn image(run: u128, transition_seq: u64) -> ProjectionRecord {
        let run_key = RunKey(Uuid::from_u128(run));
        let workflow_id = WorkflowId(format!("workflow-{run}"));
        ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(transition_seq),
            context: ProjectionContext {
                archetype_id: ArchetypeId::WORKFLOW,
                namespace_id: NamespaceId(Uuid::from_u128(1)),
                business_id: workflow_id.0.clone(),
                authority_epoch: 0,
                status_keyword: "Running".to_string(),
                lifecycle_state: VisibilityLifecycleState::Open,
                workflow_id,
                run_id: RunId(Uuid::from_u128(run)),
                workflow_type: WorkflowType("Workflow".to_string()),
                task_queue: TaskQueueName("queue".to_string()),
                execution_status: ExecutionStatus::Running,
                start_time: OffsetDateTime::from_unix_timestamp(100).unwrap(),
                update_time: OffsetDateTime::from_unix_timestamp(100).unwrap(),
                execution_time: None,
                close_time: None,
                history_length: transition_seq as i64,
                execution_duration: None,
                state_transition_count: transition_seq as i64,
                transition_count: transition_seq as i64,
                history_size_bytes: 0,
                parent_workflow_id: None,
                parent_run_id: None,
                root_workflow_id: None,
                root_run_id: None,
                search_attr_generation: transition_seq,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
            },
        }
    }

    fn key(run: u128, transition_seq: u64) -> ProjectionImageKey {
        image(run, transition_seq).key()
    }

    fn image_with_status(
        run: u128,
        transition_seq: u64,
        status: ExecutionStatus,
    ) -> ProjectionRecord {
        let mut image = image(run, transition_seq);
        image.context.execution_status = status;
        image.context.status_keyword = crate::workflow_status_keyword(status);
        if status != ExecutionStatus::Running {
            image.context.lifecycle_state = VisibilityLifecycleState::Closed;
            image.context.close_time = Some(OffsetDateTime::from_unix_timestamp(200).unwrap());
        }
        image
    }

    fn image_in(partition_id: u32, run: u128) -> ProjectionRecord {
        ProjectionRecord {
            partition_id,
            ..image(run, 1)
        }
    }

    async fn eventually(mut done: impl AsyncFnMut() -> bool) -> bool {
        for _ in 0..100 {
            if done().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    fn projector(log: &TestLog, sink: &TestSink, page_size: usize) -> Projector<TestLog, TestSink> {
        Projector {
            log: log.clone(),
            sink: sink.clone(),
            partition_id: 0,
            fanout: 1,
            page_size,
        }
    }

    // `projection-passes` criterion 2.2: a page applies each run's newest image only.
    #[tokio::test]
    async fn a_pass_applies_each_runs_newest_image() {
        let log = TestLog::default();
        let sink = TestSink::new();
        for (run, transition_seq) in [(1, 1), (1, 3), (1, 2), (2, 1)] {
            log.commit(image(run, transition_seq));
        }

        let report = projector(&log, &sink, PAGE_SIZE)
            .pass(&CancellationToken::new())
            .await;

        assert_eq!(*sink.applied.lock().unwrap(), vec![key(1, 3), key(2, 1)]);
        assert_eq!(
            report,
            PassReport {
                read: 4,
                applied: 2,
                deleted: 4,
                ..PassReport::default()
            }
        );
        assert!(log.keys().is_empty());
        assert_eq!(
            sink.stored_seq(RunKey(Uuid::from_u128(1))).await,
            Some(TransitionSeq(3))
        );
    }

    // `projection-passes` criterion 2.10: the runs of different rollup stripes are
    // applied together, and the runs of one stripe one at a time, in page order.
    #[tokio::test]
    async fn a_pass_applies_the_runs_of_different_stripes_together() {
        let (inner, log) = TestSink::with_log();
        let sink = OverlapSink::new(inner.clone());
        // Stripes 1 to 4, two runs each: run 17 shares run 1's stripe, and so on.
        let runs = [1u128, 2, 3, 4, 17, 18, 19, 20];
        for run in runs {
            log.commit(image(run, 1));
            log.commit(image(run, 2));
        }

        let report = overlap_projector(&log, &sink)
            .pass(&CancellationToken::new())
            .await;

        assert_eq!(
            sink.most(),
            Overlap {
                all: 4,
                stripe: 1,
                run: 1
            },
            "the four stripes together, one run of each at a time"
        );
        let applied = inner.applied.lock().unwrap().clone();
        assert_eq!(
            applied.iter().copied().collect::<BTreeSet<_>>(),
            runs.iter().map(|run| key(*run, 2)).collect::<BTreeSet<_>>(),
            "each run's newest image"
        );
        assert_eq!(applied.len(), runs.len(), "once each: {applied:?}");
        let position = |run| applied.iter().position(|applied| *applied == key(run, 2));
        for (first, second) in [(1, 17), (2, 18), (3, 19), (4, 20)] {
            assert!(
                position(first) < position(second),
                "a stripe's runs in page order: {applied:?}"
            );
        }
        assert_eq!(
            report,
            PassReport {
                read: 16,
                applied: 8,
                deleted: 16,
                ..PassReport::default()
            }
        );
        assert!(log.keys().is_empty());
        assert_eq!(
            status_counts(&inner.store).await,
            BTreeMap::from([(crate::workflow_status_keyword(ExecutionStatus::Running), 8)])
        );
    }

    // `projection-passes` criterion 2.3: images found current go too, as do the
    // superseded images of a run whose newest image was current.
    #[tokio::test]
    async fn a_pass_deletes_the_images_of_a_run_found_current() {
        let log = TestLog::default();
        let sink = TestSink::new();
        sink.sink.apply(&image(1, 5), 0).await.unwrap();
        log.commit(image(1, 4));
        log.commit(image(1, 5));

        let report = projector(&log, &sink, PAGE_SIZE)
            .pass(&CancellationToken::new())
            .await;

        assert_eq!((report.applied, report.current, report.deleted), (0, 1, 2));
        assert!(log.keys().is_empty());
    }

    // `projection-passes` criterion 2.3: a run whose apply fails keeps every image it
    // has in the page, and the rest of the page is applied and deleted.
    #[tokio::test]
    async fn a_failed_apply_keeps_its_runs_images_and_no_others() {
        let log = TestLog::default();
        let sink = TestSink::new();
        for (run, transition_seq) in [(1, 1), (2, 1), (2, 2), (3, 1)] {
            log.commit(image(run, transition_seq));
        }
        sink.poisoned
            .lock()
            .unwrap()
            .insert(RunKey(Uuid::from_u128(2)));

        let report = projector(&log, &sink, PAGE_SIZE)
            .pass(&CancellationToken::new())
            .await;

        assert_eq!(log.keys(), vec![key(2, 1), key(2, 2)]);
        assert_eq!(
            (report.applied, report.failed_applies, report.deleted),
            (2, 1, 2)
        );
        assert!(report.failed());
    }

    // `projection-passes` criterion 2.1, from the exploration of the cursor: an image
    // committed below a pass's position is applied by the next pass.
    #[tokio::test]
    async fn an_image_behind_the_position_is_applied_by_the_next_pass() {
        let log = TestLog::default();
        let sink = TestSink::new();
        let projector = projector(&log, &sink, PAGE_SIZE);
        log.commit(image(0x80, 1));
        projector.pass(&CancellationToken::new()).await;
        log.commit(image(0x10, 1));

        projector.pass(&CancellationToken::new()).await;

        assert_eq!(
            sink.stored_seq(RunKey(Uuid::from_u128(0x10))).await,
            Some(TransitionSeq(1))
        );
        assert!(log.keys().is_empty());
    }

    // `projection-passes` criterion 2.6, from the exploration of the cursor: an image
    // that never applies holds back no other image of its partition.
    #[tokio::test]
    async fn an_image_that_never_applies_holds_back_no_other() {
        let log = TestLog::default();
        let sink = TestSink::new();
        let projector = projector(&log, &sink, 2);
        sink.poisoned
            .lock()
            .unwrap()
            .insert(RunKey(Uuid::from_u128(1)));
        for run in 1..=5 {
            log.commit(image(run, 1));
        }

        let report = projector.pass(&CancellationToken::new()).await;

        assert_eq!(log.keys(), vec![key(1, 1)]);
        assert_eq!((report.read, report.applied), (5, 4));
        for run in 2..=5 {
            assert!(
                sink.stored_seq(RunKey(Uuid::from_u128(run)))
                    .await
                    .is_some()
            );
        }
    }

    // `projection-passes` criterion 2.1: a pass pages through the whole partition, from
    // the last key each page read.
    #[tokio::test]
    async fn a_pass_pages_to_the_end_of_its_partition() {
        let log = TestLog::default();
        let sink = TestSink::new();
        sink.poisoned
            .lock()
            .unwrap()
            .insert(RunKey(Uuid::from_u128(2)));
        for run in 1..=7 {
            log.commit(image(run, 1));
        }

        let report = projector(&log, &sink, 3)
            .pass(&CancellationToken::new())
            .await;

        assert_eq!((report.read, report.applied, report.deleted), (7, 6, 6));
        assert_eq!(log.deletes.lock().unwrap().len(), 3);
    }

    // `projection-passes` criterion 2.6: a failed read or delete ends the pass, and the
    // next pass starts again from the head.
    #[tokio::test]
    async fn a_failed_read_or_delete_ends_only_its_pass() {
        let log = TestLog::default();
        let sink = TestSink::new();
        let projector = projector(&log, &sink, PAGE_SIZE);
        log.commit(image(1, 1));
        *log.failing_reads.lock().unwrap() = 1;

        let failed_read = projector.pass(&CancellationToken::new()).await;
        *log.failing_deletes.lock().unwrap() = 1;
        let failed_delete = projector.pass(&CancellationToken::new()).await;
        let recovered = projector.pass(&CancellationToken::new()).await;

        assert_eq!((failed_read.failed_reads, failed_read.read), (1, 0));
        assert_eq!(
            (
                failed_delete.failed_deletes,
                failed_delete.applied,
                failed_delete.deleted
            ),
            (1, 1, 0)
        );
        assert_eq!((recovered.current, recovered.deleted), (1, 1));
        assert!(log.keys().is_empty());
    }

    // `projection-passes` criteria 2.6 and 2.7.
    #[test]
    fn pauses_follow_failures_and_idleness() {
        let busy = PassReport {
            applied: 1,
            ..PassReport::default()
        };
        let deleting = PassReport {
            current: 1,
            deleted: 1,
            ..PassReport::default()
        };
        let idle = PassReport {
            read: 3,
            current: 1,
            ..PassReport::default()
        };
        let failed = PassReport {
            applied: 4,
            failed_applies: 1,
            ..PassReport::default()
        };

        assert_eq!(
            next_pause(&busy, MAX_BACKOFF),
            (Duration::ZERO, FIRST_BACKOFF)
        );
        assert_eq!(
            next_pause(&deleting, MAX_BACKOFF),
            (Duration::ZERO, FIRST_BACKOFF)
        );
        assert_eq!(next_pause(&idle, MAX_BACKOFF), (IDLE_PAUSE, FIRST_BACKOFF));
        let mut backoff = FIRST_BACKOFF;
        let mut pauses = Vec::new();
        for _ in 0..6 {
            let (pause, next) = next_pause(&failed, backoff);
            pauses.push(pause.as_millis());
            backoff = next;
        }
        assert_eq!(pauses, [100, 200, 400, 800, 1000, 1000]);
    }

    // `projection-passes` criteria 2.6 and 2.7, from the exploration of the cursor: a
    // failed read ends neither the projector nor its passes; failing passes back off
    // 100, 200 and 400 ms, and an idle pass waits a second.
    #[tokio::test]
    async fn the_projector_outlives_failures_and_paces_its_passes() {
        let log = TestLog::default();
        let sink = TestSink::new();
        log.commit(image(1, 1));
        *log.failing_reads.lock().unwrap() = 3;
        let reads = Arc::new(Mutex::new(Vec::new()));
        let timed = TimedLog {
            log: log.clone(),
            reads: reads.clone(),
        };
        let projector = Arc::new(Projector {
            log: timed,
            sink: sink.clone(),
            partition_id: 0,
            fanout: 1,
            page_size: PAGE_SIZE,
        });
        let cancel = CancellationToken::new();
        let running = {
            let projector = projector.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { projector.run(cancel).await })
        };

        // Three failing passes, the one that applies, the idle pass after it, and the
        // next idle pass.
        assert!(eventually(async || reads.lock().unwrap().len() >= 6).await);
        cancel.cancel();
        running.await.unwrap();
        let reads = reads.lock().unwrap().clone();
        let gaps = reads
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect::<Vec<_>>();
        for (gap, at_least) in gaps.iter().zip([100, 200, 400]) {
            assert!(
                *gap >= Duration::from_millis(at_least),
                "backoff gaps {gaps:?}"
            );
        }
        assert!(gaps[4] >= IDLE_PAUSE, "an idle pass waits: {gaps:?}");
        assert_eq!(
            sink.stored_seq(RunKey(Uuid::from_u128(1))).await,
            Some(TransitionSeq(1))
        );
        assert!(log.keys().is_empty());
    }

    /// The test log, timing each read.
    struct TimedLog {
        log: TestLog,
        reads: Arc<Mutex<Vec<Instant>>>,
    }

    #[async_trait]
    impl ProjectionLog for TimedLog {
        async fn read_page(
            &self,
            partition_id: u32,
            fanout: u16,
            after: Option<ProjectionImageKey>,
            limit: usize,
        ) -> Result<ProjectionPage> {
            self.reads.lock().unwrap().push(Instant::now());
            self.log.read_page(partition_id, fanout, after, limit).await
        }

        async fn delete_images(
            &self,
            partition_id: u32,
            fanout: u16,
            keys: &[ProjectionImageKey],
        ) -> Result<u64> {
            self.log.delete_images(partition_id, fanout, keys).await
        }
    }

    #[tokio::test]
    async fn a_cancelled_projector_stops() {
        let log = TestLog::default();
        let sink = TestSink::new();
        log.commit(image(1, 1));
        let cancel = CancellationToken::new();
        cancel.cancel();

        projector(&log, &sink, PAGE_SIZE).run(cancel).await;

        assert!(sink.applied.lock().unwrap().is_empty());
    }

    // `projection-passes` criterion 2.12.
    #[test]
    fn a_pass_reports_its_counts() {
        let recorder = DebuggingRecorder::new();
        let log = TestLog::default();
        let sink = TestSink::new();
        for (run, transition_seq) in [(1, 1), (1, 2), (2, 1)] {
            log.commit(image(run, transition_seq));
        }
        sink.poisoned
            .lock()
            .unwrap()
            .insert(RunKey(Uuid::from_u128(2)));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        with_local_recorder(&recorder, || {
            runtime.block_on(projector(&log, &sink, PAGE_SIZE).pass(&CancellationToken::new()))
        });

        let values = recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| format!("{}={}", label.key(), label.value()))
                    .collect::<Vec<_>>()
                    .join(",");
                (format!("{}{{{labels}}}", key.key().name()), value)
            })
            .collect::<BTreeMap<_, _>>();
        let counter = |name: &str| match values.get(name) {
            Some(DebugValue::Counter(count)) => *count,
            other => panic!("{name}: {other:?}"),
        };
        assert_eq!(
            counter("tokeira_projection_images_read_total{partition_id=0}"),
            3
        );
        assert_eq!(
            counter("tokeira_projection_records_processed_total{partition_id=0,outcome=success}"),
            1
        );
        assert_eq!(
            counter("tokeira_projection_records_processed_total{partition_id=0,outcome=failure}"),
            1
        );
        assert_eq!(
            counter("tokeira_projection_images_deleted_total{partition_id=0}"),
            2
        );
        assert_eq!(
            counter("tokeira_projection_pass_failures_total{partition_id=0,kind=apply}"),
            1
        );
        assert!(values.contains_key("tokeira_projection_pass_duration_seconds{partition_id=0}"));
    }

    // `projection-passes` criterion 2.5: a partition's projector runs while this node
    // holds the partition's home, stops when the acquisition ends, and starts again
    // under a new acquisition.
    #[tokio::test]
    async fn the_supervisor_runs_projectors_where_their_homes_are() {
        let log = TestLog::default();
        let sink = TestSink::new();
        let homes = Arc::new(Mutex::new(BTreeMap::<u32, ProjectorHome>::new()));
        let held = |generation| ProjectorHome {
            generation,
            cancel: CancellationToken::new(),
        };
        homes.lock().unwrap().insert(0, held(1));
        let supervisor = ProjectorSupervisor {
            log: log.clone(),
            sink: {
                let sink = sink.clone();
                move || sink.clone()
            },
            partitions: 2,
            fanout: 1,
            page_size: PAGE_SIZE,
            homes: {
                let homes = homes.clone();
                move |partition| homes.lock().unwrap().get(&partition).cloned()
            },
        };
        let cancel = CancellationToken::new();
        let supervising = tokio::spawn(supervisor.run(cancel.clone()));
        let applied = |run| {
            let sink = sink.clone();
            async move || {
                sink.stored_seq(RunKey(Uuid::from_u128(run)))
                    .await
                    .is_some()
            }
        };
        log.commit(image_in(0, 1));
        log.commit(image_in(1, 2));

        assert!(
            eventually(applied(1)).await,
            "the held home's projector runs"
        );
        assert!(!applied(2)().await, "no projector runs without the home");
        homes.lock().unwrap().insert(1, held(1));
        assert!(
            eventually(applied(2)).await,
            "a gained home's projector starts"
        );

        let ended = homes.lock().unwrap().remove(&0).unwrap();
        ended.cancel.cancel();
        log.commit(image_in(0, 3));
        tokio::time::sleep(IDLE_PAUSE + HOME_CHECK_PERIOD).await;
        assert!(
            !applied(3)().await,
            "the ended acquisition's projector stopped"
        );
        homes.lock().unwrap().insert(0, held(2));
        assert!(
            eventually(applied(3)).await,
            "a new acquisition's projector starts"
        );

        cancel.cancel();
        supervising.await.unwrap();
    }

    use proptest::prelude::*;

    /// Each run's statuses, one an image, the last its newest.
    fn histories() -> impl Strategy<Value = Vec<Vec<ExecutionStatus>>> {
        prop::collection::vec(
            prop::collection::vec(
                prop_oneof![
                    Just(ExecutionStatus::Running),
                    Just(ExecutionStatus::Completed),
                    Just(ExecutionStatus::Failed),
                ],
                1..5,
            ),
            1..6,
        )
    }

    /// The faults of one pass: its first read fails, its first delete fails, and the
    /// applies of some runs fail. Each clears after its pass.
    fn faults() -> impl Strategy<Value = (bool, bool, BTreeSet<usize>)> {
        (
            any::<bool>(),
            any::<bool>(),
            prop::collection::btree_set(0usize..6, 0..3),
        )
    }

    /// A history, the order its images commit in, whether a pass follows each commit,
    /// each pass's faults, and the page size.
    #[allow(clippy::type_complexity)]
    fn schedules() -> impl Strategy<
        Value = (
            Vec<Vec<ExecutionStatus>>,
            Vec<usize>,
            Vec<bool>,
            Vec<(bool, bool, BTreeSet<usize>)>,
            usize,
        ),
    > {
        histories().prop_flat_map(|histories| {
            let order = histories
                .iter()
                .enumerate()
                .flat_map(|(run, statuses)| std::iter::repeat_n(run, statuses.len()))
                .collect::<Vec<_>>();
            let commits = order.len();
            (
                Just(histories),
                Just(order).prop_shuffle(),
                prop::collection::vec(any::<bool>(), commits),
                prop::collection::vec(faults(), commits),
                1usize..6,
            )
        })
    }

    fn run_number(run: usize) -> u128 {
        // Spread the runs across the key space, so a page's position falls between
        // them, and over three rollup stripes, so some runs share one and the rest
        // apply together.
        ((run as u128 + 1) << 100) | (run as u128 % 3)
    }

    async fn run_pass(
        projector: &Projector<TestLog, TestSink>,
        (fail_read, fail_delete, poisoned): &(bool, bool, BTreeSet<usize>),
    ) -> PassReport {
        *projector.log.failing_reads.lock().unwrap() = usize::from(*fail_read);
        *projector.log.failing_deletes.lock().unwrap() = usize::from(*fail_delete);
        *projector.sink.poisoned.lock().unwrap() = poisoned
            .iter()
            .map(|run| RunKey(Uuid::from_u128(run_number(*run))))
            .collect();
        let report = projector.pass(&CancellationToken::new()).await;
        *projector.log.failing_reads.lock().unwrap() = 0;
        *projector.log.failing_deletes.lock().unwrap() = 0;
        projector.sink.poisoned.lock().unwrap().clear();
        report
    }

    /// Commit `histories` in `order`, running a pass with its faults wherever
    /// `passes` says, alternating between `projectors`; then run passes without
    /// faults until one changes nothing.
    async fn converge(
        projectors: &[Projector<TestLog, TestSink>],
        histories: &[Vec<ExecutionStatus>],
        order: &[usize],
        passes: &[bool],
        faults: &[(bool, bool, BTreeSet<usize>)],
    ) {
        let log = &projectors[0].log;
        let mut committed = vec![0usize; histories.len()];
        let mut ran = 0usize;
        for (step, run) in order.iter().enumerate() {
            let status = histories[*run][committed[*run]];
            committed[*run] += 1;
            log.commit(image_with_status(
                run_number(*run),
                committed[*run] as u64,
                status,
            ));
            if passes[step] {
                run_pass(&projectors[ran % projectors.len()], &faults[step]).await;
                ran += 1;
            }
        }
        for _ in 0..3 {
            let report = run_pass(
                &projectors[ran % projectors.len()],
                &(false, false, BTreeSet::new()),
            )
            .await;
            ran += 1;
            if report.idle() && log.keys().is_empty() {
                break;
            }
        }
    }

    async fn expect_converged(
        sink: &TestSink,
        log: &TestLog,
        histories: &[Vec<ExecutionStatus>],
    ) -> Result<(), TestCaseError> {
        prop_assert_eq!(log.keys(), Vec::<ProjectionImageKey>::new());
        let mut expected_counts = BTreeMap::<String, i64>::new();
        for (run, statuses) in histories.iter().enumerate() {
            let run_key = RunKey(Uuid::from_u128(run_number(run)));
            prop_assert_eq!(
                sink.stored_seq(run_key).await,
                Some(TransitionSeq(statuses.len() as u64))
            );
            *expected_counts
                .entry(crate::workflow_status_keyword(*statuses.last().unwrap()))
                .or_default() += 1;
        }
        let counts = sink
            .store
            .count_from_rollup(
                NamespaceId(Uuid::from_u128(1)),
                ArchetypeId::WORKFLOW,
                crate::RollupDimension::ExecutionStatus,
            )
            .await
            .unwrap()
            .groups
            .into_iter()
            .map(|group| (group.value, group.count))
            .filter(|(_, count)| *count != 0)
            .collect::<BTreeMap<_, _>>();
        prop_assert_eq!(counts, expected_counts);
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: projection-passes, Property 1: Images converge
        // Committed in any order, read in any page size, through failures that later
        // clear, passes leave every run's row at its newest image and every counter at
        // the count over the rows.
        #[test]
        fn prop_images_converge((histories, order, passes, faults, page_size) in schedules()) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let (sink, log) = TestSink::with_log();
                let projectors = [projector(&log, &sink, page_size)];
                converge(&projectors, &histories, &order, &passes, &faults).await;
                expect_converged(&sink, &log, &histories).await
            })?;
        }

        // Feature: projection-passes, Property 2: Deleting is safe
        // A pass deletes an image only once its run's row is at least as new.
        #[test]
        fn prop_deleting_is_safe((histories, order, passes, faults, page_size) in schedules()) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let (sink, log) = TestSink::with_log();
                let projectors = [projector(&log, &sink, page_size)];
                converge(&projectors, &histories, &order, &passes, &faults).await;
                prop_assert_eq!(log.unsafe_deletes.lock().unwrap().clone(), Vec::<ProjectionImageKey>::new());
                Ok(())
            })?;
        }

        // Feature: projection-passes, Property 3: Every image is reached
        // Without failures, a pass applies or deletes every image visible when it
        // starts, whatever commits land behind its position meanwhile.
        #[test]
        fn prop_every_image_is_reached(
            histories in histories(),
            late in prop::collection::vec(0usize..12, 0..4),
            page_size in 1usize..6,
        ) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let (sink, log) = TestSink::with_log();
                for (run, statuses) in histories.iter().enumerate() {
                    for (index, status) in statuses.iter().enumerate() {
                        log.commit(image_with_status(run_number(run), index as u64 + 1, *status));
                    }
                }
                let visible = log.keys();
                *log.late.lock().unwrap() = late
                    .iter()
                    .map(|run| image(run_number(*run) + 1, 1))
                    .collect();
                let projector = projector(&log, &sink, page_size);

                projector.pass(&CancellationToken::new()).await;

                let left = log.keys();
                prop_assert!(visible.iter().all(|key| !left.contains(key)), "{:?} left of {:?}", left, visible);
                projector.pass(&CancellationToken::new()).await;
                prop_assert_eq!(log.keys(), Vec::<ProjectionImageKey>::new());
                Ok(())
            })?;
        }

        // Feature: projection-passes, Property 4: Count is exact under faults
        // Two projectors take turns over one log and one store, through failed reads,
        // applies and deletes, and leave every counter at the count over the rows.
        #[test]
        fn prop_count_is_exact_under_faults((histories, order, passes, faults, page_size) in schedules()) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let (sink, log) = TestSink::with_log();
                let projectors = [
                    projector(&log, &sink, page_size),
                    projector(&log, &sink, page_size + 1),
                ];
                converge(&projectors, &histories, &order, &passes, &faults).await;
                expect_converged(&sink, &log, &histories).await
            })?;
        }

        // `projection-passes` criterion 2.10: whatever stripes a page's runs fall in,
        // a pass applies one image of each run, its newest, and at most one run of a
        // stripe at a time, while the runs of different stripes apply together. Every
        // run is counted once.
        #[test]
        fn prop_runs_of_different_stripes_apply_together(
            stripes in prop::collection::vec(0u128..16, 1..12),
            transitions in 1u64..4,
        ) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let (inner, log) = TestSink::with_log();
                let sink = OverlapSink::new(inner.clone());
                let runs = stripes
                    .iter()
                    .enumerate()
                    .map(|(index, stripe)| ((index as u128 + 1) << 100) | stripe)
                    .collect::<Vec<_>>();
                for run in &runs {
                    for transition_seq in 1..=transitions {
                        log.commit(image(*run, transition_seq));
                    }
                }

                let report = overlap_projector(&log, &sink).pass(&CancellationToken::new()).await;

                let distinct = stripes.iter().collect::<BTreeSet<_>>().len();
                prop_assert_eq!(sink.most(), Overlap { all: distinct, stripe: 1, run: 1 });
                let applied = inner.applied.lock().unwrap().clone();
                prop_assert_eq!(applied.len(), runs.len());
                prop_assert_eq!(
                    applied.into_iter().collect::<BTreeSet<_>>(),
                    runs.iter().map(|run| key(*run, transitions)).collect::<BTreeSet<_>>()
                );
                prop_assert_eq!(report.applied, runs.len());
                prop_assert_eq!(log.keys(), Vec::<ProjectionImageKey>::new());
                prop_assert_eq!(
                    status_counts(&inner.store).await,
                    BTreeMap::from([(crate::workflow_status_keyword(ExecutionStatus::Running), runs.len() as i64)])
                );
                Ok(())
            })?;
        }
    }
}
