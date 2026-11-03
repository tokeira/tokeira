//! Test-only fan-out observation shared by the memory and live DSQL measurements.
//! Completion requires persisted starts, existing children and no pending start rows.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tokeira_kernel::{HistoryEventKind, LoadedRun, OperationKind, StartRequest};
use tokeira_storage::{RunRepository, ScanBound};
use tokeira_types::{RunKey, ShardId, WorkflowId};

/// Installs a process-global recorder before driver construction, or reports unavailable
/// counts when another test already owns it. Elapsed time uses an unpaused Tokio clock.
#[derive(Debug)]
pub(crate) struct FanOutMeasurement {
    snapshotter: Option<Snapshotter>,
    began: tokio::time::Instant,
    progress: Option<Arc<tokio::sync::Notify>>,
}

impl FanOutMeasurement {
    /// Call before constructing the runtime so its counters register with this recorder.
    /// An optional test-repository notification avoids polling an in-memory store.
    pub(crate) fn new(progress: Option<Arc<tokio::sync::Notify>>) -> Self {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        Self {
            snapshotter: metrics::set_global_recorder(recorder)
                .ok()
                .map(|()| snapshotter),
            began: tokio::time::Instant::now(),
            progress,
        }
    }

    /// Includes acquisition/readiness in the deadline and reports partial progress on
    /// timeout or repository failure before returning the completion assertion.
    pub(crate) async fn run(
        self,
        backend: &str,
        repo: &dyn RunRepository,
        parent: &StartRequest,
        home: ShardId,
        children: &[WorkflowId],
        deadline: Duration,
        ready: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let mut started = 0;
        let mut ready_at = None;
        let result = tokio::time::timeout_at(self.began + deadline, async {
            ready.await?;
            ready_at = Some(tokio::time::Instant::now());
            let mut after = 0;
            let mut runs = BTreeMap::new();
            loop {
                let events = repo.read_history(parent.run_key, after, 256).await?;
                for event in events {
                    after = event.event_id;
                    if let HistoryEventKind::ChildWorkflowExecutionStarted {
                        child_workflow_id,
                        child_run_id,
                        ..
                    } = event.kind
                    {
                        started += 1;
                        runs.insert(child_workflow_id, child_run_id);
                    }
                }
                if started == children.len() && runs.len() == children.len() {
                    for child in children {
                        let run = runs
                            .get(child)
                            .context("expected child has no start event")?;
                        let key = RunKey::derive(parent.namespace_id, child, *run);
                        ensure!(
                            matches!(repo.load_run(key).await?, LoadedRun::Existing(_)),
                            "started child {child:?} is missing"
                        );
                    }
                    let mut bound = ScanBound::Capture;
                    let mut cursor = None;
                    let mut pending = false;
                    loop {
                        let page = repo
                            .read_operation_page(
                                home,
                                OperationKind::ChildStart,
                                bound,
                                cursor,
                                100,
                            )
                            .await?;
                        if page.rows.is_empty() {
                            break;
                        }
                        pending |= page
                            .rows
                            .iter()
                            .any(|row| row.incarnation.key.source_run == parent.run_key);
                        cursor = page.rows.last().map(|row| row.incarnation.key);
                        if let Some(through) = page.through {
                            bound = ScanBound::Through(through);
                        }
                    }
                    if !pending {
                        return Ok(());
                    }
                }
                if let Some(progress) = &self.progress {
                    progress.notified().await;
                } else {
                    // Match the driver's TICK so live observation does not compete
                    // with delivery for a continuous stream of database connections.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        })
        .await
        .context("fan-out did not finish before the deadline")
        .and_then(|result| result);
        self.report(backend, children.len(), started, ready_at);
        result
    }

    // Measurement output is the explicit test artifact, including under nextest capture.
    #[allow(clippy::print_stdout)]
    fn report(
        &self,
        backend: &str,
        children: usize,
        started: usize,
        ready_at: Option<tokio::time::Instant>,
    ) {
        let ended = tokio::time::Instant::now();
        // A readiness failure has no fan-out interval; do not present its duration
        // as delivery time or invent a successful time-to-ready measurement.
        let (ready_ms, elapsed_ms) = ready_at.map_or_else(
            || ("unavailable".to_owned(), "unavailable".to_owned()),
            |ready| {
                (
                    (ready - self.began).as_millis().to_string(),
                    (ended - ready).as_millis().to_string(),
                )
            },
        );
        let mut counts = BTreeMap::<String, u64>::new();
        if let Some(snapshotter) = &self.snapshotter {
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                if key.key().name() == crate::metrics::OUTBOUND_ATTEMPTS_TOTAL
                    && let DebugValue::Counter(count) = value
                    && let Some(outcome) = key.key().labels().find(|label| label.key() == "outcome")
                {
                    *counts.entry(outcome.value().to_owned()).or_default() += count;
                }
            }
        }
        let count = |outcome: &str| {
            if self.snapshotter.is_some() {
                counts.get(outcome).copied().unwrap_or_default().to_string()
            } else {
                "unavailable".to_owned()
            }
        };
        println!(
            "fan-out: backend={backend} children={children} started={started} ready_ms={ready_ms} elapsed_ms={elapsed_ms} advanced={} obsolete={} retrying={} blocked_by_data={}",
            count("advanced"),
            count("obsolete"),
            count("retrying"),
            count("blocked_by_data"),
        );
    }
}
