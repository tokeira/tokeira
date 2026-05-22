//! Workflow-task start-to-close timeout tracking and scanning.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{Command, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType};
use tokeira_observability::OutcomeLabel;
use tokeira_types::{LogicalTaskSeq, RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{
    lane::LaneHandle, metrics as runtime_metrics, scanner::pick_lane_for_run_key, shard::ShardOwner,
};

#[derive(Clone, Debug, PartialEq)]
pub struct WftTimeoutEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub logical_seq: LogicalTaskSeq,
    pub started_event_id: i64,
    pub started_at: OffsetDateTime,
    pub workflow_task_timeout: Duration,
}

#[derive(Clone, Default)]
pub struct WftTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<RunKey, WftTimeoutEntry>>>,
}

impl WftTimeoutTrackingState {
    pub fn insert(&self, entry: WftTimeoutEntry) {
        self.inner.lock().unwrap().insert(entry.run_key, entry);
    }

    pub fn remove(&self, run_key: RunKey) {
        self.inner.lock().unwrap().remove(&run_key);
    }

    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .unwrap()
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    pub fn snapshot(&self) -> Vec<WftTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<WftTimeoutEntry> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.shard_id == shard_id)
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WftTimeoutScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_timeouts_per_scan: usize,
}

impl Default for WftTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

pub fn evaluate_wft_timeout(entry: &WftTimeoutEntry, now: OffsetDateTime) -> bool {
    now - entry.started_at > entry.workflow_task_timeout
        || entry.workflow_task_timeout.is_zero() && now >= entry.started_at
}

pub(crate) async fn scan_wft_timeouts_once<F, Fut>(
    tracking: &WftTimeoutTrackingState,
    shard_id: Option<ShardId>,
    config: &WftTimeoutScannerConfig,
    mut submit_timeout: F,
) where
    F: FnMut(WftTimeoutEntry, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let now = OffsetDateTime::now_utc();
    let entries = match shard_id {
        Some(shard_id) => tracking.snapshot_for_shard(shard_id),
        None => tracking.snapshot(),
    };
    let mut submitted = 0usize;

    for entry in entries {
        if submitted >= config.max_timeouts_per_scan {
            break;
        }
        if !evaluate_wft_timeout(&entry, now) {
            continue;
        }

        match submit_timeout(entry.clone(), now).await {
            Ok(()) => {
                runtime_metrics::record_workflow_task_timed_out(OutcomeLabel::Success);
                tracking.remove(entry.run_key);
            }
            Err(error) => {
                let message = error.to_string();
                if message.contains("kernel rejected") {
                    runtime_metrics::record_workflow_task_timed_out(OutcomeLabel::Rejected);
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        "wft timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove(entry.run_key);
                } else {
                    runtime_metrics::record_workflow_task_timed_out(OutcomeLabel::Failure);
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        "wft timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

pub(crate) async fn run_wft_timeout_scanner(
    tracking: WftTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: WftTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("wft_timeout", shard_id.0);
            scan_wft_timeouts_once(&tracking, Some(shard_id), &config, |entry, now| {
                runtime_metrics::record_scanner_dispatched("wft_timeout", shard_id.0);
                let lane = pick_lane_for_run_key(&lanes, lane_count, entry.run_key).clone();
                async move {
                    lane.submit(
                        entry.run_key,
                        Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                            logical_seq: entry.logical_seq,
                            started_event_id: entry.started_event_id,
                            timeout_type: WorkflowTaskTimeoutType::StartToClose,
                            now,
                        }),
                    )
                    .await
                    .map(|_| ())
                }
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_entry(run_key: RunKey, started_at: OffsetDateTime) -> WftTimeoutEntry {
        WftTimeoutEntry {
            run_key,
            shard_id: ShardId(0),
            logical_seq: LogicalTaskSeq(7),
            started_event_id: 42,
            started_at,
            workflow_task_timeout: Duration::seconds(5),
        }
    }

    #[test]
    fn evaluate_wft_timeout_respects_elapsed_and_zero_timeouts() {
        let now = fixed_now();
        let expired = sample_entry(RunKey::new(), now - Duration::seconds(6));
        assert!(evaluate_wft_timeout(&expired, now));

        let fresh = sample_entry(RunKey::new(), now - Duration::seconds(4));
        assert!(!evaluate_wft_timeout(&fresh, now));

        let mut zero = sample_entry(RunKey::new(), now);
        zero.workflow_task_timeout = Duration::ZERO;
        assert!(evaluate_wft_timeout(&zero, now));
    }

    #[tokio::test]
    async fn scan_wft_timeouts_once_submits_and_removes_only_expired_entries() {
        let tracking = WftTimeoutTrackingState::default();
        let now = OffsetDateTime::now_utc();
        let expired = sample_entry(RunKey::new(), now - Duration::seconds(6));
        let fresh = sample_entry(RunKey::new(), now - Duration::seconds(1));
        tracking.insert(expired.clone());
        tracking.insert(fresh.clone());

        let submitted = Arc::new(Mutex::new(Vec::new()));
        scan_wft_timeouts_once(
            &tracking,
            Some(ShardId(0)),
            &WftTimeoutScannerConfig::default(),
            {
                let submitted = submitted.clone();
                move |entry, _| {
                    let submitted = submitted.clone();
                    async move {
                        submitted.lock().unwrap().push(entry.run_key);
                        Ok(())
                    }
                }
            },
        )
        .await;

        assert_eq!(*submitted.lock().unwrap(), vec![expired.run_key]);
        let remaining = tracking.snapshot();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].run_key, fresh.run_key);
    }

    #[tokio::test]
    async fn completed_wft_removed_before_scan_does_not_submit_timeout() {
        let tracking = WftTimeoutTrackingState::default();
        let now = fixed_now();
        let entry = sample_entry(RunKey::new(), now - Duration::seconds(10));
        tracking.insert(entry.clone());
        tracking.remove(entry.run_key);

        scan_wft_timeouts_once(
            &tracking,
            Some(ShardId(0)),
            &WftTimeoutScannerConfig::default(),
            |_entry, _| async move {
                panic!("completed workflow task should not be submitted");
                #[allow(unreachable_code)]
                Ok(())
            },
        )
        .await;

        assert!(tracking.snapshot().is_empty());
    }
}
