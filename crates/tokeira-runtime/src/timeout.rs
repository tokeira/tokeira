//! Workflow-level timeout tracking and scanning.
//!
//! Tracks open workflow runs that have execution or run timeouts configured,
//! evaluates whether those timeouts have been violated, and drives a background
//! scanner that submits timeout commands to the appropriate lanes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    Command, RetryState, WorkflowExecutionTimedOutRequest, WorkflowTimeoutType,
};
use tokeira_types::{RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::lane::LaneHandle;
use crate::scanner::pick_lane;
use crate::shard::ShardOwner;

/// Runtime-local timeout tracking entry for one open run.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTimeoutEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub started_at: OffsetDateTime,
    pub first_run_started_at: Option<OffsetDateTime>,
    pub has_retry_policy: bool,
}

/// Shared in-memory tracking state for workflow timeouts.
#[derive(Clone, Default)]
pub struct WorkflowTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<RunKey, WorkflowTimeoutEntry>>>,
}

impl WorkflowTimeoutTrackingState {
    pub fn insert(&self, entry: WorkflowTimeoutEntry) {
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

    pub fn snapshot(&self) -> Vec<WorkflowTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<WorkflowTimeoutEntry> {
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
pub struct WorkflowTimeoutScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_timeouts_per_scan: usize,
}

impl Default for WorkflowTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowTimeoutViolation {
    ExecutionTimeout,
    RunTimeout,
}

pub fn evaluate_workflow_timeout(
    entry: &WorkflowTimeoutEntry,
    now: OffsetDateTime,
) -> Option<WorkflowTimeoutViolation> {
    let execution_started_at = entry.first_run_started_at.unwrap_or(entry.started_at);
    if let Some(timeout) = entry.workflow_execution_timeout
        && (now - execution_started_at > timeout
            || timeout.is_zero() && now >= execution_started_at)
    {
        return Some(WorkflowTimeoutViolation::ExecutionTimeout);
    }

    if let Some(timeout) = entry.workflow_run_timeout
        && (now - entry.started_at > timeout
            || timeout.is_zero() && now >= entry.started_at)
    {
        return Some(WorkflowTimeoutViolation::RunTimeout);
    }

    None
}

pub(crate) fn workflow_timeout_retry_state(entry: &WorkflowTimeoutEntry) -> RetryState {
    if entry.has_retry_policy {
        RetryState::Timeout
    } else {
        RetryState::RetryPolicyNotSet
    }
}

pub(crate) async fn scan_workflow_timeouts_once<F, Fut>(
    tracking: &WorkflowTimeoutTrackingState,
    shard_id: Option<ShardId>,
    config: &WorkflowTimeoutScannerConfig,
    mut submit_timeout: F,
) where
    F: FnMut(WorkflowTimeoutEntry, WorkflowTimeoutViolation, OffsetDateTime) -> Fut,
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
        let Some(violation) = evaluate_workflow_timeout(&entry, now) else {
            continue;
        };

        match submit_timeout(entry.clone(), violation, now).await {
            Ok(()) => tracking.remove(entry.run_key),
            Err(error) => {
                let message = error.to_string();
                if message.contains("kernel rejected") {
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        "workflow timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove(entry.run_key);
                } else {
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        "workflow timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

pub(crate) async fn run_workflow_timeout_scanner(
    tracking: WorkflowTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: WorkflowTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
        for shard_id in active_shards {
            scan_workflow_timeouts_once(
                &tracking,
                Some(shard_id),
                &config,
                |entry, violation, now| {
                    let lane = pick_lane(&lanes, lane_count, entry.shard_id).clone();
                    async move {
                        lane.submit(
                            entry.run_key,
                            Command::WorkflowExecutionTimedOut(
                                WorkflowExecutionTimedOutRequest {
                                    timeout_type: match violation {
                                        WorkflowTimeoutViolation::ExecutionTimeout => {
                                            WorkflowTimeoutType::ExecutionTimeout
                                        }
                                        WorkflowTimeoutViolation::RunTimeout => {
                                            WorkflowTimeoutType::RunTimeout
                                        }
                                    },
                                    retry_state: workflow_timeout_retry_state(&entry),
                                    now,
                                },
                            ),
                        )
                        .await
                        .map(|_| ())
                    }
                },
            )
            .await;
        }
    }
}
