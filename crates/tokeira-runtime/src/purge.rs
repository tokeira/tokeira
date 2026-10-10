//! The purger: finishes the purges of runs that storage has recorded for one
//! (`bounded-bulk-writes`).
//!
//! A deletion's first transaction makes a run unreachable and records it for
//! purging, and an abandoned materialization of a reset's successor leaves a
//! record too. The rows such a run still owns can outnumber what one DSQL
//! transaction may change, so [`RunRepository::purge_run`] removes them in
//! pages. This task runs those purges in the background and retries a failed
//! one after a backoff, until it succeeds or the runtime stops.
//!
//! The queue is volatile, like the runtime's other trackers: the durable record
//! is the task, and a node that takes over a shard hands every record it finds
//! back to its purger ([`recover_bulk_writes`]). The queue needs no scoping to
//! a shard's acquisition: a purge is idempotent and safe from any node, and a
//! stale entry can only meet a purging record or none. A materializing record
//! for the same run would need a second materialization of the same successor
//! id, and each reset gets a fresh run id.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use tokeira_storage::{BulkWritePhase, RunRepository};
use tokeira_types::{RunKey, ShardId};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::recovery::RepairAcquisition;

/// Records listed per page when a shard is taken over.
const RECORD_PAGE: usize = 256;

/// The first retry of a failed purge waits this long; each later one doubles,
/// up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// A handle that queues runs for the purger. Cloning shares the queue.
#[derive(Clone, Debug)]
pub(crate) struct RunPurger {
    sender: mpsc::UnboundedSender<RunKey>,
    /// Runs queued or being purged, so a run is queued once at a time.
    queued: Arc<Mutex<HashSet<RunKey>>>,
    /// How many runs are queued or being purged, for callers that wait for the
    /// purger to go idle.
    pending: watch::Sender<usize>,
}

/// The purger's end of the queue.
#[derive(Debug)]
pub(crate) struct PurgerQueue {
    receiver: mpsc::UnboundedReceiver<RunKey>,
    purger: RunPurger,
}

impl RunPurger {
    /// A purger handle and the queue its task drains.
    pub(crate) fn new() -> (Self, PurgerQueue) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (pending, _) = watch::channel(0);
        let purger = Self {
            sender,
            queued: Arc::new(Mutex::new(HashSet::new())),
            pending,
        };
        let queue = PurgerQueue {
            receiver,
            purger: purger.clone(),
        };
        (purger, queue)
    }

    /// Queue `run_key` for a purge, unless it is already queued. A run without
    /// a record is a no-op purge, so queueing one that needs nothing is
    /// harmless.
    pub(crate) fn enqueue(&self, run_key: RunKey) {
        let newly_queued = self
            .queued
            .lock()
            .expect("purger queue lock poisoned")
            .insert(run_key);
        if newly_queued {
            self.pending.send_modify(|pending| *pending += 1);
            // The receiver lives as long as the runtime; once it is gone the
            // runtime is stopping, and the record waits for the next owner.
            let _ = self.sender.send(run_key);
        }
    }

    /// The runs queued or being purged, in run-key order.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> Vec<RunKey> {
        let mut queued = self
            .queued
            .lock()
            .expect("purger queue lock poisoned")
            .iter()
            .copied()
            .collect::<Vec<_>>();
        queued.sort();
        queued
    }

    /// Wait until no run is queued or being purged.
    #[cfg(test)]
    pub(crate) async fn idle(&self) {
        let mut pending = self.pending.subscribe();
        // The sender lives in `self`, so the channel never closes here.
        let _ = pending.wait_for(|pending| *pending == 0).await;
    }

    fn finish(&self, run_key: RunKey) {
        self.queued
            .lock()
            .expect("purger queue lock poisoned")
            .remove(&run_key);
        self.pending
            .send_modify(|pending| *pending = pending.saturating_sub(1));
    }
}

/// Run queued purges until `cancel` fires, retrying each failure after a
/// backoff that doubles from [`FIRST_BACKOFF`] to [`MAX_BACKOFF`].
pub(crate) async fn run_purger<R>(repo: Arc<R>, mut queue: PurgerQueue, cancel: CancellationToken)
where
    R: RunRepository + ?Sized + 'static,
{
    let mut backoff: HashMap<RunKey, Duration> = HashMap::new();
    loop {
        let run_key = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            run_key = queue.receiver.recv() => match run_key {
                Some(run_key) => run_key,
                None => break,
            },
        };
        match repo.purge_run(run_key).await {
            Ok(()) => {
                backoff.remove(&run_key);
                queue.purger.finish(run_key);
            }
            Err(error) => {
                let delay = backoff
                    .get(&run_key)
                    .map_or(FIRST_BACKOFF, |delay| (*delay * 2).min(MAX_BACKOFF));
                backoff.insert(run_key, delay);
                tracing::warn!(
                    ?error,
                    run_key = ?run_key,
                    retry_in = ?delay,
                    "a run's purge failed; it will be retried"
                );
                // The run stays queued while it waits, so no other caller
                // queues it twice.
                let sender = queue.purger.sender.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = tokio::time::sleep(delay) => {
                            let _ = sender.send(run_key);
                        }
                    }
                });
            }
        }
    }
}

/// Before a taken-over shard admits commands, abandon every materialization
/// recorded on it and hand every recorded run to the purger
/// (`bounded-bulk-writes` criterion 2.9). Returns how many records it found.
///
/// A materialization recorded here was started under an earlier ownership:
/// none started under this one, since the shard admits no commands yet. One
/// still running on an earlier owner fails at its next transaction once the
/// switch commits, so none can make its successor visible after activation.
///
/// With dispatch reconciliation on, `repair` is the acquisition the recovery
/// runs under. Each page and each switch first checks that it is still
/// current, as the repair's transactions do, so a recovery a newer acquisition
/// replaced stops before it switches a materialization that ownership began.
pub(crate) async fn recover_bulk_writes<R>(
    shard_id: ShardId,
    repo: &R,
    purger: &RunPurger,
    repair: Option<&RepairAcquisition>,
) -> Result<usize>
where
    R: RunRepository + ?Sized,
{
    let mut after = None;
    let mut found = 0;
    loop {
        if let Some(repair) = repair {
            repair.validate()?;
        }
        let page = repo
            .list_run_bulk_writes(shard_id, after, RECORD_PAGE)
            .await?;
        for record in &page {
            if record.phase == BulkWritePhase::Materializing {
                if let Some(repair) = repair {
                    repair.validate()?;
                }
                repo.abandon_materialization(record.run_key).await?;
            }
            purger.enqueue(record.run_key);
        }
        found += page.len();
        match page.last() {
            Some(last) if page.len() == RECORD_PAGE => after = Some(last.run_key),
            _ => return Ok(found),
        }
    }
}
