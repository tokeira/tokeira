//! Finite, fair discovery with admission before payload hydration.
//! Cursors and retry delays are disposable. Each home/kind finishes a captured
//! key range, including past failing records, then starts again at the beginning.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{OperationKey, OperationKind};
use tokeira_storage::{
    IntentDescriptor, IntentKey, IntentKind, OperationRow, RunRepository, ScanBound, WriteFence,
};
use tokeira_types::ShardId;
use tokio::{
    task::{Id, JoinSet},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    errors::NotShardOwner,
    shard::{ShardAcquisition, ShardOwner},
};

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;

const MAX_IN_FLIGHT: usize = 32;
const MAX_PER_HOME: usize = 8;
const PAGES_PER_TICK: usize = 16;
const RETRY_SLOTS: usize = 4096;
const TICK: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub(crate) struct Attempt {
    pub(crate) acquisition: ShardAcquisition,
    pub(crate) fence: WriteFence,
}

impl Attempt {
    pub(crate) fn capture(owner: &Arc<RwLock<ShardOwner>>, home: ShardId) -> Option<Self> {
        let owner = owner.read().expect("shard owner lock poisoned");
        let acquisition = owner
            .acquisition(home)
            .filter(|a| owner.acquisition_active(a))?;
        let fence = owner.fence_at(home, acquisition.epoch);
        Some(Self { acquisition, fence })
    }

    pub(crate) fn validate(&self, owner: &Arc<RwLock<ShardOwner>>) -> Result<()> {
        if !owner
            .read()
            .expect("shard owner lock poisoned")
            .acquisition_active(&self.acquisition)
        {
            return Err(
                NotShardOwner::local(self.acquisition.shard_id, self.acquisition.epoch).into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) enum Work {
    Operation(OperationRow),
    Intent(IntentDescriptor),
    Publication(tokeira_storage::PreparationToken),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Operation(OperationKey, i64, u32),
    Intent(IntentKey),
    Publication(uuid::Uuid),
}

impl Work {
    fn key(&self) -> Key {
        match self {
            Self::Operation(row) => Key::Operation(
                row.incarnation.key,
                row.incarnation.requested_event,
                row.incarnation.attempt,
            ),
            Self::Intent(row) => Key::Intent(row.key),
            Self::Publication(token) => Key::Publication(token.id),
        }
    }
}

#[async_trait]
pub(super) trait Executor: Send + Sync + 'static {
    fn wakeup(&self) -> Option<Arc<tokio::sync::Notify>> {
        None
    }
    fn operation_kinds(&self) -> &'static [OperationKind];
    fn accepts_intent(&self, kind: IntentKind) -> bool;
    async fn deliver(&self, work: Work, attempt: Attempt) -> Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum StreamKind {
    Operation(OperationKind),
    Intent,
    Publication,
}
type Stream = (ShardId, StreamKind);

enum Cursor {
    Publication {
        bound: ScanBound<uuid::Uuid>,
        after: Option<uuid::Uuid>,
    },
    Operation {
        bound: ScanBound<OperationKey>,
        after: Option<OperationKey>,
    },
    Intent {
        bound: ScanBound<IntentKey>,
        after: Option<IntentKey>,
    },
}

impl Cursor {
    fn new(kind: StreamKind) -> Self {
        match kind {
            StreamKind::Operation(_) => Self::Operation {
                bound: ScanBound::Capture,
                after: None,
            },
            StreamKind::Intent => Self::Intent {
                bound: ScanBound::Capture,
                after: None,
            },
            StreamKind::Publication => Self::Publication {
                bound: ScanBound::Capture,
                after: None,
            },
        }
    }

    async fn page<R: RunRepository>(
        &mut self,
        repo: &R,
        stream: Stream,
        limit: usize,
    ) -> Result<Vec<Work>> {
        match self {
            Self::Publication { bound, after } => {
                let page = repo
                    .read_published_preparations(stream.0, *bound, *after, limit)
                    .await?;
                if let Some(last) = page.rows.last() {
                    *bound = ScanBound::Through(
                        page.through.expect("nonempty publication page has bound"),
                    );
                    *after = Some(last.id);
                } else {
                    *self = Self::new(StreamKind::Publication);
                }
                Ok(page.rows.into_iter().map(Work::Publication).collect())
            }
            Self::Operation { bound, after } => {
                let StreamKind::Operation(kind) = stream.1 else {
                    unreachable!("operation cursor has operation kind")
                };
                let page = repo
                    .read_operation_page(stream.0, kind, *bound, *after, limit)
                    .await?;
                if let Some(last) = page.rows.last() {
                    *bound =
                        ScanBound::Through(page.through.expect("nonempty page has finite bound"));
                    *after = Some(last.incarnation.key);
                } else {
                    *self = Self::new(stream.1);
                }
                Ok(page.rows.into_iter().map(Work::Operation).collect())
            }
            Self::Intent { bound, after } => {
                let page = repo
                    .read_intent_page(stream.0, *bound, *after, limit)
                    .await?;
                if let Some(last) = page.rows.last() {
                    *bound =
                        ScanBound::Through(page.through.expect("nonempty page has finite bound"));
                    *after = Some(last.key);
                } else {
                    *self = Self::new(StreamKind::Intent);
                }
                Ok(page.rows.into_iter().map(Work::Intent).collect())
            }
        }
    }
}

#[derive(Default)]
struct Discovery {
    streams: BTreeMap<Stream, (u64, Cursor)>,
    previous: Option<Stream>,
}

impl Discovery {
    fn refresh(&mut self, owner: &Arc<RwLock<ShardOwner>>, kinds: &[OperationKind]) {
        let active = {
            let owner = owner.read().expect("shard owner lock poisoned");
            owner
                .active_shards()
                .filter_map(|home| owner.acquisition(home).map(|a| (home, a.generation)))
                .collect::<BTreeMap<_, _>>()
        };
        self.streams
            .retain(|(home, _), (generation, _)| active.get(home) == Some(generation));
        for (home, generation) in active {
            for kind in kinds
                .iter()
                .copied()
                .map(StreamKind::Operation)
                .chain([StreamKind::Intent, StreamKind::Publication])
            {
                self.streams
                    .entry((home, kind))
                    .or_insert_with(|| (generation, Cursor::new(kind)));
            }
        }
    }

    fn next(&mut self) -> Option<Stream> {
        let next = self
            .previous
            .and_then(|last| {
                self.streams
                    .range((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded))
                    .next()
            })
            .or_else(|| self.streams.first_key_value())
            .map(|(key, _)| *key);
        self.previous = next;
        next
    }
}

/// One runtime-owned worker pool; shutdown cancels attempts, never obligations.
pub(super) async fn run<R: RunRepository + 'static, E: Executor>(
    repo: Arc<R>,
    executor: Arc<E>,
    owner: Arc<RwLock<ShardOwner>>,
    cancel: CancellationToken,
) {
    let wakeup = executor.wakeup();
    let mut discovery = Discovery::default();
    let mut tasks = JoinSet::<Result<()>>::new();
    let mut task_keys = HashMap::<Id, (Key, ShardId)>::new();
    let mut home_in_flight = BTreeMap::<ShardId, usize>::new();
    let mut busy = BTreeSet::new();
    let mut retry = BTreeMap::<Key, (u32, Instant)>::new();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            Some(result) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                let (id, outcome) = match result {
                    Ok((id, Ok(()))) => (id, "advanced"),
                    Ok((id, Err(error))) => (id, match error.downcast_ref::<tokeira_storage::RecoveryError>() {
                        Some(tokeira_storage::RecoveryError::LegacyState(_) | tokeira_storage::RecoveryError::UnsupportedVersion { .. }
                            | tokeira_storage::RecoveryError::InvalidIdentity { .. } | tokeira_storage::RecoveryError::InvalidPreparation(_)) => "blocked_by_data",
                        Some(tokeira_storage::RecoveryError::Obsolete) => "obsolete",
                        _ => "retrying",
                    }),
                    Err(error) => (error.id(), "retrying"),
                };
                crate::metrics::record_outbound_attempt(outcome);
                let failed = outcome == "retrying" || outcome == "blocked_by_data";
                if let Some((key, home)) = task_keys.remove(&id) {
                    let count = home_in_flight.get_mut(&home).expect("admitted home count exists");
                    *count -= 1;
                    if *count == 0 { home_in_flight.remove(&home); }
                    busy.remove(&key);
                    let failures = if failed { retry.get(&key).map_or(1, |(n, _)| n.saturating_add(1).min(6)) } else { 0 };
                    if retry.len() >= RETRY_SLOTS { retry.pop_first(); }
                    retry.insert(key, (failures, Instant::now() + Duration::from_secs(1 << failures)));
                    if failed { tracing::debug!("outbound attempt did not settle; committed obligation retained"); }
                }
            }
            _ = async {
                if let Some(wakeup) = &wakeup {
                    tokio::select! { _ = tick.tick() => {}, _ = wakeup.notified() => {} }
                } else { tick.tick().await; }
            } => {
                discovery.refresh(&owner, executor.operation_kinds());
                let mut first_stream = None;
                for _ in 0..PAGES_PER_TICK.min(discovery.streams.len()) {
                    if tasks.len() >= MAX_IN_FLIGHT { break; }
                    let Some(stream) = discovery.next() else { break; };
                    first_stream.get_or_insert(stream);
                    let remaining = MAX_PER_HOME.saturating_sub(home_in_flight.get(&stream.0).copied().unwrap_or(0));
                    if remaining == 0 { continue; }
                    let Some(attempt) = Attempt::capture(&owner, stream.0) else { continue; };
                    let cursor = &mut discovery.streams.get_mut(&stream).expect("selected stream exists").1;
                    let page = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        page = cursor.page(repo.as_ref(), stream, remaining.min(MAX_IN_FLIGHT - tasks.len())) => page,
                    };
                    let Ok(page) = page else {
                        tracing::warn!(home = stream.0.0, "outbound discovery failed; retrying a later scan");
                        continue;
                    };
                    crate::metrics::record_outbound_pending(page.len());
                    for work in page {
                        if matches!(&work, Work::Intent(row) if !executor.accepts_intent(row.key.kind))
                            || matches!(&work, Work::Operation(row) if row.due_at > OffsetDateTime::now_utc()) { continue; }
                        let key = work.key();
                        if busy.contains(&key) || retry.get(&key).is_some_and(|(_, due)| *due > Instant::now()) { continue; }
                        let executor = executor.clone();
                        let attempt = attempt.clone();
                        let token = cancel.clone();
                        let span = tracing::info_span!("runtime.outbound", tokeira.home = stream.0.0, ?key);
                        let handle = tasks.spawn(async move {
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => anyhow::bail!("outbound runtime stopped"),
                                _ = attempt.acquisition.cancel.cancelled() => anyhow::bail!("outbound acquisition cancelled"),
                                result = tokio::time::timeout(Duration::from_secs(60), executor.deliver(work, attempt.clone())) => result?,
                            }
                        }.instrument(span));
                        busy.insert(key);
                        task_keys.insert(handle.id(), (key, stream.0));
                        *home_in_flight.entry(stream.0).or_default() += 1;
                    }
                }
                // Completing a full stream sweep must not always restart at the
                // same kind: that kind could consume all per-home admission on
                // every tick. Rotate the starting stream even when every row is
                // permanently pending and earlier streams fill their allowance.
                if let Some(first) = first_stream { discovery.previous = Some(first); }
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
