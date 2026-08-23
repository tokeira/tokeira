//! DynamoDB-backed connection slot block ownership.
//!
//! Slot blocks are the cluster-wide connection-capacity fence. A refiller must
//! reserve a slot before opening a physical DSQL connection, and every path that
//! drops that connection must release exactly one slot. Lost DynamoDB leases
//! reduce future capacity but do not kill already checked-out connections.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use aws_sdk_dynamodb::{Client, types::AttributeValue};
use tokio::{sync::RwLock, task::JoinHandle};
use uuid::Uuid;

use crate::metrics;

pub const SLOT_BLOCK_SIZE: usize = 50;

/// Hard cap on block probes. This bounds startup latency if all low-numbered
/// blocks are already owned by other nodes.
const MAX_BLOCK_PROBES: u32 = 1024;
/// Slot ownership lease duration. It is intentionally longer than the renewal
/// interval so one transient renewal delay does not immediately shed capacity.
const BLOCK_LEASE_DURATION: Duration = Duration::from_secs(60);
const RENEW_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Debug)]
pub struct SlotBlockManager {
    client: Client,
    table_name: String,
    owner_id: String,
    owned_blocks: RwLock<HashSet<u32>>,
    total_slots: AtomicUsize,
    used_slots: AtomicUsize,
    renewer: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl SlotBlockManager {
    /// Start production slot coordination and acquire at least one block.
    ///
    /// Failing here is a startup failure. Running without slot coordination
    /// would make a multi-node deployment able to exceed DSQL connection limits.
    pub async fn start(
        client: Client,
        table_name: impl Into<String>,
        owner_hint: impl Into<String>,
    ) -> Result<Arc<Self>> {
        let manager = Arc::new(Self {
            client,
            table_name: table_name.into(),
            owner_id: format!("{}-{}", owner_hint.into(), Uuid::new_v4()),
            owned_blocks: RwLock::new(HashSet::new()),
            total_slots: AtomicUsize::new(0),
            used_slots: AtomicUsize::new(0),
            renewer: std::sync::Mutex::new(None),
        });
        manager.validate_table().await?;
        manager.ensure_capacity(SLOT_BLOCK_SIZE).await?;
        manager.start_renewer();
        Ok(manager)
    }

    #[cfg(any(test, feature = "dsql-integration"))]
    pub fn local_for_tests(total_slots: usize) -> Arc<Self> {
        Arc::new(Self {
            client: crate::dsql::aws_http::offline_ddb_client(),
            table_name: "local-test".to_owned(),
            owner_id: "local-test".to_owned(),
            owned_blocks: RwLock::new(HashSet::new()),
            total_slots: AtomicUsize::new(total_slots),
            used_slots: AtomicUsize::new(0),
            renewer: std::sync::Mutex::new(None),
        })
    }

    pub async fn validate_table(&self) -> Result<()> {
        self.client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to describe DSQL connection lease DynamoDB table {}",
                    self.table_name
                )
            })?;
        Ok(())
    }

    pub fn has_budget(&self) -> bool {
        self.used_slots.load(Ordering::Acquire) < self.total_slots.load(Ordering::Acquire)
    }

    pub async fn acquire_slot(&self) -> Result<SlotReservation> {
        // Capacity is acquired in blocks but consumed one connection at a time.
        // The CAS loop below is the local invariant that prevents two local
        // refillers from using the same slot concurrently.
        if !self.has_budget() {
            self.ensure_capacity(SLOT_BLOCK_SIZE).await?;
        }
        loop {
            let used = self.used_slots.load(Ordering::Acquire);
            let total = self.total_slots.load(Ordering::Acquire);
            if used >= total {
                bail!("DSQL connection slot capacity is exhausted");
            }
            if self
                .used_slots
                .compare_exchange(used, used + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(SlotReservation);
            }
        }
    }

    pub fn release_slot(&self) {
        // Release is intentionally saturating. Shutdown and discard paths can
        // race with already-failed creation attempts; underflow would make
        // future capacity accounting unsafe.
        let mut current = self.used_slots.load(Ordering::Acquire);
        while current > 0 {
            match self.used_slots.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    pub async fn release_all(&self) -> Result<()> {
        // Best-effort ownership cleanup for graceful shutdown. Existing
        // connections should already be retired by the reservoir; clearing the
        // owner lets another node acquire the blocks without waiting for TTL.
        let blocks = self.owned_blocks.write().await.drain().collect::<Vec<_>>();
        for block_id in blocks {
            self.client
                .update_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(block_pk(block_id)))
                .update_expression("SET expires_at_ms = :now, ttl_epoch = :ttl REMOVE owner_id")
                .condition_expression("owner_id = :owner")
                .expression_attribute_values(":owner", AttributeValue::S(self.owner_id.clone()))
                .expression_attribute_values(":now", AttributeValue::N(unix_millis().to_string()))
                .expression_attribute_values(
                    ":ttl",
                    AttributeValue::N(unix_seconds().saturating_add(300).to_string()),
                )
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed to release DSQL connection slot block {block_id} in {}",
                        self.table_name
                    )
                })?;
        }
        self.total_slots.store(0, Ordering::Release);
        self.used_slots.store(0, Ordering::Release);
        metrics::set_dsql_slot_blocks_owned(0);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn used_slots_for_tests(&self) -> usize {
        self.used_slots.load(Ordering::Acquire)
    }

    /// Number of physical connection slots currently charged to this process.
    pub fn used_slots(&self) -> usize {
        self.used_slots.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn total_slots_for_tests(&self) -> usize {
        self.total_slots.load(Ordering::Acquire)
    }

    async fn ensure_capacity(&self, requested_slots: usize) -> Result<()> {
        if self.total_slots.load(Ordering::Acquire) < requested_slots
            || self.used_slots.load(Ordering::Acquire) >= self.total_slots.load(Ordering::Acquire)
        {
            let Some(block_id) = self.try_acquire_any_block().await? else {
                bail!("failed to acquire DSQL connection slot block");
            };
            let mut owned = self.owned_blocks.write().await;
            if owned.insert(block_id) {
                self.total_slots
                    .fetch_add(SLOT_BLOCK_SIZE, Ordering::AcqRel);
                metrics::set_dsql_slot_blocks_owned(owned.len());
            }
        }
        Ok(())
    }

    async fn try_acquire_any_block(&self) -> Result<Option<u32>> {
        // Deterministic probing keeps the implementation simple. Conditional
        // writes provide fairness at the DynamoDB boundary when several nodes
        // probe the same block at once.
        for block_id in 0..MAX_BLOCK_PROBES {
            if self.try_acquire_block(block_id).await? {
                return Ok(Some(block_id));
            }
        }
        Ok(None)
    }

    async fn try_acquire_block(&self, block_id: u32) -> Result<bool> {
        let now_ms = unix_millis();
        let expires_at_ms = now_ms.saturating_add(BLOCK_LEASE_DURATION.as_millis() as u64);
        let result = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("pk", AttributeValue::S(block_pk(block_id)))
            .item("owner_id", AttributeValue::S(self.owner_id.clone()))
            .item("slots", AttributeValue::N(SLOT_BLOCK_SIZE.to_string()))
            .item(
                "expires_at_ms",
                AttributeValue::N(expires_at_ms.to_string()),
            )
            .item(
                "ttl_epoch",
                AttributeValue::N(unix_seconds().saturating_add(300).to_string()),
            )
            // Expired blocks are reusable. Active blocks remain owned by their
            // current node until that owner loses renewal or releases them.
            .condition_expression("attribute_not_exists(pk) OR expires_at_ms < :now")
            .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(ref error) if is_conditional_check_failed(error) => Ok(false),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "failed to acquire DSQL connection slot block in {}",
                    self.table_name
                )
            }),
        }
    }

    fn start_renewer(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(RENEW_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(error) = manager.renew_owned_blocks().await {
                    tracing::warn!(error = %error, "failed to renew DSQL connection slot blocks");
                }
            }
        });
        if let Ok(mut renewer) = self.renewer.lock() {
            *renewer = Some(handle);
        } else {
            handle.abort();
        }
    }

    async fn renew_owned_blocks(&self) -> Result<()> {
        let blocks = self
            .owned_blocks
            .read()
            .await
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for block_id in blocks {
            if let Err(error) = self.renew_block(block_id).await {
                if is_conditional_check_anyhow(&error) {
                    // Another node owns the block now. Reduce local capacity
                    // and let existing in-flight connections age out naturally.
                    self.handle_lost_block(block_id).await;
                } else {
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn renew_block(&self, block_id: u32) -> Result<()> {
        let expires_at_ms = unix_millis().saturating_add(BLOCK_LEASE_DURATION.as_millis() as u64);
        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(block_pk(block_id)))
            .update_expression("SET expires_at_ms = :expires, ttl_epoch = :ttl")
            .condition_expression("owner_id = :owner")
            .expression_attribute_values(":owner", AttributeValue::S(self.owner_id.clone()))
            .expression_attribute_values(":expires", AttributeValue::N(expires_at_ms.to_string()))
            .expression_attribute_values(
                ":ttl",
                AttributeValue::N(unix_seconds().saturating_add(300).to_string()),
            )
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to renew DSQL connection slot block {block_id} in {}",
                    self.table_name
                )
            })?;
        Ok(())
    }

    async fn handle_lost_block(&self, block_id: u32) {
        let mut owned = self.owned_blocks.write().await;
        if owned.remove(&block_id) {
            // If used_slots becomes greater than total_slots, `has_budget`
            // becomes false and refillers stop creating new connections until
            // returns retire enough old connections or a fresh block is acquired.
            self.total_slots
                .fetch_sub(SLOT_BLOCK_SIZE, Ordering::AcqRel);
            metrics::record_dsql_slot_block_lost();
            metrics::set_dsql_slot_blocks_owned(owned.len());
        }
    }
}

impl Drop for SlotBlockManager {
    fn drop(&mut self) {
        if let Ok(mut renewer) = self.renewer.lock()
            && let Some(handle) = renewer.take()
        {
            handle.abort();
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SlotReservation;

fn block_pk(block_id: u32) -> String {
    format!("block#{block_id}")
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// AWS SDK wraps the service error inside `SdkError` whose top-level Display
/// is just "service error". The actual exception name lives in the cause chain.
fn is_conditional_check_failed<E: std::fmt::Display + std::error::Error>(error: &E) -> bool {
    if error.to_string().contains("ConditionalCheckFailed") {
        return true;
    }
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if cause.to_string().contains("ConditionalCheckFailed") {
            return true;
        }
        source = cause.source();
    }
    false
}

/// Variant for anyhow errors which expose their own chain iterator.
fn is_conditional_check_anyhow(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("ConditionalCheckFailed"))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn slot_budget_enforcement(blocks in 0usize..10, requested in 0usize..600) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let manager = SlotBlockManager::local_for_tests(blocks * SLOT_BLOCK_SIZE);
                let mut acquired = 0usize;
                for _ in 0..requested {
                    if manager.acquire_slot().await.is_ok() {
                        acquired += 1;
                    } else {
                        break;
                    }
                }

                let limit = blocks * SLOT_BLOCK_SIZE;
                prop_assert!(acquired <= limit);
                prop_assert_eq!(manager.used_slots_for_tests(), acquired);
                prop_assert_eq!(manager.has_budget(), acquired < limit);
                for _ in 0..acquired {
                    manager.release_slot();
                }
                prop_assert_eq!(manager.used_slots_for_tests(), 0);
                prop_assert_eq!(manager.total_slots_for_tests(), limit);
                Ok(())
            })?;
        }
    }
}
