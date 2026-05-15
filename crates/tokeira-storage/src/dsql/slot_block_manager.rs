//! DynamoDB-backed connection slot block ownership.

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

const MAX_BLOCK_PROBES: u32 = 1024;
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
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .build();
        Arc::new(Self {
            client: Client::from_conf(config),
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

    async fn ensure_capacity(&self, requested_slots: usize) -> Result<()> {
        while self.total_slots.load(Ordering::Acquire) < requested_slots
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
            return Ok(());
        }
        Ok(())
    }

    async fn try_acquire_any_block(&self) -> Result<Option<u32>> {
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
            .condition_expression("attribute_not_exists(pk) OR expires_at_ms < :now")
            .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_message(&error.to_string()) => Ok(false),
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
                if is_conditional_check_message(&error.to_string()) {
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
            self.total_slots
                .fetch_sub(SLOT_BLOCK_SIZE, Ordering::AcqRel);
            metrics::record_dsql_slot_block_lost();
            metrics::set_dsql_slot_blocks_owned(owned.len());
        }
    }
}

impl Drop for SlotBlockManager {
    fn drop(&mut self) {
        if let Ok(mut renewer) = self.renewer.lock() {
            if let Some(handle) = renewer.take() {
                handle.abort();
            }
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

fn is_conditional_check_message(message: &str) -> bool {
    message.contains("ConditionalCheckFailed")
}
