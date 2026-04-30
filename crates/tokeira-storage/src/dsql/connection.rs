use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use aurora_dsql_sqlx_connector::DsqlConnectOptions;
use sqlx::{PgConnection, PgPool, Pool, Postgres, pool::PoolConnection};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::{ConnectionDirector, DbClass, metrics};

use super::{
    DsqlAuthConfig, DsqlPoolConfig, Reservoir, ReservoirEntry, TokenBucketRateLimiter,
    config::ReservoirConfig,
};

#[derive(Clone, Debug)]
pub struct DsqlConnector {
    pool: PgPool,
}

impl DsqlConnector {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(
        auth: &DsqlAuthConfig,
        config: &DsqlPoolConfig,
        role: super::DsqlRole,
    ) -> Result<Self> {
        let user = match role {
            super::DsqlRole::Admin => "admin",
            super::DsqlRole::Runtime => "admin", // same user, different IAM role via assume-role
            super::DsqlRole::Readonly => "admin",
        };
        let connection_string = auth.connection_string(user, "postgres")?;
        let options = DsqlConnectOptions::from_connection_string(&connection_string)?;
        let pool =
            aurora_dsql_sqlx_connector::pool::connect_with(&options, auth.pool_options(config))
                .await?;
        Ok(Self { pool })
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Postgres>> {
        self.pool.acquire().await.map_err(Into::into)
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

#[derive(Debug)]
pub struct ClassBudgets {
    budgets: RwLock<HashMap<DbClass, Arc<Semaphore>>>,
    totals: RwLock<HashMap<DbClass, usize>>,
    total_budget: AtomicUsize,
}

impl ClassBudgets {
    pub fn new(allocations: &HashMap<DbClass, usize>) -> Result<Self> {
        let total_budget = validate_allocations(allocations)?;
        let budgets = build_budget_map(allocations);
        let class_budgets = Self {
            budgets: RwLock::new(budgets),
            totals: RwLock::new(allocations.clone()),
            total_budget: AtomicUsize::new(total_budget),
        };
        Ok(class_budgets)
    }

    pub async fn acquire(&self, class: DbClass) -> Result<OwnedSemaphorePermit> {
        let semaphore = {
            let budgets = self.budgets.read().await;
            let Some(semaphore) = budgets.get(&class) else {
                bail!("missing budget for class {class:?}");
            };
            semaphore.clone()
        };
        self.record_class_metric(class, &semaphore).await;
        semaphore.acquire_owned().await.map_err(Into::into)
    }

    pub async fn reconfigure(&self, allocations: &HashMap<DbClass, usize>) -> Result<()> {
        let total_budget = validate_allocations(allocations)?;
        let mut budgets = self.budgets.write().await;
        *budgets = build_budget_map(allocations);
        let mut totals = self.totals.write().await;
        *totals = allocations.clone();
        self.total_budget.store(total_budget, Ordering::Release);
        drop(totals);
        drop(budgets);
        self.record_metrics().await;
        Ok(())
    }

    pub fn total_budget(&self) -> usize {
        self.total_budget.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub async fn class_available(&self, class: DbClass) -> Option<usize> {
        let budgets = self.budgets.read().await;
        budgets.get(&class).map(|budget| budget.available_permits())
    }

    async fn record_metrics(&self) {
        let budgets = self.budgets.read().await;
        for (class, semaphore) in budgets.iter() {
            self.record_class_metric(*class, semaphore).await;
        }
    }

    async fn record_class_metric(&self, class: DbClass, semaphore: &Semaphore) {
        let totals = self.totals.read().await;
        let total = totals.get(&class).copied().unwrap_or_default();
        let available = semaphore.available_permits();
        let in_use = total.saturating_sub(available);
        metrics::record_dsql_pool_class_budget(db_class_label(class), total, in_use, 0);
    }
}

#[derive(Debug)]
pub struct DsqlConnectionDirector {
    reservoir: Reservoir,
    class_budgets: ClassBudgets,
}

impl DsqlConnectionDirector {
    pub async fn start(config: DsqlPoolConfig, connector: DsqlConnector) -> Result<Self> {
        let rate_limiter =
            TokenBucketRateLimiter::new(config.connection_rate_per_second, config.burst_capacity);
        let reservoir = Reservoir::start(config.reservoir.clone(), connector, rate_limiter).await?;
        let class_budgets = ClassBudgets::new(&default_allocations(&config.reservoir))?;
        Ok(Self {
            reservoir,
            class_budgets,
        })
    }

    pub async fn reconfigure_class_budgets(
        &self,
        allocations: &HashMap<DbClass, usize>,
    ) -> Result<()> {
        self.class_budgets.reconfigure(allocations).await
    }
}

#[async_trait]
impl ConnectionDirector for DsqlConnectionDirector {
    type Permit = DsqlPermit;

    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
        let started = Instant::now();
        let class_guard = self.class_budgets.acquire(class).await?;
        let entry = self.reservoir.checkout().await?;
        metrics::record_dsql_pool_checkout_duration(db_class_label(class), started.elapsed());
        Ok(DsqlPermit::new(
            class,
            entry,
            class_guard,
            self.reservoir.return_sender(),
        ))
    }
}

#[derive(Debug)]
pub struct DsqlPermit {
    pub class: DbClass,
    connection: Option<PoolConnection<Postgres>>,
    created_at: std::time::Instant,
    max_lifetime: std::time::Duration,
    _class_guard: OwnedSemaphorePermit,
    reservoir_return: tokio::sync::mpsc::UnboundedSender<ReservoirEntry>,
}

impl DsqlPermit {
    fn new(
        class: DbClass,
        entry: ReservoirEntry,
        class_guard: OwnedSemaphorePermit,
        reservoir_return: tokio::sync::mpsc::UnboundedSender<ReservoirEntry>,
    ) -> Self {
        Self {
            class,
            connection: Some(entry.connection),
            created_at: entry.created_at,
            max_lifetime: entry.max_lifetime,
            _class_guard: class_guard,
            reservoir_return,
        }
    }

    pub fn connection(&mut self) -> Result<&mut PgConnection> {
        let Some(connection) = self.connection.as_mut() else {
            bail!("DSQL permit connection already returned");
        };
        Ok(&mut *connection)
    }
}

impl Drop for DsqlPermit {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            if self.created_at.elapsed() <= self.max_lifetime {
                let _ = self.reservoir_return.send(ReservoirEntry {
                    connection,
                    created_at: self.created_at,
                    max_lifetime: self.max_lifetime,
                });
            } else {
                metrics::record_dsql_pool_connection_retired("expired");
            }
        }
    }
}

fn validate_allocations(allocations: &HashMap<DbClass, usize>) -> Result<usize> {
    let total_budget: usize = allocations.values().sum();
    if total_budget == 0 {
        bail!("class budget total must be greater than zero");
    }
    for class in all_classes() {
        if allocations.get(&class).copied().unwrap_or_default() == 0 {
            bail!("missing positive budget for class {class:?}");
        }
    }
    Ok(total_budget)
}

fn build_budget_map(allocations: &HashMap<DbClass, usize>) -> HashMap<DbClass, Arc<Semaphore>> {
    allocations
        .iter()
        .map(|(class, permits)| (*class, Arc::new(Semaphore::new(*permits))))
        .collect()
}

/// Compute default per-class allocations that sum exactly to `target_ready`.
///
/// Priority ratios: Control 10%, Commit 50%, Read 20%, Projection 10%,
/// Maintenance gets the remainder. Each class gets at least 1 permit.
fn default_allocations(config: &ReservoirConfig) -> HashMap<DbClass, usize> {
    let total = config.target_ready.max(5);
    let control = (total / 10).max(1);
    let commit = (total / 2).max(1);
    let read = (total / 5).max(1);
    let projection = (total / 10).max(1);
    let allocated = control + commit + read + projection;
    // Maintenance absorbs the remainder so the sum equals total.
    // If the fixed allocations already exceed total (small target_ready),
    // maintenance gets 1 and the total is allowed to exceed target_ready
    // by a small amount — the reservoir will simply have more permits
    // than ready connections, causing natural backpressure.
    let maintenance = if allocated >= total {
        1
    } else {
        total - allocated
    };

    let mut allocations = HashMap::new();
    allocations.insert(DbClass::Control, control);
    allocations.insert(DbClass::Commit, commit);
    allocations.insert(DbClass::Read, read);
    allocations.insert(DbClass::Projection, projection);
    allocations.insert(DbClass::Maintenance, maintenance);
    allocations
}

fn all_classes() -> [DbClass; 5] {
    [
        DbClass::Control,
        DbClass::Commit,
        DbClass::Read,
        DbClass::Projection,
        DbClass::Maintenance,
    ]
}

fn db_class_label(class: DbClass) -> &'static str {
    match class {
        DbClass::Control => "control",
        DbClass::Commit => "commit",
        DbClass::Read => "read",
        DbClass::Projection => "projection",
        DbClass::Maintenance => "maintenance",
    }
}

#[cfg(test)]
fn test_allocations(commit: usize) -> HashMap<DbClass, usize> {
    let mut allocations = HashMap::new();
    allocations.insert(DbClass::Control, 1);
    allocations.insert(DbClass::Commit, commit);
    allocations.insert(DbClass::Read, 1);
    allocations.insert(DbClass::Projection, 1);
    allocations.insert(DbClass::Maintenance, 1);
    allocations
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[tokio::test]
    async fn class_budget_acquire_and_release() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let permit = budgets.acquire(DbClass::Commit).await.unwrap();
        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(0));
        drop(permit);
        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(1));
    }

    #[tokio::test]
    async fn class_budget_reconfiguration_replaces_allocations() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        budgets.reconfigure(&test_allocations(3)).await.unwrap();
        assert_eq!(budgets.total_budget(), 7);
        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(3));
    }

    proptest! {
        #[test]
        fn class_budget_sum_invariant(commit in 1usize..128) {
            let allocations = test_allocations(commit);
            let expected: usize = allocations.values().sum();
            let budgets = ClassBudgets::new(&allocations).unwrap();
            prop_assert_eq!(budgets.total_budget(), expected);
        }
    }
}
