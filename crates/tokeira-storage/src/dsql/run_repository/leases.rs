//! Bundle lease acquire/renew/relinquish, LeaseRepository impl, and ControlRepository impl.

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Connection;
use time::OffsetDateTime;
use tokeira_types::{GenerationCounter, ShardEpoch, ShardId};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    BudgetAllocationResult, BundleLease, ControlRepository, DbClass, GenerationAdvanceResult,
    LeaseOutcome, LeaseRepository,
};

use super::{DsqlRunRepository, epoch_from_sql, epoch_to_sql};
use crate::dsql::convert;

// The lease's statements (`bundle-lease-fence`). Only a change of ownership
// writes the epoch, which `idx_shard_lease_epoch` makes a key column: an
// acquisition's insert at epoch 1, a takeover of a released or expired lease,
// and a release. A refresh and a renewal write the expiry and the endpoint
// alone, and nothing reads the lease `FOR UPDATE`.

/// Acquisition's insert of a new lease, at epoch 1.
pub(super) const INSERT_LEASE: &str =
    "INSERT INTO shard_lease (shard_id, owner, epoch, lease_expiry, node_endpoint)
     VALUES ($1, $2, 1, $3, $4)
     ON CONFLICT (shard_id) DO NOTHING
     RETURNING epoch";

/// Acquisition by the owner of an unexpired lease, which keeps the epoch.
pub(super) const REFRESH_LEASE: &str = "UPDATE shard_lease
     SET lease_expiry = $3, node_endpoint = $4
     WHERE shard_id = $1 AND owner = $2 AND lease_expiry > $5
     RETURNING epoch";

/// Acquisition of a released or expired lease, the owner's own included, which
/// advances the epoch.
pub(super) const TAKE_OVER_LEASE: &str = "UPDATE shard_lease
     SET owner = $2, epoch = epoch + 1, lease_expiry = $3, node_endpoint = $4
     WHERE shard_id = $1 AND (owner IS NULL OR lease_expiry <= $5)
     RETURNING epoch";

/// The owner's renewal at its epoch.
pub(super) const RENEW_LEASE: &str = "UPDATE shard_lease
     SET lease_expiry = $3, node_endpoint = $4
     WHERE shard_id = $1 AND owner = $2 AND epoch = $5";

/// The owner's release, which clears the owner and advances the epoch.
pub(super) const RELEASE_LEASE: &str = "UPDATE shard_lease
     SET owner = NULL, epoch = epoch + 1, node_endpoint = NULL
     WHERE shard_id = $1 AND owner = $2 AND epoch = $3
     RETURNING epoch";

/// An acquisition statement's failure, as a serialization conflict or with the
/// statement's own context.
fn acquire_error(err: sqlx::Error, failure: &'static str) -> anyhow::Error {
    if DsqlRunRepository::is_serialization_failure(&err) {
        anyhow!(err).context("DSQL serialization conflict during lease acquire")
    } else {
        anyhow::Error::from(err).context(failure)
    }
}

#[async_trait]
impl LeaseRepository for DsqlRunRepository {
    fn bundle_lease_duration(&self) -> Option<time::Duration> {
        Some(self.lease_duration)
    }

    #[instrument(name = "dsql.try_acquire_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner))]
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        record_dsql_operation!(self, "try_acquire_bundle", Some(bundle), {
            let shard_uuid = Self::shard_id_to_uuid(bundle);
            let app_now = OffsetDateTime::now_utc();
            let new_expiry = app_now + self.lease_duration;

            let mut permit = self.director.acquire(DbClass::Control).await?;
            let mut tx = permit.connection()?.begin().await?;

            // Each statement writes the epoch only when ownership changes: the
            // owner's refresh conflicts with none of its fenced writes, and a
            // takeover with every one (`bundle-lease-fence`).
            let inserted = match sqlx::query_as::<_, (i64,)>(INSERT_LEASE)
                .bind(shard_uuid)
                .bind(&owner)
                .bind(new_expiry)
                .bind(&node_endpoint)
                .fetch_optional(&mut *tx)
                .await
            {
                Ok(row) => row,
                Err(err) => {
                    tx.rollback().await?;
                    return Err(acquire_error(err, "failed to insert shard lease"));
                }
            };
            let insert_rows_affected = u64::from(inserted.is_some());

            let mut acquired = inserted;
            for (statement, failure) in [
                (REFRESH_LEASE, "failed to refresh shard lease"),
                (TAKE_OVER_LEASE, "failed to take over shard lease"),
            ] {
                if acquired.is_some() {
                    break;
                }
                acquired = match sqlx::query_as::<_, (i64,)>(statement)
                    .bind(shard_uuid)
                    .bind(&owner)
                    .bind(new_expiry)
                    .bind(&node_endpoint)
                    .bind(app_now)
                    .fetch_optional(&mut *tx)
                    .await
                {
                    Ok(row) => row,
                    Err(err) => {
                        tx.rollback().await?;
                        return Err(acquire_error(err, failure));
                    }
                };
            }

            let outcome = match acquired {
                Some((epoch,)) => interpret_acquire(
                    insert_rows_affected,
                    1 - insert_rows_affected,
                    Some(epoch),
                    None,
                )?,
                None => {
                    let row = sqlx::query_as::<_, (Option<String>, i64)>(
                        "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
                    )
                    .bind(shard_uuid)
                    .fetch_optional(&mut *tx)
                    .await
                    .context("failed to read rejected shard lease holder")?;
                    let rejected_row = row.map(|(owner, epoch)| (owner.unwrap_or_default(), epoch));
                    interpret_acquire(0, 0, None, rejected_row)?
                }
            };

            #[cfg(all(test, feature = "dsql-integration"))]
            if matches!(outcome, LeaseOutcome::Acquired { .. }) {
                super::pause_point::reached(super::pause_point::PauseKey::Acquisition(bundle))
                    .await;
            }

            match outcome {
                LeaseOutcome::Acquired { .. } => match tx.commit().await {
                    Ok(()) => Ok(outcome),
                    Err(err) if Self::is_serialization_failure(&err) => Err(anyhow!(err))
                        .context("DSQL serialization conflict during lease acquire"),
                    Err(err) => Err(err).context("failed to commit shard lease acquire"),
                },
                LeaseOutcome::Rejected { .. } => {
                    tx.rollback().await?;
                    Ok(outcome)
                }
                LeaseOutcome::Renewed { .. } => {
                    tx.rollback().await?;
                    bail!("acquire interpretation unexpectedly returned renewed outcome");
                }
            }
        })
    }

    #[instrument(name = "dsql.renew_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner, epoch = epoch.0))]
    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        record_dsql_operation!(self, "renew_bundle", Some(bundle), {
            let caller_epoch = epoch_to_sql(epoch)?;
            let shard_uuid = Self::shard_id_to_uuid(bundle);
            let new_expiry = OffsetDateTime::now_utc() + self.lease_duration;

            let mut permit = self.director.acquire(DbClass::Control).await?;
            let mut tx = permit.connection()?.begin().await?;

            // One conditional update, which writes no key column and reads
            // nothing `FOR UPDATE`, so it aborts none of the owner's fenced
            // writes. A takeover updates the same row, so DSQL commits at most
            // one of the two (`bundle-lease-fence`).
            let renewed = match sqlx::query(RENEW_LEASE)
                .bind(shard_uuid)
                .bind(&owner)
                .bind(new_expiry)
                .bind(&node_endpoint)
                .bind(caller_epoch)
                .execute(&mut *tx)
                .await
            {
                Ok(result) => result.rows_affected(),
                Err(err) if Self::is_serialization_failure(&err) => {
                    tx.rollback().await?;
                    return Err(anyhow!(err))
                        .context("DSQL serialization conflict during lease renewal");
                }
                Err(err) => {
                    tx.rollback().await?;
                    return Err(err).context("failed to update shard lease renewal");
                }
            };
            if renewed == 1 {
                return match tx.commit().await {
                    Ok(()) => Ok(LeaseOutcome::Renewed { epoch }),
                    Err(err) if Self::is_serialization_failure(&err) => Err(anyhow!(err))
                        .context("DSQL serialization conflict during lease renewal"),
                    Err(err) => Err(err).context("failed to commit shard lease renewal"),
                };
            }

            let row = sqlx::query_as::<_, (Option<String>, i64)>(
                "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
            )
            .bind(shard_uuid)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read shard lease for renewal")?;
            tx.rollback().await?;
            match decide_renew(
                row.as_ref()
                    .map(|(owner, epoch)| (owner.as_deref().unwrap_or(""), *epoch)),
                &owner,
                caller_epoch,
            )? {
                RenewDecision::Reject {
                    current_owner,
                    current_epoch,
                } => Ok(LeaseOutcome::Rejected {
                    current_owner,
                    current_epoch,
                }),
                RenewDecision::Renew => bail!(
                    "the renewal of {bundle:?} changed no row, yet the lease names its owner at its epoch"
                ),
            }
        })
    }

    #[instrument(name = "dsql.list_bundle_leases", skip(self))]
    async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>> {
        record_dsql_operation!(self, "list_bundle_leases", None, {
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let rows =
                sqlx::query_as::<_, (Uuid, Option<String>, i64, OffsetDateTime, Option<String>)>(
                    "SELECT shard_id, owner, epoch, lease_expiry, node_endpoint FROM shard_lease",
                )
                .fetch_all(permit.connection()?)
                .await
                .context("failed to list shard leases")?;

            let mut leases = Vec::with_capacity(rows.len());
            for (shard_uuid, owner, epoch, lease_until, endpoint) in rows {
                let bundle_id = match Self::shard_id_from_uuid(shard_uuid) {
                    Ok(id) => id,
                    Err(_) => {
                        // Row written by a previous code version with a different
                        // UUID encoding. Skip it — try_acquire_bundle will overwrite
                        // it via the ON CONFLICT + expiry-based UPDATE path.
                        tracing::debug!(
                            %shard_uuid,
                            "skipping shard_lease row with unrecognized UUID encoding"
                        );
                        continue;
                    }
                };
                leases.push(BundleLease {
                    bundle_id,
                    owner_node_id: owner,
                    epoch: epoch_from_sql(epoch)?,
                    lease_until,
                    node_endpoint: endpoint,
                });
            }
            Ok(leases)
        })
    }

    #[instrument(name = "dsql.relinquish_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner, epoch = epoch.0))]
    async fn relinquish_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        record_dsql_operation!(self, "relinquish_bundle", Some(bundle), {
            let shard_uuid = Self::shard_id_to_uuid(bundle);
            let caller_epoch = epoch_to_sql(epoch)?;
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let mut tx = permit.connection()?.begin().await?;
            let row = sqlx::query_as::<_, (i64,)>(RELEASE_LEASE)
                .bind(shard_uuid)
                .bind(&owner)
                .bind(caller_epoch)
                .fetch_optional(&mut *tx)
                .await
                .context("failed to relinquish shard lease")?;

            if let Some((new_epoch,)) = row {
                tx.commit().await?;
                return Ok(LeaseOutcome::Acquired {
                    epoch: epoch_from_sql(new_epoch)?,
                });
            }

            let current = sqlx::query_as::<_, (Option<String>, i64)>(
                "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
            )
            .bind(shard_uuid)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read rejected shard lease holder after relinquish")?;
            tx.rollback().await?;
            let (current_owner, current_epoch) = current.unwrap_or((None, 0));
            Ok(LeaseOutcome::Rejected {
                current_owner: current_owner.unwrap_or_default(),
                current_epoch: epoch_from_sql(current_epoch)?,
            })
        })
    }
}

#[async_trait]
impl ControlRepository for DsqlRunRepository {
    #[instrument(name = "dsql.advance_generation", skip(self), fields(expected = expected.0))]
    async fn advance_generation(
        &self,
        expected: GenerationCounter,
    ) -> Result<GenerationAdvanceResult> {
        record_dsql_operation!(self, "advance_generation", None, {
            let expected = convert::i64_from_u64(expected.0, "routing_generation.generation")?;
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let row = sqlx::query_as::<_, (i64,)>(
                "UPDATE routing_generation
             SET generation = generation + 1,
                 updated_at = now()
             WHERE id = 1 AND generation = $1
             RETURNING generation",
            )
            .bind(expected)
            .fetch_optional(permit.connection()?)
            .await
            .context("failed to advance routing generation")?;

            match row {
                Some((generation,)) => Ok(GenerationAdvanceResult::Advanced(GenerationCounter(
                    convert::u64_from_i64(generation, "routing_generation.generation")?,
                ))),
                None => Ok(GenerationAdvanceResult::Conflict(
                    self.current_generation().await?,
                )),
            }
        })
    }

    #[instrument(name = "dsql.current_generation", skip(self))]
    async fn current_generation(&self) -> Result<GenerationCounter> {
        record_dsql_operation!(self, "current_generation", None, {
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let (generation,) = sqlx::query_as::<_, (i64,)>(
                "SELECT generation FROM routing_generation WHERE id = 1",
            )
            .fetch_one(permit.connection()?)
            .await
            .context("failed to read routing generation")?;
            Ok(GenerationCounter(convert::u64_from_i64(
                generation,
                "routing_generation.generation",
            )?))
        })
    }

    #[instrument(name = "dsql.allocate_budget", skip(self), fields(expected_version, allocator_id = %allocator_id))]
    async fn allocate_budget(
        &self,
        expected_version: u64,
        allocator_id: Uuid,
        rate_budget: f64,
        capacity_budget: u64,
    ) -> Result<BudgetAllocationResult> {
        record_dsql_operation!(self, "allocate_budget", None, {
            let expected_version =
                convert::i64_from_u64(expected_version, "budget_allocation.version")?;
            let capacity_budget =
                convert::i64_from_u64(capacity_budget, "budget_allocation.capacity_budget")?;
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let row = sqlx::query_as::<_, (i64,)>(
                "UPDATE budget_allocation
             SET version = version + 1,
                 allocator_id = $2,
                 allocated_at = now(),
                 rate_budget = $3,
                 capacity_budget = $4
             WHERE id = 1 AND version = $1
             RETURNING version",
            )
            .bind(expected_version)
            .bind(allocator_id)
            .bind(rate_budget)
            .bind(capacity_budget)
            .fetch_optional(permit.connection()?)
            .await
            .context("failed to allocate connection budget")?;

            match row {
                Some((version,)) => Ok(BudgetAllocationResult::Allocated {
                    version: convert::u64_from_i64(version, "budget_allocation.version")?,
                }),
                None => Ok(BudgetAllocationResult::Conflict {
                    current_version: self.current_budget_version().await?,
                }),
            }
        })
    }

    #[instrument(name = "dsql.current_budget_version", skip(self))]
    async fn current_budget_version(&self) -> Result<u64> {
        record_dsql_operation!(self, "current_budget_version", None, {
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let (version,) =
                sqlx::query_as::<_, (i64,)>("SELECT version FROM budget_allocation WHERE id = 1")
                    .fetch_one(permit.connection()?)
                    .await
                    .context("failed to read budget allocation version")?;
            convert::u64_from_i64(version, "budget_allocation.version")
        })
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RenewDecision {
    Renew,
    Reject {
        current_owner: String,
        current_epoch: ShardEpoch,
    },
}

pub(super) fn interpret_acquire(
    insert_rows_affected: u64,
    update_rows_affected: u64,
    acquired_epoch: Option<i64>,
    rejected_row: Option<(String, i64)>,
) -> Result<LeaseOutcome> {
    // The lease acquire SQL is split into INSERT and UPDATE for portability.
    // These row-count combinations are the contract that maps SQL effects back
    // into the storage trait's semantic outcome.
    match (insert_rows_affected, update_rows_affected) {
        (1, 0) => {
            let epoch = acquired_epoch
                .map(epoch_from_sql)
                .transpose()?
                .ok_or_else(|| anyhow!("acquired shard lease epoch was not returned"))?;
            if epoch != ShardEpoch(1) {
                bail!("new shard lease returned unexpected epoch {}", epoch.0);
            }
            Ok(LeaseOutcome::Acquired { epoch })
        }
        (0, 1) => {
            let epoch = acquired_epoch
                .map(epoch_from_sql)
                .transpose()?
                .ok_or_else(|| anyhow!("updated shard lease epoch was not returned"))?;
            Ok(LeaseOutcome::Acquired { epoch })
        }
        (0, 0) => {
            let (current_owner, current_epoch) =
                rejected_row.ok_or_else(|| anyhow!("rejected shard lease holder was not found"))?;
            Ok(LeaseOutcome::Rejected {
                current_owner,
                current_epoch: epoch_from_sql(current_epoch)?,
            })
        }
        _ => bail!(
            "invalid shard lease acquire row counts: insert={insert_rows_affected}, update={update_rows_affected}"
        ),
    }
}

pub(super) fn decide_renew(
    row: Option<(&str, i64)>,
    caller_owner: &str,
    caller_epoch: i64,
) -> Result<RenewDecision> {
    // Renewal is stricter than acquire: only the current owner at the exact
    // epoch can extend a lease. Expired-takeover behavior belongs to acquire.
    let Some((current_owner, current_epoch)) = row else {
        return Ok(RenewDecision::Reject {
            current_owner: String::new(),
            current_epoch: ShardEpoch::ZERO,
        });
    };
    if current_owner == caller_owner && current_epoch == caller_epoch {
        return Ok(RenewDecision::Renew);
    }
    Ok(RenewDecision::Reject {
        current_owner: current_owner.to_owned(),
        current_epoch: epoch_from_sql(current_epoch)?,
    })
}

#[cfg(test)]
mod tests {
    use super::{INSERT_LEASE, REFRESH_LEASE, RELEASE_LEASE, RENEW_LEASE, TAKE_OVER_LEASE};

    /// The columns an update's `SET` clause writes.
    fn set_clause(statement: &str) -> &str {
        let set = statement.find("SET").expect("an update has a SET clause");
        let condition = statement
            .find("WHERE")
            .expect("an update has a WHERE clause");
        &statement[set..condition]
    }

    // Feature: bundle-lease-fence, Property 5: Only a change of ownership writes the epoch
    #[test]
    fn only_a_change_of_ownership_writes_the_epoch() {
        for statement in [REFRESH_LEASE, RENEW_LEASE] {
            assert!(
                !set_clause(statement).contains("epoch"),
                "a refresh or a renewal writes the epoch: {statement}"
            );
        }
        for statement in [TAKE_OVER_LEASE, RELEASE_LEASE] {
            assert!(
                set_clause(statement).contains("epoch = epoch + 1"),
                "a change of ownership leaves the epoch: {statement}"
            );
        }
        assert!(INSERT_LEASE.contains("VALUES ($1, $2, 1, $3, $4)"));
        assert!(REFRESH_LEASE.contains("owner = $2 AND lease_expiry > $5"));
        assert!(TAKE_OVER_LEASE.contains("owner IS NULL OR lease_expiry <= $5"));
        assert!(RENEW_LEASE.contains("owner = $2 AND epoch = $5"));
        for statement in [
            INSERT_LEASE,
            REFRESH_LEASE,
            TAKE_OVER_LEASE,
            RENEW_LEASE,
            RELEASE_LEASE,
        ] {
            assert!(
                !statement.contains("FOR UPDATE"),
                "a lease statement reads the row FOR UPDATE: {statement}"
            );
        }
    }
}
