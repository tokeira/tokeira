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

#[async_trait]
impl LeaseRepository for DsqlRunRepository {
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

            let insert_result = sqlx::query(
                "INSERT INTO shard_lease (shard_id, owner, epoch, lease_expiry, node_endpoint)
             VALUES ($1, $2, 1, $3, $4)
             ON CONFLICT (shard_id) DO NOTHING",
            )
            .bind(shard_uuid)
            .bind(&owner)
            .bind(new_expiry)
            .bind(&node_endpoint)
            .execute(&mut *tx)
            .await;

            let insert_rows_affected = match insert_result {
                Ok(result) => result.rows_affected(),
                Err(err) if Self::is_serialization_failure(&err) => {
                    tx.rollback().await?;
                    return Err(anyhow!(err))
                        .context("DSQL serialization conflict during lease acquire");
                }
                Err(err) => {
                    tx.rollback().await?;
                    return Err(err).context("failed to insert shard lease");
                }
            };

            let mut update_rows_affected = 0;
            if insert_rows_affected == 0 {
                let update_result = sqlx::query(
                    "UPDATE shard_lease
                 SET owner = $2,
                     epoch = CASE
                         WHEN owner = $2 AND lease_expiry > $4 THEN epoch
                         ELSE epoch + 1
                     END,
                     lease_expiry = $3,
                     node_endpoint = $5
                 WHERE shard_id = $1
                   AND (owner = $2 OR owner IS NULL OR lease_expiry <= $4)",
                )
                .bind(shard_uuid)
                .bind(&owner)
                .bind(new_expiry)
                .bind(app_now)
                .bind(&node_endpoint)
                .execute(&mut *tx)
                .await;

                update_rows_affected = match update_result {
                    Ok(result) => result.rows_affected(),
                    Err(err) if Self::is_serialization_failure(&err) => {
                        tx.rollback().await?;
                        return Err(anyhow!(err))
                            .context("DSQL serialization conflict during lease acquire");
                    }
                    Err(err) => {
                        tx.rollback().await?;
                        return Err(err).context("failed to update shard lease");
                    }
                };
            }

            let outcome = if insert_rows_affected == 1 || update_rows_affected == 1 {
                let (epoch,) = sqlx::query_as::<_, (i64,)>(
                    "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                )
                .bind(shard_uuid)
                .fetch_one(&mut *tx)
                .await
                .context("failed to read acquired shard lease epoch")?;
                interpret_acquire(
                    insert_rows_affected,
                    update_rows_affected,
                    Some(epoch),
                    None,
                )?
            } else {
                let row = sqlx::query_as::<_, (Option<String>, i64)>(
                    "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
                )
                .bind(shard_uuid)
                .fetch_optional(&mut *tx)
                .await
                .context("failed to read rejected shard lease holder")?;
                let rejected_row = row.map(|(owner, epoch)| (owner.unwrap_or_default(), epoch));
                interpret_acquire(
                    insert_rows_affected,
                    update_rows_affected,
                    None,
                    rejected_row,
                )?
            };

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

            let row = sqlx::query_as::<_, (Option<String>, i64)>(
                "SELECT owner, epoch
             FROM shard_lease
             WHERE shard_id = $1
             FOR UPDATE",
            )
            .bind(shard_uuid)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read shard lease for renewal")?;

            let decision = decide_renew(
                row.as_ref()
                    .map(|(owner, epoch)| (owner.as_deref().unwrap_or(""), *epoch)),
                &owner,
                caller_epoch,
            )?;
            match decision {
                RenewDecision::Renew => {
                    let update_result = sqlx::query(
                        "UPDATE shard_lease
                     SET lease_expiry = $1,
                         node_endpoint = $3
                     WHERE shard_id = $2",
                    )
                    .bind(new_expiry)
                    .bind(shard_uuid)
                    .bind(&node_endpoint)
                    .execute(&mut *tx)
                    .await;

                    match update_result {
                        Ok(_) => match tx.commit().await {
                            Ok(()) => Ok(LeaseOutcome::Renewed { epoch }),
                            Err(err) if Self::is_serialization_failure(&err) => Err(anyhow!(err))
                                .context("DSQL serialization conflict during lease renewal"),
                            Err(err) => Err(err).context("failed to commit shard lease renewal"),
                        },
                        Err(err) if Self::is_serialization_failure(&err) => {
                            tx.rollback().await?;
                            Err(anyhow!(err))
                                .context("DSQL serialization conflict during lease renewal")
                        }
                        Err(err) => {
                            tx.rollback().await?;
                            Err(err).context("failed to update shard lease renewal")
                        }
                    }
                }
                RenewDecision::Reject {
                    current_owner,
                    current_epoch,
                } => {
                    tx.rollback().await?;
                    Ok(LeaseOutcome::Rejected {
                        current_owner,
                        current_epoch,
                    })
                }
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
            let row = sqlx::query_as::<_, (i64,)>(
                "UPDATE shard_lease
             SET owner = NULL,
                 epoch = epoch + 1,
                 node_endpoint = NULL
             WHERE shard_id = $1 AND owner = $2 AND epoch = $3
             RETURNING epoch",
            )
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
