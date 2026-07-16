//! Durable namespace Workflow Rule operations for the DSQL run repository.
//!
//! Rules are opaque transport-neutral records keyed by `(namespace_id, rule_id)`. Create performs
//! duplicate detection, capacity eviction, and insertion in one database transaction so concurrent
//! API calls cannot overfill the namespace or silently replace a rule.

use anyhow::{Result, anyhow};
use sqlx::{Connection, Row};
use tokeira_types::{NamespaceId, WorkflowRuleRecord};

use crate::{DbClass, WorkflowRuleCreateResult, WorkflowRuleDeleteResult};

use super::{DsqlRunRepository, codec};

impl DsqlRunRepository {
    pub(super) async fn do_create_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule: WorkflowRuleRecord,
        max_rules: usize,
    ) -> Result<WorkflowRuleCreateResult> {
        let max_rules = i64::try_from(max_rules)
            .map_err(|_| anyhow!("workflow rule limit {max_rules} exceeds i64 range"))?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;

        let duplicate =
            sqlx::query("SELECT 1 FROM workflow_rules WHERE namespace_id = $1 AND rule_id = $2")
                .bind(namespace_id.0)
                .bind(&rule.id)
                .fetch_optional(&mut *tx)
                .await?;
        if duplicate.is_some() {
            tx.rollback().await?;
            return Ok(WorkflowRuleCreateResult::AlreadyExists);
        }

        let count: i64 = sqlx::query(
            "SELECT COUNT(*) AS rule_count FROM workflow_rules WHERE namespace_id = $1",
        )
        .bind(namespace_id.0)
        .fetch_one(&mut *tx)
        .await?
        .try_get("rule_count")?;
        if count >= max_rules {
            let eviction = sqlx::query(
                "SELECT rule_id FROM workflow_rules
                 WHERE namespace_id = $1 AND expiration_time IS NOT NULL
                 ORDER BY expiration_time ASC, rule_id ASC
                 LIMIT 1",
            )
            .bind(namespace_id.0)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(eviction) = eviction {
                let eviction_id: String = eviction.try_get("rule_id")?;
                sqlx::query("DELETE FROM workflow_rules WHERE namespace_id = $1 AND rule_id = $2")
                    .bind(namespace_id.0)
                    .bind(eviction_id)
                    .execute(&mut *tx)
                    .await?;
                if count > max_rules {
                    tx.rollback().await?;
                    return Ok(WorkflowRuleCreateResult::LimitExceeded);
                }
            } else {
                tx.rollback().await?;
                return Ok(WorkflowRuleCreateResult::LimitExceeded);
            }
        }
        if max_rules == 0 {
            tx.rollback().await?;
            return Ok(WorkflowRuleCreateResult::LimitExceeded);
        }

        let record_data = codec::encode_workflow_rule(&rule)?;
        let write = sqlx::query(
            "INSERT INTO workflow_rules
             (namespace_id, rule_id, expiration_time, record_data)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(namespace_id.0)
        .bind(&rule.id)
        .bind(rule.expiration_time)
        .bind(record_data)
        .execute(&mut *tx)
        .await;
        if let Err(error) = write {
            if Self::is_serialization_failure(&error) {
                tx.rollback().await?;
                return Err(anyhow!("workflow rule create serialization conflict"));
            }
            return Err(error.into());
        }
        tx.commit().await?;
        Ok(WorkflowRuleCreateResult::Created)
    }

    pub(super) async fn do_get_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<Option<WorkflowRuleRecord>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let row = sqlx::query(
            "SELECT record_data FROM workflow_rules WHERE namespace_id = $1 AND rule_id = $2",
        )
        .bind(namespace_id.0)
        .bind(rule_id)
        .fetch_optional(permit.connection()?)
        .await?;
        row.map(|row| codec::decode_workflow_rule(&row.try_get::<Vec<u8>, _>("record_data")?))
            .transpose()
    }

    pub(super) async fn do_delete_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<WorkflowRuleDeleteResult> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let result =
            sqlx::query("DELETE FROM workflow_rules WHERE namespace_id = $1 AND rule_id = $2")
                .bind(namespace_id.0)
                .bind(rule_id)
                .execute(permit.connection()?)
                .await?;
        Ok(if result.rows_affected() == 0 {
            WorkflowRuleDeleteResult::NotFound
        } else {
            WorkflowRuleDeleteResult::Deleted
        })
    }

    pub(super) async fn do_list_workflow_rules(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRuleRecord>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT record_data FROM workflow_rules
             WHERE namespace_id = $1 ORDER BY rule_id ASC",
        )
        .bind(namespace_id.0)
        .fetch_all(permit.connection()?)
        .await?;
        rows.into_iter()
            .map(|row| codec::decode_workflow_rule(&row.try_get::<Vec<u8>, _>("record_data")?))
            .collect()
    }
}
