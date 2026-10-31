//! Cleanup checks the exact generation on every page, including its last page.
//! Deleting the generation and creation claim is the final step, so a successor
//! can be restaged only after the old purger has lost authority over its keys.

use super::*;

impl DsqlRunRepository {
    pub(in crate::dsql::run_repository) async fn do_abandon_preparation(
        &self,
        token: PreparationToken,
        fence: WriteFence,
    ) -> Result<bool> {
        fence.ensure_home(token.home)?;
        {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            if let Some(lost) = Self::check_fence(&mut tx, &fence, token.target_run).await? {
                return Err(lost.into());
            }
            let Some(record) = optional_by_token(&mut tx, &token).await? else {
                return Ok(false);
            };
            if record.phase >= PUBLISHED {
                return Ok(false);
            }
            sqlx::query("UPDATE run_preparation SET phase=$2 WHERE preparation_id=$1")
                .bind(token.id)
                .bind(PURGING)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE run_bulk_write SET phase=$2 WHERE run_key=$1")
                .bind(token.target_run.0)
                .bind(crate::BulkWritePhase::Purging.to_db_smallint())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        for table in &super::super::bulk_write::PURGE_TABLES {
            loop {
                let mut permit = self.director.acquire(DbClass::Commit).await?;
                let mut tx = permit.connection()?.begin().await?;
                if let Some(lost) = Self::check_fence(&mut tx, &fence, token.target_run).await? {
                    return Err(lost.into());
                }
                let Some(record) = optional_by_token(&mut tx, &token).await? else {
                    return Ok(false);
                };
                ensure!(record.phase == PURGING, "preparation is no longer purging");
                let keys = sqlx::query(table.select_page)
                    .bind(token.target_run.0)
                    .bind(900i64)
                    .fetch_all(&mut *tx)
                    .await?;
                if let Some(last) = keys.last() {
                    super::super::bulk_write::delete_through(
                        &mut tx,
                        token.target_run,
                        table,
                        last,
                    )
                    .await?;
                }
                tx.commit().await?;
                if keys.len() < 900 {
                    break;
                }
            }
        }
        loop {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            if let Some(lost) = Self::check_fence(&mut tx, &fence, token.target_run).await? {
                return Err(lost.into());
            }
            let Some(record) = optional_by_token(&mut tx, &token).await? else {
                return Ok(false);
            };
            ensure!(record.phase == PURGING, "preparation is no longer purging");
            let keys = sqlx::query_as::<_, (i32,)>("SELECT part_no FROM run_preparation_part WHERE preparation_id=$1 AND generation=$2 ORDER BY part_no LIMIT 900")
                .bind(token.id).bind(token.generation).fetch_all(&mut *tx).await?;
            if let Some((last,)) = keys.last() {
                sqlx::query("DELETE FROM run_preparation_part WHERE preparation_id=$1 AND generation=$2 AND part_no<=$3")
                    .bind(token.id).bind(token.generation).bind(last).execute(&mut *tx).await?;
            }
            if keys.len() < 900 {
                sqlx::query("DELETE FROM run_bulk_write WHERE run_key=$1")
                    .bind(token.target_run.0)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM run_creation_claim WHERE run_key=$1 AND generation=$2")
                    .bind(token.target_run.0)
                    .bind(token.generation)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(
                    "DELETE FROM run_preparation WHERE preparation_id=$1 AND generation=$2",
                )
                .bind(token.id)
                .bind(token.generation)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(true);
            }
            tx.commit().await?;
        }
    }
}
