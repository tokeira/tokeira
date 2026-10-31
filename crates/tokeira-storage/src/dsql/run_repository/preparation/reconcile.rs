//! Durable handoff after publication, including unknown COMMIT outcomes.
//! A small descriptor remains discoverable until local deadline reconstruction;
//! manifest reclamation is paged, while compact request/generation proof stays.

use super::*;
use crate::{OutboundPage, ScanBound};

type TokenRow = (Uuid, Uuid, Uuid, Uuid, Vec<u8>, Uuid, Vec<u8>);

fn token(row: TokenRow) -> Result<PreparationToken> {
    let (id, generation, run, run_id, started_data, home, digest) = row;
    let (started_at, rest) = postcard::take_from_bytes(&started_data)?;
    ensure!(rest.is_empty(), "trailing publication clock data");
    Ok(PreparationToken {
        id,
        generation,
        target_run: RunKey(run),
        run_id: RunId(run_id),
        started_at,
        home: DsqlRunRepository::shard_id_from_uuid(home)?,
        manifest_digest: digest
            .try_into()
            .map_err(|_| RecoveryError::InvalidPreparation("invalid manifest digest length"))?,
    })
}

impl DsqlRunRepository {
    pub(in crate::dsql::run_repository) async fn do_published_preparations(
        &self,
        home: ShardId,
        bound: ScanBound<Uuid>,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<OutboundPage<Uuid, PreparationToken>> {
        crate::outbound::validate_page_limit(limit)?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit
            .connection()?
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let through = match bound {
            ScanBound::Through(id) => Some(id),
            ScanBound::Capture => sqlx::query_scalar::<_, Uuid>("SELECT preparation_id FROM run_preparation WHERE source_home=$1 AND phase=4 ORDER BY preparation_id DESC LIMIT 1")
                .bind(Self::shard_id_to_uuid(home)).fetch_optional(&mut *tx).await?,
        };
        let rows = if let Some(through) = through {
            sqlx::query_as::<_, TokenRow>("SELECT preparation_id,generation,target_run_key,run_id,started_at_data,source_home,manifest_digest FROM run_preparation WHERE source_home=$1 AND phase=4 AND preparation_id<=$2 AND ($3::uuid IS NULL OR preparation_id>$3) ORDER BY preparation_id LIMIT $4")
                .bind(Self::shard_id_to_uuid(home)).bind(through).bind(after).bind(i64::try_from(limit)?)
                .fetch_all(&mut *tx).await?.into_iter().map(token).collect::<Result<_>>()?
        } else {
            Vec::new()
        };
        tx.commit().await?;
        Ok(OutboundPage { through, rows })
    }

    pub(in crate::dsql::run_repository) async fn do_complete_preparation(
        &self,
        token: PreparationToken,
        fence: WriteFence,
    ) -> Result<bool> {
        fence.ensure_home(token.home)?;
        loop {
            let mut permit = self.director.acquire(DbClass::Maintenance).await?;
            let mut tx = permit.connection()?.begin().await?;
            if let Some(lost) = Self::check_fence(&mut tx, &fence, token.target_run).await? {
                return Err(lost.into());
            }
            let Some(record) = optional_by_token(&mut tx, &token).await? else {
                return Ok(false);
            };
            if record.phase == RECONCILED {
                tx.commit().await?;
                return Ok(true);
            }
            ensure!(
                record.phase == PUBLISHED,
                "cannot complete an unpublished preparation"
            );
            let keys = sqlx::query_as::<_, (i32,)>("SELECT part_no FROM run_preparation_part WHERE preparation_id=$1 AND generation=$2 ORDER BY part_no LIMIT 900")
                .bind(token.id).bind(token.generation).fetch_all(&mut *tx).await?;
            if let Some((last,)) = keys.last() {
                sqlx::query("DELETE FROM run_preparation_part WHERE preparation_id=$1 AND generation=$2 AND part_no<=$3")
                    .bind(token.id).bind(token.generation).bind(last).execute(&mut *tx).await?;
            }
            let done = keys.len() < 900;
            if done {
                sqlx::query("UPDATE run_preparation SET phase=5,manifest_data=$2,manifest_parts=1 WHERE preparation_id=$1 AND generation=$3")
                    .bind(token.id).bind(&[] as &[u8]).bind(token.generation).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            if done {
                return Ok(true);
            }
        }
    }
}
