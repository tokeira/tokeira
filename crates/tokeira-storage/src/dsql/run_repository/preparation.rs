//! Generation-fenced invisible publication storage.
//!
//! The creation claim is shared with ordinary starts. Reading an absent staging
//! row is insufficient under snapshot isolation: both writers must contend on
//! the same key before either can create run-owned rows.

use super::*;
use crate::{
    PreparationToken, PublicationResult, RecoveryError, RunPreparation,
    dsql::value_parts,
    write_budget::{WriteBudget, WriteCost, pages},
};
use anyhow::ensure;
use sqlx::PgConnection;

mod publish;
mod purge;
mod reconcile;
mod stage;

const UPLOADING: i16 = 0;
const STAGING: i16 = 1;
const READY: i16 = 2;
const PURGING: i16 = 3;
const PUBLISHED: i16 = 4;
const RECONCILED: i16 = 5;
macro_rules! header_select { () => { "SELECT preparation_id,request_key,target_run_key,run_id,started_at_data,source_home,generation,phase,manifest_version,manifest_digest,manifest_data,manifest_parts FROM run_preparation" }; }
type Header = (
    Uuid,
    String,
    Uuid,
    Uuid,
    Vec<u8>,
    Uuid,
    Uuid,
    i16,
    i32,
    Vec<u8>,
    Vec<u8>,
    i32,
);

struct Record {
    token: PreparationToken,
    phase: i16,
    first: Vec<u8>,
    parts: i32,
}

impl Record {
    fn decode(row: Header) -> Result<Self> {
        let (
            id,
            _request_key,
            run,
            run_id,
            started_at_data,
            home,
            generation,
            phase,
            version,
            digest,
            first,
            parts,
        ) = row;
        ensure!(
            version == 1 && (UPLOADING..=RECONCILED).contains(&phase) && parts > 0,
            "unsupported publication header"
        );
        // SQL timestamps round to microseconds; the frozen successor clock must
        // retain its original nanoseconds across an interrupted preparation.
        let (started_at, clock_rest): (OffsetDateTime, _) =
            postcard::take_from_bytes(&started_at_data)?;
        ensure!(clock_rest.is_empty(), "trailing preparation clock data");
        let manifest_digest = digest
            .try_into()
            .map_err(|_| RecoveryError::InvalidPreparation("invalid manifest digest length"))?;
        Ok(Self {
            token: PreparationToken {
                id,
                generation,
                target_run: RunKey(run),
                run_id: RunId(run_id),
                started_at,
                home: DsqlRunRepository::shard_id_from_uuid(home)?,
                manifest_digest,
            },
            phase,
            first,
            parts,
        })
    }
}

async fn by_request(connection: &mut PgConnection, request: &str) -> Result<Option<Record>> {
    sqlx::query_as::<_, Header>(concat!(
        header_select!(),
        " WHERE request_key=$1 FOR UPDATE"
    ))
    .bind(request)
    .fetch_optional(connection)
    .await?
    .map(Record::decode)
    .transpose()
}

async fn optional_by_token(
    connection: &mut PgConnection,
    token: &PreparationToken,
) -> Result<Option<Record>> {
    let record = sqlx::query_as::<_, Header>(concat!(
        header_select!(),
        " WHERE preparation_id=$1 FOR UPDATE"
    ))
    .bind(token.id)
    .fetch_optional(connection)
    .await?
    .map(Record::decode)
    .transpose()?;
    if let Some(record) = &record {
        ensure!(
            record.token == *token,
            "preparation token does not match committed generation"
        );
    }
    Ok(record)
}

async fn by_token(connection: &mut PgConnection, token: &PreparationToken) -> Result<Record> {
    optional_by_token(connection, token)
        .await?
        .ok_or_else(|| RecoveryError::InvalidPreparation("preparation generation is absent").into())
}

async fn manifest(connection: &mut PgConnection, record: &Record) -> Result<Vec<u8>> {
    let rest = sqlx::query_as::<_, (i32, Vec<u8>)>(
        "SELECT part_no,data FROM run_preparation_part WHERE preparation_id=$1 AND generation=$2 ORDER BY part_no")
        .bind(record.token.id).bind(record.token.generation).fetch_all(connection).await?;
    let bytes = value_parts::join(
        || "publication manifest".into(),
        record.first.clone(),
        (record.parts > 1).then_some(record.parts),
        rest,
    )?;
    record.token.decode_manifest(&bytes)?;
    Ok(bytes)
}

/// All absent-run writers, including ordinary starts, share this durable key.
pub(super) async fn claim_creation(
    connection: &mut PgConnection,
    run: RunKey,
    generation: Uuid,
) -> Result<()> {
    sqlx::query("INSERT INTO run_creation_claim(run_key,generation) VALUES($1,$2) ON CONFLICT(run_key) DO NOTHING")
        .bind(run.0).bind(generation).execute(&mut *connection).await?;
    let (owner,) = sqlx::query_as::<_, (Uuid,)>(
        "SELECT generation FROM run_creation_claim WHERE run_key=$1 FOR UPDATE",
    )
    .bind(run.0)
    .fetch_one(connection)
    .await?;
    if owner != generation {
        return Err(RecoveryError::InvalidPreparation(
            "run storage is claimed by another generation",
        )
        .into());
    }
    Ok(())
}

impl DsqlRunRepository {
    pub(super) async fn do_find_run_preparation(
        &self,
        request: &str,
    ) -> Result<Option<PreparationToken>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let row = sqlx::query_as::<_, Header>(concat!(header_select!(), " WHERE request_key=$1"))
            .bind(request)
            .fetch_optional(permit.connection()?)
            .await?;
        row.map(Record::decode)
            .transpose()
            .map(|record| record.map(|r| r.token))
    }

    pub(super) async fn do_prepare_publication(
        &self,
        proposed: Box<RunPreparation>,
        fence: WriteFence,
    ) -> Result<PreparationToken> {
        let bytes = proposed.encode()?;
        let state = &proposed.new_run.next_state;
        let home = tokeira_types::execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            self.shard_count,
        );
        fence.ensure_home(home)?;
        let proposed_token = PreparationToken::new(state, home, &bytes);
        let split = value_parts::split(&bytes);
        let selected = {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.begin().await?;
            if let Some(lost) = Self::check_fence(&mut tx, &fence, state.run_key).await? {
                return Err(lost.into());
            }
            if let Some(record) = by_request(&mut tx, &proposed.request_key).await? {
                ensure!(
                    record.token.home == home,
                    "preparation request belongs to another home"
                );
                ensure!(
                    record.phase != PURGING,
                    "previous preparation generation is still purging"
                );
                tx.commit().await?;
                if record.phase >= PUBLISHED {
                    return Ok(record.token);
                }
                record.token
            } else {
                let (occupied,) = sqlx::query_as::<_, (bool,)>("SELECT EXISTS(SELECT 1 FROM workflow_hot WHERE run_key=$1) OR EXISTS(SELECT 1 FROM run_bulk_write WHERE run_key=$1)")
                    .bind(state.run_key.0).fetch_one(&mut *tx).await?;
                ensure!(!occupied, "preparation target already has run storage");
                claim_creation(&mut tx, state.run_key, proposed_token.generation).await?;
                sqlx::query("INSERT INTO run_preparation(preparation_id,request_key,target_run_key,run_id,started_at_data,source_home,generation,phase,manifest_version,manifest_digest,manifest_data,manifest_parts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10,$11)")
                    .bind(proposed_token.id).bind(&proposed.request_key).bind(state.run_key.0).bind(state.run_id.0).bind(postcard::to_allocvec(&state.started_at)?)
                    .bind(Self::shard_id_to_uuid(home)).bind(proposed_token.generation).bind(UPLOADING)
                    .bind(proposed_token.manifest_digest.as_slice()).bind(split.first).bind(split.parts()).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO run_bulk_write(run_key,shard_id,phase) VALUES($1,$2,$3)")
                    .bind(state.run_key.0)
                    .bind(Self::shard_id_to_uuid(home))
                    .bind(crate::BulkWritePhase::Materializing.to_db_smallint())
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                proposed_token
            }
        };
        let publication = self
            .upload_manifest(&selected, &proposed, &bytes, &fence)
            .await?;
        if let Some(publication) = publication {
            self.stage_publication(&selected, &publication, &fence)
                .await?;
        }
        Ok(selected)
    }

    async fn upload_manifest(
        &self,
        token: &PreparationToken,
        proposed: &RunPreparation,
        bytes: &[u8],
        fence: &WriteFence,
    ) -> Result<Option<RunPreparation>> {
        let same = crate::outbound::preparation::manifest_digest(bytes) == token.manifest_digest;
        if same {
            let split = value_parts::split(bytes);
            let parts: Vec<_> = split.numbered().collect();
            let costs: Vec<_> = parts
                .iter()
                .map(|(_, bytes)| WriteCost::write(bytes.len() + 64))
                .collect();
            for page in pages(&costs, WriteBudget::TRANSACTION) {
                let mut permit = self.director.acquire(DbClass::Commit).await?;
                let mut tx = permit.begin().await?;
                if let Some(lost) = Self::check_fence(&mut tx, fence, token.target_run).await? {
                    return Err(lost.into());
                }
                let record = by_token(&mut tx, token).await?;
                // Publication won while this uploader was between pages. Its
                // manifest may already be reclaimed; never resurrect those parts.
                if record.phase >= PUBLISHED {
                    tx.commit().await?;
                    return Ok(None);
                }
                ensure!(record.phase != PURGING, "manifest generation is purging");
                for (number, data) in &parts[page] {
                    sqlx::query("INSERT INTO run_preparation_part(preparation_id,generation,part_no,data) VALUES($1,$2,$3,$4) ON CONFLICT(preparation_id,generation,part_no) DO NOTHING")
                        .bind(token.id).bind(token.generation).bind(number).bind(data).execute(&mut *tx).await?;
                    let (existing,) = sqlx::query_as::<_, (Vec<u8>,)>("SELECT data FROM run_preparation_part WHERE preparation_id=$1 AND generation=$2 AND part_no=$3")
                        .bind(token.id).bind(token.generation).bind(number).fetch_one(&mut *tx).await?;
                    ensure!(existing == *data, "immutable manifest part changed");
                }
                tx.commit().await?;
            }
        }
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.begin().await?;
        if let Some(lost) = Self::check_fence(&mut tx, fence, token.target_run).await? {
            return Err(lost.into());
        }
        let record = by_token(&mut tx, token).await?;
        if record.phase >= PUBLISHED {
            tx.commit().await?;
            return Ok(None);
        }
        ensure!(record.phase != PURGING, "manifest generation is purging");
        // An interrupted upload with another proposed identity must first be
        // rebuilt using find_run_preparation's selected run ID and start clock.
        let decoded = token.decode_manifest(&manifest(&mut tx, &record).await?)?;
        ensure!(
            decoded.request_key == proposed.request_key,
            "preparation request changed"
        );
        if record.phase == UPLOADING {
            sqlx::query("UPDATE run_preparation SET phase=$2 WHERE preparation_id=$1")
                .bind(token.id)
                .bind(STAGING)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(Some(decoded))
    }
}
