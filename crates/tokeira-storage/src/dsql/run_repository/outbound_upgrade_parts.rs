//! Immutable state pages for stopped-cluster preparation of large legacy runs.
//!
//! The final hot-row swap carries a required storage envelope naming one exact
//! generation and digest. Until that swap readers see the old complete state;
//! afterwards every reader joins these pages in its hot-row transaction. Older
//! readers reject the envelope. Normal transitions may replace it with TKW3.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgConnection;
use tokeira_types::RunKey;
use uuid::Uuid;

use crate::{dsql::value_parts::PART_BYTES, schema_contract::sha256_hex};

const MAGIC: &[u8] = b"TKUP\x01";

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StatePointer {
    pub(super) generation: Uuid,
    parts: u32,
    bytes: u64,
    digest: String,
}

impl StatePointer {
    pub(super) fn new(bytes: &[u8]) -> Result<Self> {
        let digest = Sha256::digest(bytes);
        Ok(Self {
            generation: Uuid::from_slice(&digest[..16])?,
            parts: u32::try_from(bytes.len().div_ceil(PART_BYTES))?,
            bytes: u64::try_from(bytes.len())?,
            digest: format!("{digest:x}"),
        })
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(postcard::to_allocvec(self)?);
        Ok(bytes)
    }
}

pub(super) async fn read(
    connection: &mut PgConnection,
    run: RunKey,
    first: &[u8],
) -> Result<Option<Vec<u8>>> {
    if !first.starts_with(b"TKUP") {
        return Ok(None);
    }
    ensure!(
        first.starts_with(MAGIC),
        "unsupported required prepared-state envelope for {run:?}"
    );
    let (pointer, trailing) = postcard::take_from_bytes::<StatePointer>(&first[MAGIC.len()..])?;
    ensure!(
        trailing.is_empty() && pointer.parts > 0,
        "malformed prepared-state envelope for {run:?}"
    );
    let rows: Vec<(i32, Vec<u8>)> = sqlx::query_as("SELECT part_no,data FROM outbound_upgrade_state_part WHERE run_key=$1 AND generation=$2 ORDER BY part_no")
        .bind(run.0).bind(pointer.generation).fetch_all(connection).await?;
    ensure!(
        rows.len() == pointer.parts as usize,
        "incomplete prepared state for {run:?}"
    );
    let mut bytes = Vec::new();
    for (index, (part, data)) in rows.into_iter().enumerate() {
        ensure!(
            part == i32::try_from(index)?
                && data.len() <= PART_BYTES
                && (index + 1 == pointer.parts as usize || data.len() == PART_BYTES),
            "invalid prepared state part for {run:?}"
        );
        bytes.extend(data);
    }
    ensure!(
        bytes.len() as u64 == pointer.bytes && sha256_hex(&bytes) == pointer.digest,
        "prepared state digest mismatch for {run:?}"
    );
    Ok(Some(bytes))
}
