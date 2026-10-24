//! Control-claim operations for live suites outside this crate.
//!
//! A listener-backed distributed node without a controller takes the
//! `tokeirad-owner` claim before it serves. Its live test reads the claim,
//! gives it to another owner as a node elsewhere would take it, and removes it
//! so that a run starts from no claim. Nothing in a release calls these
//! functions: the module compiles only for tests and with `dsql-integration`.

use sqlx::PgConnection;
use time::OffsetDateTime;

/// One claim row: its owner, fence token and expiry on the database's clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlClaim {
    /// The owner holding the claim, or none once it is released.
    pub owner_id: Option<String>,
    /// The fence token the last acquisition set.
    pub fence_token: i64,
    /// When the hold expires, on the database's clock.
    pub expires_at: OffsetDateTime,
}

/// Read the claim named `claim_name`, if its row exists.
pub async fn claim(
    connection: &mut PgConnection,
    claim_name: &str,
) -> Result<Option<ControlClaim>, sqlx::Error> {
    Ok(sqlx::query_as::<_, (Option<String>, i64, OffsetDateTime)>(
        "SELECT owner_id, fence_token, expires_at FROM tokeira_control_lease WHERE claim_name = $1",
    )
    .bind(claim_name)
    .fetch_optional(&mut *connection)
    .await?
    .map(|(owner_id, fence_token, expires_at)| ControlClaim {
        owner_id,
        fence_token,
        expires_at,
    }))
}

/// Give the claim to `owner_id` for `hold_millis`, advancing its fence token,
/// as another owner's acquisition does.
pub async fn give_to(
    connection: &mut PgConnection,
    claim_name: &str,
    owner_id: &str,
    hold_millis: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE tokeira_control_lease SET owner_id = $2, fence_token = fence_token + 1, \
         expires_at = now() + ($3::BIGINT * INTERVAL '1 millisecond'), updated_at = now() \
         WHERE claim_name = $1",
    )
    .bind(claim_name)
    .bind(owner_id)
    .bind(hold_millis)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// Remove the claim's row, so that the next acquisition finds no claim.
pub async fn remove(connection: &mut PgConnection, claim_name: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM tokeira_control_lease WHERE claim_name = $1")
        .bind(claim_name)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// The database's clock, which decides when a hold expires.
pub async fn database_now(connection: &mut PgConnection) -> Result<OffsetDateTime, sqlx::Error> {
    sqlx::query_scalar::<_, OffsetDateTime>("SELECT now()")
        .fetch_one(&mut *connection)
        .await
}
