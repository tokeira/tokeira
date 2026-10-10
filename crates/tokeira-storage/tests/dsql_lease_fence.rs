#![cfg(feature = "dsql-integration")]

//! Live checks of the Aurora DSQL conflict rules that the lease-fence model,
//! `spec/tla/30_bundle_lease.tla`, assumes.
//!
//! DSQL takes no locks. It adjudicates conflicts when a transaction commits, and
//! of two conflicting transactions the one that commits last fails. For one row,
//! the model takes these rules from AWS's documentation:
//! - two `SELECT … FOR KEY SHARE` reads never conflict;
//! - a `FOR KEY SHARE` read doesn't conflict with an `UPDATE` of non-key columns;
//! - it conflicts with a write of a key column, and with `SELECT … FOR UPDATE`;
//! - a plain `SELECT` conflicts with nothing.
//!
//! A key column belongs to a unique index, so the lease's epoch is a key column
//! only through a unique index on `(shard_id, epoch)`. Each case below runs two
//! transactions on separate connections in a fixed interleaving and checks which
//! commit DSQL refuses. The probe tables are this suite's own: one lease table
//! with the unique index the protocol needs, one shaped like today's `shard_lease`
//! (V002), keyed on `shard_id` alone, and a table of run writes. The suite
//! connects only when `TOKEIRA_DSQL_TEST_DATABASE_URL` is set; run it serially
//! (`docs/testing/dsql-live-suites.md`).

use anyhow::{Context as _, Result, bail, ensure};
use sqlx::{Connection as _, PgConnection, Postgres, Transaction};
use uuid::Uuid;

/// One lease row's table: with `epoch` in a unique index, or keyed on `shard_id`
/// alone as `shard_lease` is today.
#[derive(Clone, Copy, Debug)]
enum Lease {
    EpochKeyed,
    ShardKeyed,
}

/// What a transaction does to the lease row.
#[derive(Clone, Copy, Debug)]
enum Access {
    /// The protocol's fence: read owner and epoch `FOR KEY SHARE`.
    KeyShare,
    /// A plain read of owner and epoch.
    PlainRead,
    /// Renewal: advance the expiry only, without `FOR UPDATE`.
    Renew,
    /// Renewal that first reads the row `FOR UPDATE`.
    RenewForUpdate,
    /// Takeover: a new owner and the next epoch.
    Takeover,
    /// A new owner with the epoch unchanged.
    OwnerOnly,
}

impl Access {
    /// A run write follows the lease access, as in a fenced run commit.
    fn writes_run(self) -> bool {
        matches!(self, Self::KeyShare | Self::PlainRead)
    }
}

const CREATE: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS lease_fence_probe_epoch_keyed (
        shard_id UUID NOT NULL, owner TEXT NOT NULL, epoch BIGINT NOT NULL,
        lease_expiry TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (shard_id), UNIQUE (shard_id, epoch))",
    "CREATE TABLE IF NOT EXISTS lease_fence_probe_shard_keyed (
        shard_id UUID NOT NULL, owner TEXT NOT NULL, epoch BIGINT NOT NULL,
        lease_expiry TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (shard_id))",
    "CREATE TABLE IF NOT EXISTS lease_fence_probe_runs (
        run_id UUID NOT NULL, shard_id UUID NOT NULL, writer TEXT NOT NULL,
        PRIMARY KEY (run_id))",
];

/// The statements for one lease table. Each names its table literally, so no
/// statement is assembled at run time.
struct Statements {
    seed: &'static str,
    key_share: &'static str,
    plain_read: &'static str,
    for_update: &'static str,
    renew: &'static str,
    takeover: &'static str,
    owner_only: &'static str,
}

impl Lease {
    fn statements(self) -> Statements {
        match self {
            Self::EpochKeyed => Statements {
                seed: "INSERT INTO lease_fence_probe_epoch_keyed (shard_id, owner, epoch, lease_expiry)
                       VALUES ($1, 'a', 1, now() + interval '30 seconds')",
                key_share: "SELECT owner, epoch FROM lease_fence_probe_epoch_keyed
                            WHERE shard_id = $1 FOR KEY SHARE",
                plain_read: "SELECT owner, epoch FROM lease_fence_probe_epoch_keyed
                             WHERE shard_id = $1",
                for_update: "SELECT owner, epoch FROM lease_fence_probe_epoch_keyed
                             WHERE shard_id = $1 FOR UPDATE",
                renew: "UPDATE lease_fence_probe_epoch_keyed
                        SET lease_expiry = lease_expiry + interval '30 seconds'
                        WHERE shard_id = $1 AND owner = 'a' AND epoch = 1",
                takeover: "UPDATE lease_fence_probe_epoch_keyed
                           SET owner = 'b', epoch = epoch + 1,
                               lease_expiry = now() + interval '30 seconds'
                           WHERE shard_id = $1 AND epoch = 1",
                owner_only: "UPDATE lease_fence_probe_epoch_keyed SET owner = 'b'
                             WHERE shard_id = $1",
            },
            Self::ShardKeyed => Statements {
                seed: "INSERT INTO lease_fence_probe_shard_keyed (shard_id, owner, epoch, lease_expiry)
                       VALUES ($1, 'a', 1, now() + interval '30 seconds')",
                key_share: "SELECT owner, epoch FROM lease_fence_probe_shard_keyed
                            WHERE shard_id = $1 FOR KEY SHARE",
                plain_read: "SELECT owner, epoch FROM lease_fence_probe_shard_keyed
                             WHERE shard_id = $1",
                for_update: "SELECT owner, epoch FROM lease_fence_probe_shard_keyed
                             WHERE shard_id = $1 FOR UPDATE",
                renew: "UPDATE lease_fence_probe_shard_keyed
                        SET lease_expiry = lease_expiry + interval '30 seconds'
                        WHERE shard_id = $1 AND owner = 'a' AND epoch = 1",
                takeover: "UPDATE lease_fence_probe_shard_keyed
                           SET owner = 'b', epoch = epoch + 1,
                               lease_expiry = now() + interval '30 seconds'
                           WHERE shard_id = $1 AND epoch = 1",
                owner_only: "UPDATE lease_fence_probe_shard_keyed SET owner = 'b'
                             WHERE shard_id = $1",
            },
        }
    }
}

const RUN_WRITE: &str =
    "INSERT INTO lease_fence_probe_runs (run_id, shard_id, writer) VALUES ($1, $2, $3)";

/// Two transactions on one lease row: both perform their access, then commit in
/// order, and DSQL refuses exactly the commits `refused` names.
struct Case {
    name: &'static str,
    lease: Lease,
    first: Access,
    second: Access,
    /// Whether DSQL refuses the first and the second commit.
    refused: (bool, bool),
}

const CASES: [Case; 10] = [
    Case {
        name: "two fenced run commits don't conflict",
        lease: Lease::EpochKeyed,
        first: Access::KeyShare,
        second: Access::KeyShare,
        refused: (false, false),
    },
    Case {
        name: "a renewal committed first doesn't abort a fenced run commit",
        lease: Lease::EpochKeyed,
        first: Access::Renew,
        second: Access::KeyShare,
        refused: (false, false),
    },
    Case {
        name: "a fenced run commit committed first doesn't abort a renewal",
        lease: Lease::EpochKeyed,
        first: Access::KeyShare,
        second: Access::Renew,
        refused: (false, false),
    },
    Case {
        name: "a takeover aborts the old owner's fenced run commit",
        lease: Lease::EpochKeyed,
        first: Access::Takeover,
        second: Access::KeyShare,
        refused: (false, true),
    },
    Case {
        name: "a fenced run commit that lands first aborts the takeover",
        lease: Lease::EpochKeyed,
        first: Access::KeyShare,
        second: Access::Takeover,
        refused: (false, true),
    },
    Case {
        name: "a change of owner alone doesn't fence: the epoch must change",
        lease: Lease::EpochKeyed,
        first: Access::OwnerOnly,
        second: Access::KeyShare,
        refused: (false, false),
    },
    Case {
        name: "without the unique index, a takeover doesn't fence the old owner",
        lease: Lease::ShardKeyed,
        first: Access::Takeover,
        second: Access::KeyShare,
        refused: (false, false),
    },
    Case {
        name: "a plain read of the epoch doesn't fence the old owner",
        lease: Lease::EpochKeyed,
        first: Access::Takeover,
        second: Access::PlainRead,
        refused: (false, false),
    },
    Case {
        name: "a renewal that reads FOR UPDATE aborts a fenced run commit",
        lease: Lease::EpochKeyed,
        first: Access::RenewForUpdate,
        second: Access::KeyShare,
        refused: (false, true),
    },
    Case {
        name: "a fenced run commit committed first aborts a FOR UPDATE renewal",
        lease: Lease::EpochKeyed,
        first: Access::KeyShare,
        second: Access::RenewForUpdate,
        refused: (false, true),
    },
];

/// The lease-fence model's conflict rules hold on DSQL through sqlx: the protocol's
/// fence stops a stale owner's commit, renewals and the owner's own commits never
/// abort one another, and each of today's weaker fences lets a stale commit land.
#[tokio::test]
async fn dsql_lease_fence_conflict_rules() -> Result<()> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(());
    };
    let mut setup = PgConnection::connect(&url).await?;
    for statement in CREATE {
        sqlx::query(statement).execute(&mut setup).await?;
    }
    let mut first = PgConnection::connect(&url).await?;
    let mut second = PgConnection::connect(&url).await?;
    for case in &CASES {
        run_case(&mut setup, &mut first, &mut second, case)
            .await
            .with_context(|| format!("case: {}", case.name))?;
    }
    Ok(())
}

async fn run_case(
    setup: &mut PgConnection,
    first: &mut PgConnection,
    second: &mut PgConnection,
    case: &Case,
) -> Result<()> {
    let shard_id = Uuid::new_v4();
    sqlx::query(case.lease.statements().seed)
        .bind(shard_id)
        .execute(&mut *setup)
        .await?;

    // Both transactions perform their access before either commits, so each
    // commit is adjudicated against the other's.
    let mut first_tx = first.begin().await?;
    access(&mut first_tx, case.lease, case.first, shard_id, "first").await?;
    let mut second_tx = second.begin().await?;
    access(&mut second_tx, case.lease, case.second, shard_id, "second").await?;

    let first_refused = refused(first_tx.commit().await).context("first commit")?;
    let second_refused = refused(second_tx.commit().await).context("second commit")?;
    ensure!(
        (first_refused, second_refused) == case.refused,
        "expected refusals {:?}, observed {:?}",
        case.refused,
        (first_refused, second_refused)
    );
    Ok(())
}

async fn access(
    tx: &mut Transaction<'_, Postgres>,
    lease: Lease,
    access: Access,
    shard_id: Uuid,
    writer: &str,
) -> Result<()> {
    let statements = lease.statements();
    match access {
        Access::KeyShare | Access::PlainRead => {
            let read = if matches!(access, Access::KeyShare) {
                statements.key_share
            } else {
                statements.plain_read
            };
            // The read precedes every commit in the case, so it sees the seeded
            // lease: this writer believes it owns epoch 1.
            let (owner, epoch): (String, i64) = sqlx::query_as(read)
                .bind(shard_id)
                .fetch_one(&mut **tx)
                .await?;
            ensure!(
                owner == "a" && epoch == 1,
                "{writer} read owner {owner} at epoch {epoch}"
            );
        }
        Access::Renew => update(tx, statements.renew, shard_id).await?,
        Access::RenewForUpdate => {
            sqlx::query(statements.for_update)
                .bind(shard_id)
                .fetch_one(&mut **tx)
                .await?;
            update(tx, statements.renew, shard_id).await?;
        }
        Access::Takeover => update(tx, statements.takeover, shard_id).await?,
        Access::OwnerOnly => update(tx, statements.owner_only, shard_id).await?,
    }
    if access.writes_run() {
        sqlx::query(RUN_WRITE)
            .bind(Uuid::new_v4())
            .bind(shard_id)
            .bind(writer)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn update(
    tx: &mut Transaction<'_, Postgres>,
    statement: &'static str,
    shard_id: Uuid,
) -> Result<()> {
    let updated = sqlx::query(statement)
        .bind(shard_id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    ensure!(updated == 1, "the lease update changed {updated} rows");
    Ok(())
}

/// Whether DSQL refused a commit for a conflict: SQLSTATE 40001. Any other
/// failure fails the case.
fn refused(commit: Result<(), sqlx::Error>) -> Result<bool> {
    match commit {
        Ok(()) => Ok(false),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("40001") => Ok(true),
        Err(error) => bail!(error),
    }
}
