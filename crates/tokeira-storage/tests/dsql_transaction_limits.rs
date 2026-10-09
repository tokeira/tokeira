#![cfg(feature = "dsql-integration")]
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

//! Aurora DSQL's transaction limits, as the paged bulk writes count on them
//! (`bounded-bulk-writes`).
//!
//! The budgets in `tokeira_storage::write_budget` sit under DSQL's limits, and
//! the in-memory store refuses what DSQL refuses for the writes they cover. Both
//! rest on how DSQL counts a transaction's rows and bytes, which these probes
//! pin on a live cluster, apart from the budgets: a change in DSQL fails a probe
//! here rather than a purge. Each probe writes only the rows of its own batch,
//! in a table of its own. The suite connects only when
//! `TOKEIRA_DSQL_TEST_DATABASE_URL` is set; run it serially
//! (`docs/testing/dsql-live-suites.md`).

use anyhow::{Result, ensure};
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgPoolOptions};
use tokeira_storage::write_budget::{
    DSQL_MAX_ROWS_PER_TRANSACTION, DSQL_MAX_VALUE_BYTES, MODEL_MAX_BYTES_PER_TRANSACTION,
};
use uuid::Uuid;

const MIB: usize = 1024 * 1024;

/// SQLSTATE `program_limit_exceeded`: DSQL's refusal of each limit probed here.
const LIMIT_EXCEEDED: &str = "54000";

/// SQLSTATE `serialization_failure`. A probe's transaction can meet one on a
/// fresh cluster; it is no answer about the limits, so the probe runs again.
const SERIALIZATION_FAILURE: &str = "40001";

const ATTEMPTS: usize = 3;

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Committed,
    /// Refused as over a limit, at a statement or at commit.
    Refused,
}

/// One step of a probe's transaction, on the rows of its batch.
enum Step<'a> {
    /// Insert `values` as rows numbered from `first`.
    Insert { first: i32, values: &'a [Vec<u8>] },
    /// Delete the rows numbered below `below`.
    Delete { below: i32 },
    /// Read the rows numbered below `below` `FOR UPDATE`, expecting `rows`.
    Lock { below: i32, rows: usize },
}

struct Probe {
    pool: PgPool,
    batch: Uuid,
}

impl Probe {
    async fn connect() -> Result<Option<Self>> {
        let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dsql_limits_probe
             (batch UUID NOT NULL, seq INT NOT NULL, data BYTEA NOT NULL,
              PRIMARY KEY (batch, seq))",
        )
        .execute(&pool)
        .await?;
        Ok(Some(Self {
            pool,
            batch: Uuid::new_v4(),
        }))
    }

    /// Run `steps` in one transaction, and say whether DSQL committed it or
    /// refused it as over a limit.
    async fn transaction(&self, steps: &[Step<'_>]) -> Result<Outcome> {
        let mut attempt = 1;
        loop {
            let error = match self.attempt(steps).await {
                Ok(()) => return Ok(Outcome::Committed),
                Err(error) => error,
            };
            let sqlstate = error
                .downcast_ref::<sqlx::Error>()
                .and_then(|error| match error {
                    sqlx::Error::Database(database) => {
                        database.code().map(|code| code.into_owned())
                    }
                    _ => None,
                });
            match sqlstate.as_deref() {
                Some(LIMIT_EXCEEDED) => return Ok(Outcome::Refused),
                Some(SERIALIZATION_FAILURE) if attempt < ATTEMPTS => attempt += 1,
                _ => return Err(error),
            }
        }
    }

    async fn attempt(&self, steps: &[Step<'_>]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for step in steps {
            match step {
                Step::Insert { first, values } => {
                    // A statement carries at most 1 MiB of values, or one value:
                    // DSQL drops a connection whose message is over 10 MiB.
                    let mut start = 0;
                    while start < values.len() {
                        let mut end = start + 1;
                        let mut bytes = values[start].len();
                        while end < values.len() && bytes + values[end].len() <= MIB {
                            bytes += values[end].len();
                            end += 1;
                        }
                        let mut insert = QueryBuilder::<Postgres>::new(
                            "INSERT INTO dsql_limits_probe (batch, seq, data) ",
                        );
                        insert.push_values(start..end, |mut row, index| {
                            row.push_bind(self.batch)
                                .push_bind(first + i32::try_from(index).unwrap())
                                .push_bind(values[index].as_slice());
                        });
                        insert.build().execute(&mut *tx).await?;
                        start = end;
                    }
                }
                Step::Delete { below } => {
                    sqlx::query("DELETE FROM dsql_limits_probe WHERE batch = $1 AND seq < $2")
                        .bind(self.batch)
                        .bind(below)
                        .execute(&mut *tx)
                        .await?;
                }
                Step::Lock { below, rows } => {
                    let locked = sqlx::query(
                        "SELECT seq FROM dsql_limits_probe WHERE batch = $1 AND seq < $2 FOR UPDATE",
                    )
                    .bind(self.batch)
                    .bind(below)
                    .fetch_all(&mut *tx)
                    .await?;
                    ensure!(
                        locked.len() == *rows,
                        "read {} rows FOR UPDATE",
                        locked.len()
                    );
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn insert(&self, first: usize, values: &[Vec<u8>]) -> Result<Outcome> {
        self.transaction(&[Step::Insert {
            first: i32::try_from(first)?,
            values,
        }])
        .await
    }

    async fn delete_below(&self, below: usize) -> Result<Outcome> {
        self.transaction(&[Step::Delete {
            below: i32::try_from(below)?,
        }])
        .await
    }
}

/// `count` values of `len` bytes no store could compress: an xorshift stream.
fn values(count: usize, len: usize) -> Vec<Vec<u8>> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    (0..count)
        .map(|_| {
            let mut value = Vec::with_capacity(len + 8);
            while value.len() < len {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                value.extend_from_slice(&state.to_le_bytes());
            }
            value.truncate(len);
            value
        })
        .collect()
}

macro_rules! probe {
    () => {
        match Probe::connect().await? {
            Some(probe) => probe,
            None => return Ok(()),
        }
    };
}

/// A transaction may write 3,000 rows, and no more.
#[tokio::test]
async fn dsql_three_thousand_rows_commit_and_one_more_is_refused() -> Result<()> {
    let probe = probe!();
    let rows = values(DSQL_MAX_ROWS_PER_TRANSACTION + 1, 16);
    assert_eq!(
        probe
            .insert(0, &rows[..DSQL_MAX_ROWS_PER_TRANSACTION])
            .await?,
        Outcome::Committed
    );
    let more = Probe {
        batch: Uuid::new_v4(),
        ..probe
    };
    assert_eq!(more.insert(0, &rows).await?, Outcome::Refused);
    Ok(())
}

/// Deleted rows count toward the 3,000.
#[tokio::test]
async fn dsql_deleted_rows_count_toward_the_rows() -> Result<()> {
    let probe = probe!();
    let rows = values(DSQL_MAX_ROWS_PER_TRANSACTION + 1, 16);
    let (first, second) = rows.split_at(rows.len() / 2);
    assert_eq!(probe.insert(0, first).await?, Outcome::Committed);
    assert_eq!(probe.insert(first.len(), second).await?, Outcome::Committed);
    assert_eq!(probe.delete_below(rows.len()).await?, Outcome::Refused);
    assert_eq!(
        probe.delete_below(DSQL_MAX_ROWS_PER_TRANSACTION).await?,
        Outcome::Committed
    );
    assert_eq!(probe.delete_below(rows.len()).await?, Outcome::Committed);
    Ok(())
}

/// Deleted rows don't count toward the 10 MiB: a transaction may delete more
/// bytes than any transaction may write.
#[tokio::test]
async fn dsql_deleted_bytes_do_not_count_toward_the_bytes() -> Result<()> {
    let probe = probe!();
    let rows = values(12, 1_000_000);
    let (first, second) = rows.split_at(6);
    assert_eq!(probe.insert(0, first).await?, Outcome::Committed);
    assert_eq!(probe.insert(6, second).await?, Outcome::Committed);
    assert!(rows.iter().map(Vec::len).sum::<usize>() > 10 * MIB);
    assert_eq!(probe.delete_below(rows.len()).await?, Outcome::Committed);
    Ok(())
}

/// Rows read `FOR UPDATE` don't count toward the 3,000: a transaction may read
/// 3,000 rows `FOR UPDATE` and write one more.
#[tokio::test]
async fn dsql_rows_read_for_update_do_not_count_toward_the_rows() -> Result<()> {
    let probe = probe!();
    let rows = values(DSQL_MAX_ROWS_PER_TRANSACTION + 1, 16);
    let (locked, written) = rows.split_at(DSQL_MAX_ROWS_PER_TRANSACTION);
    assert_eq!(probe.insert(0, locked).await?, Outcome::Committed);
    let below = i32::try_from(locked.len())?;
    assert_eq!(
        probe
            .transaction(&[
                Step::Lock {
                    below,
                    rows: locked.len(),
                },
                Step::Insert {
                    first: below,
                    values: written,
                },
            ])
            .await?,
        Outcome::Committed
    );
    Ok(())
}

/// A value may hold 1 MiB, and no more.
#[tokio::test]
async fn dsql_a_value_over_one_mib_is_refused() -> Result<()> {
    let probe = probe!();
    assert_eq!(
        probe.insert(0, &values(1, DSQL_MAX_VALUE_BYTES)).await?,
        Outcome::Committed
    );
    assert_eq!(
        probe
            .insert(1, &values(1, DSQL_MAX_VALUE_BYTES + 1))
            .await?,
        Outcome::Refused
    );
    Ok(())
}

/// A transaction may write 9 MiB of values, the in-memory store's limit, but not
/// 10 MiB: DSQL counts keys and per-row overhead with the values.
#[tokio::test]
async fn dsql_nine_mib_of_values_commit_and_ten_are_refused() -> Result<()> {
    let probe = probe!();
    let rows = MODEL_MAX_BYTES_PER_TRANSACTION / MIB;
    assert_eq!(rows * MIB, MODEL_MAX_BYTES_PER_TRANSACTION);
    assert_eq!(
        probe.insert(0, &values(rows, MIB)).await?,
        Outcome::Committed
    );
    assert_eq!(
        probe.insert(rows, &values(10, MIB)).await?,
        Outcome::Refused
    );
    Ok(())
}
