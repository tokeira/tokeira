//! Transaction budgets for the writes whose work has no fixed size, and the
//! in-memory store's model of Aurora DSQL's transaction limits.
//!
//! DSQL refuses a write transaction that changes more than 3,000 rows or writes
//! more than 10 MiB, and refuses any single value over 1 MiB. Each refusal is
//! SQLSTATE 54000 and aborts the whole transaction
//! (<https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html>).
//! Live probes found how those limits count:
//! - deleted rows count toward the 3,000 rows, but not toward the 10 MiB;
//! - rows read `FOR UPDATE` don't count toward the 3,000;
//! - 9 MiB of values commits and 10 MiB is refused, since DSQL counts keys and
//!   per-row overhead with the values.
//!
//! The live suite `dsql_transaction_limits` asserts these facts, so a change in
//! DSQL fails a test rather than a purge.
//!
//! The backlog spill, a run's purge and a reset's materialization cut their work
//! with [`pages`], so each of their transactions stays within
//! [`WriteBudget::TRANSACTION`] (`bounded-bulk-writes`). The budget sits far
//! enough under DSQL's limits that the keys, fixed-width columns and per-row
//! overhead it doesn't count never matter.
//!
//! [`TransactionModel`] is the in-memory store's account of one of those
//! transactions. It refuses what DSQL refuses, so a test on either store sees
//! the same refusals.

use std::ops::Range;

/// Rows one transaction of a paged write may change, counting the rows it
/// inserts, updates and deletes: a third of DSQL's 3,000.
pub const MAX_ROWS_PER_TRANSACTION: usize = 1_000;

/// Bytes of values one transaction of a paged write may insert or update:
/// two-fifths of DSQL's 10 MiB. Deleted rows don't count toward DSQL's 10 MiB,
/// so a delete costs rows only.
pub const MAX_BYTES_PER_TRANSACTION: usize = 4 * 1024 * 1024;

/// Encoded events, and separately encoded principals, that one history batch of
/// a reset's copied history may hold, unless it holds a single event: half of
/// DSQL's value limit.
pub const MAX_RESET_BATCH_BYTES: usize = 512 * 1024;

/// Rows DSQL lets one transaction change, deleted rows included.
pub const DSQL_MAX_ROWS_PER_TRANSACTION: usize = 3_000;

/// Bytes DSQL lets one value hold.
pub const DSQL_MAX_VALUE_BYTES: usize = 1_048_576;

/// Bytes of values the in-memory store lets one transaction insert or update.
///
/// DSQL's limit is 10 MiB, but DSQL counts keys and per-row overhead with the
/// values, which the model doesn't. 9 MiB is the largest size measured to
/// commit, so the model refuses no less than DSQL does for the writes it covers.
pub const MODEL_MAX_BYTES_PER_TRANSACTION: usize = 9 * 1024 * 1024;

/// What one item adds to a transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteCost {
    /// Rows the item inserts, updates or deletes.
    pub rows: usize,
    /// Bytes of values the item inserts or updates.
    pub bytes: usize,
}

impl WriteCost {
    /// One row inserted or updated, holding `bytes` of values.
    #[must_use]
    pub const fn write(bytes: usize) -> Self {
        Self { rows: 1, bytes }
    }

    /// One row deleted. DSQL counts it toward rows only.
    pub const DELETE: Self = Self { rows: 1, bytes: 0 };
}

/// How much one transaction may change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteBudget {
    /// Rows the transaction may change.
    pub rows: usize,
    /// Bytes of values the transaction may insert or update.
    pub bytes: usize,
}

impl WriteBudget {
    /// The budget of one transaction of a paged write.
    pub const TRANSACTION: Self = Self {
        rows: MAX_ROWS_PER_TRANSACTION,
        bytes: MAX_BYTES_PER_TRANSACTION,
    };
}

/// Cut items, in order, into consecutive pages within `budget`.
///
/// The pages cover the items exactly and in order. Every page holds at least one
/// item, so an item that alone exceeds the budget forms a page of its own. The
/// callers' items never do: each fits DSQL's value limit, and a budget holds
/// several of those.
#[must_use]
pub fn pages(costs: &[WriteCost], budget: WriteBudget) -> Vec<Range<usize>> {
    let mut pages = Vec::new();
    let mut start = 0;
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for (index, cost) in costs.iter().enumerate() {
        let over = rows.saturating_add(cost.rows) > budget.rows
            || bytes.saturating_add(cost.bytes) > budget.bytes;
        if over && index > start {
            pages.push(start..index);
            start = index;
            rows = 0;
            bytes = 0;
        }
        rows = rows.saturating_add(cost.rows);
        bytes = bytes.saturating_add(cost.bytes);
    }
    if start < costs.len() {
        pages.push(start..costs.len());
    }
    pages
}

/// A transaction of a covered write that DSQL would refuse, as the in-memory
/// store reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransactionLimitExceeded {
    /// The transaction changes more rows than DSQL allows.
    #[error("transaction row limit exceeded: {rows} rows changed, and DSQL allows {limit}")]
    Rows {
        /// Rows the transaction changes.
        rows: usize,
        /// The limit it exceeds.
        limit: usize,
    },
    /// The transaction writes more bytes of values than the model allows.
    #[error(
        "transaction size limit exceeded: {bytes} bytes written, and the largest size measured to commit is {limit}"
    )]
    Bytes {
        /// Bytes of values the transaction inserts or updates.
        bytes: usize,
        /// The limit it exceeds.
        limit: usize,
    },
    /// The transaction writes a value larger than DSQL allows.
    #[error("value size limit exceeded: a value of {bytes} bytes, and DSQL allows {limit}")]
    Value {
        /// The value's size.
        bytes: usize,
        /// The limit it exceeds.
        limit: usize,
    },
}

/// The in-memory store's account of one transaction of a covered write.
///
/// The store declares all of a transaction's writes before it applies any, so a
/// refusal changes nothing, as DSQL's SQLSTATE 54000 aborts the whole
/// transaction.
#[derive(Debug, Default)]
pub(crate) struct TransactionModel {
    total: WriteCost,
}

impl TransactionModel {
    /// Declare one row inserted or updated, holding these values.
    pub(crate) fn write(&mut self, values: &[usize]) -> Result<(), TransactionLimitExceeded> {
        if let Some(&bytes) = values.iter().find(|&&bytes| bytes > DSQL_MAX_VALUE_BYTES) {
            return Err(TransactionLimitExceeded::Value {
                bytes,
                limit: DSQL_MAX_VALUE_BYTES,
            });
        }
        self.total.rows = self.total.rows.saturating_add(1);
        self.total.bytes = values.iter().fold(self.total.bytes, |total, &bytes| {
            total.saturating_add(bytes)
        });
        self.check()
    }

    /// Declare rows deleted, which DSQL counts toward rows only.
    pub(crate) fn delete(&mut self, rows: usize) -> Result<(), TransactionLimitExceeded> {
        self.total.rows = self.total.rows.saturating_add(rows);
        self.check()
    }

    /// The rows and bytes declared so far.
    #[cfg(test)]
    pub(crate) fn total(&self) -> WriteCost {
        self.total
    }

    fn check(&self) -> Result<(), TransactionLimitExceeded> {
        if self.total.rows > DSQL_MAX_ROWS_PER_TRANSACTION {
            return Err(TransactionLimitExceeded::Rows {
                rows: self.total.rows,
                limit: DSQL_MAX_ROWS_PER_TRANSACTION,
            });
        }
        if self.total.bytes > MODEL_MAX_BYTES_PER_TRANSACTION {
            return Err(TransactionLimitExceeded::Bytes {
                bytes: self.total.bytes,
                limit: MODEL_MAX_BYTES_PER_TRANSACTION,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn the_model_refuses_rows_past_dsqls_limit() {
        let mut model = TransactionModel::default();
        model.delete(DSQL_MAX_ROWS_PER_TRANSACTION - 1).unwrap();
        model.write(&[]).unwrap();
        assert_eq!(
            model.delete(1),
            Err(TransactionLimitExceeded::Rows {
                rows: DSQL_MAX_ROWS_PER_TRANSACTION + 1,
                limit: DSQL_MAX_ROWS_PER_TRANSACTION,
            })
        );
    }

    #[test]
    fn the_model_refuses_a_value_past_dsqls_limit() {
        let mut model = TransactionModel::default();
        model.write(&[DSQL_MAX_VALUE_BYTES]).unwrap();
        assert_eq!(
            model.write(&[16, DSQL_MAX_VALUE_BYTES + 1]),
            Err(TransactionLimitExceeded::Value {
                bytes: DSQL_MAX_VALUE_BYTES + 1,
                limit: DSQL_MAX_VALUE_BYTES,
            })
        );
        assert_eq!(model.total(), WriteCost::write(DSQL_MAX_VALUE_BYTES));
    }

    #[test]
    fn the_model_refuses_bytes_past_the_largest_size_measured_to_commit() {
        let mut model = TransactionModel::default();
        for _ in 0..9 {
            model.write(&[DSQL_MAX_VALUE_BYTES]).unwrap();
        }
        assert_eq!(
            model.write(&[1]),
            Err(TransactionLimitExceeded::Bytes {
                bytes: MODEL_MAX_BYTES_PER_TRANSACTION + 1,
                limit: MODEL_MAX_BYTES_PER_TRANSACTION,
            })
        );
    }

    #[test]
    fn deletes_count_toward_rows_only() {
        let mut model = TransactionModel::default();
        model.delete(DSQL_MAX_ROWS_PER_TRANSACTION).unwrap();
        assert_eq!(
            model.total(),
            WriteCost {
                rows: DSQL_MAX_ROWS_PER_TRANSACTION,
                bytes: 0
            }
        );
    }

    proptest! {
        // Feature: bounded-bulk-writes, Property 9: The in-memory store refuses
        // what DSQL refuses
        #[test]
        fn the_model_refuses_exactly_past_the_limits(
            operations in prop::collection::vec(
                prop_oneof![
                    (0usize..1_200).prop_map(Err),
                    prop::collection::vec(0usize..1_100_000, 0..3).prop_map(Ok),
                ],
                0..40,
            ),
        ) {
            let mut model = TransactionModel::default();
            let mut rows = 0usize;
            let mut bytes = 0usize;
            for operation in operations {
                let (result, refused) = match operation {
                    Ok(values) => {
                        let too_large = values.iter().any(|&value| value > DSQL_MAX_VALUE_BYTES);
                        let result = model.write(&values);
                        if !too_large {
                            rows += 1;
                            bytes += values.iter().sum::<usize>();
                        }
                        (
                            result,
                            too_large
                                || rows > DSQL_MAX_ROWS_PER_TRANSACTION
                                || bytes > MODEL_MAX_BYTES_PER_TRANSACTION,
                        )
                    }
                    Err(deleted) => {
                        rows += deleted;
                        (
                            model.delete(deleted),
                            rows > DSQL_MAX_ROWS_PER_TRANSACTION
                                || bytes > MODEL_MAX_BYTES_PER_TRANSACTION,
                        )
                    }
                };
                prop_assert_eq!(result.is_err(), refused);
                if refused {
                    break;
                }
            }
        }
    }

    proptest! {
        // The pages cover the items in order, each within the budget unless it
        // holds a single item.
        #[test]
        fn pages_cover_the_items_within_the_budget(
            costs in prop::collection::vec((0usize..4, 0usize..2_000_000), 0..300),
            rows in 1usize..1_200,
            bytes in 1usize..5_000_000,
        ) {
            let costs = costs
                .into_iter()
                .map(|(rows, bytes)| WriteCost { rows, bytes })
                .collect::<Vec<_>>();
            let budget = WriteBudget { rows, bytes };
            let pages = pages(&costs, budget);
            let mut next = 0;
            for page in &pages {
                prop_assert_eq!(page.start, next);
                prop_assert!(page.end > page.start);
                next = page.end;
                let total = costs[page.clone()].iter().fold(WriteCost::default(), |total, cost| {
                    WriteCost { rows: total.rows + cost.rows, bytes: total.bytes + cost.bytes }
                });
                prop_assert!(
                    page.len() == 1 || (total.rows <= budget.rows && total.bytes <= budget.bytes)
                );
            }
            prop_assert_eq!(next, costs.len());
        }
    }
}
