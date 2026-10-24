//! Transaction budgets for the writes whose work has no fixed size, and the
//! Aurora DSQL transaction limits they sit under.
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
//! `TransactionModel` is the in-memory store's tally of one of those
//! transactions, which tests check against the budget. The in-memory store
//! applies none of DSQL's limits: DSQL is the reference backend, and the live
//! suites test its limits.

use std::ops::Range;

/// Rows one transaction of a paged write may change, counting the rows it
/// inserts, updates and deletes: a third of DSQL's 3,000.
pub const MAX_ROWS_PER_TRANSACTION: usize = 1_000;

/// Bytes of values one transaction of a paged write may insert or update:
/// two-fifths of DSQL's 10 MiB. Deleted rows don't count toward DSQL's 10 MiB,
/// so a delete costs rows only.
pub const MAX_BYTES_PER_TRANSACTION: usize = 4 * 1024 * 1024;

/// Source keys examined by a relocation or legacy-disposal write page.
pub const MAX_MAINTENANCE_KEYS: usize = 64;

/// Encoded events, and separately encoded principals, that one history batch of
/// a reset's copied history may hold, unless it holds a single event: half of
/// DSQL's value limit.
pub const MAX_RESET_BATCH_BYTES: usize = 512 * 1024;

/// Rows DSQL lets one transaction change, deleted rows included.
pub const DSQL_MAX_ROWS_PER_TRANSACTION: usize = 3_000;

/// Bytes DSQL lets one value hold.
pub const DSQL_MAX_VALUE_BYTES: usize = 1_048_576;

/// Bytes of values the largest DSQL transaction measured to commit inserted.
///
/// DSQL's limit is 10 MiB, but DSQL counts keys and per-row overhead with the
/// values, so 9 MiB of values commits and 10 MiB is refused.
pub const DSQL_MEASURED_BYTES_PER_TRANSACTION: usize = 9 * 1024 * 1024;

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
/// item, so an item that alone exceeds the budget forms a page of its own, as a
/// reset's copied batch of one large event can. An item's value can be over
/// DSQL's value limit; it is then written in parts that each fit it, and its cost
/// counts a row for each part (`large-value-parts`).
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

/// The in-memory store's tally of one transaction of a covered write: the rows
/// it changes and the bytes of values it inserts or updates, counted as
/// [`WriteCost`] does. Tests check the tallies against
/// [`WriteBudget::TRANSACTION`]. It refuses nothing, since the in-memory store
/// applies none of DSQL's limits.
#[derive(Debug, Default)]
pub(crate) struct TransactionModel {
    // Only tests read the tally.
    #[cfg_attr(not(test), allow(dead_code))]
    total: WriteCost,
}

impl TransactionModel {
    /// Count one row inserted or updated, holding these values.
    pub(crate) fn write(&mut self, values: &[usize]) {
        self.total.rows = self.total.rows.saturating_add(1);
        self.total.bytes = values.iter().fold(self.total.bytes, |total, &bytes| {
            total.saturating_add(bytes)
        });
    }

    /// Count rows deleted, which cost rows only.
    pub(crate) fn delete(&mut self, rows: usize) {
        self.total.rows = self.total.rows.saturating_add(rows);
    }

    /// The rows and bytes counted so far.
    #[cfg(test)]
    pub(crate) fn total(&self) -> WriteCost {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn the_tally_refuses_nothing_past_dsqls_limits() {
        let mut model = TransactionModel::default();
        model.delete(DSQL_MAX_ROWS_PER_TRANSACTION);
        model.write(&[DSQL_MAX_VALUE_BYTES + 1]);
        for _ in 0..9 {
            model.write(&[DSQL_MAX_VALUE_BYTES]);
        }
        assert_eq!(
            model.total(),
            WriteCost {
                rows: DSQL_MAX_ROWS_PER_TRANSACTION + 10,
                bytes: DSQL_MEASURED_BYTES_PER_TRANSACTION + DSQL_MAX_VALUE_BYTES + 1,
            }
        );
    }

    #[test]
    fn deletes_count_toward_rows_only() {
        let mut model = TransactionModel::default();
        model.delete(3);
        model.write(&[16, 32]);
        assert_eq!(model.total(), WriteCost { rows: 4, bytes: 48 });
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
