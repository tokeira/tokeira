//! The derived-row record: which kinds of derived row a run may hold, kept on
//! its `workflow_hot` row and stamped with the transition sequence it
//! describes (`dsql-derived-row-record`).
//!
//! A commit reads the record in its hot-row lock. For every kind a valid
//! record rules out it skips the read of that kind's rows, or, for the
//! workflow dispatch row, the delete it would make when it wants none. It
//! writes the record it leaves in the hot-row upsert, so the record costs no
//! statement of its own.
//!
//! The invariant every writer keeps: a valid record that rules a kind out
//! means the run holds no row of that kind.
//! - A commit writes an exact record: the kinds it leaves, including obsolete
//!   operation rows whose delete it deferred past its row budget.
//! - A writer outside a commit that inserts or replaces rows of a kind a valid
//!   record rules out makes the record unknown ([`forget`]). Under one
//!   derivation that never happens, because the commit that wrote a valid
//!   record derived the same wanted rows from the same state. It guards
//!   against a release whose derivation differs.
//! - A writer that only deletes leaves the record alone: removing rows can't
//!   make a ruled-out kind hold one.
//!
//! The stamp keeps the record honest against writers that move the row on
//! without knowing it. An older release's commit advances `transition_seq`
//! through its own upsert, which names neither column, so the stamp stops
//! matching and the record reads as unknown until a commit of this release
//! writes it again. A release that adds a kind can't trust a record an older
//! release wrote, since the older release leaves the new bit clear whatever the
//! run holds; it must tell those records apart before it skips anything for
//! the new kind.
//!
//! The record never decides what a run wants or whether a row may be
//! dispatched. Derived rows stay effects of the committed state (AGENTS.md §3).

use super::*;

/// The kinds of derived row a run may hold, one bit each.
///
/// The bits are persisted in `workflow_hot.derived_kinds` and never reused for
/// another kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct DerivedKinds(u8);

impl DerivedKinds {
    /// The run's `workflow_dispatch` row.
    pub(super) const WORKFLOW_DISPATCH: Self = Self(1);
    /// The run's `activity_dispatch` rows.
    pub(super) const ACTIVITY_DISPATCH: Self = Self(2);
    /// The run's `run_operation` rows.
    pub(super) const OPERATIONS: Self = Self(4);
    /// Every bit this release names.
    const NAMED: i16 = 7;

    /// The kinds a commit leaves the run holding: the kinds its next state
    /// wants, and operation rows when an obsolete one's delete was deferred.
    pub(super) fn after_commit(
        workflow_dispatch: bool,
        activity_dispatch: bool,
        operations: bool,
    ) -> Self {
        [
            (workflow_dispatch, Self::WORKFLOW_DISPATCH),
            (activity_dispatch, Self::ACTIVITY_DISPATCH),
            (operations, Self::OPERATIONS),
        ]
        .into_iter()
        .filter(|(held, _)| *held)
        .fold(Self::default(), |kinds, (_, kind)| Self(kinds.0 | kind.0))
    }

    /// Whether these kinds include `kind`.
    pub(super) fn contains(self, kind: Self) -> bool {
        self.0 & kind.0 == kind.0
    }

    /// The value stored in `workflow_hot.derived_kinds`.
    pub(super) fn to_column(self) -> i16 {
        i16::from(self.0)
    }
}

/// The record read beside `transition_seq` in one statement, or `None` when it
/// is unknown: either column NULL, or a stamp other than `transition_seq`.
///
/// Bits this release doesn't name are ignored, so the bits it names keep their
/// meaning whatever a later release adds.
pub(super) fn read_record(
    transition_seq: i64,
    kinds: Option<i16>,
    kinds_seq: Option<i64>,
) -> Option<DerivedKinds> {
    let (kinds, stamp) = kinds.zip(kinds_seq)?;
    (stamp == transition_seq).then(|| {
        DerivedKinds(
            u8::try_from(kinds & DerivedKinds::NAMED).expect("the named bits fit in a byte"),
        )
    })
}

/// Whether a run with `record` may hold rows of `kind`. An unknown record may
/// hold every kind.
pub(super) fn may_hold(record: Option<DerivedKinds>, kind: DerivedKinds) -> bool {
    record.is_none_or(|kinds| kinds.contains(kind))
}

/// Whether a writer outside a commit that inserted or replaced rows of `kind`
/// must make the run's record unknown: exactly when a valid record rules
/// `kind` out. Acquisition repairs every run with a workflow dispatch row, so
/// writing the record whenever a repair upserts would add a statement per run.
pub(super) fn must_forget(record: Option<DerivedKinds>, kind: DerivedKinds) -> bool {
    !may_hold(record, kind)
}

/// Make the run's record unknown.
///
/// Clearing the stamp is enough: a NULL stamp is unknown whatever the bits
/// say, so nothing reads or recomputes the bits. The statement writes the hot
/// row, so a commit that lands after this transaction's snapshot makes this
/// transaction conflict, even when its own read of the row wasn't `FOR UPDATE`.
pub(super) async fn forget(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: RunKey,
) -> Result<()> {
    sqlx::query("UPDATE workflow_hot SET derived_kinds_seq = NULL WHERE run_key = $1")
        .bind(run.0)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
