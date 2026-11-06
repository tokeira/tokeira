//! The scope every DSQL storage operation runs in.
//!
//! [`in_operation`] does two things for an operation. It attaches the
//! operation's `dsql.<operation>` span, which statement accounting charges each
//! round trip to (`tokeira_observability::statement_accounting`). And it makes
//! sure no connection the operation broke is handed to another caller.
//!
//! A connection is broken when its transport failed (an IO, TLS or protocol
//! error) or the server ended the session with a FATAL error. Nothing on the
//! connection says so afterwards: sqlx has no synchronous health check, and the
//! reservoir validates returned connections without network I/O. So every
//! checkout inside an operation registers its connection's `BrokenFlag` in a
//! task-local scope, and when the operation fails with an error that breaks a
//! connection, every flag registered in that scope is set. The reservoirs
//! retire a flagged connection wherever they next meet it: on its return, at
//! checkout, or when the scanner passes over it.
//!
//! The flag is set after the operation's permit dropped, so the connection may
//! already be back in the ready queue; that is why checkout and the scanner
//! look at the flag, not only the return processor. A caller that checks the
//! connection out in that window fails on it as the first did, and its own
//! scope flags it again. An operation that held more than one connection
//! retires all of them, the healthy ones included: such failures are rare, and
//! a spare reconnect costs less than handing the next caller a dead connection.

use std::{
    cell::RefCell,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use sqlx::postgres::{PgDatabaseError, PgSeverity};
use tokeira_observability::statement_accounting;
use tracing::Span;

tokio::task_local! {
    /// The broken flags of every connection checked out in the current
    /// operation. Nested operations open their own scope.
    static OPERATION_CHECKOUTS: RefCell<Vec<BrokenFlag>>;
}

/// Set once the physical connection it belongs to can no longer be used.
///
/// Created with the connection and shared by every holder of it, so a flag set
/// by a failed operation reaches the reservoir that holds the connection now.
#[derive(Clone, Debug, Default)]
pub(crate) struct BrokenFlag(Arc<AtomicBool>);

impl BrokenFlag {
    pub(crate) fn mark(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Run `future` as the storage operation named by `span`: its round trips are
/// charged to the span, and a failure that breaks a connection retires every
/// connection the operation checked out.
///
/// `span` must be named `dsql.<operation>`; see
/// [`statement_accounting::in_operation`], which boxes `future` so the
/// operation's state lives on the heap, not in each caller's future.
pub fn in_operation<F>(span: Span, future: F) -> impl Future<Output = F::Output> + Send
where
    F: Future + Send,
    F::Output: OperationOutcome,
{
    let operation = statement_accounting::in_operation(span, future);
    OPERATION_CHECKOUTS.scope(RefCell::new(Vec::new()), async move {
        let outcome = operation.await;
        if outcome.broke_connection() {
            mark_checkouts_broken();
        }
        outcome
    })
}

/// Report `error` to the enclosing operation now, for an operation whose own
/// error type cannot carry the cause back to [`in_operation`]: if `error`
/// broke a connection, every connection the operation checked out retires.
pub(crate) fn note_failure(error: &anyhow::Error) {
    if breaks_connection(error) {
        mark_checkouts_broken();
    }
}

fn mark_checkouts_broken() {
    // Outside any operation there is nothing registered to mark.
    let _ = OPERATION_CHECKOUTS.try_with(|checkouts| {
        for flag in checkouts.borrow().iter() {
            flag.mark();
        }
    });
}

/// Register a checked-out connection with the enclosing operation, if any.
/// A checkout outside [`in_operation`] is not tracked; its connection is
/// retired only when a later operation fails on it.
pub(crate) fn note_checkout(flag: &BrokenFlag) {
    // `try_with` fails only outside a scope, which is the untracked case.
    let _ = OPERATION_CHECKOUTS.try_with(|checkouts| checkouts.borrow_mut().push(flag.clone()));
}

/// What an operation's result says about the connections it used.
pub trait OperationOutcome {
    /// Whether the operation failed in a way that leaves its connection
    /// unusable.
    fn broke_connection(&self) -> bool;
}

impl<T> OperationOutcome for anyhow::Result<T> {
    fn broke_connection(&self) -> bool {
        self.as_ref().err().is_some_and(breaks_connection)
    }
}

/// Lookups that report absence instead of failure never break a connection.
impl<T> OperationOutcome for Option<T> {
    fn broke_connection(&self) -> bool {
        false
    }
}

impl<T> OperationOutcome for Result<T, crate::BacklogPersistError> {
    fn broke_connection(&self) -> bool {
        self.as_ref()
            .err()
            .is_some_and(|error| breaks_connection(&error.source))
    }
}

/// The provenance store's errors keep only a message; its error mapping
/// reports a broken connection itself, through `note_failure`.
impl<T> OperationOutcome for Result<T, crate::WorkerTaskProvenanceError> {
    fn broke_connection(&self) -> bool {
        false
    }
}

/// Whether `error` leaves the connection it came from unusable.
///
/// An IO or TLS error means the stream is gone. A protocol error means sqlx
/// read a message it did not expect, so the stream's position is unknown. A
/// FATAL or PANIC error response is the server ending the session; PostgreSQL
/// closes the connection after sending one. Every other database error leaves
/// the session usable, and sqlx's rollback-on-drop cleans up any transaction.
pub(crate) fn breaks_connection(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .any(|error| match error {
            sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => true,
            sqlx::Error::Database(database) => database
                .try_downcast_ref::<PgDatabaseError>()
                .is_some_and(|database| {
                    matches!(database.severity(), PgSeverity::Fatal | PgSeverity::Panic)
                }),
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use std::io;

    use anyhow::Context as _;
    use tracing::debug_span;

    use super::*;

    fn io_failure() -> anyhow::Error {
        anyhow::Error::new(sqlx::Error::Io(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "reset",
        )))
        .context("loading the run")
    }

    #[test]
    fn transport_failures_break_the_connection_and_sql_errors_do_not() {
        assert!(breaks_connection(&io_failure()));
        assert!(breaks_connection(&anyhow::Error::new(sqlx::Error::Tls(
            "handshake".into()
        ))));
        assert!(breaks_connection(&anyhow::Error::new(
            sqlx::Error::Protocol("unexpected message".to_owned())
        )));
        assert!(!breaks_connection(&anyhow::Error::new(
            sqlx::Error::RowNotFound
        )));
        assert!(!breaks_connection(&anyhow::anyhow!("lease fence lost")));
    }

    #[tokio::test]
    async fn a_broken_operation_flags_every_connection_it_checked_out() {
        let first = BrokenFlag::default();
        let second = BrokenFlag::default();
        let outcome: anyhow::Result<()> = in_operation(debug_span!("dsql.test"), async {
            note_checkout(&first);
            note_checkout(&second);
            Err(io_failure())
        })
        .await;
        assert!(outcome.is_err());
        assert!(first.is_set() && second.is_set());
    }

    #[tokio::test]
    async fn a_failed_operation_that_kept_its_connection_flags_nothing() {
        let flag = BrokenFlag::default();
        let outcome: anyhow::Result<()> = in_operation(debug_span!("dsql.test"), async {
            note_checkout(&flag);
            Err(anyhow::Error::new(sqlx::Error::RowNotFound)).context("no such run")
        })
        .await;
        assert!(outcome.is_err());
        assert!(!flag.is_set());
    }

    #[tokio::test]
    async fn a_nested_operation_flags_only_its_own_checkouts() {
        let outer = BrokenFlag::default();
        let inner = BrokenFlag::default();
        let outcome: anyhow::Result<()> = in_operation(debug_span!("dsql.outer"), async {
            note_checkout(&outer);
            let nested: anyhow::Result<()> = in_operation(debug_span!("dsql.inner"), async {
                note_checkout(&inner);
                Err(io_failure())
            })
            .await;
            assert!(nested.is_err());
            Ok(())
        })
        .await;
        assert!(outcome.is_ok());
        assert!(inner.is_set());
        assert!(!outer.is_set());
    }

    #[test]
    fn a_checkout_outside_any_operation_is_untracked() {
        note_checkout(&BrokenFlag::default());
    }
}
