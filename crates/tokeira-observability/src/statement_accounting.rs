//! Round trips per DSQL operation, counted from the statements sqlx reports.
//!
//! Every Aurora DSQL statement costs a network round trip, and on a commit
//! path round trips, not CPU, set the latency. This layer counts them in
//! production without a counter at each of storage's statement sites: one
//! there would be forgotten when a statement is added, and the count would
//! drift silently. It reads three things instead:
//!
//! - **sqlx's statement events.** sqlx reports each statement it runs as one
//!   `tracing` event with target `sqlx::query`, at DEBUG (WARN once slow), whose
//!   `summary` field holds the statement's first four words (sqlx-core 0.9,
//!   `src/logger.rs`, `QueryLogger::finish`). It reports the `COMMIT` or
//!   `ROLLBACK` that ends a transaction but not the `BEGIN` that opens one, so
//!   the layer adds that round trip itself when the transaction ends.
//! - **Storage's operation spans.** Storage names the span of each DSQL
//!   operation `dsql.<operation>`. A statement is charged to the *outermost*
//!   such span around it: an operation built from other operations is what its
//!   caller paid for, and a transaction stays inside one operation.
//! - **The cause.** The nearest span around the operation with a
//!   `tokeira.command_type` field names what caused it (the runtime sets it on
//!   lane processing and on its activity paths); without one the cause is
//!   `none`.
//!
//! Each sample of [`DSQL_OPERATION_ROUND_TRIPS`] is the round trips an
//! operation made up to and including the end of one transaction, labelled
//! `committed` or `rolled_back`, or those it made after its last transaction,
//! labelled `none` when the span closes. Statements = round trips − 1 for a
//! transaction sample. A statement outside every operation span increments
//! [`DSQL_UNATTRIBUTED_STATEMENTS_TOTAL`], which shows where storage still lacks
//! a span.
//!
//! The count is a floor. It leaves out the round trip sqlx makes to prepare a
//! statement the first time a connection runs it (sqlx then caches it), a
//! connection's setup, and the `ROLLBACK` sqlx queues, unreported, when a
//! transaction is dropped without one.
//!
//! Its cost: enabling sqlx's DEBUG statement events makes sqlx format each
//! statement's text for the event, microseconds against a round trip measured in
//! milliseconds. The layer's filter is per layer, so the operator's log filter
//! still decides what reaches logs and traces.

use std::{fmt, pin::Pin, sync::Arc};

use metrics::SharedString;
use tracing::{
    Event, Instrument, Metadata, Span, Subscriber,
    field::{Field, Visit},
    instrument::Instrumented,
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer,
    filter::{FilterFn, LevelFilter, filter_fn},
    layer::Context,
    registry::{LookupSpan, SpanRef},
};

use crate::metrics::{DSQL_OPERATION_ROUND_TRIPS, DSQL_UNATTRIBUTED_STATEMENTS_TOTAL};

/// The name prefix that marks a span as one DSQL operation.
pub const OPERATION_SPAN_PREFIX: &str = "dsql.";

/// The span field that names what caused the work under the span.
pub const COMMAND_TYPE_FIELD: &str = "tokeira.command_type";

/// The `tracing` target of sqlx's statement events.
const STATEMENT_TARGET: &str = "sqlx::query";

/// The statement event field holding its first words.
const SUMMARY_FIELD: &str = "summary";

/// The `command_type` label of an operation nothing named a cause for.
const NO_CAUSE: &str = "none";

/// The most verbose level the layer reads. sqlx reports statements at DEBUG by
/// default; the hint keeps the process's max level at DEBUG rather than TRACE,
/// so `log` records at TRACE are still discarded before the `log` bridge.
const MAX_LEVEL: LevelFilter = LevelFilter::DEBUG;

/// The statement accounting layer, with the filter that admits only what it
/// reads.
///
/// Add it to a subscriber beside, not under, the subscriber's log filter: its
/// own filter has to let sqlx's DEBUG statement events through whatever the log
/// filter says.
pub fn statement_accounting_layer<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    StatementAccounting.with_filter(accounted_filter())
}

/// Run one DSQL operation's future in its `dsql.<operation>` span, boxing the
/// future first.
///
/// A repository method names its operation with this rather than
/// `#[instrument]`. `#[instrument]` on an async fn keeps its body's future
/// twice in the future it returns, once bare and once instrumented (one is
/// left behind), and a caller builds that future on its stack before boxing or
/// polling it. An operation's future can hold whole run images across awaits,
/// so in an unoptimized build the doubled copy overflows a 2 MiB tokio worker
/// stack. Boxed, the instrumented future is a pointer and a span.
pub fn in_operation<F: Future>(span: Span, future: F) -> Instrumented<Pin<Box<F>>> {
    debug_assert!(
        span.metadata()
            .is_none_or(|metadata| metadata.name().starts_with(OPERATION_SPAN_PREFIX)),
        "an operation span is named `{OPERATION_SPAN_PREFIX}<operation>`"
    );
    Box::pin(future).instrument(span)
}

fn accounted_filter() -> FilterFn<fn(&Metadata<'_>) -> bool> {
    filter_fn(accounted as fn(&Metadata<'_>) -> bool).with_max_level_hint(MAX_LEVEL)
}

/// The per-callsite decision; `FilterFn` caches it, so it costs nothing after a
/// callsite's first use.
fn accounted(metadata: &Metadata<'_>) -> bool {
    // FilterFn asserts its decisions stay within its level hint.
    if *metadata.level() > MAX_LEVEL {
        return false;
    }
    if metadata.is_span() {
        return metadata.name().starts_with(OPERATION_SPAN_PREFIX)
            || metadata.fields().field(COMMAND_TYPE_FIELD).is_some();
    }
    // Events, and the hints `tracing::enabled!` asks with: sqlx emits a
    // statement only after `enabled!(target: "sqlx::query", ..)` says yes.
    metadata.target() == STATEMENT_TARGET
}

#[derive(Clone, Copy, Debug)]
struct StatementAccounting;

/// What caused the work under a span, from its `tokeira.command_type`.
struct Cause(Arc<str>);

/// The tally of an outermost operation span. Nested operation spans carry none:
/// their statements are charged here.
struct Operation {
    name: &'static str,
    /// Statements since the operation's last transaction ended.
    statements: u32,
    /// Whether a `BEGIN` among them was reported, so it isn't added again.
    begin_reported: bool,
}

impl Operation {
    /// Count one statement; at the end of a transaction, return the round trips
    /// it closes.
    fn tally(&mut self, statement: StatementKind) -> Option<(u32, Transaction)> {
        self.statements = self.statements.saturating_add(1);
        match statement {
            StatementKind::Other => None,
            StatementKind::Begin => {
                self.begin_reported = true;
                None
            }
            StatementKind::End(transaction) => {
                let round_trips = self
                    .statements
                    .saturating_add(u32::from(!self.begin_reported));
                self.statements = 0;
                self.begin_reported = false;
                Some((round_trips, transaction))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transaction {
    Committed,
    RolledBack,
    None,
}

impl Transaction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementKind {
    Begin,
    End(Transaction),
    Other,
}

impl StatementKind {
    /// Classify a statement by its summary. `ROLLBACK TO SAVEPOINT` ends no
    /// transaction; Aurora DSQL has no savepoints, but sqlx's nested
    /// transactions would send one.
    fn of(summary: &str) -> Self {
        let mut words = summary.split_ascii_whitespace();
        let Some(first) = words.next() else {
            return Self::Other;
        };
        let second = words.next();
        if first.eq_ignore_ascii_case("COMMIT") {
            Self::End(Transaction::Committed)
        } else if first.eq_ignore_ascii_case("ROLLBACK")
            && !second.is_some_and(|word| word.eq_ignore_ascii_case("TO"))
        {
            Self::End(Transaction::RolledBack)
        } else if first.eq_ignore_ascii_case("BEGIN")
            || (first.eq_ignore_ascii_case("START")
                && second.is_some_and(|word| word.eq_ignore_ascii_case("TRANSACTION")))
        {
            Self::Begin
        } else {
            Self::Other
        }
    }
}

impl<S> Layer<S> for StatementAccounting
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        if attrs
            .metadata()
            .fields()
            .field(COMMAND_TYPE_FIELD)
            .is_some()
        {
            record_cause(&span, |visitor| attrs.record(visitor));
        }
        let Some(name) = span.name().strip_prefix(OPERATION_SPAN_PREFIX) else {
            return;
        };
        let nested = span
            .scope()
            .skip(1)
            .any(|ancestor| ancestor.extensions().get::<Operation>().is_some());
        if !nested {
            span.extensions_mut().insert(Operation {
                name,
                statements: 0,
                begin_reported: false,
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(field) = span.metadata().fields().field(COMMAND_TYPE_FIELD)
            && values.contains(&field)
        {
            record_cause(&span, |visitor| values.record(visitor));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut summary = Summary::default();
        event.record(&mut summary);
        let operation_span = ctx.event_scope(event).and_then(|mut scope| {
            scope.find(|span| span.extensions().get::<Operation>().is_some())
        });
        let Some(operation_span) = operation_span else {
            metrics::counter!(DSQL_UNATTRIBUTED_STATEMENTS_TOTAL).increment(1);
            return;
        };
        let ended = {
            let mut extensions = operation_span.extensions_mut();
            extensions.get_mut::<Operation>().and_then(|operation| {
                operation
                    .tally(summary.0)
                    .map(|(round_trips, transaction)| (operation.name, round_trips, transaction))
            })
        };
        if let Some((name, round_trips, transaction)) = ended {
            record_round_trips(name, cause_of(&operation_span), transaction, round_trips);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let operation = span.extensions_mut().remove::<Operation>();
        if let Some(operation) = operation
            && operation.statements > 0
        {
            record_round_trips(
                operation.name,
                cause_of(&span),
                Transaction::None,
                operation.statements,
            );
        }
    }
}

fn record_cause<S>(span: &SpanRef<'_, S>, record: impl FnOnce(&mut CauseField))
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let mut cause = CauseField::default();
    record(&mut cause);
    if let Some(cause) = cause.0 {
        span.extensions_mut().replace(Cause(cause));
    }
}

/// The cause named nearest the operation, the operation's own span included.
/// Resolved when a sample is recorded, so a cause recorded after the operation
/// opened still counts.
fn cause_of<S>(span: &SpanRef<'_, S>) -> SharedString
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    span.scope()
        .find_map(|span| {
            span.extensions()
                .get::<Cause>()
                .map(|cause| SharedString::from_shared(Arc::clone(&cause.0)))
        })
        .unwrap_or(SharedString::const_str(NO_CAUSE))
}

fn record_round_trips(
    operation: &'static str,
    cause: SharedString,
    transaction: Transaction,
    round_trips: u32,
) {
    metrics::histogram!(
        DSQL_OPERATION_ROUND_TRIPS,
        "operation" => operation,
        "command_type" => cause,
        "transaction" => transaction.as_str(),
    )
    .record(f64::from(round_trips));
}

/// Reads a statement event's kind from its `summary`, without copying it.
struct Summary(StatementKind);

impl Default for Summary {
    fn default() -> Self {
        Self(StatementKind::Other)
    }
}

impl Visit for Summary {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == SUMMARY_FIELD {
            self.0 = StatementKind::of(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

#[derive(Default)]
struct CauseField(Option<Arc<str>>);

impl Visit for CauseField {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == COMMAND_TYPE_FIELD {
            self.0 = Some(Arc::from(value));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == COMMAND_TYPE_FIELD {
            self.0 = Some(Arc::from(format!("{value:?}")));
        }
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::{
        CompositeKey, MetricKind,
        debugging::{DebugValue, DebuggingRecorder},
    };
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    type Sample = (String, String, String, Vec<f64>);

    fn statement(summary: &str) {
        tracing::event!(target: "sqlx::query", Level::DEBUG, summary, db.statement = "");
    }

    /// Run `body` under a subscriber holding only the accounting layer, and
    /// return the round-trip samples and the unattributed count it recorded.
    fn account(body: impl FnOnce()) -> (Vec<Sample>, u64) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let subscriber = tracing_subscriber::registry().with(statement_accounting_layer());
        metrics::with_local_recorder(&recorder, || {
            tracing::subscriber::with_default(subscriber, body);
        });
        let mut samples = Vec::new();
        let mut unattributed = 0;
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let key: CompositeKey = key;
            match (key.kind(), value) {
                (MetricKind::Histogram, DebugValue::Histogram(values)) => {
                    let label = |name: &str| {
                        key.key()
                            .labels()
                            .find(|label| label.key() == name)
                            .map(|label| label.value().to_owned())
                            .unwrap_or_default()
                    };
                    samples.push((
                        label("operation"),
                        label("command_type"),
                        label("transaction"),
                        values.into_iter().map(|value| value.into_inner()).collect(),
                    ));
                }
                (MetricKind::Counter, DebugValue::Counter(count)) => unattributed = count,
                _ => {}
            }
        }
        samples.sort_by(|left, right| left.partial_cmp(right).expect("finite samples"));
        (samples, unattributed)
    }

    fn sample(operation: &str, cause: &str, transaction: &str, values: &[f64]) -> Sample {
        (
            operation.to_owned(),
            cause.to_owned(),
            transaction.to_owned(),
            values.to_vec(),
        )
    }

    #[test]
    fn a_commit_counts_its_statements_and_the_unreported_begin() {
        let (samples, unattributed) = account(|| {
            let lane = tracing::info_span!("lane.process", tokeira.command_type = "Signal");
            let _lane = lane.enter();
            let commit = tracing::info_span!("dsql.commit_transition");
            let _commit = commit.enter();
            statement("SELECT owner, epoch FROM …");
            statement("UPDATE workflow_hot SET state_data …");
            statement("COMMIT");
        });

        assert_eq!(
            samples,
            vec![sample("commit_transition", "Signal", "committed", &[4.0])]
        );
        assert_eq!(unattributed, 0);
    }

    #[test]
    fn nested_operations_charge_the_outermost() {
        let (samples, _) = account(|| {
            let outer = tracing::info_span!("dsql.commit_transition_for_bundle");
            let _outer = outer.enter();
            statement("SELECT owner, epoch FROM …");
            {
                let inner = tracing::info_span!("dsql.commit_transition");
                let _inner = inner.enter();
                statement("UPDATE workflow_hot SET state_data …");
            }
            statement("ROLLBACK");
        });

        assert_eq!(
            samples,
            vec![sample(
                "commit_transition_for_bundle",
                "none",
                "rolled_back",
                &[4.0]
            )]
        );
    }

    #[test]
    fn each_transaction_is_a_sample_and_the_rest_closes_with_the_span() {
        let (samples, _) = account(|| {
            let read = tracing::debug_span!(
                "runtime.activity_task_started",
                tokeira.command_type = "ActivityTaskStarted"
            );
            let _read = read.enter();
            let operation = tracing::info_span!("dsql.load_run");
            let _operation = operation.enter();
            statement("SELECT shard_id, transition_seq, state_data …");
            statement("COMMIT");
            statement("BEGIN");
            statement("SELECT state_data FROM …");
            statement("COMMIT");
            statement("SELECT 1");
        });

        assert_eq!(
            samples,
            vec![
                sample("load_run", "ActivityTaskStarted", "committed", &[3.0, 3.0]),
                sample("load_run", "ActivityTaskStarted", "none", &[1.0]),
            ]
        );
    }

    #[test]
    fn statements_outside_operations_are_unattributed() {
        let (samples, unattributed) = account(|| {
            let lane = tracing::info_span!("lane.process", tokeira.command_type = "Start");
            let _lane = lane.enter();
            statement("SELECT run_key FROM …");
            statement("SELECT 1");
        });

        assert!(samples.is_empty());
        assert_eq!(unattributed, 2);
    }

    #[test]
    fn a_cause_recorded_after_the_span_opened_counts() {
        let (samples, _) = account(|| {
            let lane =
                tracing::info_span!("lane.process", tokeira.command_type = tracing::field::Empty);
            let _lane = lane.enter();
            let operation = tracing::info_span!("dsql.renew_bundle");
            let _operation = operation.enter();
            lane.record("tokeira.command_type", "TimerDue");
            statement("UPDATE shard_lease SET …");
        });

        assert_eq!(
            samples,
            vec![sample("renew_bundle", "TimerDue", "none", &[1.0])]
        );
    }

    proptest::proptest! {
        // Feature: dsql-observability-metrics, Property 7: Round-trip accounting
        #[test]
        fn round_trips_sum_to_statements_plus_unreported_begins(
            summaries in proptest::collection::vec(
                proptest::sample::select(vec!["SELECT 1", "BEGIN", "COMMIT", "ROLLBACK"]),
                0..40,
            ),
        ) {
            let (samples, unattributed) = account(|| {
                let operation = tracing::info_span!("dsql.load_run");
                let _operation = operation.enter();
                for summary in &summaries {
                    statement(summary);
                }
            });

            let mut unreported_begins = 0;
            let mut begun = false;
            for summary in &summaries {
                match *summary {
                    "BEGIN" => begun = true,
                    "COMMIT" | "ROLLBACK" => {
                        unreported_begins += usize::from(!begun);
                        begun = false;
                    }
                    _ => {}
                }
            }
            let of = |transaction: &str| {
                samples
                    .iter()
                    .filter(|sample| sample.2 == transaction)
                    .flat_map(|sample| sample.3.iter().copied())
                    .collect::<Vec<_>>()
            };
            let count = |summary: &str| summaries.iter().filter(|s| **s == summary).count();
            let total: f64 = samples.iter().flat_map(|sample| sample.3.iter()).sum();
            proptest::prop_assert_eq!(total as usize, summaries.len() + unreported_begins);
            proptest::prop_assert_eq!(of("committed").len(), count("COMMIT"));
            proptest::prop_assert_eq!(of("rolled_back").len(), count("ROLLBACK"));
            proptest::prop_assert!(of("none").len() <= 1);
            proptest::prop_assert_eq!(unattributed, 0);
        }
    }

    #[test]
    fn statement_kinds_follow_the_summary() {
        assert_eq!(
            StatementKind::of("COMMIT"),
            StatementKind::End(Transaction::Committed)
        );
        assert_eq!(
            StatementKind::of("rollback"),
            StatementKind::End(Transaction::RolledBack)
        );
        assert_eq!(
            StatementKind::of("ROLLBACK TO SAVEPOINT _sqlx_savepoint_1"),
            StatementKind::Other
        );
        assert_eq!(StatementKind::of("BEGIN"), StatementKind::Begin);
        assert_eq!(
            StatementKind::of("START TRANSACTION ISOLATION LEVEL …"),
            StatementKind::Begin
        );
        assert_eq!(
            StatementKind::of("SELECT commit FROM …"),
            StatementKind::Other
        );
        assert_eq!(StatementKind::of(""), StatementKind::Other);
    }

    #[test]
    fn the_filter_admits_only_what_the_layer_reads() {
        let (samples, unattributed) = account(|| {
            assert!(tracing::enabled!(target: "sqlx::query", Level::DEBUG));
            assert!(!tracing::enabled!(target: "sqlx::query", Level::TRACE));
            assert!(!tracing::enabled!(target: "tokeira_storage", Level::DEBUG));
            let operation = tracing::trace_span!("dsql.load_run");
            let _operation = operation.enter();
            statement("SELECT 1");
        });

        assert!(samples.is_empty(), "a TRACE operation span is not read");
        assert_eq!(unattributed, 1);
    }
}
