//! Terminal UI for long-running engine operations.
//!
//! The IaC and deploy engines are headless: they raise progress events
//! through callback hooks registered on a [`ProvisionContext`]. This module
//! installs those hooks to render either spinners + pretty lines (human
//! mode) or newline-delimited JSON events (`--json` mode), while also
//! maintaining counters for the final summary line.
//!
//! # Two output paths
//!
//! - **Human, terminal attached** — each resource gets a live spinner via
//!   `indicatif`; completion flips the spinner to a green `OK` or red
//!   `FAIL` line and stops animation.
//! - **Human, non-terminal** (e.g. piping to a log file) — no spinners,
//!   lines are written line-by-line to stderr so they interleave
//!   predictably with captured output.
//! - **JSON** — every event type in [`ProgressEvent`] is emitted as a
//!   single JSON object per line on stdout. Stable schema (`#[serde(tag =
//!   "event", rename_all = "snake_case")]`).
//!
//! # Adding a new event
//!
//! 1. Add a variant to [`ProgressEvent`].
//! 2. Register the corresponding hook on [`ActionTuiHandle::install`].
//! 3. Extend the property test in this module so round-trip coverage keeps
//!    up with the wire schema.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use console::{Term, style};
use indicatif::{MultiProgress, ProgressBar};
use serde::{Deserialize, Serialize};
use tokeira_iac::{ProvisionContext, ResourceId};

/// Selects between pretty human output and newline-delimited JSON events.
///
/// Propagated from the global `--json` flag all the way down to each
/// progress hook so the engine layer doesn't have to know about render
/// modes at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Json,
}

#[derive(Debug, Default)]
struct ActionCounters {
    completed: AtomicUsize,
    failed: AtomicUsize,
    skipped: AtomicUsize,
}

#[derive(Debug, Default)]
struct ActiveSpinners {
    entries: Mutex<HashMap<ResourceId, SpinnerEntry>>,
}

struct SpinnerEntry {
    started_at: Instant,
    bar: Option<ProgressBar>,
}

impl std::fmt::Debug for SpinnerEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpinnerEntry")
            .field("started_at", &self.started_at)
            .field("bar", &self.bar.is_some())
            .finish()
    }
}

/// Owner of the live progress UI for a single engine operation.
///
/// Construct with [`ActionTuiHandle::new`], hand the engine's
/// `ProvisionContext` to [`ActionTuiHandle::install`], run the engine, then
/// call [`ActionTuiHandle::print_summary`] once the operation returns to
/// render the final `Done: x completed, y failed, z skipped` line.
#[derive(Debug, Clone)]
pub struct ActionTuiHandle {
    format: OutputFormat,
    multi: MultiProgress,
    start: Instant,
    counters: Arc<ActionCounters>,
    spinners: Arc<ActiveSpinners>,
    is_terminal: bool,
}

impl ActionTuiHandle {
    pub fn new(format: OutputFormat) -> Self {
        Self::with_terminal_detected(format, Term::stdout().is_term())
    }

    pub(crate) fn with_terminal_detected(format: OutputFormat, is_terminal: bool) -> Self {
        Self {
            format,
            multi: MultiProgress::new(),
            start: Instant::now(),
            counters: Arc::new(ActionCounters::default()),
            spinners: Arc::new(ActiveSpinners::default()),
            is_terminal,
        }
    }

    pub fn install(&self, ctx: &mut ProvisionContext) {
        let format = self.format;
        let multi = self.multi.clone();
        let spinners = Arc::clone(&self.spinners);
        let is_terminal = self.is_terminal;
        ctx.set_apply_progress(move |action, resource_id, resource_type, index, total| {
            let bar = if format == OutputFormat::Human && is_terminal {
                let bar = multi.add(ProgressBar::new_spinner());
                bar.set_message(format!(
                    "{action} {resource_type} {} ({index}/{total})",
                    resource_id.0
                ));
                Some(bar)
            } else {
                None
            };

            if format == OutputFormat::Human && !is_terminal {
                eprintln!(
                    "{action} {resource_type} {} ({index}/{total})",
                    resource_id.0
                );
            } else if format == OutputFormat::Json {
                emit_json_line(&ProgressEvent::OperationStart {
                    action: action.to_string(),
                    resource_id: resource_id.0.clone(),
                    resource_type: resource_type.0.clone(),
                    index,
                    total,
                });
            }

            if let Ok(mut entries) = spinners.entries.lock() {
                entries.insert(
                    resource_id.clone(),
                    SpinnerEntry {
                        started_at: Instant::now(),
                        bar,
                    },
                );
            } else {
                tracing::warn!("progress spinner registry mutex poisoned");
            }
        });

        let format = self.format;
        let counters = Arc::clone(&self.counters);
        let spinners = Arc::clone(&self.spinners);
        let is_terminal = self.is_terminal;
        ctx.set_complete_progress(move |action, resource_id, resource_type, elapsed| {
            counters.completed.fetch_add(1, Ordering::Relaxed);
            let entry = remove_spinner(&spinners, resource_id);
            if format == OutputFormat::Human && is_terminal {
                if let Some(entry) = entry
                    && let Some(bar) = entry.bar
                {
                    bar.finish_with_message(format!(
                        "{} {action} {resource_type} {} ({})",
                        style("OK").green(),
                        resource_id.0,
                        format_duration(elapsed)
                    ));
                }
            } else if format == OutputFormat::Human {
                eprintln!(
                    "OK {action} {resource_type} {} ({})",
                    resource_id.0,
                    format_duration(elapsed)
                );
            } else {
                emit_json_line(&ProgressEvent::OperationComplete {
                    action: action.to_string(),
                    resource_id: resource_id.0.clone(),
                    resource_type: resource_type.0.clone(),
                    elapsed_ms: elapsed.as_millis() as u64,
                });
            }
        });

        let format = self.format;
        let counters = Arc::clone(&self.counters);
        let spinners = Arc::clone(&self.spinners);
        let is_terminal = self.is_terminal;
        ctx.set_failed_progress(move |action, resource_id, resource_type, elapsed, err| {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            let entry = remove_spinner(&spinners, resource_id);
            if format == OutputFormat::Human && is_terminal {
                if let Some(entry) = entry
                    && let Some(bar) = entry.bar
                {
                    bar.finish_with_message(format!(
                        "{} {action} {resource_type} {} ({}): {err}",
                        style("FAIL").red(),
                        resource_id.0,
                        format_duration(elapsed)
                    ));
                }
            } else if format == OutputFormat::Human {
                eprintln!(
                    "FAIL {action} {resource_type} {} ({}): {err}",
                    resource_id.0,
                    format_duration(elapsed)
                );
            } else {
                emit_json_line(&ProgressEvent::OperationFailed {
                    action: action.to_string(),
                    resource_id: resource_id.0.clone(),
                    resource_type: resource_type.0.clone(),
                    elapsed_ms: elapsed.as_millis() as u64,
                    error: err.to_string(),
                });
            }
        });

        let format = self.format;
        let spinners = Arc::clone(&self.spinners);
        ctx.set_wait_progress(move |resource_id, resource_type, phase, elapsed, timeout| {
            if format == OutputFormat::Human {
                if let Some(bar) = spinner_bar(&spinners, resource_id) {
                    bar.set_message(format!(
                        "{phase}: {} elapsed, {} timeout",
                        format_duration(elapsed),
                        format_duration(timeout)
                    ));
                } else {
                    eprintln!(
                        "{phase} {resource_type} {}: {} elapsed, {} timeout",
                        resource_id.0,
                        format_duration(elapsed),
                        format_duration(timeout)
                    );
                }
            } else {
                emit_json_line(&ProgressEvent::WaitProgress {
                    resource_id: resource_id.0.clone(),
                    resource_type: resource_type.0.clone(),
                    phase: phase.to_string(),
                    elapsed_ms: elapsed.as_millis() as u64,
                    timeout_ms: timeout.as_millis() as u64,
                });
            }
        });

        let format = self.format;
        let multi = self.multi.clone();
        ctx.set_note_progress(move |resource_id, resource_type, message| {
            if format == OutputFormat::Human {
                let line = format!("note {resource_type} {}: {message}", resource_id.0);
                if let Err(err) = multi.println(line) {
                    tracing::warn!(%err, "failed to write progress note");
                }
            } else {
                emit_json_line(&ProgressEvent::Note {
                    resource_id: resource_id.0.clone(),
                    resource_type: resource_type.0.clone(),
                    message: message.to_string(),
                });
            }
        });
    }

    pub fn record_skipped(&self, n: usize) {
        self.counters.skipped.store(n, Ordering::Relaxed);
    }

    pub fn print_summary(&self) {
        let completed = self.counters.completed.load(Ordering::Relaxed);
        let failed = self.counters.failed.load(Ordering::Relaxed);
        let skipped = self.counters.skipped.load(Ordering::Relaxed);
        let elapsed = self.start.elapsed();
        match self.format {
            OutputFormat::Human => {
                println!(
                    "{} {completed} completed, {failed} failed, {skipped} skipped in {}",
                    style("Done:").bold(),
                    format_duration(elapsed)
                );
            }
            OutputFormat::Json => emit_json_line(&ProgressEvent::Summary {
                completed,
                failed,
                skipped,
                elapsed_ms: elapsed.as_millis() as u64,
            }),
        }
    }

    #[cfg(test)]
    fn counter_snapshot(&self) -> (usize, usize, usize) {
        (
            self.counters.completed.load(Ordering::Relaxed),
            self.counters.failed.load(Ordering::Relaxed),
            self.counters.skipped.load(Ordering::Relaxed),
        )
    }
}

/// Stable wire schema for machine-readable progress.
///
/// `#[serde(tag = "event", rename_all = "snake_case")]` means each variant
/// serialises to a flat JSON object with an `event` discriminator:
///
/// ```json
/// {"event": "operation_start", "action": "create", "resource_id": "vpc", ...}
/// {"event": "operation_complete", "action": "create", "elapsed_ms": 432, ...}
/// ```
///
/// The property test `progress_event_round_trips` asserts every variant
/// survives serde round-tripping; add new variants there whenever this enum
/// grows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ProgressEvent {
    OperationStart {
        action: String,
        resource_id: String,
        resource_type: String,
        index: usize,
        total: usize,
    },
    OperationComplete {
        action: String,
        resource_id: String,
        resource_type: String,
        elapsed_ms: u64,
    },
    OperationFailed {
        action: String,
        resource_id: String,
        resource_type: String,
        elapsed_ms: u64,
        error: String,
    },
    WaitProgress {
        resource_id: String,
        resource_type: String,
        phase: String,
        elapsed_ms: u64,
        timeout_ms: u64,
    },
    Note {
        resource_id: String,
        resource_type: String,
        message: String,
    },
    Summary {
        completed: usize,
        failed: usize,
        skipped: usize,
        elapsed_ms: u64,
    },
}

fn remove_spinner(spinners: &ActiveSpinners, resource_id: &ResourceId) -> Option<SpinnerEntry> {
    match spinners.entries.lock() {
        Ok(mut entries) => entries.remove(resource_id),
        Err(_) => {
            tracing::warn!("progress spinner registry mutex poisoned");
            None
        }
    }
}

fn spinner_bar(spinners: &ActiveSpinners, resource_id: &ResourceId) -> Option<ProgressBar> {
    match spinners.entries.lock() {
        Ok(entries) => entries.get(resource_id).and_then(|entry| entry.bar.clone()),
        Err(_) => {
            tracing::warn!("progress spinner registry mutex poisoned");
            None
        }
    }
}

fn emit_json_line(event: &ProgressEvent) {
    match serde_json::to_string(event) {
        Ok(line) => println!("{line}"),
        Err(err) => tracing::warn!(
            %err,
            ?event,
            "failed to serialise progress event; dropping"
        ),
    }
}

fn format_duration(duration: Duration) -> String {
    format!("{:.1}s", duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_iac::ResourceType;

    proptest! {
        #[test]
        fn progress_event_round_trips(event in progress_event_strategy()) {
            let encoded = serde_json::to_string(&event)?;
            let decoded: ProgressEvent = serde_json::from_str(&encoded)?;
            prop_assert_eq!(decoded, event);
        }

        #[test]
        fn progress_counters_match_recorded_events(
            completed in 0usize..100,
            failed in 0usize..100,
            skipped in 0usize..100
        ) {
            let tui = ActionTuiHandle::with_terminal_detected(OutputFormat::Human, false);
            let mut ctx = ProvisionContext::default();
            tui.install(&mut ctx);
            let resource_type = ResourceType("type".to_string());

            for idx in 0..completed {
                ctx.emit_complete_progress(
                    "create",
                    &ResourceId(format!("completed-{idx}")),
                    &resource_type,
                    Duration::ZERO,
                );
            }
            for idx in 0..failed {
                ctx.emit_failed_progress(
                    "create",
                    &ResourceId(format!("failed-{idx}")),
                    &resource_type,
                    Duration::ZERO,
                    &tokeira_iac::IacError::Other(anyhow::anyhow!("failed")),
                );
            }
            tui.record_skipped(skipped);

            let snapshot = tui.counter_snapshot();
            prop_assert_eq!(snapshot, (completed, failed, skipped));
            prop_assert_eq!(snapshot.0 + snapshot.1 + snapshot.2, completed + failed + skipped);
        }
    }

    #[test]
    fn records_skipped_count() {
        let tui = ActionTuiHandle::with_terminal_detected(OutputFormat::Human, false);
        tui.record_skipped(3);
        assert_eq!(tui.counter_snapshot(), (0, 0, 3));
    }

    #[test]
    fn terminal_mode_attaches_spinner() {
        let tui = ActionTuiHandle::with_terminal_detected(OutputFormat::Human, true);
        let mut ctx = ProvisionContext::default();
        tui.install(&mut ctx);
        let rid = ResourceId("resource".to_string());
        let rtype = ResourceType("type".to_string());
        ctx.emit_apply_progress("create", &rid, &rtype, 1, 1);
        let entries = tui.spinners.entries.lock().expect("spinner mutex");
        let entry = entries.get(&rid).expect("spinner entry");
        assert!(entry.bar.is_some());
    }

    #[test]
    fn non_terminal_mode_omits_spinner() {
        let tui = ActionTuiHandle::with_terminal_detected(OutputFormat::Human, false);
        let mut ctx = ProvisionContext::default();
        tui.install(&mut ctx);
        let rid = ResourceId("resource".to_string());
        let rtype = ResourceType("type".to_string());
        ctx.emit_apply_progress("create", &rid, &rtype, 1, 1);
        let entries = tui.spinners.entries.lock().expect("spinner mutex");
        let entry = entries.get(&rid).expect("spinner entry");
        assert!(entry.bar.is_none());
    }

    fn progress_event_strategy() -> impl Strategy<Value = ProgressEvent> {
        prop_oneof![
            any_string_tuple().prop_map(|(action, resource_id, resource_type)| {
                ProgressEvent::OperationStart {
                    action,
                    resource_id,
                    resource_type,
                    index: 1,
                    total: 2,
                }
            }),
            any_string_tuple().prop_map(|(action, resource_id, resource_type)| {
                ProgressEvent::OperationComplete {
                    action,
                    resource_id,
                    resource_type,
                    elapsed_ms: 10,
                }
            }),
            any_string_tuple().prop_map(|(action, resource_id, resource_type)| {
                ProgressEvent::OperationFailed {
                    action,
                    resource_id,
                    resource_type,
                    elapsed_ms: 10,
                    error: "failed".to_string(),
                }
            }),
            any_string_tuple().prop_map(|(_, resource_id, resource_type)| {
                ProgressEvent::WaitProgress {
                    resource_id,
                    resource_type,
                    phase: "waiting".to_string(),
                    elapsed_ms: 10,
                    timeout_ms: 20,
                }
            }),
            any_string_tuple().prop_map(|(_, resource_id, resource_type)| {
                ProgressEvent::Note {
                    resource_id,
                    resource_type,
                    message: "note".to_string(),
                }
            }),
            (0usize..100, 0usize..100, 0usize..100).prop_map(|(completed, failed, skipped)| {
                ProgressEvent::Summary {
                    completed,
                    failed,
                    skipped,
                    elapsed_ms: 10,
                }
            }),
        ]
    }

    fn any_string_tuple() -> impl Strategy<Value = (String, String, String)> {
        ("[a-z]{1,8}", "[a-z]{1,8}", "[a-z]{1,8}")
    }
}
