//! Progress presentation for `tkr image build`.
//!
//! Bootstrap and Dagger remain independent headless operations. This module turns
//! their coarse phases and the SDK's sanitized diagnostic stream into one bounded
//! operator-facing status line. Progress always goes to stderr so the successful
//! command result—and especially `--json` stdout—remains machine-readable.

use std::{
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use console::{Term, strip_ansi_codes, style};
use dagger_sdk::{Diagnostic, DiagnosticSink, DiagnosticSinkError, DiagnosticStream};
use indicatif::{ProgressBar, ProgressStyle};

const MAX_DETAIL_CHARS: usize = 120;

#[derive(Debug, Default)]
struct ProgressState {
    phase: Option<String>,
    bar: Option<ProgressBar>,
    last_detail: Option<String>,
}

/// Shared progress owner for one image build.
///
/// The Dagger SDK can deliver diagnostics from background stream readers, while
/// bootstrap phases run on Tokio's blocking pool. A single mutex keeps updates
/// ordered and prevents competing terminal redraws.
#[derive(Debug)]
pub(super) struct ImageBuildProgress {
    is_terminal: bool,
    state: Mutex<ProgressState>,
}

impl ImageBuildProgress {
    pub(super) fn new() -> Self {
        Self::with_terminal_detected(Term::stderr().is_term())
    }

    fn with_terminal_detected(is_terminal: bool) -> Self {
        Self {
            is_terminal,
            state: Mutex::new(ProgressState::default()),
        }
    }

    pub(super) fn announce(&self, image: &str, arch: &str) {
        if self.is_terminal {
            eprintln!("{}", style(format!("Building {image} for {arch}")).bold());
            eprintln!();
        } else {
            eprintln!("image build: building {image} for {arch}");
        }
    }

    pub(super) fn start_phase(&self, phase: impl Into<String>) {
        let phase = phase.into();
        let mut state = self.lock_state();
        clear_bar(&mut state);
        state.last_detail = None;
        state.phase = Some(phase.clone());

        if self.is_terminal {
            let bar = ProgressBar::new_spinner();
            bar.set_style(spinner_style());
            bar.set_message(phase);
            bar.enable_steady_tick(Duration::from_millis(80));
            state.bar = Some(bar);
        } else {
            eprintln!("image build: {phase}...");
        }
    }

    pub(super) fn clear_phase(&self) {
        let mut state = self.lock_state();
        clear_bar(&mut state);
        state.phase = None;
        state.last_detail = None;
    }

    pub(super) fn finish_phase(&self, message: impl AsRef<str>) {
        let message = message.as_ref();
        let mut state = self.lock_state();
        clear_bar(&mut state);
        state.phase = None;
        state.last_detail = None;
        if self.is_terminal {
            eprintln!("{} {message}", style("✓").green());
        } else {
            eprintln!("image build: {message}");
        }
    }

    pub(super) fn fail_phase(&self, message: impl AsRef<str>) {
        let message = message.as_ref();
        let mut state = self.lock_state();
        clear_bar(&mut state);
        state.phase = None;
        state.last_detail = None;
        if self.is_terminal {
            eprintln!("{} {message}", style("✗").red());
        } else {
            eprintln!("image build: {message}");
        }
    }

    fn update_detail(&self, detail: String) {
        if !self.is_terminal {
            return;
        }
        let mut state = self.lock_state();
        if state.last_detail.as_deref() == Some(&detail) {
            return;
        }
        let Some(phase) = state.phase.clone() else {
            return;
        };
        if let Some(bar) = &state.bar {
            bar.set_message(format!("{phase} — {detail}"));
            state.last_detail = Some(detail);
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ProgressState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl DiagnosticSink for ImageBuildProgress {
    fn emit(&self, diagnostic: Diagnostic<'_>) -> Result<(), DiagnosticSinkError> {
        if matches!(
            diagnostic.stream,
            DiagnosticStream::Stdout | DiagnosticStream::Stderr
        ) && let Some(detail) = diagnostic_detail(diagnostic.payload)
        {
            self.update_detail(detail);
        }
        Ok(())
    }
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {wide_msg}")
        .expect("the image-build spinner template is static and valid")
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

fn clear_bar(state: &mut ProgressState) {
    if let Some(bar) = state.bar.take() {
        bar.finish_and_clear();
    }
}

fn diagnostic_detail(payload: &[u8]) -> Option<String> {
    let decoded = String::from_utf8_lossy(payload);
    let stripped = strip_ansi_codes(&decoded);
    stripped
        .split(['\r', '\n'])
        .rev()
        .filter_map(normalize_diagnostic_line)
        .next()
}

fn normalize_diagnostic_line(line: &str) -> Option<String> {
    let line = line.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = line.to_ascii_lowercase();
    let detail_start = [
        "compiling ",
        "downloading ",
        "downloaded ",
        "finished ",
        "building ",
        "exporting ",
        "importing ",
        "uploading ",
        "loading ",
        "resolving ",
        "transferring ",
    ]
    .iter()
    .filter_map(|marker| lower.find(marker))
    .min()?;
    let mut detail = line[detail_start..].to_owned();
    if detail.starts_with("Compiling ")
        && let Some(path_start) = detail.find(" (/app/")
    {
        detail.truncate(path_start);
    }
    Some(truncate_chars(&detail, MAX_DETAIL_CHARS))
}

fn truncate_chars(value: &str, maximum: usize) -> String {
    if value.chars().count() <= maximum {
        return value.to_owned();
    }
    let mut truncated = value
        .chars()
        .take(maximum.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_select_meaningful_lines_and_strip_terminal_control() {
        let detail = diagnostic_detail(
            b"\x1b[2Knoise\r\x1b[32m25 : | [5.3s] | Compiling tokeira-runtime v0.1.0 (/app/crates/tokeira-runtime)\x1b[0m\n",
        )
        .expect("compilation line should be selected");

        assert_eq!(detail, "Compiling tokeira-runtime v0.1.0");
        assert!(diagnostic_detail(b"session heartbeat\n").is_none());
    }

    #[test]
    fn diagnostics_are_bounded_without_splitting_unicode() {
        let line = format!("building {}", "🔥".repeat(MAX_DETAIL_CHARS));
        let detail = normalize_diagnostic_line(&line).expect("build line should be selected");

        assert_eq!(detail.chars().count(), MAX_DETAIL_CHARS);
        assert!(detail.ends_with('…'));
    }

    #[test]
    fn non_terminal_progress_accepts_diagnostics_without_retaining_a_phase_detail() {
        let progress = ImageBuildProgress::with_terminal_detected(false);
        progress.start_phase("Building tokeirad");
        let detail = diagnostic_detail(b"Compiling fixture v0.1.0\n")
            .expect("compilation line should be selected");
        progress.update_detail(detail);

        assert!(progress.lock_state().last_detail.is_none());
    }
}
