//! Spike: Pydantic Monty as a Python deployment-definition frontend
//! (`.tkdp`), with a Ruff-lowered restricted `match`.
//!
//! Standalone by design (root `AGENTS.md` task contract): no tokeira crate
//! dependencies; the deployment surface is mocked in-sandbox. The pipeline:
//!
//! ```text
//! .tkdp source
//!   → preflight   parse (ruff) + restricted-subset validation + hygiene
//!   → lower       match → if-chain splice, segmented source map
//!   → assemble    prelude + lowered user region + entrypoint driver
//!   → execute     unmodified Monty; failures mapped back to .tkdp positions
//! ```
//!
//! Monty is pinned to a git revision (dataclasses landed after the 0.0.19
//! release); the ruff crates are pinned to the exact line Monty itself pins,
//! so the parser Monty re-parses the generated program with agrees with the
//! one that produced it.

pub mod diagnostics;
pub mod lower;
pub mod preflight;
pub mod program;
pub mod runner;
pub mod source_map;

use diagnostics::Diagnostic;
use lower::LowerOptions;
use program::Program;
use runner::{RunFailure, RunOutcome};

/// Parses and validates a definition without lowering it.
pub fn check(source: &str) -> Result<preflight::Preflight, Vec<Diagnostic>> {
    preflight::preflight(source)
}

/// Full front half: preflight, lower, assemble. `file_label` is the operator
/// spelling of the definition path, used in messages and the exhaustion raise.
pub fn build_program(
    source: &str,
    file_label: &str,
    options: &LowerOptions,
) -> Result<Program, Vec<Diagnostic>> {
    let preflight = preflight::preflight(source)?;
    let lowered = lower::lower(source, &preflight.module, file_label, options);
    Ok(program::assemble(lowered, preflight.entrypoints))
}

/// Whole pipeline: build then execute under Monty.
pub fn run(
    source: &str,
    file_label: &str,
    options: &LowerOptions,
) -> Result<Result<RunOutcome, RunFailure>, Vec<Diagnostic>> {
    let program = build_program(source, file_label, options)?;
    Ok(runner::execute(&program, file_label, source))
}
