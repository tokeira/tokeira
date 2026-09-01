//! Dispatch of one bound deployment's platform-owned observability checks.
//!
//! Admission happens once in the CLI shell. This module evaluates and realizes
//! the recorded definition in memory, then delegates the entire check to the
//! platform capability. The framework defines no observability stack or check
//! categories. No provider probe, gate, operation lock, or lifecycle method is
//! involved.

use std::time::Duration;

use anyhow::{Result, bail};
use tokeira_platform::declaration::ObservabilityCheckStatus;

use crate::{engine::Engine, platform::Admitted};

pub(crate) fn check<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    timeout_seconds: u64,
) -> Result<()> {
    if timeout_seconds == 0 {
        bail!("observability check timeout must be positive");
    }

    let execution = engine.execution(admitted, None)?;
    let Some(checker) = engine.platform().observability() else {
        bail!("not applicable: this platform declares no observability check");
    };
    let resources = execution
        .resources
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let report = checker.check(
        &admitted.deployment_ref,
        &resources,
        Duration::from_secs(timeout_seconds),
    )?;

    for outcome in report.checks {
        let status = match outcome.status {
            ObservabilityCheckStatus::Pass => "PASS",
            ObservabilityCheckStatus::Warn => "WARN",
        };
        println!("{status} {} - {}", outcome.name, outcome.detail);
    }
    Ok(())
}
