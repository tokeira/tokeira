//! `tkr config show` — dump the two TOML files for the selected deployment.
//!
//! Intentionally trivial: the output format is the source text itself so
//! operators see exactly what `tokeirad` and the deployment engine will
//! load. Useful when chasing "why did my change not take effect" and for
//! pasting into bug reports.

use std::fs;

use anyhow::Result;

use crate::deployment_dir::{DEPLOYMENT_TOML, DeploymentContext, TOKEIRAD_TOML};

pub(crate) fn run_show(ctx: DeploymentContext) -> Result<()> {
    println!(
        "# {}\n{}",
        ctx.path.join(DEPLOYMENT_TOML).display(),
        fs::read_to_string(ctx.path.join(DEPLOYMENT_TOML))?
    );
    println!(
        "\n# {}\n{}",
        ctx.path.join(TOKEIRAD_TOML).display(),
        fs::read_to_string(ctx.path.join(TOKEIRAD_TOML))?
    );
    Ok(())
}
