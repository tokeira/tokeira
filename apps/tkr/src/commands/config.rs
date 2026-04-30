use std::fs;

use anyhow::Result;

use crate::deployment_dir::{DEPLOYMENT_TOML, DeploymentContext, TOKEIRAD_TOML};

pub fn run_show(ctx: DeploymentContext) -> Result<()> {
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
