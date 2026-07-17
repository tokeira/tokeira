//! `tkr logs <service>` — fetch recent logs for a service on the
//! selected platform.
//!
//! Follow and tail are best-effort: the CLI accepts them for parity with
//! the interfaces operators expect, but the current local and compose
//! providers only return a snapshot. Enhancements go in the provider's
//! `logs*` implementation in `platforms/*`.

use anyhow::Result;

use crate::deployment_dir::DeploymentContext;

pub(crate) async fn run(
    service: &str,
    follow: bool,
    tail: Option<u32>,
    ctx: DeploymentContext,
) -> Result<()> {
    if follow {
        eprintln!(
            "log follow is not supported by the current local provider; printing recent logs"
        );
    }
    if let Some(tail) = tail {
        eprintln!(
            "tail={tail} is accepted by the CLI but provider-side tailing is not yet supported"
        );
    }
    let ops = super::PlatformOps::from_context(&ctx)?;
    for line in ops.logs(service).await? {
        println!("{line}");
    }
    Ok(())
}
