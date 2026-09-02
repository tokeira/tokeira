//! `tkr port-forward` reporting for the local in-process platform.
//!
//! Definition-bound platforms own forwarding through the married
//! provisioner and never enter this handler. The retained local platform
//! already publishes ports on the operator's host, so `--local-port` only
//! applies to bound platforms that create a real tunnel.

use anyhow::Result;

use crate::deployment_dir::DeploymentContext;

use super::PlatformOps;

pub(crate) async fn run(
    service: &str,
    _local_port: Option<u16>,
    ctx: DeploymentContext,
) -> Result<()> {
    let ops = PlatformOps::from_context(&ctx)?;
    let mappings = ops.port_mappings(service).await?;
    if mappings.is_empty() {
        println!("no port mappings for service {service}");
    } else {
        for mapping in mappings {
            println!(
                "{}:{} -> {}:{}/{}",
                mapping.host_addr,
                mapping.host_port,
                service,
                mapping.container_port,
                mapping.protocol
            );
        }
    }
    Ok(())
}
