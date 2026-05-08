//! `tkr port-forward <service>` — report the host port mappings a
//! service publishes.
//!
//! This is **informational**, not an active forwarder: it prints the
//! mappings the operator can reach directly. The command is named
//! `port-forward` because that's the mental model operators have for
//! "how do I reach service X", and because future platforms (ECS private
//! subnets, EKS with kubectl port-forward) will upgrade this path to an
//! actual tunnelling flow without changing the CLI surface.

use anyhow::Result;

use crate::deployment_dir::DeploymentContext;

pub async fn run(service: &str, ctx: DeploymentContext) -> Result<()> {
    let ops = super::PlatformOps::from_context(&ctx)?;
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
