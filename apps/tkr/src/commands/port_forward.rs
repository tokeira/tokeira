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
