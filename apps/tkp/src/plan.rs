//! `tkp plan` — surface the binding verdict + the infrastructure plan (read-only;
//! annotates but never gates/refuses, Req 2.5).

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::{ProvenanceStamp, check_binding};

use crate::{apply::deployment_identity, envelope_store, platform};

pub async fn plan(deployment_dir: &Path) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(deployment_dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    let resolved = platform::detect(deployment_dir);
    let verdict = check_binding(envelope.binding.as_ref(), &running);
    println!("platform: {}", resolved.label());
    println!(
        "binding:  {verdict:?}{}",
        if verdict.proceeds() {
            " — apply would proceed"
        } else {
            " — apply would REFUSE"
        }
    );

    let project_name = deployment_identity(&envelope.deployment_id);
    let changes = platform::infra_plan(deployment_dir, &project_name).await?;
    println!("infra plan: {} change(s)", changes.len());
    for change in &changes {
        println!(
            "  {:?} [{}] {}::{}",
            change.kind, change.resource_type, change.module, change.resource
        );
    }
    Ok(())
}
