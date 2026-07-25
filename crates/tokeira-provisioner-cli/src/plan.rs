//! `tkp plan` — surface the binding verdict + the infrastructure plan (read-only;
//! annotates but never gates/refuses, Req 2.5).

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::{ProvenanceStamp, check_binding};
use tokeira_report::Mode;

use crate::{ProvisionerPlatform, envelope_store, render::PlanReport};

pub(crate) async fn plan<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    mode: Mode,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(deployment_dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    let verdict = check_binding(envelope.binding.as_ref(), &running);
    let changes = platform.infra_plan(deployment_dir).await?;
    let report = PlanReport::new(
        platform.label(deployment_dir).to_string(),
        "infra plan",
        &envelope,
        verdict,
        changes,
    );
    print!("{}", tokeira_report::render(&report, mode)?);
    Ok(())
}
