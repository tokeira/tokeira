//! Gated teardown of infrastructure and the complete live deployment.
//!
//! Every destroy is a mutating verb, so the binding gate runs **before any provider
//! mutation** (versioned deployments refuse on a non-`Match` verdict; dev
//! deployments take the permissive `DevIterate` warn-and-proceed path — the same
//! policy as `apply`). Because a teardown is irreversible, it additionally
//! requires an explicit `--yes` confirmation.
//!
//! `infra destroy` removes only the substrate. The aggregate `tkp destroy`,
//! invoked by `tkr deployment destroy`, removes services first and then the
//! substrate. It deliberately leaves deployment records to `tkr`, which
//! removes the local directory only after the provisioner exits successfully;
//! operator-owned remote state remains retained under its recorded prefix.
//!
//! The engine identity binding is retained (the running binary is still the
//! authority over the now-empty state) and `effective_config_ref` is cleared to
//! record that nothing is currently applied; `config_revision` is a property of
//! the *configuration*, which the teardown does not change, so it is left as-is.

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_deployment::ProvenanceStamp;

use tokeira_platform::definition::DefinitionFrontend;

use crate::{
    deploy,
    engine::Engine,
    gate::{GateOutcome, evaluate_gate},
    platform::Admitted,
};

/// Tear down the complete live footprint while retaining the deployment
/// records needed to resume a partial failure.
///
/// Workloads stand on infrastructure, so teardown reverses the creation
/// sequence. The owning `tkr` process removes the directory only after this
/// function returns success; `tkp` cannot delete the binary and definition it
/// is actively using.
pub(crate) async fn destroy_deployment<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    yes: bool,
    mode: tokeira_report::Mode,
) -> Result<()> {
    if !yes {
        anyhow::bail!(
            "`deployment destroy` tears down services and infrastructure and is irreversible; \
             re-run with `--yes` to confirm"
        );
    }
    ordered_teardown(
        || deploy::deploy_destroy(engine, admitted, true, mode),
        || destroy(engine, admitted, None, true),
    )
    .await
}

async fn ordered_teardown<D, DFut, I, IFut>(deploy: D, infra: I) -> Result<()>
where
    D: FnOnce() -> DFut,
    DFut: std::future::Future<Output = Result<()>>,
    I: FnOnce() -> IFut,
    IFut: std::future::Future<Output = Result<()>>,
{
    deploy().await?;
    infra().await
}

pub(crate) async fn destroy<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    module: Option<&str>,
    yes: bool,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = admitted.state.envelope_store();
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Operation-marker gate: an interrupted upgrade/rollback
    // is recovered by re-running THAT verb; everything else refuses. ──
    crate::marker::refuse_if_marked(&envelope, "destroy")?;

    // ── Gate before any mutation ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `destroy` ({verdict:?}): {reason}");
        }
        // Proceeding verdicts are silent: the gate regime is a standing fact
        // of the deployment (describe's story), not news on every verb. Only
        // a refusal earns narration — and it is the error above.
        GateOutcome::Proceed { .. } => {}
    }

    // ── Irreversible → require explicit confirmation (after the gate) ──
    if !yes {
        anyhow::bail!(
            "`destroy` tears down all of the deployment's infrastructure and is irreversible; \
             re-run with `--yes` to confirm"
        );
    }

    // ── Engine destroy ──
    let removed = engine.destroy(admitted, module).await?;
    println!(
        "[{}] infra destroy: {removed} resource(s) removed",
        engine.platform().id()
    );

    // ── Record the teardown ──
    // Retain the engine identity (it authored the empty state); clear the
    // effective config ref (nothing applied). config_revision is unchanged — the
    // configuration itself did not change, only the live footprint.
    envelope.binding = Some(running);
    envelope.effective_config_ref = None;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after destroy")?;
    println!(
        "envelope: torn down (config_revision {} retained)",
        envelope.config_revision
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope_store;
    use std::cell::RefCell;
    use tokeira_deployment::DeploymentStateEnvelope;

    #[tokio::test]
    async fn destroy_requires_confirmation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        // Dev-stamped (DevIterate proceeds through the gate) so we reach the
        // confirmation guard rather than a gate refusal.
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = destroy(&engine, &admitted, None, false)
            .await
            .expect_err("destroy without --yes refuses");
        assert!(
            err.to_string().contains("irreversible"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn destroy_refuses_an_unstamped_deployment() {
        // No binding → Unknown → gate refuses before the confirmation guard.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = destroy(&engine, &admitted, None, true)
            .await
            .expect_err("an unstamped deployment refuses");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn destroy_tears_down_local_and_clears_the_config_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 3,
            effective_config_ref: Some("sha256:deadbeef".to_string()),
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let (engine, admitted) = crate::testkit::engine(tmp.path());
        destroy(&engine, &admitted, None, true)
            .await
            .expect("local destroy succeeds");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 3, "config_revision retained");
        assert!(
            after.effective_config_ref.is_none(),
            "effective config ref cleared"
        );
        assert!(after.binding.is_some(), "engine identity retained");
    }

    #[tokio::test]
    async fn complete_teardown_runs_workloads_before_infrastructure() {
        let calls = RefCell::new(Vec::new());
        ordered_teardown(
            || async {
                calls.borrow_mut().push("deploy");
                Ok(())
            },
            || async {
                calls.borrow_mut().push("infra");
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(calls.into_inner(), ["deploy", "infra"]);
    }

    #[tokio::test]
    async fn complete_teardown_never_enters_infrastructure_after_workload_failure() {
        let calls = RefCell::new(Vec::new());
        let error = ordered_teardown(
            || async {
                calls.borrow_mut().push("deploy");
                anyhow::bail!("workload deletion failed")
            },
            || async {
                calls.borrow_mut().push("infra");
                Ok(())
            },
        )
        .await
        .expect_err("the workload failure stops the aggregate teardown");

        assert!(error.to_string().contains("workload deletion failed"));
        assert_eq!(calls.into_inner(), ["deploy"]);
    }
}
