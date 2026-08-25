//! Post-commit lifecycle publications (deployment-repository spec, Req 4).
//!
//! After a transition's envelope CAS has committed, the deployment's next
//! Deployment Publication is written to its repository — a derived
//! projection, never an authority: a publication failure is reported with
//! its remedy (`tkr deployment publish`) and MUST NOT fail or unwind the
//! committed transition. Seats without `state/repository/publisher.json`
//! (fetched read-only seats, pre-repository deployments) publish nothing.

use std::path::Path;

use tokeira_deployment::repository::{
    assemble::{ClaimInputs, claim_from_dir, publication_input_from_dir},
    claim::Transition,
    config::{RepositoryConfig, TRUST_ANCHOR},
    open::{Freshness, open},
    publish::publish_transition,
};

use tokeira_platform::definition::DefinitionFrontend;

use crate::{engine::Engine, platform::Admitted};

/// Publish the committed state as the next publication. Infallible by
/// contract: every failure becomes the pending report, never an error —
/// the transition this follows has already committed.
pub(crate) async fn publish_committed_transition<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    transition: Transition,
    config_revision: u64,
) {
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    let config = match RepositoryConfig::load(deployment_dir) {
        Ok(Some(config)) => config,
        // No publisher configuration: this seat does not publish.
        Ok(None) => return,
        Err(error) => {
            report_pending(&format!("publisher configuration unreadable: {error}"));
            return;
        }
    };
    // The identity facts come from the engine's own evaluation of the
    // committed documents — the sole implementation, same as check.
    let execution = match engine.execution(admitted, None) {
        Ok(execution) => execution,
        Err(error) => {
            report_pending(&format!("committed definition did not evaluate: {error}"));
            return;
        }
    };
    match try_publish(
        deployment_dir,
        &config,
        transition,
        config_revision,
        execution.configuration_identity,
        execution.served_companions,
    )
    .await
    {
        Ok(version) => {
            eprintln!(
                "repository: publication {version} written to {}",
                config.locator.display()
            );
        }
        Err(error) => report_pending(&error),
    }
}

async fn try_publish(
    deployment_dir: &Path,
    config: &RepositoryConfig,
    transition: Transition,
    config_revision: u64,
    identity: tokeira_platform::definition::ConfigurationIdentity,
    companions: Vec<String>,
) -> Result<u64, String> {
    let anchor_path = deployment_dir.join(TRUST_ANCHOR);
    let trusted_root = std::fs::read(&anchor_path)
        .map_err(|error| format!("pinned trust anchor unreadable ({error})"))?;
    // The current version comes from the repository itself, verified from
    // the pinned anchor — publishers never track versions in mutable local
    // state.
    let opened = open(
        &config.locator,
        &trusted_root,
        Some(&deployment_dir.join(tokeira_deployment::repository::config::DATASTORE_DIR)),
        Freshness::Enforced,
        None,
    )
    .await
    .map_err(|error| format!("repository load refused: {error}"))?;
    let expected_version = opened.version();

    let claim = claim_from_dir(
        deployment_dir,
        &ClaimInputs {
            identity,
            companions,
            transition,
            config_revision,
        },
    )
    .map_err(|error| error.to_string())?;
    let input =
        publication_input_from_dir(deployment_dir, claim).map_err(|error| error.to_string())?;
    let receipt = publish_transition(config, input, expected_version, Some(&trusted_root), None)
        .await
        .map_err(|error| error.to_string())?;
    Ok(receipt.version)
}

fn report_pending(reason: &str) {
    eprintln!(
        "repository: the transition is committed, but its publication failed and is pending: \
         {reason}\ncomplete it with `tkr deployment publish`"
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokeira_deployment::{
        BinaryArtifactDescriptor, BuildAuthority, BuildManifest, BuildProfile, EngineIdentity,
        ProvisionerBundle, Sha256Digest, Target, TestEvidence,
        repository::{
            config::{DATASTORE_DIR, RepositoryConfig, TRUST_ANCHOR},
            keys::RoleKeyConfig,
            locator::RepositoryLocator,
            publish::author_trust_anchor,
        },
    };

    use super::*;

    /// Give the testkit deployment a committed engine: a fake retained
    /// artifact and its sidecar manifest — exactly what the assembly reads.
    fn stage_fake_engine(dir: &std::path::Path) {
        let engine_bytes = b"fake-engine-bytes";
        let identity = EngineIdentity {
            source_closure: Sha256Digest::from_bytes(b"source"),
            lock_closure: Sha256Digest::from_bytes(b"lock"),
            toolchain: "rustc test".to_string(),
            build_container: None,
            features: BTreeSet::new(),
            profile: BuildProfile::Dev,
        };
        let retrieval_ref = format!("binaries/{}/test-triple", identity.digest().to_hex());
        let retained = dir.join("state").join(&retrieval_ref);
        std::fs::create_dir_all(retained.parent().expect("parent")).expect("retention dir");
        std::fs::write(&retained, engine_bytes).expect("retained artifact");
        let bundle = ProvisionerBundle {
            identity,
            bound: None,
            authority: BuildAuthority::LocalDeveloper,
            provisioner_version: "0.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target("test-triple".to_string()),
                sha256: tokeira_deployment::sha256_hex(engine_bytes),
                retrieval_ref: Some(retrieval_ref),
                size_bytes: engine_bytes.len() as u64,
            }],
            tests: TestEvidence {
                command: "not run (test fixture)".to_string(),
                passed: false,
            },
            build: BuildManifest {
                request_id: "req-test".to_string(),
                source_tree_oid: "t".to_string(),
                snapshot_commit_oid: "c".to_string(),
                toolchain: "rustc test".to_string(),
                builder: "test".to_string(),
            },
        };
        std::fs::write(
            dir.join(tokeira_deployment::BUNDLE_MANIFEST_BASENAME),
            serde_json::to_vec_pretty(&bundle).expect("manifest serializes"),
        )
        .expect("sidecar writes");
    }

    /// Provision publisher state exactly as create does, and write the
    /// birth publication so lifecycle hooks have a lineage to extend.
    async fn provision_and_birth(
        dir: &std::path::Path,
        engine: &Engine<crate::testkit::StubFrontend>,
        admitted: &Admitted,
    ) -> (RepositoryConfig, Vec<u8>) {
        let keys = RoleKeyConfig::generate_local(&dir.join("repo-keys")).expect("keygen");
        let config = RepositoryConfig {
            locator: RepositoryLocator::Local {
                path: dir.join("repository"),
            },
            keys,
            lifetimes: Default::default(),
        };
        let anchor = author_trust_anchor(&config).await.expect("anchor");
        let anchor_path = dir.join(TRUST_ANCHOR);
        std::fs::create_dir_all(anchor_path.parent().expect("parent")).expect("state dir");
        std::fs::write(&anchor_path, &anchor).expect("pin");
        std::fs::create_dir_all(dir.join(DATASTORE_DIR)).expect("datastore");
        config.store(dir).expect("publisher.json");

        let execution = engine.execution(admitted, None).expect("evaluates");
        let claim = claim_from_dir(
            dir,
            &ClaimInputs {
                identity: execution.configuration_identity,
                companions: execution.served_companions,
                transition: Transition::Create,
                config_revision: 0,
            },
        )
        .expect("claim assembles");
        let input = publication_input_from_dir(dir, claim).expect("input assembles");
        let receipt = publish_transition(&config, input, 0, Some(&anchor), None)
            .await
            .expect("birth publication");
        assert_eq!(receipt.version, 1);
        (config, anchor)
    }

    // Feature: deployment-repository, Requirement 4 — apply/revert sequences
    // publish committed state: transitions and config_revision in claims;
    // revert content equals the reverted-to publication.
    #[tokio::test]
    async fn apply_and_revert_publish_their_committed_transitions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        // Realize creation before the fake sidecar lands: admission
        // self-verifies a placed manifest, and the fake engine is not the test
        // process.
        crate::testkit::realize_creation(&admitted).await;
        stage_fake_engine(tmp.path());
        let (config, anchor) = provision_and_birth(tmp.path(), &engine, &admitted).await;

        let mode = tokeira_report::Mode::resolve(false, false);
        // Revision 1, then revision 2, then revert to 1.
        std::fs::write(tmp.path().join("definition.tkd"), "// one\n").expect("rev 1");
        crate::apply::apply(&engine, &admitted, None, false, mode, None)
            .await
            .expect("apply rev 1");
        std::fs::write(tmp.path().join("definition.tkd"), "// two\n").expect("rev 2");
        crate::apply::apply(&engine, &admitted, None, false, mode, None)
            .await
            .expect("apply rev 2");
        crate::revert::revert(&engine, &admitted, 1)
            .await
            .expect("revert to rev 1");

        // Every committed transition extended the lineage: create (1),
        // apply (2), apply (3), revert (4).
        let opened = open(&config.locator, &anchor, None, Freshness::Enforced, None)
            .await
            .expect("repository opens");
        assert_eq!(opened.version(), 4, "three lifecycle publications landed");
        let publication = opened
            .verified_publication()
            .await
            .expect("the final publication verifies");
        let claim = publication.claim();
        assert_eq!(claim.transition, Transition::Revert);
        assert_eq!(
            claim.config_revision, 3,
            "revert is a forward config revision"
        );
        // Revert content equals the reverted-to state (Req 4.3): the
        // published root is revision 1's document.
        let published_root = publication
            .read(&claim.definition.root)
            .await
            .expect("published root");
        assert_eq!(published_root, b"// one\n".to_vec());
    }
}
