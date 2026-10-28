//! Peer credential generation and controller-managed daemon bootstrap.
//! Embedded construction never enters bootstrap, even with a public listener.

use std::{io::Write, os::unix::fs::PermissionsExt, path::Path, sync::Arc};

use anyhow::{Context, Result};
use tokeira_auth::peer::{PeerError, PeerMaterial, PeerVerifier};
use tokeira_config::peer::{PeerConfig, create_protected_file, load_peer_file};
use tokeira_edge::grpc::workflow_offer::PeerOfferEndpoint;

pub(crate) fn bootstrap(config: Option<&PeerConfig>) -> Result<Arc<PeerOfferEndpoint>> {
    let config = config.ok_or(PeerError::MissingMaterial)?;
    let bytes = load_peer_file(Path::new(&config.path)).map_err(|error| match error {
        // Only a definite not-found result establishes absence. Invalid paths,
        // permissions and unreadable files must not suggest automatic replacement.
        PeerError::MissingMaterial => PeerError::MissingMaterial,
        _ => PeerError::InconsistentMaterial,
    })?;
    let material = PeerMaterial::load(&bytes).map_err(|_| PeerError::InconsistentMaterial)?;
    let signer = material
        .signer(material.profile())
        .map_err(|_| PeerError::InconsistentMaterial)?;
    let verifier = PeerVerifier::new(material.profile().clone())
        .map_err(|_| PeerError::InconsistentMaterial)?;
    // Exercise the actual signing adapter before readiness, not just key parsing.
    signer
        .token_at(0)
        .map_err(|_| PeerError::InconsistentMaterial)?;
    Ok(Arc::new(PeerOfferEndpoint::new(verifier, signer)))
}

/// Only the explicit CLI command creates material. Returning path-only TOML
/// keeps private bytes out of stdout and ordinary server configuration.
pub(crate) fn generate(out: &Path) -> Result<String> {
    let path = if out.is_absolute() {
        out.to_owned()
    } else {
        std::env::current_dir()?.join(out)
    };
    let config = PeerConfig {
        path: path
            .to_str()
            .context("peer credential path must be UTF-8")?
            .to_owned(),
    };
    let output = format!("[infrastructure.peer]\n{}", toml::to_string(&config)?);
    let material = PeerMaterial::generate()?;
    material.signer(material.profile())?.token_at(0)?;
    let bytes = material.protected_bytes()?;
    // EXCL and descriptor-relative no-follow opens make refusal atomic even
    // when another generator creates the destination concurrently.
    let mut file = create_protected_file(&path)
        .context("cannot create new peer credential file; existing files are never overwritten")?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(&bytes)
        .context("cannot write peer credential file")?;
    file.sync_all()
        .context("cannot sync peer credential file")?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use proptest::prelude::*;
    use tokeira_auth::peer::REPAIR;
    use tokeira_proto::internal::runtime::{
        OfferWorkflowTaskRequest, workflow_task_dispatch_client::WorkflowTaskDispatchClient,
    };

    fn output_config(output: &str) -> PeerConfig {
        toml::from_str::<crate::TokeiraConfig>(output)
            .unwrap()
            .infrastructure
            .peer
            .unwrap()
    }

    // Feature: queue-home-delivery, Property 10
    // The actual generated file is unchanged across bootstraps; only explicit
    // generation at a new path replaces trust and revokes the previous tokens.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn generated_peer_file_restart_rotation(now in 10_000u64..1_000_000_000, restarts in 1usize..6) {
            let temporary = tempfile::tempdir().unwrap();
            let directory = temporary.path().canonicalize().unwrap();
            let path = directory.join("peer.json");
            let output = generate(&path).unwrap();
            let config = output_config(&output);
            let bytes = load_peer_file(&path).unwrap();
            let material = PeerMaterial::load(&bytes).unwrap();
            let verifier = PeerVerifier::new(material.profile().clone()).unwrap();
            let first = material.signer(material.profile()).unwrap().token_at(now).unwrap();
            for index in 0..restarts {
                prop_assert!(bootstrap(Some(&config)).is_ok());
                prop_assert!(load_peer_file(&path).unwrap() == bytes);
                let restarted = PeerMaterial::load(&load_peer_file(&path).unwrap()).unwrap();
                let later = now + index as u64 * 600;
                let token = restarted.signer(restarted.profile()).unwrap().token_at(later).unwrap();
                prop_assert!(verifier.verify_at(token.authorization(), later).is_ok());
            }
            let rotation = directory.join("replacement.json");
            let rotated_config = output_config(&generate(&rotation).unwrap());
            prop_assert!(bootstrap(Some(&rotated_config)).is_ok());
            let rotated = PeerMaterial::load(&load_peer_file(&rotation).unwrap()).unwrap();
            prop_assert_ne!(material.profile().cluster_id, rotated.profile().cluster_id);
            prop_assert!(PeerVerifier::new(rotated.profile().clone()).unwrap().verify_at(first.authorization(), now).is_err());
            let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            prop_assert!(!output.contains(document["private_pkcs8"].as_str().unwrap()));
            prop_assert!(!output.contains(first.authorization()));
        }

        // Feature: queue-home-delivery, Property 10
        // A controller-managed upgrade fails before storage/controller work
        // until an explicitly generated file supplies consistent material.
        #[test]
        fn generated_upgrade_bootstrap(mismatch in any::<bool>()) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let mut server = crate::TokeiraConfig::default();
                server.infrastructure.placement.controller_endpoint = Some("http://controller.invalid:7240".into());
                let error = crate::build_and_serve("127.0.0.1:0".parse().unwrap(), Arc::new(server)).await.unwrap_err();
                assert_diagnostic(error, PeerError::MissingMaterial, &[]);
                let temporary = tempfile::tempdir().unwrap();
                let path = temporary.path().canonicalize().unwrap().join("peer.json");
                let config = output_config(&generate(&path).unwrap());
                if mismatch {
                    let mut document: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    document["profile"]["fingerprint"] = "mismatched".into();
                    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
                }
                let result = bootstrap(Some(&config));
                assert_eq!(result.is_ok(), !mismatch);
                if let Err(error) = result { assert_diagnostic(error, PeerError::InconsistentMaterial, &[]); }
            });
        }
        // Feature: queue-home-delivery, Property 10
        // Direct single-node and embedded engines remain ready without usable
        // credentials; attaching a public listener never exposes an offer route.
        #[test]
        fn generated_no_peer_startup(embedded in any::<bool>(), listener in any::<bool>()) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let mut config = crate::TokeiraConfig::default();
                config.infrastructure.peer = Some(PeerConfig { path: "/missing-peer-credentials".into() });
                if embedded {
                    let engine = crate::Engine::start_with_config(config).await.unwrap();
                    let error = engine.endpoint().call(tokeira_edge::InProcessGrpcRequest {
                        service: "tokeira.internal.runtime.v1.WorkflowTaskDispatch".into(), rpc: "OfferWorkflowTask".into(), headers: Default::default(), proto: bytes::Bytes::new(),
                    }).await.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::Unimplemented);
                    if listener {
                        let public = engine.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
                        assert_no_offer(public.bound_addr()).await;
                        public.shutdown().await.unwrap();
                    }
                    engine.shutdown().await.unwrap();
                } else {
                    let server = crate::TokeiradHandle::start_in_memory_with_config("127.0.0.1:0".parse().unwrap(), config).await.unwrap();
                    assert_no_offer(server.bound_addr()).await;
                    server.shutdown().await.unwrap();
                }
            });
        }
    }

    #[tokio::test]
    async fn peer_generate_command_is_create_only_and_private() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("peer.json");
        let command = || {
            crate::Cli::try_parse_from([
                "tokeirad",
                "peer-credential",
                "generate",
                "--out",
                path.to_str().unwrap(),
            ])
            .unwrap()
        };
        crate::run_from_cli(command()).await.unwrap();
        let before = load_peer_file(&path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let error = crate::run_from_cli(command()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("existing files are never overwritten")
        );
        assert!(load_peer_file(&path).unwrap() == before);
        let material = PeerMaterial::load(&before).unwrap();
        let token = material
            .signer(material.profile())
            .unwrap()
            .token_at(10_000)
            .unwrap();
        PeerVerifier::new(material.profile().clone())
            .unwrap()
            .verify_at(token.authorization(), 10_000)
            .unwrap();
    }

    #[test]
    fn peer_generate_and_bootstrap_reject_symlinks_and_public_modes() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().canonicalize().unwrap();
        let path = directory.join("peer.json");
        let config = output_config(&generate(&path).unwrap());
        let before = load_peer_file(&path).unwrap();
        let link = directory.join("link.json");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(generate(&link).is_err());
        assert!(
            bootstrap(Some(&PeerConfig {
                path: link.display().to_string()
            }))
            .is_err()
        );
        let parent_link = directory.join("linked-parent");
        std::os::unix::fs::symlink(&directory, &parent_link).unwrap();
        assert!(generate(&parent_link.join("another.json")).is_err());
        assert!(
            bootstrap(Some(&PeerConfig {
                path: parent_link.join("peer.json").display().to_string()
            }))
            .is_err()
        );
        assert!(load_peer_file(&path).unwrap() == before);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_diagnostic(
            bootstrap(Some(&config)).unwrap_err(),
            PeerError::InconsistentMaterial,
            &[],
        );
    }

    #[test]
    fn peer_bootstrap_diagnostics_distinguish_absence_from_inconsistency() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("peer.json");
        let config = PeerConfig {
            path: path.display().to_string(),
        };
        assert_diagnostic(
            bootstrap(None).unwrap_err(),
            PeerError::MissingMaterial,
            &[],
        );
        assert_diagnostic(
            bootstrap(Some(&config)).unwrap_err(),
            PeerError::MissingMaterial,
            &[],
        );
        generate(&path).unwrap();
        let protected = load_peer_file(&path).unwrap();
        let mut document: serde_json::Value = serde_json::from_slice(&protected).unwrap();
        let private_key = document["private_pkcs8"].as_str().unwrap().to_owned();
        let material = PeerMaterial::load(&protected).unwrap();
        let token = material
            .signer(material.profile())
            .unwrap()
            .token_at(10_000)
            .unwrap();
        let forbidden = [
            private_key.as_str(),
            token.authorization(),
            "corrupt-protected-content",
        ];
        std::fs::write(&path, "corrupt-protected-content").unwrap();
        assert_diagnostic(
            bootstrap(Some(&config)).unwrap_err(),
            PeerError::InconsistentMaterial,
            &forbidden,
        );
        document["profile"]["fingerprint"] = "mismatched".into();
        std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
        assert_diagnostic(
            bootstrap(Some(&config)).unwrap_err(),
            PeerError::InconsistentMaterial,
            &forbidden,
        );
    }

    fn assert_diagnostic(error: anyhow::Error, expected: PeerError, forbidden: &[&str]) {
        assert_eq!(error.downcast_ref::<PeerError>(), Some(&expected));
        let diagnostic = format!("{error:#} {error:?}");
        assert!(diagnostic.contains(REPAIR));
        for content in forbidden {
            assert!(!diagnostic.contains(content));
        }
    }

    async fn assert_no_offer(addr: std::net::SocketAddr) {
        let mut client = WorkflowTaskDispatchClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let error = client
            .offer_workflow_task(OfferWorkflowTaskRequest::default())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unimplemented);
    }
}
