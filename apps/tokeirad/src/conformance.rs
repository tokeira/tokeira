//! Conformance-harness assembly for the served daemon.
//!
//! The functional-conformance harness (spec
//! `.kiro/specs/conformance-config-override/`) drives a real `tokeirad` over
//! the wire and needs live dynamic-config overrides, environment-driven
//! authorization callbacks, and an out-of-band control listener. The engine
//! and its published dependencies expose inert seams for all of that
//! (`tokeira_engine::harness`, `tokeira_edge::conformance::overrides`,
//! `tokeira_runtime::conformance`); this module — compiled only under the
//! app-level `conformance` feature — is the one place that links the
//! never-published override machinery and installs it into those seams before
//! boot. A production `tokeirad` build contains none of this.

use std::sync::Arc;

use anyhow::Result;
use tokeira_config::Cli;
use tokeira_engine::harness::{self, HarnessHooks};

use crate::{conformance_grpc_authenticator, conformance_nexus_authorizer};

/// Serve from the CLI with the conformance harness seams installed.
pub async fn run(cli: Cli) -> Result<()> {
    install_harness();
    tokeira_engine::run_from_cli(cli).await
}

/// Install every harness seam. Idempotent: each seam keeps its first install.
fn install_harness() {
    tokeira_edge::conformance::overrides::install(
        tokeira_edge::conformance::overrides::OverrideReads {
            read_bool: |key| tokeira_conformance::overrides().get_bool(key),
            read_i64: |key| tokeira_conformance::overrides().get_i64(key),
            read_duration: |key| tokeira_conformance::overrides().get_duration(key),
            read_json: |key| tokeira_conformance::overrides().get_json(key),
        },
    );
    tokeira_runtime::conformance::install(tokeira_runtime::conformance::OverrideReads {
        read_bool: |key| tokeira_conformance::overrides().get_bool(key),
        read_i64: |key| tokeira_conformance::overrides().get_i64(key),
        read_f64: |key| tokeira_conformance::overrides().get_f64(key),
        read_duration: |key| tokeira_conformance::overrides().get_duration(key),
        read_scope_generation: || tokeira_conformance::overrides().scope_generation(),
    });
    harness::install(HarnessHooks {
        fallback_grpc_authenticator:
            conformance_grpc_authenticator::ConformanceGrpcAuthenticator::from_environment()
                .map(|authenticator| Arc::new(authenticator) as _),
        wrap_nexus_http_authorizer: Some(Box::new(|production| {
            conformance_nexus_authorizer::ConformanceNexusHttpAuthorizer::from_environment(
                production.clone(),
            )
            .map(|authorizer| {
                Arc::new(authorizer) as Arc<dyn tokeira_edge::nexus_http::NexusHttpAuthorizer>
            })
            .unwrap_or(production)
        })),
        force_chasm_timer_sweeper: true,
        background_task: Some(Box::new(spawn_control_listener)),
    });
}

/// Mount the dynamic-config control service, if the fork harness enabled it.
///
/// The harness sets `TOKEIRA_CONFORMANCE_CONTROL_ADDR` to a concrete loopback
/// address when booting `tokeirad` for the corpus; its presence enables the
/// control listener and its value is the bind address. Mounted on a SEPARATE
/// loopback listener — never the public gRPC router. Like the wire-coverage
/// enable seam this is a test-harness switch, not a production configuration
/// surface: it carries no dynamic-config value, only where to listen.
fn spawn_control_listener(control_cancel: tokio_util::sync::CancellationToken) {
    let Some(control_addr) = conformance_control_addr() else {
        return;
    };
    tokio::spawn(async move {
        let router = connectrpc::Router::new().add_service(std::sync::Arc::new(
            tokeira_conformance_control::ConformanceControlHandler,
        ));
        match connectrpc::Server::bind(control_addr).await {
            Ok(bound) => {
                if let Err(error) = bound
                    .serve_with_graceful_shutdown(router, control_cancel.cancelled_owned())
                    .await
                {
                    tracing::error!(%error, "conformance control service exited with error");
                }
            }
            Err(error) => tracing::error!(
                %error,
                %control_addr,
                "failed to bind conformance control listener"
            ),
        }
    });
    tracing::warn!(%control_addr, "conformance control service mounted (conformance build)");
}

fn conformance_control_addr() -> Option<std::net::SocketAddr> {
    std::env::var("TOKEIRA_CONFORMANCE_CONTROL_ADDR")
        .ok()
        .and_then(|value| value.trim().parse().ok())
}
