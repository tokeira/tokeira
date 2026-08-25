//! Docker CLI context resolution for the local Compose provider.
//!
//! Bollard understands `DOCKER_HOST` but does not read the Docker CLI's
//! selected context. Desktop runtimes such as OrbStack publish their Unix
//! socket only through that context, so silently falling back to
//! `/var/run/docker.sock` can target a different, inactive runtime.
//!
//! Bollard merged equivalent context support in fussybeaver/bollard#724, but
//! no released crate contains it yet. Remove this module once the workspace
//! adopts a release carrying those constructors.

use std::{
    env,
    path::{Path, PathBuf},
};

use bollard::{API_DEFAULT_VERSION, Docker};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const DOCKER_TIMEOUT_SECONDS: u64 = 120;

/// One admitted local Docker endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DockerEndpoint {
    /// Docker's conventional local socket.
    LocalDefault,
    /// An explicit Unix socket from the environment or selected CLI context.
    Unix(String),
}

impl DockerEndpoint {
    /// Resolve Docker's environment overrides, then its persisted current
    /// context, preserving the CLI's precedence.
    pub(crate) fn resolve() -> Result<Self, String> {
        let context = unicode_env("DOCKER_CONTEXT")?;
        let host = unicode_env("DOCKER_HOST")?;
        let config_dir = env::var_os("DOCKER_CONFIG")
            .map(PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".docker")));
        Self::resolve_from(context.as_deref(), host.as_deref(), config_dir.as_deref())
    }

    fn resolve_from(
        context_override: Option<&str>,
        host_override: Option<&str>,
        config_dir: Option<&Path>,
    ) -> Result<Self, String> {
        if let Some(context) = context_override.filter(|context| !context.is_empty()) {
            return context_endpoint(context, config_dir);
        }
        if let Some(host) = host_override.filter(|host| !host.is_empty()) {
            return Self::from_uri(host, "DOCKER_HOST");
        }
        let Some(config_dir) = config_dir else {
            return Ok(Self::LocalDefault);
        };
        let config_path = config_dir.join("config.json");
        let bytes = match std::fs::read(&config_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::LocalDefault);
            }
            Err(error) => {
                return Err(format!(
                    "cannot read Docker CLI configuration {}: {error}",
                    config_path.display()
                ));
            }
        };
        let config: DockerCliConfig = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "Docker CLI configuration {} is invalid: {error}",
                config_path.display()
            )
        })?;
        match config.current_context.as_deref() {
            Some(context) if !context.is_empty() && context != "default" => {
                context_endpoint(context, Some(config_dir))
            }
            _ => Ok(Self::LocalDefault),
        }
    }

    fn from_uri(uri: &str, authority: &str) -> Result<Self, String> {
        if uri.starts_with("unix://") {
            return Ok(Self::Unix(uri.to_string()));
        }
        Err(format!(
            "{authority} selects unsupported Docker endpoint `{uri}`; the local Compose \
             platform requires a Unix-socket context"
        ))
    }

    /// Construct the SDK client without mutating process-global environment.
    pub(crate) fn connect(&self) -> Result<Docker, bollard::errors::Error> {
        match self {
            Self::LocalDefault => Docker::connect_with_local_defaults(),
            Self::Unix(uri) => {
                Docker::connect_with_unix(uri, DOCKER_TIMEOUT_SECONDS, API_DEFAULT_VERSION)
            }
        }
    }

    /// Operator-facing endpoint evidence retained for later probe failures.
    pub(crate) fn label(&self) -> &str {
        match self {
            Self::LocalDefault => "unix:///var/run/docker.sock",
            Self::Unix(uri) => uri,
        }
    }
}

#[derive(Debug, Deserialize)]
struct DockerCliConfig {
    #[serde(rename = "currentContext")]
    current_context: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DockerContextMetadata {
    #[serde(rename = "Endpoints")]
    endpoints: DockerContextEndpoints,
}

#[derive(Debug, Deserialize)]
struct DockerContextEndpoints {
    docker: DockerContextEndpoint,
}

#[derive(Debug, Deserialize)]
struct DockerContextEndpoint {
    #[serde(rename = "Host")]
    host: String,
}

fn context_endpoint(context: &str, config_dir: Option<&Path>) -> Result<DockerEndpoint, String> {
    if context == "default" {
        return Ok(DockerEndpoint::LocalDefault);
    }
    let Some(config_dir) = config_dir else {
        return Err(format!(
            "Docker context `{context}` is selected but no Docker configuration directory is available"
        ));
    };
    let digest = hex::encode(Sha256::digest(context.as_bytes()));
    let metadata_path = config_dir
        .join("contexts/meta")
        .join(digest)
        .join("meta.json");
    let bytes = std::fs::read(&metadata_path).map_err(|error| {
        format!(
            "cannot read selected Docker context `{context}` at {}: {error}",
            metadata_path.display()
        )
    })?;
    let metadata: DockerContextMetadata = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "selected Docker context `{context}` at {} is invalid: {error}",
            metadata_path.display()
        )
    })?;
    DockerEndpoint::from_uri(
        &metadata.endpoints.docker.host,
        "the selected Docker context",
    )
}

fn unicode_env(name: &str) -> Result<Option<String>, String> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(format!("{name} is not valid UTF-8")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "tokeira-compose-docker-context-{}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("fixture directory");
            Self(path)
        }

        fn context(&self, name: &str, host: &str) {
            std::fs::write(
                self.0.join("config.json"),
                format!(r#"{{"currentContext":"{name}"}}"#),
            )
            .expect("CLI config");
            let digest = hex::encode(Sha256::digest(name.as_bytes()));
            let parent = self.0.join("contexts/meta").join(digest);
            std::fs::create_dir_all(&parent).expect("context directory");
            std::fs::write(
                parent.join("meta.json"),
                format!(r#"{{"Endpoints":{{"docker":{{"Host":"{host}"}}}}}}"#),
            )
            .expect("context metadata");
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove fixture");
        }
    }

    #[test]
    fn selected_context_resolves_its_unix_socket() {
        let fixture = Fixture::new();
        fixture.context("orbstack", "unix:///runtime/orbstack.sock");

        let endpoint =
            DockerEndpoint::resolve_from(None, None, Some(&fixture.0)).expect("selected context");

        assert_eq!(
            endpoint,
            DockerEndpoint::Unix("unix:///runtime/orbstack.sock".to_string())
        );
    }

    #[test]
    fn context_override_precedes_docker_host() {
        let fixture = Fixture::new();
        fixture.context("orbstack", "unix:///runtime/orbstack.sock");

        let endpoint = DockerEndpoint::resolve_from(
            Some("orbstack"),
            Some("unix:///runtime/other.sock"),
            Some(&fixture.0),
        )
        .expect("context override");

        assert_eq!(
            endpoint,
            DockerEndpoint::Unix("unix:///runtime/orbstack.sock".to_string())
        );
    }

    #[test]
    fn explicit_unix_host_precedes_persisted_context() {
        let fixture = Fixture::new();
        fixture.context("orbstack", "unix:///runtime/orbstack.sock");

        let endpoint = DockerEndpoint::resolve_from(
            None,
            Some("unix:///runtime/explicit.sock"),
            Some(&fixture.0),
        )
        .expect("explicit host");

        assert_eq!(
            endpoint,
            DockerEndpoint::Unix("unix:///runtime/explicit.sock".to_string())
        );
    }

    #[test]
    fn unsupported_context_endpoint_is_actionable() {
        let fixture = Fixture::new();
        fixture.context("remote", "tcp://docker.example:2375");

        let error = DockerEndpoint::resolve_from(None, None, Some(&fixture.0))
            .expect_err("remote endpoint is outside local Compose");

        assert!(error.contains("requires a Unix-socket context"));
        assert!(error.contains("tcp://docker.example:2375"));
    }
}
