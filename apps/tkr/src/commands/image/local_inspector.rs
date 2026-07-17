//! Abstraction over "does this image exist in the local Docker daemon?".
//!
//! [`tkr image push`][super::run_push] checks for a local copy of the
//! built image before attempting to push it to ECR — it's a cheap
//! pre-flight that turns a confusing Dagger failure into a clear
//! "run `tkr image build` first" error.
//!
//! The inspector is a trait so tests can substitute a mock
//! (`MockLocalImageInspector`, `#[cfg(test)]`) instead of shelling out
//! to `docker`.

use anyhow::{Context, Result};

#[async_trait::async_trait]
pub(crate) trait LocalImageInspector: Send + Sync {
    async fn image_exists(&self, image_ref: &str) -> Result<bool>;
}

/// Production implementation that shells out to the `docker` CLI.
///
/// Returns `true` when `docker image inspect <ref>` exits successfully,
/// `false` when docker reports "No such image", and surfaces any other
/// failure as an error (so operators see a real diagnostic rather than a
/// silent "image is missing" that in fact means "docker is dead").
#[derive(Debug)]
pub(crate) struct DockerCliInspector;

#[async_trait::async_trait]
impl LocalImageInspector for DockerCliInspector {
    async fn image_exists(&self, image_ref: &str) -> Result<bool> {
        let output = tokio::process::Command::new("docker")
            .args(["image", "inspect", image_ref])
            .output()
            .await
            .with_context(|| format!("failed to run `docker image inspect {image_ref}`"))?;
        if output.status.success() {
            return Ok(true);
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("No such image") {
            return Ok(false);
        }
        Err(anyhow::anyhow!(
            "`docker image inspect {image_ref}` failed: {stderr}"
        ))
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MockLocalImageInspector {
    exists: bool,
    calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

#[cfg(test)]
impl MockLocalImageInspector {
    pub(crate) fn new(exists: bool) -> Self {
        Self {
            exists,
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls lock").clone()
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl LocalImageInspector for MockLocalImageInspector {
    async fn image_exists(&self, image_ref: &str) -> Result<bool> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(image_ref.to_owned());
        Ok(self.exists)
    }
}
