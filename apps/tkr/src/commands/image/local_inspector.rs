use anyhow::{Context, Result};

#[async_trait::async_trait]
pub trait LocalImageInspector: Send + Sync {
    async fn image_exists(&self, image_ref: &str) -> Result<bool>;
}

#[derive(Debug)]
pub struct DockerCliInspector;

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
pub struct MockLocalImageInspector {
    exists: bool,
    calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

#[cfg(test)]
impl MockLocalImageInspector {
    pub fn new(exists: bool) -> Self {
        Self {
            exists,
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub fn calls(&self) -> Vec<String> {
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
