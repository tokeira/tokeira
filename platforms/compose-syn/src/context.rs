//! The runtime context the Engine injects on every invocation. The operator's
//! definition reads it (`cx.project_name`, `cx.region`) but never writes it —
//! it is identity + ambient, supplied by `tkp`, not an operator knob.

use std::path::PathBuf;

use crate::builder::Vol;

/// Engine-provided context, read by the operator's `deployment(cfg, cx)`.
#[derive(Clone, Debug)]
pub struct Cx {
    /// Deployment identity, from `manifest.json` (set at `tkr deployment create`).
    pub project_name: String,
    /// Recorded AWS region; `None` for an in-memory deployment.
    pub region: Option<String>,
    /// Host-supplied ambient path (never persisted). NOT surfaced to the operator
    /// `.tkd` — only the path-free helpers below are.
    pub deployment_dir: PathBuf,
}

impl Cx {
    /// The deployment's state directory (host path). Author mechanics only.
    pub fn state_dir(&self) -> PathBuf {
        self.deployment_dir.join(".tokeira-state")
    }

    /// The deployment's config directory (host path). Author mechanics only.
    pub fn config_dir(&self) -> PathBuf {
        self.deployment_dir.join("config")
    }

    /// A state-anchored volume: `<state_dir>/<sub>` mounted at `at`. The operator
    /// names only the logical anchor; the host path is resolved at realize time.
    pub fn state(&self, sub: &str, at: &str) -> Vol {
        Vol::State {
            sub: sub.into(),
            at: at.into(),
        }
    }

    /// A config-anchored volume: `<config_dir>/<sub>` mounted at `at`.
    pub fn config(&self, sub: &str, at: &str) -> Vol {
        Vol::Config {
            sub: sub.into(),
            at: at.into(),
        }
    }

    /// The Docker socket bind mount — the one vetted raw `host:container` constant,
    /// so the operator surface stays fully path-free.
    pub fn docker_sock(&self) -> Vol {
        Vol::Raw("/var/run/docker.sock:/var/run/docker.sock".into())
    }
}
