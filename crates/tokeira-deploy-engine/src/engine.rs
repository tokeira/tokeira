//! Stateless service lifecycle engine.
//!
//! [`ServiceEngine`] coordinates service plan/apply operations and image
//! state recording. It is stateless — the orchestrator manages persistence.
//!
//! Services are topologically sorted by [`Service::dependencies()`] before
//! planning and applying, ensuring dependent services are processed after
//! their prerequisites. Cycle detection and missing-dependency errors use
//! the same resolution algorithm as the deploy-eks `RuntimeEngine`.

use std::collections::{BTreeSet, HashSet};

use tracing::info;

use crate::{
    error::RuntimeError,
    image::{Image, ImageContext},
    platform::Platform,
    service::{Service, ServiceContext},
};

/// A planned service change for reporting.
#[derive(Debug, Clone)]
pub struct ServiceChange {
    pub kind: ServiceChangeKind,
    pub service: String,
    pub module: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceChangeKind {
    Create,
    Update,
    NoChange,
}

/// Planned manifest paired with the service that produced it.
#[derive(Debug, Clone)]
pub struct PlannedServiceManifest {
    pub service_name: String,
    pub service_module: String,
    pub manifests: Vec<serde_json::Value>,
    /// SHA-256 hex digest of the serialized manifests.
    pub desired_hash: String,
}

/// Stateless service engine.
///
/// The orchestrator manages state persistence. This engine sorts services
/// by declared dependencies, computes plans, and delegates manifest
/// application to the supplied [`Platform`].
pub struct ServiceEngine;

impl ServiceEngine {
    pub fn new() -> Self {
        Self
    }

    /// Plan service manifests without applying them.
    ///
    /// Services are topologically sorted by `Service::dependencies()`.
    pub async fn plan_services(
        &self,
        services: &[Box<dyn Service>],
        ctx: &mut ServiceContext,
        state: &tokeira_iac::RuntimeState,
    ) -> Result<Vec<ServiceChange>, RuntimeError> {
        let ordered = ordered_services(services)?;
        let mut changes = Vec::new();
        for service in ordered {
            let manifests = service.manifests(ctx)?;
            let hash = hash_manifests(&manifests);
            let kind = match state.services.get(service.name()) {
                Some(existing) if existing.desired_hash == hash => ServiceChangeKind::NoChange,
                Some(_) => ServiceChangeKind::Update,
                None => ServiceChangeKind::Create,
            };
            changes.push(ServiceChange {
                kind,
                service: service.name().to_string(),
                module: service.module().to_string(),
            });
        }
        Ok(changes)
    }

    /// Apply service manifests via the platform and update state.
    ///
    /// Services are topologically sorted by `Service::dependencies()`.
    pub async fn apply_services(
        &self,
        services: &[Box<dyn Service>],
        platform: &dyn Platform,
        ctx: &mut ServiceContext,
        state: &mut tokeira_iac::RuntimeState,
    ) -> Result<Vec<ServiceChange>, RuntimeError> {
        let ordered = ordered_services(services)?;
        let mut changes = Vec::new();
        for service in ordered {
            let manifests = service.manifests(ctx)?;
            let hash = hash_manifests(&manifests);
            let kind = match state.services.get(service.name()) {
                Some(existing) if existing.desired_hash == hash => ServiceChangeKind::NoChange,
                Some(_) => ServiceChangeKind::Update,
                None => ServiceChangeKind::Create,
            };

            if kind != ServiceChangeKind::NoChange {
                platform.apply_manifests(&manifests).await?;
                state.services.insert(
                    service.name().to_string(),
                    tokeira_iac::ServiceState {
                        name: service.name().to_string(),
                        module: service.module().to_string(),
                        manifest_count: manifests.len(),
                        desired_hash: hash,
                        last_applied: chrono::Utc::now().to_rfc3339(),
                    },
                );
                info!(
                    service = service.name(),
                    module = service.module(),
                    manifests = manifests.len(),
                    "applied service"
                );
            }

            changes.push(ServiceChange {
                kind,
                service: service.name().to_string(),
                module: service.module().to_string(),
            });
        }
        Ok(changes)
    }

    /// Record resolved images in runtime state.
    pub async fn record_images(
        &self,
        images: &[Box<dyn Image>],
        ctx: &ImageContext,
        state: &mut tokeira_iac::RuntimeState,
    ) -> Result<(), RuntimeError> {
        for image in images {
            let desired = image.desired_ref(ctx)?;
            state.images.insert(
                image.name().to_string(),
                tokeira_iac::ImageState {
                    name: image.name().to_string(),
                    resolved_ref: desired,
                    digest: None,
                    published_at: chrono::Utc::now().to_rfc3339(),
                    source: tokeira_iac::ImageSource::Built,
                },
            );
        }
        Ok(())
    }
}

// ── Service dependency ordering ───────────────────────────────────────

/// Topological sort of services by `Service::dependencies()`.
///
/// Returns services in dependency order (dependencies first). Detects
/// duplicate names, missing dependencies, and cycles with specific error
/// messages.
fn ordered_services(services: &[Box<dyn Service>]) -> Result<Vec<&dyn Service>, RuntimeError> {
    let mut seen = HashSet::new();
    let mut all_names = HashSet::new();
    for service in services {
        let name = service.name();
        if !seen.insert(name.to_string()) {
            return Err(RuntimeError::Service(format!(
                "duplicate service name '{name}' in runtime composition"
            )));
        }
        all_names.insert(name.to_string());
    }

    let mut ordered = Vec::with_capacity(services.len());
    let mut resolved = HashSet::new();
    let mut remaining: Vec<usize> = (0..services.len()).collect();

    while !remaining.is_empty() {
        let mut progressed = false;
        let mut next_remaining = Vec::new();

        for index in remaining {
            let service = services[index].as_ref();
            let deps = service.dependencies();

            if deps.iter().all(|dep| resolved.contains(*dep)) {
                ordered.push(service);
                resolved.insert(service.name().to_string());
                progressed = true;
            } else {
                next_remaining.push(index);
            }
        }

        if !progressed {
            let mut missing = BTreeSet::new();
            let mut blocked = BTreeSet::new();

            for index in &next_remaining {
                let service = services[*index].as_ref();
                blocked.insert(service.name().to_string());
                for dep in service.dependencies() {
                    if !all_names.contains(*dep) {
                        missing.insert((*dep).to_string());
                    }
                }
            }

            if !missing.is_empty() {
                return Err(RuntimeError::Service(format!(
                    "service dependency resolution failed; missing dependencies: {}",
                    missing.into_iter().collect::<Vec<_>>().join(", ")
                )));
            }

            return Err(RuntimeError::Service(format!(
                "service dependency cycle detected among: {}",
                blocked.into_iter().collect::<Vec<_>>().join(", ")
            )));
        }

        remaining = next_remaining;
    }

    Ok(ordered)
}

fn hash_manifests(manifests: &[serde_json::Value]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for manifest in manifests {
        let bytes = serde_json::to_vec(manifest).unwrap_or_default();
        hasher.update(&bytes);
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeError, Service, ServiceContext};

    #[derive(Debug)]
    struct MockService {
        name: &'static str,
        deps: &'static [&'static str],
    }

    impl Service for MockService {
        fn name(&self) -> &str {
            self.name
        }
        fn module(&self) -> &str {
            "test"
        }
        fn dependencies(&self) -> &[&str] {
            self.deps
        }
        fn manifests(&self, _ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError> {
            Ok(vec![])
        }
    }

    #[test]
    fn ordered_services_respects_declared_dependencies() {
        let services: Vec<Box<dyn Service>> = vec![
            Box::new(MockService {
                name: "frontend",
                deps: &["matching"],
            }),
            Box::new(MockService {
                name: "history",
                deps: &[],
            }),
            Box::new(MockService {
                name: "matching",
                deps: &["history"],
            }),
        ];

        let ordered = ordered_services(&services).expect("services should order");
        let names: Vec<&str> = ordered.iter().map(|service| service.name()).collect();
        assert_eq!(names, vec!["history", "matching", "frontend"]);
    }

    #[test]
    fn ordered_services_errors_on_missing_dependency() {
        let services: Vec<Box<dyn Service>> = vec![Box::new(MockService {
            name: "frontend",
            deps: &["history"],
        })];

        let error = ordered_services(&services).expect_err("missing dependency should fail");
        assert!(error.to_string().contains("missing dependencies: history"));
    }

    #[test]
    fn ordered_services_errors_on_dependency_cycle() {
        let services: Vec<Box<dyn Service>> = vec![
            Box::new(MockService {
                name: "history",
                deps: &["frontend"],
            }),
            Box::new(MockService {
                name: "frontend",
                deps: &["history"],
            }),
        ];

        let error = ordered_services(&services).expect_err("cycle should fail");
        assert!(
            error
                .to_string()
                .contains("service dependency cycle detected")
        );
    }

    #[test]
    fn ordered_services_errors_on_duplicate_name() {
        let services: Vec<Box<dyn Service>> = vec![
            Box::new(MockService {
                name: "history",
                deps: &[],
            }),
            Box::new(MockService {
                name: "history",
                deps: &[],
            }),
        ];

        let error = ordered_services(&services).expect_err("duplicate should fail");
        assert!(error.to_string().contains("duplicate service name"));
    }

    #[test]
    fn independent_services_preserve_input_order() {
        let services: Vec<Box<dyn Service>> = vec![
            Box::new(MockService {
                name: "zebra",
                deps: &[],
            }),
            Box::new(MockService {
                name: "alpha",
                deps: &[],
            }),
        ];

        let ordered = ordered_services(&services).expect("services should order");
        let names: Vec<&str> = ordered.iter().map(|service| service.name()).collect();
        // Independent services keep input order (stable sort)
        assert_eq!(names, vec!["zebra", "alpha"]);
    }
}
