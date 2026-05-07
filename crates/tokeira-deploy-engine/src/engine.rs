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
            let resolved_ref = format!("{}:{}", desired.repository, desired.tag);
            let source = match image.source_type() {
                crate::ImageSourceType::Build => tokeira_iac::ImageSource::Built,
                crate::ImageSourceType::Mirror => {
                    let upstream_ref = desired.upstream_ref.clone().ok_or_else(|| {
                        RuntimeError::Image(format!(
                            "image '{}' is Mirror but desired_ref.upstream_ref is None",
                            image.name()
                        ))
                    })?;
                    tokeira_iac::ImageSource::Mirrored { upstream_ref }
                }
                crate::ImageSourceType::Registry => tokeira_iac::ImageSource::PullThrough {
                    upstream_ref: desired.upstream_ref.clone().unwrap_or_default(),
                },
            };
            state.images.insert(
                image.name().to_string(),
                tokeira_iac::ImageState {
                    name: image.name().to_string(),
                    resolved_ref,
                    digest: None,
                    published_at: chrono::Utc::now().to_rfc3339(),
                    source,
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
    use crate::{
        DesiredImageRef, Image, ImageContext, ImageSourceType, RuntimeError, Service,
        ServiceContext, validate_registry,
    };
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn block_on_ready<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(NoopWaker));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future unexpectedly pending in synchronous test"),
        }
    }

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

    #[derive(Debug)]
    struct MockImage {
        name: &'static str,
        repository: &'static str,
        tag: &'static str,
        source_type: ImageSourceType,
        upstream_ref: Option<&'static str>,
    }

    impl Image for MockImage {
        fn name(&self) -> &str {
            self.name
        }

        fn source_type(&self) -> ImageSourceType {
            self.source_type
        }

        fn desired_ref(&self, _ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError> {
            Ok(DesiredImageRef {
                repository: self.repository.to_string(),
                tag: self.tag.to_string(),
                upstream_ref: self.upstream_ref.map(ToString::to_string),
            })
        }
    }

    #[test]
    fn validate_registry_rejects_duplicate_names() {
        let images: Vec<Box<dyn Image>> = vec![
            Box::new(MockImage {
                name: "same",
                repository: "repo-a",
                tag: "latest",
                source_type: ImageSourceType::Build,
                upstream_ref: None,
            }),
            Box::new(MockImage {
                name: "same",
                repository: "repo-b",
                tag: "latest",
                source_type: ImageSourceType::Build,
                upstream_ref: None,
            }),
        ];

        let error =
            validate_registry(&images, &ImageContext::default()).expect_err("duplicate name fails");
        assert_eq!(
            error.to_string(),
            "Image error: image registry validation failed: duplicate name = same"
        );
    }

    #[test]
    fn validate_registry_rejects_duplicate_repositories() {
        let images: Vec<Box<dyn Image>> = vec![
            Box::new(MockImage {
                name: "build",
                repository: "same-repo",
                tag: "latest",
                source_type: ImageSourceType::Build,
                upstream_ref: None,
            }),
            Box::new(MockImage {
                name: "mirror",
                repository: "same-repo",
                tag: "latest",
                source_type: ImageSourceType::Mirror,
                upstream_ref: Some("upstream:latest"),
            }),
        ];

        let error = validate_registry(&images, &ImageContext::default())
            .expect_err("duplicate repository fails");
        assert_eq!(
            error.to_string(),
            "Image error: image registry validation failed: duplicate repository = same-repo"
        );
    }

    #[test]
    fn record_images_maps_structured_refs_to_state() {
        let engine = ServiceEngine::new();
        let ctx = ImageContext::default();
        let mut state = tokeira_iac::RuntimeState::default();
        let images: Vec<Box<dyn Image>> = vec![
            Box::new(MockImage {
                name: "built",
                repository: "project/built",
                tag: "latest",
                source_type: ImageSourceType::Build,
                upstream_ref: None,
            }),
            Box::new(MockImage {
                name: "mirrored",
                repository: "project/mirrored",
                tag: "v1",
                source_type: ImageSourceType::Mirror,
                upstream_ref: Some("source/mirrored:v1"),
            }),
            Box::new(MockImage {
                name: "registry",
                repository: "public.example/registry",
                tag: "stable",
                source_type: ImageSourceType::Registry,
                upstream_ref: Some("public.example/registry:stable"),
            }),
        ];

        block_on_ready(engine.record_images(&images, &ctx, &mut state))
            .expect("image recording succeeds");

        let built = state.images.get("built").expect("built image recorded");
        assert_eq!(built.resolved_ref, "project/built:latest");
        assert!(matches!(built.source, tokeira_iac::ImageSource::Built));

        let mirrored = state
            .images
            .get("mirrored")
            .expect("mirrored image recorded");
        assert_eq!(mirrored.resolved_ref, "project/mirrored:v1");
        assert!(matches!(
            &mirrored.source,
            tokeira_iac::ImageSource::Mirrored {
                upstream_ref
            } if upstream_ref == "source/mirrored:v1"
        ));

        let registry = state
            .images
            .get("registry")
            .expect("registry image recorded");
        assert_eq!(registry.resolved_ref, "public.example/registry:stable");
        assert!(matches!(
            &registry.source,
            tokeira_iac::ImageSource::PullThrough {
                upstream_ref
            } if upstream_ref == "public.example/registry:stable"
        ));
    }

    #[test]
    fn record_images_errors_when_mirror_has_no_upstream() {
        let engine = ServiceEngine::new();
        let mut state = tokeira_iac::RuntimeState::default();
        let images: Vec<Box<dyn Image>> = vec![Box::new(MockImage {
            name: "bad-mirror",
            repository: "project/bad-mirror",
            tag: "latest",
            source_type: ImageSourceType::Mirror,
            upstream_ref: None,
        })];

        let ctx = ImageContext::default();
        let error = engine.record_images(&images, &ctx, &mut state);
        let error = block_on_ready(error).expect_err("bad mirror should fail");

        assert_eq!(
            error.to_string(),
            "Image error: image 'bad-mirror' is Mirror but desired_ref.upstream_ref is None"
        );
    }
}
