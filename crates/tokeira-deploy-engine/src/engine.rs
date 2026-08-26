//! Stateless service lifecycle engine.
//!
//! [`ServiceEngine`] coordinates service plan/apply operations and image
//! state recording. It is stateless — the caller manages persistence.
//!
//! Services are topologically sorted by [`Service::dependencies()`] before
//! planning and applying, ensuring dependent services are processed after
//! their prerequisites. Cycle detection and missing-dependency errors use
//! the same resolution algorithm as the deploy-eks `RuntimeEngine`.

use std::{
    collections::{BTreeSet, HashSet},
    future::Future,
    pin::Pin,
};

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
    /// The service's running workload was torn down (delete-only pass —
    /// definition-driven rollback).
    Delete,
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

/// Callback invoked after one service mutation has been reflected in runtime
/// state, allowing the caller to persist partial progress before the next
/// service begins.
pub type ServiceStateSaver = Box<
    dyn Fn(
            &tokeira_iac::RuntimeState,
        ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + '_>>
        + Send
        + Sync,
>;

/// Stateless service engine.
///
/// The caller manages state persistence. This engine sorts services
/// by declared dependencies, computes plans, and delegates manifest
/// application to the supplied [`Platform`].
#[derive(Debug)]
pub struct ServiceEngine;

impl Default for ServiceEngine {
    fn default() -> Self {
        Self::new()
    }
}

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

    /// Plan services against both recorded state and the selected platform.
    ///
    /// Services are prepared and classified one at a time in dependency
    /// order. Platform preparation may resolve an image into a local cache so
    /// registry failures are reported by plan, while workload resources remain
    /// untouched.
    pub async fn plan_services_with_platform(
        &self,
        services: &[Box<dyn Service>],
        platform: &dyn Platform,
        ctx: &mut ServiceContext,
        state: &tokeira_iac::RuntimeState,
    ) -> Result<Vec<ServiceChange>, RuntimeError> {
        let ordered = ordered_services(services)?;
        let mut changes = Vec::new();
        for service in ordered {
            let manifests = service.manifests(ctx)?;
            platform.prepare_service(service.name(), &manifests).await?;
            let hash = hash_manifests(&manifests);
            let mut kind = match state.services.get(service.name()) {
                Some(existing) if existing.desired_hash == hash => ServiceChangeKind::NoChange,
                Some(_) => ServiceChangeKind::Update,
                None => ServiceChangeKind::Create,
            };
            if kind == ServiceChangeKind::NoChange
                && !platform
                    .is_service_current(service.name(), &manifests)
                    .await
            {
                kind = ServiceChangeKind::Update;
            }
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
        self.apply_services_with_saver(services, platform, ctx, state, None)
            .await
    }

    /// Apply services one at a time and optionally persist after each
    /// successful service mutation.
    ///
    /// Preparation, apply, state update, and persistence complete for service
    /// N before service N+1 begins. A failure therefore preserves the durable
    /// progress of earlier services without recording the failed service.
    pub async fn apply_services_with_saver(
        &self,
        services: &[Box<dyn Service>],
        platform: &dyn Platform,
        ctx: &mut ServiceContext,
        state: &mut tokeira_iac::RuntimeState,
        saver: Option<&ServiceStateSaver>,
    ) -> Result<Vec<ServiceChange>, RuntimeError> {
        let ordered = ordered_services(services)?;
        let mut changes = Vec::new();
        for service in ordered {
            let manifests = service.manifests(ctx)?;
            platform.prepare_service(service.name(), &manifests).await?;
            let hash = hash_manifests(&manifests);
            let mut kind = match state.services.get(service.name()) {
                Some(existing) if existing.desired_hash == hash => ServiceChangeKind::NoChange,
                Some(_) => ServiceChangeKind::Update,
                None => ServiceChangeKind::Create,
            };

            // Even when the manifest hash matches, check if the running state
            // has drifted (e.g., image rebuilt behind the same tag).
            if kind == ServiceChangeKind::NoChange
                && !platform
                    .is_service_current(service.name(), &manifests)
                    .await
            {
                kind = ServiceChangeKind::Update;
            }

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
                if let Some(save) = saver {
                    save(state).await?;
                }
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

    /// Tear down the given services' running workloads (delete-only), removing
    /// them from runtime state.
    ///
    /// This is the runtime counterpart of `Engine::destroy_selected`: the
    /// superseded binary B deletes the services it created before the binding
    /// re-pins to A (definition-driven rollback). Deletes run in
    /// reverse dependency order (dependents before dependencies). Fail-closed —
    /// if the platform cannot delete (`supports_delete() == false`) the whole
    /// pass refuses up front, before touching any workload. Idempotent to the
    /// extent the platform's `delete_service` treats an absent workload as
    /// success. `ServiceState` stores no manifest bodies, so the manifests to
    /// tear down are recomputed from each `Service`.
    pub async fn destroy_services(
        &self,
        services: &[Box<dyn Service>],
        platform: &dyn Platform,
        ctx: &mut ServiceContext,
        state: &mut tokeira_iac::RuntimeState,
    ) -> Result<Vec<ServiceChange>, RuntimeError> {
        if !services.is_empty() && !platform.supports_delete() {
            return Err(RuntimeError::Platform(
                "platform does not support service deletion; refusing delete-only pass".to_string(),
            ));
        }
        let ordered = ordered_services(services)?;
        let mut changes = Vec::new();
        for service in ordered.into_iter().rev() {
            let manifests = service.manifests(ctx)?;
            platform.delete_service(service.name(), &manifests).await?;
            state.services.remove(service.name());
            info!(
                service = service.name(),
                module = service.module(),
                "deleted service"
            );
            changes.push(ServiceChange {
                kind: ServiceChangeKind::Delete,
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
                    if !all_names.contains(dep) {
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
        task::{Context, Poll, Waker},
    };

    fn block_on_ready<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
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
        fn resource_type(&self) -> &'static str {
            "MockService"
        }

        fn name(&self) -> &str {
            self.name
        }
        fn module(&self) -> &str {
            "test"
        }
        fn dependencies(&self) -> Vec<&str> {
            self.deps.to_vec()
        }
        fn manifests(&self, _ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError> {
            Ok(vec![serde_json::json!({ "name": self.name })])
        }
    }

    #[derive(Default)]
    struct MockPlatform {
        prepared: std::sync::Mutex<Vec<String>>,
        applied: std::sync::Mutex<Vec<String>>,
        fail_prepare_on: std::sync::Mutex<Option<String>>,
        deleted: std::sync::Mutex<Vec<String>>,
        supports_delete: bool,
    }

    #[async_trait::async_trait]
    impl crate::Platform for MockPlatform {
        async fn prepare_service(
            &self,
            service_name: &str,
            _manifests: &[serde_json::Value],
        ) -> Result<(), RuntimeError> {
            self.prepared.lock().unwrap().push(service_name.to_string());
            if self.fail_prepare_on.lock().unwrap().as_deref() == Some(service_name) {
                return Err(RuntimeError::Platform(format!(
                    "image pull failed for service '{service_name}': manifest unknown"
                )));
            }
            Ok(())
        }

        async fn apply_manifests(
            &self,
            manifests: &[serde_json::Value],
        ) -> Result<usize, RuntimeError> {
            for manifest in manifests {
                if let Some(name) = manifest.get("name").and_then(serde_json::Value::as_str) {
                    self.applied.lock().unwrap().push(name.to_string());
                }
            }
            Ok(manifests.len())
        }
        fn supports_delete(&self) -> bool {
            self.supports_delete
        }
        async fn delete_service(
            &self,
            service_name: &str,
            _manifests: &[serde_json::Value],
        ) -> Result<(), RuntimeError> {
            self.deleted.lock().unwrap().push(service_name.to_string());
            Ok(())
        }
    }

    fn seed_service_state(state: &mut tokeira_iac::RuntimeState, name: &str) {
        state.services.insert(
            name.to_string(),
            tokeira_iac::ServiceState {
                name: name.to_string(),
                module: "test".to_string(),
                manifest_count: 0,
                desired_hash: String::new(),
                last_applied: String::new(),
            },
        );
    }

    fn dependent_services() -> Vec<Box<dyn Service>> {
        vec![
            Box::new(MockService {
                name: "a",
                deps: &[],
            }),
            Box::new(MockService {
                name: "b",
                deps: &["a"],
            }),
            Box::new(MockService {
                name: "c",
                deps: &["b"],
            }),
        ]
    }

    #[test]
    fn platform_aware_plan_surfaces_the_failing_service_preparation() {
        let platform = MockPlatform::default();
        *platform.fail_prepare_on.lock().unwrap() = Some("b".to_string());
        let services = dependent_services();
        let engine = ServiceEngine::new();
        let mut ctx = ServiceContext::default();
        let state = tokeira_iac::RuntimeState::default();

        let error = block_on_ready(
            engine.plan_services_with_platform(&services, &platform, &mut ctx, &state),
        )
        .expect_err("the platform's image error must fail plan");

        assert!(error.to_string().contains("manifest unknown"));
        assert_eq!(platform.prepared.lock().unwrap().as_slice(), ["a", "b"]);
        assert!(platform.applied.lock().unwrap().is_empty());
    }

    #[test]
    fn apply_persists_each_service_and_retry_resumes_after_pull_failure() {
        let platform = MockPlatform::default();
        *platform.fail_prepare_on.lock().unwrap() = Some("b".to_string());
        let services = dependent_services();
        let engine = ServiceEngine::new();
        let mut ctx = ServiceContext::default();
        let mut state = tokeira_iac::RuntimeState::default();
        let snapshots = Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
        let saved = Arc::clone(&snapshots);
        let saver: ServiceStateSaver = Box::new(move |state| {
            let mut names: Vec<String> = state.services.keys().cloned().collect();
            names.sort();
            saved.lock().unwrap().push(names);
            Box::pin(async { Ok(()) })
        });

        let error = block_on_ready(engine.apply_services_with_saver(
            &services,
            &platform,
            &mut ctx,
            &mut state,
            Some(&saver),
        ))
        .expect_err("the second service's pull fails");
        assert!(error.to_string().contains("service 'b'"));
        assert_eq!(state.services.keys().cloned().collect::<Vec<_>>(), ["a"]);
        assert_eq!(
            snapshots.lock().unwrap().as_slice(),
            [vec!["a".to_string()]]
        );
        assert_eq!(platform.applied.lock().unwrap().as_slice(), ["a"]);

        *platform.fail_prepare_on.lock().unwrap() = None;
        let changes = block_on_ready(engine.apply_services_with_saver(
            &services,
            &platform,
            &mut ctx,
            &mut state,
            Some(&saver),
        ))
        .expect("retry resumes after the persisted service");

        assert_eq!(changes[0].kind, ServiceChangeKind::NoChange);
        assert_eq!(changes[1].kind, ServiceChangeKind::Create);
        assert_eq!(changes[2].kind, ServiceChangeKind::Create);
        assert_eq!(platform.applied.lock().unwrap().as_slice(), ["a", "b", "c"]);
        assert_eq!(
            snapshots.lock().unwrap().as_slice(),
            [
                vec!["a".to_string()],
                vec!["a".to_string(), "b".to_string()],
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ]
        );
    }

    #[test]
    fn destroy_services_fails_closed_when_platform_cannot_delete() {
        // 11.3d: a platform that cannot delete refuses the whole delete-only
        // pass up front, before touching any workload.
        let platform = MockPlatform::default(); // supports_delete = false
        let services: Vec<Box<dyn Service>> = vec![Box::new(MockService {
            name: "a",
            deps: &[],
        })];
        let engine = ServiceEngine::new();
        let mut ctx = ServiceContext::default();
        let mut state = tokeira_iac::RuntimeState::default();
        let err =
            block_on_ready(engine.destroy_services(&services, &platform, &mut ctx, &mut state))
                .expect_err("fail-closed when platform can't delete");
        assert!(matches!(err, RuntimeError::Platform(_)));
    }

    #[test]
    fn destroy_services_deletes_in_reverse_dependency_order() {
        // 11.3d: delete dependents before dependencies, removing each from state.
        let platform = MockPlatform {
            deleted: std::sync::Mutex::new(Vec::new()),
            supports_delete: true,
            ..Default::default()
        };
        let services: Vec<Box<dyn Service>> = vec![
            Box::new(MockService {
                name: "history",
                deps: &[],
            }),
            Box::new(MockService {
                name: "matching",
                deps: &["history"],
            }),
            Box::new(MockService {
                name: "frontend",
                deps: &["matching"],
            }),
        ];
        let engine = ServiceEngine::new();
        let mut ctx = ServiceContext::default();
        let mut state = tokeira_iac::RuntimeState::default();
        for name in ["history", "matching", "frontend"] {
            seed_service_state(&mut state, name);
        }

        let changes =
            block_on_ready(engine.destroy_services(&services, &platform, &mut ctx, &mut state))
                .expect("destroy succeeds");

        assert!(changes.iter().all(|c| c.kind == ServiceChangeKind::Delete));
        assert!(state.services.is_empty(), "all services removed from state");
        let deleted = platform.deleted.lock().unwrap().clone();
        assert_eq!(deleted, vec!["frontend", "matching", "history"]);
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
