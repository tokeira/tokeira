//! CHASM registration and startup admission shared by embedded and daemon engines.
//! Search-attribute declarations are checked before registration because projection
//! registration alone preserves an existing id even when its type differs.

use std::sync::Arc;

use tokeira_chasm::{ChasmError, Registry, RegistryBuilder, SearchAttrKind};
use tokeira_projection::{SearchAttrType, VisibilityStore};
use tokeira_runtime::chasm::{RebuildStats, SideEffectExecutor};
use tokeira_types::NamespaceId;

#[cfg(feature = "chasm-extensions")]
use crate::{EmbeddedEngineConfig, Engine};
use crate::{EmbeddedEngineStartError, EmbeddedStartupPhase};

type LibraryRegistration = fn(&mut RegistryBuilder) -> Result<(), ChasmError>;

/// Inputs consumed once by the shared bootstrap before admission opens.
#[derive(Default)]
pub(crate) struct ChasmExtensions {
    /// Monomorphized registration functions; libraries need no runtime instance.
    pub libraries: Vec<LibraryRegistration>,
    /// Executors must all register before persisted effects can be rebuilt.
    pub executors: Vec<Arc<dyn SideEffectExecutor>>,
    /// Optional CHASM-only time source; absent preserves the runtime default.
    pub clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    // Test startup against previously persisted CHASM/projection state through the
    // real shared bootstrap, without exposing a second public storage boundary.
    #[cfg(test)]
    pub test_storage: Option<(
        Arc<dyn tokeira_storage::ChasmNodeRepository>,
        tokeira_projection::InMemoryVisibilityStore,
    )>,
}

impl std::fmt::Debug for ChasmExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChasmExtensions")
            .field("libraries", &self.libraries.len())
            .field("executors", &self.executors.len())
            .field("custom_clock", &self.clock.is_some())
            .finish_non_exhaustive()
    }
}

/// Opt-in, unstable CHASM extension registration over the ordinary engine bootstrap.
/// Built-in activities and their executors are always present. Registration and
/// storage admission complete before any endpoint can serve requests.
#[cfg(feature = "chasm-extensions")]
#[derive(Debug)]
pub struct EngineBuilder {
    pub(crate) config: EmbeddedEngineConfig,
    pub(crate) extensions: ChasmExtensions,
}

#[cfg(feature = "chasm-extensions")]
impl EngineBuilder {
    /// Add a library after built-in registration is sealed. Reserved names and
    /// colliding component/task identities fail the build without replacing them.
    pub fn library<L: tokeira_chasm::Library>(mut self) -> Self {
        self.extensions.libraries.push(L::register);
        self
    }

    /// Add an executor before startup rebuild. A duplicate task type fails the
    /// build; the executor must tolerate delivery of the same durable task again.
    pub fn side_effect_executor(mut self, executor: Arc<dyn SideEffectExecutor>) -> Self {
        self.extensions.executors.push(executor);
        self
    }

    /// Set the CHASM plane's nanosecond clock; workflow time remains unchanged.
    /// Transitions, pure deadlines, delayed dispatch, callback registration and
    /// sweeper passes all read this clock. It must be safe for concurrent calls.
    pub fn clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.extensions.clock = Some(clock);
        self
    }

    /// Validate configuration, select storage and start the shared service stack.
    /// Storage with unknown archetypes, incompatible attribute declarations or
    /// unserviceable outboxes is refused before opening admission.
    pub async fn build(self) -> Result<Engine, EmbeddedEngineStartError> {
        Engine::start_with_extensions(self.config, self.extensions).await
    }
}

/// Preserve storage order and counts so the first missing library is actionable.
pub(crate) fn check_registered_archetypes(
    stored: &[(u32, u64)],
    registry: &Registry,
) -> Result<(), ChasmError> {
    for &(archetype_id, executions) in stored {
        if registry.component_for_archetype(archetype_id).is_none() {
            return Err(ChasmError::UnregisteredArchetype {
                archetype_id,
                executions,
            });
        }
    }
    Ok(())
}

/// Keep projection type ownership here; the pure substrate cannot depend on it.
pub(crate) fn declared_search_attributes(registry: &Registry) -> Vec<(String, SearchAttrType)> {
    registry
        .search_attribute_defs()
        .map(|(_, def)| {
            let kind = match def.kind {
                SearchAttrKind::Keyword => SearchAttrType::Keyword,
                SearchAttrKind::KeywordList => SearchAttrType::KeywordList,
                SearchAttrKind::Int => SearchAttrType::Int,
                SearchAttrKind::Bool => SearchAttrType::Bool,
                SearchAttrKind::Double => SearchAttrType::Double,
                SearchAttrKind::Datetime => SearchAttrType::Datetime,
                SearchAttrKind::Text => SearchAttrType::Text,
            };
            (def.name.to_owned(), kind)
        })
        .collect()
}

/// Distinguish incompatible declarations from retryable projection access failures.
#[derive(Debug)]
pub(crate) enum SearchAttributeSeedError {
    Type {
        namespace: NamespaceId,
        key: String,
        declared: SearchAttrType,
        existing: SearchAttrType,
    },
    Storage(anyhow::Error),
}

impl std::fmt::Display for SearchAttributeSeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Type {
                namespace,
                key,
                declared,
                existing,
            } => write!(
                f,
                "namespace {} search attribute `{key}` declares {declared:?} but storage has {existing:?}; restore the matching declaration or correct the registered type before retrying",
                namespace.0
            ),
            Self::Storage(error) => write!(f, "cannot seed declared search attributes: {error}"),
        }
    }
}
impl std::error::Error for SearchAttributeSeedError {}

/// Preserve existing ids and refuse type drift. Earlier successful keys remain
/// registered after a later error, so repeating the same declarations is safe.
pub(crate) async fn seed_declared_search_attributes(
    store: &dyn VisibilityStore,
    namespace: NamespaceId,
    declared: &[(String, SearchAttrType)],
) -> Result<(), SearchAttributeSeedError> {
    for (key, kind) in declared {
        match store
            .resolve_attr(namespace, key)
            .await
            .map_err(SearchAttributeSeedError::Storage)?
        {
            Some(attr) if attr.attr_type != *kind => {
                return Err(SearchAttributeSeedError::Type {
                    namespace,
                    key: key.clone(),
                    declared: *kind,
                    existing: attr.attr_type,
                });
            }
            Some(_) => {}
            None => {
                store
                    .register_attr(namespace, key.clone(), *kind)
                    .await
                    .map_err(SearchAttributeSeedError::Storage)?;
            }
        }
    }
    Ok(())
}

/// Only declaration conflicts expose structured details at the embedded boundary.
pub(crate) fn seed_start_error(error: SearchAttributeSeedError) -> anyhow::Error {
    match error {
        SearchAttributeSeedError::Type {
            namespace,
            key,
            declared,
            existing,
        } => EmbeddedEngineStartError::SearchAttributeType {
            namespace,
            key,
            declared,
            existing,
        }
        .into(),
        SearchAttributeSeedError::Storage(error) => error,
    }
}

/// A partial recovery is useful periodically but cannot open startup admission.
pub(crate) fn check_rebuilt_outboxes(stats: RebuildStats) -> Result<(), EmbeddedEngineStartError> {
    if stats.unserviceable > 0 {
        let (archetype_id, cause) = stats
            .first_unserviceable
            .expect("unserviceable execution records its first cause");
        return Err(EmbeddedEngineStartError::UnserviceableOutbox {
            archetype_id,
            cause,
            executions: stats.unserviceable,
        });
    }
    Ok(())
}

/// Retain stored execution counts separately from registration collisions.
pub(crate) fn registry_start_error(error: ChasmError) -> EmbeddedEngineStartError {
    match error {
        ChasmError::UnregisteredArchetype {
            archetype_id,
            executions,
        } => EmbeddedEngineStartError::UnregisteredArchetype {
            archetype_id,
            executions,
        },
        error => EmbeddedEngineStartError::Registry(error),
    }
}

/// Preserve only the deliberately public CHASM diagnostics. All other stack
/// failures retain the embedded boundary's existing phase-only redaction.
pub(crate) fn embedded_stack_error(error: anyhow::Error) -> EmbeddedEngineStartError {
    error
        .downcast::<EmbeddedEngineStartError>()
        .unwrap_or(EmbeddedEngineStartError::Phase {
            phase: EmbeddedStartupPhase::RuntimeRestore,
        })
}

#[cfg(test)]
#[path = "../tests/support/chasm_root.rs"]
mod test_root;
#[cfg(test)]
mod tests {
    use super::{
        test_root::{Data, Root, TestLibrary},
        *,
    };
    use crate::{EmbeddedEngineConfig, Engine, VisibilityRegistryOperatorApi};
    use proptest::prelude::*;
    use prost::Message;
    use tokeira_chasm::{
        ChasmNode, Component, ExecutionKey, Library, LifecycleState, NodeMetadata,
        archetype_id_for_fqn,
    };
    use tokeira_edge::{
        InMemoryOperatorApi, operator_service::OperatorApi,
        translate::to_internal::namespace_id_for,
    };
    use tokeira_projection::InMemoryVisibilityStore;
    use tokeira_storage::{
        ChasmNodeRepository, CurrentRun, ExpectedVersion, InMemoryChasmNodeStore, NodeWrite,
    };

    fn registry(mask: [bool; 3]) -> Registry {
        let mut builder = Registry::builder();
        if mask[0] {
            TestLibrary::<0>::register(&mut builder).unwrap();
        }
        if mask[1] {
            TestLibrary::<1>::register(&mut builder).unwrap();
        }
        if mask[2] {
            TestLibrary::<2>::register(&mut builder).unwrap();
        }
        builder.build()
    }

    fn attr_type(index: u8) -> SearchAttrType {
        [
            SearchAttrType::Keyword,
            SearchAttrType::KeywordList,
            SearchAttrType::Int,
            SearchAttrType::Bool,
            SearchAttrType::Double,
            SearchAttrType::Datetime,
            SearchAttrType::Text,
        ][index as usize % 7]
    }

    // Feature: chasm-extension-archetypes, Property 13: fail-closed build
    // Admission succeeds exactly when every stored archetype is registered.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn stored_archetypes_require_registered_libraries(mask in any::<[bool;3]>(), stored in prop::collection::vec((0usize..4, 1u64..1000), 0..20)) {
            let names = [Root::<0>::FQN, Root::<1>::FQN, Root::<2>::FQN, Root::<3>::FQN];
            let pairs: Vec<_> = stored.iter().map(|(index, count)| (archetype_id_for_fqn(names[*index]), *count)).collect();
            let expected = stored.iter().position(|(index, _)| *index == 3 || !mask[*index]);
            let result = check_registered_archetypes(&pairs, &registry(mask));
            match expected {
                None => prop_assert!(result.is_ok()),
                Some(index) => {
                    prop_assert!(matches!(&result, Err(ChasmError::UnregisteredArchetype { .. })), "unregistered archetype must be named");
                    if let Err(ChasmError::UnregisteredArchetype { archetype_id, executions }) = result {
                        prop_assert_eq!((archetype_id, executions), pairs[index]);
                    }
                }
            }
        }
    }

    // Feature: chasm-extension-archetypes, Property 16: search-attribute registration
    // Seeding preserves ids/types, rejects conflicts, and covers each namespace.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn declared_attributes_seed_idempotently(entries in prop::collection::vec((0u8..7, 0u8..3), 0..12)) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let store = InMemoryVisibilityStore::default();
                let declared: Vec<_> = entries.iter().enumerate().map(|(i, (kind, _))| (format!("Declared{i}"), attr_type(*kind))).collect();
                for namespace_index in 0..3 {
                    let namespace = namespace_id_for(&format!("property-{namespace_index}"));
                    for (index, (kind, state)) in entries.iter().enumerate() {
                        if *state != 0 {
                            let kind = attr_type(kind + u8::from(*state == 2));
                            store.register_attr(namespace, declared[index].0.clone(), kind).await.unwrap();
                        }
                    }
                    let expected = entries.iter().position(|(_, state)| *state == 2);
                    for _ in 0..2 {
                        let result = seed_declared_search_attributes(&store, namespace, &declared).await;
                        match expected {
                            Some(index) => match result {
                                Err(SearchAttributeSeedError::Type { namespace: actual, key, declared: actual_kind, existing }) => {
                                    prop_assert_eq!(actual, namespace);
                                    prop_assert_eq!(&key, &declared[index].0);
                                    prop_assert_eq!(actual_kind, declared[index].1);
                                    prop_assert_eq!(existing, attr_type(entries[index].0 + 1));
                                }
                                other => prop_assert!(false, "expected mismatch, got {other:?}"),
                            },
                            None => prop_assert!(result.is_ok()),
                        }
                    }
                    if expected.is_none() {
                        let mut before = Vec::new();
                        for (key, _) in &declared { before.push(store.resolve_attr(namespace, key).await.unwrap()); }
                        seed_declared_search_attributes(&store, namespace, &declared).await.unwrap();
                        for (index, (key, kind)) in declared.iter().enumerate() {
                            let attr = store.resolve_attr(namespace, key).await.unwrap();
                            prop_assert_eq!(&attr, &before[index]);
                            prop_assert_eq!(attr.unwrap().attr_type, *kind);
                        }
                    }
                }
                let fresh = namespace_id_for("created-after-start");
                seed_declared_search_attributes(&store, fresh, &declared).await.unwrap();
                for (key, kind) in &declared { prop_assert_eq!(store.resolve_attr(fresh, key).await.unwrap().unwrap().attr_type, *kind); }
                Ok(())
            })?;
        }
    }

    async fn persisted_root(repo: &InMemoryChasmNodeStore, data: Option<Vec<u8>>) {
        let archetype_id = archetype_id_for_fqn(Root::<0>::FQN);
        let key = ExecutionKey::new("ns", "stored", "run");
        let root = ChasmNode {
            metadata: NodeMetadata::new(
                archetype_id,
                Some(LifecycleState::Running),
                Default::default(),
            ),
            data,
        };
        repo.persist_new_execution(
            &key,
            archetype_id,
            vec![NodeWrite {
                encoded_path: Vec::new(),
                node: root,
                expected: ExpectedVersion::Absent,
            }],
            CurrentRun {
                run_id: "run".into(),
                status: LifecycleState::Running,
                request_id: String::new(),
                vt_epoch: Default::default(),
            },
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn startup_refuses_unknown_archetype() {
        let repo = Arc::new(InMemoryChasmNodeStore::new());
        persisted_root(&repo, Some(Data::default().encode_to_vec())).await;
        let extensions = ChasmExtensions {
            test_storage: Some((repo.clone(), InMemoryVisibilityStore::default())),
            ..Default::default()
        };
        let error = Engine::start_with_extensions(EmbeddedEngineConfig::default(), extensions)
            .await
            .unwrap_err();
        assert!(
            matches!(error, EmbeddedEngineStartError::UnregisteredArchetype { archetype_id, executions: 1 } if archetype_id == archetype_id_for_fqn(Root::<0>::FQN))
        );
    }

    #[tokio::test]
    async fn startup_names_attribute_conflict() {
        let store = InMemoryVisibilityStore::default();
        let namespace = namespace_id_for("default");
        store
            .register_attr(namespace, "BuilderValue".into(), SearchAttrType::Keyword)
            .await
            .unwrap();
        let extensions = ChasmExtensions {
            libraries: vec![TestLibrary::<0>::register],
            test_storage: Some((Arc::new(InMemoryChasmNodeStore::new()), store)),
            ..Default::default()
        };
        let error = Engine::start_with_extensions(EmbeddedEngineConfig::default(), extensions)
            .await
            .unwrap_err();
        assert!(
            matches!(error, EmbeddedEngineStartError::SearchAttributeType { namespace: actual, ref key, declared: SearchAttrType::Int, existing: SearchAttrType::Keyword } if actual == namespace && key == "BuilderValue")
        );
    }

    #[tokio::test]
    async fn startup_names_missing_root_data_without_a_task_id() {
        let repo = Arc::new(InMemoryChasmNodeStore::new());
        persisted_root(&repo, None).await;
        let extensions = ChasmExtensions {
            libraries: vec![TestLibrary::<0>::register],
            test_storage: Some((repo, InMemoryVisibilityStore::default())),
            ..Default::default()
        };
        let error = Engine::start_with_extensions(EmbeddedEngineConfig::default(), extensions)
            .await
            .unwrap_err();
        assert!(
            matches!(error, EmbeddedEngineStartError::UnserviceableOutbox { archetype_id, cause: tokeira_runtime::chasm::RebuildFailure::MissingRootData, executions: 1 } if archetype_id == archetype_id_for_fqn(Root::<0>::FQN))
        );
        assert!(error.to_string().contains("root component data is missing"));
    }

    #[tokio::test]
    async fn declarations_seed_at_start_and_for_later_namespaces() {
        let store = InMemoryVisibilityStore::default();
        let declarations = declared_search_attributes(&registry([true, false, false]));
        let repo = Arc::new(InMemoryChasmNodeStore::new());
        persisted_root(&repo, Some(Data::default().encode_to_vec())).await;
        let extensions = ChasmExtensions {
            libraries: vec![TestLibrary::<0>::register],
            test_storage: Some((repo, store.clone())),
            ..Default::default()
        };
        let engine = Engine::start_with_extensions(EmbeddedEngineConfig::default(), extensions)
            .await
            .unwrap();
        assert_eq!(
            store
                .resolve_attr(namespace_id_for("default"), "BuilderValue")
                .await
                .unwrap()
                .unwrap()
                .attr_type,
            SearchAttrType::Int
        );
        let operator = VisibilityRegistryOperatorApi::new(
            InMemoryOperatorApi::new("test", "test"),
            store.clone(),
        )
        .with_declared_search_attributes(declarations);
        operator
            .seed_predefined_search_attributes("later")
            .await
            .unwrap();
        assert_eq!(
            store
                .resolve_attr(namespace_id_for("later"), "BuilderValue")
                .await
                .unwrap()
                .unwrap()
                .attr_type,
            SearchAttrType::Int
        );
        store
            .register_attr(
                namespace_id_for("conflict"),
                "BuilderValue".into(),
                SearchAttrType::Bool,
            )
            .await
            .unwrap();
        let error = operator
            .seed_predefined_search_attributes("conflict")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("BuilderValue"));
        assert!(error.to_string().contains("Bool"));
        engine.shutdown().await.unwrap();
    }
}
