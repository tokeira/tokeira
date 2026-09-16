//! Root registration and visibility for a long-lived resource. Lifecycle stays
//! Running across successful and failed operations so future updates remain legal.

use prost::Message;
use tokeira_chasm::{
    ChasmError, Context, ContextMetadata, EngineComponent, Field, Library, Lifecycle,
    LifecycleState, MutableContext, RegistryBuilder, RootComponent, SearchAttrKind,
    SearchAttributeDef, SearchAttributeProvider, SearchAttributes, TerminateReason,
    VisibilityContributor, VisibilitySnapshot,
};
use tokeira_chasm_derive::Component;
use tokeira_proto::failure::{Failure, TerminatedFailureInfo, failure::FailureInfo};
use tokeira_types::SearchAttrValue;

use crate::{
    OperationOutcome, ResourceState,
    reconcile::{ReconcileHandler, RetryHandler},
};

/// Acceptance resource with a single persisted root and transient context metadata.
#[derive(Debug, Component)]
#[chasm(fqn = "acceptance.resource")]
pub struct Resource {
    #[chasm(data)]
    state: Field<ResourceState>,
    #[chasm(transient)]
    meta: ContextMetadata,
}

impl Resource {
    /// Borrow materialized state; absent root data is an engine invariant failure.
    pub fn state(&self) -> Result<&ResourceState, ChasmError> {
        self.state
            .value()
            .ok_or_else(|| ChasmError::Internal("resource state is not materialized".into()))
    }

    pub(crate) fn state_mut(&mut self) -> Result<&mut ResourceState, ChasmError> {
        self.state
            .value_mut()
            .ok_or_else(|| ChasmError::Internal("resource state is not materialized".into()))
    }
}

impl EngineComponent for Resource {
    fn from_data(data: ResourceState) -> Self {
        Self {
            state: Field::with_value(data),
            meta: ContextMetadata::default(),
        }
    }

    fn into_data(self) -> ResourceState {
        self.state.into_value().unwrap_or_default()
    }
}

impl Lifecycle for Resource {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        // An operation finishing never closes its owning resource: later desired
        // generations and retry timers must remain writable and rebuildable.
        LifecycleState::Running
    }
}

impl RootComponent for Resource {
    fn terminate(
        &mut self,
        ctx: &mut dyn MutableContext,
        reason: &TerminateReason,
    ) -> Result<(), ChasmError> {
        let state = self.state_mut()?;
        if let Some(mut operation) = state.active_operation.take() {
            operation.outcome = OperationOutcome::Failed as i32;
            operation.finished_at_nanos = ctx.now_unix_nanos();
            operation.failure = Failure {
                message: reason.reason.clone(),
                failure_info: Some(FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo {
                    identity: reason.identity.clone().unwrap_or_default(),
                })),
                ..Default::default()
            }
            .encode_to_vec();
            state.last_failure.clone_from(&operation.failure);
            state.finish(operation);
        }
        Ok(())
    }

    fn context_metadata(&self) -> &ContextMetadata {
        &self.meta
    }
}

impl SearchAttributeProvider for Resource {
    fn search_attributes(&self) -> SearchAttributes {
        self.state()
            .map(|state| {
                vec![
                    ("DeploymentStatus".into(), state.status().into()),
                    (
                        "DesiredGeneration".into(),
                        state.desired_generation.to_string(),
                    ),
                    (
                        "ObservedGeneration".into(),
                        state.observed_generation.to_string(),
                    ),
                ]
            })
            .unwrap_or_default()
    }
}

impl VisibilityContributor for Resource {
    fn visibility_snapshot(&self) -> Option<VisibilitySnapshot> {
        let state = self.state().ok()?;
        Some(VisibilitySnapshot {
            status_keyword: "Running".into(),
            lifecycle_state: LifecycleState::Running,
            execution_type: Some("acceptance.resource".into()),
            task_queue: Some(state.task_queue.clone()),
            start_time_unix_nanos: None,
            close_time_unix_nanos: None,
            search_attributes: tokeira_types::SearchAttributes(
                [
                    (
                        "DeploymentStatus".into(),
                        SearchAttrValue::Keyword(state.status().into()),
                    ),
                    (
                        "DesiredGeneration".into(),
                        SearchAttrValue::Int(state.desired_generation as i64),
                    ),
                    (
                        "ObservedGeneration".into(),
                        SearchAttrValue::Int(state.observed_generation as i64),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            memo: Default::default(),
        })
    }
}

/// Registers only acceptance-owned behavior, using the built-in start executor.
#[derive(Debug)]
pub struct AcceptanceLibrary;

impl Library for AcceptanceLibrary {
    const NAME: &'static str = "acceptance";

    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
        builder
            .register_root::<Resource>(Self::NAME)?
            .register_side_effect_task(Self::NAME, ReconcileHandler)?
            .register_pure_task(Self::NAME, RetryHandler)?
            .register_search_attributes::<Resource>(&[
                SearchAttributeDef {
                    name: "DeploymentStatus",
                    kind: SearchAttrKind::Keyword,
                },
                SearchAttributeDef {
                    name: "DesiredGeneration",
                    kind: SearchAttrKind::Int,
                },
                SearchAttributeDef {
                    name: "ObservedGeneration",
                    kind: SearchAttrKind::Int,
                },
            ])?;
        Ok(())
    }
}
