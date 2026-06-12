//! The `ActivityExecution` root component and the `activity` library.
//!
//! Ground truth: `chasm/lib/activity/{activity.go,library.go} @ v1.31.0`. The
//! component registers under library `activity` with archetype FQN
//! `"activity.activity"` (Requirement 11.1), maps its status to a lifecycle
//! (Requirement 11.3), and contributes the `ActivityType` / `ExecutionStatus` /
//! `TaskQueue` search attributes (Requirement 10.4).
//!
//! Per the crate MVP note, the activity is modelled as a single `#[chasm(data)]`
//! field carrying the whole [`ActivityState`]; the derive macro therefore sees one
//! data field and one transient field, and the runtime engine materializes the
//! component from that one root node.

use tokeira_chasm::{
    ChasmError, Context, ContextMetadata, EngineComponent, Field, Library, Lifecycle,
    LifecycleState, MutableContext, RegistryBuilder, RootComponent, SearchAttributeProvider,
    SearchAttributes, TerminateReason,
};
use tokeira_chasm_derive::Component;

use crate::{
    state::{ActivityState, ActivityStatus, lifecycle_for},
    statemachine::{self, ActivityEvent},
};

/// The standalone-activity root component (archetype `"activity.activity"`).
///
/// Its single persisted data field is the [`ActivityState`] proto. `meta` is
/// request-scoped context provenance and is explicitly transient (not persisted).
#[derive(Component)]
#[chasm(fqn = "activity.activity")]
pub struct ActivityExecution {
    /// The activity's persisted state (status, attempt, stamp, timeouts, payloads).
    #[chasm(data)]
    state: Field<ActivityState>,
    /// Request-scoped context metadata; not persisted.
    #[chasm(transient)]
    meta: ContextMetadata,
}

impl ActivityExecution {
    /// Construct from an initial [`ActivityState`].
    pub fn new(state: ActivityState) -> Self {
        Self {
            state: Field::with_value(state),
            meta: ContextMetadata::default(),
        }
    }

    /// Borrow the in-memory activity state, if present.
    pub fn activity_state(&self) -> Option<&ActivityState> {
        self.state.value()
    }

    /// The current status, or `Unspecified` if the state is somehow absent.
    pub fn status(&self) -> ActivityStatus {
        self.activity_state()
            .map(ActivityState::status)
            .unwrap_or(ActivityStatus::Unspecified)
    }

    /// Apply an [`ActivityEvent`] to the activity, scheduling resulting tasks via
    /// `ctx`. Delegates to the stamp-fenced [`statemachine::apply`]
    /// (Requirement 11.4–11.7).
    pub fn apply(
        &mut self,
        event: ActivityEvent,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        let state = self.state.value_mut().ok_or_else(|| {
            ChasmError::Internal("activity state node not materialized".to_owned())
        })?;
        statemachine::apply(state, event, ctx)
    }
}

impl Lifecycle for ActivityExecution {
    fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
        lifecycle_for(self.status())
    }
}

impl EngineComponent for ActivityExecution {
    fn from_data(data: ActivityState) -> Self {
        Self::new(data)
    }

    fn into_data(self) -> ActivityState {
        self.state.into_value().unwrap_or_default()
    }
}

impl RootComponent for ActivityExecution {
    fn terminate(
        &mut self,
        ctx: &mut dyn MutableContext,
        reason: &TerminateReason,
    ) -> Result<(), ChasmError> {
        self.apply(
            ActivityEvent::Terminated {
                reason: reason.reason.clone(),
            },
            ctx,
        )
    }

    fn context_metadata(&self) -> &ContextMetadata {
        &self.meta
    }
}

impl SearchAttributeProvider for ActivityExecution {
    fn search_attributes(&self) -> SearchAttributes {
        let Some(state) = self.activity_state() else {
            return Vec::new();
        };
        vec![
            ("ActivityType".to_owned(), state.activity_type.clone()),
            (
                "ExecutionStatus".to_owned(),
                execution_status_name(state.status()).to_owned(),
            ),
            ("TaskQueue".to_owned(), state.task_queue.clone()),
        ]
    }
}

/// The visibility `ExecutionStatus` string for an activity status, matching the
/// names the targeted release surfaces.
fn execution_status_name(status: ActivityStatus) -> &'static str {
    match status {
        ActivityStatus::Unspecified => "Unspecified",
        ActivityStatus::Scheduled => "Scheduled",
        ActivityStatus::Started => "Started",
        ActivityStatus::CancelRequested => "CancelRequested",
        ActivityStatus::Completed => "Completed",
        ActivityStatus::Failed => "Failed",
        ActivityStatus::Canceled => "Canceled",
        ActivityStatus::Terminated => "Terminated",
        ActivityStatus::TimedOut => "TimedOut",
    }
}

/// The `activity` library: registers [`ActivityExecution`] as archetype
/// `"activity.activity"` (Requirement 11.1).
pub struct ActivityLibrary;

impl Library for ActivityLibrary {
    const NAME: &'static str = "activity";

    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
        builder.register::<ActivityExecution>(Self::NAME)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_chasm::{Component as _, Registry};

    #[test]
    fn registers_under_activity_archetype() {
        let mut builder = Registry::builder();
        builder
            .register_library::<ActivityLibrary>()
            .expect("register");
        let registry = builder.build();
        assert!(registry.archetype_id("activity.activity").is_some());
        assert_eq!(
            registry
                .component_for_fqn("activity.activity")
                .map(|e| e.library),
            Some("activity")
        );
        assert_eq!(ActivityExecution::FQN, "activity.activity");
    }

    #[test]
    fn lifecycle_tracks_status() {
        struct NoCtx;
        impl Context for NoCtx {
            fn execution_key(&self) -> &tokeira_chasm::ExecutionKey {
                unreachable!("lifecycle does not read the key")
            }
            fn execution_info(&self) -> tokeira_chasm::ExecutionInfo {
                tokeira_chasm::ExecutionInfo::default()
            }
            fn now_unix_nanos(&self) -> i64 {
                0
            }
        }
        let mut state = ActivityState::default();
        state.set_status(ActivityStatus::Completed);
        let component = ActivityExecution::new(state);
        assert_eq!(component.lifecycle_state(&NoCtx), LifecycleState::Completed);
    }

    #[test]
    fn search_attributes_present() {
        let mut state = ActivityState {
            activity_type: "PaymentActivity".to_owned(),
            task_queue: "payments".to_owned(),
            ..ActivityState::default()
        };
        state.set_status(ActivityStatus::Scheduled);
        let component = ActivityExecution::new(state);
        let sas = component.search_attributes();
        assert!(sas.contains(&("ActivityType".to_owned(), "PaymentActivity".to_owned())));
        assert!(sas.contains(&("TaskQueue".to_owned(), "payments".to_owned())));
        assert!(sas.contains(&("ExecutionStatus".to_owned(), "Scheduled".to_owned())));
    }
}
