//! The typed, component-keyed wrapper over the untyped [`Engine`] (Requirement
//! 6.1, 6.7; spec task 12.3).
//!
//! [`TypedEngine`] lets callers work in terms of a concrete
//! [`Component`](tokeira_chasm::Component) instead of untyped paths and bytes. It
//! is generic monomorphization, not runtime dispatch: there is no
//! `Box<dyn Component>` and no downcasting. It owns the **reload-and-rerun** loop
//! that the untyped engine cannot — on a fenced-commit [`CommitOutcome::Conflict`]
//! it reloads the latest state, re-runs the caller's mutation closure against it,
//! and retries, bounded by `max_commit_retries` (Requirement 9.5). Keeping the
//! closure here (not on the object-safe `Engine` trait) is what makes that loop
//! possible.

use std::marker::PhantomData;

use prost::Message as _;
use tokeira_chasm::{
    BusinessIdPolicy, ChasmError, ComponentRef, Context, EngineComponent, ExecutionKey,
    MutableContext, RootComponent, SearchAttributeProvider, VersionedTransition,
    VisibilityContributor,
};

use super::{
    CommitOutcome, Engine, StartOutcome, StartRequest, UpdateOutcome, UpdateRequest,
    engine::{ChasmEngine, TransitionContext},
};

/// A component-typed view over a [`ChasmEngine`].
///
/// `C` is the concrete root component the caller operates on. The wrapper resolves
/// `C`'s archetype id from the engine registry, (de)serializes `C::Data` at the
/// node boundary, and runs the caller's typed closures inside a transition.
pub struct TypedEngine<'e, C> {
    engine: &'e ChasmEngine,
    _marker: PhantomData<fn() -> C>,
}

impl<'e, C> TypedEngine<'e, C>
where
    C: EngineComponent + RootComponent + SearchAttributeProvider + VisibilityContributor,
{
    /// Wrap an engine for component-typed access.
    pub fn new(engine: &'e ChasmEngine) -> Self {
        Self {
            engine,
            _marker: PhantomData,
        }
    }

    /// Start a new execution rooted at a `C` carrying `data` (Requirement 6.1).
    pub async fn start(
        &self,
        key: ExecutionKey,
        data: C::Data,
        request_id: Option<String>,
        policy: BusinessIdPolicy,
    ) -> Result<StartOutcome, ChasmError> {
        let archetype_id = self.engine.registry().archetype_id(C::FQN).ok_or_else(|| {
            ChasmError::Internal(format!("archetype `{}` is not registered", C::FQN))
        })?;
        // Capture the creating transition's visibility snapshot from the initial
        // component before encoding, so a started-but-not-yet-updated execution is
        // still listable (Requirement 10.2).
        let component = C::from_data(data);
        let visibility = component.visibility_snapshot();
        let data = component.into_data().encode_to_vec();
        self.engine
            .start_execution(StartRequest {
                key,
                archetype_id,
                data,
                request_id,
                policy,
                visibility,
            })
            .await
    }

    /// Run a typed mutation inside a transition, reloading and re-running on a
    /// fenced-commit conflict up to the configured bound (Requirement 6.1, 9.5).
    ///
    /// `f` receives the materialized component and a [`MutableContext`]; its return
    /// value is threaded back out alongside the [`UpdateOutcome`]. The closure must
    /// be re-runnable (it may execute more than once under contention), so it takes
    /// `FnMut`.
    pub async fn update<R>(
        &self,
        reference: &ComponentRef,
        mut f: impl FnMut(&mut C, &mut dyn MutableContext) -> Result<R, ChasmError>,
    ) -> Result<(R, UpdateOutcome), ChasmError> {
        let attempts = self.engine.config().max_commit_retries.max(1);
        for _ in 0..attempts {
            let snapshot = self.engine.read_component(&reference.execution_key).await?;
            let data_bytes = snapshot.data.ok_or(ChasmError::ExecutionNotFound)?;
            let data = C::Data::decode(data_bytes.as_slice())
                .map_err(|e| ChasmError::Validation(format!("decode component data: {e}")))?;
            let mut component = C::from_data(data);

            let mut ctx = TransitionContext::new(
                reference.execution_key.clone(),
                snapshot.execution_vt,
                self.engine.now(),
            );
            let returned = f(&mut component, &mut ctx)?;

            // Compute derived outputs before consuming the component for its data.
            let lifecycle = component.lifecycle_state(&ctx);
            let search_attributes = component.search_attributes();
            let visibility = component.visibility_snapshot();
            let new_root_data = component.into_data().encode_to_vec();
            let tasks = ctx.take_staged_tasks();

            let request = UpdateRequest {
                key: reference.execution_key.clone(),
                expected_execution_vt: snapshot.execution_vt,
                new_root_data,
                new_lifecycle: lifecycle,
                tasks,
                search_attributes,
                visibility,
            };
            match self.engine.update_component(request).await? {
                CommitOutcome::Applied(outcome) => return Ok((returned, outcome)),
                CommitOutcome::Conflict => continue,
            }
        }
        Err(ChasmError::RetriesExhausted { attempts })
    }

    /// Start-if-absent, then update in the same call (Requirement 6.1, 6.2).
    ///
    /// If the execution already exists, the start is skipped (its business-id
    /// reuse/conflict is treated as "already present") and the update runs against
    /// the live state. MVP note: this is a two-phase start-then-update, not a
    /// single fused transition; the fused form is a refinement once business-id
    /// reuse policies are modelled (Requirement 6.2).
    pub async fn update_with_start<R>(
        &self,
        key: ExecutionKey,
        initial: C::Data,
        request_id: Option<String>,
        f: impl FnMut(&mut C, &mut dyn MutableContext) -> Result<R, ChasmError>,
    ) -> Result<(R, UpdateOutcome), ChasmError> {
        // MVP: update-with-start always starts with the default policy
        // (AllowDuplicate/Fail). Per-request reuse/conflict plumbing for the
        // UpdateWithStart operation is a separate refinement (Requirement 6.2); the
        // activity Start path that consumes the request policy goes through `start`.
        match self
            .start(
                key.clone(),
                initial,
                request_id,
                BusinessIdPolicy::default(),
            )
            .await
        {
            Ok(_) | Err(ChasmError::BusinessIdConflict(_)) => {}
            Err(other) => return Err(other),
        }
        let archetype_id = self.engine.registry().archetype_id(C::FQN).ok_or_else(|| {
            ChasmError::Internal(format!("archetype `{}` is not registered", C::FQN))
        })?;
        // `update` reads the live state by execution key; the VT fields of this
        // reference are placeholders it does not consult.
        let reference = ComponentRef::new(
            key,
            archetype_id,
            VersionedTransition::default(),
            Vec::new(),
            VersionedTransition::default(),
        );
        self.update(&reference, f).await
    }

    /// Run a read-only closure against a snapshot of the component (Requirement
    /// 6.1, 5.3). No dirty nodes, no tasks, no mutation.
    pub async fn read<R>(
        &self,
        reference: &ComponentRef,
        f: impl FnOnce(&C, &dyn Context) -> Result<R, ChasmError>,
    ) -> Result<R, ChasmError> {
        let snapshot = self.engine.read_component(&reference.execution_key).await?;
        let data_bytes = snapshot.data.ok_or(ChasmError::ExecutionNotFound)?;
        let data = C::Data::decode(data_bytes.as_slice())
            .map_err(|e| ChasmError::Validation(format!("decode component data: {e}")))?;
        let component = C::from_data(data);
        let ctx = TransitionContext::new(
            reference.execution_key.clone(),
            snapshot.execution_vt,
            self.engine.now(),
        );
        f(&component, &ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokeira_chasm::{
        BusinessIdPolicy, ChasmError, Component, Context, ContextMetadata, ExecutionKey,
        FieldRegistry, Lifecycle, LifecycleState, MutableContext, Registry, RootComponent,
        SearchAttributeProvider, SearchAttributes, TaskKind, TerminateReason,
        VisibilityContributor, VisibilitySnapshot,
    };
    use tokeira_storage::InMemoryChasmNodeStore;

    use super::TypedEngine;
    use crate::chasm::{
        Engine, PollOutcome, PollRequest,
        engine::{
            ChasmEngine, ChasmEngineConfig, CollectingDispatchSink, CollectingVisibilitySink,
        },
    };

    // A minimal proto data payload for the test root component.
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct CounterData {
        #[prost(int32, tag = "1")]
        counter: i32,
        #[prost(bool, tag = "2")]
        done: bool,
    }

    // A minimal root component: a counter that closes when `done` is set. It
    // implements the traits by hand (the derive macro is exercised in Layer 3);
    // only the data, lifecycle, and the engine/visibility bridges matter here.
    struct Counter {
        data: CounterData,
        meta: ContextMetadata,
    }

    impl Lifecycle for Counter {
        fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
            if self.data.done {
                LifecycleState::Completed
            } else {
                LifecycleState::Running
            }
        }
    }

    impl Component for Counter {
        type Data = CounterData;
        const FQN: &'static str = "test.counter";
        fn fields(&self) -> FieldRegistry<'_> {
            FieldRegistry::new(&[])
        }
    }

    impl RootComponent for Counter {
        fn terminate(
            &mut self,
            _ctx: &mut dyn MutableContext,
            _reason: &TerminateReason,
        ) -> Result<(), ChasmError> {
            self.data.done = true;
            Ok(())
        }
        fn context_metadata(&self) -> &ContextMetadata {
            &self.meta
        }
    }

    impl super::EngineComponent for Counter {
        fn from_data(data: CounterData) -> Self {
            Self {
                data,
                meta: ContextMetadata::default(),
            }
        }
        fn into_data(self) -> CounterData {
            self.data
        }
    }

    impl SearchAttributeProvider for Counter {
        fn search_attributes(&self) -> SearchAttributes {
            vec![(
                "ExecutionStatus".to_owned(),
                if self.data.done {
                    "Completed"
                } else {
                    "Running"
                }
                .to_owned(),
            )]
        }
    }

    impl VisibilityContributor for Counter {
        fn visibility_snapshot(&self) -> Option<VisibilitySnapshot> {
            let (status, lifecycle) = if self.data.done {
                ("Completed", LifecycleState::Completed)
            } else {
                ("Running", LifecycleState::Running)
            };
            Some(VisibilitySnapshot {
                status_keyword: status.to_owned(),
                lifecycle_state: lifecycle,
                execution_type: Some(Counter::FQN.to_owned()),
                task_queue: None,
                start_time_unix_nanos: None,
                close_time_unix_nanos: None,
                search_attributes: Default::default(),
                memo: Default::default(),
            })
        }
    }

    struct Fixture {
        engine: ChasmEngine,
        dispatch: Arc<CollectingDispatchSink>,
        visibility: Arc<CollectingVisibilitySink>,
    }

    fn fixture() -> Fixture {
        let mut builder = Registry::builder();
        builder.register::<Counter>("test").expect("register");
        let registry = Arc::new(builder.build());
        let dispatch = Arc::new(CollectingDispatchSink::default());
        let visibility = Arc::new(CollectingVisibilitySink::default());
        let engine = ChasmEngine::new(
            Arc::new(InMemoryChasmNodeStore::new()),
            registry,
            dispatch.clone(),
            visibility.clone(),
        )
        .with_config(ChasmEngineConfig {
            long_poll_timeout: std::time::Duration::from_millis(40),
            long_poll_buffer: std::time::Duration::from_millis(0),
            max_commit_retries: 8,
        });
        Fixture {
            engine,
            dispatch,
            visibility,
        }
    }

    fn key() -> ExecutionKey {
        ExecutionKey::new("ns", "counter-1", "run-1")
    }

    #[tokio::test]
    async fn start_update_read_round_trip() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let reference = typed
            .start(
                key(),
                CounterData::default(),
                Some("req-1".to_owned()),
                BusinessIdPolicy::default(),
            )
            .await
            .expect("start")
            .reference;

        let count = typed
            .read(&reference, |c, _ctx| Ok(c.data.counter))
            .await
            .expect("read");
        assert_eq!(count, 0);

        let (_r, outcome) = typed
            .update(&reference, |c, _ctx| {
                c.data.counter += 1;
                Ok(())
            })
            .await
            .expect("update");
        assert!(!outcome.closed);

        let count = typed
            .read(&reference, |c, _ctx| Ok(c.data.counter))
            .await
            .expect("read");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn side_effect_task_is_dispatched_post_commit() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let reference = typed
            .start(
                key(),
                CounterData::default(),
                None,
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap()
            .reference;
        typed
            .update(&reference, |c, ctx| {
                c.data.counter += 1;
                ctx.add_task(TaskKind::SideEffect, 11, vec![7], None)?;
                Ok(())
            })
            .await
            .unwrap();
        let dispatched = fx.dispatch.dispatched.lock().unwrap();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].1.task.task_type_id, 11);
    }

    #[tokio::test]
    async fn pure_task_arms_single_timer() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let reference = typed
            .start(
                key(),
                CounterData::default(),
                None,
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap()
            .reference;
        typed
            .update(&reference, |c, ctx| {
                c.data.counter += 1;
                ctx.add_task(TaskKind::Pure, 20, vec![], Some(9_000))?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(fx.engine.armed_timer(&key()), Some(9_000));
    }

    #[tokio::test]
    async fn lifecycle_close_blocks_further_mutation() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let reference = typed
            .start(
                key(),
                CounterData::default(),
                None,
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap()
            .reference;
        let (_r, outcome) = typed
            .update(&reference, |c, _ctx| {
                c.data.done = true;
                Ok(())
            })
            .await
            .unwrap();
        assert!(outcome.closed);
        // A further mutating transition on a closed execution is rejected.
        let err = typed
            .update(&reference, |c, _ctx| {
                c.data.counter += 1;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ChasmError::ExecutionClosed));
    }

    #[tokio::test]
    async fn poll_advances_then_empties() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let reference = typed
            .start(
                key(),
                CounterData::default(),
                None,
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap()
            .reference;
        let start_vt = reference.execution_versioned_transition;

        // An update advances the clock; a poll since the start VT resolves at once.
        typed
            .update(&reference, |c, _ctx| {
                c.data.counter += 1;
                Ok(())
            })
            .await
            .unwrap();
        let outcome = fx
            .engine
            .poll_component(PollRequest {
                key: key(),
                since: start_vt,
            })
            .await
            .unwrap();
        let current_vt = match outcome {
            PollOutcome::Advanced(read) => read.execution_vt,
            PollOutcome::Empty => panic!("expected advance"),
        };

        // Polling since the *current* VT with no further commits returns empty on
        // the (short, test-configured) deadline.
        let outcome = fx
            .engine
            .poll_component(PollRequest {
                key: key(),
                since: current_vt,
            })
            .await
            .unwrap();
        assert_eq!(outcome, PollOutcome::Empty);
    }

    #[tokio::test]
    async fn delete_removes_execution_and_visibility_is_recorded() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let reference = typed
            .start(
                key(),
                CounterData::default(),
                None,
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap()
            .reference;
        typed
            .update(&reference, |c, _ctx| {
                c.data.counter += 1;
                Ok(())
            })
            .await
            .unwrap();
        // The visibility hook recorded the contributing component's search attrs.
        assert!(!fx.visibility.recorded.lock().unwrap().is_empty());

        fx.engine.delete_execution(&key()).await.unwrap();
        let err = fx.engine.read_component(&key()).await.unwrap_err();
        assert!(matches!(err, ChasmError::ExecutionNotFound));
    }

    #[tokio::test]
    async fn update_with_start_creates_then_mutates() {
        let fx = fixture();
        let typed = TypedEngine::<Counter>::new(&fx.engine);
        let (_r, outcome) = typed
            .update_with_start(key(), CounterData::default(), None, |c, _ctx| {
                c.data.counter += 5;
                Ok(())
            })
            .await
            .expect("update_with_start");
        assert!(!outcome.closed);
        let count = typed
            .read(&outcome.reference, |c, _ctx| Ok(c.data.counter))
            .await
            .unwrap();
        assert_eq!(count, 5);
    }
}
