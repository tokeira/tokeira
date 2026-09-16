//! Typed task behavior registered against a single root component.
//!
//! Registration erases these handlers into monomorphized byte-codec closures,
//! without reflection. Root bounds enforce the single-root materialization
//! contract; child materialization is not supported. Every method is pure:
//! side-effect I/O belongs to runtime executors, and only its outcome returns here.

use crate::{
    ChasmError, Context, EngineComponent, MutableContext, RootComponent, Task, TaskOutcome,
    TaskValidity,
};

/// Validation and mutation for a task executed inside a fenced transition.
/// Mirrors `chasm/task.go` and `chasm/registrable_task.go @ v1.31.0`:
/// validation gates execution, which must perform no I/O.
pub trait PureTaskHandler: Send + Sync + 'static {
    /// Materializable execution root owning the task.
    type Component: EngineComponent + RootComponent;
    /// Task payload; its codec is owned by the defining library.
    type Task: Task;
    /// Drop obsolete work without mutating the component or context.
    fn validate(&self, c: &Self::Component, t: &Self::Task, ctx: &dyn Context) -> TaskValidity;
    /// Apply the task's state transition; errors abort the enclosing transition.
    fn execute(
        &self,
        c: &mut Self::Component,
        t: &Self::Task,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError>;
}

/// Validation and outcome mutation for externally executed work.
/// Unlike `chasm/task.go` and `chasm/registrable_task.go @ v1.31.0`, this
/// contract deliberately omits side-effect `Execute`/`Discard`: runtime executors
/// own I/O (extension-archetypes decision D3), preserving substrate purity.
pub trait SideEffectTaskHandler: Send + Sync + 'static {
    /// Materializable execution root that staged the side effect.
    type Component: EngineComponent + RootComponent;
    /// Task payload; its codec is owned by the defining library.
    type Task: Task;
    /// Drop obsolete work without performing the effect.
    fn validate(&self, c: &Self::Component, t: &Self::Task, ctx: &dyn Context) -> TaskValidity;
    /// Apply an external result inside a fenced transition. No I/O is permitted;
    /// errors abort the transition, including any task resolutions it staged.
    fn on_outcome(
        &self,
        c: &mut Self::Component,
        t: &Self::Task,
        outcome: &TaskOutcome,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError>;
}
