use tokeira_chasm::{
    ChasmError, Component, Context, EngineComponent, FieldRegistry, Lifecycle, LifecycleState,
    MutableContext, RootComponent, StartActivityTask, Task, TaskOutcome, TaskValidity,
};

// Compile the actual trait source here so diagnostics resolve its source span
// consistently, including when dependency metadata has remapped cache paths.
#[path = "../../../tokeira-chasm/src/handler.rs"]
mod handler;
use handler::PureTaskHandler;

struct Child;
impl Lifecycle for Child {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        LifecycleState::Running
    }
}
impl Component for Child {
    type Data = ();
    const FQN: &'static str = "test.child";
    fn fields(&self) -> FieldRegistry<'_> { FieldRegistry::new(&[]) }
}
impl EngineComponent for Child {
    fn from_data(_: ()) -> Self { Self }
    fn into_data(self) {}
}

struct Handler;
impl PureTaskHandler for Handler {
    type Component = Child;
    type Task = StartActivityTask;
    fn validate(&self, _: &Child, _: &StartActivityTask, _: &dyn Context) -> TaskValidity {
        TaskValidity::Valid
    }
    fn execute(&self, _: &mut Child, _: &StartActivityTask, _: &mut dyn MutableContext) -> Result<(), ChasmError> {
        Ok(())
    }
}

fn main() {}
