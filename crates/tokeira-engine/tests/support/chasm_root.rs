//! Independent roots for extension registration and admission tests.

use prost::Message;
use tokeira_chasm::{
    ChasmError, Component, Context, ContextMetadata, EngineComponent, FieldRegistry, Library,
    Lifecycle, LifecycleState, MutableContext, RegistryBuilder, RootComponent, SearchAttrKind,
    SearchAttributeDef, SearchAttributeProvider, SearchAttributes, TerminateReason,
    VisibilityContributor, VisibilitySnapshot,
};

#[derive(Clone, PartialEq, Message)]
pub(super) struct Data {
    #[prost(uint32, tag = "1")]
    pub(super) value: u32,
    #[prost(bool, tag = "2")]
    pub(super) closed: bool,
}

#[derive(Debug)]
pub(super) struct Root<const ID: usize> {
    pub(super) data: Data,
    meta: ContextMetadata,
}
impl<const ID: usize> Component for Root<ID> {
    type Data = Data;
    const FQN: &'static str = [
        "builder_one.root",
        "builder_two.root",
        "builder_three.root",
        "absent.root",
    ][ID];
    fn fields(&self) -> FieldRegistry<'_> {
        FieldRegistry::new(&[])
    }
}
impl<const ID: usize> Lifecycle for Root<ID> {
    fn lifecycle_state(&self, _: &dyn Context) -> LifecycleState {
        if self.data.closed {
            LifecycleState::Completed
        } else {
            LifecycleState::Running
        }
    }
}
impl<const ID: usize> RootComponent for Root<ID> {
    fn terminate(
        &mut self,
        _: &mut dyn MutableContext,
        _: &TerminateReason,
    ) -> Result<(), ChasmError> {
        self.data.closed = true;
        Ok(())
    }
    fn context_metadata(&self) -> &ContextMetadata {
        &self.meta
    }
}
impl<const ID: usize> EngineComponent for Root<ID> {
    fn from_data(data: Data) -> Self {
        Self {
            data,
            meta: ContextMetadata::default(),
        }
    }
    fn into_data(self) -> Data {
        self.data
    }
}
impl<const ID: usize> SearchAttributeProvider for Root<ID> {
    fn search_attributes(&self) -> SearchAttributes {
        Vec::new()
    }
}
impl<const ID: usize> VisibilityContributor for Root<ID> {
    fn visibility_snapshot(&self) -> Option<VisibilitySnapshot> {
        None
    }
}

#[derive(Debug)]
pub(super) struct TestLibrary<const ID: usize>;
impl<const ID: usize> Library for TestLibrary<ID> {
    const NAME: &'static str = ["builder_one", "builder_two", "builder_three"][ID];
    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
        builder.register_root::<Root<ID>>(Self::NAME)?;
        builder.register_search_attributes::<Root<ID>>(&[SearchAttributeDef {
            name: "BuilderValue",
            kind: SearchAttrKind::Int,
        }])?;
        Ok(())
    }
}
