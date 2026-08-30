//! Behaviour of the generated `Component` impl, proven against the real
//! `tokeira-chasm` trait surface (accept-side of the shape rules; the reject
//! side is pinned by `tests/compile.rs` via trybuild).

use tokeira_chasm::{Component, Context, Field, FieldKind, LifecycleState, Map, ParentPtr};
use tokeira_chasm_derive::Component;

/// Minimal prost payload standing in for a component's proto data type.
#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(uint64, tag = "1")]
    value: u64,
}

/// Second payload type so child/map fields differ from the data field.
#[derive(Clone, PartialEq, prost::Message)]
struct ChildPayload {
    #[prost(string, tag = "1")]
    label: String,
}

// Field values are never read here: the derive classifies fields by type at
// expansion time, and these tests assert the generated registry metadata only.
#[allow(dead_code)]
#[derive(Component)]
#[chasm(fqn = "test.minimal")]
struct Minimal {
    #[chasm(data)]
    state: Field<Payload>,
}

impl tokeira_chasm::Lifecycle for Minimal {
    fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
        LifecycleState::Running
    }
}

// Field values are never read here: the derive classifies fields by type at
// expansion time, and these tests assert the generated registry metadata only.
#[allow(dead_code)]
#[derive(Component)]
#[chasm(fqn = "test.full")]
struct Full {
    #[chasm(data)]
    state: Field<Payload>,
    child: Field<ChildPayload>,
    children: Map<String, ChildPayload>,
    parent: ParentPtr<Minimal>,
    #[chasm(transient)]
    scratch: u32,
}

impl tokeira_chasm::Lifecycle for Full {
    fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
        LifecycleState::Running
    }
}

/// Generics survive the derive: the impl is emitted with the struct's own
/// generic parameters and where-clause.
// Field values are never read here: the derive classifies fields by type at
// expansion time, and these tests assert the generated registry metadata only.
#[allow(dead_code)]
#[derive(Component)]
#[chasm(fqn = "test.generic")]
struct Generic<P>
where
    P: prost::Message + Default + 'static,
{
    #[chasm(data)]
    state: Field<P>,
    #[chasm(transient)]
    marker: std::marker::PhantomData<P>,
}

impl<P> tokeira_chasm::Lifecycle for Generic<P>
where
    P: prost::Message + Default + 'static,
{
    fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
        LifecycleState::Running
    }
}

fn minimal() -> Minimal {
    Minimal {
        state: Field::with_value(Payload::default()),
    }
}

fn full() -> Full {
    Full {
        state: Field::with_value(Payload::default()),
        child: Field::with_value(ChildPayload::default()),
        children: Map::new(),
        parent: ParentPtr::uninitialized(),
        scratch: 0,
    }
}

#[test]
fn fqn_comes_from_the_container_attribute() {
    assert_eq!(Minimal::FQN, "test.minimal");
    assert_eq!(Full::FQN, "test.full");
    assert_eq!(Generic::<Payload>::FQN, "test.generic");
}

#[test]
fn data_type_is_the_data_fields_payload() {
    // Compile-time assertion: `Data` is exactly the `Field<T>` payload.
    fn assert_data<C: Component<Data = D>, D>() {}
    assert_data::<Minimal, Payload>();
    assert_data::<Full, Payload>();
    assert_data::<Generic<ChildPayload>, ChildPayload>();
}

#[test]
fn registry_preserves_declaration_order_and_kinds() {
    let component = full();
    let registry = component.fields();
    let named: Vec<(&str, FieldKind)> = registry
        .iter()
        .map(|descriptor| (descriptor.name, descriptor.kind))
        .collect();
    assert_eq!(
        named,
        vec![
            ("state", FieldKind::Data),
            ("child", FieldKind::Component),
            ("children", FieldKind::Map),
            ("parent", FieldKind::Parent),
            ("scratch", FieldKind::Transient),
        ]
    );
}

#[test]
fn registry_exposes_exactly_one_data_field() {
    let component = full();
    let registry = component.fields();
    let data = registry
        .data_field()
        .expect("derive guarantees a data field");
    assert_eq!(data.name, "state");
    assert_eq!(minimal().fields().len(), 1);
}

#[test]
fn registry_lookup_by_name() {
    let component = full();
    let registry = component.fields();
    assert_eq!(
        registry.get("children").expect("declared field").kind,
        FieldKind::Map
    );
    assert!(registry.get("absent").is_none());
}
