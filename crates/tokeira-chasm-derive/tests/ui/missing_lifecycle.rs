use tokeira_chasm::Field;
use tokeira_chasm_derive::Component;

#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(uint64, tag = "1")]
    value: u64,
}

// A shape-valid component that never implements Lifecycle: the generated
// `impl Component` must fail on the `Component: Lifecycle` supertrait bound,
// proving the requirement is a real bound, not a naming convention.
#[derive(Component)]
#[chasm(fqn = "test.no_lifecycle")]
struct NoLifecycle {
    #[chasm(data)]
    state: Field<Payload>,
}

fn main() {}
