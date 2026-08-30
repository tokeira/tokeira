use tokeira_chasm::Field;
use tokeira_chasm_derive::Component;

#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(uint64, tag = "1")]
    value: u64,
}

#[derive(Component)]
#[chasm(fqn = "test.conflict")]
struct Conflict {
    #[chasm(data, transient)]
    state: Field<Payload>,
}

fn main() {}
