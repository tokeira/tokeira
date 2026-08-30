use tokeira_chasm::Field;
use tokeira_chasm_derive::Component;

#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(uint64, tag = "1")]
    value: u64,
}

#[derive(Component)]
#[chasm(fqn = "test.unknown_field")]
struct UnknownField {
    #[chasm(data)]
    state: Field<Payload>,
    #[chasm(persisted)]
    extra: Field<Payload>,
}

fn main() {}
