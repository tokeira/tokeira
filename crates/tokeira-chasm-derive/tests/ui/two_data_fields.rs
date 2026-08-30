use tokeira_chasm::Field;
use tokeira_chasm_derive::Component;

#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(uint64, tag = "1")]
    value: u64,
}

#[derive(Component)]
#[chasm(fqn = "test.two_data")]
struct TwoData {
    #[chasm(data)]
    first: Field<Payload>,
    #[chasm(data)]
    second: Field<Payload>,
}

fn main() {}
