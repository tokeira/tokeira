use tokeira_chasm_derive::Component;

struct Field;

#[derive(Component)]
#[chasm(fqn = "test.bare_field")]
struct BareField {
    #[chasm(data)]
    state: Field,
}

fn main() {}
