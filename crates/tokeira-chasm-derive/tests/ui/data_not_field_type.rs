use tokeira_chasm_derive::Component;

#[derive(Component)]
#[chasm(fqn = "test.bad_data")]
struct BadData {
    #[chasm(data)]
    state: u32,
}

fn main() {}
