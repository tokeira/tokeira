use tokeira_chasm_derive::Component;

#[derive(Component)]
#[chasm(fqn = "test.enum")]
enum NotAStruct {
    Variant,
}

fn main() {}
