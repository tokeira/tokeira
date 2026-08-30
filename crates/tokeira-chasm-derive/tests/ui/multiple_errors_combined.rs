use tokeira_chasm_derive::Component;

// Two independent violations in one struct: both are reported in a single
// expansion (the macro combines errors instead of stopping at the first).
#[derive(Component)]
#[chasm(fqn = "test.multi")]
struct Multi {
    plain: String,
    also_plain: u64,
}

fn main() {}
