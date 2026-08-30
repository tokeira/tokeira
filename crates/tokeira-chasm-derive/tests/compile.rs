//! Compile-time contract of `#[derive(Component)]`: every shape-rule rejection
//! (Requirement 3.2-3.5) is pinned — message and span — so a macro change that
//! weakens or reworded an enforcement is a visible diff, not a silent drift.
//! The accept side lives in `tests/derive_behavior.rs`.

#[test]
fn shape_rules_are_enforced_at_compile_time() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
