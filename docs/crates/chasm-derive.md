# tokeira-chasm-derive

Procedural macro that generates the static field registry and
`tokeira_chasm::Component` implementation for a CHASM component.

## Where it sits

This compile-time companion supports the pure CHASM substrate. It replaces
runtime reflection with generated, monomorphized field metadata.

## Public surface

The crate exports one macro:

```rust
#[derive(Component)]
#[chasm(fqn = "example.component")]
struct Example {
    #[chasm(data)]
    state: Field<ExampleState>,
}
```

The generated `Component` implementation supplies the component's `Data`
associated type, fully qualified name, and static `FieldRegistry`.

## Shape rules

- A component is a named-field struct with exactly one `#[chasm(data)]`
  member.
- The data member must be `Field<T>`; `T` becomes `Component::Data`.
- Persistent members must use managed `Field`, `Map`, or `ParentPtr`
  shapes.
- An unmanaged member must be explicitly marked `#[chasm(transient)]`; it is
  never silently omitted from persistence.
- `#[chasm(fqn = "...")]` supplies the component's stable fully qualified
  name.
- The macro does not generate lifecycle behaviour. The author implements
  `Lifecycle`, and the `Component: Lifecycle` bound makes omission a compile
  error.

## Invariants

All classification happens from syntax during macro expansion. The generated
code uses no `unsafe`, performs no runtime type inspection, and reports invalid
component shapes as compile errors.

The macro records a `Map` value as a managed child kind; normal Rust trait
checking verifies the payload or component bounds when that field is used.

## It does not own

The crate does not define component runtime semantics, persist fields, execute
transitions, or choose lifecycle state. Those contracts live in
`tokeira-chasm` and component crates.

## Pointers

- [Macro implementation and rustdoc](../../crates/tokeira-chasm-derive/src/lib.rs)
- [CHASM substrate](chasm.md)
- [Standalone activity component](chasm-activity.md)
