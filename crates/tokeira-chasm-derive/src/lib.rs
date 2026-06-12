//! `#[derive(Component)]` for Tokeira CHASM components.
//!
//! This proc-macro crate is one half of the CHASM substrate's answer to the no-runtime-reflection
//! rule (`AGENTS.md §1`). Temporal's Go CHASM iterates a component's fields by sniffing
//! `"chasm.Field["` type-name strings and mutating user structs in place via reflection
//! (`fields_iterator.go`, `tree.go:810 syncSubComponents @ v1.31.0`). Tokeira forbids runtime
//! reflection, so it moves that work to compile time: `#[derive(Component)]` reads a struct's
//! declared `Field` / `Map` / `ParentPtr` members and generates the *static field registry*
//! (`Component::fields()`) that `tokeira-chasm` consumes — monomorphized, statically-known field
//! iteration in place of reflection (design HP1, Requirement 3).
//!
//! The macro is also where the component shape rules are enforced as `compile_error!`s rather than
//! runtime checks (Requirement 3.2–3.5):
//!
//! 1. **Exactly one `#[chasm(data)]`** field, and it must be a `Field<T>`. `T` becomes the
//!    component's `Data` associated type. Zero or many is a compile error.
//! 2. **Persistent fields are `Field` / `Map` / `ParentPtr`** — never bare values/pointers (node
//!    identity is positional). A non-transient field of an unrecognised type is a compile error
//!    (this also covers rule 4: unmanaged fields are rejected, not silently dropped — they must be
//!    explicitly `#[chasm(transient)]`).
//! 3. **`Map<K, T>` value `T`** must be a valid field payload (proto data or a child `Component`).
//!    At this layer the value type is recorded as a `Map`-kind child; the payload bound is enforced
//!    where the field is materialised against the tree (a deliberate, documented limit of the
//!    syntactic-only classification — the macro cannot see whether `T: Component` without type
//!    information). Layer 3's activity component exercises the real bound.
//!
//! It emits no `unsafe` and performs no runtime type inspection; all classification is from the
//! syntactic field types at expansion time.
//!
//! ## Generated shape
//!
//! `#[derive(Component)]` generates `impl Component`, supplying `Data` (the `#[chasm(data)]` field's
//! payload type), `FQN` (from `#[chasm(fqn = "...")]`), and `fields()` (the static registry). It does
//! **not** generate `lifecycle_state`: that is the one piece of real per-component behaviour the
//! author writes, by implementing the `Lifecycle` trait. Because `Component: Lifecycle`, a component
//! that derives `Component` but forgets `Lifecycle` fails to compile — the requirement is a real
//! bound, not a naming convention (see `tokeira_chasm::component` for the rationale).

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Field, Fields, GenericArgument, PathArguments, Type, parse_macro_input,
};

/// Derives `tokeira_chasm::Component` for a named-field struct, generating its static field registry
/// from the declared `Field` / `Map` / `ParentPtr` members and enforcing the component shape rules at
/// compile time (design HP1, Requirement 3).
///
/// The `#[chasm(...)]` helper attribute carries the component's fully-qualified name
/// (`#[chasm(fqn = "...")]`) and per-field markers (`#[chasm(data)]`, `#[chasm(transient)]`).
#[proc_macro_derive(Component, attributes(chasm))]
pub fn derive_component(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// The `FieldKind` a declared field maps to (mirrors `tokeira_chasm::FieldKind`).
#[derive(Clone, Copy)]
enum Kind {
    Data,
    Component,
    Map,
    Parent,
    Transient,
}

impl Kind {
    /// The `FieldKind` variant ident emitted into the generated registry.
    fn variant(self) -> proc_macro2::TokenStream {
        match self {
            Kind::Data => quote!(Data),
            Kind::Component => quote!(Component),
            Kind::Map => quote!(Map),
            Kind::Parent => quote!(Parent),
            Kind::Transient => quote!(Transient),
        }
    }
}

/// Build the token stream emitted for a `#[derive(Component)]` invocation, or a `syn::Error` carrying
/// the first violated shape rule.
fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;

    let fqn = parse_container_fqn(input)?;

    let fields = named_fields(input)?;

    let mut descriptors: Vec<(String, Kind)> = Vec::new();
    let mut data_ty: Option<Type> = None;
    let mut error: Option<syn::Error> = None;
    let record = |e: syn::Error, error: &mut Option<syn::Error>| match error {
        Some(existing) => existing.combine(e),
        None => *error = Some(e),
    };

    for field in fields {
        // A named-field struct guarantees an ident.
        let name = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "expected a named field"))?
            .to_string();
        let (is_data, is_transient) = parse_field_flags(field)?;
        let outer = outer_type_ident(&field.ty);

        if is_data {
            // Rule 1: the data field must be a `Field<T>`, declared exactly once.
            if outer.as_deref() != Some("Field") {
                record(
                    syn::Error::new_spanned(
                        &field.ty,
                        "#[chasm(data)] must annotate a `Field<T>` whose `T` is the component's proto data type",
                    ),
                    &mut error,
                );
            } else if data_ty.is_some() {
                record(
                    syn::Error::new_spanned(
                        field,
                        "a component may declare only one #[chasm(data)] field (Requirement 3.2)",
                    ),
                    &mut error,
                );
            } else {
                match first_generic_type(&field.ty) {
                    Some(ty) => data_ty = Some(ty),
                    None => record(
                        syn::Error::new_spanned(
                            &field.ty,
                            "#[chasm(data)] `Field<T>` must name its payload type `T`",
                        ),
                        &mut error,
                    ),
                }
            }
            descriptors.push((name, Kind::Data));
        } else if is_transient {
            descriptors.push((name, Kind::Transient));
        } else {
            // Rules 2 & 4: a non-transient field must be a recognised managed kind.
            match outer.as_deref() {
                Some("Field") => descriptors.push((name, Kind::Component)),
                Some("Map") => descriptors.push((name, Kind::Map)),
                Some("ParentPtr") => descriptors.push((name, Kind::Parent)),
                _ => record(
                    syn::Error::new_spanned(
                        field,
                        "unmanaged field: declare it as `Field`/`Map`/`ParentPtr`, or mark it `#[chasm(transient)]` if it must not be persisted (Requirement 3.3, 3.5)",
                    ),
                    &mut error,
                ),
            }
        }
    }

    if data_ty.is_none() && error.is_none() {
        record(
            syn::Error::new_spanned(
                ident,
                "a component must declare exactly one #[chasm(data)] field (Requirement 3.2)",
            ),
            &mut error,
        );
    }

    if let Some(error) = error {
        return Err(error);
    }

    // Unwrap is sound: the absence of a data type was turned into an error above.
    let data_ty = data_ty.ok_or_else(|| {
        syn::Error::new_spanned(ident, "internal: missing data type after validation")
    })?;

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let descriptor_tokens = descriptors.iter().map(|(name, kind)| {
        let variant = kind.variant();
        quote! {
            ::tokeira_chasm::FieldDescriptor::new(
                #name,
                ::tokeira_chasm::FieldKind::#variant,
            )
        }
    });

    Ok(quote! {
        impl #impl_generics ::tokeira_chasm::Component for #ident #ty_generics #where_clause {
            type Data = #data_ty;
            const FQN: &'static str = #fqn;
            fn fields(&self) -> ::tokeira_chasm::FieldRegistry<'_> {
                const FIELDS: &[::tokeira_chasm::FieldDescriptor] = &[
                    #(#descriptor_tokens),*
                ];
                ::tokeira_chasm::FieldRegistry::new(FIELDS)
            }
        }
    })
}

/// Read the required `#[chasm(fqn = "...")]` container attribute.
fn parse_container_fqn(input: &DeriveInput) -> syn::Result<String> {
    let mut fqn: Option<String> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("chasm") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("fqn") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                fqn = Some(lit.value());
                Ok(())
            } else {
                Err(meta
                    .error("unknown #[chasm(...)] container attribute; expected `fqn = \"...\"`"))
            }
        })?;
    }
    fqn.ok_or_else(|| {
        syn::Error::new_spanned(
            input,
            "#[derive(Component)] requires #[chasm(fqn = \"...\")] (the component's fully-qualified name)",
        )
    })
}

/// Read a field's `#[chasm(data)]` / `#[chasm(transient)]` markers. Returns `(is_data, is_transient)`.
fn parse_field_flags(field: &Field) -> syn::Result<(bool, bool)> {
    let mut is_data = false;
    let mut is_transient = false;
    for attr in &field.attrs {
        if !attr.path().is_ident("chasm") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("data") {
                is_data = true;
                Ok(())
            } else if meta.path.is_ident("transient") {
                is_transient = true;
                Ok(())
            } else {
                Err(meta
                    .error("unknown #[chasm(...)] field attribute; expected `data` or `transient`"))
            }
        })?;
    }
    if is_data && is_transient {
        return Err(syn::Error::new_spanned(
            field,
            "a field cannot be both #[chasm(data)] and #[chasm(transient)]",
        ));
    }
    Ok((is_data, is_transient))
}

/// The ident of a type's outermost path segment, e.g. `Field` for `Field<T>`.
fn outer_type_ident(ty: &Type) -> Option<String> {
    if let Type::Path(type_path) = ty {
        type_path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
    } else {
        None
    }
}

/// The first generic type argument of a path type, e.g. `T` for `Field<T>` or `K` for `Map<K, T>`.
fn first_generic_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    })
}

/// The named fields of a struct, or an error for non-struct / non-named-field inputs.
fn named_fields(
    input: &DeriveInput,
) -> syn::Result<&syn::punctuated::Punctuated<Field, syn::token::Comma>> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => Ok(&named.named),
            _ => Err(syn::Error::new_spanned(
                input,
                "#[derive(Component)] requires a struct with named fields",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            input,
            "#[derive(Component)] can only be applied to a struct",
        )),
    }
}
