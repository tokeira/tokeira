//! Static tables extracted from the parsed `.tkd`: the config types it defines
//! (`TypeTable`) and its functions (`FnTable`). Engine-agnostic — names no
//! `crate::builder`/`crate::kinds`.

use std::collections::HashMap;

use syn::{Fields, File, Item, ItemEnum, ItemFn, ItemStruct};

use super::value::EvalError;

/// The `struct`/`enum` types the `.tkd` defines (the config schema). Used to
/// decide whether a named type is a config type (generic `Value`) or an author
/// type (routed to the registry).
pub struct TypeTable {
    structs: HashMap<String, ItemStruct>,
    enums: HashMap<String, ItemEnum>,
}

impl TypeTable {
    pub fn is_struct(&self, name: &str) -> bool {
        self.structs.contains_key(name)
    }

    pub fn is_enum(&self, name: &str) -> bool {
        self.enums.contains_key(name)
    }

    pub fn enum_has_unit_variant(&self, ty: &str, variant: &str) -> bool {
        self.enums.get(ty).is_some_and(|e| {
            e.variants
                .iter()
                .any(|v| v.ident == variant && matches!(v.fields, Fields::Unit))
        })
    }

    /// Whether `ty` is an enum with a variant named `variant` (any shape). Used
    /// to reject typo'd variants that would otherwise build a phantom enum value.
    pub fn enum_has_variant(&self, ty: &str, variant: &str) -> bool {
        self.enums
            .get(ty)
            .is_some_and(|e| e.variants.iter().any(|v| v.ident == variant))
    }

    /// The declared named-field names of a config struct, for exact-set
    /// validation of struct literals (no missing / no unknown fields).
    pub fn struct_field_names(&self, name: &str) -> Option<Vec<String>> {
        self.structs.get(name).map(|s| match &s.fields {
            Fields::Named(named) => named
                .named
                .iter()
                .filter_map(|f| f.ident.as_ref().map(|i| i.to_string()))
                .collect(),
            _ => Vec::new(),
        })
    }
}

/// The `.tkd`'s functions (`config`, `deployment`, and any pure helpers).
pub struct FnTable {
    fns: HashMap<String, ItemFn>,
}

impl FnTable {
    pub fn get(&self, name: &str) -> Option<&ItemFn> {
        self.fns.get(name)
    }
}

/// Collect the type + function tables from a parsed file. (The subset pass is
/// responsible for rejecting disallowed items; this just indexes what's present.)
pub fn collect(file: &File) -> Result<(TypeTable, FnTable), EvalError> {
    let mut structs = HashMap::new();
    let mut enums = HashMap::new();
    let mut fns = HashMap::new();
    for item in &file.items {
        match item {
            Item::Struct(s) => {
                structs.insert(s.ident.to_string(), s.clone());
            }
            Item::Enum(e) => {
                enums.insert(e.ident.to_string(), e.clone());
            }
            Item::Fn(f) => {
                fns.insert(f.sig.ident.to_string(), f.clone());
            }
            _ => {}
        }
    }
    Ok((TypeTable { structs, enums }, FnTable { fns }))
}

/// The parameter binding names of a function (`deployment(cfg, cx)` → `[cfg, cx]`).
pub fn fn_param_names(f: &ItemFn) -> Result<Vec<String>, EvalError> {
    let mut names = Vec::new();
    for input in &f.sig.inputs {
        match input {
            syn::FnArg::Typed(pt) => match &*pt.pat {
                syn::Pat::Ident(pi) => names.push(pi.ident.to_string()),
                _ => return Err(EvalError::new("unsupported function parameter pattern")),
            },
            syn::FnArg::Receiver(_) => {
                return Err(EvalError::new("`self` parameters are not allowed"))
            }
        }
    }
    Ok(names)
}
