//! Static tables extracted from the parsed `.tkd`: the config types it defines
//! (`TypeTable`) and its functions (`FnTable`). Engine-agnostic — names no
//! platform type.

use std::collections::HashMap;

use syn::{Fields, File, Item, ItemEnum, ItemFn, ItemStruct};

use crate::value::EvalError;

/// The `struct`/`enum` types the `.tkd` defines (the config schema). Used to
/// decide whether a named type is a config type (generic `Value`) or an author
/// type (routed to the bridge).
#[derive(Debug, Clone)]
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

    /// Every declared type name (structs and enums), for part shadowing
    /// checks.
    pub fn type_names(&self) -> impl Iterator<Item = &str> {
        self.structs
            .keys()
            .chain(self.enums.keys())
            .map(String::as_str)
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

    /// Whether the named type is declared `pub` — the export gate for
    /// cross-document `use`.
    pub fn is_pub_type(&self, name: &str) -> bool {
        let vis_is_pub = |vis: &syn::Visibility| matches!(vis, syn::Visibility::Public(_));
        self.structs
            .get(name)
            .map(|s| vis_is_pub(&s.vis))
            .or_else(|| self.enums.get(name).map(|e| vis_is_pub(&e.vis)))
            .unwrap_or(false)
    }

    /// Copies the named type from `source` into this table under the same
    /// name. Returns whether the source had it. Used to build a document's
    /// effective table (own types plus everything its `use` declarations
    /// bring in) for the subset pass; `use` admits no renames, so the local
    /// and source names are always identical and constructed values keep one
    /// type identity everywhere.
    pub fn adopt(&mut self, source: &TypeTable, name: &str) -> bool {
        if let Some(item) = source.structs.get(name) {
            self.structs.insert(name.to_string(), item.clone());
            return true;
        }
        if let Some(item) = source.enums.get(name) {
            self.enums.insert(name.to_string(), item.clone());
            return true;
        }
        false
    }

    /// Copies every type absent from this table in from `source` — the
    /// root-types backdrop a part's effective table stands on (own-first:
    /// nothing already present is overwritten).
    pub fn adopt_missing(&mut self, source: &TypeTable) {
        for (name, item) in &source.structs {
            self.structs
                .entry(name.clone())
                .or_insert_with(|| item.clone());
        }
        for (name, item) in &source.enums {
            self.enums
                .entry(name.clone())
                .or_insert_with(|| item.clone());
        }
    }
}

/// The `.tkd`'s functions (`config`, `deployment`, and any pure helpers).
#[derive(Debug)]
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

/// One `use` declaration, normalized: `use part::Name;` or
/// `use part::{A, B};` — a two-level path taking pub types from a declared
/// part by their own names.
#[derive(Debug, Clone)]
pub struct UseDecl {
    /// The part the names come from.
    pub part: String,
    /// The taken names, in source order.
    pub items: Vec<String>,
}

/// Collect and normalize the file's `use` declarations. The admitted form
/// is exactly `use <part>::<Name>;` / `use <part>::{<Name>, …};` — no
/// renames (`as` would split a type's identity between documents), no
/// globs (takes are explicit), no deeper paths (definitions are one level
/// deep).
pub fn collect_uses(file: &File) -> Result<Vec<UseDecl>, EvalError> {
    let mut uses = Vec::new();
    for item in &file.items {
        let Item::Use(u) = item else { continue };
        let syn::UseTree::Path(path) = &u.tree else {
            return Err(EvalError::new(
                "a `use` names a part and takes items from it: `use part::Name;` or \
                 `use part::{A, B};`",
            ));
        };
        let part = path.ident.to_string();
        let mut items = Vec::new();
        collect_use_leaves(&path.tree, &part, &mut items)?;
        uses.push(UseDecl { part, items });
    }
    Ok(uses)
}

fn collect_use_leaves(
    tree: &syn::UseTree,
    part: &str,
    items: &mut Vec<String>,
) -> Result<(), EvalError> {
    match tree {
        syn::UseTree::Name(name) => {
            items.push(name.ident.to_string());
            Ok(())
        }
        syn::UseTree::Group(group) => {
            for entry in &group.items {
                collect_use_leaves(entry, part, items)?;
            }
            Ok(())
        }
        syn::UseTree::Rename(rename) => Err(EvalError::new(format!(
            "`use {part}::{} as {}` is not allowed: a rename would split the type's \
             identity between documents — take the name as declared",
            rename.ident, rename.rename
        ))),
        syn::UseTree::Glob(_) => Err(EvalError::new(format!(
            "`use {part}::*` is not allowed; take names explicitly"
        ))),
        syn::UseTree::Path(nested) => Err(EvalError::new(format!(
            "`use {part}::{}::…` is not allowed; definitions are one level deep",
            nested.ident
        ))),
    }
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
                return Err(EvalError::new("`self` parameters are not allowed"));
            }
        }
    }
    Ok(names)
}
