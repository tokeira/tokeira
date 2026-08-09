//! Multi-document definitions: the root's `mod <name>;` declarations load
//! sibling part documents through the platform's source resolver. A part is
//! a namespace of items in the same language, evaluated under one-way
//! visibility: the root calls a part's `pub` functions; a part sees its own
//! items plus the root's types (the shared configuration language); parts
//! see nothing of each other — all wiring flows through the root.

use std::collections::BTreeMap;

use syn::{File, Item};
use tokeira_platform::definition::SourceResolver;

use crate::{
    bridge::HostBridge,
    schema::{self, FnTable, TypeTable},
    subset::{self, SubsetScope},
    value::EvalError,
};

/// One document's item tables — the root's or one part's.
#[derive(Debug)]
pub struct Tables {
    pub types: TypeTable,
    pub fns: FnTable,
}

/// The complete loaded program: the root plus every declared part.
#[derive(Debug)]
pub struct Scopes {
    pub root: Tables,
    pub parts: BTreeMap<String, Tables>,
}

/// Load every part the root declares: resolve, parse, subset-check under
/// part rules, and refuse shadowing — all before evaluation sees a single
/// expression. Part errors carry the part's file label in the message; a
/// span would mislocate against the root document, so they carry none.
pub fn load<B: HostBridge>(
    root_file: &File,
    root: Tables,
    bridge: &B,
    resolver: &dyn SourceResolver,
) -> Result<Scopes, EvalError> {
    let root_type_names: Vec<String> = root.types.type_names().map(str::to_string).collect();
    let mut parts = BTreeMap::new();
    for item in &root_file.items {
        let Item::Mod(declared) = item else { continue };
        let name = declared.ident.to_string();
        if declared.content.is_some() {
            return Err(EvalError::new(format!(
                "inline module bodies are not allowed; declare `mod {name};` with the part in \
                 `{name}.tkd`"
            )));
        }
        if parts.contains_key(&name) {
            return Err(EvalError::new(format!("part `{name}` is declared twice")));
        }
        // A part named like a root type would make `Name::x` ambiguous
        // between an enum path and a part item; refuse by name instead of
        // ranking one silently.
        if root_type_names.iter().any(|ty| ty == &name) {
            return Err(EvalError::new(format!(
                "part `{name}` shares its name with a root type; rename one so path \
                 resolution stays unambiguous"
            )));
        }
        let label = format!("{name}.tkd");
        let bytes = resolver
            .resolve(&name)
            .map_err(|error| EvalError::new(error.to_string()))?;
        let text = std::str::from_utf8(&bytes).map_err(|error| {
            EvalError::new(format!("{label}: part source is not UTF-8: {error}"))
        })?;
        let file = syn::parse_file(text).map_err(|error| {
            EvalError::new(format!("{label}: {}", crate::located_parse_error(&error)))
        })?;
        let (types, fns) = schema::collect(&file)?;
        let tables = Tables { types, fns };
        // Inside a part, bare names resolve own-first then root types; a
        // part redefining a root type would shadow the shared configuration
        // language silently. Refused by name.
        if let Some(shadow) = tables
            .types
            .type_names()
            .find(|ty| root_type_names.iter().any(|root_ty| root_ty == ty))
        {
            return Err(EvalError::new(format!(
                "{label}: `{shadow}` shadows a root type; parts may not redefine the shared \
                 configuration language"
            )));
        }
        subset::check(&file, bridge, &tables.types, SubsetScope::Part).map_err(|diagnostics| {
            EvalError::new(format!(
                "{label}: part is outside the interpreted subset:\n{diagnostics}"
            ))
        })?;
        parts.insert(name, tables);
    }
    Ok(Scopes { root, parts })
}
