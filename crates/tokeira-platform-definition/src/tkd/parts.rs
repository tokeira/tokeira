//! Multi-document definitions: the root's `mod <name>;` declarations load
//! sibling part documents through the platform's source resolver, and `use`
//! declarations take pub types across documents.
//!
//! Visibility model: the root declares every part (`mod` never appears in a
//! part) and calls a part's `pub` functions qualified
//! (`part::function(…)`); types cross documents only through
//! `use part::{Name, …}` — the root and parts alike take pub types from any
//! declared part. A part additionally sees the root's types as its backdrop
//! (the shared configuration language). Use edges among parts must form a
//! DAG; a cycle is refused naming the path. `use` admits no renames, so a
//! type keeps one identity everywhere it travels.
//!
//! Everything here runs before evaluation sees a single expression: parts
//! resolve, parse, and subset-check under part rules; the root
//! subset-checks against its effective types (own plus taken); and the
//! whole set's `#[create]`/`#[require]` admission is merged. Part errors
//! carry the part's file label in the message; a span would mislocate
//! against the root document, so they carry none.

use std::collections::BTreeMap;

use syn::{File, Item};
use tokeira_platform::definition::SourceResolver;

use crate::tkd::{
    admission::{self, Admission},
    bridge::HostBridge,
    schema::{self, FnTable, TypeTable, UseDecl},
    subset::{self, SubsetScope},
    value::EvalError,
};

/// One document's item tables — the root's or one part's.
#[derive(Debug)]
pub struct Tables {
    pub(crate) types: TypeTable,
    pub(crate) fns: FnTable,
    /// The document's `use` takes: local type name → source part. `use`
    /// admits no renames, so the local name is the item's name in the
    /// source part.
    pub(crate) uses: BTreeMap<String, String>,
}

/// The complete loaded program: the root plus every declared part, with the
/// set's merged admission schema.
#[derive(Debug)]
pub struct Scopes {
    pub(crate) root: Tables,
    pub(crate) parts: BTreeMap<String, Tables>,
    /// `#[create]`/`#[require]` over the whole set — the root's first, then
    /// each part's in name order. Admission reads the set, not one file.
    pub(crate) admission: Admission,
}

/// One parsed-but-not-yet-admitted part, held between the discovery pass
/// and the validations that need every part's tables present.
struct PendingPart {
    label: String,
    file: File,
    types: TypeTable,
    fns: FnTable,
    uses: Vec<UseDecl>,
}

/// Load the program: resolve and parse every part the root declares,
/// validate every document's `use` takes and their acyclicity, subset-check
/// each document against its effective types, and merge the set's
/// admission.
pub(crate) fn load<B: HostBridge>(
    root_file: &File,
    root_types: TypeTable,
    root_fns: FnTable,
    bridge: &B,
    resolver: &dyn SourceResolver,
) -> Result<Scopes, EvalError> {
    let root_type_names: Vec<String> = root_types.type_names().map(str::to_string).collect();

    // Discovery: resolve, parse, and collect every declared part. Nothing
    // cross-document is judged yet — those validations need the full set.
    let mut pending: BTreeMap<String, PendingPart> = BTreeMap::new();
    for item in &root_file.items {
        let Item::Mod(declared) = item else { continue };
        let name = declared.ident.to_string();
        if declared.content.is_some() {
            return Err(EvalError::new(format!(
                "inline module bodies are not allowed; declare `mod {name};` with the part in \
                 `{name}.tkd`"
            )));
        }
        if pending.contains_key(&name) {
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
            EvalError::new(format!(
                "{label}: {}",
                crate::tkd::located_parse_error(&error)
            ))
        })?;
        let (types, fns) = schema::collect(&file)?;
        // Inside a part, bare names resolve own-first then root types; a
        // part redefining a root type would shadow the shared configuration
        // language silently. Refused by name.
        if let Some(shadow) = types
            .type_names()
            .find(|ty| root_type_names.iter().any(|root_ty| root_ty == ty))
        {
            return Err(EvalError::new(format!(
                "{label}: `{shadow}` shadows a root type; parts may not redefine the shared \
                 configuration language"
            )));
        }
        let uses = schema::collect_uses(&file)
            .map_err(|error| EvalError::new(format!("{label}: {}", error.msg)))?;
        pending.insert(
            name,
            PendingPart {
                label,
                file,
                types,
                fns,
                uses,
            },
        );
    }

    // Validate the root's takes, then each part's, against the full set.
    let root_uses = schema::collect_uses(root_file)?;
    let root_use_map = validate_uses(&root_uses, None, &root_types, &pending)?;
    let mut part_use_maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (name, part) in &pending {
        let map = validate_uses(&part.uses, Some((name, &part.label)), &part.types, &pending)?;
        part_use_maps.insert(name.clone(), map);
    }
    refuse_use_cycles(&part_use_maps)?;

    // Subset: each document checks against its effective types — its own,
    // plus everything its takes bring in, plus (for parts) the root's types
    // as the backdrop. The parts map is assembled first because admitting
    // the root's `part::fn(...)` calls needs the loaded part tables.
    let mut parts: BTreeMap<String, Tables> = BTreeMap::new();
    let mut admission = admission::extract(root_file);
    for (name, part) in &pending {
        let mut effective = part.types.clone();
        let uses = &part_use_maps[name];
        for (local, source) in uses {
            effective.adopt(&pending[source].types, local);
        }
        effective.adopt_missing(&root_types);
        subset::check(&part.file, bridge, &effective, SubsetScope::Part).map_err(
            |diagnostics| {
                EvalError::new(format!(
                    "{}: part is outside the interpreted subset:\n{diagnostics}",
                    part.label
                ))
            },
        )?;
        let part_admission = admission::extract(&part.file);
        admission.creates.extend(part_admission.creates);
        admission.requires.extend(part_admission.requires);
    }
    for (name, part) in pending {
        parts.insert(
            name.clone(),
            Tables {
                types: part.types,
                fns: part.fns,
                uses: part_use_maps.remove(&name).expect("validated above"),
            },
        );
    }

    let mut root_effective = root_types.clone();
    for (local, source) in &root_use_map {
        root_effective.adopt(&parts[source].types, local);
    }
    subset::check(
        root_file,
        bridge,
        &root_effective,
        SubsetScope::Root { parts: &parts },
    )
    .map_err(|diagnostics| {
        let span = diagnostics.0.first().map(|diagnostic| diagnostic.span);
        EvalError::new(format!(
            "definition is outside the interpreted subset:\n{diagnostics}"
        ))
        .with_optional_span(span)
    })?;

    Ok(Scopes {
        root: Tables {
            types: root_types,
            fns: root_fns,
            uses: root_use_map,
        },
        parts,
        admission,
    })
}

/// Validate one document's `use` takes against the declared set, producing
/// the local-name → source-part map. `own` is `None` for the root.
fn validate_uses(
    uses: &[UseDecl],
    own: Option<(&String, &str)>,
    own_types: &TypeTable,
    pending: &BTreeMap<String, PendingPart>,
) -> Result<BTreeMap<String, String>, EvalError> {
    let prefix = |message: String| match own {
        Some((_, label)) => EvalError::new(format!("{label}: {message}")),
        None => EvalError::new(message),
    };
    let mut map = BTreeMap::new();
    for decl in uses {
        if own.is_some_and(|(name, _)| name == &decl.part) {
            return Err(prefix("a part does not `use` itself".to_string()));
        }
        let Some(target) = pending.get(&decl.part) else {
            return Err(prefix(format!(
                "`use {part}::…` names no declared part; the root declares parts with \
                 `mod {part};`",
                part = decl.part
            )));
        };
        for item in &decl.items {
            if own_types.is_struct(item) || own_types.is_enum(item) {
                return Err(prefix(format!(
                    "`use {}::{item}` collides with this document's own type `{item}`",
                    decl.part
                )));
            }
            if let Some(previous) = map.get(item) {
                return Err(prefix(format!(
                    "`{item}` is taken twice, from `{previous}` and `{}`",
                    decl.part
                )));
            }
            if target.fns.get(item).is_some() {
                return Err(prefix(format!(
                    "`use {}::{item}` takes a function; functions are called qualified \
                     (`{}::{item}(…)`) — `use` takes types",
                    decl.part, decl.part
                )));
            }
            if !(target.types.is_struct(item) || target.types.is_enum(item)) {
                return Err(prefix(format!("part `{}` has no type `{item}`", decl.part)));
            }
            if !target.types.is_pub_type(item) {
                return Err(prefix(format!(
                    "`{}::{item}` is not `pub`; a part exports what other documents may take",
                    decl.part
                )));
            }
            map.insert(item.clone(), decl.part.clone());
        }
    }
    Ok(map)
}

/// Refuse a cycle among the parts' use edges, naming the path. The root
/// cannot participate (nothing can `use` the root), so only part→part edges
/// are walked.
fn refuse_use_cycles(
    part_use_maps: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<(), EvalError> {
    const UNVISITED: u8 = 0;
    const IN_STACK: u8 = 1;
    const DONE: u8 = 2;

    fn visit(
        node: &str,
        edges: &BTreeMap<String, Vec<&str>>,
        marks: &mut BTreeMap<String, u8>,
        stack: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        marks.insert(node.to_string(), IN_STACK);
        stack.push(node.to_string());
        if let Some(nexts) = edges.get(node) {
            for next in nexts {
                match marks.get(*next).copied().unwrap_or(UNVISITED) {
                    IN_STACK => {
                        let start = stack
                            .iter()
                            .position(|n| n == next)
                            .expect("in-stack node is on the stack");
                        let mut cycle = stack[start..].to_vec();
                        cycle.push((*next).to_string());
                        return Some(cycle);
                    }
                    UNVISITED => {
                        if let Some(cycle) = visit(next, edges, marks, stack) {
                            return Some(cycle);
                        }
                    }
                    _ => {}
                }
            }
        }
        stack.pop();
        marks.insert(node.to_string(), DONE);
        None
    }

    let mut edges: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for (name, uses) in part_use_maps {
        let mut targets: Vec<&str> = uses.values().map(String::as_str).collect();
        targets.sort_unstable();
        targets.dedup();
        edges.insert(name.clone(), targets);
    }
    let mut marks = BTreeMap::new();
    let mut stack = Vec::new();
    for name in part_use_maps.keys() {
        if marks.get(name.as_str()).copied().unwrap_or(UNVISITED) == UNVISITED
            && let Some(cycle) = visit(name, &edges, &mut marks, &mut stack)
        {
            return Err(EvalError::new(format!(
                "use declarations form a cycle among parts: {}",
                cycle.join(" -> ")
            )));
        }
    }
    Ok(())
}
