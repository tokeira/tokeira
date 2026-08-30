//! Assembly of the transient program: lowered operator source + driver, with
//! the facade riding beside it as a registered module.
//!
//! The program is transient by contract — never persisted, never surfaced,
//! covered byte-for-byte by the source map. Its final expression is the whole
//! structural result: the operator's config value plus the deployment
//! builder's envelope, crossing the sandbox boundary exactly once. The
//! facade is not part of the program text: it registers with Monty as a
//! genuine `tokeira` module, imported by the driver below and by the
//! operator's own `from tokeira import …` lines.

use monty_types::SourceModule;
use ruff_text_size::TextSize;

use crate::tkdp::{
    facade::{FACADE_FILE_NAME, FACADE_MODULE_NAME},
    lower::Lowered,
    preflight::CreateField,
    source_map::{Origin, Segment, SourceMap, SourceMapBuilder},
};

/// The driver appended after the operator source. `config` and `deployment`
/// exist and have exact arities (preflight), so the calls cannot miss. Its
/// internal names come from the facade module like everyone else's — the
/// explicit from-import of underscore names is ordinary Python. The match
/// helper is imported here too because the lowering's scaffolding calls it
/// as a main-program global; the import runs before `deployment()` does, so
/// the binding exists by the time any lowered match executes.
const DRIVER: &str = "\n\
from tokeira import __tokeira_internal_export, __tokeira_internal_match, __tokeira_internal_Context\n\
__tokeira_internal_cfg = config()\n\
__tokeira_internal_dep = deployment(__tokeira_internal_cfg, __tokeira_internal_Context())\n\
{\"config\": __tokeira_internal_export(__tokeira_internal_cfg), \"deployment\": __tokeira_internal_dep.__tokeira_internal_envelope()}\n";

/// One prepared definition part heading into assembly: validated, lowered,
/// carrying its original text for traceback translation.
#[derive(Debug)]
pub struct PartUnit {
    /// The importable module name.
    pub(crate) name: String,
    /// The part's file name, shown in tracebacks (`<name>.tkdp`).
    pub(crate) file_name: String,
    /// The part's original source, for position translation and previews.
    pub(crate) original: String,
    /// The part's lowered form (match splice applied).
    pub(crate) lowered: Lowered,
    /// Create-time admission metadata declared by this companion.
    pub(crate) creates: Vec<CreateField>,
}

/// Per-part translation data: everything the runner needs to render a frame
/// from this part in the operator's own coordinates.
#[derive(Debug)]
pub struct PartTranslation {
    /// The part's file name — the key Monty traceback frames carry.
    pub(crate) file_name: String,
    /// The part's original source.
    pub(crate) original: String,
    /// The lowered text Monty executes for this part.
    pub(crate) lowered: String,
    /// Map over exactly `lowered`, back to `original` positions.
    pub(crate) map: SourceMap,
}

/// A fully assembled program: the main text with its byte-covering map, the
/// source modules Monty registers beside it (the facade always; the
/// definition's parts when it has them), and per-part translation data.
#[derive(Debug)]
pub struct Program {
    /// Complete transient-program text.
    pub text: String,
    /// Map over exactly `text`.
    pub map: SourceMap,
    /// Modules registered with the run: the facade first, then parts.
    pub modules: Vec<SourceModule>,
    /// Translation data for each part, parallel to `modules[1..]`.
    pub parts: Vec<PartTranslation>,
}

/// Composes the lowered operator region and driver into the main program,
/// with the rendered facade as the first registered module and each part as
/// a further one.
pub(crate) fn assemble(facade: String, lowered: Lowered, parts: Vec<PartUnit>) -> Program {
    let mut text = String::with_capacity(lowered.text.len() + DRIVER.len());
    let mut map = SourceMapBuilder::new();

    let user_base = map.cursor();
    text.push_str(&lowered.text);
    for Segment { generated, origin } in lowered.segments {
        debug_assert_eq!(user_base + generated.start(), map.cursor());
        map.push(generated.len(), origin);
    }

    text.push_str(DRIVER);
    map.push(TextSize::of(DRIVER), Origin::Driver);

    let mut modules = vec![SourceModule {
        name: FACADE_MODULE_NAME.to_owned(),
        file_name: FACADE_FILE_NAME.to_owned(),
        code: facade,
    }];
    let mut translations = Vec::with_capacity(parts.len());
    for part in parts {
        let mut part_map = SourceMapBuilder::new();
        for Segment { generated, origin } in part.lowered.segments {
            debug_assert_eq!(generated.start(), part_map.cursor());
            part_map.push(generated.len(), origin);
        }
        modules.push(SourceModule {
            name: part.name,
            file_name: part.file_name.clone(),
            code: part.lowered.text.clone(),
        });
        translations.push(PartTranslation {
            file_name: part.file_name,
            original: part.original,
            lowered: part.lowered.text,
            map: part_map.finish(),
        });
    }

    Program {
        text,
        map: map.finish(),
        modules,
        parts: translations,
    }
}
