//! Assembly of the transient program: facade + lowered operator source +
//! driver.
//!
//! The program is transient by contract — never persisted, never surfaced,
//! covered byte-for-byte by the source map. Its final expression is the whole
//! structural result: the operator's config value plus the deployment
//! builder's envelope, crossing the sandbox boundary exactly once.

use ruff_text_size::TextSize;

use crate::{
    lower::Lowered,
    source_map::{Origin, Segment, SourceMap, SourceMapBuilder},
};

/// The driver appended after the operator source. `config` and `deployment`
/// exist and have exact arities (preflight), so the calls cannot miss.
const DRIVER: &str = "\n\
__tokeira_internal_cfg = config()\n\
__tokeira_internal_dep = deployment(__tokeira_internal_cfg, __tokeira_internal_Context())\n\
{\"config\": __tokeira_internal_export(__tokeira_internal_cfg), \"deployment\": __tokeira_internal_dep.__tokeira_internal_envelope()}\n";

/// A fully assembled program with its byte-covering map.
#[derive(Debug)]
pub struct Program {
    /// Complete transient-program text.
    pub text: String,
    /// Map over exactly `text`.
    pub map: SourceMap,
}

/// Composes facade + lowered operator region + driver into one program.
pub fn assemble(facade: &str, lowered: Lowered) -> Program {
    let mut text = String::with_capacity(facade.len() + lowered.text.len() + DRIVER.len());
    let mut map = SourceMapBuilder::new();

    text.push_str(facade);
    map.push(TextSize::of(facade), Origin::Facade);

    let user_base = map.cursor();
    text.push_str(&lowered.text);
    for Segment { generated, origin } in lowered.segments {
        debug_assert_eq!(user_base + generated.start(), map.cursor());
        map.push(generated.len(), origin);
    }

    text.push_str(DRIVER);
    map.push(TextSize::of(DRIVER), Origin::Driver);

    Program {
        text,
        map: map.finish(),
    }
}
