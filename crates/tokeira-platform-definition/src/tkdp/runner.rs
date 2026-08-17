//! Execution of the transient program through unmodified Monty, with every
//! reported position translated back to operator terms.
//!
//! Monty only ever sees the transient program, so parse errors and runtime
//! tracebacks arrive in transient coordinates. This module owns the
//! translation: transient (line, char-column) → byte offset → source-map
//! segment → original offset → original (line, char-column). Operators see
//! `.tkdp` positions or a named internal region; transient coordinates never
//! escape.

use monty::MontyRun;
use monty_types::{CompileOptions, MontyException, MontyObject, PrintWriter, ResourceTracker};
use ruff_text_size::{TextRange, TextSize};

use crate::tkdp::{
    facade::FACADE_FILE_NAME,
    program::Program,
    source_map::{LineTable, Mapped, SourceMap},
};

/// The main program's file name in Monty tracebacks; frames from it map
/// through the source map to operator positions.
const TRANSIENT_FILE_NAME: &str = "definition.tkdp.transient";

/// Cap on captured print output attached to failure diagnostics.
const CAPTURED_OUTPUT_LIMIT: usize = 64 * 1024;

/// A failure already rendered in operator terms.
#[derive(Debug)]
pub struct RunFailure {
    /// Multi-line operator-facing rendering (summary plus mapped frames).
    pub message: String,
    /// Most specific operator-source range, when one frame mapped.
    pub range: Option<TextRange>,
}

/// Compiles and runs the program; failures translate through the maps.
pub fn execute(program: &Program, original: &str) -> Result<MontyObject, RunFailure> {
    let translator = Translator {
        root: FileContext {
            map: &program.map,
            lowered_table: LineTable::new(&program.text),
            lowered_text: &program.text,
            original_table: LineTable::new(original),
            original,
            display: "definition.tkdp",
        },
        parts: program
            .parts
            .iter()
            .map(|part| FileContext {
                map: &part.map,
                lowered_table: LineTable::new(&part.lowered),
                lowered_text: &part.lowered,
                original_table: LineTable::new(&part.original),
                original: &part.original,
                display: &part.file_name,
            })
            .collect(),
    };

    let runner = MontyRun::new_with_modules(
        program.text.clone(),
        TRANSIENT_FILE_NAME,
        Vec::new(),
        program.modules.clone(),
        CompileOptions::default(),
    )
    .map_err(|exc| translator.render(&exc, None))?;

    let mut output = String::new();
    let result = runner.run(
        Vec::new(),
        ResourceTracker::default(),
        PrintWriter::CollectString(&mut output, Some(CAPTURED_OUTPUT_LIMIT)),
    );
    result.map_err(|exc| translator.render(&exc, Some(&output)))
}

struct Translator<'a> {
    /// The main program: frames from it may carry the diagnostic range.
    root: FileContext<'a>,
    /// One context per registered part, keyed by its traceback file name.
    parts: Vec<FileContext<'a>>,
}

/// Everything needed to translate one file's Monty positions (over its
/// lowered text) back into the operator's original coordinates.
struct FileContext<'a> {
    map: &'a SourceMap,
    lowered_table: LineTable,
    lowered_text: &'a str,
    original_table: LineTable,
    original: &'a str,
    display: &'a str,
}

impl Translator<'_> {
    fn render(&self, exc: &MontyException, output: Option<&str>) -> RunFailure {
        let summary = match exc.message() {
            Some(msg) => format!("{}: {msg}", exc.exc_type()),
            None => exc.exc_type().to_string(),
        };
        let mut message = summary;
        let mut range = None;
        for frame in exc.traceback() {
            let (line, rendered) = match frame.filename.as_str() {
                // Main-program frames translate through the root map and may
                // carry the diagnostic range — ranges are root-source-relative
                // only.
                TRANSIENT_FILE_NAME => {
                    self.root
                        .render_frame(frame.start.line, frame.start.column, true)
                }
                // Facade frames are internal machinery, never operator code.
                FACADE_FILE_NAME => (None, "  in the tokeira facade (internal)".to_string()),
                // Part frames translate through the part's own map to its
                // original coordinates; they contribute text but no range.
                other => match self.parts.iter().find(|part| part.display == other) {
                    Some(part) => part.render_frame(frame.start.line, frame.start.column, false),
                    None => (
                        None,
                        format!("  at {other}:{}:{}", frame.start.line, frame.start.column),
                    ),
                },
            };
            message.push('\n');
            message.push_str(&rendered);
            if range.is_none() {
                range = line;
            }
        }
        if let Some(output) = output.filter(|output| !output.is_empty()) {
            message.push_str("\ncaptured output:\n");
            message.push_str(output.trim_end());
        }
        RunFailure { message, range }
    }
}

impl FileContext<'_> {
    /// Renders one frame; returns the mapped operator range when the frame
    /// landed in operator-derived text and this file may carry the range.
    fn render_frame(
        &self,
        line: u32,
        column: u32,
        allow_range: bool,
    ) -> (Option<TextRange>, String) {
        let offset = self.lowered_offset(line, column);
        match self.map.translate(offset) {
            Some(Mapped::Original { offset, context }) => {
                let (oline, ocol) = self.original_position(offset);
                let mut rendered = format!("  at {}:{oline}:{ocol}", self.display);
                if let Some(kind) = context {
                    rendered.push_str(&format!(" ({})", kind.describe()));
                }
                if let Some(text) = self.original_line_text(oline) {
                    rendered.push_str(&format!("\n    {}", text.trim_end()));
                }
                let range = allow_range.then_some(TextRange::empty(offset));
                (range, rendered)
            }
            Some(Mapped::Facade) => (None, "  in the tokeira facade (internal)".to_string()),
            Some(Mapped::Driver) => (None, "  in the tokeira driver (internal)".to_string()),
            None => (None, format!("  at <transient>:{line}:{column} (unmapped)")),
        }
    }

    /// Lowered-text (1-based line, 1-based char column) → byte offset.
    fn lowered_offset(&self, line: u32, column: u32) -> TextSize {
        let line_start = self.lowered_table.line_start(line);
        let line_text = &self.lowered_text[usize::from(line_start)..];
        let line_text = line_text.split('\n').next().unwrap_or("");
        let want = column.saturating_sub(1) as usize;
        let byte_col = line_text
            .char_indices()
            .nth(want)
            .map_or(line_text.len(), |(i, _)| i);
        line_start + TextSize::new(byte_col as u32)
    }

    /// Original byte offset → (1-based line, 1-based char column).
    fn original_position(&self, offset: TextSize) -> (u32, u32) {
        let (line, byte_col) = self.original_table.line_column(offset);
        let line_start = self.original_table.line_start(line);
        let line_text = &self.original[usize::from(line_start)..];
        let char_col = line_text[..(byte_col - 1) as usize].chars().count() as u32 + 1;
        (line, char_col)
    }

    fn original_line_text(&self, line: u32) -> Option<&str> {
        let start = usize::from(self.original_table.line_start(line));
        self.original[start..].split('\n').next()
    }
}
