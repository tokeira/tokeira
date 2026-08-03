//! Segmented mapping from the generated (lowered) program back to the
//! operator-authored `.tkdp` source.
//!
//! The lowered program is assembled from three regions — prelude, transformed
//! user source, driver — and the transformed region interleaves verbatim
//! copies of original text with generated scaffolding. Every byte of the
//! generated program is covered by exactly one [`Segment`], so any position a
//! Monty traceback reports can be resolved to either an original source
//! position (verbatim copies map linearly; scaffolding maps to the construct
//! that motivated it) or to a named internal region.
//!
//! Offsets are byte offsets. Only the original `.tkdp` file is ever shown to
//! the operator; the generated program is transient and inspectable on demand.

use ruff_text_size::{TextRange, TextSize};

/// What kind of construct a generated (non-verbatim) segment was emitted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginKind {
    /// The single evaluation of the match subject expression.
    MatchSubject,
    /// The type/field probe emitted for a case pattern.
    Pattern,
    /// A capture-variable binding extracted from a matched pattern.
    CaptureBinding,
    /// The wrapper around a verbatim case guard expression.
    Guard,
    /// The strict-exhaustion `raise` appended when no case matched.
    Exhaustion,
    /// Structural scaffolding tied to the match statement as a whole.
    Scaffold,
}

impl OriginKind {
    /// Operator-facing description used when annotating mapped tracebacks.
    pub fn describe(self) -> &'static str {
        match self {
            Self::MatchSubject => "while evaluating the match subject",
            Self::Pattern => "while probing a case pattern",
            Self::CaptureBinding => "while binding a pattern capture",
            Self::Guard => "while evaluating a case guard",
            Self::Exhaustion => "match statement fell through",
            Self::Scaffold => "in lowered match scaffolding",
        }
    }
}

/// Where a run of generated-program bytes came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// Byte-for-byte copy of original source. A position `p` inside the
    /// segment maps to `original_start + (p - segment.start)` — exact, not
    /// just line-granular, which is what makes caret positions in mapped
    /// tracebacks trustworthy.
    Verbatim { original_start: TextSize },
    /// Text synthesized by the lowering; `original` is the source construct
    /// that motivated it and is where diagnostics should point.
    Generated {
        original: TextRange,
        kind: OriginKind,
    },
    /// The fixed Tokeira model prelude prepended to every program.
    Prelude,
    /// The entrypoint driver appended after the user source.
    Driver,
}

/// One contiguous run of generated-program bytes with a single origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub generated: TextRange,
    pub origin: Origin,
}

/// A position in the generated program resolved back to operator terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mapped {
    /// Maps into the original `.tkdp` source. `context` carries the
    /// generated-segment kind when the position fell inside scaffolding
    /// rather than copied text.
    Original {
        offset: TextSize,
        context: Option<OriginKind>,
    },
    Prelude,
    Driver,
}

/// Complete map for one assembled program.
///
/// Invariant: segments are sorted, contiguous from offset 0, and cover the
/// entire generated text — enforced by [`SourceMapBuilder`], which is the only
/// way to construct one. Coverage is what lets `resolve` be a plain binary
/// search with no failure path for in-bounds offsets.
#[derive(Debug, Clone)]
pub struct SourceMap {
    segments: Vec<Segment>,
}

impl SourceMap {
    /// Resolves a byte offset in the generated program to its segment.
    /// Returns `None` only for offsets at or past the end of the program.
    pub fn resolve(&self, offset: TextSize) -> Option<&Segment> {
        let idx = self
            .segments
            .partition_point(|seg| seg.generated.end() <= offset);
        let seg = self.segments.get(idx)?;
        seg.generated.contains(offset).then_some(seg)
    }

    /// Translates a generated-program offset into operator terms.
    pub fn translate(&self, offset: TextSize) -> Option<Mapped> {
        let seg = self.resolve(offset)?;
        Some(match &seg.origin {
            Origin::Verbatim { original_start } => Mapped::Original {
                offset: *original_start + (offset - seg.generated.start()),
                context: None,
            },
            Origin::Generated { original, kind } => Mapped::Original {
                offset: original.start(),
                context: Some(*kind),
            },
            Origin::Prelude => Mapped::Prelude,
            Origin::Driver => Mapped::Driver,
        })
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
}

/// Builder enforcing the contiguous-coverage invariant of [`SourceMap`].
#[derive(Debug, Default)]
pub struct SourceMapBuilder {
    segments: Vec<Segment>,
    cursor: TextSize,
}

impl SourceMapBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current end offset of the covered generated text.
    pub fn cursor(&self) -> TextSize {
        self.cursor
    }

    /// Records `len` generated bytes with the given origin. Zero-length
    /// pushes are dropped so emitters can call this unconditionally.
    pub fn push(&mut self, len: TextSize, origin: Origin) {
        if len == TextSize::new(0) {
            return;
        }
        let start = self.cursor;
        self.cursor += len;
        // Merge adjacent verbatim runs that continue the same linear mapping,
        // so line-by-line emission of an unshifted region collapses into one
        // segment instead of one per line.
        if let (
            Some(Segment {
                generated,
                origin: Origin::Verbatim { original_start },
            }),
            Origin::Verbatim {
                original_start: new_start,
            },
        ) = (self.segments.last_mut(), &origin)
        {
            let continued = *original_start + generated.len() == *new_start;
            if continued && generated.end() == start {
                *generated = TextRange::new(generated.start(), self.cursor);
                return;
            }
        }
        self.segments.push(Segment {
            generated: TextRange::new(start, self.cursor),
            origin,
        });
    }

    pub fn finish(self) -> SourceMap {
        SourceMap {
            segments: self.segments,
        }
    }
}

/// Line table over a text buffer: byte offset ⇄ 1-based line/column.
///
/// Columns are 1-based byte columns within the line. Monty reports 1-based
/// lines and columns in tracebacks, so both sides of the translation use the
/// same convention and no ±1 adjustment leaks into call sites.
#[derive(Debug)]
pub struct LineTable {
    /// Byte offset of the start of each line; always begins with 0.
    starts: Vec<u32>,
}

impl LineTable {
    pub fn new(text: &str) -> Self {
        let mut starts = vec![0u32];
        for (i, b) in text.bytes().enumerate() {
            if b == b'\n' {
                // Safe cast: monty and ruff both reject > 4 GiB sources long
                // before this point.
                starts.push(u32::try_from(i + 1).unwrap_or(u32::MAX));
            }
        }
        Self { starts }
    }

    /// Byte offset of the start of a 1-based line, clamped to the last line.
    pub fn line_start(&self, line: u32) -> TextSize {
        let idx = (line.max(1) as usize - 1).min(self.starts.len() - 1);
        TextSize::new(self.starts[idx])
    }

    /// 1-based (line, column) for a byte offset.
    pub fn line_column(&self, offset: TextSize) -> (u32, u32) {
        let off = offset.to_u32();
        let line_idx = self.starts.partition_point(|&s| s <= off).saturating_sub(1);
        let col = off - self.starts[line_idx] + 1;
        // Casting the index is safe for the same size reasons as `new`.
        (u32::try_from(line_idx).unwrap_or(u32::MAX) + 1, col)
    }

    /// Byte offset for a 1-based (line, column).
    pub fn offset(&self, line: u32, column: u32) -> TextSize {
        self.line_start(line) + TextSize::new(column.saturating_sub(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verbatim_runs_merge_when_contiguous() {
        let mut b = SourceMapBuilder::new();
        b.push(
            TextSize::new(5),
            Origin::Verbatim {
                original_start: TextSize::new(10),
            },
        );
        b.push(
            TextSize::new(3),
            Origin::Verbatim {
                original_start: TextSize::new(15),
            },
        );
        let map = b.finish();
        assert_eq!(map.segments().len(), 1);
        assert_eq!(
            map.translate(TextSize::new(6)),
            Some(Mapped::Original {
                offset: TextSize::new(16),
                context: None
            })
        );
    }

    #[test]
    fn discontinuous_verbatim_runs_stay_separate() {
        let mut b = SourceMapBuilder::new();
        b.push(
            TextSize::new(5),
            Origin::Verbatim {
                original_start: TextSize::new(10),
            },
        );
        b.push(
            TextSize::new(3),
            Origin::Verbatim {
                original_start: TextSize::new(40),
            },
        );
        assert_eq!(b.finish().segments().len(), 2);
    }

    #[test]
    fn generated_segment_translates_to_origin_start() {
        let mut b = SourceMapBuilder::new();
        b.push(
            TextSize::new(8),
            Origin::Generated {
                original: TextRange::new(TextSize::new(3), TextSize::new(9)),
                kind: OriginKind::Pattern,
            },
        );
        let map = b.finish();
        assert_eq!(
            map.translate(TextSize::new(7)),
            Some(Mapped::Original {
                offset: TextSize::new(3),
                context: Some(OriginKind::Pattern)
            })
        );
        assert_eq!(map.translate(TextSize::new(8)), None);
    }

    #[test]
    fn line_table_round_trips() {
        let t = LineTable::new("ab\ncdef\n\nx");
        assert_eq!(t.line_column(TextSize::new(0)), (1, 1));
        assert_eq!(t.line_column(TextSize::new(4)), (2, 2));
        assert_eq!(t.line_column(TextSize::new(8)), (3, 1));
        assert_eq!(t.line_column(TextSize::new(9)), (4, 1));
        assert_eq!(t.offset(2, 2), TextSize::new(4));
        assert_eq!(t.offset(4, 1), TextSize::new(9));
    }
}
