//! Lowering of restricted `match` statements into Monty-supported Python.
//!
//! The transform is a *splice*, not a re-generation: everything outside a
//! `match` statement is copied byte-for-byte (so comments, formatting, and —
//! crucially — source ranges survive), and each `match` is replaced by a flat
//! `if` chain over a done-flag. Only scaffolding lines are synthesized; case
//! bodies, guards, and subject expressions are verbatim slices of the
//! original text. That choice is what makes the source map exact where it
//! matters: any position inside operator-authored code maps back linearly.
//!
//! Import statements are untouched: `from tokeira import …` resolves against
//! the registered facade module, part imports resolve against their
//! registered modules, and everything else is Monty's to answer at runtime —
//! so every import line survives byte-for-byte and the map stays linear.
//!
//! Lowered shape for `match <subj>:` (match index `n`, case index `k`):
//!
//! ```text
//! __tokeira_internal_subject_n = (<subj verbatim>)
//! __tokeira_internal_done_n = False
//! if not __tokeira_internal_done_n:                  # case k
//!     <probe>                                        # kind-specific, below
//!         <captures>
//!         if (<guard verbatim>):                     # only when guarded
//!             __tokeira_internal_done_n = True
//!             <case body, verbatim lines>
//! if not __tokeira_internal_done_n:                  # unless an irrefutable
//!     raise RuntimeError("<file>:<line>: …")         # case exists
//! ```
//!
//! Probes: class patterns call the facade helper
//! `__tokeira_internal_match(subject, Cls, ["field", …])` (exact
//! `type(subject) is Cls` identity — algebraic-variant semantics); literals
//! compare `== (<lit>)`; singletons compare `is`; capture/wildcard probe
//! `if True:`.
//!
//! Semantics are CPython-faithful by construction: one subject evaluation,
//! first match wins, captures bind before the guard runs and stay bound if
//! the guard fails, later guards never run once a case has taken. The one
//! sanctioned deviation is strict exhaustion: a match whose cases all fail
//! raises with the definition position — a configuration that matches
//! nothing is a defect, not a no-op.
//!
//! Depth is bounded: every case restarts from the match's own indentation, so
//! a 30-case match nests no deeper than a 2-case one.

use ruff_python_ast::{self as ast, MatchCase, ModModule, Pattern, Stmt};
use ruff_text_size::{Ranged, TextRange, TextSize};

use crate::{
    preflight::RESERVED_PREFIX,
    source_map::{LineTable, Origin, OriginKind, Segment, SourceMapBuilder},
};

/// One generated indentation step. Independent of the definition's own indent
/// width; scaffolding is always four-space.
const STEP: &str = "    ";

/// The lowered operator region: transformed text plus segments relative to it.
#[derive(Debug)]
pub struct Lowered {
    /// Transformed source text.
    pub text: String,
    /// Byte-covering segments relative to `text`.
    pub segments: Vec<Segment>,
}

/// Lowers all match statements in a preflight-validated module.
pub fn lower(source: &str, module: &ModModule, file_label: &str) -> Lowered {
    let mut emitter = Emitter {
        source,
        file_label,
        line_table: LineTable::new(source),
        out: String::new(),
        map: SourceMapBuilder::new(),
        match_counter: 0,
    };
    let bounds = TextRange::new(TextSize::new(0), TextSize::of(source));
    emitter.emit_region(&module.body, bounds, 0);
    // A definition whose last line lacks a newline would glue the driver onto
    // it during assembly.
    if !emitter.out.ends_with('\n') && !emitter.out.is_empty() {
        emitter.push_generated("\n", TextRange::empty(bounds.end()), OriginKind::Scaffold);
    }
    Lowered {
        text: emitter.out,
        segments: emitter.map.finish().segments().to_vec(),
    }
}

struct Emitter<'a> {
    source: &'a str,
    file_label: &'a str,
    line_table: LineTable,
    out: String,
    map: SourceMapBuilder,
    match_counter: usize,
}

impl Emitter<'_> {
    // ------------------------------------------------------------------
    // Low-level emission
    // ------------------------------------------------------------------

    fn push_generated(&mut self, text: &str, original: TextRange, kind: OriginKind) {
        self.out.push_str(text);
        self.map
            .push(TextSize::of(text), Origin::Generated { original, kind });
    }

    fn push_verbatim(&mut self, range: TextRange) {
        let slice = &self.source[range];
        self.out.push_str(slice);
        self.map.push(
            range.len(),
            Origin::Verbatim {
                original_start: range.start(),
            },
        );
    }

    // ------------------------------------------------------------------
    // Line geometry over the original source
    // ------------------------------------------------------------------

    /// Offset of the first byte of the line containing `offset`.
    fn line_begin(&self, offset: TextSize) -> TextSize {
        match self.source[..usize::from(offset)].rfind('\n') {
            Some(i) => TextSize::new(i as u32 + 1),
            None => TextSize::new(0),
        }
    }

    /// Offset one past the newline ending the line containing `offset`
    /// (or end of file).
    fn line_end_incl(&self, offset: TextSize) -> TextSize {
        match self.source[usize::from(offset)..].find('\n') {
            Some(i) => offset + TextSize::new(i as u32 + 1),
            None => TextSize::of(self.source),
        }
    }

    /// Leading whitespace of the line containing `offset`.
    fn leading_ws(&self, offset: TextSize) -> &str {
        let begin = usize::from(self.line_begin(offset));
        let line = &self.source[begin..];
        let end = line.len() - line.trim_start_matches([' ', '\t']).len();
        &line[..end]
    }

    /// Byte column (0-based) of `offset` within its line.
    fn column(&self, offset: TextSize) -> usize {
        usize::from(offset) - usize::from(self.line_begin(offset))
    }

    // ------------------------------------------------------------------
    // Region copying
    // ------------------------------------------------------------------

    /// Copies a line-aligned range, shifting each line's indentation by
    /// `delta` bytes of spaces. `delta == 0` degenerates to one verbatim
    /// segment; blank lines are never padded.
    ///
    /// Documented boundary: a shift also displaces the continuation lines of
    /// triple-quoted strings, since the copy is line-based, not token-aware.
    /// `delta != 0` only occurs under guarded case bodies and
    /// non-four-space indentation.
    fn copy_shifted(&mut self, range: TextRange, delta: i32) {
        if range.is_empty() {
            return;
        }
        if delta == 0 {
            self.push_verbatim(range);
            return;
        }
        let mut cursor = range.start();
        while cursor < range.end() {
            let end = self.line_end_incl(cursor).min(range.end());
            let line = &self.source[TextRange::new(cursor, end)];
            let blank_line = line.trim().is_empty();
            if blank_line {
                self.push_verbatim(TextRange::new(cursor, end));
            } else if delta > 0 {
                let pad = " ".repeat(delta as usize);
                self.push_generated(&pad, TextRange::empty(cursor), OriginKind::Scaffold);
                self.push_verbatim(TextRange::new(cursor, end));
            } else {
                let ws_len = line.len() - line.trim_start_matches(' ').len();
                let strip = ws_len.min((-delta) as usize);
                self.push_verbatim(TextRange::new(cursor + TextSize::new(strip as u32), end));
            }
            cursor = end;
        }
    }

    /// Emits a statement region: statements free of `match` copy through in
    /// bulk (preserving interleaved comments and blank lines); `match`
    /// statements expand; other compounds that *contain* a match recurse
    /// suite-by-suite with their headers copied verbatim.
    fn emit_region(&mut self, stmts: &[Stmt], bounds: TextRange, delta: i32) {
        let mut cursor = bounds.start();
        for stmt in stmts {
            if !contains_match(stmt) {
                continue;
            }
            let stmt_line_begin = self.line_begin(stmt.range().start());
            self.copy_shifted(TextRange::new(cursor, stmt_line_begin), delta);
            if let Stmt::Match(m) = stmt {
                self.expand_match(m, delta);
            } else {
                self.emit_compound(stmt, delta);
            }
            cursor = self.line_end_incl(stmt.range().end());
        }
        self.copy_shifted(TextRange::new(cursor, bounds.end()), delta);
    }

    /// Recurses into the suites of a non-match compound statement that has a
    /// match somewhere beneath it. Headers (`if cond:`, `else:`, `except:`)
    /// are copied verbatim via the inter-suite gaps.
    fn emit_compound(&mut self, stmt: &Stmt, delta: i32) {
        let mut cursor = self.line_begin(stmt.range().start());
        for suite in match_bearing_suites(stmt) {
            let (Some(first), Some(last)) = (suite.first(), suite.last()) else {
                continue;
            };
            let begin = self.line_begin(first.range().start());
            self.copy_shifted(TextRange::new(cursor, begin), delta);
            let bounds = TextRange::new(begin, self.line_end_incl(last.range().end()));
            self.emit_region(suite, bounds, delta);
            cursor = bounds.end();
        }
        self.copy_shifted(
            TextRange::new(cursor, self.line_end_incl(stmt.range().end())),
            delta,
        );
    }

    // ------------------------------------------------------------------
    // Match expansion
    // ------------------------------------------------------------------

    fn expand_match(&mut self, m: &ast::StmtMatch, delta: i32) {
        let n = self.match_counter;
        self.match_counter += 1;

        let prefix = shift_ws(self.leading_ws(m.range().start()), delta);
        // The `match` keyword itself, used as the origin for whole-statement
        // scaffolding.
        let kw = TextRange::at(m.range().start(), TextSize::new(5));

        // Single subject evaluation; parentheses admit multi-line subjects.
        self.push_generated(
            &format!("{prefix}{RESERVED_PREFIX}subject_{n} = ("),
            m.subject.range(),
            OriginKind::MatchSubject,
        );
        self.push_verbatim(m.subject.range());
        self.push_generated(")\n", m.subject.range(), OriginKind::MatchSubject);
        self.push_generated(
            &format!("{prefix}{RESERVED_PREFIX}done_{n} = False\n"),
            kw,
            OriginKind::Scaffold,
        );

        for (k, case) in m.cases.iter().enumerate() {
            self.emit_case(n, k, case, &prefix);
        }

        // Dead code after an unguarded irrefutable case, so not emitted then.
        let has_irrefutable = m.cases.iter().any(is_irrefutable);
        if !has_irrefutable {
            let (line, _) = self.line_table.line_column(m.range().start());
            let file = self.file_label.replace('\\', "\\\\").replace('"', "\\\"");
            self.push_generated(
                &format!(
                    "{prefix}if not {RESERVED_PREFIX}done_{n}:\n\
                     {prefix}{STEP}raise RuntimeError(\"{file}:{line}: match fell through: no \
                     case matched \" + repr({RESERVED_PREFIX}subject_{n}))\n"
                ),
                kw,
                OriginKind::Exhaustion,
            );
        }
    }

    fn emit_case(&mut self, n: usize, k: usize, case: &MatchCase, prefix: &str) {
        let pattern_range = case.pattern.range();
        self.push_generated(
            &format!("{prefix}if not {RESERVED_PREFIX}done_{n}:\n"),
            pattern_range,
            OriginKind::Pattern,
        );

        // Probe + captures. Every shape leaves the "matched" suite open at
        // `prefix + 2*STEP`, so the guard/body emission below is uniform.
        match &case.pattern {
            Pattern::MatchClass(class) => {
                let cls = &self.source[class.cls.range()];
                let fields: Vec<String> = class
                    .arguments
                    .keywords
                    .iter()
                    .map(|kw| format!("\"{}\"", kw.attr.as_str()))
                    .collect();
                self.push_generated(
                    &format!(
                        "{prefix}{STEP}{RESERVED_PREFIX}case_{n}_{k} = {RESERVED_PREFIX}match(\
                         {RESERVED_PREFIX}subject_{n}, {cls}, [{}])\n",
                        fields.join(", ")
                    ),
                    pattern_range,
                    OriginKind::Pattern,
                );
                self.push_generated(
                    &format!("{prefix}{STEP}if {RESERVED_PREFIX}case_{n}_{k} is not None:\n"),
                    pattern_range,
                    OriginKind::Pattern,
                );
                for (i, kw) in class.arguments.keywords.iter().enumerate() {
                    // `field=_` probes the field but binds nothing.
                    let Pattern::MatchAs(sub) = &kw.pattern else {
                        continue;
                    };
                    let Some(name) = &sub.name else { continue };
                    self.push_generated(
                        &format!(
                            "{prefix}{STEP}{STEP}{name} = {RESERVED_PREFIX}case_{n}_{k}[{i}]\n",
                            name = name.as_str()
                        ),
                        kw.range(),
                        OriginKind::CaptureBinding,
                    );
                }
            }
            Pattern::MatchValue(value) => {
                self.push_generated(
                    &format!("{prefix}{STEP}if {RESERVED_PREFIX}subject_{n} == ("),
                    pattern_range,
                    OriginKind::Pattern,
                );
                self.push_verbatim(value.value.range());
                self.push_generated("):\n", pattern_range, OriginKind::Pattern);
            }
            Pattern::MatchSingleton(singleton) => {
                // PEP 634: None/True/False compare by identity, not equality.
                let literal = match singleton.value {
                    ast::Singleton::None => "None",
                    ast::Singleton::True => "True",
                    ast::Singleton::False => "False",
                };
                self.push_generated(
                    &format!("{prefix}{STEP}if {RESERVED_PREFIX}subject_{n} is {literal}:\n"),
                    pattern_range,
                    OriginKind::Pattern,
                );
            }
            Pattern::MatchAs(as_pattern) => {
                self.push_generated(
                    &format!("{prefix}{STEP}if True:\n"),
                    pattern_range,
                    OriginKind::Pattern,
                );
                if let Some(name) = &as_pattern.name {
                    self.push_generated(
                        &format!(
                            "{prefix}{STEP}{STEP}{name} = {RESERVED_PREFIX}subject_{n}\n",
                            name = name.as_str()
                        ),
                        pattern_range,
                        OriginKind::CaptureBinding,
                    );
                }
            }
            // Preflight rejected everything else before lowering ran.
            unsupported => unreachable!("preflight admitted unsupported pattern: {unsupported:?}"),
        }

        // Guard evaluates after captures are bound (CPython order); a failed
        // guard leaves them bound, exactly as CPython does.
        let mut body_col = prefix.len() + 2 * STEP.len();
        if let Some(guard) = &case.guard {
            self.push_generated(
                &format!("{prefix}{STEP}{STEP}if ("),
                guard.range(),
                OriginKind::Guard,
            );
            self.push_verbatim(guard.range());
            self.push_generated("):\n", guard.range(), OriginKind::Guard);
            body_col += STEP.len();
        }
        let body_prefix = " ".repeat(body_col);
        self.push_generated(
            &format!("{body_prefix}{RESERVED_PREFIX}done_{n} = True\n"),
            pattern_range,
            OriginKind::Scaffold,
        );

        self.emit_case_body(case, body_col);
    }

    /// Emits a case body at the target column. Block bodies re-indent by a
    /// per-case delta (zero for four-space-indented sources with unguarded
    /// cases); inline bodies (`case _: x = 1`) splice onto their own line.
    fn emit_case_body(&mut self, case: &MatchCase, body_col: usize) {
        let (Some(first), Some(last)) = (case.body.first(), case.body.last()) else {
            return;
        };
        let case_line = self.line_begin(case.range().start());
        let first_line = self.line_begin(first.range().start());
        if case_line == first_line {
            // Inline suite. It cannot contain a match statement (compound
            // statements are not valid in an inline suite), so a plain text
            // splice is complete.
            let pad = " ".repeat(body_col);
            self.push_generated(
                &pad,
                TextRange::empty(first.range().start()),
                OriginKind::Scaffold,
            );
            self.push_verbatim(TextRange::new(first.range().start(), last.range().end()));
            self.push_generated(
                "\n",
                TextRange::empty(last.range().end()),
                OriginKind::Scaffold,
            );
            return;
        }
        // `body_col` already carries the accumulated shift (it is derived
        // from the shifted prefix), so the body delta is measured against the
        // body's *original* column with no further adjustment.
        let body_delta = body_col as i32 - self.column(first.range().start()) as i32;
        let bounds = TextRange::new(first_line, self.line_end_incl(last.range().end()));
        self.emit_region(&case.body, bounds, body_delta);
    }
}

/// `prefix` for a line whose original leading whitespace was `ws`, shifted by
/// the accumulated re-indent `delta`.
fn shift_ws(ws: &str, delta: i32) -> String {
    if delta >= 0 {
        format!("{ws}{}", " ".repeat(delta as usize))
    } else {
        let keep = ws.len().saturating_sub((-delta) as usize);
        ws[..keep].to_owned()
    }
}

/// Wildcard or bare capture without a guard — the case always takes, so the
/// strict-exhaustion raise would be dead code after it.
fn is_irrefutable(case: &MatchCase) -> bool {
    case.guard.is_none() && matches!(&case.pattern, Pattern::MatchAs(p) if p.pattern.is_none())
}

fn contains_match(stmt: &Stmt) -> bool {
    if matches!(stmt, Stmt::Match(_)) {
        return true;
    }
    all_suites(stmt)
        .iter()
        .any(|suite| suite.iter().any(contains_match))
}

/// Suites of `stmt` that transitively contain a match statement.
fn match_bearing_suites(stmt: &Stmt) -> Vec<&[Stmt]> {
    all_suites(stmt)
        .into_iter()
        .filter(|suite| suite.iter().any(contains_match))
        .collect()
}

/// Every directly nested suite of a compound statement, in source order.
fn all_suites(stmt: &Stmt) -> Vec<&[Stmt]> {
    match stmt {
        Stmt::FunctionDef(s) => vec![&s.body],
        Stmt::ClassDef(s) => vec![&s.body],
        Stmt::If(s) => {
            let mut suites: Vec<&[Stmt]> = vec![&s.body];
            suites.extend(s.elif_else_clauses.iter().map(|c| c.body.as_slice()));
            suites
        }
        Stmt::While(s) => vec![&s.body, &s.orelse],
        Stmt::For(s) => vec![&s.body, &s.orelse],
        Stmt::With(s) => vec![&s.body],
        Stmt::Try(s) => {
            let mut suites: Vec<&[Stmt]> = vec![&s.body];
            suites.extend(s.handlers.iter().map(|h| {
                let ast::ExceptHandler::ExceptHandler(h) = h;
                h.body.as_slice()
            }));
            suites.push(&s.orelse);
            suites.push(&s.finalbody);
            suites
        }
        Stmt::Match(s) => s.cases.iter().map(|c| c.body.as_slice()).collect(),
        _ => Vec::new(),
    }
}
