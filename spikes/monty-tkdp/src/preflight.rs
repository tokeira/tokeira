//! Static validation of a `.tkdp` definition before lowering.
//!
//! The spike admits a deliberately restricted `match` subset — the boundary is
//! the contract, not a temporary gap:
//!
//! - wildcard `case _:`
//! - capture `case name:`
//! - literal `case "dsql":` / `case 3:` / `case -1.5:` and singleton
//!   `case None:` / `case True:` / `case False:`
//! - keyword-only class patterns `case ManagedDsql(region=region):` whose
//!   sub-patterns are bare captures or `_`
//! - any of the above with a guard `case P if expr:`
//!
//! Everything else in PEP 634 — positional class args, sequence, mapping,
//! OR (`|`), `as`, star, value (dotted) patterns — is rejected here with a
//! spanned diagnostic instead of failing later inside Monty. Preflight also
//! owns hygiene (the `__tokeira_internal_` namespace is reserved for the
//! lowering) and CPython's irrefutable-case-must-be-last rule, so that every
//! definition the lowering sees is one it can translate without judgement
//! calls.

use ruff_python_ast::{
    self as ast, Expr, Identifier, MatchCase, ModModule, Number, Pattern, Stmt,
    visitor::{self, Visitor},
};
use ruff_text_size::Ranged;

use crate::diagnostics::Diagnostic;

/// Prefix reserved for lowering-generated names. Preflight rejects it in all
/// user-authored identifier positions, which is what makes generated names
/// collision-proof without any renaming pass.
pub const RESERVED_PREFIX: &str = "__tokeira_internal_";

/// Entrypoints the driver may invoke, detected from top-level `def`s.
#[derive(Debug, Clone, Copy, Default)]
pub struct Entrypoints {
    pub has_config: bool,
    pub has_deployment: bool,
}

/// A validated definition: the parsed module plus everything the later
/// stages need without re-deriving.
#[derive(Debug)]
pub struct Preflight {
    pub module: ModModule,
    pub entrypoints: Entrypoints,
}

/// Parses and validates a definition. All findings are collected in one pass
/// — an operator fixing a definition sees the full list, not one error per
/// attempt.
pub fn preflight(source: &str) -> Result<Preflight, Vec<Diagnostic>> {
    let parsed = match ruff_python_parser::parse_module(source) {
        Ok(parsed) => parsed,
        Err(err) => {
            return Err(vec![Diagnostic::new(
                "TKDP001",
                format!("syntax error: {}", err.error),
                err.location,
            )]);
        }
    };
    let module = parsed.into_syntax();

    let mut checker = Checker::default();
    check_indentation(source, &mut checker.diagnostics);
    for stmt in &module.body {
        checker.visit_stmt(stmt);
    }
    let entrypoints = check_entrypoints(&module, &mut checker.diagnostics);

    if checker.diagnostics.is_empty() {
        Ok(Preflight {
            module,
            entrypoints,
        })
    } else {
        Err(checker.diagnostics)
    }
}

/// Rejects tab indentation. The lowering re-indents copied lines with space
/// arithmetic; a tab-indented body would shift by the wrong visual width and
/// could change block structure silently — rejecting up front is the only
/// safe answer the spike gives.
fn check_indentation(source: &str, diagnostics: &mut Vec<Diagnostic>) {
    let mut offset = 0u32;
    for line in source.split_inclusive('\n') {
        let ws_len = line.len() - line.trim_start_matches([' ', '\t']).len();
        if line[..ws_len].contains('\t') {
            diagnostics.push(Diagnostic::new(
                "TKDP011",
                "tab indentation is not supported in .tkdp definitions; use spaces",
                ruff_text_size::TextRange::at(
                    ruff_text_size::TextSize::new(offset),
                    ruff_text_size::TextSize::new(ws_len as u32),
                ),
            ));
            return;
        }
        offset += line.len() as u32;
    }
}

/// Detects `config()` / `deployment(cfg, cx)` and validates their arity.
/// Signature errors would otherwise surface as runtime `TypeError`s from the
/// generated driver — a confusing place to learn your entrypoint is wrong.
fn check_entrypoints(module: &ModModule, diagnostics: &mut Vec<Diagnostic>) -> Entrypoints {
    let mut entrypoints = Entrypoints::default();
    for stmt in &module.body {
        let Stmt::FunctionDef(def) = stmt else {
            continue;
        };
        let params = &def.parameters;
        let positional = params.posonlyargs.len() + params.args.len();
        let plain_signature =
            params.vararg.is_none() && params.kwarg.is_none() && params.kwonlyargs.is_empty();
        match def.name.as_str() {
            "config" => {
                entrypoints.has_config = true;
                if positional != 0 || !plain_signature {
                    diagnostics.push(Diagnostic::new(
                        "TKDP008",
                        "entrypoint `config` must take no parameters",
                        def.name.range(),
                    ));
                }
            }
            "deployment" => {
                entrypoints.has_deployment = true;
                if positional != 2 || !plain_signature {
                    diagnostics.push(Diagnostic::new(
                        "TKDP008",
                        "entrypoint `deployment` must take exactly two parameters (cfg, cx)",
                        def.name.range(),
                    ));
                }
            }
            _ => {}
        }
    }
    entrypoints
}

/// One-pass walker: match-subset validation on every `match` statement it
/// meets, hygiene on every identifier position.
#[derive(Default)]
struct Checker {
    diagnostics: Vec<Diagnostic>,
}

impl Checker {
    fn reserved(&mut self, ident: &Identifier) {
        if ident.as_str().starts_with(RESERVED_PREFIX) {
            self.diagnostics.push(Diagnostic::new(
                "TKDP007",
                format!(
                    "identifier `{}` uses the reserved `{RESERVED_PREFIX}` prefix",
                    ident.as_str()
                ),
                ident.range(),
            ));
        }
    }

    fn check_match(&mut self, stmt: &ast::StmtMatch) {
        for (index, case) in stmt.cases.iter().enumerate() {
            self.check_pattern(&case.pattern);
            let last = index + 1 == stmt.cases.len();
            if !last && irrefutable(case) {
                self.diagnostics.push(Diagnostic::new(
                    "TKDP006",
                    "irrefutable case makes remaining cases unreachable; move it last",
                    case.pattern.range(),
                ));
            }
        }
    }

    /// Validates one case pattern against the restricted subset. Deliberately
    /// shallow: sub-patterns only exist for class keywords, and those must be
    /// bare captures — there is no general recursion to get wrong.
    fn check_pattern(&mut self, pattern: &Pattern) {
        match pattern {
            Pattern::MatchAs(as_pattern) => {
                // `pattern: None` is a bare capture or `_`; `X as y` carries
                // an inner pattern and is out of the subset.
                if as_pattern.pattern.is_some() {
                    self.diagnostics.push(Diagnostic::new(
                        "TKDP002",
                        "`as` patterns are not supported in .tkdp match",
                        as_pattern.range(),
                    ));
                }
            }
            Pattern::MatchSingleton(_) => {}
            Pattern::MatchValue(value) => self.check_literal(&value.value),
            Pattern::MatchClass(class) => self.check_class_pattern(class),
            Pattern::MatchSequence(p) => self.diagnostics.push(Diagnostic::new(
                "TKDP002",
                "sequence patterns are not supported in .tkdp match",
                p.range(),
            )),
            Pattern::MatchMapping(p) => self.diagnostics.push(Diagnostic::new(
                "TKDP002",
                "mapping patterns are not supported in .tkdp match",
                p.range(),
            )),
            Pattern::MatchOr(p) => self.diagnostics.push(Diagnostic::new(
                "TKDP002",
                "OR (`|`) patterns are not supported in .tkdp match",
                p.range(),
            )),
            Pattern::MatchStar(p) => self.diagnostics.push(Diagnostic::new(
                "TKDP002",
                "star patterns are not supported in .tkdp match",
                p.range(),
            )),
        }
    }

    /// Literal patterns admit exactly the forms the lowering compares with
    /// `==`: strings, bytes, ints, floats, and their negations. Dotted value
    /// patterns (`case Color.RED:`) and complex literals are out.
    fn check_literal(&mut self, value: &Expr) {
        match value {
            Expr::StringLiteral(_) | Expr::BytesLiteral(_) => {}
            Expr::NumberLiteral(number) => {
                if matches!(number.value, Number::Complex { .. }) {
                    self.diagnostics.push(Diagnostic::new(
                        "TKDP002",
                        "complex-number literal patterns are not supported in .tkdp match",
                        value.range(),
                    ));
                }
            }
            Expr::UnaryOp(unary) => {
                if matches!(unary.op, ast::UnaryOp::USub)
                    && matches!(
                        &*unary.operand,
                        Expr::NumberLiteral(n) if !matches!(n.value, Number::Complex { .. })
                    )
                {
                    return;
                }
                self.diagnostics.push(Diagnostic::new(
                    "TKDP002",
                    "only negated number literals are supported here",
                    value.range(),
                ));
            }
            Expr::Attribute(_) => self.diagnostics.push(Diagnostic::new(
                "TKDP002",
                "value patterns (dotted names) are not supported in .tkdp match",
                value.range(),
            )),
            other => self.diagnostics.push(Diagnostic::new(
                "TKDP002",
                "unsupported literal pattern in .tkdp match",
                other.range(),
            )),
        }
    }

    fn check_class_pattern(&mut self, class: &ast::PatternMatchClass) {
        if !matches!(&*class.cls, Expr::Name(_)) {
            self.diagnostics.push(Diagnostic::new(
                "TKDP005",
                "class patterns must name the variant with a bare name",
                class.cls.range(),
            ));
        }
        if !class.arguments.patterns.is_empty() {
            self.diagnostics.push(Diagnostic::new(
                "TKDP003",
                "positional class-pattern arguments are not supported; use keyword fields",
                class.arguments.range(),
            ));
        }
        let mut seen_fields: Vec<&str> = Vec::new();
        let mut seen_captures: Vec<&str> = Vec::new();
        for keyword in &class.arguments.keywords {
            self.reserved(&keyword.attr);
            let field = keyword.attr.as_str();
            if seen_fields.contains(&field) {
                self.diagnostics.push(Diagnostic::new(
                    "TKDP009",
                    format!("duplicate field `{field}` in class pattern"),
                    keyword.attr.range(),
                ));
            }
            seen_fields.push(field);
            match &keyword.pattern {
                Pattern::MatchAs(sub) if sub.pattern.is_none() => {
                    if let Some(name) = &sub.name {
                        if seen_captures.contains(&name.as_str()) {
                            self.diagnostics.push(Diagnostic::new(
                                "TKDP010",
                                format!(
                                    "capture `{}` bound more than once in this case",
                                    name.as_str()
                                ),
                                name.range(),
                            ));
                        }
                        seen_captures.push(name.as_str());
                    }
                }
                other => self.diagnostics.push(Diagnostic::new(
                    "TKDP004",
                    "class-pattern fields must capture into a bare name (or `_`)",
                    other.range(),
                )),
            }
        }
    }
}

/// A case that always matches: wildcard or bare capture, with no guard.
fn irrefutable(case: &MatchCase) -> bool {
    case.guard.is_none() && matches!(&case.pattern, Pattern::MatchAs(p) if p.pattern.is_none())
}

impl Visitor<'_> for Checker {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::Match(m) => self.check_match(m),
            Stmt::FunctionDef(def) => self.reserved(&def.name),
            Stmt::ClassDef(def) => self.reserved(&def.name),
            Stmt::Global(g) => g.names.iter().for_each(|n| self.reserved(n)),
            Stmt::Nonlocal(g) => g.names.iter().for_each(|n| self.reserved(n)),
            _ => {}
        }
        visitor::walk_stmt(self, stmt);
    }

    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Name(name) => {
                if name.id.as_str().starts_with(RESERVED_PREFIX) {
                    self.diagnostics.push(Diagnostic::new(
                        "TKDP007",
                        format!(
                            "identifier `{}` uses the reserved `{RESERVED_PREFIX}` prefix",
                            name.id
                        ),
                        name.range(),
                    ));
                }
            }
            Expr::Attribute(attr) => self.reserved(&attr.attr),
            _ => {}
        }
        visitor::walk_expr(self, expr);
    }

    fn visit_parameter(&mut self, parameter: &ast::Parameter) {
        self.reserved(&parameter.name);
        visitor::walk_parameter(self, parameter);
    }

    fn visit_keyword(&mut self, keyword: &ast::Keyword) {
        if let Some(arg) = &keyword.arg {
            self.reserved(arg);
        }
        visitor::walk_keyword(self, keyword);
    }

    fn visit_alias(&mut self, alias: &ast::Alias) {
        self.reserved(&alias.name);
        if let Some(asname) = &alias.asname {
            self.reserved(asname);
        }
        visitor::walk_alias(self, alias);
    }

    fn visit_pattern(&mut self, pattern: &Pattern) {
        match pattern {
            Pattern::MatchAs(p) => {
                if let Some(name) = &p.name {
                    self.reserved(name);
                }
            }
            Pattern::MatchStar(p) => {
                if let Some(name) = &p.name {
                    self.reserved(name);
                }
            }
            Pattern::MatchMapping(p) => {
                if let Some(rest) = &p.rest {
                    self.reserved(rest);
                }
            }
            _ => {}
        }
        visitor::walk_pattern(self, pattern);
    }
}
