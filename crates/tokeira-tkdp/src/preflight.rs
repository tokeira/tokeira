//! Static validation of a `.tkdp` definition before lowering.
//!
//! The admitted `match` subset is the contract, not a temporary gap: wildcard,
//! bare capture, literal and singleton patterns, keyword-only class patterns
//! whose sub-patterns are bare captures or `_`, and guards over any of those.
//! Everything else in PEP 634 is rejected here with a spanned finding, as are
//! reserved `__tokeira_internal_` identifiers (the lowering's private
//! namespace), tab indentation (the lowering re-indents with space
//! arithmetic), entrypoint violations, and `tokeira` imports outside the
//! facade contract. All findings of one pass are collected together
//! (TKDP001–TKDP012), so an operator sees the full list, not one error per
//! attempt.

use ruff_python_ast::{
    self as ast, Expr, Identifier, MatchCase, ModModule, Number, Pattern, Stmt,
    visitor::{self, Visitor},
};
use ruff_text_size::{Ranged, TextRange, TextSize};

/// Prefix reserved for lowering-generated names. Rejecting it in every
/// user-authored identifier position is what makes generated names
/// collision-proof without a renaming pass.
pub const RESERVED_PREFIX: &str = "__tokeira_internal_";

/// The facade module name the import contract admits.
pub const FACADE_MODULE: &str = "tokeira";

/// One spanned preflight finding against the original source.
#[derive(Debug, Clone)]
pub struct Finding {
    /// Stable `TKDP0xx` code.
    pub code: &'static str,
    /// Actionable detail.
    pub message: String,
    /// Range in the operator's source.
    pub range: TextRange,
}

impl Finding {
    fn new(code: &'static str, message: impl Into<String>, range: TextRange) -> Self {
        Self {
            code,
            message: message.into(),
            range,
        }
    }
}

/// A facade name imported by the definition, with its bound alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacadeImport {
    /// Name published by the facade.
    pub name: String,
    /// Name bound in the definition (`as` alias or the name itself).
    pub bound_as: String,
}

/// One builder-verb call site with a statically known name argument.
///
/// Graph invariants make module names, `(module, resource)` ids, and
/// writeback keys unique, so a literal first argument correlates an envelope
/// entry back to its declaring call — the location used for kind-decode and
/// structural findings. Calls with non-literal names simply provide no
/// correlation and fall back to the entrypoint's range.
#[derive(Debug, Clone)]
pub struct CallSite {
    /// `module`, `resource`, or `writeback`.
    pub verb: &'static str,
    /// The literal string first argument.
    pub name: String,
    /// Range of the whole call expression.
    pub range: TextRange,
}

/// A validated definition plus everything later stages need.
#[derive(Debug)]
pub struct Preflight {
    /// Parsed module, reused by the lowering.
    pub module: ModModule,
    /// Facade names the definition imports, in source order.
    pub imports: Vec<FacadeImport>,
    /// Non-facade module names this file imports, in source order — the
    /// candidates the frontend offers to the part resolver. A name the
    /// resolver serves is a definition part; a name it does not is left for
    /// Monty (a built-in, or a runtime `ModuleNotFoundError`).
    pub part_imports: Vec<PartImport>,
    /// Builder-verb call sites for range correlation.
    pub call_sites: Vec<CallSite>,
    /// Range of the `config` entrypoint's name.
    pub config_range: TextRange,
    /// Range of the `deployment` entrypoint's name.
    pub deployment_range: TextRange,
}

/// A validated definition part: the same admission surface as the root minus
/// the entrypoint requirement — a part is a plain module of the dialect.
#[derive(Debug)]
pub struct PartPreflight {
    /// Parsed module, reused by the lowering.
    pub module: ModModule,
    /// Non-facade module names this part imports (part-to-part edges and
    /// built-ins alike), in source order.
    pub part_imports: Vec<PartImport>,
}

/// One non-facade import: a candidate definition part.
#[derive(Debug)]
pub struct PartImport {
    /// The imported module name.
    pub name: String,
    /// The import statement's range in this file.
    pub range: TextRange,
}

/// Parses and validates a definition against the facade surface
/// (importable names: builders plus the engine kind inventory).
pub fn preflight(source: &str, facade_names: &[&str]) -> Result<Preflight, Vec<Finding>> {
    let parsed = match ruff_python_parser::parse_module(source) {
        Ok(parsed) => parsed,
        Err(err) => {
            return Err(vec![Finding::new(
                "TKDP001",
                format!("syntax error: {}", err.error),
                err.location,
            )]);
        }
    };
    let module = parsed.into_syntax();

    let mut checker = Checker {
        facade_names,
        ..Checker::default()
    };
    check_indentation(source, &mut checker.findings);
    for stmt in &module.body {
        checker.visit_stmt(stmt);
    }
    check_import_shadowing(&module, &mut checker);
    let entrypoints = check_entrypoints(&module, &mut checker.findings);

    match (entrypoints, checker.findings.is_empty()) {
        (Some((config_range, deployment_range)), true) => Ok(Preflight {
            module,
            imports: checker.imports,
            part_imports: checker.part_imports,
            call_sites: checker.call_sites,
            config_range,
            deployment_range,
        }),
        _ => Err(checker.findings),
    }
}

/// Parses and validates a definition part: the root's admission surface
/// without the entrypoint requirement.
pub fn preflight_part(source: &str, facade_names: &[&str]) -> Result<PartPreflight, Vec<Finding>> {
    let parsed = match ruff_python_parser::parse_module(source) {
        Ok(parsed) => parsed,
        Err(err) => {
            return Err(vec![Finding::new(
                "TKDP001",
                format!("syntax error: {}", err.error),
                err.location,
            )]);
        }
    };
    let module = parsed.into_syntax();

    let mut checker = Checker {
        facade_names,
        ..Checker::default()
    };
    check_indentation(source, &mut checker.findings);
    for stmt in &module.body {
        checker.visit_stmt(stmt);
    }
    check_import_shadowing(&module, &mut checker);

    if checker.findings.is_empty() {
        Ok(PartPreflight {
            module,
            part_imports: checker.part_imports,
        })
    } else {
        Err(checker.findings)
    }
}

/// Refuses a plain `import X` that this file's own module-level `X` would
/// silently shadow — Python rebinds the name, leaving the module unreachable
/// and the author confused. The from-form binds only the names it takes and
/// is always safe.
fn check_import_shadowing(module: &ModModule, checker: &mut Checker<'_>) {
    let mut bound: Vec<&str> = Vec::new();
    for stmt in &module.body {
        match stmt {
            Stmt::FunctionDef(def) => bound.push(def.name.as_str()),
            Stmt::ClassDef(def) => bound.push(def.name.as_str()),
            Stmt::Assign(assign) => {
                for target in &assign.targets {
                    if let Expr::Name(name) = target {
                        bound.push(name.id.as_str());
                    }
                }
            }
            Stmt::AnnAssign(assign) => {
                if let Expr::Name(name) = &*assign.target {
                    bound.push(name.id.as_str());
                }
            }
            _ => {}
        }
    }
    for import in &checker.plain_imports {
        if bound.contains(&import.name.as_str()) {
            checker.findings.push(Finding::new(
                "TKDP014",
                format!(
                    "`import {name}` is shadowed by this file's own `{name}`; \
                     take names explicitly: `from {name} import …`",
                    name = import.name
                ),
                import.range,
            ));
        }
    }
}

/// Rejects tab indentation: the lowering re-indents copied lines with space
/// arithmetic, and a tab-indented body would shift by the wrong visual width
/// and could change block structure silently.
fn check_indentation(source: &str, findings: &mut Vec<Finding>) {
    let mut offset = 0u32;
    for line in source.split_inclusive('\n') {
        let ws_len = line.len() - line.trim_start_matches([' ', '\t']).len();
        if line[..ws_len].contains('\t') {
            findings.push(Finding::new(
                "TKDP011",
                "tab indentation is not supported in .tkdp definitions; use spaces",
                TextRange::at(TextSize::new(offset), TextSize::new(ws_len as u32)),
            ));
            return;
        }
        offset += line.len() as u32;
    }
}

/// Both entrypoints are required, exactly once, with exact arities; signature
/// errors would otherwise surface as runtime `TypeError`s from the generated
/// driver — a confusing place to learn your entrypoint is wrong.
fn check_entrypoints(
    module: &ModModule,
    findings: &mut Vec<Finding>,
) -> Option<(TextRange, TextRange)> {
    let mut config = Vec::new();
    let mut deployment = Vec::new();
    for stmt in &module.body {
        let Stmt::FunctionDef(def) = stmt else {
            continue;
        };
        let params = &def.parameters;
        let positional = params.posonlyargs.len() + params.args.len();
        let plain =
            params.vararg.is_none() && params.kwarg.is_none() && params.kwonlyargs.is_empty();
        match def.name.as_str() {
            "config" => {
                if positional != 0 || !plain {
                    findings.push(Finding::new(
                        "TKDP008",
                        "entrypoint `config` must take no parameters",
                        def.name.range(),
                    ));
                }
                config.push(def.name.range());
            }
            "deployment" => {
                if positional != 2 || !plain {
                    findings.push(Finding::new(
                        "TKDP008",
                        "entrypoint `deployment` must take exactly two parameters (cfg, cx)",
                        def.name.range(),
                    ));
                }
                deployment.push(def.name.range());
            }
            _ => {}
        }
    }
    let module_start = TextRange::empty(TextSize::new(0));
    let mut require_one = |name: &str, sites: &[TextRange]| match sites {
        [] => {
            findings.push(Finding::new(
                "TKDP008",
                format!("entrypoint `{name}` must be defined"),
                module_start,
            ));
            None
        }
        [single] => Some(*single),
        [.., last] => {
            findings.push(Finding::new(
                "TKDP008",
                format!("entrypoint `{name}` must be defined exactly once"),
                *last,
            ));
            None
        }
    };
    let config = require_one("config", &config);
    let deployment = require_one("deployment", &deployment);
    Some((config?, deployment?))
}

/// One-pass walker: match-subset validation, hygiene, the facade import
/// contract, and call-site collection.
#[derive(Default)]
struct Checker<'a> {
    facade_names: &'a [&'a str],
    findings: Vec<Finding>,
    imports: Vec<FacadeImport>,
    part_imports: Vec<PartImport>,
    /// The subset of `part_imports` written as plain `import X` — the form
    /// the shadow check guards, because a later module-level `X` rebinds it.
    plain_imports: Vec<PartImport>,
    call_sites: Vec<CallSite>,
}

impl Checker<'_> {
    fn reserved(&mut self, ident: &Identifier) {
        if ident.as_str().starts_with(RESERVED_PREFIX) {
            self.findings.push(Finding::new(
                "TKDP007",
                format!(
                    "identifier `{}` uses the reserved `{RESERVED_PREFIX}` prefix",
                    ident.as_str()
                ),
                ident.range(),
            ));
        }
    }

    fn check_import(&mut self, import: &ast::StmtImportFrom) {
        let Some(module) = &import.module else {
            return;
        };
        if module.as_str() != FACADE_MODULE {
            if import.level != 0 {
                self.findings.push(Finding::new(
                    "TKDP013",
                    "relative imports are not supported in definitions",
                    import.range(),
                ));
            } else if module.as_str().contains('.') {
                self.findings.push(Finding::new(
                    "TKDP013",
                    format!(
                        "module imports are single-level in definitions: `{}` has no meaning here",
                        module.as_str()
                    ),
                    import.range(),
                ));
            } else {
                // A candidate definition part (or a Monty built-in, which the
                // resolver miss leaves for the runtime to answer).
                self.part_imports.push(PartImport {
                    name: module.to_string(),
                    range: import.range(),
                });
            }
            return;
        }
        if import.level != 0 {
            self.findings.push(Finding::new(
                "TKDP012",
                "relative `tokeira` imports are not supported",
                import.range(),
            ));
            return;
        }
        for alias in &import.names {
            if alias.name.as_str() == "*" {
                self.findings.push(Finding::new(
                    "TKDP012",
                    "`from tokeira import *` is not supported; import names explicitly",
                    alias.range(),
                ));
                continue;
            }
            if !self.facade_names.contains(&alias.name.as_str()) {
                self.findings.push(Finding::new(
                    "TKDP012",
                    format!(
                        "`{}` is not published by the tokeira facade for this platform",
                        alias.name.as_str()
                    ),
                    alias.name.range(),
                ));
                continue;
            }
            let bound_as = alias
                .asname
                .as_ref()
                .map_or_else(|| alias.name.to_string(), ToString::to_string);
            self.imports.push(FacadeImport {
                name: alias.name.to_string(),
                bound_as,
            });
        }
    }

    fn check_call(&mut self, call: &ast::ExprCall) {
        let Expr::Attribute(attribute) = &*call.func else {
            return;
        };
        let verb = match attribute.attr.as_str() {
            "module" => "module",
            "resource" => "resource",
            "writeback" => "writeback",
            _ => return,
        };
        let Some(Expr::StringLiteral(name)) = call.arguments.args.first() else {
            return;
        };
        self.call_sites.push(CallSite {
            verb,
            name: name.value.to_string(),
            range: call.range(),
        });
    }

    fn check_match(&mut self, stmt: &ast::StmtMatch) {
        for (index, case) in stmt.cases.iter().enumerate() {
            self.check_pattern(&case.pattern);
            let last = index + 1 == stmt.cases.len();
            if !last && irrefutable(case) {
                self.findings.push(Finding::new(
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
                if as_pattern.pattern.is_some() {
                    self.findings.push(Finding::new(
                        "TKDP002",
                        "`as` patterns are not supported in .tkdp match",
                        as_pattern.range(),
                    ));
                }
            }
            Pattern::MatchSingleton(_) => {}
            Pattern::MatchValue(value) => self.check_literal(&value.value),
            Pattern::MatchClass(class) => self.check_class_pattern(class),
            Pattern::MatchSequence(p) => self.findings.push(Finding::new(
                "TKDP002",
                "sequence patterns are not supported in .tkdp match",
                p.range(),
            )),
            Pattern::MatchMapping(p) => self.findings.push(Finding::new(
                "TKDP002",
                "mapping patterns are not supported in .tkdp match",
                p.range(),
            )),
            Pattern::MatchOr(p) => self.findings.push(Finding::new(
                "TKDP002",
                "OR (`|`) patterns are not supported in .tkdp match",
                p.range(),
            )),
            Pattern::MatchStar(p) => self.findings.push(Finding::new(
                "TKDP002",
                "star patterns are not supported in .tkdp match",
                p.range(),
            )),
        }
    }

    /// Literal patterns admit exactly the forms the lowering compares with
    /// `==`: strings, bytes, ints, floats, and their negations.
    fn check_literal(&mut self, value: &Expr) {
        match value {
            Expr::StringLiteral(_) | Expr::BytesLiteral(_) => {}
            Expr::NumberLiteral(number) => {
                if matches!(number.value, Number::Complex { .. }) {
                    self.findings.push(Finding::new(
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
                self.findings.push(Finding::new(
                    "TKDP002",
                    "only negated number literals are supported here",
                    value.range(),
                ));
            }
            Expr::Attribute(_) => self.findings.push(Finding::new(
                "TKDP002",
                "value patterns (dotted names) are not supported in .tkdp match",
                value.range(),
            )),
            other => self.findings.push(Finding::new(
                "TKDP002",
                "unsupported literal pattern in .tkdp match",
                other.range(),
            )),
        }
    }

    fn check_class_pattern(&mut self, class: &ast::PatternMatchClass) {
        if !matches!(&*class.cls, Expr::Name(_)) {
            self.findings.push(Finding::new(
                "TKDP005",
                "class patterns must name the variant with a bare name",
                class.cls.range(),
            ));
        }
        if !class.arguments.patterns.is_empty() {
            self.findings.push(Finding::new(
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
                self.findings.push(Finding::new(
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
                            self.findings.push(Finding::new(
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
                other => self.findings.push(Finding::new(
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

impl Visitor<'_> for Checker<'_> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::Match(m) => self.check_match(m),
            Stmt::FunctionDef(def) => self.reserved(&def.name),
            Stmt::ClassDef(def) => self.reserved(&def.name),
            Stmt::Global(g) => g.names.iter().for_each(|n| self.reserved(n)),
            Stmt::Nonlocal(g) => g.names.iter().for_each(|n| self.reserved(n)),
            Stmt::ImportFrom(import) => self.check_import(import),
            Stmt::Import(import) => {
                for alias in &import.names {
                    if alias.name.as_str() == FACADE_MODULE
                        || alias.name.as_str().starts_with("tokeira.")
                    {
                        self.findings.push(Finding::new(
                            "TKDP012",
                            "use `from tokeira import <name>` for facade names",
                            alias.range(),
                        ));
                    } else if alias.name.as_str().contains('.') {
                        self.findings.push(Finding::new(
                            "TKDP013",
                            format!(
                                "module imports are single-level in definitions: `{}` has no meaning here",
                                alias.name.as_str()
                            ),
                            alias.range(),
                        ));
                    } else {
                        let entry = PartImport {
                            name: alias.name.to_string(),
                            range: alias.range(),
                        };
                        self.plain_imports.push(PartImport {
                            name: entry.name.clone(),
                            range: entry.range,
                        });
                        self.part_imports.push(entry);
                    }
                }
            }
            _ => {}
        }
        visitor::walk_stmt(self, stmt);
    }

    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Name(name) => {
                if name.id.as_str().starts_with(RESERVED_PREFIX) {
                    self.findings.push(Finding::new(
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
            Expr::Call(call) => self.check_call(call),
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
