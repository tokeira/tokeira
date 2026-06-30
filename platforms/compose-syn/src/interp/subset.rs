//! The interpreted subset — a reject-by-default allow-list over the parsed
//! `.tkd`. `syn` accepts all of Rust; this pass walks the AST and rejects every
//! node outside the allow-list (Proposal 003 §4 / 004 §6), collecting all
//! violations as spanned diagnostics. It runs *before* evaluation, so the
//! evaluator only ever sees vetted nodes — the no-panic security model.
//!
//! Engine-agnostic except for the registry (method/kind/assoc names), which it
//! consults to validate calls at check time rather than letting them fail at run
//! time.

use proc_macro2::Span;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::{Expr, File, Item, Pat, Stmt};

use super::registry::Registry;
use super::schema::TypeTable;

/// One subset violation.
pub struct Diagnostic {
    pub msg: String,
    pub span: Span,
}

/// All violations found in a `.tkd` (non-empty ⇒ rejected).
pub struct Diagnostics(pub Vec<Diagnostic>);

impl Diagnostics {
    pub fn into_messages(self) -> Vec<String> {
        self.0.into_iter().map(|d| d.msg).collect()
    }

    pub fn messages(&self) -> Vec<&str> {
        self.0.iter().map(|d| d.msg.as_str()).collect()
    }
}

impl std::fmt::Display for Diagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for d in &self.0 {
            writeln!(f, "  - {}", d.msg)?;
        }
        Ok(())
    }
}

/// Validate that `file` is within the interpreted subset.
pub fn check(file: &File, reg: &Registry, types: &TypeTable) -> Result<(), Diagnostics> {
    let mut c = Checker { reg, types, diags: Vec::new() };
    for item in &file.items {
        c.item(item);
    }
    if c.diags.is_empty() {
        Ok(())
    } else {
        Err(Diagnostics(c.diags))
    }
}

struct Checker<'a> {
    reg: &'a Registry,
    types: &'a TypeTable,
    diags: Vec<Diagnostic>,
}

impl Checker<'_> {
    fn reject(&mut self, span: Span, msg: impl Into<String>) {
        self.diags.push(Diagnostic { msg: msg.into(), span });
    }

    fn item(&mut self, item: &Item) {
        match item {
            // config schema: opaque, except #[require(expr)] which admission later
            // evaluates — so its expression must be vetted by the subset too.
            Item::Struct(s) => self.attr_requires(&s.attrs),
            Item::Enum(e) => self.attr_requires(&e.attrs),
            Item::Fn(f) => self.block(&f.block),
            other => self.reject(
                other.span(),
                format!("item not allowed in a `.tkd`: `{}`", item_kind(other)),
            ),
        }
    }

    /// Vet the expression inside each `#[require(...)]` so admission only ever
    /// evaluates subset-valid nodes (no free calls, ranges, indexing, etc.).
    fn attr_requires(&mut self, attrs: &[syn::Attribute]) {
        for a in attrs {
            if a.path().is_ident("require") {
                match a.parse_args::<Expr>() {
                    Ok(e) => self.expr(&e),
                    Err(_) => self.reject(a.span(), "#[require(...)] must contain a single expression"),
                }
            }
        }
    }

    fn block(&mut self, b: &syn::Block) {
        for s in &b.stmts {
            self.stmt(s);
        }
    }

    fn stmt(&mut self, s: &Stmt) {
        match s {
            Stmt::Local(local) => {
                match &local.pat {
                    Pat::Ident(_) | Pat::Wild(_) => {}
                    other => self.reject(other.span(), "only simple `let` bindings are allowed"),
                }
                match &local.init {
                    Some(init) => {
                        if init.diverge.is_some() {
                            self.reject(local.span(), "`let ... else` is not allowed");
                        }
                        self.reject_unplaced_kind(&init.expr);
                        self.expr(&init.expr);
                    }
                    None => self.reject(local.span(), "`let` without an initializer is not allowed"),
                }
            }
            Stmt::Expr(e, _) => self.expr(e),
            Stmt::Macro(m) => self.macro_call(&m.mac),
            Stmt::Item(it) => self.reject(it.span(), "nested items are not allowed in a function body"),
        }
    }

    /// A kind literal/path may only appear inline as a `resource`/`service`
    /// argument — binding it to a `let` would create a take-once handle that the
    /// builder can move out from under a later reference.
    fn reject_unplaced_kind(&mut self, e: &Expr) {
        let is_kind = match e {
            Expr::Struct(es) => {
                let segs = path_segs(&es.path);
                segs.len() == 1 && self.reg.is_kind(&segs[0])
            }
            Expr::Path(ep) => {
                let segs = path_segs(&ep.path);
                segs.len() == 1 && self.reg.is_kind(&segs[0])
            }
            _ => false,
        };
        if is_kind {
            self.reject(
                e.span(),
                "a kind must be used inline as a `resource`/`service` argument, not bound to a `let`",
            );
        }
    }

    fn expr(&mut self, e: &Expr) {
        match e {
            Expr::Lit(_) | Expr::Path(_) => {}
            Expr::Reference(r) => self.expr(&r.expr),
            Expr::Paren(p) => self.expr(&p.expr),
            Expr::Group(g) => self.expr(&g.expr),
            Expr::Array(a) => self.exprs(&a.elems),
            Expr::Tuple(t) => self.exprs(&t.elems),
            Expr::Struct(es) => {
                for fv in &es.fields {
                    self.expr(&fv.expr);
                }
                if let Some(rest) = &es.rest {
                    let ok = matches!(&**rest, Expr::Path(p)
                        if { let s = path_segs(&p.path); s.len() == 2 && s[1] == "EMPTY" });
                    if !ok {
                        self.reject(rest.span(), "struct spread must be `..<Type>::EMPTY`");
                    }
                }
            }
            Expr::Call(ec) => self.call(ec),
            Expr::MethodCall(em) => {
                let name = em.method.to_string();
                if !self.method_allowed(&name) {
                    self.reject(
                        em.span(),
                        format!(
                            "method `{name}` is not allowed (no I/O, filesystem, env, or arbitrary calls in a `.tkd`)"
                        ),
                    );
                }
                self.expr(&em.receiver);
                self.exprs(&em.args);
            }
            Expr::Field(ef) => self.expr(&ef.base),
            Expr::If(ei) => {
                if let Expr::Let(elet) = &*ei.cond {
                    self.pat(&elet.pat);
                    self.expr(&elet.expr);
                } else {
                    self.expr(&ei.cond);
                }
                self.block(&ei.then_branch);
                if let Some((_, elseb)) = &ei.else_branch {
                    self.expr(elseb);
                }
            }
            Expr::Match(em) => {
                self.expr(&em.expr);
                for arm in &em.arms {
                    self.pat(&arm.pat);
                    if let Some((_, g)) = &arm.guard {
                        self.expr(g);
                    }
                    self.expr(&arm.body);
                }
            }
            Expr::Macro(em) => self.macro_call(&em.mac),
            Expr::Binary(eb) => {
                use syn::BinOp::{And, Eq, Ge, Gt, Le, Lt, Ne, Or};
                if !matches!(eb.op, Eq(_) | Ne(_) | Lt(_) | Le(_) | Gt(_) | Ge(_) | And(_) | Or(_)) {
                    self.reject(eb.span(), "binary operator not allowed");
                }
                self.expr(&eb.left);
                self.expr(&eb.right);
            }
            Expr::Unary(eu) => {
                if !matches!(eu.op, syn::UnOp::Not(_)) {
                    self.reject(eu.span(), "unary operator not allowed (only `!`)");
                }
                self.expr(&eu.expr);
            }
            Expr::Block(eb) => self.block(&eb.block),
            other => self.reject(other.span(), format!("expression not allowed: `{}`", expr_kind(other))),
        }
    }

    fn exprs(&mut self, args: &Punctuated<Expr, Comma>) {
        for a in args {
            self.expr(a);
        }
    }

    fn call(&mut self, ec: &syn::ExprCall) {
        if let Expr::Path(ep) = &*ec.func {
            let segs = path_segs(&ep.path);
            let joined = segs.join("::");
            let allowed = joined == "Some"
                || self.reg.assoc(&joined).is_some()
                || (segs.len() == 2 && self.types.is_enum(&segs[0]));
            if !allowed {
                self.reject(ec.span(), format!("call `{joined}` is not allowed"));
            }
        } else {
            self.reject(ec.span(), "only path calls are allowed");
        }
        self.exprs(&ec.args);
    }

    fn macro_call(&mut self, mac: &syn::Macro) {
        let name = mac
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        match name.as_str() {
            "vec" | "format" => {
                if let Ok(exprs) = mac.parse_body_with(Punctuated::<Expr, Comma>::parse_terminated) {
                    self.exprs(&exprs);
                }
            }
            "matches" => {
                if let Ok((e, p)) = mac.parse_body_with(|input: syn::parse::ParseStream| {
                    let e: Expr = input.parse()?;
                    let _comma: Comma = input.parse()?;
                    let p = Pat::parse_multi_with_leading_vert(input)?;
                    Ok((e, p))
                }) {
                    self.expr(&e);
                    self.pat(&p);
                }
            }
            other => self.reject(mac.span(), format!("macro `{other}!` is not allowed")),
        }
    }

    fn method_allowed(&self, name: &str) -> bool {
        self.reg.knows_method(name)
            || matches!(
                name,
                "clone" | "into" | "to_string" | "as_deref" | "as_str" | "is_some" | "is_none"
            )
    }

    fn pat(&mut self, p: &Pat) {
        match p {
            Pat::Ident(pi) => {
                if let Some((_, sub)) = &pi.subpat {
                    self.pat(sub);
                }
            }
            Pat::Wild(_) | Pat::Path(_) | Pat::Rest(_) | Pat::Lit(_) => {}
            Pat::Reference(pr) => self.pat(&pr.pat),
            Pat::Paren(pp) => self.pat(&pp.pat),
            Pat::Struct(ps) => {
                for f in &ps.fields {
                    self.pat(&f.pat);
                }
            }
            Pat::TupleStruct(pts) => {
                for el in &pts.elems {
                    self.pat(el);
                }
            }
            Pat::Tuple(pt) => {
                for el in &pt.elems {
                    self.pat(el);
                }
            }
            other => self.reject(other.span(), "pattern not allowed"),
        }
    }
}

fn path_segs(p: &syn::Path) -> Vec<String> {
    p.segments.iter().map(|s| s.ident.to_string()).collect()
}

fn item_kind(item: &Item) -> &'static str {
    match item {
        Item::Use(_) => "use",
        Item::Impl(_) => "impl",
        Item::Mod(_) => "mod",
        Item::Trait(_) => "trait",
        Item::Const(_) => "const",
        Item::Static(_) => "static",
        Item::Macro(_) => "macro",
        _ => "item",
    }
}

fn expr_kind(e: &Expr) -> &'static str {
    match e {
        Expr::Closure(_) => "closure",
        Expr::ForLoop(_) => "for-loop",
        Expr::While(_) => "while-loop",
        Expr::Loop(_) => "loop",
        Expr::Unsafe(_) => "unsafe-block",
        Expr::Async(_) => "async-block",
        Expr::Await(_) => "await",
        Expr::Try(_) => "?-operator",
        Expr::Return(_) => "return",
        Expr::Let(_) => "let-expression",
        Expr::Range(_) => "range",
        Expr::Index(_) => "index",
        _ => "expression",
    }
}
