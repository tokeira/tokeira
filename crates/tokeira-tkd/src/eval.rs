//! The tree-walking evaluator. Walks the `.tkd` AST against the value model and
//! the platform [`HostBridge`], producing the platform's deployment (via opaque
//! host handles) without ever naming a concrete kind or builder type.
//!
//! No operator-reachable path panics: every failure is an [`EvalError`]. Host
//! construction/dispatch/field-reads all route through the bridge.

use std::collections::HashMap;

use syn::{Expr, Pat, punctuated::Punctuated, spanned::Spanned, token::Comma};

use crate::{
    bridge::HostBridge,
    parts::Scopes,
    schema::fn_param_names,
    value::{EnumPath, EvalError, FieldMap, Value, VariantBody},
};

/// Lexical scopes. `child()` snapshots the parent (cheap; host handles share via
/// the platform's `Rc`, so builder mutations stay shared while new bindings don't
/// leak outward).
struct Env<H> {
    scopes: Vec<HashMap<String, Value<H>>>,
}

impl<H: Clone> Env<H> {
    fn new() -> Self {
        Self {
            scopes: vec![HashMap::new()],
        }
    }

    fn child(&self) -> Self {
        let mut scopes = self.scopes.clone();
        scopes.push(HashMap::new());
        Self { scopes }
    }

    fn get(&self, name: &str) -> Option<&Value<H>> {
        self.scopes.iter().rev().find_map(|s| s.get(name))
    }

    fn insert(&mut self, name: String, v: Value<H>) {
        self.scopes
            .last_mut()
            .expect("at least one scope")
            .insert(name, v);
    }
}

/// The evaluator — borrows the bridge, the type/fn tables, and the context.
#[derive(Debug)]
pub struct Interp<'a, B: HostBridge> {
    pub bridge: &'a B,
    pub scope: Scope<'a>,
    pub cx: &'a B::Cx,
}

/// One evaluation scope over the loaded program: the root, or one part.
/// Name resolution is one-way — a part sees its own items first and then
/// the root's types (the shared configuration language); the root reaches
/// a part only through `part::function(...)` calls; parts see nothing of
/// each other.
#[derive(Clone, Copy, Debug)]
pub struct Scope<'a> {
    scopes: &'a Scopes,
    /// `None` is the root; a part scope borrows its key from the loaded
    /// program so entering a scope allocates nothing.
    current: Option<&'a str>,
}

/// What `part::function` resolved to, so refusals name the exact gap.
#[derive(Debug)]
pub enum PartFnLookup<'a> {
    /// No part by that name — the caller falls through to other path
    /// meanings.
    NotAPart,
    /// The part exists; the function does not.
    Missing,
    /// The function exists but is not `pub`.
    Private,
    /// Callable.
    Found(&'a syn::ItemFn),
}

impl<'a> Scope<'a> {
    /// The root scope over a loaded program.
    pub fn root(scopes: &'a Scopes) -> Self {
        Self {
            scopes,
            current: None,
        }
    }

    fn tables(&self) -> &'a crate::parts::Tables {
        match self.current {
            None => &self.scopes.root,
            Some(name) => &self.scopes.parts[name],
        }
    }

    fn in_part(&self) -> bool {
        self.current.is_some()
    }

    /// The table a type name resolves in, for this scope: the document's
    /// own types first, then its `use` takes (validated pub at load), then
    /// — for parts — the root's types as the backdrop. `use` admits no
    /// renames, so the name is the same in the resolved table.
    fn type_home(&self, name: &str) -> Option<&'a crate::schema::TypeTable> {
        let own = &self.tables().types;
        if own.is_struct(name) || own.is_enum(name) {
            return Some(own);
        }
        if let Some(source) = self.tables().uses.get(name) {
            return Some(&self.scopes.parts[source].types);
        }
        if self.in_part() {
            let root = &self.scopes.root.types;
            if root.is_struct(name) || root.is_enum(name) {
                return Some(root);
            }
        }
        None
    }

    pub fn is_struct(&self, name: &str) -> bool {
        self.type_home(name).is_some_and(|t| t.is_struct(name))
    }

    pub fn is_enum(&self, name: &str) -> bool {
        self.type_home(name).is_some_and(|t| t.is_enum(name))
    }

    pub fn enum_has_unit_variant(&self, ty: &str, variant: &str) -> bool {
        self.type_home(ty)
            .is_some_and(|t| t.enum_has_unit_variant(ty, variant))
    }

    pub fn enum_has_variant(&self, ty: &str, variant: &str) -> bool {
        self.type_home(ty)
            .is_some_and(|t| t.enum_has_variant(ty, variant))
    }

    pub fn struct_field_names(&self, name: &str) -> Option<Vec<String>> {
        self.type_home(name)?.struct_field_names(name)
    }

    /// The current scope's own function, for bare-name calls.
    pub fn local_fn(&self, name: &str) -> Option<&'a syn::ItemFn> {
        self.tables().fns.get(name)
    }

    /// Resolve `part::function` from the root. Parts get `NotAPart` for
    /// everything: wiring flows through the root.
    pub fn part_fn(&self, part: &str, name: &str) -> PartFnLookup<'a> {
        if self.in_part() {
            return PartFnLookup::NotAPart;
        }
        let Some(tables) = self.scopes.parts.get(part) else {
            return PartFnLookup::NotAPart;
        };
        match tables.fns.get(name) {
            None => PartFnLookup::Missing,
            Some(f) if matches!(f.vis, syn::Visibility::Public(_)) => PartFnLookup::Found(f),
            Some(_) => PartFnLookup::Private,
        }
    }

    /// The named part's scope; the key borrow comes from the program map so
    /// the scope stays `Copy`.
    pub fn enter(&self, part: &str) -> Option<Scope<'a>> {
        let (key, _) = self.scopes.parts.get_key_value(part)?;
        Some(Scope {
            scopes: self.scopes,
            current: Some(key.as_str()),
        })
    }
}

impl<B: HostBridge> Interp<'_, B> {
    /// Evaluate a `.tkd` function with the given argument values.
    pub fn eval_fn(
        &self,
        name: &str,
        args: Vec<Value<B::Host>>,
    ) -> Result<Value<B::Host>, EvalError> {
        let f = self
            .scope
            .local_fn(name)
            .ok_or_else(|| EvalError::new(format!("no function `{name}`")))?;
        self.call_fn_item(f, name, args)
    }

    /// Invoke one resolved function item in THIS interp's scope — the part
    /// boundary is crossed by the caller constructing a part-scoped interp
    /// first, so the body's names resolve where the function was written.
    fn call_fn_item(
        &self,
        f: &syn::ItemFn,
        name: &str,
        args: Vec<Value<B::Host>>,
    ) -> Result<Value<B::Host>, EvalError> {
        let params = fn_param_names(f)?;
        if params.len() != args.len() {
            return Err(EvalError::new(format!(
                "`{name}` expects {} argument(s), got {}",
                params.len(),
                args.len()
            )));
        }
        let mut env = Env::new();
        for (p, a) in params.into_iter().zip(args) {
            env.insert(p, a);
        }
        self.eval_block(&f.block, &mut env)
    }

    /// Evaluate a standalone expression (a `#[require]` clause) with the given
    /// fields bound — the admission pass uses this to check config constraints.
    pub fn eval_with_fields(
        &self,
        e: &Expr,
        fields: &FieldMap<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let mut env = Env::new();
        for (k, v) in fields {
            env.insert(k.clone(), v.clone());
        }
        self.eval_expr(e, &mut env)
    }

    fn eval_block(
        &self,
        block: &syn::Block,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let mut last = Value::Unit;
        for stmt in &block.stmts {
            match stmt {
                syn::Stmt::Local(local) => {
                    self.eval_local(local, env)?;
                    last = Value::Unit;
                }
                syn::Stmt::Expr(expr, semi) => {
                    let v = self.eval_expr(expr, env)?;
                    last = if semi.is_some() { Value::Unit } else { v };
                }
                syn::Stmt::Macro(sm) => {
                    let v = self.eval_macro(&sm.mac, env)?;
                    last = if sm.semi_token.is_some() {
                        Value::Unit
                    } else {
                        v
                    };
                }
                syn::Stmt::Item(_) => {
                    return Err(EvalError::new(
                        "nested items are not supported in a function body",
                    ));
                }
            }
        }
        Ok(last)
    }

    fn eval_local(&self, local: &syn::Local, env: &mut Env<B::Host>) -> Result<(), EvalError> {
        let init = local
            .init
            .as_ref()
            .ok_or_else(|| EvalError::new("`let` without an initializer"))?;
        if init.diverge.is_some() {
            return Err(EvalError::new("`let ... else` is not supported"));
        }
        let val = self.eval_expr(&init.expr, env)?;
        match &local.pat {
            Pat::Ident(pi) => {
                env.insert(pi.ident.to_string(), val);
                Ok(())
            }
            Pat::Wild(_) => Ok(()),
            _ => Err(EvalError::new("only simple `let` bindings are supported")),
        }
    }

    fn eval_expr(&self, e: &Expr, env: &mut Env<B::Host>) -> Result<Value<B::Host>, EvalError> {
        let result = match e {
            Expr::Lit(el) => eval_lit(&el.lit),
            Expr::Path(ep) => self.eval_path(&ep.path, env),
            Expr::Reference(er) => self.eval_expr(&er.expr, env),
            Expr::Paren(ep) => self.eval_expr(&ep.expr, env),
            Expr::Group(eg) => self.eval_expr(&eg.expr, env),
            Expr::Array(ea) => {
                let mut v = Vec::with_capacity(ea.elems.len());
                for el in &ea.elems {
                    v.push(self.eval_expr(el, env)?);
                }
                Ok(Value::Vec(v))
            }
            Expr::Tuple(et) => {
                let mut v = Vec::with_capacity(et.elems.len());
                for el in &et.elems {
                    v.push(self.eval_expr(el, env)?);
                }
                Ok(Value::Tuple(v))
            }
            Expr::Struct(es) => self.eval_struct(es, env),
            Expr::Call(ec) => self.eval_call(ec, env),
            Expr::MethodCall(em) => self.eval_method(em, env),
            Expr::Field(ef) => self.eval_field(ef, env),
            Expr::If(ei) => self.eval_if(ei, env),
            Expr::Match(em) => self.eval_match(em, env),
            Expr::Macro(em) => self.eval_macro(&em.mac, env),
            Expr::Binary(eb) => self.eval_binary(eb, env),
            Expr::Unary(eu) => self.eval_unary(eu, env),
            // a block expression (a block-bodied match arm, or `let x = { .. }`)
            // — eval its statements in a child scope, like the subset descends.
            Expr::Block(eb) => self.eval_block(&eb.block, &mut env.child()),
            other => Err(EvalError::new(format!(
                "unsupported expression `{}`",
                expr_kind(other)
            ))),
        };
        result.map_err(|error| error.at_if_missing(e.span()))
    }

    fn eval_path(&self, path: &syn::Path, env: &Env<B::Host>) -> Result<Value<B::Host>, EvalError> {
        let segs = path_segs(path);
        match segs.len() {
            1 => {
                let name = &segs[0];
                if let Some(v) = env.get(name) {
                    return Ok(v.clone());
                }
                if name == "None" {
                    return Ok(Value::Opt(None));
                }
                if self.bridge.is_kind(name) {
                    return self
                        .bridge
                        .construct_kind(name, FieldMap::new(), self.cx)
                        .map(Value::Host);
                }
                Err(EvalError::new(format!("unbound name `{name}`")))
            }
            2 => {
                let (ty, variant) = (&segs[0], &segs[1]);
                if self.scope.is_enum(ty) {
                    if self.scope.enum_has_unit_variant(ty, variant) {
                        return Ok(Value::Enum {
                            path: EnumPath {
                                ty: ty.clone(),
                                segments: segs.clone(),
                            },
                            variant: variant.clone(),
                            body: VariantBody::Unit,
                        });
                    }
                    return Err(EvalError::new(format!(
                        "`{ty}` has no unit variant `{variant}`"
                    )));
                }
                if !matches!(self.scope.part_fn(ty, variant), PartFnLookup::NotAPart) {
                    return Err(EvalError::new(format!(
                        "part functions are called, not referenced: `{ty}::{variant}(…)`"
                    )));
                }
                Err(EvalError::new(format!(
                    "unresolved path `{}`",
                    segs.join("::")
                )))
            }
            _ => Err(EvalError::new(format!(
                "unresolved path `{}`",
                segs.join("::")
            ))),
        }
    }

    fn eval_struct(
        &self,
        es: &syn::ExprStruct,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let segs = path_segs(&es.path);
        let head = segs[0].clone();

        // enum struct-variant: `Storage::Dsql { .. }`
        if segs.len() == 2 && self.scope.is_enum(&head) {
            let variant = segs[1].clone();
            if !self.scope.enum_has_variant(&head, &variant) {
                return Err(EvalError::new(format!(
                    "`{head}` has no variant `{variant}`"
                )));
            }
            let mut fields = FieldMap::new();
            for fv in &es.fields {
                fields.insert(member_name(&fv.member)?, self.eval_expr(&fv.expr, env)?);
            }
            return Ok(Value::Enum {
                path: EnumPath {
                    ty: head,
                    segments: segs,
                },
                variant,
                body: VariantBody::Struct(fields),
            });
        }

        // config struct: `Compose { .. }` — validated against its declared fields
        // (no missing, no unknown, no `..` rest; config structs have no defaults).
        if segs.len() == 1 && self.scope.is_struct(&head) {
            if es.rest.is_some() {
                return Err(EvalError::new(format!(
                    "`{head}` literal may not use `..` (config structs have no defaults)"
                )));
            }
            let declared = self.scope.struct_field_names(&head).unwrap_or_default();
            let mut fields = FieldMap::new();
            for fv in &es.fields {
                let name = member_name(&fv.member)?;
                if !declared.contains(&name) {
                    return Err(EvalError::new(format!("`{head}` has no field `{name}`")));
                }
                fields.insert(name, self.eval_expr(&fv.expr, env)?);
            }
            for d in &declared {
                if !fields.contains_key(d) {
                    return Err(EvalError::new(format!("`{head}` is missing field `{d}`")));
                }
            }
            return Ok(Value::Struct { ty: head, fields });
        }

        // author kind: `Service { .. ..Service::EMPTY }`, `DsqlCluster { .. }`, …
        if segs.len() == 1 && self.bridge.is_kind(&head) {
            let mut fields = if let Some(rest) = &es.rest {
                check_empty_spread(rest, &head)?;
                self.bridge
                    .kind_defaults(&head)
                    .ok_or_else(|| EvalError::new(format!("`{head}` has no `..EMPTY` defaults")))?
            } else {
                FieldMap::new()
            };
            for fv in &es.fields {
                fields.insert(member_name(&fv.member)?, self.eval_expr(&fv.expr, env)?);
            }
            return self
                .bridge
                .construct_kind(&head, fields, self.cx)
                .map(Value::Host);
        }

        Err(EvalError::new(format!(
            "unknown struct type `{}`",
            segs.join("::")
        )))
    }

    fn eval_call(
        &self,
        ec: &syn::ExprCall,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let Expr::Path(ep) = &*ec.func else {
            return Err(EvalError::new("only path calls are supported"));
        };
        let segs = path_segs(&ep.path);

        // Some(x)
        if segs.len() == 1 && segs[0] == "Some" {
            let arg = ec
                .args
                .first()
                .ok_or_else(|| EvalError::new("`Some` needs an argument"))?;
            let inner = self.eval_expr(arg, env)?;
            return Ok(Value::Opt(Some(Box::new(inner))));
        }

        // an associated builder fn, e.g. `Deployment::new(..)`
        let joined = segs.join("::");
        if self.bridge.knows_assoc(&joined) {
            let args = self.eval_args(&ec.args, env)?;
            return self.bridge.assoc(&joined, args, self.cx).map(Value::Host);
        }

        // enum tuple-variant: `Ty::Variant(..)`
        if segs.len() == 2 && self.scope.is_enum(&segs[0]) {
            if !self.scope.enum_has_variant(&segs[0], &segs[1]) {
                return Err(EvalError::new(format!(
                    "`{}` has no variant `{}`",
                    segs[0], segs[1]
                )));
            }
            let args = self.eval_args(&ec.args, env)?;
            return Ok(Value::Enum {
                path: EnumPath {
                    ty: segs[0].clone(),
                    segments: segs.clone(),
                },
                variant: segs[1].clone(),
                body: VariantBody::Tuple(args),
            });
        }

        // A declared part's `pub` function, called from the root — THE
        // cross-document seam: arguments evaluate here, the body evaluates
        // in the part's own scope, and the returned value flows back as
        // plain data. Wiring stays in the root by construction, because a
        // part's scope answers `NotAPart` for every part name.
        if segs.len() == 2 {
            match self.scope.part_fn(&segs[0], &segs[1]) {
                PartFnLookup::Found(f) => {
                    let args = self.eval_args(&ec.args, env)?;
                    let entered = Interp {
                        bridge: self.bridge,
                        scope: self
                            .scope
                            .enter(&segs[0])
                            .expect("part_fn resolved, so the part exists"),
                        cx: self.cx,
                    };
                    return entered.call_fn_item(f, &joined, args);
                }
                PartFnLookup::Private => {
                    return Err(EvalError::new(format!(
                        "`{joined}` is not `pub`; a part exports what the root may call"
                    )));
                }
                PartFnLookup::Missing => {
                    return Err(EvalError::new(format!(
                        "part `{}` has no function `{}`",
                        segs[0], segs[1]
                    )));
                }
                PartFnLookup::NotAPart => {}
            }
        }

        Err(EvalError::new(format!("unsupported call `{joined}`")))
    }

    fn eval_method(
        &self,
        em: &syn::ExprMethodCall,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let method = em.method.to_string();
        let recv = self.eval_expr(&em.receiver, env)?;

        // coercions on plain values. `clone`/`into` are identity in this model;
        // `to_string` genuinely stringifies (matching Rust, unlike a no-op);
        // `as_str`/`as_deref` only make sense on a string/Option.
        match method.as_str() {
            "clone" | "into" => return Ok(recv),
            "to_string" => return value_to_display(&recv).map(Value::Str),
            "as_str" | "as_deref" => {
                return match recv {
                    Value::Str(_) | Value::Opt(_) => Ok(recv),
                    other => Err(EvalError::new(format!(
                        "`{method}` expects a string value, got {other:?}"
                    ))),
                };
            }
            "is_some" => return Ok(Value::Bool(matches!(recv, Value::Opt(Some(_))))),
            "is_none" => return Ok(Value::Bool(matches!(recv, Value::Opt(None)))),
            _ => {}
        }

        if let Value::Host(host) = &recv {
            let args = self.eval_args(&em.args, env)?;
            return self.bridge.call_method(host, &method, args, self.cx);
        }
        Err(EvalError::new(format!(
            "method `{method}` on a non-host value"
        )))
    }

    fn eval_field(
        &self,
        ef: &syn::ExprField,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let base = self.eval_expr(&ef.base, env)?;
        let name = member_name(&ef.member)?;
        match base {
            Value::Struct { fields, .. } => fields
                .get(&name)
                .cloned()
                .ok_or_else(|| EvalError::new(format!("no field `{name}`"))),
            Value::Host(host) => self.bridge.host_field(&host, &name),
            other => Err(EvalError::new(format!(
                "cannot read field `{name}` of {other:?}"
            ))),
        }
    }

    fn eval_if(
        &self,
        ei: &syn::ExprIf,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        if let Expr::Let(elet) = &*ei.cond {
            let scrut = self.eval_expr(&elet.expr, env)?;
            let mut child = env.child();
            if self.match_pattern(&elet.pat, &scrut, &mut child)? {
                return self.eval_block(&ei.then_branch, &mut child);
            }
        } else {
            let cond = self.eval_expr(&ei.cond, env)?;
            if as_bool(&cond)? {
                return self.eval_block(&ei.then_branch, env);
            }
        }
        match &ei.else_branch {
            Some((_, else_expr)) => self.eval_expr(else_expr, env),
            None => Ok(Value::Unit),
        }
    }

    fn eval_match(
        &self,
        em: &syn::ExprMatch,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let scrut = self.eval_expr(&em.expr, env)?;
        for arm in &em.arms {
            let mut child = env.child();
            if self.match_pattern(&arm.pat, &scrut, &mut child)? {
                if let Some((_, guard)) = &arm.guard {
                    let g = self.eval_expr(guard, &mut child)?;
                    if !as_bool(&g)? {
                        continue;
                    }
                }
                return self.eval_expr(&arm.body, &mut child);
            }
        }
        Err(EvalError::new("no match arm matched"))
    }

    /// Refutable pattern match used by `if let`/`match`. Returns whether the
    /// pattern matched, binding its idents into `env`.
    fn match_pattern(
        &self,
        pat: &Pat,
        val: &Value<B::Host>,
        env: &mut Env<B::Host>,
    ) -> Result<bool, EvalError> {
        match pat {
            Pat::Wild(_) => Ok(true),
            Pat::Ident(pi) => {
                // `name @ subpat` or plain `name`
                if let Some((_, sub)) = &pi.subpat
                    && !self.match_pattern(sub, val, env)?
                {
                    return Ok(false);
                }
                env.insert(pi.ident.to_string(), val.clone());
                Ok(true)
            }
            Pat::Reference(pr) => self.match_pattern(&pr.pat, val, env),
            Pat::Paren(pp) => self.match_pattern(&pp.pat, val, env),
            Pat::Path(pp) => {
                let segs = path_segs(&pp.path);
                if segs.len() == 1 && segs[0] == "None" {
                    return Ok(matches!(val, Value::Opt(None)));
                }
                match val {
                    Value::Enum { path, variant, .. } => {
                        check_pat_enum_ty(&segs, &path.ty)?;
                        Ok(segs.last() == Some(variant))
                    }
                    _ => Ok(false),
                }
            }
            Pat::Struct(ps) => {
                let segs = path_segs(&ps.path);
                let want = segs.last().cloned().unwrap_or_default();
                let Value::Enum {
                    path,
                    variant,
                    body: VariantBody::Struct(fields),
                } = val
                else {
                    return Ok(false);
                };
                check_pat_enum_ty(&segs, &path.ty)?;
                if *variant != want {
                    return Ok(false);
                }
                for fp in &ps.fields {
                    let name = member_name(&fp.member)?;
                    let sub = fields.get(&name).ok_or_else(|| {
                        EvalError::new(format!("pattern names unknown field `{name}`"))
                    })?;
                    if !self.match_pattern(&fp.pat, sub, env)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Pat::TupleStruct(pts) => {
                let segs = path_segs(&pts.path);
                let want = segs.last().cloned().unwrap_or_default();
                let Value::Enum {
                    path,
                    variant,
                    body: VariantBody::Tuple(items),
                } = val
                else {
                    return Ok(false);
                };
                check_pat_enum_ty(&segs, &path.ty)?;
                if *variant != want {
                    return Ok(false);
                }
                self.match_seq(&pts.elems, items, env)
            }
            Pat::Tuple(pt) => match val {
                Value::Tuple(items) => self.match_seq(&pt.elems, items, env),
                _ => Ok(false),
            },
            _ => Err(EvalError::new("unsupported pattern")),
        }
    }

    /// Positionally match a pattern sequence against values, honouring a single
    /// `..` rest (which matches zero or more middle elements).
    fn match_seq(
        &self,
        pats: &Punctuated<Pat, Comma>,
        vals: &[Value<B::Host>],
        env: &mut Env<B::Host>,
    ) -> Result<bool, EvalError> {
        let pats: Vec<&Pat> = pats.iter().collect();
        let Some(rest_idx) = pats.iter().position(|p| matches!(p, Pat::Rest(_))) else {
            if pats.len() != vals.len() {
                return Ok(false);
            }
            for (p, v) in pats.iter().zip(vals) {
                if !self.match_pattern(p, v, env)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        };
        if pats[rest_idx + 1..]
            .iter()
            .any(|p| matches!(p, Pat::Rest(_)))
        {
            return Err(EvalError::new("at most one `..` is allowed in a pattern"));
        }
        let head = &pats[..rest_idx];
        let tail = &pats[rest_idx + 1..];
        if vals.len() < head.len() + tail.len() {
            return Ok(false);
        }
        for (p, v) in head.iter().zip(&vals[..head.len()]) {
            if !self.match_pattern(p, v, env)? {
                return Ok(false);
            }
        }
        let tail_start = vals.len() - tail.len();
        for (p, v) in tail.iter().zip(&vals[tail_start..]) {
            if !self.match_pattern(p, v, env)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn eval_macro(
        &self,
        mac: &syn::Macro,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let name = mac
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        match name.as_str() {
            "vec" => {
                let exprs = mac
                    .parse_body_with(Punctuated::<Expr, Comma>::parse_terminated)
                    .map_err(|e| EvalError::new(format!("vec! parse error: {e}")))?;
                let mut out = Vec::with_capacity(exprs.len());
                for e in &exprs {
                    out.push(self.eval_expr(e, env)?);
                }
                Ok(Value::Vec(out))
            }
            "format" => self.eval_format(mac, env),
            "matches" => self.eval_matches(mac, env),
            other => Err(EvalError::new(format!("macro `{other}!` is not allowed"))),
        }
    }

    fn eval_format(
        &self,
        mac: &syn::Macro,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let args = mac
            .parse_body_with(Punctuated::<Expr, Comma>::parse_terminated)
            .map_err(|e| EvalError::new(format!("format! parse error: {e}")))?;
        let mut it = args.iter();
        let tmpl_expr = it
            .next()
            .ok_or_else(|| EvalError::new("format! needs a template"))?;
        let tmpl = match self.eval_expr(tmpl_expr, env)? {
            Value::Str(s) => s,
            _ => return Err(EvalError::new("format! template must be a string literal")),
        };
        let mut parts = Vec::new();
        for e in it {
            parts.push(value_to_display(&self.eval_expr(e, env)?)?);
        }
        Ok(Value::Str(interpolate(&tmpl, &parts)?))
    }

    fn eval_matches(
        &self,
        mac: &syn::Macro,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let (expr, pat) = mac
            .parse_body_with(|input: syn::parse::ParseStream| {
                let expr: Expr = input.parse()?;
                let _comma: Comma = input.parse()?;
                let pat = Pat::parse_multi_with_leading_vert(input)?;
                Ok((expr, pat))
            })
            .map_err(|e| EvalError::new(format!("matches! parse error: {e}")))?;
        let v = self.eval_expr(&expr, env)?;
        let mut probe = env.child();
        Ok(Value::Bool(self.match_pattern(&pat, &v, &mut probe)?))
    }

    fn eval_binary(
        &self,
        eb: &syn::ExprBinary,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        use syn::BinOp;
        // short-circuit the boolean connectives
        match eb.op {
            BinOp::And(_) => {
                let l = as_bool(&self.eval_expr(&eb.left, env)?)?;
                return Ok(Value::Bool(l && as_bool(&self.eval_expr(&eb.right, env)?)?));
            }
            BinOp::Or(_) => {
                let l = as_bool(&self.eval_expr(&eb.left, env)?)?;
                return Ok(Value::Bool(l || as_bool(&self.eval_expr(&eb.right, env)?)?));
            }
            _ => {}
        }
        let l = self.eval_expr(&eb.left, env)?;
        let r = self.eval_expr(&eb.right, env)?;
        // Host handles are not value-comparable; reject rather than reach the
        // `PartialEq` tripwire (which would panic in debug / be wrong in release).
        if matches!(eb.op, BinOp::Eq(_) | BinOp::Ne(_)) && (l.contains_host() || r.contains_host())
        {
            return Err(EvalError::new("cannot compare host handles with `==`/`!=`"));
        }
        let b = match eb.op {
            BinOp::Eq(_) => l == r,
            BinOp::Ne(_) => l != r,
            BinOp::Lt(_) => int_of(&l)? < int_of(&r)?,
            BinOp::Le(_) => int_of(&l)? <= int_of(&r)?,
            BinOp::Gt(_) => int_of(&l)? > int_of(&r)?,
            BinOp::Ge(_) => int_of(&l)? >= int_of(&r)?,
            _ => return Err(EvalError::new("unsupported binary operator")),
        };
        Ok(Value::Bool(b))
    }

    fn eval_unary(
        &self,
        eu: &syn::ExprUnary,
        env: &mut Env<B::Host>,
    ) -> Result<Value<B::Host>, EvalError> {
        let v = self.eval_expr(&eu.expr, env)?;
        match eu.op {
            syn::UnOp::Not(_) => Ok(Value::Bool(!as_bool(&v)?)),
            _ => Err(EvalError::new("unsupported unary operator")),
        }
    }

    fn eval_args(
        &self,
        args: &Punctuated<Expr, Comma>,
        env: &mut Env<B::Host>,
    ) -> Result<Vec<Value<B::Host>>, EvalError> {
        let mut out = Vec::with_capacity(args.len());
        for a in args {
            out.push(self.eval_expr(a, env)?);
        }
        Ok(out)
    }
}

// ── pure helpers (host-generic; never touch a host handle) ──────────────────

fn path_segs(p: &syn::Path) -> Vec<String> {
    p.segments.iter().map(|s| s.ident.to_string()).collect()
}

/// A variant pattern (`Ty::Variant{..}` / `Ty::Variant(..)` / `Ty::Variant`) names
/// its enum via the second-to-last path segment. Matching it against a value of a
/// *different* enum type is a type error in Rust — reject it, rather than silently
/// matching/missing on a shared leaf name.
fn check_pat_enum_ty(segs: &[String], value_ty: &str) -> Result<(), EvalError> {
    if segs.len() >= 2 {
        let pat_ty = &segs[segs.len() - 2];
        if pat_ty != value_ty {
            return Err(EvalError::new(format!(
                "pattern names enum `{pat_ty}` but the value is `{value_ty}`"
            )));
        }
    }
    Ok(())
}

fn member_name(m: &syn::Member) -> Result<String, EvalError> {
    match m {
        syn::Member::Named(i) => Ok(i.to_string()),
        syn::Member::Unnamed(_) => Err(EvalError::new("tuple-field access is not supported")),
    }
}

fn check_empty_spread(rest: &Expr, ty: &str) -> Result<(), EvalError> {
    if let Expr::Path(p) = rest {
        let segs = path_segs(&p.path);
        if segs.len() == 2 && segs[0] == ty && segs[1] == "EMPTY" {
            return Ok(());
        }
    }
    Err(EvalError::new(format!(
        "struct spread must be `..{ty}::EMPTY`"
    )))
}

fn eval_lit<H>(lit: &syn::Lit) -> Result<Value<H>, EvalError> {
    match lit {
        syn::Lit::Str(s) => Ok(Value::Str(s.value())),
        syn::Lit::Bool(b) => Ok(Value::Bool(b.value)),
        syn::Lit::Int(i) => i
            .base10_parse::<i128>()
            .map(Value::Int)
            .map_err(|e| EvalError::new(format!("invalid integer: {e}"))),
        _ => Err(EvalError::new("unsupported literal")),
    }
}

fn as_bool<H: std::fmt::Debug>(v: &Value<H>) -> Result<bool, EvalError> {
    match v {
        Value::Bool(b) => Ok(*b),
        other => Err(EvalError::new(format!("expected a boolean, got {other:?}"))),
    }
}

fn int_of<H: std::fmt::Debug>(v: &Value<H>) -> Result<i128, EvalError> {
    match v {
        Value::Int(n) => Ok(*n),
        other => Err(EvalError::new(format!(
            "expected an integer, got {other:?}"
        ))),
    }
}

fn value_to_display<H: std::fmt::Debug>(v: &Value<H>) -> Result<String, EvalError> {
    match v {
        Value::Str(s) => Ok(s.clone()),
        Value::Int(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => Err(EvalError::new(format!("cannot format {other:?}"))),
    }
}

/// Minimal `format!` interpolation: positional `{}` and `{{`/`}}` escapes. Any
/// other brace form (named, indexed, or with a format spec) is rejected — the
/// `.tkd` subset only uses bare `{}`.
fn interpolate(tmpl: &str, args: &[String]) -> Result<String, EvalError> {
    let mut out = String::with_capacity(tmpl.len());
    let mut chars = tmpl.chars().peekable();
    let mut next = 0usize;
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                if chars.peek() == Some(&'{') {
                    chars.next();
                    out.push('{');
                } else if chars.peek() == Some(&'}') {
                    chars.next();
                    let a = args
                        .get(next)
                        .ok_or_else(|| EvalError::new("format!: too few arguments"))?;
                    out.push_str(a);
                    next += 1;
                } else {
                    return Err(EvalError::new(
                        "format!: only bare `{}` placeholders are supported",
                    ));
                }
            }
            '}' => {
                if chars.peek() == Some(&'}') {
                    chars.next();
                    out.push('}');
                } else {
                    return Err(EvalError::new("format!: stray `}`"));
                }
            }
            other => out.push(other),
        }
    }
    if next != args.len() {
        return Err(EvalError::new("format!: too many arguments"));
    }
    Ok(out)
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
        _ => "expression",
    }
}
