//! Evaluation: a checked [`Program`] × [`RuntimeContext`] → neutral
//! [`Composition`] (the "execute" phase).
//!
//! Pure and total: it reads only the program and the injected context, performs
//! no I/O, and always terminates (Property 1 extends to execution as a pure
//! function of its explicit inputs). Determinism holds in the composition
//! *structure* and the set of references given the context; deferred
//! [`OutputRef`]s are resolved later by the engine (Property 2 / Requirement
//! 15).
//!
//! Run [`crate::resolve`] and [`crate::typeck`] first; evaluation assumes a
//! resolved, type-checked program and reports the residual *evaluation* errors
//! (a required input left unbound, a builtin misuse, a non-record field access)
//! as diagnostics, returning `Err` if any occur so no partial composition is
//! handed downstream (Property 6).

use std::collections::{HashMap, HashSet};

use crate::{
    Span,
    ast::{
        Expr, Item, KindInstance, MatchExpr, ModuleDecl, ModuleItem, Program, RecordExpr, Spanned,
    },
    diagnostic::Diag,
    value::{
        Composition, ItemRole, LoweredImage, LoweredItem, LoweredModule, LoweredWriteback,
        OutputRef, RuntimeContext, Value,
    },
};

/// Evaluate a program with no operator input overrides (inputs take defaults).
pub fn evaluate(program: &Program, ctx: &RuntimeContext) -> Result<Composition, Vec<Diag>> {
    evaluate_with_inputs(program, ctx, &HashMap::new())
}

/// Evaluate a program, overriding named inputs with operator-supplied values.
///
/// An input with neither an override nor a default is a required-input error
/// (Requirement 8.3).
pub fn evaluate_with_inputs(
    program: &Program,
    ctx: &RuntimeContext,
    inputs: &HashMap<String, Value>,
) -> Result<Composition, Vec<Diag>> {
    let mut module_names = HashSet::new();
    for item in &program.items {
        if let Item::Module(module) = item {
            module_names.insert(module.name.node.clone());
        }
    }
    let mut evaluator = Evaluator {
        ctx,
        env: vec![HashMap::new()],
        module_names,
        diagnostics: Vec::new(),
    };
    let composition = evaluator.run(program, inputs);
    if evaluator.diagnostics.iter().any(Diag::is_error) {
        Err(evaluator.diagnostics)
    } else {
        Ok(composition)
    }
}

struct Evaluator<'a> {
    ctx: &'a RuntimeContext,
    /// Lexical value frames; frame 0 holds inputs+lets, inner frames hold
    /// match-arm payload bindings.
    env: Vec<HashMap<String, Value>>,
    module_names: HashSet<String>,
    diagnostics: Vec<Diag>,
}

impl Evaluator<'_> {
    fn run(&mut self, program: &Program, inputs: &HashMap<String, Value>) -> Composition {
        // Inputs first, then lets (which may read inputs/ctx/earlier lets).
        for item in &program.items {
            if let Item::Input(decl) = item {
                let value = if let Some(override_value) = inputs.get(&decl.name.node) {
                    override_value.clone()
                } else if let Some(default) = &decl.default {
                    self.eval_expr(default)
                } else {
                    self.error(
                        decl.name.span.clone(),
                        format!("required input `{}` is unbound", decl.name.node),
                    );
                    Value::Absent
                };
                self.bind(decl.name.node.clone(), value);
            }
        }
        for item in &program.items {
            if let Item::Let(decl) = item {
                let value = self.eval_expr(&decl.value);
                self.bind(decl.name.node.clone(), value);
            }
        }

        let mut composition = Composition::default();
        for item in &program.items {
            match item {
                Item::Module(module) => {
                    if let Some(lowered) = self.eval_module(module) {
                        composition.modules.push(lowered);
                    }
                }
                Item::Image(instance) => composition.images.push(self.eval_image(instance)),
                Item::Namespaces(list) => {
                    composition
                        .namespaces
                        .extend(list.node.iter().map(|name| name.node.clone()));
                }
                Item::Writeback(decl) => {
                    let active = match &decl.when {
                        Some(when) => self.eval_bool(when),
                        None => true,
                    };
                    if active {
                        for target in &decl.targets {
                            if let Some(source) = self.eval_output_ref(&target.value) {
                                composition.writeback.push(LoweredWriteback {
                                    key: target.key.node.clone(),
                                    source,
                                });
                            }
                        }
                    }
                }
                Item::Use(_) | Item::Input(_) | Item::Let(_) => {}
            }
        }
        composition
    }

    // ── Modules and items ─────────────────────────────────────────────

    fn eval_module(&mut self, module: &ModuleDecl) -> Option<LoweredModule> {
        if let Some(when) = &module.when
            && !self.eval_bool(when)
        {
            return None; // conditionally absent module
        }
        let depends_on = module
            .depends_on
            .as_ref()
            .map(|expr| self.eval_name_list(expr))
            .unwrap_or_default();
        let mut items = Vec::new();
        for item in &module.items {
            self.eval_module_item(item, &mut items);
        }
        Some(LoweredModule {
            name: module.name.node.clone(),
            depends_on,
            items,
        })
    }

    fn eval_module_item(&mut self, item: &ModuleItem, out: &mut Vec<LoweredItem>) {
        match item {
            ModuleItem::Resource(instance) => {
                out.push(self.eval_item(instance, ItemRole::Resource))
            }
            ModuleItem::Service(instance) => out.push(self.eval_item(instance, ItemRole::Service)),
            ModuleItem::Match(block) => {
                let scrutinee = self.eval_expr(&block.scrutinee);
                let variant = self.variant_name(&scrutinee, block.span.clone());
                // Find the arm matching the variant, else the `_` wildcard.
                let arm = block
                    .arms
                    .iter()
                    .find(|arm| Some(arm.variant.node.as_str()) == variant.as_deref())
                    .or_else(|| block.arms.iter().find(|arm| arm.variant.node == "_"));
                if let Some(arm) = arm {
                    self.push_binding(arm.binding.as_ref(), &scrutinee);
                    for inner in &arm.items {
                        self.eval_module_item(inner, out);
                    }
                    self.env.pop();
                }
            }
        }
    }

    fn eval_item(&mut self, instance: &KindInstance, role: ItemRole) -> LoweredItem {
        let mut fields = std::collections::BTreeMap::new();
        let mut depends_on = Vec::new();
        for field in &instance.fields {
            if field.key.node == "depends_on" {
                depends_on = self.eval_name_list(&field.value);
            } else {
                fields.insert(field.key.node.clone(), self.eval_expr(&field.value));
            }
        }
        LoweredItem {
            id: instance.name.node.clone(),
            kind: instance.kind.node.clone(),
            role,
            fields,
            depends_on,
        }
    }

    fn eval_image(&mut self, instance: &KindInstance) -> LoweredImage {
        let mut fields = std::collections::BTreeMap::new();
        for field in &instance.fields {
            fields.insert(field.key.node.clone(), self.eval_expr(&field.value));
        }
        LoweredImage {
            name: instance.name.node.clone(),
            kind: instance.kind.node.clone(),
            fields,
        }
    }

    // ── Expressions ───────────────────────────────────────────────────

    fn eval_expr(&mut self, expr: &Expr) -> Value {
        match expr {
            Expr::Str(value) => Value::Str(value.node.clone()),
            Expr::Int(value) => Value::Int(value.node),
            Expr::Bool(value) => Value::Bool(value.node),
            Expr::Ident(name) => self.eval_ident(name),
            Expr::Field(..) => self.eval_field(expr),
            Expr::List(items) => {
                Value::List(items.node.iter().map(|e| self.eval_expr(e)).collect())
            }
            Expr::Record(record) => self.eval_record(record),
            Expr::Concat(left, right) => self.eval_concat(left, right),
            Expr::PathJoin(left, right) => self.eval_path_join(left, right),
            Expr::Is(inner, variant) => {
                let value = self.eval_expr(inner);
                match self.variant_name(&value, expr_span(inner)) {
                    Some(name) => Value::Bool(name == variant.node),
                    None => Value::Bool(false),
                }
            }
            Expr::Match(match_expr) => self.eval_match(match_expr),
            Expr::Call(call) => self.eval_call(call),
        }
    }

    fn eval_ident(&mut self, name: &Spanned<String>) -> Value {
        // Built-in mount-mode constants.
        if name.node == "ro" || name.node == "rw" {
            return Value::Str(name.node.clone());
        }
        // Bare UpperCamelCase identifier in value position: a payload-less
        // variant constructor (e.g. `InMemory`, `Managed`).
        if is_constructor(&name.node) {
            return Value::Variant {
                name: name.node.clone(),
                payload: None,
            };
        }
        if let Some(value) = self.lookup(&name.node) {
            return value;
        }
        // A bare resource/module name in value position is only valid in
        // depends_on (handled separately); anywhere else it is an error.
        self.error(
            name.span.clone(),
            format!("`{}` cannot be used as a value here", name.node),
        );
        Value::Absent
    }

    fn eval_field(&mut self, expr: &Expr) -> Value {
        let Some((root, segments)) = flatten(expr) else {
            self.error(expr_span(expr), "unsupported field access");
            return Value::Absent;
        };

        if root.node == "ctx" {
            return self.ctx_field(&segments);
        }
        if let Some(mut value) = self.lookup(&root.node) {
            // Record field access on a bound value (e.g. a match payload `d`).
            for segment in &segments {
                value = match value {
                    Value::Record(map) => map.get(&segment.node).cloned().unwrap_or(Value::Absent),
                    other => {
                        self.error(
                            segment.span.clone(),
                            format!("cannot read field `{}` of a non-record value", segment.node),
                        );
                        return other;
                    }
                };
            }
            return value;
        }
        // Otherwise an output reference (root is a resource or module).
        match self.output_ref(&root, &segments) {
            Some(reference) => Value::Output(reference),
            None => Value::Absent,
        }
    }

    fn ctx_field(&mut self, segments: &[Spanned<String>]) -> Value {
        match segments.first().map(|s| s.node.as_str()) {
            Some("deployment_dir") => Value::Path(self.ctx.deployment_dir.clone()),
            Some("home") => Value::Path(self.ctx.home.clone()),
            Some("region") => Value::Str(self.ctx.region.clone()),
            other => {
                let span = segments.first().map(|s| s.span.clone()).unwrap_or(0..0);
                self.error(
                    span,
                    format!("`ctx` has no field `{}`", other.unwrap_or("")),
                );
                Value::Absent
            }
        }
    }

    fn eval_record(&mut self, record: &RecordExpr) -> Value {
        let mut map = std::collections::BTreeMap::new();
        for spread in &record.spreads {
            match self.eval_expr(spread) {
                Value::Record(inner) => map.extend(inner),
                Value::Absent => {}
                _ => self.error(expr_span(spread), "spread (`..`) requires a record"),
            }
        }
        for field in &record.fields {
            map.insert(field.key.node.clone(), self.eval_expr(&field.value));
        }
        Value::Record(map)
    }

    fn eval_concat(&mut self, left: &Expr, right: &Expr) -> Value {
        let left_value = self.eval_expr(left);
        let right_value = self.eval_expr(right);
        match (left_value, right_value) {
            (Value::List(mut a), Value::List(b)) => {
                a.extend(b);
                Value::List(a)
            }
            _ => {
                self.error(expr_span(left), "`++` requires two lists");
                Value::Absent
            }
        }
    }

    fn eval_path_join(&mut self, left: &Expr, right: &Expr) -> Value {
        let left_value = self.eval_expr(left);
        let right_value = self.eval_expr(right);
        match (as_path_str(&left_value), as_path_str(&right_value)) {
            (Some(a), Some(b)) => Value::Path(format!("{}/{}", a.trim_end_matches('/'), b)),
            _ => {
                self.error(expr_span(left), "`/` requires path or string operands");
                Value::Absent
            }
        }
    }

    fn eval_match(&mut self, match_expr: &MatchExpr) -> Value {
        let scrutinee = self.eval_expr(&match_expr.scrutinee);
        let variant = self.variant_name(&scrutinee, match_expr.span.clone());
        let arm = match_expr
            .arms
            .iter()
            .find(|arm| Some(arm.variant.node.as_str()) == variant.as_deref())
            .or_else(|| match_expr.arms.iter().find(|arm| arm.variant.node == "_"));
        match arm {
            Some(arm) => {
                self.push_binding(arm.binding.as_ref(), &scrutinee);
                let value = self.eval_expr(&arm.body);
                self.env.pop();
                value
            }
            None => {
                self.error(
                    match_expr.span.clone(),
                    "no match arm matched the scrutinee",
                );
                Value::Absent
            }
        }
    }

    fn eval_call(&mut self, call: &crate::ast::CallExpr) -> Value {
        let args: Vec<Value> = call.args.iter().map(|arg| self.eval_expr(arg)).collect();
        match call.func.node.as_str() {
            // port(n) → "n:n"
            "port" => match args.first() {
                Some(Value::Int(port)) => Value::Str(format!("{port}:{port}")),
                _ => {
                    self.error(call.span.clone(), "port(n) expects an integer port");
                    Value::Absent
                }
            },
            // bind(src, dst, ro|rw) → "src:dst:ro" or "src:dst"
            "bind" => self.eval_bind(&args, call.span.clone()),
            // present_only(record) → record with Absent entries removed
            "present_only" => match args.into_iter().next() {
                Some(Value::Record(map)) => Value::Record(
                    map.into_iter()
                        .filter(|(_, value)| !matches!(value, Value::Absent))
                        .collect(),
                ),
                _ => {
                    self.error(call.span.clone(), "present_only(record) expects a record");
                    Value::Absent
                }
            },
            other => {
                self.error(call.func.span.clone(), format!("unknown builtin `{other}`"));
                Value::Absent
            }
        }
    }

    fn eval_bind(&mut self, args: &[Value], span: Span) -> Value {
        let (Some(src), Some(dst), Some(mode)) = (args.first(), args.get(1), args.get(2)) else {
            self.error(span, "bind(src, dst, ro|rw) expects three arguments");
            return Value::Absent;
        };
        let (Some(src), Some(dst)) = (as_path_str(src), as_path_str(dst)) else {
            self.error(span, "bind expects path/string src and dst");
            return Value::Absent;
        };
        // `ro` appends a read-only suffix; `rw` is the default with none —
        // matching the compose volume string convention.
        match mode {
            Value::Str(m) if m == "ro" => Value::Str(format!("{src}:{dst}:ro")),
            Value::Str(m) if m == "rw" => Value::Str(format!("{src}:{dst}")),
            _ => {
                self.error(span, "bind mode must be `ro` or `rw`");
                Value::Absent
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────

    /// Evaluate an expression that must be a list of bare names (a `depends_on`
    /// list or a `match` selecting one), returning the names. Bare identifiers
    /// here are dependency references, not values.
    fn eval_name_list(&mut self, expr: &Expr) -> Vec<String> {
        match expr {
            Expr::List(items) => items
                .node
                .iter()
                .filter_map(|item| match item {
                    Expr::Ident(name) => Some(name.node.clone()),
                    other => {
                        self.error(expr_span(other), "depends_on entries must be names");
                        None
                    }
                })
                .collect(),
            Expr::Match(match_expr) => {
                let scrutinee = self.eval_expr(&match_expr.scrutinee);
                let variant = self.variant_name(&scrutinee, match_expr.span.clone());
                let arm = match_expr
                    .arms
                    .iter()
                    .find(|arm| Some(arm.variant.node.as_str()) == variant.as_deref())
                    .or_else(|| match_expr.arms.iter().find(|arm| arm.variant.node == "_"));
                match arm {
                    Some(arm) => {
                        self.push_binding(arm.binding.as_ref(), &scrutinee);
                        let names = self.eval_name_list(&arm.body);
                        self.env.pop();
                        names
                    }
                    None => Vec::new(),
                }
            }
            other => {
                self.error(expr_span(other), "expected a list of dependency names");
                Vec::new()
            }
        }
    }

    fn eval_bool(&mut self, expr: &Expr) -> bool {
        match self.eval_expr(expr) {
            Value::Bool(value) => value,
            _ => {
                self.error(expr_span(expr), "condition must evaluate to a boolean");
                false
            }
        }
    }

    fn output_ref(
        &mut self,
        root: &Spanned<String>,
        segments: &[Spanned<String>],
    ) -> Option<OutputRef> {
        if self.module_names.contains(&root.node) {
            if segments.len() >= 2 {
                return Some(OutputRef {
                    module: Some(root.node.clone()),
                    resource: segments[0].node.clone(),
                    output: segments[1].node.clone(),
                });
            }
            self.error(
                root.span.clone(),
                "qualified reference needs `module.resource.output`",
            );
            return None;
        }
        match segments.first() {
            Some(output) => Some(OutputRef {
                module: None,
                resource: root.node.clone(),
                output: output.node.clone(),
            }),
            None => {
                self.error(root.span.clone(), "expected an output reference");
                None
            }
        }
    }

    /// Evaluate an expression expected to be an output reference (writeback
    /// source).
    fn eval_output_ref(&mut self, expr: &Expr) -> Option<OutputRef> {
        match self.eval_expr(expr) {
            Value::Output(reference) => Some(reference),
            _ => {
                self.error(
                    expr_span(expr),
                    "writeback source must be a resource output",
                );
                None
            }
        }
    }

    fn variant_name(&mut self, value: &Value, span: Span) -> Option<String> {
        match value {
            Value::Variant { name, .. } => Some(name.clone()),
            _ => {
                self.error(span, "expected a sum-type (enum) value");
                None
            }
        }
    }

    fn push_binding(&mut self, binding: Option<&Spanned<String>>, scrutinee: &Value) {
        let mut frame = HashMap::new();
        if let Some(name) = binding {
            let payload = match scrutinee {
                Value::Variant {
                    payload: Some(payload),
                    ..
                } => (**payload).clone(),
                _ => Value::Absent,
            };
            frame.insert(name.node.clone(), payload);
        }
        self.env.push(frame);
    }

    fn bind(&mut self, name: String, value: Value) {
        self.env
            .first_mut()
            .expect("base env frame")
            .insert(name, value);
    }

    fn lookup(&self, name: &str) -> Option<Value> {
        for frame in self.env.iter().rev() {
            if let Some(value) = frame.get(name) {
                return Some(value.clone());
            }
        }
        None
    }

    fn error(&mut self, span: Span, message: impl Into<String>) {
        self.diagnostics.push(Diag::error(span, message));
    }
}

/// Whether `name` is an UpperCamelCase constructor (a variant in value
/// position).
fn is_constructor(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_uppercase())
}

/// Render a value as a path/string operand for `/` and `bind`.
fn as_path_str(value: &Value) -> Option<String> {
    match value {
        Value::Path(s) | Value::Str(s) => Some(s.clone()),
        _ => None,
    }
}

/// Flatten an identifier-rooted `.`-chain into `(root, [segments…])`.
fn flatten(expr: &Expr) -> Option<(Spanned<String>, Vec<Spanned<String>>)> {
    match expr {
        Expr::Ident(name) => Some((name.clone(), Vec::new())),
        Expr::Field(base, field) => {
            let (root, mut segments) = flatten(base)?;
            segments.push(field.clone());
            Some((root, segments))
        }
        _ => None,
    }
}

/// A best-effort span for an expression, for diagnostics.
fn expr_span(expr: &Expr) -> Span {
    match expr {
        Expr::Str(s) => s.span.clone(),
        Expr::Int(s) => s.span.clone(),
        Expr::Bool(s) => s.span.clone(),
        Expr::Ident(s) => s.span.clone(),
        Expr::Field(_, field) => field.span.clone(),
        Expr::List(list) => list.span.clone(),
        Expr::Record(record) => record.span.clone(),
        Expr::Concat(left, _) | Expr::PathJoin(left, _) | Expr::Is(left, _) => expr_span(left),
        Expr::Match(match_expr) => match_expr.span.clone(),
        Expr::Call(call) => call.span.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{lex, parser::parse};

    fn ctx() -> RuntimeContext {
        RuntimeContext {
            deployment_dir: "/dep".into(),
            home: "/home/u".into(),
            region: "eu-west-2".into(),
        }
    }

    fn program(source: &str) -> Program {
        let (tokens, lex_diags) = lex(source);
        assert!(lex_diags.is_empty(), "lex errors: {lex_diags:?}");
        let (program, parse_diags) = parse(&tokens, source.len());
        assert!(parse_diags.is_empty(), "parse errors: {parse_diags:?}");
        program.expect("program")
    }

    fn module<'a>(composition: &'a Composition, name: &str) -> Option<&'a LoweredModule> {
        composition.modules.iter().find(|m| m.name == name)
    }

    fn item<'a>(module: &'a LoweredModule, id: &str) -> &'a LoweredItem {
        module.items.iter().find(|i| i.id == id).expect("item")
    }

    #[test]
    fn lowers_inmemory_composition_omitting_conditional_module() {
        let program = program(
            r#"platform compose {
                input storage: Storage = InMemory
                module local_state { resource state_dir = LocalStateDir { } }
                module dsql when storage is Dsql {
                    resource cluster = DsqlCluster { mode: "Managed", region: "x" }
                }
                module runtime {
                    depends_on [ local_state ]
                    service tokeirad = ComposeService {
                        image: "tokeirad:latest",
                        ports: [ port(7233) ],
                        volumes: [ bind(ctx.deployment_dir / "tokeirad.toml", "/etc/tokeira/tokeirad.toml", ro) ],
                        depends_on: [ state_dir ],
                    }
                }
            }"#,
        );
        let composition = evaluate(&program, &ctx()).expect("composition");

        let names: Vec<&str> = composition
            .modules
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["local_state", "runtime"],
            "dsql is conditionally absent"
        );

        let runtime = module(&composition, "runtime").unwrap();
        assert_eq!(runtime.depends_on, vec!["local_state"]);
        let tokeirad = item(runtime, "tokeirad");
        assert_eq!(tokeirad.role, ItemRole::Service);
        assert_eq!(tokeirad.depends_on, vec!["state_dir"]);
        assert_eq!(
            tokeirad.fields.get("image"),
            Some(&Value::Str("tokeirad:latest".into()))
        );
        assert_eq!(
            tokeirad.fields.get("ports"),
            Some(&Value::List(vec![Value::Str("7233:7233".into())]))
        );
        assert_eq!(
            tokeirad.fields.get("volumes"),
            Some(&Value::List(vec![Value::Str(
                "/dep/tokeirad.toml:/etc/tokeira/tokeirad.toml:ro".into()
            )]))
        );
        // depends_on must not leak into the field map.
        assert!(!tokeirad.fields.contains_key("depends_on"));
    }

    #[test]
    fn lowers_dsql_arm_with_input_override_and_payload_binding() {
        let program = program(
            r#"platform compose {
                input storage: Storage = InMemory
                module infra {
                    match storage {
                        Dsql(d) => { resource cluster = DsqlCluster { mode: "Managed", region: d.region } }
                        InMemory => { }
                    }
                }
            }"#,
        );
        let mut payload = BTreeMap::new();
        payload.insert("region".to_string(), Value::Str("eu-west-2".into()));
        let mut inputs = std::collections::HashMap::new();
        inputs.insert(
            "storage".to_string(),
            Value::Variant {
                name: "Dsql".into(),
                payload: Some(Box::new(Value::Record(payload))),
            },
        );

        let composition = evaluate_with_inputs(&program, &ctx(), &inputs).expect("composition");
        let infra = module(&composition, "infra").unwrap();
        let cluster = item(infra, "cluster");
        // The payload binding `d` resolved `d.region` to the override value.
        assert_eq!(
            cluster.fields.get("region"),
            Some(&Value::Str("eu-west-2".into()))
        );
    }

    #[test]
    fn output_reference_lowers_to_a_deferred_ref() {
        let program = program(
            r#"platform compose {
                module m {
                    resource cluster = DsqlCluster { mode: "Managed", region: "x" }
                    service s = ComposeService { image: "x", env: { "ARN": cluster.cluster_arn } }
                }
            }"#,
        );
        let composition = evaluate(&program, &ctx()).expect("composition");
        let s = item(module(&composition, "m").unwrap(), "s");
        let Value::Record(env) = s.fields.get("env").unwrap() else {
            panic!("env should be a record");
        };
        assert_eq!(
            env.get("ARN"),
            Some(&Value::Output(OutputRef {
                module: None,
                resource: "cluster".into(),
                output: "cluster_arn".into(),
            }))
        );
    }

    #[test]
    fn writeback_lowers_to_qualified_output_ref_when_active() {
        let program = program(
            r#"platform compose {
                input storage: Storage = InMemory
                module dsql { resource cluster = DsqlCluster { mode: "Managed", region: "x" } }
                writeback when storage is Dsql {
                    "infrastructure.dsql.endpoint" : dsql.cluster.cluster_endpoint,
                }
            }"#,
        );
        // InMemory → writeback guard is false → no targets.
        let inmemory = evaluate(&program, &ctx()).expect("composition");
        assert!(inmemory.writeback.is_empty());

        // Dsql → guard true → one qualified output-ref target.
        let mut inputs = std::collections::HashMap::new();
        inputs.insert(
            "storage".to_string(),
            Value::Variant {
                name: "Dsql".into(),
                payload: None,
            },
        );
        let dsql = evaluate_with_inputs(&program, &ctx(), &inputs).expect("composition");
        assert_eq!(dsql.writeback.len(), 1);
        assert_eq!(
            dsql.writeback[0].source,
            OutputRef {
                module: Some("dsql".into()),
                resource: "cluster".into(),
                output: "cluster_endpoint".into(),
            }
        );
    }

    #[test]
    fn required_input_without_default_or_override_is_an_error() {
        let program = program(
            r#"platform compose {
                input region: String
                let r = region
            }"#,
        );
        let result = evaluate(&program, &ctx());
        let diags = result.expect_err("should error");
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("required input `region` is unbound")),
            "got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }
}
