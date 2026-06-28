//! Type checking: sum-variant validation, output-reference validation,
//! per-field value typing, secret taint, and validation-parity constraints.
//!
//! This is the second half of the compile-time "resolve → type-check" phase. It
//! runs after [`crate::resolve`] and enforces the DSL's type rules the
//! correctness properties depend on:
//!
//! - **Sum-variant validation** (Property 5) — a variant used in value position
//!   (an `input` default) or as a `match` arm must belong to the relevant sum
//!   type. This makes the typed conditional a *type* obligation, not a runtime
//!   guard.
//! - **Output-reference validation** (Property 21 / Requirement 15.3) — a
//!   reference `<resource>.<output>` (optionally module-qualified) must name a
//!   resource that exists and an output its kind declares.
//! - **Per-field value typing** (task 4.3 / Requirements 3.2, 5.1) — a field's
//!   supplied value must have a type compatible with the kind's declared
//!   [`FieldType`]. Inference is **sound, not complete**: an expression whose
//!   type cannot be determined (a sum-payload access, a deferred output ref, a
//!   `match`) infers to [`FieldType::Any`] and is never rejected, so the checker
//!   produces no false positives on a valid definition.
//! - **Secret taint** (task 4.4 / Requirement 12.3) — a value originating from a
//!   `Secret<T>` input/binding may flow only into a field typed `Secret<_>`; a
//!   secret reaching a non-secret field is a diagnostic. Diagnostics carry only
//!   names and spans, never values, so no secret is echoed.
//! - **Validation-parity constraints** (task 4.5 / Requirement 5) — a kind's
//!   [`Constraint`]s (ranges, paired presence) are checked against statically-
//!   known values; a constraint over a value not statically known is skipped,
//!   not failed (the same soundness rule as field typing).

use std::collections::{HashMap, HashSet};

use crate::{
    ast::{
        Expr, Item, KindInstance, MatchBlock, MatchExpr, ModuleDecl, ModuleItem, Program, TypeExpr,
    },
    diagnostic::Diag,
    kind::{Constraint, FieldType, KindLibrary},
};

/// The built-in sum types of the language, as `(name, variants)`.
///
/// User-declared sums are a later addition; today the sums are fixed by the
/// language (`Storage`, `DsqlMode`). Variant payloads are not modelled here —
/// only the legal variant names per sum.
const ENUMS: &[(&str, &[&str])] = &[
    ("Storage", &["InMemory", "Dsql"]),
    ("DsqlMode", &["Managed", "Preexisting"]),
];

fn enum_variants(name: &str) -> Option<&'static [&'static str]> {
    ENUMS
        .iter()
        .find(|(enum_name, _)| *enum_name == name)
        .map(|(_, variants)| *variants)
}

/// Type-check `program` against `kinds`, returning all diagnostics. Assumes the
/// program already parsed; run [`crate::resolve`] first for name resolution.
pub fn typeck(program: &Program, kinds: &KindLibrary) -> Vec<Diag> {
    let mut checker = TypeChecker {
        kinds,
        diagnostics: Vec::new(),
        input_enum: HashMap::new(),
        input_type: HashMap::new(),
        let_type: HashMap::new(),
        tainted: HashSet::new(),
        resource_kind: HashMap::new(),
        module_resources: HashMap::new(),
    };
    checker.collect(program);
    checker.check_program(program);
    checker.diagnostics
}

/// The output surface of one resource instance, captured for output-reference
/// validation without holding a borrow on the kind library.
#[derive(Clone)]
struct ResourceOutputs {
    kind: String,
    outputs: Vec<&'static str>,
}

struct TypeChecker<'a> {
    kinds: &'a KindLibrary,
    diagnostics: Vec<Diag>,
    /// Input name → declared sum-type name, for inputs typed as a known enum.
    input_enum: HashMap<String, String>,
    /// Input name → declared field type, for per-field value typing.
    input_type: HashMap<String, FieldType>,
    /// `let` name → inferred type, for per-field value typing.
    let_type: HashMap<String, FieldType>,
    /// Names bound to a secret-tainted value (a `Secret<…>` input, or a `let`
    /// whose value is tainted). A tainted value may flow only into a
    /// `Secret<_>` field (Requirement 12.3).
    tainted: HashSet<String>,
    /// Resource/service name → its kind's output surface (program-wide).
    resource_kind: HashMap<String, ResourceOutputs>,
    /// Module name → (resource/service name → output surface), for qualified
    /// `<module>.<resource>.<output>` references.
    module_resources: HashMap<String, HashMap<String, ResourceOutputs>>,
}

impl TypeChecker<'_> {
    // ── Collection ────────────────────────────────────────────────────

    fn collect(&mut self, program: &Program) {
        // Inputs first: their declared types seed the type environment that the
        // subsequent `let` inference and the field checks read.
        for item in &program.items {
            if let Item::Input(decl) = item {
                if let TypeExpr::Named(name) = &decl.ty.node
                    && enum_variants(name).is_some()
                {
                    self.input_enum.insert(decl.name.node.clone(), name.clone());
                }
                let ty = field_type_of(&decl.ty.node);
                if matches!(ty, FieldType::Secret(_)) {
                    self.tainted.insert(decl.name.node.clone());
                }
                self.input_type.insert(decl.name.node.clone(), ty);
            }
        }
        // Lets next, in source order: a `let` may read inputs and earlier lets.
        // A forward reference infers to `Any` (sound — never a false reject).
        for item in &program.items {
            if let Item::Let(decl) = item {
                let ty = self.infer(&decl.value);
                if self.is_tainted_expr(&decl.value) {
                    self.tainted.insert(decl.name.node.clone());
                }
                self.let_type.insert(decl.name.node.clone(), ty);
            }
        }
        for item in &program.items {
            if let Item::Module(module) = item {
                self.collect_module(module);
            }
        }
    }

    fn collect_module(&mut self, module: &ModuleDecl) {
        let mut local: HashMap<String, ResourceOutputs> = HashMap::new();
        for item in &module.items {
            self.collect_module_item(item, &mut local);
        }
        self.module_resources
            .insert(module.name.node.clone(), local);
    }

    fn collect_module_item(
        &mut self,
        item: &ModuleItem,
        local: &mut HashMap<String, ResourceOutputs>,
    ) {
        match item {
            ModuleItem::Resource(instance) | ModuleItem::Service(instance) => {
                if let Some(schema) = self.kinds.get(&instance.kind.node) {
                    let outputs = ResourceOutputs {
                        kind: schema.name.to_string(),
                        outputs: schema.outputs.clone(),
                    };
                    self.resource_kind
                        .insert(instance.name.node.clone(), outputs.clone());
                    local.insert(instance.name.node.clone(), outputs);
                }
            }
            ModuleItem::Match(block) => {
                for arm in &block.arms {
                    for inner in &arm.items {
                        self.collect_module_item(inner, local);
                    }
                }
            }
        }
    }

    // ── Checking ──────────────────────────────────────────────────────

    fn check_program(&mut self, program: &Program) {
        for item in &program.items {
            match item {
                Item::Input(decl) => {
                    if let (TypeExpr::Named(enum_name), Some(default)) =
                        (&decl.ty.node, &decl.default)
                        && let Some(variants) = enum_variants(enum_name)
                    {
                        self.check_variant_value(default, enum_name, variants);
                    }
                    if let Some(default) = &decl.default {
                        self.check_expr(default);
                    }
                }
                Item::Let(decl) => self.check_expr(&decl.value),
                Item::Image(instance) => self.check_kind_instance(instance),
                Item::Module(module) => self.check_module(module),
                Item::Writeback(decl) => {
                    if let Some(when) = &decl.when {
                        self.check_expr(when);
                    }
                    for target in &decl.targets {
                        self.check_expr(&target.value);
                    }
                }
                Item::Use(_) | Item::Namespaces(_) => {}
            }
        }
    }

    fn check_module(&mut self, module: &ModuleDecl) {
        if let Some(when) = &module.when {
            self.check_expr(when);
        }
        if let Some(depends_on) = &module.depends_on {
            self.check_expr(depends_on);
        }
        for item in &module.items {
            self.check_module_item(item);
        }
    }

    fn check_module_item(&mut self, item: &ModuleItem) {
        match item {
            ModuleItem::Resource(instance) | ModuleItem::Service(instance) => {
                self.check_kind_instance(instance)
            }
            ModuleItem::Match(block) => self.check_match_block(block),
        }
    }

    /// Check one kind instance: walk field values for nested diagnostics, then
    /// enforce per-field typing (4.3), secret taint (4.4), and the kind's
    /// constraints (4.5).
    fn check_kind_instance(&mut self, instance: &KindInstance) {
        for field in &instance.fields {
            self.check_expr(&field.value);
        }
        self.check_field_types(instance);
        self.check_constraints(instance);
    }

    fn check_field_types(&mut self, instance: &KindInstance) {
        let Some(schema) = self.kinds.get(&instance.kind.node) else {
            return; // unknown kind already reported by resolve
        };
        // Clone the small per-field type facts so the kind-library borrow does
        // not collide with the mutable diagnostics borrow below.
        let field_types: HashMap<&str, FieldType> = schema
            .fields
            .iter()
            .map(|spec| (spec.name, spec.ty.clone()))
            .collect();

        for field in &instance.fields {
            // `depends_on` carries bare dependency names, not values; resolve
            // handled it. Skip it here (its schema type is `Any` anyway).
            if field.key.node == "depends_on" {
                continue;
            }
            let Some(declared) = field_types.get(field.key.node.as_str()) else {
                continue; // unknown field already reported by resolve
            };
            let inferred = self.infer(&field.value);

            // Secret taint: a tainted value may sink only into a `Secret<_>`
            // field. Report names/spans only — never the value (Req 12.3).
            if matches!(inferred, FieldType::Secret(_)) && !matches!(declared, FieldType::Secret(_))
            {
                self.diagnostics.push(
                    Diag::error(
                        expr_span(&field.value),
                        format!(
                            "secret value cannot flow into non-secret field `{}`",
                            field.key.node
                        ),
                    )
                    .with_hint("a Secret<…> value may only be supplied to a secret-typed field"),
                );
                continue;
            }

            if !compatible(declared, &inferred) {
                self.diagnostics.push(Diag::error(
                    expr_span(&field.value),
                    format!(
                        "field `{}` expects {}, found {}",
                        field.key.node,
                        describe(declared),
                        describe(&inferred)
                    ),
                ));
            }
        }
    }

    fn check_constraints(&mut self, instance: &KindInstance) {
        let Some(schema) = self.kinds.get(&instance.kind.node) else {
            return;
        };
        let constraints = schema.constraints.clone();
        for constraint in &constraints {
            match constraint {
                Constraint::Range { field, min, max } => {
                    // Only checkable when the value is a statically-known
                    // integer literal; an input/output ref is skipped (sound).
                    if let Some(value) = self.field_expr(instance, field)
                        && let Some(n) = literal_int(value)
                        && (n < *min || n > *max)
                    {
                        self.diagnostics.push(Diag::error(
                            expr_span(value),
                            format!("field `{field}` must be in {min}..={max}, found {n}"),
                        ));
                    }
                }
                Constraint::PairedPresence { first, second } => {
                    let has_first = self.field_expr(instance, first).is_some();
                    let has_second = self.field_expr(instance, second).is_some();
                    if has_first != has_second {
                        self.diagnostics.push(
                            Diag::error(
                                instance.span.clone(),
                                format!(
                                    "fields `{first}` and `{second}` must be supplied together"
                                ),
                            )
                            .with_hint("supply both or neither"),
                        );
                    }
                }
            }
        }
    }

    fn field_expr<'i>(&self, instance: &'i KindInstance, name: &str) -> Option<&'i Expr> {
        instance
            .fields
            .iter()
            .find(|field| field.key.node == name)
            .map(|field| &field.value)
    }

    fn check_match_block(&mut self, block: &MatchBlock) {
        self.check_expr(&block.scrutinee);
        let variants = self.scrutinee_variants(&block.scrutinee);
        for arm in &block.arms {
            self.check_arm_variant(&arm.variant.node, &arm.variant.span, variants);
            for inner in &arm.items {
                self.check_module_item(inner);
            }
        }
    }

    fn check_match_expr(&mut self, match_expr: &MatchExpr) {
        self.check_expr(&match_expr.scrutinee);
        let variants = self.scrutinee_variants(&match_expr.scrutinee);
        for arm in &match_expr.arms {
            self.check_arm_variant(&arm.variant.node, &arm.variant.span, variants);
            self.check_expr(&arm.body);
        }
    }

    /// The legal variants of a match scrutinee, when it is an input of known
    /// sum type. `None` means the scrutinee type is not a known sum, so arm
    /// variants are not validated here.
    fn scrutinee_variants(&self, scrutinee: &Expr) -> Option<&'static [&'static str]> {
        match scrutinee {
            Expr::Ident(name) => self
                .input_enum
                .get(&name.node)
                .and_then(|enum_name| enum_variants(enum_name)),
            _ => None,
        }
    }

    fn check_arm_variant(
        &mut self,
        variant: &str,
        span: &crate::Span,
        variants: Option<&'static [&'static str]>,
    ) {
        // `_` is the wildcard arm and is always legal.
        if variant == "_" {
            return;
        }
        if let Some(variants) = variants
            && !variants.contains(&variant)
        {
            self.diagnostics.push(Diag::error(
                span.clone(),
                format!("`{variant}` is not a variant of the matched sum type"),
            ));
        }
    }

    fn check_variant_value(&mut self, expr: &Expr, enum_name: &str, variants: &[&str]) {
        if let Expr::Ident(name) = expr
            && !variants.contains(&name.node.as_str())
        {
            self.diagnostics.push(Diag::error(
                name.span.clone(),
                format!("unknown variant `{}` of enum `{enum_name}`", name.node),
            ));
        }
    }

    // ── Type inference (sound, not complete) ──────────────────────────

    /// Infer the [`FieldType`] of an expression. Returns [`FieldType::Any`] for
    /// anything it cannot determine confidently, so a valid program is never
    /// rejected (the checker only fires on a *known* mismatch).
    fn infer(&self, expr: &Expr) -> FieldType {
        match expr {
            Expr::Str(_) => FieldType::Str,
            Expr::Int(_) => FieldType::Int,
            Expr::Bool(_) => FieldType::Bool,
            Expr::Ident(name) => self.infer_ident(&name.node),
            Expr::Field(base, seg) => self.infer_field(base, &seg.node),
            Expr::List(items) => {
                let elem = unify_all(items.node.iter().map(|item| self.infer(item)));
                FieldType::List(Box::new(elem))
            }
            Expr::Record(record) => {
                if !record.spreads.is_empty() {
                    return FieldType::Record(Box::new(FieldType::Any));
                }
                let elem = unify_all(record.fields.iter().map(|field| self.infer(&field.value)));
                FieldType::Record(Box::new(elem))
            }
            // `++` concatenates lists; element type is left unconstrained.
            Expr::Concat(_, _) => FieldType::List(Box::new(FieldType::Any)),
            Expr::PathJoin(_, _) => FieldType::Path,
            Expr::Is(_, _) => FieldType::Bool,
            // A `match` value could unify its arms, but the conservative (sound)
            // choice is `Any`: arms commonly include payload-derived values.
            Expr::Match(_) => FieldType::Any,
            Expr::Call(call) => infer_builtin(&call.func.node),
        }
    }

    fn infer_ident(&self, name: &str) -> FieldType {
        if name == "ro" || name == "rw" {
            return FieldType::Str;
        }
        // UpperCamelCase is a variant constructor in value position; its type is
        // the sum, not a field type — leave it unconstrained.
        if name.chars().next().is_some_and(char::is_uppercase) {
            return FieldType::Any;
        }
        if self.tainted.contains(name) {
            // Surface taint through the type so a secret sink is detectable even
            // when the underlying type is otherwise known.
            let inner = self
                .input_type
                .get(name)
                .or_else(|| self.let_type.get(name))
                .cloned()
                .unwrap_or(FieldType::Any);
            return match inner {
                FieldType::Secret(_) => inner,
                other => FieldType::Secret(Box::new(other)),
            };
        }
        self.input_type
            .get(name)
            .or_else(|| self.let_type.get(name))
            .cloned()
            .unwrap_or(FieldType::Any)
    }

    fn infer_field(&self, base: &Expr, _segment: &str) -> FieldType {
        // `ctx.<field>` reads the closed runtime context with known leaf types.
        if let Expr::Ident(root) = base
            && root.node == "ctx"
        {
            return match _segment {
                "deployment_dir" | "home" => FieldType::Path,
                "region" => FieldType::Str,
                _ => FieldType::Any,
            };
        }
        // Record field access on a binding, or a resource output reference:
        // both are values we cannot type statically — leave unconstrained.
        FieldType::Any
    }

    /// Whether an expression is secret-tainted (used to propagate taint through
    /// a `let`). Conservative: only a direct tainted ident propagates.
    fn is_tainted_expr(&self, expr: &Expr) -> bool {
        matches!(self.infer(expr), FieldType::Secret(_))
    }

    // ── Expression walk + output references ───────────────────────────

    fn check_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Str(_) | Expr::Int(_) | Expr::Bool(_) | Expr::Ident(_) => {}
            Expr::Field(..) => self.check_field_chain(expr),
            Expr::List(items) => {
                for item in &items.node {
                    self.check_expr(item);
                }
            }
            Expr::Record(record) => {
                for spread in &record.spreads {
                    self.check_expr(spread);
                }
                for field in &record.fields {
                    self.check_expr(&field.value);
                }
            }
            Expr::Concat(left, right) | Expr::PathJoin(left, right) => {
                self.check_expr(left);
                self.check_expr(right);
            }
            Expr::Is(inner, _variant) => self.check_expr(inner),
            Expr::Match(match_expr) => self.check_match_expr(match_expr),
            Expr::Call(call) => {
                for arg in &call.args {
                    self.check_expr(arg);
                }
                for kwarg in &call.kwargs {
                    self.check_expr(&kwarg.value);
                }
            }
        }
    }

    /// Validate a `.`-chain. If it is an output reference rooted at a resource
    /// or module, check the resource exists and the output is declared
    /// (Property 21). `ctx.*` and chains rooted at a value binding are not
    /// output references and are left for inference.
    fn check_field_chain(&mut self, expr: &Expr) {
        let Some((root, segments)) = flatten(expr) else {
            if let Expr::Field(base, _) = expr {
                self.check_expr(base);
            }
            return;
        };

        if root.node == "ctx" {
            return;
        }

        if let Some(resource) = self.resource_kind.get(&root.node) {
            if let Some(output) = segments.first()
                && !resource.outputs.contains(&output.node.as_str())
            {
                self.diagnostics.push(Diag::error(
                    output.span.clone(),
                    format!(
                        "`{}` is not an output of kind `{}`",
                        output.node, resource.kind
                    ),
                ));
            }
            return;
        }

        if let Some(resources) = self.module_resources.get(&root.node)
            && segments.len() >= 2
        {
            let resource_name = &segments[0];
            let output = &segments[1];
            match resources.get(&resource_name.node) {
                Some(resource) => {
                    if !resource.outputs.contains(&output.node.as_str()) {
                        self.diagnostics.push(Diag::error(
                            output.span.clone(),
                            format!(
                                "`{}` is not an output of kind `{}`",
                                output.node, resource.kind
                            ),
                        ));
                    }
                }
                None => self.diagnostics.push(Diag::error(
                    resource_name.span.clone(),
                    format!(
                        "no resource `{}` in module `{}`",
                        resource_name.node, root.node
                    ),
                )),
            }
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────

/// Map a surface [`TypeExpr`] to the [`FieldType`] used for value typing.
fn field_type_of(ty: &TypeExpr) -> FieldType {
    match ty {
        TypeExpr::Named(name) => match name.as_str() {
            "String" => FieldType::Str,
            "Int" => FieldType::Int,
            "Bool" => FieldType::Bool,
            "Port" => FieldType::Port,
            "Path" => FieldType::Path,
            // The lexer does not yet emit `<`/`>`, so `Secret<T>` is written as a
            // bare `Secret` and recognised by name here (parser note in
            // `type_expr`); its payload type is unconstrained until the generic
            // syntax lands. This is what gives task 4.4 a surface form today.
            "Secret" => FieldType::Secret(Box::new(FieldType::Any)),
            // A sum type (`Storage`, `DsqlMode`) or any other named type is not
            // a field-value type; leave it unconstrained.
            _ => FieldType::Any,
        },
        TypeExpr::Secret(inner) => FieldType::Secret(Box::new(field_type_of(inner))),
        TypeExpr::Optional(inner) => field_type_of(inner),
        TypeExpr::List(inner) => FieldType::List(Box::new(field_type_of(inner))),
    }
}

/// The return type of a builtin call.
fn infer_builtin(name: &str) -> FieldType {
    match name {
        // port(n) → "n:n"; bind(src, dst, mode) → "src:dst[:ro]".
        "port" | "bind" => FieldType::Str,
        "present_only" => FieldType::Record(Box::new(FieldType::Any)),
        // value_from / generated_password / secret_read yield secret-bearing
        // material; treat them as tainted so they may sink only into a secret.
        "secret_read" | "generated_password" => FieldType::Secret(Box::new(FieldType::Str)),
        _ => FieldType::Any,
    }
}

/// Whether a value of inferred type `inferred` is acceptable where `declared` is
/// expected. Errs toward acceptance: `Any` on either side is always compatible,
/// so an un-inferable expression never produces a false rejection.
fn compatible(declared: &FieldType, inferred: &FieldType) -> bool {
    use FieldType::{Any, Bool, Int, List, Path, Port, Record, Secret, Str};
    match (declared, inferred) {
        (Any, _) | (_, Any) => true,
        (Str, Str) | (Int, Int) | (Bool, Bool) | (Path, Path) => true,
        // A port is an integer; a path or a string interoperate as text.
        (Port, Int) | (Port, Port) => true,
        (Path, Str) | (Str, Path) => true,
        (List(a), List(b)) | (Record(a), Record(b)) | (Secret(a), Secret(b)) => compatible(a, b),
        // A concrete value supplied to a secret field is acceptable (it declares
        // the need); the taint *direction* (secret → non-secret) is checked
        // separately in `check_field_types`.
        (Secret(a), other) => compatible(a, other),
        _ => false,
    }
}

/// Unify a sequence of inferred element types into one: a single shared type if
/// all agree, else [`FieldType::Any`].
fn unify_all(types: impl Iterator<Item = FieldType>) -> FieldType {
    let mut result: Option<FieldType> = None;
    for ty in types {
        match &result {
            None => result = Some(ty),
            Some(existing) if compatible(existing, &ty) && compatible(&ty, existing) => {}
            Some(_) => return FieldType::Any,
        }
    }
    result.unwrap_or(FieldType::Any)
}

/// A human-readable name for a field type, for diagnostics.
fn describe(ty: &FieldType) -> String {
    match ty {
        FieldType::Str => "a string".into(),
        FieldType::Int => "an integer".into(),
        FieldType::Bool => "a boolean".into(),
        FieldType::Port => "a port".into(),
        FieldType::Path => "a path".into(),
        FieldType::List(inner) => format!("a list of {}", describe(inner)),
        FieldType::Record(inner) => format!("a record of {}", describe(inner)),
        FieldType::Secret(inner) => format!("a secret {}", describe(inner)),
        FieldType::Any => "a value".into(),
    }
}

/// The integer value of a literal expression, if it is one.
fn literal_int(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Int(value) => Some(value.node),
        _ => None,
    }
}

/// Flatten an identifier-rooted `.`-chain into `(root, [seg, seg, …])`.
fn flatten(
    expr: &Expr,
) -> Option<(
    crate::ast::Spanned<String>,
    Vec<crate::ast::Spanned<String>>,
)> {
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
fn expr_span(expr: &Expr) -> crate::Span {
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
    use super::*;
    use crate::{lex, parser::parse};

    fn diagnostics(source: &str) -> Vec<Diag> {
        let (tokens, lex_diags) = lex(source);
        assert!(lex_diags.is_empty(), "lex errors: {lex_diags:?}");
        let (program, parse_diags) = parse(&tokens, source.len());
        assert!(parse_diags.is_empty(), "parse errors: {parse_diags:?}");
        typeck(&program.expect("program"), &KindLibrary::compose())
    }

    fn messages(diags: &[Diag]) -> Vec<&str> {
        diags.iter().map(|d| d.message.as_str()).collect()
    }

    #[test]
    fn valid_variants_and_output_refs_check_clean() {
        let diags = diagnostics(
            r#"platform compose {
                input storage: Storage = InMemory
                module dsql {
                    resource cluster = DsqlCluster { mode: "Managed", region: "us-east-1" }
                    resource role = DynamoDbTable { hash_key: "pk" }
                }
                module runtime {
                    match storage {
                        Dsql(d) => { resource c2 = DsqlCluster { mode: "Managed", region: "x" } }
                        InMemory => { }
                    }
                    service tokeirad = ComposeService {
                        image: "x",
                        env: { "ARN": cluster.cluster_arn },
                    }
                }
                writeback when storage is Dsql {
                    "infrastructure.dsql.endpoint" : dsql.cluster.cluster_endpoint,
                }
            }"#,
        );
        assert!(
            diags.is_empty(),
            "unexpected diagnostics: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn bad_input_default_variant_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                input storage: Storage = Managed
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("unknown variant `Managed` of enum `Storage`")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn bad_match_arm_variant_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                input storage: Storage = InMemory
                module m {
                    match storage {
                        Bogus => { }
                        _ => { }
                    }
                }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("`Bogus` is not a variant of the matched sum type")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn unknown_output_on_resource_ref_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                module m {
                    resource cluster = DsqlCluster { mode: "Managed", region: "x" }
                    service s = ComposeService { image: "x", env: { "A": cluster.nope } }
                }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("`nope` is not an output of kind `DsqlCluster`")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn unknown_resource_in_qualified_ref_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                module dsql {
                    resource cluster = DsqlCluster { mode: "Managed", region: "x" }
                }
                writeback {
                    "k" : dsql.ghost.cluster_endpoint,
                }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("no resource `ghost` in module `dsql`")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn ctx_access_is_not_treated_as_an_output_ref() {
        let diags = diagnostics(
            r#"platform compose {
                let dir = ctx.deployment_dir / ".state"
            }"#,
        );
        assert!(
            diags.is_empty(),
            "unexpected diagnostics: {:?}",
            messages(&diags)
        );
    }

    // ── 4.3 per-field value typing ────────────────────────────────────

    #[test]
    fn wrong_typed_field_value_is_reported() {
        // `image` expects a string; an integer is a type error (task 4.3).
        let diags = diagnostics(
            r#"platform compose {
                module m { service s = ComposeService { image: 7233 } }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("field `image` expects a string, found an integer")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn list_field_given_a_scalar_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                module m { service s = ComposeService { image: "x", ports: "9009:9009" } }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("field `ports` expects a list of a string")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn input_typed_value_flows_into_a_field_cleanly() {
        // An `Int` input into the `replicas` (Int) field type-checks; a `Port`
        // input into `metrics_target_port` (Port) too. No false positives.
        let diags = diagnostics(
            r#"platform compose {
                input replicas: Int = 2
                input mport: Port = 9090
                module m {
                    service s = ComposeService { image: "x", replicas: replicas }
                    resource oc = ObservabilityConfigFiles {
                        metrics_target_host: "tokeirad",
                        metrics_target_port: mport,
                    }
                }
            }"#,
        );
        assert!(
            diags.is_empty(),
            "unexpected diagnostics: {:?}",
            messages(&diags)
        );
    }

    // ── 4.4 secret taint ──────────────────────────────────────────────

    #[test]
    fn secret_value_into_non_secret_field_is_reported() {
        // A Secret<String> input may not flow into the plain-string `image`.
        let diags = diagnostics(
            r#"platform compose {
                input token: Secret = "seed"
                module m { service s = ComposeService { image: token } }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("secret value cannot flow into non-secret field `image`")),
            "got: {:?}",
            messages(&diags)
        );
        // The diagnostic must not echo the secret seed value.
        assert!(
            !messages(&diags).iter().any(|m| m.contains("seed")),
            "diagnostic leaked a secret value: {:?}",
            messages(&diags)
        );
    }

    // ── 4.5 validation-parity constraints ─────────────────────────────

    #[test]
    fn out_of_range_port_constant_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                module m {
                    resource oc = ObservabilityConfigFiles {
                        metrics_target_host: "tokeirad",
                        metrics_target_port: 70000,
                    }
                }
            }"#,
        );
        assert!(
            messages(&diags).iter().any(
                |m| m.contains("field `metrics_target_port` must be in 1..=65535, found 70000")
            ),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn match_arm_binding_value_does_not_false_positive() {
        // `d.region` (a sum payload access) infers to Any, so a Dsql arm that
        // feeds payload fields into typed fields must not be rejected (sound).
        let diags = diagnostics(
            r#"platform compose {
                input storage: Storage = InMemory
                module dsql {
                    match storage {
                        Dsql(d) => {
                            resource cluster = DsqlCluster { mode: d.mode, region: d.region }
                        }
                        _ => { }
                    }
                }
            }"#,
        );
        assert!(
            diags.is_empty(),
            "unexpected diagnostics: {:?}",
            messages(&diags)
        );
    }
}
