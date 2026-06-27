//! Type checking: sum-variant validation and output-reference validation.
//!
//! This is the second half of the compile-time "resolve → type-check" phase. It
//! runs after [`crate::resolve`] and enforces the two DSL-specific type rules
//! the correctness properties most depend on:
//!
//! - **Sum-variant validation** (Property 5) — a variant used in value position
//!   (an `input` default) or as a `match` arm must be a variant of the relevant
//!   sum type. This is what makes the typed conditional (`storage` /
//!   `dsql` managed-vs-preexisting) a *type* obligation rather than a runtime
//!   guard, and it underpins the validation-parity story.
//! - **Output-reference validation** (Property 21 / Requirement 15.3) — a
//!   reference `<resource>.<output>` or `<module>.<resource>.<output>` must name
//!   a resource that exists and an output its kind declares. The reference's
//!   value is resolved by the engine at apply; here we only check it is
//!   well-formed.
//!
//! Deliberately **not** yet done (next slice): per-field value typing (String
//! vs Int vs Port …), `Secret<T>` taint flow (Property 16), and the
//! kind-specific validation-parity constraints (canonical ports, cpu/memory).
//! Those require every kind field to carry a [`crate::kind::Type`]-like schema,
//! a kind-library expansion tracked separately.

use std::collections::HashMap;

use crate::{
    ast::{Expr, Item, KindInstance, MatchBlock, MatchExpr, ModuleDecl, ModuleItem, Program},
    diagnostic::Diag,
    kind::KindLibrary,
};

/// The built-in sum types of the language, as `(name, variants)`.
///
/// User-declared sums are a later addition; today the sums are fixed by the
/// language (`Storage`, `DsqlMode`) and the kind library. Variant payloads are
/// not modelled here — only the legal variant names per sum.
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
        diagnostics: Vec::new(),
        input_enum: HashMap::new(),
        resource_kind: HashMap::new(),
        module_resources: HashMap::new(),
    };
    checker.collect(program, kinds);
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

struct TypeChecker {
    diagnostics: Vec<Diag>,
    /// Input name → declared sum-type name, for inputs typed as a known enum.
    input_enum: HashMap<String, String>,
    /// Resource/service name → its kind's output surface (program-wide).
    resource_kind: HashMap<String, ResourceOutputs>,
    /// Module name → (resource/service name → output surface), for qualified
    /// `<module>.<resource>.<output>` references.
    module_resources: HashMap<String, HashMap<String, ResourceOutputs>>,
}

impl TypeChecker {
    // ── Collection ────────────────────────────────────────────────────

    fn collect(&mut self, program: &Program, kinds: &KindLibrary) {
        for item in &program.items {
            match item {
                Item::Input(decl) => {
                    if let crate::ast::TypeExpr::Named(name) = &decl.ty.node
                        && enum_variants(name).is_some()
                    {
                        self.input_enum.insert(decl.name.node.clone(), name.clone());
                    }
                }
                Item::Module(module) => self.collect_module(module, kinds),
                _ => {}
            }
        }
    }

    fn collect_module(&mut self, module: &ModuleDecl, kinds: &KindLibrary) {
        let mut local: HashMap<String, ResourceOutputs> = HashMap::new();
        for item in &module.items {
            self.collect_module_item(item, kinds, &mut local);
        }
        self.module_resources
            .insert(module.name.node.clone(), local);
    }

    fn collect_module_item(
        &mut self,
        item: &ModuleItem,
        kinds: &KindLibrary,
        local: &mut HashMap<String, ResourceOutputs>,
    ) {
        match item {
            ModuleItem::Resource(instance) | ModuleItem::Service(instance) => {
                if let Some(schema) = kinds.get(&instance.kind.node) {
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
                        self.collect_module_item(inner, kinds, local);
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
                    if let (crate::ast::TypeExpr::Named(enum_name), Some(default)) =
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

    fn check_kind_instance(&mut self, instance: &KindInstance) {
        for field in &instance.fields {
            self.check_expr(&field.value);
        }
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
    /// (Property 21). `ctx.*` (RuntimeContext) and chains rooted at a value
    /// binding (record field access) are not output references and are left for
    /// the next slice; chains that do not flatten to an identifier root recurse
    /// into their base.
    fn check_field_chain(&mut self, expr: &Expr) {
        let Some((root, segments)) = flatten(expr) else {
            // Not an identifier-rooted chain (e.g. `match(...).x`); check the
            // inner expression instead.
            if let Expr::Field(base, _) = expr {
                self.check_expr(base);
            }
            return;
        };

        if root.node == "ctx" {
            return; // RuntimeContext access; validated in a later slice.
        }

        // `<resource>.<output>`
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

        // `<module>.<resource>.<output>`
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
        // Otherwise the root is an input/let/binding: record field access,
        // deferred to the per-field typing slice.
    }
}

/// Flatten an identifier-rooted `.`-chain into `(root, [seg, seg, …])`.
/// Returns `None` if the base is not ultimately a bare identifier.
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
}
