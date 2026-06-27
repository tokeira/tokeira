//! Name resolution and kind/field existence checks over an [`ast::Program`].
//!
//! This is the first half of the compile-time "resolve → type-check" phase. It
//! establishes the facts later phases and lowering depend on, and emits a
//! diagnostic for every violation (compilation must not proceed to lowering
//! while any error diagnostic is present — Property 6, enforced by the
//! pipeline):
//!
//! - **Unresolved names** — every `Ident` reference (the leftmost identifier of
//!   any expression, including the base of a `.` chain) must resolve to a
//!   binding: an `input`, `let`, `module`, `image`, `resource`, or `service`
//!   declaration, the runtime-context root `ctx`, or a match-arm payload
//!   binding in scope (Property 4).
//! - **Duplicate top-level declarations** — `input`/`let`/`module`/`image`
//!   names must be unique across the composed program (Requirement 13.5).
//! - **Kind and field existence** — every `resource`/`service`/`image`
//!   instantiates a kind in the [`KindLibrary`], supplies only fields in that
//!   kind's schema, and supplies every required field (Property 3 /
//!   Requirements 2.2, 2.3).
//!
//! Full type checking (sum types, `Secret<T>` taint, output-name validation,
//! validation parity) is the next slice and is intentionally not done here;
//! field *values* are walked only for name resolution, not yet type-checked.

use std::collections::{HashMap, HashSet};

use crate::{
    ast::{Expr, Item, KindInstance, ModuleDecl, ModuleItem, Program},
    diagnostic::Diag,
    kind::{KindLibrary, KindSchema},
};

/// The runtime-context root the composition reads typed fields from
/// (`ctx.deployment_dir`, …). Always in scope; never a provider (Requirement
/// 12.1 / 14.4).
const CONTEXT_ROOT: &str = "ctx";

/// The fixed set of builtin functions callable in expressions. Provider sources
/// (`env`) join this set when the context-declaration grammar lands.
const BUILTINS: &[&str] = &[
    "port",
    "bind",
    "present_only",
    "value_from",
    "secret_read",
    "generated_password",
];

/// Resolve names and check kind/field existence, returning all diagnostics.
///
/// An empty result means the program is name-resolved and every kind reference
/// is well-formed; it does **not** yet imply the program type-checks.
pub fn resolve(program: &Program, kinds: &KindLibrary) -> Vec<Diag> {
    let mut resolver = Resolver {
        kinds,
        names: HashSet::new(),
        scopes: Vec::new(),
        diagnostics: Vec::new(),
    };
    resolver.collect_names(program);
    resolver.resolve_program(program);
    resolver.diagnostics
}

struct Resolver<'a> {
    kinds: &'a KindLibrary,
    /// All value-binding names visible program-wide (declaration order is
    /// irrelevant; resolution is whole-program — design §Worked example).
    names: HashSet<String>,
    /// Match-arm payload bindings, innermost last.
    scopes: Vec<HashSet<String>>,
    diagnostics: Vec<Diag>,
}

impl Resolver<'_> {
    // ── Collection pass ───────────────────────────────────────────────

    /// Populate the global name set and flag duplicate top-level declarations.
    fn collect_names(&mut self, program: &Program) {
        self.names.insert(CONTEXT_ROOT.to_string());
        // Built-in mount-mode constants used by `bind(src, dst, ro|rw)`.
        self.names.insert("ro".to_string());
        self.names.insert("rw".to_string());
        let mut top_level: HashMap<String, ()> = HashMap::new();
        let mut note_top_level = |this: &mut Self, name: &str, span: &crate::Span| {
            if top_level.insert(name.to_string(), ()).is_some() {
                this.diagnostics.push(
                    Diag::error(span.clone(), format!("duplicate top-level declaration `{name}`"))
                        .with_hint("each input/let/module/image name must be unique across the deployment definition"),
                );
            }
            this.names.insert(name.to_string());
        };

        for item in &program.items {
            match item {
                Item::Input(decl) => note_top_level(self, &decl.name.node, &decl.name.span),
                Item::Let(decl) => note_top_level(self, &decl.name.node, &decl.name.span),
                Item::Image(instance) => {
                    note_top_level(self, &instance.name.node, &instance.name.span)
                }
                Item::Module(module) => {
                    note_top_level(self, &module.name.node, &module.name.span);
                    // Resource/service ids inside the module (and its match
                    // arms) are referenceable program-wide (depends_on, output
                    // refs), so they join the global name set.
                    self.collect_module_decl_names(module);
                }
                Item::Use(_) | Item::Namespaces(_) | Item::Writeback(_) => {}
            }
        }
    }

    fn collect_module_decl_names(&mut self, module: &ModuleDecl) {
        for item in &module.items {
            self.collect_module_item_names(item);
        }
    }

    fn collect_module_item_names(&mut self, item: &ModuleItem) {
        match item {
            ModuleItem::Resource(instance) | ModuleItem::Service(instance) => {
                self.names.insert(instance.name.node.clone());
            }
            ModuleItem::Match(block) => {
                for arm in &block.arms {
                    for inner in &arm.items {
                        self.collect_module_item_names(inner);
                    }
                }
            }
        }
    }

    // ── Resolution pass ───────────────────────────────────────────────

    fn resolve_program(&mut self, program: &Program) {
        for item in &program.items {
            match item {
                Item::Use(_) | Item::Namespaces(_) => {}
                Item::Input(decl) => {
                    if let Some(default) = &decl.default {
                        self.resolve_expr(default);
                    }
                }
                Item::Let(decl) => self.resolve_expr(&decl.value),
                Item::Image(instance) => self.resolve_kind_instance(instance),
                Item::Module(module) => self.resolve_module(module),
                Item::Writeback(decl) => {
                    if let Some(when) = &decl.when {
                        self.resolve_expr(when);
                    }
                    for target in &decl.targets {
                        self.resolve_expr(&target.value);
                    }
                }
            }
        }
    }

    fn resolve_module(&mut self, module: &ModuleDecl) {
        if let Some(when) = &module.when {
            self.resolve_expr(when);
        }
        if let Some(depends_on) = &module.depends_on {
            self.resolve_expr(depends_on);
        }
        for item in &module.items {
            self.resolve_module_item(item);
        }
    }

    fn resolve_module_item(&mut self, item: &ModuleItem) {
        match item {
            ModuleItem::Resource(instance) | ModuleItem::Service(instance) => {
                self.resolve_kind_instance(instance)
            }
            ModuleItem::Match(block) => {
                self.resolve_expr(&block.scrutinee);
                for arm in &block.arms {
                    self.push_arm_binding(arm.binding.as_ref().map(|b| b.node.clone()));
                    for inner in &arm.items {
                        self.resolve_module_item(inner);
                    }
                    self.scopes.pop();
                }
            }
        }
    }

    fn resolve_kind_instance(&mut self, instance: &KindInstance) {
        let schema = match self.kinds.get(&instance.kind.node) {
            Some(schema) => schema,
            None => {
                self.diagnostics.push(
                    Diag::error(
                        instance.kind.span.clone(),
                        format!("unknown kind `{}`", instance.kind.node),
                    )
                    .with_hint("the running tkp's kind library does not provide this kind"),
                );
                // Still resolve field values so unresolved names inside are
                // reported in the same pass (Requirement 6.2).
                for field in &instance.fields {
                    self.resolve_expr(&field.value);
                }
                return;
            }
        };
        // Clone the small schema facts we need so the borrow on `self.kinds`
        // does not conflict with the mutable diagnostics borrow below.
        let schema = KindFacts::from(schema);

        let mut seen: HashSet<&str> = HashSet::new();
        for field in &instance.fields {
            let key = field.key.node.as_str();
            if let Some(name) = schema.field_name(key) {
                seen.insert(name);
            } else {
                self.diagnostics.push(Diag::error(
                    field.key.span.clone(),
                    format!("unknown field `{key}` on kind `{}`", schema.name),
                ));
            }
            self.resolve_expr(&field.value);
        }
        for required in &schema.required {
            if !seen.contains(required) {
                self.diagnostics.push(
                    Diag::error(
                        instance.span.clone(),
                        format!(
                            "missing required field `{required}` on kind `{}`",
                            schema.name
                        ),
                    )
                    .with_hint("supply every required field of the kind"),
                );
            }
        }
    }

    // ── Expression walk ───────────────────────────────────────────────

    fn resolve_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Str(_) | Expr::Int(_) | Expr::Bool(_) => {}
            Expr::Ident(name) => {
                // UpperCamelCase identifiers in value position are enum-variant
                // / type constructors (e.g. `InMemory`, `Managed`); they are
                // validated against their declared sum type in the type-check
                // slice, not by name resolution. Only lower_snake bindings are
                // resolved here (Property 4).
                if !is_constructor(&name.node) && !self.is_bound(&name.node) {
                    self.diagnostics.push(Diag::error(
                        name.span.clone(),
                        format!("unresolved name `{}`", name.node),
                    ));
                }
            }
            // Only the base (leftmost identifier) is resolved here; the field /
            // output name is validated in the type-check/output-ref slice
            // (Requirement 15.3), so `ctx.deployment_dir` and
            // `dsql.cluster.cluster_arn` do not false-positive now.
            Expr::Field(base, _field) => self.resolve_expr(base),
            Expr::List(items) => {
                for item in &items.node {
                    self.resolve_expr(item);
                }
            }
            Expr::Record(record) => {
                for spread in &record.spreads {
                    self.resolve_expr(spread);
                }
                for field in &record.fields {
                    self.resolve_expr(&field.value);
                }
            }
            Expr::Concat(left, right) | Expr::PathJoin(left, right) => {
                self.resolve_expr(left);
                self.resolve_expr(right);
            }
            Expr::Is(inner, _variant) => self.resolve_expr(inner),
            Expr::Match(match_expr) => {
                self.resolve_expr(&match_expr.scrutinee);
                for arm in &match_expr.arms {
                    self.push_arm_binding(arm.binding.as_ref().map(|b| b.node.clone()));
                    self.resolve_expr(&arm.body);
                    self.scopes.pop();
                }
            }
            Expr::Call(call) => {
                if !BUILTINS.contains(&call.func.node.as_str()) {
                    self.diagnostics.push(
                        Diag::error(
                            call.func.span.clone(),
                            format!("unknown builtin `{}`", call.func.node),
                        )
                        .with_hint("calls are limited to the fixed builtin set"),
                    );
                }
                for arg in &call.args {
                    self.resolve_expr(arg);
                }
                for kwarg in &call.kwargs {
                    self.resolve_expr(&kwarg.value);
                }
            }
        }
    }

    fn push_arm_binding(&mut self, binding: Option<String>) {
        let mut scope = HashSet::new();
        if let Some(name) = binding {
            scope.insert(name);
        }
        self.scopes.push(scope);
    }

    fn is_bound(&self, name: &str) -> bool {
        self.scopes.iter().any(|scope| scope.contains(name)) || self.names.contains(name)
    }
}

/// Whether `name` is an enum-variant / type constructor by the language naming
/// convention (UpperCamelCase). Such names are resolved against their sum type
/// in the type-check phase, not by name resolution.
fn is_constructor(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_uppercase())
}

/// Owned copy of the schema facts the resolver needs, so the (shared) kind
/// borrow does not collide with the (mutable) diagnostics borrow.
struct KindFacts {
    name: &'static str,
    fields: Vec<&'static str>,
    required: Vec<&'static str>,
}

impl KindFacts {
    fn from(schema: &KindSchema) -> Self {
        Self {
            name: schema.name,
            fields: schema.fields.iter().map(|spec| spec.name).collect(),
            required: schema.required_fields().collect(),
        }
    }

    fn field_name(&self, key: &str) -> Option<&'static str> {
        self.fields.iter().copied().find(|name| *name == key)
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
        let program = program.expect("program");
        resolve(&program, &KindLibrary::compose())
    }

    fn messages(diags: &[Diag]) -> Vec<&str> {
        diags.iter().map(|d| d.message.as_str()).collect()
    }

    #[test]
    fn well_formed_program_resolves_clean() {
        let diags = diagnostics(
            r#"platform compose {
                input region: String = "us-east-1"
                let state_dir = ctx.deployment_dir / ".tokeira-state"
                module infra {
                    resource cluster = DsqlCluster { mode: "Managed", region: region }
                    resource role = DynamoDbTable { hash_key: "pk" }
                }
                module runtime {
                    depends_on [ infra ]
                    service tokeirad = ComposeService {
                        image: "tokeirad:latest",
                        ports: [ port(7233) ],
                        depends_on: [ cluster ],
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

    #[test]
    fn unresolved_name_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                let state_dir = ctx.deployment_dir / base_dir
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("unresolved name `base_dir`")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn duplicate_top_level_declaration_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                input region: String = "a"
                input region: String = "b"
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("duplicate top-level declaration `region`")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn unknown_kind_is_reported() {
        let diags = diagnostics(
            r#"platform compose {
                module m { resource x = NoSuchKind { } }
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("unknown kind `NoSuchKind`")),
            "got: {:?}",
            messages(&diags)
        );
    }

    #[test]
    fn unknown_and_missing_fields_are_reported() {
        let diags = diagnostics(
            r#"platform compose {
                module m { resource c = DsqlCluster { bogus: "x" } }
            }"#,
        );
        let msgs = messages(&diags);
        assert!(
            msgs.iter().any(|m| m.contains("unknown field `bogus`")),
            "got: {msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("missing required field `mode`")),
            "got: {msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("missing required field `region`")),
            "got: {msgs:?}"
        );
    }

    #[test]
    fn match_arm_binding_is_in_scope_for_the_arm() {
        let diags = diagnostics(
            r#"platform compose {
                input storage: Storage = InMemory
                module infra {
                    match storage {
                        Dsql(d) => { resource c = DsqlCluster { mode: "Managed", region: d } }
                        InMemory => { }
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

    #[test]
    fn match_arm_binding_does_not_leak_outside_the_arm() {
        let diags = diagnostics(
            r#"platform compose {
                input storage: Storage = InMemory
                let leaked = d
            }"#,
        );
        assert!(
            messages(&diags)
                .iter()
                .any(|m| m.contains("unresolved name `d`")),
            "got: {:?}",
            messages(&diags)
        );
    }
}
