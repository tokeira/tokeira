//! Untyped syntax tree for the platform DSL.
//!
//! This is the parser's output: a faithful, span-carrying representation of one
//! composed deployment definition, before name resolution or type checking. It
//! intentionally encodes *shape only* — references are unresolved `Ident`s,
//! kinds are unresolved names, and expressions are untyped. The resolve/
//! type-check phase turns this into the typed IR that lowers to a `Composition`.
//!
//! Every node carries a [`Span`] (directly or via [`Spanned`]) so any later
//! diagnostic points at exact source (Requirement 6.1).

use crate::token::Span;

/// A value paired with the source range it came from.
#[derive(Debug, Clone)]
pub struct Spanned<T> {
    /// The wrapped syntax node.
    pub node: T,
    /// Source range of `node`.
    pub span: Span,
}

impl<T> Spanned<T> {
    /// Wrap `node` with its `span`.
    pub fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }
}

/// One composed deployment definition.
///
/// After import assembly, all files' top-level items are unioned here under a
/// single `platform` name; duplicate top-level declarations across files are a
/// resolve-phase diagnostic (Requirement 13.5).
#[derive(Debug, Clone)]
pub struct Program {
    /// The declared platform name (e.g. `compose`); pins no version
    /// (Requirement 9 — the version is derived from the compiling `tkp`).
    pub platform: Spanned<String>,
    /// Top-level items in source order (order is not semantically significant;
    /// resolution is whole-program).
    pub items: Vec<Item>,
}

/// A top-level declaration.
#[derive(Debug, Clone)]
pub enum Item {
    /// `use "relative.platform"` — a contained import (resolved during assembly).
    Use(Spanned<String>),
    /// `input <name>: <Type> = <default?>`.
    Input(InputDecl),
    /// `let <name> = <expr>`.
    Let(LetDecl),
    /// `module <name> [when <cond>] { … }`.
    Module(ModuleDecl),
    /// `image <name> = <Kind> { … }`.
    Image(KindInstance),
    /// `namespaces [ "a", "b" ]`.
    Namespaces(Spanned<Vec<Spanned<String>>>),
    /// `writeback [when <cond>] { key = <output-ref>, … }`.
    Writeback(WritebackDecl),
}

/// An operator-tunable configuration input.
#[derive(Debug, Clone)]
pub struct InputDecl {
    /// Input name.
    pub name: Spanned<String>,
    /// Declared type expression (resolved later against the type system).
    pub ty: Spanned<TypeExpr>,
    /// Optional default value; a required input has none.
    pub default: Option<Expr>,
    /// Span of the whole declaration.
    pub span: Span,
}

/// A local binding.
#[derive(Debug, Clone)]
pub struct LetDecl {
    /// Binding name.
    pub name: Spanned<String>,
    /// Bound expression.
    pub value: Expr,
    /// Span of the whole declaration.
    pub span: Span,
}

/// A module grouping with optional conditional presence and dependencies.
#[derive(Debug, Clone)]
pub struct ModuleDecl {
    /// Module name (lexical ownership for its resources/services).
    pub name: Spanned<String>,
    /// `when <cond>` guard; the module is absent when the condition is false.
    pub when: Option<Expr>,
    /// `depends_on <expr>` — module-level dependency edges (may be conditional).
    pub depends_on: Option<Expr>,
    /// Resource and service declarations within the module.
    pub items: Vec<ModuleItem>,
    /// Span of the whole module.
    pub span: Span,
}

/// A declaration inside a module body.
#[derive(Debug, Clone)]
pub enum ModuleItem {
    /// `resource <id> = <Kind> { … }`.
    Resource(KindInstance),
    /// `service <id> = <Kind> { … }`.
    Service(KindInstance),
    /// A `match <scrutinee> { <Variant>(b?) => { … } … }` over module items,
    /// for conditional resource/service sets (e.g. DSQL managed vs preexisting).
    Match(MatchBlock),
}

/// An instantiation of a resource/service/image kind.
#[derive(Debug, Clone)]
pub struct KindInstance {
    /// The bound name (used as a `ResourceId` and for `depends_on` references).
    pub name: Spanned<String>,
    /// The kind name, resolved later against the kind library.
    pub kind: Spanned<String>,
    /// Field assignments (`key: expr`).
    pub fields: Vec<Field>,
    /// Span of the whole instance.
    pub span: Span,
}

/// A `key: value` field assignment.
#[derive(Debug, Clone)]
pub struct Field {
    /// Field key (a slot in the kind's schema, not a name reference).
    pub key: Spanned<String>,
    /// Field value expression.
    pub value: Expr,
}

/// A `match` over a scrutinee with variant arms.
#[derive(Debug, Clone)]
pub struct MatchBlock {
    /// The matched expression (typically a sum-typed input like `storage`).
    pub scrutinee: Expr,
    /// The arms, each a variant pattern and its body.
    pub arms: Vec<MatchArm>,
    /// Span of the whole match.
    pub span: Span,
}

/// One arm of a [`MatchBlock`].
#[derive(Debug, Clone)]
pub struct MatchArm {
    /// Variant name, or `_` for the wildcard arm.
    pub variant: Spanned<String>,
    /// Optional payload binding (`Dsql(d)` binds `d`).
    pub binding: Option<Spanned<String>>,
    /// Module items contributed when this arm is taken.
    pub items: Vec<ModuleItem>,
    /// Span of the whole arm.
    pub span: Span,
}

/// `writeback [when <cond>] { key = <expr>, … }`.
#[derive(Debug, Clone)]
pub struct WritebackDecl {
    /// Optional `when` guard.
    pub when: Option<Expr>,
    /// Dotted config key → source expression (usually an output reference).
    pub targets: Vec<Field>,
    /// Span of the whole declaration.
    pub span: Span,
}

/// A type expression in an `input` declaration.
///
/// Resolution against the type system (including sum types like `Storage`)
/// happens in the type-check phase; this is the surface form only.
#[derive(Debug, Clone)]
pub enum TypeExpr {
    /// A named type or enum (`String`, `Port`, `Path`, `Storage`, …).
    Named(String),
    /// `Secret<T>` — a tainted value (Requirement 12.3).
    Secret(Box<TypeExpr>),
    /// `T?` — an optional value.
    Optional(Box<TypeExpr>),
    /// `List<T>`.
    List(Box<TypeExpr>),
}

/// An expression. Untyped at this stage.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A string literal.
    Str(Spanned<String>),
    /// An integer literal.
    Int(Spanned<i64>),
    /// A boolean literal.
    Bool(Spanned<bool>),
    /// A bare name reference (binding, input, or declared resource/service).
    Ident(Spanned<String>),
    /// `expr.field` — field or output access (output refs become dependency
    /// edges at lowering, Requirement 15).
    Field(Box<Expr>, Spanned<String>),
    /// A list literal `[a, b, …]`.
    List(Spanned<Vec<Expr>>),
    /// A record literal `{ key: v, ..spread }`.
    Record(RecordExpr),
    /// `a ++ b` — list concatenation.
    Concat(Box<Expr>, Box<Expr>),
    /// `a / b` — path join.
    PathJoin(Box<Expr>, Box<Expr>),
    /// `<expr> is <Variant>` — a boolean test used in `when` conditions
    /// (e.g. `when storage is Dsql`).
    Is(Box<Expr>, Spanned<String>),
    /// A `match` expression yielding a value.
    Match(Box<MatchExpr>),
    /// A call to a builtin (`port`, `bind`, `value_from`, `secret_read`, …) or
    /// a provider source in a context declaration; resolved later.
    Call(CallExpr),
}

/// A record literal, possibly with leading `..spread` entries.
#[derive(Debug, Clone)]
pub struct RecordExpr {
    /// Spread sources merged (right-wins) before the explicit fields.
    pub spreads: Vec<Expr>,
    /// Explicit `key: value` entries.
    pub fields: Vec<Field>,
    /// Span of the whole record.
    pub span: Span,
}

/// A value-producing `match` (distinct from the module-item [`MatchBlock`]).
#[derive(Debug, Clone)]
pub struct MatchExpr {
    /// The matched expression.
    pub scrutinee: Expr,
    /// Arms producing a value each.
    pub arms: Vec<MatchExprArm>,
    /// Span of the whole match.
    pub span: Span,
}

/// One arm of a value [`MatchExpr`].
#[derive(Debug, Clone)]
pub struct MatchExprArm {
    /// Variant name or `_`.
    pub variant: Spanned<String>,
    /// Optional payload binding.
    pub binding: Option<Spanned<String>>,
    /// The value this arm produces.
    pub body: Expr,
    /// Span of the whole arm.
    pub span: Span,
}

/// A builtin/function-style call: `func(arg, key: arg, …)`.
#[derive(Debug, Clone)]
pub struct CallExpr {
    /// The callee name (a fixed builtin; resolved later).
    pub func: Spanned<String>,
    /// Positional arguments.
    pub args: Vec<Expr>,
    /// Keyword arguments (`key: expr`).
    pub kwargs: Vec<Field>,
    /// Span of the whole call.
    pub span: Span,
}
