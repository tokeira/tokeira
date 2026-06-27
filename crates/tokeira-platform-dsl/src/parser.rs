//! Recursive-descent parser: `(Token, Span)` stream → untyped [`ast::Program`].
//!
//! Deliberately hand-rolled rather than combinator-generated: the grammar is
//! small and fixed, recursive descent yields precise per-construct spans for
//! diagnostics (Requirement 6.1), and it carries no monomorphization or
//! dependency cost. The parser is pure and total — it consumes a finite token
//! slice and always terminates (Property 1).
//!
//! Error handling collects multiple diagnostics in one pass (Requirement 6.2):
//! on a malformed top-level item the parser records the error and synchronises
//! to the next item boundary rather than aborting. It returns the best-effort
//! `Program` alongside the diagnostics; callers MUST NOT lower when any error
//! diagnostic is present (Property 6 is enforced by the pipeline, not here).

use crate::{
    ast::{
        CallExpr, Expr, Field, InputDecl, Item, KindInstance, LetDecl, MatchArm, MatchBlock,
        MatchExpr, MatchExprArm, ModuleDecl, ModuleItem, Program, RecordExpr, Spanned, TypeExpr,
        WritebackDecl,
    },
    diagnostic::Diag,
    token::{Span, Token},
};

/// Parse a token slice into a best-effort [`Program`] plus diagnostics.
///
/// Returns `(Some(program), diags)` when a platform header was found (the
/// program may still carry errors in `diags`), or `(None, diags)` when the
/// input is not a platform definition at all.
pub fn parse(tokens: &[(Token, Span)], eof: usize) -> (Option<Program>, Vec<Diag>) {
    let mut parser = Parser {
        tokens,
        pos: 0,
        eof,
        diagnostics: Vec::new(),
    };
    let program = parser.program();
    (program, parser.diagnostics)
}

struct Parser<'a> {
    tokens: &'a [(Token, Span)],
    pos: usize,
    /// Byte offset just past the last token, used to span end-of-input errors.
    eof: usize,
    diagnostics: Vec<Diag>,
}

/// Internal control signal: a parse step failed and the caller should
/// synchronise. The diagnostic has already been recorded.
struct Bail;

type PResult<T> = Result<T, Bail>;

impl<'a> Parser<'a> {
    // ── Cursor primitives ─────────────────────────────────────────────

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(token, _)| token)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset).map(|(token, _)| token)
    }

    fn span_here(&self) -> Span {
        match self.tokens.get(self.pos) {
            Some((_, span)) => span.clone(),
            // At end of input, point a zero-width span at the end so the
            // operator still gets a location rather than a bare "somewhere".
            None => self.eof..self.eof,
        }
    }

    fn prev_span(&self) -> Span {
        let idx = self.pos.saturating_sub(1);
        self.tokens
            .get(idx)
            .map(|(_, span)| span.clone())
            .unwrap_or(self.eof..self.eof)
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: &Token, what: &str) -> PResult<Span> {
        if self.peek() == Some(expected) {
            let span = self.span_here();
            self.pos += 1;
            Ok(span)
        } else {
            Err(self.error_here(format!("expected {what}")))
        }
    }

    fn ident(&mut self, what: &str) -> PResult<Spanned<String>> {
        match self.tokens.get(self.pos) {
            Some((Token::Ident(name), span)) => {
                let result = Spanned::new(name.clone(), span.clone());
                self.pos += 1;
                Ok(result)
            }
            _ => Err(self.error_here(format!("expected {what}"))),
        }
    }

    fn string(&mut self, what: &str) -> PResult<Spanned<String>> {
        match self.tokens.get(self.pos) {
            Some((Token::Str(value), span)) => {
                let result = Spanned::new(value.clone(), span.clone());
                self.pos += 1;
                Ok(result)
            }
            _ => Err(self.error_here(format!("expected {what}"))),
        }
    }

    fn error_here(&mut self, message: impl Into<String>) -> Bail {
        let span = self.span_here();
        self.diagnostics.push(Diag::error(span, message));
        Bail
    }

    // ── Top level ─────────────────────────────────────────────────────

    fn program(&mut self) -> Option<Program> {
        if !self.eat(&Token::Platform) {
            self.error_here("a deployment definition must start with `platform <name> { … }`");
            return None;
        }
        let platform = match self.ident("a platform name after `platform`") {
            Ok(name) => name,
            Err(Bail) => return None,
        };
        if self
            .expect(&Token::LBrace, "`{` to open the platform body")
            .is_err()
        {
            return None;
        }

        let mut items = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            match self.item() {
                Ok(item) => items.push(item),
                Err(Bail) => self.synchronise(),
            }
        }
        let _ = self.expect(&Token::RBrace, "`}` to close the platform body");
        Some(Program { platform, items })
    }

    /// Skip tokens until the next item boundary so one bad item does not derail
    /// the rest of the definition (Requirement 6.2).
    fn synchronise(&mut self) {
        while let Some(token) = self.peek() {
            if matches!(
                token,
                Token::Use
                    | Token::Input
                    | Token::Let
                    | Token::Module
                    | Token::Image
                    | Token::Namespaces
                    | Token::Writeback
                    | Token::RBrace
            ) {
                return;
            }
            self.pos += 1;
        }
    }

    fn item(&mut self) -> PResult<Item> {
        match self.peek() {
            Some(Token::Use) => {
                self.pos += 1;
                let path = self.string("an import path string after `use`")?;
                Ok(Item::Use(path))
            }
            Some(Token::Input) => Ok(Item::Input(self.input_decl()?)),
            Some(Token::Let) => Ok(Item::Let(self.let_decl()?)),
            Some(Token::Module) => Ok(Item::Module(self.module_decl()?)),
            Some(Token::Image) => {
                self.pos += 1;
                Ok(Item::Image(self.kind_instance()?))
            }
            Some(Token::Namespaces) => {
                let start = self.span_here();
                self.pos += 1;
                let list = self.string_list()?;
                let span = start.start..self.prev_span().end;
                Ok(Item::Namespaces(Spanned::new(list, span)))
            }
            Some(Token::Writeback) => Ok(Item::Writeback(self.writeback_decl()?)),
            _ => Err(self.error_here(
                "expected an item: use / input / let / module / image / namespaces / writeback",
            )),
        }
    }

    fn input_decl(&mut self) -> PResult<InputDecl> {
        let start = self.span_here();
        self.pos += 1; // `input`
        let name = self.ident("an input name")?;
        self.expect(&Token::Colon, "`:` and a type for the input")?;
        let ty = self.type_expr()?;
        let default = if self.eat(&Token::Eq) {
            Some(self.expr()?)
        } else {
            None
        };
        let span = start.start..self.prev_span().end;
        Ok(InputDecl {
            name,
            ty,
            default,
            span,
        })
    }

    fn let_decl(&mut self) -> PResult<LetDecl> {
        let start = self.span_here();
        self.pos += 1; // `let`
        let name = self.ident("a binding name")?;
        self.expect(&Token::Eq, "`=` and a value for the binding")?;
        let value = self.expr()?;
        let span = start.start..self.prev_span().end;
        Ok(LetDecl { name, value, span })
    }

    fn module_decl(&mut self) -> PResult<ModuleDecl> {
        let start = self.span_here();
        self.pos += 1; // `module`
        let name = self.ident("a module name")?;
        let when = if self.eat(&Token::When) {
            Some(self.condition()?)
        } else {
            None
        };
        self.expect(&Token::LBrace, "`{` to open the module body")?;
        let mut depends_on = None;
        let mut items = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            if token == &Token::DependsOn {
                self.pos += 1;
                depends_on = Some(self.expr()?);
                self.eat(&Token::Comma);
                continue;
            }
            items.push(self.module_item()?);
        }
        self.expect(&Token::RBrace, "`}` to close the module body")?;
        let span = start.start..self.prev_span().end;
        Ok(ModuleDecl {
            name,
            when,
            depends_on,
            items,
            span,
        })
    }

    fn module_item(&mut self) -> PResult<ModuleItem> {
        match self.peek() {
            Some(Token::Resource) => {
                self.pos += 1;
                Ok(ModuleItem::Resource(self.kind_instance()?))
            }
            Some(Token::Service) => {
                self.pos += 1;
                Ok(ModuleItem::Service(self.kind_instance()?))
            }
            Some(Token::Match) => Ok(ModuleItem::Match(self.match_block()?)),
            _ => Err(self.error_here("expected `resource`, `service`, or `match` in a module")),
        }
    }

    /// `<name> = <Kind> { fields }` (the leading keyword is already consumed).
    fn kind_instance(&mut self) -> PResult<KindInstance> {
        let name = self.ident("a declaration name")?;
        self.expect(&Token::Eq, "`=` and a kind name")?;
        let kind = self.ident("a kind name")?;
        let fields = self.field_block()?;
        let span = name.span.start..self.prev_span().end;
        Ok(KindInstance {
            name,
            kind,
            fields,
            span,
        })
    }

    fn match_block(&mut self) -> PResult<MatchBlock> {
        let start = self.span_here();
        self.pos += 1; // `match`
        let scrutinee = self.expr()?;
        self.expect(&Token::LBrace, "`{` to open the match arms")?;
        let mut arms = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            arms.push(self.match_block_arm()?);
        }
        self.expect(&Token::RBrace, "`}` to close the match")?;
        let span = start.start..self.prev_span().end;
        Ok(MatchBlock {
            scrutinee,
            arms,
            span,
        })
    }

    fn match_block_arm(&mut self) -> PResult<MatchArm> {
        let (variant, binding, start) = self.match_pattern()?;
        self.expect(&Token::FatArrow, "`=>` after a match pattern")?;
        self.expect(&Token::LBrace, "`{` to open the arm body")?;
        let mut items = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            items.push(self.module_item()?);
        }
        self.expect(&Token::RBrace, "`}` to close the arm body")?;
        self.eat(&Token::Comma);
        let span = start..self.prev_span().end;
        Ok(MatchArm {
            variant,
            binding,
            items,
            span,
        })
    }

    /// A match pattern: `Variant`, `Variant(binding)`, or `_`. Returns the
    /// variant name, optional binding, and the pattern's start offset.
    fn match_pattern(&mut self) -> PResult<(Spanned<String>, Option<Spanned<String>>, usize)> {
        let variant = self.ident("a variant name or `_`")?;
        let start = variant.span.start;
        let binding = if self.eat(&Token::LParen) {
            let bound = self.ident("a payload binding name")?;
            self.expect(&Token::RParen, "`)` after the payload binding")?;
            Some(bound)
        } else {
            None
        };
        Ok((variant, binding, start))
    }

    fn writeback_decl(&mut self) -> PResult<WritebackDecl> {
        let start = self.span_here();
        self.pos += 1; // `writeback`
        let when = if self.eat(&Token::When) {
            Some(self.condition()?)
        } else {
            None
        };
        let targets = self.field_block()?;
        let span = start.start..self.prev_span().end;
        Ok(WritebackDecl {
            when,
            targets,
            span,
        })
    }

    // ── Fields and lists ──────────────────────────────────────────────

    /// `{ key: value, key: value, }` — trailing comma allowed.
    fn field_block(&mut self) -> PResult<Vec<Field>> {
        self.expect(&Token::LBrace, "`{` to open a field block")?;
        let mut fields = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            fields.push(self.field()?);
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RBrace, "`}` to close a field block")?;
        Ok(fields)
    }

    fn field(&mut self) -> PResult<Field> {
        let key = self.field_key()?;
        self.expect(&Token::Colon, "`:` after a field key")?;
        let value = self.expr()?;
        Ok(Field { key, value })
    }

    /// A field key: an identifier, a string literal, or a keyword used
    /// contextually as a key. Kind schemas have field names like `image:` that
    /// collide with reserved keywords; in key position the keyword is accepted
    /// as its lexeme so `image:`/`module:`/etc. are valid field names.
    fn field_key(&mut self) -> PResult<Spanned<String>> {
        match self.tokens.get(self.pos) {
            Some((Token::Ident(name), span)) => {
                let result = Spanned::new(name.clone(), span.clone());
                self.pos += 1;
                Ok(result)
            }
            Some((Token::Str(value), span)) => {
                let result = Spanned::new(value.clone(), span.clone());
                self.pos += 1;
                Ok(result)
            }
            Some((token, span)) if keyword_lexeme(token).is_some() => {
                let result = Spanned::new(keyword_lexeme(token).unwrap().to_string(), span.clone());
                self.pos += 1;
                Ok(result)
            }
            _ => Err(self.error_here("expected a field key (identifier or string)")),
        }
    }

    fn string_list(&mut self) -> PResult<Vec<Spanned<String>>> {
        self.expect(&Token::LBracket, "`[` to open a list")?;
        let mut items = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBracket {
                break;
            }
            items.push(self.string("a string literal")?);
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RBracket, "`]` to close a list")?;
        Ok(items)
    }

    // ── Types ─────────────────────────────────────────────────────────

    fn type_expr(&mut self) -> PResult<Spanned<TypeExpr>> {
        // The lexer does not yet emit `<` / `>` / `?` tokens, so the type
        // grammar currently accepts only bare named types (`String`, `Port`,
        // `Path`, `Storage`, …). `Secret<T>`, `List<T>`, and `T?` are recognised
        // by name in the type-check phase; the surface generic/optional syntax
        // lands when the lexer grows those tokens.
        let name = self.ident("a type name")?;
        let span = name.span.clone();
        Ok(Spanned::new(TypeExpr::Named(name.node), span))
    }

    // ── Expressions ───────────────────────────────────────────────────

    /// A `when`/`writeback` condition: an expression optionally followed by
    /// `is <Variant>` (e.g. `storage is Dsql`).
    fn condition(&mut self) -> PResult<Expr> {
        let expr = self.expr()?;
        if self.eat(&Token::Is) {
            let variant = self.ident("a variant name after `is`")?;
            Ok(Expr::Is(Box::new(expr), variant))
        } else {
            Ok(expr)
        }
    }

    /// Lowest precedence: list concatenation `a ++ b`.
    fn expr(&mut self) -> PResult<Expr> {
        let mut left = self.join_expr()?;
        while self.eat(&Token::PlusPlus) {
            let right = self.join_expr()?;
            left = Expr::Concat(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// Path join `a / b`, tighter than `++`.
    fn join_expr(&mut self) -> PResult<Expr> {
        let mut left = self.postfix_expr()?;
        while self.eat(&Token::Slash) {
            let right = self.postfix_expr()?;
            left = Expr::PathJoin(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// Postfix field/output access `a.b.c`, tightest.
    fn postfix_expr(&mut self) -> PResult<Expr> {
        let mut expr = self.primary_expr()?;
        while self.eat(&Token::Dot) {
            let field = self.ident("a field or output name after `.`")?;
            expr = Expr::Field(Box::new(expr), field);
        }
        Ok(expr)
    }

    fn primary_expr(&mut self) -> PResult<Expr> {
        match self.peek() {
            Some(Token::Str(_)) => {
                let value = self.string("a string")?;
                Ok(Expr::Str(value))
            }
            Some(Token::Int(_)) => {
                if let Some((Token::Int(value), span)) = self.tokens.get(self.pos).cloned() {
                    self.pos += 1;
                    Ok(Expr::Int(Spanned::new(value, span)))
                } else {
                    Err(self.error_here("expected an integer"))
                }
            }
            Some(Token::True) => {
                let span = self.span_here();
                self.pos += 1;
                Ok(Expr::Bool(Spanned::new(true, span)))
            }
            Some(Token::False) => {
                let span = self.span_here();
                self.pos += 1;
                Ok(Expr::Bool(Spanned::new(false, span)))
            }
            Some(Token::LBracket) => self.list_expr(),
            Some(Token::LBrace) => self.record_expr(),
            Some(Token::Match) => Ok(Expr::Match(Box::new(self.match_expr()?))),
            Some(Token::Ident(_)) => {
                let name = self.ident("an identifier")?;
                // A call if directly followed by `(`; otherwise a name reference.
                if self.peek() == Some(&Token::LParen) {
                    self.call_expr(name)
                } else {
                    Ok(Expr::Ident(name))
                }
            }
            _ => Err(self.error_here("expected a value")),
        }
    }

    fn list_expr(&mut self) -> PResult<Expr> {
        let start = self.span_here();
        self.expect(&Token::LBracket, "`[`")?;
        let mut items = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBracket {
                break;
            }
            items.push(self.expr()?);
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RBracket, "`]` to close a list")?;
        let span = start.start..self.prev_span().end;
        Ok(Expr::List(Spanned::new(items, span)))
    }

    fn record_expr(&mut self) -> PResult<Expr> {
        let start = self.span_here();
        self.expect(&Token::LBrace, "`{`")?;
        let mut spreads = Vec::new();
        let mut fields = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            if self.eat(&Token::DotDot) {
                spreads.push(self.expr()?);
            } else {
                fields.push(self.field()?);
            }
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RBrace, "`}` to close a record")?;
        let span = start.start..self.prev_span().end;
        Ok(Expr::Record(RecordExpr {
            spreads,
            fields,
            span,
        }))
    }

    fn match_expr(&mut self) -> PResult<MatchExpr> {
        let start = self.span_here();
        self.pos += 1; // `match`
        let scrutinee = self.expr()?;
        self.expect(&Token::LBrace, "`{` to open the match arms")?;
        let mut arms = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RBrace {
                break;
            }
            let (variant, binding, arm_start) = self.match_pattern()?;
            self.expect(&Token::FatArrow, "`=>` after a match pattern")?;
            let body = self.expr()?;
            self.eat(&Token::Comma);
            let span = arm_start..self.prev_span().end;
            arms.push(MatchExprArm {
                variant,
                binding,
                body,
                span,
            });
        }
        self.expect(&Token::RBrace, "`}` to close the match")?;
        let span = start.start..self.prev_span().end;
        Ok(MatchExpr {
            scrutinee,
            arms,
            span,
        })
    }

    fn call_expr(&mut self, func: Spanned<String>) -> PResult<Expr> {
        let start = func.span.start;
        self.expect(&Token::LParen, "`(`")?;
        let mut args = Vec::new();
        let mut kwargs = Vec::new();
        while let Some(token) = self.peek() {
            if token == &Token::RParen {
                break;
            }
            // A keyword argument is `ident:` followed by a value; anything else
            // is positional. Two-token lookahead distinguishes them.
            if matches!(self.peek(), Some(Token::Ident(_)))
                && self.peek_at(1) == Some(&Token::Colon)
            {
                let key = self.ident("a keyword argument name")?;
                self.expect(&Token::Colon, "`:` after a keyword argument name")?;
                let value = self.expr()?;
                kwargs.push(Field { key, value });
            } else {
                args.push(self.expr()?);
            }
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen, "`)` to close a call")?;
        let span = start..self.prev_span().end;
        Ok(Expr::Call(CallExpr {
            func,
            args,
            kwargs,
            span,
        }))
    }
}

/// The lexeme of a keyword token, or `None` for non-keyword tokens.
///
/// Used to accept reserved words as contextual field keys (e.g. `image:` in a
/// `ComposeService`), where treating them as keywords would be wrong.
fn keyword_lexeme(token: &Token) -> Option<&'static str> {
    Some(match token {
        Token::Platform => "platform",
        Token::Use => "use",
        Token::Input => "input",
        Token::Let => "let",
        Token::Module => "module",
        Token::When => "when",
        Token::Match => "match",
        Token::Is => "is",
        Token::Namespaces => "namespaces",
        Token::Writeback => "writeback",
        Token::Service => "service",
        Token::Resource => "resource",
        Token::Image => "image",
        Token::DependsOn => "depends_on",
        Token::True => "true",
        Token::False => "false",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lex;

    fn parse_str(source: &str) -> (Option<Program>, Vec<Diag>) {
        let (tokens, lex_diags) = lex(source);
        assert!(lex_diags.is_empty(), "lex errors: {lex_diags:?}");
        parse(&tokens, source.len())
    }

    #[test]
    fn parses_platform_header_and_use_imports() {
        let (program, diags) = parse_str(
            r#"platform compose {
                use "runtime.platform"
                use "observability.platform"
            }"#,
        );
        assert!(diags.is_empty(), "diags: {diags:?}");
        let program = program.expect("program");
        assert_eq!(program.platform.node, "compose");
        assert_eq!(program.items.len(), 2);
        assert!(matches!(&program.items[0], Item::Use(p) if p.node == "runtime.platform"));
    }

    #[test]
    fn parses_inputs_lets_and_namespaces() {
        let (program, diags) = parse_str(
            r#"platform compose {
                input project_name: String = "tokeira"
                input grpc_port: Port = 7233
                let state_dir = ctx.deployment_dir / ".tokeira-state"
                namespaces [ "default" ]
            }"#,
        );
        assert!(diags.is_empty(), "diags: {diags:?}");
        let program = program.expect("program");
        assert_eq!(program.items.len(), 4);
        assert!(matches!(&program.items[0], Item::Input(d) if d.name.node == "project_name"));
        assert!(matches!(&program.items[2], Item::Let(d) if d.name.node == "state_dir"));
    }

    #[test]
    fn parses_module_with_conditional_presence_and_match() {
        let (program, diags) = parse_str(
            r#"platform compose {
                module dsql when storage is Dsql {
                    depends_on [ local_state ]
                    resource cluster = DsqlCluster { region: ctx.region }
                    resource role = DsqlIamRole { cluster_arn: cluster.cluster_arn }
                }
                module runtime {
                    depends_on match storage { Dsql(_) => [ observability ], _ => [ local_state ] }
                    service tokeirad = ComposeService {
                        image: tokeirad_image,
                        aws_auth: match storage { Dsql(_) => true, _ => false },
                        ports: [ port(grpc_port) ],
                        env: { "K": "v", ..present_only({ "A": ctx.region }) },
                    }
                }
            }"#,
        );
        assert!(diags.is_empty(), "diags: {diags:?}");
        let program = program.expect("program");
        assert_eq!(program.items.len(), 2);
        let Item::Module(dsql) = &program.items[0] else {
            panic!("expected module");
        };
        assert_eq!(dsql.name.node, "dsql");
        assert!(dsql.when.is_some(), "dsql module should be conditional");
        assert!(dsql.depends_on.is_some());
        // cluster + role resources
        assert_eq!(dsql.items.len(), 2);
    }

    #[test]
    fn parses_image_declarations_and_writeback() {
        let (program, diags) = parse_str(
            r#"platform compose {
                image tokeirad = Build { repository: "tokeira/tokeirad" }
                image mimir = Mirror { repository: "tokeira/grafana-mimir", upstream: mimir_image }
                writeback when storage is Dsql {
                    "infrastructure.dsql.endpoint" : dsql.cluster.cluster_endpoint,
                }
            }"#,
        );
        assert!(diags.is_empty(), "diags: {diags:?}");
        let program = program.expect("program");
        assert_eq!(program.items.len(), 3);
        assert!(matches!(&program.items[0], Item::Image(k) if k.kind.node == "Build"));
        assert!(matches!(&program.items[2], Item::Writeback(w) if w.when.is_some()));
    }

    #[test]
    fn missing_platform_header_is_a_diagnostic() {
        let (program, diags) = parse_str(r#"module runtime { }"#);
        assert!(program.is_none());
        assert!(!diags.is_empty());
    }

    #[test]
    fn recovers_from_a_bad_item_and_continues() {
        // The `let` is malformed (no `=`), but the following input still parses.
        let (program, diags) = parse_str(
            r#"platform compose {
                let broken
                input project_name: String = "tokeira"
            }"#,
        );
        let program = program.expect("program");
        assert!(!diags.is_empty(), "expected a diagnostic for the bad let");
        assert!(
            program
                .items
                .iter()
                .any(|item| matches!(item, Item::Input(d) if d.name.node == "project_name")),
            "parser should recover and parse the following input"
        );
    }
}
