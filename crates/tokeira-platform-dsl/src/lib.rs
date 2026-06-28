//! The platform configuration DSL compiler.
//!
//! This crate is the embedded compiler for the platform DSL specified in
//! `.kiro/specs/platform-config-dsl/`. It sits on the **engine surface** (part
//! of `source_tree_hash`) and turns a deployment definition — a rooted set of
//! `.platform` files — into a `Composition` the IaC/deploy engines consume.
//!
//! The pipeline has two phases, kept strictly separate (design §Overview):
//!
//! 1. **Compile** (pure, total, deterministic): assemble imports → lex → parse →
//!    resolve → type-check → lower to a typed program. No I/O into the language.
//! 2. **Execute** (in `tkp`, with an injected `RuntimeContext`): evaluate the
//!    program to a `Composition`. Effects live only at the host edge.
//!
//! Compilation does no environment, network, clock, or arbitrary-filesystem
//! access; the only files read are those inside the deployment root, and import
//! resolution is fail-closed (Requirements 4, 12, 13).
//!
//! ## Phases present
//!
//! [`assemble`] (multi-file `use` import assembly with containment), the
//! [`token`] lexer, the recursive-descent [`parser`], the [`resolve`]r, the
//! [`typeck`] checker, the [`eval`]uator, and the neutral [`value::Composition`]
//! IR. [`compose_program`] composes an assembled [`SourceSet`] into one program
//! for the resolve→type-check→evaluate phases.

pub mod assemble;
pub mod ast;
pub mod diagnostic;
pub mod eval;
pub mod kind;
pub mod parser;
pub mod resolve;
pub mod token;
pub mod typeck;
pub mod value;

pub use assemble::{AssemblyError, Bounds, SourceFile, SourceSet, assemble, compose_program};
pub use diagnostic::{Diag, Severity};
pub use eval::{evaluate, evaluate_with_inputs};
pub use kind::{KindCategory, KindLibrary, KindSchema};
pub use parser::parse;
pub use resolve::resolve;
pub use token::{Span, Token};
pub use typeck::typeck;
pub use value::{Composition, ItemRole, RuntimeContext, Value};

use logos::Logos;

/// Lex one source string into `(Token, Span)` pairs.
///
/// Returns the successfully-lexed tokens plus a diagnostic for every span that
/// failed to lex. Lexing never aborts on the first error — it collects all of
/// them so the parser/reporter can surface as many problems as possible in one
/// pass (Requirement 6.2). This function performs no I/O and always terminates
/// (Property 1).
pub fn lex(source: &str) -> (Vec<(Token, Span)>, Vec<Diag>) {
    let mut tokens = Vec::new();
    let mut diagnostics = Vec::new();
    for (result, span) in Token::lexer(source).spanned() {
        match result {
            Ok(token) => tokens.push((token, span)),
            // `logos` reports an unrecognised byte run as a lex error; we point
            // a diagnostic at its span and keep going rather than failing closed
            // mid-stream, so multiple lex errors are reported together.
            Err(()) => diagnostics.push(
                Diag::error(span, "unexpected character(s) in deployment definition")
                    .with_hint("the platform DSL accepts identifiers, strings, integers, and the documented operators"),
            ),
        }
    }
    (tokens, diagnostics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexes_keywords_idents_and_operators() {
        let (tokens, diags) = lex("platform compose { use \"runtime.platform\" }");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:?}");
        let kinds: Vec<Token> = tokens.into_iter().map(|(token, _)| token).collect();
        assert_eq!(
            kinds,
            vec![
                Token::Platform,
                Token::Ident("compose".into()),
                Token::LBrace,
                Token::Use,
                Token::Str("runtime.platform".into()),
                Token::RBrace,
            ]
        );
    }

    #[test]
    fn longest_match_disambiguates_overlapping_operators() {
        // `++` must not lex as two tokens; `..` must beat `.`; `=>` must beat `=`.
        let (tokens, diags) = lex("a ++ b .. c => d . e");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:?}");
        let kinds: Vec<Token> = tokens.into_iter().map(|(token, _)| token).collect();
        assert_eq!(
            kinds,
            vec![
                Token::Ident("a".into()),
                Token::PlusPlus,
                Token::Ident("b".into()),
                Token::DotDot,
                Token::Ident("c".into()),
                Token::FatArrow,
                Token::Ident("d".into()),
                Token::Dot,
                Token::Ident("e".into()),
            ]
        );
    }

    #[test]
    fn skips_whitespace_and_line_comments() {
        let (tokens, diags) = lex("// a comment\n  module   runtime // trailing\n");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:?}");
        let kinds: Vec<Token> = tokens.into_iter().map(|(token, _)| token).collect();
        assert_eq!(kinds, vec![Token::Module, Token::Ident("runtime".into())]);
    }

    #[test]
    fn strings_strip_surrounding_quotes() {
        let (tokens, _) = lex("\"grafana/mimir:3.0.6\"");
        assert_eq!(tokens[0].0, Token::Str("grafana/mimir:3.0.6".into()));
    }
}
