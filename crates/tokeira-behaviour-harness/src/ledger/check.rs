//! Check comparison declarations without compiling production crates in this crate.
//!
//! Token parsing excludes comments and strings and retains attributes inside
//! proptest macros, whose `in` parameter syntax is not an ordinary Rust signature.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path},
};

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use syn::{Attribute, Expr, Lit, Meta, parse::Parser};

use super::{Ledger, LedgerError, Verdict, invalid};

/// Exact nextest selection for the named comparisons, deduplicated and sorted.
/// Shared comparisons run once even when they substantiate several rules.
pub fn comparison_filter(ledger: &Ledger) -> String {
    ledger
        .decisions
        .iter()
        .flat_map(|decision| &decision.tests)
        .map(|test| test.name.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|name| format!("test(={name})"))
        .collect::<Vec<_>>()
        .join(" |\n")
}

/// Require the nextest profile to select exactly the maintained comparisons.
/// Only whitespace is ignored: extra, reordered or missing terms fail, with a
/// complete replacement filter suitable for the profile's multiline string.
pub fn check_profile_filter(ledger: &Ledger, config: &str) -> Result<(), LedgerError> {
    let expected = comparison_filter(ledger);
    let config: toml::Value =
        toml::from_str(config).map_err(|error| invalid(format!("nextest config: {error}")))?;
    let actual = config
        .get("profile")
        .and_then(|profile| profile.get("behaviour-ledger"))
        .and_then(|profile| profile.get("default-filter"))
        .and_then(toml::Value::as_str);
    let compact = |filter: &str| {
        filter
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>()
    };
    if actual.is_none_or(|actual| compact(actual) != compact(&expected)) {
        return Err(invalid(format!(
            "behaviour-ledger default-filter differs from named comparisons; expected:\n{expected}"
        )));
    }
    Ok(())
}

fn declarations(
    tokens: TokenStream,
    found: &mut BTreeMap<String, Option<String>>,
) -> Result<(), LedgerError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    let mut attrs = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if matches!(&tokens[index], TokenTree::Punct(p) if p.as_char() == '#')
            && let Some(TokenTree::Group(group)) = tokens.get(index + 1)
            && group.delimiter() == Delimiter::Bracket
        {
            let parsed = Attribute::parse_outer
                .parse2(tokens[index..index + 2].iter().cloned().collect())
                .map_err(|e| invalid(format!("test attributes: {e}")))?;
            attrs.extend(parsed);
            index += 2;
            continue;
        }
        match &tokens[index] {
            TokenTree::Ident(ident) if ident == "fn" => {
                if let Some(TokenTree::Ident(name)) = tokens.get(index + 1) {
                    let test = attrs.iter().any(|attr| {
                        attr.path()
                            .segments
                            .last()
                            .is_some_and(|s| s.ident == "test")
                    });
                    if test {
                        let mut ignored = None;
                        for attr in &attrs {
                            // Conditional ignores cannot be proved from a source file
                            // alone; fail closed instead of guessing the active cfg.
                            if attr.path().is_ident("cfg_attr") {
                                return Err(invalid(format!("conditional test attribute: {name}")));
                            }
                            if attr.path().is_ident("ignore") {
                                ignored = Some(match &attr.meta {
                                    Meta::NameValue(value) => match &value.value {
                                        Expr::Lit(value) => match &value.lit {
                                            Lit::Str(reason) => reason.value(),
                                            _ => {
                                                return Err(invalid(
                                                    "ignore reason must be a string",
                                                ));
                                            }
                                        },
                                        _ => {
                                            return Err(invalid("ignore reason must be a literal"));
                                        }
                                    },
                                    Meta::Path(_) => String::new(),
                                    _ => return Err(invalid("unsupported ignore attribute")),
                                });
                            }
                        }
                        if found.insert(name.to_string(), ignored).is_some() {
                            return Err(invalid(format!("ambiguous test declaration: {name}")));
                        }
                    }
                }
                attrs.clear();
            }
            TokenTree::Group(group) => {
                if group.delimiter() == Delimiter::Brace {
                    declarations(group.stream(), found)?;
                }
                attrs.clear();
            }
            TokenTree::Ident(ident) if ident == "async" || ident == "pub" => {}
            _ => attrs.clear(),
        }
        index += 1;
    }
    Ok(())
}

pub(super) fn test_declarations(
    source: &str,
) -> Result<BTreeMap<String, Option<String>>, LedgerError> {
    let mut found = BTreeMap::new();
    declarations(
        source
            .parse()
            .map_err(|e| invalid(format!("Rust tokens: {e}")))?,
        &mut found,
    )?;
    Ok(found)
}

pub(super) fn ignore_matches(verdict: Verdict, reason: &str, ignored: Option<&str>) -> bool {
    ignored == (verdict == Verdict::IgnoredDifference).then_some(reason)
}

/// Require real test declarations in their named crate files and exact ignore
/// reasons. Ordinary, fixed, and untested comparisons must not be ignored.
/// Only library tests are currently supported; JUnit identifies them by package.
pub fn check_sources(ledger: &Ledger, root: &Path) -> Result<(), LedgerError> {
    let mut files = BTreeMap::new();
    for decision in &ledger.decisions {
        for test in &decision.tests {
            if test.crate_name == "tokeira-behaviour-model"
                || test.crate_name == "tokeira-behaviour-harness"
                || !test
                    .file
                    .starts_with(&format!("crates/{}/src/", test.crate_name))
                || Path::new(&test.file)
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err(invalid(format!(
                    "not an implementation comparison: {}",
                    test.identity()
                )));
            }
            if !files.contains_key(&test.file) {
                let source = fs::read_to_string(root.join(&test.file))
                    .map_err(|e| invalid(format!("{}: {e}", test.file)))?;
                files.insert(test.file.clone(), test_declarations(&source)?);
            }
            let name = test
                .name
                .rsplit("::")
                .next()
                .expect("name has a last segment");
            let ignored = files[&test.file]
                .get(name)
                .ok_or_else(|| invalid(format!("missing test {} in {}", test.name, test.file)))?;
            if !ignore_matches(decision.verdict, &decision.reason, ignored.as_deref()) {
                return Err(invalid(format!(
                    "ignore attribute/reason mismatch: {}",
                    test.identity()
                )));
            }
        }
    }
    Ok(())
}
