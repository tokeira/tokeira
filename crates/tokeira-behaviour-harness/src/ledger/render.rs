//! Stable Markdown blocks leave surrounding prose untouched.
//!
//! RPC evidence is deliberately empty until executed scenarios exist. Matrix
//! implementation claims and decision comparisons are provenance, not coverage.

use std::collections::BTreeSet;

use tokeira_behaviour_model::catalog::{Evidence, RuleCatalog};

use super::{Ledger, LedgerError, Outcome, invalid, surface};

pub(super) fn cell(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('|', "&#124;")
        .replace('\r', "")
        .replace('\n', "<br>")
}

fn local(path: &str, label: &str) -> String {
    format!("[{}](../../{})", cell(label), path)
}

fn support(evidence: &Evidence) -> String {
    match evidence {
        Evidence::AwaitingCalibration => "awaiting calibration".into(),
        Evidence::SourceOnly { reason } => format!("source-only: {}", cell(reason)),
        Evidence::Calibrated { corpus_cases } => {
            let mut cases = corpus_cases.clone();
            cases.sort();
            format!(
                "catalog calibration: {} (no scenario evidence imported)",
                cell(&cases.join(", "))
            )
        }
    }
}

fn decisions(ledger: &Ledger, catalog: &RuleCatalog) -> String {
    let mut result = String::from(
        "| Decision | Area | v1.31.0 branches and source | Tokeira counterpart | Comparison tests and outcomes | Model support | Verdict (provisional) | Reason | Run provenance |\n|---|---|---|---|---|---|---|---|---|\n",
    );
    let mut rules: Vec<_> = catalog.rules.iter().collect();
    rules.sort_by(|a, b| a.key.cmp(&b.key));
    for rule in rules {
        let record = ledger
            .decisions
            .iter()
            .find(|d| d.rule == rule.key)
            .expect("validated keys");
        let branches = rule.rows.iter().enumerate().map(|(index, row)| {
            let mut citations: Vec<_> = row.citations.iter().map(|c| format!(
                "{} — [{}:{}–{} @ {}](https://github.com/temporalio/temporal/blob/{}/{}#L{}-L{})",
                cell(&c.function), cell(&c.path), c.start, c.end, cell(&c.tag), c.tag, c.path, c.start, c.end,
            )).collect();
            citations.sort();
            format!("{}. {} — {}", index+1, cell(&row.transition), citations.join("; "))
        }).collect::<Vec<_>>().join("<br>");
        let mut counterparts: Vec<_> = record
            .counterparts
            .iter()
            .map(|c| local(&c.path, &format!("{}: {}", c.path, c.item)))
            .collect();
        counterparts.sort();
        let mut tests: Vec<_> = record
            .tests
            .iter()
            .map(|test| {
                let outcome = record
                    .run
                    .as_ref()
                    .and_then(|run| run.tests.get(&test.identity()))
                    .copied()
                    .unwrap_or(Outcome::Missing);
                format!(
                    "{}: {}",
                    local(&test.file, &test.identity()),
                    outcome.label()
                )
            })
            .collect();
        tests.sort();
        let (verdict, mut reason) = record.effective_verdict(rule);
        if let Some(commit) = &record.fix_commit {
            if !reason.ends_with('.') {
                reason.push('.');
            }
            reason.push_str(&format!(" Corrected in {commit}."));
        }
        let provenance = record.run.as_ref().map_or_else(
            || "none yet".into(),
            |run| {
                format!(
                    "rule sha256:{}<br>JUnit sha256:{}",
                    run.rule_digest, run.junit_digest
                )
            },
        );
        result.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {}{} | {} | {} |\n",
            cell(&rule.key),
            cell(&rule.area),
            branches,
            counterparts.join("<br>"),
            tests.join("<br>"),
            support(&rule.evidence),
            verdict.label(),
            if matches!(rule.evidence, Evidence::SourceOnly { .. }) {
                " (source-only)"
            } else {
                ""
            },
            cell(&reason),
            provenance
        ));
    }
    result
}

fn rpcs(ledger: &Ledger) -> Result<String, LedgerError> {
    let rpcs = surface::rpcs()?;
    let known: BTreeSet<_> = rpcs.iter().map(|rpc| rpc.key.as_str()).collect();
    for record in &ledger.decisions {
        let mut seen = BTreeSet::new();
        for rpc in &record.rpcs {
            if !known.contains(rpc.as_str()) || !seen.insert(rpc) {
                return Err(invalid(format!(
                    "{}: unknown or duplicate RPC {rpc}",
                    record.rule
                )));
            }
        }
    }
    let mut output = String::new();
    for newer in [false, true] {
        let rows: Vec<_> = rpcs.iter().filter(|rpc| rpc.newer == newer).collect();
        output.push_str(&format!(
            "\n**{}: {} methods.**\n\n",
            if newer {
                "Newer vendored wire, outside v1.31.0 target"
            } else {
                "v1.31.0 target surface"
            },
            rows.len()
        ));
        output.push_str("| RPC | Target origin | Feature claim metadata | Reachable model rules (not scenarios) | Scenarios | Corpus | Tier 2 | Modes | Evidence revisions | Verdict |\n|---|---|---|---|---|---|---|---|---|---|\n");
        for rpc in rows {
            let f = rpc.feature;
            let mut enablement = f.catalog.enablement.kind.label().to_owned();
            if let Some(reference) = f.catalog.enablement.reference {
                enablement.push(' ');
                enablement.push_str(reference);
            }
            let metadata = format!(
                "{}; origin={}; state={}; conformance={}; maturity={}; Temporal default={}; Tokeira default={}; enablement={}; scopes={}; mutability={}; prerequisites={}; dynamic config={}; capabilities={}",
                f.id,
                f.catalog.origin.label(),
                f.state.label(),
                f.catalog.conformance.label(),
                f.catalog.temporal_maturity.label(),
                f.catalog.temporal_default.label(),
                f.catalog.tokeira_default.label(),
                enablement,
                labels_or_none(f.catalog.scopes.iter().map(|scope| scope.label())),
                f.catalog.mutability.label(),
                labels_or_none(f.catalog.prerequisites.iter().copied()),
                f.dynamic_config_key.unwrap_or("none"),
                labels_or_none(f.capability_fields())
            );
            let mut rules: Vec<_> = ledger
                .decisions
                .iter()
                .filter(|d| d.rpcs.contains(&rpc.key))
                .map(|d| d.rule.as_str())
                .collect();
            rules.sort();
            output.push_str(&format!("| {} | {} | {} | {} | none yet | none yet | none yet | none yet | none yet | no behavioural evidence yet |\n",
                cell(&rpc.key), f.catalog.origin.label(), cell(&metadata), if rules.is_empty() { "none yet".into() } else { cell(&rules.join(", ")) }));
        }
    }
    Ok(output)
}

fn labels_or_none<'a>(labels: impl Iterator<Item = &'a str>) -> String {
    let labels = labels.collect::<Vec<_>>();
    if labels.is_empty() {
        "none".into()
    } else {
        labels.join(", ")
    }
}

fn areas(catalog: &RuleCatalog) -> String {
    let areas: BTreeSet<_> = catalog.rules.iter().map(|r| r.area.as_str()).collect();
    let mut output = String::from(
        "| Area | Model provenance | Awaiting calibration | Source-only rows | Transitions reached / never reached | Sometimes counts / budgets | Feature pairs reached / never reached | Mutation results | DSQL evidence | Fault evidence |\n|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for area in areas {
        let rules: Vec<_> = catalog.rules.iter().filter(|r| r.area == area).collect();
        let awaiting = rules
            .iter()
            .filter(|r| matches!(r.evidence, Evidence::AwaitingCalibration))
            .count();
        let source = rules
            .iter()
            .filter(|r| matches!(r.evidence, Evidence::SourceOnly { .. }))
            .count();
        output.push_str(&format!("| {} | [rule catalog](../../crates/tokeira-behaviour-model/rules.json); {} decision rows | {awaiting} | {source} | not yet measured | not yet measured | not yet measured | not yet measured | not yet measured | not yet measured |\n", cell(area), rules.len()));
    }
    output
}

fn replace_block(page: &str, name: &str, contents: &str) -> Result<String, LedgerError> {
    let begin = format!("<!-- BEGIN GENERATED {name} -->");
    let end = format!("<!-- END GENERATED {name} -->");
    if page.matches(&begin).count() != 1 || page.matches(&end).count() != 1 {
        return Err(invalid(format!("missing or duplicate {name} markers")));
    }
    let start = page.find(&begin).expect("counted begin") + begin.len();
    let finish = page.find(&end).expect("counted end");
    if finish < start {
        return Err(invalid(format!("reversed {name} markers")));
    }
    Ok(format!(
        "{}\n{}{}",
        &page[..start],
        contents,
        &page[finish..]
    ))
}

/// Render all three row kinds from current sources while preserving every byte
/// outside their marker pairs. Missing/unknown records or RPCs are errors; stale
/// records render conservatively and are rejected separately by check mode.
pub fn render_page(
    page: &str,
    ledger: &Ledger,
    catalog: &RuleCatalog,
) -> Result<String, LedgerError> {
    ledger.validate(catalog)?;
    let page = replace_block(page, "decisions", &decisions(ledger, catalog))?;
    let page = replace_block(&page, "rpcs", &rpcs(ledger)?)?;
    replace_block(&page, "areas", &areas(catalog))
}

/// Refresh generated blocks without importing or mutating run evidence.
/// A catalog change still requires new comparisons: stale evidence is rejected
/// before any replacement page can be written.
pub fn refresh_page(
    page: &str,
    ledger: &Ledger,
    catalog: &RuleCatalog,
) -> Result<String, LedgerError> {
    ledger.check_current(catalog)?;
    render_page(page, ledger, catalog)
}
