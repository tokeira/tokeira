//! Operator-facing rendering for binding verdicts and plans.
//!
//! Reports here follow the operator output contract
//! (`docs/platforms/operator-output-contract.md`): each is a serializable
//! model rendered through `tokeira-report` — summary states the answer,
//! `--detail` adds the evidence, `--json` emits the complete model. The house
//! value behind the copy (§Values 3, operator empathy): every report says
//! what happened, why, and what to do next — in operator language, not ours.
//! "Unknown — apply would REFUSE" is a true statement about internals and a
//! useless one to a person staring at a fresh deployment; "not initialized —
//! `apply` stamps it on first run" is the same fact, usable.

use tokeira_explain::{DeploymentExplanation, ExplainedChange};
use tokeira_iac::{ChangeKind, RefreshStatus};
use tokeira_provisioner::BindingVerdict;
use tokeira_report::{Depth, Report, symbol};

/// The read-only plan report: the deployment explanation, framed by the
/// verb-level annotations (platform line, attention-only binding).
///
/// `--json` emits the **explanation model alone** (evidence-model Req 6.3):
/// the manual `Serialize` delegates to it, so the artifact and the `--json`
/// output are one schema. The binding verdict is verb framing, not model
/// content — `describe` owns it.
///
/// C5 migration note (evidence-model design): if a consumer outside `tkp`
/// ever needs to render an explanation — an artifact viewer, Feature 5's
/// analysis bundles — this `Report` impl moves into `tokeira-explain` and
/// that crate takes `tokeira-report` (a serde-only crate) as a dependency.
/// The move is mechanical; recorded here so it is a decision, not a
/// surprise.
#[derive(Debug)]
pub(crate) struct ExplanationReport {
    /// Whether the deployment carries a Day-0 binding stamp yet.
    pub initialized: bool,
    pub binding: BindingVerdict,
    pub explanation: DeploymentExplanation,
}

impl serde::Serialize for ExplanationReport {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.explanation.serialize(serializer)
    }
}

impl Report for ExplanationReport {
    fn narrative(&self, depth: Depth, out: &mut String) {
        let explanation = &self.explanation;
        // `# Infra Plan` — the operation, title-cased.
        out.push_str(&format!("# {}\n", title_case(&explanation.operation)));
        // The header anchors the deployment's revision once — the one place
        // the document states a revision number, and no coverage clause: a
        // plan only renders what could execute (output-templates §header).
        out.push_str(&format!(
            "**Plan for {}** {}\n",
            explanation.platform,
            revision_anchor(explanation),
        ));
        // Verdict narration is attention-only: a verdict that lets the apply
        // proceed is a standing fact (describe's story), not news on every
        // plan. Only what blocks or qualifies the apply earns a line.
        if let Some(line) = binding_attention(self.initialized, self.binding) {
            out.push_str(&format!("\n**binding:** {line}\n"));
        }
        // A platform issue suppresses every section below it: nothing that
        // could not execute is rendered (output-templates rule 1).
        if !explanation.platform_issues.is_empty() {
            platform_issues_section(explanation, out);
            return;
        }
        action_sections(explanation, depth, out);
        drift_section(explanation, depth, out);
        unchanged_section(explanation, depth, out);
        impacts_section(explanation, out);
    }
}

/// The `## Platform Issue[s]` section: the fact, the SDK error verbatim as
/// evidence (the platform's words, never blended into Tokeira's sentence),
/// and a direction only where the error itself establishes one.
fn platform_issues_section(explanation: &DeploymentExplanation, out: &mut String) {
    out.push_str(&format!(
        "\n## {}\n",
        counted_heading("Platform Issue", explanation.platform_issues.len())
    ));
    for issue in &explanation.platform_issues {
        out.push_str(&format!("- {}:\n", issue.fact));
        out.push_str(&format!("  - {}\n", code_span(&issue.evidence)));
        if let Some(direction) = &issue.direction {
            out.push_str(&format!("  - **{direction}**\n"));
        }
    }
}

/// A heading pluralized by count — the computed-plural rule applied to
/// headings (output-templates rule 3).
fn counted_heading(noun: &str, count: usize) -> String {
    if count == 1 {
        noun.to_string()
    } else {
        format!("{noun}s")
    }
}

/// Title-case an operation ("infra plan" → "Infra Plan").
fn title_case(operation: &str) -> String {
    operation
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The header's revision anchor: the one place the document states which
/// revision the plan compares against (`output-templates.md` §header).
fn revision_anchor(explanation: &DeploymentExplanation) -> String {
    match explanation.current_revision {
        0 => "before its first apply".to_string(),
        n => format!("at revision {n}"),
    }
}

/// The operator noun phrase for a change: "the *tokeirad* service" when the
/// plan holds more than one resource of the kind, "the *Aurora DSQL
/// cluster*" when the kind noun alone identifies it; the engine id when the
/// kind declares no noun.
fn noun_phrase(explanation: &DeploymentExplanation, change: &ExplainedChange) -> String {
    match &change.display {
        Some(noun) => {
            let siblings = explanation
                .changes
                .iter()
                .filter(|c| c.display.as_deref() == Some(noun.as_str()))
                .count();
            if siblings > 1 {
                format!("the *{}* {noun}", change.module)
            } else {
                format!("the *{noun}*")
            }
        }
        None => format!("`{}::{}`", change.module, change.resource_id),
    }
}

/// Wrap a value as a Markdown code span, surviving embedded backticks.
fn code_span(value: &str) -> String {
    if value.contains('`') {
        format!("`` {value} ``")
    } else {
        format!("`{value}`")
    }
}

/// Render a citation for detail depth: documentation as a link, code by
/// module identity as a code span (change-semantics Req 9.7).
fn cite(citation: &tokeira_iac::Citation) -> String {
    match citation {
        tokeira_iac::Citation::Code(reference) => code_span(reference),
        tokeira_iac::Citation::Doc { title, url, .. } => format!("[{title}]({url})"),
    }
}

/// A behaviour claim in its confidence voice with its citation (Req 9.2/9.3
/// at detail): an engine fact speaks plainly, a provider guarantee is
/// attributed by its linked document, an inference owns itself.
fn voiced(claim: &str, confidence_citation: &tokeira_iac::Confidence<impl Sized>) -> String {
    use tokeira_iac::Confidence;
    match confidence_citation {
        Confidence::Unknown => claim.to_string(),
        Confidence::EngineFact { citation, .. } => format!("{claim} - {}", cite(citation)),
        Confidence::ProviderGuarantee { citation, .. } => {
            format!("{claim} - per {}", cite(citation))
        }
        Confidence::Inference { citation, .. } => {
            format!("{claim}; Tokeira derives this - per {}", cite(citation))
        }
    }
}

/// The change with the given resource id, when the plan holds one.
fn change_for<'a>(
    explanation: &'a DeploymentExplanation,
    resource_id: &str,
) -> Option<&'a ExplainedChange> {
    explanation
        .changes
        .iter()
        .find(|c| c.resource_id == resource_id)
}

/// The clause on a change line (causality Req 6.1; `output-templates.md`
/// §cause-clauses): **the concrete change, never a cause category**. A
/// definition edit puts the diff itself on the line (the changed definition
/// is implicit — it is the operator's own edit); drift names the fields
/// that changed outside the definition; derived causes name their concrete
/// trigger. Creates and deletes from the operator's own edit carry no
/// clause — the verb already states the change. `None` also for an unknown
/// cause: detail carries its uncertainty in place (Req 6.3). No clause ever
/// references a revision number — the header anchors the revision once.
fn cause_phrase(explanation: &DeploymentExplanation, change: &ExplainedChange) -> Option<String> {
    use tokeira_explain::Cause;
    match change.cause.value()? {
        Cause::DefinitionEdit { .. } => match change.kind {
            ChangeKind::Update | ChangeKind::Replace => diff_clause(change),
            _ => None,
        },
        // Drift never renders as a clause: drifted resources have their own
        // section with their own grammar (output-templates rule 2).
        Cause::ProviderDrift => None,
        Cause::EngineAdvance => {
            Some("this provisioner realizes the definition differently".to_string())
        }
        Cause::ReplacementCascade { root } => Some(format!(
            "forced by {} replacement",
            change_for(explanation, root)
                .map(|c| noun_phrase(explanation, c))
                .unwrap_or_else(|| code_span(root))
        )),
        Cause::DependencyOutputChanged { dependency } => Some(format!(
            "an output of {} changed",
            change_for(explanation, dependency)
                .map(|c| noun_phrase(explanation, c))
                .unwrap_or_else(|| code_span(dependency))
        )),
    }
}

/// An edit's concrete change for its line: the first field diff with its
/// values, "and N more fields" when several. A diff observed without
/// captured values names the field alone; a change with no field evidence
/// carries no clause.
fn diff_clause(change: &ExplainedChange) -> Option<String> {
    let first = change.field_diffs.first()?;
    let head = match (&first.before, &first.after) {
        (Some(before), Some(after)) => format!(
            "{}: {} → {}",
            code_span(&first.field),
            code_span(&truncate(before)),
            code_span(&truncate(after)),
        ),
        _ => code_span(&first.field),
    };
    Some(more_fields(head, change.field_diffs.len() - 1))
}

fn more_fields(head: String, extra: usize) -> String {
    match extra {
        0 => head,
        1 => format!("{head} and 1 more field"),
        n => format!("{head} and {n} more fields"),
    }
}

/// The engine-kind verb in would-mood.
fn kind_verb(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Create => "created",
        ChangeKind::Update => "updated",
        ChangeKind::Replace => "replaced",
        ChangeKind::Delete => "deleted",
        ChangeKind::NoChange => "unchanged",
    }
}

/// The `##` action sections: one per present action, each change a line of
/// templated would-mood prose with its engine id stated once. Detail adds
/// field evidence, the declared behaviour in its confidence voice, and the
/// in-place live-state statement.
/// Whether a change renders in `## Resource Drift` rather than an action
/// section: the world moved it, the definition did not (output-templates
/// rule 2). An edited-and-drifted resource stays in its action section —
/// the edit owns the change — with its drifted fields annotated per diff.
fn is_drift(change: &ExplainedChange) -> bool {
    matches!(
        change.cause.value(),
        Some(tokeira_explain::Cause::ProviderDrift)
    )
}

fn action_sections(explanation: &DeploymentExplanation, depth: Depth, out: &mut String) {
    let acting_exists = explanation
        .changes
        .iter()
        .any(|c| c.kind != ChangeKind::NoChange);
    if !acting_exists {
        out.push_str("\nNo changes - everything matches the definition.\n");
    }
    for (kind, section) in [
        (ChangeKind::Create, "## Create"),
        (ChangeKind::Update, "## Update"),
        (ChangeKind::Replace, "## Replace"),
        (ChangeKind::Delete, "## Delete"),
    ] {
        let members: Vec<&ExplainedChange> = explanation
            .changes
            .iter()
            .filter(|c| c.kind == kind && !is_drift(c))
            .collect();
        if members.is_empty() {
            continue;
        }
        out.push_str(&format!("\n{section}\n"));
        for change in members {
            // A named resource reads as prose with its id once at the tail;
            // a kind without a noun leads with the id and never repeats it.
            // An established cause joins the line as a clause; an unknown
            // cause renders no clause here and speaks through its
            // uncertainty at detail (causality Req 6.1/6.3).
            let clause = cause_phrase(explanation, change)
                .map(|phrase| format!(" - {phrase}"))
                .unwrap_or_default();
            let line = if change.display.is_some() {
                format!(
                    "- {} would be {}{clause} - `{}::{}`\n",
                    noun_phrase(explanation, change),
                    kind_verb(kind),
                    change.module,
                    change.resource_id,
                )
            } else {
                format!(
                    "- `{}::{}` would be {}{clause}\n",
                    change.module,
                    change.resource_id,
                    kind_verb(kind),
                )
            };
            out.push_str(&line);
            if depth == Depth::Detail {
                change_detail(explanation, change, out);
            }
        }
    }
}

/// The `## Resource Drift` section: what the world did, then what the plan
/// does about it — one line per drifted resource, definition-driven
/// sections kept clean of it (output-templates §Resource Drift lines).
fn drift_section(explanation: &DeploymentExplanation, depth: Depth, out: &mut String) {
    let members: Vec<&ExplainedChange> = explanation
        .changes
        .iter()
        .filter(|c| c.kind != ChangeKind::NoChange && is_drift(c))
        .collect();
    if members.is_empty() {
        return;
    }
    out.push_str("\n## Resource Drift\n");
    for change in members {
        let noun = noun_phrase(explanation, change);
        let line = match change.kind {
            // A confirmed absence: the world lost it; the plan restores it.
            ChangeKind::Create => format!(
                "- {noun} could not be found - it would be recreated - `{}::{}`\n",
                change.module, change.resource_id,
            ),
            _ => {
                let fields = match change.field_diffs.first() {
                    Some(first) => {
                        more_fields(code_span(&first.field), change.field_diffs.len() - 1)
                    }
                    None => "live state".to_string(),
                };
                format!(
                    "- {noun}'s {fields} changed outside the definition - it would be {} - `{}::{}`\n",
                    kind_verb(change.kind),
                    change.module,
                    change.resource_id,
                )
            }
        };
        out.push_str(&line);
        if depth == Depth::Detail {
            change_detail(explanation, change, out);
        }
    }
}

/// The `## Unchanged` section, detail depth only.
fn unchanged_section(explanation: &DeploymentExplanation, depth: Depth, out: &mut String) {
    if depth != Depth::Detail {
        return;
    }
    let unchanged: Vec<&ExplainedChange> = explanation
        .changes
        .iter()
        .filter(|c| c.kind == ChangeKind::NoChange)
        .collect();
    if unchanged.is_empty() {
        return;
    }
    out.push_str("\n## Unchanged\n");
    for change in unchanged {
        let line = if change.display.is_some() {
            format!(
                "- {} - `{}::{}`\n",
                noun_phrase(explanation, change),
                change.module,
                change.resource_id,
            )
        } else {
            format!("- `{}::{}`\n", change.module, change.resource_id)
        };
        out.push_str(&line);
    }
}

/// Detail sub-bullets for one change: field diffs as code spans, then the
/// declared behaviour — mechanism, data effect, reversibility — each in its
/// confidence voice with its citation, then the cause in its voice (or its
/// uncertainty in place, causality Req 6.2/6.3), the dependants
/// (Req 5.2/5.4), and — on a chain's first member — the story line
/// (Req 6.4). Unknown semantic fields render nothing (knowledge renders;
/// gaps enforce); an unknown *cause* renders its uncertainty, because a
/// cause gap is operator-actionable knowledge about the plan.
fn change_detail(explanation: &DeploymentExplanation, change: &ExplainedChange, out: &mut String) {
    use tokeira_iac::{DataEffect, LifecycleOperation, Reversibility};
    if matches!(change.kind, ChangeKind::Update | ChangeKind::Replace) {
        for diff in &change.field_diffs {
            // A field whose live value departed from the record carries its
            // annotation in place — the edit owns the change; the drift fact
            // stays visible per field (output-templates §detail, item 1).
            // Redundant on a drift-section line, whose fields are the story.
            let departed = !is_drift(change) && change.departed_fields.contains(&diff.field);
            let annotation = if departed {
                " - changed outside the definition"
            } else {
                ""
            };
            if diff.before.is_none() && diff.after.is_none() {
                out.push_str(&format!("  - {}{annotation}\n", diff.field));
            } else {
                out.push_str(&format!(
                    "  - {}: {} → {}{annotation}\n",
                    code_span(&diff.field),
                    code_span(&truncate(diff.before.as_deref().unwrap_or("(none)"))),
                    code_span(&truncate(diff.after.as_deref().unwrap_or("(none)"))),
                ));
            }
        }
    }
    // Interim latitude until the platform-issue seam lands: an unreadable
    // live read still states the diffs' provenance in place. The agreement's
    // worlds never reach here (rule 1 turns unreachability into a Platform
    // Issue); this line covers the transitional gap honestly.
    if matches!(change.refresh_status, Some(RefreshStatus::Unknown)) {
        if change.cause.value().is_none() && change.kind != ChangeKind::NoChange {
            out.push_str(
                "  - compared against the record - live state could not be read, so why \
                 this differs is unknown\n",
            );
        } else {
            out.push_str("  - compared against the record - live state could not be read\n");
        }
    }

    let semantics = &change.semantics;
    // The mechanism: an update effected as a replacement is the load-bearing
    // truth; the kind-authored statement carries the how when one exists.
    let effected_as_replace = change.kind == ChangeKind::Update
        && semantics.operation.value() == Some(&LifecycleOperation::Replaced);
    match (&semantics.statement, effected_as_replace) {
        (Some(statement), true) => out.push_str(&format!(
            "  - {}\n",
            voiced(
                &format!("the update replaces it - {statement}"),
                &semantics.operation
            )
        )),
        (Some(statement), false) => {
            out.push_str(&format!(
                "  - {}\n",
                voiced(statement, &semantics.operation)
            ));
        }
        (None, true) => out.push_str(&format!(
            "  - {}\n",
            voiced(
                "the update replaces it - it would be destroyed, then recreated",
                &semantics.operation
            )
        )),
        (None, false) => {}
    }
    if let Some(effect) = semantics.data_effect.value() {
        let claim = match effect {
            DataEffect::NoDataHeld => None,
            DataEffect::Preserved => Some("the data it holds would be preserved"),
            DataEffect::Migrated => Some("its data would be migrated"),
            DataEffect::Destroyed => Some("its stored data would be destroyed"),
            // The general value; the kind's statement carries the specific
            // policy ("items past their declared expiry would be deleted").
            DataEffect::Policy => Some("its data follows its declared policy"),
        };
        if let Some(claim) = claim {
            out.push_str(&format!("  - {}\n", voiced(claim, &semantics.data_effect)));
        }
    }
    if let Some(reversibility) = semantics.reversibility.value() {
        let claim = match (reversibility, change.kind) {
            (Reversibility::Reversible, ChangeKind::Update) => {
                "re-applying the prior definition would restore it"
            }
            (Reversibility::Reversible, _) => "re-applying the definition would restore it",
            (Reversibility::ReversibleWithDataLoss, _) => {
                "reversing it would lose data written since"
            }
            (Reversibility::Irreversible, _) => "it could not be reversed",
        };
        out.push_str(&format!(
            "  - {}\n",
            voiced(claim, &semantics.reversibility)
        ));
    }

    // The cause's voice line renders only where it adds information beyond
    // the line clause: a derived cause owns itself with its citation
    // (Req 6.2). Engine-fact causes already state their concrete change on
    // the line, and an unknown cause under an unreadable live state spoke
    // with the provenance line above. The remaining unknown arms (a missing
    // or broken baseline) state their one operator-usable fact — never the
    // machine consequence text, which stays in the model for agents and CI.
    let derived = matches!(change.cause, tokeira_iac::Confidence::Inference { .. });
    if derived && let Some(phrase) = cause_phrase(explanation, change) {
        out.push_str(&format!("  - why: {}\n", voiced(&phrase, &change.cause)));
    } else if change.cause.value().is_none()
        && change.kind != ChangeKind::NoChange
        && !matches!(change.refresh_status, Some(RefreshStatus::Unknown))
    {
        let baseline_missing = explanation
            .uncertainties
            .iter()
            .find_map(|u| match &u.reason {
                tokeira_explain::UncertaintyReason::BaselineUnavailable { revision } => {
                    Some(*revision)
                }
                _ => None,
            });
        match baseline_missing {
            Some(revision) => out.push_str(&format!(
                "  - why this differs is unknown - revision {revision}'s definition is \
                 not retained for comparison\n"
            )),
            None => out.push_str("  - why this differs is unknown\n"),
        }
    }

    // Dependants: what depends on this resource, changing or not — the
    // relationship continuing unchanged is a statement, never an omission.
    let (changing, unchanged): (Vec<&String>, Vec<&String>) =
        change.dependants.iter().partition(|dep| {
            change_for(explanation, dep)
                .map(|c| c.kind != ChangeKind::NoChange)
                .unwrap_or(false)
        });
    let names = |deps: &[&String]| {
        deps.iter()
            .map(|dep| {
                change_for(explanation, dep)
                    .map(|c| noun_phrase(explanation, c))
                    .unwrap_or_else(|| code_span(dep))
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    if !changing.is_empty() {
        out.push_str(&format!(
            "  - dependants changing with it: {}\n",
            names(&changing)
        ));
    }
    if !unchanged.is_empty() {
        out.push_str(&format!(
            "  - dependants continuing unchanged: {}\n",
            names(&unchanged)
        ));
    }

    // A multi-member chain tells its story once, on its first member, root
    // first and members along the dependency path (causality Req 6.4).
    if let Some(group) = explanation
        .causal_groups
        .iter()
        .find(|g| g.members.len() > 1 && g.members.first() == Some(&change.evidence_id))
    {
        use tokeira_explain::CausalRoot;
        let root_phrase = match &group.root {
            CausalRoot::RevisionComparison { baseline: 0 } => {
                "the first apply of this definition".to_string()
            }
            // The header anchors the revision; the chain names the story.
            CausalRoot::RevisionComparison { .. } => "the definition change".to_string(),
            CausalRoot::ProvisionerAdvance => "the provisioner advance".to_string(),
            CausalRoot::Resource(id) => explanation
                .changes
                .iter()
                .find(|c| &c.evidence_id == id)
                .map(|c| noun_phrase(explanation, c))
                .unwrap_or_else(|| code_span(id.as_str())),
        };
        let members = group
            .members
            .iter()
            .filter_map(|member| {
                explanation
                    .changes
                    .iter()
                    .find(|c| &c.evidence_id == member)
                    .map(|c| noun_phrase(explanation, c))
            })
            .collect::<Vec<_>>()
            .join(", then ");
        out.push_str(&format!("  - chain: {root_phrase}: {members}\n"));
    }
}

/// The impact section: **one line per resource**, carrying every impact on
/// it — consequences merged permanent-first (data destruction, dependency
/// loss, unavailability, replacement, then the transients), the would-mood
/// shared, data destruction leading its line with further consequences
/// back-referencing. Resources ordered by their most severe impact, ties by
/// id; the heading pluralized by count (output-templates §Impact lines).
fn impacts_section(explanation: &DeploymentExplanation, out: &mut String) {
    use std::collections::BTreeMap;
    use tokeira_explain::{EvidenceId, ImpactClass};
    use tokeira_iac::Reversibility;
    if explanation.impacts.is_empty() {
        return;
    }

    // Collect per resource: its classes (BTreeSet keeps severity order —
    // the enum order is the severity order) and any lost dependency.
    struct ResourceImpacts<'a> {
        change: &'a ExplainedChange,
        classes: std::collections::BTreeSet<ImpactClass>,
        lost: Vec<&'a EvidenceId>,
    }
    let mut by_resource: BTreeMap<&EvidenceId, ResourceImpacts<'_>> = BTreeMap::new();
    for impact in &explanation.impacts {
        for subject in &impact.subjects {
            let Some(change) = explanation
                .changes
                .iter()
                .find(|c| &c.evidence_id == subject)
            else {
                continue;
            };
            let entry = by_resource
                .entry(subject)
                .or_insert_with(|| ResourceImpacts {
                    change,
                    classes: std::collections::BTreeSet::new(),
                    lost: Vec::new(),
                });
            entry.classes.insert(impact.class);
            if impact.class == ImpactClass::DependencyLoss
                && let Some(lost) = &impact.lost
            {
                entry.lost.push(lost);
            }
        }
    }

    // Resources ordered by their most severe impact, ties by id.
    let mut resources: Vec<&ResourceImpacts<'_>> = by_resource.values().collect();
    resources.sort_by(|a, b| {
        let sa = a.classes.iter().next();
        let sb = b.classes.iter().next();
        sa.cmp(&sb)
            .then_with(|| a.change.evidence_id.cmp(&b.change.evidence_id))
    });

    out.push_str(&format!(
        "\n## {}\n",
        counted_heading("Impact", resources.len())
    ));
    for resource in resources {
        let change = resource.change;
        let noun = noun_phrase(explanation, change);
        // The verb phrases after the shared "would", in severity order. The
        // first `be`-phrase keeps its "be"; later ones elide it.
        let mut phrases: Vec<(bool, String)> = Vec::new();
        let mut data_destroyed = false;
        for class in &resource.classes {
            match class {
                ImpactClass::DataDestroyed => data_destroyed = true,
                ImpactClass::DependencyLoss => {
                    for lost in &resource.lost {
                        let lost_noun = explanation
                            .changes
                            .iter()
                            .find(|c| &&c.evidence_id == lost)
                            .map(|c| noun_phrase(explanation, c))
                            .unwrap_or_else(|| "a deleted dependency".to_string());
                        phrases.push((false, format!("continue without {lost_noun}")));
                    }
                }
                ImpactClass::Unavailability => {
                    if change.kind == ChangeKind::Delete {
                        phrases.push((false, "no longer be available".to_string()));
                    } else {
                        phrases.push((true, "unavailable while the change applies".to_string()));
                    }
                }
                ImpactClass::Replacement => phrases.push((true, "replaced".to_string())),
                ImpactClass::BriefInterruption => {
                    phrases.push((true, "briefly interrupted".to_string()));
                }
                ImpactClass::RollingReplacement => {
                    phrases.push((true, "replaced one at a time".to_string()));
                }
            }
        }
        let joined = join_would_phrases(&phrases);
        let line = if data_destroyed {
            let irreversibly =
                change.semantics.reversibility.value() == Some(&Reversibility::Irreversible);
            let lead = if irreversibly {
                format!("data held by {noun} would be destroyed, irreversibly")
            } else {
                format!("data held by {noun} would be destroyed")
            };
            if joined.is_empty() {
                lead
            } else {
                format!("{lead}, and it would {joined}")
            }
        } else {
            format!("{noun} would {joined}")
        };
        out.push_str(&format!("- {line}\n"));
    }
}

/// Join would-mood verb phrases: the first `be`-phrase keeps its "be",
/// later ones elide it; comma-joined with "and" before the last.
fn join_would_phrases(phrases: &[(bool, String)]) -> String {
    let mut be_spoken = false;
    let rendered: Vec<String> = phrases
        .iter()
        .map(|(needs_be, text)| {
            if *needs_be && !be_spoken {
                be_spoken = true;
                format!("be {text}")
            } else {
                text.clone()
            }
        })
        .collect();
    match rendered.len() {
        0 => String::new(),
        1 => rendered.into_iter().next().expect("one phrase"),
        2 => format!("{}, and {}", rendered[0], rendered[1]),
        _ => {
            let (last, rest) = rendered.split_last().expect("non-empty");
            format!("{}, and {last}", rest.join(", "))
        }
    }
}

/// The attention-worthy binding line for a **read-only** report: `Some` only
/// when the verdict would block the apply, or the deployment is fresh (the
/// first apply does more than the plan shows — the Day-0 stamp). Proceeding
/// verdicts return `None`.
fn binding_attention(initialized: bool, verdict: BindingVerdict) -> Option<&'static str> {
    if !initialized {
        return Some("not initialized — `apply` stamps this deployment on first run");
    }
    match verdict {
        BindingVerdict::Match | BindingVerdict::DevIterate => None,
        BindingVerdict::Mismatch => Some(
            "MISMATCH — the running provisioner is not the one this deployment recorded; \
             apply refuses (run the recorded provisioner, or `upgrade` to advance)",
        ),
        BindingVerdict::Downgrade => Some(
            "DOWNGRADE — the running provisioner is older than the one this deployment \
             recorded; apply refuses (run the recorded provisioner, or `rollback` to re-pin)",
        ),
        BindingVerdict::ModeRegression => Some(
            "MODE REGRESSION — a dev build cannot operate a versioned deployment; \
             apply refuses (use the released binary)",
        ),
        // Unreachable today (Unknown ⇔ no recorded binding) — kept exhaustive
        // so a future verdict cannot fall through silently.
        BindingVerdict::Unknown => Some("unknown — apply refuses"),
    }
}

/// Print what an apply actually committed — one line per resource, the
/// audit entries as the operator report (`+` created, `~` updated, `-`
/// deleted). An apply that hides its work behind a count blinds the
/// operator during the highest-stakes verb.
pub(crate) fn print_applied(entries: &[tokeira_provisioner::ChangeLogEntry]) {
    use tokeira_provisioner::ChangeOp;
    for entry in entries {
        let glyph = match entry.op {
            ChangeOp::Created => symbol::CREATE,
            ChangeOp::Updated => symbol::UPDATE,
            ChangeOp::Deleted => symbol::DELETE,
        };
        println!("  {glyph} {}", entry.id);
    }
}

/// Clamp a diff value for the one-line field report.
fn truncate(value: &str) -> String {
    const MAX: usize = 72;
    if value.len() <= MAX {
        return value.to_string();
    }
    let mut end = MAX;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_explain::{DeploymentContext, explain_plan};
    use tokeira_iac::{Change, PlanOutcome, RefreshCoverage, ResourceId};
    use tokeira_report::{Mode, render};

    fn change(kind: ChangeKind, resource: &str) -> Change {
        Change {
            kind,
            resource_type: "t".into(),
            module: "m".into(),
            resource: resource.into(),
            details: vec![tokeira_iac::FieldDiff {
                field: "image".into(),
                before: Some("a".into()),
                after: Some("b".into()),
            }],
        }
    }

    fn context() -> DeploymentContext {
        DeploymentContext {
            deployment: "test-deployment".into(),
            platform: "test".into(),
            operation: "infra plan".into(),
            current_revision: 1,
            proposed_revision: None,
            definition_ref: None,
        }
    }

    fn report_for(
        outcome: &PlanOutcome,
        binding: BindingVerdict,
        initialized: bool,
    ) -> ExplanationReport {
        ExplanationReport {
            initialized,
            binding,
            explanation: explain_plan(context(), outcome),
        }
    }

    /// A fully-declared semantics value: these tests exercise refresh and
    /// depth behaviour, so declarations are complete to keep the
    /// undeclared-semantics activation out of their frame (it has its own
    /// tests below).
    fn declared() -> tokeira_iac::ChangeSemantics {
        use tokeira_iac::{
            Citation, Confidence, DataEffect, Disruption, LifecycleOperation, ReplacementPolicy,
            Reversibility,
        };
        const CITE: Citation = Citation::code("test/declared");
        tokeira_iac::ChangeSemantics {
            operation: Confidence::EngineFact {
                value: LifecycleOperation::UpdatedInPlace,
                citation: CITE,
            },
            replacement: Confidence::EngineFact {
                value: ReplacementPolicy::NotRequired,
                citation: CITE,
            },
            disruption: Confidence::EngineFact {
                value: Disruption::None,
                citation: CITE,
            },
            data_effect: Confidence::EngineFact {
                value: DataEffect::Preserved,
                citation: CITE,
            },
            reversibility: Confidence::EngineFact {
                value: Reversibility::Reversible,
                citation: CITE,
            },
            statement: None,
        }
    }

    fn examined(changes: Vec<Change>) -> PlanOutcome {
        // Every planned resource confirmed live — the fully-confirmed case.
        let status_by_id = changes
            .iter()
            .map(|c| (ResourceId(c.resource.clone()), RefreshStatus::DesiredLive))
            .collect();
        let semantics_by_id = changes
            .iter()
            .filter(|c| c.kind != ChangeKind::NoChange)
            .map(|c| (ResourceId(c.resource.clone()), declared()))
            .collect();
        PlanOutcome {
            changes,
            refresh: RefreshCoverage {
                status_by_id,
                examined: true,
                ..Default::default()
            },
            semantics_by_id,
            ..Default::default()
        }
    }

    // Depth gates the evidence, never the answer: the summary names the acting
    // resource but withholds field diffs and the unchanged listing; detail
    // shows both. Counts never inflate — unchanged is not a "change".
    #[test]
    fn depth_gates_evidence_not_the_answer() {
        let outcome = examined(vec![
            change(ChangeKind::Update, "r"),
            change(ChangeKind::NoChange, "r2"),
        ]);
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);
        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(summary.contains("# Infra Plan\n"));
        assert!(summary.contains("## Update\n- `m::r` would be updated\n"));
        assert!(!summary.contains("`image`") && !summary.contains("## Unchanged"));

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(detail.contains("  - `image`: `a` → `b`"));
        assert!(detail.contains("## Unchanged\n- `m::r2`"));
    }

    // The collapse rule end-to-end: JSON is the explanation model alone —
    // schema-versioned, evidence-indexed — whatever the depth flags said.
    #[test]
    fn json_is_the_explanation_model() {
        let outcome = examined(vec![change(ChangeKind::Update, "r")]);
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);
        let json = render(&r, Mode::resolve(true, true)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["changes"][0]["field_diffs"][0]["field"], "image");
        assert!(
            value.get("binding").is_none(),
            "binding is verb framing, not model content"
        );
    }

    // Verdict narration is attention-only: proceeding verdicts are silent
    // (describe's story); blocking verdicts and the fresh-deployment case
    // speak.
    #[test]
    fn binding_narration_is_attention_only() {
        let outcome = examined(Vec::new());
        let proceeding = report_for(&outcome, BindingVerdict::DevIterate, true);
        let text = render(&proceeding, Mode::resolve(false, true)).unwrap();
        assert!(!text.contains("binding:"), "silent on proceed: {text}");

        let blocked = report_for(&outcome, BindingVerdict::Mismatch, true);
        let text = render(&blocked, Mode::resolve(false, false)).unwrap();
        assert!(text.contains("MISMATCH"), "blocking verdicts speak: {text}");

        let fresh = report_for(&outcome, BindingVerdict::DevIterate, false);
        let text = render(&fresh, Mode::resolve(false, false)).unwrap();
        assert!(
            text.contains("not initialized"),
            "fresh case speaks: {text}"
        );
    }

    // Requirement 4.5: no changes is a statement, not a silence — and the
    // header is the plan's anchor alone (the templates agreement: coverage
    // never rides the header).
    #[test]
    fn a_quiet_plan_states_full_confirmation() {
        let outcome = examined(Vec::new());
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);
        let text = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(text.contains("No changes - everything matches the definition."));
        assert!(text.contains("**Plan for test** at revision 1\n"));
    }

    // The templates agreement on an unread resource: the summary line still
    // states only the concrete change, and the detail carries the
    // record-baseline provenance (the transitional line until the
    // platform-issue seam reports what the describe answered).
    #[test]
    fn uncertainty_presence_at_summary_members_at_detail() {
        let mut outcome = examined(vec![change(ChangeKind::Update, "r")]);
        outcome
            .refresh
            .status_by_id
            .insert(ResourceId("r".into()), RefreshStatus::Unknown);
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);

        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(
            !summary.contains("unconfirmed"),
            "coverage prose never rides the summary: {summary}"
        );
        assert!(
            summary.contains("- `m::r` would be updated\n"),
            "the line stays the concrete change: {summary}"
        );

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(
            detail.contains("  - compared against the record - live state could not be read"),
            "record-baseline provenance at detail: {detail}"
        );
    }

    // Phase 6 checkpoint (task 6.7, in-crate half): the storage plan renders
    // the Markdown target — declared behaviour in confidence voices with
    // citations, impacts per resource with consequences merged
    // permanent-first, and NO gap prose: the undeclared DSQL fields stay
    // machine-channel (model and artifact) under the knowledge-renders
    // doctrine.
    #[test]
    fn a_storage_plan_renders_knowledge_and_keeps_gaps_machine_side() {
        use tokeira_iac::{Citation, Confidence, Disruption, LifecycleOperation};
        const CITE: Citation = Citation::code("test/dsql-delete");

        let mut outcome = examined(vec![
            change(ChangeKind::Update, "compose/tokeirad"),
            change(ChangeKind::Delete, "dsql/monitored"),
        ]);
        // Mirror the real ComposeService update declaration, statement
        // included.
        outcome.semantics_by_id.insert(
            ResourceId("compose/tokeirad".into()),
            tokeira_iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Replaced,
                    citation: CITE,
                },
                replacement: Confidence::EngineFact {
                    value: tokeira_iac::ReplacementPolicy::DestroyBeforeCreate,
                    citation: CITE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: CITE,
                },
                data_effect: Confidence::EngineFact {
                    value: tokeira_iac::DataEffect::Preserved,
                    citation: CITE,
                },
                reversibility: Confidence::EngineFact {
                    value: tokeira_iac::Reversibility::Reversible,
                    citation: CITE,
                },
                statement: Some(std::borrow::Cow::Borrowed(
                    "it would be stopped, removed, and recreated from the definition",
                )),
            },
        );
        // Mirror the real DsqlCluster managed-delete declaration shape with
        // its fields-not-yet-declared world: operation/disruption stated,
        // data effect and reversibility Unknown.
        outcome.semantics_by_id.insert(
            ResourceId("dsql/monitored".into()),
            tokeira_iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: CITE,
                },
                replacement: Confidence::EngineFact {
                    value: tokeira_iac::ReplacementPolicy::NotRequired,
                    citation: CITE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: CITE,
                },
                data_effect: Confidence::Unknown,
                reversibility: Confidence::Unknown,
                statement: Some(std::borrow::Cow::Borrowed(
                    "deletion protection would be disabled first, then the cluster deleted",
                )),
            },
        );
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);

        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(summary.contains("# Infra Plan\n"), "title: {summary}");
        assert!(
            summary.contains("**Plan for test** at revision 1\n"),
            "header anchor: {summary}"
        );
        assert!(
            summary.contains("## Update\n- `m::compose/tokeirad` would be updated\n"),
            "update section: {summary}"
        );
        assert!(
            summary.contains("## Delete\n- `m::dsql/monitored` would be deleted\n"),
            "delete section: {summary}"
        );
        assert!(
            summary.contains(
                "## Impacts\n- `m::compose/tokeirad` would be unavailable while the change applies, and replaced\n- `m::dsql/monitored` would no longer be available\n"
            ),
            "impacts per resource, severity-first, consequences merged: {summary}"
        );

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(
            detail.contains(
                "  - the update replaces it - it would be stopped, removed, and recreated from the definition - `test/dsql-delete`"
            ),
            "mechanism in the engine's voice with its citation: {detail}"
        );
        assert!(
            detail.contains("  - the data it holds would be preserved - `test/dsql-delete`"),
            "data effect voiced and cited: {detail}"
        );
        assert!(
            detail.contains(
                "  - deletion protection would be disabled first, then the cluster deleted - `test/dsql-delete`"
            ),
            "the delete's kind-authored statement: {detail}"
        );

        // Knowledge renders; gaps enforce: no gap prose at either depth —
        // the undeclared fields live machine-side only.
        for text in [&summary, &detail] {
            assert!(
                !text.contains("no declaration states"),
                "gap prose leaked into narrative: {text}"
            );
        }
        use tokeira_explain::UncertaintyReason;
        let machine_gaps = r
            .explanation
            .uncertainties
            .iter()
            .filter(|u| matches!(u.reason, UncertaintyReason::SemanticsUndeclared { .. }))
            .count();
        assert_eq!(machine_gaps, 2, "the gaps stay in the model for machines");
        assert_eq!(
            r.explanation.impacts.len(),
            2,
            "unavailability + replacement"
        );
    }

    // ---- Property suite (evidence-model Properties 5, 6, 8, 11) ----
    //
    // These bind the shipped renderer itself — the same `ExplanationReport`
    // the binary prints, binding lines included — so a copy change that leaks
    // internal vocabulary or breaks depth discipline fails here, not in a
    // mirror that can drift. Operator content (resource names, field values)
    // passes through reports verbatim by design, so the generators keep
    // content to fixed clean tokens: any banned term in the output is the
    // renderer's own copy.

    use std::collections::BTreeMap;

    use proptest::prelude::*;

    /// The vocabulary ban enforced over this report's narrative: every term
    /// from `operator-language.md`'s banned list, plus the lexicon-table
    /// "banned in prose" synonyms this report's domain could plausibly leak.
    /// Cross-checked against the doc below so the suite and the doc cannot
    /// drift.
    const BANNED: &[&str] = &[
        // §The banned list.
        "envelope",
        "config_revision",
        "marker",
        "ownership transferred",
        "[A final]",
        "advisory",
        "authoritative",
        "DevIterate",
        "candidate",
        "married",
        "driving",
        "forwarding",
        "launching",
        "reconcile",
        "(s)",
        // §The lexicon, third column.
        "provider",
        "backend",
        "delta",
        "physical id",
        "state entry",
        "unknown status",
        "refresh status",
        "checkpoint",
        "snapshot",
        "classification",
        "algebra",
        "drifted",
        "downstream",
        "cascade",
        "causal group",
        "ultimate root",
    ];

    /// The reference causality fixture behind the executable transcripts in
    /// `.kiro/specs/operator-explanation/output-templates.md` — a real-world
    /// compose+DSQL deployment: an edited service (grafana, with its compose
    /// declaration speaking), a replaced cluster (an edit) cascading into
    /// tokeirad, a drifted service (mimir), and an unchanged dependant
    /// (alloy).
    fn causality_reference_report() -> ExplanationReport {
        use std::collections::{BTreeMap, BTreeSet};
        use tokeira_explain::{BaselineView, CausalityView, apply_causality};
        use tokeira_iac::{
            Citation, Confidence, Disruption, FieldDiff, LifecycleOperation, ReplacementPolicy,
            ResourceState, ResourceType,
        };

        const RECONCILE: Citation = Citation::code("tokeira_compose::reconcile");
        let service = |module: &str, id: &str, kind: ChangeKind, details: Vec<FieldDiff>| Change {
            module: module.into(),
            resource: id.into(),
            resource_type: "compose_service".into(),
            kind,
            details,
        };

        let changes = vec![
            service(
                "grafana",
                "compose/grafana",
                ChangeKind::Update,
                vec![FieldDiff {
                    field: "image".into(),
                    before: Some("grafana/grafana-oss:12.4.3".into()),
                    after: Some("grafana/grafana-oss:12.5.0".into()),
                }],
            ),
            service(
                "mimir",
                "compose/mimir",
                ChangeKind::Update,
                vec![FieldDiff::observation("environment")],
            ),
            service(
                "tokeirad",
                "compose/tokeirad",
                ChangeKind::Replace,
                Vec::new(),
            ),
            Change {
                module: "storage".into(),
                resource: "dsql/cluster".into(),
                resource_type: "dsql_cluster".into(),
                kind: ChangeKind::Replace,
                details: vec![FieldDiff {
                    field: "deletion_protection".into(),
                    before: Some("enabled".into()),
                    after: Some("disabled".into()),
                }],
            },
            service("alloy", "compose/alloy", ChangeKind::NoChange, Vec::new()),
        ];

        let rid = |id: &str| ResourceId(id.to_string());
        let mut status_by_id = BTreeMap::new();
        for id in [
            "compose/grafana",
            "compose/mimir",
            "compose/tokeirad",
            "compose/alloy",
            "dsql/cluster",
        ] {
            status_by_id.insert(rid(id), tokeira_iac::RefreshStatus::DesiredLive);
        }
        let mut live_departed = BTreeSet::new();
        live_departed.insert(rid("compose/mimir"));

        let mut display_by_id = BTreeMap::new();
        for id in [
            "compose/grafana",
            "compose/mimir",
            "compose/tokeirad",
            "compose/alloy",
        ] {
            display_by_id.insert(rid(id), "service".to_string());
        }
        display_by_id.insert(rid("dsql/cluster"), "Aurora DSQL cluster".to_string());

        let mut semantics_by_id = BTreeMap::new();
        // The real compose update declaration: effected as a
        // destroy-before-create replacement, data preserved on bind mounts.
        semantics_by_id.insert(
            rid("compose/grafana"),
            tokeira_iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Replaced,
                    citation: RECONCILE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::DestroyBeforeCreate,
                    citation: RECONCILE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: RECONCILE,
                },
                data_effect: Confidence::EngineFact {
                    value: tokeira_iac::DataEffect::Preserved,
                    citation: RECONCILE,
                },
                reversibility: Confidence::EngineFact {
                    value: tokeira_iac::Reversibility::Reversible,
                    citation: RECONCILE,
                },
                statement: Some(std::borrow::Cow::Borrowed(
                    "it would be stopped, removed, and recreated from the definition",
                )),
            },
        );

        let outcome = PlanOutcome {
            changes,
            refresh: RefreshCoverage {
                status_by_id,
                examined: true,
                live_departed,
            },
            semantics_by_id,
            display_by_id,
            ..Default::default()
        };
        let mut r = report_for(&outcome, BindingVerdict::DevIterate, true);
        r.explanation.current_revision = 4;
        r.explanation.platform = "compose".to_string();

        let manifest = |v: &str| serde_json::json!({ "field": v });
        let state = |id: &str| ResourceState {
            resource_type: ResourceType::new("compose_service"),
            physical_id: id.to_string(),
            properties: serde_json::json!({}),
            dependencies: Vec::new(),
            created_at: "t0".into(),
            updated_at: "t0".into(),
            module: "m".into(),
        };
        let same = [
            "compose/mimir",
            "compose/loki",
            "compose/tokeirad",
            "compose/alloy",
        ];
        let mut desired = BTreeMap::new();
        let mut baseline = BTreeMap::new();
        for id in same {
            desired.insert(rid(id), manifest("same"));
            baseline.insert(rid(id), manifest("same"));
        }
        desired.insert(rid("compose/grafana"), manifest("image-12.5.0"));
        baseline.insert(rid("compose/grafana"), manifest("image-12.4.3"));
        desired.insert(rid("dsql/cluster"), manifest("protection-off"));
        baseline.insert(rid("dsql/cluster"), manifest("protection-on"));
        let recorded: BTreeMap<ResourceId, ResourceState> = [
            "compose/grafana",
            "compose/mimir",
            "compose/tokeirad",
            "compose/alloy",
            "dsql/cluster",
        ]
        .into_iter()
        .map(|id| (rid(id), state(id)))
        .collect();
        let mut edges: BTreeMap<ResourceId, Vec<ResourceId>> = BTreeMap::new();
        edges.insert(rid("compose/tokeirad"), vec![rid("dsql/cluster")]);
        edges.insert(rid("compose/alloy"), vec![rid("compose/mimir")]);
        let desired_edges = edges.clone();

        let view = CausalityView {
            desired: Some(desired),
            baseline: BaselineView::Realized(baseline),
            baseline_revision: 4,
            recorded,
            edges,
            desired_edges,
            refresh: outcome.refresh.clone(),
        };
        apply_causality(&mut r.explanation, &view);
        r
    }

    // Causality Req 6.1–6.4/6.7 over the real-world reference: clauses on the
    // lines, the record-baseline provenance line, derived voices, dependants,
    // the chain, and `--json` carrying assessments, groups, and dependants.
    #[test]
    fn causality_renders_clauses_voices_dependants_and_the_chain() {
        let r = causality_reference_report();
        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(
            summary.contains("**Plan for compose** at revision 4\n"),
            "header anchor: {summary}"
        );
        assert!(
            summary.contains("- the *grafana* service would be updated - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0` - `grafana::compose/grafana`\n"),
            "edit clause is the diff: {summary}"
        );
        assert!(
            summary.contains("## Resource Drift\n- the *mimir* service's `environment` changed outside the definition - it would be updated - `mimir::compose/mimir`\n"),
            "drift speaks in its own section, fields named: {summary}"
        );
        assert!(
            summary.contains("- the *tokeirad* service would be replaced - forced by the *Aurora DSQL cluster* replacement - `tokeirad::compose/tokeirad`\n"),
            "cascade clause: {summary}"
        );

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(
            !detail.contains("could not be established"),
            "no machine voice in narrative: {detail}"
        );
        assert!(
            detail.contains("  - the update replaces it - it would be stopped, removed, and recreated from the definition - `tokeira_compose::reconcile`"),
            "the compose declaration speaks: {detail}"
        );
        assert!(
            detail.contains("Tokeira derives this - per `tokeira_explain::causality`"),
            "cascade owned as derived: {detail}"
        );
        assert!(
            detail.contains("  - dependants changing with it: the *tokeirad* service"),
            "changing dependant stated: {detail}"
        );
        assert!(
            detail.contains("  - dependants continuing unchanged: the *alloy* service"),
            "unchanged dependant stated: {detail}"
        );
        assert!(
            detail.contains("  - chain: the definition change: "),
            "the chain told once, root first: {detail}"
        );

        let json: serde_json::Value =
            serde_json::from_str(&render(&r, Mode::resolve(true, false)).unwrap()).unwrap();
        // The reference world groups exactly twice: the revision-comparison
        // root (grafana, the cluster, tokeirad) and mimir's own drift root.
        assert_eq!(
            json["causal_groups"].as_array().map(Vec::len).unwrap_or(0),
            2,
            "groups serialized: {json}"
        );
        assert!(
            json["changes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["cause"].get("engine-fact").is_some()),
            "assessments serialized: {json}"
        );
    }

    // The output-templates doc is executable: its reference transcripts ARE
    // the renderer's output for the reference fixture, byte-for-byte — the
    // managed-template guarantee (umbrella D10). A template change is an
    // amendment to the doc first; this test is what makes that mechanical.
    #[test]
    fn the_output_templates_doc_is_executable() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../.kiro/specs/operator-explanation/output-templates.md"
        );
        let doc = std::fs::read_to_string(path).expect("output-templates.md exists");
        let r = causality_reference_report();
        for (marker, detail) in [
            ("<!-- reference: infra-plan-summary -->", false),
            ("<!-- reference: infra-plan-detail -->", true),
        ] {
            let rendered = render(&r, Mode::resolve(false, detail)).unwrap();
            let block = fenced_block_after(&doc, marker).unwrap_or_else(|| {
                panic!("{marker} block missing from output-templates.md.\nRendered:\n{rendered}")
            });
            assert_eq!(
                block.trim_end(),
                rendered.trim_end(),
                "output-templates.md and the renderer have drifted ({marker})"
            );
        }
    }

    fn fenced_block_after(doc: &str, marker: &str) -> Option<String> {
        let after = &doc[doc.find(marker)? + marker.len()..];
        let start = after.find("```markdown\n")? + "```markdown\n".len();
        let rest = &after[start..];
        let end = rest.find("\n```")?;
        Some(rest[..end].to_string())
    }

    #[test]
    fn the_banned_list_matches_the_language_doc() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/platforms/operator-language.md"
        );
        let doc = std::fs::read_to_string(path).expect("operator-language.md exists");
        for term in BANNED {
            assert!(
                doc.contains(*term),
                "banned term {term:?} is not recorded in operator-language.md — \
                 the suite and the doc have drifted"
            );
        }
    }

    fn arb_kind() -> impl Strategy<Value = ChangeKind> {
        prop_oneof![
            Just(ChangeKind::Create),
            Just(ChangeKind::Update),
            Just(ChangeKind::Replace),
            Just(ChangeKind::Delete),
            Just(ChangeKind::NoChange),
        ]
    }

    fn arb_verdict() -> impl Strategy<Value = BindingVerdict> {
        prop_oneof![
            Just(BindingVerdict::Match),
            Just(BindingVerdict::DevIterate),
            Just(BindingVerdict::Mismatch),
            Just(BindingVerdict::Downgrade),
            Just(BindingVerdict::ModeRegression),
            Just(BindingVerdict::Unknown),
        ]
    }

    prop_compose! {
        /// A report over an arbitrary plan outcome: every change kind, per-
        /// resource refresh statuses, both examined states, every binding
        /// verdict, and diff values long enough to cross the truncation
        /// boundary.
        fn arb_report()(
            rows in proptest::collection::vec((arb_kind(), any::<bool>(), 0usize..90), 0..6),
            examined in any::<bool>(),
            binding in arb_verdict(),
            initialized in any::<bool>(),
        ) -> ExplanationReport {
            let mut changes = Vec::new();
            let mut status_by_id = BTreeMap::new();
            for (index, (kind, unknown, value_len)) in rows.into_iter().enumerate() {
                let resource = format!("compose/r{index}");
                changes.push(Change {
                    kind,
                    resource_type: "compose_service".to_string(),
                    module: format!("m{index}"),
                    resource: resource.clone(),
                    details: vec![
                        tokeira_iac::FieldDiff::observation("dependencies changed"),
                        tokeira_iac::FieldDiff {
                            field: "image".to_string(),
                            before: Some("x".repeat(value_len)),
                            after: Some("y".repeat(value_len)),
                        },
                    ],
                });
                status_by_id.insert(
                    ResourceId(resource),
                    if unknown { RefreshStatus::Unknown } else { RefreshStatus::DesiredLive },
                );
            }
            let outcome = PlanOutcome {
                changes,
                refresh: RefreshCoverage {
                    status_by_id,
                    examined,
                    ..Default::default()
                },
                ..Default::default()
            };
            ExplanationReport {
                initialized,
                binding,
                explanation: explain_plan(context(), &outcome),
            }
        }
    }

    proptest! {
        // Property 5 — detail is a superset of summary: depth gates evidence,
        // never the answer, so every line the summary states appears verbatim
        // in the detail rendering.
        #[test]
        fn property_5_detail_is_a_superset_of_summary(report in arb_report()) {
            let summary = render(&report, Mode::resolve(false, false)).unwrap();
            let detail = render(&report, Mode::resolve(false, true)).unwrap();
            let detail_lines: std::collections::HashSet<&str> = detail.lines().collect();
            for line in summary.lines() {
                prop_assert!(
                    detail_lines.contains(line),
                    "summary line missing from detail: {:?}\ndetail:\n{}",
                    line,
                    detail,
                );
            }
        }

        // Property 6 — the structured form is complete and depth-blind:
        // identical JSON whatever the depth flag said, round-tripping to a
        // model equal to the one the report carries (the report serializes as
        // the explanation model alone).
        #[test]
        fn property_6_json_is_complete_and_depth_blind(report in arb_report()) {
            let a = render(&report, Mode::resolve(true, false)).unwrap();
            let b = render(&report, Mode::resolve(true, true)).unwrap();
            prop_assert_eq!(&a, &b, "JSON must be depth-blind");
            let back: DeploymentExplanation = serde_json::from_str(&a).unwrap();
            prop_assert_eq!(&back, &report.explanation, "JSON must round-trip to an equal model");
        }

        // Property 8 — not-determined slots make no claims. Evolved twice:
        // an undeclared field may appear as a *gap* (a SemanticsUndeclared
        // uncertainty naming it), and the engine floor (Requirement 5.5) may
        // state unavailability/replacement for Replace and Delete kinds —
        // those are grounded in the kind, which is never Unknown. What can
        // never appear is a declaration-only consequence, and the
        // still-dormant slots (cause, dependants, source) stay wholly
        // silent.
        #[test]
        fn property_8_not_determined_slots_are_silent(report in arb_report()) {
            for detail in [false, true] {
                let text = render(&report, Mode::resolve(false, detail)).unwrap().to_lowercase();
                for claim in ["would be destroyed",
                              "would be briefly interrupted",
                              "one at a time"] {
                    prop_assert!(
                        !text.contains(claim),
                        "undeclared semantics produced a consequence claim {:?}: {}",
                        claim,
                        text,
                    );
                }
                for slot_word in ["cause", "derived", "tokeira derives", "because of"] {
                    prop_assert!(
                        !text.contains(slot_word),
                        "dormant slot vocabulary {:?} leaked into narrative: {}",
                        slot_word,
                        text,
                    );
                }
            }
        }

        // Property 11 — the narrative stays inside the lexicon at every depth,
        // over every binding verdict the generator ranges across.
        #[test]
        fn property_11_rendering_stays_inside_the_lexicon(report in arb_report()) {
            for detail in [false, true] {
                let text = render(&report, Mode::resolve(false, detail)).unwrap().to_lowercase();
                for term in BANNED {
                    prop_assert!(
                        !text.contains(&term.to_lowercase()),
                        "banned term {:?} appeared in narrative: {}",
                        term,
                        text,
                    );
                }
            }
        }
    }
}
