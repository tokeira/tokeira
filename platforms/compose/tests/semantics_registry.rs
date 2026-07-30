//! The executable half of the change-semantics kind inventory
//! (explanation-change-semantics Requirement 7). Every kind the operating
//! platform realizes — across the reference definition and both DSQL storage
//! variants — is classified in [`REGISTRY`], and the classification is
//! enforced: Property 9 (tier coverage holds) and Property 10 (every claim
//! cites) probe the realized instances through the platform's own factory,
//! never hand-built doubles. Adding a kind to the platform without a registry
//! row fails the accounting test; adding a row for a Tier 1/2 kind without a
//! full declaration fails Property 9.

use std::{path::PathBuf, sync::LazyLock};

use proptest::{prelude::*, sample::select};
use tokeira_compose_deployment::{DEFAULT_TKD, context::Cx, interp};
use tokeira_iac::{ChangeKind, ChangeSemantics, Citation, FieldDiff, Resource, SemanticsContext};

/// Requirements-table tier (explanation-change-semantics, "Kind Inventory and
/// Declaration Policy"). Tier 3 kinds are recorded as deliberately unknown in
/// the requirements table; none are realized by this platform today, so the
/// registry carries no Tier 3 arm — the accounting test fails the moment one
/// appears, forcing a classification decision in the same change (Req 7.4).
#[derive(Clone, Copy, Debug)]
enum Tier {
    /// Tier 1 — the operating set.
    OperatingSet,
    /// Tier 2 — the storage path.
    StoragePath,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::OperatingSet => "Tier 1 (operating set)",
            Tier::StoragePath => "Tier 2 (storage path)",
        }
    }
}

/// One classified kind: its `resource_type` identity, its tier, the
/// `ChangeKind`s an operator can be shown for it, and the field-diff names
/// its own `diff` produces.
struct RegisteredKind {
    resource_type: &'static str,
    tier: Tier,
    /// The `ChangeKind`s the kind declares for. The complement (including
    /// `NoChange` everywhere) must answer the all-`Unknown` default — a
    /// scenario the engine cannot produce is reported inapplicable, never
    /// guessed at.
    applicable: &'static [ChangeKind],
    /// Real diff field names, folded into generated contexts alongside
    /// arbitrary names so a declaration that branches on them (DynamoDB's
    /// TTL) has both branches probed by construction, not luck.
    diff_vocabulary: &'static [&'static str],
}

const ALL_KINDS: [ChangeKind; 5] = [
    ChangeKind::Create,
    ChangeKind::Update,
    ChangeKind::Replace,
    ChangeKind::Delete,
    ChangeKind::NoChange,
];

const REGISTRY: &[RegisteredKind] = &[
    RegisteredKind {
        resource_type: "compose_service",
        tier: Tier::OperatingSet,
        applicable: &[
            ChangeKind::Create,
            ChangeKind::Update,
            ChangeKind::Replace,
            ChangeKind::Delete,
        ],
        // Manifest-key diffs from `manifest_field_diffs`.
        diff_vocabulary: &["image", "ports", "environment", "volumes", "command"],
    },
    RegisteredKind {
        resource_type: "observability_config_files",
        tier: Tier::OperatingSet,
        applicable: &[
            ChangeKind::Create,
            ChangeKind::Update,
            ChangeKind::Replace,
            ChangeKind::Delete,
        ],
        diff_vocabulary: &["config/mimir.yaml changed", "missing: config/loki.yaml"],
    },
    RegisteredKind {
        resource_type: "local_state_dir",
        tier: Tier::OperatingSet,
        // The diff only ever answers NoChange after creation, but the kind
        // declares Update/Replace anyway (totality); the declarations are
        // held to the same bar.
        applicable: &[
            ChangeKind::Create,
            ChangeKind::Update,
            ChangeKind::Replace,
            ChangeKind::Delete,
        ],
        diff_vocabulary: &[],
    },
    RegisteredKind {
        resource_type: "DsqlCluster",
        tier: Tier::StoragePath,
        applicable: &[
            ChangeKind::Create,
            ChangeKind::Update,
            ChangeKind::Replace,
            ChangeKind::Delete,
        ],
        diff_vocabulary: &[],
    },
    RegisteredKind {
        resource_type: "DynamoDbTable",
        tier: Tier::StoragePath,
        // Replace is deliberately absent: the table's diff never produces
        // one, and the declaration reports it inapplicable via the default.
        applicable: &[ChangeKind::Create, ChangeKind::Update, ChangeKind::Delete],
        diff_vocabulary: &["tags changed", "ttl attribute changed"],
    },
];

/// Every resource the operating platform realizes, pooled across the
/// reference definition and both DSQL storage variants (managed and
/// preexisting — the DSQL declaration is mode-aware, so both constructions
/// must be probed). Realization is pure: no provider, no filesystem.
static REALIZED: LazyLock<Vec<Box<dyn Resource>>> = LazyLock::new(|| {
    let cx = Cx {
        project_name: "registry".into(),
        region: Some("us-east-1".into()),
        deployment_dir: PathBuf::from("/tmp/semantics-registry"),
    };
    let managed = DEFAULT_TKD.replace(
        "storage: Storage::InMemory,",
        "storage: Storage::Dsql { region: \"us-east-1\".into(), mode: DsqlMode::Managed, \
         endpoint: None, arn: None },",
    );
    let preexisting = DEFAULT_TKD.replace(
        "storage: Storage::InMemory,",
        "storage: Storage::Dsql { region: \"us-east-1\".into(), mode: DsqlMode::Preexisting, \
         endpoint: Some(\"x.dsql.us-east-1.on.aws\".into()), arn: None },",
    );

    let mut resources = Vec::new();
    for source in [DEFAULT_TKD.to_string(), managed, preexisting] {
        let (dep, _config) = interp::interpret(&source, &cx).expect("reference definition parses");
        for module in dep.module_names() {
            resources.extend(
                dep.realize_module(module, &cx)
                    .expect("named module realizes"),
            );
        }
    }
    resources
});

fn row_for(resource_type: &str) -> Option<&'static RegisteredKind> {
    REGISTRY.iter().find(|r| r.resource_type == resource_type)
}

/// The five semantic fields, uniformly: name, whether declared, citation.
fn fields(semantics: &ChangeSemantics) -> [(&'static str, bool, Option<&Citation>); 5] {
    [
        (
            "operation",
            semantics.operation.is_known(),
            semantics.operation.citation(),
        ),
        (
            "replacement",
            semantics.replacement.is_known(),
            semantics.replacement.citation(),
        ),
        (
            "disruption",
            semantics.disruption.is_known(),
            semantics.disruption.citation(),
        ),
        (
            "data_effect",
            semantics.data_effect.is_known(),
            semantics.data_effect.citation(),
        ),
        (
            "reversibility",
            semantics.reversibility.is_known(),
            semantics.reversibility.citation(),
        ),
    ]
}

/// Requirement 7.1/7.4: the registry and the platform agree exactly — every
/// realized kind is classified, and every classified kind is still realized.
#[test]
fn the_registry_accounts_for_every_realized_kind() {
    let mut realized: Vec<&str> = Vec::new();
    for resource in REALIZED.iter() {
        let resource_type = resource.resource_type().0;
        let row = row_for(&resource_type).unwrap_or_else(|| {
            panic!(
                "the platform realizes `{resource_type}` but the semantics registry has no \
                 row for it — classify the kind (tier, applicable change kinds, diff \
                 vocabulary) in the same change that adds it (Req 7.4)"
            )
        });
        realized.push(row.resource_type);
    }
    for row in REGISTRY {
        assert!(
            realized.contains(&row.resource_type),
            "the registry classifies `{}` but the platform no longer realizes it — \
             remove the stale row",
            row.resource_type
        );
    }
}

/// A generated probe: a registry row, an applicable change kind, and a diff
/// list mixing the kind's real field names with arbitrary ones.
fn arb_probe() -> impl Strategy<Value = (usize, ChangeKind, Vec<FieldDiff>)> {
    (0..REGISTRY.len()).prop_flat_map(|index| {
        let row = &REGISTRY[index];
        let name = if row.diff_vocabulary.is_empty() {
            "[a-z ]{1,16}".boxed()
        } else {
            prop_oneof![
                select(row.diff_vocabulary).prop_map(str::to_owned),
                "[a-z ]{1,16}",
            ]
            .boxed()
        };
        let diffs = prop::collection::vec(name.prop_map(FieldDiff::observation), 0..4);
        (Just(index), select(row.applicable), diffs)
    })
}

proptest! {
    // Feature: explanation-change-semantics, Property 9
    //
    // Tier coverage holds: for any registered Tier 1/2 kind, any applicable
    // change kind, and any field-diff list, every semantic field is declared
    // above Unknown — and every inapplicable change kind answers the
    // all-Unknown default.
    #[test]
    fn property_9_tier_coverage_holds((index, kind, diffs) in arb_probe()) {
        let row = &REGISTRY[index];
        for resource in REALIZED.iter().filter(|r| r.resource_type().0 == row.resource_type) {
            let semantics = resource.change_semantics(&SemanticsContext {
                kind,
                current: None,
                field_diffs: &diffs,
            });
            for (field, known, _) in fields(&semantics) {
                prop_assert!(
                    known,
                    "{} kind `{}` leaves `{field}` unknown for {kind:?} — a registered \
                     kind declares every applicable field (Req 7.2)",
                    row.tier.label(),
                    row.resource_type,
                );
            }

            for inapplicable in ALL_KINDS.iter().filter(|k| !row.applicable.contains(k)) {
                let answered = resource.change_semantics(&SemanticsContext {
                    kind: *inapplicable,
                    current: None,
                    field_diffs: &diffs,
                });
                prop_assert_eq!(
                    &answered,
                    &ChangeSemantics::default(),
                    "kind `{}` answers a non-default declaration for inapplicable \
                     {:?} — inapplicable scenarios report the all-Unknown default",
                    row.resource_type,
                    inapplicable,
                );
            }
        }
    }

    // Feature: explanation-change-semantics, Property 10
    //
    // Every claim cites: for the same probe space, every declared field
    // carries a citation, code citations are non-empty, and documentation
    // citations carry a non-empty title and URL (and a non-empty quote when
    // one is given).
    #[test]
    fn property_10_every_claim_cites((index, kind, diffs) in arb_probe()) {
        let row = &REGISTRY[index];
        for resource in REALIZED.iter().filter(|r| r.resource_type().0 == row.resource_type) {
            let semantics = resource.change_semantics(&SemanticsContext {
                kind,
                current: None,
                field_diffs: &diffs,
            });
            for (field, known, citation) in fields(&semantics) {
                if !known {
                    continue;
                }
                match citation {
                    None => prop_assert!(
                        false,
                        "kind `{}` declares `{field}` without a citation",
                        row.resource_type
                    ),
                    Some(Citation::Code(reference)) => prop_assert!(
                        !reference.is_empty(),
                        "kind `{}` cites empty code for `{field}`",
                        row.resource_type
                    ),
                    Some(Citation::Doc { title, url, quote }) => {
                        prop_assert!(
                            !title.is_empty() && !url.is_empty(),
                            "kind `{}` cites a document without a title or URL for `{field}`",
                            row.resource_type
                        );
                        if let Some(quote) = quote {
                            prop_assert!(
                                !quote.is_empty(),
                                "kind `{}` carries an empty establishing quote for `{field}`",
                                row.resource_type
                            );
                        }
                    }
                }
            }
        }
    }
}
