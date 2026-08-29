//! Forward-only state-schema migrations, run at the upgrade boundary (task 5.1).
//!
//! An `upgrade` runs a forward migration **before any mutation** only when the
//! state schema changes; a new `source_tree_hash` at the same schema is a
//! re-stamp, not a migration. Migrations are forward-only, keyed by the schema
//! they start from (a linear chain), and an **unbridged** transition (no
//! registered migration for a required step) is refused before the upgrade
//! proceeds (task 5.2, Property 4's sibling — no reverse migration, Req 9.4).

use std::collections::BTreeMap;

/// Transforms a raw state document from one schema to the next. Returns the
/// migrated document, or an error reason on failure.
pub type MigrationFn = fn(serde_json::Value) -> Result<serde_json::Value, String>;

/// Failure planning or running a schema migration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MigrationError {
    #[error(
        "no migration path from state schema {from} to {to} — a required schema migration is \
         unbridged"
    )]
    NoPath { from: u32, to: u32 },
    #[error("cannot migrate backward from schema {from} to {to}; migrations are forward-only")]
    Backward { from: u32, to: u32 },
    #[error("migration {from}→{to} failed: {reason}")]
    Failed { from: u32, to: u32, reason: String },
}

struct Migration {
    to_schema: u32,
    apply: MigrationFn,
}

// Manual impl: `apply` is a function value with no `Debug`.
impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migration")
            .field("to_schema", &self.to_schema)
            .finish_non_exhaustive()
    }
}

/// Forward-only migrations keyed by their starting schema (at most one per
/// `from_schema` — a linear chain).
#[derive(Default, Debug)]
pub struct MigrationRegistry {
    by_from: BTreeMap<u32, Migration>,
}

impl MigrationRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register a forward migration `from_schema → to_schema`. Panics if the
    /// transition is not forward, or a migration is already registered from
    /// `from_schema` (the chain must be unambiguous).
    pub(crate) fn register(mut self, from_schema: u32, to_schema: u32, apply: MigrationFn) -> Self {
        assert!(
            to_schema > from_schema,
            "migrations are forward-only ({from_schema} → {to_schema})"
        );
        assert!(
            self.by_from
                .insert(from_schema, Migration { to_schema, apply })
                .is_none(),
            "duplicate migration registered from schema {from_schema}"
        );
        self
    }

    /// Whether a schema transition needs a migration. A new `source_tree_hash` at
    /// the same schema is a re-stamp, not a migration.
    pub fn needs_migration(from: u32, to: u32) -> bool {
        from != to
    }

    /// Verify a forward path exists from `from` to `to` without applying it —
    /// used at the upgrade boundary to refuse an unbridged schema migration
    /// before any provider mutation. `Ok(())` when `from == to` (no migration).
    pub fn check_path(&self, from: u32, to: u32) -> Result<(), MigrationError> {
        self.chain(from, to).map(|_| ())
    }

    fn chain(&self, from: u32, to: u32) -> Result<Vec<(u32, &Migration)>, MigrationError> {
        if from == to {
            return Ok(Vec::new());
        }
        if to < from {
            return Err(MigrationError::Backward { from, to });
        }
        let mut steps = Vec::new();
        let mut current = from;
        while current < to {
            let migration = self
                .by_from
                .get(&current)
                .ok_or(MigrationError::NoPath { from, to })?;
            // Overshooting the target means there is no exact bridge to `to`.
            if migration.to_schema > to {
                return Err(MigrationError::NoPath { from, to });
            }
            steps.push((current, migration));
            current = migration.to_schema;
        }
        Ok(steps)
    }

    /// Migrate a raw state document from schema `from` to `to` by applying the
    /// forward chain in order. Errors if the path is missing/backward or a step
    /// fails.
    pub fn migrate(
        &self,
        mut doc: serde_json::Value,
        from: u32,
        to: u32,
    ) -> Result<serde_json::Value, MigrationError> {
        for (step_from, migration) in self.chain(from, to)? {
            doc = (migration.apply)(doc).map_err(|reason| MigrationError::Failed {
                from: step_from,
                to: migration.to_schema,
                reason,
            })?;
        }
        Ok(doc)
    }
}

// ── The canonical envelope chain ──────────────────────────────────────────
//
// Task 5.1's registry, first populated by the manifest re-key of task 16.2.
// The upgrade boundary (`tkp upgrade`) refuses an unbridged transition via
// `check_path` and runs this chain forward on a schema change.

/// The canonical [`DeploymentStateEnvelope`](crate::DeploymentStateEnvelope)
/// migration chain.
pub fn envelope_migrations() -> MigrationRegistry {
    MigrationRegistry::new().register(1, 2, envelope_v1_to_v2)
}

/// v1 → v2 (task 16.2): the integrity manifest is re-keyed by engine identity.
/// Per-artifact `version` keys are dropped (the artifact's key half is the
/// manifest's `engine_identity`); the new `engine_identity`/`authority` fields
/// are **absent**, which the v2 shape reads as no-identity / lowest-tier —
/// correct for every v1 document, all of which predate identity-keyed builds.
/// Applies to the live manifest and the checkpoint's retained `[A final]` copy.
fn envelope_v1_to_v2(mut doc: serde_json::Value) -> Result<serde_json::Value, String> {
    fn strip_artifact_versions(manifest: &mut serde_json::Value) {
        if let Some(artifacts) = manifest
            .get_mut("artifacts")
            .and_then(serde_json::Value::as_array_mut)
        {
            for artifact in artifacts {
                if let Some(fields) = artifact.as_object_mut() {
                    fields.remove("version");
                }
            }
        }
    }

    let root = doc
        .as_object_mut()
        .ok_or_else(|| "envelope document is not an object".to_string())?;
    root.insert("schema_version".to_string(), serde_json::json!(2));
    if let Some(manifest) = root.get_mut("integrity") {
        strip_artifact_versions(manifest);
    }
    if let Some(from_integrity) = root
        .get_mut("checkpoint")
        .and_then(|c| c.get_mut("from_integrity"))
    {
        strip_artifact_versions(from_integrity);
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set_flag(mut doc: serde_json::Value, key: &str) -> serde_json::Value {
        doc.as_object_mut()
            .unwrap()
            .insert(key.to_string(), json!(true));
        doc
    }

    #[test]
    fn same_schema_needs_no_migration() {
        let reg = MigrationRegistry::new();
        assert!(!MigrationRegistry::needs_migration(1, 1));
        reg.check_path(1, 1).expect("same schema is a no-op");
        assert_eq!(reg.migrate(json!({"v": 1}), 1, 1).unwrap(), json!({"v": 1}));
    }

    #[test]
    fn linear_chain_applies_each_step_in_order() {
        let reg = MigrationRegistry::new()
            .register(1, 2, |d| Ok(set_flag(d, "m12")))
            .register(2, 3, |d| Ok(set_flag(d, "m23")));
        reg.check_path(1, 3).expect("bridged");
        let out = reg.migrate(json!({}), 1, 3).unwrap();
        assert_eq!(out, json!({"m12": true, "m23": true}));
    }

    #[test]
    fn unbridged_transition_is_no_path() {
        let reg = MigrationRegistry::new().register(1, 2, Ok);
        assert_eq!(
            reg.check_path(1, 3),
            Err(MigrationError::NoPath { from: 1, to: 3 })
        );
        assert_eq!(
            reg.migrate(json!({}), 1, 3),
            Err(MigrationError::NoPath { from: 1, to: 3 })
        );
    }

    #[test]
    fn missing_first_step_is_no_path() {
        let reg = MigrationRegistry::new().register(2, 3, Ok);
        assert_eq!(
            reg.check_path(1, 3),
            Err(MigrationError::NoPath { from: 1, to: 3 })
        );
    }

    #[test]
    fn backward_transition_is_refused() {
        let reg = MigrationRegistry::new();
        assert_eq!(
            reg.check_path(3, 1),
            Err(MigrationError::Backward { from: 3, to: 1 })
        );
    }

    #[test]
    fn a_failing_step_surfaces_its_reason() {
        let reg = MigrationRegistry::new().register(1, 2, |_| Err("bad doc".to_string()));
        assert_eq!(
            reg.migrate(json!({}), 1, 2),
            Err(MigrationError::Failed {
                from: 1,
                to: 2,
                reason: "bad doc".to_string()
            })
        );
    }

    #[test]
    fn envelope_v1_to_v2_rekeys_both_manifests_and_loads() {
        let v1 = json!({
            "schema_version": 1,
            "deployment_id": "dep-1",
            "binding": null,
            "integrity": {
                "provisioner_version": "0.1.0",
                "artifacts": [
                    {"version": "0.1.0", "target": "t1", "sha256": "aa", "retrieval_ref": null, "size_bytes": 1}
                ]
            },
            "config_revision": 1,
            "checkpoint": {
                "from_provenance": {
                    "version": "0.1.0", "git_sha": "s", "source_tree_hash": "h",
                    "build_mode": "Dev", "recorded_at": "2026-07-01T00:00:00Z"
                },
                "from_integrity": {
                    "provisioner_version": "0.0.9",
                    "artifacts": [
                        {"version": "0.0.9", "target": "t1", "sha256": "bb", "retrieval_ref": null, "size_bytes": 2}
                    ]
                },
                "from_infra_head": null,
                "from_runtime_head": null,
                "from_config_ref": null,
                "recorded_at": "2026-07-01T00:00:00Z"
            },
            "operation": null,
            "lock": null,
            "infra_head": null,
            "runtime_head": null,
            "effective_config_ref": null
        });

        let migrated = envelope_migrations()
            .migrate(v1, 1, 2)
            .expect("the canonical chain bridges 1 → 2");
        assert_eq!(migrated["schema_version"], 2);
        assert!(
            migrated["integrity"]["artifacts"][0]
                .get("version")
                .is_none()
        );
        assert!(
            migrated["checkpoint"]["from_integrity"]["artifacts"][0]
                .get("version")
                .is_none(),
            "the checkpoint's retained [A final] manifest is re-keyed too"
        );

        // The migrated document is a valid current-schema envelope.
        let env: crate::DeploymentStateEnvelope =
            serde_json::from_value(migrated).expect("migrated document loads");
        assert_eq!(env.schema_version, crate::ENVELOPE_SCHEMA_VERSION);
        let manifest = env.integrity.expect("manifest kept");
        assert!(manifest.engine_identity.is_none());
        assert_eq!(manifest.authority, crate::BuildAuthority::LocalDeveloper);
    }

    #[test]
    fn canonical_chain_bridges_exactly_one_to_current() {
        let reg = envelope_migrations();
        reg.check_path(1, crate::ENVELOPE_SCHEMA_VERSION)
            .expect("v1 envelopes are bridged to current");
        reg.check_path(
            crate::ENVELOPE_SCHEMA_VERSION,
            crate::ENVELOPE_SCHEMA_VERSION,
        )
        .expect("current → current is a no-op");
    }
}
