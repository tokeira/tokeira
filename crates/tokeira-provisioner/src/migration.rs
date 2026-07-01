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

/// Forward-only migrations keyed by their starting schema (at most one per
/// `from_schema` — a linear chain).
#[derive(Default)]
pub struct MigrationRegistry {
    by_from: BTreeMap<u32, Migration>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a forward migration `from_schema → to_schema`. Panics if the
    /// transition is not forward, or a migration is already registered from
    /// `from_schema` (the chain must be unambiguous).
    pub fn register(mut self, from_schema: u32, to_schema: u32, apply: MigrationFn) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set_flag(mut doc: serde_json::Value, key: &str) -> serde_json::Value {
        doc.as_object_mut().unwrap().insert(key.to_string(), json!(true));
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
        assert_eq!(reg.check_path(1, 3), Err(MigrationError::NoPath { from: 1, to: 3 }));
        assert_eq!(reg.migrate(json!({}), 1, 3), Err(MigrationError::NoPath { from: 1, to: 3 }));
    }

    #[test]
    fn missing_first_step_is_no_path() {
        let reg = MigrationRegistry::new().register(2, 3, Ok);
        assert_eq!(reg.check_path(1, 3), Err(MigrationError::NoPath { from: 1, to: 3 }));
    }

    #[test]
    fn backward_transition_is_refused() {
        let reg = MigrationRegistry::new();
        assert_eq!(reg.check_path(3, 1), Err(MigrationError::Backward { from: 3, to: 1 }));
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
}
