//! Conformance-only dynamic-override read seam.
//!
//! Production runtime code pins its tuned limits to release defaults. The
//! functional-conformance harness needs the same sites to honour live per-test
//! overrides delivered out of process (spec
//! `.kiro/specs/conformance-config-override/`). The registry that stores those
//! overrides is test-harness machinery and is never published, so this crate
//! cannot name it: the application assembling a conformance server installs
//! plain read functions here before serving instead. With nothing installed
//! every read reports "no override", so a `conformance`-featured build whose
//! host never installs a source behaves exactly like production.

use std::sync::OnceLock;

/// Read functions over the harness's override registry, installed once by the
/// serving application before boot. Function pointers rather than a trait
/// object so an install needs no allocation or leaked adapter.
#[derive(Clone, Copy)]
pub struct OverrideReads {
    /// Resolve a boolean override for the given dynamic-config key.
    pub read_bool: fn(&str) -> Option<bool>,
    /// Resolve an integer override for the given dynamic-config key.
    pub read_i64: fn(&str) -> Option<i64>,
    /// Resolve a float override for the given dynamic-config key.
    pub read_f64: fn(&str) -> Option<f64>,
    /// Resolve a duration override for the given dynamic-config key.
    pub read_duration: fn(&str) -> Option<std::time::Duration>,
    /// The registry's scope generation: a counter bumped when the harness
    /// resets override scope between tests, letting cached decisions detect a
    /// new test's configuration epoch.
    pub read_scope_generation: fn() -> u64,
}

impl std::fmt::Debug for OverrideReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverrideReads").finish_non_exhaustive()
    }
}

impl OverrideReads {
    pub(crate) fn get_bool(&self, key: &str) -> Option<bool> {
        (self.read_bool)(key)
    }

    pub(crate) fn get_i64(&self, key: &str) -> Option<i64> {
        (self.read_i64)(key)
    }

    pub(crate) fn get_f64(&self, key: &str) -> Option<f64> {
        (self.read_f64)(key)
    }

    pub(crate) fn get_duration(&self, key: &str) -> Option<std::time::Duration> {
        (self.read_duration)(key)
    }

    pub(crate) fn scope_generation(&self) -> u64 {
        (self.read_scope_generation)()
    }
}

/// Inert default: no overrides configured, scope generation pinned to zero.
const NO_OVERRIDES: OverrideReads = OverrideReads {
    read_bool: |_| None,
    read_i64: |_| None,
    read_f64: |_| None,
    read_duration: |_| None,
    read_scope_generation: || 0,
};

static READS: OnceLock<OverrideReads> = OnceLock::new();

/// Install the override read functions. The first install wins; later calls
/// are ignored so repeated harness setup stays idempotent against the same
/// registry.
pub fn install(reads: OverrideReads) {
    let _ = READS.set(reads);
}

/// The installed reads, or the inert default when no harness installed any.
pub(crate) fn reads() -> &'static OverrideReads {
    READS.get().unwrap_or(&NO_OVERRIDES)
}

/// Test-only convenience: point the read seam at the real override registry so
/// a test can `set`/`clear` through `tokeira_conformance::overrides()` and
/// observe the effect through the production accessors.
#[cfg(test)]
pub(crate) fn install_registry_reads() {
    install(OverrideReads {
        read_bool: |key| tokeira_conformance::overrides().get_bool(key),
        read_i64: |key| tokeira_conformance::overrides().get_i64(key),
        read_f64: |key| tokeira_conformance::overrides().get_f64(key),
        read_duration: |key| tokeira_conformance::overrides().get_duration(key),
        read_scope_generation: || tokeira_conformance::overrides().scope_generation(),
    });
}
