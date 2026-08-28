//! Conformance-only dynamic-override read seam.
//!
//! Production edge code reads its tuned limits from source-cited constants.
//! The functional-conformance harness needs the same admission sites to honour
//! live per-test overrides delivered out of process (spec
//! `.kiro/specs/conformance-config-override/`). The registry that stores those
//! overrides is test-harness machinery and is never published, so this crate
//! cannot name it: the application assembling a conformance server installs
//! plain read functions here before serving instead. With nothing installed
//! every read reports "no override", so a `conformance`-featured build whose
//! host never installs a source behaves exactly like production.

use std::{sync::OnceLock, time::Duration};

/// Read functions over the harness's override registry, installed once by the
/// serving application before boot.
///
/// Fields are plain function pointers rather than a trait object so an install
/// needs no allocation or leaked adapter; the wrapper methods keep call sites
/// identical in shape to the registry's own typed getters.
#[derive(Clone, Copy)]
pub struct OverrideReads {
    /// Resolve a boolean override for the given dynamic-config key.
    pub read_bool: fn(&str) -> Option<bool>,
    /// Resolve an integer override for the given dynamic-config key.
    pub read_i64: fn(&str) -> Option<i64>,
    /// Resolve a duration override for the given dynamic-config key.
    pub read_duration: fn(&str) -> Option<Duration>,
    /// Resolve a JSON-document override for the given dynamic-config key.
    pub read_json: fn(&str) -> Option<String>,
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

    pub(crate) fn get_duration(&self, key: &str) -> Option<Duration> {
        (self.read_duration)(key)
    }

    pub(crate) fn get_json(&self, key: &str) -> Option<String> {
        (self.read_json)(key)
    }
}

/// Inert default: every read reports "no override configured".
const NO_OVERRIDES: OverrideReads = OverrideReads {
    read_bool: |_| None,
    read_i64: |_| None,
    read_duration: |_| None,
    read_json: |_| None,
};

static READS: OnceLock<OverrideReads> = OnceLock::new();

/// Install the override read functions. The first install wins; later calls
/// are ignored so repeated harness setup (one install per test in a shared
/// process) stays idempotent against the same registry.
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
        read_duration: |key| tokeira_conformance::overrides().get_duration(key),
        read_json: |key| tokeira_conformance::overrides().get_json(key),
    });
}
