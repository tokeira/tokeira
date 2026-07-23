//! Conformance-only dynamic-config override registry.
//!
//! This crate lets the Temporal functional test corpus deliver its
//! `OverrideDynamicConfig(setting, value)` calls to an out-of-process `tokeirad`
//! (spec `.kiro/specs/conformance-config-override/`). It holds a process-global
//! store of `setting-key -> typed value` overrides that consult sites in the
//! runtime / edge / projection planes read **live** at request time.
//!
//! # Where it sits
//!
//! Runtime/edge/projection plane, never the kernel. A pure deterministic engine
//! must not read a runtime-mutable global — that would make history replay
//! non-deterministic — so the kernel's tunables stay compile-time `const`s and
//! this registry is off-limits to it (enforced by a dependency-graph test:
//! `tokeira-kernel` must never depend on this crate). The overridable set is
//! therefore exactly the values consulted live at request time.
//!
//! # Production isolation
//!
//! Consumer crates depend on this crate **only** under their own `conformance`
//! Cargo feature (an optional dependency). A production `tokeirad` — built
//! without that feature — never links this crate, so the process-global registry
//! does not exist in production. The mutable global is a sanctioned,
//! feature-gated exception to the engine's config-as-constant posture, confined
//! to conformance builds.
//!
//! # Honesty boundary
//!
//! [`KEY_CLASSIFICATION`] is the single source of truth for what is honourable.
//! An override takes effect only when its key's [`Disposition`] is
//! [`Disposition::Wired`], which is set only in the same change that adds a real
//! consult-site accessor for that key. Unknown keys, kernel-excluded keys, and
//! keys tokeira does not enforce are rejected — never silently honoured, never
//! fabricated — so a test can never turn green on an override tokeira does not
//! actually apply.

use std::{
    collections::HashMap,
    sync::{LazyLock, RwLock},
    time::Duration,
};

use thiserror::Error;

/// A typed override value, covering the Temporal dynamic-config value kinds the
/// corpus sets. Its kind must match the target key's declared [`ValueType`]
/// (enforced by [`ConformanceOverrides::set`]).
#[derive(Clone, Debug, PartialEq)]
pub enum OverrideValue {
    /// An integer setting (e.g. a count or threshold).
    Int(i64),
    /// A floating-point setting (e.g. a rate).
    Double(f64),
    /// A boolean feature toggle.
    Bool(bool),
    /// A string setting.
    Text(String),
    /// A duration setting (e.g. a timeout or interval).
    Duration(Duration),
    /// A structured setting serialized as JSON; the wired consumer owns its schema.
    Json(String),
}

impl OverrideValue {
    /// The value's kind, used to type-check a `set` against the key's declared type.
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Int(_) => ValueType::Int,
            Self::Double(_) => ValueType::Double,
            Self::Bool(_) => ValueType::Bool,
            Self::Text(_) => ValueType::Text,
            Self::Duration(_) => ValueType::Duration,
            Self::Json(_) => ValueType::Json,
        }
    }
}

/// The declared kind of a dynamic-config key's value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    /// Integer-valued setting.
    Int,
    /// Floating-point-valued setting.
    Double,
    /// Boolean-valued setting.
    Bool,
    /// String-valued setting.
    Text,
    /// Duration-valued setting.
    Duration,
    /// Structured setting transported as JSON text.
    Json,
}

/// Whether — and why — a key may be overridden.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// A real consult-site accessor reads this key live; overrides are honoured.
    Wired,
    /// Consulted inside the pure kernel as a compile-time constant; never
    /// overridable, because overriding it would break replay determinism.
    KernelExcluded,
    /// tokeira has no consult site for this key (e.g. a size limit it does not
    /// enforce); an override cannot bite, so it is rejected rather than faked.
    NotEnforced,
}

/// Classification of one known dynamic-config key: its declared value type and
/// its override disposition.
///
/// `key` is the Temporal setting key (e.g.
/// `"history.MaxBufferedQueryCount"`), matched verbatim against the corpus's
/// `setting.Key()`.
#[derive(Clone, Copy, Debug)]
pub struct KeySpec {
    /// The Temporal setting key string.
    pub key: &'static str,
    /// The value kind this key carries.
    pub value_type: ValueType,
    /// Whether this key may be overridden (and if not, why).
    pub disposition: Disposition,
}

/// The single source of truth for what is honourable.
///
/// Only [`Disposition::Wired`] keys are honoured; a key becomes `Wired` in the
/// same change that adds its consult-site accessor. Wirable-but-not-yet-wired
/// keys are deliberately absent (an unknown key is rejected), so this table's
/// `Wired` set always equals the set of keys a real accessor reads — which is
/// exactly the honesty boundary the spec requires.
pub static KEY_CLASSIFICATION: &[KeySpec] = &[
    // The HTTP gateway consults the global Host allow-list before body decode
    // on every annotated request (`allowedHostsMiddleware`,
    // service/frontend/http_api_server.go @ v1.31.0).
    KeySpec {
        key: "frontend.httpAllowedHosts",
        value_type: ValueType::Json,
        disposition: Disposition::Wired,
    },
    // The Nexus HTTP authorization adapter consults this gate live before it
    // surfaces an authorizer failure. Denials are never exposed by the gate.
    KeySpec {
        key: "frontend.exposeAuthorizerErrors",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Task-token namespace mismatch validation is a frontend admission rule
    // read after authorization, matching the v1.31.0 interceptor gate.
    KeySpec {
        key: "frontend.enableTokenNamespaceEnforcement",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Temporal scopes this gate per namespace. Tokeira's harness registry is
    // intentionally process-global, so the override collapses that scope while
    // the static production setting remains `[policy.authorization].principal_attribution`.
    KeySpec {
        key: "frontend.enablePrincipalPropagation",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Cross-namespace workflow commands are re-authorized at the edge before
    // their workflow-task completion reaches the authoritative runtime.
    KeySpec {
        key: "system.enableCrossNamespaceCommands",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Workflow-task admission consults this namespace policy before invoking
    // the pure notification transition. v1.31.0 defaults it true
    // (`common/dynamicconfig/constants.go:931-935 @ v1.31.0`).
    KeySpec {
        key: "system.enableSendTargetVersionChanged",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Workflow-rule CRUD and evaluation are namespace frontend policy. The
    // v1.31.0 corpus enables the otherwise-false gate in suite setup
    // (`service/frontend/workflow_handler.go:6985-7088 @ v1.31.0`).
    KeySpec {
        key: "frontend.workflowRulesAPIsEnabled",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Standalone-activity admission is a namespace-scoped frontend policy read
    // live on every activity RPC (`Config.Enabled(namespace)` in
    // `chasm/lib/activity/frontend.go @ v1.31.0`). The conformance corpus turns
    // it on in suite setup while the production default remains false.
    KeySpec {
        key: "activity.enableStandalone",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Standalone Describe/Poll read both values at long-poll admission. The
    // corpus overrides them per sub-test to keep deadline assertions fast.
    KeySpec {
        key: "activity.longPollTimeout",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "activity.longPollBuffer",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    // ListActivityExecutions caps each request at this namespace policy
    // (`chasm/lib/activity/config.go @ v1.31.0`).
    KeySpec {
        key: "frontend.visibilityMaxPageSize",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    // Proving consumer (Tier 3.22): the reported-problems threshold, consulted
    // live where `TemporalReportedProblems` is derived on Describe / at the
    // WFT-failure check. Wired here because this change adds that accessor.
    KeySpec {
        key: "system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    // WFT completion admission resolves these namespace-scoped policy values
    // in the runtime immediately before invoking the pure kernel. v1.31.0
    // defaults every limit to 2000; non-positive values disable that limit
    // (`common/dynamicconfig/constants.go` and
    // `respondworkflowtaskcompleted/workflow_size_checker.go @ v1.31.0`).
    KeySpec {
        key: "limit.numPendingChildExecutions.error",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "limit.numPendingActivities.error",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "limit.numPendingSignals.error",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "limit.numPendingCancelRequests.error",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    // Schedule starts are paced at a live runtime consult site. v1.31.0 owns
    // this as a namespace-scoped scheduler-worker rate
    // (`service/worker/scheduler/fx.go:116-133 @ v1.31.0`).
    KeySpec {
        key: "worker.schedulerNamespaceStartWorkflowRPS",
        value_type: ValueType::Double,
        disposition: Disposition::Wired,
    },
    // Shutdown poll cancellation is a matching/runtime liveness policy. The
    // v1.31.0 default is false; the task-queue corpus enables it per test.
    KeySpec {
        key: "frontend.enableCancelWorkerPollsOnShutdown",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "matching.maxFairnessKeyWeightOverrides",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    // Worker Deployment admission and drainage are runtime-registry policy. The
    // v1.31.0 corpus overrides these limits/intervals to exercise boundary and
    // lifecycle behavior without changing the production defaults
    // (`common/dynamicconfig/constants.go:492-503,1380-1396 @ v1.31.0`).
    KeySpec {
        key: "matching.maxVersionsInDeployment",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "matching.maxTaskQueuesInDeploymentVersion",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "matching.PollerHistoryTTL",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "matching.wv.VersionDrainageStatusVisibilityGracePeriod",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "matching.wv.VersionDrainageStatusRefreshInterval",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    // Pinned-override admission caches both membership outcomes, and successful
    // pinned operations deduplicate version reactivation. These are runtime
    // registry policies in Tokeira rather than Temporal history/matching service
    // objects (`version_membership_cache.go` and
    // `version_reactivation_signal_cache.go @ v1.31.0`).
    KeySpec {
        key: "history.versionMembershipCacheTTL",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "history.versionReactivationSignalCacheTTL",
        value_type: ValueType::Duration,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "history.enableVersionReactivationSignals",
        value_type: ValueType::Bool,
        disposition: Disposition::Wired,
    },
    // Callback limits are frontend admission policy, read live before a start is
    // translated into runtime state (`validateWorkflowCompletionCallbacks`,
    // service/frontend/workflow_handler.go:6300-6350 @ v1.31.0).
    KeySpec {
        key: "frontend.callbackURLMaxLength",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "frontend.callbackHeaderMaxLength",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    KeySpec {
        key: "system.maxCallbacksPerWorkflow",
        value_type: ValueType::Int,
        disposition: Disposition::Wired,
    },
    // Temporal models callback address policy as a namespace-scoped typed
    // setting. The conformance bridge transports the Go rule slice as JSON;
    // tokeira-edge owns its schema and wildcard semantics.
    KeySpec {
        key: "component.callbacks.allowedAddresses",
        value_type: ValueType::Json,
        disposition: Disposition::Wired,
    },
    // Kernel-consulted constants — never overridable. A runtime-mutable read
    // inside the pure kernel would make the same history replay differently
    // (`CONTINUE_AS_NEW_MIN_INTERVAL`, `MAX_BUFFERED_EVENTS` @ tokeira-kernel).
    KeySpec {
        key: "history.workflowIdReuseMinimalInterval",
        value_type: ValueType::Duration,
        disposition: Disposition::KernelExcluded,
    },
    KeySpec {
        key: "history.maximumBufferedEventsBatch",
        value_type: ValueType::Int,
        disposition: Disposition::KernelExcluded,
    },
    // Size limits tokeira does not enforce — there is no consult site to
    // override, so an override is rejected rather than fabricating a limit.
    KeySpec {
        key: "limit.mutableStateSize.error",
        value_type: ValueType::Int,
        disposition: Disposition::NotEnforced,
    },
    KeySpec {
        key: "limit.blobSize.error",
        value_type: ValueType::Int,
        disposition: Disposition::NotEnforced,
    },
    KeySpec {
        key: "limit.historySize.error",
        value_type: ValueType::Int,
        disposition: Disposition::NotEnforced,
    },
    KeySpec {
        key: "limit.historyCount.error",
        value_type: ValueType::Int,
        disposition: Disposition::NotEnforced,
    },
    KeySpec {
        key: "limit.historySize.suggestContinueAsNew",
        value_type: ValueType::Int,
        disposition: Disposition::NotEnforced,
    },
];

/// Look up a key's classification, if the key is known.
///
/// Matching is ASCII-case-insensitive. Temporal dynamic-config keys are
/// case-insensitive and the fork delivers `setting.Key()` lowercased
/// (`common/dynamicconfig/key.go` `Key.String()` @ v1.31.0), while this table
/// spells the keys in the canonical mixed case of `constants.go`. Both spellings
/// therefore resolve to the same [`KeySpec`], whose `key` is the canonical
/// storage identity every consult site reads back under.
pub fn classification(key: &str) -> Option<&'static KeySpec> {
    KEY_CLASSIFICATION
        .iter()
        .find(|spec| spec.key.eq_ignore_ascii_case(key))
}

/// Why a `set` was rejected. The `Display` text is surfaced verbatim by the
/// conformance control service as the Connect error message, and the fork bridge
/// treats any rejection as a signal to fall back to the skip registry.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum OverrideError {
    /// The key is absent from [`KEY_CLASSIFICATION`].
    #[error("unsupported override key: {0}")]
    UnknownKey(String),
    /// The key is consulted in the pure kernel and is not overridable.
    #[error("kernel-consulted constant is not overridable: {0}")]
    KernelExcluded(String),
    /// tokeira has no consult site for the key, so an override cannot take effect.
    #[error("tokeira does not enforce {0}")]
    NotEnforced(String),
    /// The value's kind does not match the key's declared [`ValueType`].
    #[error("value type mismatch for {key}: expected {expected:?}, got {got:?}")]
    TypeMismatch {
        /// The key whose declared type was violated.
        key: String,
        /// The type the key declares.
        expected: ValueType,
        /// The type the caller supplied.
        got: ValueType,
    },
    /// The key was empty.
    #[error("override key must not be empty")]
    MissingKey,
}

/// Process-global store of active overrides.
///
/// Reads happen on the request path (via the typed getters); writes happen only
/// from the conformance control service. Reads observe the current state with no
/// caching, so a mid-run override change — as Tier 3.22's `DynamicConfigChanges`
/// leaf requires — takes effect on the next consult.
#[derive(Debug, Default)]
pub struct ConformanceOverrides {
    inner: RwLock<HashMap<&'static str, OverrideValue>>,
}

impl ConformanceOverrides {
    /// Honour an override, or reject it.
    ///
    /// This is the honesty gate: an override is stored only for a
    /// [`Disposition::Wired`] key whose declared [`ValueType`] matches the
    /// value's kind. An unknown key, a kernel-excluded or not-enforced key, an
    /// empty key, or a type mismatch is rejected and **records nothing**.
    pub fn set(&self, key: &str, value: OverrideValue) -> Result<(), OverrideError> {
        if key.is_empty() {
            return Err(OverrideError::MissingKey);
        }
        let spec = classification(key).ok_or_else(|| OverrideError::UnknownKey(key.to_string()))?;
        match spec.disposition {
            Disposition::KernelExcluded => {
                return Err(OverrideError::KernelExcluded(key.to_string()));
            }
            Disposition::NotEnforced => {
                return Err(OverrideError::NotEnforced(key.to_string()));
            }
            Disposition::Wired => {}
        }
        let got = value.value_type();
        if got != spec.value_type {
            return Err(OverrideError::TypeMismatch {
                key: key.to_string(),
                expected: spec.value_type,
                got,
            });
        }
        // Key by the interned `&'static str` from the classification so getters
        // (which take `&str`) resolve against a stable key identity.
        self.inner
            .write()
            .expect("conformance overrides poisoned")
            .insert(spec.key, value);
        Ok(())
    }

    /// Drop the override for `key`, if any; subsequent consults observe the default.
    pub fn clear(&self, key: &str) {
        if let Some(spec) = classification(key) {
            self.inner
                .write()
                .expect("conformance overrides poisoned")
                .remove(spec.key);
        }
    }

    /// Drop every override — used for per-test isolation on teardown.
    pub fn reset(&self) {
        self.inner
            .write()
            .expect("conformance overrides poisoned")
            .clear();
    }

    fn get(&self, key: &str) -> Option<OverrideValue> {
        // Resolve through the (case-insensitive) classification so a consult
        // site's key spelling matches the canonical `spec.key` the override was
        // stored under, regardless of case.
        let spec = classification(key)?;
        self.inner
            .read()
            .expect("conformance overrides poisoned")
            .get(spec.key)
            .cloned()
    }

    /// Live integer override for `key`, if one is set to an integer.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.get(key)? {
            OverrideValue::Int(v) => Some(v),
            _ => None,
        }
    }

    /// Live floating-point override for `key`, if one is set to a double.
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        match self.get(key)? {
            OverrideValue::Double(v) => Some(v),
            _ => None,
        }
    }

    /// Live boolean override for `key`, if one is set to a bool.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.get(key)? {
            OverrideValue::Bool(v) => Some(v),
            _ => None,
        }
    }

    /// Live string override for `key`, if one is set to a string.
    pub fn get_str(&self, key: &str) -> Option<String> {
        match self.get(key)? {
            OverrideValue::Text(v) => Some(v),
            _ => None,
        }
    }

    /// Live duration override for `key`, if one is set to a duration.
    pub fn get_duration(&self, key: &str) -> Option<Duration> {
        match self.get(key)? {
            OverrideValue::Duration(v) => Some(v),
            _ => None,
        }
    }

    /// Live structured JSON override for `key`, if one is set to JSON text.
    pub fn get_json(&self, key: &str) -> Option<String> {
        match self.get(key)? {
            OverrideValue::Json(v) => Some(v),
            _ => None,
        }
    }
}

/// The process-global override registry.
///
/// Present only in a `conformance`-feature build (consumer crates gate their
/// dependency on this crate behind that feature), so a production `tokeirad`
/// never instantiates it.
pub fn overrides() -> &'static ConformanceOverrides {
    static OVERRIDES: LazyLock<ConformanceOverrides> = LazyLock::new(ConformanceOverrides::default);
    &OVERRIDES
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const REPORTED_PROBLEMS_KEY: &str =
        "system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute";
    const CALLBACK_ALLOWED_ADDRESSES_KEY: &str = "component.callbacks.allowedAddresses";
    const STANDALONE_ACTIVITIES_KEY: &str = "activity.enableStandalone";
    const AUTH_BOOL_KEYS: &[&str] = &[
        "frontend.enablePrincipalPropagation",
        "frontend.exposeAuthorizerErrors",
        "frontend.enableTokenNamespaceEnforcement",
        "system.enableCrossNamespaceCommands",
    ];
    const WORKER_DEPLOYMENT_KEYS: &[(&str, ValueType)] = &[
        ("matching.maxVersionsInDeployment", ValueType::Int),
        ("matching.maxTaskQueuesInDeploymentVersion", ValueType::Int),
        ("matching.PollerHistoryTTL", ValueType::Duration),
        (
            "matching.wv.VersionDrainageStatusVisibilityGracePeriod",
            ValueType::Duration,
        ),
        (
            "matching.wv.VersionDrainageStatusRefreshInterval",
            ValueType::Duration,
        ),
        ("history.versionMembershipCacheTTL", ValueType::Duration),
        (
            "history.versionReactivationSignalCacheTTL",
            ValueType::Duration,
        ),
        ("history.enableVersionReactivationSignals", ValueType::Bool),
    ];
    const PENDING_COMMAND_LIMIT_KEYS: &[&str] = &[
        "limit.numPendingChildExecutions.error",
        "limit.numPendingActivities.error",
        "limit.numPendingSignals.error",
        "limit.numPendingCancelRequests.error",
    ];

    /// The classification table is internally consistent: keys are unique, and
    /// the reported-problems threshold — the proving wired consumer — is present
    /// as a `Wired` `Int`.
    #[test]
    fn classification_table_is_consistent() {
        let mut seen = std::collections::HashSet::new();
        for spec in KEY_CLASSIFICATION {
            assert!(
                seen.insert(spec.key),
                "duplicate key in classification: {}",
                spec.key
            );
        }
        let reported = classification(REPORTED_PROBLEMS_KEY).expect("reported-problems classified");
        assert_eq!(reported.disposition, Disposition::Wired);
        assert_eq!(reported.value_type, ValueType::Int);
        let standalone =
            classification(STANDALONE_ACTIVITIES_KEY).expect("standalone activities classified");
        assert_eq!(standalone.disposition, Disposition::Wired);
        assert_eq!(standalone.value_type, ValueType::Bool);
        for key in AUTH_BOOL_KEYS {
            let auth = classification(key).expect("authorization key classified");
            assert_eq!(auth.disposition, Disposition::Wired);
            assert_eq!(auth.value_type, ValueType::Bool);
        }
        for (key, value_type) in WORKER_DEPLOYMENT_KEYS {
            let deployment = classification(key).expect("worker deployment key classified");
            assert_eq!(deployment.disposition, Disposition::Wired);
            assert_eq!(deployment.value_type, *value_type);
        }
    }

    /// A fresh registry: set on a wired key is observed live; clear reverts to
    /// the default (None); reset drops everything.
    #[test]
    fn lifecycle_set_clear_reset() {
        let overrides = ConformanceOverrides::default();
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);

        overrides
            .set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(2))
            .expect("wired set");
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), Some(2));

        overrides
            .set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(0))
            .expect("re-set");
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), Some(0));

        overrides.clear(REPORTED_PROBLEMS_KEY);
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);

        overrides
            .set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(2))
            .expect("wired set");
        overrides.reset();
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
    }

    #[test]
    fn authorization_bool_keys_follow_registry_lifecycle() {
        // Feature: authorization-foundation, Property 7: all four live auth
        // gates share the same typed set/clear/reset semantics (Requirement 9).
        let overrides = ConformanceOverrides::default();
        for key in AUTH_BOOL_KEYS {
            assert_eq!(overrides.get_bool(key), None);
            overrides
                .set(key, OverrideValue::Bool(true))
                .expect("auth key is wired");
            assert_eq!(overrides.get_bool(key), Some(true));
            overrides.clear(key);
            assert_eq!(overrides.get_bool(key), None);
        }
        for key in AUTH_BOOL_KEYS {
            overrides
                .set(key, OverrideValue::Bool(false))
                .expect("auth key is wired");
        }
        overrides.reset();
        for key in AUTH_BOOL_KEYS {
            assert_eq!(overrides.get_bool(key), None);
        }
    }

    /// Keys are matched case-insensitively. The fork bridge delivers
    /// `setting.Key()` lowercased (Temporal keys are case-insensitive), so a
    /// lowercase set must be honoured and observed by the canonical-case consult
    /// site — the exact end-to-end path Tier 3.22 exercises. Regression guard for
    /// the lowercased-key mismatch that made the reported-problems override land
    /// as `UnknownKey`.
    #[test]
    fn key_matching_is_case_insensitive() {
        let overrides = ConformanceOverrides::default();
        let lowercased = REPORTED_PROBLEMS_KEY.to_ascii_lowercase();
        assert_ne!(
            lowercased, REPORTED_PROBLEMS_KEY,
            "sanity: the canonical key has uppercase"
        );

        overrides
            .set(&lowercased, OverrideValue::Int(2))
            .expect("lowercase set is honoured");
        // The runtime consult site reads back under the canonical mixed-case key.
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), Some(2));

        // A lowercase clear drops the canonically-stored override.
        overrides.clear(&lowercased);
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
    }

    /// The honesty gate rejects (recording nothing) unknown, kernel-excluded,
    /// not-enforced, empty, and type-mismatched sets.
    #[test]
    fn set_rejects_unhonourable_keys_and_type_mismatches() {
        let overrides = ConformanceOverrides::default();

        assert!(matches!(
            overrides.set("nope.not.a.key", OverrideValue::Int(1)),
            Err(OverrideError::UnknownKey(_))
        ));
        assert!(matches!(
            overrides.set(
                "history.workflowIdReuseMinimalInterval",
                OverrideValue::Duration(Duration::ZERO)
            ),
            Err(OverrideError::KernelExcluded(_))
        ));
        assert!(matches!(
            overrides.set("limit.mutableStateSize.error", OverrideValue::Int(410_000)),
            Err(OverrideError::NotEnforced(_))
        ));
        assert!(matches!(
            overrides.set("", OverrideValue::Int(1)),
            Err(OverrideError::MissingKey)
        ));
        assert!(matches!(
            overrides.set(REPORTED_PROBLEMS_KEY, OverrideValue::Bool(true)),
            Err(OverrideError::TypeMismatch { .. })
        ));

        // None of the rejected sets recorded anything.
        assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
    }

    fn any_override_value() -> impl Strategy<Value = OverrideValue> {
        prop_oneof![
            any::<i64>().prop_map(OverrideValue::Int),
            any::<f64>().prop_map(OverrideValue::Double),
            any::<bool>().prop_map(OverrideValue::Bool),
            any::<String>().prop_map(OverrideValue::Text),
            (0u64..1_000_000u64).prop_map(|n| OverrideValue::Duration(Duration::from_millis(n))),
            any::<String>().prop_map(OverrideValue::Json),
        ]
    }

    fn any_key() -> impl Strategy<Value = String> {
        let known: Vec<String> = KEY_CLASSIFICATION
            .iter()
            .map(|spec| spec.key.to_string())
            .collect();
        // The random arm is `[a-z.]`-only. Matching is now case-insensitive, so a
        // random lowercase string could in principle equal a known key's lowercased
        // form; the honesty property below derives its expectation from
        // `classification()` itself (which `set` also uses), so the two always agree
        // whether or not such a collision occurs.
        prop_oneof![
            proptest::sample::select(known),
            proptest::string::string_regex("[a-z][a-z.]{0,24}").expect("valid key regex"),
        ]
    }

    fn any_key_stored(overrides: &ConformanceOverrides) -> bool {
        KEY_CLASSIFICATION.iter().any(|spec| match spec.value_type {
            ValueType::Int => overrides.get_i64(spec.key).is_some(),
            ValueType::Double => overrides.get_f64(spec.key).is_some(),
            ValueType::Bool => overrides.get_bool(spec.key).is_some(),
            ValueType::Text => overrides.get_str(spec.key).is_some(),
            ValueType::Duration => overrides.get_duration(spec.key).is_some(),
            ValueType::Json => overrides.get_json(spec.key).is_some(),
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        // Feature: conformance-config-override, Property 2: override lifecycle and liveness.
        // A wired override is observed live; re-set replaces it; clear reverts to the
        // default; reset drops all.
        #[test]
        fn prop_lifecycle_and_liveness(first in any::<i64>(), second in any::<i64>()) {
            let overrides = ConformanceOverrides::default();
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
            overrides.set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(first)).unwrap();
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), Some(first));
            overrides.set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(second)).unwrap();
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), Some(second));
            overrides.clear(REPORTED_PROBLEMS_KEY);
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
            overrides.set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(first)).unwrap();
            overrides.reset();
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
        }

        #[test]
        fn pending_command_limit_keys_share_live_registry_lifecycle(
            first in any::<i64>(),
            second in any::<i64>(),
        ) {
            // Feature: api-conformance-client-misc, Property 1: live completion-limit resolution
            let overrides = ConformanceOverrides::default();
            for key in PENDING_COMMAND_LIMIT_KEYS {
                let classified = classification(key).expect("pending limit key classified");
                prop_assert_eq!(classified.disposition, Disposition::Wired);
                prop_assert_eq!(classified.value_type, ValueType::Int);
                overrides.set(key, OverrideValue::Int(first)).unwrap();
                prop_assert_eq!(overrides.get_i64(key), Some(first));
                overrides.set(key, OverrideValue::Int(second)).unwrap();
                prop_assert_eq!(overrides.get_i64(key), Some(second));
                overrides.clear(key);
                prop_assert_eq!(overrides.get_i64(key), None);
            }
            for key in PENDING_COMMAND_LIMIT_KEYS {
                overrides.set(key, OverrideValue::Int(first)).unwrap();
            }
            overrides.reset();
            for key in PENDING_COMMAND_LIMIT_KEYS {
                prop_assert_eq!(overrides.get_i64(key), None);
            }
        }

        // Feature: conformance-config-override, Property 3: value-type fidelity.
        // On a wired key, a matching-kind value round-trips; a mismatched kind is
        // rejected and records nothing.
        #[test]
        fn prop_value_type_fidelity(value in any_override_value()) {
            let overrides = ConformanceOverrides::default();
            let result = overrides.set(REPORTED_PROBLEMS_KEY, value.clone());
            match value {
                OverrideValue::Int(expected) => {
                    prop_assert!(result.is_ok());
                    prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), Some(expected));
                }
                _ => {
                    prop_assert!(
                        matches!(result, Err(OverrideError::TypeMismatch { .. })),
                        "expected type mismatch",
                    );
                    prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
                }
            }
        }

        // Feature: conformance-config-override, Property 3: structured JSON fidelity.
        // Structured values round-trip without registry interpretation, participate in
        // lifecycle operations, and remain type-separated from scalar keys.
        #[test]
        fn prop_structured_json_fidelity(json in any::<String>()) {
            let overrides = ConformanceOverrides::default();
            overrides
                .set(CALLBACK_ALLOWED_ADDRESSES_KEY, OverrideValue::Json(json.clone()))
                .unwrap();
            prop_assert_eq!(
                overrides.get_json(CALLBACK_ALLOWED_ADDRESSES_KEY),
                Some(json.clone())
            );
            overrides.clear(CALLBACK_ALLOWED_ADDRESSES_KEY);
            prop_assert_eq!(overrides.get_json(CALLBACK_ALLOWED_ADDRESSES_KEY), None);

            let mismatch = overrides.set(REPORTED_PROBLEMS_KEY, OverrideValue::Json(json));
            prop_assert!(
                matches!(mismatch, Err(OverrideError::TypeMismatch { .. })),
                "JSON value must not be accepted by an integer key",
            );
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
        }

        // Feature: conformance-config-override, Property 4: honesty boundary.
        // set() honours iff the key is Wired and the value kind matches its declared
        // type; every other case returns the mapped error and records nothing.
        #[test]
        fn prop_honesty_boundary(key in any_key(), value in any_override_value()) {
            let overrides = ConformanceOverrides::default();
            let result = overrides.set(&key, value.clone());
            match classification(&key) {
                None => prop_assert!(matches!(result, Err(OverrideError::UnknownKey(_)))),
                Some(spec) => match spec.disposition {
                    Disposition::KernelExcluded => {
                        prop_assert!(matches!(result, Err(OverrideError::KernelExcluded(_))));
                    }
                    Disposition::NotEnforced => {
                        prop_assert!(matches!(result, Err(OverrideError::NotEnforced(_))));
                    }
                    Disposition::Wired => {
                        if value.value_type() == spec.value_type {
                            prop_assert!(result.is_ok());
                        } else {
                            prop_assert!(
                                matches!(result, Err(OverrideError::TypeMismatch { .. })),
                                "expected type mismatch",
                            );
                        }
                    }
                },
            }
            // Nothing is stored unless the set was honoured.
            prop_assert_eq!(result.is_ok(), any_key_stored(&overrides));
        }

        // Feature: conformance-config-override, Property 7: between-test isolation.
        // Whatever a "test" sets or clears, reset() at teardown clears all state so the
        // next test starts from defaults.
        #[test]
        fn prop_between_test_isolation(ops in prop::collection::vec(any::<Option<i64>>(), 0..24)) {
            let overrides = ConformanceOverrides::default();
            for op in ops {
                match op {
                    Some(value) => {
                        let _ = overrides.set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(value));
                    }
                    None => overrides.clear(REPORTED_PROBLEMS_KEY),
                }
            }
            overrides.reset();
            prop_assert_eq!(overrides.get_i64(REPORTED_PROBLEMS_KEY), None);
        }
    }
}
