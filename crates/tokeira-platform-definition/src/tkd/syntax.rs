//! Platform-free bridge for the `.tkd` frontend syntax tier.
//!
//! Validation still runs the real parser, schema collector, part loader, and
//! reject-by-default subset pass. The bridge only keeps vocabulary-dependent
//! recognition open until an engine interprets the source against a concrete
//! deployment platform; every evaluation operation refuses defensively.

use crate::tkd::{EvalError, FieldMap, HostBridge, Value};

/// Recognition-only bridge used by [`super::validate_syntax`].
pub(super) struct SyntaxBridge;

/// Opaque placeholder required by [`HostBridge`]; syntax validation never
/// constructs one.
#[derive(Clone, Debug)]
pub(super) struct SyntaxHost;

impl HostBridge for SyntaxBridge {
    type Host = SyntaxHost;
    type Cx = ();
    type Output = ();

    fn is_kind(&self, name: &str) -> bool {
        // `subset::Checker::reject_unplaced_kind` classifies any recognized
        // single-segment path as a kind literal. Accepting every name would
        // therefore swallow ordinary `snake_case` bindings; the Rust
        // type-name convention keeps only leading-uppercase candidates open
        // for the engine tier to resolve against the platform inventory.
        name.chars().next().is_some_and(char::is_uppercase)
    }

    fn knows_method(&self, _name: &str) -> bool {
        true
    }

    fn knows_assoc(&self, _path: &str) -> bool {
        true
    }

    fn kind_defaults(&self, _name: &str) -> Option<FieldMap<Self::Host>> {
        None
    }

    fn construct_kind(
        &self,
        _name: &str,
        _fields: FieldMap<Self::Host>,
        _cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        Err(evaluation_refusal())
    }

    fn assoc(
        &self,
        _path: &str,
        _args: Vec<Value<Self::Host>>,
        _cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        Err(evaluation_refusal())
    }

    fn call_method(
        &self,
        _recv: &Self::Host,
        _method: &str,
        _args: Vec<Value<Self::Host>>,
        _cx: &Self::Cx,
    ) -> Result<Value<Self::Host>, EvalError> {
        Err(evaluation_refusal())
    }

    fn host_field(&self, _host: &Self::Host, _field: &str) -> Result<Value<Self::Host>, EvalError> {
        Err(evaluation_refusal())
    }

    fn cx_host(&self, _cx: &Self::Cx) -> Self::Host {
        SyntaxHost
    }

    fn finish(&self, _ret: Self::Host) -> Result<Self::Output, EvalError> {
        Err(evaluation_refusal())
    }
}

fn evaluation_refusal() -> EvalError {
    EvalError::new("the frontend syntax tier never evaluates")
}

#[cfg(test)]
mod tests {
    use tokeira_platform::definition::NoPartSources;

    #[test]
    fn ordinary_bindings_are_not_classified_as_kinds() {
        let source = r#"
struct Config {}

fn config() -> Config {
    Config {}
}

fn deployment(cfg: Config, cx: Context) -> Deployment {
    let ordinary_binding = cfg;
    Deployment::new(&["default"])
}
"#;
        assert!(crate::tkd::validate_syntax(source, &NoPartSources).is_ok());
    }

    #[test]
    fn uppercase_candidates_keep_kind_placement_rules() {
        let source = r#"
struct Config {}

fn config() -> Config {
    Config {}
}

fn deployment(cfg: Config, cx: Context) -> Deployment {
    let misplaced = PlatformKind;
    Deployment::new(&["default"])
}
"#;
        let findings = crate::tkd::validate_syntax(source, &NoPartSources)
            .expect_err("a kind candidate cannot be bound to a local");
        assert!(
            findings
                .iter()
                .any(|finding| finding.contains("kind must be used inline"))
        );
    }
}
