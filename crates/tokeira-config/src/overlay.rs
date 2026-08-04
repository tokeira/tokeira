//! Lay a partial configuration document over a schema's defaults.
//!
//! A config-document node authors only the fields it means to set; this
//! module turns that partial document into a complete, typed one. The
//! overlay semantics are the schema's own serde defaults: deserializing a
//! partial document fills every unnamed field with its default, at every
//! depth, and `deny_unknown_fields` makes a misspelled field an error
//! instead of a silent no-op. Rendering serializes the complete document,
//! so the printout an operator reads shows every effective value, not just
//! the overrides.
//!
//! The functions are deliberately schema-generic: `TokeiraConfig` today,
//! the controller and autoscaler documents when they gain rendered nodes.
//! A schema qualifies when every field at every depth carries a serde
//! default — that is what makes "partial document" and "overlay onto
//! defaults" the same operation. `TokeiraConfig` holds that invariant and
//! the tests below pin it.

use serde::{Serialize, de::DeserializeOwned};

use crate::ConfigError;

/// Overlay a partial document, given as a TOML value, onto the schema's
/// defaults. Unknown fields fail with the field named.
pub fn overlay_document<T: DeserializeOwned>(overlay: toml::Value) -> Result<T, ConfigError> {
    Ok(overlay.try_into::<T>()?)
}

/// Overlay a partial document, given as TOML text, onto the schema's
/// defaults. Unknown fields fail with the field named.
pub fn overlay_document_str<T: DeserializeOwned>(document: &str) -> Result<T, ConfigError> {
    Ok(toml::from_str(document)?)
}

/// Serialize a complete document for delivery and inspection.
pub fn render_document<T: Serialize>(config: &T) -> Result<String, ConfigError> {
    Ok(toml::to_string_pretty(config)?)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::TokeiraConfig;

    // The invariant the overlay semantics stand on: an empty document IS the
    // default document, at every depth.
    #[test]
    fn empty_overlay_is_the_default_document() {
        let config: TokeiraConfig = overlay_document_str("").unwrap();
        assert_eq!(config, TokeiraConfig::default());
    }

    #[test]
    fn nested_overlay_keeps_sibling_defaults() {
        let config: TokeiraConfig =
            overlay_document_str("[infrastructure.network]\ngrpc_addr = \"0.0.0.0:9999\"").unwrap();
        let defaults = TokeiraConfig::default();
        assert_eq!(config.infrastructure.network.grpc_addr, "0.0.0.0:9999");
        assert_eq!(
            config.infrastructure.network.metrics_addr,
            defaults.infrastructure.network.metrics_addr
        );
        assert_eq!(
            config.infrastructure.cluster_name,
            defaults.infrastructure.cluster_name
        );
    }

    #[test]
    fn misspelled_field_is_named_not_ignored() {
        let err = overlay_document_str::<TokeiraConfig>("[infrastructure]\nclustre_name = \"x\"")
            .unwrap_err();
        assert!(err.to_string().contains("clustre_name"), "{err}");
    }

    #[test]
    fn render_overlaid_produces_a_complete_valid_document() {
        let rendered = TokeiraConfig::render_overlaid(
            toml::from_str("[infrastructure]\ncluster_name = \"acme\"").unwrap(),
        )
        .unwrap();
        // The rendered document is complete: values the overlay never named
        // are present for the operator to read.
        assert!(rendered.contains("cluster_name = \"acme\""), "{rendered}");
        assert!(rendered.contains("grpc_addr"), "{rendered}");
        // And it reloads to the same document.
        let reloaded: TokeiraConfig = overlay_document_str(&rendered).unwrap();
        assert_eq!(reloaded.infrastructure.cluster_name, "acme");
    }

    #[test]
    fn render_overlaid_refuses_an_invalid_document() {
        let err = TokeiraConfig::render_overlaid(
            toml::from_str("[infrastructure.placement]\nbundle_count = 0").unwrap(),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("infrastructure.placement.bundle_count"),
            "{err}"
        );
    }

    proptest! {
        // Property: an overlaid document renders and reloads to itself — the
        // render is a faithful, complete printout of the effective config.
        #[test]
        fn overlaid_documents_render_and_reload_losslessly(
            cluster in "[a-z][a-z0-9-]{0,18}",
            port in 1u16..,
        ) {
            let overlay = format!(
                "[infrastructure]\ncluster_name = \"{cluster}\"\n\n[infrastructure.network]\ngrpc_addr = \"0.0.0.0:{port}\"",
            );
            let config: TokeiraConfig = overlay_document_str(&overlay).unwrap();
            let rendered = render_document(&config).unwrap();
            let reloaded: TokeiraConfig = overlay_document_str(&rendered).unwrap();
            prop_assert_eq!(reloaded, config);
        }
    }
}
