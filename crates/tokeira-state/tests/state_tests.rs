//! Tests for the generic state store types.

#[cfg(test)]
mod manifest_tests {
    use tokeira_state::StateManifest;

    #[test]
    fn manifest_empty_has_no_head_or_lock() {
        let manifest = StateManifest::empty();
        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.revision, 0);
        assert!(manifest.head.is_none());
        assert!(manifest.lock.is_none());
    }

    #[test]
    fn manifest_serialization_round_trip() {
        let manifest = StateManifest::empty();
        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: StateManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.schema_version, manifest.schema_version);
        assert_eq!(deserialized.revision, manifest.revision);
    }
}

#[cfg(test)]
mod store_layout_tests {
    use tokeira_state::S3StateStore;

    /// A minimal document type for testing StateStore layout.
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    struct TestDoc {
        value: String,
    }

    impl tokeira_state::Validate for TestDoc {
        fn validate(&self) -> Result<(), tokeira_state::StateError> {
            Ok(())
        }
    }

    #[test]
    fn manifest_key_contains_prefix() {
        let (_prefix, manifest_key, _snapshot_prefix) =
            S3StateStore::<TestDoc>::layout_for_prefix("my-project/infra");
        assert!(manifest_key.contains("my-project/infra/manifest.json"));
    }

    #[test]
    fn snapshot_prefix_contains_prefix() {
        let (_prefix, _manifest_key, snapshot_prefix) =
            S3StateStore::<TestDoc>::layout_for_prefix("my-project/runtime");
        assert!(snapshot_prefix.contains("my-project/runtime/snapshots"));
    }

    #[test]
    fn trailing_slash_is_trimmed() {
        let (prefix, manifest_key, snapshot_prefix) =
            S3StateStore::<TestDoc>::layout_for_prefix("prefix/infra/");
        assert_eq!(prefix, "prefix/infra");
        assert_eq!(manifest_key, "prefix/infra/manifest.json");
        assert_eq!(snapshot_prefix, "prefix/infra/snapshots");
    }
}
