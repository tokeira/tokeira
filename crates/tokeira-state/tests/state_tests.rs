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

    fn stub_client() -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

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
        let store = S3StateStore::<TestDoc>::new(
            stub_client(),
            "my-bucket".into(),
            "my-project/infra".into(),
        );
        assert!(
            store
                .manifest_key()
                .contains("my-project/infra/manifest.json")
        );
    }

    #[test]
    fn snapshot_prefix_contains_prefix() {
        let store = S3StateStore::<TestDoc>::new(
            stub_client(),
            "my-bucket".into(),
            "my-project/runtime".into(),
        );
        assert!(
            store
                .snapshot_prefix()
                .contains("my-project/runtime/snapshots")
        );
    }

    #[test]
    fn trailing_slash_is_trimmed() {
        let store =
            S3StateStore::<TestDoc>::new(stub_client(), "bucket".into(), "prefix/infra/".into());
        assert_eq!(store.manifest_key(), "prefix/infra/manifest.json");
        assert_eq!(store.snapshot_prefix(), "prefix/infra/snapshots");
    }
}
