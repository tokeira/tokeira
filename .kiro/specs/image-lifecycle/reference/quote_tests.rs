use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// For any string (including special characters like `"`, `\`, newlines),
    /// `quote()` produces a valid JSON string that round-trips correctly.
    #[test]
    fn quote_produces_valid_json_string(
        s in ".*",
    ) {
        let quoted = dagger_client::quote(&s);
        // Must start and end with double quotes
        prop_assert!(quoted.starts_with('"') && quoted.ends_with('"'));
        // Must be valid JSON when parsed
        let parsed: Result<String, _> = serde_json::from_str(&quoted);
        prop_assert!(parsed.is_ok(), "quote() produced invalid JSON: {quoted}");
        // Round-trip: parsed value must equal original
        prop_assert_eq!(parsed.unwrap(), s);
    }
}
