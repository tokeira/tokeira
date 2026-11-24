//! Conservative redaction for config snapshots and diagnostic messages.
//!
//! This is structural redaction, not a full secret scanner. It is intentionally
//! biased toward hiding too much when a key name looks sensitive or when a URL
//! authority contains embedded credentials. Callers should pass already-redacted
//! values to outward-facing endpoints whenever possible.

use serde_json::Value;

const SENSITIVE_KEYS: &[&str] = &[
    "password",
    "token",
    "secret",
    "credential",
    "authorization",
    "connection_string",
    "private_key",
    "prompt",
    "tool_input",
    "tool_output",
    "payload",
    "error_chain",
];

/// Redact sensitive fields in-place.
pub fn redact_value(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object.iter_mut() {
                if is_sensitive_key(key) {
                    *value = Value::String("[redacted]".to_string());
                } else {
                    redact_value(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_value(value);
            }
        }
        Value::String(string) if looks_like_credentialed_connection_string(string) => {
            *value = Value::String("[redacted]".to_string());
        }
        _ => {}
    }
}

/// Return whether a config key should be redacted regardless of its value.
pub fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SENSITIVE_KEYS
        .iter()
        .any(|sensitive| lower.contains(sensitive))
}

/// Replace a diagnostic with `[redacted]` if it mentions credentials or a
/// credentialed URL; otherwise return the message unchanged.
///
/// Matching is case-insensitive and deliberately hides the entire message.
/// Callers formatting errors must supply only the outermost `Display`, never
/// debug output or a source chain.
pub fn redact_message(message: &str) -> &str {
    let lower = message.to_lowercase();
    if ["password", "token", "secret", "credential", "authorization"]
        .iter()
        .any(|marker| lower.contains(marker))
        || looks_like_credentialed_connection_string(message)
    {
        "[redacted]"
    } else {
        message
    }
}

fn looks_like_credentialed_connection_string(value: &str) -> bool {
    value.match_indices("://").any(|(scheme_index, _)| {
        let authority_start = scheme_index + 3;
        let authority = value[authority_start..]
            .split(|character: char| {
                matches!(character, '/' | '?' | '#') || character.is_whitespace()
            })
            .next()
            .unwrap_or_default();
        authority.contains('@')
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    #[test]
    fn redacts_sensitive_diagnostic_messages() {
        for message in [
            "password=fake-password-canary",
            "TOKEN=fake-token-canary",
            "a SECRET was supplied",
            "Credential lookup failed",
            "AUTHORIZATION rejected",
            "failed postgres://user:pass@host/db",
            "failed postgres://user@host/db",
            "https://example.invalid/path then postgres://user:pass@host/db",
        ] {
            assert_eq!(redact_message(message), "[redacted]");
        }
        for message in [
            "index build failed",
            "https://example.invalid/path",
            "https://example.invalid/path contact user@example.invalid",
        ] {
            assert_eq!(redact_message(message), message);
        }
    }

    #[test]
    fn redacts_sensitive_keys_recursively() {
        let mut value = json!({
            "database": {
                "password": "secret",
                "authorization_header": "Bearer token",
                "nested": [{"private_key": "key"}]
            },
            "safe": "visible"
        });

        redact_value(&mut value);

        assert_eq!(value["database"]["password"], "[redacted]");
        assert_eq!(value["database"]["authorization_header"], "[redacted]");
        assert_eq!(value["database"]["nested"][0]["private_key"], "[redacted]");
        assert_eq!(value["safe"], "visible");
    }

    #[test]
    fn redacts_credentialed_connection_strings() {
        let mut value = json!({
            "endpoint": "postgres://user:pass@example.com/database",
            "public_endpoint": "https://example.com/path"
        });

        redact_value(&mut value);

        assert_eq!(value["endpoint"], "[redacted]");
        assert_eq!(value["public_endpoint"], "https://example.com/path");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 18: sensitive content is absent by default
        #[test]
        fn sensitive_content_is_absent_by_default(
            prompt in "prompt-canary-[a-zA-Z0-9]{8,32}",
            tool_input in "tool-input-canary-[a-zA-Z0-9]{8,32}",
            tool_output in "tool-output-canary-[a-zA-Z0-9]{8,32}",
            payload in "payload-canary-[a-zA-Z0-9]{8,32}",
            credential in "credential-canary-[a-zA-Z0-9]{8,32}",
            auth_token in "auth-token-canary-[a-zA-Z0-9]{8,32}",
            creation_token in "creation-token-canary-[a-zA-Z0-9]{8,32}",
            password in "password-canary-[a-zA-Z0-9]{8,32}",
            error_chain in "error-canary-[a-zA-Z0-9]{8,32}",
            host_limit in 0usize..128,
        ) {
            let canaries = [
                prompt.clone(),
                tool_input.clone(),
                tool_output.clone(),
                payload.clone(),
                credential.clone(),
                auth_token.clone(),
                creation_token.clone(),
                password.clone(),
                error_chain.clone(),
            ];
            let mut value = json!({
                "prompt": prompt,
                "tool_input": tool_input,
                "tool_output": tool_output,
                "workflow_payload": payload,
                "aws_credentials": credential,
                "dsql_auth_token": auth_token,
                "creation_client_token": creation_token,
                "connection_password": password,
                "nested": { "error_chain": error_chain },
                "public": "bounded-operational-value",
            });
            redact_value(&mut value);
            let rendered = serde_json::to_string(&value).expect("redacted JSON");
            for canary in &canaries {
                prop_assert!(!rendered.contains(canary));
            }

            // Deliberate content capture belongs to the host. This fixture
            // models a host redactor and only asserts its own explicit bound;
            // Tokeira defines no switch or provider API for it.
            let host_output = canaries[0].chars().take(host_limit).collect::<String>();
            prop_assert!(host_output.chars().count() <= host_limit);
        }
    }
}
