//! Conservative redaction for config snapshots.
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

fn looks_like_credentialed_connection_string(value: &str) -> bool {
    let Some(scheme_index) = value.find("://") else {
        return false;
    };
    let authority_start = scheme_index + 3;
    let authority = value[authority_start..]
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    authority.contains('@') && authority.contains(':')
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
}
