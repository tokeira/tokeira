use std::collections::HashMap;

use serde_json::{Map, Value};

pub(crate) fn decode_policy_string(policy: &str) -> String {
    match urlencoding::decode(policy) {
        Ok(decoded) => decoded.into_owned(),
        Err(_) => policy.to_string(),
    }
}

pub(crate) fn inline_policies_equal(
    current: &HashMap<String, String>,
    desired: &HashMap<String, String>,
) -> bool {
    if current.len() != desired.len() {
        return false;
    }

    current.iter().all(|(name, current_policy)| {
        desired
            .get(name)
            .is_some_and(|desired_policy| policies_equal(current_policy, desired_policy))
    })
}

pub(crate) fn canonical_policy_string(policy: &str) -> String {
    normalized_policy_value(policy)
        .map(|value| canonical_json_string(&value))
        .unwrap_or_else(|| decode_policy_string(policy))
}

pub(crate) fn inline_policies_diff_summary(
    current: &HashMap<String, String>,
    desired: &HashMap<String, String>,
) -> String {
    let mut names: Vec<&str> = current
        .keys()
        .map(String::as_str)
        .chain(desired.keys().map(String::as_str))
        .collect();
    names.sort_unstable();
    names.dedup();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut modified = Vec::new();

    for name in names {
        match (current.get(name), desired.get(name)) {
            (None, Some(_)) => added.push(name),
            (Some(_), None) => removed.push(name),
            (Some(current_policy), Some(desired_policy)) => {
                if !policies_equal(current_policy, desired_policy) {
                    modified.push(name);
                }
            }
            (None, None) => {}
        }
    }

    let mut parts = Vec::new();
    if !added.is_empty() {
        parts.push(format!("added={}", added.join(",")));
    }
    if !removed.is_empty() {
        parts.push(format!("removed={}", removed.join(",")));
    }
    if !modified.is_empty() {
        parts.push(format!("modified={}", modified.join(",")));
    }

    if parts.is_empty() {
        "details unavailable".into()
    } else {
        parts.join(" ")
    }
}

fn policies_equal(current: &str, desired: &str) -> bool {
    match (
        normalized_policy_value(current),
        normalized_policy_value(desired),
    ) {
        (Some(current), Some(desired)) => current == desired,
        (None, None) => decode_policy_string(current) == decode_policy_string(desired),
        _ => false,
    }
}

fn normalized_policy_value(policy: &str) -> Option<Value> {
    let decoded = decode_policy_string(policy);

    if let Ok(parsed) = aws_iam::io::read_from_string(&decoded) {
        let value = serde_json::to_value(parsed).ok()?;
        return Some(normalize_policy(value));
    }

    serde_json::from_str::<Value>(&decoded)
        .ok()
        .map(normalize_policy)
}

fn normalize_policy(value: Value) -> Value {
    normalize_named_value(None, value)
}

fn normalize_named_value(key: Option<&str>, value: Value) -> Value {
    match value {
        Value::Object(map) => normalize_object(key, map),
        Value::Array(values) => normalize_array(key, values),
        other => other,
    }
}

fn normalize_object(_key: Option<&str>, map: Map<String, Value>) -> Value {
    let mut normalized = Map::new();
    for (child_key, child_value) in map {
        let normalized_value = if child_key == "Statement" {
            normalize_statement_value(child_value)
        } else if is_set_like_field(&child_key) {
            normalize_set_like_value(child_value)
        } else {
            normalize_named_value(Some(&child_key), child_value)
        };
        normalized.insert(child_key, normalized_value);
    }

    Value::Object(normalized)
}

fn normalize_array(key: Option<&str>, values: Vec<Value>) -> Value {
    let mut normalized: Vec<Value> = values
        .into_iter()
        .map(|value| normalize_named_value(key, value))
        .collect();

    if should_sort_array(key, &normalized) {
        normalized.sort_by_key(canonical_json_string);
    }

    Value::Array(normalized)
}

fn normalize_statement_value(value: Value) -> Value {
    let mut statements = match value {
        Value::Array(values) => values
            .into_iter()
            .map(|statement| normalize_named_value(Some("StatementEntry"), statement))
            .collect::<Vec<_>>(),
        other => vec![normalize_named_value(Some("StatementEntry"), other)],
    };
    statements.sort_by_key(canonical_json_string);
    Value::Array(statements)
}

fn should_sort_array(key: Option<&str>, values: &[Value]) -> bool {
    is_set_like_key(key) || values.iter().all(Value::is_string)
}

fn canonical_json_string(value: &Value) -> String {
    serde_json::to_string(value).expect("value should serialize")
}

fn normalize_set_like_value(value: Value) -> Value {
    let values = match value {
        Value::Array(values) => values,
        other => vec![other],
    };
    normalize_array(None, values)
}

fn is_set_like_field(key: &str) -> bool {
    matches!(
        key,
        "Action"
            | "NotAction"
            | "Resource"
            | "NotResource"
            | "AWS"
            | "Service"
            | "Federated"
            | "CanonicalUser"
    )
}

fn is_set_like_key(key: Option<&str>) -> bool {
    key.is_some_and(is_set_like_field)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        canonical_policy_string, decode_policy_string, inline_policies_diff_summary,
        inline_policies_equal,
    };

    #[test]
    fn ignores_statement_and_action_order() {
        let current = HashMap::from([(
            "policy".to_string(),
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "B",
                        "Effect": "Allow",
                        "Action": ["s3:PutObject", "s3:GetObject"],
                        "Resource": "arn:aws:s3:::bucket/*"
                    },
                    {
                        "Sid": "A",
                        "Effect": "Allow",
                        "Action": "dynamodb:GetItem",
                        "Resource": "*"
                    }
                ]
            })
            .to_string(),
        )]);

        let desired = HashMap::from([(
            "policy".to_string(),
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "A",
                        "Effect": "Allow",
                        "Action": ["dynamodb:GetItem"],
                        "Resource": ["*"]
                    },
                    {
                        "Sid": "B",
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": ["arn:aws:s3:::bucket/*"]
                    }
                ]
            })
            .to_string(),
        )]);

        assert!(inline_policies_equal(&current, &desired));
    }

    #[test]
    fn detects_semantic_policy_change() {
        let current = HashMap::from([(
            "policy".to_string(),
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::bucket/*"
                }]
            })
            .to_string(),
        )]);

        let desired = HashMap::from([(
            "policy".to_string(),
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "s3:PutObject",
                    "Resource": "arn:aws:s3:::bucket/*"
                }]
            })
            .to_string(),
        )]);

        assert!(!inline_policies_equal(&current, &desired));
    }

    #[test]
    fn canonicalizes_singletons_and_statement_order() {
        let current = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        })
        .to_string();
        let desired = serde_json::json!({
            "Statement": [{
                "Resource": ["*"],
                "Action": ["s3:GetObject"],
                "Effect": "Allow"
            }],
            "Version": "2012-10-17"
        })
        .to_string();

        assert_eq!(
            canonical_policy_string(&current),
            canonical_policy_string(&desired)
        );
    }

    #[test]
    fn decodes_url_encoded_policy_documents() {
        let encoded = "%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Action%22%3A%22s3%3AGetObject%22%2C%22Resource%22%3A%22%2A%22%7D%5D%7D";
        let decoded = decode_policy_string(encoded);

        assert_eq!(
            decoded,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#
        );
    }

    #[test]
    fn summarizes_added_removed_and_modified_policies() {
        let current = HashMap::from([
            (
                "a".to_string(),
                r#"{"Statement":[{"Action":"s3:GetObject"}]}"#.to_string(),
            ),
            (
                "b".to_string(),
                r#"{"Statement":[{"Action":"s3:GetObject"}]}"#.to_string(),
            ),
        ]);
        let desired = HashMap::from([
            (
                "a".to_string(),
                r#"{"Statement":[{"Action":"s3:PutObject"}]}"#.to_string(),
            ),
            (
                "c".to_string(),
                r#"{"Statement":[{"Action":"dynamodb:GetItem"}]}"#.to_string(),
            ),
        ]);

        let summary = inline_policies_diff_summary(&current, &desired);

        assert!(summary.contains("added=c"));
        assert!(summary.contains("removed=b"));
        assert!(summary.contains("modified=a"));
    }
}
