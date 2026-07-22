//! Host admission and HTTP-header to gRPC-metadata translation.

use std::collections::HashSet;

use super::HttpApiError;
use base64::{Engine as _, engine::general_purpose::STANDARD};

#[cfg(feature = "conformance")]
const HTTP_ALLOWED_HOSTS_KEY: &str = "frontend.httpAllowedHosts";

const PERMANENT_HEADERS: &[&str] = &[
    "accept",
    "accept-charset",
    "accept-language",
    "accept-ranges",
    "authorization",
    "cache-control",
    "content-type",
    "cookie",
    "date",
    "expect",
    "from",
    "host",
    "if-match",
    "if-modified-since",
    "if-none-match",
    "if-schedule-tag-match",
    "if-unmodified-since",
    "max-forwards",
    "origin",
    "pragma",
    "referer",
    "te",
    "user-agent",
    "via",
    "warning",
];

const TEMPORAL_ADDITIONS: &[&str] = &[
    "authorization-extras",
    "x-forwarded-for",
    "client-name",
    "client-version",
];

/// Compiled static policy for Temporal's public HTTP/JSON gateway.
#[derive(Clone, Debug)]
pub struct HttpApiPolicy {
    allowed_hosts: Vec<String>,
    exact_headers: HashSet<String>,
    header_prefixes: Vec<String>,
}

/// One neutral gRPC metadata entry emitted by the header policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpApiMetadata {
    /// Lower-case gRPC metadata key.
    pub name: String,
    /// Wire value; binary metadata remains base64 text for the synthetic request.
    pub value: Vec<u8>,
}

impl HttpApiPolicy {
    /// Compile validated config values into request-path policy.
    pub fn new(
        allowed_hosts: Vec<String>,
        additional_forwarded_headers: Vec<String>,
    ) -> Result<Self, HttpApiError> {
        let mut exact_headers = TEMPORAL_ADDITIONS
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<HashSet<_>>();
        let mut header_prefixes = Vec::new();
        for rule in additional_forwarded_headers {
            let normalized = rule.trim().to_ascii_lowercase();
            let stars = normalized.bytes().filter(|byte| *byte == b'*').count();
            if normalized.is_empty() || stars > 1 || (stars == 1 && !normalized.ends_with('*')) {
                return Err(HttpApiError::Descriptor(format!(
                    "invalid forwarded-header rule `{rule}`"
                )));
            }
            if let Some(prefix) = normalized.strip_suffix('*') {
                header_prefixes.push(prefix.to_owned());
            } else {
                exact_headers.insert(normalized);
            }
        }
        Ok(Self {
            allowed_hosts,
            exact_headers,
            header_prefixes,
        })
    }

    /// Default allow-all Host policy and only Temporal's built-in header additions.
    pub fn permissive() -> Self {
        Self::new(vec!["*".to_owned()], Vec::new()).expect("static default policy is valid")
    }

    /// Whether the complete Host value is admitted by the current effective policy.
    pub fn host_allowed(&self, host: &str) -> bool {
        let effective = self.effective_allowed_hosts();
        effective
            .iter()
            .any(|pattern| wildcard_match(pattern, host))
    }

    /// Convert eligible HTTP headers into the metadata visible to the inner Tonic request.
    pub fn forward_headers(
        &self,
        incoming: &[(String, Vec<u8>)],
        host: &str,
        peer_ip: Option<&str>,
    ) -> Result<Vec<HttpApiMetadata>, HttpApiError> {
        let mut outgoing = Vec::new();
        for (name, value) in incoming {
            let lower = name.as_str();
            if matches!(lower, "x-forwarded-for" | "x-forwarded-host") {
                continue;
            }
            if lower == "authorization" {
                append(&mut outgoing, "authorization", value.clone());
            }
            let target = if self.exact_headers.contains(lower)
                || self
                    .header_prefixes
                    .iter()
                    .any(|prefix| lower.starts_with(prefix))
            {
                Some(lower.to_owned())
            } else if PERMANENT_HEADERS.contains(&lower) {
                Some(format!("grpcgateway-{lower}"))
            } else {
                lower.strip_prefix("grpc-metadata-").map(ToOwned::to_owned)
            };
            let Some(target) = target else {
                continue;
            };
            if target.ends_with("-bin") {
                STANDARD.decode(value).map_err(|error| {
                    HttpApiError::InvalidRequest(format!(
                        "invalid binary header `{}`: {error}",
                        name
                    ))
                })?;
            } else if value.iter().any(|byte| !(0x20..=0x7e).contains(byte)) {
                // gRPC-Gateway logs and skips non-ASCII text metadata rather
                // than failing the request (`runtime/context.go @ v2.27.1`).
                continue;
            }
            append(&mut outgoing, &target, value.clone());
        }

        let forwarded_host = incoming
            .iter()
            .find(|(name, _)| name == "x-forwarded-host")
            .and_then(|(_, value)| std::str::from_utf8(value).ok())
            .unwrap_or(host);
        if !forwarded_host.is_empty() {
            append(
                &mut outgoing,
                "x-forwarded-host",
                forwarded_host.as_bytes().to_vec(),
            );
        }

        let mut forwarded_for = incoming
            .iter()
            .filter(|(name, _)| name == "x-forwarded-for")
            .filter_map(|(_, value)| std::str::from_utf8(value).ok())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if let Some(peer_ip) = peer_ip.filter(|value| !value.is_empty()) {
            forwarded_for.push(peer_ip.to_owned());
        }
        if !forwarded_for.is_empty() {
            append(
                &mut outgoing,
                "x-forwarded-for",
                forwarded_for.join(", ").into_bytes(),
            );
        }

        if !outgoing.iter().any(|entry| entry.name == "client-name") {
            append(
                &mut outgoing,
                "client-name",
                b"temporal-server-http".to_vec(),
            );
        }
        if !outgoing.iter().any(|entry| entry.name == "client-version") {
            append(
                &mut outgoing,
                "client-version",
                tokeira_build_info::TEMPORAL_SERVER_COMPAT
                    .as_bytes()
                    .to_vec(),
            );
        }
        Ok(outgoing)
    }

    fn effective_allowed_hosts(&self) -> Vec<String> {
        #[cfg(feature = "conformance")]
        if let Some(json) = tokeira_conformance::overrides().get_json(HTTP_ALLOWED_HOSTS_KEY) {
            return serde_json::from_str::<Vec<String>>(&json).unwrap_or_else(|error| {
                tracing::error!(
                    key = HTTP_ALLOWED_HOSTS_KEY,
                    %error,
                    "invalid conformance HTTP Host override; failing closed"
                );
                Vec::new()
            });
        }
        self.allowed_hosts.clone()
    }
}

impl Default for HttpApiPolicy {
    fn default() -> Self {
        Self::permissive()
    }
}

fn append(headers: &mut Vec<HttpApiMetadata>, name: &str, value: Vec<u8>) {
    headers.push(HttpApiMetadata {
        name: name.to_owned(),
        value,
    });
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for byte in pattern {
        let mut next = vec![false; value.len() + 1];
        if *byte == b'*' {
            next[0] = previous[0];
            for index in 1..=value.len() {
                next[index] = previous[index] || next[index - 1];
            }
        } else {
            for index in 1..=value.len() {
                next[index] = previous[index - 1] && value[index - 1] == *byte;
            }
        }
        previous = next;
    }
    previous[value.len()]
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn wildcard_reference(pattern: &str, value: &str) -> bool {
        fn matches(
            pattern: &[u8],
            value: &[u8],
            pattern_at: usize,
            value_at: usize,
            memo: &mut std::collections::HashMap<(usize, usize), bool>,
        ) -> bool {
            if let Some(result) = memo.get(&(pattern_at, value_at)) {
                return *result;
            }
            let result = match pattern.get(pattern_at) {
                None => value_at == value.len(),
                Some(b'*') => {
                    matches(pattern, value, pattern_at + 1, value_at, memo)
                        || (value_at < value.len()
                            && matches(pattern, value, pattern_at, value_at + 1, memo))
                }
                Some(byte) => {
                    value.get(value_at) == Some(byte)
                        && matches(pattern, value, pattern_at + 1, value_at + 1, memo)
                }
            };
            memo.insert((pattern_at, value_at), result);
            result
        }

        matches(
            pattern.as_bytes(),
            value.as_bytes(),
            0,
            0,
            &mut std::collections::HashMap::new(),
        )
    }

    #[test]
    fn wildcard_hosts_are_full_string_and_case_sensitive() {
        assert!(wildcard_match("*.example:*", "api.example:7233"));
        assert!(!wildcard_match("*.example", "api.example:7233"));
        assert!(!wildcard_match("API.*", "api.example"));
        assert!(wildcard_match("*", "anything"));
    }

    #[test]
    fn forwarding_preserves_temporal_and_operator_headers_only() {
        let policy = HttpApiPolicy::new(
            vec!["*".to_owned()],
            vec!["this-exact".to_owned(), "this-prefix-*".to_owned()],
        )
        .expect("policy");
        let incoming = vec![
            ("authorization".to_owned(), b"token".to_vec()),
            ("this-exact".to_owned(), b"exact".to_vec()),
            ("this-prefix-one".to_owned(), b"prefix".to_vec()),
            ("not-forwarded".to_owned(), b"no".to_vec()),
            ("x-forwarded-for".to_owned(), b"1.2.3.4:5".to_vec()),
        ];
        let outgoing = policy
            .forward_headers(&incoming, "host", Some("127.0.0.1"))
            .expect("headers");
        let find = |name: &str| {
            outgoing
                .iter()
                .find(|entry| entry.name == name)
                .map(|entry| entry.value.as_slice())
        };
        assert_eq!(find("authorization"), Some(b"token".as_slice()));
        assert_eq!(find("grpcgateway-authorization"), Some(b"token".as_slice()));
        assert_eq!(find("this-exact"), Some(b"exact".as_slice()));
        assert_eq!(find("this-prefix-one"), Some(b"prefix".as_slice()));
        assert_eq!(find("not-forwarded"), None);
        assert_eq!(
            find("x-forwarded-for"),
            Some(b"1.2.3.4:5, 127.0.0.1".as_slice())
        );
        assert_eq!(
            find("client-name"),
            Some(b"temporal-server-http".as_slice())
        );
    }

    #[cfg(feature = "conformance")]
    #[test]
    fn live_host_override_replaces_clears_and_fails_closed() {
        static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let overrides = tokeira_conformance::overrides();
        overrides.reset();
        let policy =
            HttpApiPolicy::new(vec!["static.example".to_owned()], Vec::new()).expect("policy");

        overrides
            .set(
                HTTP_ALLOWED_HOSTS_KEY,
                tokeira_conformance::OverrideValue::Json(r#"["override.example"]"#.to_owned()),
            )
            .expect("override");
        assert!(policy.host_allowed("override.example"));
        assert!(!policy.host_allowed("static.example"));

        overrides
            .set(
                HTTP_ALLOWED_HOSTS_KEY,
                tokeira_conformance::OverrideValue::Json("not-json".to_owned()),
            )
            .expect("typed override");
        assert!(!policy.host_allowed("override.example"));
        assert!(!policy.host_allowed("static.example"));

        overrides.clear(HTTP_ALLOWED_HOSTS_KEY);
        assert!(policy.host_allowed("static.example"));
        overrides.reset();
    }

    proptest! {
        // Feature: edge-http-api-gateway, Property 6
        #[test]
        fn host_glob_matches_reference(
            pattern in "[A-Za-z0-9.*:-]{0,16}",
            host in "[A-Za-z0-9.:-]{0,16}",
        ) {
            prop_assert_eq!(wildcard_match(&pattern, &host), wildcard_reference(&pattern, &host));
        }

        // Feature: edge-http-api-gateway, Property 7
        #[test]
        fn configured_header_rules_do_not_escape_their_exact_or_prefix_scope(
            suffix in "[a-z0-9-]{1,24}",
            value in "[A-Za-z0-9 ]{0,24}",
        ) {
            let policy = HttpApiPolicy::new(
                vec!["*".to_owned()],
                vec!["x-exact".to_owned(), "x-prefix-*".to_owned()],
            ).expect("policy");
            let unknown = format!("x-unrelated-{suffix}");
            let prefixed = format!("x-prefix-{suffix}");
            let incoming = vec![
                (unknown.clone(), value.as_bytes().to_vec()),
                (prefixed.clone(), value.as_bytes().to_vec()),
                ("x-exact".to_owned(), value.as_bytes().to_vec()),
            ];
            let outgoing = policy.forward_headers(&incoming, "host", None).expect("headers");
            prop_assert!(!outgoing.iter().any(|entry| entry.name == unknown));
            prop_assert!(outgoing.iter().any(|entry| entry.name == prefixed));
            prop_assert!(outgoing.iter().any(|entry| entry.name == "x-exact"));
        }
    }
}
