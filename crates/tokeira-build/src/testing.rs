//! Offline fakes for the pipelines: a scripted wire under the real SDK
//! client. No Dagger engine is required — and no seam of ours stands
//! between the pipelines and the client they actually ship with.

use std::sync::{Arc, Mutex};

/// A canned wire: the SDK's own [`dagger_sdk::EngineConnection`] seam, driven
/// by tests. Every GraphQL request the client executes is recorded verbatim;
/// responses are synthesized by nesting a leaf value under the request's own
/// selection path, so the *real* client — codegen, query building, lazy id
/// resolution, decode — runs end to end with no engine.
///
/// Leaf defaults: `id` leaves answer a unique canned identifier, `publish`
/// echoes its address with a canned digest, `entries` answers an empty list,
/// and everything else answers JSON null (which projects through any
/// selection and satisfies Void-typed leaves). [`CannedWire::fail_next`]
/// makes the next execution answer as a GraphQL error.
#[derive(Clone, Debug, Default)]
pub struct CannedWire {
    wire_state: Arc<Mutex<CannedWireState>>,
}

#[derive(Debug, Default)]
struct CannedWireState {
    requests: Vec<String>,
    fail_next: Option<String>,
    ids_issued: usize,
}

impl CannedWire {
    /// Every GraphQL request executed so far, in order, verbatim.
    pub fn requests(&self) -> Vec<String> {
        self.wire_state
            .lock()
            .expect("canned wire lock")
            .requests
            .clone()
    }

    /// All requests joined into one transcript. Lazy object arguments
    /// (directories, files, secrets, cache volumes) resolve through their
    /// own id requests, so chain fragments spread across requests —
    /// containment assertions belong here.
    pub fn transcript(&self) -> String {
        self.requests().join("\n---\n")
    }

    /// Answer the next execution with a GraphQL error carrying `message`.
    pub fn fail_next(&self, message: &str) {
        self.wire_state.lock().expect("canned wire lock").fail_next = Some(message.to_owned());
    }

    fn answer(&self, query: &str) -> dagger_sdk::ResponseData {
        let path = selection_path(query);
        let leaf = match path.last().map(String::as_str) {
            Some("id") => {
                let mut state = self.wire_state.lock().expect("canned wire lock");
                state.ids_issued += 1;
                serde_json::Value::String(format!("canned-id-{}", state.ids_issued))
            }
            Some("publish") => {
                let address = string_argument(query, "publish", "address").unwrap_or_default();
                serde_json::Value::String(format!("{address}@sha256:canned"))
            }
            Some("export") => {
                // A real engine writes the exported file host-side through
                // the session; the canned wire does the same with
                // deterministic bytes, so pipelines that hash what they
                // exported exercise real host-side checksumming.
                let host_path = string_argument(query, "export", "path").unwrap_or_default();
                if let Some(parent) = std::path::Path::new(&host_path).parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                std::fs::write(&host_path, canned_artifact_bytes(&host_path))
                    .expect("canned export writes");
                serde_json::Value::String(host_path)
            }
            Some("stdout" | "stderr") => serde_json::Value::String(String::new()),
            Some("entries") => serde_json::Value::Array(Vec::new()),
            _ => serde_json::Value::Null,
        };
        if leaf.is_null() {
            return dagger_sdk::ResponseData::Value(serde_json::Value::Null);
        }
        let nested = path.iter().rev().fold(
            leaf,
            |value, name| serde_json::json!({ name.as_str(): value }),
        );
        dagger_sdk::ResponseData::Value(nested)
    }
}

#[async_trait::async_trait]
impl dagger_sdk::EngineConnection for CannedWire {
    async fn execute(
        &self,
        request: dagger_sdk::RawRequest,
    ) -> Result<dagger_sdk::RawResponse, dagger_sdk::EngineConnectionError> {
        let query = request.query().to_owned();
        let failure = {
            let mut state = self.wire_state.lock().expect("canned wire lock");
            state.requests.push(query.clone());
            state.fail_next.take()
        };
        if let Some(message) = failure {
            return Ok(dagger_sdk::RawResponse::new(dagger_sdk::ResponseData::Null)
                .with_errors(vec![dagger_sdk::GraphQlError::new(message)]));
        }
        Ok(dagger_sdk::RawResponse::new(self.answer(&query)))
    }

    async fn close(&self) -> Result<(), dagger_sdk::EngineConnectionError> {
        Ok(())
    }

    fn abort(&self) {}
}

/// A connected [`dagger_sdk::Client`] over a [`CannedWire`]. An injected
/// connection is the caller's responsibility by construction, so the SDK
/// runs no compatibility validation against it.
pub async fn canned_client() -> (dagger_sdk::Client, CannedWire) {
    let wire = CannedWire::default();
    let config = dagger_sdk::ClientConfig::builder()
        .connection(Box::new(wire.clone()))
        .build()
        .expect("canned client config");
    let client = dagger_sdk::connect_with(config)
        .await
        .expect("canned client connects");
    (client, wire)
}

/// The chain of selected field names in a rendered GraphQL document,
/// outermost first. The renderer's format is `query{a{b(args){leaf}}}` —
/// the scan is quote- and paren-aware so argument content never miscounts
/// nesting.
fn selection_path(query: &str) -> Vec<String> {
    let mut path = Vec::new();
    let mut identifier = String::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut paren_depth = 0usize;
    for ch in query.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '{' if paren_depth == 0 => {
                if !identifier.is_empty() && identifier != "query" {
                    path.push(std::mem::take(&mut identifier));
                }
                identifier.clear();
            }
            '}' if paren_depth == 0 => {
                if !identifier.is_empty() {
                    path.push(std::mem::take(&mut identifier));
                }
            }
            _ if paren_depth == 0 && (ch.is_alphanumeric() || ch == '_') => identifier.push(ch),
            _ => {}
        }
    }
    if !identifier.is_empty() && identifier != "query" {
        path.push(identifier);
    }
    path
}

/// The deterministic bytes a [`CannedWire`] export writes for a host path —
/// tests derive expected checksums from this.
pub(crate) fn canned_artifact_bytes(host_path: &str) -> Vec<u8> {
    format!("canned-artifact:{host_path}").into_bytes()
}

/// The string value of `argument` on the selected `field` in a rendered
/// document, e.g. `publish(address:"X")` → `X`.
fn string_argument(query: &str, field: &str, argument: &str) -> Option<String> {
    let field_at = query.find(&format!("{field}("))?;
    let rest = &query[field_at..];
    let arg_at = rest.find(&format!("{argument}:"))?;
    let after = &rest[arg_at + argument.len() + 1..];
    let opening = after.find('"')?;
    let mut value = String::new();
    let mut escaped = false;
    for ch in after[opening + 1..].chars() {
        if escaped {
            value.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            break;
        } else {
            value.push(ch);
        }
    }
    Some(value)
}
