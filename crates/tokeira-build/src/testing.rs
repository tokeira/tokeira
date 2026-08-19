use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{BuildError, ContainerRef, DaggerClient, DirectoryRef, FileRef, SecretRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MockCall {
    HostDirectory(String),
    HostDirectoryFiltered {
        path: String,
        exclude: Vec<String>,
        include: Vec<String>,
    },
    ContainerFrom(String),
    SetSecret(String),
    WithExec(Vec<String>),
    WithEnv {
        key: String,
        value: String,
    },
    WithWorkdir(String),
    WithDirectory(String),
    WithFile(String),
    WithEntrypoint(Vec<String>),
    WithUser(String),
    WithRegistryAuth {
        registry: String,
        username: String,
    },
    File(String),
    ExportImage(String),
    ExportFile {
        source: String,
        host_path: String,
    },
    Publish(String),
}

#[derive(Debug, Default, Clone)]
pub struct MockDaggerClient {
    state: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    calls: Vec<MockCall>,
    fail_publish: bool,
}

impl MockDaggerClient {
    pub fn calls(&self) -> Vec<MockCall> {
        self.state.lock().expect("mock state lock").calls.clone()
    }

    pub fn with_publish_error(self) -> Self {
        self.state.lock().expect("mock state lock").fail_publish = true;
        self
    }

    fn record(&self, call: MockCall) {
        self.state.lock().expect("mock state lock").calls.push(call);
    }
}

impl DaggerClient for MockDaggerClient {
    fn host_directory<'client>(
        &'client self,
        path: &Path,
    ) -> Result<Box<dyn DirectoryRef<'client> + 'client>, BuildError> {
        self.record(MockCall::HostDirectory(path.display().to_string()));
        Ok(Box::new(MockDirectory {
            state: Arc::clone(&self.state),
        }))
    }

    fn host_directory_filtered<'client>(
        &'client self,
        path: &Path,
        exclude: &[&str],
        include: &[&str],
    ) -> Result<Box<dyn DirectoryRef<'client> + 'client>, BuildError> {
        self.record(MockCall::HostDirectoryFiltered {
            path: path.display().to_string(),
            exclude: to_strings(exclude),
            include: to_strings(include),
        });
        Ok(Box::new(MockDirectory {
            state: Arc::clone(&self.state),
        }))
    }

    fn container_from<'client>(
        &'client self,
        image: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::ContainerFrom(image.to_owned()));
        Ok(Box::new(MockContainer {
            state: Arc::clone(&self.state),
        }))
    }

    fn set_secret<'client>(
        &'client self,
        name: &str,
        _value: &str,
    ) -> Result<Box<dyn SecretRef + 'client>, BuildError> {
        self.record(MockCall::SetSecret(name.to_owned()));
        Ok(Box::new(MockSecret))
    }
}

#[derive(Debug, Clone)]
struct MockContainer {
    state: Arc<Mutex<MockState>>,
}

impl MockContainer {
    fn record(&self, call: MockCall) {
        self.state.lock().expect("mock state lock").calls.push(call);
    }
}

impl<'client> ContainerRef<'client> for MockContainer {
    fn clone_ref(&self) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        Ok(Box::new(self.clone()))
    }

    fn with_exec(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithExec(to_strings(args)));
        Ok(self)
    }

    fn with_env(
        self: Box<Self>,
        key: &str,
        value: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithEnv {
            key: key.to_owned(),
            value: value.to_owned(),
        });
        Ok(self)
    }

    fn with_workdir(
        self: Box<Self>,
        path: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithWorkdir(path.to_owned()));
        Ok(self)
    }

    fn with_directory(
        self: Box<Self>,
        path: &str,
        _dir: &dyn DirectoryRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithDirectory(path.to_owned()));
        Ok(self)
    }

    fn with_file(
        self: Box<Self>,
        path: &str,
        _file: &dyn FileRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithFile(path.to_owned()));
        Ok(self)
    }

    fn with_entrypoint(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithEntrypoint(to_strings(args)));
        Ok(self)
    }

    fn with_user(
        self: Box<Self>,
        user: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithUser(user.to_owned()));
        Ok(self)
    }

    fn with_registry_auth(
        self: Box<Self>,
        registry: &str,
        user: &str,
        _secret: &dyn SecretRef,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithRegistryAuth {
            registry: registry.to_owned(),
            username: user.to_owned(),
        });
        Ok(self)
    }

    fn file(&self, path: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError> {
        self.record(MockCall::File(path.to_owned()));
        Ok(Box::new(MockFile {
            state: Arc::clone(&self.state),
            source: path.to_owned(),
        }))
    }

    fn export_image(&self, tag: &str) -> Result<(), BuildError> {
        self.record(MockCall::ExportImage(tag.to_owned()));
        Ok(())
    }

    fn publish(&self, remote_ref: &str) -> Result<String, BuildError> {
        self.record(MockCall::Publish(remote_ref.to_owned()));
        if self.state.lock().expect("mock state lock").fail_publish {
            return Err(BuildError::Validation {
                reason: "mock publish failure".to_owned(),
            });
        }
        Ok(format!("{remote_ref}@sha256:mock"))
    }
}

#[derive(Debug, Clone)]
struct MockDirectory {
    state: Arc<Mutex<MockState>>,
}

impl<'client> DirectoryRef<'client> for MockDirectory {
    fn file(&self, name: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError> {
        self.state
            .lock()
            .expect("mock state lock")
            .calls
            .push(MockCall::File(name.to_owned()));
        Ok(Box::new(MockFile {
            state: Arc::clone(&self.state),
            source: name.to_owned(),
        }))
    }
}

/// A mock file that knows the container path it came from; `export` writes
/// deterministic bytes derived from that path, so pipelines that hash their
/// exported artifacts (the provisioner build) exercise real host-side
/// checksumming under the mock.
#[derive(Debug, Clone)]
struct MockFile {
    state: Arc<Mutex<MockState>>,
    source: String,
}

impl<'client> FileRef<'client> for MockFile {
    fn export(&self, host_path: &Path) -> Result<(), BuildError> {
        self.state
            .lock()
            .expect("mock state lock")
            .calls
            .push(MockCall::ExportFile {
                source: self.source.clone(),
                host_path: host_path.display().to_string(),
            });
        if let Some(parent) = host_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| BuildError::Validation {
                reason: format!("mock export mkdir: {e}"),
            })?;
        }
        std::fs::write(host_path, mock_artifact_bytes(&self.source)).map_err(|e| {
            BuildError::Validation {
                reason: format!("mock export write: {e}"),
            }
        })
    }
}

/// The deterministic bytes [`MockFile::export`] writes for a container path —
/// tests derive expected checksums from this.
pub fn mock_artifact_bytes(source: &str) -> Vec<u8> {
    format!("mock-artifact:{source}").into_bytes()
}

#[derive(Debug, Clone)]
struct MockSecret;

impl SecretRef for MockSecret {}

fn to_strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_owned()).collect()
}

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
            _ if paren_depth == 0 => {
                if ch.is_alphanumeric() || ch == '_' {
                    identifier.push(ch);
                }
            }
            _ => {}
        }
    }
    if !identifier.is_empty() && identifier != "query" {
        path.push(identifier);
    }
    path
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
