//! Thin Dagger GraphQL client.
//!
//! Sends flat sequential queries with ID passing between steps.
//!
//! Uses `loadContainerFromID` / `loadDirectoryFromID` / `loadFileFromID`
//! to resume objects from their opaque IDs between queries.
//!
//! # Usage
//!
//! Launch your binary via `dagger run cargo run --package tokeira-build -- build`.
//! Dagger injects `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` automatically.
//!
//! ```no_run
//! # fn main() -> eyre::Result<()> {
//! let client = dagger_client::Client::from_env()?;
//! let src = client.host_directory("/path/to/source")?;
//! let ctr = client.container_from("alpine:3.22")?;
//! let ctr = ctr.with_directory("/src", &src)?;
//! let ctr = ctr.with_exec(&["ls", "-la"])?;
//! # Ok(())
//! # }
//! ```

use base64::Engine as _;
use eyre::{Result, WrapErr, bail};
use serde_json::Value;

// ── Newtype IDs ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ContainerId(String);

#[derive(Debug, Clone)]
pub struct DirectoryId(String);

#[derive(Debug, Clone)]
pub struct FileId(String);

#[derive(Debug, Clone)]
pub struct SecretId(String);

// ── Client ──────────────────────────────────────────────────

pub struct Client {
    http: reqwest::blocking::Client,
    url: String,
    auth: String,
}

impl std::fmt::Debug for Client {
    /// Masks the `auth` header (which carries the session token) so Debug
    /// output is safe to log. AGENTS.md requires `Debug` on all public types;
    /// a raw derive would leak the token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("url", &self.url)
            .field("auth", &"<redacted>")
            .finish()
    }
}

impl Client {
    /// Create a client from `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN`
    /// environment variables (injected by `dagger run`).
    pub fn from_env() -> Result<Self> {
        let port = std::env::var("DAGGER_SESSION_PORT")
            .wrap_err("DAGGER_SESSION_PORT not set — run via `dagger run`")?;
        let token = std::env::var("DAGGER_SESSION_TOKEN")
            .wrap_err("DAGGER_SESSION_TOKEN not set — run via `dagger run`")?;

        let auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{token}:"))
        );

        Ok(Self {
            http: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(600))
                .build()
                .wrap_err("Failed to build HTTP client")?,
            url: format!("http://127.0.0.1:{port}/query"),
            auth,
        })
    }

    // ── Top-level constructors ──────────────────────────────

    /// Load a directory from the host filesystem.
    pub fn host_directory(&self, path: &str) -> Result<Directory<'_>> {
        self.host_directory_filtered(path, &[], &[])
    }

    /// Load a directory from the host filesystem with gitignore-style
    /// exclude and include patterns.
    ///
    /// Avoids uploading large directories (Rust `target/`, `.git/`, caches)
    /// that Dagger would otherwise hash and ship to the engine. Patterns are
    /// interpreted by the Dagger engine the same way a `.dockerignore` does.
    /// When both slices are empty, this is equivalent to `host_directory`.
    pub fn host_directory_filtered(
        &self,
        path: &str,
        exclude: &[&str],
        include: &[&str],
    ) -> Result<Directory<'_>> {
        let mut args = format!("path: {}", quote(path));
        if !exclude.is_empty() {
            args.push_str(&format!(", exclude: {}", patterns_to_graphql(exclude)));
        }
        if !include.is_empty() {
            args.push_str(&format!(", include: {}", patterns_to_graphql(include)));
        }
        let q = format!(r#"{{ host {{ directory({args}) {{ id }} }} }}"#);
        let id = self.query_path(&q, &["host", "directory", "id"])?;
        Ok(Directory {
            client: self,
            id: DirectoryId(id),
        })
    }

    /// Load a single file from the host filesystem.
    pub fn host_file(&self, path: &str) -> Result<File<'_>> {
        let q = format!(
            r#"{{ host {{ file(path: {path}) {{ id }} }} }}"#,
            path = quote(path),
        );
        let id = self.query_path(&q, &["host", "file", "id"])?;
        Ok(File {
            client: self,
            id: FileId(id),
        })
    }

    /// Create a container from a base image.
    pub fn container_from(&self, address: &str) -> Result<Container<'_>> {
        let q = format!(
            r#"{{ container {{ from(address: {addr}) {{ id }} }} }}"#,
            addr = quote(address),
        );
        let id = self.query_path(&q, &["container", "from", "id"])?;
        Ok(Container {
            client: self,
            id: ContainerId(id),
        })
    }

    /// Build a container from a Dockerfile in a host directory context.
    pub fn container_build(
        &self,
        context: &Directory<'_>,
        dockerfile: &str,
    ) -> Result<Container<'_>> {
        let q = format!(
            r#"{{ container {{ build(context: {context}, dockerfile: {dockerfile}) {{ id }} }} }}"#,
            context = quote(&context.id.0),
            dockerfile = quote(dockerfile),
        );
        let id = self.query_path(&q, &["container", "build", "id"])?;
        Ok(Container {
            client: self,
            id: ContainerId(id),
        })
    }

    /// Create a Dagger secret from plaintext.
    pub fn set_secret(&self, name: &str, plaintext: &str) -> Result<SecretId> {
        let q = format!(
            r#"{{ setSecret(name: {name}, plaintext: {plaintext}) {{ id }} }}"#,
            name = quote(name),
            plaintext = quote(plaintext),
        );
        let id = self.query_path(&q, &["setSecret", "id"])?;
        Ok(SecretId(id))
    }

    // ── Internal query engine ───────────────────────────────

    fn query_path(&self, graphql: &str, path: &[&str]) -> Result<String> {
        let body = serde_json::json!({ "query": graphql });
        let resp: Value = self
            .http
            .post(&self.url)
            .header("Authorization", &self.auth)
            .json(&body)
            .send()
            .wrap_err("Dagger HTTP request failed")?
            .json()
            .wrap_err("Dagger response is not valid JSON")?;

        check_graphql_errors(&resp)?;

        let mut node = resp
            .get("data")
            .ok_or_else(|| eyre::eyre!("Dagger response missing 'data' field: {resp}"))?;

        for &key in path {
            node = node.get(key).ok_or_else(|| {
                eyre::eyre!("Dagger response missing key '{key}' in path {path:?}: {resp}")
            })?;
        }

        node.as_str()
            .map(|s| s.to_owned())
            .ok_or_else(|| eyre::eyre!("Expected string at path {path:?}, got: {node}"))
    }
}

// ── Container ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Container<'c> {
    client: &'c Client,
    id: ContainerId,
}

/// Helper: wrap a query that loads a container from ID, calls a method, and returns the new ID.
/// Pattern: `{ loadContainerFromID(id: "...") { <method>(<args>) { id } } }`
macro_rules! container_op {
    ($self:expr, $method:expr, $args:expr) => {{
        let q = format!(
            r#"{{ loadContainerFromID(id: {id}) {{ {method}({args}) {{ id }} }} }}"#,
            id = quote(&$self.id.0),
            method = $method,
            args = $args,
        );
        let id = $self
            .client
            .query_path(&q, &["loadContainerFromID", $method, "id"])?;
        Ok(Container {
            client: $self.client,
            id: ContainerId(id),
        })
    }};
}

impl<'c> Container<'c> {
    pub fn with_exec(self, args: &[&str]) -> Result<Self> {
        let args_json = format!(
            "[{}]",
            args.iter().map(|a| quote(a)).collect::<Vec<_>>().join(", ")
        );
        container_op!(self, "withExec", format!("args: {args_json}"))
    }

    pub fn with_directory(self, path: &str, dir: &Directory) -> Result<Self> {
        container_op!(
            self,
            "withDirectory",
            format!("path: {}, source: {}", quote(path), quote(&dir.id.0))
        )
    }

    pub fn with_workdir(self, path: &str) -> Result<Self> {
        container_op!(self, "withWorkdir", format!("path: {}", quote(path)))
    }

    pub fn with_env_variable(self, name: &str, value: &str) -> Result<Self> {
        container_op!(
            self,
            "withEnvVariable",
            format!("name: {}, value: {}", quote(name), quote(value))
        )
    }

    pub fn with_file(self, path: &str, source: &File) -> Result<Self> {
        container_op!(
            self,
            "withFile",
            format!("path: {}, source: {}", quote(path), quote(&source.id.0))
        )
    }

    pub fn with_new_file(self, path: &str, contents: &str) -> Result<Self> {
        container_op!(
            self,
            "withNewFile",
            format!("path: {}, contents: {}", quote(path), quote(contents))
        )
    }

    pub fn with_user(self, name: &str) -> Result<Self> {
        container_op!(self, "withUser", format!("name: {}", quote(name)))
    }

    pub fn with_entrypoint(self, args: &[&str]) -> Result<Self> {
        let args_json = format!(
            "[{}]",
            args.iter().map(|a| quote(a)).collect::<Vec<_>>().join(", ")
        );
        container_op!(self, "withEntrypoint", format!("args: {args_json}"))
    }

    pub fn with_default_args(self, args: &[&str]) -> Result<Self> {
        let args_json = format!(
            "[{}]",
            args.iter().map(|a| quote(a)).collect::<Vec<_>>().join(", ")
        );
        container_op!(self, "withDefaultArgs", format!("args: {args_json}"))
    }

    pub fn with_registry_auth(
        self,
        address: &str,
        username: &str,
        secret: &SecretId,
    ) -> Result<Self> {
        container_op!(
            self,
            "withRegistryAuth",
            format!(
                "address: {}, username: {}, secret: {}",
                quote(address),
                quote(username),
                quote(&secret.0)
            )
        )
    }

    /// Extract a file from the container (returns a lazy `File` reference).
    pub fn file(&self, path: &str) -> Result<File<'c>> {
        let q = format!(
            r#"{{ loadContainerFromID(id: {id}) {{ file(path: {path}) {{ id }} }} }}"#,
            id = quote(&self.id.0),
            path = quote(path),
        );
        let id = self
            .client
            .query_path(&q, &["loadContainerFromID", "file", "id"])?;
        Ok(File {
            client: self.client,
            id: FileId(id),
        })
    }

    /// Export the container as a Docker image to the local daemon.
    /// Writes an OCI tarball via Dagger, then `docker load`s it.
    pub fn export_image(&self, tag: &str) -> Result<()> {
        let tarball = format!("/tmp/dagger-export-{}.tar", tag.replace([':', '/'], "-"));
        let q = format!(
            r#"{{ loadContainerFromID(id: {id}) {{ export(path: {path}) }} }}"#,
            id = quote(&self.id.0),
            path = quote(&tarball),
        );
        self.client
            .query_path(&q, &["loadContainerFromID", "export"])?;

        // `docker load` returns "Loaded image ID: sha256:..." — capture it
        // so we can tag the image with the requested name.
        let output = std::process::Command::new("docker")
            .args(["load", "-i", &tarball])
            .output()
            .wrap_err("Failed to run `docker load`")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("`docker load` failed: {stderr}");
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // Parse image ID from "Loaded image ID: sha256:abc123..."
        if let Some(id) = stdout
            .lines()
            .find_map(|line| line.strip_prefix("Loaded image ID: "))
        {
            let status = std::process::Command::new("docker")
                .args(["tag", id.trim(), tag])
                .status()
                .wrap_err("Failed to run `docker tag`")?;
            if !status.success() {
                bail!("`docker tag {id} {tag}` failed");
            }
        }

        let _ = std::fs::remove_file(&tarball);
        Ok(())
    }

    /// Publish this container image to a registry reference.
    pub fn publish(&self, address: &str) -> Result<String> {
        let q = format!(
            r#"{{ loadContainerFromID(id: {id}) {{ publish(address: {address}) }} }}"#,
            id = quote(&self.id.0),
            address = quote(address),
        );
        self.client
            .query_path(&q, &["loadContainerFromID", "publish"])
    }
}

// ── Directory ───────────────────────────────────────────────

#[derive(Debug)]
pub struct Directory<'c> {
    client: &'c Client,
    id: DirectoryId,
}

impl<'c> Directory<'c> {
    /// Get a file from this directory.
    pub fn file(&self, path: &str) -> Result<File<'c>> {
        let q = format!(
            r#"{{ loadDirectoryFromID(id: {id}) {{ file(path: {path}) {{ id }} }} }}"#,
            id = quote(&self.id.0),
            path = quote(path),
        );
        let id = self
            .client
            .query_path(&q, &["loadDirectoryFromID", "file", "id"])?;
        Ok(File {
            client: self.client,
            id: FileId(id),
        })
    }
}

// ── File ────────────────────────────────────────────────────

#[derive(Debug)]
pub struct File<'c> {
    client: &'c Client,
    id: FileId,
}

impl File<'_> {
    /// Export the file to a path on the host.
    pub fn export(&self, path: &str) -> Result<()> {
        let q = format!(
            r#"{{ loadFileFromID(id: {id}) {{ export(path: {path}) }} }}"#,
            id = quote(&self.id.0),
            path = quote(path),
        );
        self.client.query_path(&q, &["loadFileFromID", "export"])?;
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────

/// Check a GraphQL response for errors and bail if any are present.
pub fn check_graphql_errors(resp: &Value) -> Result<()> {
    if let Some(arr) = resp.get("errors").and_then(|e| e.as_array())
        && !arr.is_empty()
    {
        let msgs: Vec<&str> = arr
            .iter()
            .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
            .collect();
        bail!("Dagger GraphQL error: {}", msgs.join("; "));
    }
    Ok(())
}

/// JSON-escape and quote a string for embedding in a GraphQL query.
pub fn quote(s: &str) -> String {
    serde_json::to_string(s).expect("string serialization cannot fail")
}

/// Render a list of pattern strings as a GraphQL list literal. Each pattern
/// is JSON-quoted the same way `quote` quotes a single string so that
/// glob metacharacters ride through unescaped by our code and are interpreted
/// by the Dagger engine.
fn patterns_to_graphql(patterns: &[&str]) -> String {
    let items = patterns
        .iter()
        .map(|p| quote(p))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{items}]")
}

#[cfg(test)]
// Edition-2024 `std::env` mutation in tests, serialized by ENV_MUTEX — each site
// carries its SAFETY comment.
#[allow(unsafe_code)]
mod tests {
    use super::*;

    /// Serialise env-mutating tests with a process-wide mutex so parallel
    /// tests in this module don't race on `DAGGER_SESSION_*`. The env is a
    /// shared resource and `std::env::set_var` / `remove_var` are not safe
    /// to call concurrently (marked `unsafe` in 2024 edition).
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Save a snapshot of both session env vars, mutate for the test, and
    /// restore on drop so one test's state never leaks to the next.
    struct EnvGuard {
        port: Option<String>,
        token: Option<String>,
    }

    impl EnvGuard {
        fn capture() -> Self {
            Self {
                port: std::env::var("DAGGER_SESSION_PORT").ok(),
                token: std::env::var("DAGGER_SESSION_TOKEN").ok(),
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: tests that call `set_env` / `unset_env` all acquire
            // ENV_MUTEX first, so no concurrent access can occur here.
            unsafe {
                match &self.port {
                    Some(v) => std::env::set_var("DAGGER_SESSION_PORT", v),
                    None => std::env::remove_var("DAGGER_SESSION_PORT"),
                }
                match &self.token {
                    Some(v) => std::env::set_var("DAGGER_SESSION_TOKEN", v),
                    None => std::env::remove_var("DAGGER_SESSION_TOKEN"),
                }
            }
        }
    }

    fn set_env(key: &str, value: &str) {
        // SAFETY: caller holds ENV_MUTEX.
        unsafe { std::env::set_var(key, value) }
    }

    fn unset_env(key: &str) {
        // SAFETY: caller holds ENV_MUTEX.
        unsafe { std::env::remove_var(key) }
    }

    #[test]
    fn from_env_errs_when_session_vars_missing() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let _guard = EnvGuard::capture();

        unset_env("DAGGER_SESSION_PORT");
        unset_env("DAGGER_SESSION_TOKEN");

        let err = Client::from_env().expect_err("from_env must fail without session env");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("DAGGER_SESSION_PORT"),
            "error should name the missing var, got: {msg}"
        );
    }

    #[test]
    fn from_env_succeeds_with_session_vars_set() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let _guard = EnvGuard::capture();

        set_env("DAGGER_SESSION_PORT", "12345");
        set_env("DAGGER_SESSION_TOKEN", "dummy-token");

        let client = Client::from_env().expect("from_env should succeed with both vars set");
        // The client constructs its URL from the port; no network call is made.
        assert!(
            client.url.contains("12345"),
            "client url should embed the session port, got: {}",
            client.url
        );
    }

    #[test]
    fn patterns_to_graphql_emits_a_quoted_list() {
        // Each pattern must be JSON-quoted, comma-separated, and wrapped in
        // a GraphQL list literal so the engine receives a `[String!]` value.
        // Special characters inside patterns are escaped by serde_json.
        let rendered = patterns_to_graphql(&["target/", ".git/", "a\"b"]);
        assert_eq!(rendered, r#"["target/", ".git/", "a\"b"]"#);
    }

    #[test]
    fn patterns_to_graphql_handles_empty() {
        assert_eq!(patterns_to_graphql(&[]), "[]");
    }
}
