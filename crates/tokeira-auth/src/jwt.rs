//! JWT authentication and per-issuer JWKS key rotation.
//!
//! Tokens are routed by their exact signed `iss` value to one isolated profile.
//! Each profile owns an atomically replaced `kid` index, so a failed refresh can
//! neither clear its last known-good keys nor affect another issuer. The public
//! behavior follows Temporal's default claim mapper and key provider while the
//! exact-issuer routing is Tokeira's documented fail-closed extension.

use std::{collections::HashMap, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use base64::Engine as _;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::{
    AuthError, ClaimMapper, Claims, GrantRules, Role, WorkerScope, WorkerScopeRules,
    resolve_effective_scope,
};

const DEFAULT_PERMISSIONS_CLAIM: &str = "permissions";
const TOKEIRA_WORKER_SCOPE_CLAIM: &str = "tokeira_worker_scope";
const WORKER_SCOPE_VERSION: u32 = 1;

#[derive(Clone)]
struct VerificationKey {
    algorithm: Algorithm,
    key: DecodingKey,
}

impl std::fmt::Debug for VerificationKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerificationKey")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

/// One issuer's atomically refreshed JWKS key index.
pub struct JwksKeyProvider {
    jwks_uri: String,
    client: Option<reqwest::Client>,
    keys: ArcSwap<HashMap<String, VerificationKey>>,
}

impl std::fmt::Debug for JwksKeyProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JwksKeyProvider")
            .field("jwks_uri", &self.jwks_uri)
            .field("key_count", &self.keys.load().len())
            .finish()
    }
}

impl JwksKeyProvider {
    /// Fetch the initial key set and optionally start an isolated refresh loop.
    ///
    /// Initial retrieval failure is deliberately non-fatal: v1.31.0 starts with
    /// an empty set, causing token verification to fail closed until a refresh
    /// succeeds (`default_token_key_provider.go:43-53 @ v1.31.0`).
    pub async fn start(jwks_uri: String, refresh_interval: Option<Duration>) -> Arc<Self> {
        let provider = Arc::new(Self {
            jwks_uri,
            client: Some(
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .expect("static reqwest client configuration is valid"),
            ),
            keys: ArcSwap::from_pointee(HashMap::new()),
        });
        if let Err(error) = provider.refresh().await {
            tracing::error!(%error, uri = %provider.jwks_uri, "initial JWKS retrieval failed");
        }
        if let Some(interval) = refresh_interval {
            let weak = Arc::downgrade(&provider);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                // The initial fetch above owns the immediate observation; discard
                // interval's eager first tick so refresh cadence starts after it.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Some(provider) = weak.upgrade() else {
                        break;
                    };
                    if let Err(error) = provider.refresh().await {
                        // A failed refresh retains the previous Arc and therefore
                        // cannot turn a transient IdP outage into a key wipe.
                        tracing::error!(%error, uri = %provider.jwks_uri, "JWKS refresh failed");
                    }
                }
            });
        }
        provider
    }

    /// Build a provider from a JWKS document without network I/O.
    ///
    /// This is the deterministic fixture seam used by authentication tests.
    pub fn from_jwks(jwks_uri: String, document: &[u8]) -> Result<Arc<Self>, AuthError> {
        Ok(Arc::new(Self {
            jwks_uri,
            // Static fixtures exercise parsing/verification without initializing
            // a platform TLS backend or permitting accidental network I/O.
            client: None,
            keys: ArcSwap::from_pointee(parse_jwks(document)?),
        }))
    }

    /// Fetch, validate, and atomically install one complete replacement set.
    pub async fn refresh(&self) -> Result<(), AuthError> {
        let response = self
            .client
            .as_ref()
            .ok_or_else(|| AuthError::new("static JWKS provider cannot refresh"))?
            .get(&self.jwks_uri)
            .send()
            .await
            .map_err(|error| AuthError::new(format!("retrieve JWKS: {error}")))?
            .error_for_status()
            .map_err(|error| AuthError::new(format!("retrieve JWKS: {error}")))?;
        let document = response
            .bytes()
            .await
            .map_err(|error| AuthError::new(format!("read JWKS: {error}")))?;
        self.install_document(&document)
    }

    fn install_document(&self, document: &[u8]) -> Result<(), AuthError> {
        // Parse the complete candidate before swapping. A partial or malformed
        // response therefore cannot erase the last known-good key set.
        let replacement = parse_jwks(document)?;
        self.keys.store(Arc::new(replacement));
        Ok(())
    }

    fn key(&self, kid: &str, algorithm: Algorithm) -> Result<DecodingKey, AuthError> {
        let keys = self.keys.load();
        let key = keys.get(kid).ok_or_else(|| match algorithm {
            Algorithm::RS256 => AuthError::new(format!("RSA key not found for key ID: {kid}")),
            Algorithm::ES256 => AuthError::new(format!("ECDSA key not found for key ID: {kid}")),
            _ => AuthError::new(format!("unexpected signing algorithm: {algorithm:?}")),
        })?;
        if key.algorithm != algorithm {
            return Err(AuthError::new(format!(
                "unexpected signing algorithm: {algorithm:?}"
            )));
        }
        Ok(key.key.clone())
    }
}

#[derive(Debug, Deserialize)]
struct RawJwks {
    keys: Vec<RawJwk>,
}

#[derive(Debug, Deserialize)]
struct RawJwk {
    kid: String,
    kty: String,
    #[serde(default)]
    alg: String,
    #[serde(default)]
    n: String,
    #[serde(default)]
    e: String,
    #[serde(default)]
    crv: String,
    #[serde(default)]
    x: String,
    #[serde(default)]
    y: String,
}

fn parse_jwks(document: &[u8]) -> Result<HashMap<String, VerificationKey>, AuthError> {
    let jwks: RawJwks = serde_json::from_slice(document)
        .map_err(|error| AuthError::new(format!("decode JWKS: {error}")))?;
    let mut keys = HashMap::new();
    for jwk in jwks.keys {
        let key = match (jwk.kty.as_str(), jwk.alg.as_str()) {
            ("RSA", "" | "RS256") => DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
                .map(|key| VerificationKey {
                    algorithm: Algorithm::RS256,
                    key,
                }),
            ("EC", "" | "ES256") if jwk.crv == "P-256" => {
                DecodingKey::from_ec_components(&jwk.x, &jwk.y).map(|key| VerificationKey {
                    algorithm: Algorithm::ES256,
                    key,
                })
            }
            _ => {
                tracing::warn!(kid = %jwk.kid, kty = %jwk.kty, alg = %jwk.alg, "ignoring unsupported JWKS key");
                continue;
            }
        }
        .map_err(|error| AuthError::new(format!("decode JWKS key {}: {error}", jwk.kid)))?;
        keys.insert(jwk.kid, key);
    }
    Ok(keys)
}

/// Exact-issuer JWT verification profile.
#[derive(Clone, Debug)]
pub struct JwtIssuerProfile {
    /// Stable operator label used in diagnostics.
    pub name: String,
    /// Exact, case-sensitive signed issuer value.
    pub issuer: String,
    /// Optional exact audience; blank skips the check.
    pub audience: String,
    /// Array claim containing `namespace:role` permissions.
    pub permissions_claim: String,
    /// Subject-based role augmentation.
    pub grants: GrantRules,
    /// Subject-based Worker-scope attenuation.
    pub worker_scopes: WorkerScopeRules,
    /// Isolated JWKS source for this profile.
    pub keys: Arc<JwksKeyProvider>,
}

impl JwtIssuerProfile {
    /// Normalize the upstream blank permissions-claim default.
    pub fn new(
        name: String,
        issuer: String,
        audience: String,
        permissions_claim: String,
        grants: GrantRules,
        keys: Arc<JwksKeyProvider>,
    ) -> Self {
        Self {
            name,
            issuer,
            audience,
            permissions_claim: if permissions_claim.trim().is_empty() {
                DEFAULT_PERMISSIONS_CLAIM.to_owned()
            } else {
                permissions_claim
            },
            grants,
            worker_scopes: WorkerScopeRules::default(),
            keys,
        }
    }

    /// Attach validated subject-to-Worker-scope mappings.
    pub fn with_worker_scopes(mut self, worker_scopes: WorkerScopeRules) -> Self {
        self.worker_scopes = worker_scopes;
        self
    }
}

/// Multi-issuer Temporal-compatible default JWT claim mapper.
#[derive(Clone, Debug)]
pub struct JwtAuthenticator {
    profiles: Vec<JwtIssuerProfile>,
}

impl JwtAuthenticator {
    /// Construct a mapper from independently validated issuer profiles.
    pub fn new(profiles: Vec<JwtIssuerProfile>) -> Self {
        Self { profiles }
    }

    fn route(&self, token: &str) -> Result<&JwtIssuerProfile, AuthError> {
        let issuer = unverified_issuer(token)?;
        self.profiles
            .iter()
            .find(|profile| profile.issuer == issuer)
            .ok_or_else(|| AuthError::new("JWT issuer is not configured"))
    }
}

#[async_trait]
impl ClaimMapper for JwtAuthenticator {
    async fn get_claims(&self, token: &str, _extra: &str) -> Result<Claims, AuthError> {
        let mut claims = Claims {
            auth_type: "jwt".to_owned(),
            ..Default::default()
        };
        if token.is_empty() {
            return Ok(claims);
        }
        let Some((scheme, encoded)) = token.split_once(' ') else {
            return Err(AuthError::new("unexpected authorization token format"));
        };
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(AuthError::new("unexpected name in authorization token"));
        }
        let profile = self.route(encoded)?;
        let header = decode_header(encoded)
            .map_err(|error| AuthError::new(format!("invalid JWT header: {error}")))?;
        let algorithm = match header.alg {
            Algorithm::RS256 | Algorithm::ES256 => header.alg,
            other => {
                return Err(AuthError::new(format!(
                    "unexpected signing method for algorithm: {other:?}"
                )));
            }
        };
        let kid = header
            .kid
            .as_deref()
            .ok_or_else(|| AuthError::new("malformed token - no \"kid\" header"))?;
        let key = profile.keys.key(kid, algorithm)?;
        let mut validation = Validation::new(algorithm);
        // Go's MapClaims treats exp/nbf as optional but validates them when present.
        validation.required_spec_claims.clear();
        validation.validate_nbf = true;
        validation.set_issuer(&[profile.issuer.as_str()]);
        validation.validate_aud = false;
        let verified = decode::<JsonValue>(encoded, &key, &validation)
            .map_err(|error| AuthError::new(format!("JWT verification failed: {error}")))?
            .claims;
        if !profile.audience.trim().is_empty() && !audience_matches(&verified, &profile.audience) {
            return Err(AuthError::new("audience mismatch"));
        }
        let subject = verified
            .get("sub")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| AuthError::new("unexpected value type of \"sub\" claim"))?;
        claims.subject = subject.to_owned();
        let signed_scope = verified
            .get(TOKEIRA_WORKER_SCOPE_CLAIM)
            .map(parse_worker_scope_claim)
            .transpose()?;
        let configured_scope = profile
            .worker_scopes
            .resolve(subject)
            .map_err(|_| AuthError::new("conflicting configured Worker scopes"))?;
        claims.worker_scope = resolve_effective_scope(signed_scope, configured_scope)
            .map_err(|_| AuthError::new("signed and configured Worker scopes conflict"))?;
        if let Some(permissions) = verified
            .get(&profile.permissions_claim)
            .and_then(JsonValue::as_array)
        {
            apply_permissions(permissions, &mut claims);
        }
        profile.grants.apply(subject, &mut claims);
        Ok(claims)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerScopeClaimV1 {
    version: u32,
    namespace: String,
    task_queues: Vec<String>,
    deployment_name: String,
    build_id: String,
}

fn parse_worker_scope_claim(value: &JsonValue) -> Result<WorkerScope, AuthError> {
    let claim = serde_json::from_value::<WorkerScopeClaimV1>(value.clone())
        .map_err(|_| AuthError::new("invalid tokeira_worker_scope claim"))?;
    if claim.version != WORKER_SCOPE_VERSION {
        return Err(AuthError::new(
            "unsupported tokeira_worker_scope claim version",
        ));
    }
    WorkerScope::try_new(
        claim.namespace,
        claim.task_queues,
        claim.deployment_name,
        claim.build_id,
    )
    .map_err(|_| AuthError::new("invalid tokeira_worker_scope resources"))
}

fn unverified_issuer(token: &str) -> Result<String, AuthError> {
    let mut parts = token.split('.');
    let _header = parts.next();
    let payload = parts
        .next()
        .ok_or_else(|| AuthError::new("invalid JWT compact form"))?;
    if parts.next().is_none() || parts.next().is_some() {
        return Err(AuthError::new("invalid JWT compact form"));
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|error| AuthError::new(format!("invalid JWT payload: {error}")))?;
    let payload: JsonValue = serde_json::from_slice(&payload)
        .map_err(|error| AuthError::new(format!("invalid JWT payload: {error}")))?;
    payload
        .get("iss")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .ok_or_else(|| AuthError::new("JWT issuer is missing"))
}

fn audience_matches(claims: &JsonValue, expected: &str) -> bool {
    match claims.get("aud") {
        Some(JsonValue::String(value)) => value == expected,
        Some(JsonValue::Array(values)) => values
            .iter()
            .any(|value| value.as_str().is_some_and(|value| value == expected)),
        _ => false,
    }
}

fn apply_permissions(permissions: &[JsonValue], claims: &mut Claims) {
    for permission in permissions {
        let Some(permission) = permission.as_str() else {
            tracing::warn!(?permission, "ignoring JWT permission that is not a string");
            continue;
        };
        let Some((namespace, role)) = permission.split_once(':') else {
            tracing::warn!(permission, "ignoring JWT permission with unexpected format");
            continue;
        };
        let role = match role.to_ascii_lowercase().as_str() {
            "worker" => Role::WORKER,
            "read" => Role::READER,
            "write" => Role::WRITER,
            "admin" => Role::ADMIN,
            _ => Role::UNDEFINED,
        };
        if namespace == "temporal-system" {
            claims.system |= role;
        } else {
            *claims
                .namespaces
                .entry(namespace.to_owned())
                .or_insert(Role::UNDEFINED) |= role;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::OnceLock};

    use jsonwebtoken::{EncodingKey, Header, encode};
    use proptest::prelude::*;
    use ring::{
        rand::SystemRandom,
        signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
    };
    use time::OffsetDateTime;

    use super::*;
    use crate::{
        Access, Authorizer, AuthzDecision, CallClassification, CallTarget, DefaultAuthorizer,
        GrantRule, Scope,
    };

    fn empty_provider() -> Arc<JwksKeyProvider> {
        JwksKeyProvider::from_jwks("https://issuer.example/keys".to_owned(), br#"{"keys":[]}"#)
            .expect("empty JWKS")
    }

    fn unsigned_token(issuer: &str) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"RS256","kid":"key"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::json!({"iss": issuer}).to_string());
        format!("{header}.{payload}.signature")
    }

    /// Process-wide ES256 fixture keypair, generated fresh on every test run
    /// so no private-key bytes live in the source tree (a committed key,
    /// however inert, trips secret scanners forever).
    ///
    /// One shared keypair rather than one per call: `signed_token*` (the
    /// signing side) and `signed_profile` (the JWKS verification side) must
    /// agree on the key for cross-helper token flows to verify.
    struct EcKeyMaterial {
        /// PKCS#8 v1 DER document holding the P-256 signing key.
        pkcs8: Vec<u8>,
        /// Base64url (unpadded) JWK coordinates of the public point.
        x: String,
        y: String,
    }

    fn ec_key() -> &'static EcKeyMaterial {
        static KEY: OnceLock<EcKeyMaterial> = OnceLock::new();
        KEY.get_or_init(|| {
            let rng = SystemRandom::new();
            let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
                .expect("generate P-256 keypair");
            let pair =
                EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
                    .expect("round-trip freshly generated keypair");
            // Uncompressed SEC1 point: an 0x04 tag, then X and Y as 32-byte
            // big-endian coordinates — exactly the JWK `x`/`y` payload.
            let point = pair.public_key().as_ref();
            let x = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&point[1..33]);
            let y = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&point[33..65]);
            EcKeyMaterial {
                pkcs8: pkcs8.as_ref().to_vec(),
                x,
                y,
            }
        })
    }

    fn signed_token(claims: JsonValue) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("ec01".to_owned());
        signed_token_with_header(header, claims)
    }

    fn signed_token_with_header(header: Header, claims: JsonValue) -> String {
        encode(&header, &claims, &EncodingKey::from_ec_der(&ec_key().pkcs8)).expect("signed JWT")
    }

    fn signed_profile(audience: &str) -> JwtIssuerProfile {
        let key = ec_key();
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "EC", "crv": "P-256", "alg": "ES256", "kid": "ec01",
                "x": key.x.as_str(),
                "y": key.y.as_str()
            }]
        });
        let keys = JwksKeyProvider::from_jwks(
            "https://issuer.example/keys".to_owned(),
            jwks.to_string().as_bytes(),
        )
        .expect("JWKS");
        JwtIssuerProfile::new(
            "fixture".to_owned(),
            "https://issuer.example".to_owned(),
            audience.to_owned(),
            String::new(),
            GrantRules::new(vec![
                crate::GrantRule::new("worker-*", ["prod:worker"]).expect("validated grant"),
            ]),
            keys,
        )
    }

    // Feature: scoped-worker-authorization, Property 3: Fixed JWT claim parsing is fail-closed
    proptest! {
        #[test]
        fn property_fixed_worker_scope_claim_parsing_is_fail_closed(
            namespace in "[a-z]{1,12}",
            queue in "[a-z]{1,12}",
            deployment in "[a-z]{1,12}",
            build_id in "[a-z0-9]{1,12}",
            mutation in 0u8..7,
        ) {
            let mut claim = serde_json::json!({
                "version": 1,
                "namespace": namespace,
                "task_queues": [queue],
                "deployment_name": deployment,
                "build_id": build_id,
            });
            match mutation {
                0 => {}
                1 => {
                    claim.as_object_mut().expect("object").remove("version");
                }
                2 => claim["version"] = serde_json::json!(2),
                3 => claim["unknown"] = serde_json::json!(true),
                4 => claim["task_queues"] = serde_json::json!("queue"),
                5 => {
                    let queue = claim["task_queues"][0].clone();
                    claim["task_queues"] = serde_json::json!([queue.clone(), queue]);
                }
                _ => claim["deployment_name"] = serde_json::json!("*"),
            }
            prop_assert_eq!(parse_worker_scope_claim(&claim).is_ok(), mutation == 0);
        }
    }

    #[tokio::test]
    async fn header_format_errors_match_v1_31() {
        let mapper = JwtAuthenticator::new(Vec::new());
        assert_eq!(
            mapper
                .get_claims("not-a-token", "")
                .await
                .expect_err("format")
                .to_string(),
            "unexpected authorization token format"
        );
        assert_eq!(
            mapper
                .get_claims("Basic token", "")
                .await
                .expect_err("scheme")
                .to_string(),
            "unexpected name in authorization token"
        );
    }

    #[tokio::test]
    async fn signed_and_subject_mapped_worker_scopes_must_agree() {
        let scope = WorkerScope::try_new(
            "payments".to_owned(),
            vec!["payments-worker".to_owned()],
            "payments".to_owned(),
            "build-a".to_owned(),
        )
        .expect("scope");
        let profile = signed_profile("").with_worker_scopes(WorkerScopeRules::new(vec![
            crate::WorkerScopeRule::new("worker-*", scope.clone()).expect("scope rule"),
        ]));
        let mapper = JwtAuthenticator::new(vec![profile]);
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let token = signed_token(serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "worker-a",
            "exp": now + 300,
            "tokeira_worker_scope": {
                "version": 1,
                "namespace": "payments",
                "task_queues": ["payments-worker"],
                "deployment_name": "payments",
                "build_id": "build-a"
            }
        }));
        let claims = mapper
            .get_claims(&format!("Bearer {token}"), "")
            .await
            .expect("equal scopes");
        assert_eq!(claims.worker_scope, Some(scope));

        let conflicting = signed_token(serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "worker-a",
            "exp": now + 300,
            "permissions": ["payments:admin"],
            "tokeira_worker_scope": {
                "version": 1,
                "namespace": "payments",
                "task_queues": ["other-worker"],
                "deployment_name": "payments",
                "build_id": "build-a"
            }
        }));
        assert!(
            mapper
                .get_claims(&format!("Bearer {conflicting}"), "")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn jwt_admission_diagnostics_match_v1_31() {
        let mapper = JwtAuthenticator::new(vec![signed_profile("")]);
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let missing_issuer_payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::json!({"sub": "worker"}).to_string());
        let token = format!("header.{missing_issuer_payload}.signature");
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {token}"), "")
                .await
                .expect_err("missing issuer")
                .to_string(),
            "JWT issuer is missing"
        );

        let token = unsigned_token("https://foreign.example");
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {token}"), "")
                .await
                .expect_err("unconfigured issuer")
                .to_string(),
            "JWT issuer is not configured"
        );

        let no_kid = signed_token_with_header(
            Header::new(Algorithm::ES256),
            serde_json::json!({
                "iss": "https://issuer.example",
                "sub": "worker",
                "exp": now + 600,
            }),
        );
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {no_kid}"), "")
                .await
                .expect_err("missing kid")
                .to_string(),
            "malformed token - no \"kid\" header"
        );

        let mut unknown_header = Header::new(Algorithm::ES256);
        unknown_header.kid = Some("unknown".to_owned());
        let unknown_kid = signed_token_with_header(
            unknown_header,
            serde_json::json!({
                "iss": "https://issuer.example",
                "sub": "worker",
                "exp": now + 600,
            }),
        );
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {unknown_kid}"), "")
                .await
                .expect_err("unknown kid")
                .to_string(),
            "ECDSA key not found for key ID: unknown"
        );
    }

    #[test]
    fn permissions_are_split_once_and_union_roles() {
        let permissions = serde_json::json!([
            "temporal-system:read",
            "prod:worker",
            "prod:write",
            "prod:unknown",
            "malformed",
            42
        ]);
        let mut claims = Claims::default();
        apply_permissions(permissions.as_array().expect("array"), &mut claims);
        assert_eq!(claims.system, Role::READER);
        assert_eq!(
            claims.namespaces.get("prod"),
            Some(&(Role::WORKER | Role::WRITER))
        );
    }

    fn reference_permission_roles(permissions: &[JsonValue]) -> Claims {
        let mut claims = Claims::default();
        for permission in permissions {
            let Some(permission) = permission.as_str() else {
                continue;
            };
            let mut components = permission.splitn(2, ':');
            let Some(namespace) = components.next() else {
                continue;
            };
            let Some(role) = components.next() else {
                continue;
            };
            let role = match role.to_ascii_lowercase().as_str() {
                "worker" => Role::WORKER,
                "read" => Role::READER,
                "write" => Role::WRITER,
                "admin" => Role::ADMIN,
                _ => Role::UNDEFINED,
            };
            if namespace == "temporal-system" {
                claims.system |= role;
            } else {
                *claims
                    .namespaces
                    .entry(namespace.to_owned())
                    .or_insert(Role::UNDEFINED) |= role;
            }
        }
        claims
    }

    fn role_name(index: u8) -> &'static str {
        match index {
            0 => "worker",
            1 => "read",
            2 => "write",
            _ => "admin",
        }
    }

    // Feature: authorization-foundation, Property 2: the permissive JWT
    // grammar and strict prevalidated grants union exactly like v1.31.0.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_permissions_grammar_and_grants_union(
            raw_permissions in proptest::collection::vec(
                prop_oneof![
                    "[A-Za-z0-9_:-]{0,24}".prop_map(JsonValue::String),
                    any::<i16>().prop_map(|value| JsonValue::from(i64::from(value))),
                    any::<bool>().prop_map(JsonValue::Bool),
                ],
                0..40,
            ),
            grants in proptest::collection::vec(
                ("[A-Za-z][A-Za-z0-9_-]{0,7}", 0u8..4),
                0..16,
            ),
        ) {
            let mut actual = Claims::default();
            apply_permissions(&raw_permissions, &mut actual);

            let rules = GrantRules::new(
                grants
                    .iter()
                    .map(|(namespace, role)| {
                        GrantRule::new(
                            "*",
                            [format!("{namespace}:{}", role_name(*role))],
                        )
                        .expect("generated grant is valid")
                    })
                    .collect(),
            );
            rules.apply("arbitrary\nsubject", &mut actual);

            let mut expected = reference_permission_roles(&raw_permissions);
            let mut expected_namespace_roles = HashMap::<String, Role>::new();
            for (namespace, role) in grants {
                *expected_namespace_roles
                    .entry(namespace)
                    .or_insert(Role::UNDEFINED) |= match role {
                        0 => Role::WORKER,
                        1 => Role::READER,
                        2 => Role::WRITER,
                        _ => Role::ADMIN,
                    };
            }
            for (namespace, role) in expected_namespace_roles {
                *expected
                    .namespaces
                    .entry(namespace)
                    .or_insert(Role::UNDEFINED) |= role;
            }

            prop_assert_eq!(actual, expected);
        }
    }

    #[test]
    fn jwks_accepts_only_rs256_and_es256_key_shapes() {
        let provider = JwksKeyProvider::from_jwks(
            "https://issuer.example/keys".to_owned(),
            br#"{
                "keys": [
                    {"kid":"rsa","kty":"RSA","alg":"RS256","n":"AQAB","e":"AQAB"},
                    {"kid":"hmac","kty":"oct","alg":"HS256","k":"secret"}
                ]
            }"#,
        )
        .expect("JWKS");
        assert_eq!(provider.keys.load().len(), 1);
        assert!(provider.keys.load().contains_key("rsa"));
    }

    #[test]
    fn jwks_rotation_is_atomic_and_profile_isolated() {
        let first = JwksKeyProvider::from_jwks(
            "https://first.example/keys".to_owned(),
            br#"{"keys":[{"kid":"old","kty":"RSA","alg":"RS256","n":"AQAB","e":"AQAB"}]}"#,
        )
        .expect("first");
        let second = JwksKeyProvider::from_jwks(
            "https://second.example/keys".to_owned(),
            br#"{"keys":[{"kid":"other","kty":"RSA","alg":"RS256","n":"AQAB","e":"AQAB"}]}"#,
        )
        .expect("second");

        assert!(first.install_document(b"not-json").is_err());
        assert!(first.keys.load().contains_key("old"));
        assert!(second.keys.load().contains_key("other"));

        first
            .install_document(
                br#"{"keys":[{"kid":"new","kty":"RSA","alg":"RS256","n":"AQAB","e":"AQAB"}]}"#,
            )
            .expect("replacement");
        assert!(!first.keys.load().contains_key("old"));
        assert!(first.keys.load().contains_key("new"));
        assert!(second.keys.load().contains_key("other"));
    }

    #[tokio::test]
    async fn initial_jwks_failure_is_nonfatal_and_fail_closed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        drop(listener);

        let provider = JwksKeyProvider::start(format!("http://{address}/keys"), None).await;

        assert!(provider.keys.load().is_empty());
        let Err(error) = provider.key("missing", Algorithm::RS256) else {
            panic!("empty provider must fail closed");
        };
        assert_eq!(error.to_string(), "RSA key not found for key ID: missing");
    }

    #[tokio::test]
    async fn signed_jwt_maps_permissions_and_matching_grants() {
        let mapper = JwtAuthenticator::new(vec![signed_profile("tokeira")]);
        let token = signed_token(serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "worker-prod",
            "aud": ["other", "tokeira"],
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 600,
            "permissions": ["prod:write", "temporal-system:read"]
        }));
        let claims = mapper
            .get_claims(&format!("Bearer {token}"), "ignored")
            .await
            .expect("verified claims");
        assert_eq!(claims.subject, "worker-prod");
        assert_eq!(claims.auth_type, "jwt");
        assert_eq!(claims.system, Role::READER);
        assert_eq!(
            claims.namespaces.get("prod"),
            Some(&(Role::WORKER | Role::WRITER))
        );

        let write_target = CallTarget {
            api_name: "/test.Service/Write",
            namespace: Some("prod"),
            classification: CallClassification {
                scope: Scope::Namespace,
                access: Access::Write,
            },
            worker: None,
        };
        assert!(matches!(
            DefaultAuthorizer
                .authorize(Some(&claims), &write_target)
                .expect("decision"),
            AuthzDecision::Allow { .. }
        ));
        let admin_target = CallTarget {
            classification: CallClassification {
                access: Access::Admin,
                ..write_target.classification
            },
            ..write_target
        };
        assert!(matches!(
            DefaultAuthorizer
                .authorize(Some(&claims), &admin_target)
                .expect("decision"),
            AuthzDecision::Deny { .. }
        ));
    }

    #[tokio::test]
    async fn signed_multi_issuer_routing_is_profile_isolated() {
        let keys = signed_profile("").keys;
        let mapper = JwtAuthenticator::new(vec![
            JwtIssuerProfile::new(
                "issuer-a".to_owned(),
                "https://issuer-a.example".to_owned(),
                "audience-a".to_owned(),
                String::new(),
                GrantRules::default(),
                keys.clone(),
            ),
            JwtIssuerProfile::new(
                "issuer-b".to_owned(),
                "https://issuer-b.example".to_owned(),
                "audience-b".to_owned(),
                String::new(),
                GrantRules::default(),
                keys,
            ),
        ]);
        for (issuer, audience) in [
            ("https://issuer-a.example", "audience-a"),
            ("https://issuer-b.example", "audience-b"),
        ] {
            let token = signed_token(serde_json::json!({
                "iss": issuer,
                "sub": "subject",
                "aud": audience,
                "exp": OffsetDateTime::now_utc().unix_timestamp() + 600,
            }));
            mapper
                .get_claims(&format!("Bearer {token}"), "")
                .await
                .expect("matching profile");
        }

        let cross_profile = signed_token(serde_json::json!({
            "iss": "https://issuer-a.example",
            "sub": "subject",
            "aud": "audience-b",
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 600,
        }));
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {cross_profile}"), "")
                .await
                .expect_err("issuer-a must not borrow issuer-b policy")
                .to_string(),
            "audience mismatch"
        );
    }

    #[tokio::test]
    async fn eks_shaped_subject_maps_grants_without_permissions_claim() {
        let keys = signed_profile("").keys;
        let mapper = JwtAuthenticator::new(vec![JwtIssuerProfile::new(
            "eks".to_owned(),
            "https://issuer.example".to_owned(),
            String::new(),
            String::new(),
            GrantRules::new(vec![
                GrantRule::new(
                    "system:serviceaccount:prod:*",
                    ["prod:worker", "prod:write"],
                )
                .expect("grant"),
            ]),
            keys,
        )]);
        let token = signed_token(serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "system:serviceaccount:prod:worker",
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 600,
        }));

        let claims = mapper
            .get_claims(&format!("Bearer {token}"), "")
            .await
            .expect("grant-mapped token");

        assert_eq!(
            claims.namespaces.get("prod"),
            Some(&(Role::WORKER | Role::WRITER))
        );
    }

    #[tokio::test]
    async fn signed_jwt_audience_and_subject_errors_are_exact() {
        let mapper = JwtAuthenticator::new(vec![signed_profile("tokeira")]);
        let wrong_audience = signed_token(serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "worker",
            "aud": "other",
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 600
        }));
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {wrong_audience}"), "")
                .await
                .expect_err("audience")
                .to_string(),
            "audience mismatch"
        );
        let invalid_subject = signed_token(serde_json::json!({
            "iss": "https://issuer.example",
            "sub": 42,
            "aud": "tokeira",
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 600
        }));
        assert_eq!(
            mapper
                .get_claims(&format!("Bearer {invalid_subject}"), "")
                .await
                .expect_err("subject")
                .to_string(),
            "unexpected value type of \"sub\" claim"
        );
    }

    // Feature: authorization-foundation, Property 8: an arbitrary token issuer
    // routes to exactly one profile iff the signed issuer matches exactly.
    proptest! {
        #[test]
        fn property_issuer_routing_is_exact(
            issuers in proptest::collection::btree_set("https://issuer[0-9]{1,3}\\.example", 1..8),
            candidate in "https://issuer[0-9]{1,3}\\.example/?",
        ) {
            let provider = empty_provider();
            let profiles = issuers
                .iter()
                .map(|issuer| JwtIssuerProfile::new(
                    issuer.clone(),
                    issuer.clone(),
                    String::new(),
                    String::new(),
                    GrantRules::default(),
                    provider.clone(),
                ))
                .collect();
            let mapper = JwtAuthenticator::new(profiles);
            let token = unsigned_token(&candidate);
            prop_assert_eq!(mapper.route(&token).is_ok(), issuers.contains(&candidate));
        }
    }
}
