//! Transport-independent identity and authorization primitives.
//!
//! This crate owns Temporal-compatible claims, roles, call classification, grant
//! matching, and authorization decisions. It deliberately has no HTTP, gRPC,
//! runtime, storage, or kernel dependency: callers adapt transport credentials
//! into [`ClaimMapper`] and pass resolved API intent to [`Authorizer`].

use std::{
    collections::HashMap,
    ops::{BitOr, BitOrAssign},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod jwt;
mod sts;

pub use jwt::{JwksKeyProvider, JwtAuthenticator, JwtIssuerProfile};
pub use sts::{
    StsAuthenticator, ValidatedStsRequest, is_aws_bearer, strip_aws_bearer_prefix,
    validate_presigned_sts_url,
};

/// Presence-enabled claim mapper combining configured JWT and AWS IAM sources.
#[derive(Debug)]
pub struct MultiSourceClaimMapper {
    jwt: Option<JwtAuthenticator>,
    sts: Option<StsAuthenticator>,
}

impl MultiSourceClaimMapper {
    /// Construct the mapper from the configured identity-source subset.
    pub fn new(jwt: Option<JwtAuthenticator>, sts: Option<StsAuthenticator>) -> Self {
        Self { jwt, sts }
    }
}

#[async_trait]
impl ClaimMapper for MultiSourceClaimMapper {
    async fn get_claims(&self, token: &str, extra: &str) -> Result<Claims, AuthError> {
        if token.is_empty() {
            return match &self.jwt {
                Some(jwt) => jwt.get_claims(token, extra).await,
                None => Ok(Claims::default()),
            };
        }
        let Some((scheme, encoded)) = token.split_once(' ') else {
            return Err(AuthError::new("unexpected authorization token format"));
        };
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(AuthError::new("unexpected name in authorization token"));
        }
        if let (Some(sts), Some(encoded_url)) = (&self.sts, strip_aws_bearer_prefix(encoded)) {
            return sts.authenticate_encoded(encoded_url).await;
        }
        self.jwt
            .as_ref()
            .ok_or_else(|| AuthError::new("JWT identity source is not configured"))?
            .get_claims(token, extra)
            .await
    }
}

/// Temporal role bitmask (`common/authorization/roles.go @ v1.31.0`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Role(pub i16);

impl Role {
    /// No granted role bits.
    pub const UNDEFINED: Self = Self(0);
    /// Task-worker permission bit.
    pub const WORKER: Self = Self(1);
    /// Read-only permission bit.
    pub const READER: Self = Self(2);
    /// Write permission bit.
    pub const WRITER: Self = Self(4);
    /// Administrative permission bit.
    pub const ADMIN: Self = Self(8);

    const ALL: i16 = Self::WORKER.0 | Self::READER.0 | Self::WRITER.0 | Self::ADMIN.0;

    /// Whether the bitmask contains only defined Temporal role bits.
    pub const fn is_valid(self) -> bool {
        self.0 & !Self::ALL == 0
    }

    /// Apply Temporal v1.31.0's numeric role comparison.
    ///
    /// This intentionally preserves the upstream quirk that `WORKER` alone
    /// satisfies none of Reader, Writer, or Admin.
    pub const fn satisfies(self, required: Self) -> bool {
        self.0 >= required.0
    }
}

impl BitOr for Role {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for Role {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Authenticated identity and its cluster/namespace roles.
///
/// The unused Go `Extensions` bucket is omitted because Tokeira has no consumer
/// for it (`common/authorization/roles.go @ v1.31.0`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// Identity-provider subject.
    pub subject: String,
    /// Cluster-wide role bits.
    pub system: Role,
    /// Per-namespace role bits keyed by the case-sensitive namespace name.
    pub namespaces: HashMap<String, Role>,
    /// Authentication method, such as `jwt` or `aws-iam`.
    pub auth_type: String,
}

/// Server-computed identity eligible for durable history attribution.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthPrincipal {
    /// Authentication method that established the identity.
    pub principal_type: String,
    /// Authenticated subject name.
    pub name: String,
}

impl AuthPrincipal {
    /// Whether both principal fields are empty.
    pub fn is_empty(&self) -> bool {
        self.principal_type.is_empty() && self.name.is_empty()
    }
}

/// Authentication failure retained for logging while the edge exposes only the
/// generic v1.31.0 permission-denied response.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct AuthError {
    message: String,
}

impl AuthError {
    /// Construct an authentication failure with its internal diagnostic.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Convert raw authorization header values into claims.
#[async_trait]
pub trait ClaimMapper: Send + Sync {
    /// Authenticate one raw authorization value plus optional extras data.
    async fn get_claims(&self, token: &str, extra: &str) -> Result<Claims, AuthError>;
}

/// Upstream authorization scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// A namespace name participates in the decision.
    Namespace,
    /// Only cluster-wide roles participate.
    Cluster,
    /// Unknown scopes fail closed.
    Unknown,
}

/// Upstream authorization access level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// Read-only access requires Reader.
    ReadOnly,
    /// Mutating access requires Writer.
    Write,
    /// Administrative access requires Admin.
    Admin,
    /// Unknown access conservatively requires Admin.
    Unknown,
}

/// Temporal method metadata reduced to authorization-relevant fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallClassification {
    /// Cluster, namespace, or unknown scope.
    pub scope: Scope,
    /// Read-only, write, admin, or unknown access.
    pub access: Access,
}

/// Authorization target constructed before namespace existence is resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallTarget<'a> {
    /// Exact full public method name.
    pub api_name: &'a str,
    /// Unresolved namespace name, absent for cluster-scoped calls.
    pub namespace: Option<&'a str>,
    /// Ground-truthed method classification.
    pub classification: CallClassification,
}

/// Successful authorizer decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthzDecision {
    /// Permit the call, optionally computing a principal for attribution.
    Allow { principal: Option<AuthPrincipal> },
    /// Reject the call, optionally retaining a public denial reason.
    Deny { reason: Option<String> },
}

/// Authorizer implementation failure, distinct from an intentional denial.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct AuthzError {
    message: String,
}

impl AuthzError {
    /// Construct an authorizer failure with its internal/public diagnostic.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Decide whether authenticated claims may invoke a target method.
pub trait Authorizer: Send + Sync {
    /// Return an intentional decision or an authorizer implementation failure.
    fn authorize(
        &self,
        claims: Option<&Claims>,
        target: &CallTarget<'_>,
    ) -> Result<AuthzDecision, AuthzError>;
}

/// Temporal v1.31.0's built-in role authorizer.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultAuthorizer;

impl Authorizer for DefaultAuthorizer {
    fn authorize(
        &self,
        claims: Option<&Claims>,
        target: &CallTarget<'_>,
    ) -> Result<AuthzDecision, AuthzError> {
        // Upstream permits these two exact APIs before checking claims
        // (`common/authorization/default_authorizer.go:37-43 @ v1.31.0`).
        if matches!(
            target.api_name,
            "/grpc.health.v1.Health/Check"
                | "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
        ) {
            return Ok(AuthzDecision::Allow { principal: None });
        }
        let Some(claims) = claims else {
            return Ok(AuthzDecision::Deny { reason: None });
        };
        let role = match target.classification.scope {
            Scope::Cluster => claims.system,
            Scope::Namespace => {
                claims.system
                    | target
                        .namespace
                        .and_then(|namespace| claims.namespaces.get(namespace).copied())
                        .unwrap_or(Role::UNDEFINED)
            }
            Scope::Unknown => return Ok(AuthzDecision::Deny { reason: None }),
        };
        let required = match target.classification.access {
            Access::ReadOnly => Role::READER,
            Access::Write => Role::WRITER,
            Access::Admin | Access::Unknown => Role::ADMIN,
        };
        if !role.satisfies(required) {
            return Ok(AuthzDecision::Deny { reason: None });
        }
        Ok(AuthzDecision::Allow {
            principal: Some(AuthPrincipal {
                principal_type: claims.auth_type.clone(),
                name: claims.subject.clone(),
            }),
        })
    }
}

/// One parsed `namespace:role` grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    /// Namespace name or the exact `temporal-system` pseudo-namespace.
    pub namespace: String,
    /// Parsed role bit.
    pub role: Role,
}

/// Strict operator-authored grant parsing error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum GrantParseError {
    /// The value did not contain the `namespace:role` delimiter.
    #[error("grant `{0}` must use namespace:role syntax")]
    MissingDelimiter(String),
    /// The role name was not one of read, write, admin, or worker.
    #[error("grant `{grant}` has unknown role `{role}`")]
    UnknownRole {
        /// Complete rejected grant.
        grant: String,
        /// Rejected role component.
        role: String,
    },
}

/// Parse one operator-authored grant using Temporal's case rules.
pub fn parse_grant(value: &str) -> Result<Grant, GrantParseError> {
    let Some((namespace, role)) = value.split_once(':') else {
        return Err(GrantParseError::MissingDelimiter(value.to_owned()));
    };
    let parsed = match role.to_ascii_lowercase().as_str() {
        "worker" => Role::WORKER,
        "read" => Role::READER,
        "write" => Role::WRITER,
        "admin" => Role::ADMIN,
        _ => {
            return Err(GrantParseError::UnknownRole {
                grant: value.to_owned(),
                role: role.to_owned(),
            });
        }
    };
    Ok(Grant {
        namespace: namespace.to_owned(),
        role: parsed,
    })
}

/// Full-string, case-sensitive glob with `*` as its only metacharacter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobPattern {
    pattern: String,
}

/// Invalid grants-pattern error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PatternError {
    /// Empty patterns would accidentally match no intentional identity.
    #[error("grant match pattern must not be empty")]
    Empty,
}

impl GlobPattern {
    /// Validate and compile a grants pattern.
    pub fn new(pattern: impl Into<String>) -> Result<Self, PatternError> {
        let pattern = pattern.into();
        if pattern.is_empty() {
            return Err(PatternError::Empty);
        }
        Ok(Self { pattern })
    }

    /// Return the original configured pattern.
    pub fn as_str(&self) -> &str {
        &self.pattern
    }

    /// Match the complete identity; `*` crosses separators and newlines.
    pub fn is_match(&self, candidate: &str) -> bool {
        let pattern = self.pattern.chars().collect::<Vec<_>>();
        let candidate = candidate.chars().collect::<Vec<_>>();
        let mut previous = vec![false; candidate.len() + 1];
        previous[0] = true;
        for token in pattern {
            let mut current = vec![false; candidate.len() + 1];
            if token == '*' {
                current[0] = previous[0];
                for index in 1..=candidate.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            } else {
                for index in 1..=candidate.len() {
                    current[index] = previous[index - 1] && candidate[index - 1] == token;
                }
            }
            previous = current;
        }
        previous[candidate.len()]
    }
}

/// One subject/ARN grants rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantRule {
    pattern: GlobPattern,
    grants: Vec<Grant>,
}

impl GrantRule {
    /// Construct one validated rule from operator-authored strings.
    pub fn new(
        pattern: impl Into<String>,
        grants: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, GrantRuleError> {
        let pattern = GlobPattern::new(pattern).map_err(GrantRuleError::Pattern)?;
        let grants = grants
            .into_iter()
            .map(|grant| parse_grant(grant.as_ref()).map_err(GrantRuleError::Grant))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { pattern, grants })
    }
}

/// Invalid grants-rule construction.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum GrantRuleError {
    /// Invalid identity pattern.
    #[error(transparent)]
    Pattern(PatternError),
    /// Invalid role grant.
    #[error(transparent)]
    Grant(GrantParseError),
}

/// Validated grant rules shared by JWT subjects and AWS IAM ARNs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GrantRules {
    rules: Vec<GrantRule>,
}

impl GrantRules {
    /// Construct a ruleset from validated rules.
    pub fn new(rules: Vec<GrantRule>) -> Self {
        Self { rules }
    }

    /// OR every matching rule into the supplied claims.
    pub fn apply(&self, identity: &str, claims: &mut Claims) {
        for rule in &self.rules {
            if !rule.pattern.is_match(identity) {
                continue;
            }
            for grant in &rule.grants {
                if grant.namespace == "temporal-system" {
                    claims.system |= grant.role;
                } else {
                    *claims
                        .namespaces
                        .entry(grant.namespace.clone())
                        .or_insert(Role::UNDEFINED) |= grant.role;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use regex::Regex;

    use super::*;

    #[test]
    fn worker_role_does_not_satisfy_reader_write_or_admin() {
        assert!(!Role::WORKER.satisfies(Role::READER));
        assert!(!Role::WORKER.satisfies(Role::WRITER));
        assert!(!Role::WORKER.satisfies(Role::ADMIN));
        assert!((Role::WORKER | Role::READER).satisfies(Role::READER));
    }

    #[test]
    fn grants_union_system_and_namespace_roles() {
        let rules = GrantRules::new(vec![
            GrantRule::new(
                "system:serviceaccount:prod:*",
                ["prod:worker", "prod:write"],
            )
            .expect("rule"),
            GrantRule::new("system:*", ["temporal-system:read"]).expect("rule"),
        ]);
        let mut claims = Claims::default();
        rules.apply("system:serviceaccount:prod:worker", &mut claims);
        assert_eq!(claims.system, Role::READER);
        assert_eq!(
            claims.namespaces.get("prod"),
            Some(&(Role::WORKER | Role::WRITER))
        );
    }

    // Feature: authorization-foundation, grants matcher property: the native
    // matcher is identical to an anchored regex where only `*` expands.
    proptest! {
        #[test]
        fn property_glob_matches_anchored_regex_reference(
            pattern in "[a-zA-Z0-9_:/?\\.\\-*\\n]{1,32}",
            candidate in "[a-zA-Z0-9_:/?\\.\\-\\n]{0,40}",
        ) {
            let native = GlobPattern::new(pattern.clone()).expect("non-empty pattern");
            let reference = format!(
                "^(?s:{})$",
                pattern
                    .split('*')
                    .map(regex::escape)
                    .collect::<Vec<_>>()
                    .join(".*")
            );
            let reference = Regex::new(&reference).expect("escaped reference regex");
            prop_assert_eq!(native.is_match(&candidate), reference.is_match(&candidate));
        }
    }

    // Feature: authorization-foundation, Property 3: the v1.31 role decision
    // table uses system-or-namespace bits and numeric comparison.
    proptest! {
        #[test]
        fn property_default_authorizer_decision_table(
            system in 0i16..16,
            namespace_role in 0i16..16,
            scope_namespace in any::<bool>(),
            access_index in 0u8..3,
        ) {
            let mut claims = Claims {
                subject: "subject".to_owned(),
                system: Role(system),
                auth_type: "jwt".to_owned(),
                ..Default::default()
            };
            claims.namespaces.insert("namespace".to_owned(), Role(namespace_role));
            let access = match access_index {
                0 => Access::ReadOnly,
                1 => Access::Write,
                _ => Access::Admin,
            };
            let target = CallTarget {
                api_name: "/test.Service/Method",
                namespace: scope_namespace.then_some("namespace"),
                classification: CallClassification {
                    scope: if scope_namespace { Scope::Namespace } else { Scope::Cluster },
                    access,
                },
            };
            let decision = DefaultAuthorizer.authorize(Some(&claims), &target).expect("decision");
            let effective = if scope_namespace { system | namespace_role } else { system };
            let required = match access {
                Access::ReadOnly => Role::READER.0,
                Access::Write => Role::WRITER.0,
                Access::Admin | Access::Unknown => Role::ADMIN.0,
            };
            prop_assert_eq!(
                matches!(decision, AuthzDecision::Allow { .. }),
                effective >= required
            );
        }
    }
}
