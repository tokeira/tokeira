//! Cluster-scoped offer credentials, independent of public Worker/Admin grants.
//!
//! Operators supply protected files; this module generates and validates the
//! bounded document and renews tokens locally. A peer proves cluster membership,
//! never execution ownership. No secret-bearing type exposes its contents in Debug.

use std::{fmt, sync::Mutex};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use p256::{
    SecretKey,
    elliptic_curve::{rand_core::OsRng, sec1::ToEncodedPoint},
    pkcs8::{DecodePrivateKey, EncodePrivateKey},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Maximum size of either the protected document or the public JWKS.
pub const MAX_DOCUMENT_BYTES: usize = 64 * 1024;
/// Includes the `Bearer ` framing in an authorization value.
pub const MAX_AUTHORIZATION_BYTES: usize = 8 * 1024;
/// The only subject granted permission to submit internal workflow offers.
pub const PEER_SUBJECT: &str = "tokeira-runtime-peer";
/// Shared repair instructions contain no rejected input or secret material.
pub const REPAIR: &str = "generate a credential file with `tokeirad peer-credential generate --out <path>`, stop every node, install the same file on every peer-capable node, and restart";

/// Bounded errors deliberately contain no parser input or token/key bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PeerError {
    /// Configuration or a protected source is definitively absent.
    #[error("peer credentials missing; {REPAIR}")]
    MissingMaterial,
    /// Bootstrap could not validate or use the configured protected material.
    #[error("peer credentials inconsistent; {REPAIR}")]
    InconsistentMaterial,
    /// Bootstrap material cannot be trusted against its public configuration.
    #[error("peer credentials inconsistent; {REPAIR}")]
    Material,
    /// The supplied envelope exceeds a fixed admission bound.
    #[error("peer credential size limit exceeded")]
    TooLarge,
    /// No acceptable signature or validity interval was supplied.
    #[error("invalid peer authentication")]
    Unauthenticated,
    /// The verified signer did not supply the separate peer identity grant.
    #[error("identity has no workflow-offer peer grant")]
    PermissionDenied,
    /// Local signing failed; callers must retain durable intent for recovery.
    #[error("peer token signing unavailable")]
    Signing,
}

/// Public trust embedded in the protected file; contains no signing material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerProfile {
    /// Random cluster identity, replaced only by explicitly generating a new file.
    pub cluster_id: Uuid,
    /// Exact issuer derived from the random cluster identity.
    pub issuer: String,
    /// Exact audience derived from the random cluster identity.
    pub audience: String,
    /// Exactly one allowed subject; no public role grants are consulted.
    pub subjects: Vec<String>,
    /// The sole allowed signing-key identifier.
    pub kid: String,
    /// SHA-256 of the uncompressed SEC1 public point, base64url encoded.
    pub fingerprint: String,
    /// Public, single-key ES256 JWKS document.
    pub jwks: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    profile: PeerProfile,
    private_pkcs8: String,
}

/// Protected JSON document. Only a protected-storage adapter should serialize it.
pub struct PeerMaterial(Document);

impl fmt::Debug for PeerMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerMaterial([REDACTED])")
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Jwk {
    kty: String,
    crv: String,
    alg: String,
    kid: String,
    x: String,
    y: String,
}

fn public_key(secret: &SecretKey, kid: &str) -> (String, String) {
    let point = secret.public_key().to_encoded_point(false);
    let bytes = point.as_bytes();
    let fingerprint = URL_SAFE_NO_PAD.encode(Sha256::digest(bytes));
    let jwks = serde_json::to_string(&Jwks {
        keys: vec![Jwk {
            kty: "EC".into(),
            crv: "P-256".into(),
            alg: "ES256".into(),
            kid: kid.into(),
            x: URL_SAFE_NO_PAD.encode(&bytes[1..33]),
            y: URL_SAFE_NO_PAD.encode(&bytes[33..65]),
        }],
    })
    .expect("public key JSON serialization is infallible");
    (jwks, fingerprint)
}

impl PeerMaterial {
    /// Generate a fresh cluster identity and ES256 key for explicit installation.
    /// Runtime startup never calls this; rotation requires a new protected file.
    pub fn generate() -> Result<Self, PeerError> {
        let cluster_id = Uuid::new_v4();
        let secret = SecretKey::random(&mut OsRng);
        let kid = Uuid::new_v4().to_string();
        let (jwks, fingerprint) = public_key(&secret, &kid);
        let private = secret.to_pkcs8_der().map_err(|_| PeerError::Material)?;
        Ok(Self(Document {
            version: 1,
            profile: PeerProfile {
                cluster_id,
                issuer: format!("urn:tokeira:peer:{cluster_id}"),
                audience: format!("urn:tokeira:workflow-offers:{cluster_id}"),
                subjects: vec![PEER_SUBJECT.into()],
                kid,
                fingerprint,
                jwks,
            },
            private_pkcs8: URL_SAFE_NO_PAD.encode(private.as_bytes()),
        }))
    }

    /// Parse a size-limited protected document and check its own key agreement.
    pub fn load(bytes: &[u8]) -> Result<Self, PeerError> {
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(PeerError::TooLarge);
        }
        let material = Self(serde_json::from_slice(bytes).map_err(|_| PeerError::Material)?);
        material.signer(&material.0.profile)?;
        Ok(material)
    }

    /// Public profile checked against the private key on load.
    pub fn profile(&self) -> &PeerProfile {
        &self.0.profile
    }

    /// Serialize exclusively into a protected credential file, never logs or config.
    pub fn protected_bytes(&self) -> Result<Vec<u8>, PeerError> {
        let bytes = serde_json::to_vec(&self.0).map_err(|_| PeerError::Material)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(PeerError::TooLarge);
        }
        Ok(bytes)
    }

    /// Reject corrupt keys, profile drift and unrelated trust
    /// before a runtime can become ready. The signing key is not inferred from JWTs.
    pub fn signer(&self, expected: &PeerProfile) -> Result<PeerSigner, PeerError> {
        if self.0.version != 1 || &self.0.profile != expected {
            return Err(PeerError::Material);
        }
        PeerVerifier::new(expected.clone())?;
        let private = URL_SAFE_NO_PAD
            .decode(&self.0.private_pkcs8)
            .map_err(|_| PeerError::Material)?;
        let key = SecretKey::from_pkcs8_der(&private).map_err(|_| PeerError::Material)?;
        let (jwks, fingerprint) = public_key(&key, &expected.kid);
        if fingerprint != expected.fingerprint || jwks != expected.jwks {
            return Err(PeerError::Material);
        }
        Ok(PeerSigner {
            profile: expected.clone(),
            key: EncodingKey::from_ec_der(&private),
            cached: Mutex::new(None),
        })
    }
}

/// A redacted bearer value, accessed only at the transport boundary.
#[derive(Clone)]
pub struct PeerToken {
    value: String,
    issued_at: u64,
    expires_at: u64,
}

impl fmt::Debug for PeerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerToken([REDACTED])")
    }
}

impl PeerToken {
    /// Full authorization value; must never enter diagnostics or durable state.
    pub fn authorization(&self) -> &str {
        &self.value
    }
}

/// Five-minute token cache. Renewal is local and lazy, without a background task
/// or file read; a running node retains the validated startup credential.
pub struct PeerSigner {
    profile: PeerProfile,
    key: EncodingKey,
    cached: Mutex<Option<PeerToken>>,
}

impl fmt::Debug for PeerSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerSigner([REDACTED])")
    }
}

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    iss: String,
    aud: serde_json::Value,
    sub: String,
    iat: u64,
    exp: u64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "numeric_not_before"
    )]
    nbf: Option<u64>,
}

fn numeric_not_before<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    // Absence is permitted via `default`; a present null/string is not a date.
    u64::deserialize(deserializer).map(Some)
}

impl PeerSigner {
    /// Reuse only with at least one minute remaining. Clock regression forces a
    /// new token so a cached future-issued token is never sent.
    pub fn token_at(&self, now: u64) -> Result<PeerToken, PeerError> {
        let mut cached = self.cached.lock().map_err(|_| PeerError::Signing)?;
        if let Some(token) = cached
            .as_ref()
            .filter(|token| now >= token.issued_at && token.expires_at.saturating_sub(now) >= 60)
        {
            return Ok(token.clone());
        }
        let exp = now.checked_add(300).ok_or(PeerError::Signing)?;
        let header = Header {
            kid: Some(self.profile.kid.clone()),
            ..Header::new(Algorithm::ES256)
        };
        let claims = TokenClaims {
            iss: self.profile.issuer.clone(),
            aud: self.profile.audience.clone().into(),
            sub: PEER_SUBJECT.into(),
            iat: now,
            exp,
            nbf: None,
        };
        let encoded = encode(&header, &claims, &self.key).map_err(|_| PeerError::Signing)?;
        let token = PeerToken {
            value: format!("Bearer {encoded}"),
            issued_at: now,
            expires_at: exp,
        };
        if token.value.len() > MAX_AUTHORIZATION_BYTES {
            return Err(PeerError::TooLarge);
        }
        *cached = Some(token.clone());
        Ok(token)
    }
}

/// Separate offer-only policy; it neither reads nor grants public roles.
pub struct PeerVerifier {
    profile: PeerProfile,
    key: DecodingKey,
}

impl fmt::Debug for PeerVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerVerifier")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

impl PeerVerifier {
    /// Admit exactly the ES256 key and identity from the validated file profile.
    pub fn new(profile: PeerProfile) -> Result<Self, PeerError> {
        if profile.jwks.len() > MAX_DOCUMENT_BYTES {
            return Err(PeerError::TooLarge);
        }
        if profile.cluster_id.is_nil()
            || profile.kid.is_empty()
            || profile.issuer != format!("urn:tokeira:peer:{}", profile.cluster_id)
            || profile.audience != format!("urn:tokeira:workflow-offers:{}", profile.cluster_id)
            || profile.subjects != [PEER_SUBJECT]
        {
            return Err(PeerError::Material);
        }
        let jwks: Jwks = serde_json::from_str(&profile.jwks).map_err(|_| PeerError::Material)?;
        let [jwk]: [Jwk; 1] = jwks.keys.try_into().map_err(|_| PeerError::Material)?;
        if jwk.kty != "EC" || jwk.crv != "P-256" || jwk.alg != "ES256" || jwk.kid != profile.kid {
            return Err(PeerError::Material);
        }
        let x = URL_SAFE_NO_PAD
            .decode(&jwk.x)
            .map_err(|_| PeerError::Material)?;
        let y = URL_SAFE_NO_PAD
            .decode(&jwk.y)
            .map_err(|_| PeerError::Material)?;
        if x.len() != 32 || y.len() != 32 {
            return Err(PeerError::Material);
        }
        let point = [vec![4], x, y].concat();
        p256::PublicKey::from_sec1_bytes(&point).map_err(|_| PeerError::Material)?;
        if URL_SAFE_NO_PAD.encode(Sha256::digest(&point)) != profile.fingerprint {
            return Err(PeerError::Material);
        }
        let key =
            DecodingKey::from_ec_components(&jwk.x, &jwk.y).map_err(|_| PeerError::Material)?;
        Ok(Self { profile, key })
    }

    /// Verify a bounded bearer at an explicit Unix clock reading. Numeric claims
    /// are mandatory; public mapper defaults never apply. Thirty seconds is the
    /// sole clock leeway, and the signed lifetime cannot exceed one hour.
    pub fn verify_at(&self, authorization: &str, now: u64) -> Result<(), PeerError> {
        if authorization.len() > MAX_AUTHORIZATION_BYTES {
            return Err(PeerError::TooLarge);
        }
        let token = authorization
            .strip_prefix("Bearer ")
            .ok_or(PeerError::Unauthenticated)?;
        let header = decode_header(token).map_err(|_| PeerError::Unauthenticated)?;
        if header.alg != Algorithm::ES256 || header.kid.as_deref() != Some(&self.profile.kid) {
            return Err(PeerError::Unauthenticated);
        }
        let mut validation = Validation::new(Algorithm::ES256);
        // Signature verification remains mandatory. Explicit time/identity checks
        // below use the injected clock, allowing deterministic boundary tests.
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        let claims = decode::<TokenClaims>(token, &self.key, &validation)
            .map_err(|_| PeerError::Unauthenticated)?
            .claims;
        if claims.exp <= claims.iat
            || claims.exp - claims.iat > 3600
            || claims.iat > now.saturating_add(30)
            || now >= claims.exp.saturating_add(30)
            || claims
                .nbf
                .is_some_and(|nbf| nbf > now.saturating_add(30) || nbf >= claims.exp)
        {
            return Err(PeerError::Unauthenticated);
        }
        let audience_matches = match &claims.aud {
            serde_json::Value::String(value) => value == &self.profile.audience,
            serde_json::Value::Array(values) => {
                values.iter().all(|value| value.is_string())
                    && values
                        .iter()
                        .any(|value| value.as_str() == Some(&self.profile.audience))
            }
            _ => false,
        };
        if claims.iss != self.profile.issuer || !audience_matches || claims.sub != PEER_SUBJECT {
            return Err(PeerError::PermissionDenied);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn fixture_material() -> PeerMaterial {
        PeerMaterial::generate().unwrap()
    }

    fn sign(material: &PeerMaterial, claims: serde_json::Value) -> String {
        let private = URL_SAFE_NO_PAD.decode(&material.0.private_pkcs8).unwrap();
        let header = Header {
            kid: Some(material.profile().kid.clone()),
            ..Header::new(Algorithm::ES256)
        };
        format!(
            "Bearer {}",
            encode(&header, &claims, &EncodingKey::from_ec_der(&private)).unwrap()
        )
    }

    // Feature: queue-home-delivery, Property 7
    // Only the separate signed peer grant with bounded validity is accepted.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn generated_peer_policy(now in 10_000u64..1_000_000_000, mutation in 0u8..15) {
            let material = fixture_material();
            let verifier = PeerVerifier::new(material.profile().clone()).unwrap();
            let mut claims = serde_json::json!({"iss": material.profile().issuer, "aud": material.profile().audience, "sub": PEER_SUBJECT, "iat": now, "exp": now + 300});
            match mutation {
                0 => {},
                1 => claims["iss"] = "other".into(),
                2 => claims["aud"] = "other".into(),
                3 => claims["sub"] = "worker".into(),
                4 => { claims.as_object_mut().unwrap().remove("exp"); },
                5 => claims["exp"] = (now - 30).into(),
                6 => claims["iat"] = (now + 31).into(),
                7 => claims["nbf"] = (now + 31).into(),
                8 => claims["exp"] = (now + 3601).into(),
                9 => claims["exp"] = now.into(),
                10 => claims["iat"] = now.to_string().into(),
                11 => { claims.as_object_mut().unwrap().remove("iat"); },
                12 => claims["aud"] = serde_json::json!(["other", material.profile().audience]),
                13 => claims["nbf"] = serde_json::Value::Null,
                _ => claims["exp"] = "later".into(),
            }
            claims["permissions"] = serde_json::json!(["temporal-system:admin", "default:worker"]);
            let authorization = sign(&material, claims);
            let result = verifier.verify_at(&authorization, now);
            prop_assert_eq!(result.is_ok(), mutation == 0 || mutation == 12);
            if let Err(error) = result { let diagnostic = format!("{error:?} {error}"); prop_assert!(!diagnostic.contains(&authorization)); }
            prop_assert!(verifier.verify_at("", now).is_err());
        }

        // Feature: queue-home-delivery, Property 7
        // Every public/private mismatch fails before serving without disclosing
        // the rejected protected input in a diagnostic.
        #[test]
        fn generated_material_agreement(mutation in 0u8..7) {
            let material = fixture_material();
            let bytes = material.protected_bytes().unwrap();
            let mut document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let private = document["private_pkcs8"].as_str().unwrap().to_owned();
            match mutation {
                0 => document["private_pkcs8"] = "corrupt".into(),
                1 => document["private_pkcs8"] = fixture_material().0.private_pkcs8.into(),
                2 => document["profile"]["fingerprint"] = "wrong".into(),
                3 => document["profile"]["kid"] = "unexpected-key".into(),
                4 => document["profile"]["issuer"] = "other".into(),
                5 => document["profile"]["subjects"] = serde_json::json!(["worker"]),
                _ => document["profile"]["jwks"] = "corrupt".into(),
            }
            let error = PeerMaterial::load(&serde_json::to_vec(&document).unwrap()).unwrap_err();
            let diagnostic = format!("{error:?} {error}");
            prop_assert!(!diagnostic.contains(&private));
        }

        // Feature: queue-home-delivery, Property 10
        // Restarts and renewal reuse trust; explicit file replacement revokes it.
        #[test]
        fn generated_renewal_and_rotation(now in 10_000u64..1_000_000_000, restarts in 1usize..6) {
            let material = fixture_material();
            let profile = material.profile().clone();
            let signer = material.signer(&profile).unwrap();
            let verifier = PeerVerifier::new(profile.clone()).unwrap();
            let first = signer.token_at(now).unwrap();
            let reusable = signer.token_at(now + 240).unwrap();
            let renewed = signer.token_at(now + 241).unwrap();
            prop_assert!(first.authorization() == reusable.authorization());
            prop_assert!(first.authorization() != renewed.authorization());
            for index in 1..=restarts {
                let bytes = material.protected_bytes().unwrap();
                let restarted = PeerMaterial::load(&bytes).unwrap().signer(&profile).unwrap();
                let later = now + index as u64 * 600;
                let token = restarted.token_at(later).unwrap();
                prop_assert!(verifier.verify_at(token.authorization(), later).is_ok());
                prop_assert!(verifier.verify_at(first.authorization(), later).is_err());
                let diagnostic = format!("{restarted:?} {token:?}"); prop_assert!(!diagnostic.contains(token.authorization()));
            }
            let rotated = PeerMaterial::generate().unwrap();
            prop_assert!(PeerVerifier::new(rotated.profile().clone()).unwrap().verify_at(first.authorization(), now).is_err());
            prop_assert!(material.signer(rotated.profile()).is_err());
            let other = fixture_material();
            prop_assert!(PeerVerifier::new(other.profile().clone()).unwrap().verify_at(first.authorization(), now).is_err());
        }
    }

    #[test]
    fn exact_document_and_authorization_bounds_and_corrupt_material() {
        let material = fixture_material();
        let mut bytes = material.protected_bytes().unwrap();
        bytes.resize(MAX_DOCUMENT_BYTES, b' ');
        assert!(PeerMaterial::load(&bytes).is_ok());
        bytes.push(b' ');
        assert_eq!(PeerMaterial::load(&bytes).unwrap_err(), PeerError::TooLarge);
        let verifier = PeerVerifier::new(material.profile().clone()).unwrap();
        assert_eq!(
            verifier.verify_at(&"x".repeat(MAX_AUTHORIZATION_BYTES), 1),
            Err(PeerError::Unauthenticated)
        );
        assert_eq!(
            verifier.verify_at(&"x".repeat(MAX_AUTHORIZATION_BYTES + 1), 1),
            Err(PeerError::TooLarge)
        );
        let mut profile = material.profile().clone();
        profile
            .jwks
            .push_str(&" ".repeat(MAX_DOCUMENT_BYTES - profile.jwks.len()));
        assert!(PeerVerifier::new(profile.clone()).is_ok());
        profile.jwks.push(' ');
        assert_eq!(PeerVerifier::new(profile).unwrap_err(), PeerError::TooLarge);
        let mut document: serde_json::Value =
            serde_json::from_slice(&material.protected_bytes().unwrap()).unwrap();
        document["private_pkcs8"] = "corrupt".into();
        assert!(PeerMaterial::load(&serde_json::to_vec(&document).unwrap()).is_err());
        document["private_pkcs8"] = fixture_material().0.private_pkcs8.into();
        assert!(PeerMaterial::load(&serde_json::to_vec(&document).unwrap()).is_err());
    }

    #[test]
    fn validity_boundaries_are_explicit() {
        let material = fixture_material();
        let verifier = PeerVerifier::new(material.profile().clone()).unwrap();
        for (iat, exp, nbf, accepted) in [
            (1030, 1100, 1030, true),
            (1031, 1100, 1000, false),
            (900, 970, 900, false),
            (900, 971, 900, true),
            (1000, 4600, 1000, true),
            (1000, 4601, 1000, false),
        ] {
            let token = sign(
                &material,
                serde_json::json!({"iss": material.profile().issuer, "aud": material.profile().audience, "sub": PEER_SUBJECT, "iat": iat, "exp": exp, "nbf": nbf}),
            );
            assert_eq!(verifier.verify_at(&token, 1000).is_ok(), accepted);
        }
    }
}
