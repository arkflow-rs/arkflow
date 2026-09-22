/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! OIDC JWT bearer authentication for Hub operator APIs.
//!
//! The Hub acts as an OAuth2 resource server: bearer tokens issued by an
//! external OIDC identity provider are validated against the provider's
//! JWKS (signature, expiry, issuer, audience) and mapped onto the existing
//! [`OperatorPrincipal`] role/scope model. Static operator credentials
//! keep priority; an unconfigured authenticator changes nothing.
//!
//! Only asymmetric algorithms (ES256/RS256) are accepted — treating a
//! JWKS endpoint as a shared-secret (HS256) oracle is never sound.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api_contract::{OperatorPrincipal, OperatorRole, ResourceScope};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Value;
use tokio::sync::Mutex;

/// Only asymmetric algorithms: a JWKS endpoint publishes public keys, and
/// allowing HMAC algorithms would turn it into a shared-secret oracle.
const ALLOWED_ALGORITHMS: [Algorithm; 2] = [Algorithm::ES256, Algorithm::RS256];
/// Minimum interval between forced JWKS refetches, so garbage `kid`s
/// cannot hammer the identity provider. Known kids stay usable forever
/// (availability over strict revocation; recommend short-lived tokens at
/// the IdP).
const JWKS_REFRESH_THROTTLE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct OidcAuthenticator {
    issuer: String,
    audience: String,
    jwks_url: String,
    role_claim: String,
    scopes_claim: String,
    client: reqwest::Client,
    cache: Arc<Mutex<Option<CachedJwks>>>,
    refresh_throttle: Duration,
}

#[derive(Clone)]
struct CachedJwks {
    keys: HashMap<String, DecodingKey>,
    fetched_at: Instant,
}

impl OidcAuthenticator {
    /// Builds an authenticator from the standard environment variables.
    /// `Some` only when both `ARKFLOW_OIDC_ISSUER` and
    /// `ARKFLOW_OIDC_AUDIENCE` are set — an unconfigured Hub behaves
    /// exactly as before.
    pub fn new(issuer: String, audience: String, jwks_url: String, role_claim: String, scopes_claim: String) -> Self {
        Self {
            issuer,
            audience,
            jwks_url,
            role_claim,
            scopes_claim,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("OIDC HTTP client must build"),
            cache: Arc::new(Mutex::new(None)),
            refresh_throttle: JWKS_REFRESH_THROTTLE,
        }
    }

    /// Validates a bearer token and maps it to a principal, or `None` when
    /// anything at all fails to check out (callers answer 401).
    pub async fn authenticate(&self, token: &str) -> Option<OperatorPrincipal> {
        let header = decode_header(token).ok()?;
        if !ALLOWED_ALGORITHMS.contains(&header.alg) {
            return None;
        }
        let kid = header.kid.as_deref()?;
        let key = self.decoding_key(kid).await?;

        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);

        let data = decode::<Value>(token, &key, &validation).ok()?;
        let claims = data.claims;

        let id = claims.get("sub")?.as_str()?.to_owned();
        if id.is_empty() {
            return None;
        }
        let role = role_from_claims(&claims, &self.role_claim)?;
        let scopes = scopes_from_claims(&claims, &self.scopes_claim);
        Some(OperatorPrincipal {
            id,
            roles: vec![role],
            scopes,
        })
    }

    /// Known kids are served from cache even past the TTL (a provider key
    /// rotation must not take the Hub down); unknown kids trigger one
    /// refetch, throttled so garbage kids cannot hammer the provider.
    async fn decoding_key(&self, kid: &str) -> Option<DecodingKey> {
        let mut cache = self.cache.lock().await;
        if let Some(cached) = cache.as_ref() {
            if let Some(key) = cached.keys.get(kid) {
                return Some(key.clone());
            }
            if cached.fetched_at.elapsed() < self.refresh_throttle {
                return None;
            }
        }
        let fetched = self.fetch_keys().await?;
        let key = fetched.keys.get(kid).cloned();
        *cache = Some(fetched);
        key
    }

    async fn fetch_keys(&self) -> Option<CachedJwks> {
        let text = self
            .client
            .get(&self.jwks_url)
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .text()
            .await
            .ok()?;
        let jwks: JwkSet = serde_json::from_str(&text).ok()?;
        let mut keys = HashMap::new();
        for jwk in &jwks.keys {
            let kid = jwk.common.key_id.clone().unwrap_or_default();
            if kid.is_empty() {
                continue;
            }
            if let Ok(key) = DecodingKey::from_jwk(jwk) {
                keys.insert(kid, key);
            }
        }
        Some(CachedJwks {
            keys,
            fetched_at: Instant::now(),
        })
    }
}

/// Federation facade: the JWT authenticator plus optional browser
/// authorization-code login (client credentials + discovery) and the
/// in-memory session table backing the `arkflow_session` cookie.
pub struct OidcFederation {
    authenticator: OidcAuthenticator,
    login: Option<OidcLoginClient>,
    sessions: Arc<std::sync::Mutex<HashMap<String, (OperatorPrincipal, Instant)>>>,
    http: reqwest::Client,
}

#[derive(Clone)]
pub struct OidcLoginClient {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}

/// Environment-backed settings for [`OidcFederation::from_env`].
#[derive(Debug, Clone, Default)]
pub struct OidcSettings {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: Option<String>,
    pub role_claim: Option<String>,
    pub scopes_claim: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub redirect_uri: Option<String>,
}

const SESSION_TTL: Duration = Duration::from_secs(8 * 3600);

impl OidcFederation {
    /// Reads the standard `ARKFLOW_OIDC_*` environment variables. Returns
    /// `None` when issuer/audience are absent (no federation, no routes).
    pub async fn from_env() -> Option<Arc<Self>> {
        let settings = OidcSettings {
            issuer: std::env::var("ARKFLOW_OIDC_ISSUER").unwrap_or_default(),
            audience: std::env::var("ARKFLOW_OIDC_AUDIENCE").unwrap_or_default(),
            jwks_url: non_empty("ARKFLOW_OIDC_JWKS_URL"),
            role_claim: non_empty("ARKFLOW_OIDC_ROLE_CLAIM"),
            scopes_claim: non_empty("ARKFLOW_OIDC_SCOPES_CLAIM"),
            client_id: non_empty("ARKFLOW_OIDC_CLIENT_ID"),
            client_secret: non_empty("ARKFLOW_OIDC_CLIENT_SECRET"),
            redirect_uri: non_empty("ARKFLOW_OIDC_REDIRECT_URI"),
        };
        Self::from_settings(settings).await
    }

    /// Builds the federation from explicit settings. Login flow activates
    /// only when all three client fields are present; discovery failure
    /// downgrades to bearer-only with a warning.
    pub async fn from_settings(settings: OidcSettings) -> Option<Arc<Self>> {
        if settings.issuer.trim().is_empty() || settings.audience.trim().is_empty() {
            return None;
        }
        let issuer = settings.issuer.trim_end_matches('/').to_string();
        let jwks_url = settings.jwks_url.clone().unwrap_or_else(|| {
            format!("{issuer}/.well-known/jwks.json")
        });
        let role_claim = settings.role_claim.clone().unwrap_or_else(|| "roles".to_string());
        let scopes_claim = settings.scopes_claim.clone().unwrap_or_else(|| "scopes".to_string());
        let authenticator =
            OidcAuthenticator::new(issuer.clone(), settings.audience.clone(), jwks_url, role_claim, scopes_claim);

        let login = match (
            settings.client_id.as_deref().filter(|v| !v.trim().is_empty()),
            settings.client_secret.as_deref().filter(|v| !v.trim().is_empty()),
            settings.redirect_uri.as_deref().filter(|v| !v.trim().is_empty()),
        ) {
            (Some(client_id), Some(client_secret), Some(redirect_uri)) => {
                match discover_endpoints(&issuer).await {
                    Ok((authorization_endpoint, token_endpoint)) => Some(OidcLoginClient {
                        client_id: client_id.to_string(),
                        client_secret: client_secret.to_string(),
                        redirect_uri: redirect_uri.to_string(),
                        authorization_endpoint,
                        token_endpoint,
                    }),
                    Err(error) => {
                        eprintln!(
                            "OIDC discovery failed (login disabled, bearer federation kept): {error}"
                        );
                        None
                    }
                }
            }
            _ => None,
        };

        Some(Arc::new(Self {
            authenticator,
            login,
            sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("OIDC HTTP client must build"),
        }))
    }

    /// Whether the browser authorization-code login routes are active.
    pub fn login_enabled(&self) -> bool {
        self.login.is_some()
    }

    /// Synchronous construction without discovery: the browser login flow
    /// stays disabled (bearer federation only).
    pub fn from_settings_blocking(settings: OidcSettings) -> Option<Arc<Self>> {
        if settings.issuer.trim().is_empty() || settings.audience.trim().is_empty() {
            return None;
        }
        let issuer = settings.issuer.trim_end_matches('/').to_string();
        let jwks_url = settings.jwks_url.clone().unwrap_or_else(|| {
            format!("{issuer}/.well-known/jwks.json")
        });
        let role_claim = settings.role_claim.clone().unwrap_or_else(|| "roles".to_string());
        let scopes_claim = settings.scopes_claim.clone().unwrap_or_else(|| "scopes".to_string());
        let authenticator = OidcAuthenticator::new(
            issuer,
            settings.audience.clone(),
            jwks_url,
            role_claim,
            scopes_claim,
        );
        Some(Arc::new(Self {
            authenticator,
            login: None,
            sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("OIDC HTTP client must build"),
        }))
    }

    pub async fn authenticate(&self, token: &str) -> Option<OperatorPrincipal> {
        self.authenticator.authenticate(token).await
    }

    /// The IdP redirect for starting a login: random state, authorization
    /// endpoint, code flow, openid scope.
    pub fn authorization_redirect(&self, state: &str) -> String {
        let login = self.login.as_ref().expect("login enabled");
        format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope=openid&state={}",
            login.authorization_endpoint,
            urlencode(&login.client_id),
            urlencode(&login.redirect_uri),
            urlencode(state),
        )
    }

    /// Exchanges an authorization code for an id_token via the token
    /// endpoint (form-encoded POST with the client credentials).
    pub async fn exchange_code(&self, code: &str) -> Option<String> {
        let login = self.login.as_ref()?;
        let form = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", login.redirect_uri.as_str()),
            ("client_id", login.client_id.as_str()),
            ("client_secret", login.client_secret.as_str()),
        ];
        let encoded: Vec<String> = form
            .iter()
            .map(|(key, value)| format!("{}={}", urlencode(key), urlencode(value)))
            .collect();
        let response = self
            .http
            .post(&login.token_endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(encoded.join("&"))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = response.json().await.ok()?;
        body.get("id_token").and_then(Value::as_str).map(str::to_string)
    }

    /// Creates an 8-hour session for the principal and returns the random
    /// session id (the cookie value).
    pub fn create_session(&self, principal: OperatorPrincipal) -> String {
        use rand::TryRngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut bytes).expect("OS randomness");
        let sid: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, (_, created)| created.elapsed() < SESSION_TTL);
        sessions.insert(sid.clone(), (principal, Instant::now()));
        sid
    }

    /// Resolves a session id to its principal, dropping expired entries.
    pub fn resolve_session(&self, session_id: &str) -> Option<OperatorPrincipal> {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, (_, created)| created.elapsed() < SESSION_TTL);
        sessions.get(session_id).map(|(principal, _)| principal.clone())
    }

    /// Deletes a session (logout).
    pub fn remove_session(&self, session_id: &str) {
        self.sessions.lock().unwrap().remove(session_id);
    }
}

/// Fetches authorization/token endpoints from the provider's discovery
/// document (`{issuer}/.well-known/openid-configuration`).
async fn discover_endpoints(issuer: &str) -> Result<(String, String), String> {
    let url = format!("{issuer}/.well-known/openid-configuration");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;
    let document: Value = client
        .get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let authorization_endpoint = document
        .get("authorization_endpoint")
        .and_then(Value::as_str)
        .ok_or("discovery document lacks authorization_endpoint")?
        .to_string();
    let token_endpoint = document
        .get("token_endpoint")
        .and_then(Value::as_str)
        .ok_or("discovery document lacks token_endpoint")?
        .to_string();
    Ok((authorization_endpoint, token_endpoint))
}

fn urlencode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn non_empty(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|value| !value.trim().is_empty())
}

/// Maps the configured role claim to the highest matching role: a user in
/// several groups gets the most privileged one, unknown groups are ignored.
fn role_from_claims(claims: &Value, role_claim: &str) -> Option<OperatorRole> {
    let claimed = claim_values(claims, role_claim)?;
    for (name, role) in [
        ("admin", OperatorRole::Admin),
        ("operator", OperatorRole::Operator),
        ("viewer", OperatorRole::Viewer),
    ] {
        if claimed.iter().any(|value| value == name) {
            return Some(role);
        }
    }
    None
}

fn scopes_from_claims(claims: &Value, scopes_claim: &str) -> Vec<ResourceScope> {
    claim_values(claims, scopes_claim)
        .unwrap_or_default()
        .iter()
        .filter_map(|value| parse_scope(value))
        .collect()
}

/// Claim values as strings, accepting either a JSON array or a single
/// (comma-separated for scopes) string.
fn claim_values(claims: &Value, claim: &str) -> Option<Vec<String>> {
    match claims.get(claim) {
        Some(Value::Array(items)) => Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.to_string())
                .collect(),
        ),
        Some(Value::String(value)) => Some(vec![value.clone()]),
        _ => None,
    }
}

fn parse_scope(value: &str) -> Option<ResourceScope> {
    let value = value.trim();
    let (resource_type, resource_id) = value.split_once('=')?;
    if resource_type.trim().is_empty() {
        return None;
    }
    Some(ResourceScope {
        resource_type: resource_type.trim().to_owned(),
        resource_id: (!resource_id.is_empty()).then(|| resource_id.to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use serde_json::json;
    use jsonwebtoken::{encode, EncodingKey, Header};

    const TEST_EC_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgFKaU4QngGKYHGH+b\nFq3SU0eDMNlPgd3sNKkgaiFo2OahRANCAAS/UI48yy85lD5Gl6/4kIkN4hZj4vzV\nAUhlVj77ptTqegrClIS4WkTACFVD7+/VGnXDGvVdxPRIx6G9ZMJ6YPZg\n-----END PRIVATE KEY-----";
    const TEST_EC_WRONG_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgXdW84DyTHfX0zZ+y\nlSPXGHNeMSHmBFz1hX6WE/kyWRShRANCAASYmpicaPNUmsulnnZVid0Goz14cWwO\nQR9rJt0zomK913pFKVCY64fHIn88VwmJMv/HADRjdbK7KDgXlbKsGxS9\n-----END PRIVATE KEY-----";

    const TEST_KID: &str = "test-key-1";
    const TEST_X: &str = "v1COPMsvOZQ-Rpev-JCJDeIWY-L81QFIZVY--6bU6no";
    const TEST_Y: &str = "CsKUhLhaRMAIVUPv79UadcMa9V3E9EjHob1kwnpg9mA";
    const ISSUER: &str = "https://idp.example.com";
    const AUDIENCE: &str = "arkflow-hub";

    struct MockJwks {
        addr: std::net::SocketAddr,
        fetches: Arc<std::sync::atomic::AtomicUsize>,
        body: Arc<std::sync::Mutex<String>>,
    }

    impl MockJwks {
        fn spawn(initial_body: String) -> Self {
            let body = Arc::new(std::sync::Mutex::new(initial_body));
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let fetch_count = fetches.clone();
            let shared_body = body.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let shared_body = shared_body.clone();
                    let fetch_count = fetch_count.clone();
                    std::thread::spawn(move || {
                        let mut buffer = Vec::new();
                        let mut byte = [0u8; 1];
                        loop {
                            use std::io::Read;
                            if stream.read_exact(&mut byte).is_err() {
                                break;
                            }
                            buffer.push(byte[0]);
                            if buffer.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        let response_body = shared_body.lock().unwrap().clone();
                        fetch_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                            response_body.len()
                        );
                        use std::io::Write;
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    });
                }
            });
            Self {
                addr,
                fetches,
                body,
            }
        }

        fn fetch_count(&self) -> usize {
            self.fetches.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn set_body(&self, new_body: String) {
            *self.body.lock().unwrap() = new_body;
        }
    }

    fn authenticator(jwks_url: String) -> OidcAuthenticator {
        let mut auth = OidcAuthenticator::new(
            ISSUER.to_string(),
            AUDIENCE.to_string(),
            jwks_url,
            "roles".to_string(),
            "scopes".to_string(),
        );
        auth.refresh_throttle = Duration::ZERO;
        auth
    }

    fn jwks_body(kids: &[&str]) -> String {
        let keys: Vec<String> = kids
            .iter()
            .map(|kid| {
                format!(
                    r#"{{"kty":"EC","crv":"P-256","kid":"{kid}","x":"{TEST_X}","y":"{TEST_Y}"}}"#
                )
            })
            .collect();
        format!(r#"{{"keys":[{}]}}"#, keys.join(","))
    }

    fn mint(claims: Value, kid: Option<&str>) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = kid.map(str::to_string);
        let key = EncodingKey::from_ec_pem(TEST_EC_PRIVATE_PEM.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    fn claims(sub: &str, roles: Value, exp_in: i64) -> Value {
        json!({
            "sub": sub,
            "roles": roles,
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": chrono_offset_now_ms() / 1000 + exp_in,
        })
    }

    fn chrono_offset_now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[tokio::test]
    async fn valid_viewer_token_maps_to_principal() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let auth = authenticator(format!("http://{}/jwks", mock.addr));
        let token = mint(claims("u1", json!(["viewer"]), 600), Some(TEST_KID));

        let principal = auth.authenticate(&token).await.unwrap();
        assert_eq!(principal.id, "u1");
        assert_eq!(principal.roles, vec![OperatorRole::Viewer]);
        assert!(principal.scopes.is_empty());
    }

    #[tokio::test]
    async fn multiple_roles_take_highest_privilege() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let auth = authenticator(format!("http://{}/jwks", mock.addr));
        let token = mint(claims("u2", json!(["viewer", "operator"]), 600), Some(TEST_KID));
        let principal = auth.authenticate(&token).await.unwrap();
        assert_eq!(principal.roles, vec![OperatorRole::Operator]);

        let token = mint(claims("u3", json!(["viewer", "admin", "operator"]), 600), Some(TEST_KID));
        let principal = auth.authenticate(&token).await.unwrap();
        assert_eq!(principal.roles, vec![OperatorRole::Admin]);
    }

    #[tokio::test]
    async fn scopes_claim_maps_to_resource_scopes() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let mut claims = claims("u4", json!(["operator"]), 600);
        claims["scopes"] = json!(["node=node-a", "stream=orders"]);
        let auth = authenticator(format!("http://{}/jwks", mock.addr));
        let token = mint(claims, Some(TEST_KID));
        let principal = auth.authenticate(&token).await.unwrap();
        assert_eq!(principal.scopes.len(), 2);
        assert_eq!(principal.scopes[0].resource_type, "node");
        assert_eq!(principal.scopes[0].resource_id.as_deref(), Some("node-a"));
        assert_eq!(principal.scopes[1].resource_type, "stream");
        assert_eq!(principal.scopes[1].resource_id.as_deref(), Some("orders"));
    }

    #[tokio::test]
    async fn no_matching_role_rejects() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let auth = authenticator(format!("http://{}/jwks", mock.addr));
        let token = mint(claims("u5", json!(["superuser"]), 600), Some(TEST_KID));
        assert!(auth.authenticate(&token).await.is_none());
    }

    #[tokio::test]
    async fn expired_wrong_issuer_wrong_audience_reject() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let auth = authenticator(format!("http://{}/jwks", mock.addr));

        let expired = mint(claims("u6", json!(["viewer"]), -100), Some(TEST_KID));
        assert!(auth.authenticate(&expired).await.is_none());

        let mut wrong_iss = claims("u6", json!(["viewer"]), 600);
        wrong_iss["iss"] = json!("https://evil.example.com");
        let token = mint(wrong_iss, Some(TEST_KID));
        assert!(auth.authenticate(&token).await.is_none());

        let mut wrong_aud = claims("u6", json!(["viewer"]), 600);
        wrong_aud["aud"] = json!("other-service");
        let token = mint(wrong_aud, Some(TEST_KID));
        assert!(auth.authenticate(&token).await.is_none());
    }

    #[tokio::test]
    async fn bad_signature_and_hs256_reject() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let auth = authenticator(format!("http://{}/jwks", mock.addr));

        // Signed by a different key than the JWKS publishes.
        let other_key = EncodingKey::from_ec_pem(TEST_EC_WRONG_PEM.as_bytes()).unwrap();
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(TEST_KID.to_string());
        let forged = encode(&header, &claims("u7", json!(["admin"]), 600), &other_key).unwrap();
        assert!(auth.authenticate(&forged).await.is_none());

        // HS256 is outside the asymmetric allowlist: rejected before any
        // key material is consulted.
        let hmac_key = EncodingKey::from_secret(b"shared-secret");
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(TEST_KID.to_string());
        let hmac = encode(&header, &claims("u7", json!(["admin"]), 600), &hmac_key).unwrap();
        assert!(auth.authenticate(&hmac).await.is_none());
    }

    #[tokio::test]
    async fn unknown_kid_triggers_single_refresh() {
        let mock = MockJwks::spawn(jwks_body(&[])); // empty at first
        let auth = authenticator(format!("http://{}/jwks", mock.addr));
        let token = mint(claims("u8", json!(["viewer"]), 600), Some(TEST_KID));
        assert!(auth.authenticate(&token).await.is_none(), "kid not published yet");
        assert_eq!(mock.fetch_count(), 1);

        // IdP publishes the key; the next attempt refetches and succeeds.
        mock.set_body(jwks_body(&[TEST_KID]));
        std::thread::sleep(std::time::Duration::from_millis(20));
        let principal = auth.authenticate(&token).await.unwrap();
        assert_eq!(principal.id, "u8");
        assert_eq!(mock.fetch_count(), 2);

        // Known kid is served from cache: no further fetches.
        let _ = auth.authenticate(&token).await.unwrap();
        assert_eq!(mock.fetch_count(), 2);
    }

    #[tokio::test]
    async fn static_credential_priority_skips_jwks() {
        use crate::hub::Hub;
        use crate::hub::HubConfig;

        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let federation = OidcFederation::from_settings(OidcSettings {
            issuer: ISSUER.to_string(),
            audience: AUDIENCE.to_string(),
            jwks_url: Some(format!("http://{}/jwks", mock.addr)),
            ..OidcSettings::default()
        })
        .await
        .unwrap();
        let hub = Hub::new(HubConfig {
            operator_token: Some("breakglass|admin|static-secret".into()),
            node_token: None,
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: 3_600_000,
        })
        .with_oidc(federation);

        // Static credential wins without touching the JWKS endpoint.
        let principal = hub.operator_principal(Some("static-secret")).await.unwrap();
        assert_eq!(principal.id, "breakglass");
        assert_eq!(principal.roles, vec![OperatorRole::Admin]);
        assert_eq!(mock.fetch_count(), 0);
        assert!(hub.operator_authorized(Some("static-secret")).await);
    }

    /// A full mock IdP: discovery + token + jwks endpoints, plus minting.
    struct MockIdp {
        addr: std::net::SocketAddr,
        jwks_url: String,
        discovery_url: String,
        token_url: String,
        requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    }

    impl MockIdp {
        fn spawn() -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let request_log = requests.clone();
            let base = format!("http://{addr}");
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let request_log = request_log.clone();
                    let base = base.clone();
                    std::thread::spawn(move || {
                        let mut buffer = Vec::new();
                        let mut byte = [0u8; 1];
                        loop {
                            use std::io::Read;
                            if stream.read_exact(&mut byte).is_err() {
                                break;
                            }
                            buffer.push(byte[0]);
                            if buffer.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        let head = String::from_utf8_lossy(&buffer).to_string();
                        let content_length = head
                            .to_ascii_lowercase()
                            .split("content-length:")
                            .nth(1)
                            .and_then(|rest| rest.split("\r\n").next())
                            .and_then(|value| value.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut body_bytes = vec![0u8; content_length];
                        if content_length > 0 {
                            use std::io::Read;
                            let _ = stream.read_exact(&mut body_bytes);
                        }
                        let body = String::from_utf8_lossy(&body_bytes).to_string();
                        request_log.lock().unwrap().push((head.clone(), body.clone()));

                        let path = head.split(' ').nth(1).unwrap_or("/").to_string();
                        let response_body = if path.starts_with("/.well-known/openid-configuration") {
                            format!(
                                r#"{{"authorization_endpoint":"http://{addr}/authorize","token_endpoint":"http://{addr}/token"}}"#
                            )
                        } else if path.starts_with("/jwks") {
                            jwks_body(&[TEST_KID])
                        } else {
                            // token endpoint: mint an id_token whose iss is
                            // this provider (the Hub validates it).
                            let mut token_claims =
                                claims("console-user", json!(["viewer"]), 600);
                            token_claims["iss"] = json!(base);
                            let id_token = mint(token_claims, Some(TEST_KID));
                            format!(r#"{{"code":0,"id_token":"{id_token}"}}"#)
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                            response_body.len()
                        );
                        use std::io::Write;
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    });
                }
            });
            Self {
                addr,
                jwks_url: format!("http://{addr}/jwks"),
                discovery_url: format!("http://{addr}/discovery"),
                token_url: format!("http://{addr}/token"),
                requests,
            }
        }
    }

    #[tokio::test]
    async fn full_login_flow_creates_session_that_authorizes() {
        use crate::api_contract::OperatorAction;
        use axum::extract::State;
        use crate::hub::Hub;
        use crate::hub::HubConfig;
        use std::collections::HashMap as StdMap;

        let idp = MockIdp::spawn();
        let issuer = format!("http://{}", idp.addr);
        let federation = OidcFederation::from_settings(OidcSettings {
            issuer: issuer.clone(),
            audience: AUDIENCE.to_string(),
            jwks_url: Some(idp.jwks_url.clone()),
            client_id: Some("arkflow-console".to_string()),
            client_secret: Some("console-secret".to_string()),
            redirect_uri: Some(format!("http://console:3000/auth/callback")),
            ..OidcSettings::default()
        })
        .await
        .unwrap();
        assert!(federation.login_enabled());

        let hub = Hub::new(HubConfig {
            operator_token: None,
            node_token: None,
            // Non-insecure: the session must resolve through the federation
            // (a legacy admin principal would bypass RBAC here).
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: 3_600_000,
        })
        .with_oidc(federation.clone());

        // 1. login: 302 to the IdP authorization endpoint + state cookie.
        let response = crate::hub_oidc_login(State(hub.clone())).await;
        assert_eq!(response.status(), axum::http::StatusCode::FOUND);
        let cookies = response
            .headers()
            .get_all(axum::http::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect::<Vec<_>>()
            .join("; ");
        let state = cookies
            .split("; ")
            .find_map(|cookie| cookie.strip_prefix("arkflow_oidc_state="))
            .expect("state cookie")
            .to_string();
        assert!(
            cookies.contains("HttpOnly") && cookies.contains("SameSite=Lax"),
            "state cookie must be HttpOnly/SameSite=Lax: {cookies}"
        );

        // 2. callback: code + matching state -> session cookie.
        let code = "auth-code-1";
        let mut callback_headers = axum::http::HeaderMap::new();
        callback_headers.insert(
            axum::http::header::COOKIE,
            format!("arkflow_oidc_state={state}").parse().unwrap(),
        );
        let mut query = StdMap::new();
        query.insert("code".to_string(), code.to_string());
        query.insert("state".to_string(), state.clone());
        let response = crate::hub_oidc_callback(
            State(hub.clone()),
            axum::extract::Query(query),
            callback_headers,
        )
        .await;
        assert_eq!(response.status(), axum::http::StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(axum::http::header::LOCATION),
            Some(&axum::http::HeaderValue::from_static("/"))
        );
        let set_cookie = response
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .unwrap()
            .to_string();
        assert!(set_cookie.starts_with("arkflow_session="), "{set_cookie}");
        assert!(set_cookie.contains("HttpOnly"), "{set_cookie}");
        let session_id = set_cookie
            .split(';')
            .next()
            .unwrap()
            .trim_start_matches("arkflow_session=")
            .to_string();

        // 3. the session authorizes browser requests through RBAC.
        let read_ok = hub
            .operator_can(Some(&format!("session:{session_id}")), OperatorAction::Read)
            .await;
        let mutate_ok = hub
            .operator_can(Some(&format!("session:{session_id}")), OperatorAction::Operate)
            .await;
        assert!(read_ok, "viewer session must read");
        assert!(!mutate_ok, "viewer session must not mutate");

        // 4. the token endpoint received the code exchange.
        let requests = idp.requests.lock().unwrap();
        assert!(
            requests.iter().any(|(head, _)| head.starts_with("POST /token ")),
            "code exchange must hit the token endpoint"
        );
    }

    #[tokio::test]
    async fn status_endpoint_reflects_login_and_session_state() {
        use crate::hub::Hub;
        use crate::hub::HubConfig;

        let idp = MockIdp::spawn();
        let federation = OidcFederation::from_settings(OidcSettings {
            issuer: format!("http://{}", idp.addr),
            audience: AUDIENCE.to_string(),
            jwks_url: Some(idp.jwks_url.clone()),
            client_id: Some("arkflow-console".to_string()),
            client_secret: Some("console-secret".to_string()),
            redirect_uri: Some("http://console:3000/auth/callback".to_string()),
            ..OidcSettings::default()
        })
        .await
        .unwrap();
        let hub = Hub::new(HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: 3_600_000,
        })
        .with_oidc(federation.clone());

        // Enabled + no session yet.
        let response = crate::hub_oidc_status(State(hub.clone()), HeaderMap::new()).await;
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["login_enabled"], true);
        assert_eq!(body["authenticated"], false);
        assert_eq!(body["principal"], Value::Null);

        // Log in via the flow, then the session must show as authenticated.
        let login = crate::hub_oidc_login(State(hub.clone())).await;
        let cookies = login
            .headers()
            .get_all(axum::http::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect::<Vec<_>>()
            .join("; ");
        let state = cookies
            .split("; ")
            .find_map(|cookie| cookie.strip_prefix("arkflow_oidc_state="))
            .unwrap()
            .to_string();
        let mut callback_headers = axum::http::HeaderMap::new();
        callback_headers.insert(
            axum::http::header::COOKIE,
            format!("arkflow_oidc_state={state}").parse().unwrap(),
        );
        let mut query = std::collections::HashMap::new();
        query.insert("code".to_string(), "auth-code-2".to_string());
        query.insert("state".to_string(), state);
        let callback = crate::hub_oidc_callback(State(hub.clone()), axum::extract::Query(query), callback_headers)
            .await;
        let session_cookie = callback
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .unwrap()
            .to_string();
        let session_id = session_cookie
            .split(';')
            .next()
            .unwrap()
            .trim_start_matches("arkflow_session=")
            .to_string();

        let mut authed_headers = HeaderMap::new();
        authed_headers.insert(
            axum::http::header::COOKIE,
            format!("arkflow_session={session_id}").parse().unwrap(),
        );
        let response = crate::hub_oidc_status(State(hub), authed_headers).await;
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["authenticated"], true);
        assert_eq!(body["principal"]["id"], "console-user");
        assert_eq!(body["principal"]["roles"][0], "viewer");
    }

    #[tokio::test]
    async fn state_mismatch_rejects_without_session() {
        use axum::extract::State;
        use crate::hub::Hub;
        use crate::hub::HubConfig;
        use std::collections::HashMap as StdMap;

        let idp = MockIdp::spawn();
        let issuer = format!("http://{}", idp.addr);
        let federation = OidcFederation::from_settings(OidcSettings {
            issuer: issuer.clone(),
            audience: AUDIENCE.to_string(),
            jwks_url: Some(idp.jwks_url.clone()),
            client_id: Some("arkflow-console".to_string()),
            client_secret: Some("console-secret".to_string()),
            redirect_uri: Some("http://console:3000/auth/callback".to_string()),
            ..OidcSettings::default()
        })
        .await
        .unwrap();
        let hub = Hub::new(HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: 3_600_000,
        })
        .with_oidc(federation.clone());

        let response = crate::hub_oidc_login(State(hub.clone())).await;
        let cookies = response
            .headers()
            .get_all(axum::http::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect::<Vec<_>>()
            .join("; ");
        let state = cookies
            .split("; ")
            .find_map(|cookie| cookie.strip_prefix("arkflow_oidc_state="))
            .unwrap()
            .to_string();

        let mut callback_headers = axum::http::HeaderMap::new();
        callback_headers.insert(
            axum::http::header::COOKIE,
            "arkflow_oidc_state=tampered".parse().unwrap(),
        );
        let mut query = StdMap::new();
        query.insert("code".to_string(), "auth-code".to_string());
        query.insert("state".to_string(), state);
        let response = crate::hub_oidc_callback(
            State(hub.clone()),
            axum::extract::Query(query),
            callback_headers,
        )
        .await;
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert!(
            federation.resolve_session("anything").is_none(),
            "no session may exist after a rejected login"
        );
    }

    #[test]
    fn federation_disabled_without_client_config() {
        let settings = OidcSettings {
            issuer: ISSUER.to_string(),
            audience: AUDIENCE.to_string(),
            ..OidcSettings::default()
        };
        // No client credentials: bearer-only federation, login disabled.
        let federation = OidcFederation::from_settings_blocking(settings).unwrap();
        assert!(!federation.login_enabled());
    }

    #[test]
    fn federation_disabled_without_issuer() {
        let settings = OidcSettings {
            issuer: String::new(),
            ..OidcSettings::default()
        };
        assert!(OidcFederation::from_settings_blocking(settings).is_none());
    }

    #[test]
    fn custom_claim_names_are_honored() {
        let auth = OidcAuthenticator::new(
            ISSUER.to_string(),
            AUDIENCE.to_string(),
            "http://unused/jwks".to_string(),
            "groups".to_string(),
            "permissions".to_string(),
        );
        let claims = json!({
            "sub": "u10",
            "groups": ["viewer"],
            "permissions": ["node=node-a"],
            "iss": ISSUER,
            "aud": AUDIENCE,
        });
        let role = role_from_claims(&claims, &auth.role_claim).unwrap();
        assert_eq!(role, OperatorRole::Viewer);
        let scopes = scopes_from_claims(&claims, &auth.scopes_claim);
        assert_eq!(scopes[0].resource_type, "node");
    }

    #[tokio::test]
    async fn missing_sub_or_kid_reject() {
        let mock = MockJwks::spawn(jwks_body(&[TEST_KID]));
        let auth = authenticator(format!("http://{}/jwks", mock.addr));

        let mut no_sub = claims("", json!(["viewer"]), 600);
        no_sub["sub"] = json!(null);
        let token = mint(no_sub, Some(TEST_KID));
        assert!(auth.authenticate(&token).await.is_none());

        let token = mint(claims("u9", json!(["viewer"]), 600), None);
        assert!(auth.authenticate(&token).await.is_none());
    }
}
