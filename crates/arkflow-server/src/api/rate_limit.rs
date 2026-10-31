//! Per-credential request rate limiting for both control planes
//! (`control-plane-rate-limiting` capability).
//!
//! One in-process token bucket per credential identity: static/structured
//! operator credentials are keyed per credential, OIDC browser sessions per
//! session, agent requests per node credential (Bearer header and the legacy
//! query-token channel resolve to the same bucket), and unauthenticated local
//! mode shares one anonymous bucket. Keys are truncated SHA-256 digests — no
//! credential material is ever stored. The bucket table is capacity-bounded;
//! unrecognized identities beyond the bound share one catch-all bucket so a
//! forged-credential flood degrades to a slow shared lane instead of growing
//! memory. Idle buckets are reclaimed lazily on the request path.

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Bucket-table bound (fleet cap 256 × 4 headroom). Legitimate deployments
/// hold one bucket per credential and per node, plus browser sessions.
pub const MAX_BUCKETS: usize = 1024;

/// Which control plane a limiter guards; indexes the rejection counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plane {
    Local,
    Hub,
}

/// Rejection-counter key kind: which bucket class absorbed the rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Principal,
    Node,
    CatchAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Key([u8; 16]);

impl Key {
    fn anonymous() -> Self {
        Self([1u8; 16])
    }

    fn catch_all() -> Self {
        // No SHA-256 digest prefixes to all zeros, so this constant can never
        // collide with a hashed credential.
        Self([0u8; 16])
    }
}

fn credential_key(space: &str, credential: &str) -> Key {
    let mut hasher = Sha256::new();
    hasher.update(space.as_bytes());
    hasher.update([0]);
    hasher.update(credential.as_bytes());
    let digest = hasher.finalize();
    let mut key = [0u8; 16];
    key.copy_from_slice(&digest[..16]);
    Key(key)
}

#[derive(Debug, Clone, Copy)]
struct ClassLimit {
    rps: f64,
    burst: f64,
}

impl ClassLimit {
    /// The idle span after which a bucket is fully refilled (×2 headroom);
    /// the lazy reclamation window under capacity pressure.
    fn idle_window(&self) -> Duration {
        Duration::from_secs_f64(2.0 * self.burst / self.rps)
    }
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Rejection counters shared by both planes (static precedent:
/// `storage::STORAGE_QUEUE_DEPTH`); rendered by `metrics.rs`.
static REJECTIONS: [[AtomicU64; 3]; 2] = [
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
];

pub fn rejection_total(plane: Plane, kind: KeyKind) -> u64 {
    REJECTIONS[plane as usize][kind as usize].load(Ordering::Relaxed)
}

/// Test-only hook so rendering tests can produce rejections without standing
/// up a server (`#[cfg(test)]` keeps it out of the public API surface).
#[cfg(test)]
pub(crate) fn record_rejection_for_tests(plane: Plane, kind: KeyKind) {
    record_rejection(plane, kind);
}

fn record_rejection(plane: Plane, kind: KeyKind) {
    REJECTIONS[plane as usize][kind as usize].fetch_add(1, Ordering::Relaxed);
}

pub struct RateLimiter {
    plane: Plane,
    enabled: bool,
    principal: ClassLimit,
    node: ClassLimit,
    buckets: Mutex<HashMap<Key, Bucket>>,
}

impl RateLimiter {
    /// Build a limiter. Non-finite or non-positive rates are clamped to the
    /// defaults with a startup warning: a misconfigured limit must fail
    /// closed (protective) rather than divide by zero or lock everything out.
    pub fn new(plane: Plane, config: &arkflow_core::config::RateLimitConfig) -> Arc<Self> {
        let defaults = arkflow_core::config::RateLimitConfig::default();
        let clamp = |value: f64, fallback: f64, name: &str| {
            if value.is_finite() && value > 0.0 {
                value
            } else {
                tracing::warn!(
                    plane = ?plane,
                    value,
                    name,
                    "rate_limit value must be finite and positive; falling back to the default"
                );
                fallback
            }
        };
        Arc::new(Self {
            plane,
            enabled: config.enabled,
            principal: ClassLimit {
                rps: clamp(
                    config.principal_rps,
                    defaults.principal_rps,
                    "principal_rps",
                ),
                burst: clamp(
                    config.principal_burst,
                    defaults.principal_burst,
                    "principal_burst",
                ),
            },
            node: ClassLimit {
                rps: clamp(config.node_rps, defaults.node_rps, "node_rps"),
                burst: clamp(config.node_burst, defaults.node_burst, "node_burst"),
            },
            buckets: Mutex::new(HashMap::new()),
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Admit one request presented under `space`-prefixed credential
    /// `credential` (None = anonymous). Returns the refill wait on rejection.
    fn admit(
        &self,
        space: &str,
        credential: Option<&str>,
        mut limit: ClassLimit,
        mut kind: KeyKind,
        now: Instant,
    ) -> Result<(), Duration> {
        if !self.enabled {
            return Ok(());
        }
        let key = match credential {
            Some(credential) if !credential.is_empty() => credential_key(space, credential),
            _ => Key::anonymous(),
        };
        {
            let mut buckets = self.buckets.lock().expect("rate limiter mutex");
            if !buckets.contains_key(&key) && buckets.len() >= MAX_BUCKETS {
                let window = limit.idle_window();
                buckets.retain(|_, bucket| now.duration_since(bucket.last_refill) < window);
                if buckets.len() >= MAX_BUCKETS && key != Key::catch_all() {
                    // Table still saturated after reclamation: unidentified
                    // traffic shares one slow lane instead of growing memory.
                    kind = KeyKind::CatchAll;
                    limit = self.principal;
                    let bucket = buckets.entry(Key::catch_all()).or_insert_with(|| Bucket {
                        tokens: limit.burst,
                        last_refill: now,
                    });
                    return Self::consume(bucket, limit, self.plane, kind, now);
                }
            }
            let bucket = buckets.entry(key).or_insert_with(|| Bucket {
                tokens: limit.burst,
                last_refill: now,
            });
            Self::consume(bucket, limit, self.plane, kind, now)
        }
    }

    fn consume(
        bucket: &mut Bucket,
        limit: ClassLimit,
        plane: Plane,
        kind: KeyKind,
        now: Instant,
    ) -> Result<(), Duration> {
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * limit.rps).min(limit.burst);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let wait = Duration::from_secs_f64((1.0 - bucket.tokens) / limit.rps);
            record_rejection(plane, kind);
            Err(wait)
        }
    }
}

/// Inner-router paths exempt from limiting, mirroring the operator-auth
/// exemption: the OIDC login flow must stay reachable under credential
/// floods. Health probes and metrics are outer-router paths and never reach
/// this middleware.
const RATE_LIMIT_EXEMPT: &[&str] = &["/auth/oidc/"];

/// Agent requests present the node credential via the Bearer header; the
/// legacy query-parameter channel is accepted for older Agents and must draw
/// from the same bucket.
fn agent_credential(request: &axum::extract::Request) -> Option<String> {
    let headers = request.headers();
    if let Some(token) = super::agent_endpoints::bearer_session_token(headers) {
        return Some(token);
    }
    let query = request.uri().query()?;
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix("session_token="))
        .filter(|token| !token.is_empty())
        .map(percent_decode)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 3 <= bytes.len() => {
                let hex = bytes.get(index + 1..index + 3).unwrap_or_default();
                if let Ok(byte) = u8::from_str_radix(std::str::from_utf8(hex).unwrap_or(""), 16) {
                    out.push(byte);
                    index += 3;
                    continue;
                }
                out.push(b'%');
                index += 1;
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub async fn rate_limit_middleware(
    State(limiter): State<Arc<RateLimiter>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !limiter.enabled() {
        return next.run(request).await;
    }
    let path = request.uri().path();
    if RATE_LIMIT_EXEMPT
        .iter()
        .any(|exempt| path.starts_with(exempt))
    {
        return next.run(request).await;
    }
    let now = Instant::now();
    let outcome = if path.starts_with("/agent/") {
        let credential = agent_credential(&request);
        limiter.admit(
            "node",
            credential.as_deref(),
            limiter.node,
            KeyKind::Node,
            now,
        )
    } else {
        let credential = super::bearer(request.headers());
        limiter.admit(
            "operator",
            credential.as_deref(),
            limiter.principal,
            KeyKind::Principal,
            now,
        )
    };
    match outcome {
        Ok(()) => next.run(request).await,
        Err(wait) => too_many_requests(wait),
    }
}

fn too_many_requests(wait: Duration) -> Response {
    let seconds = (wait.as_secs_f64().ceil() as u64).max(1);
    let mut response = super::problem(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "Control-plane request rate limit exceeded; retry after the indicated interval".into(),
    );
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, seconds.into());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> arkflow_core::config::RateLimitConfig {
        arkflow_core::config::RateLimitConfig {
            enabled: true,
            principal_rps: 10.0,
            principal_burst: 3.0,
            node_rps: 10.0,
            node_burst: 3.0,
        }
    }

    fn limiter() -> RateLimiter {
        let limiter = RateLimiter::new(Plane::Local, &config());
        Arc::try_unwrap(limiter).ok().expect("sole owner")
    }

    fn base() -> Instant {
        Instant::now()
    }

    #[test]
    fn burst_within_capacity_is_admitted() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            assert!(limiter
                .admit(
                    "operator",
                    Some("a"),
                    limiter.principal,
                    KeyKind::Principal,
                    now
                )
                .is_ok());
        }
        assert!(limiter
            .admit(
                "operator",
                Some("a"),
                limiter.principal,
                KeyKind::Principal,
                now
            )
            .is_err());
    }

    #[test]
    fn refill_restores_tokens_over_time() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit(
                "operator",
                Some("a"),
                limiter.principal,
                KeyKind::Principal,
                now,
            );
        }
        assert!(limiter
            .admit(
                "operator",
                Some("a"),
                limiter.principal,
                KeyKind::Principal,
                now
            )
            .is_err());
        // 10 rps refills one token in 100ms.
        let later = now + Duration::from_millis(100);
        assert!(limiter
            .admit(
                "operator",
                Some("a"),
                limiter.principal,
                KeyKind::Principal,
                later
            )
            .is_ok());
    }

    #[test]
    fn credentials_are_isolated() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit(
                "operator",
                Some("a"),
                limiter.principal,
                KeyKind::Principal,
                now,
            );
        }
        assert!(limiter
            .admit(
                "operator",
                Some("a"),
                limiter.principal,
                KeyKind::Principal,
                now
            )
            .is_err());
        assert!(limiter
            .admit(
                "operator",
                Some("b"),
                limiter.principal,
                KeyKind::Principal,
                now
            )
            .is_ok());
    }

    #[test]
    fn agent_channels_share_one_bucket() {
        let limiter = limiter();
        let now = base();
        // The middleware delivers the same token string from either channel,
        // so the limiter keys both on the token alone.
        for _ in 0..3 {
            let _ = limiter.admit("node", Some("token-1"), limiter.node, KeyKind::Node, now);
        }
        assert!(limiter
            .admit("node", Some("token-1"), limiter.node, KeyKind::Node, now)
            .is_err());
        assert!(limiter
            .admit("node", Some("token-2"), limiter.node, KeyKind::Node, now)
            .is_ok());
        let buckets = limiter.buckets.lock().unwrap();
        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key(&credential_key("node", "token-1")));
    }

    #[test]
    fn anonymous_requests_share_one_bucket() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit("operator", None, limiter.principal, KeyKind::Principal, now);
        }
        assert!(limiter
            .admit("operator", None, limiter.principal, KeyKind::Principal, now)
            .is_err());
        assert!(limiter
            .admit(
                "operator",
                Some(""),
                limiter.principal,
                KeyKind::Principal,
                now
            )
            .is_err());
    }

    #[test]
    fn saturated_table_falls_back_to_catch_all() {
        let limiter = limiter();
        let now = base();
        for index in 0..MAX_BUCKETS {
            let _ = limiter.admit(
                "operator",
                Some(format!("credential-{index}").as_str()),
                limiter.principal,
                KeyKind::Principal,
                now,
            );
        }
        let before = rejection_total(Plane::Local, KeyKind::CatchAll);
        // A fresh identity must be served from the catch-all bucket and
        // counted as such; the catch-all has the same burst, so exhaust it.
        for _ in 0..4 {
            let _ = limiter.admit(
                "operator",
                Some("stranger"),
                limiter.principal,
                KeyKind::Principal,
                now,
            );
        }
        assert!(rejection_total(Plane::Local, KeyKind::CatchAll) > before);
        // All strangers share the one catch-all bucket.
        assert!(limiter
            .admit(
                "operator",
                Some("other-stranger"),
                limiter.principal,
                KeyKind::Principal,
                now
            )
            .is_err());
        let buckets = limiter.buckets.lock().unwrap();
        assert_eq!(buckets.len(), MAX_BUCKETS + 1);
    }

    #[test]
    fn idle_buckets_are_reclaimed_under_pressure() {
        let limiter = limiter();
        let start = base();
        for index in 0..MAX_BUCKETS {
            let _ = limiter.admit(
                "operator",
                Some(format!("credential-{index}").as_str()),
                limiter.principal,
                KeyKind::Principal,
                start,
            );
        }
        // The idle window at these defaults is 2 × 3/10 = 0.6s.
        let later = start + Duration::from_secs(1);
        let _ = limiter.admit(
            "operator",
            Some("fresh"),
            limiter.principal,
            KeyKind::Principal,
            later,
        );
        let buckets = limiter.buckets.lock().unwrap();
        assert!(buckets.len() < MAX_BUCKETS);
        assert!(buckets.contains_key(&credential_key("operator", "fresh")));
    }

    #[test]
    fn disabled_limiter_admits_everything() {
        let mut config = config();
        config.enabled = false;
        let limiter = RateLimiter::new(Plane::Local, &config);
        let now = base();
        for _ in 0..100 {
            assert!(limiter
                .admit(
                    "operator",
                    Some("a"),
                    limiter.principal,
                    KeyKind::Principal,
                    now
                )
                .is_ok());
        }
    }

    #[test]
    fn invalid_config_values_clamp_to_defaults() {
        // Clamping falls back to the production defaults, not the values the
        // misconfigured deployment happened to supply.
        let defaults = arkflow_core::config::RateLimitConfig::default();
        let mut config = config();
        config.principal_rps = 0.0;
        config.node_burst = f64::NAN;
        let limiter = RateLimiter::new(Plane::Local, &config);
        assert_eq!(limiter.principal.rps, defaults.principal_rps);
        assert_eq!(limiter.node.burst, defaults.node_burst);
    }

    #[test]
    fn rejections_are_counted_per_plane_and_kind() {
        let limiter = RateLimiter::new(Plane::Hub, &config());
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit("node", Some("n"), limiter.node, KeyKind::Node, now);
        }
        let before = rejection_total(Plane::Hub, KeyKind::Node);
        assert!(limiter
            .admit("node", Some("n"), limiter.node, KeyKind::Node, now)
            .is_err());
        assert_eq!(rejection_total(Plane::Hub, KeyKind::Node), before + 1);
    }

    #[test]
    fn percent_decode_matches_urlencoded_semantics() {
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("a+b"), "a b");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%2"), "%2");
    }

    /// Middleware-level checks: 429 shape, exemption boundary, and the
    /// default limits admitting legitimate cadences through a real router.
    mod middleware_tests {
        use super::*;
        use crate::api::ServerConfig;
        use axum::body::Body;
        use axum::response::IntoResponse;
        use axum::Router;
        use tower::ServiceExt;

        async fn marker() -> impl IntoResponse {
            (StatusCode::OK, "handler-ran")
        }

        fn test_app(config: &arkflow_core::config::RateLimitConfig) -> Router {
            let limiter = RateLimiter::new(Plane::Local, config);
            Router::new()
                .fallback(marker)
                .layer(axum::middleware::from_fn_with_state(
                    limiter,
                    rate_limit_middleware,
                ))
        }

        async fn send(app: Router, path: &str, bearer: Option<&str>) -> Response {
            let mut builder = axum::http::Request::builder().uri(path);
            if let Some(bearer) = bearer {
                builder = builder.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
            }
            app.oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap()
        }

        async fn body_text(response: Response) -> String {
            let bytes = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            String::from_utf8_lossy(&bytes).into_owned()
        }

        fn tiny_config() -> arkflow_core::config::RateLimitConfig {
            arkflow_core::config::RateLimitConfig {
                enabled: true,
                principal_rps: 10.0,
                principal_burst: 1.0,
                node_rps: 10.0,
                node_burst: 1.0,
            }
        }

        #[tokio::test]
        async fn over_limit_request_is_rejected_with_retry_after() {
            let app = test_app(&tiny_config());
            let first = send(app.clone(), "/status", Some("cred")).await;
            assert_eq!(first.status(), StatusCode::OK);
            assert_eq!(body_text(first).await, "handler-ran");

            let second = send(app, "/status", Some("cred")).await;
            assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(second.headers().get(header::RETRY_AFTER).unwrap(), "1");
            let text = body_text(second).await;
            assert!(text.contains("rate_limited"), "problem body: {text}");
            assert!(
                !text.contains("handler-ran"),
                "handler must not run: {text}"
            );
        }

        #[tokio::test]
        async fn exempt_oidc_paths_bypass_the_limiter() {
            let app = test_app(&tiny_config());
            for _ in 0..5 {
                let response = send(app.clone(), "/auth/oidc/login", Some("cred")).await;
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(body_text(response).await, "handler-ran");
            }
        }

        #[tokio::test]
        async fn default_node_limits_admit_the_agent_polling_cadence() {
            // Defaults (burst 1000) must absorb a sustained agent sequence —
            // register/heartbeat/report/commands/observations at the default
            // 1s poll interval are three orders of magnitude below the cap.
            let app = test_app(&arkflow_core::config::RateLimitConfig::default());
            for _ in 0..500 {
                let response = send(app.clone(), "/agent/heartbeat", Some("node-token")).await;
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(body_text(response).await, "handler-ran");
            }
        }

        #[test]
        fn config_without_rate_limit_section_parses_unchanged() {
            let config: ServerConfig =
                serde_yaml::from_str("enabled: true\naddress: \"127.0.0.1:8080\"\n").unwrap();
            assert_eq!(
                config.rate_limit,
                arkflow_core::config::RateLimitConfig::default()
            );

            let explicit: ServerConfig = serde_yaml::from_str(
                "rate_limit:\n  enabled: false\n  principal_rps: 5\n  principal_burst: 10\n  node_rps: 50\n  node_burst: 60\n",
            )
            .unwrap();
            assert!(!explicit.rate_limit.enabled);
            assert_eq!(explicit.rate_limit.principal_rps, 5.0);
            assert_eq!(explicit.rate_limit.node_burst, 60.0);
        }
    }
}
