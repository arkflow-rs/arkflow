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

/// One bounded bucket table per key space. Agent routes are unauthenticated,
/// so forging millions of node credentials must not be able to saturate the
/// table that operator credentials live in: each space gets its own bound and
/// its own catch-all lane.
struct KeySpace {
    limit: ClassLimit,
    kind: KeyKind,
    buckets: Mutex<HashMap<Key, Bucket>>,
}

pub struct RateLimiter {
    plane: Plane,
    enabled: bool,
    operator: KeySpace,
    node: KeySpace,
}

impl RateLimiter {
    /// Build a limiter. Non-finite or non-positive rates are clamped to the
    /// defaults with a startup warning: a misconfigured limit must fail
    /// closed (protective) rather than divide by zero or lock everything out.
    /// A burst below one token can never admit a request (`consume` needs
    /// `tokens >= 1.0`), so bursts clamp up to 1.0 instead.
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
        let clamp_burst =
            |value: f64, fallback: f64, name: &str| clamp(value, fallback, name).max(1.0);
        Arc::new(Self {
            plane,
            enabled: config.enabled,
            operator: KeySpace {
                limit: ClassLimit {
                    rps: clamp(
                        config.principal_rps,
                        defaults.principal_rps,
                        "principal_rps",
                    ),
                    burst: clamp_burst(
                        config.principal_burst,
                        defaults.principal_burst,
                        "principal_burst",
                    ),
                },
                kind: KeyKind::Principal,
                buckets: Mutex::new(HashMap::new()),
            },
            node: KeySpace {
                limit: ClassLimit {
                    rps: clamp(config.node_rps, defaults.node_rps, "node_rps"),
                    burst: clamp_burst(config.node_burst, defaults.node_burst, "node_burst"),
                },
                kind: KeyKind::Node,
                buckets: Mutex::new(HashMap::new()),
            },
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Admit one request against a key space under `space`-prefixed
    /// `credential` (None = anonymous). Returns the refill wait on rejection.
    fn admit(
        &self,
        space: &str,
        credential: Option<&str>,
        keyspace: &KeySpace,
        now: Instant,
    ) -> Result<(), Duration> {
        if !self.enabled {
            return Ok(());
        }
        let key = match credential {
            Some(credential) if !credential.is_empty() => credential_key(space, credential),
            _ => Key::anonymous(),
        };
        let mut kind = keyspace.kind;
        let mut buckets = keyspace.buckets.lock().expect("rate limiter mutex");
        if !buckets.contains_key(&key) && buckets.len() >= MAX_BUCKETS {
            let window = keyspace.limit.idle_window();
            buckets.retain(|_, bucket| now.duration_since(bucket.last_refill) < window);
            if buckets.len() >= MAX_BUCKETS && key != Key::catch_all() {
                // Table still saturated after reclamation: unidentified
                // traffic shares one slow lane instead of growing memory.
                kind = KeyKind::CatchAll;
                let bucket = buckets.entry(Key::catch_all()).or_insert_with(|| Bucket {
                    tokens: keyspace.limit.burst,
                    last_refill: now,
                });
                return Self::consume(bucket, keyspace.limit, self.plane, kind, now);
            }
        }
        let bucket = buckets.entry(key).or_insert_with(|| Bucket {
            tokens: keyspace.limit.burst,
            last_refill: now,
        });
        Self::consume(bucket, keyspace.limit, self.plane, kind, now)
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

/// Inner-router paths exempt from limiting. The OIDC login flow must stay
/// reachable under credential floods (prefix match, like the auth exemption);
/// `/metrics` is the metrics export registered inside both versioned routers
/// — matched exactly so no future route sharing the prefix inherits the
/// exemption. Outer-router health probes never reach this middleware.
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
    if path == "/metrics"
        || RATE_LIMIT_EXEMPT
            .iter()
            .any(|exempt| path.starts_with(exempt))
    {
        return next.run(request).await;
    }
    let now = Instant::now();
    let outcome = if path.starts_with("/agent/") {
        let credential = agent_credential(&request);
        limiter.admit("node", credential.as_deref(), &limiter.node, now)
    } else {
        let credential = super::bearer(request.headers());
        limiter.admit("operator", credential.as_deref(), &limiter.operator, now)
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
                .admit("operator", Some("a"), &limiter.operator, now)
                .is_ok());
        }
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, now)
            .is_err());
    }

    #[test]
    fn refill_restores_tokens_over_time() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit("operator", Some("a"), &limiter.operator, now);
        }
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, now)
            .is_err());
        // 10 rps refills one token in 100ms.
        let later = now + Duration::from_millis(100);
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, later)
            .is_ok());
    }

    #[test]
    fn credentials_are_isolated() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit("operator", Some("a"), &limiter.operator, now);
        }
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, now)
            .is_err());
        assert!(limiter
            .admit("operator", Some("b"), &limiter.operator, now)
            .is_ok());
    }

    #[test]
    fn agent_channels_share_one_bucket() {
        let limiter = limiter();
        let now = base();
        // The middleware delivers the same token string from either channel,
        // so the limiter keys both on the token alone.
        for _ in 0..3 {
            let _ = limiter.admit("node", Some("token-1"), &limiter.node, now);
        }
        assert!(limiter
            .admit("node", Some("token-1"), &limiter.node, now)
            .is_err());
        assert!(limiter
            .admit("node", Some("token-2"), &limiter.node, now)
            .is_ok());
        let buckets = limiter.node.buckets.lock().unwrap();
        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key(&credential_key("node", "token-1")));
    }

    #[test]
    fn anonymous_requests_share_one_bucket() {
        let limiter = limiter();
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit("operator", None, &limiter.operator, now);
        }
        assert!(limiter
            .admit("operator", None, &limiter.operator, now)
            .is_err());
        assert!(limiter
            .admit("operator", Some(""), &limiter.operator, now)
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
                &limiter.operator,
                now,
            );
        }
        let before = rejection_total(Plane::Local, KeyKind::CatchAll);
        // A fresh identity must be served from the catch-all bucket and
        // counted as such; the catch-all has the same burst, so exhaust it.
        for _ in 0..4 {
            let _ = limiter.admit("operator", Some("stranger"), &limiter.operator, now);
        }
        assert!(rejection_total(Plane::Local, KeyKind::CatchAll) > before);
        // All strangers share the one catch-all bucket.
        assert!(limiter
            .admit("operator", Some("other-stranger"), &limiter.operator, now)
            .is_err());
        let buckets = limiter.operator.buckets.lock().unwrap();
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
                &limiter.operator,
                start,
            );
        }
        // The idle window at these defaults is 2 × 3/10 = 0.6s.
        let later = start + Duration::from_secs(1);
        let _ = limiter.admit("operator", Some("fresh"), &limiter.operator, later);
        let buckets = limiter.operator.buckets.lock().unwrap();
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
                .admit("operator", Some("a"), &limiter.operator, now)
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
        assert_eq!(limiter.operator.limit.rps, defaults.principal_rps);
        assert_eq!(limiter.node.limit.burst, defaults.node_burst);
    }

    #[test]
    fn burst_below_one_token_clamps_up() {
        let mut config = config();
        config.principal_burst = 0.5;
        let limiter = RateLimiter::new(Plane::Local, &config);
        assert_eq!(limiter.operator.limit.burst, 1.0);
        let now = base();
        // Exactly one request fits; the bucket is not a permanent lockout.
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, now)
            .is_ok());
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, now)
            .is_err());
        let later = now + Duration::from_millis(100);
        assert!(limiter
            .admit("operator", Some("a"), &limiter.operator, later)
            .is_ok());
    }

    #[test]
    fn saturated_node_table_does_not_evict_operators() {
        // Agent routes are unauthenticated: a forged-credential flood must
        // stay contained in the node key space and never push operator
        // credentials into the shared catch-all lane.
        let limiter = limiter();
        let now = base();
        assert!(limiter
            .admit("operator", Some("ops"), &limiter.operator, now)
            .is_ok());
        for index in 0..MAX_BUCKETS {
            let _ = limiter.admit(
                "node",
                Some(format!("forged-{index}").as_str()),
                &limiter.node,
                now,
            );
        }
        // Node space is saturated; fresh node identities hit its catch-all.
        let before = rejection_total(Plane::Local, KeyKind::CatchAll);
        for _ in 0..4 {
            let _ = limiter.admit("node", Some("more-forged"), &limiter.node, now);
        }
        assert!(rejection_total(Plane::Local, KeyKind::CatchAll) > before);
        // The operator space is untouched: a fresh operator credential gets
        // its own bucket, not the catch-all lane.
        assert!(limiter
            .admit("operator", Some("fresh-ops"), &limiter.operator, now)
            .is_ok());
        let operators = limiter.operator.buckets.lock().unwrap();
        assert!(operators.contains_key(&credential_key("operator", "fresh-ops")));
        assert!(!operators.contains_key(&Key::catch_all()));
    }

    #[test]
    fn rejections_are_counted_per_plane_and_kind() {
        let limiter = RateLimiter::new(Plane::Hub, &config());
        let now = base();
        for _ in 0..3 {
            let _ = limiter.admit("node", Some("n"), &limiter.node, now);
        }
        let before = rejection_total(Plane::Hub, KeyKind::Node);
        assert!(limiter
            .admit("node", Some("n"), &limiter.node, now)
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
        async fn metrics_export_is_exempt_from_limiting() {
            let app = test_app(&tiny_config());
            // Exhaust the principal bucket first: /metrics must stay
            // reachable regardless.
            let first = send(app.clone(), "/status", Some("cred")).await;
            assert_eq!(first.status(), StatusCode::OK);
            let second = send(app.clone(), "/status", Some("cred")).await;
            assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
            let metrics = send(app, "/metrics", Some("cred")).await;
            assert_eq!(metrics.status(), StatusCode::OK);
            assert_eq!(body_text(metrics).await, "handler-ran");
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
