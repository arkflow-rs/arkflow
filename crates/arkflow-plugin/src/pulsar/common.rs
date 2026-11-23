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

//! Common Pulsar utilities and types
//!
//! Shared functionality used by both input and output Pulsar components

use arkflow_core::Error;
use pulsar::executor::TokioExecutor;
use pulsar::{Authentication, Pulsar};
use serde::{Deserialize, Serialize};

/// Pulsar authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PulsarAuth {
    /// Token authentication
    Token { token: String },
    /// OAuth2 authentication
    OAuth2 {
        issuer_url: String,
        credentials_url: String,
        audience: String,
    },
}

/// Pulsar subscription type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SubscriptionType {
    #[default]
    Exclusive,
    Shared,
    Failover,
    KeyShared,
}

/// Common Pulsar client utilities
pub struct PulsarClientUtils;

impl PulsarClientUtils {
    /// Create a Pulsar client builder with authentication
    pub fn create_client_builder(
        service_url: &str,
        auth: &Option<PulsarAuth>,
    ) -> Result<pulsar::PulsarBuilder<TokioExecutor>, Error> {
        let mut builder = Pulsar::builder(service_url, TokioExecutor);

        // Configure authentication if provided
        if let Some(auth_config) = auth {
            let auth_method = match auth_config {
                PulsarAuth::Token { token } => Authentication {
                    name: "token".to_string(),
                    data: token.clone().into_bytes(),
                },
                PulsarAuth::OAuth2 {
                    issuer_url,
                    credentials_url,
                    audience,
                } => {
                    let oauth2_data = serde_json::json!({
                        "type": "client_credentials",
                        "issuer_url": issuer_url,
                        "credentials_url": credentials_url,
                        "audience": audience,
                    });
                    Authentication {
                        name: "oauth2".to_string(),
                        data: oauth2_data.to_string().into_bytes(),
                    }
                }
            };
            builder = builder.with_auth(auth_method);
        }

        Ok(builder)
    }
}

// RetryConfig/RetryUtils moved to `crate::retry` (consolidate-connector-
// skeleton): one shared implementation for every retrying component.
pub use crate::retry::{RetryConfig, RetryUtils};

/// Configuration validation utilities
/// TLS configuration for Pulsar connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarTlsConfig {
    /// Certificate chain file (PEM) the broker certificate is verified
    /// against: the trust anchor of a private CA. The driver adds it to
    /// its trust store; it does not present a client identity.
    #[serde(default)]
    pub certificate_chain_file: Option<String>,
    /// Whether to enable hostname verification (defaults to true)
    #[serde(default = "default_hostname_verification")]
    pub hostname_verification: Option<bool>,
}

fn default_hostname_verification() -> Option<bool> {
    Some(true)
}

/// Rejects `tls.ca_file`, removed from the configuration surface: the pulsar
/// driver has no separate `ca_file` setting — a private-CA trust anchor is
/// provided via `tls.certificate_chain_file`, which the driver installs to
/// authenticate the server, so `ca_file` never took effect. serde silently drops
/// unknown keys, so removing the field alone would keep carrying the key
/// invisibly — failing loudly keeps the removal honest (spec:
/// component-config-honesty, 从配置面移除的键 SHALL 在存量配置中显式拒绝).
pub(crate) fn reject_removed_ca_file(config: &Option<serde_json::Value>) -> Result<(), Error> {
    if let Some(tls) = config.as_ref().and_then(|config| config.get("tls")) {
        if tls.get("ca_file").is_some() {
            return Err(Error::Config(
                "pulsar: `tls.ca_file` is not supported: the driver has no separate \
                 `ca_file` setting, so the field never took effect. A private-CA trust \
                 anchor is provided via `tls.certificate_chain_file` (the driver installs \
                 that chain to authenticate the server). Remove `tls.ca_file` from the \
                 configuration."
                    .to_string(),
            ));
        }
    }
    Ok(())
}

pub struct PulsarConfigValidator;

impl PulsarConfigValidator {
    /// Validate service URL format
    pub fn validate_service_url(url: &str) -> Result<(), Error> {
        if url.is_empty() {
            return Err(Error::Config("Service URL cannot be empty".to_string()));
        }

        if !url.starts_with("pulsar://") && !url.starts_with("pulsar+ssl://") {
            return Err(Error::Config(format!(
                "Invalid service URL format: {}. Must start with 'pulsar://' or 'pulsar+ssl://'",
                url
            )));
        }

        // Check that a host is actually present: `pulsar:///missing-host`
        // passes a pure length check but has no authority.
        let rest = url
            .strip_prefix("pulsar+ssl://")
            .or_else(|| url.strip_prefix("pulsar://"))
            .unwrap_or_default();
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        // Strip userinfo, if any: the host follows the last '@'.
        let host = authority.rsplit('@').next().unwrap_or(authority);
        if host.is_empty() || host.starts_with(':') {
            return Err(Error::Config("Service URL must include host".to_string()));
        }

        Ok(())
    }

    /// Validate topic name
    pub fn validate_topic(topic: &str) -> Result<(), Error> {
        if topic.is_empty() {
            return Err(Error::Config("Topic name cannot be empty".to_string()));
        }

        // Check for valid topic name characters
        let has_invalid_double_slash = topic.contains("//")
            && !topic.starts_with("persistent://")
            && !topic.starts_with("non-persistent://");

        if topic.contains("..")
            || has_invalid_double_slash
            || topic.starts_with('/')
            || topic.ends_with('/')
        {
            return Err(Error::Config(format!(
                "Invalid topic name: '{}'. Topic names cannot contain '..', '//' or start/end with '/'",
                topic
            )));
        }

        // Check length limit (Pulsar has a limit of 255 characters for topic names)
        if topic.len() > 255 {
            return Err(Error::Config(format!(
                "Topic name too long: {} characters (max 255)",
                topic.len()
            )));
        }

        Ok(())
    }

    /// Validate subscription name
    pub fn validate_subscription_name(subscription: &str) -> Result<(), Error> {
        if subscription.is_empty() {
            return Err(Error::Config(
                "Subscription name cannot be empty".to_string(),
            ));
        }

        // Subscription names must be alphanumeric with some special characters
        if !subscription
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return Err(Error::Config(format!(
                "Invalid subscription name: '{}'. Only alphanumeric characters, '-', '_', and '.' are allowed",
                subscription
            )));
        }

        Ok(())
    }

    /// Validate retry configuration
    pub fn validate_retry_config(config: &RetryConfig) -> Result<(), Error> {
        if config.max_attempts == 0 {
            return Err(Error::Config("Max attempts must be at least 1".to_string()));
        }

        if config.initial_delay_ms == 0 {
            return Err(Error::Config(
                "Initial delay must be greater than 0".to_string(),
            ));
        }

        if config.max_delay_ms < config.initial_delay_ms {
            return Err(Error::Config(
                "Max delay must be greater than or equal to initial delay".to_string(),
            ));
        }

        if config.backoff_multiplier <= 1.0 {
            return Err(Error::Config(
                "Backoff multiplier must be greater than 1.0".to_string(),
            ));
        }

        Ok(())
    }

    /// Validate authentication configuration
    pub fn validate_auth_config(auth: &PulsarAuth) -> Result<(), Error> {
        match auth {
            PulsarAuth::Token { token } => {
                if token.is_empty() {
                    return Err(Error::Config("Token cannot be empty".to_string()));
                }
                if token.len() > 4096 {
                    return Err(Error::Config(
                        "Token too long (max 4096 characters)".to_string(),
                    ));
                }
            }
            PulsarAuth::OAuth2 {
                issuer_url,
                credentials_url,
                audience,
            } => {
                if issuer_url.is_empty() {
                    return Err(Error::Config(
                        "OAuth2 issuer URL cannot be empty".to_string(),
                    ));
                }
                if credentials_url.is_empty() {
                    return Err(Error::Config(
                        "OAuth2 credentials URL cannot be empty".to_string(),
                    ));
                }
                if audience.is_empty() {
                    return Err(Error::Config("OAuth2 audience cannot be empty".to_string()));
                }

                // Validate URL formats
                Self::validate_url(issuer_url)?;
                Self::validate_url(credentials_url)?;
            }
        }

        Ok(())
    }

    /// Validate URL format
    fn validate_url(url: &str) -> Result<(), Error> {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(Error::Config(format!("Invalid URL format: {}", url)));
        }
        Ok(())
    }
}

/// Type aliases for commonly used Pulsar types
pub type PulsarClient = Pulsar<TokioExecutor>;
pub type PulsarConsumer<T = Vec<u8>> = pulsar::Consumer<T, TokioExecutor>;
pub type PulsarProducer = pulsar::Producer<TokioExecutor>;
pub type PulsarMessage<T = Vec<u8>> = pulsar::consumer::Message<T>;

#[cfg(test)]
mod tls_url_tests {
    use super::PulsarConfigValidator;

    #[test]
    fn pulsar_ssl_url_passes_validation() {
        assert!(PulsarConfigValidator::validate_service_url(
            "pulsar+ssl://broker.example.com:6651"
        )
        .is_ok());
    }

    #[test]
    fn plain_pulsar_url_passes_validation() {
        assert!(
            PulsarConfigValidator::validate_service_url("pulsar://broker.example.com:6650").is_ok()
        );
    }

    #[test]
    fn invalid_scheme_is_rejected() {
        assert!(
            PulsarConfigValidator::validate_service_url("http://broker.example.com:6650").is_err()
        );
    }

    #[test]
    fn ssl_prefix_without_host_rejected() {
        assert!(PulsarConfigValidator::validate_service_url("pulsar+ssl://").is_err());
    }

    #[test]
    fn empty_url_rejected() {
        assert!(PulsarConfigValidator::validate_service_url("").is_err());
    }

    #[test]
    fn plain_prefix_without_host_rejected() {
        assert!(PulsarConfigValidator::validate_service_url("pulsar://").is_err());
    }
}

#[cfg(test)]
mod topic_tests {
    use super::PulsarConfigValidator;

    #[test]
    fn valid_topics_pass() {
        for topic in [
            "my-topic",
            "persistent://tenant/namespace/topic",
            "non-persistent://tenant/namespace/topic",
            "tenant/namespace/my-topic-v2",
        ] {
            assert!(
                PulsarConfigValidator::validate_topic(topic).is_ok(),
                "'{topic}' should be valid"
            );
        }
    }

    #[test]
    fn empty_topic_rejected() {
        assert!(PulsarConfigValidator::validate_topic("").is_err());
    }

    #[test]
    fn dotdot_and_slash_topics_rejected() {
        for topic in ["a/../b", "a//b", "/leading", "trailing/"] {
            assert!(
                PulsarConfigValidator::validate_topic(topic).is_err(),
                "'{topic}' should be rejected"
            );
        }
    }

    #[test]
    fn fully_qualified_topic_may_contain_double_slash() {
        assert!(PulsarConfigValidator::validate_topic("persistent://tenant/ns/topic").is_ok());
    }

    #[test]
    fn overlong_topic_rejected() {
        let topic = "t".repeat(256);
        assert!(PulsarConfigValidator::validate_topic(&topic).is_err());
        let topic = "t".repeat(255);
        assert!(PulsarConfigValidator::validate_topic(&topic).is_ok());
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::PulsarConfigValidator;

    #[test]
    fn valid_subscription_names_pass() {
        for name in ["sub-1", "my_sub", "group.name", "Sub42"] {
            assert!(PulsarConfigValidator::validate_subscription_name(name).is_ok());
        }
    }

    #[test]
    fn empty_subscription_name_rejected() {
        assert!(PulsarConfigValidator::validate_subscription_name("").is_err());
    }

    #[test]
    fn invalid_characters_rejected() {
        for name in ["sub name", "sub/name", "sub:name", "sub#1"] {
            assert!(
                PulsarConfigValidator::validate_subscription_name(name).is_err(),
                "'{name}' should be rejected"
            );
        }
    }
}

#[cfg(test)]
mod retry_config_tests {
    use super::{PulsarConfigValidator, RetryConfig};

    fn config() -> RetryConfig {
        RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        }
    }

    #[test]
    fn default_config_is_valid() {
        assert!(PulsarConfigValidator::validate_retry_config(&RetryConfig::default()).is_ok());
        // The documented defaults.
        assert_eq!(RetryConfig::default().max_attempts, 3);
        assert_eq!(RetryConfig::default().initial_delay_ms, 100);
        assert_eq!(RetryConfig::default().max_delay_ms, 5000);
        assert_eq!(RetryConfig::default().backoff_multiplier, 2.0);
    }

    #[test]
    fn zero_attempts_rejected() {
        let mut config = config();
        config.max_attempts = 0;
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());
    }

    #[test]
    fn zero_initial_delay_rejected() {
        let mut config = config();
        config.initial_delay_ms = 0;
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());
    }

    #[test]
    fn max_delay_below_initial_rejected() {
        let mut config = config();
        config.max_delay_ms = 50;
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());
    }

    #[test]
    fn backoff_multiplier_at_or_below_one_rejected() {
        for multiplier in [1.0, 0.5] {
            let mut config = config();
            config.backoff_multiplier = multiplier;
            assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());
        }
    }
}

#[cfg(test)]
mod auth_config_tests {
    use super::{PulsarAuth, PulsarConfigValidator};

    #[test]
    fn valid_token_passes() {
        assert!(
            PulsarConfigValidator::validate_auth_config(&PulsarAuth::Token {
                token: "secret".to_string()
            })
            .is_ok()
        );
    }

    #[test]
    fn empty_token_rejected() {
        assert!(
            PulsarConfigValidator::validate_auth_config(&PulsarAuth::Token {
                token: String::new()
            })
            .is_err()
        );
    }

    #[test]
    fn overlong_token_rejected() {
        let token = "t".repeat(4097);
        assert!(PulsarConfigValidator::validate_auth_config(&PulsarAuth::Token { token }).is_err());
    }

    fn oauth2(issuer_url: &str, credentials_url: &str, audience: &str) -> PulsarAuth {
        PulsarAuth::OAuth2 {
            issuer_url: issuer_url.to_string(),
            credentials_url: credentials_url.to_string(),
            audience: audience.to_string(),
        }
    }

    #[test]
    fn valid_oauth2_passes() {
        assert!(PulsarConfigValidator::validate_auth_config(&oauth2(
            "https://issuer.example.com",
            "https://issuer.example.com/credentials",
            "urn:audience"
        ))
        .is_ok());
    }

    #[test]
    fn empty_oauth2_fields_rejected() {
        for auth in [
            oauth2("", "https://issuer.example.com/creds", "aud"),
            oauth2("https://issuer.example.com", "", "aud"),
            oauth2(
                "https://issuer.example.com",
                "https://issuer.example.com/creds",
                "",
            ),
        ] {
            assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());
        }
    }

    #[test]
    fn non_http_oauth2_urls_rejected() {
        for auth in [
            oauth2(
                "issuer.example.com",
                "https://issuer.example.com/creds",
                "aud",
            ),
            oauth2("https://issuer.example.com", "file:///tmp/creds", "aud"),
        ] {
            assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());
        }
    }

    #[test]
    fn auth_deserializes_from_snake_case_tags() {
        let token: PulsarAuth =
            serde_json::from_value(serde_json::json!({"type": "token", "token": "t"})).unwrap();
        assert!(matches!(token, PulsarAuth::Token { .. }));

        let oauth2: PulsarAuth = serde_json::from_value(serde_json::json!({
            "type": "o_auth2",
            "issuer_url": "https://issuer",
            "credentials_url": "https://issuer/creds",
            "audience": "aud"
        }))
        .unwrap();
        assert!(matches!(oauth2, PulsarAuth::OAuth2 { .. }));
    }
}

#[cfg(test)]
mod client_builder_tests {
    use super::{PulsarAuth, PulsarClientUtils};

    #[test]
    fn builder_without_auth() {
        assert!(PulsarClientUtils::create_client_builder("pulsar://127.0.0.1:6650", &None).is_ok());
    }

    #[test]
    fn builder_with_token_auth() {
        let auth = Some(PulsarAuth::Token {
            token: "t".to_string(),
        });
        assert!(PulsarClientUtils::create_client_builder("pulsar://127.0.0.1:6650", &auth).is_ok());
    }

    #[test]
    fn builder_with_oauth2_auth() {
        let auth = Some(PulsarAuth::OAuth2 {
            issuer_url: "https://issuer".to_string(),
            credentials_url: "https://issuer/creds".to_string(),
            audience: "aud".to_string(),
        });
        assert!(
            PulsarClientUtils::create_client_builder("pulsar+ssl://127.0.0.1:6651", &auth).is_ok()
        );
    }
}

#[cfg(test)]
mod retry_backoff_tests {
    use super::{RetryConfig, RetryUtils};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn fast_config(max_attempts: u32) -> RetryConfig {
        RetryConfig {
            max_attempts,
            initial_delay_ms: 1,
            max_delay_ms: 2,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        }
    }

    #[tokio::test]
    async fn succeeds_on_first_attempt_without_sleeping() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let start = std::time::Instant::now();
        let result: Result<u32, String> = RetryUtils::retry_with_backoff(
            || {
                counter.fetch_add(1, Ordering::SeqCst);
                std::future::ready::<Result<u32, String>>(Ok(7))
            },
            &fast_config(3),
            "first-try",
        )
        .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[tokio::test]
    async fn retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let result: Result<u32, String> = RetryUtils::retry_with_backoff(
            || {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                std::future::ready(if n < 2 {
                    Err("transient".to_string())
                } else {
                    Ok(42)
                })
            },
            &fast_config(5),
            "flaky",
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_last_error_after_exhausting_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let result: Result<(), String> = RetryUtils::retry_with_backoff(
            || {
                counter.fetch_add(1, Ordering::SeqCst);
                std::future::ready::<Result<(), String>>(Err("always fails".to_string()))
            },
            &fast_config(3),
            "doomed",
        )
        .await;
        assert_eq!(result.unwrap_err(), "always fails");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}

#[cfg(test)]
mod tls_config_tests {
    use super::{reject_removed_ca_file, PulsarTlsConfig};

    #[test]
    fn defaults_apply_when_fields_omitted() {
        let config: PulsarTlsConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(config.certificate_chain_file.is_none());
        assert_eq!(config.hostname_verification, Some(true));
    }

    #[test]
    fn explicit_fields_are_parsed() {
        let config: PulsarTlsConfig = serde_json::from_value(serde_json::json!({
            "certificate_chain_file": "/etc/cert.pem",
            "hostname_verification": false
        }))
        .unwrap();
        assert_eq!(
            config.certificate_chain_file.as_deref(),
            Some("/etc/cert.pem")
        );
        assert_eq!(config.hostname_verification, Some(false));
    }

    /// Regression (2026-11 review P1 #20): `tls.ca_file` was parsed, tested,
    /// and documented but never consumed — the driver has no separate
    /// `ca_file` setting (the private-CA trust anchor is
    /// `tls.certificate_chain_file`),
    /// so users believed they had a private trust anchor while connections
    /// used the system store. The key must fail the build loudly.
    #[test]
    fn removed_ca_file_key_is_rejected_loudly() {
        let err = reject_removed_ca_file(&Some(serde_json::json!({
            "service_url": "pulsar://localhost:6650",
            "tls": {"ca_file": "/etc/ca.pem"}
        })))
        .expect_err("removed key must fail the build");
        let msg = err.to_string();
        assert!(msg.contains("ca_file"), "error must name the key: {msg}");

        reject_removed_ca_file(&Some(serde_json::json!({
            "service_url": "pulsar://localhost:6650",
            "tls": {"certificate_chain_file": "/etc/cert.pem"}
        })))
        .expect("surviving sibling keys stay accepted");
        reject_removed_ca_file(&None).expect("absent config passes");
        reject_removed_ca_file(&Some(serde_json::json!({
            "service_url": "pulsar://localhost:6650"
        })))
        .expect("config without a tls block passes");
    }
}
