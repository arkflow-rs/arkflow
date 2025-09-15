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
use std::time::Duration;

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
pub enum SubscriptionType {
    Exclusive,
    Shared,
    Failover,
    KeyShared,
}

impl Default for SubscriptionType {
    fn default() -> Self {
        SubscriptionType::Exclusive
    }
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

/// Retry configuration for Pulsar operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay between retries (in milliseconds)
    pub initial_delay_ms: u64,
    /// Maximum delay between retries (in milliseconds)
    pub max_delay_ms: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Retry utility for Pulsar operations
pub struct RetryUtils;

impl RetryUtils {
    /// Execute an operation with retry logic
    pub async fn retry_with_backoff<F, Fut, T, E>(
        operation: F,
        config: &RetryConfig,
        operation_name: &str,
    ) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut attempt = 0;
        let mut delay = Duration::from_millis(config.initial_delay_ms);

        loop {
            attempt += 1;

            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt >= config.max_attempts {
                        tracing::error!(
                            "Failed {} after {} attempts: {}",
                            operation_name,
                            attempt,
                            e
                        );
                        return Err(e);
                    }

                    tracing::warn!(
                        "Attempt {} for {} failed, retrying in {:?}: {}",
                        attempt,
                        operation_name,
                        delay,
                        e
                    );

                    tokio::time::sleep(delay).await;

                    // Exponential backoff with cap
                    delay = Duration::from_millis(
                        (delay.as_millis() as f64 * config.backoff_multiplier)
                            .min(config.max_delay_ms as f64) as u64,
                    );
                }
            }
        }
    }
}

/// Configuration validation utilities
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

        // Check if URL has more than just the prefix
        if url.len() <= "pulsar://".len()
            || (url.starts_with("pulsar+ssl://") && url.len() <= "pulsar+ssl://".len())
        {
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
