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

//! Pulsar component tests
//!
//! Unit tests for Pulsar input and output components

mod pulsar_tests {
    use arkflow_core::{input::InputBuilder, output::OutputBuilder};
    use arkflow_plugin::input::pulsar::PulsarInputConfig;
    use arkflow_plugin::output::pulsar::PulsarOutputConfig;
    use arkflow_plugin::pulsar::{
        PulsarAuth, PulsarConfigValidator, RetryConfig, SubscriptionType,
    };

    #[test]
    fn test_validate_service_url_valid() {
        // Valid URLs
        assert!(PulsarConfigValidator::validate_service_url("pulsar://localhost:6650").is_ok());
        assert!(
            PulsarConfigValidator::validate_service_url("pulsar+ssl://example.com:6651").is_ok()
        );
    }

    #[test]
    fn test_validate_service_url_invalid() {
        // Invalid URLs
        assert!(PulsarConfigValidator::validate_service_url("").is_err());
        assert!(PulsarConfigValidator::validate_service_url("http://localhost:6650").is_err());
        assert!(PulsarConfigValidator::validate_service_url("localhost:6650").is_err());
        assert!(PulsarConfigValidator::validate_service_url("pulsar://").is_err());
    }

    #[test]
    fn test_validate_topic_valid() {
        // Valid topics
        assert!(
            PulsarConfigValidator::validate_topic("persistent://tenant/namespace/topic").is_ok()
        );
        assert!(
            PulsarConfigValidator::validate_topic("non-persistent://tenant/namespace/topic")
                .is_ok()
        );
        assert!(PulsarConfigValidator::validate_topic("my-topic").is_ok());
        assert!(PulsarConfigValidator::validate_topic("topic/subtopic").is_ok());
        assert!(PulsarConfigValidator::validate_topic("a".repeat(255).as_str()).is_ok());
    }

    #[test]
    fn test_validate_topic_invalid() {
        // Invalid topics
        assert!(PulsarConfigValidator::validate_topic("").is_err());
        assert!(PulsarConfigValidator::validate_topic("topic//subtopic").is_err());
        assert!(PulsarConfigValidator::validate_topic("/topic").is_err());
        assert!(PulsarConfigValidator::validate_topic("topic/").is_err());
        assert!(PulsarConfigValidator::validate_topic("a".repeat(256).as_str()).is_err());
    }

    #[test]
    fn test_validate_subscription_name_valid() {
        // Valid subscription names
        assert!(PulsarConfigValidator::validate_subscription_name("my-subscription").is_ok());
        assert!(PulsarConfigValidator::validate_subscription_name("my_subscription").is_ok());
        assert!(PulsarConfigValidator::validate_subscription_name("my-subscription-123").is_ok());
        assert!(PulsarConfigValidator::validate_subscription_name("subscription.1").is_ok());
    }

    #[test]
    fn test_validate_subscription_name_invalid() {
        // Invalid subscription names
        assert!(PulsarConfigValidator::validate_subscription_name("").is_err());
        assert!(PulsarConfigValidator::validate_subscription_name("my@subscription").is_err());
        assert!(PulsarConfigValidator::validate_subscription_name("subscription#1").is_err());
    }

    #[test]
    fn test_validate_retry_config_valid() {
        // Valid retry configurations
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_ok());
    }

    #[test]
    fn test_validate_retry_config_invalid() {
        // Invalid retry configurations
        let config = RetryConfig {
            max_attempts: 0,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());

        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 0,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());

        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 100,
            max_delay_ms: 50,
            backoff_multiplier: 2.0,
        };
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());

        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 0.5,
        };
        assert!(PulsarConfigValidator::validate_retry_config(&config).is_err());
    }

    #[test]
    fn test_validate_auth_config_token_valid() {
        // Valid token authentication
        let auth = PulsarAuth::Token {
            token: "valid-token-123".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_ok());
    }

    #[test]
    fn test_validate_auth_config_token_invalid() {
        // Invalid token authentication
        let auth = PulsarAuth::Token {
            token: "".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());

        let auth = PulsarAuth::Token {
            token: "a".repeat(4097).to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());
    }

    #[test]
    fn test_validate_auth_config_oauth2_valid() {
        // Valid OAuth2 authentication
        let auth = PulsarAuth::OAuth2 {
            issuer_url: "https://accounts.google.com".to_string(),
            credentials_url: "https://oauth2.googleapis.com/token".to_string(),
            audience: "my-audience".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_ok());
    }

    #[test]
    fn test_validate_auth_config_oauth2_invalid() {
        // Invalid OAuth2 authentication
        let auth = PulsarAuth::OAuth2 {
            issuer_url: "".to_string(),
            credentials_url: "https://oauth2.googleapis.com/token".to_string(),
            audience: "my-audience".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());

        let auth = PulsarAuth::OAuth2 {
            issuer_url: "invalid-url".to_string(),
            credentials_url: "https://oauth2.googleapis.com/token".to_string(),
            audience: "my-audience".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());

        let auth = PulsarAuth::OAuth2 {
            issuer_url: "https://accounts.google.com".to_string(),
            credentials_url: "".to_string(),
            audience: "my-audience".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());

        let auth = PulsarAuth::OAuth2 {
            issuer_url: "https://accounts.google.com".to_string(),
            credentials_url: "https://oauth2.googleapis.com/token".to_string(),
            audience: "".to_string(),
        };
        assert!(PulsarConfigValidator::validate_auth_config(&auth).is_err());
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 5000);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_subscription_type_default() {
        let default_type = SubscriptionType::default();
        assert!(matches!(default_type, SubscriptionType::Exclusive));
    }

    #[test]
    fn test_pulsar_input_config_deserialization() {
        let config_json = serde_json::json!({
            "service_url": "pulsar://localhost:6650",
            "topic": "test-topic",
            "subscription_name": "test-subscription",
            "subscription_type": "shared",
            "auth": {
                "type": "token",
                "token": "test-token"
            },
            "retry_config": {
                "max_attempts": 5,
                "initial_delay_ms": 200,
                "max_delay_ms": 10000,
                "backoff_multiplier": 1.5
            }
        });

        let config: PulsarInputConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.service_url, "pulsar://localhost:6650");
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.subscription_name, "test-subscription");
        assert!(matches!(
            config.subscription_type.unwrap(),
            SubscriptionType::Shared
        ));
        assert!(config.auth.is_some());
        assert!(config.retry_config.is_some());
    }

    #[test]
    fn test_pulsar_output_config_deserialization() {
        let config_json = serde_json::json!({
            "service_url": "pulsar://localhost:6650",
            "topic": {
                "type": "value",
                "value": "test-topic"
            },
            "auth": {
                "type": "o_auth2",
                "issuer_url": "https://accounts.google.com",
                "credentials_url": "https://oauth2.googleapis.com/token",
                "audience": "test-audience"
            },
            "value_field": "data"
        });

        let config: PulsarOutputConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.service_url, "pulsar://localhost:6650");
        assert_eq!(config.value_field, Some("data".to_string()));
        assert!(config.auth.is_some());
    }

    #[test]
    fn test_pulsar_input_builder_with_valid_config() {
        let config_json = serde_json::json!({
            "service_url": "pulsar://localhost:6650",
            "topic": "test-topic",
            "subscription_name": "test-subscription"
        });

        let builder = arkflow_plugin::input::pulsar::PulsarInputBuilder;
        let result = builder.build(
            Some(&"test-input".to_string()),
            &Some(config_json),
            None,
            &arkflow_core::Resource {
                temporary: std::collections::HashMap::new(),
                input_names: std::cell::RefCell::new(Vec::new()),
            },
        );

        // Should succeed with valid configuration
        assert!(result.is_ok());
    }

    #[test]
    fn test_pulsar_input_builder_with_invalid_config() {
        let config_json = serde_json::json!({
            "service_url": "invalid-url",
            "topic": "",
            "subscription_name": "test-subscription"
        });

        let builder = arkflow_plugin::input::pulsar::PulsarInputBuilder;
        let result = builder.build(
            Some(&"test-input".to_string()),
            &Some(config_json),
            None,
            &arkflow_core::Resource {
                temporary: std::collections::HashMap::new(),
                input_names: std::cell::RefCell::new(Vec::new()),
            },
        );

        // Should fail with invalid configuration
        assert!(result.is_err());
    }

    #[test]
    fn test_pulsar_output_builder_with_valid_config() {
        let config_json = serde_json::json!({
            "service_url": "pulsar://localhost:6650",
            "topic": {
                "type": "value",
                "value": "test-topic"
            }
        });

        let builder = arkflow_plugin::output::pulsar::PulsarOutputBuilder;
        let result = builder.build(
            Some(&"test-output".to_string()),
            &Some(config_json),
            &arkflow_core::Resource {
                temporary: std::collections::HashMap::new(),
                input_names: std::cell::RefCell::new(Vec::new()),
            },
        );

        // Should succeed with valid configuration
        assert!(result.is_ok());
    }

    #[test]
    fn test_pulsar_output_builder_with_invalid_config() {
        let config_json = serde_json::json!({
            "service_url": "invalid-url",
            "topic": "test-topic"
        });

        let builder = arkflow_plugin::output::pulsar::PulsarOutputBuilder;
        let result = builder.build(
            Some(&"test-output".to_string()),
            &Some(config_json),
            &arkflow_core::Resource {
                temporary: std::collections::HashMap::new(),
                input_names: std::cell::RefCell::new(Vec::new()),
            },
        );

        // Should fail with invalid configuration
        assert!(result.is_err());
    }
}
