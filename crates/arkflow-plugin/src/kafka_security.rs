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

//! Shared Kafka security configuration (SASL authentication + TLS).
//!
//! One config shape and one assembly path for the Kafka input and the Kafka
//! output: `apply()` translates the `security` block into librdkafka client
//! properties. All checks run offline — no broker round-trip — so the
//! builders can fail fast on inconsistent configuration and `--validate`
//! catches it before any stream starts.

use serde::{Deserialize, Serialize};

use arkflow_core::Error;
use rdkafka::config::ClientConfig;

/// Every PEM block opens with this marker, and a valid filesystem path cannot
/// start with it — so it safely distinguishes inline PEM text from a path.
const PEM_BEGIN_PREFIX: &str = "-----BEGIN";

/// TLS transport security protocol (`security.protocol`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityProtocol {
    #[serde(rename = "plaintext")]
    Plaintext,
    #[serde(rename = "ssl")]
    Ssl,
    #[serde(rename = "sasl_plaintext")]
    SaslPlaintext,
    #[serde(rename = "sasl_ssl")]
    SaslSsl,
}

impl SecurityProtocol {
    fn as_librdkafka_value(&self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::Ssl => "ssl",
            SecurityProtocol::SaslPlaintext => "sasl_plaintext",
            SecurityProtocol::SaslSsl => "sasl_ssl",
        }
    }
}

/// SASL authentication mechanism (`sasl.mechanisms`). All mechanisms
/// currently supported are username/password based.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "plain")]
    Plain,
    #[serde(rename = "scram-sha-256")]
    ScramSha256,
    #[serde(rename = "scram-sha-512")]
    ScramSha512,
}

impl SaslMechanism {
    fn as_librdkafka_value(&self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// SASL credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// TLS settings. `ca` / `cert` / `key` each accept a file path or inline PEM
/// text (auto-detected via the `-----BEGIN` marker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub ca: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
    pub key_password: Option<String>,
    pub insecure_skip_verify: Option<bool>,
}

/// The `security` block shared by the Kafka input and output configs.
///
/// The effective protocol is the explicitly declared `protocol`, or — when
/// omitted — inferred from which sub-blocks are present: both → `sasl_ssl`,
/// only `sasl` → `sasl_plaintext`, only `tls` → `ssl`, neither → `plaintext`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSecurityConfig {
    pub protocol: Option<SecurityProtocol>,
    pub sasl: Option<SaslConfig>,
    pub tls: Option<TlsConfig>,
}

impl KafkaSecurityConfig {
    /// Resolve the effective `security.protocol`. An explicitly declared
    /// protocol wins over inference, but a provided `sasl`/`tls` block that
    /// the protocol cannot use is a configuration mistake and errors here
    /// instead of being silently ignored.
    pub fn resolved_protocol(&self) -> Result<SecurityProtocol, Error> {
        let Some(protocol) = self.protocol else {
            return Ok(match (&self.sasl, &self.tls) {
                (Some(_), Some(_)) => SecurityProtocol::SaslSsl,
                (Some(_), None) => SecurityProtocol::SaslPlaintext,
                (None, Some(_)) => SecurityProtocol::Ssl,
                (None, None) => SecurityProtocol::Plaintext,
            });
        };

        if self.sasl.is_some()
            && !matches!(
                protocol,
                SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
            )
        {
            return Err(Error::Config(format!(
                "Kafka security: a sasl block requires a sasl_* protocol, but protocol is explicitly declared as '{}'",
                protocol.as_librdkafka_value()
            )));
        }
        if self.tls.is_some()
            && !matches!(protocol, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl)
        {
            return Err(Error::Config(format!(
                "Kafka security: a tls block requires the ssl or sasl_ssl protocol, but protocol is explicitly declared as '{}'",
                protocol.as_librdkafka_value()
            )));
        }
        Ok(protocol)
    }

    /// Check the configuration is self-consistent without connecting to a
    /// broker. Called by the component builders so `--validate` catches bad
    /// security configuration before any stream starts.
    pub fn validate(&self) -> Result<(), Error> {
        let protocol = self.resolved_protocol()?;
        if matches!(
            protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        ) {
            let sasl = self.sasl.as_ref().ok_or_else(|| {
                Error::Config(
                    "Kafka security: a sasl_* protocol requires a security.sasl block".to_string(),
                )
            })?;
            if sasl
                .username
                .as_deref()
                .is_none_or(|v| v.trim().is_empty())
            {
                return Err(Error::Config(format!(
                    "Kafka security: sasl mechanism {} requires a non-empty security.sasl.username",
                    sasl.mechanism.as_librdkafka_value()
                )));
            }
            if sasl
                .password
                .as_deref()
                .is_none_or(|v| v.trim().is_empty())
            {
                return Err(Error::Config(format!(
                    "Kafka security: sasl mechanism {} requires a non-empty security.sasl.password",
                    sasl.mechanism.as_librdkafka_value()
                )));
            }
        }
        Ok(())
    }

    /// Set every security property on the client config. The caller decides
    /// whether to call this at all: with no `security` block the client
    /// config carries no security properties at all (plaintext, as before).
    pub fn apply(&self, client_config: &mut ClientConfig) -> Result<(), Error> {
        let protocol = self.resolved_protocol()?;
        self.validate()?;

        client_config.set("security.protocol", protocol.as_librdkafka_value());

        if let Some(sasl) = &self.sasl {
            client_config.set("sasl.mechanisms", sasl.mechanism.as_librdkafka_value());
            if let Some(username) = sasl.username.as_deref() {
                client_config.set("sasl.username", username);
            }
            if let Some(password) = sasl.password.as_deref() {
                client_config.set("sasl.password", password);
            }
        }

        if let Some(tls) = &self.tls {
            set_certificate(
                client_config,
                "ssl.ca.location",
                "ssl.ca.pem",
                tls.ca.as_deref(),
            );
            set_certificate(
                client_config,
                "ssl.certificate.location",
                "ssl.certificate.pem",
                tls.cert.as_deref(),
            );
            set_certificate(
                client_config,
                "ssl.key.location",
                "ssl.key.pem",
                tls.key.as_deref(),
            );
            if let Some(key_password) = tls.key_password.as_deref() {
                client_config.set("ssl.key.password", key_password);
            }
            if tls.insecure_skip_verify.unwrap_or(false) {
                tracing::warn!(
                    "Kafka TLS certificate verification is DISABLED (security.tls.insecure_skip_verify); use only in development or testing"
                );
                client_config.set("enable.ssl.certificate.verification", "false");
            }
        }

        Ok(())
    }
}

/// Set one TLS certificate property from a value that is a file path or
/// inline PEM text.
fn set_certificate(
    client_config: &mut ClientConfig,
    location_key: &str,
    pem_key: &str,
    value: Option<&str>,
) {
    let Some(value) = value else {
        return;
    };
    if value.trim_start().starts_with(PEM_BEGIN_PREFIX) {
        client_config.set(pem_key, value);
    } else {
        client_config.set(location_key, value);
    }
}

/// The `security` JSON-schema fragment shared verbatim by the Kafka input
/// and output component metadata, so the two registries cannot drift apart.
pub fn json_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "SASL authentication and TLS settings. Omit entirely for plaintext.",
        "properties": {
            "protocol": {"type": "string", "enum": ["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"], "description": "Security protocol. Defaults to inferred from which sub-blocks are present: both -> sasl_ssl, only sasl -> sasl_plaintext, only tls -> ssl, neither -> plaintext."},
            "sasl": {
                "type": "object",
                "description": "SASL credentials (PLAIN or SCRAM).",
                "properties": {
                    "mechanism": {"type": "string", "enum": ["plain", "scram-sha-256", "scram-sha-512"], "description": "SASL mechanism."},
                    "username": {"type": "string", "description": "SASL username (required for plain/scram)."},
                    "password": {"type": "string", "description": "SASL password (required for plain/scram)."}
                },
                "required": ["mechanism"]
            },
            "tls": {
                "type": "object",
                "description": "TLS settings. ca/cert/key accept a file path or inline PEM text (auto-detected).",
                "properties": {
                    "ca": {"type": "string", "description": "CA certificate for verifying the broker: file path or inline PEM."},
                    "cert": {"type": "string", "description": "Client certificate for mTLS: file path or inline PEM."},
                    "key": {"type": "string", "description": "Client private key for mTLS: file path or inline PEM."},
                    "key_password": {"type": "string", "description": "Password protecting the client private key."},
                    "insecure_skip_verify": {"type": "boolean", "default": false, "description": "Disable broker certificate verification. Development/testing only."}
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn security(json: serde_json::Value) -> KafkaSecurityConfig {
        serde_json::from_value(json).expect("valid security config")
    }

    fn applied(json: serde_json::Value) -> (KafkaSecurityConfig, ClientConfig) {
        let config = security(json);
        let mut client_config = ClientConfig::new();
        config.apply(&mut client_config).expect("apply succeeds");
        (config, client_config)
    }

    // --- protocol inference (spec: 安全协议选择与推断) ---

    #[test]
    fn infers_protocol_from_block_presence() {
        let cases = [
            (serde_json::json!({}), SecurityProtocol::Plaintext),
            (
                serde_json::json!({"sasl": {"mechanism": "plain", "username": "u", "password": "p"}}),
                SecurityProtocol::SaslPlaintext,
            ),
            (
                serde_json::json!({"tls": {"ca": "/etc/ca.crt"}}),
                SecurityProtocol::Ssl,
            ),
            (
                serde_json::json!({
                    "sasl": {"mechanism": "scram-sha-256", "username": "u", "password": "p"},
                    "tls": {"ca": "/etc/ca.crt"}
                }),
                SecurityProtocol::SaslSsl,
            ),
        ];
        for (json, expected) in cases {
            assert_eq!(
                security(json).resolved_protocol().unwrap(),
                expected,
                "inferred protocol mismatch"
            );
        }
    }

    #[test]
    fn explicit_protocol_wins_over_inference() {
        // sasl_ssl declared while only a sasl block is present: legal, the
        // missing tls block must not force a fallback or an error.
        let config = security(serde_json::json!({
            "protocol": "sasl_ssl",
            "sasl": {"mechanism": "scram-sha-512", "username": "u", "password": "p"}
        }));
        assert_eq!(
            config.resolved_protocol().unwrap(),
            SecurityProtocol::SaslSsl
        );
    }

    #[test]
    fn contradictions_between_explicit_protocol_and_blocks_error() {
        let cases = [
            serde_json::json!({
                "protocol": "plaintext",
                "sasl": {"mechanism": "plain", "username": "u", "password": "p"}
            }),
            serde_json::json!({"protocol": "plaintext", "tls": {"ca": "/ca.crt"}}),
            serde_json::json!({
                "protocol": "ssl",
                "sasl": {"mechanism": "plain", "username": "u", "password": "p"}
            }),
            serde_json::json!({"protocol": "sasl_plaintext", "tls": {"ca": "/ca.crt"}}),
        ];
        for json in cases {
            let err = security(json).resolved_protocol().unwrap_err();
            assert!(
                err.to_string().contains("protocol is explicitly declared"),
                "expected a contradiction error, got: {err}"
            );
        }
    }

    // --- SASL property assembly (spec: SASL 认证属性装配) ---

    #[test]
    fn assembles_scram_sha_256_credentials() {
        let (_, client_config) = applied(serde_json::json!({
            "sasl": {"mechanism": "scram-sha-256", "username": "u", "password": "p"}
        }));
        assert_eq!(client_config.get("security.protocol"), Some("sasl_plaintext"));
        assert_eq!(client_config.get("sasl.mechanisms"), Some("SCRAM-SHA-256"));
        assert_eq!(client_config.get("sasl.username"), Some("u"));
        assert_eq!(client_config.get("sasl.password"), Some("p"));
    }

    #[test]
    fn assembles_plain_mechanism_under_sasl_ssl() {
        let (_, client_config) = applied(serde_json::json!({
            "protocol": "sasl_ssl",
            "sasl": {"mechanism": "plain", "username": "u", "password": "p"},
            "tls": {"ca": "/etc/ca.crt"}
        }));
        assert_eq!(client_config.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(client_config.get("sasl.mechanisms"), Some("PLAIN"));
    }

    // --- TLS property assembly (spec: TLS 证书装配) ---

    #[test]
    fn inline_pem_goes_to_pem_properties_and_paths_to_locations() {
        let ca_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----";
        let (_, client_config) = applied(serde_json::json!({
            "tls": {
                "ca": ca_pem,
                "cert": "/etc/certs/client.crt",
                "key": "/etc/certs/client.key",
                "key_password": "secret",
                "insecure_skip_verify": true
            }
        }));
        assert_eq!(client_config.get("security.protocol"), Some("ssl"));
        assert_eq!(client_config.get("ssl.ca.pem"), Some(ca_pem));
        assert_eq!(client_config.get("ssl.ca.location"), None);
        assert_eq!(
            client_config.get("ssl.certificate.location"),
            Some("/etc/certs/client.crt")
        );
        assert_eq!(
            client_config.get("ssl.key.location"),
            Some("/etc/certs/client.key")
        );
        assert_eq!(client_config.get("ssl.key.password"), Some("secret"));
        assert_eq!(
            client_config.get("enable.ssl.certificate.verification"),
            Some("false")
        );
    }

    // --- validation (spec: 构建期校验与错误语义) ---

    #[test]
    fn sasl_protocol_without_sasl_block_errors() {
        let err = security(serde_json::json!({"protocol": "sasl_ssl"}))
            .validate()
            .unwrap_err();
        assert!(
            err.to_string().contains("security.sasl"),
            "expected the error to name security.sasl, got: {err}"
        );
    }

    #[test]
    fn scram_without_password_errors() {
        let err = security(serde_json::json!({
            "sasl": {"mechanism": "scram-sha-512", "username": "u"}
        }))
        .validate()
        .unwrap_err();
        assert!(
            err.to_string().contains("security.sasl.password"),
            "expected the error to name security.sasl.password, got: {err}"
        );
    }

    #[test]
    fn scram_with_empty_username_errors() {
        let err = security(serde_json::json!({
            "sasl": {"mechanism": "plain", "username": "  ", "password": "p"}
        }))
        .validate()
        .unwrap_err();
        assert!(
            err.to_string().contains("security.sasl.username"),
            "expected the error to name security.sasl.username, got: {err}"
        );
    }

    /// Unknown mechanisms are rejected at deserialization time and never
    /// reach the builder (spec scenario: 未知机制被拒绝).
    #[test]
    fn unknown_mechanism_is_rejected_by_serde() {
        let result = serde_json::from_value::<KafkaSecurityConfig>(serde_json::json!({
            "sasl": {"mechanism": "gssapi"}
        }));
        assert!(result.is_err(), "gssapi must not deserialize");
    }
}
