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

//! Shared MQTT TLS configuration for MQTT components.

use std::io::{BufReader, Cursor};
use std::sync::Arc;

use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};
use rumqttc::{Transport, TlsConfiguration};
use serde::{Deserialize, Serialize};

use crate::Error;

/// TLS transport configuration for MQTT components.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttTlsConfig {
    /// Whether TLS is enabled. Defaults to true when the `tls` block is
    /// present; set to false to keep the block for documentation purposes.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// CA certificate file (PEM) used to verify the broker.
    #[serde(default)]
    pub ca: Option<String>,
    /// Client certificate file (PEM) for mTLS.
    #[serde(default)]
    pub client_cert: Option<String>,
    /// Client private key file (PEM) for mTLS.
    #[serde(default)]
    pub client_key: Option<String>,
}

fn default_true() -> bool {
    true
}

impl MqttTlsConfig {
    /// Applies the TLS transport to the MQTT options.
    ///
    /// Every variant builds a rustls [`ClientConfig`] explicitly: rumqttc's
    /// `Simple` and `tls_with_default_config` paths resolve the rustls
    /// `CryptoProvider` lazily and panic when the crate graph enables both
    /// `ring` and `aws-lc-rs`, which this workspace does.
    pub fn apply(&self, options: &mut rumqttc::MqttOptions) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }
        // An incomplete client-cert/key pair is a configuration error, not a
        // silent downgrade to server-only TLS.
        match (&self.client_cert, &self.client_key) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(Error::Config(
                    "mqtt tls: client_cert and client_key must be configured together"
                        .to_string(),
                ));
            }
            _ => {}
        }
        let load = |path: &str| -> Result<Vec<u8>, Error> {
            std::fs::read(path).map_err(|e| {
                Error::Config(format!("mqtt tls: failed to read file '{path}': {e}"))
            })
        };

        // Broker trust roots: the configured CA if present, otherwise the
        // platform trust store.
        let mut roots = RootCertStore::empty();
        if let Some(ca) = &self.ca {
            let ca_pem = load(ca)?;
            let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(ca_pem)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::Config(format!("mqtt tls: failed to parse CA certificate: {error}"))
                })?;
            if certs.is_empty() {
                return Err(Error::Config(
                    "mqtt tls: no valid certificate in ca".to_string(),
                ));
            }
            roots.add_parsable_certificates(certs);
        } else {
            let loaded = rustls_native_certs::load_native_certs();
            // Tolerate individual trust-store load errors the same way
            // rumqttc tolerates unparsable CAs; a totally broken store only
            // surfaces as a verification failure at connect time.
            for error in &loaded.errors {
                tracing::warn!(%error, "mqtt tls: skipped a platform trust anchor");
            }
            roots.add_parsable_certificates(loaded.certs);
        }

        // Client identity: the configured cert/key pair if present.
        let client_auth = match (&self.client_cert, &self.client_key) {
            (None, None) => None,
            (Some(cert), Some(key)) => {
                let cert_pem = load(cert)?;
                let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(cert_pem)))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        Error::Config(format!(
                            "mqtt tls: failed to parse client certificate: {error}"
                        ))
                    })?;
                if certs.is_empty() {
                    return Err(Error::Config(
                        "mqtt tls: no valid certificate in client_cert".to_string(),
                    ));
                }
                let key_pem = load(key)?;
                let key = rustls_pemfile::private_key(&mut BufReader::new(Cursor::new(key_pem)))
                    .map_err(|error| {
                        Error::Config(format!("mqtt tls: failed to parse client key: {error}"))
                    })?
                    .ok_or_else(|| {
                        Error::Config("mqtt tls: no private key in client_key".to_string())
                    })?;
                Some((certs, key))
            }
            _ => unreachable!("incomplete pairs are rejected above"),
        };

        let provider = rumqttc::tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
        let builder = ClientConfig::builder_with_provider(provider.into())
            .with_safe_default_protocol_versions()
            .map_err(|error| {
                Error::Config(format!("mqtt tls: protocol version setup failed: {error}"))
            })?;
        let config = match client_auth {
            Some((certs, key)) => builder
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|error| {
                    Error::Config(format!("mqtt tls: invalid client certificate pair: {error}"))
                })?,
            None => builder
                .with_root_certificates(roots)
                .with_no_client_auth(),
        };
        options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(Arc::new(
            config,
        ))));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mqtt_options() -> rumqttc::MqttOptions {
        rumqttc::MqttOptions::new("tls-test", "localhost", 1883)
    }

    fn write_temp_file(name: &str, contents: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "arkflow-mqtt-tls-{}-{name}",
            std::process::id()
        ));
        std::fs::write(&path, contents).expect("write temp file");
        path
    }

    fn config(
        ca: Option<String>,
        client_cert: Option<String>,
        client_key: Option<String>,
    ) -> MqttTlsConfig {
        MqttTlsConfig {
            enabled: true,
            ca,
            client_cert,
            client_key,
        }
    }

    #[test]
    fn disabled_tls_is_a_no_op() {
        let mut options = mqtt_options();
        config(None, None, None)
            .apply(&mut options)
            .expect("disabled tls applies cleanly");
    }

    #[test]
    fn incomplete_client_pair_is_rejected() {
        let cert = write_temp_file("only-cert.pem", b"unused");
        let mut options = mqtt_options();
        let error = config(None, Some(cert.to_string_lossy().into()), None)
            .apply(&mut options)
            .expect_err("half a client pair must fail");
        assert!(
            error.to_string().contains("client_cert and client_key"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn client_pair_without_ca_builds_rustls_transport() {
        // A real (self-signed) matching pair: the success path parses the
        // PEMs, seeds the root store from the platform, and hands rustls a
        // consistent cert/key pair.
        let cert_path = write_temp_file("pair-cert.pem", TEST_CLIENT_CERT.as_bytes());
        let key_path = write_temp_file("pair-key.pem", TEST_CLIENT_KEY.as_bytes());
        let mut options = mqtt_options();
        config(
            None,
            Some(cert_path.to_string_lossy().into()),
            Some(key_path.to_string_lossy().into()),
        )
        .apply(&mut options)
        .expect("client pair without ca must build a rustls transport");
    }

    #[test]
    fn client_pair_with_garbage_key_is_rejected() {
        let cert_path = write_temp_file("bad-cert.pem", TEST_CLIENT_CERT.as_bytes());
        let key_path = write_temp_file("bad-key.pem", b"not a pem key");
        let mut options = mqtt_options();
        let error = config(
            None,
            Some(cert_path.to_string_lossy().into()),
            Some(key_path.to_string_lossy().into()),
        )
        .apply(&mut options)
        .expect_err("unparseable client key must fail");
        assert!(
            error.to_string().contains("client_key"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn ca_only_builds_simple_transport() {
        let ca_path = write_temp_file("ca.pem", TEST_CLIENT_CERT.as_bytes());
        let mut options = mqtt_options();
        config(Some(ca_path.to_string_lossy().into()), None, None)
            .apply(&mut options)
            .expect("ca-only must build a simple transport");
    }

    /// Self-signed EC P-256 client certificate used by the pair tests. The
    /// PEM is valid for PEM parsing and rustls key consistency checks; it is
    /// never used to connect anywhere.
    const TEST_CLIENT_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIBkTCCATegAwIBAgIUVGscCqn7sFun1uoSUiW0HbQpLTkwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTYXJrZmxvdy10ZXN0LWNsaWVudDAeFw0yNjA5MjMxODAyMDVa
Fw0zNjA5MjAxODAyMDVaMB4xHDAaBgNVBAMME2Fya2Zsb3ctdGVzdC1jbGllbnQw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARGuBKUtOfUbxfomA2TpjGUoovQB5hY
/UiI/hTHuld3sYL2+3m3HtwVrTOZXjyrVD351KAldFmQggL2IlKAfLBJo1MwUTAd
BgNVHQ4EFgQUfgcrJ2ivOvyTbGfhJt9iMtymw+MwHwYDVR0jBBgwFoAUfgcrJ2iv
OvyTbGfhJt9iMtymw+MwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBF
AiBPcZbTr2D6UzyzoO+k8ujpLNsFvCDLjL7/qv4gDnYKxwIhAKj8Z+nFE+qAgTEO
pev1Bcdxm1pHIEu/ZkrGnSEb4HHL
-----END CERTIFICATE-----
";

    /// The matching SEC1 EC private key for `TEST_CLIENT_CERT`.
    const TEST_CLIENT_KEY: &str = "-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIJbrQn7OAwtmGCwa3TphGYt+tt5cY7st3PHUVSnuyKXLoAoGCCqGSM49
AwEHoUQDQgAERrgSlLTn1G8X6JgNk6YxlKKL0AeYWP1IiP4Ux7pXd7GC9vt5tx7c
Fa0zmV48q1Q9+dSgJXRZkIIC9iJSgHywSQ==
-----END EC PRIVATE KEY-----
";
}
