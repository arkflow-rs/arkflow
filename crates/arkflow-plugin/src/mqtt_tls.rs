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
        let transport = match (&self.ca, &self.client_cert, &self.client_key) {
            (Some(ca), Some(cert), Some(key)) => Transport::tls_with_config(TlsConfiguration::Simple {
                ca: load(ca)?,
                alpn: None,
                client_auth: Some((load(cert)?, load(key)?)),
            }),
            (Some(ca), None, None) => Transport::tls_with_config(TlsConfiguration::Simple {
                ca: load(ca)?,
                alpn: None,
                client_auth: None,
            }),
            (None, Some(cert), Some(key)) => Transport::tls_with_config(TlsConfiguration::Simple {
                ca: load(cert)?,
                alpn: None,
                client_auth: Some((load(cert)?, load(key)?)),
            }),
            _ => Transport::tls_with_default_config(),
        };
        options.set_transport(transport);
        Ok(())
    }
}
