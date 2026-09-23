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
        let transport = match (&self.ca, (&self.client_cert, &self.client_key)) {
            (Some(ca), (Some(cert), Some(key))) => {
                let ca = std::fs::read(ca)
                    .map_err(|e| Error::Config(format!("mqtt tls: failed to read CA file: {e}")))?;
                let cert = std::fs::read(cert)
                    .map_err(|e| Error::Config(format!("mqtt tls: failed to read client cert: {e}")))?;
                let key = std::fs::read(key)
                    .map_err(|e| Error::Config(format!("mqtt tls: failed to read client key: {e}")))?;
                Transport::tls_with_config(TlsConfiguration::Simple {
                    ca,
                    alpn: None,
                    client_auth: Some((cert, key)),
                })
            }
            (Some(ca), _) => {
                let ca = std::fs::read(ca)
                    .map_err(|e| Error::Config(format!("mqtt tls: failed to read CA file: {e}")))?;
                Transport::tls_with_config(TlsConfiguration::Simple {
                    ca,
                    alpn: None,
                    client_auth: None,
                })
            }
            _ => Transport::tls_with_default_config(),
        };
        options.set_transport(transport);
        Ok(())
    }
}
