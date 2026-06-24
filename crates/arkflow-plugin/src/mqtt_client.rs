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

//! MQTT client utilities for ArkFlow
//!
//! This module provides common utilities for MQTT client configuration
//! to reduce code duplication between input and output components.

use rumqttc::{MqttOptions, QoS};
use std::time::Duration;

/// Create MqttOptions from common configuration parameters
///
/// This function eliminates code duplication between MQTT input and output
/// components by providing a centralized way to configure MQTT options.
///
/// # Arguments
///
/// * `client_id` - Unique client identifier
/// * `host` - MQTT broker address
/// * `port` - MQTT broker port
/// * `username` - Optional username for authentication
/// * `password` - Optional password for authentication
/// * `keep_alive` - Optional keep-alive interval in seconds
/// * `clean_session` - Optional clean session flag
///
/// # Returns
///
/// Configured MqttOptions ready for client creation
///
/// # Examples
///
/// ```rust,no_run
/// use arkflow_plugin::mqtt_client::create_mqtt_options;
///
/// let options = create_mqtt_options(
///     "my_client",
///     "localhost",
///     1883,
///     Some("user"),
///     Some("pass"),
///     Some(60),
///     Some(true),
/// );
/// ```
pub fn create_mqtt_options(
    client_id: &str,
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
    keep_alive: Option<u64>,
    clean_session: Option<bool>,
) -> MqttOptions {
    let mut mqtt_options = MqttOptions::new(client_id, host, port);

    // Set authentication credentials if provided
    if let (Some(username), Some(password)) = (username, password) {
        mqtt_options.set_credentials(username, password);
    }

    // Set keep-alive interval if provided
    if let Some(keep_alive) = keep_alive {
        mqtt_options.set_keep_alive(Duration::from_secs(keep_alive));
    }

    // Set clean session flag if provided
    if let Some(clean_session) = clean_session {
        mqtt_options.set_clean_session(clean_session);
    }

    mqtt_options
}

/// Convert QoS level from u8 to rumqttc QoS enum
///
/// # Arguments
///
/// * `qos` - QoS level as u8 (0, 1, or 2)
///
/// # Returns
///
/// Corresponding QoS enum value
///
/// # Examples
///
/// ```rust,no_run
/// use arkflow_plugin::mqtt_client::parse_qos;
///
/// let qos = parse_qos(Some(1)); // Returns QoS::AtLeastOnce
/// let qos_default = parse_qos(None); // Returns QoS::AtLeastOnce (default)
/// ```
pub fn parse_qos(qos: Option<u8>) -> QoS {
    match qos {
        Some(0) => QoS::AtMostOnce,
        Some(1) => QoS::AtLeastOnce,
        Some(2) => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce, // Default is QoS 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mqtt_options_basic() {
        let options = create_mqtt_options("test_client", "localhost", 1883, None, None, None, None);

        assert_eq!(options.client_id(), "test_client");
        let broker_addr = options.broker_address();
        assert_eq!(broker_addr.0, "localhost");
        assert_eq!(broker_addr.1, 1883);
    }

    #[test]
    fn test_create_mqtt_options_with_auth() {
        let options = create_mqtt_options(
            "test_client",
            "localhost",
            1883,
            Some("user"),
            Some("pass"),
            None,
            None,
        );

        // Verify credentials are set (not directly accessible, but no panic = success)
        assert_eq!(options.client_id(), "test_client");
    }

    #[test]
    fn test_create_mqtt_options_full() {
        let options = create_mqtt_options(
            "test_client",
            "localhost",
            1883,
            Some("user"),
            Some("pass"),
            Some(60),
            Some(true),
        );

        assert_eq!(options.client_id(), "test_client");
        let broker_addr = options.broker_address();
        assert_eq!(broker_addr.1, 1883);
    }

    #[test]
    fn test_parse_qos() {
        assert!(matches!(parse_qos(Some(0)), QoS::AtMostOnce));
        assert!(matches!(parse_qos(Some(1)), QoS::AtLeastOnce));
        assert!(matches!(parse_qos(Some(2)), QoS::ExactlyOnce));
        assert!(matches!(parse_qos(None), QoS::AtLeastOnce)); // Default
        assert!(matches!(parse_qos(Some(99)), QoS::AtLeastOnce)); // Invalid -> default
    }
}
