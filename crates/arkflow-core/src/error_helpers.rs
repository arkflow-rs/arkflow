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

//! Error handling helper utilities for ArkFlow
//!
//! This module provides common error handling patterns and utilities
//! to reduce code duplication and improve error messages.

use crate::Error;
use serde_json::Value;

/// Parse configuration from a JSON value with proper error handling
///
/// This function replaces the common pattern of `config.clone().unwrap().serde_json::from_value()`
/// with proper error handling and contextual error messages.
///
/// # Arguments
///
/// * `config` - Optional JSON value containing the configuration
/// * `component_name` - Name of the component for error messages
///
/// # Returns
///
/// * `Ok(T)` - Successfully parsed configuration
/// * `Err(Error::Config)` - Configuration is missing or invalid
///
/// # Examples
///
/// ```rust,no_run
/// use arkflow_core::error_helpers::parse_config;
/// use serde_json::json;
///
/// let config = Some(json!({"host": "localhost", "port": 8080}));
///
/// #[derive(serde::Deserialize)]
/// struct MyConfig {
///     host: String,
///     port: u16,
/// }
///
/// let result: MyConfig = parse_config(&config, "MyComponent").unwrap();
/// assert_eq!(result.host, "localhost");
/// ```
pub fn parse_config<T>(config: &Option<Value>, component_name: &str) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    let config_value = config
        .as_ref()
        .ok_or_else(|| Error::Config(format!("{} configuration is missing", component_name)))?;

    serde_json::from_value(config_value.clone()).map_err(|e| {
        Error::Config(format!(
            "Failed to parse {} configuration: {}",
            component_name, e
        ))
    })
}

/// Macro for creating configuration errors with context
///
/// # Examples
///
/// ```rust,no_run
/// use arkflow_core::{config_error, Error};
///
/// let error: Error = config_error!("HttpInput", "invalid port number");
/// assert!(matches!(error, Error::Config(_)));
/// ```
#[macro_export]
macro_rules! config_error {
    ($component:expr, $msg:expr) => {
        $crate::Error::Config(format!("{} configuration error: {}", $component, $msg))
    };
}

/// Map an Arrow error to a process error with context
///
/// # Arguments
///
/// * `error` - The original Arrow error
/// * `operation` - Description of the operation that failed
///
/// # Returns
///
/// * `Err(Error::Process)` - Wrapped error with context
pub fn map_arrow_error<T>(error: impl std::error::Error, operation: &str) -> Result<T, Error> {
    Err(Error::Process(format!(
        "Arrow {} failed: {}",
        operation, error
    )))
}

/// Map a DataFusion error to a process error with context
///
/// # Arguments
///
/// * `error` - The original DataFusion error
/// * `operation` - Description of the operation that failed
///
/// # Returns
///
/// * `Err(Error::Process)` - Wrapped error with context
pub fn map_datafusion_error<T>(error: impl std::error::Error, operation: &str) -> Result<T, Error> {
    Err(Error::Process(format!(
        "DataFusion {} failed: {}",
        operation, error
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_config_success() {
        let config = Some(json!({"host": "localhost", "port": 8080}));

        #[derive(serde::Deserialize)]
        struct TestConfig {
            host: String,
            port: u16,
        }

        let result: TestConfig = parse_config(&config, "test").unwrap();
        assert_eq!(result.host, "localhost");
        assert_eq!(result.port, 8080);
    }

    #[test]
    fn test_parse_config_missing() {
        let config: Option<Value> = None;

        #[derive(serde::Deserialize)]
        struct TestConfig {}

        let result: Result<TestConfig, Error> = parse_config(&config, "test");
        assert!(result.is_err());

        if let Err(Error::Config(msg)) = result {
            assert!(msg.contains("test configuration is missing"));
        } else {
            panic!("Expected Config error");
        }
    }

    #[test]
    fn test_parse_config_invalid_json() {
        let config = Some(json!({"host": "localhost", "port": "invalid"}));

        #[derive(serde::Deserialize)]
        struct TestConfig {
            host: String,
            port: u16,
        }

        let result: Result<TestConfig, Error> = parse_config(&config, "test");
        assert!(result.is_err());

        if let Err(Error::Config(msg)) = result {
            assert!(msg.contains("Failed to parse test configuration"));
        } else {
            panic!("Expected Config error");
        }
    }

    #[test]
    fn test_config_error_macro() {
        let error: Error = config_error!("TestComponent", "invalid value");
        assert!(matches!(error, Error::Config(_)));

        if let Error::Config(msg) = error {
            assert!(msg.contains("TestComponent"));
            assert!(msg.contains("invalid value"));
        }
    }

    #[test]
    fn test_map_arrow_error() {
        use datafusion::arrow::error::ArrowError;

        let arrow_err = ArrowError::ComputeError("test error".to_string());
        let result: Result<(), Error> = map_arrow_error(arrow_err, "test operation");

        assert!(result.is_err());
        if let Err(Error::Process(msg)) = result {
            assert!(msg.contains("Arrow test operation failed"));
        }
    }

    #[test]
    fn test_map_datafusion_error() {
        use datafusion::error::DataFusionError;

        let df_err = DataFusionError::Plan("test error".to_string());
        let result: Result<(), Error> = map_datafusion_error(df_err, "test operation");

        assert!(result.is_err());
        if let Err(Error::Process(msg)) = result {
            assert!(msg.contains("DataFusion test operation failed"));
        }
    }
}
