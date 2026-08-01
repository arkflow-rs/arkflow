//! ArkFlow control-plane HTTP server.
//!
//! The initial crate boundary keeps HTTP concerns separate from the core
//! runtime. Routes are added incrementally as RuntimeManager becomes
//! available.

pub const API_VERSION: &str = "v1";

/// Minimal server configuration shared by the future Router builder.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_api_prefix")]
    pub api_prefix: String,
}

fn default_enabled() -> bool {
    true
}

fn default_address() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_api_prefix() -> String {
    "/api/v1".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            address: default_address(),
            api_prefix: default_api_prefix(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_defaults_are_local_and_versioned() {
        let config = ServerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.address, "127.0.0.1:8080");
        assert_eq!(config.api_prefix, "/api/v1");
    }
}
