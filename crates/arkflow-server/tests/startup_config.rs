//! Startup configuration file loading, secret resolution, unknown-key
//! rejection, and environment override semantics for `arkflow-server
//! --config`.

use arkflow_core::config::TracingConfig;
use arkflow_server::{apply_env_overrides, load_startup_config, HubStartupConfig, ServerConfig};

fn write_file(dir: &tempfile::TempDir, name: &str, content: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, content).unwrap();
    path.to_str().unwrap().to_string()
}

fn env_map<'a>(entries: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    let map: std::collections::HashMap<String, String> = entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| map.get(name).cloned()
}

const FULL_YAML: &str = "\
address: 127.0.0.1:9000
node_token: node-secret
insecure_local: true
lease_ttl_ms: 20000
cors_origins:
  - https://console.example.com
logging:
  level: debug
  format: json
observability:
  tracing:
    enabled: true
    endpoint: http://127.0.0.1:4318/v1/traces
    service_name: arkflow-hub
hub:
  operator_token: operator-secret
ha:
  enabled: true
  lease_ttl_ms: 9000
  advertise_url: https://hub.example.com
";

#[test]
fn yaml_full_document_parses_every_section() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "hub.yaml", FULL_YAML);
    let config = load_startup_config(&path).unwrap();
    assert_eq!(config.server.address, "127.0.0.1:9000");
    assert_eq!(config.server.node_token.as_deref(), Some("node-secret"));
    assert!(config.server.insecure_local);
    assert_eq!(config.server.lease_ttl_ms, 20000);
    assert_eq!(
        config.server.cors_origins,
        vec!["https://console.example.com".to_string()]
    );
    assert_eq!(config.server.logging.level, "debug");
    assert!(config.server.observability.tracing.enabled);
    assert_eq!(
        config.server.observability.tracing.service_name,
        "arkflow-hub"
    );
    assert_eq!(
        config.hub.operator_token.as_deref(),
        Some("operator-secret")
    );
    assert!(config.ha.enabled);
    assert_eq!(config.ha.lease_ttl_ms, 9000);
    assert_eq!(
        config.ha.advertise_url.as_deref(),
        Some("https://hub.example.com")
    );
}

#[test]
fn json_and_toml_parse_equally() {
    let dir = tempfile::tempdir().unwrap();
    let json = write_file(
        &dir,
        "hub.json",
        r#"{"address":"127.0.0.1:9001","logging":{"level":"warn"},"ha":{"enabled":true}}"#,
    );
    let from_json = load_startup_config(&json).unwrap();
    assert_eq!(from_json.server.address, "127.0.0.1:9001");
    assert_eq!(from_json.server.logging.level, "warn");
    assert!(from_json.ha.enabled);

    let toml = write_file(
        &dir,
        "hub.toml",
        "address = \"127.0.0.1:9002\"\n[ha]\nenabled = true\n",
    );
    let from_toml = load_startup_config(&toml).unwrap();
    assert_eq!(from_toml.server.address, "127.0.0.1:9002");
    assert!(from_toml.ha.enabled);
}

#[test]
fn empty_mapping_uses_builtin_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "hub.yaml", "{}\n");
    let config = load_startup_config(&path).unwrap();
    let expected = HubStartupConfig::default();
    assert_eq!(config.server.address, expected.server.address);
    assert!(!config.server.observability.tracing.enabled);
    assert_eq!(config.server.logging.level, "info");
    assert_eq!(config.ha.lease_ttl_ms, 15_000);
    assert_eq!(config.hub.operator_token, None);
    // Tracing stays inert by default.
    assert_eq!(
        config.server.observability.tracing,
        TracingConfig::default()
    );
}

#[test]
fn unknown_top_level_key_is_rejected_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "hub.yaml", "adress: 127.0.0.1:9000\n");
    let error = load_startup_config(&path).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Unknown configuration keys: adress"),
        "unexpected error: {error}"
    );
}

#[test]
fn non_mapping_document_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "hub.yaml", "- just\n- a\n- list\n");
    let error = load_startup_config(&path).unwrap_err();
    assert!(error.to_string().contains("must be a mapping"));
}

#[test]
fn missing_file_unknown_extension_and_bad_syntax_are_reported() {
    let error = load_startup_config("/nonexistent/nope.yaml").unwrap_err();
    assert!(error.to_string().contains("Failed to read config file"));

    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "hub.txt", "address: x\n");
    let error = load_startup_config(&path).unwrap_err();
    assert!(error
        .to_string()
        .contains("Unsupported config file extension"));

    let path = write_file(&dir, "hub.yaml", "address: [unclosed\n");
    let error = load_startup_config(&path).unwrap_err();
    assert!(error.to_string().contains("YAML parsing error"));
}

#[test]
fn env_reference_in_operator_token_is_resolved() {
    // Unique variable name: this test binary runs its tests in parallel
    // threads sharing one process environment.
    std::env::set_var("ARKFLOW_TEST_TOKEN_STARTUP", "resolved-token-value");
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(
        &dir,
        "hub.yaml",
        "hub:\n  operator_token: ${env:ARKFLOW_TEST_TOKEN_STARTUP}\n",
    );
    let config = load_startup_config(&path).unwrap();
    assert_eq!(
        config.hub.operator_token.as_deref(),
        Some("resolved-token-value")
    );
    std::env::remove_var("ARKFLOW_TEST_TOKEN_STARTUP");
}

#[test]
fn secret_reference_resolves_through_arkflow_secret_prefix() {
    std::env::set_var("ARKFLOW_SECRET_TEST_HUB_TOKEN", "secret-scheme-value");
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(
        &dir,
        "hub.yaml",
        "hub:\n  operator_token: ${secret:TEST_HUB_TOKEN}\n",
    );
    let config = load_startup_config(&path).unwrap();
    assert_eq!(
        config.hub.operator_token.as_deref(),
        Some("secret-scheme-value")
    );
    std::env::remove_var("ARKFLOW_SECRET_TEST_HUB_TOKEN");
}

#[test]
fn type_error_after_resolution_uses_fixed_message_without_leaking() {
    std::env::set_var("ARKFLOW_TEST_TTL_BAD", "not-a-number-at-all");
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(
        &dir,
        "hub.yaml",
        "lease_ttl_ms: ${env:ARKFLOW_TEST_TTL_BAD}\n",
    );
    let error = load_startup_config(&path).unwrap_err().to_string();
    assert_eq!(
        error,
        "Configuration error: validation failed after secret resolution"
    );
    assert!(!error.contains("not-a-number-at-all"));
    std::env::remove_var("ARKFLOW_TEST_TTL_BAD");
}

#[test]
fn explicit_env_overrides_file_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(
        &dir,
        "hub.yaml",
        "address: 127.0.0.1:9000\nnode_token: from-file\nlogging:\n  level: debug\nobservability:\n  tracing:\n    enabled: false\nha:\n  enabled: true\n",
    );
    let mut config = load_startup_config(&path).unwrap();
    apply_env_overrides(
        &mut config,
        &env_map(&[
            ("ARKFLOW_HUB_ADDRESS", "127.0.0.1:9001"),
            ("ARKFLOW_NODE_TOKEN", "from-env"),
            ("ARKFLOW_LOG_LEVEL", "warn"),
            ("ARKFLOW_OTEL_TRACING_ENABLED", "true"),
            ("ARKFLOW_OTEL_ENDPOINT", "http://collector:4318/v1/traces"),
            ("ARKFLOW_OTEL_SERVICE_NAME", "arkflow-hub"),
            ("ARKFLOW_HUB_HA_ENABLED", "false"),
            ("ARKFLOW_OPERATOR_TOKEN", "operator-from-env"),
        ]),
    );
    assert_eq!(config.server.address, "127.0.0.1:9001");
    assert_eq!(config.server.node_token.as_deref(), Some("from-env"));
    assert_eq!(config.server.logging.level, "warn");
    assert!(config.server.observability.tracing.enabled);
    assert_eq!(
        config.server.observability.tracing.endpoint,
        "http://collector:4318/v1/traces"
    );
    assert_eq!(
        config.server.observability.tracing.service_name,
        "arkflow-hub"
    );
    assert!(!config.ha.enabled);
    assert_eq!(
        config.hub.operator_token.as_deref(),
        Some("operator-from-env")
    );
}

#[test]
fn env_absent_keeps_file_values_and_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(&dir, "hub.yaml", "address: 127.0.0.1:9000\n");
    let mut config = load_startup_config(&path).unwrap();
    apply_env_overrides(&mut config, &env_map(&[]));
    assert_eq!(config.server.address, "127.0.0.1:9000");
    assert_eq!(
        config.server.session_ttl_ms,
        ServerConfig::default().session_ttl_ms
    );
}

#[test]
fn invalid_env_value_warns_and_keeps_underlying_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(
        &dir,
        "hub.yaml",
        "insecure_local: true\napproval_ttl_ms: 60000\n",
    );
    let mut config = load_startup_config(&path).unwrap();
    apply_env_overrides(
        &mut config,
        &env_map(&[
            ("ARKFLOW_HUB_INSECURE_LOCAL", "maybe"),
            ("ARKFLOW_APPROVAL_TTL_MS", "500"),
        ]),
    );
    // Invalid boolean spelling keeps the file value; a sub-floor TTL keeps
    // the file value too (not the default).
    assert!(config.server.insecure_local);
    assert_eq!(config.server.approval_ttl_ms, 60000);
}

#[test]
fn env_only_matches_previous_binary_behavior_field_by_field() {
    // Regression: with no file, defaults + valid env must reproduce the old
    // env-only parser exactly.
    let mut config = HubStartupConfig::default();
    apply_env_overrides(
        &mut config,
        &env_map(&[
            ("ARKFLOW_HUB_ADDRESS", "0.0.0.0:8080"),
            ("ARKFLOW_NODE_TOKEN", "tok"),
            ("ARKFLOW_HUB_STORAGE", "sqlite:/tmp/hub.db"),
            ("ARKFLOW_HUB_INSECURE_LOCAL", "yes"),
            ("ARKFLOW_APPROVAL_REQUIRED", "true"),
            (
                "ARKFLOW_APPROVAL_TTL_MS",
                "7_200_000".replace('_', "").as_str(),
            ),
            ("ARKFLOW_RATE_LIMIT_ENABLED", "false"),
            ("ARKFLOW_RATE_LIMIT_PRINCIPAL_RPS", "250"),
            ("ARKFLOW_RATE_LIMIT_NODE_BURST", "4000"),
            ("ARKFLOW_HUB_HA_ENABLED", "1"),
            ("ARKFLOW_HUB_HA_LEASE_TTL_MS", "30000"),
            ("ARKFLOW_HUB_HA_HOLDER_ID", "hub-a"),
            (
                "ARKFLOW_HUB_HA_ADVERTISE_URL",
                "  https://hub-a.example.com  ",
            ),
        ]),
    );
    assert_eq!(config.server.address, "0.0.0.0:8080");
    assert_eq!(config.server.node_token.as_deref(), Some("tok"));
    assert_eq!(
        config.server.hub_storage.as_deref(),
        Some("sqlite:/tmp/hub.db")
    );
    assert!(config.server.insecure_local);
    assert!(config.server.approval_required);
    assert_eq!(config.server.approval_ttl_ms, 7_200_000);
    assert!(!config.server.rate_limit.enabled);
    assert_eq!(config.server.rate_limit.principal_rps, 250.0);
    assert_eq!(config.server.rate_limit.node_burst, 4000.0);
    assert_eq!(
        config.server.rate_limit.node_rps,
        arkflow_core::config::default_node_rps()
    );
    assert_eq!(
        config.server.rate_limit.principal_burst,
        arkflow_core::config::default_principal_burst()
    );
    assert!(config.ha.enabled);
    assert_eq!(config.ha.lease_ttl_ms, 30_000);
    assert_eq!(config.ha.holder_id.as_deref(), Some("hub-a"));
    assert_eq!(
        config.ha.advertise_url.as_deref(),
        Some("https://hub-a.example.com")
    );
}

#[test]
fn env_only_with_no_variables_equals_built_in_defaults() {
    let mut config = HubStartupConfig::default();
    apply_env_overrides(&mut config, &env_map(&[]));
    assert_eq!(config.server, ServerConfig::default());
    assert_eq!(config.ha, arkflow_server::hub::HubHaConfig::default());
    assert_eq!(config.hub.operator_token, None);
}

#[test]
fn file_rate_limit_values_survive_unset_rate_limit_env() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_file(
        &dir,
        "hub.yaml",
        "rate_limit:\n  enabled: true\n  principal_rps: 12.5\n  node_rps: 99\n",
    );
    let mut config = load_startup_config(&path).unwrap();
    apply_env_overrides(&mut config, &env_map(&[]));
    assert_eq!(config.server.rate_limit.principal_rps, 12.5);
    assert_eq!(config.server.rate_limit.node_rps, 99.0);
    // Unspecified fields keep the built-in defaults, not zeros.
    assert_eq!(
        config.server.rate_limit.node_burst,
        arkflow_core::config::default_node_burst()
    );
}
