/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 */

//! Runs `init_logging` in its own process: it installs the process-global
//! tracing subscriber, which would otherwise race the lib-test OTel
//! subscriber installed by the executor span tests.

use arkflow_core::cli::{init_logging, init_logging_from};
use arkflow_core::config::{EngineConfig, LoggingConfig, TracingConfig};

fn config_from_yaml(yaml: &str) -> EngineConfig {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, yaml).unwrap();
    EngineConfig::from_file(path.to_str().unwrap()).unwrap()
}

#[test]
fn logging_initializes_from_config_for_each_format_and_writer() {
    // try_init keeps the first global subscriber; later calls exercise
    // the branch construction without replacing it.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("logs").join("engine.log");
    let file_str = file.to_str().unwrap().to_string();

    let base = "logging:\n  level: {level}\n  format: {format}\n  file_path: {file}\nstreams: []\n";
    let mut yaml = base
        .replace("{level}", "debug")
        .replace("{format}", "plain")
        .replace("{file}", &file_str);
    let config = config_from_yaml(&yaml);
    init_logging(&config);

    yaml = base
        .replace("{level}", "trace")
        .replace("{format}", "json")
        .replace("{file}", &file_str);
    init_logging(&config_from_yaml(&yaml));

    yaml = base
        .replace("{level}", "warn")
        .replace("{format}", "json")
        .replace("{file}", "");
    init_logging(&config_from_yaml(&yaml));

    yaml = base
        .replace("{level}", "bogus-level-falls-back-to-info")
        .replace("{format}", "plain")
        .replace("{file}", "");
    init_logging(&config_from_yaml(&yaml));

    // The file writer created the parent directory.
    assert!(dir.path().join("logs").exists());
}

/// RUST_LOG semantics, re-executed as a child process so the env var and the
/// global subscriber cannot leak into sibling tests: with parseable
/// directives the EnvFilter overrides the static level; with an unparsable
/// value a warning is printed and the static level applies.
#[test]
fn rust_log_overrides_static_level_and_falls_back_when_invalid() {
    if let Ok(case) = std::env::var("ARKFLOW_TEST_RUST_LOG_CHILD") {
        // Child mode: the parent re-executed this exact test with the env
        // already prepared. Emit one event per level and stop.
        if case == "override" {
            std::env::set_var("RUST_LOG", "warn");
        } else if case == "fallback" {
            std::env::set_var("RUST_LOG", "arkflow_core=definitely_not_a_level");
        }
        init_logging_from(
            &LoggingConfig {
                level: "info".to_string(),
                file_path: None,
                format: arkflow_core::config::LogFormat::PLAIN,
            },
            &TracingConfig::default(),
        );
        tracing::info!("ARKFLOW_CHILD_INFO_EVENT");
        tracing::warn!("ARKFLOW_CHILD_WARN_EVENT");
        return;
    }

    let exe = std::env::current_exe().unwrap();
    for (case, expect_info) in [("override", false), ("fallback", true), ("unset", true)] {
        let child = std::process::Command::new(&exe)
            .args([
                "--exact",
                "rust_log_overrides_static_level_and_falls_back_when_invalid",
                "--nocapture",
            ])
            .env("ARKFLOW_TEST_RUST_LOG_CHILD", case)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&child.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&child.stderr).into_owned();
        assert_eq!(
            stdout.contains("ARKFLOW_CHILD_INFO_EVENT"),
            expect_info,
            "case {case}: info event visibility mismatch"
        );
        assert!(
            stdout.contains("ARKFLOW_CHILD_WARN_EVENT"),
            "case {case}: warn event must always be visible"
        );
        if case == "fallback" {
            assert!(
                stderr.contains("Ignoring invalid RUST_LOG"),
                "case {case}: fallback warning missing"
            );
        }
    }
}
