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

use arkflow_core::cli::init_logging;
use arkflow_core::config::EngineConfig;

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

