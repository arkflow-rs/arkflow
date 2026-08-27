//! Golden compilation tests: every example YAML with `streams:` must compile
//! to JobSpecs (control-plane examples without streams are skipped; the
//! join-buffer example fails compilation by contract with a migration
//! message).

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::stream_compiler::compile_engine_streams;

#[test]
fn every_stream_example_compiles_to_job_specs() {
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples");
    let mut compiled = 0;
    for entry in std::fs::read_dir(&examples).unwrap() {
        let path = entry.unwrap().path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".yaml") || name.contains("control_plane") || name.contains("hub") || name.contains("node") {
            continue;
        }
        let Ok(config) = EngineConfig::from_file(path.to_str().unwrap()) else {
            continue;
        };
        if config.streams.is_empty() {
            continue;
        }
        match compile_engine_streams(&config) {
            Ok(specs) => {
                assert_eq!(specs.len(), config.streams.len(), "{name}");
                for spec in &specs {
                    assert!(
                        !spec.sources.is_empty() && !spec.sinks.is_empty(),
                        "{name}: compiled spec {} lacks sources/sinks",
                        spec.id
                    );
                }
                compiled += 1;
            }
            Err(error) => {
                // Only the join buffer is allowed to fail (by contract).
                let message = error.to_string();
                assert!(
                    name.contains("join"),
                    "{name} failed to compile: {message}"
                );
                assert!(message.contains("join"), "{name}: {message}");
            }
        }
    }
    assert!(compiled >= 20, "expected most examples to compile, got {compiled}");
}

#[test]
fn yaml_declared_jobs_are_parsed_and_validated() {
    let yaml = r#"
streams: []
jobs:
  - id: local-job
    version: 1
    max_parallelism: 1
    parallelism: 1
    operators:
      - id: source
        kind: source
      - id: sink
        kind: sink
    edges:
      - id: e1
        from: source
        to: sink
    sources:
      - operator_id: source
        input_type: generate
        time:
          mode: processing_time
    sinks:
      - operator_id: sink
        output_type: stdout
    recovery: latest_checkpoint
"#;
    let config: EngineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.jobs.len(), 1);
    assert_eq!(config.job_specs().unwrap().len(), 1);
    assert_eq!(config.jobs[0].id.as_str(), "local-job");
}

#[test]
fn duplicate_job_ids_are_rejected() {
    let yaml = r#"
streams: []
jobs:
  - id: dup
    version: 1
    max_parallelism: 1
    parallelism: 1
    operators:
      - id: source
        kind: source
      - id: sink
        kind: sink
    edges:
      - id: e1
        from: source
        to: sink
    sources:
      - operator_id: source
        input_type: generate
        time:
          mode: processing_time
    sinks:
      - operator_id: sink
        output_type: stdout
    recovery: latest_checkpoint
  - id: dup
    version: 1
    max_parallelism: 1
    parallelism: 1
    operators:
      - id: source
        kind: source
      - id: sink
        kind: sink
    edges:
      - id: e1
        from: source
        to: sink
    sources:
      - operator_id: source
        input_type: generate
        time:
          mode: processing_time
    sinks:
      - operator_id: sink
        output_type: stdout
    recovery: latest_checkpoint
"#;
    let config: EngineConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.job_specs().is_err());
}
