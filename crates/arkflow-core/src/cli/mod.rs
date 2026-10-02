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

use crate::component::{self, ComponentKind};
use crate::config::{EngineConfig, LogFormat};
use crate::engine::Engine;
use clap::{Arg, ArgMatches, Command};
use std::process;
use tracing::{info, Level};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

#[derive(Default)]
pub struct Cli {
    pub config: Option<EngineConfig>,
}

/// Outcome of command-line parsing: `Run` continues into [`Cli::run`];
/// `Exit(code)` marks a subcommand result or a config failure that already
/// printed its message and wants the process to stop with that code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseOutcome {
    Run,
    Exit(i32),
}

impl Cli {
    pub fn config(&self) -> Option<EngineConfig> {
        self.config.clone()
    }

    pub fn parse(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let argv: Vec<String> = std::env::args().skip(1).collect();
        match self.parse_from(argv) {
            Ok(ParseOutcome::Run) => Ok(()),
            Ok(ParseOutcome::Exit(code)) => process::exit(code),
            // clap errors (bad flags) keep clap's own usage output + exit 2.
            Err(e) if e.is::<clap::Error>() => {
                e.downcast_ref::<clap::Error>().unwrap().exit()
            }
            Err(e) => Err(e),
        }
    }

    /// Testable core of [`Cli::parse`]: identical logic, but the argument
    /// vector is passed explicitly and exits surface as [`ParseOutcome`]
    /// instead of terminating the process.
    pub fn parse_from(
        &mut self,
        argv: impl IntoIterator<Item = String>,
    ) -> Result<ParseOutcome, Box<dyn std::error::Error>> {
        let matches = Command::new("arkflow")
            .version(env!("CARGO_PKG_VERSION"))
            .author("chenquan")
            .about("High-performance Rust stream processing engine, providing powerful data stream processing capabilities, supporting multiple input/output sources and processors.")
            .subcommand(
                Command::new("components")
                    .about("Discover registered components and their configuration schemas.")
                    .subcommand(
                        Command::new("list")
                            .about("List every registered component, grouped by kind.")
                            .arg(
                                Arg::new("kind")
                                    .long("kind")
                                    .short('k')
                                    .value_name("KIND")
                                    .help("Filter by component kind: input, output, processor, buffer, codec, temporary."),
                            )
                            .arg(
                                Arg::new("format")
                                    .long("format")
                                    .short('f')
                                    .value_name("FORMAT")
                                    .default_value("text")
                                    .help("Output format: text or json."),
                            ),
                    )
                    .subcommand(
                        Command::new("show")
                            .about("Print the configuration schema for a specific component.")
                            .arg(
                                Arg::new("kind")
                                    .value_name("KIND")
                                    .required(true)
                                    .help("Component kind: input, output, processor, buffer, codec, temporary."),
                            )
                            .arg(
                                Arg::new("name")
                                    .value_name("NAME")
                                    .required(true)
                                    .help("Registered component type name."),
                            )
                            .arg(
                                Arg::new("format")
                                    .long("format")
                                    .short('f')
                                    .value_name("FORMAT")
                                    .default_value("text")
                                    .help("Output format: text or json."),
                            ),
                    ),
            )
            .subcommand(
                Command::new("schema")
                    .about("Print the JSON Schema for the engine configuration (useful for IDE auto-completion)."),
            )
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_name("FILE")
                    .help("Specify the profile path.")
                    .required(false),
            )
            .arg(
                Arg::new("validate")
                    .short('v')
                    .long("validate")
                    .help("Only the profile is verified, not the engine is started.")
                    .action(clap::ArgAction::SetTrue),
            )
            .try_get_matches_from(
                std::iter::once("arkflow".to_string()).chain(argv),
            )?;

        // Dispatch subcommands that don't require a config file.
        match matches.subcommand() {
            Some(("components", sub)) => {
                handle_components_subcommand(sub)?;
                return Ok(ParseOutcome::Exit(0));
            }
            Some(("schema", _)) => {
                let schema = component::build_config_schema();
                println!("{}", serde_json::to_string_pretty(&schema)?);
                return Ok(ParseOutcome::Exit(0));
            }
            _ => {}
        }

        // Get the profile path; required when not running a subcommand.
        let Some(config_path) = matches.get_one::<String>("config") else {
            return Err(Box::new(Error::Config(
                "missing --config <FILE> (or run a subcommand: components, schema)".to_string(),
            )));
        };

        // Get the profile path
        let config = match EngineConfig::from_file(config_path) {
            Ok(config) => config,
            Err(e) => {
                println!("Failed to load configuration file: {}", e);
                return Ok(ParseOutcome::Exit(1));
            }
        };

        // Deep validation: stream ids and declared Job specs (graph checks,
        // duplicate ids, operator references) beyond deserialization.
        if let Err(e) = config.stream_ids() {
            println!("Invalid configuration: {}", e);
            return Ok(ParseOutcome::Exit(1));
        }
        if let Err(e) = config.job_specs() {
            println!("Invalid configuration: {}", e);
            return Ok(ParseOutcome::Exit(1));
        }
        let validation = crate::configuration::validate_config(&config);
        if !validation.valid {
            let details = validation
                .errors
                .iter()
                .map(|issue| format!("{}: {}", issue.path, issue.message))
                .collect::<Vec<_>>()
                .join("; ");
            println!("Invalid configuration: {details}");
            return Ok(ParseOutcome::Exit(1));
        }

        // If you just verify the configuration, exit it
        if matches.get_flag("validate") {
            info!("The config is validated.");
            return Ok(ParseOutcome::Run);
        }
        self.config = Some(config);
        Ok(ParseOutcome::Run)
    }
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // `--validate` (and the subcommands handled inside `parse`) return
        // without loading a config, so the engine should not be started.
        let Some(config) = self.config.clone() else {
            return Ok(());
        };
        // Initialize the logging system
        init_logging(&config);
        let engine = Engine::new(config);
        engine.run().await?;
        Ok(())
    }
}

fn handle_components_subcommand(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    match matches.subcommand() {
        Some(("list", sub)) => {
            let filter: Option<ComponentKind> = sub
                .get_one::<String>("kind")
                .map(|k| k.parse())
                .transpose()?;
            let format = sub
                .get_one::<String>("format")
                .map(|s| s.as_str())
                .unwrap_or("text");
            if format == "json" {
                print_component_list_json(filter)?;
            } else {
                print_component_list(filter);
            }
            Ok(())
        }
        Some(("show", sub)) => {
            let kind: ComponentKind = sub.get_one::<String>("kind").unwrap().parse()?;
            let name = sub.get_one::<String>("name").unwrap();
            let format = sub
                .get_one::<String>("format")
                .map(|s| s.as_str())
                .unwrap_or("text");
            print_component_details(kind, name, format)
        }
        _ => {
            // `arkflow components` with no subcommand behaves like
            // `arkflow components list` to keep the UX forgiving.
            print_component_list(None);
            Ok(())
        }
    }
}

fn print_component_list(filter: Option<ComponentKind>) {
    let entries: Vec<(ComponentKind, _)> = match filter {
        Some(kind) => component::list_components_by_kind(kind)
            .into_iter()
            .map(|m| (kind, m))
            .collect(),
        None => component::list_components(),
    };

    if entries.is_empty() {
        println!("No components registered.");
        return;
    }

    let mut current_kind: Option<ComponentKind> = None;
    let name_width = entries
        .iter()
        .map(|(_, m)| m.name.len())
        .max()
        .unwrap_or(0)
        .max(4);

    for (kind, metadata) in &entries {
        if current_kind != Some(*kind) {
            if current_kind.is_some() {
                println!();
            }
            println!("{}:", kind);
            current_kind = Some(*kind);
        }
        println!(
            "  {:<width$}  {}",
            metadata.name,
            metadata.description,
            width = name_width
        );
    }
}

fn print_component_list_json(
    filter: Option<ComponentKind>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut payload = component::export_registry();
    if let Some(kind) = filter {
        if let Some(components) = payload.get_mut("components").and_then(|c| c.as_array_mut()) {
            components.retain(|c| c["kind"].as_str() == Some(kind.as_str()));
        }
    }
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn print_component_details(
    kind: ComponentKind,
    name: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = component::get_component_metadata(kind, name).ok_or_else(|| {
        let known: Vec<String> = component::list_components_by_kind(kind)
            .into_iter()
            .map(|m| m.name.clone())
            .collect();
        let available = if known.is_empty() {
            " (no components are registered for this kind)".to_string()
        } else {
            format!(". Available {} types: {}", kind, known.join(", "))
        };
        Error::Config(format!("Unknown {} type: {}{}", kind, name, available))
    })?;

    match format {
        "json" => {
            let payload = serde_json::json!({
                "kind": kind,
                "name": metadata.name,
                "description": metadata.description,
                "config_optional": metadata.config_optional,
                "config_schema": metadata.config_schema,
                "config_example": metadata.config_example,
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
        }
        _ => {
            println!("{}: {}", metadata.name, metadata.description);
            println!("kind: {}", kind);
            println!(
                "config_optional: {}",
                if metadata.config_optional {
                    "yes"
                } else {
                    "no"
                }
            );
            if let Some(example) = &metadata.config_example {
                println!("\nExample:");
                println!("{}", serde_json::to_string_pretty(example)?);
            }
            println!("\nConfig schema:");
            println!("{}", serde_json::to_string_pretty(&metadata.config_schema)?);
        }
    }
    Ok(())
}

use crate::Error;
pub fn init_logging(config: &EngineConfig) {
    let log_level = match config.logging.level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    let level_filter = LevelFilter::from(log_level);

    // Open the log file when a path is configured; failures fall back to
    // console logging like before.
    let file = config.logging.file_path.as_ref().and_then(|file_path| {
        if let Some(parent) = std::path::Path::new(file_path).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
        {
            Ok(file) => {
                info!("Logging to file: {}", file_path);
                Some(std::sync::Mutex::new(file))
            }
            Err(e) => {
                eprintln!("Failed to open log file {}: {}", file_path, e);
                None
            }
        }
    });

    // The fmt layer reproduces the original subscriber behavior matrix
    // (writer x format); the optional OTel layer adds span export without
    // touching it.
    let fmt_layer: Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync> =
        match (file, config.logging.format.clone()) {
            (Some(writer), LogFormat::JSON) => fmt::layer()
                .with_writer(writer)
                .json()
                .with_filter(level_filter)
                .boxed(),
            (Some(writer), LogFormat::PLAIN) => fmt::layer()
                .with_writer(writer)
                .pretty()
                .with_filter(level_filter)
                .boxed(),
            (None, LogFormat::JSON) => {
                fmt::layer().json().with_filter(level_filter).boxed()
            }
            (None, LogFormat::PLAIN) => {
                fmt::layer().pretty().with_filter(level_filter).boxed()
            }
        };

    let otel_layer = build_otel_layer(&config.health_check.observability.tracing)
        .map(|layer| layer.with_filter(level_filter));

    // try_init: a second initialization (e.g. tests, or an engine restart in
    // the same process) keeps the first subscriber instead of panicking.
    let _ = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .try_init();
}

/// Builds the OTel span-export layer when tracing is enabled. Any build
/// failure is isolated: a warning is printed and `None` is returned so the
/// data plane is never affected by the export pipeline.
fn build_otel_layer<S>(
    config: &crate::config::TracingConfig,
) -> Option<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + Send + Sync,
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if !config.enabled {
        return None;
    }
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(config.endpoint.clone())
        .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
        .build();
    let exporter = match exporter {
        Ok(exporter) => exporter,
        Err(error) => {
            eprintln!(
                "Failed to build the OTLP span exporter (tracing disabled): {}",
                error
            );
            return None;
        }
    };
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(config.service_name.clone())
                .build(),
        )
        .build();
    let tracer = provider.tracer("arkflow");
    let _ = opentelemetry::global::set_tracer_provider(provider.clone());
    // Keep a handle so graceful shutdown can flush buffered spans (see
    // [`shutdown_otel_tracing`]); the global provider itself holds a clone.
    let _ = OTEL_PROVIDER.set(provider);
    Some(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
}

/// Handle to the process-global OTel tracer provider, kept so buffered spans
/// can be flushed on graceful shutdown (the global registry holds a clone).
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Flushes and shuts down the OTel tracer provider installed by
/// [`init_logging`]. Export failures are logged and swallowed: shutdown
/// must proceed. A no-op when tracing was never enabled.
pub fn shutdown_otel_tracing() {
    if let Some(provider) = OTEL_PROVIDER.get() {
        if let Err(error) = provider.shutdown() {
            eprintln!("Failed to shut down the OTel tracer provider cleanly: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn write_config(dir: &tempfile::TempDir, name: &str, body: &str) -> String {
        let path = dir.path().join(name);
        std::fs::write(&path, body).unwrap();
        path.to_str().unwrap().to_string()
    }

    const MINIMAL_CONFIG: &str = "logging:\n  level: info\nstreams: []\n";

    #[test]
    fn default_cli_has_no_config_and_config_accessor_round_trips() {
        let cli = Cli::default();
        assert!(cli.config().is_none());
    }

    #[test]
    fn missing_config_without_subcommand_is_an_error() {
        let mut cli = Cli::default();
        let err = cli.parse_from(argv(&[])).unwrap_err();
        assert!(err.to_string().contains("missing --config"));
    }

    #[test]
    fn unknown_flag_surfaces_clap_error() {
        let mut cli = Cli::default();
        let err = cli
            .parse_from(argv(&["--definitely-not-a-flag"]))
            .unwrap_err();
        assert!(err.is::<clap::Error>());
    }

    #[test]
    fn config_file_that_cannot_be_read_exits_one() {
        let mut cli = Cli::default();
        let outcome = cli
            .parse_from(argv(&[
                "--config",
                "/nonexistent/arkflow/does-not-exist.yaml",
            ]))
            .unwrap();
        assert_eq!(outcome, ParseOutcome::Exit(1));
        assert!(cli.config.is_none());
    }

    #[test]
    fn valid_config_loads_and_is_retained() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, "ok.yaml", MINIMAL_CONFIG);
        let mut cli = Cli::default();
        let outcome = cli.parse_from(argv(&["--config", &path])).unwrap();
        assert_eq!(outcome, ParseOutcome::Run);
        assert!(cli.config().is_some());
        assert!(cli.config().unwrap().streams.is_empty());
    }

    #[test]
    fn validate_flag_reports_run_without_retaining_the_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, "ok.yaml", MINIMAL_CONFIG);
        let mut cli = Cli::default();
        let outcome = cli
            .parse_from(argv(&["--config", &path, "--validate"]))
            .unwrap();
        assert_eq!(outcome, ParseOutcome::Run);
        assert!(
            cli.config.is_none(),
            "--validate must not keep a config for Cli::run"
        );
    }

    #[test]
    fn malformed_yaml_exits_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, "broken.yaml", "logging: [oops\nstreams: ]]");
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["--config", &path])).unwrap(),
            ParseOutcome::Exit(1)
        );
    }

    #[tokio::test]
    async fn run_without_config_is_a_no_op() {
        // Covers the early return in `run` when `--validate` left no config.
        Cli::default().run().await.unwrap();
    }

    #[test]
    fn schema_subcommand_exits_zero() {
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["schema"])).unwrap(),
            ParseOutcome::Exit(0)
        );
        assert!(cli.config.is_none());
    }

    #[test]
    fn components_bare_invocation_lists_and_exits_zero() {
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["components"])).unwrap(),
            ParseOutcome::Exit(0)
        );
    }

    #[test]
    fn components_list_text_and_json_exit_zero() {
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["components", "list"])).unwrap(),
            ParseOutcome::Exit(0)
        );
        assert_eq!(
            cli.parse_from(argv(&["components", "list", "--format", "json"]))
                .unwrap(),
            ParseOutcome::Exit(0)
        );
        assert_eq!(
            cli.parse_from(argv(&["components", "list", "--kind", "input"]))
                .unwrap(),
            ParseOutcome::Exit(0)
        );
        assert_eq!(
            cli.parse_from(argv(&[
                "components",
                "list",
                "--kind",
                "input",
                "--format",
                "json"
            ]))
            .unwrap(),
            ParseOutcome::Exit(0)
        );
    }

    #[test]
    fn components_list_with_unknown_kind_is_an_error() {
        let mut cli = Cli::default();
        let err = cli
            .parse_from(argv(&["components", "list", "--kind", "not-a-kind"]))
            .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn components_show_for_unknown_type_reports_available_types() {
        let err = print_component_details(
            "not-a-kind".parse().unwrap_or(ComponentKind::Input),
            "definitely-missing",
            "text",
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("Unknown"), "{message}");
    }

    #[test]
    fn print_helpers_cover_formats_without_registered_components() {
        // With an empty registry the list prints the fallback line; the JSON
        // export still emits the envelope.
        print_component_list(None);
        print_component_list(Some(ComponentKind::Input));
        print_component_list_json(None).unwrap();
        print_component_list_json(Some(ComponentKind::Codec)).unwrap();
    }

    #[test]
    fn registered_components_render_in_list_and_details() {
        // Unique type names keep the process-global registry idempotent.
        {
            use crate::component::{ComponentMetadata, ComponentKind};
            let metadata = ComponentMetadata {
                name: "cli-test-fake".into(),
                description: "fake component for cli tests".into(),
                config_optional: true,
                config_schema: serde_json::json!({"type": "object"}),
                config_example: Some(serde_json::json!({"answer": 42})),
            };
            // Ignore the "already registered" error on re-runs in the same
            // process: the important part is that listing finds it.
            let _ = crate::component::register_component_metadata(
                ComponentKind::Input,
                metadata,
            );

            print_component_list(Some(ComponentKind::Input));
            print_component_list(None);
            print_component_list_json(Some(ComponentKind::Input)).unwrap();

            print_component_details(ComponentKind::Input, "cli-test-fake", "text").unwrap();
            print_component_details(ComponentKind::Input, "cli-test-fake", "json").unwrap();
            print_component_details(
                ComponentKind::Input,
                "cli-test-fake",
                "unknown-format",
            )
            .unwrap();
        }
    }

    #[test]
    fn components_show_subcommand_round_trip() {
        {
            use crate::component::{ComponentMetadata, ComponentKind};
            let metadata = ComponentMetadata {
                name: "cli-show-fake".into(),
                description: "fake component for the show subcommand".into(),
                config_optional: false,
                config_schema: serde_json::json!({"type": "object"}),
                config_example: None,
            };
            let _ = crate::component::register_component_metadata(
                ComponentKind::Buffer,
                metadata,
            );
            let mut cli = Cli::default();
            assert_eq!(
                cli.parse_from(argv(&[
                    "components", "show", "buffer", "cli-show-fake"
                ]))
                .unwrap(),
                ParseOutcome::Exit(0)
            );
            assert_eq!(
                cli.parse_from(argv(&[
                    "components",
                    "show",
                    "buffer",
                    "cli-show-fake",
                    "--format",
                    "json"
                ]))
                .unwrap(),
                ParseOutcome::Exit(0)
            );
        }
    }

    #[test]
    fn otel_layer_is_absent_when_tracing_is_disabled() {
        let config = crate::config::TracingConfig::default();
        let layer =
            build_otel_layer::<tracing_subscriber::registry::Registry>(&config);
        assert!(layer.is_none());
    }

    #[test]
    fn shutdown_without_provider_is_a_no_op() {
        shutdown_otel_tracing();
    }

    const DUPLICATE_STREAM_ID_CONFIG: &str = "logging:\n  level: info\nstreams:\n  - id: dup\n    input: {type: generate}\n    pipeline: {processors: []}\n    output: {type: stdout}\n  - id: dup\n    input: {type: generate}\n    pipeline: {processors: []}\n    output: {type: stdout}\n";

    #[test]
    fn duplicate_stream_ids_exit_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, "dup-stream.yaml", DUPLICATE_STREAM_ID_CONFIG);
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["--config", &path])).unwrap(),
            ParseOutcome::Exit(1)
        );
        assert!(cli.config.is_none());
    }

    const BROKEN_JOB_CONFIG: &str = "logging:\n  level: info\nstreams: []\njobs:\n  - id: broken-job\n    version: 1\n    operators:\n      - id: only\n        kind: source\n    edges:\n      - id: e1\n        from: only\n        to: ghost\n    sources:\n      - operator_id: only\n        input_type: generate\n        time:\n          mode: processing_time\n";

    #[test]
    fn invalid_job_spec_exits_one_at_job_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, "broken-job.yaml", BROKEN_JOB_CONFIG);
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["--config", &path])).unwrap(),
            ParseOutcome::Exit(1)
        );
        assert!(cli.config.is_none());
    }

    const UNKNOWN_INPUT_CONFIG: &str = "logging:\n  level: info\nstreams:\n  - id: s1\n    input: {type: definitely-not-an-input}\n    pipeline: {processors: []}\n    output: {type: stdout}\n";

    #[test]
    fn unknown_input_type_fails_deep_validation_and_exits_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, "unknown-input.yaml", UNKNOWN_INPUT_CONFIG);
        let mut cli = Cli::default();
        assert_eq!(
            cli.parse_from(argv(&["--config", &path])).unwrap(),
            ParseOutcome::Exit(1)
        );
        assert!(cli.config.is_none());
    }

    #[test]
    fn components_show_rejects_an_unknown_kind() {
        let mut cli = Cli::default();
        let err = cli
            .parse_from(argv(&["components", "show", "not-a-kind", "some-name"]))
            .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn components_list_renders_multiple_kinds_and_filters_the_json_export() {
        // Unique type names keep the process-global registry idempotent.
        {
            use crate::component::{ComponentMetadata, ComponentKind};
            for (kind, name) in [
                (ComponentKind::Codec, "cli-coverage-codec"),
                (ComponentKind::Temporary, "cli-coverage-temp"),
            ] {
                let _ = crate::component::register_component_metadata(
                    kind,
                    ComponentMetadata {
                        name: name.into(),
                        description: "coverage helper component".into(),
                        config_optional: false,
                        config_schema: serde_json::json!({"type": "object"}),
                        config_example: None,
                    },
                );
            }

            // Two populated kinds exercise the separator between kind groups.
            print_component_list(None);
            // The JSON export filter retains only the matching kind.
            print_component_list_json(Some(ComponentKind::Codec)).unwrap();
        }
    }

    #[test]
    fn unknown_type_for_a_populated_kind_lists_available_types() {
        {
            use crate::component::{ComponentMetadata, ComponentKind};
            let _ = crate::component::register_component_metadata(
                ComponentKind::Temporary,
                ComponentMetadata {
                    name: "cli-hint-temp".into(),
                    description: "coverage helper component".into(),
                    config_optional: false,
                    config_schema: serde_json::json!({"type": "object"}),
                    config_example: None,
                },
            );
            let err = print_component_details(ComponentKind::Temporary, "definitely-missing", "text")
                .unwrap_err();
            let message = err.to_string();
            assert!(message.contains("Available temporary types:"), "{message}");
            assert!(message.contains("cli-hint-temp"), "{message}");
        }
    }

    /// An invalid OTLP endpoint makes the exporter build fail and the failure
    /// is isolated: the data plane keeps a `None` layer instead of losing its
    /// console logging. (otlp 0.33 rejects syntactically invalid endpoints
    /// eagerly at build; a merely unreachable endpoint no longer fails here —
    /// that surfaces at export time instead.)
    #[test]
    fn otel_layer_isolates_exporter_build_failures() {
        let config = crate::config::TracingConfig {
            enabled: true,
            endpoint: "not a valid url".to_string(),
            service_name: "arkflow-coverage".to_string(),
        };
        let layer = build_otel_layer::<tracing_subscriber::registry::Registry>(&config);
        assert!(layer.is_none());
        shutdown_otel_tracing();
    }

}
