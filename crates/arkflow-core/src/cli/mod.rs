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
use tracing_subscriber::fmt;

#[derive(Default)]
pub struct Cli {
    pub config: Option<EngineConfig>,
}

impl Cli {
    pub fn config(&self) -> Option<EngineConfig> {
        self.config.clone()
    }

    pub fn parse(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let matches = Command::new("arkflow")
            .version("0.4.0-rc1")
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
                                    .help("Filter by component kind: input, output, processor, buffer, codec."),
                            ),
                    )
                    .subcommand(
                        Command::new("show")
                            .about("Print the configuration schema for a specific component.")
                            .arg(
                                Arg::new("kind")
                                    .value_name("KIND")
                                    .required(true)
                                    .help("Component kind: input, output, processor, buffer, codec."),
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
            .get_matches();

        // Dispatch subcommands that don't require a config file.
        match matches.subcommand() {
            Some(("components", sub)) => {
                handle_components_subcommand(sub)?;
                process::exit(0);
            }
            Some(("schema", _)) => {
                let schema = component::build_config_schema();
                println!("{}", serde_json::to_string_pretty(&schema)?);
                process::exit(0);
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
                process::exit(1);
            }
        };

        // If you just verify the configuration, exit it
        if matches.get_flag("validate") {
            info!("The config is validated.");
            return Ok(());
        }
        self.config = Some(config);
        Ok(())
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
            print_component_list(filter);
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

    let subscriber_builder = fmt::Subscriber::builder().with_max_level(log_level);

    // Check if we need to output logs to a file
    if let Some(file_path) = &config.logging.file_path {
        // Create the file and parent directories if they don't exist
        if let Some(parent) = std::path::Path::new(file_path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        // Open the file for writing
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
        {
            Ok(file) => {
                match config.logging.format {
                    LogFormat::JSON => {
                        let subscriber = subscriber_builder
                            .with_writer(std::sync::Mutex::new(file))
                            .pretty()
                            .json()
                            .finish();
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("You can't set a global default log subscriber");
                    }
                    LogFormat::PLAIN => {
                        let subscriber = subscriber_builder
                            .with_writer(std::sync::Mutex::new(file))
                            .pretty()
                            .finish();
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("You can't set a global default log subscriber");
                    }
                }

                info!("Logging to file: {}", file_path);
                return;
            }
            Err(e) => {
                eprintln!("Failed to open log file {}: {}", file_path, e);
                // Fall back to console logging
            }
        }
    }

    match config.logging.format {
        LogFormat::JSON => {
            let subscriber = subscriber_builder.pretty().json().finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("You can't set a global default log subscriber");
        }
        LogFormat::PLAIN => {
            let subscriber = subscriber_builder.pretty().finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("You can't set a global default log subscriber");
        }
    }
}
