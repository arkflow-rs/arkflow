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

use crate::config::{EngineConfig, LogFormat};
use crate::engine::Engine;
use clap::{Arg, Command};
use std::process;
use tracing::{info, Level};
use tracing_subscriber::fmt;

pub struct Cli {
    pub config: Option<EngineConfig>,
}
impl Default for Cli {
    fn default() -> Self {
        Self { config: None }
    }
}

impl Cli {
    pub fn parse(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let matches = Command::new("arkflow")
            .version("0.2.0")
            .author("chenquan")
            .about("High-performance Rust stream processing engine, providing powerful data stream processing capabilities, supporting multiple input/output sources and processors.")
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_name("FILE")
                    .help("Specify the profile path.")
                    .required(true),
            )
            .arg(
                Arg::new("validate")
                    .short('v')
                    .long("validate")
                    .help("Only the profile is verified, not the engine is started.")
                    .action(clap::ArgAction::SetTrue),
            )
            .get_matches();

        // Get the profile path
        let config_path = matches.get_one::<String>("config").unwrap();

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
        // Initialize the logging system
        let config = self.config.clone().unwrap();
        init_logging(&config);
        let engine = Engine::new(config);
        engine.run().await?;
        Ok(())
    }
}
fn init_logging(config: &EngineConfig) -> () {
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
                            .json()
                            .finish();
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("You can't set a global default log subscriber");
                    }
                    LogFormat::TEXT => {
                        let subscriber = subscriber_builder
                            .with_writer(std::sync::Mutex::new(file))
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
            let subscriber = subscriber_builder.json().finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("You can't set a global default log subscriber");
        }
        LogFormat::TEXT => {
            let subscriber = subscriber_builder.finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("You can't set a global default log subscriber");
        }
    }
}
