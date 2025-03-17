use crate::config::EngineConfig;
use crate::engine::Engine;
use clap::{Arg, Command};
use std::process;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

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
            .version("0.1.0")
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
                error!("Failed to load configuration file: {}", e);
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
    let log_level = if let Some(logging) = &config.logging {
        match logging.level.as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        }
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("You can't set a global default log subscriber");
}
