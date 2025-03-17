use arkflow_core::cli::Cli;
use arkflow_plugin::{input, output, processor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    input::init();
    output::init();
    processor::init();

    let mut cli = Cli::default();
    cli.parse()?;
    cli.run().await
}
