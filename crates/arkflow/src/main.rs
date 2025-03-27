use arkflow_core::cli::Cli;
use arkflow_plugin::{buffer, input, output, processor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    input::init();
    output::init();
    processor::init();
    buffer::init();

    let mut cli = Cli::default();
    cli.parse()?;
    cli.run().await
}
