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

use arkflow_core::cli::Cli;
use arkflow_core::engine::Engine;
use arkflow_plugin::{buffer, codec, input, output, processor, temporary, wal};
use arkflow_server::{agent, serve, ServerConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    input::init()?;
    output::init()?;
    processor::init()?;
    buffer::init()?;
    temporary::init()?;
    codec::init()?;
    wal::init()?;
    let mut cli = Cli::default();
    cli.parse()?;
    let Some(config) = cli.config() else {
        return cli.run().await;
    };
    arkflow_core::cli::init_logging(&config);
    let engine = Engine::new(config.clone());
    let cancellation = CancellationToken::new();
    let agent_config = agent::NodeAgentConfig::from_engine(&config);
    let server_task = if agent_config.is_none() {
        Some(tokio::spawn(serve(
            engine.control_plane(),
            ServerConfig::from_engine(&config),
            cancellation.clone(),
        )))
    } else {
        None
    };
    let agent_task = agent_config.map(|agent_config| {
        tokio::spawn(agent::run(
            engine.control_plane(),
            agent_config,
            cancellation.clone(),
        ))
    });
    let engine_result = engine.run_with_cancellation(cancellation.clone()).await;
    cancellation.cancel();
    if let Some(server_task) = server_task {
        let result = server_task.await?;
        result.map_err(|error| -> Box<dyn std::error::Error> { error.to_string().into() })?;
    }
    if let Some(agent_task) = agent_task {
        let result = agent_task.await?;
        result.map_err(|error| -> Box<dyn std::error::Error> { error.to_string().into() })?;
    }
    engine_result?;
    Ok(())
}
