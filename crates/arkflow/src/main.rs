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
use arkflow_plugin::initialize;
use arkflow_server::{agent, serve, serve_observability, ServerConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    initialize()?;
    let mut cli = Cli::default();
    cli.parse()?;
    let Some(config) = cli.config() else {
        return cli.run().await;
    };
    arkflow_core::cli::init_logging(&config);
    let engine = Engine::new(config.clone());
    let cancellation = CancellationToken::new();
    let server_config = ServerConfig::from_engine(&config);
    let agent_config = agent::NodeAgentConfig::from_engine(&config);
    // The observability listener is skipped only when this very process also
    // serves the control-plane API router, which already exposes the same
    // endpoints under the server address.
    let local_api_server = agent_config.is_none() && server_config.enabled;
    let server_task = if agent_config.is_none() {
        Some(tokio::spawn(serve(
            engine.control_plane(),
            server_config.clone(),
            cancellation.clone(),
        )))
    } else {
        None
    };
    let observability_task = if !local_api_server {
        Some(tokio::spawn(serve_observability(
            engine.control_plane(),
            server_config,
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
    if let Some(observability_task) = observability_task {
        let result = observability_task.await?;
        result.map_err(|error| -> Box<dyn std::error::Error> { error.to_string().into() })?;
    }
    if let Some(agent_task) = agent_task {
        let result = agent_task.await?;
        result.map_err(|error| -> Box<dyn std::error::Error> { error.to_string().into() })?;
    }
    engine_result?;
    Ok(())
}
