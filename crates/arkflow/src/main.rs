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
    let mut server_task = if agent_config.is_none() {
        Some(tokio::spawn(serve(
            engine.control_plane(),
            server_config.clone(),
            cancellation.clone(),
        )))
    } else {
        None
    };
    let mut observability_task = if !local_api_server {
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
    // The engine future is pinned locally (its error is not `Send`, so it
    // cannot be spawned) and raced against the serve tasks: both `serve`
    // loops are supposed to run until cancelled, so either one exiting
    // while the engine still runs is abnormal — typically the standalone
    // startup guard refusing an unsafe bind, or a bind failure. Surfacing
    // that immediately (cancel + fail) keeps the guard's refusal from
    // being an invisible side-task error while the engine streams on
    // without its control API. (`agent::run` keeps its original
    // await-after-engine semantics: it owns its own retry/reconnect loop.)
    let engine_fut = engine.run_with_cancellation(cancellation.clone());
    tokio::pin!(engine_fut);
    let mut engine_result: Option<Result<(), Box<dyn std::error::Error>>> = None;
    loop {
        if engine_result.is_some() {
            break;
        }
        tokio::select! {
            result = &mut engine_fut => {
                engine_result = Some(result);
            }
            result = async { server_task.as_mut().unwrap().await }, if server_task.is_some() => {
                let result = result
                    .unwrap_or_else(|e: tokio::task::JoinError| Err(e.into()));
                cancellation.cancel();
                engine_result = Some(match result {
                    Ok(()) => Err("control API server exited before cancellation".into()),
                    Err(e) => Err(format!("control API server failed: {e}").into()),
                });
            }
            result = async { observability_task.as_mut().unwrap().await }, if observability_task.is_some() => {
                let result = result
                    .unwrap_or_else(|e: tokio::task::JoinError| Err(e.into()));
                cancellation.cancel();
                engine_result = Some(match result {
                    Ok(()) => Err("observability server exited before cancellation".into()),
                    Err(e) => Err(format!("observability server failed: {e}").into()),
                });
            }
        }
    }
    cancellation.cancel();
    let engine_result = engine_result.expect("the loop always sets the result");
    if engine_result.is_ok() {
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
    }
    engine_result?;
    // Flush buffered OTel spans before the process exits so the tail of the
    // trace is not lost.
    arkflow_core::cli::shutdown_otel_tracing();
    Ok(())
}
