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

//! Engine builder for creating streams with state management support

use crate::config::EngineConfig;
use crate::stream::Stream;
use crate::{Error, Resource};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Engine builder that creates streams with state management
pub struct EngineBuilder {
    config: EngineConfig,
    state_managers: HashMap<String, Arc<RwLock<crate::state::enhanced::EnhancedStateManager>>>,
}

impl EngineBuilder {
    /// Create new engine builder
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            state_managers: HashMap::new(),
        }
    }

    /// Build all streams with state management
    pub async fn build_streams(&mut self) -> Result<Vec<Stream>, Error> {
        let mut streams = Vec::with_capacity(self.config.streams.len());

        // Initialize state managers if enabled
        if self.config.state_management.enabled {
            info!(
                "State management is enabled with backend: {:?}",
                self.config.state_management.backend_type
            );

            // Create a shared state manager for all streams that need it
            for stream_config in &self.config.streams {
                if let Some(stream_state) = &stream_config.state {
                    if stream_state.enabled {
                        let enhanced_config = crate::state::enhanced::EnhancedStateConfig {
                            enabled: true,
                            backend_type: match self.config.state_management.backend_type {
                                crate::config::StateBackendType::Memory => {
                                    crate::state::enhanced::StateBackendType::Memory
                                }
                                crate::config::StateBackendType::S3 => {
                                    crate::state::enhanced::StateBackendType::S3
                                }
                                crate::config::StateBackendType::Hybrid => {
                                    crate::state::enhanced::StateBackendType::Hybrid
                                }
                            },
                            s3_config: self.config.state_management.s3_config.as_ref().map(
                                |config| crate::state::s3_backend::S3StateBackendConfig {
                                    bucket: config.bucket.clone(),
                                    region: config.region.clone(),
                                    endpoint: config.endpoint_url.clone(),
                                    access_key_id: config.access_key_id.clone(),
                                    secret_access_key: config.secret_access_key.clone(),
                                    prefix: Some(config.prefix.clone()),
                                    use_ssl: true,
                                },
                            ),
                            checkpoint_interval_ms: self
                                .config
                                .state_management
                                .checkpoint_interval_ms,
                            retained_checkpoints: self.config.state_management.retained_checkpoints,
                            exactly_once: self.config.state_management.exactly_once,
                            state_timeout_ms: stream_state
                                .state_timeout_ms
                                .unwrap_or(self.config.state_management.state_timeout_ms),
                        };

                        let state_manager =
                            crate::state::enhanced::EnhancedStateManager::new(enhanced_config)
                                .await?;
                        self.state_managers.insert(
                            stream_state.operator_id.clone(),
                            Arc::new(RwLock::new(state_manager)),
                        );
                    }
                }
            }
        } else {
            info!("State management is disabled");
        }

        // Build each stream
        for stream_config in &self.config.streams {
            let stream = if let Some(stream_state) = &stream_config.state {
                if stream_state.enabled {
                    if let Some(state_manager) = self.state_managers.get(&stream_state.operator_id)
                    {
                        // Build with state management
                        self.build_stream_with_state(stream_config, state_manager.clone())
                            .await?
                    } else {
                        warn!(
                            "No state manager found for operator: {}",
                            stream_state.operator_id
                        );
                        stream_config.build()?
                    }
                } else {
                    stream_config.build()?
                }
            } else {
                stream_config.build()?
            };

            streams.push(stream);
        }

        Ok(streams)
    }

    /// Build a single stream with state management
    async fn build_stream_with_state(
        &self,
        stream_config: &crate::stream::StreamConfig,
        state_manager: Arc<RwLock<crate::state::enhanced::EnhancedStateManager>>,
    ) -> Result<Stream, Error> {
        let mut resource = Resource {
            temporary: HashMap::new(),
            input_names: std::cell::RefCell::default(),
        };

        // Build temporary resources
        if let Some(temporary_configs) = &stream_config.temporary {
            resource.temporary = HashMap::with_capacity(temporary_configs.len());
            for temporary_config in temporary_configs {
                resource.temporary.insert(
                    temporary_config.name.clone(),
                    temporary_config.build(&resource)?,
                );
            }
        }

        // Build components
        let input = stream_config.input.build(&resource)?;
        let (pipeline, thread_num) = stream_config.pipeline.build(&resource)?;
        let output = stream_config.output.build(&resource)?;
        let error_output = stream_config
            .error_output
            .as_ref()
            .map(|config| config.build(&resource))
            .transpose()?;
        let buffer = stream_config
            .buffer
            .as_ref()
            .map(|config| config.build(&resource))
            .transpose()?;

        // Create stream with state manager
        Ok(Stream::new(
            input,
            pipeline,
            output,
            error_output,
            buffer,
            resource,
            thread_num,
            Some(state_manager),
        ))
    }

    /// Get state managers by operator ID
    pub fn get_state_managers(
        &self,
    ) -> &HashMap<String, Arc<RwLock<crate::state::enhanced::EnhancedStateManager>>> {
        &self.state_managers
    }

    /// Shutdown all state managers
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        for (_, state_manager) in self.state_managers.iter() {
            let mut manager = state_manager.write().await;
            manager.shutdown().await?;
        }
        Ok(())
    }
}
