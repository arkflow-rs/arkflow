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

//! Pulsar output component
//!
//! Send data to a Pulsar topic

use crate::expr::Expr;
use crate::pulsar::{
    PulsarAuth, PulsarClient, PulsarClientUtils, PulsarConfigValidator, PulsarProducer, RetryConfig,
};
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource, DEFAULT_BINARY_VALUE_FIELD};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, warn};

/// Pulsar output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarOutputConfig {
    /// Pulsar service URL
    pub service_url: String,
    /// Topic to publish to
    pub topic: Expr<String>,
    /// Authentication (optional)
    pub auth: Option<PulsarAuth>,
    /// Value field to use for message payload
    pub value_field: Option<String>,
    /// Message batching configuration (optional)
    pub batching: Option<PulsarBatchingConfig>,
    /// Retry configuration (optional)
    pub retry_config: Option<RetryConfig>,
    /// Producer pool configuration (optional)
    pub producer_pool: Option<PulsarProducerPoolConfig>,
}

/// Pulsar producer pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarProducerPoolConfig {
    /// Number of producers in the pool
    pub pool_size: Option<u32>,
    /// Maximum number of pending messages per producer
    pub max_pending_messages: Option<u32>,
    /// Strategy for selecting producer from pool
    pub selection_strategy: Option<ProducerSelectionStrategy>,
}

/// Producer selection strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerSelectionStrategy {
    /// Round-robin selection
    RoundRobin,
    /// Random selection
    Random,
    /// Least loaded (fewest pending messages)
    LeastLoaded,
}

impl Default for ProducerSelectionStrategy {
    fn default() -> Self {
        ProducerSelectionStrategy::RoundRobin
    }
}

/// Pulsar message batching configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarBatchingConfig {
    /// Maximum number of messages in a batch
    pub max_messages: Option<u32>,
    /// Maximum size of a batch in bytes
    pub max_size: Option<u32>,
    /// Maximum delay before sending a batch
    pub max_delay_ms: Option<u64>,
}

/// Producer wrapper with metrics
pub struct ProducerWrapper {
    producer: Arc<Mutex<Option<PulsarProducer>>>,
    pending_messages: Arc<RwLock<u32>>,
    topic: String,
}

impl ProducerWrapper {
    pub fn new(producer: PulsarProducer, topic: String) -> Self {
        Self {
            producer: Arc::new(Mutex::new(Some(producer))),
            pending_messages: Arc::new(RwLock::new(0)),
            topic,
        }
    }

    pub async fn send_message(&self, payload: Vec<u8>) -> Result<(), Error> {
        let mut pending = self.pending_messages.write().await;
        *pending += 1;
        drop(pending);

        let result = async {
            let mut producer_guard = self.producer.lock().await;
            let producer = producer_guard
                .as_mut()
                .ok_or_else(|| Error::Connection("Producer not available".to_string()))?;

            match producer.send_non_blocking(payload).await {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Process(format!("Failed to send message: {}", e))),
            }
        }
        .await;

        let mut pending = self.pending_messages.write().await;
        *pending = pending.saturating_sub(1);
        drop(pending);

        result
    }

    pub async fn get_pending_count(&self) -> u32 {
        *self.pending_messages.read().await
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }
}

/// Pulsar producer pool
pub struct PulsarProducerPool {
    producers: Vec<ProducerWrapper>,
    strategy: ProducerSelectionStrategy,
    current_index: Arc<RwLock<usize>>,
    client: Arc<RwLock<Option<PulsarClient>>>,
}

impl PulsarProducerPool {
    pub fn new(
        client: Arc<RwLock<Option<PulsarClient>>>,
        strategy: ProducerSelectionStrategy,
    ) -> Self {
        Self {
            producers: Vec::new(),
            strategy,
            current_index: Arc::new(RwLock::new(0)),
            client,
        }
    }

    pub async fn initialize(&mut self, topics: &[String], pool_size: u32) -> Result<(), Error> {
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("Pulsar client not connected".to_string()))?;

        self.producers.clear();

        for i in 0..pool_size {
            let topic = topics[i as usize % topics.len()].clone();
            let producer = client
                .producer()
                .with_topic(&topic)
                .build()
                .await
                .map_err(|e| {
                    Error::Connection(format!("Failed to create producer {}: {}", i, e))
                })?;

            self.producers.push(ProducerWrapper::new(producer, topic));
        }

        Ok(())
    }

    pub async fn get_producer(&self) -> Result<&ProducerWrapper, Error> {
        if self.producers.is_empty() {
            return Err(Error::Connection("No producers available".to_string()));
        }

        match self.strategy {
            ProducerSelectionStrategy::RoundRobin => {
                let mut index = self.current_index.write().await;
                let producer = &self.producers[*index];
                *index = (*index + 1) % self.producers.len();
                Ok(producer)
            }
            ProducerSelectionStrategy::Random => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let index = rng.gen_range(0..self.producers.len());
                Ok(&self.producers[index])
            }
            ProducerSelectionStrategy::LeastLoaded => {
                let mut min_load = u32::MAX;
                let mut selected_index = 0;

                for (i, producer) in self.producers.iter().enumerate() {
                    let load = producer.get_pending_count().await;
                    if load < min_load {
                        min_load = load;
                        selected_index = i;
                    }
                }

                Ok(&self.producers[selected_index])
            }
        }
    }

    pub async fn send_message(&self, payload: Vec<u8>) -> Result<(), Error> {
        let producer = self.get_producer().await?;
        producer.send_message(payload).await
    }

    pub async fn get_pool_stats(&self) -> Vec<(String, u32)> {
        let mut stats = Vec::new();
        for producer in &self.producers {
            let pending = producer.get_pending_count().await;
            stats.push((producer.get_topic().to_string(), pending));
        }
        stats
    }
}

/// Pulsar output component
pub struct PulsarOutput {
    config: PulsarOutputConfig,
    client: Arc<RwLock<Option<PulsarClient>>>,
    producer: Arc<RwLock<Option<PulsarProducer>>>,
    producer_pool: Arc<RwLock<Option<PulsarProducerPool>>>,
}

impl PulsarOutput {
    /// Create a new Pulsar output component
    fn new(config: PulsarOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            producer: Arc::new(RwLock::new(None)),
            producer_pool: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl Output for PulsarOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Validate configuration before connecting
        PulsarConfigValidator::validate_service_url(&self.config.service_url)?;

        // Get initial topic for validation
        let empty_batch = MessageBatch::new_binary(Vec::new())?;
        let initial_topic = self
            .config
            .topic
            .evaluate_expr(&empty_batch)
            .await?
            .get(0)
            .cloned()
            .unwrap_or_else(|| "default-topic".to_string());
        PulsarConfigValidator::validate_topic(&initial_topic)?;

        if let Some(ref auth) = self.config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        if let Some(ref retry_config) = self.config.retry_config {
            PulsarConfigValidator::validate_retry_config(retry_config)?;
        }

        if let Some(ref producer_pool_config) = self.config.producer_pool {
            PulsarConfigValidator::validate_producer_pool_config(producer_pool_config)?;
        }

        let _retry_config = self.config.retry_config.clone().unwrap_or_default();

        // Use shared client builder with authentication
        let builder =
            PulsarClientUtils::create_client_builder(&self.config.service_url, &self.config.auth)?;

        // Connect to Pulsar
        let client = builder
            .build()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Pulsar: {}", e)))?;

        // Store client
        let mut client_guard = self.client.write().await;
        *client_guard = Some(client.clone());

        // Get initial topic for producer creation
        let empty_batch = MessageBatch::new_binary(Vec::new())?;
        let initial_topic = self
            .config
            .topic
            .evaluate_expr(&empty_batch)
            .await?
            .get(0)
            .cloned()
            .unwrap_or_else(|| "default-topic".to_string());

        // Check if producer pool is configured
        if let Some(pool_config) = &self.config.producer_pool {
            let strategy = pool_config.selection_strategy.clone().unwrap_or_default();
            let pool_size = pool_config.pool_size.unwrap_or(3);

            // Initialize producer pool
            let mut pool = PulsarProducerPool::new(self.client.clone(), strategy);
            pool.initialize(&[initial_topic], pool_size).await?;

            let mut pool_guard = self.producer_pool.write().await;
            *pool_guard = Some(pool);
        } else {
            // Create single producer (original behavior)
            let producer = client
                .producer()
                .with_topic(&initial_topic)
                .build()
                .await
                .map_err(|e| Error::Connection(format!("Failed to create producer: {}", e)))?;

            let mut producer_guard = self.producer.write().await;
            *producer_guard = Some(producer);
        }

        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        // Check client connection
        let client_guard = self.client.read().await;
        let _client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("Pulsar client not connected".to_string()))?;

        // Get value field
        let value_field = self
            .config
            .value_field
            .as_deref()
            .unwrap_or(DEFAULT_BINARY_VALUE_FIELD);

        // Get message payloads
        let payloads = msg.to_binary(value_field)?;
        if payloads.is_empty() {
            return Ok(());
        }

        // Clone payloads to avoid lifetime issues
        let owned_payloads: Vec<Vec<u8>> = payloads.into_iter().map(|p| p.to_vec()).collect();

        // Get topics for each message
        let topics_result = self.config.topic.evaluate_expr(&msg).await?;
        let topics: Vec<String> = match topics_result {
            crate::expr::EvaluateResult::Scalar(topic) => vec![topic],
            crate::expr::EvaluateResult::Vec(topics_vec) => topics_vec,
        };

        // Check if producer pool is configured
        if self.config.producer_pool.is_some() {
            let pool_guard = self.producer_pool.read().await;
            let pool = pool_guard.as_ref().ok_or_else(|| {
                Error::Connection("Pulsar producer pool not initialized".to_string())
            })?;

            // Use producer pool
            if let Some(batching_config) = &self.config.batching {
                self.send_messages_batched_with_pool(
                    pool,
                    &topics,
                    &owned_payloads,
                    batching_config,
                )
                .await
            } else {
                self.send_messages_individually_with_pool(pool, &topics, &owned_payloads)
                    .await
            }
        } else {
            // Use single producer (original behavior)
            let mut producer_guard = self.producer.write().await;
            let producer = producer_guard
                .as_mut()
                .ok_or_else(|| Error::Connection("Pulsar producer not initialized".to_string()))?;

            if let Some(batching_config) = &self.config.batching {
                self.send_messages_batched(producer, &topics, &owned_payloads, batching_config)
                    .await
            } else {
                self.send_messages_individually(producer, &topics, &owned_payloads)
                    .await
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Close producer pool if active
        let mut pool_guard = self.producer_pool.write().await;
        *pool_guard = None;

        // Close producer if active
        let mut producer_guard = self.producer.write().await;
        *producer_guard = None;

        // Close Pulsar client
        let mut client_guard = self.client.write().await;
        if let Some(_client) = client_guard.take() {
            // Client will be dropped automatically
        }

        Ok(())
    }
}

impl PulsarOutput {
    async fn send_messages_individually(
        &self,
        producer: &mut PulsarProducer,
        _topics: &[String],
        payloads: &[Vec<u8>],
    ) -> Result<(), Error> {
        for (index, payload) in payloads.iter().enumerate() {
            // Send message to Pulsar with simple retry
            let mut attempts = 0;
            let max_attempts = 3;

            loop {
                attempts += 1;

                match producer.send_non_blocking(payload.clone()).await {
                    Ok(_) => {
                        tracing::debug!("Successfully sent message {} to Pulsar", index);
                        break;
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            error!(
                                "Failed to send message {} to Pulsar after {} attempts: {}",
                                index, attempts, e
                            );
                            return Err(Error::Process(format!(
                                "Failed to send message to Pulsar: {}",
                                e
                            )));
                        }
                        warn!(
                            "Attempt {} for message {} failed, retrying: {}",
                            attempts, index, e
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(100 * attempts as u64))
                            .await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_messages_batched(
        &self,
        producer: &mut PulsarProducer,
        _topics: &[String],
        payloads: &[Vec<u8>],
        batching_config: &PulsarBatchingConfig,
    ) -> Result<(), Error> {
        let max_messages = batching_config.max_messages.unwrap_or(1000);
        let _max_size = batching_config.max_size.unwrap_or(1024 * 1024); // 1MB default
        let max_delay = batching_config.max_delay_ms.unwrap_or(100); // 100ms default

        // Simple batching logic - send messages in batches
        for chunk in payloads.chunks(max_messages as usize) {
            // Send each message in the chunk
            for payload in chunk {
                if let Err(e) = producer.send_non_blocking(payload.clone()).await {
                    error!("Failed to send message to Pulsar: {}", e);
                    return Err(Error::Process(format!(
                        "Failed to send message to Pulsar: {}",
                        e
                    )));
                }
            }

            // Add small delay between batches if configured
            if max_delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(max_delay)).await;
            }
        }

        Ok(())
    }

    async fn send_messages_individually_with_pool(
        &self,
        pool: &PulsarProducerPool,
        _topics: &[String],
        payloads: &[Vec<u8>],
    ) -> Result<(), Error> {
        for (index, payload) in payloads.iter().enumerate() {
            // Send message to Pulsar with simple retry
            let mut attempts = 0;
            let max_attempts = 3;

            loop {
                attempts += 1;

                match pool.send_message(payload.clone()).await {
                    Ok(_) => {
                        tracing::debug!("Successfully sent message {} to Pulsar via pool", index);
                        break;
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            error!(
                                "Failed to send message {} to Pulsar after {} attempts: {}",
                                index, attempts, e
                            );
                            return Err(Error::Process(format!(
                                "Failed to send message to Pulsar: {}",
                                e
                            )));
                        }
                        warn!(
                            "Attempt {} for message {} failed, retrying: {}",
                            attempts, index, e
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(100 * attempts as u64))
                            .await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_messages_batched_with_pool(
        &self,
        pool: &PulsarProducerPool,
        _topics: &[String],
        payloads: &[Vec<u8>],
        batching_config: &PulsarBatchingConfig,
    ) -> Result<(), Error> {
        let max_messages = batching_config.max_messages.unwrap_or(1000);
        let _max_size = batching_config.max_size.unwrap_or(1024 * 1024); // 1MB default
        let max_delay = batching_config.max_delay_ms.unwrap_or(100); // 100ms default

        // Simple batching logic - send messages in batches
        for chunk in payloads.chunks(max_messages as usize) {
            // Send each message in the chunk
            for payload in chunk {
                if let Err(e) = pool.send_message(payload.clone()).await {
                    error!("Failed to send message to Pulsar via pool: {}", e);
                    return Err(Error::Process(format!(
                        "Failed to send message to Pulsar: {}",
                        e
                    )));
                }
            }

            // Add small delay between batches if configured
            if max_delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(max_delay)).await;
            }
        }

        Ok(())
    }

    /// Get producer pool statistics
    pub async fn get_pool_stats(&self) -> Result<Vec<(String, u32)>, Error> {
        let pool_guard = self.producer_pool.read().await;
        match pool_guard.as_ref() {
            Some(pool) => Ok(pool.get_pool_stats().await),
            None => Err(Error::Connection(
                "Producer pool not initialized".to_string(),
            )),
        }
    }
}

pub struct PulsarOutputBuilder;

impl OutputBuilder for PulsarOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Pulsar output configuration is missing".to_string(),
            ));
        }
        let config: PulsarOutputConfig = serde_json::from_value(config.clone().unwrap())?;

        // Validate configuration during build
        PulsarConfigValidator::validate_service_url(&config.service_url)?;

        // Note: We can't fully validate the topic here since it's an expression
        // that needs to be evaluated at runtime with a MessageBatch

        if let Some(ref auth) = config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        if let Some(ref retry_config) = config.retry_config {
            PulsarConfigValidator::validate_retry_config(retry_config)?;
        }

        if let Some(ref producer_pool_config) = config.producer_pool {
            PulsarConfigValidator::validate_producer_pool_config(producer_pool_config)?;
        }

        Ok(Arc::new(PulsarOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("pulsar", Arc::new(PulsarOutputBuilder))
}
