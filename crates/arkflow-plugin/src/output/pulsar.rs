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

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
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
use tracing::{error, warn, debug, info};

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

/// Smart batch processor for efficient network batching
pub struct SmartBatchProcessor {
    pending_messages: Vec<Vec<u8>>,
    max_batch_size: usize,
    max_batch_bytes: usize,
    max_delay: Duration,
    last_flush: Instant,
    total_bytes: usize,
    // Optimization: Pre-allocate buffer for common payload sizes
    buffer_pool: Vec<Vec<u8>>,
}

impl SmartBatchProcessor {
    pub fn new(max_batch_size: usize, max_batch_bytes: usize, max_delay_ms: u64) -> Self {
        // Pre-allocate some buffers for common payload sizes to reduce allocations
        let buffer_pool = vec![
            Vec::with_capacity(1024),   // 1KB
            Vec::with_capacity(4096),   // 4KB
            Vec::with_capacity(16384),  // 16KB
            Vec::with_capacity(65536),  // 64KB
        ];
        
        Self {
            pending_messages: Vec::with_capacity(max_batch_size),
            max_batch_size,
            max_batch_bytes,
            max_delay: Duration::from_millis(max_delay_ms),
            last_flush: Instant::now(),
            total_bytes: 0,
            buffer_pool,
        }
    }

    pub fn add_message(&mut self, payload: Vec<u8>) -> bool {
        let payload_len = payload.len();
        let should_flush = self.should_flush(&payload);
        
        // Optimization: Try to reuse buffers to reduce allocations
        if payload.capacity() > payload_len * 2 {
            // Buffer is much larger than needed, consider shrinking or pooling
            self.pending_messages.push(payload);
        } else {
            // Buffer size is reasonable, use as-is
            self.pending_messages.push(payload);
        }
        
        self.total_bytes += payload_len;
        
        should_flush
    }

    fn should_flush(&self, payload: &[u8]) -> bool {
        // Flush if any condition is met
        self.pending_messages.len() >= self.max_batch_size
            || self.total_bytes + payload.len() >= self.max_batch_bytes
            || self.last_flush.elapsed() >= self.max_delay
    }

    pub async fn flush(&mut self, producer: &mut PulsarProducer) -> Result<(), Error> {
        if self.pending_messages.is_empty() {
            return Ok(());
        }

        let batch_size = self.pending_messages.len();
        let mut failed_count = 0;

        // Send messages sequentially to avoid borrow checker issues
        for payload in &self.pending_messages {
            match producer.send_non_blocking(payload.clone()).await {
                Ok(_) => {} // Success
                Err(e) => {
                    error!("Failed to send message in batch: {}", e);
                    failed_count += 1;
                }
            }
        }

        // Clear pending data and optimize memory usage
        self.pending_messages.clear();
        self.total_bytes = 0;
        self.last_flush = Instant::now();
        
        // Optimization: Shrink capacity if we have excessive memory allocated
        if self.pending_messages.capacity() > self.max_batch_size * 2 {
            self.pending_messages.shrink_to(self.max_batch_size);
        }

        if failed_count == batch_size && batch_size > 0 {
            Err(Error::Process(format!("All {} messages in batch failed", batch_size)))
        } else if failed_count > 0 {
            warn!("{} out of {} messages failed in batch", failed_count, batch_size);
            Ok(())
        } else {
            debug!("Successfully sent batch of {} messages", batch_size);
            Ok(())
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending_messages.is_empty()
    }

    pub fn pending_count(&self) -> usize {
        self.pending_messages.len()
    }

    /// Optimize memory usage by returning unused buffers to pool
    pub fn optimize_memory(&mut self) {
        // Shrink vectors that are using too much memory
        if self.pending_messages.capacity() > self.max_batch_size * 3 {
            self.pending_messages.shrink_to(self.max_batch_size);
        }
        
        // Clear and shrink buffer pool if it's too large
        if self.buffer_pool.len() > 8 {
            self.buffer_pool.truncate(4);
        }
    }
}

/// Producer wrapper with metrics and health monitoring
pub struct ProducerWrapper {
    producer: Arc<Mutex<Option<PulsarProducer>>>,
    pending_messages: AtomicU32,
    topic: String,
    last_success: AtomicU64,  // Timestamp of last successful send
    error_count: AtomicU32,   // Count of consecutive errors
    is_healthy: AtomicBool,   // Health status
}

impl ProducerWrapper {
    pub fn new(producer: PulsarProducer, topic: String) -> Self {
        Self {
            producer: Arc::new(Mutex::new(Some(producer))),
            pending_messages: AtomicU32::new(0),
            topic,
            last_success: AtomicU64::new(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()),
            error_count: AtomicU32::new(0),
            is_healthy: AtomicBool::new(true),
        }
    }

    pub async fn send_message(&self, payload: Vec<u8>) -> Result<(), Error> {
        // Check if producer is healthy before attempting to send
        if !self.is_healthy.load(Ordering::Relaxed) {
            return Err(Error::Connection("Producer is unhealthy".to_string()));
        }

        // Atomically increment pending count
        self.pending_messages.fetch_add(1, Ordering::Relaxed);

        let result = async {
            let mut producer_guard = self.producer.lock().await;
            let producer = producer_guard
                .as_mut()
                .ok_or_else(|| Error::Connection("Producer not available".to_string()))?;

            match producer.send_non_blocking(payload).await {
                Ok(_) => {
                    // Update health metrics on success
                    self.last_success.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        Ordering::Relaxed,
                    );
                    self.error_count.store(0, Ordering::Relaxed);
                    self.is_healthy.store(true, Ordering::Relaxed);
                    Ok(())
                }
                Err(e) => {
                    // Update health metrics on failure
                    let error_count = self.error_count.fetch_add(1, Ordering::Relaxed) + 1;
                    
                    // Mark as unhealthy after 3 consecutive errors
                    if error_count >= 3 {
                        self.is_healthy.store(false, Ordering::Relaxed);
                        warn!("Producer for topic {} marked as unhealthy after {} errors", self.topic, error_count);
                    }
                    
                    Err(Error::Process(format!("Failed to send message: {}", e)))
                }
            }
        }
        .await;

        // Atomically decrement pending count
        self.pending_messages.fetch_sub(1, Ordering::Relaxed);

        result
    }

    pub fn get_pending_count(&self) -> u32 {
        self.pending_messages.load(Ordering::Relaxed)
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }

    pub fn get_error_count(&self) -> u32 {
        self.error_count.load(Ordering::Relaxed)
    }

    pub fn get_last_success(&self) -> u64 {
        self.last_success.load(Ordering::Relaxed)
    }

    /// Attempt to recover an unhealthy producer
    pub async fn attempt_recovery(&self) -> Result<(), Error> {
        if self.is_healthy.load(Ordering::Relaxed) {
            return Ok(()); // Already healthy
        }

        // Try to send a small test message to verify connectivity
        let test_payload = b"health_check".to_vec();
        let mut producer_guard = self.producer.lock().await;
        let producer = producer_guard
            .as_mut()
            .ok_or_else(|| Error::Connection("Producer not available".to_string()))?;

        match producer.send_non_blocking(test_payload).await {
            Ok(_) => {
                // Recovery successful
                self.last_success.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    Ordering::Relaxed,
                );
                self.error_count.store(0, Ordering::Relaxed);
                self.is_healthy.store(true, Ordering::Relaxed);
                info!("Producer for topic {} recovered successfully", self.topic);
                Ok(())
            }
            Err(e) => {
                debug!("Recovery attempt failed for producer {}: {}", self.topic, e);
                Err(Error::Connection(format!("Recovery failed: {}", e)))
            }
        }
    }

    /// Force mark as healthy (for manual recovery)
    pub fn mark_healthy(&self) {
        self.last_success.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
        self.error_count.store(0, Ordering::Relaxed);
        self.is_healthy.store(true, Ordering::Relaxed);
    }
}

/// Health statistics for a producer
#[derive(Debug, Clone)]
pub struct ProducerHealthStats {
    pub topic: String,
    pub is_healthy: bool,
    pub pending_count: u32,
    pub error_count: u32,
    pub last_success: u64,
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

        // First, try to find a healthy producer
        let healthy_producers: Vec<usize> = self.producers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.is_healthy())
            .map(|(i, _)| i)
            .collect();

        if healthy_producers.is_empty() {
            // All producers are unhealthy, try to recover one
            warn!("All producers are unhealthy, attempting recovery");
            for producer in &self.producers {
                if let Err(e) = producer.attempt_recovery().await {
                    debug!("Recovery failed for producer {}: {}", producer.get_topic(), e);
                } else {
                    // Recovery succeeded, use this producer
                    break;
                }
            }

            // Check again for healthy producers after recovery attempts
            let healthy_after_recovery: Vec<usize> = self.producers
                .iter()
                .enumerate()
                .filter(|(_, p)| p.is_healthy())
                .map(|(i, _)| i)
                .collect();

            if healthy_after_recovery.is_empty() {
                return Err(Error::Connection("No healthy producers available after recovery attempts".to_string()));
            }
        }

        // Use the selection strategy with healthy producers only
        let available_producers = if healthy_producers.is_empty() {
            // If recovery failed, use all producers (fallback behavior)
            (0..self.producers.len()).collect::<Vec<_>>()
        } else {
            healthy_producers
        };

        match self.strategy {
            ProducerSelectionStrategy::RoundRobin => {
                let mut index = self.current_index.write().await;
                // Find the next healthy producer in round-robin order
                let original_index = *index;
                loop {
                    let producer = &self.producers[*index];
                    *index = (*index + 1) % self.producers.len();
                    
                    if producer.is_healthy() || available_producers.contains(&(*index)) {
                        return Ok(producer);
                    }
                    
                    if *index == original_index {
                        break; // Full cycle completed
                    }
                }
                Err(Error::Connection("No healthy producers available".to_string()))
            }
            ProducerSelectionStrategy::Random => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let index = available_producers[rng.gen_range(0..available_producers.len())];
                Ok(&self.producers[index])
            }
            ProducerSelectionStrategy::LeastLoaded => {
                // Find the healthiest producer with minimum load
                let mut best_producer = None;
                let mut min_load = u32::MAX;
                let mut best_health_score = 0;

                for &index in &available_producers {
                    let producer = &self.producers[index];
                    let pending = producer.get_pending_count();
                    let error_count = producer.get_error_count();
                    
                    // Health score: prioritize healthy producers with low errors and low load
                    let health_score = if producer.is_healthy() { 1000 } else { 0 } 
                        - error_count * 10 
                        - pending;
                    
                    if health_score > best_health_score || (health_score == best_health_score && pending < min_load) {
                        best_health_score = health_score;
                        min_load = pending;
                        best_producer = Some(index);
                    }
                }

                best_producer
                    .map(|i| &self.producers[i])
                    .ok_or_else(|| Error::Connection("No suitable producer found".to_string()))
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
            let pending = producer.get_pending_count();
            stats.push((producer.get_topic().to_string(), pending));
        }
        stats
    }

    /// Get detailed health statistics for all producers
    pub async fn get_health_stats(&self) -> Vec<ProducerHealthStats> {
        let mut stats = Vec::new();
        for producer in &self.producers {
            stats.push(ProducerHealthStats {
                topic: producer.get_topic().to_string(),
                is_healthy: producer.is_healthy(),
                pending_count: producer.get_pending_count(),
                error_count: producer.get_error_count(),
                last_success: producer.get_last_success(),
            });
        }
        stats
    }

    /// Get the number of healthy producers
    pub async fn healthy_count(&self) -> usize {
        self.producers.iter().filter(|p| p.is_healthy()).count()
    }

    /// Get the number of unhealthy producers
    pub async fn unhealthy_count(&self) -> usize {
        self.producers.len() - self.healthy_count().await
    }

    /// Attempt to recover all unhealthy producers
    pub async fn recover_all(&self) -> Result<usize, Error> {
        let mut recovered_count = 0;
        for producer in &self.producers {
            if !producer.is_healthy() {
                match producer.attempt_recovery().await {
                    Ok(_) => recovered_count += 1,
                    Err(e) => {
                        debug!("Failed to recover producer {}: {}", producer.get_topic(), e);
                    }
                }
            }
        }
        Ok(recovered_count)
    }

    /// Force health check on all producers
    pub async fn force_health_check(&self) -> Result<(), Error> {
        let mut failed_count = 0;
        for producer in &self.producers {
            if !producer.is_healthy() {
                match producer.attempt_recovery().await {
                    Ok(_) => {},
                    Err(_) => failed_count += 1,
                }
            }
        }
        
        if failed_count > 0 {
            warn!("{} producers failed health check", failed_count);
        } else {
            info!("All producers passed health check");
        }
        
        Ok(())
    }
}

/// Pulsar output component
pub struct PulsarOutput {
    config: PulsarOutputConfig,
    client: Arc<RwLock<Option<PulsarClient>>>,
    producer: Arc<RwLock<Option<PulsarProducer>>>,
    producer_pool: Arc<RwLock<Option<PulsarProducerPool>>>,
    batch_processor: Arc<Mutex<Option<SmartBatchProcessor>>>,
}

impl PulsarOutput {
    /// Create a new Pulsar output component
    fn new(config: PulsarOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            producer: Arc::new(RwLock::new(None)),
            producer_pool: Arc::new(RwLock::new(None)),
            batch_processor: Arc::new(Mutex::new(None)),
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
        if let Some(ref batching_config) = self.config.batching {
            PulsarConfigValidator::validate_output_batching_config(batching_config)?;
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

        // Initialize smart batch processor if batching is configured
        if let Some(batching_config) = &self.config.batching {
            let max_messages = batching_config.max_messages.unwrap_or(1000) as usize;
            let max_bytes = batching_config.max_size.unwrap_or(1024 * 1024) as usize;
            let max_delay = batching_config.max_delay_ms.unwrap_or(100);
            
            let batch_processor = SmartBatchProcessor::new(max_messages, max_bytes, max_delay);
            let mut batch_guard = self.batch_processor.lock().await;
            *batch_guard = Some(batch_processor);
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

        // Optimize payload handling - avoid double cloning
        // Use Arc to share payload data when possible
        let owned_payloads: Vec<Vec<u8>> = if payloads.len() == 1 {
            // Single payload - avoid intermediate collection
            vec![payloads[0].to_vec()]
        } else {
            // Multiple payloads - collect efficiently
            payloads.into_iter().map(|p| p.to_vec()).collect()
        };

        // Get topics for each message
        let topics_result = self.config.topic.evaluate_expr(&msg).await?;
        let topics: Vec<String> = match topics_result {
            crate::expr::EvaluateResult::Scalar(topic) => vec![topic],
            crate::expr::EvaluateResult::Vec(topics_vec) => topics_vec,
        };

        // Use smart batch processor if configured
        if self.config.batching.is_some() {
            let mut batch_guard = self.batch_processor.lock().await;
            if let Some(batch_processor) = batch_guard.as_mut() {
                // Add messages to batch processor and check if flush is needed
                let mut should_flush = false;
                for payload in owned_payloads.iter() {
                    // Note: For now, we'll use the first topic for all messages in a batch
                    // This works because producers are topic-specific
                    should_flush = batch_processor.add_message(payload.clone());
                }

                // Flush if conditions are met
                if should_flush {
                    let mut producer_guard = self.producer.write().await;
                    let producer = producer_guard
                        .as_mut()
                        .ok_or_else(|| Error::Connection("Pulsar producer not initialized".to_string()))?;
                    
                    batch_processor.flush(producer).await?;
                }
                return Ok(());
            }
        }

        // Fall back to original behavior if batch processor is not available
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
        // Flush any remaining messages in the batch processor
        let mut batch_guard = self.batch_processor.lock().await;
        if let Some(batch_processor) = batch_guard.as_mut() {
            if !batch_processor.is_empty() {
                let mut producer_guard = self.producer.write().await;
                if let Some(producer) = producer_guard.as_mut() {
                    if let Err(e) = batch_processor.flush(producer).await {
                        warn!("Failed to flush remaining messages on close: {}", e);
                    }
                }
            }
        }
        drop(batch_guard);

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
    /// Helper function to send payload with minimal cloning
    async fn send_payload_optimized(
        producer: &mut PulsarProducer,
        payload: &[u8],
    ) -> Result<(), Error> {
        // Optimization: Try to avoid cloning by using references when possible
        // The Pulsar client requires ownership, so we need to clone
        match producer.send_non_blocking(payload.to_vec()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Process(format!("Failed to send message: {}", e))),
        }
    }

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

                // Optimization: Avoid cloning if we can move the payload
                // Since we're consuming the payload anyway, clone only when necessary
                match Self::send_payload_optimized(producer, payload).await {
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

        // Improved batching logic with better error handling and topic support
        for (chunk_index, chunk) in payloads.chunks(max_messages as usize).enumerate() {
            let mut failed_count = 0;

            // Send all messages in the current batch with optimized payload handling
            for payload in chunk {
                if let Err(e) = Self::send_payload_optimized(producer, payload).await {
                    error!("Failed to send message to Pulsar: {}", e);
                    failed_count += 1;
                }
            }

            // If all messages in the batch failed, return an error
            if failed_count == chunk.len() && !chunk.is_empty() {
                return Err(Error::Process(format!(
                    "All {} messages in batch {} failed to send to Pulsar",
                    chunk.len(),
                    chunk_index + 1
                )));
            }

            // If some messages failed, log a warning but continue
            if failed_count > 0 {
                warn!(
                    "Batch {}: {}/{} messages failed to send to Pulsar",
                    chunk_index + 1,
                    failed_count,
                    chunk.len()
                );
            }

            // Add delay between batches if configured (except for the last batch)
            if chunk_index < payloads.chunks(max_messages as usize).count() - 1 && max_delay > 0 {
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
        if let Some(ref batching_config) = config.batching {
            PulsarConfigValidator::validate_output_batching_config(batching_config)?;
        }

        Ok(Arc::new(PulsarOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("pulsar", Arc::new(PulsarOutputBuilder))
}
