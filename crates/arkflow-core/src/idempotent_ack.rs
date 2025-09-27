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

//! Idempotent acknowledgment mechanism
//!
//! This module provides idempotent acknowledgment wrappers that prevent duplicate
//! acknowledgments and ensure exactly-once processing semantics.

use crate::input::Ack;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const ACK_CACHE_SIZE: usize = 10000;
const ACK_CACHE_TTL: Duration = Duration::from_secs(3600); // 1 hour

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AckId {
    pub source_id: String,
    pub message_id: String,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
}

impl AckId {
    pub fn new(source_id: String, message_id: String) -> Self {
        Self {
            source_id,
            message_id,
            partition: None,
            offset: None,
        }
    }

    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }
}

pub struct AckCache {
    acknowledged: Arc<Mutex<HashSet<AckId>>>,
    cache_timestamps: Arc<Mutex<Vec<(Instant, AckId)>>>,
}

impl AckCache {
    pub fn new() -> Self {
        Self {
            acknowledged: Arc::new(Mutex::new(HashSet::new())),
            cache_timestamps: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn is_acknowledged(&self, ack_id: &AckId) -> bool {
        self.cleanup_expired_entries().await;
        let acknowledged = self.acknowledged.lock().await;
        acknowledged.contains(ack_id)
    }

    pub async fn mark_acknowledged(&self, ack_id: AckId) -> bool {
        self.cleanup_expired_entries().await;

        let mut acknowledged = self.acknowledged.lock().await;
        let mut timestamps = self.cache_timestamps.lock().await;

        if acknowledged.contains(&ack_id) {
            return false; // Already acknowledged
        }

        acknowledged.insert(ack_id.clone());
        timestamps.push((Instant::now(), ack_id));

        // Enforce size limit
        if timestamps.len() > ACK_CACHE_SIZE {
            if let Some((_, oldest_id)) = timestamps.first() {
                acknowledged.remove(oldest_id);
                timestamps.remove(0);
            }
        }

        true
    }

    async fn cleanup_expired_entries(&self) {
        let mut acknowledged = self.acknowledged.lock().await;
        let mut timestamps = self.cache_timestamps.lock().await;

        let now = Instant::now();
        timestamps.retain(|(timestamp, ack_id)| {
            if now.duration_since(*timestamp) > ACK_CACHE_TTL {
                acknowledged.remove(ack_id);
                false
            } else {
                true
            }
        });
    }

    pub async fn clear(&self) {
        let mut acknowledged = self.acknowledged.lock().await;
        let mut timestamps = self.cache_timestamps.lock().await;
        acknowledged.clear();
        timestamps.clear();
    }
}

pub struct IdempotentAck {
    inner: Arc<dyn Ack>,
    ack_id: AckId,
    cache: Arc<AckCache>,
}

impl IdempotentAck {
    pub fn new(inner: Arc<dyn Ack>, ack_id: AckId, cache: Arc<AckCache>) -> Self {
        Self {
            inner,
            ack_id,
            cache,
        }
    }
}

#[async_trait]
impl Ack for IdempotentAck {
    async fn ack(&self) {
        if self.cache.mark_acknowledged(self.ack_id.clone()).await {
            self.inner.ack().await;
        } else {
            tracing::debug!("Duplicate acknowledgment for {:?}", self.ack_id);
        }
    }
}

pub struct DeduplicatingAck {
    inner: Arc<dyn Ack>,
    attempts: Arc<Mutex<HashSet<u64>>>,
    attempt_id: u64,
}

impl DeduplicatingAck {
    pub fn new(inner: Arc<dyn Ack>, attempt_id: u64, attempts: Arc<Mutex<HashSet<u64>>>) -> Self {
        Self {
            inner,
            attempts,
            attempt_id,
        }
    }
}

#[async_trait]
impl Ack for DeduplicatingAck {
    async fn ack(&self) {
        let mut attempts = self.attempts.lock().await;

        if attempts.contains(&self.attempt_id) {
            tracing::debug!("Duplicate ack attempt {}", self.attempt_id);
            return;
        }

        attempts.insert(self.attempt_id);
        drop(attempts); // Release lock before async operation

        self.inner.ack().await;
    }
}

pub struct RetryableAck {
    inner: Arc<dyn Ack>,
    max_retries: u32,
    retry_delay: Duration,
}

impl RetryableAck {
    pub fn new(inner: Arc<dyn Ack>) -> Self {
        Self {
            inner,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }
}

#[async_trait]
impl Ack for RetryableAck {
    async fn ack(&self) {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match tokio::time::timeout(self.retry_delay * (attempt + 1), self.inner.ack()).await {
                Ok(_) => return, // Success
                Err(timeout_error) => {
                    last_error = Some(timeout_error);
                    if attempt < self.max_retries {
                        tracing::warn!("Ack attempt {} timed out, retrying...", attempt + 1);
                        tokio::time::sleep(self.retry_delay * (attempt + 1)).await;
                    }
                }
            }
        }

        if let Some(error) = last_error {
            tracing::error!("Ack failed after {} retries: {:?}", self.max_retries, error);
        }
    }
}

pub struct TracedAck {
    inner: Arc<dyn Ack>,
    ack_id: AckId,
    start_time: Instant,
}

impl TracedAck {
    pub fn new(inner: Arc<dyn Ack>, ack_id: AckId) -> Self {
        Self {
            inner,
            ack_id,
            start_time: Instant::now(),
        }
    }
}

#[async_trait]
impl Ack for TracedAck {
    async fn ack(&self) {
        tracing::debug!("Starting ack for {:?}", self.ack_id);

        let result = self.inner.ack().await;

        let duration = self.start_time.elapsed();
        tracing::debug!("Ack completed for {:?} in {:?}", self.ack_id, duration);

        if duration > Duration::from_millis(100) {
            tracing::warn!("Slow ack detected for {:?}: {:?}", self.ack_id, duration);
        }

        result
    }
}

pub struct CompositeAck {
    acks: Vec<Arc<dyn Ack>>,
}

impl CompositeAck {
    pub fn new(acks: Vec<Arc<dyn Ack>>) -> Self {
        Self { acks }
    }
}

#[async_trait]
impl Ack for CompositeAck {
    async fn ack(&self) {
        let futures: Vec<_> = self.acks.iter().map(|ack| ack.ack()).collect();

        // Use join_all to wait for all acks to complete
        futures::future::join_all(futures).await;
        tracing::debug!("All composite acks completed");
    }
}

pub struct AckBuilder {
    inner: Arc<dyn Ack>,
    ack_id: Option<AckId>,
    cache: Option<Arc<AckCache>>,
    attempts: Option<Arc<Mutex<HashSet<u64>>>>,
    attempt_id: Option<u64>,
    enable_tracing: bool,
}

impl AckBuilder {
    pub fn new(inner: Arc<dyn Ack>) -> Self {
        Self {
            inner,
            ack_id: None,
            cache: None,
            attempts: None,
            attempt_id: None,
            enable_tracing: false,
        }
    }

    pub fn with_ack_id(mut self, ack_id: AckId) -> Self {
        self.ack_id = Some(ack_id);
        self
    }

    pub fn with_cache(mut self, cache: Arc<AckCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn with_deduplication(
        mut self,
        attempt_id: u64,
        attempts: Arc<Mutex<HashSet<u64>>>,
    ) -> Self {
        self.attempts = Some(attempts);
        self.attempt_id = Some(attempt_id);
        self
    }

    pub fn with_tracing(mut self) -> Self {
        self.enable_tracing = true;
        self
    }

    pub fn build(self) -> Arc<dyn Ack> {
        let mut ack: Arc<dyn Ack> = self.inner;
        let ack_id_for_tracing = self.ack_id.clone();

        // Add idempotency if cache and ack_id are provided
        if let (Some(cache), Some(ack_id)) = (self.cache, self.ack_id) {
            ack = Arc::new(IdempotentAck::new(ack, ack_id, cache));
        }

        // Add deduplication if provided
        if let (Some(attempts), Some(attempt_id)) = (self.attempts, self.attempt_id) {
            ack = Arc::new(DeduplicatingAck::new(ack, attempt_id, attempts));
        }

        // Add retryable behavior
        ack = Arc::new(RetryableAck::new(ack));

        // Add tracing if enabled
        if self.enable_tracing {
            if let Some(ack_id) = ack_id_for_tracing {
                ack = Arc::new(TracedAck::new(ack, ack_id));
            }
        }

        ack
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::NoopAck;

    #[tokio::test]
    async fn test_ack_cache() {
        let cache = AckCache::new();
        let ack_id = AckId::new("test_source".to_string(), "test_message".to_string());

        assert!(!cache.is_acknowledged(&ack_id).await);
        assert!(cache.mark_acknowledged(ack_id.clone()).await);
        assert!(cache.is_acknowledged(&ack_id).await);
        assert!(!cache.mark_acknowledged(ack_id.clone()).await); // Duplicate
    }

    #[tokio::test]
    async fn test_idempotent_ack() {
        let cache = Arc::new(AckCache::new());
        let ack_id = AckId::new("test_source".to_string(), "test_message".to_string());
        let inner = Arc::new(NoopAck);

        let idempotent_ack = IdempotentAck::new(inner.clone(), ack_id.clone(), cache.clone());

        // First ack should succeed
        idempotent_ack.ack().await;

        // Second ack should be ignored
        idempotent_ack.ack().await;

        // Verify it's marked as acknowledged
        assert!(cache.is_acknowledged(&ack_id).await);
    }

    #[tokio::test]
    async fn test_ack_builder() {
        let cache = Arc::new(AckCache::new());
        let ack_id = AckId::new("test_source".to_string(), "test_message".to_string());
        let inner = Arc::new(NoopAck);

        let ack = AckBuilder::new(inner)
            .with_ack_id(ack_id.clone())
            .with_cache(cache.clone())
            .with_tracing()
            .build();

        ack.ack().await;

        assert!(cache.is_acknowledged(&ack_id).await);
    }

    #[tokio::test]
    async fn test_deduplicating_ack() {
        let attempts = Arc::new(Mutex::new(HashSet::new()));
        let inner = Arc::new(NoopAck);
        let attempt_id = 42;

        let dedup_ack = DeduplicatingAck::new(inner.clone(), attempt_id, attempts.clone());

        // First ack should succeed
        dedup_ack.ack().await;

        // Second ack should be ignored
        dedup_ack.ack().await;

        // Verify attempt was recorded
        let attempts_guard = attempts.lock().await;
        assert!(attempts_guard.contains(&attempt_id));
    }
}
