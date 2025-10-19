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

//! Enhanced acknowledgment task with smart retry capabilities

use crate::distributed_ack_error::{DistributedAckError, RetryConfig};
use crate::input::Ack;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Enhanced acknowledgment task with smart retry capabilities
#[derive(Clone)]
pub struct EnhancedAckTask {
    ack: Arc<dyn Ack>,
    retry_count: u32,
    created_at: Instant,
    last_attempt: Option<Instant>,
    next_retry_delay: Duration,
    sequence: u64,
    ack_type: String,
    payload: Vec<u8>,
    retry_config: RetryConfig,
    permanent_failure: bool,
}

impl std::fmt::Debug for EnhancedAckTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnhancedAckTask")
            .field("retry_count", &self.retry_count)
            .field("created_at", &self.created_at)
            .field("last_attempt", &self.last_attempt)
            .field("next_retry_delay", &self.next_retry_delay)
            .field("sequence", &self.sequence)
            .field("ack_type", &self.ack_type)
            .field("payload_len", &self.payload.len())
            .field("permanent_failure", &self.permanent_failure)
            .finish()
    }
}

impl EnhancedAckTask {
    /// Create a new enhanced acknowledgment task
    pub fn new(
        ack: Arc<dyn Ack>,
        sequence: u64,
        ack_type: String,
        payload: Vec<u8>,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            ack,
            retry_count: 0,
            created_at: Instant::now(),
            last_attempt: None,
            next_retry_delay: retry_config.next_delay(0),
            sequence,
            ack_type,
            payload,
            retry_config,
            permanent_failure: false,
        }
    }

    /// Get the acknowledgment object
    pub fn ack(&self) -> &Arc<dyn Ack> {
        &self.ack
    }

    /// Get the retry count
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Get the creation time
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Get the last attempt time
    pub fn last_attempt(&self) -> Option<Instant> {
        self.last_attempt
    }

    /// Get the next retry delay
    pub fn next_retry_delay(&self) -> Duration {
        self.next_retry_delay
    }

    /// Get the sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Get the acknowledgment type
    pub fn ack_type(&self) -> &str {
        &self.ack_type
    }

    /// Get the payload
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Check if the task is expired
    pub fn is_expired(&self, timeout_ms: u64) -> bool {
        self.created_at.elapsed() > Duration::from_millis(timeout_ms)
    }

    /// Check if the task is a permanent failure
    pub fn is_permanent_failure(&self) -> bool {
        self.permanent_failure
    }

    /// Mark the task as a permanent failure
    pub fn mark_permanent_failure(&mut self) {
        self.permanent_failure = true;
    }

    /// Check if the task should be retried
    pub fn should_retry(&self, error: Option<&DistributedAckError>) -> bool {
        if self.permanent_failure {
            return false;
        }

        if self.retry_count >= self.retry_config.max_retries {
            return false;
        }

        if let Some(error) = error {
            self.retry_config.should_retry(error, self.retry_count)
        } else {
            true
        }
    }

    /// Check if the task is ready for retry
    pub fn is_ready_for_retry(&self) -> bool {
        if let Some(last_attempt) = self.last_attempt {
            last_attempt.elapsed() >= self.next_retry_delay
        } else {
            true
        }
    }

    /// Increment the retry count and calculate next delay
    pub fn increment_retry(
        &mut self,
        error: Option<&DistributedAckError>,
    ) -> Result<(), DistributedAckError> {
        if !self.should_retry(error) {
            return Err(DistributedAckError::Retry(format!(
                "Max retries exceeded for task sequence {}",
                self.sequence
            )));
        }

        self.retry_count += 1;
        self.last_attempt = Some(Instant::now());
        self.next_retry_delay = self.retry_config.next_delay(self.retry_count);

        Ok(())
    }

    /// Calculate time until next retry
    pub fn time_until_next_retry(&self) -> Duration {
        if let Some(last_attempt) = self.last_attempt {
            let elapsed = last_attempt.elapsed();
            if elapsed < self.next_retry_delay {
                self.next_retry_delay - elapsed
            } else {
                Duration::from_millis(0)
            }
        } else {
            Duration::from_millis(0)
        }
    }

    /// Get the time elapsed since creation
    pub fn elapsed_since_creation(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get the time elapsed since last attempt
    pub fn elapsed_since_last_attempt(&self) -> Option<Duration> {
        self.last_attempt.map(|last| last.elapsed())
    }

    /// Convert to AckRecord for persistence
    pub fn to_record(&self) -> AckRecord {
        AckRecord {
            sequence: self.sequence,
            ack_type: self.ack_type.clone(),
            payload: self.payload.clone(),
            retry_count: self.retry_count,
            created_at: SystemTime::now()
                .checked_sub(self.elapsed_since_creation())
                .unwrap_or(SystemTime::now()),
            last_retry: self.last_attempt.map(|last| {
                SystemTime::now()
                    .checked_sub(last.elapsed())
                    .unwrap_or(SystemTime::now())
            }),
        }
    }

    /// Create from AckRecord
    pub fn from_record(record: AckRecord, ack: Arc<dyn Ack>, retry_config: RetryConfig) -> Self {
        let elapsed = record.created_at.elapsed().unwrap_or_default();
        let retry_delay = retry_config.next_delay(record.retry_count);

        Self {
            ack,
            retry_count: record.retry_count,
            created_at: Instant::now()
                .checked_sub(elapsed)
                .unwrap_or(Instant::now()),
            last_attempt: record.last_retry.map(|last| {
                let last_elapsed = last.elapsed().unwrap_or_default();
                Instant::now()
                    .checked_sub(last_elapsed)
                    .unwrap_or(Instant::now())
            }),
            next_retry_delay: retry_delay,
            sequence: record.sequence,
            ack_type: record.ack_type,
            payload: record.payload,
            retry_config,
            permanent_failure: false,
        }
    }

    /// Get task statistics
    pub fn get_stats(&self) -> AckTaskStats {
        AckTaskStats {
            sequence: self.sequence,
            retry_count: self.retry_count,
            elapsed_since_creation: self.elapsed_since_creation(),
            elapsed_since_last_attempt: self.elapsed_since_last_attempt(),
            next_retry_delay: self.next_retry_delay,
            is_expired: self.is_expired(30000), // 30 seconds default
            is_permanent_failure: self.permanent_failure,
            is_ready_for_retry: self.is_ready_for_retry(),
        }
    }
}

/// Acknowledgment task statistics
#[derive(Debug, Clone)]
pub struct AckTaskStats {
    pub sequence: u64,
    pub retry_count: u32,
    pub elapsed_since_creation: Duration,
    pub elapsed_since_last_attempt: Option<Duration>,
    pub next_retry_delay: Duration,
    pub is_expired: bool,
    pub is_permanent_failure: bool,
    pub is_ready_for_retry: bool,
}

/// Task pool for managing enhanced acknowledgment tasks
pub struct AckTaskPool {
    tasks: Arc<tokio::sync::RwLock<Vec<EnhancedAckTask>>>,
    _retry_config: RetryConfig,
}

impl AckTaskPool {
    /// Create a new task pool
    pub fn new(retry_config: RetryConfig) -> Self {
        Self {
            tasks: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            _retry_config: retry_config,
        }
    }

    /// Add a task to the pool
    pub async fn add_task(&self, task: EnhancedAckTask) {
        let mut tasks = self.tasks.write().await;
        tasks.push(task);
    }

    /// Get tasks that are ready for retry
    pub async fn get_ready_tasks(&self) -> Vec<EnhancedAckTask> {
        let mut tasks = self.tasks.write().await;
        let ready_tasks: Vec<EnhancedAckTask> = tasks
            .iter()
            .filter(|task| task.is_ready_for_retry() && !task.is_permanent_failure())
            .cloned()
            .collect();

        // Remove the ready tasks from the pool
        tasks.retain(|task| {
            !ready_tasks
                .iter()
                .any(|ready| ready.sequence == task.sequence)
        });

        ready_tasks
    }

    /// Get expired tasks
    pub async fn get_expired_tasks(&self, timeout_ms: u64) -> Vec<EnhancedAckTask> {
        let mut tasks = self.tasks.write().await;
        let expired_tasks: Vec<EnhancedAckTask> = tasks
            .iter()
            .filter(|task| task.is_expired(timeout_ms))
            .cloned()
            .collect();

        // Remove expired tasks
        tasks.retain(|task| {
            !expired_tasks
                .iter()
                .any(|expired| expired.sequence == task.sequence)
        });

        expired_tasks
    }

    /// Get all tasks in the pool
    pub async fn get_all_tasks(&self) -> Vec<EnhancedAckTask> {
        let tasks = self.tasks.read().await;
        tasks.clone()
    }

    /// Clear all tasks
    pub async fn clear(&self) {
        let mut tasks = self.tasks.write().await;
        tasks.clear();
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> AckTaskPoolStats {
        let tasks = self.tasks.read().await;
        let total_tasks = tasks.len();
        let retrying_tasks = tasks.iter().filter(|task| task.retry_count > 0).count();
        let expired_tasks = tasks.iter().filter(|task| task.is_expired(30000)).count();
        let permanent_failures = tasks
            .iter()
            .filter(|task| task.is_permanent_failure())
            .count();

        AckTaskPoolStats {
            total_tasks,
            retrying_tasks,
            expired_tasks,
            permanent_failures,
            active_tasks: total_tasks - expired_tasks - permanent_failures,
        }
    }
}

/// Task pool statistics
#[derive(Debug, Clone)]
pub struct AckTaskPoolStats {
    pub total_tasks: usize,
    pub active_tasks: usize,
    pub retrying_tasks: usize,
    pub expired_tasks: usize,
    pub permanent_failures: usize,
}

// Re-export AckRecord for compatibility
pub use crate::reliable_ack::AckRecord;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone)]
    struct TestAck {
        result: Result<(), String>,
        call_count: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Ack for TestAck {
        async fn ack(&self) -> Result<(), String> {
            self.call_count.store(true, Ordering::Relaxed);
            self.result.clone()
        }
    }

    #[tokio::test]
    async fn test_enhanced_ack_task_creation() {
        let retry_config = RetryConfig::default();
        let ack = Arc::new(TestAck {
            result: Ok(()),
            call_count: Arc::new(AtomicBool::new(false)),
        });

        let task = EnhancedAckTask::new(ack, 1, "test".to_string(), b"test".to_vec(), retry_config);

        assert_eq!(task.sequence(), 1);
        assert_eq!(task.retry_count(), 0);
        assert!(!task.is_permanent_failure());
        assert!(task.should_retry(None));
    }

    #[tokio::test]
    async fn test_retry_logic() {
        let retry_config = RetryConfig::default();
        let ack = Arc::new(TestAck {
            result: Ok(()),
            call_count: Arc::new(AtomicBool::new(false)),
        });

        let mut task = EnhancedAckTask::new(
            ack,
            1,
            "test".to_string(),
            b"test".to_vec(),
            retry_config.clone(),
        );

        // Test increment retry
        assert!(task.increment_retry(None).is_ok());
        assert_eq!(task.retry_count(), 1);
        assert!(task.last_attempt().is_some());

        // Test retry delay calculation
        let delay = task.next_retry_delay();
        assert!(delay > Duration::from_millis(0));

        // Test max retries
        for _ in 0..retry_config.max_retries {
            let _ = task.increment_retry(None);
        }
        assert!(task.increment_retry(None).is_err());
    }

    #[tokio::test]
    async fn test_task_pool() {
        let retry_config = RetryConfig::default();
        let pool = AckTaskPool::new(retry_config.clone());

        let ack = Arc::new(TestAck {
            result: Ok(()),
            call_count: Arc::new(AtomicBool::new(false)),
        });

        let task = EnhancedAckTask::new(ack, 1, "test".to_string(), b"test".to_vec(), retry_config);

        // Add task to pool
        pool.add_task(task.clone()).await;

        // Get all tasks
        let all_tasks = pool.get_all_tasks().await;
        assert_eq!(all_tasks.len(), 1);

        // Get pool stats
        let stats = pool.get_stats().await;
        assert_eq!(stats.total_tasks, 1);
        assert_eq!(stats.active_tasks, 1);

        // Clear pool
        pool.clear().await;
        let stats = pool.get_stats().await;
        assert_eq!(stats.total_tasks, 0);
    }
}
