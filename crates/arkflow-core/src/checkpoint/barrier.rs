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

//! Barrier mechanism for aligned checkpoints
//!
//! This module implements Flink-style barrier injection for consistent distributed snapshots.
//! Barriers flow through the stream processing pipeline, ensuring all processors are aligned
//! at the same checkpoint point.

use super::{CheckpointId, CheckpointResult};
use crate::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::{timeout, Instant};

/// Unique identifier for a barrier
pub type BarrierId = u64;

/// Barrier injected into the stream for checkpoint alignment
#[derive(Debug, Clone)]
pub struct Barrier {
    /// Unique barrier identifier
    pub id: BarrierId,

    /// Associated checkpoint ID
    pub checkpoint_id: CheckpointId,

    /// Timestamp when barrier was created
    pub timestamp: Instant,

    /// Number of expected acknowledgments
    pub expected_acks: usize,
}

impl Barrier {
    /// Create a new barrier
    pub fn new(id: BarrierId, checkpoint_id: CheckpointId, expected_acks: usize) -> Self {
        Self {
            id,
            checkpoint_id,
            timestamp: Instant::now(),
            expected_acks,
        }
    }

    /// Get barrier age
    pub fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

/// State of a barrier in the system
#[derive(Debug)]
pub enum BarrierState {
    /// Barrier is in progress
    InProgress {
        /// Number of acknowledgments received so far
        received: usize,
        /// Number of acknowledgments expected
        expected: usize,
    },
    /// Barrier completed successfully
    Completed,
    /// Barrier timed out
    TimedOut,
}

/// Barrier manager for coordinating aligned checkpoints
pub struct BarrierManager {
    /// Active barriers
    barriers: Arc<RwLock<std::collections::HashMap<BarrierId, BarrierState>>>,
    /// Notification for barrier completions
    notify: Arc<Notify>,
    /// Barrier alignment timeout
    timeout: Duration,
    /// Next barrier ID
    next_barrier_id: Arc<RwLock<BarrierId>>,
}

impl BarrierManager {
    /// Create a new barrier manager
    pub fn new(timeout: Duration) -> Self {
        Self {
            barriers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            notify: Arc::new(Notify::new()),
            timeout,
            next_barrier_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Generate next barrier ID
    pub async fn next_barrier_id(&self) -> BarrierId {
        let mut id = self.next_barrier_id.write().await;
        let current = *id;
        *id += 1;
        current
    }

    /// Inject a barrier into the stream
    pub async fn inject_barrier(
        &self,
        checkpoint_id: CheckpointId,
        expected_acks: usize,
    ) -> Barrier {
        let barrier_id = self.next_barrier_id().await;
        let barrier = Barrier::new(barrier_id, checkpoint_id, expected_acks);

        // Register barrier
        let mut barriers = self.barriers.write().await;
        barriers.insert(
            barrier_id,
            BarrierState::InProgress {
                received: 0,
                expected: expected_acks,
            },
        );

        barrier
    }

    /// Acknowledge a barrier (called by processor workers)
    pub async fn acknowledge_barrier(&self, barrier_id: BarrierId) -> CheckpointResult<bool> {
        let mut barriers = self.barriers.write().await;

        match barriers.get_mut(&barrier_id) {
            Some(BarrierState::InProgress { received, expected }) => {
                *received += 1;

                tracing::debug!(
                    "Barrier {} acknowledged: {}/{}",
                    barrier_id,
                    *received,
                    *expected
                );

                // Check if all acknowledgments received
                if *received >= *expected {
                    // Mark as completed
                    barriers.insert(barrier_id, BarrierState::Completed);

                    // Notify waiting tasks
                    self.notify.notify_waiters();

                    tracing::info!("Barrier {} completed", barrier_id);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Some(_) => {
                // Already completed or timed out
                Ok(false)
            }
            None => Err(Error::Process(format!(
                "Unknown barrier ID: {}",
                barrier_id
            ))),
        }
    }

    /// Wait for barrier to complete (with timeout)
    pub async fn wait_for_barrier(&self, barrier_id: BarrierId) -> CheckpointResult<()> {
        let start = Instant::now();

        loop {
            // Check if barrier is completed
            {
                let barriers = self.barriers.read().await;
                match barriers.get(&barrier_id) {
                    Some(BarrierState::Completed) => {
                        tracing::debug!(
                            "Barrier {} completed after {:?}",
                            barrier_id,
                            start.elapsed()
                        );
                        return Ok(());
                    }
                    Some(BarrierState::TimedOut) => {
                        return Err(Error::Process(format!("Barrier {} timed out", barrier_id)));
                    }
                    Some(BarrierState::InProgress { .. }) => {
                        // Still in progress, continue waiting
                    }
                    None => {
                        return Err(Error::Process(format!("Barrier {} not found", barrier_id)));
                    }
                }
            }

            // Check timeout
            if start.elapsed() >= self.timeout {
                // Mark as timed out
                let mut barriers = self.barriers.write().await;
                barriers.insert(barrier_id, BarrierState::TimedOut);

                tracing::warn!("Barrier {} timed out after {:?}", barrier_id, self.timeout);
                return Err(Error::Process(format!("Barrier {} timed out", barrier_id)));
            }

            // Wait for notification with a small timeout
            let _ = timeout(Duration::from_millis(100), self.notify.notified()).await;
        }
    }

    /// Check if a barrier is completed
    pub async fn is_barrier_completed(&self, barrier_id: BarrierId) -> bool {
        let barriers = self.barriers.read().await;
        match barriers.get(&barrier_id) {
            Some(BarrierState::Completed) => true,
            _ => false,
        }
    }

    /// Remove a barrier from tracking
    pub async fn remove_barrier(&self, barrier_id: BarrierId) {
        let mut barriers = self.barriers.write().await;
        barriers.remove(&barrier_id);
    }

    /// Clean up old barriers (should be called periodically)
    pub async fn cleanup_old_barriers(&self, _max_age: Duration) {
        let mut barriers = self.barriers.write().await;

        barriers.retain(|_barrier_id, state| {
            match state {
                BarrierState::Completed | BarrierState::TimedOut => {
                    // These should eventually be cleaned up, but we need to track age
                    // For now, keep them until explicitly removed
                    true
                }
                BarrierState::InProgress { .. } => {
                    // Check if barrier has timed out
                    // We'd need to add timestamp to BarrierState for proper implementation
                    true
                }
            }
        });
    }

    /// Get current number of active barriers
    pub async fn active_barrier_count(&self) -> usize {
        let barriers = self.barriers.read().await;
        barriers.len()
    }

    /// Force complete all barriers (for shutdown)
    pub async fn force_complete_all(&self) {
        let mut barriers = self.barriers.write().await;

        for (barrier_id, state) in barriers.iter_mut() {
            if let BarrierState::InProgress { .. } = state {
                *state = BarrierState::Completed;
                tracing::warn!("Barrier {} force completed", barrier_id);
            }
        }

        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_barrier_creation() {
        let barrier = Barrier::new(1, 100, 3);
        assert_eq!(barrier.id, 1);
        assert_eq!(barrier.checkpoint_id, 100);
        assert_eq!(barrier.expected_acks, 3);
    }

    #[tokio::test]
    async fn test_barrier_injection() {
        let manager = BarrierManager::new(Duration::from_secs(5));

        let barrier = manager.inject_barrier(1, 3).await;
        assert_eq!(barrier.expected_acks, 3);

        // Check barrier is registered
        let barriers = manager.barriers.read().await;
        assert!(barriers.contains_key(&barrier.id));
    }

    #[tokio::test]
    async fn test_barrier_acknowledgement() {
        let manager = BarrierManager::new(Duration::from_secs(5));

        let barrier = manager.inject_barrier(1, 2).await;

        // First acknowledgment
        let completed = manager.acknowledge_barrier(barrier.id).await.unwrap();
        assert!(!completed);

        // Second acknowledgment (should complete)
        let completed = manager.acknowledge_barrier(barrier.id).await.unwrap();
        assert!(completed);
        assert!(manager.is_barrier_completed(barrier.id).await);
    }

    #[tokio::test]
    async fn test_barrier_wait() {
        let manager = Arc::new(BarrierManager::new(Duration::from_secs(5)));

        let barrier = manager.inject_barrier(1, 2).await;
        let barrier_id = barrier.id;

        // Spawn task to acknowledge barrier
        let manager_clone = Arc::clone(&manager);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = manager_clone.acknowledge_barrier(barrier_id).await;
            let _ = manager_clone.acknowledge_barrier(barrier_id).await;
        });

        // Wait for completion
        let result = manager.wait_for_barrier(barrier_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_barrier_timeout() {
        let manager = BarrierManager::new(Duration::from_millis(100));

        let barrier = manager.inject_barrier(1, 2).await;

        // Wait for timeout
        let result = manager.wait_for_barrier(barrier.id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_barrier_sequence() {
        let manager = BarrierManager::new(Duration::from_secs(5));

        let id1 = manager.next_barrier_id().await;
        let id2 = manager.next_barrier_id().await;
        let id3 = manager.next_barrier_id().await;

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[tokio::test]
    async fn test_active_barrier_count() {
        let manager = BarrierManager::new(Duration::from_secs(5));

        assert_eq!(manager.active_barrier_count().await, 0);

        manager.inject_barrier(1, 2).await;
        manager.inject_barrier(2, 2).await;
        manager.inject_barrier(3, 2).await;

        assert_eq!(manager.active_barrier_count().await, 3);
    }
}
