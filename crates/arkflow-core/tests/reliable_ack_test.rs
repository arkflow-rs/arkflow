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

use arkflow_core::idempotent_ack::{AckCache, AckId};
use arkflow_core::input::NoopAck;
use arkflow_core::reliable_ack::ReliableAckProcessor;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[tokio::test]
async fn test_reliable_ack_processor_creation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let result = ReliableAckProcessor::new(&tracker, cancellation_token.clone(), &wal_path);
    assert!(result.is_ok());

    let processor = result.unwrap();
    let metrics = processor.get_metrics();
    assert_eq!(
        metrics
            .total_acks
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    cancellation_token.cancel();
}

#[tokio::test]
async fn test_reliable_ack_processor_ack() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let processor =
        ReliableAckProcessor::new(&tracker, cancellation_token.clone(), &wal_path).unwrap();

    let ack = Arc::new(NoopAck);
    let result = processor.ack(ack, "test".to_string(), vec![1, 2, 3]).await;
    assert!(result.is_ok());

    let metrics = processor.get_metrics();
    assert_eq!(
        metrics
            .total_acks
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics
            .persisted_acks
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    cancellation_token.cancel();
}

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
async fn test_ack_task() {
    use arkflow_core::reliable_ack::AckTask;

    let ack = Arc::new(NoopAck);
    let task = AckTask::new(ack, 1, "test".to_string(), vec![1, 2, 3]);

    assert!(!task.is_expired());
    assert!(task.should_retry());
}
