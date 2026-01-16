/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file otherwise comply with the License.
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

//! Zero-copy message passing tests

use arkflow_core::{MessageBatch, MessageBatchRef, ProcessResult};
use std::sync::Arc;

#[test]
fn test_message_batch_into_arc() {
    let data = vec![b"test1".to_vec(), b"test2".to_vec()];
    let batch = MessageBatch::new_binary(data).unwrap();

    // Convert to Arc
    let arc: MessageBatchRef = batch.into_arc();

    // Verify data is intact
    assert_eq!(arc.len(), 2);
}

#[test]
fn test_message_batch_from_arc() {
    let data = vec![b"test1".to_vec(), b"test2".to_vec()];
    let batch = MessageBatch::new_binary(data).unwrap();
    let arc: MessageBatchRef = Arc::new(batch);

    // Arc can be cloned cheaply
    let arc_clone = arc.clone();

    // Verify data is intact
    assert_eq!(arc.len(), 2);
    assert_eq!(arc_clone.len(), 2);
    assert!(Arc::ptr_eq(&arc, &arc_clone));
}

#[test]
fn test_process_result_single() {
    let data = vec![b"test".to_vec()];
    let batch = MessageBatch::new_binary(data).unwrap();
    let arc: MessageBatchRef = Arc::new(batch);

    // Create single result
    let result = ProcessResult::Single(arc.clone());

    // Test methods
    assert!(!result.is_empty());
    assert_eq!(result.len(), 1);

    // Convert to vec
    let vec = result.into_vec();
    assert_eq!(vec.len(), 1);
}

#[test]
fn test_process_result_multiple() {
    let batch1 = MessageBatch::new_binary(vec![b"test1".to_vec()]).unwrap();
    let batch2 = MessageBatch::new_binary(vec![b"test2".to_vec()]).unwrap();

    let arcs: Vec<MessageBatchRef> = vec![Arc::new(batch1), Arc::new(batch2)];
    let result = ProcessResult::Multiple(arcs);

    // Test methods
    assert!(!result.is_empty());
    assert_eq!(result.len(), 2);

    // Convert to vec
    let vec = result.into_vec();
    assert_eq!(vec.len(), 2);
}

#[test]
fn test_process_result_none() {
    let result = ProcessResult::None;

    // Test methods
    assert!(result.is_empty());
    assert_eq!(result.len(), 0);

    // Convert to vec
    let vec = result.into_vec();
    assert_eq!(vec.len(), 0);
}

#[test]
fn test_process_result_from_vec() {
    let batch1 = MessageBatch::new_binary(vec![b"test1".to_vec()]).unwrap();
    let batch2 = MessageBatch::new_binary(vec![b"test2".to_vec()]).unwrap();

    let vec = vec![batch1, batch2];
    let result = ProcessResult::from_vec(vec);

    assert!(!result.is_empty());
    assert_eq!(result.len(), 2);
}

#[test]
fn test_arc_clone_is_cheap() {
    let data = vec![b"test".to_vec(); 10000];
    let batch = MessageBatch::new_binary(data).unwrap();
    let arc: MessageBatchRef = Arc::new(batch);

    // Clone Arc (should be cheap, just incrementing reference count)
    let arc_clone = arc.clone();

    // Both point to same data
    assert!(Arc::ptr_eq(&arc, &arc_clone));
    assert_eq!(arc.len(), arc_clone.len());
}

#[test]
fn test_process_result_empty_vec() {
    let vec: Vec<MessageBatch> = vec![];
    let result = ProcessResult::from_vec(vec);

    assert!(result.is_empty());
    assert_eq!(result.len(), 0);
}

#[test]
fn test_process_result_single_vec() {
    let batch = MessageBatch::new_binary(vec![b"test".to_vec()]).unwrap();
    let vec = vec![batch];
    let result = ProcessResult::from_vec(vec);

    assert!(!result.is_empty());
    assert_eq!(result.len(), 1);
}

#[test]
fn test_message_batch_arc_roundtrip() {
    let original = MessageBatch::new_binary(vec![b"data".to_vec()]).unwrap();
    let original_len = original.len();

    // Into Arc
    let arc = original.into_arc();

    // Clone Arc (cheap operation)
    let arc_clone = arc.clone();

    // Data preserved
    assert_eq!(arc.len(), original_len);
    assert_eq!(arc_clone.len(), original_len);
}

#[cfg(test)]
mod benchmark_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    #[ignore] // Run with: cargo test --release -- --ignored
    fn benchmark_clone_vs_arc() {
        let data = vec![b"test".to_vec(); 10000];
        let batch = MessageBatch::new_binary(data).unwrap();

        // Benchmark Arc cloning
        let arc = Arc::new(batch);
        let start = Instant::now();
        for _ in 0..100000 {
            let _ = arc.clone();
        }
        let arc_duration = start.elapsed();

        println!("Arc clone time: {:?}", arc_duration);

        // Arc should be very fast (< 10ms for 100k clones)
        assert!(arc_duration.as_millis() < 10);
    }
}
