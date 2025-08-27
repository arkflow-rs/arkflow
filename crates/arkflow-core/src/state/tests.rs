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

//! Tests for state management and transaction features

#[cfg(test)]
mod tests {
    use crate::{
        state::{
            helper::{SimpleMemoryState, StateHelper},
            monitoring::StateMonitor,
            Metadata,
        },
        MessageBatch,
    };

    #[tokio::test]
    async fn test_metadata_embed_and_extract() {
        let batch = MessageBatch::from_string("test message").unwrap();

        // Create simple metadata
        let metadata = Metadata::new();

        // Test embedding and extracting
        let batch_with_metadata = metadata.embed_to_batch(batch).unwrap();
        let extracted_metadata = batch_with_metadata.metadata();
        assert!(extracted_metadata.is_some());
    }

    #[test]
    fn test_simple_memory_state_operations() {
        let mut state = SimpleMemoryState::new();

        // Test basic operations
        state.put_typed("string_key", "hello".to_string()).unwrap();
        state.put_typed("number_key", 42u64).unwrap();
        state.put_typed("bool_key", true).unwrap();

        // Test retrieval
        let string_val: Option<String> = state.get_typed("string_key").unwrap();
        assert_eq!(string_val, Some("hello".to_string()));

        let number_val: Option<u64> = state.get_typed("number_key").unwrap();
        assert_eq!(number_val, Some(42));

        let bool_val: Option<bool> = state.get_typed("bool_key").unwrap();
        assert_eq!(bool_val, Some(true));

        // Test non-existent key
        let missing: Option<String> = state.get_typed("missing_key").unwrap();
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn test_state_monitoring() {
        let monitor = StateMonitor::new().unwrap();

        // Test basic monitoring operations
        monitor.update_state_size(1024);
        monitor.update_checkpoint_size(512);
        monitor.update_active_transactions(3);

        // Test cache operations
        monitor.record_cache_hit();
        monitor.record_cache_miss();

        // Test health status
        let health = monitor.health_status();
        assert!(health.healthy);
        assert_eq!(health.state_size, 1024);
        assert_eq!(health.active_transactions, 3);

        // Test metrics export
        let metrics_export = monitor.export_metrics().unwrap();
        assert!(!metrics_export.is_empty());
        assert!(metrics_export.contains("arkflow_state"));
    }
}
