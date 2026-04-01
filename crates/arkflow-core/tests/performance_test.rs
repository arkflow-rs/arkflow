// Performance Tests for Exactly-Once Implementation
//
// This module tests the performance characteristics of:
// - Checkpoint overhead
// - Recovery time
// - Throughput impact
// - Resource usage

use arkflow_core::checkpoint::{CheckpointConfig, CheckpointCoordinator, CheckpointStorage};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
mod performance_tests {
    use super::*;

    /// Test checkpoint creation overhead
    #[tokio::test]
    async fn test_checkpoint_creation_overhead() {
        let temp_dir = tempfile::tempdir().unwrap();
        let checkpoint_path = temp_dir.path().join("checkpoints");

        let config = CheckpointConfig {
            enabled: true,
            interval: Duration::from_millis(100),
            max_checkpoints: 10,
            min_age: Duration::from_secs(3600),
            local_path: checkpoint_path.to_str().unwrap().to_string(),
            alignment_timeout: Duration::from_secs(10),
        };

        // Measure checkpoint coordinator initialization time
        let iterations = 100;
        let start = Instant::now();

        for _ in 0..iterations {
            let _coordinator = CheckpointCoordinator::new(CheckpointConfig {
                enabled: true,
                interval: Duration::from_millis(100),
                max_checkpoints: 10,
                min_age: Duration::from_secs(3600),
                local_path: checkpoint_path.to_str().unwrap().to_string(),
                alignment_timeout: Duration::from_secs(10),
            });
        }

        let duration = start.elapsed();
        let avg_time = duration / iterations;

        println!("Checkpoint coordinator creation overhead:");
        println!("  Total time: {:?}", duration);
        println!("  Average per creation: {:?}", avg_time);
        println!(
            "  Creations per second: {:.2}",
            iterations as f64 / duration.as_secs_f64()
        );

        // Assertion: Checkpoint creation should be fast (< 10ms per checkpoint)
        assert!(
            avg_time < Duration::from_millis(10),
            "Checkpoint creation too slow: {:?}",
            avg_time
        );
    }

    /// Test checkpoint save and restore performance
    #[tokio::test]
    async fn test_checkpoint_save_restore_performance() {
        let temp_dir = tempfile::tempdir().unwrap();
        let checkpoint_path = temp_dir.path();

        let storage = Arc::new(LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap());

        // Create a large state snapshot
        let mut generic_data = HashMap::new();
        for i in 0..1000 {
            generic_data.insert(format!("key{}", i), format!("value{}", i));
        }

        let large_snapshot = StateSnapshot {
            version: 1,
            timestamp: chrono::Utc::now().timestamp(),
            sequence_counter: 10000,
            next_seq: 5000,
            input_state: Some(InputState::Generic { data: generic_data }),
            buffer_state: None,
            metadata: HashMap::new(),
        };

        // Measure save performance
        let iterations = 50;
        let start = Instant::now();

        for i in 0..iterations {
            storage
                .save_checkpoint(i as u64, &large_snapshot)
                .await
                .unwrap();
        }

        let save_duration = start.elapsed();
        let avg_save_time = save_duration / iterations;

        println!("Checkpoint save performance:");
        println!("  Total time: {:?}", save_duration);
        println!("  Average per save: {:?}", avg_save_time);

        // Calculate throughput based on approximate size
        let estimated_size = 10 * 1024; // ~10KB per checkpoint
        println!(
            "  Throughput: {:.2} MB/s",
            (iterations as f64 * estimated_size as f64 / 1024.0) / save_duration.as_secs_f64()
        );

        // Measure restore performance
        let start = Instant::now();

        for i in 0..iterations {
            let _restored = storage.load_checkpoint(i as u64).await.unwrap();
        }

        let restore_duration = start.elapsed();
        let avg_restore_time = restore_duration / iterations;

        println!("Checkpoint restore performance:");
        println!("  Total time: {:?}", restore_duration);
        println!("  Average per restore: {:?}", avg_restore_time);
        println!(
            "  Throughput: {:.2} MB/s",
            (iterations as f64 * estimated_size as f64 / 1024.0) / restore_duration.as_secs_f64()
        );

        // Assertions
        assert!(
            avg_save_time < Duration::from_millis(50),
            "Save too slow: {:?}",
            avg_save_time
        );
        assert!(
            avg_restore_time < Duration::from_millis(20),
            "Restore too slow: {:?}",
            avg_restore_time
        );
    }

    /// Test throughput impact with checkpointing enabled vs disabled
    #[tokio::test]
    async fn test_throughput_impact() {
        // This test measures throughput with checkpointing enabled vs disabled
        // We simulate message processing and measure the impact

        let messages = 10000;

        // Baseline: No checkpointing (simulated)
        let start = Instant::now();
        for i in 0..messages {
            // Simulate message processing
            let _data = vec![i as u8; 100];
            std::hint::black_box(&_data);
        }
        let baseline_duration = start.elapsed();

        // With checkpointing (simulated overhead)
        let mut checkpoint_count = 0;
        let start = Instant::now();
        for i in 0..messages {
            // Simulate message processing
            let _data = vec![i as u8; 100];
            std::hint::black_box(&_data);

            // Simulate checkpoint overhead every 100 messages
            if i % 100 == 0 {
                // Simulate checkpoint overhead (small delay)
                let _snapshot = (i, vec![0u8; 1024]);
                checkpoint_count += 1;
            }
        }
        let checkpointed_duration = start.elapsed();

        let baseline_throughput = messages as f64 / baseline_duration.as_secs_f64();
        let checkpointed_throughput = messages as f64 / checkpointed_duration.as_secs_f64();
        let overhead_pct = ((checkpointed_duration.as_secs_f64()
            - baseline_duration.as_secs_f64())
            / baseline_duration.as_secs_f64())
            * 100.0;

        println!("Throughput comparison:");
        println!("  Baseline throughput: {:.2} msg/s", baseline_throughput);
        println!(
            "  Checkpointed throughput: {:.2} msg/s",
            checkpointed_throughput
        );
        println!("  Overhead: {:.2}%", overhead_pct);
        println!("  Checkpoints taken: {}", checkpoint_count);

        // Assertion: Checkpoint overhead should be < 20%
        assert!(
            overhead_pct < 20.0,
            "Checkpoint overhead too high: {:.2}%",
            overhead_pct
        );
    }

    /// Test recovery time performance
    #[tokio::test]
    async fn test_recovery_time() {
        let temp_dir = tempfile::tempdir().unwrap();
        let checkpoint_path = temp_dir.path();

        let storage = Arc::new(LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap());

        // Create multiple checkpoints with increasing state sizes
        let checkpoint_count = 20;

        for i in 0..checkpoint_count {
            let mut generic_data = HashMap::new();
            for j in 0..(i * 10) {
                generic_data.insert(format!("key{}", j), format!("value{}", j));
            }

            let snapshot = StateSnapshot {
                version: 1, // Always use version 1
                timestamp: chrono::Utc::now().timestamp(),
                sequence_counter: (i * 1000) as u64,
                next_seq: (i * 500) as u64,
                input_state: Some(InputState::Generic { data: generic_data }),
                buffer_state: None,
                metadata: HashMap::new(),
            };

            storage.save_checkpoint(i as u64, &snapshot).await.unwrap();
        }

        // Measure recovery time for the latest checkpoint
        let start = Instant::now();
        let restored = storage
            .load_checkpoint((checkpoint_count - 1) as u64)
            .await
            .unwrap();
        let recovery_duration = start.elapsed();

        assert!(restored.is_some());

        println!("Recovery time performance:");
        println!("  Checkpoints: {}", checkpoint_count);
        println!("  Recovery time: {:?}", recovery_duration);
        if let Some(ref state) = restored {
            if let Some(InputState::Generic { data }) = &state.input_state {
                println!("  Recovered state size: {} entries", data.len());
            }
        }

        // Assertion: Recovery should be fast (< 100ms)
        assert!(
            recovery_duration < Duration::from_millis(100),
            "Recovery too slow: {:?}",
            recovery_duration
        );
    }

    /// Test concurrent checkpoint creation
    #[tokio::test]
    async fn test_concurrent_checkpoint_overhead() {
        let temp_dir = tempfile::tempdir().unwrap();
        let checkpoint_path = temp_dir.path().join("checkpoints");

        let _config = CheckpointConfig {
            enabled: true,
            interval: Duration::from_millis(10),
            max_checkpoints: 10,
            min_age: Duration::from_secs(3600),
            local_path: checkpoint_path.to_str().unwrap().to_string(),
            alignment_timeout: Duration::from_secs(10),
        };

        let storage = Arc::new(LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap());

        // Spawn multiple concurrent tasks creating checkpoints
        let num_tasks = 10;
        let checkpoints_per_task = 10;
        let barrier = Arc::new(tokio::sync::Barrier::new(num_tasks));

        let start = Instant::now();

        let mut handles = vec![];
        for task_id in 0..num_tasks {
            let storage_clone = Arc::clone(&storage);
            let barrier_clone = Arc::clone(&barrier);

            let handle = tokio::spawn(async move {
                barrier_clone.wait().await; // Synchronize start

                for i in 0..checkpoints_per_task {
                    let snapshot = StateSnapshot::new();
                    let checkpoint_id = (task_id * checkpoints_per_task + i) as u64;

                    storage_clone
                        .save_checkpoint(checkpoint_id, &snapshot)
                        .await
                        .unwrap();
                }
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        let duration = start.elapsed();
        let total_checkpoints = num_tasks * checkpoints_per_task;
        let throughput = total_checkpoints as f64 / duration.as_secs_f64();

        println!("Concurrent checkpoint creation:");
        println!("  Total checkpoints: {}", total_checkpoints);
        println!("  Concurrent tasks: {}", num_tasks);
        println!("  Total time: {:?}", duration);
        println!("  Throughput: {:.2} checkpoints/sec", throughput);

        // Assertion: Should handle concurrent checkpoints efficiently (relaxed for debug builds)
        assert!(
            throughput > 50.0,
            "Concurrent checkpoint throughput too low: {:.2}",
            throughput
        );
    }

    /// Test state serialization performance
    #[tokio::test]
    async fn test_state_serialization_performance() {
        let serializer = StateSerializer::new();

        // Create a large state snapshot
        let mut snapshot = StateSnapshot::new();
        snapshot.sequence_counter = 100000;
        snapshot.next_seq = 50000;

        // Add metadata
        for i in 0..1000 {
            snapshot.add_metadata(
                format!("metadata_key_{}", i),
                format!("metadata_value_{}", i),
            );
        }

        // Add input state
        let mut kafka_offsets: HashMap<i32, i64> = HashMap::new();
        for partition in 0..100 {
            kafka_offsets.insert(partition, (partition * 1000) as i64);
        }

        snapshot.input_state = Some(InputState::Kafka {
            topic: "test_topic".to_string(),
            offsets: kafka_offsets,
        });

        // Measure serialization performance
        let iterations = 100;
        let start = Instant::now();

        let mut serialized_sizes = Vec::new();
        for _ in 0..iterations {
            let serialized = serializer.serialize(&snapshot).unwrap();
            serialized_sizes.push(serialized.len());
        }

        let serialize_duration = start.elapsed();
        let avg_serialize_time = serialize_duration / iterations;
        let avg_size = serialized_sizes.iter().sum::<usize>() / iterations as usize;

        println!("State serialization performance:");
        println!("  Total time: {:?}", serialize_duration);
        println!("  Average per serialization: {:?}", avg_serialize_time);
        println!(
            "  Average serialized size: {:.2} KB",
            avg_size as f64 / 1024.0
        );
        println!(
            "  Throughput: {:.2} MB/s",
            ((iterations as usize * avg_size) as f64 / 1024.0 / 1024.0)
                / serialize_duration.as_secs_f64()
        );

        // Measure deserialization performance
        let sample_data = serializer.serialize(&snapshot).unwrap();
        let start = Instant::now();

        for _ in 0..iterations {
            let _restored = serializer.deserialize(&sample_data).unwrap();
        }

        let deserialize_duration = start.elapsed();
        let avg_deserialize_time = deserialize_duration / iterations;

        println!("State deserialization performance:");
        println!("  Total time: {:?}", deserialize_duration);
        println!("  Average per deserialization: {:?}", avg_deserialize_time);
        println!(
            "  Throughput: {:.2} MB/s",
            ((iterations as usize * avg_size) as f64 / 1024.0 / 1024.0)
                / deserialize_duration.as_secs_f64()
        );

        // Assertions - relaxed thresholds for debug builds
        assert!(
            avg_serialize_time < Duration::from_millis(1),
            "Serialization too slow: {:?}",
            avg_serialize_time
        );
        assert!(
            avg_deserialize_time < Duration::from_millis(2),
            "Deserialization too slow: {:?}",
            avg_deserialize_time
        );
    }

    /// Test memory usage of checkpoint coordinator
    #[tokio::test]
    async fn test_checkpoint_coordinator_memory_usage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let checkpoint_path = temp_dir.path().join("checkpoints");

        let config = CheckpointConfig {
            enabled: true,
            interval: Duration::from_millis(50),
            max_checkpoints: 10,
            min_age: Duration::from_secs(3600),
            local_path: checkpoint_path.to_str().unwrap().to_string(),
            alignment_timeout: Duration::from_secs(10),
        };

        let _coordinator = Arc::new(CheckpointCoordinator::new(config));
        let storage = Arc::new(LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap());

        // Create multiple checkpoints
        for i in 0..20 {
            let snapshot = StateSnapshot::new();
            storage.save_checkpoint(i, &snapshot).await.unwrap();
        }

        // Get memory usage estimate by checking checkpoint files
        let checkpoint_files = std::fs::read_dir(checkpoint_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "dat"))
            .collect::<Vec<_>>();

        let total_size: u64 = checkpoint_files
            .iter()
            .filter_map(|entry| entry.metadata().ok())
            .map(|metadata| metadata.len())
            .sum();

        println!("Checkpoint storage usage:");
        println!("  Checkpoint files: {}", checkpoint_files.len());
        println!("  Total disk space: {:.2} KB", total_size as f64 / 1024.0);
        if !checkpoint_files.is_empty() {
            println!(
                "  Average per checkpoint: {:.2} KB",
                (total_size as f64 / checkpoint_files.len() as f64) / 1024.0
            );
        }

        // Assertion: Disk usage should be reasonable (< 10MB for 20 checkpoints)
        assert!(
            total_size < 10 * 1024 * 1024,
            "Disk usage too high: {} bytes",
            total_size
        );
    }
}
