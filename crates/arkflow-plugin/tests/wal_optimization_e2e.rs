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

//! End-to-end integration tests for the comprehensive WAL optimization.
//!
//! Tests cover:
//! - All three segment tuning strategies (aggressive, balanced, low_latency)
//! - Parallel PUT workers (1, 2, 4 workers)
//! - Compression (none, zstd, lz4)
//! - Backward compatibility with old configs
//!
//! Requires MinIO running locally:
//! ```sh
//! docker run -d --rm -p 9000:9000 -p 9001:9001 \
//!   -e MINIO_ROOT_USER=minioadmin \
//!   -e MINIO_ROOT_PASSWORD=minioadmin \
//!   quay.io/minio/minio:latest server /data --console-address :9001
//! docker exec <container> mc alias set local http://localhost:9000 minioadmin minioadmin
//! docker exec <container> mc mb local/arkflow-wal-e2e --ignore-existing
//! ```

use arkflow_core::wal::config::{
    CompressionConfig, CursorFlushConfig, ObjectStoreS3Config, ObjectStoreWalConfig,
    ParallelPutConfig, SegmentConfig, SegmentStrategy, SegmentTuningConfig,
};
use arkflow_core::wal::{SyncPolicy, WalBackend, WalConfig};
use arkflow_plugin::wal::init;
use std::sync::Once;
use std::time::Duration;

static INIT: Once = Once::new();

fn ensure_init() {
    // `init()` returns `Err` if the builder is already registered; that's fine,
    // we only need to call it once per process.
    INIT.call_once(|| {
        let _ = init();
    });
}

fn minio_endpoint() -> Option<String> {
    std::env::var("MINIO_ENDPOINT").ok()
}

fn skip_if_no_minio() -> bool {
    if minio_endpoint().is_none() {
        eprintln!("MINIO_ENDPOINT not set; skipping.");
        return true;
    }
    false
}

fn s3_config() -> ObjectStoreS3Config {
    ObjectStoreS3Config {
        bucket: std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "arkflow-wal-e2e".into()),
        region: Some("us-east-1".into()),
        endpoint: minio_endpoint(),
        access_key_id: Some(
            std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into()),
        ),
        secret_access_key: Some(
            std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into()),
        ),
        allow_http: true,
    }
}

fn osc(s3: ObjectStoreS3Config, prefix: &str) -> ObjectStoreWalConfig {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ObjectStoreWalConfig {
        node_id: format!("e2e-pod-{}", unique),
        stream_id: "e2e-stream".into(),
        prefix: prefix.into(),
        s3,
        segment: SegmentConfig {
            max_entries: 4,
            max_bytes: 1024 * 1024,
            flush_interval: Duration::from_millis(50),
        },
        cursor: CursorFlushConfig {
            max_entries: 1000,
            interval: Duration::from_millis(50),
        },
        segment_tuning: SegmentTuningConfig::default(),
        parallel_put: ParallelPutConfig::default(),
        compression: CompressionConfig::default(),
        sync: SyncPolicy::GroupCommit,
    }
}

fn make_payload() -> Vec<u8> {
    use arkflow_core::wal::store::serialize;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut mb = MessageBatch::new_arrow(batch);
    mb.set_input_name(Some("e2e".into()));
    serialize(&mb).unwrap()
}

fn run_with_config(label: &str, osc: ObjectStoreWalConfig) {
    if skip_if_no_minio() {
        return;
    }
    ensure_init();

    println!("--- E2E: {} ---", label);
    let cfg = WalConfig {
        enabled: true,
        path: String::new(),
        sync: SyncPolicy::GroupCommit,
        backend: Some(WalBackend::ObjectStore(osc)),
    };
    cfg.validate().expect("config must validate");
    let store = arkflow_core::wal::build_wal_store(&cfg).expect("build store");

    let payload = make_payload();
    for i in 1u64..=20 {
        store
            .append_batch(vec![(i, payload.clone())])
            .expect("append");
    }

    // Wait for PUT workers to drain
    std::thread::sleep(Duration::from_millis(500));

    let replayed = store.read_after_cursor().expect("read_after_cursor");
    println!("    replayed {} entries (expected 20)", replayed.len());
    assert_eq!(replayed.len(), 20);

    store.close().expect("close");
    println!("    OK");
}

#[test]
#[ignore]
fn e2e_aggressive_strategy() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-aggressive");
    config.segment_tuning = SegmentTuningConfig {
        strategy: SegmentStrategy::Aggressive,
        // Override flush_interval for the test so segments seal quickly.
        max_entries: Some(4),
        max_bytes: None,
        flush_interval: Some(Duration::from_millis(50)),
    };
    run_with_config("aggressive strategy", config);
}

#[test]
#[ignore]
fn e2e_low_latency_strategy() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-low-latency");
    config.segment_tuning = SegmentTuningConfig {
        strategy: SegmentStrategy::LowLatency,
        max_entries: None,
        max_bytes: None,
        flush_interval: None,
    };
    run_with_config("low_latency strategy", config);
}

#[test]
#[ignore]
fn e2e_parallel_workers() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-parallel");
    config.parallel_put = ParallelPutConfig {
        workers: 4,
        shutdown_timeout: Duration::from_secs(30),
    };
    run_with_config("parallel PUT (4 workers)", config);
}

#[test]
#[ignore]
fn e2e_zstd_compression() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-zstd");
    config.compression = CompressionConfig::Zstd { level: 3 };
    run_with_config("zstd compression", config);
}

#[test]
#[ignore]
fn e2e_lz4_compression() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-lz4");
    config.compression = CompressionConfig::Lz4 { level: 4 };
    run_with_config("lz4 compression", config);
}

#[test]
#[ignore]
fn e2e_combined_optimizations() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-combined");
    config.segment_tuning = SegmentTuningConfig {
        strategy: SegmentStrategy::Aggressive,
        // Override for test: small segments so they seal within the wait window.
        max_entries: Some(4),
        max_bytes: None,
        flush_interval: Some(Duration::from_millis(50)),
    };
    config.parallel_put = ParallelPutConfig {
        workers: 2,
        shutdown_timeout: Duration::from_secs(30),
    };
    config.compression = CompressionConfig::Zstd { level: 3 };
    run_with_config("aggressive + parallel + zstd", config);
}

#[test]
#[ignore]
fn e2e_backward_compat() {
    // Old-style config (only legacy fields), new fields use defaults
    let config = osc(s3_config(), "arkflow/wal-e2e-compat");
    run_with_config("backward compatible defaults", config);
}

#[test]
fn e2e_validation_rejects_zero_workers() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-validate");
    config.parallel_put = ParallelPutConfig {
        workers: 0, // invalid
        shutdown_timeout: Duration::from_secs(30),
    };
    let cfg = WalConfig {
        enabled: true,
        path: String::new(),
        sync: SyncPolicy::GroupCommit,
        backend: Some(WalBackend::ObjectStore(config)),
    };
    let err = cfg.validate().expect_err("zero workers must be rejected");
    assert!(
        err.to_string().contains("workers"),
        "expected workers validation error, got: {}",
        err
    );
}

#[test]
fn e2e_validation_rejects_invalid_zstd_level() {
    let mut config = osc(s3_config(), "arkflow/wal-e2e-validate");
    config.compression = CompressionConfig::Zstd { level: 25 }; // out of range
    let cfg = WalConfig {
        enabled: true,
        path: String::new(),
        sync: SyncPolicy::GroupCommit,
        backend: Some(WalBackend::ObjectStore(config)),
    };
    let err = cfg.validate().expect_err("zstd level 25 must be rejected");
    assert!(
        err.to_string().contains("zstd"),
        "expected zstd level error, got: {}",
        err
    );
}
