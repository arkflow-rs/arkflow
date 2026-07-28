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

//! Integration tests against a real S3-compatible object store (MinIO).
//!
//! Task 4.4: the WAL must recover correctly across S3-compatible
//! implementations whose LIST/GET consistency may lag a concurrent PUT
//! (MinIO, Alibaba OSS, Ceph). These tests exercise the LIST-fallback
//! recovery path against a running MinIO instance.
//!
//! By default the tests are `#[ignore]` so `cargo test` doesn't try to
//! connect to anything. To run:
//!
//! ```sh
//! docker run --rm -d -p 9000:9000 -p 9001:9001 \
//!     -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin \
//!     quay.io/minio/minio:latest server /data --console-address :9001
//!
//! # Wait for MinIO to be ready, then:
//! cargo test -p arkflow-plugin --test minio_integration -- --ignored --nocapture
//! ```
//!
//! Required env vars: `MINIO_ENDPOINT` (e.g. `http://localhost:9000`).
//! Optional: `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` (default
//! `minioadmin`/`minioadmin`), `MINIO_BUCKET` (default `arkflow-wal-test`),
//! `MINIO_REGION` (default `us-east-1`).

use arkflow_core::wal::config::{
    CursorFlushConfig, ObjectStoreS3Config, ObjectStoreWalConfig, SegmentConfig,
};
use arkflow_core::wal::store::serialize;
use arkflow_core::wal::{SyncPolicy, WalConfig};
use arkflow_core::MessageBatch;
use arkflow_plugin::wal::init;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn sample_payload() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    serialize(&MessageBatch::new_arrow(batch)).unwrap()
}

fn minio_endpoint() -> Option<String> {
    std::env::var("MINIO_ENDPOINT").ok()
}

fn s3_config() -> ObjectStoreS3Config {
    ObjectStoreS3Config {
        bucket: std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "arkflow-wal-test".into()),
        region: Some(std::env::var("MINIO_REGION").unwrap_or_else(|_| "us-east-1".into())),
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

fn osc(s3: ObjectStoreS3Config) -> ObjectStoreWalConfig {
    ObjectStoreWalConfig {
        node_id: "pod-a".into(),
        stream_id: "minio-integration".into(),
        prefix: format!("arkflow/wal-test-{}", SEQ.fetch_add(1, Ordering::SeqCst)),
        s3,
        segment: SegmentConfig {
            max_entries: 4,
            max_bytes: 1024,
            flush_interval: std::time::Duration::from_millis(50),
        },
        cursor: CursorFlushConfig {
            max_entries: 1000,
            interval: std::time::Duration::from_millis(50),
        },
        sync: SyncPolicy::GroupCommit,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn round_trip_against_minio() {
    if minio_endpoint().is_none() {
        eprintln!("MINIO_ENDPOINT not set; skipping. Run MinIO locally to exercise this test.");
        return;
    }

    // Ensure plugin builders (and the S3 store builder) are registered.
    init().expect("plugin init");

    let cfg = WalConfig {
        enabled: true,
        path: String::new(),
        sync: SyncPolicy::GroupCommit,
        backend: Some(arkflow_core::wal::WalBackend::ObjectStore(osc(s3_config()))),
    };
    cfg.validate().expect("config must validate");

    // Use the local `Wal::open` path so we exercise the full integration
    // (registry lookup + builder dispatch + recovery).
    let store = arkflow_core::wal::build_wal_store(&cfg).unwrap();

    let payload = sample_payload();
    // 4 entries → forces a seal.
    for i in 1u64..=4 {
        store.append_batch(vec![(i, payload.clone())]).unwrap();
    }

    let replayed = store.read_after_cursor().unwrap();
    assert_eq!(replayed.len(), 4);

    store.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn high_qps_workload() {
    if minio_endpoint().is_none() {
        eprintln!("MINIO_ENDPOINT not set; skipping. Run MinIO locally to exercise this test.");
        return;
    }

    // Ensure plugin builders (and the S3 store builder) are registered.
    init().expect("plugin init");

    let cfg = WalConfig {
        enabled: true,
        path: String::new(),
        sync: SyncPolicy::GroupCommit,
        backend: Some(arkflow_core::wal::WalBackend::ObjectStore(osc(s3_config()))),
    };
    cfg.validate().expect("config must validate");

    let store = arkflow_core::wal::build_wal_store(&cfg).unwrap();

    let payload = sample_payload();

    // Simulate high QPS: write 100 entries rapidly
    // With max_entries=4, this creates 25 segments
    // The new pipeline should handle this without blocking
    let start = std::time::Instant::now();
    for i in 1u64..=100 {
        store.append_batch(vec![(i, payload.clone())]).unwrap();
    }
    let write_duration = start.elapsed();

    // Give PUT worker time to finish processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let replayed = store.read_after_cursor().unwrap();
    assert_eq!(replayed.len(), 100, "all entries should be replayed");

    store.close().unwrap();

    println!(
        "High QPS test: wrote 100 entries in {:?}, {:.2} entries/sec",
        write_duration,
        100.0 / write_duration.as_secs_f64()
    );
}
