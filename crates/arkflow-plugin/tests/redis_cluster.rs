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

//! End-to-end tests for the Redis **cluster** input path against a real
//! single-node `redis:7-alpine` cluster (all 16384 slots on one node,
//! announced at a fixed host port so `CLUSTER SLOTS` routes the client
//! back to the host). This is the path that goes through
//! `ClusterClientBuilder::push_sender` / the `ClusterPushForwarder` —
//! the single-node E2E suite (`redis_input.rs`) cannot reach it.
//!
//! Skips with a note when Docker is unavailable, following the shared
//! broker-lease discipline of the other I/O suites.

use arkflow_core::input::{Input, InputConfig};
use arkflow_core::{MessageBatchRef, Resource};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Fixed host port: the node must announce a stable address in
/// `CLUSTER SLOTS`, so the mapping cannot be random.
const CLUSTER_HOST_PORT: u16 = 6390;

static CLUSTER: LazyLock<tokio::sync::Mutex<Option<ContainerAsync<GenericImage>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Minimal RESP client. Unlike the one in `redis_input.rs` it reads full
/// bulk strings, because `CLUSTER INFO` (used as the readiness probe) is
/// a multi-line bulk reply.
struct RespClient {
    stream: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl RespClient {
    async fn connect(addr: &str) -> Self {
        let stream = tokio::time::timeout(Duration::from_secs(10), tokio::net::TcpStream::connect(addr))
            .await
            .expect("tcp connect timeout")
            .expect("tcp connect");
        Self { stream, buf: Vec::new() }
    }

    async fn cmd(&mut self, args: &[String]) -> String {
        let mut request = format!("*{}\r\n", args.len());
        for arg in args {
            request.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.stream.write_all(request.as_bytes()).await.unwrap();
        self.read_response().await
    }

    /// Pulls more bytes from the socket into the buffer. The deadline is
    /// per-fill, so a reply that never completes fails the test instead
    /// of parking it forever.
    async fn fill(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut chunk = [0u8; 1024];
        let n = tokio::time::timeout_at(deadline.into(), self.stream.read(&mut chunk))
            .await
            .expect("resp read timeout")
            .expect("resp read");
        assert!(n > 0, "redis closed the connection");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    /// Reads exactly `n` bytes out of the buffer, refilling from the
    /// socket only once the buffer is drained. Reading from the socket
    /// directly here would deadlock whenever the payload already arrived
    /// in the same TCP segment as the header.
    async fn read_exact_n(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() < n {
            self.fill().await;
        }
        self.buf.drain(..n).collect()
    }

    async fn read_response(&mut self) -> String {
        // Read one RESP payload: `+...`/`:...` single lines or `$N` bulk.
        let header = self.read_line().await;
        if let Some(len) = header.strip_prefix('$') {
            let len: usize = len.trim().parse().expect("bulk length");
            let payload = self.read_exact_n(len + 2).await; // payload + CRLF
            return String::from_utf8_lossy(&payload[..len]).to_string();
        }
        header
    }

    async fn read_line(&mut self) -> String {
        loop {
            if let Some(idx) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line: Vec<u8> = self.buf.drain(..idx + 2).collect();
                return String::from_utf8_lossy(&line[..idx]).to_string();
            }
            self.fill().await;
        }
    }
}

struct ClusterLease {
    addr: String,
}

async fn cluster_lease() -> Option<ClusterLease> {
    let mut cluster = CLUSTER.lock().await;
    if cluster.is_none() {
        if !docker_available() {
            eprintln!("skipping redis_cluster tests: Docker is unavailable");
            return None;
        }
        let container = GenericImage::new("redis", "7-alpine")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_startup_timeout(Duration::from_secs(120))
            .with_cmd([
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                "nodes.conf",
                "--cluster-announce-ip",
                "127.0.0.1",
                "--cluster-announce-port",
                &CLUSTER_HOST_PORT.to_string(),
                "--cluster-announce-bus-port",
                "16379",
            ])
            .with_mapped_port(CLUSTER_HOST_PORT, 6379.tcp())
            .with_mapped_port(16379, 16379.tcp())
            .start()
            .await
            .expect("redis cluster container start");
        *cluster = Some(container);

        let addr = format!("127.0.0.1:{CLUSTER_HOST_PORT}");
        // Turn the fresh node into a one-node cluster holding every slot.
        let mut admin = RespClient::connect(&addr).await;
        admin
            .cmd(&["CLUSTER".into(), "SET-CONFIG-EPOCH".into(), "1".into()])
            .await;
        let mut addslots = vec!["CLUSTER".into(), "ADDSLOTS".into()];
        addslots.extend((0..16384).map(|slot| slot.to_string()));
        let added = admin.cmd(&addslots).await;
        assert!(added.starts_with('+'), "CLUSTER ADDSLOTS failed: {added}");

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(Instant::now() < deadline, "redis cluster never reached state ok");
            let info = admin.cmd(&["CLUSTER".into(), "INFO".into()]).await;
            if info.contains("cluster_state:ok") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ACTIVE.fetch_add(1, Ordering::SeqCst);
        return Some(ClusterLease { addr });
    }
    ACTIVE.fetch_add(1, Ordering::SeqCst);
    Some(ClusterLease {
        addr: format!("127.0.0.1:{CLUSTER_HOST_PORT}"),
    })
}

impl Drop for ClusterLease {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        // Shared across tests in this binary; removed at process teardown.
    }
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

fn ensure_init() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        arkflow_plugin::input::init().expect("plugin input init");
    });
}

async fn build_cluster_input(redis_type: serde_json::Value) -> Arc<dyn Input> {
    ensure_init();
    let input = InputConfig {
        input_type: "redis".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "mode": {"type": "cluster", "urls": [format!("redis://127.0.0.1:{CLUSTER_HOST_PORT}")]},
            "redis_type": redis_type,
        })),
    };
    input.build(&resource()).expect("build redis cluster input")
}

async fn read_with_timeout(input: &Arc<dyn Input>) -> MessageBatchRef {
    let input = input.clone();
    tokio::time::timeout(Duration::from_secs(20), async move {
        loop {
            match input.read().await {
                Ok((batch, _)) => return batch,
                Err(arkflow_core::Error::Disconnection) => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(e) => panic!("redis cluster input read failed: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for a redis cluster delivery")
}

fn first_payload(batch: &MessageBatchRef) -> Vec<u8> {
    use datafusion::arrow::array::{BinaryArray, StringArray};
    let record = batch.record_batch();
    assert!(record.num_rows() > 0, "expected at least one row");
    let column = record.column(0);
    if let Some(array) = column.as_any().downcast_ref::<BinaryArray>() {
        return array.value(0).to_vec();
    }
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return array.value(0).as_bytes().to_vec();
    }
    panic!("unexpected payload column type: {column:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_channels_subscription_delivers_published_payloads() {
    let Some(lease) = cluster_lease().await else { return };
    let input = build_cluster_input(serde_json::json!({
        "type": "subscribe",
        "subscribe": {"type": "channels", "channels": ["ark-cluster-ch"]}
    }))
    .await;
    input.connect().await.expect("connect redis cluster input");

    // Let the subscription register cluster-wide before publishing.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut publisher = RespClient::connect(&lease.addr).await;
    publisher
        .cmd(&["PUBLISH".into(), "ark-cluster-ch".into(), "hello-cluster".into()])
        .await;
    publisher
        .cmd(&["PUBLISH".into(), "other-cluster-ch".into(), "must-not-arrive".into()])
        .await;

    let batch = read_with_timeout(&input).await;
    assert_eq!(first_payload(&batch), b"hello-cluster");
    input.close().await.expect("close redis cluster input");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_list_mode_pops_pushed_entries() {
    let Some(lease) = cluster_lease().await else { return };
    let input = build_cluster_input(serde_json::json!({
        "type": "list", "list": ["ark-cluster-list"]
    }))
    .await;
    input.connect().await.expect("connect redis cluster input");

    let mut producer = RespClient::connect(&lease.addr).await;
    producer
        .cmd(&["LPUSH".into(), "ark-cluster-list".into(), "hello-cluster-list".into()])
        .await;

    let batch = read_with_timeout(&input).await;
    assert_eq!(first_payload(&batch), b"hello-cluster-list");
    input.close().await.expect("close redis cluster input");
}
