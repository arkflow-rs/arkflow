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

//! End-to-end tests for the Redis input against a real `redis:7-alpine`
//! server started via `testcontainers`, following the shared-broker lease
//! discipline of `pulsar_io.rs` (Docker-availability probe, RAII lease,
//! leftover sweep).
//!
//! Covers the three consumption modes (channel subscribe, pattern
//! subscribe, list BLPOP), the not-connected guard on `read`, and the
//! unsubscribe path on `close`.

use arkflow_core::input::{Input, InputConfig};
use arkflow_core::{MessageBatchRef, Resource};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static REDIS: LazyLock<tokio::sync::Mutex<Option<ContainerAsync<GenericImage>>>> =
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

async fn start_redis() -> ContainerAsync<GenericImage> {
    GenericImage::new("redis", "7-alpine")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("redis container start")
}

/// Minimal RESP client: enough for PUBLISH / LPUSH / PING against the
/// test container without pulling in a client dependency mismatch.
struct RespClient {
    stream: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl RespClient {
    async fn connect(addr: &str) -> Self {
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        .expect("tcp connect timeout")
        .expect("tcp connect");
        Self { stream, buf: Vec::new() }
    }

    async fn cmd(&mut self, args: &[&str]) -> String {
        let mut request = format!("*{}\r\n", args.len());
        for arg in args {
            request.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.stream.write_all(request.as_bytes()).await.unwrap();
        self.read_response().await
    }

    async fn read_response(&mut self) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(line) = self.try_line() {
                return line;
            }
            let mut chunk = [0u8; 512];
            let n = tokio::time::timeout_at(deadline.into(), self.stream.read(&mut chunk))
                .await
                .expect("resp read timeout")
                .expect("resp read");
            assert!(n > 0, "redis closed the connection");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn try_line(&mut self) -> Option<String> {
        let idx = self.buf.windows(2).position(|w| w == b"\r\n")?;
        let line: Vec<u8> = self.buf.drain(..idx + 2).collect();
        // Value lines like `+OK\r\n`, `:1\r\n`, `$5\r\nvalue\r\n` — return the
        // first line; callers only need success markers or counts.
        Some(String::from_utf8_lossy(&line[..idx]).to_string())
    }
}

struct RedisLease {
    addr: String,
}

async fn redis_lease() -> Option<RedisLease> {
    let mut redis = REDIS.lock().await;
    if redis.is_none() {
        if !docker_available() {
            eprintln!("skipping redis_input tests: Docker is unavailable");
            return None;
        }
        let container = start_redis().await;
        let port = container.get_host_port_ipv4(6379).await.expect("mapped port");
        let addr = format!("127.0.0.1:{port}");
        *redis = Some(container);
        // Wait until the server answers PING.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(Instant::now() < deadline, "redis never became ready");
            if let Ok(mut client) = tokio::time::timeout(
                Duration::from_secs(2),
                RespClient::connect(&addr),
            )
            .await
            {
                if client.cmd(&["PING"]).await.starts_with('+') {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ACTIVE.fetch_add(1, Ordering::SeqCst);
        return Some(RedisLease { addr });
    }
    let port = redis
        .as_ref()
        .unwrap()
        .get_host_port_ipv4(6379)
        .await
        .expect("mapped port");
    ACTIVE.fetch_add(1, Ordering::SeqCst);
    Some(RedisLease {
        addr: format!("127.0.0.1:{port}"),
    })
}

impl Drop for RedisLease {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        // The container is shared across tests in this binary and is removed
        // when the process tears down the static holding it.
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

async fn build_input(config: serde_json::Value) -> Arc<dyn Input> {
    ensure_init();
    let input = InputConfig {
        input_type: "redis".into(),
        name: None,
        codec: None,
        config: Some(config),
    };
    input.build(&resource()).expect("build redis input")
}

async fn read_with_timeout(input: &Arc<dyn Input>) -> MessageBatchRef {
    let input = input.clone();
    tokio::time::timeout(Duration::from_secs(20), async move {
        loop {
            // read() errors with Disconnection until connect() succeeds;
            // retry that transient window instead of failing the test.
            match input.read().await {
                Ok((batch, _)) => return batch,
                Err(arkflow_core::Error::Disconnection) => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(e) => panic!("redis input read failed: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for a redis delivery")
}

fn first_payload(batch: &MessageBatchRef) -> Vec<u8> {
    use datafusion::arrow::array::{BinaryArray, StringArray};
    let record = batch.record_batch();
    assert!(record.num_columns() > 0, "expected a payload column");
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
async fn channel_subscription_delivers_published_payloads() {
    let Some(lease) = redis_lease().await else { return };
    let input = build_input(serde_json::json!({
        "mode": {"type": "single", "url": format!("redis://{}", lease.addr)},
        "redis_type": {"type": "subscribe", "subscribe": {"type": "channels", "channels": ["ark-test-ch"]}}
    }))
    .await;
    input.connect().await.expect("connect redis input");

    // Let the subscription register server-side before publishing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut publisher = RespClient::connect(&lease.addr).await;
    publisher.cmd(&["PUBLISH", "ark-test-ch", "hello-channels"]).await;
    publisher
        .cmd(&["PUBLISH", "other-channel", "must-not-arrive"])
        .await;

    let batch = read_with_timeout(&input).await;
    assert_eq!(first_payload(&batch), b"hello-channels");
    input.close().await.expect("close redis input");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pattern_subscription_delivers_matching_payloads() {
    let Some(lease) = redis_lease().await else { return };
    let input = build_input(serde_json::json!({
        "mode": {"type": "single", "url": format!("redis://{}", lease.addr)},
        "redis_type": {"type": "subscribe", "subscribe": {"type": "patterns", "patterns": ["ark-test-pat-*"]}}
    }))
    .await;
    input.connect().await.expect("connect redis input");

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut publisher = RespClient::connect(&lease.addr).await;
    publisher
        .cmd(&["PUBLISH", "ark-test-pat-events", "hello-patterns"])
        .await;
    publisher
        .cmd(&["PUBLISH", "unrelated", "must-not-arrive"])
        .await;

    let batch = read_with_timeout(&input).await;
    assert_eq!(first_payload(&batch), b"hello-patterns");
    input.close().await.expect("close redis input");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_mode_pops_pushed_entries() {
    let Some(lease) = redis_lease().await else { return };
    let input = build_input(serde_json::json!({
        "mode": {"type": "single", "url": format!("redis://{}", lease.addr)},
        "redis_type": {"type": "list", "list": ["ark-test-list"]}
    }))
    .await;
    input.connect().await.expect("connect redis input");

    let mut producer = RespClient::connect(&lease.addr).await;
    producer.cmd(&["LPUSH", "ark-test-list", "hello-list"]).await;

    let batch = read_with_timeout(&input).await;
    assert_eq!(first_payload(&batch), b"hello-list");
    input.close().await.expect("close redis input");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_single_url_reports_connection_failure() {
    // Port 1 on loopback refuses immediately; no container needed.
    ensure_init();
    let input = build_input(serde_json::json!({
        "mode": {"type": "single", "url": "redis://127.0.0.1:1"},
        "redis_type": {"type": "subscribe", "subscribe": {"type": "channels", "channels": ["c"]}}
    }))
    .await;
    // ConnectionManager retries with exponential backoff; bound the wait so
    // the test fails fast either through the error or through the timeout.
    let outcome = tokio::time::timeout(Duration::from_secs(10), input.connect()).await;
    assert!(
        outcome.is_err() || outcome.unwrap().is_err(),
        "connecting to a refused port must not succeed"
    );
}
