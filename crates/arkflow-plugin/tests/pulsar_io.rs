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

//! End-to-end correctness tests for the Pulsar input/output components
//! against a real Pulsar standalone broker (`apachepulsar/pulsar:3.3.3`)
//! started via `testcontainers`, following the hygiene discipline of
//! `kafka_eos.rs` (shared broker on a fixed host port, RAII lease, leftover
//! sweep, Docker-availability probe).
//!
//! Covers the two code-review findings this change fixes (P1-7 / P1-11):
//!
//! - the output actually delivers and only reports success once the broker
//!   receipt arrives (a stopped broker must surface as a write error, not a
//!   fire-and-forget `Ok`);
//! - an acknowledgement completes within a bounded time even when no new
//!   messages flow (the consumer mutex is no longer held across an
//!   unbounded `next()` wait).

use arkflow_core::input::{Input, InputConfig};
use arkflow_core::output::{Output, OutputConfig};
use arkflow_core::{MessageBatch, MessageBatchRef, Resource};
use datafusion::arrow::array::{BinaryArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;
use pulsar::{Pulsar, SubType, TokioExecutor};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Image, ImageExt};

/// `GenericImage` cannot carry a `CMD` in testcontainers 0.27, and an image
/// without an explicit cmd gets the runner's `/bin/sh` fallback (which
/// exits immediately). The pulsar image has no ENTRYPOINT, so wrap it to
/// run `bin/pulsar standalone` explicitly.
#[derive(Debug, Clone)]
struct PulsarStandalone {
    image: GenericImage,
}

impl Image for PulsarStandalone {
    fn name(&self) -> &str {
        self.image.name()
    }

    fn tag(&self) -> &str {
        self.image.tag()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        self.image.ready_conditions()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        self.image.expose_ports()
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        ["bin/pulsar", "standalone"]
    }
}

/// Fixed host port: the standalone broker advertises `localhost:6650` in
/// its broker lookup response, so a dynamic mapped port would break every
/// subsequent client redirect.
const PULSAR_HOST_PORT: u16 = 6650;
const SERVICE_URL: &str = "pulsar://127.0.0.1:6650";

static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

/// Shared broker — started once, reused by every test (see kafka_eos.rs for
/// the rationale and the lease/restart discipline).
static BROKER: LazyLock<tokio::sync::Mutex<Option<ContainerAsync<PulsarStandalone>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

struct BrokerLease;

async fn broker_lease() -> Option<BrokerLease> {
    let mut broker = BROKER.lock().await;
    if broker.is_none() {
        if !docker_available() {
            eprintln!("skipping pulsar_io tests: Docker is unavailable");
            return None;
        }
        sweep_leftover_brokers();
        *broker = Some(start_broker().await);
    }
    ACTIVE.fetch_add(1, Ordering::SeqCst);
    Some(BrokerLease)
}

impl Drop for BrokerLease {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        remove_shared_broker();
    }
}

fn remove_shared_broker() {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let container = loop {
        match BROKER.try_lock() {
            Ok(mut broker) => {
                if ACTIVE.load(Ordering::SeqCst) != 0 {
                    return;
                }
                break broker.take();
            }
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return,
        }
    };
    let Some(container) = container else {
        return;
    };
    let (done, released) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        if let Err(error) = runtime.block_on(container.rm()) {
            eprintln!("pulsar_io: broker container removal failed: {error}");
        }
        let _ = done.send(());
    });
    let _ = released.recv_timeout(Duration::from_secs(15));
}

/// Remove Pulsar testcontainers containers leaked by crashed runs — a
/// leftover holding host port 6650 breaks every later broker start.
fn sweep_leftover_brokers() {
    let Ok(listing) = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "label=org.testcontainers.managed-by=testcontainers",
            "--filter",
            "ancestor=apachepulsar/pulsar:3.3.3",
        ])
        .output()
    else {
        return;
    };
    if !listing.status.success() {
        return;
    }
    for id in String::from_utf8_lossy(&listing.stdout).split_whitespace() {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", id])
            .output();
    }
}

fn skip_without_docker(case: &str) {
    if std::env::var_os("ARKFLOW_REQUIRE_DOCKER").is_some() {
        panic!(
            "{case}: Docker is required (ARKFLOW_REQUIRE_DOCKER is set) but no daemon is reachable"
        );
    }
    eprintln!("--- SKIPPED {case}: Docker is unavailable ---");
}

fn docker_available() -> bool {
    if let Ok(host) = std::env::var("DOCKER_HOST") {
        if host.starts_with("tcp://") || host.starts_with("http://") {
            return true;
        }
        if let Some(path) = host.strip_prefix("unix://") {
            return std::path::Path::new(path).exists();
        }
    }
    ["/var/run/docker.sock", "/run/docker.sock"]
        .iter()
        .any(|path| std::path::Path::new(path).exists())
        || std::env::var_os("HOME").is_some_and(|home| {
            let home = std::path::PathBuf::from(home);
            [
                ".docker/run/docker.sock",
                ".colima/default/docker.sock",
                ".rd/docker.sock",
                ".local/share/containers/podman/machine/podman.sock",
            ]
            .iter()
            .any(|relative| home.join(relative).exists())
        })
}

static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_init() {
    INIT.call_once(|| {
        arkflow_plugin::input::init().expect("plugin input init");
        arkflow_plugin::output::init().expect("plugin output init");
    });
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

/// The image's default command already runs `bin/pulsar standalone`.
async fn start_broker() -> ContainerAsync<PulsarStandalone> {
    let image = PulsarStandalone {
        image: GenericImage::new("apachepulsar/pulsar", "3.3.3"),
    }
    .with_mapped_port(PULSAR_HOST_PORT, PULSAR_HOST_PORT.tcp())
    .with_startup_timeout(Duration::from_secs(180));
    let container = image.start().await.expect("pulsar container start");
    wait_for_broker().await;
    container
}

/// Poll until a Pulsar client can connect AND complete a topic lookup
/// (standalone opens the binary port before the topic service is ready).
async fn wait_for_broker() {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = async {
            let client = Pulsar::builder(SERVICE_URL, TokioExecutor)
                .build()
                .await
                .map_err(|e| e.to_string())?;
            let probe = unique("ready-probe");
            client
                .producer()
                .with_topic(&probe)
                .build()
                .await
                .map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        }
        .await
        .is_ok();
        if ready {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("Pulsar broker not ready within 120s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn build_output(topic_expr: serde_json::Value) -> Arc<dyn Output> {
    ensure_init();
    let out = OutputConfig {
        output_type: "pulsar".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "service_url": SERVICE_URL,
            "topic": topic_expr,
        })),
    };
    let out = out.build(&resource()).expect("build pulsar output");
    out.connect().await.expect("connect pulsar output");
    out
}

async fn build_input(topic: &str, subscription: &str) -> Arc<dyn Input> {
    ensure_init();
    let input = InputConfig {
        input_type: "pulsar".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "service_url": SERVICE_URL,
            "topic": topic,
            "subscription_name": subscription,
            "subscription_type": "shared",
        })),
    };
    let input = input.build(&resource()).expect("build pulsar input");
    input.connect().await.expect("connect pulsar input");
    input
}

/// Consume `expected` messages from `topic` under a fresh subscription,
/// returning the payloads in arrival order.
async fn consume_all(topic: &str, expected: usize) -> Vec<Vec<u8>> {
    let client = Pulsar::builder(SERVICE_URL, TokioExecutor)
        .build()
        .await
        .expect("verify client");
    let mut consumer: pulsar::consumer::Consumer<Vec<u8>, TokioExecutor> = client
        .consumer()
        .with_topic(topic)
        .with_subscription(unique("verify"))
        .with_subscription_type(SubType::Shared)
        .with_options(
            pulsar::consumer::ConsumerOptions::default()
                .with_initial_position(pulsar::consumer::InitialPosition::Earliest),
        )
        .build()
        .await
        .expect("verify consumer");
    let mut payloads = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while payloads.len() < expected {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let message = tokio::time::timeout(remaining, consumer.next())
            .await
            .expect("timed out waiting for produced messages")
            .expect("consumer stream ended")
            .expect("consumer error");
        payloads.push(message.payload.data);
    }
    payloads
}

/// Produce one message with the raw client, awaiting the broker receipt.
async fn raw_produce(topic: &str, payload: &[u8]) {
    let client = Pulsar::builder(SERVICE_URL, TokioExecutor)
        .build()
        .await
        .expect("raw client");
    let mut producer = client
        .producer()
        .with_topic(topic)
        .build()
        .await
        .expect("raw producer");
    producer
        .send_non_blocking(payload.to_vec())
        .await
        .expect("raw enqueue")
        .await
        .expect("raw receipt");
}

fn binary_batch(payload: &[u8]) -> MessageBatchRef {
    Arc::new(MessageBatch::new_binary(vec![payload.to_vec()]).expect("binary batch"))
}

/// An arrow batch whose payloads come from `__value__` and whose per-message
/// topics come from `topic_col` (exercising the `Vec` expression path).
fn routed_batch(rows: &[(&[u8], &str)]) -> MessageBatchRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("__value__", DataType::Binary, false),
        Field::new("topic_col", DataType::Utf8, false),
    ]));
    let values: Vec<Option<&[u8]>> = rows.iter().map(|(v, _)| Some(*v)).collect();
    let topics: Vec<Option<&str>> = rows.iter().map(|(_, t)| Some(*t)).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(values)),
            Arc::new(StringArray::from(topics)),
        ],
    )
    .expect("routed batch");
    Arc::new(MessageBatch::new_arrow(batch))
}

#[tokio::test]
#[serial_test::serial]
async fn output_delivers_messages_verified_by_real_consumer() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("output delivers messages");
        return;
    };
    let topic = unique("e2e-out");
    let output = build_output(serde_json::json!({"type": "value", "value": topic})).await;

    let expected: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("payload-{i}").into_bytes())
        .collect();
    for payload in &expected {
        output.write(binary_batch(payload)).await.expect("write");
    }
    output.close().await.expect("close");

    let received = consume_all(&topic, expected.len()).await;
    assert_eq!(received, expected, "delivered payloads must match exactly");
}

/// Build the output through the public registry with `value_field`.
#[tokio::test]
#[serial_test::serial]
async fn output_value_field_selects_payload_column() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("output value_field selects payload column");
        return;
    };
    let topic = unique("vf-out");
    ensure_init();
    let out = OutputConfig {
        output_type: "pulsar".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "service_url": SERVICE_URL,
            "topic": {"type": "value", "value": topic},
            "value_field": "payload_col",
        })),
    };
    let out = out.build(&resource()).expect("build pulsar output");
    out.connect().await.expect("connect pulsar output");

    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload_col",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("vf-one"),
            Some("vf-two"),
        ]))],
    )
    .expect("value-field batch");
    out.write(Arc::new(MessageBatch::new_arrow(batch)))
        .await
        .expect("write");
    out.close().await.expect("close");

    assert_eq!(
        consume_all(&topic, 2).await,
        vec![b"vf-one".to_vec(), b"vf-two".to_vec()],
        "value_field column values must be the payloads"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn output_routes_per_message_topics() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("output routes per-message topics");
        return;
    };
    let topic_a = unique("route-a");
    let topic_b = unique("route-b");
    let output = build_output(serde_json::json!({"type": "expr", "expr": "topic_col"})).await;

    output
        .write(routed_batch(&[
            (b"to-a".as_slice(), &topic_a),
            (b"to-b".as_slice(), &topic_b),
        ]))
        .await
        .expect("routed write");
    output.close().await.expect("close");

    assert_eq!(
        consume_all(&topic_a, 1).await,
        vec![b"to-a".to_vec()],
        "row 0 must land on its own topic"
    );
    assert_eq!(
        consume_all(&topic_b, 1).await,
        vec![b"to-b".to_vec()],
        "row 1 must land on its own topic"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn output_write_fails_when_broker_stops() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("output write fails when broker stops");
        return;
    };
    let topic = unique("broker-down");
    let output = build_output(serde_json::json!({"type": "value", "value": topic})).await;

    // Prove the path works first, then kill the broker.
    output
        .write(binary_batch(b"before"))
        .await
        .expect("write before stop");

    stop_shared_broker().await;

    // A reliable output must resolve to an error (not fire-and-forget Ok,
    // and not hang) once the broker is gone.
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        output.write(binary_batch(b"after")),
    )
    .await
    .expect("write must resolve after broker loss, not hang");
    assert!(
        outcome.is_err(),
        "write after broker loss must fail: {outcome:?}"
    );
}

/// Stop and remove the shared broker, clearing the slot so the next lease
/// starts a fresh container.
async fn stop_shared_broker() {
    let container = { BROKER.lock().await.take() };
    let Some(container) = container else { return };
    let _ = container.stop().await;
    let _ = container.rm().await;
}

#[tokio::test]
#[serial_test::serial]
async fn input_reads_and_ack_prevents_redelivery() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker(" input reads and ack prevents redelivery");
        return;
    };
    let topic = unique("e2e-in");
    let subscription = unique("sub");
    for i in 0..3 {
        raw_produce(&topic, format!("in-{i}").as_bytes()).await;
    }

    let input = build_input(&topic, &subscription).await;
    let mut acks = Vec::new();
    let mut payloads = Vec::new();
    for _ in 0..3 {
        let (batch, ack) = tokio::time::timeout(Duration::from_secs(30), input.read())
            .await
            .expect("timed out waiting for input read")
            .expect("input read");
        let binary = batch.to_binary("__value__").expect("binary payload");
        assert_eq!(binary.len(), 1, "one delivery per read");
        payloads.push(binary[0].to_vec());
        acks.push(ack);
    }
    for ack in &acks {
        ack.ack().await.expect("ack");
    }
    input.close().await.expect("close input");
    payloads.sort();
    assert_eq!(
        payloads,
        vec![b"in-0".to_vec(), b"in-1".to_vec(), b"in-2".to_vec()]
    );

    // A fresh consumer on the same subscription must not re-receive the
    // acknowledged messages.
    let client = Pulsar::builder(SERVICE_URL, TokioExecutor)
        .build()
        .await
        .expect("client");
    let mut verifier: pulsar::consumer::Consumer<Vec<u8>, TokioExecutor> = client
        .consumer()
        .with_topic(&topic)
        .with_subscription(&subscription)
        .with_subscription_type(SubType::Shared)
        .build()
        .await
        .expect("verify consumer");
    if let Ok(Some(Ok(message))) =
        tokio::time::timeout(Duration::from_secs(5), verifier.next()).await
    {
        panic!(
            "acknowledged message re-delivered: {:?}",
            message.payload.data
        );
    }
}

#[tokio::test]
#[serial_test::serial]
async fn ack_completes_when_message_flow_stops() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("ack completes when message flow stops");
        return;
    };
    let topic = unique("idle-ack");
    let subscription = unique("sub");
    raw_produce(&topic, b"only-message").await;

    let input = build_input(&topic, &subscription).await;
    let (_batch, ack) = tokio::time::timeout(Duration::from_secs(30), input.read())
        .await
        .expect("timed out waiting for input read")
        .expect("input read");

    // Let the consumer task park on `next()` again — the exact situation
    // where the unbounded lock hold used to starve the ack forever.
    tokio::time::sleep(Duration::from_millis(500)).await;

    tokio::time::timeout(Duration::from_secs(3), ack.ack())
        .await
        .expect("ack must complete within the bounded lock budget, not hang")
        .expect("ack must succeed");
    input.close().await.expect("close input");
}
