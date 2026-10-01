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

//! End-to-end tests for the MQTT input component against a real
//! `eclipse-mosquitto:2` broker started via `testcontainers`, following
//! the hygiene discipline of `pulsar_io.rs` / `kafka_eos.rs` (shared
//! container on a fixed host port, RAII lease, leftover sweep,
//! Docker-availability probe).

use arkflow_core::input::{Input, InputConfig};
use arkflow_core::Resource;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::IntoContainerPort};

const MQTT_HOST_PORT: u16 = 1883;
const MQTT_HOST: &str = "127.0.0.1";

static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

/// Shared broker — started once, reused by every test in this binary.
/// The image's default command runs `mosquitto -c /mosquitto-no-auth.conf`
/// (anonymous access on 1883).
static BROKER: LazyLock<tokio::sync::Mutex<Option<ContainerAsync<GenericImage>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

struct BrokerLease;

async fn broker_lease() -> Option<BrokerLease> {
    let mut broker = BROKER.lock().await;
    if broker.is_none() {
        if !docker_available() {
            eprintln!("skipping mqtt_io tests: Docker is unavailable");
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
            eprintln!("mqtt_io: broker container removal failed: {error}");
        }
        let _ = done.send(());
    });
    let _ = released.recv_timeout(Duration::from_secs(15));
}

/// Remove testcontainers mosquitto containers leaked by crashed runs — a
/// leftover holding host port 1883 breaks every later broker start.
fn sweep_leftover_brokers() {
    let Ok(listing) = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "label=org.testcontainers.managed-by=testcontainers",
            "--filter",
            "ancestor=eclipse-mosquitto:2",
        ])
        .output()
    else {
        return;
    };
    if !listing.status.success() {
        return;
    }
    for id in String::from_utf8_lossy(&listing.stdout).split_whitespace() {
        let _ = std::process::Command::new("docker").args(["rm", "-f", id]).output();
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
    });
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

async fn start_broker() -> ContainerAsync<GenericImage> {
    let image = GenericImage::new("eclipse-mosquitto", "2")
        .with_mapped_port(MQTT_HOST_PORT, MQTT_HOST_PORT.tcp())
        .with_startup_timeout(Duration::from_secs(120));
    let container = image.start().await.expect("mosquitto container start");
    wait_for_broker().await;
    container
}

/// Poll until the MQTT listener accepts TCP connections.
async fn wait_for_broker() {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if std::net::TcpStream::connect((MQTT_HOST, MQTT_HOST_PORT)).is_ok() {
            tokio::time::sleep(Duration::from_millis(300)).await;
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("Mosquitto broker not ready within 60s");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn build_input(topics: Vec<String>, qos: u8) -> Arc<dyn Input> {
    ensure_init();
    let input = InputConfig {
        input_type: "mqtt".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "host": MQTT_HOST,
            "port": MQTT_HOST_PORT,
            "client_id": unique("input"),
            "topics": topics,
            "qos": qos,
            "keep_alive": 30,
        })),
    };
    let input = input.build(&resource()).expect("build mqtt input");
    input.connect().await.expect("connect mqtt input");
    input
}

/// A short-lived publisher client with its eventloop polled in the
/// background.
async fn publish(topic: &str, payloads: &[&[u8]]) {
    let mut options = MqttOptions::new(unique("publisher"), MQTT_HOST, MQTT_HOST_PORT);
    options.set_keep_alive(Duration::from_secs(30));
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    tokio::spawn(async move {
        loop {
            if eventloop.poll().await.is_err() {
                break;
            }
        }
    });
    for payload in payloads {
        client
            .publish(topic.to_string(), QoS::AtLeastOnce, false, payload.to_vec())
            .await
            .expect("publish");
    }
    // Let the broker route the messages before the assertions read them.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn read_one(input: &Arc<dyn Input>) -> (Vec<u8>, Arc<dyn arkflow_core::input::Ack>) {
    let (batch, ack) = tokio::time::timeout(Duration::from_secs(20), input.read())
        .await
        .expect("timed out waiting for MQTT input read")
        .expect("MQTT input read");
    let binary = batch.to_binary("__value__").expect("binary payload");
    assert_eq!(binary.len(), 1, "one delivery per read");
    (binary[0].to_vec(), ack)
}

#[tokio::test]
#[serial_test::serial]
async fn input_reads_and_acks_published_messages() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("mqtt input reads and acks");
        return;
    };
    let topic = unique("sensors/room");
    let input = build_input(vec![topic.clone()], 1).await;
    // Give the broker time to register the subscription: messages published
    // before it is active are dropped (MQTT has no replay for new subs).
    tokio::time::sleep(Duration::from_millis(2_000)).await;

    let expected: Vec<Vec<u8>> = (0..3).map(|i| format!("reading-{i}").into_bytes()).collect();
    let payloads: Vec<&[u8]> = expected.iter().map(|v| v.as_slice()).collect();
    publish(&topic, &payloads).await;

    let mut received = Vec::new();
    for _ in 0..3 {
        let (payload, ack) = read_one(&input).await;
        received.push(payload);
        ack.ack().await.expect("manual ack must succeed");
    }
    received.sort();
    assert_eq!(received, expected, "delivered payloads must match");
    input.close().await.expect("close input");
}

#[tokio::test]
#[serial_test::serial]
async fn wildcard_subscription_receives_nested_topics() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("mqtt wildcard subscription");
        return;
    };
    let base = unique("plant");
    let input = build_input(vec![format!("{base}/#")], 1).await;
    tokio::time::sleep(Duration::from_millis(2_000)).await;

    publish(&format!("{base}/line1/machine"), &[b"on".as_slice()]).await;

    let (payload, ack) = read_one(&input).await;
    assert_eq!(payload, b"on".to_vec());
    ack.ack().await.expect("ack");
    input.close().await.expect("close input");
}

#[tokio::test]
#[serial_test::serial]
async fn close_prevents_further_reads() {
    let Some(_lease) = broker_lease().await else {
        skip_without_docker("mqtt close prevents further reads");
        return;
    };
    let topic = unique("shutdown");
    let input = build_input(vec![topic.clone()], 0).await;
    input.close().await.expect("close input");

    // The client handle is cleared by close; read must surface it.
    let _ = topic;
    let result = tokio::time::timeout(Duration::from_secs(5), input.read()).await;
    match result {
        Err(_) => panic!("read after close must resolve, not hang"),
        Ok(Err(_)) => {} // Disconnection: expected
        Ok(Ok(_)) => panic!("read after close must not deliver messages"),
    }
}
