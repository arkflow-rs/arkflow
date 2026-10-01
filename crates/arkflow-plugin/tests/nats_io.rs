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

//! End-to-end tests for the NATS input component against a real
//! `nats:2-alpine` server (JetStream enabled, username/password auth)
//! started via `testcontainers`, following the hygiene discipline of
//! `pulsar_io.rs` / `kafka_eos.rs` (shared container on a fixed host
//! port, RAII lease, leftover sweep, Docker-availability probe).

use arkflow_core::input::{Input, InputConfig};
use arkflow_core::{Error, Resource};
use async_nats::ConnectOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::Duration;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Image, ImageExt, core::IntoContainerPort};

const NATS_HOST_PORT: u16 = 4222;
const NATS_URL: &str = "nats://127.0.0.1:4222";
const NATS_USER: &str = "ark";
const NATS_PASS: &str = "flow";

/// `GenericImage` cannot carry a `CMD` in testcontainers 0.27, and the
/// nats image would otherwise start without JetStream or auth. Wrap it to
/// run the server with an in-memory JetStream store and test credentials
/// (exercising the input's user/password auth branch on every connect).
#[derive(Debug, Clone)]
struct NatsServer {
    image: GenericImage,
}

impl Image for NatsServer {
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
        [
            "-js",
            "-sd",
            "/tmp",
            "--user",
            NATS_USER,
            "--pass",
            NATS_PASS,
        ]
    }
}

static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{}_{}-{}",
        prefix,
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

/// Shared server — started once, reused by every test in this binary.
static SERVER: LazyLock<tokio::sync::Mutex<Option<ContainerAsync<NatsServer>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

struct ServerLease;

async fn server_lease() -> Option<ServerLease> {
    let mut server = SERVER.lock().await;
    if server.is_none() {
        if !docker_available() {
            eprintln!("skipping nats_io tests: Docker is unavailable");
            return None;
        }
        sweep_leftover_servers();
        *server = Some(start_server().await);
    }
    ACTIVE.fetch_add(1, Ordering::SeqCst);
    Some(ServerLease)
}

impl Drop for ServerLease {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        remove_shared_server();
    }
}

fn remove_shared_server() {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let container = loop {
        match SERVER.try_lock() {
            Ok(mut server) => {
                if ACTIVE.load(Ordering::SeqCst) != 0 {
                    return;
                }
                break server.take();
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
            eprintln!("nats_io: container removal failed: {error}");
        }
        let _ = done.send(());
    });
    let _ = released.recv_timeout(Duration::from_secs(15));
}

/// Remove testcontainers NATS containers leaked by crashed runs — a
/// leftover holding host port 4222 breaks every later server start.
fn sweep_leftover_servers() {
    let Ok(listing) = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "label=org.testcontainers.managed-by=testcontainers",
            "--filter",
            "ancestor=nats:2-alpine",
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

async fn start_server() -> ContainerAsync<NatsServer> {
    let image = NatsServer {
        image: GenericImage::new("nats", "2-alpine"),
    }
    .with_mapped_port(NATS_HOST_PORT, NATS_HOST_PORT.tcp())
    .with_startup_timeout(Duration::from_secs(180));
    let container = image.start().await.expect("nats container start");
    wait_for_server().await;
    container
}

/// Poll until an authenticated client can connect AND JetStream answers.
async fn wait_for_server() {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = async {
            let client = admin_client().await?;
            let jetstream = async_nats::jetstream::new(client);
            jetstream
                .get_or_create_stream(async_nats::jetstream::stream::Config {
                    name: "READY_PROBE".to_string(),
                    subjects: vec!["ready.probe".to_string()],
                    ..Default::default()
                })
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        .await
        .is_ok();
        if ready {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("NATS server not ready within 120s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn admin_client() -> Result<async_nats::Client, String> {
    ConnectOptions::new()
        .user_and_password(NATS_USER.to_string(), NATS_PASS.to_string())
        .connect(NATS_URL)
        .await
        .map_err(|e| e.to_string())
}

async fn build_input(config: serde_json::Value) -> Arc<dyn Input> {
    ensure_init();
    let input = InputConfig {
        input_type: "nats".into(),
        name: None,
        codec: None,
        config: Some(config),
    };
    let input = input.build(&resource()).expect("build nats input");
    input.connect().await.expect("connect nats input");
    input
}

fn auth() -> serde_json::Value {
    serde_json::json!({
        "username": NATS_USER,
        "password": NATS_PASS,
    })
}

async fn read_one(input: &Arc<dyn Input>) -> (Vec<u8>, Arc<dyn arkflow_core::input::Ack>) {
    let (batch, ack) = tokio::time::timeout(Duration::from_secs(20), input.read())
        .await
        .expect("timed out waiting for NATS input read")
        .expect("NATS input read");
    let binary = batch.to_binary("__value__").expect("binary payload");
    assert_eq!(binary.len(), 1, "one delivery per read");
    (binary[0].to_vec(), ack)
}

#[tokio::test]
#[serial_test::serial]
async fn regular_input_reads_and_acks_published_messages() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("nats regular input reads and acks");
        return;
    };
    let subject = unique("test.subject");
    let input = build_input(serde_json::json!({
        "url": NATS_URL,
        "mode": {"type": "regular", "subject": subject},
        "auth": auth(),
    }))
    .await;

    let client = admin_client().await.expect("publisher client");
    let expected: Vec<Vec<u8>> = (0..3).map(|i| format!("payload-{i}").into_bytes()).collect();
    for payload in &expected {
        client
            .publish(subject.clone(), payload.clone().into())
            .await
            .expect("publish");
    }
    client.flush().await.expect("flush");

    let mut received = Vec::new();
    for _ in 0..3 {
        let (payload, ack) = read_one(&input).await;
        received.push(payload);
        ack.ack().await.expect("regular ack is a no-op success");
    }
    received.sort();
    assert_eq!(received, expected, "delivered payloads must match");
    input.close().await.expect("close input");
}

#[tokio::test]
#[serial_test::serial]
async fn regular_input_with_queue_group_reads_messages() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("nats regular input with queue group");
        return;
    };
    let subject = unique("queue.subject");
    let input = build_input(serde_json::json!({
        "url": NATS_URL,
        "mode": {"type": "regular", "subject": subject, "queue_group": "workers"},
        "auth": auth(),
    }))
    .await;

    let client = admin_client().await.expect("publisher client");
    client
        .publish(subject.clone(), b"queue-payload".to_vec().into())
        .await
        .expect("publish");
    client.flush().await.expect("flush");

    let (payload, ack) = read_one(&input).await;
    assert_eq!(payload, b"queue-payload".to_vec());
    ack.ack().await.expect("ack");
    input.close().await.expect("close input");
}

#[tokio::test]
#[serial_test::serial]
async fn jetstream_input_reads_and_acks_durable_messages() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("nats jetstream input reads and acks");
        return;
    };
    let stream_name = unique("STREAM").replace('-', "_");
    let subject = unique("js.subject");
    let consumer_name = unique("consumer").replace('-', "_");

    let client = admin_client().await.expect("admin client");
    let jetstream = async_nats::jetstream::new(client.clone());
    jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .expect("create stream");

    // More than the initial fetch batch (10): the remainder must arrive
    // through the spawned background fetch loop.
    const TOTAL: usize = 25;
    for i in 0..TOTAL {
        let ack = jetstream
            .publish(subject.clone(), format!("js-{i}").into_bytes().into())
            .await
            .expect("js publish");
        ack.await.expect("js publish ack");
    }

    let input = build_input(serde_json::json!({
        "url": NATS_URL,
        "mode": {
            "type": "jet_stream",
            "stream": stream_name,
            "consumer_name": consumer_name,
            "durable_name": consumer_name,
        },
        "auth": auth(),
    }))
    .await;

    let mut payloads = Vec::new();
    for _ in 0..TOTAL {
        let (payload, ack) = read_one(&input).await;
        payloads.push(payload);
        ack.ack().await.expect("jetstream ack");
    }
    let mut expected: Vec<Vec<u8>> = (0..TOTAL).map(|i| format!("js-{i}").into_bytes()).collect();
    payloads.sort();
    expected.sort();
    assert_eq!(payloads, expected, "all published messages must be delivered");
    input.close().await.expect("close input");

    // A second durable consumer on the same subscription must not
    // re-receive the acknowledged messages.
    let verifier = build_input(serde_json::json!({
        "url": NATS_URL,
        "mode": {
            "type": "jet_stream",
            "stream": stream_name,
            "consumer_name": consumer_name,
            "durable_name": consumer_name,
        },
        "auth": auth(),
    }))
    .await;
    match tokio::time::timeout(Duration::from_secs(2), verifier.read()).await {
        Err(_) => {} // no redelivery within the window: pass
        Ok(Err(_)) => {} // surfaced as disconnection, not redelivered data: pass
        Ok(Ok((batch, _))) => panic!(
            "acknowledged JetStream messages were re-delivered: {:?}",
            batch.to_binary("__value__")
        ),
    }
    verifier.close().await.expect("close verifier");
}

#[tokio::test]
#[serial_test::serial]
async fn jetstream_unknown_stream_fails_at_connect() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("nats jetstream unknown stream");
        return;
    };
    ensure_init();
    let missing = unique("missing");
    let input = InputConfig {
        input_type: "nats".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "url": NATS_URL,
            "mode": {
                "type": "jet_stream",
                "stream": missing,
                "consumer_name": "c",
                "durable_name": "c",
            },
            "auth": auth(),
        })),
    };
    let input = input.build(&resource()).expect("build nats input");
    let error = input.connect().await.unwrap_err();
    assert!(
        matches!(error, Error::Connection(_)),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("JetStream"),
        "{error}"
    );
}
