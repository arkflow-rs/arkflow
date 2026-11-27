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

#![cfg(feature = "connectors-amqp09")]

//! End-to-end tests for the AMQP 0.9/0.9.1 (RabbitMQ) input and output
//! components against a real `rabbitmq:3-alpine` broker started via
//! `testcontainers`.

use arkflow_core::input::InputConfig;
use arkflow_core::output::OutputConfig;
use arkflow_core::{MessageBatch, Resource};
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use lapin::{Connection, ConnectionProperties};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const AMQP_CONTAINER_PORT: u16 = 5672;

static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

struct SharedServer {
    container: ContainerAsync<GenericImage>,
    url: String,
}

static SERVER: LazyLock<tokio::sync::Mutex<Option<SharedServer>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

struct ServerLease {
    url: String,
}

async fn server_lease() -> Option<ServerLease> {
    let mut server = SERVER.lock().await;
    if server.is_none() {
        if !docker_available() {
            eprintln!("skipping amqp09_io tests: Docker is unavailable");
            return None;
        }
        sweep_leftover_servers();
        *server = Some(start_server().await);
    }
    let url = server
        .as_ref()
        .expect("shared server initialized")
        .url
        .clone();
    ACTIVE.fetch_add(1, Ordering::SeqCst);
    Some(ServerLease { url })
}

impl Drop for ServerLease {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        remove_shared_server();
    }
}

/// Remove the shared container once the last lease drops. Keeps `SERVER`
/// locked across `container.rm()` (and uses dynamic host ports per container)
/// so no subsequent lease can ever race with a tearing-down container.
fn remove_shared_server() {
    let (done, released) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            let _ = done.send(());
            return;
        };
        runtime.block_on(async {
            let mut server = SERVER.lock().await;
            if ACTIVE.load(Ordering::SeqCst) != 0 {
                return;
            }
            if let Some(shared) = server.take() {
                if let Err(error) = shared.container.rm().await {
                    eprintln!("amqp09_io: container removal failed: {error}");
                }
            }
        });
        let _ = done.send(());
    });
    let _ = released.recv();
}

fn sweep_leftover_servers() {
    let Ok(listing) = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "status=exited",
            "--filter",
            "label=org.testcontainers.managed-by=testcontainers",
            "--filter",
            "ancestor=rabbitmq:3-alpine",
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
        arkflow_plugin::initialize().expect("plugin initialize");
    });
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

async fn start_server() -> SharedServer {
    let image = GenericImage::new("rabbitmq", "3-alpine")
        .with_exposed_port(AMQP_CONTAINER_PORT.tcp())
        .with_startup_timeout(Duration::from_secs(180));
    let container = image.start().await.expect("rabbitmq container start");
    let host_port = container
        .get_host_port_ipv4(AMQP_CONTAINER_PORT)
        .await
        .expect("rabbitmq mapped AMQP port");
    let url = format!("amqp://guest:guest@127.0.0.1:{host_port}/%2f");
    wait_for_server(&url).await;
    SharedServer { container, url }
}

async fn wait_for_server(url: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = async {
            let conn = Connection::connect(url, ConnectionProperties::default()).await?;
            let ch = conn.create_channel().await?;
            ch.close(200, "probe".into()).await?;
            conn.close(200, "probe".into()).await?;
            Ok::<_, lapin::Error>(())
        }
        .await
        .is_ok();
        if ready {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("RabbitMQ server not ready within 120s at {url}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn amqp09_roundtrip_with_confirms_properties_and_manual_ack() {
    ensure_init();
    let Some(lease) = server_lease().await else {
        skip_without_docker("amqp09_roundtrip_with_confirms_properties_and_manual_ack");
        return;
    };

    let exchange = unique("ex_direct");
    let queue = unique("q_direct");
    let rk = "orders.created";

    let input_cfg: InputConfig = serde_json::from_value(serde_json::json!({
        "type": "amqp09",
        "name": "amqp_in",
        "url": lease.url,
        "queue": queue,
        "exchange": exchange,
        "exchange_type": "direct",
        "routing_key": rk,
        "declare_exchange": true,
        "declare_queue": true,
        "durable": false,
        "auto_ack": false,
        "prefetch_count": 10
    }))
    .unwrap();
    let input = input_cfg.build(&resource()).unwrap();
    input.connect().await.unwrap();

    let output_cfg: OutputConfig = serde_json::from_value(serde_json::json!({
        "type": "amqp09",
        "url": lease.url,
        "exchange": exchange,
        "exchange_type": "direct",
        "routing_key": { "type": "value", "value": rk },
        "declare_exchange": true,
        "durable": false,
        "persistent": true,
        "confirm": true,
        "priority": 3,
        "expiration": 60000,
        "content_type": "application/json",
        "content_encoding": "utf-8"
    }))
    .unwrap();
    let output = output_cfg.build(&resource()).unwrap();
    output.connect().await.unwrap();

    let batch = Arc::new(
        MessageBatch::new_binary(vec![b"hello-amqp-1".to_vec(), b"hello-amqp-2".to_vec()]).unwrap(),
    );
    output.write(batch).await.unwrap();

    let mut received = Vec::new();
    for _ in 0..2 {
        let (msg, ack) = tokio::time::timeout(Duration::from_secs(10), input.read())
            .await
            .expect("timed out reading from amqp09")
            .expect("read succeeded");
        assert_eq!(msg.get_input_name(), Some("amqp_in".to_string()));
        let payloads = msg
            .to_binary(arkflow_core::DEFAULT_BINARY_VALUE_FIELD)
            .unwrap();
        assert_eq!(payloads.len(), 1);
        received.push(payloads[0].to_vec());
        ack.ack().await.expect("manual ack succeeds");
    }

    assert_eq!(
        received,
        vec![b"hello-amqp-1".to_vec(), b"hello-amqp-2".to_vec()]
    );

    output.close().await.unwrap();
    input.close().await.unwrap();
}

#[tokio::test]
async fn amqp09_fanout_and_dynamic_routing_key_and_reconnect() {
    ensure_init();
    let Some(lease) = server_lease().await else {
        skip_without_docker("amqp09_fanout_and_dynamic_routing_key_and_reconnect");
        return;
    };

    // 1. Fanout exchange: queue binds without specifying a routing_key.
    let fanout_ex = unique("ex_fanout");
    let fanout_q = unique("q_fanout");

    let input_cfg: InputConfig = serde_json::from_value(serde_json::json!({
        "type": "amqp09",
        "url": lease.url,
        "queue": fanout_q,
        "exchange": fanout_ex,
        "exchange_type": "fanout",
        "declare_exchange": true,
        "declare_queue": true,
        "durable": false,
        "auto_ack": true
    }))
    .unwrap();
    let input = input_cfg.build(&resource()).unwrap();
    input.connect().await.unwrap();

    let output_cfg: OutputConfig = serde_json::from_value(serde_json::json!({
        "type": "amqp09",
        "url": lease.url,
        "exchange": fanout_ex,
        "exchange_type": "fanout",
        "routing_key": { "type": "expr", "expr": "rk" },
        "declare_exchange": true,
        "durable": false,
        "confirm": true,
        "value_field": "body"
    }))
    .unwrap();
    let output = output_cfg.build(&resource()).unwrap();
    output.connect().await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["ignored.by.fanout"])),
            Arc::new(StringArray::from(vec!["fanout-payload-1"])),
        ],
    )
    .unwrap();
    output
        .write(Arc::new(MessageBatch::new_arrow(rb)))
        .await
        .unwrap();

    let (msg, ack) = tokio::time::timeout(Duration::from_secs(10), input.read())
        .await
        .expect("timed out reading fanout message")
        .expect("read succeeded");
    let payloads = msg
        .to_binary(arkflow_core::DEFAULT_BINARY_VALUE_FIELD)
        .unwrap();
    assert_eq!(payloads, vec![b"fanout-payload-1".as_slice()]);
    ack.ack().await.unwrap();

    // 2. Close and reconnect input: fresh CancellationToken must allow reading new messages.
    input.close().await.unwrap();
    input.connect().await.unwrap();

    let schema2 = Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let rb2 = RecordBatch::try_new(
        schema2,
        vec![
            Arc::new(StringArray::from(vec!["after.reconnect"])),
            Arc::new(StringArray::from(vec!["fanout-payload-2"])),
        ],
    )
    .unwrap();
    output
        .write(Arc::new(MessageBatch::new_arrow(rb2)))
        .await
        .unwrap();

    let (msg2, ack2) = tokio::time::timeout(Duration::from_secs(10), input.read())
        .await
        .expect("timed out reading after reconnect")
        .expect("read after reconnect succeeded");
    let payloads2 = msg2
        .to_binary(arkflow_core::DEFAULT_BINARY_VALUE_FIELD)
        .unwrap();
    assert_eq!(payloads2, vec![b"fanout-payload-2".as_slice()]);
    ack2.ack().await.unwrap();

    output.close().await.unwrap();
    input.close().await.unwrap();
}
