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

//! End-to-end tests for the MongoDB output component against a real
//! `mongo:7` server started via `testcontainers`, following the hygiene
//! discipline of `pulsar_io.rs` / `kafka_eos.rs` (shared container on a
//! fixed host port, RAII lease, leftover sweep, Docker-availability probe).

use arkflow_core::output::{Output, OutputConfig};
use arkflow_core::{MessageBatch, MessageBatchRef, Resource};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{Field, Schema};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers::{core::IntoContainerPort, ContainerAsync, GenericImage, ImageExt};

const MONGO_HOST_PORT: u16 = 27017;
const MONGO_URI: &str = "mongodb://127.0.0.1:27017";

static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

/// Shared server — started once, reused by every test in this binary.
static SERVER: LazyLock<tokio::sync::Mutex<Option<ContainerAsync<GenericImage>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

struct ServerLease;

async fn server_lease() -> Option<ServerLease> {
    let mut server = SERVER.lock().await;
    if server.is_none() {
        if !docker_available() {
            eprintln!("skipping mongodb_io tests: Docker is unavailable");
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
            eprintln!("mongodb_io: container removal failed: {error}");
        }
        let _ = done.send(());
    });
    let _ = released.recv_timeout(Duration::from_secs(15));
}

/// Remove testcontainers Mongo containers leaked by crashed runs — a
/// leftover holding host port 27017 breaks every later server start.
fn sweep_leftover_servers() {
    let Ok(listing) = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "label=org.testcontainers.managed-by=testcontainers",
            "--filter",
            "ancestor=mongo:7",
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
        arkflow_plugin::output::init().expect("plugin output init");
    });
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

async fn start_server() -> ContainerAsync<GenericImage> {
    let image = GenericImage::new("mongo", "7")
        .with_mapped_port(MONGO_HOST_PORT, MONGO_HOST_PORT.tcp())
        .with_startup_timeout(Duration::from_secs(180));
    let container = image.start().await.expect("mongo container start");
    wait_for_server().await;
    container
}

/// Poll until the wire protocol answers a ping command.
async fn wait_for_server() {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = async {
            let client = Client::with_uri_str(MONGO_URI)
                .await
                .map_err(|e| e.to_string())?;
            client
                .database("admin")
                .run_command(doc! { "ping": 1 })
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
            panic!("MongoDB server not ready within 120s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn build_output(database: &str, collection: &str) -> Arc<dyn Output> {
    ensure_init();
    let out = OutputConfig {
        output_type: "mongodb".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "uri": MONGO_URI,
            "database": database,
            "collection": collection,
        })),
    };
    let out = out.build(&resource()).expect("build mongodb output");
    out.connect().await.expect("connect mongodb output");
    out
}

fn typed_batch() -> MessageBatchRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", datafusion::arrow::datatypes::DataType::Utf8, true),
        Field::new("count", datafusion::arrow::datatypes::DataType::Int64, true),
        Field::new(
            "score",
            datafusion::arrow::datatypes::DataType::Float64,
            true,
        ),
        Field::new(
            "active",
            datafusion::arrow::datatypes::DataType::Boolean,
            true,
        ),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("Ada"), Some("Bob")])),
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(Float64Array::from(vec![9.5, -0.25])),
        Arc::new(BooleanArray::from(vec![true, false])),
    ];
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, columns).expect("typed batch"),
    ))
}

#[tokio::test]
#[serial_test::serial]
async fn output_writes_documents_verified_by_client() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("mongodb output writes documents");
        return;
    };
    let database = unique("db");
    let collection = unique("events");
    let output = build_output(&database, &collection).await;

    output.write(typed_batch()).await.expect("write");
    output.close().await.expect("close");

    let client = Client::with_uri_str(MONGO_URI)
        .await
        .expect("verify client");
    let mut cursor = client
        .database(&database)
        .collection::<Document>(&collection)
        .find(mongodb::bson::doc! {})
        .sort(doc! { "count": 1 })
        .await
        .expect("find");
    let mut documents = Vec::new();
    while let Some(document) = cursor.next().await {
        documents.push(document.expect("cursor document"));
    }
    assert_eq!(documents.len(), 2, "both rows must be inserted");
    assert_eq!(documents[0].get_str("name").unwrap(), "Ada");
    assert_eq!(documents[0].get_i64("count").unwrap(), 1);
    assert_eq!(documents[0].get_f64("score").unwrap(), 9.5);
    assert!(documents[0].get_bool("active").unwrap());
    assert_eq!(documents[1].get_str("name").unwrap(), "Bob");
    assert!(!documents[1].get_bool("active").unwrap());
}

#[tokio::test]
#[serial_test::serial]
async fn write_after_close_reports_disconnection() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("mongodb write after close");
        return;
    };
    let database = unique("db");
    let collection = unique("events");
    let output = build_output(&database, &collection).await;
    output.close().await.expect("close");

    assert!(matches!(
        output.write(typed_batch()).await,
        Err(arkflow_core::Error::Disconnection)
    ));
}

#[tokio::test]
#[serial_test::serial]
async fn empty_batch_write_is_a_no_op() {
    let Some(_lease) = server_lease().await else {
        skip_without_docker("mongodb empty batch write");
        return;
    };
    let database = unique("db");
    let collection = unique("events");
    let output = build_output(&database, &collection).await;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "name",
        datafusion::arrow::datatypes::DataType::Utf8,
        true,
    )]));
    let empty = Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))],
        )
        .expect("empty batch"),
    ));
    output.write(empty).await.expect("empty write");
    output.close().await.expect("close");

    let client = Client::with_uri_str(MONGO_URI)
        .await
        .expect("verify client");
    let count = client
        .database(&database)
        .collection::<Document>(&collection)
        .count_documents(mongodb::bson::doc! {})
        .await
        .expect("count");
    assert_eq!(count, 0, "no documents must be inserted for an empty batch");
}
