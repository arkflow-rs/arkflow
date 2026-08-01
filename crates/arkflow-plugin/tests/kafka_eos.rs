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

//! End-to-end exactly-once integration tests against a real Kafka broker.
//!
//! Spins up a single-node Confluent Kafka (KRaft) broker via `testcontainers`,
//! then drives `KafkaOutput` (exactly_once + transactional_id) through the
//! public `Output::write_batch` contract and verifies the transactional
//! semantics with a `read_committed` rdkafka consumer.
//!
//! Broker choice: the spec/design name redpanda as the example broker. These
//! tests use `confluentinc/cp-kafka` because (a) it is the Kafka reference
//! implementation for transactions, (b) it is reliably pullable in CI, and
//! (c) redpanda is wire-compatible — the EOS semantics under test (atomic
//! commit, zombie fencing, the post-commit duplicate window) are identical.
//! `tasks.md` 3.1 explicitly permits a broker fallback.
//!
//! These tests are NOT `#[ignore]` — testcontainers manages the broker
//! lifecycle, so `cargo test --test kafka_eos` works wherever Docker is
//! available. All cases share one broker (fixed host port 9092) and run
//! serially; each isolates by unique topic / transactional id / group.

use arkflow_core::output::{Output, OutputConfig};
use arkflow_core::{MessageBatch, MessageBatchRef, Resource};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;

/// KRaft cluster id: must be a base64-encoded UUID (16 bytes → 22 chars,
/// no padding). An arbitrary UUID string is rejected by cp-kafka and the
/// container exits(1) before opening the Kafka port.
const CLUSTER_ID: &str = "1RlfgIc1TZWvdfLySKufPw";
/// Fixed host port so the broker's advertised listener matches where clients
/// connect (KRaft `KAFKA_ADVERTISED_LISTENERS` is baked in at startup, so the
/// host port cannot be dynamic).
const KAFKA_HOST_PORT: u16 = 9092;

/// Shared broker — started once, reused by every (serial) test to avoid the
/// fixed-port release race between back-to-back containers.
static BROKER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();

async fn broker() {
    BROKER
        .get_or_init(start_broker)
        .await;
}

/// Register all output builders exactly once.
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

/// A single-row binary `MessageBatch` whose payload is the given bytes. The
/// column is `__value__` (`DEFAULT_BINARY_VALUE_FIELD`), so the default
/// no-codec encode path emits exactly one Kafka message per batch.
fn binary_batch(payload: &[u8]) -> MessageBatchRef {
    Arc::new(MessageBatch::new_binary(vec![payload.to_vec()]).expect("binary batch"))
}

/// Start a single-node KRaft Kafka broker on fixed host port 9092.
async fn start_broker() -> ContainerAsync<GenericImage> {
    let image = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_env_var("CLUSTER_ID", CLUSTER_ID)
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093",
        )
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_LOG_DIRS", "/tmp/kraft-combined")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_mapped_port(KAFKA_HOST_PORT, KAFKA_HOST_PORT.tcp())
        .with_startup_timeout(Duration::from_secs(180));
    let container = image.start().await.expect("broker container start");
    wait_for_broker("localhost:9092").await;
    container
}

/// Poll broker metadata until a Kafka client can connect (or ~90s elapse).
/// cp-kafka's stdout "ready" log line varies across versions, so a metadata
/// probe is the source of truth.
async fn wait_for_broker(brokers: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let ok = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("request.timeout.ms", "3000")
            .create::<rdkafka::producer::BaseProducer>()
            .and_then(|p| p.client().fetch_metadata(None, Duration::from_secs(3)))
            .is_ok();
        if ok {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("Kafka broker not ready (metadata probe failed) within 90s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Build a `KafkaOutput` via the public registry path. When `exactly_once` is
/// set, `tx_id` must be provided and is used as the stable transactional id.
async fn build_output(topic: &str, exactly_once: bool, tx_id: Option<&str>) -> Arc<dyn Output> {
    ensure_init();
    let mut cfg = serde_json::json!({
        "brokers": ["localhost:9092"],
        "topic": {"type": "value", "value": topic},
    });
    if exactly_once {
        cfg["exactly_once"] = serde_json::json!(true);
        cfg["transactional_id"] = serde_json::json!(tx_id.expect("tx id required"));
    }
    let out = OutputConfig {
        output_type: "kafka".into(),
        name: None,
        codec: None,
        config: Some(cfg),
    }
    .build(&resource())
    .expect("build kafka output");
    out.connect().await.expect("kafka output connect");
    out
}

/// A raw transactional producer (for the fencing test, which must control
/// transaction boundaries directly rather than through `write_batch`).
fn txn_producer(tx_id: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("transactional.id", tx_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "15000")
        .create()
        .expect("txn producer create")
}

/// `init_transactions` is a blocking broker round-trip — run it off the async
/// worker.
async fn txn_init(producer: FutureProducer) -> FutureProducer {
    let p = producer.clone();
    tokio::task::spawn_blocking(move || p.init_transactions(Duration::from_secs(30)))
        .await
        .expect("init_transactions join")
        .expect("init_transactions");
    producer
}

/// `begin_transaction` (blocking).
async fn txn_begin(producer: &FutureProducer) {
    let p = producer.clone();
    tokio::task::spawn_blocking(move || p.begin_transaction())
        .await
        .expect("begin join")
        .expect("begin_transaction");
}

/// `commit_transaction` (blocking).
async fn txn_commit(producer: &FutureProducer) {
    let p = producer.clone();
    tokio::task::spawn_blocking(move || p.commit_transaction(Duration::from_secs(30)))
        .await
        .expect("commit join")
        .expect("commit_transaction");
}

/// `flush` (blocking) — ensure queued records reach the broker.
async fn txn_flush(producer: &FutureProducer) {
    let p = producer.clone();
    tokio::task::spawn_blocking(move || p.flush(Duration::from_secs(15)))
        .await
        .expect("flush join")
        .expect("flush");
}

/// A `read_committed` consumer (the lens through which EOS is observed).
fn read_committed_consumer(group: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group)
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .create()
        .expect("consumer create")
}

/// Subscribe, let the group rebalance, then count non-empty messages within
/// `timeout`.
async fn subscribe_and_drain(consumer: &StreamConsumer, topic: &str, timeout: Duration) -> usize {
    consumer.subscribe(&[topic]).expect("subscribe");
    // Allow the consumer group to rebalance and acquire a partition
    // assignment before draining.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut n = 0;
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(m)) => {
                if m.payload().is_some() {
                    n += 1;
                }
            }
            Ok(Err(_)) => {} // transient (mid-rebalance); keep going
            Err(_) => {}    // per-poll timeout; keep going until overall deadline
        }
    }
    n
}

/// Smoke test: the broker starts, a non-transactional `write_batch` produces,
/// and a `read_committed` consumer observes the messages. Validates the whole
/// fixture before the transactional cases lean on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn smoke_broker_and_roundtrip() {
    broker().await;
    let topic = format!("eos-smoke-{}", std::process::id());
    let output = build_output(&topic, false, None).await;
    output
        .write_batch(&[binary_batch(b"one"), binary_batch(b"two")])
        .await
        .expect("non-txn write_batch");
    output.close().await.expect("output close");

    let consumer = read_committed_consumer(&topic);
    let received = subscribe_and_drain(&consumer, &topic, Duration::from_secs(20)).await;
    assert!(
        received >= 1,
        "smoke: expected at least one message, got {received}"
    );
}

/// 3.2 — One `write_batch` with N messages commits as one atomic unit: a
/// `read_committed` consumer observes all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn atomic_commit_observes_whole_batch() {
    broker().await;
    let topic = format!("eos-atomic-{}", std::process::id());
    let tx_id = format!("atomic-tx-{}", std::process::id());

    let output = build_output(&topic, true, Some(&tx_id)).await;
    output
        .write_batch(&[
            binary_batch(b"a"),
            binary_batch(b"b"),
            binary_batch(b"c"),
        ])
        .await
        .expect("transactional write_batch");
    output.close().await.expect("output close");

    let consumer = read_committed_consumer(&format!("{tx_id}-c"));
    let received = subscribe_and_drain(&consumer, &topic, Duration::from_secs(20)).await;
    assert_eq!(
        received, 3,
        "atomic commit: read_committed consumer must see all 3 messages, got {received}"
    );
}

/// 3.3 — Zombie fencing across a simulated restart. Producer 1 begins a
/// transaction and sends "zombie", then "crashes" (dropped without
/// committing). Producer 2 reuses the same `transactional.id`; its
/// `init_transactions` bumps the broker epoch and fences producer 1's
/// in-flight (uncommitted) transaction. Producer 2 then commits "winner". A
/// `read_committed` consumer sees only "winner" — the fenced "zombie" write
/// is never visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn zombie_fenced_across_restart() {
    broker().await;
    let topic = format!("eos-fence-{}", std::process::id());
    let tx_id = format!("fence-tx-{}", std::process::id());

    // Producer 1: begin + send + flush, then "crash" (drop, no commit).
    let p1 = txn_init(txn_producer(&tx_id)).await;
    txn_begin(&p1).await;
    p1.send_result(FutureRecord::<(), _>::to(&topic).payload(b"zombie"))
        .expect("zombie send_result");
    txn_flush(&p1).await;
    drop(p1); // crash

    // Producer 2 (same tx_id) fences producer 1, then commits "winner".
    let p2 = txn_init(txn_producer(&tx_id)).await;
    txn_begin(&p2).await;
    p2.send_result(FutureRecord::<(), _>::to(&topic).payload(b"winner"))
        .expect("winner send_result");
    txn_commit(&p2).await;
    txn_flush(&p2).await;

    let consumer = read_committed_consumer(&format!("{tx_id}-c"));
    let received = subscribe_and_drain(&consumer, &topic, Duration::from_secs(20)).await;
    assert_eq!(
        received, 1,
        "fencing: only producer 2's committed message should be visible (zombie fenced), got {received}"
    );
}

/// 3.4 — Honest L2 boundary. Producer commits a batch, then the process
/// "crashes" before the source offset is committed; on recovery the same
/// `transactional.id` replays the identical batch. Both writes are visible
/// to a `read_committed` consumer — i.e. L2 does NOT eliminate this
/// duplicate window (downstream idempotency / future L3 is required).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn post_commit_crash_duplicates() {
    broker().await;
    let topic = format!("eos-dup-{}", std::process::id());
    let tx_id = format!("dup-tx-{}", std::process::id());

    // First commit, then "crash" before the source offset commits.
    let out1 = build_output(&topic, true, Some(&tx_id)).await;
    out1.write_batch(&[binary_batch(b"x")])
        .await
        .expect("first commit");
    drop(out1); // crash — source offset never committed

    // Recovery: same tx_id, replay the identical batch.
    let out2 = build_output(&topic, true, Some(&tx_id)).await;
    out2.write_batch(&[binary_batch(b"x")])
        .await
        .expect("replay commit");
    out2.close().await.expect("output close");

    let consumer = read_committed_consumer(&format!("{tx_id}-c"));
    let received = subscribe_and_drain(&consumer, &topic, Duration::from_secs(20)).await;
    assert_eq!(
        received, 2,
        "L2 honest boundary: post-commit crash must duplicate (both writes visible), got {received}"
    );
}
