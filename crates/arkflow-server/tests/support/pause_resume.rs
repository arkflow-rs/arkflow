//! Replayable test connectors and bounded Hub/Agent tasks. Recovery artifacts
//! are produced exclusively by the real Agent kernels, never by this fixture.

use arkflow_core::checkpoint::SourcePosition;
use arkflow_core::codec::Codec;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use arkflow_server::agent::{self, NodeAgentConfig};
use arkflow_server::hub::{AgentCommand, Hub, HubConfig};
use arkflow_server::storage::{ControlPlaneStore, JobRecord, StorageActor};
use arkflow_server::{hub_router, ServerConfig};
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use datafusion::arrow::array::{Int64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Feed {
    available: AtomicU64,
    acknowledged: AtomicU64,
    rows: Mutex<Vec<(u64, i64)>>,
    restored: Mutex<Vec<u64>>,
}

/// Each reader starts at zero and can only reach its previous cursor through
/// Input::restore_positions. New records can be published while it is paused.
#[derive(Clone)]
pub(super) struct ReplayFixture {
    pub input_type: String,
    pub output_type: String,
    feeds: BTreeMap<String, Arc<Feed>>,
}

impl ReplayFixture {
    pub fn register(job_id: &str) -> Self {
        let fixture = Self {
            input_type: format!("{job_id}-replay"),
            output_type: format!("{job_id}-collect"),
            feeds: ["source-a", "source-b"]
                .into_iter()
                .map(|source| (source.to_owned(), Arc::new(Feed::default())))
                .collect(),
        };
        register_input_builder(&fixture.input_type, Arc::new(fixture.clone())).unwrap();
        register_output_builder(&fixture.output_type, Arc::new(fixture.clone())).unwrap();
        fixture
    }

    fn feed(&self, config: &Option<serde_json::Value>) -> (String, Arc<Feed>) {
        let source = config.as_ref().unwrap()["source"].as_str().unwrap();
        (source.to_owned(), self.feeds[source].clone())
    }

    pub fn publish_through(&self, count: u64) {
        for feed in self.feeds.values() {
            feed.available.store(count, Ordering::SeqCst);
        }
    }

    pub fn settled(&self, count: u64) -> bool {
        self.feeds.values().all(|feed| {
            feed.acknowledged.load(Ordering::SeqCst) == count
                && feed.rows.lock().unwrap().len() == count as usize
        })
    }

    pub fn diagnostics(&self) -> serde_json::Value {
        self.feeds
            .iter()
            .map(|(source, feed)| {
                (
                    source.clone(),
                    serde_json::json!({
                        "available": feed.available.load(Ordering::SeqCst),
                        "acknowledged": feed.acknowledged.load(Ordering::SeqCst),
                        "rows": *feed.rows.lock().unwrap(),
                        "restored": *feed.restored.lock().unwrap(),
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    pub fn assert_rows(&self, count: u64) {
        let expected = (0..count)
            .map(|sequence| (sequence, sequence as i64 + 1))
            .collect::<Vec<_>>();
        for (source, feed) in &self.feeds {
            assert_eq!(
                *feed.rows.lock().unwrap(),
                expected,
                "{source}: every input must reach the sink exactly once, with restored keyed counts"
            );
        }
    }

    pub fn assert_restored(&self, offsets: &[u64]) {
        for (source, feed) in &self.feeds {
            assert_eq!(
                *feed.restored.lock().unwrap(),
                offsets,
                "{source}: each new reader must restore the selected manifest cursor"
            );
        }
    }
}

struct ReplayInput {
    source: String,
    feed: Arc<Feed>,
    next: AtomicU64,
    acknowledged: Arc<AtomicU64>,
}

struct SequenceAck {
    next: u64,
    acknowledged: Arc<AtomicU64>,
    feed: Arc<Feed>,
}

#[async_trait]
impl Ack for SequenceAck {
    async fn ack(&self) -> Result<(), Error> {
        // One FIFO source -> processor -> sink path per component: successful
        // sink writes acknowledge in sequence, including across a barrier.
        self.acknowledged.fetch_max(self.next, Ordering::SeqCst);
        self.feed
            .acknowledged
            .fetch_max(self.next, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Input for ReplayInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        loop {
            // The only await precedes cursor mutation: a checkpoint or stop
            // cancelling this read cannot consume a record without delivery.
            tokio::time::sleep(Duration::from_millis(5)).await;
            let sequence = self.next.load(Ordering::SeqCst);
            if sequence >= self.feed.available.load(Ordering::SeqCst) {
                continue;
            }
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("sequence", DataType::UInt64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![self.source.as_str()])),
                    Arc::new(UInt64Array::from(vec![sequence])),
                ],
            )
            .map_err(|error| Error::Read(error.to_string()))?;
            self.next.store(sequence + 1, Ordering::SeqCst);
            return Ok((
                Arc::new(MessageBatch::new_arrow(batch)),
                Arc::new(SequenceAck {
                    next: sequence + 1,
                    acknowledged: self.acknowledged.clone(),
                    feed: self.feed.clone(),
                }),
            ));
        }
    }

    async fn current_positions(&self) -> Result<Vec<SourcePosition>, Error> {
        Ok(vec![SourcePosition {
            topic: Some(self.source.clone()),
            partition: 0,
            offset: self.acknowledged.load(Ordering::SeqCst),
        }])
    }

    async fn restore_positions(&self, positions: &[SourcePosition]) -> Result<(), Error> {
        let position = positions
            .iter()
            .find(|position| {
                position.topic.as_deref() == Some(self.source.as_str()) && position.partition == 0
            })
            .ok_or_else(|| Error::Process(format!("missing cursor for {}", self.source)))?;
        self.next.store(position.offset, Ordering::SeqCst);
        self.acknowledged.store(position.offset, Ordering::SeqCst);
        self.feed.restored.lock().unwrap().push(position.offset);
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl InputBuilder for ReplayFixture {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        _codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let (source, feed) = self.feed(config);
        Ok(Arc::new(ReplayInput {
            source,
            feed,
            next: AtomicU64::new(0),
            acknowledged: Arc::new(AtomicU64::new(0)),
        }))
    }
}

struct CollectOutput(Arc<Feed>);

#[async_trait]
impl Output for CollectOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, batch: MessageBatchRef) -> Result<(), Error> {
        let record = batch.record_batch();
        let sequences = record
            .column_by_name("sequence")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let counts = record
            .column_by_name("__arkflow_state_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut rows = self.0.rows.lock().unwrap();
        for index in 0..record.num_rows() {
            rows.push((sequences.value(index), counts.value(index)));
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl OutputBuilder for ReplayFixture {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        _codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(Arc::new(CollectOutput(self.feed(config).1)))
    }
}

/// Abort on unwinding as well as on a bounded graceful-shutdown timeout.
/// Nothing spawned by these tests may survive a failing assertion.
pub(super) struct SmokeTask {
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl SmokeTask {
    fn spawn(
        cancellation: CancellationToken,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Self {
        Self {
            cancellation,
            task: tokio::spawn(future),
        }
    }

    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        match tokio::time::timeout(Duration::from_secs(15), &mut self.task).await {
            Ok(result) => result.expect("smoke task panicked"),
            Err(_) => {
                self.task.abort();
                let _ = (&mut self.task).await;
                panic!("smoke task did not shut down within 15 seconds");
            }
        }
    }
}

impl Drop for SmokeTask {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

pub(super) fn start_agents(hub_url: &str) -> Vec<SmokeTask> {
    ["node-a", "node-b"]
        .into_iter()
        .map(|node_id| {
            let cancellation = CancellationToken::new();
            let agent_stop = cancellation.clone();
            let config = NodeAgentConfig {
                hub_url: hub_url.to_owned(),
                hub_urls: vec![hub_url.to_owned()],
                api_prefix: "/api/v1".into(),
                node_id: node_id.into(),
                node_token: String::new(),
                boot_id: format!("pause-smoke-{node_id}"),
                heartbeat_interval: Duration::from_millis(50),
                report_interval: Duration::from_millis(50),
                poll_interval: Duration::from_millis(20),
                data_port: None,
                data_host: None,
            };
            SmokeTask::spawn(cancellation, async move {
                agent::run(super::empty_control_plane(), config, agent_stop)
                    .await
                    .unwrap();
            })
        })
        .collect()
}

pub(super) struct HubHarness {
    pub hub: Hub,
    pub address: SocketAddr,
    pub url: String,
    tasks: Vec<SmokeTask>,
}

impl HubHarness {
    pub async fn start(
        sqlite_path: &Path,
        address: SocketAddr,
        commands: Arc<Mutex<Vec<AgentCommand>>>,
    ) -> Self {
        // A new backend and actor on every invocation, including restart. No
        // in-memory Job, session, checkpoint or operation cache is reused.
        let store = ControlPlaneStore::open(sqlite_path.to_str().unwrap())
            .await
            .unwrap();
        let hub = Hub::with_storage(
            HubConfig {
                operator_token: None,
                node_token: None,
                insecure_local: true,
                lease_ttl_ms: 2_000,
                poll_interval_ms: 20,
                session_ttl_ms: 2_000,
                approval_required: false,
                approval_ttl_ms: arkflow_server::hub::default_approval_ttl_ms(),
            },
            StorageActor::start(store, 128),
        );
        hub.recover_persisted_state().await.unwrap();
        let listener = TcpListener::bind(address).await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = hub_router(hub.clone(), &ServerConfig::default()).layer(middleware::from_fn(
            move |request: Request, next: Next| {
                let commands = commands.clone();
                async move {
                    let capture = request.uri().path() == "/api/v1/agent/commands";
                    let response = next.run(request).await;
                    if !capture || !response.status().is_success() {
                        return response;
                    }
                    // Observe exactly what the real Agent receives; neither
                    // steal a poll nor synthesize a command or result.
                    let (parts, body) = response.into_parts();
                    let bytes = to_bytes(body, 4 * 1024 * 1024).await.unwrap();
                    let polled: Vec<AgentCommand> = serde_json::from_slice(&bytes).unwrap();
                    commands.lock().unwrap().extend(polled);
                    Response::from_parts(parts, Body::from(bytes))
                }
            },
        ));
        let cancellation = CancellationToken::new();
        let server_stop = cancellation.clone();
        let server = SmokeTask::spawn(cancellation, async move {
            axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(server_stop.cancelled_owned())
                .await
                .unwrap();
        });
        let cancellation = CancellationToken::new();
        let reconcile_stop = cancellation.clone();
        let reconcile_hub = hub.clone();
        let reconcile = SmokeTask::spawn(cancellation, async move {
            let mut tick = tokio::time::interval(Duration::from_millis(20));
            loop {
                tokio::select! {
                    _ = reconcile_stop.cancelled() => return,
                    _ = tick.tick() => {
                        // Match production ordering. These tests create only
                        // pause artifacts, without competing periodic rounds.
                        let _ = reconcile_hub.reconcile_jobs().await;
                        let _ = reconcile_hub.reconcile_job_pauses().await;
                        let _ = reconcile_hub.expire_stale_job_operations().await;
                    }
                }
            }
        });
        Self {
            hub,
            address,
            url: format!("http://{address}"),
            tasks: vec![reconcile, server],
        }
    }

    pub async fn action(&self, client: &reqwest::Client, job_id: &str, action: &str) -> JobRecord {
        let response = client
            .post(format!(
                "{}/api/v1/jobs/{job_id}/actions/{action}",
                self.url
            ))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{action}: {body}");
        serde_json::from_str(&body).unwrap()
    }

    pub async fn shutdown(self) {
        for task in self.tasks {
            task.shutdown().await;
        }
    }
}
