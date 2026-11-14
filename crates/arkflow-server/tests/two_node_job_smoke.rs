use arkflow_core::checkpoint::{
    recovery_manifest_key, CheckpointManifest, CheckpointRepository, FileCheckpointStore,
    RecoveryArtifactKind,
};
use arkflow_core::config::{EngineConfig, LoggingConfig, NodeConfig};
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::job::{
    CheckpointSpec, JobId, JobPlan, JobSpec, JobVersion, OperatorKind, OperatorSpec,
    RecoveryPolicy, SinkSpec, SourceSpec, StateSpec, TimeMode, TimeSpec,
};
use arkflow_core::runtime::RuntimeManager;
use arkflow_server::agent::{self, NodeAgentConfig};
use arkflow_server::hub::{Hub, HubConfig, HubOperationState};
use arkflow_server::storage::JobRecord;
use arkflow_server::{hub_router, ServerConfig};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[path = "support/pause_resume.rs"]
mod pause_resume;
use pause_resume::{start_agents, HubHarness, ReplayFixture};

/// These smoke tests each run a Hub, two Agents, and real kernels; in
/// parallel they saturate a shared CI runner and stretch the restart round
/// trip past its budget (observed as a condition-4 timeout on #1264's CI).
/// Serialize them: wall clock stays comparable, load halves.
static ONE_AT_A_TIME: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn empty_control_plane() -> ControlPlane {
    ControlPlane::new(
        EngineConfig {
            streams: Vec::new(),
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        },
        RuntimeManager::new(),
    )
}

fn two_component_job(id: JobId, checkpoint_uri: String, state_root: String) -> JobSpec {
    let processing_time = || TimeSpec {
        mode: TimeMode::ProcessingTime,
        timestamp_field: None,
        watermark: None,
        allowed_lateness_ms: 0,
        late_event_policy: Default::default(),
        late_event_route: None,
    };
    JobSpec {
        resources: Default::default(),
        rebalance: None,
        rescale: false,
        placement: arkflow_core::job::PlacementStrategy::Colocated,
        id,
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            OperatorSpec {
                id: "source-a".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "sink-a".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "source-b".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "sink-b".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        ],
        edges: vec![
            arkflow_core::job::EdgeSpec {
                id: "edge-a".into(),
                from: "source-a".into(),
                to: "sink-a".into(),
                partitioned: false,
            },
            arkflow_core::job::EdgeSpec {
                id: "edge-b".into(),
                from: "source-b".into(),
                to: "sink-b".into(),
                partitioned: false,
            },
        ],
        sources: vec![
            SourceSpec {
                codec: None,
                operator_id: "source-a".into(),
                input_type: "generate".into(),
                config: serde_json::json!({
                    "context": "node-a",
                    "interval": "10ms",
                    "batch_size": 1
                }),
                time: processing_time(),
            },
            SourceSpec {
                operator_id: "source-b".into(),
                codec: None,
                input_type: "generate".into(),
                config: serde_json::json!({
                    "context": "node-b",
                    "interval": "10ms",
                    "batch_size": 1
                }),
                time: processing_time(),
            },
        ],
        sinks: vec![
            SinkSpec {
                operator_id: "sink-a".into(),
                output_type: "drop".into(),
                codec: None,
                config: serde_json::json!({}),
            },
            SinkSpec {
                operator_id: "sink-b".into(),
                output_type: "drop".into(),
                codec: None,
                config: serde_json::json!({}),
            },
        ],
        state: Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: arkflow_core::job::StateDurability::Durable,
            root: Some(state_root),
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        }),
        checkpoint: Some(CheckpointSpec {
            // The 1_000 ms value is the minimum gap between periodic
            // checkpoint rounds enforced by schedule_periodic_checkpoints().
            // The loop re-triggers after each 5-second wait; 60_000 ms
            // previously blocked re-triggers for the full test budget.
            interval_ms: 1_000,
            retention: 3,
            object_store_uri: checkpoint_uri,
        }),
        recovery: Default::default(),
    }
}

async fn wait_until<F, Fut>(condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    wait_until_with_budget(condition, Duration::from_secs(60)).await;
}

async fn wait_until_with_budget<F, Fut>(mut condition: F, budget: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    static WAIT_INDEX: AtomicUsize = AtomicUsize::new(0);
    let wait_index = WAIT_INDEX.fetch_add(1, Ordering::Relaxed) + 1;
    // Generous under load: the full workspace suite runs other binaries on
    // the same machine, and cold caches can stretch kernel startup. A full
    // restart-recovery round trip (re-register, recovery, dispatch, run,
    // complete) has been observed to exceed 30s under that load.
    tokio::time::timeout(budget, async {
        loop {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("two-node smoke condition {wait_index} timed out"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_node_hub_agent_checkpoint_and_restart_recover() {
    let _serial = ONE_AT_A_TIME.lock().await;
    arkflow_plugin::initialize().unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let job_id = format!("two-node-smoke-{}", std::process::id());
    let spec = two_component_job(
        JobId::new(&job_id).unwrap(),
        format!("file://{}", checkpoint_dir.path().display()),
        checkpoint_dir.path().join("state").display().to_string(),
    );

    let hub = Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 2_000,
        poll_interval_ms: 20,
        // The default (1h) would leave a killed Agent's session live for the
        // whole test; heartbeats renew every 50ms so 2s bounds any
        // stale-session-gated replacement step without spurious expiry.
        session_ttl_ms: 2_000,
        approval_required: false,
        approval_ttl_ms: arkflow_server::hub::default_approval_ttl_ms(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let hub_cancel = CancellationToken::new();
    let server_hub = hub.clone();
    let server_cancel = hub_cancel.clone();
    let hub_task = tokio::spawn(async move {
        axum::serve(
            listener,
            hub_router(server_hub, &ServerConfig::default()).into_make_service(),
        )
        .with_graceful_shutdown(server_cancel.cancelled_owned())
        .await
    });
    let reconcile_cancel = CancellationToken::new();
    let reconcile_hub = hub.clone();
    let reconcile_stop = reconcile_cancel.clone();
    let reconcile_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                _ = reconcile_stop.cancelled() => return,
                _ = tick.tick() => {
                    let _ = reconcile_hub.reconcile_jobs().await;
                }
            }
        }
    });

    let hub_url = format!("http://{}", address);
    let cancel_a = CancellationToken::new();
    let cancel_b = CancellationToken::new();
    let mut agent_a = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
            hub_urls: vec![hub_url.clone()],
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            node_token: String::new(),
            boot_id: "smoke-boot-a".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(20),
            data_port: None,
            data_host: None,
        },
        cancel_a.clone(),
    ));
    let mut agent_b = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
            hub_urls: vec![hub_url.clone()],
            api_prefix: "/api/v1".into(),
            node_id: "node-b".into(),
            node_token: String::new(),
            boot_id: "smoke-boot-b".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(20),
            data_port: None,
            data_host: None,
        },
        cancel_b.clone(),
    ));

    wait_until(|| {
        let hub = hub.clone();
        async move { hub.nodes().await.len() == 2 }
    })
    .await;

    hub.upsert_job(JobRecord {
        job_id: job_id.clone(),
        version: 1,
        spec_json: serde_json::to_string(&spec).unwrap(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into(), "node-b".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,

        pause_phase: None,
        pause_artifact_id: None,
        pause_prev_checkpoint_id: None,
        pause_retry_count: 0,
        paused_at_ms: None,
        paused_by: None,
    })
    .await
    .unwrap();

    // Both assignments have started. Reconcile dispatch is at-least-once:
    // under a loaded runner a retry can land before the first success
    // report arrives, legitimately producing a third Succeeded job_start
    // (the Agent executes redelivered job_start commands idempotently), so
    // count with >= — an exact == never settles on a triple dispatch.
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.operations(None)
                .await
                .iter()
                .filter(|operation| {
                    operation.resource_id == job_id
                        && operation.operation.as_str() == "job_start"
                        && operation.state == HubOperationState::Succeeded
                })
                .count()
                >= 2
        }
    })
    .await;

    // Data-plane observability: both Agents report per-Job kernel metric
    // snapshots to the Hub, so the Hub export covers both nodes.
    wait_until(|| {
        let hub = hub.clone();
        async move { hub.job_metrics().await.len() == 2 }
    })
    .await;
    let exported = hub.job_metrics().await;
    for (node_id, jobs) in &exported {
        assert!(
            jobs.contains_key(&job_id),
            "node {node_id} must export the smoke Job, got {:?}",
            jobs.keys().collect::<Vec<_>>()
        );
    }

    assert_eq!(hub.schedule_periodic_checkpoints().await.unwrap(), 1);
    // The manual trigger is single-shot: under load a round can stall, so
    // re-trigger periodically while waiting (the Hub fences duplicate
    // checkpoint rounds per generation).
    let checkpoint_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        hub.schedule_periodic_checkpoints().await.unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = hub
                    .job_checkpoints(&job_id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|record| record.status == "completed");
                if done {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if completed.is_ok() || std::time::Instant::now() >= checkpoint_deadline {
            break;
        }
    }
    let checkpoint_reached = hub
        .job_checkpoints(&job_id)
        .await
        .unwrap()
        .iter()
        .any(|record| record.status == "completed");
    assert!(
        checkpoint_reached,
        "checkpoint never completed even with periodic re-triggering"
    );

    // Kill node-a, then boot a fresh Agent identity on the same node. The Hub
    // must fence the old start attempt, dispatch the checkpoint recovery, and
    // converge both assignments again.
    cancel_a.cancel();
    tokio::time::timeout(Duration::from_secs(15), &mut agent_a)
        .await
        .expect("node-a agent did not stop")
        .unwrap()
        .unwrap();

    let cancel_a_restart = CancellationToken::new();
    let mut restarted_a = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
            hub_urls: vec![hub_url],
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            node_token: String::new(),
            boot_id: "smoke-boot-a-restarted".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(20),
            poll_interval: Duration::from_millis(20),
            data_port: None,
            data_host: None,
        },
        cancel_a_restart.clone(),
    ));

    let initial_start_ids = hub
        .operations(None)
        .await
        .into_iter()
        .filter(|operation| {
            operation.resource_id == job_id
                && operation.operation.as_str() == "job_start"
                && operation.state == HubOperationState::Succeeded
        })
        .map(|operation| operation.id)
        .collect::<Vec<_>>();
    // The replacement round trip (kill → re-register → fence → recovery
    // dispatch → kernel restart → success report) is the longest chain in
    // the suite; give it triple the default budget instead of letting a
    // loaded runner turn it into a false red.
    wait_until_with_budget(
        || {
            let hub = hub.clone();
            let job_id = job_id.clone();
            let initial_start_ids = initial_start_ids.clone();
            async move {
                hub.operations(None).await.iter().any(|operation| {
                    operation.resource_id == job_id
                        && operation.operation.as_str() == "job_start"
                        && operation.state == HubOperationState::Succeeded
                        && !initial_start_ids.contains(&operation.id)
                })
            }
        },
        Duration::from_secs(180),
    )
    .await;

    cancel_a_restart.cancel();
    cancel_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut restarted_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut agent_b).await;
    // Deterministic teardown: a task that ignored its cancellation token
    // must not outlive the test. A pending task polled during runtime drop
    // can panic and abort the whole test binary *after* the summary printed
    // (exit 101 with an all-green log), so abort anything left over.
    restarted_a.abort();
    agent_b.abort();
    reconcile_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), reconcile_task).await;
    hub_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
}

/// Split placement end to end: the source runs on node-a, the sink on
/// node-b, and the forward edge between them crosses the real TCP data
/// plane. Proof points: both agents build remote-edge graphs (kernel metrics
/// show sink-side rows on node-b), the Hub-dispatched checkpoint barriers
/// align through the network edge, and the checkpoint completes through the
/// existing aggregation path (all_nodes_succeeded → job_checkpoint_commit).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn split_job_runs_across_nodes_and_aggregates_checkpoint() {
    let _serial = ONE_AT_A_TIME.lock().await;
    arkflow_plugin::initialize().unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let job_id = format!("split-smoke-{}", std::process::id());

    let mut spec = two_component_job(
        JobId::new(&job_id).unwrap(),
        format!("file://{}", checkpoint_dir.path().display()),
        checkpoint_dir.path().join("state").display().to_string(),
    );
    // Reshape into a two-task split job: generate source → drop sink, with
    // the edge crossing the network.
    spec.placement = arkflow_core::job::PlacementStrategy::Split;
    spec.operators
        .retain(|operator| operator.id == "source-a" || operator.id == "sink-a");
    spec.edges.retain(|edge| edge.id == "edge-a");
    spec.sources
        .retain(|source| source.operator_id == "source-a");
    spec.sinks.retain(|sink| sink.operator_id == "sink-a");

    let hub = Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 2_000,
        poll_interval_ms: 20,
        // The default (1h) would leave a killed Agent's session live for the
        // whole test; heartbeats renew every 50ms so 2s bounds any
        // stale-session-gated replacement step without spurious expiry.
        session_ttl_ms: 2_000,
        approval_required: false,
        approval_ttl_ms: arkflow_server::hub::default_approval_ttl_ms(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let hub_cancel = CancellationToken::new();
    let server_hub = hub.clone();
    let server_cancel = hub_cancel.clone();
    let hub_task = tokio::spawn(async move {
        axum::serve(
            listener,
            hub_router(server_hub, &ServerConfig::default()).into_make_service(),
        )
        .with_graceful_shutdown(server_cancel.cancelled_owned())
        .await
    });
    let reconcile_cancel = CancellationToken::new();
    let reconcile_hub = hub.clone();
    let reconcile_stop = reconcile_cancel.clone();
    let reconcile_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                _ = reconcile_stop.cancelled() => return,
                _ = tick.tick() => {
                    let _ = reconcile_hub.reconcile_jobs().await;
                }
            }
        }
    });

    let hub_url = format!("http://{}", address);
    // The TCP shuffle plane is authenticated independently of the in-process
    // Hub test mode. Both Agents use the same explicit test secret so this
    // exercises the production handshake instead of the unauthenticated
    // in-memory transport used by core unit tests.
    let data_plane_secret = "split-data-plane-secret".to_string();
    let cancel_a = CancellationToken::new();
    let cancel_b = CancellationToken::new();
    let mut agent_a = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
            hub_urls: vec![hub_url.clone()],
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            node_token: data_plane_secret.clone(),
            boot_id: "split-boot-a".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(20),
            data_port: Some(29_601),
            data_host: Some("127.0.0.1".into()),
        },
        cancel_a.clone(),
    ));
    let mut agent_b = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
            hub_urls: vec![hub_url.clone()],
            api_prefix: "/api/v1".into(),
            node_id: "node-b".into(),
            node_token: data_plane_secret,
            boot_id: "split-boot-b".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(20),
            data_port: Some(29_602),
            data_host: Some("127.0.0.1".into()),
        },
        cancel_b.clone(),
    ));

    wait_until(|| {
        let hub = hub.clone();
        async move {
            let nodes = hub.nodes().await;
            nodes.len() == 2
                && nodes.iter().all(|node| {
                    node.data_address.is_some()
                        && node
                            .capabilities
                            .iter()
                            .any(|capability| capability == "network_shuffle")
                })
        }
    })
    .await;

    hub.upsert_job(JobRecord {
        job_id: job_id.clone(),
        version: 1,
        spec_json: serde_json::to_string(&spec).unwrap(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into(), "node-b".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,

        pause_phase: None,
        pause_artifact_id: None,
        pause_prev_checkpoint_id: None,
        pause_retry_count: 0,
        paused_at_ms: None,
        paused_by: None,
    })
    .await
    .unwrap();

    // Both assignments start: the split placement dispatched, meaning the
    // data-plane capability validation passed for both nodes. See the
    // dispatch note in the restart test: >=, not an exact count, because a
    // benign at-least-once retry can add a third Succeeded job_start.
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.operations(None)
                .await
                .iter()
                .filter(|operation| {
                    operation.resource_id == job_id
                        && operation.operation.as_str() == "job_start"
                        && operation.state == HubOperationState::Succeeded
                })
                .count()
                >= 2
        }
    })
    .await;

    // Data actually crossed the network: node-b's kernel shows the sink-side
    // chain consuming remote batches.
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.job_metrics()
                .await
                .iter()
                .find(|(node, _)| node == "node-b")
                .and_then(|(_, jobs)| jobs.get(&job_id))
                .cloned()
                .is_some_and(|snapshot| snapshot.chains.values().any(|chain| chain.batches_in > 0))
        }
    })
    .await;

    // The checkpoint must aggregate: both nodes snapshot, the coordinator
    // merges both manifests, and the record completes.
    assert_eq!(hub.schedule_periodic_checkpoints().await.unwrap(), 1);
    // The manual trigger is single-shot: under load a round can stall, so
    // re-trigger periodically while waiting (the Hub fences duplicate
    // checkpoint rounds per generation).
    let checkpoint_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        hub.schedule_periodic_checkpoints().await.unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = hub
                    .job_checkpoints(&job_id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|record| record.status == "completed");
                if done {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if completed.is_ok() || std::time::Instant::now() >= checkpoint_deadline {
            break;
        }
    }
    let checkpoint_reached = hub
        .job_checkpoints(&job_id)
        .await
        .unwrap()
        .iter()
        .any(|record| record.status == "completed");
    assert!(
        checkpoint_reached,
        "checkpoint never completed even with periodic re-triggering"
    );

    cancel_a.cancel();
    cancel_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut agent_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut agent_b).await;
    // See the teardown note above: nothing may outlive the test.
    agent_a.abort();
    agent_b.abort();
    reconcile_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), reconcile_task).await;
    hub_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
}

async fn pause_resume_diagnostics(hub: &Hub, job_id: &str, fixture: &ReplayFixture) -> String {
    let mut job = serde_json::to_value(hub.job(job_id).await.unwrap()).unwrap();
    if let Some(fields) = job.as_object_mut() {
        fields.remove("spec_json");
    }
    let operations = hub
        .operations(None)
        .await
        .into_iter()
        .filter(|operation| operation.resource_id == job_id)
        .collect::<Vec<_>>();
    format!(
        "job: {job}\ntasks: {:?}\noperations: {operations:#?}\nartifacts: {:?}\nnodes: {:?}\nfixture: {}",
        hub.observed_job_tasks(job_id).await,
        hub.job_checkpoints(job_id).await,
        hub.nodes().await,
        fixture.diagnostics(),
    )
}

async fn wait_for_pause_resume<F, Fut>(
    hub: &Hub,
    job_id: &str,
    fixture: &ReplayFixture,
    stage: &str,
    mut condition: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if result.is_err() {
        panic!(
            "{stage} timed out:\n{}",
            pause_resume_diagnostics(hub, job_id, fixture).await
        );
    }
}

async fn wait_for_running_rows(
    hub: &Hub,
    plan: &JobPlan,
    generation: u64,
    fixture: &ReplayFixture,
    count: u64,
) {
    let expected_tasks = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    wait_for_pause_resume(
        hub,
        plan.spec.id.as_str(),
        fixture,
        &format!("running generation {generation}, acknowledged rows {count}"),
        || {
            let hub = hub.clone();
            let job_id = plan.spec.id.as_str().to_owned();
            let expected_tasks = expected_tasks.clone();
            let fixture = fixture.clone();
            async move {
                let job = hub.job(&job_id).await.unwrap().unwrap();
                let tasks = hub.observed_job_tasks(&job_id).await;
                job.generation == generation
                    && job.desired_state == "running"
                    && job.observed_state == "running"
                    && job.convergence == "converged"
                    && job.pause_phase.is_none()
                    && tasks.keys().cloned().collect::<BTreeSet<_>>() == expected_tasks
                    && tasks.values().map(String::as_str).collect::<BTreeSet<_>>()
                        == BTreeSet::from(["node-a", "node-b"])
                    && fixture.settled(count)
            }
        },
    )
    .await;
    fixture.assert_rows(count);
}

async fn exercise_two_node_pause_resume(recovery: RecoveryPolicy) {
    let _serial = ONE_AT_A_TIME.lock().await;
    arkflow_plugin::initialize().unwrap();
    let (kind, artifact_kind) = match recovery {
        RecoveryPolicy::LatestCheckpoint => ("checkpoint", RecoveryArtifactKind::Checkpoint),
        RecoveryPolicy::LatestSavepoint => ("savepoint", RecoveryArtifactKind::Savepoint),
        RecoveryPolicy::Fail => unreachable!("pause requires a recovery policy"),
    };
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("hub.sqlite");
    let job_id = format!("two-node-pause-{kind}-{}", std::process::id());
    let fixture = ReplayFixture::register(&job_id);
    let mut spec = two_component_job(
        JobId::new(&job_id).unwrap(),
        format!("file://{}", directory.path().display()),
        directory.path().join("state").display().to_string(),
    );
    spec.recovery = recovery;
    for source in &mut spec.sources {
        source.input_type = fixture.input_type.clone();
        source.config = serde_json::json!({"source": source.operator_id});
    }
    for (sink, source) in spec.sinks.iter_mut().zip(&spec.sources) {
        sink.output_type = fixture.output_type.clone();
        sink.config = serde_json::json!({"source": source.operator_id});
    }
    for suffix in ["a", "b"] {
        let counter_id = format!("count-{suffix}");
        spec.operators.push(OperatorSpec {
            id: counter_id.clone(),
            kind: OperatorKind::Map,
            stateful: true,
            key_field: Some("key".into()),
            config: serde_json::json!({"type": "sql", "query": "SELECT * FROM flow"}),
        });
        spec.edges
            .iter_mut()
            .find(|edge| edge.id == format!("edge-{suffix}"))
            .unwrap()
            .to = counter_id.clone();
        spec.edges.push(arkflow_core::job::EdgeSpec {
            id: format!("count-sink-{suffix}"),
            from: counter_id,
            to: format!("sink-{suffix}"),
            partitioned: false,
        });
    }
    let plan = JobPlan::compile(spec.clone()).unwrap();
    let planned_tasks = plan
        .tasks
        .iter()
        .map(|task| task.id.as_str())
        .collect::<BTreeSet<_>>();
    let commands = Arc::new(Mutex::new(Vec::new()));
    let mut server = HubHarness::start(
        &sqlite_path,
        "127.0.0.1:0".parse().unwrap(),
        commands.clone(),
    )
    .await;
    let agents = start_agents(&server.url);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    wait_until(|| {
        let hub = server.hub.clone();
        async move { hub.nodes().await.len() == 2 }
    })
    .await;
    fixture.publish_through(8);
    server
        .hub
        .upsert_job(JobRecord {
            job_id: job_id.clone(),
            version: 1,
            spec_json: serde_json::to_string(&spec).unwrap(),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec!["node-a".into(), "node-b".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
            pause_phase: None,
            pause_artifact_id: None,
            pause_prev_checkpoint_id: None,
            pause_retry_count: 0,
            paused_at_ms: None,
            paused_by: None,
        })
        .await
        .unwrap();
    wait_for_running_rows(&server.hub, &plan, 1, &fixture, 8).await;

    // Round one persists paused state across a Hub-only restart. The Agents
    // keep their boot identities. Round two pauses and resumes the recovered
    // running Job, exercising a complete new capture on the restarted Hub.
    for generation in 1..=2 {
        let cut = generation * 8;
        let pausing = server.action(&client, &job_id, "pause").await;
        assert_eq!(pausing.desired_state, "paused");
        assert_eq!(pausing.generation, generation);
        assert!(pausing.pause_artifact_id.is_some());
        assert_eq!(pausing.checkpoint_id, pausing.pause_artifact_id);
        wait_for_pause_resume(
            &server.hub,
            &job_id,
            &fixture,
            &format!("paused generation {generation}"),
            || {
                let hub = server.hub.clone();
                let job_id = job_id.clone();
                let fixture = fixture.clone();
                async move {
                    let job = hub.job(&job_id).await.unwrap().unwrap();
                    if job.desired_state != "paused" {
                        panic!(
                            "pause aborted:\n{}",
                            pause_resume_diagnostics(&hub, &job_id, &fixture).await
                        );
                    }
                    let operations = hub.operations(None).await;
                    job.pause_phase.as_deref() == Some("paused")
                        && job.observed_state == "stopped"
                        && job.convergence == "converged"
                        && hub.observed_job_tasks(&job_id).await.is_empty()
                        && ["node-a", "node-b"].iter().all(|node| {
                            operations.iter().any(|operation| {
                                operation.resource_id == job_id
                                    && operation.node_id == *node
                                    && operation.generation == generation
                                    && operation.operation.as_str() == "job_stop"
                                    && operation.state == HubOperationState::Succeeded
                            })
                        })
                }
            },
        )
        .await;
        let paused = server.hub.job(&job_id).await.unwrap().unwrap();
        // A bounded capture retry may replace the initial artifact. The pin
        // that reached paused is the one resume must restore.
        let artifact_id = paused.pause_artifact_id.clone().unwrap();
        assert_eq!(paused.checkpoint_id, paused.pause_artifact_id);
        assert_eq!(paused.node_ids, ["node-a", "node-b"]);
        let artifact = server
            .hub
            .job_checkpoints(&job_id)
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.checkpoint_id == artifact_id)
            .unwrap();
        assert_eq!(artifact.kind, kind);
        assert_eq!(artifact.status, "completed");
        assert_eq!(artifact.job_version, spec.version.0);
        let manifest_key = recovery_manifest_key(artifact_kind, &artifact_id);
        let manifest_path = directory.path().join(&manifest_key);
        assert_eq!(
            artifact.manifest_uri.as_deref(),
            Some(format!("file://{}", manifest_path.display()).as_str())
        );
        let manifest: CheckpointManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert!(manifest.verify(), "kernel manifest checksum must verify");
        assert_eq!(manifest.checkpoint_id, artifact_id);
        assert_eq!(manifest.job_id, spec.id);
        assert_eq!(manifest.job_version, spec.version);
        assert_eq!(manifest.generation, generation);
        assert_eq!(manifest.in_flight_barrier.generation, generation);
        assert_eq!(manifest.in_flight_barrier.checkpoint_id, artifact_id);
        assert_eq!(manifest.task_attempts.len(), planned_tasks.len());
        assert_eq!(
            manifest
                .task_attempts
                .iter()
                .map(|attempt| attempt.task_id.as_str())
                .collect::<BTreeSet<_>>(),
            planned_tasks
        );
        assert_eq!(
            manifest
                .task_attempts
                .iter()
                .map(|attempt| attempt.node_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["node-a", "node-b"])
        );
        assert_eq!(manifest.source_positions.len(), 2);
        assert_eq!(
            manifest
                .source_positions
                .iter()
                .map(|position| (
                    position.topic.as_deref(),
                    position.partition,
                    position.offset
                ))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([(Some("source-a"), 0, cut), (Some("source-b"), 0, cut)])
        );
        let repository =
            CheckpointRepository::new(FileCheckpointStore::new(directory.path()).unwrap());
        let mut state_nodes = BTreeSet::new();
        for reference in &manifest.state_snapshots {
            // Validate the actual files and their checksums, including every
            // planned source/processor/sink reference in both node manifests.
            let snapshot = repository.read_state_snapshot(reference).unwrap();
            if !snapshot.entries.is_empty() {
                state_nodes.insert(reference.node_id.as_deref().unwrap());
            }
        }
        assert_eq!(state_nodes, BTreeSet::from(["node-a", "node-b"]));
        let operations = server.hub.operations(None).await;
        for node in ["node-a", "node-b"] {
            assert!(operations.iter().any(|operation| {
                operation.resource_id == job_id
                    && operation.node_id == node
                    && operation.generation == generation
                    && operation.operation.as_str() == format!("job_{kind}")
                    && operation.checkpoint_id.as_deref() == Some(artifact_id.as_str())
                    && operation.state == HubOperationState::Succeeded
            }));
            assert!(
                operations.iter().any(|operation| {
                    operation.resource_id == job_id
                        && operation.node_id == node
                        && operation.generation == generation
                        && operation.operation.as_str() == "job_stop"
                        && operation.state == HubOperationState::Succeeded
                        && operation.created_at_ms >= artifact.updated_at_ms
                }),
                "{node}: stop must follow durable artifact completion"
            );
        }

        if generation == 1 {
            let address = server.address;
            server.shutdown().await;
            server = HubHarness::start(&sqlite_path, address, commands.clone()).await;
            let restored = server.hub.job(&job_id).await.unwrap().unwrap();
            assert_eq!(restored.generation, paused.generation);
            assert_eq!(restored.desired_state, "paused");
            assert_eq!(restored.observed_state, "stopped");
            assert_eq!(restored.convergence, "converged");
            assert_eq!(restored.pause_phase.as_deref(), Some("paused"));
            assert_eq!(restored.pause_artifact_id, paused.pause_artifact_id);
            assert_eq!(restored.checkpoint_id, paused.checkpoint_id);
            assert_eq!(
                restored.pause_prev_checkpoint_id,
                paused.pause_prev_checkpoint_id
            );
            assert_eq!(restored.pause_retry_count, paused.pause_retry_count);
            assert_eq!(restored.paused_at_ms, paused.paused_at_ms);
            assert_eq!(restored.paused_by, paused.paused_by);
            assert_eq!(restored.node_ids, paused.node_ids);
            assert!(server
                .hub
                .job_checkpoints(&job_id)
                .await
                .unwrap()
                .contains(&artifact));
            wait_until(|| {
                let hub = server.hub.clone();
                async move { hub.nodes().await.len() == 2 }
            })
            .await;
        }

        // Publish a backlog while paused and wait for fresh reports from both
        // Agents: a stopped Job must neither consume it nor dispatch starts.
        let command_count = commands.lock().unwrap().len();
        let last_seen = server
            .hub
            .nodes()
            .await
            .iter()
            .map(|node| node.last_seen_at_ms)
            .max()
            .unwrap();
        fixture.publish_through(cut + 8);
        for _ in 0..3 {
            assert_eq!(server.hub.reconcile_jobs().await.unwrap(), 0);
            assert_eq!(server.hub.reconcile_job_pauses().await.unwrap(), 0);
        }
        wait_until(|| {
            let hub = server.hub.clone();
            async move {
                let nodes = hub.nodes().await;
                nodes.len() == 2 && nodes.iter().all(|node| node.last_seen_at_ms > last_seen)
            }
        })
        .await;
        fixture.assert_rows(cut);
        assert!(server.hub.observed_job_tasks(&job_id).await.is_empty());
        assert_eq!(commands.lock().unwrap().len(), command_count);

        let resuming = server.action(&client, &job_id, "resume").await;
        assert_eq!(resuming.desired_state, "running");
        assert_eq!(resuming.generation, generation + 1);
        assert_eq!(
            resuming.checkpoint_id.as_deref(),
            Some(artifact_id.as_str())
        );
        wait_for_running_rows(&server.hub, &plan, generation + 1, &fixture, cut + 8).await;
        {
            let delivered = commands.lock().unwrap();
            let starts = delivered
                .iter()
                .filter(|command| {
                    command.resource_id == job_id
                        && command.operation.as_str() == "job_start"
                        && command.generation == generation + 1
                })
                .collect::<Vec<_>>();
            assert_eq!(
                starts
                    .iter()
                    .map(|command| command.node_id.as_str())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from(["node-a", "node-b"])
            );
            for command in starts {
                let payload = command.payload.as_ref().unwrap();
                assert_eq!(payload["recovery"]["checkpoint_id"], artifact_id);
                assert_eq!(payload["recovery"]["savepoint"], kind == "savepoint");
                assert_eq!(payload["recovery_required"], true);
            }
        }
        fixture.assert_restored(&(1..=generation).map(|round| round * 8).collect::<Vec<_>>());
        let running = server.hub.job(&job_id).await.unwrap().unwrap();
        assert_eq!(running.checkpoint_id.as_deref(), Some(artifact_id.as_str()));
        assert!(running.pause_artifact_id.is_none());
        assert!(running.paused_at_ms.is_none());
        assert!(running.paused_by.is_none());
    }
    fixture.assert_rows(24);
    for agent in agents {
        agent.shutdown().await;
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_node_pause_resume_latest_checkpoint_survives_sqlite_hub_restart() {
    exercise_two_node_pause_resume(RecoveryPolicy::LatestCheckpoint).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_node_pause_resume_latest_savepoint_survives_sqlite_hub_restart() {
    exercise_two_node_pause_resume(RecoveryPolicy::LatestSavepoint).await;
}
