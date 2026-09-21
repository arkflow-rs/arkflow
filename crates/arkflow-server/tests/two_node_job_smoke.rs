use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::job::{
    CheckpointSpec, JobId, JobSpec, JobVersion, OperatorKind, OperatorSpec, SinkSpec, SourceSpec,
    StateSpec, TimeMode, TimeSpec,
};
use arkflow_core::runtime::RuntimeManager;
use arkflow_server::agent::{self, NodeAgentConfig};
use arkflow_server::hub::{Hub, HubConfig, HubOperationState};
use arkflow_server::storage::JobRecord;
use arkflow_server::{hub_router, ServerConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

fn empty_control_plane() -> ControlPlane {
    ControlPlane::new(
        EngineConfig {
            streams: Vec::new(),
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
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
        rebalance: None,
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
                config: serde_json::json!({}),
            },
            SinkSpec {
                operator_id: "sink-b".into(),
                output_type: "drop".into(),
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
            interval_ms: 60_000,
            retention: 3,
            object_store_uri: checkpoint_uri,
        }),
        recovery: Default::default(),
    }
}

async fn wait_until<F, Fut>(mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    static WAIT_INDEX: AtomicUsize = AtomicUsize::new(0);
    let wait_index = WAIT_INDEX.fetch_add(1, Ordering::Relaxed) + 1;
    // Generous under load: the full workspace suite runs other binaries on
    // the same machine, and cold caches can stretch kernel startup.
    tokio::time::timeout(Duration::from_secs(30), async {
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
        session_ttl_ms: arkflow_server::hub::default_session_ttl_ms(),
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
    let agent_a = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
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
    let agent_b = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
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
    })
    .await
    .unwrap();

    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.operations(None)
                .await
                .iter()
                .filter(|operation| {
                    operation.resource_id == job_id
                        && operation.operation == "job_start"
                        && operation.state == HubOperationState::Succeeded
                })
                .count()
                == 2
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
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.job_checkpoints(&job_id)
                .await
                .unwrap()
                .iter()
                .any(|record| record.status == "completed")
        }
    })
    .await;

    // Kill node-a, then boot a fresh Agent identity on the same node. The Hub
    // must fence the old start attempt, dispatch the checkpoint recovery, and
    // converge both assignments again.
    cancel_a.cancel();
    tokio::time::timeout(Duration::from_secs(5), agent_a)
        .await
        .expect("node-a agent did not stop")
        .unwrap()
        .unwrap();

    let cancel_a_restart = CancellationToken::new();
    let restarted_a = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url,
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
                && operation.operation == "job_start"
                && operation.state == HubOperationState::Succeeded
        })
        .map(|operation| operation.id)
        .collect::<Vec<_>>();
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        let initial_start_ids = initial_start_ids.clone();
        async move {
            hub.operations(None).await.iter().any(|operation| {
                operation.resource_id == job_id
                    && operation.operation == "job_start"
                    && operation.state == HubOperationState::Succeeded
                    && !initial_start_ids.contains(&operation.id)
            })
        }
    })
    .await;

    cancel_a_restart.cancel();
    cancel_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), restarted_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), agent_b).await;
    reconcile_cancel.cancel();
    let _ = reconcile_task.await;
    hub_cancel.cancel();
    let _ = hub_task.await;
}

/// Split placement end to end: the source runs on node-a, the sink on
/// node-b, and the forward edge between them crosses the real TCP data
/// plane. Proof points: both agents build remote-edge graphs (kernel metrics
/// show sink-side rows on node-b), the Hub-dispatched checkpoint barriers
/// align through the network edge, and the checkpoint completes through the
/// existing aggregation path (all_nodes_succeeded → job_checkpoint_commit).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn split_job_runs_across_nodes_and_aggregates_checkpoint() {
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
    spec.operators.retain(|operator| {
        operator.id == "source-a" || operator.id == "sink-a"
    });
    spec.edges.retain(|edge| edge.id == "edge-a");
    spec.sources.retain(|source| source.operator_id == "source-a");
    spec.sinks.retain(|sink| sink.operator_id == "sink-a");

    let hub = Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 2_000,
        poll_interval_ms: 20,
        session_ttl_ms: arkflow_server::hub::default_session_ttl_ms(),
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
    let agent_a = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
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
    let agent_b = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: hub_url.clone(),
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
    })
    .await
    .unwrap();

    // Both assignments start: the split placement dispatched, meaning the
    // data-plane capability validation passed for both nodes.
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.operations(None)
                .await
                .iter()
                .filter(|operation| {
                    operation.resource_id == job_id
                        && operation.operation == "job_start"
                        && operation.state == HubOperationState::Succeeded
                })
                .count()
                == 2
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
                .is_some_and(|snapshot| {
                    snapshot
                        .chains
                        .values()
                        .any(|chain| chain.batches_in > 0)
                })
        }
    })
    .await;

    // The checkpoint must aggregate: both nodes snapshot, the coordinator
    // merges both manifests, and the record completes.
    assert_eq!(hub.schedule_periodic_checkpoints().await.unwrap(), 1);
    wait_until(|| {
        let hub = hub.clone();
        let job_id = job_id.clone();
        async move {
            hub.job_checkpoints(&job_id)
                .await
                .unwrap()
                .iter()
                .any(|record| record.status == "completed")
        }
    })
    .await;

    cancel_a.cancel();
    cancel_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), agent_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), agent_b).await;
    reconcile_cancel.cancel();
    let _ = reconcile_task.await;
    hub_cancel.cancel();
    let _ = hub_task.await;
}
