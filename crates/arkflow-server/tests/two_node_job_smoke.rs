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

fn two_component_job(id: JobId, checkpoint_uri: String) -> JobSpec {
    let processing_time = || TimeSpec {
        mode: TimeMode::ProcessingTime,
        timestamp_field: None,
        watermark: None,
        allowed_lateness_ms: 0,
        late_event_policy: Default::default(),
        late_event_route: None,
    };
    JobSpec {
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
            namespace: None,
            ttl_ms: None,
            format_version: 1,
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
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("two-node smoke condition timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_node_hub_agent_checkpoint_and_restart_recover() {
    arkflow_plugin::initialize().unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let job_id = format!("two-node-smoke-{}", std::process::id());
    let spec = two_component_job(
        JobId::new(&job_id).unwrap(),
        format!("file://{}", checkpoint_dir.path().display()),
    );

    let hub = Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        lease_ttl_ms: 2_000,
        poll_interval_ms: 20,
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
        },
        cancel_a_restart.clone(),
    ));

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
                >= 3
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
