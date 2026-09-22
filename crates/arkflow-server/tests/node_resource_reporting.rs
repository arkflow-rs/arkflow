//! End-to-end resource gauge flow: a real Agent session samples host
//! resources, ships them inside the regular report, and the Hub serves them
//! through its metrics export. Uses the real 5s sampling cadence — the first
//! CPU publish needs one full interval after the baseline refresh.

use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::runtime::RuntimeManager;
use arkflow_server::agent::{self, NodeAgentConfig};
use arkflow_server::hub::{Hub, HubConfig};
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

async fn metrics_export(base_url: &str) -> String {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("{base_url}/api/v1/metrics"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_resource_gauges_reach_the_hub_metrics_export() {
    let hub = Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 30_000,
        poll_interval_ms: 20,
        session_ttl_ms: arkflow_server::hub::default_session_ttl_ms(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let hub_cancel = CancellationToken::new();
    let server_hub = hub.clone();
    let server_cancel = hub_cancel.clone();
    let mut hub_task = tokio::spawn(async move {
        axum::serve(
            listener,
            hub_router(server_hub, &ServerConfig::default()).into_make_service(),
        )
        .with_graceful_shutdown(server_cancel.cancelled_owned())
        .await
    });

    let agent_cancel = CancellationToken::new();
    let mut agent_task = tokio::spawn(agent::run(
        empty_control_plane(),
        NodeAgentConfig {
            hub_url: format!("http://{address}"),
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            node_token: String::new(),
            boot_id: "resource-boot-a".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(50),
            data_port: None,
            data_host: None,
        },
        agent_cancel.clone(),
    ));

    // Gauges arrive with the regular report once the sampler has published;
    // the freshness window keeps them flowing on every subsequent report.
    let deadline = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let Some(view) = hub
                .metrics_by_node(Some("node-a"))
                .await
                .into_iter()
                .next()
            else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            if view.metrics.contains_key("node_cpu_usage_percent") {
                for key in [
                    "node_cpu_usage_percent",
                    "node_memory_used_bytes",
                    "node_memory_total_bytes",
                    "node_memory_available_bytes",
                ] {
                    assert!(
                        view.metrics.contains_key(key),
                        "expected {key} alongside the CPU gauge"
                    );
                }
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        deadline.is_ok(),
        "resource gauges never surfaced at the Hub within the sampling deadline"
    );

    // The export renders the gauges as arkflow_node_metric series with the
    // node label, values passed through unchanged.
    let body = metrics_export(&format!("http://{address}")).await;
    let view = hub
        .metrics_by_node(Some("node-a"))
        .await
        .into_iter()
        .next()
        .unwrap();
    for key in [
        "node_cpu_usage_percent",
        "node_memory_used_bytes",
        "node_memory_total_bytes",
        "node_memory_available_bytes",
    ] {
        let value = view.metrics[key];
        assert!(
            body.contains(&format!(
                "arkflow_node_metric{{node_id=\"node-a\",metric=\"{key}\"}} {value}"
            )),
            "export missing {key}={value}:\n{}",
            body.lines()
                .filter(|line| line.contains("node_metric"))
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    agent_cancel.cancel();
    hub_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut agent_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut hub_task).await;
    // Deterministic teardown: a pending task polled during runtime drop can
    // panic and abort the test binary after the summary printed, so abort
    // anything the cancellation did not stop in time.
    agent_task.abort();
    hub_task.abort();
}
