//! End-to-end data-plane observability: a locally running YAML-style Job
//! exports kernel metrics through the process observability endpoints, and
//! the exported counters advance while data flows.

use arkflow_core::config::EngineConfig;
use arkflow_core::engine::Engine;
use arkflow_server::{observability_router, ServerConfig};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn job_config() -> EngineConfig {
    serde_json::from_str(
        r#"{
        "streams": [],
        "jobs": [
            {
                "id": "sensor-window-job",
                "version": 1,
                "parallelism": 1,
                "max_parallelism": 128,
                "operators": [
                    {"id": "source", "kind": "source"},
                    {"id": "sink", "kind": "sink"}
                ],
                "edges": [
                    {"id": "e1", "from": "source", "to": "sink", "partitioned": true}
                ],
                "sources": [
                    {
                        "operator_id": "source",
                        "input_type": "generate",
                        "config": {
                            "type": "generate",
                            "context": "{ \"sensor\": \"temp_1\", \"value\": 10, \"ts\": 1757000000000 }",
                            "interval": "20ms",
                            "batch_size": 10
                        },
                        "time": {
                            "mode": "processing_time"
                        }
                    }
                ],
                "sinks": [
                    {"operator_id": "sink", "output_type": "drop"}
                ]
            }
        ],
        "logging": {"level": "warn"},
        "health_check": {}
    }"#,
    )
    .expect("job config must parse")
}

async fn scrape(app: &axum::Router) -> String {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

/// Sum the `arkflow_job_chain_batches_in_total` samples for one Job.
fn batches_in_total(exposition: &str, job: &str) -> u64 {
    exposition
        .lines()
        .filter(|line| {
            line.starts_with("arkflow_job_chain_batches_in_total{")
                && line.contains(&format!("job=\"{job}\""))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .unwrap()
                .parse::<u64>()
                .expect("sample value must be an unsigned number")
        })
        .sum()
}

/// Drive the engine future one step; a premature exit fails the test with
/// the engine's own result instead of leaving a confusing empty registry.
async fn drive_engine<F: std::future::Future>(
    engine_fut: &mut std::pin::Pin<&mut F>,
    phase: &str,
) where
    F::Output: std::fmt::Debug,
{
    use std::task::Poll;
    if let Poll::Ready(result) = futures_util::poll!(engine_fut.as_mut()) {
        panic!("engine exited during {phase}: {result:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_job_exports_kernel_metrics_and_counters_advance() {
    arkflow_plugin::initialize().unwrap();

    let engine = Engine::new(job_config());
    let cancellation = CancellationToken::new();
    let control_plane = engine.control_plane();
    // `run_with_cancellation` is not Send (its error type is not), so the
    // test drives it manually instead of spawning it.
    let mut engine_fut = std::pin::pin!(engine.run_with_cancellation(cancellation.clone()));

    // The engine flips readiness only after the Job's resource startup has
    // actually completed.
    let mut ready = false;
    for _ in 0..400 {
        drive_engine(&mut engine_fut, "startup").await;
        if control_plane.health().is_ready() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        ready && control_plane.health().is_ready(),
        "engine did not become ready"
    );

    let config = ServerConfig::default();
    let app = observability_router(control_plane.clone(), &config);
    // Chain families appear once the graph's event loops registered their
    // counters; poll briefly rather than racing the first data flow.
    let mut first = String::new();
    for _ in 0..200 {
        drive_engine(&mut engine_fut, "first scrape").await;
        first = scrape(&app).await;
        if first.contains("# TYPE arkflow_job_chain_batches_in_total counter") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        first.contains("# TYPE arkflow_job_chain_batches_in_total counter"),
        "missing batches_in TYPE line in:\n{first}"
    );
    assert!(
        first.contains("job=\"sensor-window-job\""),
        "missing job label in:\n{first}"
    );
    // The full vocabulary is present, including the event-time gauges.
    assert!(first.contains("# TYPE arkflow_job_watermark_lag_ms gauge"));
    assert!(first.contains("# TYPE arkflow_job_checkpoint_duration_ms gauge"));
    assert!(first.contains("# TYPE arkflow_job_late_events_total counter"));

    let before = batches_in_total(&first, "sensor-window-job");
    tokio::time::sleep(Duration::from_millis(200)).await;
    drive_engine(&mut engine_fut, "second scrape").await;
    let second = scrape(&app).await;
    let after = batches_in_total(&second, "sensor-window-job");
    assert!(
        after > before,
        "batches_in must advance while the Job consumes data (before={before}, after={after})"
    );

    cancellation.cancel();
    loop {
        use std::task::Poll;
        if let Poll::Ready(result) = futures_util::poll!(engine_fut.as_mut()) {
            result.expect("engine failed during shutdown");
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
