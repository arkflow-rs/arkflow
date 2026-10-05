//! Host resource sampling and data-plane mTLS material.
use super::now_ms;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// A snapshot older than this multiple of the sampling interval is stale and
/// omitted from reports: a dead sampler must not produce a lying dashboard.
const RESOURCE_FRESHNESS_FACTOR: u64 = 2;
/// Lower bound on the derived sampling interval: below this, sysinfo
/// refreshes cost more than fresher gauges are worth.
pub(super) const MIN_RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// One host resource sample. CPU is only meaningful from the second refresh
/// onward (sysinfo needs a prior window to average over); memory is valid
/// immediately.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ResourceSnapshot {
    pub sampled_at_ms: u64,
    pub cpu_usage_percent: Option<f64>,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
    /// Logical CPU cores — static capacity for placement feasibility.
    pub cpu_cores: u32,
}

/// Shared latest-snapshot slot between the sampler task and the report path.
#[derive(Clone)]
pub(crate) struct ResourceSampler {
    pub(super) sample_interval: Duration,
    latest: Arc<std::sync::RwLock<Option<ResourceSnapshot>>>,
}

impl ResourceSampler {
    /// Sample at half the report cadence (bounded below) so every report
    /// reads a snapshot the previous report cannot have seen: a Hub-side
    /// sustained-pressure streak then counts independent observations
    /// instead of one sample echoed across consecutive reports.
    pub(super) fn new(report_interval: Duration) -> Self {
        Self {
            sample_interval: (report_interval / 2).max(MIN_RESOURCE_SAMPLE_INTERVAL),
            latest: Arc::default(),
        }
    }

    pub(super) fn publish(&self, snapshot: ResourceSnapshot) {
        *self
            .latest
            .write()
            .expect("resource sampler slot lock poisoned") = Some(snapshot);
    }

    pub(super) fn fresh_window_ms(&self) -> u64 {
        self.sample_interval.as_millis() as u64 * RESOURCE_FRESHNESS_FACTOR
    }

    /// The latest snapshot when it is still fresh for `now_ms`, else `None`.
    pub(crate) fn fresh(&self, now_ms: u64) -> Option<ResourceSnapshot> {
        let snapshot = *self
            .latest
            .read()
            .expect("resource sampler slot lock poisoned")
            .as_ref()?;
        (now_ms.saturating_sub(snapshot.sampled_at_ms) <= self.fresh_window_ms())
            .then_some(snapshot)
    }
}

/// Merge a fresh snapshot into the report's metrics map under the fixed
/// `node_*` vocabulary; the CPU gauge is skipped until it has a real window.
/// Build the data-plane mTLS material from `ARKFLOW_DATA_PLANE_TLS_CERT`,
/// `_KEY`, and `_CA` (PEM file paths). All three or none: a partial set is
/// an explicit configuration error (a half-loaded TLS config must fail, not
/// silently degrade to plaintext). Files are read once at startup.
pub(super) fn data_plane_tls_from_env(
) -> Result<Option<arkflow_core::executor::remote::DataPlaneTlsConfig>, String> {
    let cert = std::env::var("ARKFLOW_DATA_PLANE_TLS_CERT").ok();
    let key = std::env::var("ARKFLOW_DATA_PLANE_TLS_KEY").ok();
    let ca = std::env::var("ARKFLOW_DATA_PLANE_TLS_CA").ok();
    let declared = [cert.is_some(), key.is_some(), ca.is_some()];
    if declared == [false, false, false] {
        return Ok(None);
    }
    if declared != [true, true, true] {
        // Fail closed per the authenticated-network-shuffle contract: a
        // half-loaded TLS config must fail startup, never degrade to
        // plaintext.
        return Err(
            "ARKFLOW_DATA_PLANE_TLS_CERT/_KEY/_CA must be set together (partial TLS configuration)"
                .into(),
        );
    }
    let read = |value: Option<String>, name: &str| -> Result<String, String> {
        value
            .map(|path| {
                std::fs::read_to_string(&path).map_err(|error| {
                    format!("data-plane TLS {name} '{path}' could not be read: {error}")
                })
            })
            .transpose()
            .map(|value| value.expect("checked Some above"))
    };
    let cert = read(cert, "certificate")?;
    let key = read(key, "private key")?;
    let ca = read(ca, "fleet CA")?;
    match arkflow_core::executor::remote::DataPlaneTlsConfig::from_pem(&cert, &key, &ca) {
        Ok(tls) => {
            info!("data-plane mTLS enabled (fleet CA anchored)");
            Ok(Some(tls))
        }
        Err(error) => Err(format!("data-plane TLS material rejected: {error}")),
    }
}

pub(super) fn merge_resource_gauges(
    metrics: &mut BTreeMap<String, f64>,
    snapshot: ResourceSnapshot,
) {
    if let Some(cpu) = snapshot.cpu_usage_percent {
        metrics.insert("node_cpu_usage_percent".into(), cpu);
    }
    metrics.insert(
        "node_memory_used_bytes".into(),
        snapshot.memory_used_bytes as f64,
    );
    metrics.insert(
        "node_memory_total_bytes".into(),
        snapshot.memory_total_bytes as f64,
    );
    metrics.insert(
        "node_memory_available_bytes".into(),
        snapshot.memory_available_bytes as f64,
    );
    if snapshot.cpu_cores > 0 {
        metrics.insert("node_cpu_cores".into(), f64::from(snapshot.cpu_cores));
    }
}

/// Spawn the host resource sampler: a fixed-interval task publishing into the
/// shared slot. Best-effort by construction — every failure mode (unsupported
/// platform, poisoned state, task death) leaves reports running without
/// resource gauges and never touches the session loop.
pub(crate) fn spawn_resource_sampler(
    report_interval: Duration,
    cancellation: CancellationToken,
) -> ResourceSampler {
    let sampler = ResourceSampler::new(report_interval);
    let sample_interval = sampler.sample_interval;
    let task_sampler = sampler.clone();
    tokio::spawn(async move {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        // Baseline CPU refresh: publishes below only start once a refresh has
        // a prior window to average over, so no bogus 0% is ever reported.
        system.refresh_cpu_usage();
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(sample_interval) => {}
            }
            system.refresh_memory();
            system.refresh_cpu_usage();
            task_sampler.publish(ResourceSnapshot {
                sampled_at_ms: now_ms(),
                cpu_usage_percent: Some(f64::from(system.global_cpu_usage())),
                memory_used_bytes: system.used_memory(),
                memory_total_bytes: system.total_memory(),
                memory_available_bytes: system.available_memory(),
                cpu_cores: system.cpus().len() as u32,
            });
        }
    });
    sampler
}
