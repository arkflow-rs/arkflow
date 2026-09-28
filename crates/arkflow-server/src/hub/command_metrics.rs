//! Bounded-cardinality command dispatch accounting (Prometheus text).

use super::*;

/// Fixed latency buckets (milliseconds) for command dispatch accounting.
const COMMAND_METRIC_BUCKETS_MS: &[u64] = &[5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// Low-cardinality command dispatch accounting: fixed command-type labels,
/// fixed outcome classes, fixed latency buckets. Counters live in process
/// memory and reset on Hub restart, in line with Prometheus counter
/// semantics.
#[derive(Default)]
pub struct CommandMetrics {
    latencies: std::sync::Mutex<BTreeMap<String, CommandLatency>>,
    outcomes: std::sync::Mutex<BTreeMap<(String, String), u64>>,
}

#[derive(Default)]
struct CommandLatency {
    buckets: Vec<u64>,
    sum_ms: u64,
    count: u64,
}

impl CommandMetrics {
    /// Bound the label vocabulary: recognized command names pass through;
    /// anything else collapses into `other` so dynamic operation strings
    /// cannot grow the series space.
    fn command_label(operation: &str) -> String {
        const KNOWN: &[&str] = &[
            "job_start",
            "job_stop",
            "job_checkpoint",
            "job_savepoint",
            "job_checkpoint_commit",
            "job_savepoint_commit",
            "start",
            "stop",
            "restart",
            "apply_configuration",
        ];
        if KNOWN.contains(&operation) {
            operation.to_owned()
        } else {
            "other".to_owned()
        }
    }

    pub(crate) fn outcome_label(state: HubOperationState) -> Option<&'static str> {
        match state {
            HubOperationState::Succeeded => Some("succeeded"),
            HubOperationState::Failed => Some("failed"),
            HubOperationState::TimedOut => Some("timed_out"),
            HubOperationState::NodeUnavailable => Some("node_unavailable"),
            HubOperationState::Cancelled => Some("cancelled"),
            HubOperationState::Superseded => Some("superseded"),
            HubOperationState::Queued
            | HubOperationState::Dispatched
            | HubOperationState::Acknowledged
            | HubOperationState::Running => None,
        }
    }

    pub(crate) fn record_outcome(&self, operation: &str, outcome: &str) {
        let label = Self::command_label(operation);
        if let Ok(mut outcomes) = self.outcomes.lock() {
            *outcomes.entry((label, outcome.to_owned())).or_default() += 1;
        }
    }

    pub(crate) fn record_latency(&self, operation: &str, duration_ms: u64) {
        let label = Self::command_label(operation);
        let Ok(mut latencies) = self.latencies.lock() else {
            return;
        };
        let latency = latencies.entry(label).or_default();
        if latency.buckets.is_empty() {
            latency.buckets = vec![0; COMMAND_METRIC_BUCKETS_MS.len() + 1];
        }
        let index = COMMAND_METRIC_BUCKETS_MS.partition_point(|bound| *bound < duration_ms);
        latency.buckets[index] += 1;
        latency.sum_ms += duration_ms;
        latency.count += 1;
    }

    /// Render the Prometheus text exposition for command dispatch metrics.
    /// Series are bounded by the fixed command-type and outcome-class
    /// enumerations; resource IDs and error texts never appear.
    pub fn render(&self) -> String {
        let mut body = String::new();
        if let Ok(latencies) = self.latencies.lock() {
            for (command, latency) in latencies.iter() {
                for (index, bound) in COMMAND_METRIC_BUCKETS_MS.iter().enumerate() {
                    body.push_str(&format!(
                        "arkflow_command_duration_bucket{{command=\"{command}\",le=\"{bound}\"}} {}\n",
                        latency.buckets[index]
                    ));
                }
                body.push_str(&format!(
                    "arkflow_command_duration_bucket{{command=\"{command}\",le=\"+Inf\"}} {}\n",
                    latency.count
                ));
                body.push_str(&format!(
                    "arkflow_command_duration_count{{command=\"{command}\"}} {}\n",
                    latency.count
                ));
                body.push_str(&format!(
                    "arkflow_command_duration_sum{{command=\"{command}\"}} {}\n",
                    latency.sum_ms
                ));
            }
        }
        if let Ok(outcomes) = self.outcomes.lock() {
            for ((command, outcome), total) in outcomes.iter() {
                body.push_str(&format!(
                    "arkflow_command_total{{command=\"{command}\",outcome=\"{outcome}\"}} {total}\n"
                ));
            }
        }
        body
    }
}
