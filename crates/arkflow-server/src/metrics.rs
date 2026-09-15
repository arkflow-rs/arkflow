//! Data-plane Prometheus export: kernel Job and legacy Stream metric
//! snapshots rendered as Prometheus text exposition (format 0.0.4).
//!
//! Families are rebuilt from snapshots on every scrape — no global registry,
//! so Job start/stop cannot race collector registration. The label vocabulary
//! is closed: `node`, `job`, `chain`, and `stream_id` only. Message content,
//! error text, and correlation IDs never become labels.

use arkflow_core::control::StreamMetricsSnapshot;
use arkflow_core::executor::metrics::KernelMetricsSnapshot;
use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{Encoder, TextEncoder};
use std::collections::BTreeMap;

type Label = (&'static str, String);

fn label_pairs(pairs: &[Label]) -> Vec<LabelPair> {
    pairs
        .iter()
        .map(|(name, value)| {
            let mut label = LabelPair::default();
            label.set_name((*name).to_string());
            label.set_value(value.clone());
            label
        })
        .collect()
}

/// Build one family from `(labels, value)` series; skipped when empty so a
/// Job without chains does not produce an empty exposition entry.
fn family(
    name: &str,
    help: &str,
    metric_type: MetricType,
    series: Vec<(Vec<Label>, u64)>,
) -> Option<MetricFamily> {
    if series.is_empty() {
        return None;
    }
    let mut output = MetricFamily::default();
    output.set_name(name.to_string());
    output.set_help(help.to_string());
    output.set_field_type(metric_type);
    for (labels, value) in series {
        let mut metric = Metric::default();
        metric.set_label(label_pairs(&labels).into());
        match metric_type {
            MetricType::COUNTER => {
                let mut counter = Counter::default();
                counter.set_value(value as f64);
                metric.set_counter(counter);
            }
            _ => {
                let mut gauge = Gauge::default();
                gauge.set_value(value as f64);
                metric.set_gauge(gauge);
            }
        }
        output.mut_metric().push(metric);
    }
    Some(output)
}

/// Prometheus families for one kernel Job snapshot. `extra` carries
/// aggregation labels (currently only `node` for the Hub export).
pub fn kernel_job_families(
    job_id: &str,
    snapshot: &KernelMetricsSnapshot,
    extra: &[Label],
) -> Vec<MetricFamily> {
    let with_chain = |chain: &str| {
        let mut labels = extra.to_vec();
        labels.push(("job", job_id.to_string()));
        labels.push(("chain", chain.to_string()));
        labels
    };
    let job_labels = || {
        let mut labels = extra.to_vec();
        labels.push(("job", job_id.to_string()));
        labels
    };

    let mut families = Vec::new();
    let mut batches_in = Vec::new();
    let mut batches_out = Vec::new();
    let mut rows = Vec::new();
    let mut errors = Vec::new();
    let mut in_flight = Vec::new();
    let mut mean_latency_us = Vec::new();
    for (chain_id, chain) in &snapshot.chains {
        batches_in.push((with_chain(chain_id), chain.batches_in));
        batches_out.push((with_chain(chain_id), chain.batches_out));
        rows.push((with_chain(chain_id), chain.rows));
        errors.push((with_chain(chain_id), chain.errors));
        in_flight.push((with_chain(chain_id), chain.in_flight));
        mean_latency_us.push((with_chain(chain_id), chain.mean_latency_us));
    }
    families.extend(family(
        "arkflow_job_chain_batches_in_total",
        "Batches accepted into the chain.",
        MetricType::COUNTER,
        batches_in,
    ));
    families.extend(family(
        "arkflow_job_chain_batches_out_total",
        "Batches emitted downstream (or written by a sink chain).",
        MetricType::COUNTER,
        batches_out,
    ));
    families.extend(family(
        "arkflow_job_chain_rows_total",
        "Rows flowing through the chain.",
        MetricType::COUNTER,
        rows,
    ));
    families.extend(family(
        "arkflow_job_chain_errors_total",
        "Processing errors in the chain.",
        MetricType::COUNTER,
        errors,
    ));
    families.extend(family(
        "arkflow_job_chain_in_flight",
        "Envelopes currently parked on the chain's outbound edges (backlog).",
        MetricType::GAUGE,
        in_flight,
    ));
    families.extend(family(
        "arkflow_job_chain_mean_latency_us",
        "Mean per-batch processing time in microseconds.",
        MetricType::GAUGE,
        mean_latency_us,
    ));

    families.extend(family(
        "arkflow_job_checkpoint_duration_ms",
        "Last checkpoint duration in milliseconds.",
        MetricType::GAUGE,
        vec![(job_labels(), snapshot.checkpoint_duration_ms)],
    ));
    families.extend(family(
        "arkflow_job_checkpoint_failures_total",
        "Failed checkpoints.",
        MetricType::COUNTER,
        vec![(job_labels(), snapshot.checkpoint_failures)],
    ));
    families.extend(family(
        "arkflow_job_watermark_lag_ms",
        "Max watermark lag across sources in milliseconds.",
        MetricType::GAUGE,
        vec![(job_labels(), snapshot.watermark_lag_ms)],
    ));
    families.extend(family(
        "arkflow_job_late_events_total",
        "Late events dropped/routed/updated across event-time gates.",
        MetricType::COUNTER,
        vec![(job_labels(), snapshot.late_events)],
    ));
    families
}

/// Prometheus families for one Stream. Names, labels, and meanings of the
/// legacy `arkflow_stream_*` series are preserved; HELP/TYPE metadata is new.
pub fn stream_families(
    stream_id: &str,
    metrics: &StreamMetricsSnapshot,
    extra: &[Label],
) -> Vec<MetricFamily> {
    let labels = || {
        let mut all = extra.to_vec();
        all.push(("stream_id", stream_id.to_string()));
        all
    };
    let one = |value: u64| vec![(labels(), value)];
    let mut families = Vec::new();
    families.extend(family(
        "arkflow_stream_input_messages",
        "Cumulative input messages per Stream.",
        MetricType::COUNTER,
        one(metrics.input_messages),
    ));
    families.extend(family(
        "arkflow_stream_output_messages",
        "Cumulative output messages per Stream.",
        MetricType::COUNTER,
        one(metrics.output_messages),
    ));
    families.extend(family(
        "arkflow_stream_restarts",
        "Cumulative Stream restarts.",
        MetricType::COUNTER,
        one(metrics.restarts),
    ));
    families.extend(family(
        "arkflow_stream_in_flight",
        "Envelopes in flight across the Stream's kernel chains.",
        MetricType::GAUGE,
        one(metrics.in_flight),
    ));
    families.extend(family(
        "arkflow_stream_mean_latency_us",
        "Max mean per-batch processing time across kernel chains (microseconds).",
        MetricType::GAUGE,
        one(metrics.mean_latency_us),
    ));
    families.extend(family(
        "arkflow_stream_checkpoint_duration_ms",
        "Last checkpoint duration in milliseconds.",
        MetricType::GAUGE,
        one(metrics.checkpoint_duration_ms),
    ));
    families.extend(family(
        "arkflow_stream_checkpoint_failures",
        "Cumulative failed checkpoints.",
        MetricType::COUNTER,
        one(metrics.checkpoint_failures),
    ));
    families.extend(family(
        "arkflow_stream_watermark_lag_ms",
        "Max watermark lag across sources in milliseconds.",
        MetricType::GAUGE,
        one(metrics.watermark_lag_ms),
    ));
    families.extend(family(
        "arkflow_stream_late_events",
        "Cumulative late events across event-time gates.",
        MetricType::COUNTER,
        one(metrics.late_events),
    ));
    families
}

/// Render the full local data-plane exposition: legacy Stream series plus
/// every locally running kernel Job. Streams sort by id, jobs by id, and
/// chain series follow the snapshot's (sorted) chain map order.
pub fn data_plane_exposition(
    streams: &[(String, StreamMetricsSnapshot)],
    jobs: &BTreeMap<String, KernelMetricsSnapshot>,
) -> String {
    let mut families = Vec::new();
    for (stream_id, metrics) in streams {
        families.extend(stream_families(stream_id, metrics, &[]));
    }
    for (job_id, snapshot) in jobs {
        families.extend(kernel_job_families(job_id, snapshot, &[]));
    }
    encode_families(families)
}

/// Encode families as Prometheus text format 0.0.4 (HELP/TYPE included).
pub fn encode_families(families: Vec<MetricFamily>) -> String {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&families, &mut buffer)
        .expect("Prometheus text encoding cannot fail");
    String::from_utf8(buffer).expect("Prometheus text exposition is UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::executor::metrics::ChainMetricsSnapshot;
    use std::collections::BTreeMap;

    fn chain(batches_in: u64, errors: u64, in_flight: u64) -> ChainMetricsSnapshot {
        ChainMetricsSnapshot {
            batches_in,
            batches_out: batches_in,
            rows: batches_in * 3,
            errors,
            in_flight,
            mean_latency_us: 42,
        }
    }

    fn job_snapshot() -> KernelMetricsSnapshot {
        let mut chains = BTreeMap::new();
        chains.insert("src".to_string(), chain(7, 0, 2));
        chains.insert("sql".to_string(), chain(5, 3, 0));
        KernelMetricsSnapshot {
            chains,
            checkpoint_duration_ms: 120,
            checkpoint_failures: 1,
            watermark_lag_ms: 250,
            late_events: 4,
        }
    }

    fn exposition_text() -> String {
        let mut jobs = BTreeMap::new();
        jobs.insert("orders-job".to_string(), job_snapshot());
        data_plane_exposition(&[], &jobs)
    }

    #[test]
    fn counters_declared_with_total_names_and_gauges_without() {
        let text = exposition_text();
        assert!(text.contains("# TYPE arkflow_job_chain_batches_in_total counter"));
        assert!(text.contains("# TYPE arkflow_job_chain_errors_total counter"));
        assert!(text.contains("# TYPE arkflow_job_checkpoint_failures_total counter"));
        assert!(text.contains("# TYPE arkflow_job_late_events_total counter"));
        assert!(text.contains("# TYPE arkflow_job_chain_in_flight gauge"));
        assert!(text.contains("# TYPE arkflow_job_chain_mean_latency_us gauge"));
        assert!(text.contains("# TYPE arkflow_job_checkpoint_duration_ms gauge"));
        assert!(text.contains("# TYPE arkflow_job_watermark_lag_ms gauge"));
        for name in [
            "arkflow_job_chain_batches_in_total",
            "arkflow_job_chain_in_flight",
        ] {
            assert!(
                text.contains(&format!("# HELP {name} ")),
                "missing HELP for {name}"
            );
        }
    }

    #[test]
    fn exposition_is_parseable_and_values_match_the_snapshot() {
        let text = exposition_text();
        // Format 0.0.4 sample lines: name{labels} value. Label order follows
        // the vocabulary insertion order: extra labels, then job, then chain.
        assert!(
            text.contains(
                "arkflow_job_chain_batches_in_total{job=\"orders-job\",chain=\"src\"} 7"
            ),
            "unexpected exposition:\n{text}"
        );
        assert!(text.contains(
            "arkflow_job_chain_errors_total{job=\"orders-job\",chain=\"sql\"} 3"
        ));
        assert!(text.contains("arkflow_job_watermark_lag_ms{job=\"orders-job\"} 250"));
        assert!(text.contains("arkflow_job_checkpoint_duration_ms{job=\"orders-job\"} 120"));
        // Only the closed vocabulary appears as label names.
        for line in text.lines().filter(|line| !line.starts_with('#')) {
            let (name, rest) = line.split_once('{').expect("sample line with labels");
            assert!(name.starts_with("arkflow_"), "unexpected series {name}");
            for label in rest.trim_end_matches('}').split("\",") {
                let key = label.split('=').next().unwrap_or("").trim_matches('"');
                assert!(
                    matches!(key, "job" | "chain" | "stream_id" | "node" | ""),
                    "unexpected label `{key}` in line `{line}`"
                );
            }
        }
    }

    #[test]
    fn error_values_never_change_the_series_vocabulary() {
        let mut quiet = job_snapshot();
        quiet.chains.get_mut("sql").unwrap().errors = 0;
        let mut loud = job_snapshot();
        loud.chains.get_mut("sql").unwrap().errors = 99_999;

        let render = |snapshot: &KernelMetricsSnapshot| {
            let mut jobs = BTreeMap::new();
            jobs.insert("orders-job".to_string(), snapshot.clone());
            // Compare the series vocabulary (family + label sets), not values.
            data_plane_exposition(&[], &jobs)
                .lines()
                .filter(|line| !line.starts_with('#'))
                .map(|line| line.split(' ').next().unwrap_or(line).to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(render(&quiet), render(&loud));
    }

    #[test]
    fn empty_snapshots_produce_a_valid_but_empty_exposition() {
        let text = data_plane_exposition(&[], &BTreeMap::new());
        assert!(text.is_empty());
    }

    #[test]
    fn stream_families_preserve_legacy_names_and_labels() {
        let metrics = StreamMetricsSnapshot {
            input_batches: 1,
            input_messages: 11,
            processing_errors: 0,
            output_batches: 1,
            output_messages: 9,
            input_errors: 0,
            input_reconnects: 0,
            output_errors: 0,
            restarts: 2,
            kernel_chains: BTreeMap::new(),
            in_flight: 0,
            mean_latency_us: 15,
            checkpoint_duration_ms: 0,
            checkpoint_failures: 0,
            watermark_lag_ms: 0,
            late_events: 0,
        };
        let text = encode_families(stream_families("orders", &metrics, &[]));
        assert!(text.contains("arkflow_stream_input_messages{stream_id=\"orders\"} 11"));
        assert!(text.contains("arkflow_stream_output_messages{stream_id=\"orders\"} 9"));
        assert!(text.contains("arkflow_stream_restarts{stream_id=\"orders\"} 2"));
        assert!(text.contains("# TYPE arkflow_stream_input_messages counter"));
        assert!(text.contains("# TYPE arkflow_stream_in_flight gauge"));
    }

    #[test]
    fn hub_export_adds_a_node_label_in_front_of_the_vocabulary() {
        let text = encode_families(kernel_job_families(
            "orders-job",
            &job_snapshot(),
            &[("node", "node-a".to_string())],
        ));
        assert!(
            text.contains(
                "arkflow_job_chain_batches_in_total{node=\"node-a\",job=\"orders-job\",chain=\"src\"} 7"
            )
        );
    }
}
