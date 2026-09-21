//! StreamConfig → JobSpec compiler.
//!
//! Every legacy YAML stream compiles deterministically into a JobSpec executed
//! by the unified kernel: input → source operator, the linear processor chain
//! → fused Map chain operators (same processor configs), output → sink
//! operator. Window buffers become window operators in processing-time mode;
//! the error output becomes a side sink reachable from a filter chain; WAL
//! durability passes through to the built input unchanged (the WAL is a
//! property of the input, not the execution model).

use crate::job::{
    EdgeSpec, JobId, JobSpec, JobVersion, LateEventPolicy, OperatorKind, OperatorSpec, SinkSpec,
    SourceSpec, TimeMode, TimeSpec,
};
use crate::stream::StreamConfig;
use crate::Error;
use serde_json::json;

/// Key under which the compiled source/sink payloads carry the original
/// codec config (see `StreamJobAdapter::decode_codec`).
pub const CODEC_PAYLOAD_KEY: &str = "__stream_codec";

/// Compile one stream (by config index) into a JobSpec.
pub fn compile_stream(stream: &StreamConfig, index: usize) -> Result<JobSpec, Error> {
    let stream_id = stream
        .id
        .as_deref()
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("stream-{index}"));
    let job_id = JobId::new(sanitize_id(&stream_id)).map_err(|_| {
        Error::Config(format!(
            "stream id '{stream_id}' cannot map to a Job id; use letters, numbers, '-' or '_'"
        ))
    })?;

    let mut operators = Vec::new();
    let mut edges = Vec::new();
    let mut sources = Vec::new();
    let mut sinks = Vec::new();

    // Source operator: the stream's input config rides along in `config` so
    // the registry adapter can rebuild it verbatim (type + codec + settings).
    let source_operator_id = "source".to_string();
    operators.push(OperatorSpec {
        id: source_operator_id.clone(),
        kind: OperatorKind::Source,
        stateful: false,
        key_field: None,
        // Preserve `pipeline.thread_num` as bounded chain-level processor
        // worker concurrency (the legacy Stream contract) without changing
        // the source partition topology: the value rides the source config
        // and the graph builder copies it onto the source chain.
        config: json!({
            "__arkflow_processor_parallelism": stream.pipeline.thread_num.max(1),
        }),
    });
    sources.push(SourceSpec {
        operator_id: source_operator_id.clone(),
        input_type: stream.input.input_type.clone(),
        config: input_config_payload(stream),
        time: TimeSpec {
            mode: TimeMode::ProcessingTime,
            timestamp_field: None,
            watermark: None,
            allowed_lateness_ms: 0,
            late_event_policy: LateEventPolicy::Drop,
            late_event_route: None,
        },
    });

    // A legacy window emits a batch before the configured pipeline processors
    // see it. Keep that established order in the compiled graph so processors
    // transform the concatenated payload, rather than changing what is
    // grouped and when it is emitted.
    let mut upstream = source_operator_id.clone();
    let mut error_source_operator_ids = Vec::new();
    if let Some(buffer) = &stream.buffer {
        let buffer_type = buffer.buffer_type.as_str();
        match buffer_type {
            "memory" => { /* no-op: bounded channels already provide buffering */ }
            "tumbling_window" | "session_window" => {
                let window_config = window_config(buffer_type, buffer)?;
                let operator_id = "window".to_string();
                operators.push(OperatorSpec {
                    id: operator_id.clone(),
                    kind: OperatorKind::Window,
                    stateful: true,
                    key_field: Some(
                        window_config
                            .get("key_field")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("__arkflow_window_all")
                            .to_string(),
                    ),
                    config: window_config,
                });
                edges.push(EdgeSpec {
                    id: format!("edge-{upstream}-{operator_id}"),
                    from: upstream.clone(),
                    to: operator_id.clone(),
                    partitioned: false,
                });
                upstream = operator_id.clone();
                error_source_operator_ids.push(operator_id);
            }
            "sliding_window" => {
                return Err(Error::Config(
                    "legacy sliding_window uses row-count window_size/slide_size and cannot be \
                     represented by the event-time window operator; declare a Job window or use \
                     an explicit processor for row-count batching"
                        .into(),
                ));
            }
            "join" => {
                return Err(Error::Config(
                    "buffer type 'join' cannot compile to the unified kernel; declare a Job DAG \
                     with an explicit join operator instead (see the Job API documentation)"
                        .into(),
                ));
            }
            other => {
                return Err(Error::Config(format!(
                    "unknown buffer type '{other}' while compiling stream '{stream_id}'"
                )));
            }
        }
    }

    // Processor chain: one operator per configured processor, wired linearly.
    // Stateful window buffers already sit upstream, preserving legacy
    // buffer-before-processor semantics.
    for (position, processor) in stream.pipeline.processors.iter().enumerate() {
        let operator_id = format!("p{position}-{}", sanitize_id(&processor.processor_type));
        operators.push(OperatorSpec {
            id: operator_id.clone(),
            kind: OperatorKind::Map,
            stateful: false,
            key_field: None,
            config: processor_config_payload(processor),
        });
        edges.push(EdgeSpec {
            id: format!("edge-{upstream}-{operator_id}"),
            from: upstream.clone(),
            to: operator_id.clone(),
            partitioned: false,
        });
        upstream = operator_id;
        error_source_operator_ids.push(upstream.clone());
    }

    // Sink operator: the stream's output config rides in `config`.
    let sink_operator_id = "sink".to_string();
    operators.push(OperatorSpec {
        id: sink_operator_id.clone(),
        kind: OperatorKind::Sink,
        stateful: false,
        key_field: None,
        config: json!({}),
    });
    sinks.push(SinkSpec {
        operator_id: sink_operator_id.clone(),
        output_type: stream.output.output_type.clone(),
        config: output_config_payload(&stream.output),
    });
    edges.push(EdgeSpec {
        id: format!("edge-{upstream}-{sink_operator_id}"),
        from: upstream.clone(),
        to: sink_operator_id,
        partitioned: false,
    });

    // Error output: an additional sink operator routes failures from every
    // relevant processor/window chain. This keeps failures before a window or
    // in an unfused processor chain on the configured side output.
    if let Some(error_output) = &stream.error_output {
        let error_operator_id = "error-sink".to_string();
        operators.push(OperatorSpec {
            id: error_operator_id.clone(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            // The graph keeps this edge separate from successful data. The
            // marker is internal JobSpec metadata and is not sent to the
            // output builder.
            config: json!({"__arkflow_error_sink": true}),
        });
        let error_sources = if error_source_operator_ids.is_empty() {
            vec![upstream.clone()]
        } else {
            error_source_operator_ids
        };
        for error_source in error_sources {
            edges.push(EdgeSpec {
                id: format!("edge-{error_source}-{error_operator_id}"),
                from: error_source,
                to: error_operator_id.clone(),
                partitioned: false,
            });
        }
        sinks.push(SinkSpec {
            operator_id: error_operator_id,
            output_type: error_output.output_type.clone(),
            config: output_config_payload(error_output),
        });
    }

    let spec = JobSpec {
        rebalance: None,
        id: job_id,
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators,
        edges,
        sources,
        sinks,
        // Legacy streams must opt into a state contract when they contain a
        // window. A stream-level state section is copied into the compiled
        // Job so the same validation rules apply to both configuration forms.
        state: stream.state.clone(),
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    };
    spec.validate()?;
    Ok(spec)
}

/// Compile every stream in an engine config.
pub fn compile_engine_streams(config: &crate::config::EngineConfig) -> Result<Vec<JobSpec>, Error> {
    config
        .streams
        .iter()
        .enumerate()
        .map(|(index, stream)| compile_stream(stream, index))
        .collect()
}

fn sanitize_id(raw: &str) -> String {
    raw.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

/// The input's full config (type + codec + settings) as the source payload;
/// the registry adapter unwraps it back into an `InputConfig`.
fn input_config_payload(stream: &StreamConfig) -> serde_json::Value {
    let mut payload = stream.input.config.clone().unwrap_or(json!({}));
    if let Some(object) = payload.as_object_mut() {
        if let Some(name) = &stream.input.name {
            object.insert("name".into(), json!(name));
        }
        if let Some(codec) = &stream.input.codec {
            object.insert(
                "__stream_codec".into(),
                serde_json::to_value(codec).unwrap_or_default(),
            );
        }
    }
    payload
}

fn output_config_payload(output: &crate::output::OutputConfig) -> serde_json::Value {
    let mut payload = output.config.clone().unwrap_or(json!({}));
    if let Some(object) = payload.as_object_mut() {
        if let Some(name) = &output.name {
            object.insert("name".into(), json!(name));
        }
        if let Some(codec) = &output.codec {
            object.insert(
                "__stream_codec".into(),
                serde_json::to_value(codec).unwrap_or_default(),
            );
        }
    }
    payload
}

fn processor_config_payload(processor: &crate::processor::ProcessorConfig) -> serde_json::Value {
    let mut payload = json!({ "type": processor.processor_type });
    if let Some(config) = &processor.config {
        if let (Some(target), Some(source)) = (payload.as_object_mut(), config.as_object()) {
            for (key, value) in source {
                target.insert(key.clone(), value.clone());
            }
        }
    }
    if let Some(name) = &processor.name {
        if let Some(object) = payload.as_object_mut() {
            object.insert("name".into(), json!(name));
        }
    }
    payload
}

/// Map a legacy buffer config to a window operator config
/// (processing-time mode to preserve the batching semantics).
fn window_config(
    buffer_type: &str,
    buffer: &crate::buffer::BufferConfig,
) -> Result<serde_json::Value, Error> {
    let config = buffer.config.clone().unwrap_or(json!({}));
    if matches!(buffer_type, "tumbling_window" | "session_window") && config.get("join").is_some() {
        return Err(Error::Config(
            "legacy window buffers with 'join' cannot compile to the unified kernel; declare a Job \
             DAG with an explicit join operator instead (see the Job API documentation)"
                .into(),
        ));
    }
    let get_duration_ms = |field: &str| -> Result<i64, Error> {
        let value = config
            .get(field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Error::Config(format!(
                    "buffer '{buffer_type}' requires a '{field}' duration (e.g. \"1s\")"
                ))
            })?;
        parse_duration_ms(value).ok_or_else(|| {
            Error::Config(format!(
                "buffer '{buffer_type}' field '{field}' has an unsupported duration '{value}': use a duration like '500ms', '1s', '2m', '1h', '500us', or a compound '1h30m'"
            ))
        })
    };
    // Legacy buffer field names: tumbling keys on `interval`, session on
    // `gap`. Sliding buffers are row-count based (`window_size` and
    // `slide_size`) and are rejected explicitly by the caller because a
    // time-window operator cannot preserve those cardinality semantics.
    let interval_ms = |field: &str| -> Result<i64, Error> {
        config
            .get(field)
            .and_then(serde_json::Value::as_str)
            .and_then(parse_duration_ms)
            .ok_or_else(|| {
                Error::Config(format!(
                    "buffer '{buffer_type}' requires a '{field}' duration (e.g. \"1s\")"
                ))
            })
    };
    let kind = match buffer_type {
        "tumbling_window" => {
            json!({"kind": "tumbling", "size_ms": interval_ms("interval").or_else(|_| get_duration_ms("size"))?})
        }
        "sliding_window" => {
            return Err(Error::Config(
                "legacy sliding_window uses row-count window_size/slide_size and cannot be \
                 represented by the event-time window operator; declare a Job window or use an \
                 explicit processor for row-count batching"
                    .into(),
            ));
        }
        "session_window" => {
            json!({"kind": "session", "gap_ms": interval_ms("gap").or_else(|_| get_duration_ms("gap"))?})
        }
        _ => unreachable!("caller matched window buffer types"),
    };
    let timestamp_field = config
        .get("time_field")
        .or_else(|| config.get("timestamp_field"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("__meta_timestamp")
        .to_string();
    let key_field = config
        .get("key_field")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("__arkflow_window_all")
        .to_string();
    let value_fields = config
        .get("aggregate_fields")
        .or_else(|| config.get("value_fields"))
        .and_then(serde_json::Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| field.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut window = kind;
    if let Some(object) = window.as_object_mut() {
        let trigger_interval_ms = object
            .get("size_ms")
            .or_else(|| object.get("gap_ms"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(5_000);
        object.insert("type".into(), json!("window"));
        object.insert("timestamp_field".into(), json!(timestamp_field));
        object.insert("key_field".into(), json!(key_field));
        object.insert("value_fields".into(), json!(value_fields));
        object.insert("trigger".into(), json!("processing_time"));
        object.insert(
            "trigger_interval_ms".into(),
            json!(trigger_interval_ms.max(1)),
        );
        if matches!(buffer_type, "tumbling_window" | "session_window") {
            object.insert("legacy_payload".into(), json!(true));
        }
    }
    Ok(window)
}

/// Parse a human duration string to milliseconds. Accepts the legacy
/// `humantime` grammar the retired Stream runtime used — compound forms such
/// as `1h30m` and sub-millisecond units such as `500us`/`100ns` — in addition
/// to the plain `500ms`/`1s`/`2m`/`1h` forms, so migrated configs keep
/// compiling with identical values.
fn parse_duration_ms(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if let Ok(duration) = humantime::parse_duration(raw) {
        // Sub-millisecond durations truncate toward zero, matching the
        // legacy runtime's millisecond resolution.
        let millis = i64::try_from(duration.as_millis()).ok()?;
        return Some(millis);
    }
    let (value, unit) =
        raw.split_at(raw.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-')?);
    let value: f64 = value.parse().ok()?;
    let multiplier = match unit.trim() {
        "ms" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hour" | "hours" => 3_600_000.0,
        _ => return None,
    };
    Some((value * multiplier) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_stream() -> StreamConfig {
        StreamConfig {
            id: Some("orders".into()),
            input: crate::input::InputConfig {
                input_type: "generate".into(),
                name: None,
                codec: None,
                config: Some(json!({"interval": "1s"})),
            },
            pipeline: crate::pipeline::PipelineConfig {
                thread_num: 1,
                processors: vec![crate::processor::ProcessorConfig {
                    processor_type: "sql".into(),
                    name: None,
                    config: Some(json!({"query": "SELECT 1"})),
                }],
            },
            output: crate::output::OutputConfig {
                output_type: "stdout".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            state: None,
            temporary: None,
        }
    }

    #[test]
    fn compiles_linear_pipeline_deterministically() {
        let stream = minimal_stream();
        let first = compile_stream(&stream, 0).unwrap();
        let second = compile_stream(&stream, 0).unwrap();
        assert_eq!(first, second);
        // source → p0-sql → sink
        assert_eq!(first.operators.len(), 3);
        assert_eq!(first.edges.len(), 2);
        assert_eq!(first.sources[0].input_type, "generate");
        assert_eq!(first.sinks[0].output_type, "stdout");
        assert!(first.operators.iter().any(|op| op.id == "p0-sql"));
        // Processor config preserved verbatim.
        let sql = first.operators.iter().find(|op| op.id == "p0-sql").unwrap();
        assert_eq!(sql.config.get("type").unwrap(), "sql");
        assert_eq!(sql.config.get("query").unwrap(), "SELECT 1");
    }

    #[test]
    fn tumbling_buffer_becomes_processing_time_window_operator() {
        let mut stream = minimal_stream();
        stream.buffer = Some(crate::buffer::BufferConfig {
            buffer_type: "tumbling_window".into(),
            name: None,
            config: Some(json!({"size": "1m"})),
        });
        stream.state = Some(crate::job::StateSpec {
            backend: "embedded_kv".into(),
            durability: crate::job::StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        let spec = compile_stream(&stream, 0).unwrap();
        let window = spec
            .operators
            .iter()
            .find(|op| op.kind == OperatorKind::Window)
            .expect("window operator expected");
        assert!(window.stateful);
        assert_eq!(window.config.get("trigger").unwrap(), "processing_time");
        assert_eq!(window.config.get("size_ms").unwrap(), 60_000);
    }

    #[test]
    fn join_buffer_rejected_with_migration_message() {
        let mut stream = minimal_stream();
        stream.buffer = Some(crate::buffer::BufferConfig {
            buffer_type: "join".into(),
            name: None,
            config: Some(json!({})),
        });
        let error = compile_stream(&stream, 0).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("join"), "{message}");
        assert!(message.contains("Job"), "{message}");
    }

    #[test]
    fn memory_buffer_is_noop() {
        let mut stream = minimal_stream();
        stream.buffer = Some(crate::buffer::BufferConfig {
            buffer_type: "memory".into(),
            name: None,
            config: Some(json!({"capacity": 128})),
        });
        let spec = compile_stream(&stream, 0).unwrap();
        assert!(!spec
            .operators
            .iter()
            .any(|op| op.kind == OperatorKind::Window));
    }

    #[test]
    fn error_output_compiles_to_extra_sink() {
        let mut stream = minimal_stream();
        stream.error_output = Some(crate::output::OutputConfig {
            output_type: "kafka".into(),
            name: None,
            codec: None,
            config: Some(json!({"topic": "dead-letter"})),
        });
        let spec = compile_stream(&stream, 0).unwrap();
        assert_eq!(spec.sinks.len(), 2);
        assert_eq!(spec.sinks[1].output_type, "kafka");
    }

    #[test]
    fn stream_ids_sanitize_into_valid_job_ids() {
        // Invalid characters map to '-', keeping the compiled Job id valid.
        let mut stream = minimal_stream();
        stream.id = Some("my stream!".into());
        let spec = compile_stream(&stream, 0).unwrap();
        assert_eq!(spec.id.as_str(), "my-stream-");
        // An empty id falls back to the deterministic index id (same rule as
        // effective_id), so it still compiles.
        stream.id = Some("".into());
        let spec = compile_stream(&stream, 3).unwrap();
        assert_eq!(spec.id.as_str(), "stream-3");
    }

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration_ms("500ms"), Some(500));
        assert_eq!(parse_duration_ms("1s"), Some(1_000));
        assert_eq!(parse_duration_ms("2m"), Some(120_000));
        assert_eq!(parse_duration_ms("1h"), Some(3_600_000));
        // The legacy `humantime` grammar keeps migrated configs compiling.
        assert_eq!(parse_duration_ms("500us"), Some(0));
        assert_eq!(parse_duration_ms("1500us"), Some(1));
        assert_eq!(parse_duration_ms("1h30m"), Some(5_400_000));
        assert_eq!(parse_duration_ms("1m30s"), Some(90_000));
        assert_eq!(parse_duration_ms("bogus"), None);
    }
}

#[cfg(test)]
mod window_mapping_tests {
    use super::*;

    fn window_stream(buffer_type: &str, buffer_config: serde_json::Value) -> JobSpec {
        let stream = StreamConfig {
            id: Some("w".into()),
            input: crate::input::InputConfig {
                input_type: "generate".into(),
                name: None,
                codec: None,
                config: Some(json!({"count": 1})),
            },
            pipeline: crate::pipeline::PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: crate::output::OutputConfig {
                output_type: "drop".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: Some(crate::buffer::BufferConfig {
                buffer_type: buffer_type.into(),
                name: None,
                config: Some(buffer_config),
            }),
            durability: None,
            state: Some(crate::job::StateSpec {
                backend: "embedded_kv".into(),
                durability: crate::job::StateDurability::Ephemeral,
                root: None,
                namespace: None,
                ttl_ms: None,
                format_version: 1,
                max_pending_transactions: None,
                max_bytes: None,
            }),
            temporary: None,
        };
        compile_stream(&stream, 0).unwrap()
    }

    #[test]
    fn tumbling_uses_legacy_interval_field() {
        let spec = window_stream("tumbling_window", json!({"interval": "1m"}));
        let window = spec
            .operators
            .iter()
            .find(|op| op.kind == OperatorKind::Window)
            .unwrap();
        assert_eq!(window.config.get("kind").unwrap(), "tumbling");
        assert_eq!(window.config.get("size_ms").unwrap(), 60_000);
    }

    #[test]
    fn sliding_row_count_configuration_is_rejected_explicitly() {
        let error = compile_stream(
            &StreamConfig {
                id: Some("w".into()),
                input: crate::input::InputConfig {
                    input_type: "generate".into(),
                    name: None,
                    codec: None,
                    config: Some(json!({"count": 1})),
                },
                pipeline: crate::pipeline::PipelineConfig {
                    thread_num: 1,
                    processors: vec![],
                },
                output: crate::output::OutputConfig {
                    output_type: "drop".into(),
                    name: None,
                    codec: None,
                    config: None,
                },
                error_output: None,
                buffer: Some(crate::buffer::BufferConfig {
                    buffer_type: "sliding_window".into(),
                    name: None,
                    config: Some(json!({"interval": "30s", "slide_size": 5})),
                }),
                durability: None,
                state: None,
                temporary: None,
            },
            0,
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("row-count"), "{message}");
        assert!(message.contains("Job"), "{message}");
    }

    #[test]
    fn sliding_config_with_row_count_is_not_reinterpreted_as_time() {
        let error = compile_stream(
            &StreamConfig {
                id: Some("w".into()),
                input: crate::input::InputConfig {
                    input_type: "generate".into(),
                    name: None,
                    codec: None,
                    config: Some(json!({"count": 1})),
                },
                pipeline: crate::pipeline::PipelineConfig {
                    thread_num: 1,
                    processors: vec![],
                },
                output: crate::output::OutputConfig {
                    output_type: "drop".into(),
                    name: None,
                    codec: None,
                    config: None,
                },
                error_output: None,
                buffer: Some(crate::buffer::BufferConfig {
                    buffer_type: "sliding_window".into(),
                    name: None,
                    config: Some(json!({
                        "window_size": 100,
                        "interval": "5s",
                        "slide_size": 10
                    })),
                }),
                durability: None,
                state: None,
                temporary: None,
            },
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("row-count"));
    }

    #[test]
    fn session_maps_gap() {
        let spec = window_stream("session_window", json!({"gap": "2s"}));
        let window = spec
            .operators
            .iter()
            .find(|op| op.kind == OperatorKind::Window)
            .unwrap();
        assert_eq!(window.config.get("kind").unwrap(), "session");
        assert_eq!(window.config.get("gap_ms").unwrap(), 2_000);
    }
}
