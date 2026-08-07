//! SQL-first Job validation and physical-plan explanation.

use crate::job::{JobPlan, JobSpec};
use crate::Error;
use datafusion::optimizer::OptimizerConfig;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustExtensionSpec {
    pub name: String,
    #[serde(default = "default_true")]
    pub deterministic: bool,
    #[serde(default)]
    pub stateful: bool,
    #[serde(default)]
    pub keyed: bool,
    #[serde(default)]
    pub asynchronous: bool,
    #[serde(default = "default_true")]
    pub checkpoint_compatible: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledStreamingJob {
    pub sql: String,
    pub plan: JobPlan,
    pub extensions: Vec<RustExtensionSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingDdl {
    pub sql: String,
    pub source: String,
    pub sink: String,
    pub key: String,
    pub timestamp_field: String,
    pub watermark: crate::job::WatermarkSpec,
    pub window: Option<serde_json::Value>,
    pub recovery: crate::job::RecoveryPolicy,
}

impl CompiledStreamingJob {
    pub fn explain(&self) -> serde_json::Value {
        serde_json::json!({
            "job_id": self.plan.spec.id,
            "version": self.plan.spec.version,
            "parallelism": self.plan.spec.parallelism,
            "max_parallelism": self.plan.spec.max_parallelism,
            "operators": self.plan.spec.operators,
            "edges": self.plan.spec.edges,
            "tasks": self.plan.tasks,
            "sources": self.plan.spec.sources,
            "sinks": self.plan.spec.sinks,
            "state": self.plan.spec.state,
            "checkpoint": self.plan.spec.checkpoint,
            "extensions": self.extensions,
        })
    }
}

pub struct StreamingSqlCompiler;

impl StreamingSqlCompiler {
    pub fn compile_ddl(
        ddl: StreamingDdl,
        spec: JobSpec,
        extensions: Vec<RustExtensionSpec>,
    ) -> Result<CompiledStreamingJob, Error> {
        for field in [&ddl.source, &ddl.sink, &ddl.key, &ddl.timestamp_field] {
            if field.trim().is_empty() {
                return Err(Error::Config(
                    "streaming DDL connector/key/timestamp fields are required".into(),
                ));
            }
        }
        let mut spec = spec;
        spec.recovery = ddl.recovery;
        Self::compile(ddl.sql, spec, extensions)
    }

    pub fn compile(
        sql: impl Into<String>,
        mut spec: JobSpec,
        extensions: Vec<RustExtensionSpec>,
    ) -> Result<CompiledStreamingJob, Error> {
        let sql = sql.into();
        let query = extract_query(&sql)?;
        let context = SessionContext::new();
        context
            .state()
            .sql_to_statement(&query, &context.state().options().sql_parser.dialect)
            .map_err(|error| Error::Config(format!("streaming SQL validation failed: {error}")))?;
        for extension in &extensions {
            if extension.name.trim().is_empty() {
                return Err(Error::Config("Rust extension name is required".into()));
            }
            if extension.stateful && !extension.keyed {
                return Err(Error::Config(format!(
                    "stateful Rust extension '{}' must declare keyed=true",
                    extension.name
                )));
            }
            if extension.stateful && !extension.checkpoint_compatible {
                return Err(Error::Config(format!(
                    "stateful Rust extension '{}' is not checkpoint-compatible",
                    extension.name
                )));
            }
        }
        // The SQL adapter's minimal compatibility spec may omit physical
        // edges.  Materialize the declared source-to-sink routes before the
        // stricter Job graph validation runs.
        if spec.edges.is_empty() {
            spec.edges = spec
                .sources
                .iter()
                .flat_map(|source| {
                    spec.sinks.iter().map(move |sink| crate::job::EdgeSpec {
                        id: format!("{}-to-{}", source.operator_id, sink.operator_id),
                        from: source.operator_id.clone(),
                        to: sink.operator_id.clone(),
                        partitioned: false,
                    })
                })
                .collect();
        }
        Ok(CompiledStreamingJob {
            sql,
            plan: JobPlan::compile(spec)?,
            extensions,
        })
    }
}

fn extract_query(sql: &str) -> Result<String, Error> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(Error::Config("streaming SQL must not be empty".into()));
    }
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("CREATE STREAM") {
        let prefix_len = "CREATE STREAM".len();
        let declaration = &trimmed[prefix_len..];
        let declaration_upper = &upper[prefix_len..];
        if let Some(relative_index) = find_sql_keyword(declaration_upper, "AS") {
            let query = declaration[relative_index + 2..].trim();
            if !query.is_empty() {
                return Ok(query.trim_end_matches(';').to_owned());
            }
            return Err(Error::Config(
                "CREATE STREAM must include a query after AS".into(),
            ));
        }
        return Err(Error::Config(
            "CREATE STREAM must include an AS query wrapper".into(),
        ));
    }
    Ok(trimmed.trim_end_matches(';').to_owned())
}

fn find_sql_keyword(sql: &str, keyword: &str) -> Option<usize> {
    sql.match_indices(keyword).find_map(|(index, _)| {
        let before_is_boundary = sql[..index]
            .chars()
            .next_back()
            .is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_');
        let end = index + keyword.len();
        let after_is_boundary = sql[end..]
            .chars()
            .next()
            .is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_');
        (before_is_boundary && after_is_boundary).then_some(index)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{
        JobId, JobVersion, OperatorKind, OperatorSpec, SinkSpec, SourceSpec, TimeMode, TimeSpec,
    };

    fn spec() -> JobSpec {
        JobSpec {
            id: JobId::new("orders").unwrap(),
            version: JobVersion(1),
            max_parallelism: 4,
            parallelism: 1,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
                OperatorSpec {
                    id: "sink".into(),
                    kind: OperatorKind::Sink,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
            ],
            sources: vec![SourceSpec {
                operator_id: "source".into(),
                input_type: "memory".into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "drop".into(),
                config: serde_json::json!({}),
            }],
            edges: vec![],
            state: None,
            checkpoint: None,
            recovery: Default::default(),
        }
    }

    #[test]
    fn compiles_select_and_exposes_plan() {
        let compiled = StreamingSqlCompiler::compile(
            "CREATE STREAM orders AS SELECT * FROM flow",
            spec(),
            vec![],
        )
        .unwrap();
        assert_eq!(compiled.explain()["job_id"], "orders");
        assert_eq!(compiled.plan.tasks.len(), 2);
    }

    #[test]
    fn preserves_aliases_inside_create_stream_query() {
        let compiled = StreamingSqlCompiler::compile(
            "CREATE STREAM orders AS SELECT amount AS total FROM flow",
            spec(),
            vec![],
        )
        .unwrap();
        assert_eq!(
            compiled.sql,
            "CREATE STREAM orders AS SELECT amount AS total FROM flow"
        );
    }

    #[test]
    fn rejects_non_checkpointable_stateful_extension() {
        let result = StreamingSqlCompiler::compile(
            "SELECT * FROM flow",
            spec(),
            vec![RustExtensionSpec {
                name: "enrich".into(),
                deterministic: true,
                stateful: true,
                keyed: true,
                asynchronous: false,
                checkpoint_compatible: false,
            }],
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("checkpoint-compatible"));
    }
}
