//! L3 placement affinity with the real Kafka builders: the affinity key is
//! derived from configuration only (no broker), split placement pins the
//! transactional pair onto one node while the middle operator stays spread,
//! and a hand-split task map is rejected fail-closed.

use arkflow_core::job::{JobPlan, JobSpec};
use arkflow_core::Error;

fn l3_job() -> JobSpec {
    serde_json::from_value(serde_json::json!({
        "id": "l3-pair",
        "version": 1,
        "placement": "split",
        "parallelism": 2,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sql", "kind": "map"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [
            {"id": "e1", "from": "source", "to": "sql"},
            {"id": "e2", "from": "sql", "to": "sink"}
        ],
        "sources": [{
            "operator_id": "source",
            "input_type": "kafka",
            "config": {
                "brokers": ["localhost:9092"],
                "topics": ["in"],
                "consumer_group": "g1",
                "start_from_latest": false,
                "transactional_offsets": true
            },
            "time": {"mode": "processing_time"}
        }],
        "sinks": [{
            "operator_id": "sink",
            "output_type": "kafka",
            "config": {
                "brokers": ["localhost:9092"],
                "topic": {"type": "value", "value": "out"},
                "exactly_once": true,
                "transactional_id": "t1",
                "offset_commit_group": "g1"
            }
        }]
    }))
    .expect("valid L3 job spec")
}

fn node_of<'a>(assignments: &'a [arkflow_core::job::TaskAttempt], task_id: &str) -> &'a str {
    &assignments
        .iter()
        .find(|attempt| attempt.task_id == task_id)
        .unwrap_or_else(|| panic!("no assignment for {task_id}"))
        .node_id
}

#[test]
fn kafka_builders_pin_the_l3_pair_under_split_placement() {
    arkflow_plugin::initialize().expect("plugin registry");
    let plan = JobPlan::compile(l3_job()).expect("compiles");

    // The affinity derivation reads configuration only and never connects.
    let groups = plan.spec.affinity_groups().expect("affinity groups");
    assert_eq!(groups.len(), 1, "groups: {groups:?}");
    assert_eq!(groups[0].key, "kafka-txn:g1");

    // Without the pairing flags no group forms (the plain-job case).
    let mut plain = l3_job();
    plain.sources[0]
        .config
        .as_object_mut()
        .unwrap()
        .remove("transactional_offsets");
    plain.sinks[0]
        .config
        .as_object_mut()
        .unwrap()
        .remove("offset_commit_group");
    let plain = JobPlan::compile(plain).expect("compiles");
    assert!(plain.spec.affinity_groups().unwrap().is_empty());

    // Split round-robin would spread source-0..sink-1 as a/b/a/b/a/b; the
    // pin puts every pair member on the source's node while the map operator
    // keeps its two-subtask spread across both nodes.
    let nodes = vec!["a".to_string(), "b".to_string()];
    let assignments = plan.assignments_for_nodes(&nodes, 1).expect("placement");
    for task_id in ["source-0", "source-1", "sink-0", "sink-1"] {
        assert_eq!(node_of(&assignments, task_id), "a", "task {task_id}");
    }
    let sql_nodes: std::collections::BTreeSet<String> = assignments
        .iter()
        .filter(|attempt| attempt.task_id.starts_with("sql"))
        .map(|attempt| attempt.node_id.clone())
        .collect();
    assert_eq!(sql_nodes.len(), 2, "middle operator stays spread");
}

#[test]
fn a_hand_split_l3_pair_is_rejected_fail_closed() {
    arkflow_plugin::initialize().expect("plugin registry");
    let plan = JobPlan::compile(l3_job()).expect("compiles");
    let split = std::collections::BTreeMap::from([
        ("source-0".to_string(), "a".to_string()),
        ("source-1".to_string(), "a".to_string()),
        ("sql-0".to_string(), "a".to_string()),
        ("sql-1".to_string(), "b".to_string()),
        ("sink-0".to_string(), "b".to_string()),
        ("sink-1".to_string(), "b".to_string()),
    ]);
    let error = plan
        .validate_affinity_nodes(&split)
        .expect_err("a split L3 pair must be rejected");
    assert!(matches!(error, Error::Config(_)), "got: {error:?}");
    let message = error.to_string();
    assert!(
        message.contains("kafka-txn:g1"),
        "names the group: {message}"
    );
    assert!(message.contains("source-"), "names the tasks: {message}");
    assert!(message.contains("sink-"), "names the tasks: {message}");
}

#[test]
fn affinity_derivation_fails_closed_on_malformed_config() {
    arkflow_plugin::initialize().expect("plugin registry");
    // The hook parses the component's own config: a malformed value fails
    // placement before dispatch instead of silently dropping the pairing.
    let malformed = Some(serde_json::json!({"brokers": 42}));
    assert!(arkflow_core::input::placement_affinity("kafka", &malformed).is_err());
    // An output without configuration contributes no affinity.
    assert_eq!(
        arkflow_core::output::placement_affinity("kafka", &None).unwrap(),
        None
    );
    // A plain (non-L3) Kafka output contributes no affinity either.
    let plain = Some(serde_json::json!({
        "brokers": ["localhost:9092"],
        "topic": {"type": "value", "value": "out"}
    }));
    assert_eq!(
        arkflow_core::output::placement_affinity("kafka", &plain).unwrap(),
        None
    );
}
