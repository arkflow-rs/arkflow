//! L3 placement affinity with the real Kafka builders: the affinity key is
//! derived from configuration only (no broker), a parallel transactional
//! sink is rejected before placement (every instance would share one
//! transactional.id and fence its siblings), and a hand-split task map for
//! the single-instance shape is rejected fail-closed.

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

#[test]
fn kafka_builders_derive_the_l3_pair_group_and_reject_parallel_transactional_sinks() {
    arkflow_plugin::initialize().expect("plugin registry");
    // One transactional sink instance is the supported shape; the pair's
    // affinity key is derived from configuration only (no broker).
    let mut job = l3_job();
    job.parallelism = 1;
    let plan = JobPlan::compile(job).expect("compiles");
    let groups = plan.spec.affinity_groups().expect("affinity groups");
    assert_eq!(groups.len(), 1, "groups: {groups:?}");
    assert_eq!(groups[0].key, "kafka-txn:g1");

    // Without the pairing flags no group forms (the plain-job case).
    let mut plain = l3_job();
    plain.parallelism = 1;
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

    // Every parallel sink instance builds its own producer with the SAME
    // transactional.id, and Kafka fences the older producer. Until
    // per-subtask ids exist the multi-instance shape is rejected at compile
    // time instead of silently fencing the siblings at runtime.
    let error = JobPlan::compile(l3_job())
        .expect_err("parallelism 2 with one transactional_id must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("exactly_once") && message.contains("parallelism 1"),
        "explains the transactional fencing: {message}"
    );
}

#[test]
fn a_hand_split_l3_pair_is_rejected_fail_closed() {
    arkflow_plugin::initialize().expect("plugin registry");
    let mut job = l3_job();
    job.parallelism = 1;
    let plan = JobPlan::compile(job).expect("compiles");
    let split = std::collections::BTreeMap::from([
        ("source-0".to_string(), "a".to_string()),
        ("sql-0".to_string(), "a".to_string()),
        ("sink-0".to_string(), "b".to_string()),
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
    assert!(message.contains("source-0"), "names both tasks: {message}");
    assert!(message.contains("sink-0"), "names both tasks: {message}");
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
