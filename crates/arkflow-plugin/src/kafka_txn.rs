//! Process-level bridge for Kafka L3 exactly-once: the input side publishes
//! its live consumer-group metadata (and suppresses its own broker offset
//! stores) while the transactional output commits source offsets inside its
//! producer transaction via `send_offsets_to_transaction`.
//!
//! The registry is keyed by consumer group id: the output declares
//! `offset_commit_group` naming the input whose offsets ride its
//! transactions. Entries are weak so a dropped input's group metadata does
//! not leak.
use rdkafka::consumer::ConsumerGroupMetadata;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::RwLock;

pub(crate) type SharedMetadata = Arc<RwLock<Option<Arc<ConsumerGroupMetadata>>>>;
pub(crate) type WeakShared = Weak<RwLock<Option<Arc<ConsumerGroupMetadata>>>>;

#[derive(Clone)]
pub(crate) struct GroupRegistration {
    pub(crate) metadata: WeakShared,
    /// Subscribed topics of the input; the transactional offset commit maps
    /// batch partitions back to topics through this list. L3 supports
    /// single-topic inputs: a partition alone cannot name its topic in the
    /// batch metadata.
    pub(crate) topics: Vec<String>,
}

fn registry() -> &'static std::sync::Mutex<BTreeMap<String, GroupRegistration>> {
    static REGISTRY: OnceLock<std::sync::Mutex<BTreeMap<String, GroupRegistration>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

/// Register (or re-register after a reconnect) the handle slot for one
/// consumer group. Returns the shared slot the input keeps filling with its
/// live group metadata.
pub(crate) fn register_group(group_id: &str, topics: Vec<String>) -> SharedMetadata {
    let slot = Arc::new(RwLock::new(None));
    registry()
        .lock()
        .expect("kafka txn registry lock")
        .insert(
            group_id.to_owned(),
            GroupRegistration {
                metadata: Arc::downgrade(&slot),
                topics,
            },
        );
    slot
}

/// Look up the live group metadata for a consumer group, if its input is
/// still running in this process.
pub(crate) async fn group_metadata(
    group_id: &str,
) -> Option<Arc<ConsumerGroupMetadata>> {
    let weak = registry()
        .lock()
        .expect("kafka txn registry lock")
        .get(group_id)?
        .metadata
        .clone();
    // Weak upgrade: a dropped input's registration dies with it — the
    // output then fails closed with the explicit "no live input" error
    // instead of committing against a dead consumer's group metadata.
    let shared = weak.upgrade()?;
    let metadata = shared.read().await.clone();
    metadata
}

/// The single subscribed topic of a group, when the input declares exactly
/// one. Transactional offset commits route partitions through it.
pub(crate) fn single_topic(group_id: &str) -> Option<String> {
    let registration = registry()
        .lock()
        .expect("kafka txn registry lock")
        .get(group_id)
        .cloned()?;
    match registration.topics.as_slice() {
        [topic] => Some(topic.clone()),
        _ => None,
    }
}
