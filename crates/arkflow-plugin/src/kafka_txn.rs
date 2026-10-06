//! Process-level bridge for Kafka L3 exactly-once: the input side publishes
//! its live consumer-group metadata (and suppresses its own broker offset
//! stores) while the transactional output commits source offsets inside its
//! producer transaction via `send_offsets_to_transaction`.
//!
//! The registry is keyed by consumer group id: the output declares
//! `offset_commit_group` naming the input whose offsets ride its
//! transactions. Entries are weak so a dropped input's group metadata does
//! not leak.
//!
//! Each entry also carries the input's [`CommitFrontier`] — the SAME
//! instance its `KafkaAck::ack` advances — so the output can clamp its
//! transactional offset commits to the contiguous acknowledged frontier
//! (never skipping records still in flight elsewhere in the graph).
//!
//! Pairing is validated fail-closed at startup: every group declared by an
//! input with `transactional_offsets` must be claimed by at least one
//! output's `offset_commit_group`, otherwise the misconfiguration would
//! silently leave the group's broker offsets uncommitted (a crash then
//! restarts per `auto.offset.reset`, skipping everything under `latest`).
use arkflow_core::executor::commit::CommitFrontier;
use arkflow_core::Error;
use rdkafka::consumer::ConsumerGroupMetadata;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
    /// The paired input's acknowledged-position frontier — the same instance
    /// its acks advance. The output clamps every transactional offset commit
    /// to `min(batch max next, frontier next)` per partition. Weak: the
    /// frontier dies with its input.
    pub(crate) frontier: Weak<CommitFrontier>,
}

fn registry() -> &'static std::sync::Mutex<BTreeMap<String, GroupRegistration>> {
    static REGISTRY: OnceLock<std::sync::Mutex<BTreeMap<String, GroupRegistration>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

/// Consumer groups claimed by some Kafka output's `offset_commit_group`.
/// Declared at output build time; consulted by the startup pairing
/// validation. Unlike the group registry the claim is only a set entry: it
/// must survive the config dry builds (whose output instances are dropped
/// immediately), so it cannot be weakly tied to an instance. A claim
/// therefore also outlives the output that declared it — the validation is
/// a startup gate for declared configuration, liveness stays a write-time
/// check.
fn committers() -> &'static std::sync::Mutex<BTreeSet<String>> {
    static COMMITTERS: OnceLock<std::sync::Mutex<BTreeSet<String>>> = OnceLock::new();
    COMMITTERS.get_or_init(|| std::sync::Mutex::new(BTreeSet::new()))
}

/// Lazily install the pairing validator in the core's startup validation
/// hook. Done from the declaration paths (`register_group`,
/// `declare_offset_committer`) rather than plugin `init()` so every entry
/// point that can build an L3 component (engine, server, tests) wires the
/// validation before the first graph connect.
fn ensure_pairing_validator_registered() {
    static VALIDATOR: OnceLock<Arc<dyn Fn() -> Result<(), Error> + Send + Sync>> = OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| Arc::new(validate_pairings));
    arkflow_core::executor::resource_guard::register_startup_validator(validator.clone());
}

/// Register (or re-register after a rebuild) the handle slot for one
/// consumer group. `frontier` must be the SAME instance the input's acks
/// advance (the output clamps against it). Returns the shared slot the
/// input keeps filling with its live group metadata.
pub(crate) fn register_group(
    group_id: &str,
    topics: Vec<String>,
    frontier: Arc<CommitFrontier>,
) -> SharedMetadata {
    ensure_pairing_validator_registered();
    // A second input registering the same consumer group silently replaces
    // the first's metadata/frontier slot — its partitions then never reach
    // the output's clamp frontier (safe direction: replay), but that is a
    // silent degradation, so warn loudly instead.
    if registry()
        .lock()
        .expect("kafka txn registry lock")
        .contains_key(group_id)
    {
        tracing::warn!(
            group = group_id,
            "a second Kafka input registered consumer group; the previous \
             registration's frontier is replaced and its offsets will not be \
             transactionally committed"
        );
    }
    let slot = Arc::new(RwLock::new(None));
    registry().lock().expect("kafka txn registry lock").insert(
        group_id.to_owned(),
        GroupRegistration {
            metadata: Arc::downgrade(&slot),
            topics,
            frontier: Arc::downgrade(&frontier),
        },
    );
    slot
}

/// Look up the live group metadata for a consumer group, if its input is
/// still running in this process.
pub(crate) async fn group_metadata(group_id: &str) -> Option<Arc<ConsumerGroupMetadata>> {
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

/// The live frontier of a consumer group's paired input, if the input is
/// still running in this process. The transactional output snapshots the
/// contiguous acknowledged positions from this instance before every
/// offset-carrying commit.
pub(crate) fn group_frontier(group_id: &str) -> Option<Arc<CommitFrontier>> {
    let weak = registry()
        .lock()
        .expect("kafka txn registry lock")
        .get(group_id)?
        .frontier
        .clone();
    weak.upgrade()
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

/// Declare that a Kafka output commits the source offsets of `group_id`
/// inside its producer transactions (`offset_commit_group`). Called at
/// output build time so the startup pairing validation sees the claim.
pub(crate) fn declare_offset_committer(group_id: &str) {
    ensure_pairing_validator_registered();
    committers()
        .lock()
        .expect("kafka txn committers lock")
        .insert(group_id.to_owned());
}

/// Startup pairing validation (fail-closed): every consumer group declared
/// by a Kafka input with `transactional_offsets: true` must be claimed by
/// at least one Kafka output's `offset_commit_group`. An unpaired input
/// suppresses its own `store_offset` from the first ack, so its group
/// offsets would never be committed by anyone — a crash then restarts the
/// group purely per `auto.offset.reset` (with `latest`: everything produced
/// during the outage is skipped). The error names the group and both
/// configuration keys so the operator can fix either side.
pub(crate) fn validate_pairings() -> Result<(), Error> {
    // Prune registrations whose input is gone before deciding: a dropped
    // input (config dry build, rejected job, deleted stream) leaves its
    // Weak frontier dead, and an immortal key would fail every later
    // `connect` in this process — including unrelated jobs' — until
    // restart. Liveness of an L3 input is exactly "its frontier still has
    // a strong reference" (the input's acks advance that instance).
    let groups = {
        let mut registry = registry().lock().expect("kafka txn registry lock");
        registry.retain(|_, registration| registration.frontier.strong_count() > 0);
        registry.keys().cloned().collect::<Vec<_>>()
    };
    if groups.is_empty() {
        return Ok(());
    }
    let committers = committers()
        .lock()
        .expect("kafka txn committers lock")
        .clone();
    let unpaired: Vec<String> = groups
        .into_iter()
        .filter(|group| !committers.contains(group))
        .collect();
    if unpaired.is_empty() {
        return Ok(());
    }
    // Report every unpaired group at once: the error names each group and
    // both configuration keys so the operator can fix either side.
    Err(Error::Config(format!(
        "Kafka input(s) declare `transactional_offsets: true` for consumer group(s) [{}], \
         but no Kafka output in this process declares the matching \
         `offset_commit_group`; those groups' broker offsets would never be \
         committed (a crash restarts per auto.offset.reset and can skip records). \
         Pair each input with a transactional Kafka output setting \
         `offset_commit_group: <consumer_group>`, or remove \
         `transactional_offsets` from the input.",
        unpaired.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::checkpoint::SourcePosition;

    fn unique_group(prefix: &str) -> String {
        format!("{prefix}-{}", std::process::id())
    }

    /// Registering a group exposes its frontier through the registry, and
    /// the exposed instance is the one that was registered (same Arc).
    #[tokio::test]
    async fn registry_exposes_the_registered_frontier_instance() {
        let group = unique_group("ktx-frontier");
        let frontier = Arc::new(CommitFrontier::new());
        let slot = register_group(&group, vec!["orders".into()], frontier.clone());
        let exposed = group_frontier(&group).expect("frontier registered");
        assert!(
            Arc::ptr_eq(&frontier, &exposed),
            "the registry must expose the same frontier instance the input's acks advance"
        );
        // The metadata slot is the one the input fills.
        *slot.write().await = None;
        assert!(group_metadata(&group).await.is_none());

        // A frontier snapshot reflects live acknowledgements made through
        // the input's own instance.
        exposed.acknowledge(&SourcePosition {
            topic: Some("orders".into()),
            partition: 2,
            offset: 7,
        });
        let positions = exposed.contiguous_positions();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].offset, 7);
    }

    /// A dropped input's registration dies with it: the frontier upgrade
    /// fails (the output's write-time check then fails closed).
    #[test]
    fn frontier_registration_dies_with_the_input() {
        let group = unique_group("ktx-dead");
        let frontier = Arc::new(CommitFrontier::new());
        let _slot = register_group(&group, vec!["orders".into()], frontier.clone());
        drop(frontier);
        assert!(
            group_frontier(&group).is_none(),
            "a dropped input's frontier must not stay reachable"
        );
    }

    /// Pairing validation: an unpaired `transactional_offsets` group fails
    /// with an error naming the group and both configuration keys; a
    /// declared `offset_commit_group` claim removes it from the reported
    /// set. (The registry is process-global and other tests register their
    /// own groups, so the accept side asserts the claim rather than a
    /// blanket Ok. The frontier must stay alive here — validation prunes
    /// registrations whose input is gone.)
    #[test]
    fn pairing_validation_rejects_unclaimed_groups() {
        let group = unique_group("ktx-unpaired");
        let frontier = Arc::new(CommitFrontier::new());
        let _keep_alive = frontier.clone();
        let _slot = register_group(&group, vec!["orders".into()], frontier);
        let err = validate_pairings().expect_err("an unpaired group must fail validation");
        let message = err.to_string();
        assert!(matches!(err, Error::Config(_)), "got: {err:?}");
        assert!(message.contains(&group), "names the group: {message}");
        assert!(
            message.contains("transactional_offsets"),
            "names the input key: {message}"
        );
        assert!(
            message.contains("offset_commit_group"),
            "names the output key: {message}"
        );

        declare_offset_committer(&group);
        match validate_pairings() {
            Ok(()) => {}
            Err(e) => assert!(
                !e.to_string().contains(&group),
                "the claim must remove the group from the reported set: {e}"
            ),
        }
    }

    /// CR follow-up: a dead registration (its input dropped without any
    /// output ever claiming the group) must be pruned by validation instead
    /// of poisoning every later `connect` in this process with the
    /// "unpaired" error.
    #[test]
    fn pairing_validation_prunes_dead_registrations() {
        let group = unique_group("ktx-dead-unclaimed");
        let frontier = Arc::new(CommitFrontier::new());
        let _slot = register_group(&group, vec!["orders".into()], frontier.clone());
        // Live registration: reachable through the registry.
        assert!(group_frontier(&group).is_some());

        // Drop the input's frontier — nothing strong references it anymore,
        // but the registry key survives until validation prunes it.
        drop(frontier);
        drop(_slot);

        // The dead, unclaimed key must NOT fail validation...
        validate_pairings()
            .unwrap_or_else(|e| assert!(!e.to_string().contains(&group), "dead key poisoned: {e}"));
        // ...and it must actually be gone from the registry.
        assert!(
            group_frontier(&group).is_none(),
            "validation must prune the dead registration"
        );
    }
}
