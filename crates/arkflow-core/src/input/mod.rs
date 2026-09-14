/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Input component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::checkpoint::SourcePosition;
use crate::codec::{Codec, CodecConfig};
use crate::{Error, MessageBatchRef, Resource};

lazy_static::lazy_static! {
    static ref INPUT_BUILDERS: RwLock<HashMap<String, Arc<dyn InputBuilder>>> = RwLock::new(HashMap::new());
}

pub trait InputBuilder: Send + Sync {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error>;
}

#[async_trait]
pub trait Ack: Send + Sync {
    /// Acknowledge the message.
    ///
    /// Returns `Err` if the acknowledgement could not be completed (e.g. a
    /// durable cursor could not be advanced or a source-side commit failed).
    /// A returned `Err` does not lose data under at-least-once semantics — the
    /// unacknowledged message will be re-delivered — but it lets the stream
    /// observe the failure to apply backpressure or stop.
    async fn ack(&self) -> Result<(), Error>;

    /// Compensate a successful acknowledgement when a sibling in the same
    /// logical delivery fails. Connectors that cannot move their durable
    /// cursor backwards may keep the default no-op and surface their existing
    /// at-least-once limitation; local composites and WALs override it.
    async fn undo(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Abort an acknowledgement that was handed to a downstream delivery
    /// which could not be published.  Unlike `undo`, abort also prevents a
    /// queued sibling from completing a fan-out parent after the routing
    /// operation has already failed.  Connectors with a stronger rollback
    /// boundary inherit the default compensation behavior.
    async fn abort(&self) -> Result<(), Error> {
        self.undo().await
    }

    /// Signal that this acknowledgement is now held by a buffering operator
    /// (event-time gate or window) and may complete much later, or never
    /// before shutdown. Checkpoint barrier draining waits for in-flight
    /// acknowledgements to complete so sealed positions and committed state
    /// describe the same acknowledged set; held acknowledgements are excluded
    /// from that wait because their state mutations stay staged in the
    /// execution-local journal until the buffer fires. The default is a
    /// no-op for acknowledgements that complete in ordinary sink latency.
    fn mark_held(&self) {}

    /// Signal that a previously held acknowledgement is being released to
    /// ordinary downstream processing.  Buffering operators use this to
    /// re-enter the barrier's in-flight set before state/output/source
    /// acknowledgement completes.  Ordinary acknowledgements remain a
    /// no-op.
    fn release_held(&self) {}
}

/// Split one source acknowledgement across several downstream deliveries.
///
/// A DAG fan-out must not acknowledge the source as soon as the first branch
/// succeeds: every terminal branch has to finish first. The returned child
/// acknowledgements are idempotent individually and invoke `parent` exactly
/// once, after all children have been acknowledged successfully.
pub fn fanout_ack(parent: Arc<dyn Ack>, branches: usize) -> Vec<Arc<dyn Ack>> {
    if branches <= 1 {
        return vec![parent];
    }

    let state = Arc::new(FanoutAckState {
        parent,
        remaining: AtomicUsize::new(branches),
        parent_lock: tokio::sync::Mutex::new(()),
        parent_acked: AtomicBool::new(false),
        aborted: AtomicBool::new(false),
        children: std::sync::Mutex::new(Vec::with_capacity(branches)),
    });
    (0..branches)
        .map(|_| {
            let acknowledged = Arc::new(AtomicBool::new(false));
            state.children.lock().unwrap().push(acknowledged.clone());
            Arc::new(FanoutAckPart {
                state: state.clone(),
                acknowledged,
                held: Arc::new(AtomicBool::new(false)),
            }) as Arc<dyn Ack>
        })
        .collect()
}

struct FanoutAckState {
    parent: Arc<dyn Ack>,
    remaining: AtomicUsize,
    /// Serialize the final parent acknowledgement. A transient parent error
    /// can then roll the group back for a safe retry.
    parent_lock: tokio::sync::Mutex<()>,
    parent_acked: AtomicBool,
    aborted: AtomicBool,
    children: std::sync::Mutex<Vec<Arc<AtomicBool>>>,
}

struct FanoutAckPart {
    state: Arc<FanoutAckState>,
    acknowledged: Arc<AtomicBool>,
    /// Per-branch hold state. Siblings of one fan-out hold and release
    /// independently (for example a gate dispatch splits one delivery into a
    /// held window group and ready groups), so the shared parent must only
    /// observe hold *transitions* of each branch, never a branch's release of
    /// a hold it does not own.
    held: Arc<AtomicBool>,
}

#[async_trait]
impl Ack for FanoutAckPart {
    async fn ack(&self) -> Result<(), Error> {
        // Serialize the child state transition with abort.  A sibling may
        // already be queued in a channel when another send fails; it must see
        // the aborted flag before it can decrement the parent group.
        let _guard = self.state.parent_lock.lock().await;
        if self.state.aborted.load(Ordering::Acquire) {
            // A queued sibling must not report a successful downstream
            // acknowledgement after the fan-out was aborted.  Returning an
            // error lets any state/output wrapper around that sibling roll
            // back its own mutation instead of committing a delivery that can
            // no longer reach the source parent.
            return Err(Error::Process(
                "fan-out acknowledgement was aborted".to_owned(),
            ));
        }
        // A downstream retry or a duplicated control path must not decrement
        // the group more than once.
        if self.acknowledged.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // A branch that completes while still marked held (its operator acked
        // without an explicit release) must not leave the shared tracker
        // excluding it from barrier draining forever.
        if self.held.swap(false, Ordering::AcqRel) {
            self.state.parent.release_held();
        }
        if self.state.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            match self.state.parent.ack().await {
                Ok(()) => {
                    self.state.parent_acked.store(true, Ordering::Release);
                    Ok(())
                }
                Err(error) => {
                    // The child was only tentatively acknowledged. Keep the
                    // group pending when the parent could not commit so the
                    // caller can retry this child without losing the source
                    // acknowledgement.
                    self.state.remaining.fetch_add(1, Ordering::AcqRel);
                    self.acknowledged.store(false, Ordering::Release);
                    Err(error)
                }
            }
        } else {
            Ok(())
        }
    }

    async fn undo(&self) -> Result<(), Error> {
        let _guard = self.state.parent_lock.lock().await;
        if self.state.aborted.load(Ordering::Acquire) {
            return Ok(());
        }
        let parent_was_acked = self.state.parent_acked.load(Ordering::Acquire);
        if parent_was_acked {
            self.state.parent.undo().await?;
            self.state.parent_acked.store(false, Ordering::Release);
        }
        let children = self.state.children.lock().unwrap();
        for child in children.iter() {
            child.store(false, Ordering::Release);
        }
        self.state
            .remaining
            .store(children.len(), Ordering::Release);
        Ok(())
    }

    async fn abort(&self) -> Result<(), Error> {
        let _guard = self.state.parent_lock.lock().await;
        if self.state.aborted.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let parent_was_acked = self.state.parent_acked.swap(false, Ordering::AcqRel);
        // An aborted fan-out may have no completed parent acknowledgement yet,
        // while the parent still owns staged state (for example a
        // CommitGroupOnAck wrapped around the source ack).  Abort that parent
        // as well so a failed send cannot leave a transaction live forever.
        let parent_result = if parent_was_acked {
            self.state.parent.undo().await
        } else {
            self.state.parent.abort().await
        };
        let children = self.state.children.lock().unwrap();
        for child in children.iter() {
            child.store(false, Ordering::Release);
        }
        self.state
            .remaining
            .store(children.len(), Ordering::Release);
        parent_result
    }

    fn mark_held(&self) {
        // The held child may never ack before shutdown; propagate so the
        // group's source-side tracker excludes it from barrier draining. Only
        // this branch's first hold transitions the parent — a sibling's hold
        // or release must not consume it.
        if !self.held.swap(true, Ordering::AcqRel) {
            self.state.parent.mark_held();
        }
    }

    fn release_held(&self) {
        if self.held.swap(false, Ordering::AcqRel) {
            self.state.parent.release_held();
        }
    }
}

#[async_trait]
pub trait Input: Send + Sync {
    /// Connect to the input source
    async fn connect(&self) -> Result<(), Error>;

    /// Read a message using Arc for zero-copy
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error>;

    /// Restore source cursors before a distributed Job resumes processing.
    /// Legacy inputs remain compatible through the no-op default.
    async fn restore_positions(&self, _positions: &[SourcePosition]) -> Result<(), Error> {
        Ok(())
    }

    /// Return the latest durable source positions for checkpointing.
    /// Legacy inputs remain compatible through the empty default.
    async fn current_positions(&self) -> Result<Vec<SourcePosition>, Error> {
        Ok(Vec::new())
    }

    /// Return the physical event-time partitions assigned to this reader.
    /// Seeded partitions participate in the watermark minimum before their
    /// first record arrives.
    async fn watermark_partitions(
        &self,
    ) -> Result<Vec<crate::event_time::EventTimePartition>, Error> {
        Ok(Vec::new())
    }

    /// Reconstruct an acknowledgement for a checkpoint/WAL source position
    /// during replay. Connectors without a native position ack use `None` and
    /// retain the legacy local-WAL-only behavior.
    async fn ack_for_position(
        &self,
        _position: &SourcePosition,
    ) -> Result<Option<Arc<dyn Ack>>, Error> {
        Ok(None)
    }

    /// Bind this reader to a physical source partition owned by its task.
    /// Connectors that do not support partition assignment remain compatible
    /// with single-partition Jobs through the no-op default.
    fn assign_partition(&self, _partition: u32) -> Result<(), Error> {
        Ok(())
    }

    /// Whether this connector enforces the assigned partition at the source.
    fn supports_partitioning(&self) -> bool {
        false
    }

    /// Close the input source connection
    async fn close(&self) -> Result<(), Error>;
}

pub struct NoopAck;

#[async_trait]
impl Ack for NoopAck {
    async fn ack(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct VecAck(pub Vec<Arc<dyn Ack>>);

#[async_trait]
impl Ack for VecAck {
    async fn ack(&self) -> Result<(), Error> {
        for (index, ack) in self.0.iter().enumerate() {
            match ack.ack().await {
                Ok(()) => {}
                Err(error) => {
                    // An acknowledgement may have advanced its durable
                    // source before reporting an error. Compensate the
                    // failed child as well as the earlier successful
                    // children; otherwise a composite failure can roll back
                    // state while one source cursor remains past the input.
                    for child in self.0[..=index].iter().rev() {
                        let _ = child.undo().await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn undo(&self) -> Result<(), Error> {
        let mut first_error = None;
        for ack in self.0.iter().rev() {
            if let Err(error) = ack.undo().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn abort(&self) -> Result<(), Error> {
        let mut first_error = None;
        for ack in self.0.iter().rev() {
            if let Err(error) = ack.abort().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn mark_held(&self) {
        for ack in &self.0 {
            ack.mark_held();
        }
    }

    fn release_held(&self) {
        for ack in &self.0 {
            ack.release_held();
        }
    }
}

impl Deref for VecAck {
    type Target = Vec<Arc<dyn Ack>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VecAck {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Arc<dyn Ack>> for VecAck {
    fn from(ack: Arc<dyn Ack>) -> Self {
        VecAck(vec![ack])
    }
}

/// Acknowledgement composite for independent source records. Unlike
/// [`VecAck`], it invokes all children concurrently so a later Kafka/WAL
/// sequence cannot block the earlier sequence from running in the same
/// composite. The caller still observes a single success only after every
/// child succeeds.
pub struct ConcurrentAck(pub Vec<Arc<dyn Ack>>);

#[async_trait]
impl Ack for ConcurrentAck {
    async fn ack(&self) -> Result<(), Error> {
        let results = futures::future::join_all(self.0.iter().map(|ack| ack.ack())).await;
        let mut first_error = None;
        for result in results {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        if first_error.is_some() {
            // Any child may have advanced its durable source before returning
            // an error. Compensate successful and failed children alike so a
            // composite failure cannot roll back state while one source
            // cursor remains past the input. Connector acks are required to
            // make undo idempotent for the not-yet-committed case.
            // Reverse order matters for source positions in one partition:
            // undo a later durable cursor before compensating an earlier one.
            for ack in self.0.iter().rev() {
                if let Err(error) = ack.undo().await {
                    first_error.get_or_insert(Error::Process(format!(
                        "source acknowledgement failed and compensation failed: {error}"
                    )));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn undo(&self) -> Result<(), Error> {
        let mut first_error = None;
        for ack in self.0.iter().rev() {
            if let Err(error) = ack.undo().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn abort(&self) -> Result<(), Error> {
        let mut first_error = None;
        for ack in self.0.iter().rev() {
            if let Err(error) = ack.abort().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn mark_held(&self) {
        for ack in &self.0 {
            ack.mark_held();
        }
    }

    fn release_held(&self) {
        for ack in &self.0 {
            ack.release_held();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailOnceAck {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Ack for FailOnceAck {
        async fn ack(&self) -> Result<(), Error> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call == 0 {
                Err(Error::Process("transient parent ack failure".into()))
            } else {
                Ok(())
            }
        }
    }

    struct RecordingAck {
        acked: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct CompensatingAck {
        acked: AtomicUsize,
        undone: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Ack for CompensatingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn undo(&self) -> Result<(), Error> {
            self.undone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn fanout_ack_retries_parent_after_transient_failure() {
        let parent_impl = Arc::new(FailOnceAck {
            calls: AtomicUsize::new(0),
        });
        let parent = parent_impl.clone() as Arc<dyn Ack>;
        let children = fanout_ack(parent.clone(), 2);

        children[0].ack().await.unwrap();
        assert!(children[1].ack().await.is_err());
        assert!(children[1].ack().await.is_ok());
        // A duplicate downstream acknowledgement is idempotent.
        children[1].ack().await.unwrap();
        assert_eq!(parent_impl.calls.load(Ordering::Relaxed), 2);
    }

    /// A gate dispatch splits one source delivery into independently held and
    /// ready branches. The ready branch's release must not cancel the held
    /// branch's exclusion from barrier draining, or every checkpoint round
    /// waits out the drain timeout while an open window holds rows.
    #[tokio::test]
    async fn fanout_branches_hold_and_release_independently() {
        use crate::executor::commit::{AckTracker, TrackingAck};

        let tracker = Arc::new(AckTracker::new());
        let source = Arc::new(RecordingAck {
            acked: AtomicUsize::new(0),
        });
        let tracking = Arc::new(TrackingAck::new(
            tracker.clone(),
            source.clone() as Arc<dyn Ack>,
        ));
        let children = fanout_ack(tracking.clone() as Arc<dyn Ack>, 2);

        // Branch 0 is held by an open window; branch 1 is ready immediately.
        children[0].mark_held();
        assert_eq!(
            tracker.blocking(),
            0,
            "a held branch must not block barrier draining"
        );

        // The ready branch passes through the gate (mark→release would also
        // happen on a re-classified held batch) and completes downstream.
        children[1].mark_held();
        children[1].release_held();
        assert_eq!(
            tracker.blocking(),
            0,
            "a sibling release must not resurrect the held branch as blocking"
        );
        children[1].ack().await.unwrap();
        assert_eq!(
            tracker.blocking(),
            0,
            "a ready sibling completing must not block on the held branch"
        );

        // The window eventually fires branch 0.
        children[0].release_held();
        children[0].ack().await.unwrap();
        assert_eq!(tracker.blocking(), 0);
        assert_eq!(source.acked.load(Ordering::Relaxed), 1);
    }

    /// Task 1.5: a composite acknowledgement must surface a constituent's
    /// failure instead of reporting success while one branch's state or
    /// durability commit failed.
    #[tokio::test]
    async fn vec_ack_propagates_constituent_failures() {
        let healthy = Arc::new(RecordingAck {
            acked: AtomicUsize::new(0),
        });
        let failing = Arc::new(FailOnceAck {
            calls: AtomicUsize::new(0),
        });
        let composite = VecAck(vec![
            healthy.clone() as Arc<dyn Ack>,
            failing.clone() as Arc<dyn Ack>,
            Arc::new(NoopAck),
        ]);
        assert!(composite.ack().await.is_err());
        // The constituents before the failing one completed; the failure is
        // not hidden by a later successful branch.
        assert_eq!(healthy.acked.load(Ordering::Relaxed), 1);
        assert_eq!(failing.calls.load(Ordering::Relaxed), 1);
        // A composite of successful constituents (including the no-op)
        // acknowledges cleanly.
        let clean = VecAck(vec![
            Arc::new(NoopAck),
            Arc::new(RecordingAck {
                acked: AtomicUsize::new(0),
            }),
        ]);
        assert!(clean.ack().await.is_ok());
    }

    #[tokio::test]
    async fn concurrent_ack_runs_independent_children_together() {
        struct BarrierAck {
            barrier: Arc<tokio::sync::Barrier>,
        }

        #[async_trait]
        impl Ack for BarrierAck {
            async fn ack(&self) -> Result<(), Error> {
                self.barrier.wait().await;
                Ok(())
            }
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let composite = ConcurrentAck(vec![
            Arc::new(BarrierAck {
                barrier: barrier.clone(),
            }),
            Arc::new(BarrierAck { barrier }),
        ]);
        tokio::time::timeout(std::time::Duration::from_secs(1), composite.ack())
            .await
            .expect("independent acknowledgements must not be serialized")
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_ack_compensates_successful_siblings() {
        let healthy = Arc::new(CompensatingAck {
            acked: AtomicUsize::new(0),
            undone: AtomicUsize::new(0),
        });
        let failing = Arc::new(FailOnceAck {
            calls: AtomicUsize::new(0),
        });
        let composite = ConcurrentAck(vec![
            healthy.clone() as Arc<dyn Ack>,
            failing as Arc<dyn Ack>,
        ]);

        assert!(composite.ack().await.is_err());
        assert_eq!(healthy.acked.load(Ordering::Relaxed), 1);
        assert_eq!(healthy.undone.load(Ordering::Relaxed), 1);
    }
}

/// Input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    #[serde(rename = "type")]
    pub input_type: String,
    pub name: Option<String>,
    pub codec: Option<CodecConfig>,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl InputConfig {
    /// Building input components
    pub fn build(&self, resource: &Resource) -> Result<Arc<dyn Input>, Error> {
        let builders = INPUT_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.input_type) {
            // Build codec if configured
            let codec = if let Some(codec_config) = &self.codec {
                Some(codec_config.build(resource)?)
            } else {
                None
            };

            builder.build(self.name.as_ref(), &self.config, codec, resource)
        } else {
            Err(Error::Config(format!(
                "Unknown input type: {}",
                self.input_type
            )))
        }
    }
}

pub fn register_input_builder(
    type_name: &str,
    builder: Arc<dyn InputBuilder>,
) -> Result<(), Error> {
    let mut builders = INPUT_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        return Err(Error::Config(format!(
            "Input type already registered: {}",
            type_name
        )));
    }
    builders.insert(type_name.to_string(), builder);
    Ok(())
}
