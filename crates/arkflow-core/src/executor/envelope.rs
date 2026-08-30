//! Control-plane envelopes that flow through execution edges alongside data.
//!
//! Edges carry [`Envelope`]s in strict FIFO order so that checkpoint barriers
//! and watermarks maintain their position relative to the data batches they
//! were emitted with — the same invariant Flink's StreamElement guarantees.

use crate::checkpoint::CheckpointBarrier;
use crate::input::Ack;
use crate::MessageBatchRef;
use std::sync::Arc;

/// One element travelling on an execution edge.
#[derive(Clone)]
pub enum Envelope {
    /// A data batch paired with the source ack that commits it.
    Data(MessageBatchRef, Arc<dyn Ack>),
    /// A checkpoint barrier; snapshots align input positions with state.
    Barrier(CheckpointBarrier),
    /// Event-time progress from a source, in epoch milliseconds.
    Watermark(i64),
    /// End-of-stream for bounded sources; downstream vertices drain and stop.
    Eos,
}

impl Envelope {
    pub fn data(batch: MessageBatchRef, ack: Arc<dyn Ack>) -> Self {
        Self::Data(batch, ack)
    }

    pub fn barrier(barrier: CheckpointBarrier) -> Self {
        Self::Barrier(barrier)
    }

    pub fn watermark(watermark_ms: i64) -> Self {
        Self::Watermark(watermark_ms)
    }
}
