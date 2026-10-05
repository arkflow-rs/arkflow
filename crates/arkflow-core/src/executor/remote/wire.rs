//! Wire-format types for the cross-node data plane: routing quads, frame
//! headers and kinds, control signals, receipts, and data-payload metadata.

use crate::checkpoint::CheckpointBarrier;
use crate::executor::envelope::Envelope;
use crate::Error;
use serde::{Deserialize, Serialize};

/// Upper bound on one frame's payload. A frame declares its length up front;
/// anything past this bound is a protocol error rather than an allocation.
pub(crate) const MAX_FRAME_LEN: u32 = 1 << 28;

/// Fixed frame header length: six little-endian `u32`s.
pub(crate) const FRAME_HEADER_LEN: usize = 24;

/// Routing identity of one (upstream task subtask → downstream subtask)
/// channel. All frames on the wire address a quad; one TCP connection serves
/// one quad.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Quad {
    pub src_op: u32,
    pub src_subtask: u32,
    pub dst_op: u32,
    pub dst_subtask: u32,
}

/// Wire frame kinds. `0` is never valid so an all-zero (zeroed-memory) header
/// is rejected instead of silently misparsed.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FrameKind {
    /// A [`MessageBatch`] in Arrow IPC form.
    Data = 1,
    /// A serialized [`WireSignal`].
    Signal = 2,
    /// A serialized [`ReceiptFrame`] mirrored back to the upstream writer.
    Receipt = 3,
    /// The first frame on an authenticated network session, and the server's
    /// acknowledgement of that handshake.
    Handshake = 4,
}

impl FrameKind {
    fn from_u32(raw: u32) -> Result<Self, Error> {
        match raw {
            1 => Ok(Self::Data),
            2 => Ok(Self::Signal),
            3 => Ok(Self::Receipt),
            4 => Ok(Self::Handshake),
            other => Err(Error::Process(format!(
                "remote edge frame has unknown kind {other}"
            ))),
        }
    }
}

/// Fixed 24-byte frame header preceding every payload.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    pub quad: Quad,
    /// Payload length in bytes; never exceeds [`MAX_FRAME_LEN`].
    pub len: u32,
    pub kind: FrameKind,
}

impl FrameHeader {
    #[cfg(test)]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_max(bytes, MAX_FRAME_LEN)
    }
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(FRAME_HEADER_LEN);
        out.extend_from_slice(&self.quad.src_op.to_le_bytes());
        out.extend_from_slice(&self.quad.src_subtask.to_le_bytes());
        out.extend_from_slice(&self.quad.dst_op.to_le_bytes());
        out.extend_from_slice(&self.quad.dst_subtask.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&(self.kind as u32).to_le_bytes());
    }

    pub fn decode_with_max(bytes: &[u8], max_frame_len: u32) -> Result<Self, Error> {
        let raw: [u8; FRAME_HEADER_LEN] = bytes
            .try_into()
            .map_err(|_| Error::Process("frame header truncated".into()))?;
        let read_u32 = |offset: usize| {
            u32::from_le_bytes(raw[offset..offset + 4].try_into().expect("sliced u32"))
        };
        let quad = Quad {
            src_op: read_u32(0),
            src_subtask: read_u32(4),
            dst_op: read_u32(8),
            dst_subtask: read_u32(12),
        };
        let len = read_u32(16);
        if len > max_frame_len {
            return Err(Error::Process(format!(
                "remote edge frame declares {len} bytes, above the {max_frame_len} limit"
            )));
        }
        Ok(Self {
            quad,
            len,
            kind: FrameKind::from_u32(read_u32(20))?,
        })
    }
}

/// Control elements that travel alongside data on a remote edge, in the same
/// strict FIFO order as the in-process `Envelope` they encode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum WireSignal {
    Barrier(CheckpointBarrier),
    Watermark(i64),
    Eos,
}

impl WireSignal {
    pub fn from_envelope(envelope: &Envelope) -> Option<Self> {
        match envelope {
            Envelope::Barrier(barrier) => Some(Self::Barrier(barrier.clone())),
            Envelope::Watermark(watermark_ms) => Some(Self::Watermark(*watermark_ms)),
            Envelope::Eos => Some(Self::Eos),
            Envelope::Data(_, _) => None,
        }
    }

    pub fn into_envelope(self) -> Envelope {
        match self {
            Self::Barrier(barrier) => Envelope::Barrier(barrier),
            Self::Watermark(watermark_ms) => Envelope::Watermark(watermark_ms),
            Self::Eos => Envelope::Eos,
        }
    }
}

/// A downstream acknowledgement-lifecycle event mirrored back to the upstream
/// edge writer. `Acked` completes the mirrored fan-out branch once every
/// replica of the batch reported it; `Held`/`Released` relay the buffering
/// operator's hold so the upstream source excludes the acknowledgement from
/// checkpoint barrier draining exactly as it would for a local window;
/// `Failed` reports that the receiving side aborted the delivery (processor
/// failure), letting the upstream abort the branch immediately instead of
/// waiting for the barrier-drain timeout.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ReceiptKind {
    Acked,
    Held,
    Released,
    Failed,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReceiptFrame {
    pub kind: ReceiptKind,
    /// Sender-assigned sequence number of the data frame this receipt refers
    /// to. Sequence numbers are per quad channel and start at 0.
    pub seq: u64,
}

/// Metadata block prefixed to every data payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireDataMeta {
    pub(crate) input_name: Option<String>,
    /// Content hash of the batch schema. The full schema flatbuffer rides the
    /// first data frame of a connection (and any later frame whose schema
    /// differs); receivers cache by this id.
    pub(crate) schema_id: u64,
    pub(crate) has_schema: bool,
    /// Outbound pump-assigned sequence number. Receipts echo it back so the
    /// upstream pending map can complete the right branch acknowledgement.
    pub(crate) seq: u64,
}

/// Outcome of a tiered receipt-channel read: a full frame, or a
/// dead-peer-idle timeout that raced a pending-batch registration and may be
/// retried under the long budget without corrupting the byte stream.
pub(crate) enum TieredFrame {
    Frame((FrameHeader, Vec<u8>)),
    /// The short (empty-pending) budget elapsed with zero bytes consumed,
    /// and a registration appeared during the wait — restart the read; the
    /// next pass uses the receipt-wait budget.
    RacedIdleRestart,
}
