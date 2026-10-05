//! Cross-node execution-edge transport: wire frames, envelope codec, and the
//! network manager.
//!
//! Execution edges normally carry [`Envelope`]s over bounded in-process flume
//! channels. When the Hub's placement splits an edge across two Agents, the
//! same envelopes travel instead through this module's frames: a fixed 24-byte
//! header (source/destination quad, length, kind) followed by a payload whose
//! encoding depends on the kind. Data payloads embed Arrow IPC record batches
//! (schema and dictionaries are sent once per connection and cached); signal
//! payloads are serde JSON; receipt payloads mirror the downstream
//! `Ack` lifecycle back to the upstream edge writer.
//!
//! Per-quad channels stay strict-FIFO end to end, so checkpoint barriers and
//! watermarks keep their position relative to the data they were emitted with
//! — the same invariant the in-process kernel guarantees (see
//! [`super::envelope`]). Backpressure is inherited from the bounded local
//! queues on both ends plus TCP's own window. Receipt control frames use their
//! own bounded queue and overflow is reported as a connection failure.
//!
//! # Pump cancellation semantics ("void writes")
//!
//! The outbound pump registers a branch acknowledgement before its frame is
//! written, so a delivery may end up "written into the void": encoded and
//! dispatched, but with no receipt that can ever come back. The exit paths
//! split by whether the connection survives the exit:
//!
//! - **wire failure, shutdown, registration failure** tear the connection
//!   down. The pump aborts everything still registered (a branch whose
//!   receipt cannot arrive is **aborted, never acknowledged**; at-least-once
//!   is preserved and the upstream re-delivers — duplicates downstream are
//!   allowed) after flushing already-encoded frames best-effort, so an
//!   in-flight delivery still reaches the wire when the connection is alive.
//!   A wire write failure additionally fails the pump (`Err`).
//! - **upstream channel close** is a clean drain: only the send half ends,
//!   and the receipt read loop on the same connection stays alive. The pump
//!   leaves registered branches alone so late receipts for flushed frames
//!   keep applying; that read loop owns the final abort sweep once the peer
//!   closes or the read idle timeout fires with receipts still outstanding.
//!
//! The failure modes live in [`pump_edge`] (write error / shutdown funnel
//! into `PendingReceipts::abort_all`; the drained path defers to the receipt
//! read loop) and are pinned by the `pump_cancel_tests`.

mod auth;
mod codec;
mod manager;
mod transport;
mod wire;

#[cfg(test)]
mod tests;

pub use auth::{DataPlaneCredentials, PeerExpectation};
pub use manager::{NetworkManager, NetworkManagerConfig, RemoteEdge};
pub use transport::{DataPlaneTlsConfig, EdgeTransport, RemoteStream};
pub use wire::Quad;

// Crate-internal items reached by executor siblings through
// `super::remote::X` (graph.rs, task.rs).
pub(crate) use manager::{capture_trace_context, extract_trace_context};
pub(crate) use transport::TcpEdgeTransport;

// Names below exist for the extracted `tests` module (and any in-crate test
// support), reached through `use super::*`; they are compiled out of the
// non-test library build.
#[cfg(test)]
pub(crate) use auth::{EdgeSessionKey, SessionAuth, DATA_PLANE_PROTOCOL_VERSION};
#[cfg(test)]
pub(crate) use codec::{read_piece, DataDecoder, DataEncoder, Piece};
#[cfg(test)]
pub(crate) use manager::{pump_edge, run_edge_connection, PendingReceipts, RemoteAck};
#[cfg(test)]
pub(crate) use transport::{
    read_exact_with_idle, read_frame, read_frame_with_limits, write_frame, write_frame_with_limit,
    DATA_PLANE_TLS_SERVER_NAME,
};
#[cfg(test)]
pub(crate) use wire::{
    FrameHeader, FrameKind, ReceiptFrame, ReceiptKind, WireDataMeta, WireSignal, FRAME_HEADER_LEN,
    MAX_FRAME_LEN,
};

// Bindings the extracted `tests` module reaches through `use super::*`,
// mirroring the former file-level import list (test-used subset only).
#[cfg(test)]
use crate::checkpoint::CheckpointBarrier;
#[cfg(test)]
use crate::executor::envelope::Envelope;
#[cfg(test)]
use crate::Error;
#[cfg(test)]
use crate::MessageBatch;
#[cfg(test)]
use datafusion::arrow::array::ArrayRef;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
