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
//! queues on both ends plus TCP's own window; nothing in this module buffers
//! unboundedly except receipt control frames, which are tiny and bounded by
//! the data window that produced them.

use crate::checkpoint::CheckpointBarrier;
use crate::executor::envelope::Envelope;
use crate::MessageBatch;
use crate::Error;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use datafusion::arrow::ipc::reader::{read_dictionary, read_record_batch};
use datafusion::arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
};
use datafusion::arrow::ipc::{root_as_message, MessageHeader};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Upper bound on one frame's payload. A frame declares its length up front;
/// anything past this bound is a protocol error rather than an allocation.
pub const MAX_FRAME_LEN: u32 = 1 << 28;

/// Fixed frame header length: six little-endian `u32`s.
pub const FRAME_HEADER_LEN: usize = 24;

/// Routing identity of one (upstream task subtask → downstream subtask)
/// channel. All frames on the wire address a quad; one TCP connection serves
/// one quad.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Quad {
    pub src_op: u32,
    pub src_subtask: u32,
    pub dst_op: u32,
    pub dst_subtask: u32,
}

/// Wire frame kinds. `0` is never valid so an all-zero (zeroed-memory) header
/// is rejected instead of silently misparsed.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// A [`MessageBatch`] in Arrow IPC form.
    Data = 1,
    /// A serialized [`WireSignal`].
    Signal = 2,
    /// A serialized [`ReceiptFrame`] mirrored back to the upstream writer.
    Receipt = 3,
}

impl FrameKind {
    fn from_u32(raw: u32) -> Result<Self, Error> {
        match raw {
            1 => Ok(Self::Data),
            2 => Ok(Self::Signal),
            3 => Ok(Self::Receipt),
            other => Err(Error::Process(format!(
                "remote edge frame has unknown kind {other}"
            ))),
        }
    }
}

/// Fixed 24-byte frame header preceding every payload.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub quad: Quad,
    /// Payload length in bytes; never exceeds [`MAX_FRAME_LEN`].
    pub len: u32,
    pub kind: FrameKind,
}

impl FrameHeader {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(FRAME_HEADER_LEN);
        out.extend_from_slice(&self.quad.src_op.to_le_bytes());
        out.extend_from_slice(&self.quad.src_subtask.to_le_bytes());
        out.extend_from_slice(&self.quad.dst_op.to_le_bytes());
        out.extend_from_slice(&self.quad.dst_subtask.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&(self.kind as u32).to_le_bytes());
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
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
        if len > MAX_FRAME_LEN {
            return Err(Error::Process(format!(
                "remote edge frame declares {len} bytes, above the {MAX_FRAME_LEN} limit"
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
pub enum WireSignal {
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
/// checkpoint barrier draining exactly as it would for a local window.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptKind {
    Acked,
    Held,
    Released,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptFrame {
    pub kind: ReceiptKind,
    /// Sender-assigned sequence number of the data frame this receipt refers
    /// to. Sequence numbers are per quad channel and start at 0.
    pub seq: u64,
}

/// Metadata block prefixed to every data payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WireDataMeta {
    input_name: Option<String>,
    /// Content hash of the batch schema. The full schema flatbuffer rides the
    /// first data frame of a connection (and any later frame whose schema
    /// differs); receivers cache by this id.
    schema_id: u64,
    has_schema: bool,
    /// Outbound pump-assigned sequence number. Receipts echo it back so the
    /// upstream pending map can complete the right branch acknowledgement.
    seq: u64,
}

// ---------------------------------------------------------------------------
// Data payload encode/decode
// ---------------------------------------------------------------------------

/// Encodes record batches into data-frame payloads on one connection.
///
/// Schema and dictionary pieces are emitted only when needed: the schema on
/// the first frame (or when it changes), dictionaries whenever the tracker
/// observes a new dictionary. A mid-connection schema change re-sends the
/// schema so the receiver's cache tracks it; a stream edge's schema is fixed
/// by the plan, so in practice this never fires.
pub struct DataEncoder {
    generator: IpcDataGenerator,
    tracker: DictionaryTracker,
    options: IpcWriteOptions,
    compression: CompressionContext,
    schema_id: Option<u64>,
}

impl Default for DataEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DataEncoder {
    pub fn new() -> Self {
        Self {
            generator: IpcDataGenerator {},
            tracker: DictionaryTracker::new(true),
            options: IpcWriteOptions::default(),
            compression: CompressionContext::default(),
            schema_id: None,
        }
    }

    pub fn encode(&mut self, batch: &MessageBatch, seq: u64) -> Result<Vec<u8>, Error> {
        let record_batch = batch.record_batch();
        let schema = record_batch.schema();
        let schema_id = hash_schema(&schema);
        let has_schema = self.schema_id != Some(schema_id);
        let input_name = batch.get_input_name();

        let mut payload = Vec::new();
        let meta = serde_json::to_vec(&WireDataMeta {
            input_name,
            schema_id,
            has_schema,
            seq,
        })
        .map_err(|error| Error::Process(format!("data frame meta encode failed: {error}")))?;
        payload.extend_from_slice(&(meta.len() as u32).to_le_bytes());
        payload.extend_from_slice(&meta);

        if has_schema {
            let encoded = self.generator.schema_to_bytes_with_dictionary_tracker(
                &schema,
                &mut self.tracker,
                &self.options,
            );
            write_piece(&mut payload, &encoded)?;
            self.schema_id = Some(schema_id);
        }
        let (dictionaries, batch_message) = self
            .generator
            .encode(
                record_batch,
                &mut self.tracker,
                &self.options,
                &mut self.compression,
            )
            .map_err(|error| Error::Process(format!("record batch IPC encode failed: {error}")))?;
        for dictionary in &dictionaries {
            write_piece(&mut payload, dictionary)?;
        }
        write_piece(&mut payload, &batch_message)?;
        Ok(payload)
    }
}

/// Decodes data-frame payloads on one connection. Schemas and dictionaries
/// accumulate across frames; the first frame of a connection must carry the
/// schema (the encoder guarantees it) — a frame referencing an unknown schema
/// id is a protocol error.
#[derive(Default)]
pub struct DataDecoder {
    schema: Option<(u64, SchemaRef)>,
    dictionaries: HashMap<i64, ArrayRef>,
}

impl DataDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn decode(&mut self, payload: &[u8]) -> Result<(MessageBatch, u64), Error> {
        let mut cursor = 0usize;
        let meta_len = read_u32(payload, &mut cursor)? as usize;
        let meta_end = cursor
            .checked_add(meta_len)
            .ok_or_else(|| Error::Process("data frame meta length overflows".into()))?;
        let meta: WireDataMeta = serde_json::from_slice(
            payload
                .get(cursor..meta_end)
                .ok_or_else(|| Error::Process("data frame meta truncated".into()))?,
        )
        .map_err(|error| Error::Process(format!("data frame meta decode failed: {error}")))?;
        cursor = meta_end;

        if meta.has_schema {
            let (piece, next) = read_piece(payload, cursor)?;
            let schema = try_schema_from_flatbuffer_bytes(piece.flatbuf()).map_err(|error| {
                Error::Process(format!("data frame schema decode failed: {error}"))
            })?;
            self.schema = Some((meta.schema_id, Arc::new(schema)));
            cursor = next;
        }
        let schema = match self.schema.as_ref() {
            Some((id, schema)) if *id == meta.schema_id => schema.clone(),
            Some((id, _)) => {
                return Err(Error::Process(format!(
                    "data frame schema id {id} does not match declared id {}",
                    meta.schema_id
                )));
            }
            None => {
                return Err(Error::Process(format!(
                    "data frame references unknown schema id {} before any schema frame",
                    meta.schema_id
                )));
            }
        };

        let mut decoded_batch = None;
        while cursor < payload.len() {
            let (piece, next) = read_piece(payload, cursor)?;
            cursor = next;
            let message = root_as_message(piece.flatbuf())
                .map_err(|error| Error::Process(format!("data frame IPC message invalid: {error}")))?;
            let body_len = usize::try_from(message.bodyLength())
                .map_err(|_| Error::Process("negative IPC body length".into()))?;
            let body = piece.body(body_len);
            match message.header_type() {
                MessageHeader::Schema => {
                    return Err(Error::Process(
                        "unexpected schema message without has_schema".into(),
                    ));
                }
                MessageHeader::DictionaryBatch => {
                    let Some(dictionary) = message.header_as_dictionary_batch() else {
                        return Err(Error::Process("dictionary header unreadable".into()));
                    };
                    read_dictionary(
                        &body,
                        dictionary,
                        &schema,
                        &mut self.dictionaries,
                        &message.version(),
                    )
                    .map_err(|error| {
                        Error::Process(format!("data frame dictionary decode failed: {error}"))
                    })?;
                }
                MessageHeader::RecordBatch => {
                    let Some(batch_header) = message.header_as_record_batch() else {
                        return Err(Error::Process("record batch header unreadable".into()));
                    };
                    let decoded = read_record_batch(
                        &body,
                        batch_header,
                        schema.clone(),
                        &self.dictionaries,
                        None,
                        &message.version(),
                    )
                    .map_err(|error| {
                        Error::Process(format!("data frame batch decode failed: {error}"))
                    })?;
                    decoded_batch = Some(decoded);
                }
                other => {
                    return Err(Error::Process(format!(
                        "unexpected IPC message header {other:?} in data frame"
                    )));
                }
            }
        }
        let record_batch = decoded_batch
            .ok_or_else(|| Error::Process("data frame carried no record batch".into()))?;
        let mut message_batch = MessageBatch::new_arrow(record_batch);
        message_batch.set_input_name(meta.input_name);
        Ok((message_batch, meta.seq))
    }
}

/// One length-prefixed IPC piece inside a data payload:
/// `[u32 total][u32 flatbuf_len][flatbuf][body..]`.
struct Piece<'a> {
    bytes: &'a [u8],
    flatbuf_len: usize,
}

impl<'a> Piece<'a> {
    fn flatbuf(&self) -> &'a [u8] {
        &self.bytes[8..8 + self.flatbuf_len]
    }

    fn body(&self, body_len: usize) -> Buffer {
        let start = 8 + self.flatbuf_len;
        let end = start.saturating_add(body_len);
        Buffer::from_vec(self.bytes[start..end].to_vec())
    }
}

fn write_piece(out: &mut Vec<u8>, encoded: &EncodedData) -> Result<(), Error> {
    let total = 8 + encoded.ipc_message.len() + encoded.arrow_data.len();
    if total > u32::MAX as usize {
        return Err(Error::Process("IPC piece exceeds u32 length".into()));
    }
    out.extend_from_slice(&(total as u32).to_le_bytes());
    out.extend_from_slice(&(encoded.ipc_message.len() as u32).to_le_bytes());
    out.extend_from_slice(&encoded.ipc_message);
    out.extend_from_slice(&encoded.arrow_data);
    Ok(())
}

fn read_piece<'a>(payload: &'a [u8], cursor: usize) -> Result<(Piece<'a>, usize), Error> {
    let total = peek_u32(payload, cursor)? as usize;
    if total < 8 {
        return Err(Error::Process("IPC piece length underflows".into()));
    }
    let end = cursor
        .checked_add(total)
        .ok_or_else(|| Error::Process("IPC piece length overflows".into()))?;
    let bytes = payload
        .get(cursor..end)
        .ok_or_else(|| Error::Process("IPC piece truncated".into()))?;
    let flatbuf_len = peek_u32(bytes, 4)? as usize;
    if 8 + flatbuf_len > total {
        return Err(Error::Process(
            "IPC piece flatbuf exceeds piece length".into(),
        ));
    }
    Ok((Piece { bytes, flatbuf_len }, end))
}

fn peek_u32(payload: &[u8], offset: usize) -> Result<u32, Error> {
    let end = offset.checked_add(4).ok_or_else(|| {
        Error::Process("payload length overflows".into())
    })?;
    let bytes = payload
        .get(offset..end)
        .ok_or_else(|| Error::Process("payload truncated".into()))?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("sliced u32")))
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32, Error> {
    let value = peek_u32(payload, *cursor)?;
    *cursor += 4;
    Ok(value)
}

/// Stable per-process schema content hash used as the receiver's cache key.
/// Only equality within one connection matters, so a plain `DefaultHasher`
/// (fixed keys, not `RandomState`) is sufficient.
fn hash_schema(schema: &Schema) -> u64 {
    let mut hasher = DefaultHasher::new();
    format!("{schema:?}").hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Frame framing over async sockets
// ---------------------------------------------------------------------------

/// Reads one frame: a 24-byte header followed by its payload.
pub async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(FrameHeader, Vec<u8>), Error> {
    let mut header_bytes = vec![0u8; FRAME_HEADER_LEN];
    reader
        .read_exact(&mut header_bytes)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading header: {error}"))
        })?;
    let header = FrameHeader::decode(&header_bytes)?;
    let mut payload = vec![0u8; header.len as usize];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading payload: {error}"))
        })?;
    Ok((header, payload))
}

/// Writes one frame: header then payload, ideally buffered behind the caller's
/// `BufWriter` so small frames coalesce until the flush tick.
pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
) -> Result<(), Error> {
    if (payload.len() as u64) > u64::from(MAX_FRAME_LEN) {
        return Err(Error::Process(format!(
            "remote edge frame payload of {} bytes exceeds the {} limit",
            payload.len(),
            MAX_FRAME_LEN
        )));
    }
    let mut bytes = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    FrameHeader {
        quad,
        len: payload.len() as u32,
        kind,
    }
    .encode_into(&mut bytes);
    bytes.extend_from_slice(payload);
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| Error::Process(format!("remote edge write failed: {error}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Node-side network manager
// ---------------------------------------------------------------------------

/// A byte stream carrying remote-edge frames. `TcpStream` in production; the
/// loopback halves of `tokio::io::duplex` in kernel semantic tests.
pub trait RemoteStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> RemoteStream for T {}

/// Establishes the outbound connection for one quad. TCP in production; a
/// duplex factory in tests.
#[async_trait::async_trait]
pub trait EdgeTransport: Send + Sync {
    /// Connect to the node serving `quad`. Implementations should retry
    /// transient failures; returning `Err` fails the edge (and with it the
    /// attempt) closed.
    async fn connect(&self, quad: Quad) -> Result<Box<dyn RemoteStream>, Error>;
}

/// TCP transport with bounded exponential backoff, mirroring Arroyo's
/// `OutNetworkLink::connect`.
#[derive(Clone)]
pub struct TcpEdgeTransport {
    pub addr: std::net::SocketAddr,
    pub max_attempts: usize,
}

#[async_trait::async_trait]
impl EdgeTransport for TcpEdgeTransport {
    async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
        let max_attempts = self.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..max_attempts {
            match tokio::net::TcpStream::connect(self.addr).await {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    return Ok(Box::new(stream));
                }
                Err(error) => {
                    let backoff = connect_backoff(attempt);
                    tracing::warn!(
                        "remote edge connect to {} failed (attempt {}/{}): {error}; retrying in {backoff:?}",
                        self.addr,
                        attempt + 1,
                        max_attempts
                    );
                    last_error = Some(error);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        Err(Error::Process(format!(
            "remote edge connect to {} failed after {max_attempts} attempts: {}",
            self.addr,
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown error".into())
        )))
    }
}

fn connect_backoff(attempt: usize) -> std::time::Duration {
    // Grows with the attempt count, capped well below a checkpoint timeout so
    // a partitioned peer fails the attempt closed rather than hanging it.
    std::time::Duration::from_millis((50u64.saturating_mul(attempt as u64 + 1)).min(1_000))
}

/// Accepted-connection queue shared between the transport listener and the
/// serve loop.
type AcceptedQueue = (
    flume::Sender<Box<dyn RemoteStream>>,
    flume::Receiver<Box<dyn RemoteStream>>,
);

/// Mirrors one branch acknowledgement's lifecycle back across the wire. The
/// downstream chain wraps every decoded remote batch in one of these; the
/// kernel's existing fan-out and state wrappers compose on top of it exactly
/// as they do around local acknowledgements.
pub struct RemoteAck {
    outbox: flume::Sender<(Quad, ReceiptFrame)>,
    quad: Quad,
    seq: u64,
}

#[async_trait::async_trait]
impl crate::input::Ack for RemoteAck {
    async fn ack(&self) -> Result<(), Error> {
        self.outbox
            .send_async((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Acked,
                    seq: self.seq,
                },
            ))
            .await
            .map_err(|_| Error::Process("remote edge receipt channel closed".into()))
    }

    fn mark_held(&self) {
        // Synchronous by trait contract (called from the Aligner's sync
        // context): fire-and-forget. A lost Held degrades to a barrier drain
        // timeout — fail-closed, never data loss.
        let _ = self.outbox.try_send((
            self.quad,
            ReceiptFrame {
                kind: ReceiptKind::Held,
                seq: self.seq,
            },
        ));
    }

    fn release_held(&self) {
        let _ = self.outbox.try_send((
            self.quad,
            ReceiptFrame {
                kind: ReceiptKind::Released,
                seq: self.seq,
            },
        ));
    }
}

/// One in-flight batch awaiting its replicas' receipts on a quad.
struct PendingBatch {
    branch: Arc<dyn crate::input::Ack>,
    remaining_replicas: usize,
    held_replicas: usize,
}

/// Receipts for one outbound quad, shared between the outbound pump (which
/// registers pending batches) and the connection's receipt read loop.
#[derive(Default)]
struct PendingReceipts {
    map: std::sync::Mutex<BTreeMap<u64, PendingBatch>>,
    next_seq: std::sync::atomic::AtomicU64,
}

impl PendingReceipts {
    fn register(&self, branch: Arc<dyn crate::input::Ack>, replicas: usize) -> u64 {
        let seq = self
            .next_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.map.lock().expect("pending receipts lock").insert(
            seq,
            PendingBatch {
                branch,
                remaining_replicas: replicas,
                held_replicas: 0,
            },
        );
        seq
    }

    fn apply(&self, receipt: ReceiptFrame, failures: &flume::Sender<Error>) {
        let mut map = self.map.lock().expect("pending receipts lock");
        let Some(pending) = map.get_mut(&receipt.seq) else {
            // Unknown or already-completed sequence: a duplicate receipt from a
            // retried acknowledgement. The branch itself is idempotent; drop.
            return;
        };
        match receipt.kind {
            ReceiptKind::Acked => {
                pending.remaining_replicas = pending.remaining_replicas.saturating_sub(1);
                if pending.remaining_replicas == 0 {
                    let PendingBatch { branch, .. } = map.remove(&receipt.seq).expect("checked");
                    // Acknowledge off the read loop: a durable commit can
                    // block, and the connection must keep draining.
                    let failures = failures.clone();
                    tokio::spawn(async move {
                        if let Err(error) = branch.ack().await {
                            let _ = failures.send(Error::Process(format!(
                                "remote edge branch acknowledgement failed: {error}"
                            )));
                        }
                    });
                }
            }
            ReceiptKind::Held => {
                // The branch's hold flag is transition-tracked; forwarding
                // mark_held on every Held would re-assert the parent hold on
                // each replica, so forward only the 0→1 transition.
                pending.held_replicas += 1;
                if pending.held_replicas == 1 {
                    pending.branch.mark_held();
                }
            }
            ReceiptKind::Released => {
                pending.held_replicas = pending.held_replicas.saturating_sub(1);
                if pending.held_replicas == 0 {
                    pending.branch.release_held();
                }
            }
        }
    }

    fn abort_all(&self) {
        let mut map = self.map.lock().expect("pending receipts lock");
        let drained: BTreeMap<u64, PendingBatch> = std::mem::take(&mut *map);
        for (_, pending) in drained {
            let branch = pending.branch;
            tokio::spawn(async move {
                let _ = branch.abort().await;
            });
        }
    }
}

/// Local end of a remote outbound edge. `graph.rs` places its sender into
/// `EdgeTarget` channel vectors exactly like a local channel sender; the pump
/// task on the receiver side encodes envelopes, sequences data frames, and
/// tracks receipts. Closing the sender closes the edge after draining.
pub struct RemoteEdge {
    pub sender: flume::Sender<super::envelope::Envelope>,
}

/// Per-node exchange: inbound routing registry + outbound edges.
pub struct NetworkManager {
    /// Quad → local input channel (downstream side).
    inbound: std::sync::RwLock<BTreeMap<Quad, flume::Sender<super::envelope::Envelope>>>,
    /// Quad → pending receipts (upstream side), consulted by receipt read loops.
    outbound: std::sync::RwLock<BTreeMap<Quad, Arc<PendingReceipts>>>,
    /// Fatal edge errors (ack commit failures, aborted edges) for the kernel.
    failures: flume::Sender<Error>,
    failure_receiver: flume::Receiver<Error>,
    /// Accepted streams feed this queue so serving works over any transport.
    accepted: AcceptedQueue,
    channel_capacity: usize,
    shutdown: tokio_util::sync::CancellationToken,
}

impl NetworkManager {
    pub fn new(channel_capacity: usize) -> Arc<Self> {
        let (failure_sender, failure_receiver) = flume::unbounded();
        let accepted = flume::unbounded();
        Arc::new(Self {
            inbound: std::sync::RwLock::new(BTreeMap::new()),
            outbound: std::sync::RwLock::new(BTreeMap::new()),
            failures: failure_sender,
            failure_receiver,
            accepted,
            channel_capacity,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Fatal edge failures for the kernel's failure reporter.
    pub fn failure_receiver(&self) -> flume::Receiver<Error> {
        self.failure_receiver.clone()
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Registers the local input channel that remote peer quad writes into
    /// (downstream side of a remote edge). The channel's sender drops when the
    /// peer's connection dies, ending the chain's input exactly like a local
    /// upstream finishing (or failing — at-least-once recovery handles both).
    pub fn register_inbound(&self, quad: Quad, sender: flume::Sender<super::envelope::Envelope>) {
        self.inbound
            .write()
            .expect("inbound registry lock")
            .insert(quad, sender);
    }

    /// Injects an accepted stream (server side). The TCP listener task calls
    /// this per `TcpStream`; tests inject duplex halves.
    pub fn accept_stream(&self, stream: Box<dyn RemoteStream>) {
        let _ = self.accepted.0.send(stream);
    }

    /// Runs the accept loop until shutdown. Call once per manager.
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            while let Ok(stream) = manager.accepted.1.recv_async().await {
                let per_stream = manager.clone();
                tokio::spawn(async move {
                    per_stream.serve_stream(stream).await;
                });
            }
        })
    }

    /// Binds the TCP data-plane listener and feeds accepted connections into
    /// the serve loop. Returns the bound port (for registry advertisement).
    pub async fn bind_tcp(self: &Arc<Self>, addr: std::net::SocketAddr) -> Result<u16, Error> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|error| Error::Process(format!("data plane bind failed: {error}")))?;
        let port = listener
            .local_addr()
            .map_err(|error| Error::Process(format!("data plane local addr failed: {error}")))?
            .port();
        let manager = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, _peer)) => {
                        let _ = stream.set_nodelay(true);
                        manager.accept_stream(Box::new(stream));
                    }
                    Err(error) => {
                        tracing::warn!("data plane accept failed: {error}");
                        break;
                    }
                }
            }
        });
        Ok(port)
    }

    /// Opens the upstream side of a remote edge: connects, spawns the outbound
    /// pump and receipt read loop, and returns the edge channel sender to drop
    /// into `EdgeTarget` channel vectors.
    pub async fn open_edge(
        self: &Arc<Self>,
        transport: &dyn EdgeTransport,
        quad: Quad,
    ) -> Result<RemoteEdge, Error> {
        let stream = transport.connect(quad).await?;
        Ok(self.open_edge_with_stream(stream, quad))
    }

    /// Like [`Self::open_edge`] but over a caller-provided stream (tests).
    pub fn open_edge_with_stream(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
    ) -> RemoteEdge {
        let (sender, receiver) = flume::bounded::<super::envelope::Envelope>(self.channel_capacity);
        let pending = Arc::new(PendingReceipts::default());
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(quad, pending.clone());
        self.attach_stream(stream, quad, receiver, pending);
        RemoteEdge { sender }
    }

    /// Like [`Self::open_edge_with_stream`] but connects on a background task,
    /// so synchronous graph builders can wire remote edges without awaiting.
    /// The edge channel already accepts envelopes while connecting — its bound
    /// applies backpressure. A failed connect fails the edge closed: pending
    /// branch acknowledgements abort and the sender's sends error out.
    pub fn open_edge_deferred(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
    ) -> RemoteEdge {
        let (sender, receiver) = flume::bounded::<super::envelope::Envelope>(self.channel_capacity);
        let pending = Arc::new(PendingReceipts::default());
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(quad, pending.clone());
        let manager = self.clone();
        let failures = self.failures.clone();
        tokio::spawn(async move {
            match transport.connect(quad).await {
                Ok(stream) => {
                    manager.attach_stream(stream, quad, receiver, pending);
                }
                Err(error) => {
                    let _ = failures.send(Error::Process(format!(
                        "remote edge connect for quad {quad:?} failed: {error}"
                    )));
                    pending.abort_all();
                    // Remove the stale registry entry so a later edge for the
                    // same quad does not route receipts into an aborted map.
                    self_remove_outbound(&manager, quad);
                    // Drop the receiver: the graph's sender side observes the
                    // closed edge on its next send.
                    drop(receiver);
                }
            }
        });
        RemoteEdge { sender }
    }

    /// Spawns the receipt read loop and outbound pump over an established
    /// stream (shared by the eager and deferred open paths).
    fn attach_stream(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        receiver: flume::Receiver<super::envelope::Envelope>,
        pending: Arc<PendingReceipts>,
    ) {
        let (reader, writer) = tokio::io::split(stream);
        let failures = self.failures.clone();
        let shutdown = self.shutdown.clone();

        // Receipt read loop: routes receipts to this quad's pending map.
        let receipt_pending = pending.clone();
        let receipt_failures = failures.clone();
        tokio::spawn(async move {
            let mut reader = reader;
            let cancelled = shutdown.child_token();
            loop {
                tokio::select! {
                    _ = cancelled.cancelled() => break,
                    frame = read_frame(&mut reader) => match frame {
                        Ok((header, payload)) if header.kind == FrameKind::Receipt => {
                            if let Ok(receipt) = serde_json::from_slice::<ReceiptFrame>(&payload) {
                                receipt_pending.apply(receipt, &receipt_failures);
                            }
                        }
                        Ok((header, _)) => {
                            let _ = receipt_failures.send(Error::Process(format!(
                                "unexpected {:?} frame on a receipt channel",
                                header.kind
                            )));
                            break;
                        }
                        Err(_) => break, // edge gone; the pump notices via writes
                    },
                }
            }
            receipt_pending.abort_all();
        });

        // Outbound pump: drains the edge channel onto the wire.
        let pump_pending = pending.clone();
        let pump_failures = failures.clone();
        let pump_shutdown = self.shutdown.child_token();
        tokio::spawn(async move {
            if let Err(error) =
                pump_edge(receiver, writer, quad, pump_pending, pump_shutdown).await
            {
                let _ = pump_failures.send(error);
            }
        });
    }

    /// Handles one inbound connection (downstream side): decodes data frames
    /// into the registered local channel, forwards signals, and writes the
    /// receipts produced by `RemoteAck`s on this connection back upstream.
    async fn serve_stream(self: Arc<Self>, stream: Box<dyn RemoteStream>) {
        let (mut reader, writer) = tokio::io::split(stream);
        let (receipt_tx, receipt_rx) = flume::unbounded::<(Quad, ReceiptFrame)>();
        let mut decoder = DataDecoder::new();

        // Receipt writer: drains RemoteAck outboxes onto the wire, flushed
        // immediately — receipts gate upstream source progress.
        let receipt_shutdown = self.shutdown.child_token();
        let receipt_writer_handle = tokio::spawn(async move {
            let mut writer = tokio::io::BufWriter::with_capacity(16 * 1024, writer);
            let cancelled = receipt_shutdown;
            loop {
                let send = tokio::select! {
                    _ = cancelled.cancelled() => break,
                    next = receipt_rx.recv_async() => next,
                };
                let Ok((quad, receipt)) = send else { break };
                let Ok(payload) = serde_json::to_vec(&receipt) else { break };
                if write_frame(&mut writer, quad, FrameKind::Receipt, &payload).await.is_err() {
                    break;
                }
                if writer.flush().await.is_err() {
                    break;
                }
            }
        });

        let mut served_quads: BTreeMap<Quad, flume::Sender<super::envelope::Envelope>> =
            BTreeMap::new();
        // Quads whose chain observed a forwarded Eos. A connection that ends
        // WITHOUT Eos for a served quad is an upstream death mid-stream, not a
        // clean finish: the chain must fail (at-least-once) instead of
        // silently publishing a prefix and reporting the subgraph finished.
        let mut eos_seen: BTreeSet<Quad> = BTreeSet::new();
        let cancelled = self.shutdown.child_token();
        let failure_reason: Result<(), Error> = loop {
            let frame = tokio::select! {
                _ = cancelled.cancelled() => break Ok(()),
                frame = read_frame(&mut reader) => frame,
            };
            match frame {
                Ok((header, payload)) => {
                    let sender = match wait_for_route(
                        &self.inbound,
                        &mut served_quads,
                        header.quad,
                        REGISTRATION_GRACE,
                    )
                    .await
                    {
                        Some(sender) => sender,
                        None => break Err(Error::Process(format!(
                            "inbound frame for unregistered quad {:?}",
                            header.quad
                        ))),
                    };
                    match header.kind {
                        FrameKind::Data => {
                            let (batch, seq) = match decoder.decode(&payload) {
                                Ok(decoded) => decoded,
                                Err(error) => break Err(Error::Process(format!(
                                    "inbound data frame decode failed: {error}"
                                ))),
                            };
                            let envelope = Envelope::Data(
                                Arc::new(batch),
                                Arc::new(RemoteAck {
                                    outbox: receipt_tx.clone(),
                                    quad: header.quad,
                                    seq,
                                }),
                            );
                            if sender.send_async(envelope).await.is_err() {
                                break Ok(()); // local chain gone; close the edge
                            }
                        }
                        FrameKind::Signal => {
                            let Ok(signal) =
                                serde_json::from_slice::<WireSignal>(&payload)
                            else {
                                break Err(Error::Process(
                                    "inbound signal frame malformed".into(),
                                ));
                            };
                            if matches!(signal, WireSignal::Eos) {
                                eos_seen.insert(header.quad);
                            }
                            if sender.send_async(signal.into_envelope()).await.is_err() {
                                break Ok(());
                            }
                        }
                        FrameKind::Receipt => {
                            break Err(Error::Process(
                                "receipt frame on an inbound (downstream) connection".into(),
                            ));
                        }
                    }
                }
                Err(_) => break Ok(()), // peer closed or protocol error: end the edge
            }
        };
        // Dropping the served quads' senders closes the chains' input channels.
        // A quad whose connection died without a forwarded Eos is an upstream
        // death mid-stream: surface a failure so the chain (via its edge
        // watcher) fails closed instead of treating the drop as a clean
        // upstream finish and publishing a prefix.
        for (quad, sender) in &served_quads {
            if !eos_seen.contains(quad) {
                let _ = self.failures.send(Error::Process(format!(
                    "remote edge for quad {quad:?} ended without Eos; upstream likely died mid-stream"
                )));
                let _ = sender;
            }
        }
        // Only entries this connection inserted are removed; a reconnecting
        // upstream re-registers through the graph anyway.
        for (quad, sender) in served_quads {
            let mut registry = self.inbound.write().expect("inbound registry lock");
            if registry
                .get(&quad)
                .is_some_and(|registered| registered.same_channel(&sender))
            {
                registry.remove(&quad);
            }
        }
        receipt_writer_handle.abort();
        if let Err(error) = failure_reason {
            let _ = self.failures.send(error);
        }
    }
}

use std::collections::BTreeMap;

fn self_remove_outbound(manager: &NetworkManager, quad: Quad) {
    manager
        .outbound
        .write()
        .expect("outbound registry lock")
        .remove(&quad);
}

/// Looks up (and caches on first sight) the local input channel serving a
/// quad on this connection.
fn resolve_route(
    registry: &std::sync::RwLock<BTreeMap<Quad, flume::Sender<super::envelope::Envelope>>>,
    served_quads: &mut BTreeMap<Quad, flume::Sender<super::envelope::Envelope>>,
    quad: Quad,
) -> Option<flume::Sender<super::envelope::Envelope>> {
    if let Some(sender) = served_quads.get(&quad) {
        return Some(sender.clone());
    }
    let registered = registry
        .read()
        .expect("inbound registry lock")
        .get(&quad)
        .cloned()?;
    served_quads.insert(quad, registered.clone());
    Some(registered)
}

/// How long an inbound connection waits for the peer quad's local channel to
/// be registered: Agents build their graphs independently, so an upstream may
/// connect (its transport retries only the connect itself) before the
/// downstream agent finished wiring its side of the edge.
const REGISTRATION_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

async fn wait_for_route(
    registry: &std::sync::RwLock<BTreeMap<Quad, flume::Sender<super::envelope::Envelope>>>,
    served_quads: &mut BTreeMap<Quad, flume::Sender<super::envelope::Envelope>>,
    quad: Quad,
    grace: std::time::Duration,
) -> Option<flume::Sender<super::envelope::Envelope>> {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        if let Some(sender) = resolve_route(registry, served_quads, quad) {
            return Some(sender);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Drains one quad's edge channel onto its connection, stamping sequences and
/// registering pending receipts. Exits when the channel closes (drained) or
/// the connection fails; failures abort all pending branch acknowledgements.
async fn pump_edge(
    receiver: flume::Receiver<super::envelope::Envelope>,
    writer: impl AsyncWrite + Unpin + Send + 'static,
    quad: Quad,
    pending: Arc<PendingReceipts>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), Error> {
    let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, writer);
    let mut encoder = DataEncoder::new();
    let mut flush_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let result: Result<(), Error> = loop {
        tokio::select! {
            _ = shutdown.cancelled() => break Ok(()),
            _ = flush_tick.tick() => {
                if writer.flush().await.is_err() {
                    break Err(Error::Process("remote edge flush failed".into()));
                }
            }
            envelope = receiver.recv_async() => {
                let Ok(envelope) = envelope else { break Ok(()) };
                match envelope {
                    Envelope::Data(batch, branch) => {
                        // Register before writing so a concurrent receipt can
                        // never race the pending map; the wire is strict FIFO
                        // per quad, so receipts cannot precede their frame.
                        let seq = pending.register(branch, 1);
                        let payload = match encoder.encode(&batch, seq) {
                            Ok(payload) => payload,
                            Err(error) => break Err(error),
                        };
                        if write_frame(&mut writer, quad, FrameKind::Data, &payload)
                            .await
                            .is_err()
                        {
                            break Err(Error::Process("remote edge data write failed".into()));
                        }
                    }
                    other => {
                        let Some(signal) = WireSignal::from_envelope(&other) else {
                            continue;
                        };
                        let payload = match serde_json::to_vec(&signal) {
                            Ok(bytes) => bytes,
                            Err(error) => break Err(Error::Process(format!(
                                "signal frame encode failed: {error}"
                            ))),
                        };
                        // Control elements broadcast on every quad of the edge;
                        // each pump owns exactly one quad, so one frame each.
                        if write_frame(&mut writer, quad, FrameKind::Signal, &payload)
                            .await
                            .is_err()
                        {
                            break Err(Error::Process("remote edge signal write failed".into()));
                        }
                    }
                }
            }
        }
    };
    let _ = writer.flush().await;
    if result.is_err() {
        pending.abort_all();
    }
    result
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::Ack as _;
    use datafusion::arrow::array::{DictionaryArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    fn dictionary_batch(input_name: Option<&str>) -> MessageBatch {
        let dictionary: DictionaryArray<Int32Type> = vec!["alpha", "beta", "alpha"]
            .into_iter()
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "label",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(dictionary),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid batch");
        let mut message_batch = MessageBatch::new_arrow(batch);
        message_batch.set_input_name(input_name.map(str::to_owned));
        message_batch
    }

    #[test]
    fn header_roundtrips_and_rejects_garbage() {
        let quad = Quad {
            src_op: 12_341,
            src_subtask: 3,
            dst_op: 908,
            dst_subtask: 17,
        };
        let mut bytes = Vec::new();
        FrameHeader {
            quad,
            len: 512,
            kind: FrameKind::Signal,
        }
        .encode_into(&mut bytes);
        assert_eq!(bytes.len(), FRAME_HEADER_LEN);
        let decoded = FrameHeader::decode(&bytes).expect("valid header");
        assert_eq!(decoded.quad, quad);
        assert_eq!(decoded.len, 512);
        assert_eq!(decoded.kind, FrameKind::Signal);

        // A zeroed header (kind 0) is invalid rather than misparsed.
        let zeroed = vec![0u8; FRAME_HEADER_LEN];
        assert!(FrameHeader::decode(&zeroed).is_err());
        // A declared length above the bound is rejected.
        let mut oversized = Vec::new();
        FrameHeader {
            quad,
            len: MAX_FRAME_LEN + 1,
            kind: FrameKind::Data,
        }
        .encode_into(&mut oversized);
        assert!(FrameHeader::decode(&oversized).is_err());
    }

    #[test]
    fn signals_and_receipts_roundtrip() {
        let barrier = WireSignal::Barrier(CheckpointBarrier {
            checkpoint_id: "c-7".into(),
            generation: 9,
        });
        let envelope = Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "c-7".into(),
            generation: 9,
        });
        assert_eq!(WireSignal::from_envelope(&envelope), Some(barrier.clone()));
        match barrier.clone().into_envelope() {
            Envelope::Barrier(rebuilt) => {
                assert_eq!(
                    rebuilt,
                    CheckpointBarrier {
                        checkpoint_id: "c-7".into(),
                        generation: 9
                    }
                );
            }
            _ => panic!("expected barrier envelope"),
        }

        for signal in [
            WireSignal::Watermark(1_724_000_000),
            WireSignal::Eos,
            barrier,
        ] {
            let bytes = serde_json::to_vec(&signal).expect("serialize");
            let decoded: WireSignal = serde_json::from_slice(&bytes).expect("deserialize");
            assert_eq!(decoded, signal);
        }

        for kind in [ReceiptKind::Acked, ReceiptKind::Held, ReceiptKind::Released] {
            let receipt = ReceiptFrame { kind, seq: 42 };
            let bytes = serde_json::to_vec(&receipt).expect("serialize");
            let decoded: ReceiptFrame = serde_json::from_slice(&bytes).expect("deserialize");
            assert_eq!(decoded, receipt);
        }
    }

    #[test]
    fn data_frames_roundtrip_with_dictionary_and_schema_cache() {
        let first = dictionary_batch(Some("kafka-in"));
        let second = dictionary_batch(None);

        let mut encoder = DataEncoder::new();
        let mut decoder = DataDecoder::new();

        let payload_a = encoder.encode(&first, 7).expect("encode first");
        let (decoded_a, seq_a) = decoder.decode(&payload_a).expect("decode first");
        assert_eq!(seq_a, 7);
        assert_eq!(decoded_a.get_input_name(), Some("kafka-in".to_owned()));
        assert_eq!(decoded_a.record_batch(), first.record_batch());

        // Second frame with the same schema reuses the cached schema and
        // dictionaries (the encoder emits neither again).
        let payload_b = encoder.encode(&second, 8).expect("encode second");
        let (decoded_b, seq_b) = decoder.decode(&payload_b).expect("decode second");
        assert_eq!(seq_b, 8);
        assert_eq!(decoded_b.get_input_name(), None);
        assert_eq!(decoded_b.record_batch(), second.record_batch());
    }

    #[test]
    fn decoder_rejects_frames_before_any_schema() {
        let mut encoder = DataEncoder::new();
        let mut decoder = DataDecoder::new();
        let payload = encoder.encode(&dictionary_batch(None), 0).expect("encode");
        // Strip the schema piece: corrupt meta to claim no schema is attached.
        let meta_len = u32::from_le_bytes(payload[0..4].try_into().expect("sliced")) as usize;
        let mut meta: WireDataMeta =
            serde_json::from_slice(&payload[4..4 + meta_len]).expect("meta");
        meta.has_schema = false;
        let rewritten = serde_json::to_vec(&meta).expect("meta encode");
        let mut corrupted = Vec::new();
        corrupted.extend_from_slice(&(rewritten.len() as u32).to_le_bytes());
        corrupted.extend_from_slice(&rewritten);
        corrupted.extend_from_slice(&payload[4 + meta_len..]);
        assert!(decoder.decode(&corrupted).is_err());
    }

    #[tokio::test]
    async fn frames_roundtrip_over_async_buffers() {
        let quad = Quad {
            src_op: 1,
            src_subtask: 0,
            dst_op: 2,
            dst_subtask: 5,
        };
        let (mut client, mut server) = tokio::io::duplex(256);
        write_frame(&mut client, quad, FrameKind::Receipt, b"stub")
            .await
            .expect("write frame");
        let (header, payload) = read_frame(&mut server).await.expect("read frame");
        assert_eq!(header.quad, quad);
        assert_eq!(header.kind, FrameKind::Receipt);
        assert_eq!(payload, b"stub");
    }

    // ------------------------------------------------------------------
    // End-to-end manager tests (task 2.4)
    // ------------------------------------------------------------------

    /// A branch acknowledgement that records lifecycle transitions so tests
    /// can assert receipt aggregation without a real source.
    #[derive(Default)]
    struct RecordingAck {
        acked: std::sync::atomic::AtomicBool,
        aborted: std::sync::atomic::AtomicBool,
        held: std::sync::atomic::AtomicBool,
        released: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::input::Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn mark_held(&self) {
            self.held.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn release_held(&self) {
            self.released.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        async fn abort(&self) -> Result<(), Error> {
            self.aborted.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn quad_a_to_b() -> Quad {
        Quad {
            src_op: 1,
            src_subtask: 0,
            dst_op: 2,
            dst_subtask: 0,
        }
    }

    async fn next_envelope(rx: &flume::Receiver<Envelope>) -> Envelope {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv_async())
            .await
            .expect("envelope within timeout")
            .expect("channel open")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn data_barrier_receipts_flow_end_to_end() {
        let quad = quad_a_to_b();
        let upstream = NetworkManager::new(64);
        let downstream = NetworkManager::new(64);
        upstream.spawn();
        downstream.spawn();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
        downstream.register_inbound(quad, input_tx);

        // Loopback wiring: the upstream connects to a duplex half whose other
        // half is fed into the downstream manager as an accepted stream.
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

        // Data with a recorded branch acknowledgement.
        let branch = std::sync::Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                std::sync::Arc::new(dictionary_batch(Some("in-a"))),
                branch.clone(),
            ))
            .await
            .expect("send data");

        let received = next_envelope(&input_rx).await;
        let (batch, ack) = match received {
            Envelope::Data(batch, ack) => (batch, ack),
            _other => panic!("expected data envelope, got a different variant"),
        };
        assert_eq!(batch.get_input_name(), Some("in-a".to_owned()));
        assert!(!branch.acked.load(std::sync::atomic::Ordering::SeqCst));

        // Downstream processes: acking the RemoteAck mirrors the receipt back
        // and completes the upstream branch.
        ack.ack().await.expect("remote ack");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("branch acked via receipt");

        // Signals ride the same edge in FIFO position.
        edge.sender
            .send_async(Envelope::Barrier(CheckpointBarrier {
                checkpoint_id: "c-1".into(),
                generation: 1,
            }))
            .await
            .expect("send barrier");
        edge.sender.send_async(Envelope::Watermark(42)).await.expect("send watermark");
        edge.sender.send_async(Envelope::Eos).await.expect("send eos");
        for expected in ["barrier", "watermark", "eos"] {
            match next_envelope(&input_rx).await {
                Envelope::Barrier(_) => assert_eq!(expected, "barrier"),
                Envelope::Watermark(ms) => {
                    assert_eq!((expected, ms), ("watermark", 42));
                }
                Envelope::Eos => assert_eq!(expected, "eos"),
                Envelope::Data(_, _) => panic!("unexpected data"),
            }
        }

        // Held/Released mirror as parent transitions on the branch.
        let branch2 = std::sync::Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                std::sync::Arc::new(dictionary_batch(None)),
                branch2.clone(),
            ))
            .await
            .expect("send data 2");
        let received = next_envelope(&input_rx).await;
        let Envelope::Data(_, ack2) = received else {
            panic!("expected data");
        };
        ack2.mark_held();
        ack2.release_held();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !branch2.held.load(std::sync::atomic::Ordering::SeqCst)
                || !branch2.released.load(std::sync::atomic::Ordering::SeqCst)
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("held/released mirrored");

        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disconnect_aborts_pending_and_closes_edge() {
        let quad = quad_a_to_b();
        let upstream = NetworkManager::new(64);
        let downstream = NetworkManager::new(64);
        upstream.spawn();
        downstream.spawn();
        let failures = upstream.failure_receiver();

        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

        let branch = std::sync::Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                std::sync::Arc::new(dictionary_batch(None)),
                branch.clone(),
            ))
            .await
            .expect("send data");

        // Kill the downstream manager: the connection dies under the pending
        // receipt. The pump fails (or the drain tick flush fails), the pending
        // branch aborts, and the failure surfaces on the manager.
        downstream.shutdown();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pending branch aborted on disconnect");
        assert!(
            !branch.acked.load(std::sync::atomic::Ordering::SeqCst),
            "an aborted branch must never complete its source ack"
        );
        let failure = tokio::time::timeout(std::time::Duration::from_secs(5), failures.recv_async())
            .await
            .expect("failure within timeout")
            .expect("failure channel open");
        assert!(failure.to_string().contains("remote edge") || failure.to_string().contains("inbound"));

        // The edge channel sender fails for new sends after the pump exits.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let branch_retry = std::sync::Arc::new(RecordingAck::default());
                if edge
                    .sender
                    .send_async(Envelope::Data(
                        std::sync::Arc::new(dictionary_batch(None)),
                        branch_retry,
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("edge sender eventually fails");

        upstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_consumer_backpressures_the_edge_sender() {
        // The load-bearing invariant: a stalled downstream must halt the
        // upstream sender instead of growing buffers without bound. The
        // bounded edge channel fills, then the pump blocks on the socket
        // (BufWriter + duplex window), so further sends pend.
        let quad = quad_a_to_b();
        // Upstream edge capacity 1 + downstream input capacity 1: any third
        // in-flight envelope must pend.
        let upstream = NetworkManager::new(1);
        let downstream = NetworkManager::new(64);
        upstream.spawn();
        downstream.spawn();

        // Downstream input capacity 1 makes the bounded-buffer assertion
        // deterministic: one envelope accepted, then pressure must reach the
        // upstream sender (channel bound -> pump write -> TCP window).
        let (input_tx, input_rx) = flume::bounded::<Envelope>(1);
        downstream.register_inbound(quad, input_tx);

        // A tiny duplex window so one large batch saturates pump + socket.
        let (client_side, server_side) = tokio::io::duplex(16 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

        let big_batch = Arc::new(MessageBatch::new_arrow(large_batch(4_000)));
        // Send until one send PENDS. The in-flight budget is finite and small
        // (upstream channel 1 + pump-held 1 + downstream channel 1 + the
        // serve-held envelope + socket buffers), so with a never-draining
        // consumer some send must block after a bounded number of batches —
        // if sends kept completing, buffering would be unbounded.
        let mut blocked = None;
        for sequence in 0..32 {
            let send = edge
                .sender
                .send_async(Envelope::Data(
                    big_batch.clone(),
                    Arc::new(crate::input::NoopAck),
                ));
            match tokio::time::timeout(std::time::Duration::from_millis(200), send).await {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => panic!("edge failed prematurely at {sequence}: {error}"),
                Err(_elapsed) => {
                    // This send never completed within 200ms: backpressure
                    // reached the sender. Drop the future (the in-flight
                    // envelope is abandoned); the drain check below proves
                    // the edge still works for a fresh consumer.
                    blocked = Some(sequence);
                    break;
                }
            }
        }
        let blocked_at = blocked.expect(
            "no send ever blocked with a stalled consumer — buffers grew without bound",
        );
        assert!(
            blocked_at < 16,
            "backpressure engaged only after {blocked_at} in-flight batches"
        );

        // Draining the consumer releases the pressure: the pending send
        // completes once the downstream channel accepts frames again.
        let mut received = 0;
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while received < 1 {
                if input_rx.try_recv().is_ok() {
                    received += 1;
                }
            }
        })
        .await;

        upstream.shutdown();
        downstream.shutdown();
    }

    /// A lost Held receipt (synchronous outbox send on a dead edge) must be a
    /// harmless no-op at the ack site — degradation is the drain timeout
    /// upstream, never a panic or data loss here.
    #[tokio::test(flavor = "multi_thread")]
    async fn held_receipt_on_dead_edge_is_a_noop() {
        let (outbox, _outbox_rx) = flume::unbounded::<(Quad, ReceiptFrame)>();
        drop(_outbox_rx); // the edge writer is gone
        let ack = RemoteAck {
            outbox,
            quad: quad_a_to_b(),
            seq: 3,
        };
        ack.mark_held(); // must not panic on a closed outbox
        ack.release_held();
        assert!(ack.ack().await.is_err(), "acking a dead edge reports failure");
    }

    fn large_batch(rows: usize) -> datafusion::arrow::record_batch::RecordBatch {
        use datafusion::arrow::array::{LargeBinaryArray, Int64Array};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::LargeBinary, false),
        ]));
        let payloads: LargeBinaryArray = (0..rows)
            .map(|row| vec![(row % 256) as u8; 128])
            .collect::<Vec<Vec<u8>>>()
            .into_iter()
            .map(Some)
            .collect();
        datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(payloads),
            ],
        )
        .expect("valid large batch")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn peer_death_without_eos_fails_the_downstream_side() {
        // Spec: a remote edge that dies mid-stream terminates BOTH sides in
        // error. The upstream observes the write failure; here the downstream
        // side must surface a failure too — not treat the dropped connection
        // as a clean upstream finish.
        let quad = quad_a_to_b();
        let upstream = NetworkManager::new(8);
        let downstream = NetworkManager::new(8);
        upstream.spawn();
        downstream.spawn();
        let failures = downstream.failure_receiver();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);

        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(Some("prefix"))),
                Arc::new(crate::input::NoopAck),
            ))
            .await
            .expect("send data");
        let received = next_envelope(&input_rx).await;
        assert!(matches!(received, Envelope::Data(_, _)));

        // Drop the upstream side WITHOUT Eos: mid-stream death.
        drop(edge);
        upstream.shutdown();

        let failure = tokio::time::timeout(std::time::Duration::from_secs(5), failures.recv_async())
            .await
            .expect("failure within timeout")
            .expect("failure channel open");
        assert!(
            failure.to_string().contains("without Eos"),
            "expected an abnormal-termination failure, got: {failure}"
        );

        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tcp_transport_connects_and_receives() {
        let quad = quad_a_to_b();
        let upstream = NetworkManager::new(64);
        let downstream = NetworkManager::new(64);
        upstream.spawn();
        downstream.spawn();

        let port = downstream
            .bind_tcp("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("bind");
        let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
        downstream.register_inbound(quad, input_tx);

        let transport = TcpEdgeTransport {
            addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            max_attempts: 3,
        };
        let edge = upstream.open_edge(&transport, quad).await.expect("connect");

        let branch = std::sync::Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                std::sync::Arc::new(dictionary_batch(Some("tcp"))),
                branch.clone(),
            ))
            .await
            .expect("send");
        let received = next_envelope(&input_rx).await;
        let Envelope::Data(batch, ack) = received else {
            panic!("expected data over TCP");
        };
        assert_eq!(batch.get_input_name(), Some("tcp".to_owned()));
        ack.ack().await.expect("ack");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("receipt over TCP");

        // Task 2.4's cleanup claim, as an explicit assertion: after shutdown
        // the data-plane listener is gone — a fresh connection to the bound
        // port must be refused, not accepted into a dead backlog.
        upstream.shutdown();
        downstream.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let refused = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
        assert!(
            refused.is_err(),
            "data-plane listener still accepts after shutdown"
        );
    }
}
