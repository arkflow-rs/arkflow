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

use crate::checkpoint::CheckpointBarrier;
use crate::executor::envelope::Envelope;
use crate::Error;
use crate::MessageBatch;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use datafusion::arrow::ipc::reader::{read_dictionary, read_record_batch};
use datafusion::arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
};
use datafusion::arrow::ipc::{root_as_message, MessageHeader};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

/// Upper bound on one frame's payload. A frame declares its length up front;
/// anything past this bound is a protocol error rather than an allocation.
pub const MAX_FRAME_LEN: u32 = 1 << 28;

/// Version of the authenticated data-plane handshake.  A version mismatch
/// is a protocol error rather than a best-effort downgrade.
pub const DATA_PLANE_PROTOCOL_VERSION: u16 = 1;
const HANDSHAKE_NONCE_LEN: usize = 32;

/// Fixed frame header length: six little-endian `u32`s.
pub const FRAME_HEADER_LEN: usize = 24;

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
pub enum FrameKind {
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
        Self::decode_with_max(bytes, MAX_FRAME_LEN)
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

/// Credentials shared by the two Agents participating in a data-plane
/// session.  The secret is never serialized into a Job command or logged.
#[derive(Clone)]
pub struct DataPlaneCredentials {
    pub local_node: String,
    shared_secret: Arc<[u8]>,
}

impl std::fmt::Debug for DataPlaneCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataPlaneCredentials")
            .field("local_node", &self.local_node)
            .field("shared_secret", &"<redacted>")
            .finish()
    }
}

impl DataPlaneCredentials {
    pub fn new(
        local_node: impl Into<String>,
        shared_secret: impl AsRef<[u8]>,
    ) -> Result<Self, Error> {
        let shared_secret = shared_secret.as_ref();
        if shared_secret.is_empty() {
            return Err(Error::Config(
                "network shuffle data-plane secret must not be empty".into(),
            ));
        }
        Ok(Self {
            local_node: local_node.into(),
            shared_secret: Arc::from(shared_secret.to_vec().into_boxed_slice()),
        })
    }

    fn session(
        &self,
        peer_node: impl Into<String>,
        job_id: impl Into<String>,
        generation: u64,
        quad: Quad,
    ) -> SessionAuth {
        SessionAuth {
            source_node: self.local_node.clone(),
            destination_node: peer_node.into(),
            job_id: job_id.into(),
            generation,
            quad,
            shared_secret: self.shared_secret.clone(),
        }
    }
}

/// Expected identity for an inbound quad registration.  The manager's local
/// node and secret are process-wide; the peer, Job, and generation are bound
/// when the Agent builds the particular Job graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerExpectation {
    pub source_node: String,
    pub job_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct JobSessionKey {
    job_id: String,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeSessionKey {
    job: Option<JobSessionKey>,
    quad: Quad,
}

impl EdgeSessionKey {
    fn legacy(quad: Quad) -> Self {
        Self { job: None, quad }
    }

    fn for_job(job_id: impl Into<String>, generation: u64, quad: Quad) -> Self {
        Self {
            job: Some(JobSessionKey {
                job_id: job_id.into(),
                generation,
            }),
            quad,
        }
    }

    fn job(&self) -> Option<&JobSessionKey> {
        self.job.as_ref()
    }
}

impl PeerExpectation {
    fn edge_key(&self, quad: Quad) -> EdgeSessionKey {
        EdgeSessionKey::for_job(self.job_id.clone(), self.generation, quad)
    }
}

#[derive(Clone)]
struct SessionAuth {
    source_node: String,
    destination_node: String,
    job_id: String,
    generation: u64,
    quad: Quad,
    shared_secret: Arc<[u8]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HandshakePayload {
    protocol_version: u16,
    source_node: String,
    destination_node: String,
    job_id: String,
    generation: u64,
    quad: Quad,
    nonce: Vec<u8>,
    acknowledgement: bool,
    mac: Vec<u8>,
}

type HmacSha256 = Hmac<Sha256>;

impl SessionAuth {
    fn edge_key(&self) -> EdgeSessionKey {
        EdgeSessionKey::for_job(self.job_id.clone(), self.generation, self.quad)
    }

    fn client_handshake(&self) -> Result<HandshakePayload, Error> {
        let mut nonce = vec![0; HANDSHAKE_NONCE_LEN];
        rand::rng().fill(nonce.as_mut_slice());
        let mut payload = HandshakePayload {
            protocol_version: DATA_PLANE_PROTOCOL_VERSION,
            source_node: self.source_node.clone(),
            destination_node: self.destination_node.clone(),
            job_id: self.job_id.clone(),
            generation: self.generation,
            quad: self.quad,
            nonce,
            acknowledgement: false,
            mac: Vec::new(),
        };
        payload.mac = handshake_mac(&self.shared_secret, &payload, b"client");
        Ok(payload)
    }

    fn server_ack(&self, request: &HandshakePayload, local_node: &str) -> HandshakePayload {
        let mut response = HandshakePayload {
            protocol_version: DATA_PLANE_PROTOCOL_VERSION,
            source_node: local_node.to_owned(),
            destination_node: request.source_node.clone(),
            job_id: request.job_id.clone(),
            generation: request.generation,
            quad: request.quad,
            nonce: request.nonce.clone(),
            acknowledgement: true,
            mac: Vec::new(),
        };
        response.mac = handshake_mac(&self.shared_secret, &response, b"server");
        response
    }

    fn verify_server_ack(&self, payload: &HandshakePayload) -> Result<(), Error> {
        if payload.protocol_version != DATA_PLANE_PROTOCOL_VERSION
            || payload.source_node != self.destination_node
            || payload.destination_node != self.source_node
            || payload.job_id != self.job_id
            || payload.generation != self.generation
            || payload.quad != self.quad
            || !payload.acknowledgement
            || payload.nonce.len() != HANDSHAKE_NONCE_LEN
        {
            return Err(Error::Process(
                "remote edge handshake acknowledgement identity mismatch".into(),
            ));
        }
        verify_handshake_mac(&self.shared_secret, payload, b"server")
    }
}

fn handshake_mac(secret: &[u8], payload: &HandshakePayload, direction: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts every key length");
    mac.update(&canonical_handshake_bytes(payload, direction));
    mac.finalize().into_bytes().to_vec()
}

fn verify_handshake_mac(
    secret: &[u8],
    payload: &HandshakePayload,
    direction: &[u8],
) -> Result<(), Error> {
    let expected = handshake_mac(secret, payload, direction);
    if expected.as_slice().ct_eq(payload.mac.as_slice()).into() {
        Ok(())
    } else {
        Err(Error::Process(
            "remote edge handshake credential mismatch".into(),
        ))
    }
}

fn canonical_handshake_bytes(payload: &HandshakePayload, direction: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&payload.protocol_version.to_le_bytes());
    for value in [
        payload.source_node.as_bytes(),
        payload.destination_node.as_bytes(),
        payload.job_id.as_bytes(),
    ] {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value);
    }
    bytes.extend_from_slice(&payload.generation.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.src_op.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.src_subtask.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.dst_op.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.dst_subtask.to_le_bytes());
    bytes.extend_from_slice(&(payload.nonce.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&payload.nonce);
    bytes.push(u8::from(payload.acknowledgement));
    bytes.extend_from_slice(&(direction.len() as u64).to_le_bytes());
    bytes.extend_from_slice(direction);
    bytes
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
/// checkpoint barrier draining exactly as it would for a local window;
/// `Failed` reports that the receiving side aborted the delivery (processor
/// failure), letting the upstream abort the branch immediately instead of
/// waiting for the barrier-drain timeout.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptKind {
    Acked,
    Held,
    Released,
    Failed,
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
            let message = root_as_message(piece.flatbuf()).map_err(|error| {
                Error::Process(format!("data frame IPC message invalid: {error}"))
            })?;
            let body_len = usize::try_from(message.bodyLength())
                .map_err(|_| Error::Process("negative IPC body length".into()))?;
            let body = piece.body(body_len)?;
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

    fn body(&self, body_len: usize) -> Result<Buffer, Error> {
        let start = 8 + self.flatbuf_len;
        let end = start
            .checked_add(body_len)
            .ok_or_else(|| Error::Process("IPC body length overflows".into()))?;
        let body = self
            .bytes
            .get(start..end)
            .ok_or_else(|| Error::Process("IPC body exceeds IPC piece".into()))?;
        Ok(Buffer::from_vec(body.to_vec()))
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
    let flatbuf_end = 8usize
        .checked_add(flatbuf_len)
        .ok_or_else(|| Error::Process("IPC piece flatbuf length overflows".into()))?;
    if flatbuf_end > total {
        return Err(Error::Process(
            "IPC piece flatbuf exceeds piece length".into(),
        ));
    }
    Ok((Piece { bytes, flatbuf_len }, end))
}

fn peek_u32(payload: &[u8], offset: usize) -> Result<u32, Error> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| Error::Process("payload length overflows".into()))?;
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
    read_frame_with_limits(reader, MAX_FRAME_LEN, None).await
}

/// Reads a frame while applying the configured payload and per-read idle
/// limits.  The header is decoded before allocating the payload buffer.
pub async fn read_frame_with_limits(
    reader: &mut (impl AsyncRead + Unpin),
    max_frame_len: u32,
    idle_timeout: Option<std::time::Duration>,
) -> Result<(FrameHeader, Vec<u8>), Error> {
    let mut header_bytes = vec![0u8; FRAME_HEADER_LEN];
    read_exact_with_idle(reader, &mut header_bytes, idle_timeout)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading header: {error}"))
        })?;
    let header = FrameHeader::decode_with_max(&header_bytes, max_frame_len)?;
    let mut payload = vec![0u8; header.len as usize];
    read_exact_with_idle(reader, &mut payload, idle_timeout)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading payload: {error}"))
        })?;
    Ok((header, payload))
}

/// Outcome of a tiered receipt-channel read: a full frame, or a
/// dead-peer-idle timeout that raced a pending-batch registration and may be
/// retried under the long budget without corrupting the byte stream.
enum TieredFrame {
    Frame((FrameHeader, Vec<u8>)),
    /// The short (empty-pending) budget elapsed with zero bytes consumed,
    /// and a registration appeared during the wait — restart the read; the
    /// next pass uses the receipt-wait budget.
    RacedIdleRestart,
}

/// Receipt-channel frame read with a two-level idle budget: a connection
/// that still awaits receipts belongs to a (possibly slow) live downstream
/// and gets the long budget; an idle one keeps fast dead-peer discovery.
/// Zero bytes are consumed on the restart path, so retrying is framing-safe;
/// after the first byte the frame always completes under the long budget.
async fn read_receipt_frame(
    reader: &mut (impl AsyncRead + Unpin),
    max_frame_len: u32,
    short_idle: std::time::Duration,
    receipt_wait: std::time::Duration,
    pending: &PendingReceipts,
) -> Result<TieredFrame, Error> {
    let had_pending = !pending.is_empty();
    let budget = if had_pending {
        receipt_wait
    } else {
        short_idle
    };
    let mut header_bytes = vec![0u8; FRAME_HEADER_LEN];
    match timeout(budget, reader.read(&mut header_bytes[..1])).await {
        Err(_) => {
            if !had_pending && !pending.is_empty() {
                return Ok(TieredFrame::RacedIdleRestart);
            }
            return Err(Error::Process(
                "remote edge closed while reading header: read idle timeout".into(),
            ));
        }
        Ok(Err(error)) => {
            return Err(Error::Process(format!(
                "remote edge closed while reading header: {error}"
            )));
        }
        Ok(Ok(0)) => {
            return Err(Error::Process(
                "remote edge closed while reading header: early eof".into(),
            ));
        }
        Ok(Ok(_)) => {}
    }
    read_exact_with_idle(reader, &mut header_bytes[1..], Some(receipt_wait))
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading header: {error}"))
        })?;
    let header = FrameHeader::decode_with_max(&header_bytes, max_frame_len)?;
    let mut payload = vec![0u8; header.len as usize];
    read_exact_with_idle(reader, &mut payload, Some(receipt_wait))
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading payload: {error}"))
        })?;
    Ok(TieredFrame::Frame((header, payload)))
}

async fn read_exact_with_idle(
    reader: &mut (impl AsyncRead + Unpin),
    buffer: &mut [u8],
    idle_timeout: Option<std::time::Duration>,
) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        let read = match idle_timeout {
            Some(idle_timeout) => timeout(idle_timeout, reader.read(&mut buffer[offset..]))
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "read idle timeout")
                })??,
            None => reader.read(&mut buffer[offset..]).await?,
        };
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "early eof",
            ));
        }
        // A successful partial read is progress.  The next iteration starts a
        // fresh idle timer instead of charging the peer for the whole frame.
        offset += read;
    }
    Ok(())
}

/// Writes one frame: header then payload, ideally buffered behind the caller's
/// `BufWriter` so small frames coalesce until the flush tick.
pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
) -> Result<(), Error> {
    write_frame_with_limit(writer, quad, kind, payload, MAX_FRAME_LEN).await
}

pub async fn write_frame_with_limit(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
    max_frame_len: u32,
) -> Result<(), Error> {
    if (payload.len() as u64) > u64::from(max_frame_len) {
        return Err(Error::Process(format!(
            "remote edge frame payload of {} bytes exceeds the {} limit",
            payload.len(),
            max_frame_len
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
    /// When set, the outbound connection completes the fleet-CA mTLS
    /// handshake before the edge protocol runs. Handshake failures share
    /// the same retry/backoff budget as TCP failures.
    pub tls: Option<DataPlaneTlsConfig>,
}

#[async_trait::async_trait]
impl EdgeTransport for TcpEdgeTransport {
    async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
        let max_attempts = self.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..max_attempts {
            let stream = match tokio::net::TcpStream::connect(self.addr).await {
                Ok(stream) => stream,
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
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let stream = match &self.tls {
                Some(tls) => match tls.connect(stream).await {
                    Ok(tls_stream) => Box::new(tls_stream) as Box<dyn RemoteStream>,
                    Err(error) => {
                        let backoff = connect_backoff(attempt);
                        tracing::warn!(
                            "remote edge TLS connect to {} failed (attempt {}/{}): {error}; retrying in {backoff:?}",
                            self.addr,
                            attempt + 1,
                            max_attempts
                        );
                        last_error = Some(std::io::Error::other(error.to_string()));
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                },
                None => Box::new(stream) as Box<dyn RemoteStream>,
            };
            return Ok(stream);
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

/// Plaintext transport constructor for tests and existing call sites; the
/// TLS-carrying form is built by the graph from the job's edge context.
impl TcpEdgeTransport {
    pub fn plaintext(addr: std::net::SocketAddr, max_attempts: usize) -> Self {
        Self {
            addr,
            max_attempts,
            tls: None,
        }
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

struct FailureChannel {
    sender: flume::Sender<Error>,
    receiver: flume::Receiver<Error>,
}

struct ConnectionFailure {
    job: Option<JobSessionKey>,
    error: Error,
}

impl From<Error> for ConnectionFailure {
    fn from(error: Error) -> Self {
        Self { job: None, error }
    }
}

/// TLS material for the cross-node data plane: the local node's
/// certificate and key plus the fleet CA that anchors peer verification.
/// Present = mTLS in both directions (the server requires a client
/// certificate chained to the same CA; the client verifies the server the
/// same way under the fixed name below). The HMAC session handshake still
/// runs on top: TLS is transport encryption plus a "certificate issued by
/// the fleet CA" gate, while node identity and job/generation binding stay
/// with the application-layer handshake.
#[derive(Clone)]
pub struct DataPlaneTlsConfig {
    connector: std::sync::Arc<tokio_rustls::TlsConnector>,
    acceptor: std::sync::Arc<tokio_rustls::TlsAcceptor>,
}

/// The fixed SNI/ServerName the outbound side verifies and node
/// certificates must carry as a SAN. Node identity itself is proven by the
/// HMAC handshake, so the name is only the chain-verification carrier.
pub const DATA_PLANE_TLS_SERVER_NAME: &str = "arkflow-data-plane";

impl DataPlaneTlsConfig {
    /// Build both directions from PEM material. Any parse or key-mismatch
    /// error is explicit: a half-loaded TLS config must fail startup, not
    /// silently degrade to plaintext.
    pub fn from_pem(cert_pem: &str, key_pem: &str, ca_pem: &str) -> Result<Self, Error> {
        // Feature unification across the workspace can enable more than one
        // rustls crypto provider; pin ring explicitly (idempotent).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let fail = |context: &str, error: &str| {
            Error::Config(format!("data-plane TLS {context}: {error}"))
        };
        let read_certs =
            |pem: &str,
             context: &str|
             -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Error> {
                let mut reader = std::io::BufReader::new(pem.as_bytes());
                let mut certs = Vec::new();
                for result in rustls_pemfile::certs(&mut reader) {
                    certs.push(result.map_err(|error| fail(context, &error.to_string()))?);
                }
                if certs.is_empty() {
                    return Err(fail(context, "no PEM certificates found"));
                }
                Ok(certs)
            };
        let cert_chain = read_certs(cert_pem, "certificate")?;
        let ca_certs = read_certs(ca_pem, "fleet CA")?;
        let private_key = {
            let mut reader = std::io::BufReader::new(key_pem.as_bytes());
            rustls_pemfile::private_key(&mut reader)
                .map_err(|error| fail("private key", &error.to_string()))?
                .ok_or_else(|| fail("private key", "no PEM private key found"))?
        };
        let mut ca_store = rustls::RootCertStore::empty();
        ca_store.add_parsable_certificates(ca_certs.clone());
        if ca_store.is_empty() {
            return Err(fail("fleet CA", "no parsable certificates"));
        }
        let ca_store_for_client = ca_store.clone();
        let server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(ca_store))
                    .build()
                    .map_err(|error| fail("client verifier", &error.to_string()))?,
            )
            .with_single_cert(cert_chain.clone(), private_key.clone_key())
            .map_err(|error| fail("server config", &error.to_string()))?;
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(ca_store_for_client)
            .with_client_auth_cert(cert_chain, private_key)
            .map_err(|error| fail("client config", &error.to_string()))?;
        Ok(Self {
            connector: std::sync::Arc::new(tokio_rustls::TlsConnector::from(std::sync::Arc::new(
                client_config,
            ))),
            acceptor: std::sync::Arc::new(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
                server_config,
            ))),
        })
    }

    /// Outbound TLS wrap of an established TCP stream.
    pub async fn connect(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, Error> {
        let name = rustls::pki_types::ServerName::try_from(DATA_PLANE_TLS_SERVER_NAME.to_owned())
            .map_err(|error| fail_tls_name(&error.to_string()))?;
        self.connector
            .connect(name, stream)
            .await
            .map_err(|error| Error::Process(format!("data-plane TLS connect failed: {error}")))
    }

    /// Inbound TLS handshake of an accepted TCP stream.
    pub async fn accept(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, Error> {
        self.acceptor
            .accept(stream)
            .await
            .map_err(|error| Error::Process(format!("data-plane TLS accept failed: {error}")))
    }
}

fn fail_tls_name(error: &str) -> Error {
    Error::Config(format!("data-plane TLS server name invalid: {error}"))
}

/// Bounded resource and authentication policy for one node's data plane.
#[derive(Clone)]
pub struct NetworkManagerConfig {
    pub channel_capacity: usize,
    pub max_connections: usize,
    pub max_pending_receipts: usize,
    pub max_receipt_queue: usize,
    pub max_failure_queue: usize,
    pub max_frame_len: u32,
    pub read_idle_timeout: std::time::Duration,
    /// Receipt-read idle budget while batches still await receipts. A slow
    /// downstream withholds receipts for in-flight work; the short
    /// `read_idle_timeout` is for dead-peer discovery on otherwise idle
    /// connections and must not tear down a busy one.
    pub receipt_wait_timeout: std::time::Duration,
    /// Approximate byte budget for retained replay batches. Rejection takes
    /// the existing send-failure path; the bound is an order-of-magnitude
    /// guard, not an exact quota.
    pub max_pending_bytes: u64,
    /// Bounded transparent reconnect: how many times an upstream edge
    /// redials the peer after a stream failure before failing the edge
    /// closed. 0 disables reconnect and preserves the immediate fail-closed
    /// behavior byte for byte.
    pub reconnect_attempts: usize,
    /// How long the downstream side waits for a re-registering connection
    /// after losing one without Eos before reporting the loss as a failure.
    pub reconnect_grace: std::time::Duration,
    pub registration_grace: std::time::Duration,
    /// How long a successfully authenticated client nonce remains rejected.
    /// The cache closes the replay window without retaining handshake state
    /// forever; its capacity is bounded by the connection limit below.
    pub handshake_replay_ttl: std::time::Duration,
    pub credentials: Option<DataPlaneCredentials>,
    /// Fleet-CA-anchored mTLS for every cross-node connection. `None`
    /// (the default) keeps the plaintext protocol byte for byte.
    pub tls: Option<DataPlaneTlsConfig>,
}

impl Default for NetworkManagerConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            max_connections: 256,
            max_pending_receipts: 4096,
            max_receipt_queue: 4096,
            max_failure_queue: 1024,
            max_frame_len: MAX_FRAME_LEN,
            read_idle_timeout: std::time::Duration::from_secs(30),
            receipt_wait_timeout: std::time::Duration::from_secs(10 * 60),
            max_pending_bytes: 256 * 1024 * 1024,
            reconnect_attempts: 5,
            reconnect_grace: std::time::Duration::from_secs(10),
            registration_grace: std::time::Duration::from_secs(10),
            handshake_replay_ttl: std::time::Duration::from_secs(10 * 60),
            credentials: None,
            tls: None,
        }
    }
}

impl NetworkManagerConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.channel_capacity == 0
            || self.max_connections == 0
            || self.max_pending_receipts == 0
            || self.max_receipt_queue == 0
            || self.max_failure_queue == 0
            || self.max_frame_len == 0
            || self.read_idle_timeout.is_zero()
            || self.receipt_wait_timeout.is_zero()
            || self.max_pending_bytes == 0
            || self.reconnect_grace.is_zero()
            || self.registration_grace.is_zero()
            || self.handshake_replay_ttl.is_zero()
        {
            return Err(Error::Config(
                "network shuffle resource limits must be positive".into(),
            ));
        }
        if self.max_frame_len > MAX_FRAME_LEN {
            return Err(Error::Config(format!(
                "network shuffle max_frame_len must not exceed {MAX_FRAME_LEN}"
            )));
        }
        if self
            .credentials
            .as_ref()
            .is_some_and(|credentials| credentials.local_node.trim().is_empty())
        {
            return Err(Error::Config(
                "network shuffle credential local_node must not be empty".into(),
            ));
        }
        Ok(())
    }
}

/// Mirrors one branch acknowledgement's lifecycle back across the wire. The
/// downstream chain wraps every decoded remote batch in one of these; the
/// kernel's existing fan-out and state wrappers compose on top of it exactly
/// as they do around local acknowledgements.
pub struct RemoteAck {
    outbox: flume::Sender<(Quad, ReceiptFrame)>,
    quad: Quad,
    seq: u64,
    failures: flume::Sender<Error>,
}

#[async_trait::async_trait]
impl crate::input::Ack for RemoteAck {
    async fn ack(&self) -> Result<(), Error> {
        let result = self
            .outbox
            .send_async((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Acked,
                    seq: self.seq,
                },
            ))
            .await;
        result.map_err(|_| Error::Process("remote edge receipt channel closed".into()))
    }

    async fn abort(&self) -> Result<(), Error> {
        // Mirror the abort to the upstream pending map as a Failed receipt so
        // the branch acknowledgement aborts immediately; the barrier-drain
        // timeout stays only as the fallback for a lost frame.
        self.outbox
            .send_async((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Failed,
                    seq: self.seq,
                },
            ))
            .await
            .map_err(|_| Error::Process("remote edge receipt channel closed".into()))
    }

    fn mark_held(&self) {
        // Synchronous by trait contract (called from the Aligner's sync
        // context).  A full failure queue must not block a tokio worker: a
        // full queue means the drain side has already stalled, and the edge
        // is torn down by the stalled read loop regardless.  Escalate
        // non-blockingly and let the connection teardown carry the signal.
        if self
            .outbox
            .try_send((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Held,
                    seq: self.seq,
                },
            ))
            .is_err()
            && self
                .failures
                .try_send(Error::Process(format!(
                    "remote edge receipt queue overflow for quad {:?}",
                    self.quad
                )))
                .is_err()
        {
            tracing::error!(
                quad = ?self.quad,
                seq = self.seq,
                "remote edge receipt queue and failure queue both full; dropping the overflow signal"
            );
        }
    }

    fn release_held(&self) {
        if self
            .outbox
            .try_send((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Released,
                    seq: self.seq,
                },
            ))
            .is_err()
            && self
                .failures
                .try_send(Error::Process(format!(
                    "remote edge receipt queue overflow for quad {:?}",
                    self.quad
                )))
                .is_err()
        {
            tracing::error!(
                quad = ?self.quad,
                seq = self.seq,
                "remote edge receipt queue and failure queue both full; dropping the overflow signal"
            );
        }
    }
}

/// One in-flight batch awaiting its replicas' receipts on a quad.
struct PendingBatch {
    branch: Arc<dyn crate::input::Ack>,
    /// Retained for transparent-reconnect replay until the acknowledgement
    /// completes; entries leave the map on completion, so an acknowledged
    /// frame never replays.
    replay: Option<crate::MessageBatchRef>,
    /// Approximate Arrow memory of `replay`, accounted against the byte
    /// budget while this entry lives in the map.
    bytes: u64,
    remaining_replicas: usize,
    held_replicas: usize,
}

/// Receipts for one outbound quad, shared between the outbound pump (which
/// registers pending batches) and the connection's receipt read loop.
struct PendingReceipts {
    map: std::sync::Mutex<BTreeMap<u64, PendingBatch>>,
    next_seq: std::sync::atomic::AtomicU64,
    pending_bytes: std::sync::atomic::AtomicU64,
    max_entries: usize,
    max_pending_bytes: u64,
}

impl PendingReceipts {
    #[cfg(test)]
    fn new(max_entries: usize) -> Self {
        Self::with_byte_budget(max_entries, u64::MAX)
    }

    fn with_byte_budget(max_entries: usize, max_pending_bytes: u64) -> Self {
        Self {
            map: std::sync::Mutex::new(BTreeMap::new()),
            next_seq: std::sync::atomic::AtomicU64::new(0),
            pending_bytes: std::sync::atomic::AtomicU64::new(0),
            max_entries,
            max_pending_bytes,
        }
    }

    fn register(
        &self,
        branch: &Arc<dyn crate::input::Ack>,
        replicas: usize,
        replay: Option<crate::MessageBatchRef>,
    ) -> Result<u64, Error> {
        // Approximate Arrow memory (buffers + validity), an order-of-magnitude
        // guard against a handful of huge retained batches blowing the heap.
        let bytes = replay
            .as_ref()
            .map(|batch| batch.record_batch().get_array_memory_size() as u64)
            .unwrap_or(0);
        let mut map = self.map.lock().expect("pending receipts lock");
        if map.len() >= self.max_entries {
            return Err(Error::Process(format!(
                "remote edge pending receipt limit {} reached",
                self.max_entries
            )));
        }
        let retained = self
            .pending_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        if retained.saturating_add(bytes) > self.max_pending_bytes {
            return Err(Error::Process(format!(
                "remote edge pending replay byte budget {} reached ({} bytes retained)",
                self.max_pending_bytes, retained
            )));
        }
        let seq = self
            .next_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.pending_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        map.insert(
            seq,
            PendingBatch {
                branch: branch.clone(),
                replay,
                bytes,
                remaining_replicas: replicas,
                held_replicas: 0,
            },
        );
        Ok(seq)
    }

    /// Release a completed entry's byte accounting back to the budget.
    fn release_bytes(&self, bytes: u64) {
        self.pending_bytes
            .fetch_sub(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_empty(&self) -> bool {
        self.map.lock().expect("pending receipts lock").is_empty()
    }

    /// Every unacknowledged batch in sequence order, for replay after a
    /// transparent reconnect. Entries leave the map when their acknowledgement
    /// completes, so this is exactly the set the peer may not have processed.
    fn replay_snapshot(&self) -> Vec<(u64, crate::MessageBatchRef)> {
        let map = self.map.lock().expect("pending receipts lock");
        map.iter()
            .filter_map(|(seq, pending)| pending.replay.clone().map(|batch| (*seq, batch)))
            .collect()
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
                    let PendingBatch { branch, bytes, .. } =
                        map.remove(&receipt.seq).expect("checked");
                    self.release_bytes(bytes);
                    // Acknowledge off the read loop: a durable commit can
                    // block, and the connection must keep draining.
                    let failures = failures.clone();
                    tokio::spawn(async move {
                        if let Err(error) = branch.ack().await {
                            let _ = failures
                                .send_async(Error::Process(format!(
                                    "remote edge branch acknowledgement failed: {error}"
                                )))
                                .await;
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
            ReceiptKind::Failed => {
                let PendingBatch { branch, bytes, .. } = map.remove(&receipt.seq).expect("checked");
                self.release_bytes(bytes);
                // Abort off the read loop: branch compensation may block on
                // journal/WAL undo, and the connection must keep draining.
                let failures = failures.clone();
                tokio::spawn(async move {
                    if let Err(error) = branch.abort().await {
                        let _ = failures
                            .send_async(Error::Process(format!(
                                "remote edge branch abort failed: {error}"
                            )))
                            .await;
                    }
                });
            }
        }
    }

    fn abort_all(&self) {
        let mut map = self.map.lock().expect("pending receipts lock");
        let drained: BTreeMap<u64, PendingBatch> = std::mem::take(&mut *map);
        // The whole table is gone; zero the byte accounting so the budget
        // cannot outlive the entries it was charging for (every caller drops
        // or replaces this instance right after, but the invariant should
        // not depend on instance death).
        self.pending_bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
        for (_, pending) in drained {
            let branch = pending.branch;
            tokio::spawn(async move {
                let _ = branch.abort().await;
            });
        }
    }
}

/// Capture the current span context as a W3C `traceparent` string, or `None`
/// when the current context carries no valid span — including tracing being
/// disabled entirely, which keeps barriers byte-identical on the wire.
pub fn capture_trace_context() -> Option<String> {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let cx = tracing::Span::current().context();
    if !cx.span().span_context().is_valid() {
        return None;
    }
    let mut carrier = std::collections::HashMap::new();
    opentelemetry_sdk::propagation::TraceContextPropagator::new().inject_context(&cx, &mut carrier);
    carrier.get("traceparent").cloned()
}

/// Extract a remote parent context from a barrier's captured `traceparent`.
/// Returns `None` for absent or unparsable values so a malformed hop can
/// never detach a downstream span from its local parent.
pub fn extract_trace_context(trace_context: &str) -> Option<opentelemetry::Context> {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;

    let mut carrier = std::collections::HashMap::new();
    carrier.insert("traceparent".to_string(), trace_context.to_string());
    let cx = opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&carrier);
    if cx.span().span_context().is_valid() {
        Some(cx)
    } else {
        None
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
    /// Job/generation/quad → local input channel (downstream side).
    inbound: std::sync::RwLock<BTreeMap<EdgeSessionKey, flume::Sender<super::envelope::Envelope>>>,
    /// Job/generation/quad → pending receipts (upstream side), consulted by
    /// receipt read loops.
    outbound: std::sync::RwLock<BTreeMap<EdgeSessionKey, Arc<PendingReceipts>>>,
    /// Job/generation/quad → expected authenticated upstream identity.
    inbound_auth: std::sync::RwLock<BTreeMap<EdgeSessionKey, PeerExpectation>>,
    /// Fatal edge errors (ack commit failures, aborted edges) for the kernel.
    failures: flume::Sender<Error>,
    failure_receiver: flume::Receiver<Error>,
    /// Per-Job failure channels. The process-wide channel is retained only for
    /// legacy, unauthenticated callers that do not carry a Job identity.
    failure_channels: std::sync::RwLock<BTreeMap<JobSessionKey, FailureChannel>>,
    /// Client nonces that have already completed MAC verification.  A client
    /// nonce is otherwise self-generated and therefore cannot prove freshness
    /// to the server on its own.  The TTL and capacity keep this replay guard
    /// bounded while rejecting duplicate handshakes during a live session.
    handshake_nonces: std::sync::Mutex<BTreeMap<Vec<u8>, std::time::Instant>>,
    /// The unauthenticated in-memory compatibility transport has no Job
    /// identity on the wire.  Keep it explicitly single-Job so two graphs
    /// cannot overwrite one another's quad routes.
    legacy_job: std::sync::RwLock<Option<JobSessionKey>>,
    /// Session-scoped receipt routing: RemoteAcks hand their receipts to a
    /// queue owned by the SESSION, not the connection, so an acknowledgement
    /// started on one connection still completes after a transparent
    /// reconnect swaps the wire underneath it. A forwarder task drains the
    /// queue into whichever connection is currently serving the session.
    session_receipts: std::sync::Mutex<BTreeMap<EdgeSessionKey, SessionReceiptRoute>>,
    /// Delivery-level dedup state per session key: the highest data-frame
    /// sequence number already routed into the local channel. A transparent
    /// reconnect replays every frame the upstream has not receipted, so the
    /// receiver drops (and mirror-acks) sequences at or below this mark
    /// instead of double-delivering them.
    delivered_seq: std::sync::Mutex<BTreeMap<EdgeSessionKey, u64>>,
    /// Per-session-key delivery locks serializing "dedup check → local
    /// channel send → delivered-seq advance". During a transparent
    /// reconnect two connections can serve the same key concurrently (the
    /// old one still parked on a backpressured send); without this lock
    /// both pass the dedup check for the same sequence, and the old
    /// connection's late completion overwrites the delivered mark with a
    /// lower value, letting later replays slip through the dedup.
    delivery_locks: std::sync::Mutex<BTreeMap<EdgeSessionKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Registration generation per session key, bumped whenever a connection
    /// successfully (re)routes the key. The reconnect grace watcher compares
    /// its snapshot against the current generation to decide whether the
    /// upstream came back.
    session_registrations: std::sync::Mutex<BTreeMap<EdgeSessionKey, u64>>,
    /// Accepted streams feed this queue so serving works over any transport.
    accepted: AcceptedQueue,
    config: NetworkManagerConfig,
    active_connections: std::sync::atomic::AtomicUsize,
    shutdown: tokio_util::sync::CancellationToken,
}

/// One session's receipt route: the queue RemoteAcks send into, plus the
/// connection-swappable writer slot the forwarder drains toward.
struct SessionReceiptRoute {
    queue: flume::Sender<(Quad, ReceiptFrame)>,
    current: std::sync::RwLock<Option<flume::Sender<(Quad, ReceiptFrame)>>>,
    forwarder: tokio_util::sync::CancellationToken,
}

impl NetworkManager {
    /// Look up (creating on first use) the session-scoped receipt route.
    /// The bounded queue applies backpressure to acknowledgements when the
    /// peer stops draining, and the forwarder retries failed connection
    /// writes until the next connection takes the slot.
    fn session_receipt_route(
        self: &Arc<Self>,
        key: &EdgeSessionKey,
    ) -> flume::Sender<(Quad, ReceiptFrame)> {
        let mut routes = self.session_receipts.lock().expect("session receipt lock");
        if let Some(route) = routes.get(key) {
            return route.queue.clone();
        }
        let (queue_tx, queue_rx) =
            flume::bounded::<(Quad, ReceiptFrame)>(self.config.max_receipt_queue);
        let cancel = tokio_util::sync::CancellationToken::new();
        let route = SessionReceiptRoute {
            queue: queue_tx.clone(),
            current: std::sync::RwLock::new(None),
            forwarder: cancel.clone(),
        };
        routes.insert(key.clone(), route);
        let manager = self.clone();
        let key = key.clone();
        let shutdown_watch = manager.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    // The manager's shutdown must also stop a forwarder that
                    // is merely parked waiting for receipts — otherwise the
                    // task (and its strong manager Arc) outlives shutdown.
                    _ = shutdown_watch.cancelled() => break,
                    item = queue_rx.recv_async() => {
                        let Ok(item) = item else { break };
                        // Forward to the current connection's writer; a dead
                        // slot (connection swapped or gone) retries until a
                        // live one appears or the session is torn down.
                        loop {
                            if manager.shutdown.is_cancelled() || cancel.is_cancelled() {
                                break;
                            }
                            let target = {
                                let routes = manager
                                    .session_receipts
                                    .lock()
                                    .expect("session receipt lock");
                                routes
                                    .get(&key)
                                    .and_then(|route| {
                                        route
                                            .current
                                            .read()
                                            .expect("session receipt slot lock")
                                            .clone()
                                    })
                            };
                            let Some(target) = target else {
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                continue;
                            };
                            match target.send_async(item).await {
                                Ok(()) => break,
                                // Writer gone: retry on the next slot.
                                Err(_) => {
                                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                }
                            }
                        }
                    }
                }
            }
        });
        queue_tx
    }

    /// Install `sender` as the live writer for a session's receipts (called
    /// by each connection that starts serving the key).
    fn set_session_receipt_writer(
        &self,
        key: &EdgeSessionKey,
        sender: Option<flume::Sender<(Quad, ReceiptFrame)>>,
    ) {
        let routes = self.session_receipts.lock().expect("session receipt lock");
        if let Some(route) = routes.get(key) {
            *route.current.write().expect("session receipt slot lock") = sender;
        }
    }

    /// Clear the session's writer slot only when it still belongs to
    /// `ours`. During a transparent reconnect the replacement connection
    /// can install its own sender before the dying connection's cleanup
    /// runs; an unconditional clear would clobber the live slot and stall
    /// every later receipt until the next reconnect.
    fn clear_session_receipt_writer_if_owned(
        &self,
        key: &EdgeSessionKey,
        ours: &flume::Sender<(Quad, ReceiptFrame)>,
    ) {
        let routes = self.session_receipts.lock().expect("session receipt lock");
        let Some(route) = routes.get(key) else {
            return;
        };
        let mut slot = route.current.write().expect("session receipt slot lock");
        if slot
            .as_ref()
            .is_some_and(|current| current.same_channel(ours))
        {
            *slot = None;
        }
    }

    /// Tear a session's receipt route down (session loss confirmed or job
    /// removed): queued acknowledgements no longer have a destination.
    fn remove_session_receipt_route(&self, key: &EdgeSessionKey) {
        let route = self
            .session_receipts
            .lock()
            .expect("session receipt lock")
            .remove(key);
        if let Some(route) = route {
            route.forwarder.cancel();
        }
    }
}

impl NetworkManager {
    pub fn new(channel_capacity: usize) -> Arc<Self> {
        let config = NetworkManagerConfig {
            channel_capacity: channel_capacity.max(1),
            ..NetworkManagerConfig::default()
        };
        Self::with_config(config).expect("default network manager configuration is valid")
    }

    pub fn with_config(config: NetworkManagerConfig) -> Result<Arc<Self>, Error> {
        config.validate()?;
        let (failure_sender, failure_receiver) = flume::bounded(config.max_failure_queue);
        let accepted = flume::bounded(config.max_connections);
        Ok(Arc::new(Self {
            inbound: std::sync::RwLock::new(BTreeMap::new()),
            outbound: std::sync::RwLock::new(BTreeMap::new()),
            inbound_auth: std::sync::RwLock::new(BTreeMap::new()),
            failures: failure_sender,
            failure_receiver,
            failure_channels: std::sync::RwLock::new(BTreeMap::new()),
            handshake_nonces: std::sync::Mutex::new(BTreeMap::new()),
            legacy_job: std::sync::RwLock::new(None),
            session_receipts: std::sync::Mutex::new(BTreeMap::new()),
            delivered_seq: std::sync::Mutex::new(BTreeMap::new()),
            delivery_locks: std::sync::Mutex::new(BTreeMap::new()),
            session_registrations: std::sync::Mutex::new(BTreeMap::new()),
            accepted,
            active_connections: std::sync::atomic::AtomicUsize::new(0),
            config,
            shutdown: tokio_util::sync::CancellationToken::new(),
        }))
    }

    /// Fatal edge failures for the kernel's failure reporter.
    pub fn failure_receiver(&self) -> flume::Receiver<Error> {
        self.failure_receiver.clone()
    }

    /// Fatal failures for one Job attempt.  A process-wide data-plane listener
    /// can serve several Jobs, so their failure notifications must not share a
    /// competing receiver: otherwise one Job can consume another Job's edge
    /// failure and cancel the wrong attempt.
    pub fn failure_receiver_for_job(
        &self,
        job_id: &str,
        generation: u64,
    ) -> flume::Receiver<Error> {
        if self.config.credentials.is_none() {
            // The legacy wire format has no Job/session identity, so its
            // failure path is intentionally process-wide as well.
            return self.failure_receiver();
        }
        let key = JobSessionKey {
            job_id: job_id.to_owned(),
            generation,
        };
        self.failure_receiver_for_job_key(&key)
    }

    fn failure_receiver_for_job_key(&self, key: &JobSessionKey) -> flume::Receiver<Error> {
        let mut channels = self
            .failure_channels
            .write()
            .expect("failure channel registry lock");
        channels
            .entry(key.clone())
            .or_insert_with(|| {
                let (sender, receiver) = flume::bounded(self.config.max_failure_queue);
                FailureChannel { sender, receiver }
            })
            .receiver
            .clone()
    }

    fn failure_sender_for_key(&self, key: &EdgeSessionKey) -> flume::Sender<Error> {
        let Some(job) = key.job() else {
            return self.failures.clone();
        };
        self.failure_sender_for_job_key(job)
    }

    fn failure_sender_for_job_key(&self, key: &JobSessionKey) -> flume::Sender<Error> {
        let mut channels = self
            .failure_channels
            .write()
            .expect("failure channel registry lock");
        channels
            .entry(key.clone())
            .or_insert_with(|| {
                let (sender, receiver) = flume::bounded(self.config.max_failure_queue);
                FailureChannel { sender, receiver }
            })
            .sender
            .clone()
    }

    fn failure_sender_for_registered_job(
        &self,
        key: &JobSessionKey,
    ) -> Option<flume::Sender<Error>> {
        self.failure_channels
            .read()
            .expect("failure channel registry lock")
            .get(key)
            .map(|channel| channel.sender.clone())
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
            .insert(EdgeSessionKey::legacy(quad), sender);
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .remove(&EdgeSessionKey::legacy(quad));
    }

    /// Registers a downstream quad together with the identity that is allowed
    /// to open it.  In-memory test managers without credentials retain the
    /// old transport behavior; configured data planes require this binding.
    pub fn register_inbound_for_session(
        &self,
        quad: Quad,
        sender: flume::Sender<super::envelope::Envelope>,
        expectation: PeerExpectation,
    ) -> Result<(), Error> {
        if self.config.credentials.is_none() {
            self.claim_legacy_job_key(JobSessionKey {
                job_id: expectation.job_id.clone(),
                generation: expectation.generation,
            })?;
            self.register_inbound(quad, sender);
            return Ok(());
        }
        let key = expectation.edge_key(quad);
        self.inbound
            .write()
            .expect("inbound registry lock")
            .insert(key.clone(), sender);
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .insert(key, expectation);
        Ok(())
    }

    fn remove_inbound_session(&self, key: &EdgeSessionKey) {
        self.inbound
            .write()
            .expect("inbound registry lock")
            .remove(key);
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .remove(key);
    }

    /// The per-session-key delivery lock (see `delivery_locks`). Shared by
    /// every connection serving the key, so the check→send→record sequence
    /// is atomic with respect to overlapping connections.
    fn delivery_lock(&self, key: &EdgeSessionKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.delivery_locks.lock().expect("delivery lock registry");
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn claim_legacy_job_key(&self, key: JobSessionKey) -> Result<(), Error> {
        let mut current = self.legacy_job.write().expect("legacy job registry lock");
        match current.as_ref() {
            None => {
                *current = Some(key);
                Ok(())
            }
            Some(existing) if existing == &key => Ok(()),
            Some(existing) => Err(Error::Config(format!(
                "unauthenticated remote edge transport already serves Job '{}' generation {}; it cannot carry Job '{}' generation {}",
                existing.job_id, existing.generation, key.job_id, key.generation
            ))),
        }
    }

    /// Remove every data-plane registration owned by one Job attempt.  This is
    /// called after the kernel has stopped (and on failed graph construction)
    /// so a Job that never established a connection does not leave a failure
    /// channel or route behind for future generations.
    pub fn remove_job_session(&self, job_id: &str, generation: u64) {
        let job = JobSessionKey {
            job_id: job_id.to_owned(),
            generation,
        };
        let legacy_owned = self
            .legacy_job
            .read()
            .expect("legacy job registry lock")
            .as_ref()
            == Some(&job);
        let outbound_keys = {
            let outbound = self.outbound.read().expect("outbound registry lock");
            outbound
                .keys()
                .filter(|key| key.job() == Some(&job) || (legacy_owned && key.job().is_none()))
                .cloned()
                .collect::<Vec<_>>()
        };
        self.session_receipts
            .lock()
            .expect("session receipt lock")
            .retain(|key, route| {
                let keep = key.job() != Some(&job) && !(legacy_owned && key.job().is_none());
                if !keep {
                    route.forwarder.cancel();
                }
                keep
            });
        self.delivered_seq
            .lock()
            .expect("delivered seq lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.delivery_locks
            .lock()
            .expect("delivery lock registry")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.session_registrations
            .lock()
            .expect("session registration lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.inbound
            .write()
            .expect("inbound registry lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        let pending = {
            let mut outbound = self.outbound.write().expect("outbound registry lock");
            outbound_keys
                .iter()
                .filter_map(|key| outbound.remove(key))
                .collect::<Vec<_>>()
        };
        for receipts in pending {
            receipts.abort_all();
        }
        self.failure_channels
            .write()
            .expect("failure channel registry lock")
            .remove(&job);
        let mut legacy = self.legacy_job.write().expect("legacy job registry lock");
        if legacy.as_ref() == Some(&job) {
            *legacy = None;
        }
    }

    fn claim_handshake_nonce(&self, nonce: &[u8]) -> Result<(), Error> {
        let now = std::time::Instant::now();
        let mut nonces = self
            .handshake_nonces
            .lock()
            .expect("handshake nonce registry lock");
        nonces.retain(|_, seen| {
            now.checked_duration_since(*seen)
                .is_some_and(|age| age <= self.config.handshake_replay_ttl)
        });
        if nonces.contains_key(nonce) {
            return Err(Error::Authentication(
                "remote edge handshake nonce was replayed".into(),
            ));
        }
        let capacity = self.config.max_connections.saturating_mul(4).max(1);
        if nonces.len() >= capacity {
            return Err(Error::RateLimit(
                "remote edge handshake replay cache is full".into(),
            ));
        }
        nonces.insert(nonce.to_vec(), now);
        Ok(())
    }

    /// Injects an accepted stream (server side). The TCP listener task calls
    /// this per `TcpStream`; tests inject duplex halves.
    pub fn accept_stream(&self, stream: Box<dyn RemoteStream>) {
        let current = self.active_connections.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Relaxed,
            |count| (count < self.config.max_connections).then_some(count + 1),
        );
        if current.is_err() {
            self.report_failure(Error::Process(format!(
                "remote edge accepted-connection limit {} reached",
                self.config.max_connections
            )));
            return;
        }
        if self.accepted.0.try_send(stream).is_err() {
            self.active_connections
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            self.report_failure(Error::Process(
                "remote edge accepted-connection queue is full".into(),
            ));
        }
    }

    /// Runs the accept loop until shutdown. Call once per manager.
    /// The configured fleet-CA mTLS material, when enabled. Outbound edge
    /// contexts carry this so remote transports wrap their connections.
    pub fn tls_config(&self) -> Option<&DataPlaneTlsConfig> {
        self.config.tls.as_ref()
    }

    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    _ = manager.shutdown.cancelled() => break,
                    stream = manager.accepted.1.recv_async() => match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    },
                };
                let per_stream = manager.clone();
                tokio::spawn(async move {
                    per_stream.clone().serve_stream(stream).await;
                    per_stream
                        .active_connections
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                });
            }
        })
    }

    /// Binds the TCP data-plane listener and feeds accepted connections into
    /// the serve loop. Returns the bound port (for registry advertisement).
    pub async fn bind_tcp(self: &Arc<Self>, addr: std::net::SocketAddr) -> Result<u16, Error> {
        if self.config.credentials.is_none() {
            // Serving without credentials would skip the session handshake for
            // every inbound connection; refuse the bind instead.
            return Err(Error::Config(
                "refusing to bind the data-plane listener without credentials".into(),
            ));
        }
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
                        match manager.config.tls.clone() {
                            // Inbound TLS first: the peer must present a
                            // fleet-CA certificate before any frame (and
                            // before the HMAC session handshake) is read.
                            // The handshake runs OFF the accept loop so one
                            // slow or malicious peer cannot stall new
                            // connections; failures take the
                            // connection-failure path — never a plaintext
                            // fallback.
                            Some(tls) => {
                                let handshake_manager = manager.clone();
                                tokio::spawn(async move {
                                    match tls.accept(stream).await {
                                        Ok(tls_stream) => {
                                            handshake_manager.accept_stream(Box::new(tls_stream));
                                        }
                                        Err(error) => {
                                            handshake_manager.report_failure(error);
                                        }
                                    }
                                });
                            }
                            None => manager.accept_stream(Box::new(stream)),
                        }
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
    ///
    /// Test-only: bypasses the authenticated session handshake. Production
    /// code opens edges through [`Self::open_edge_for_session`] or
    /// [`Self::open_edge_deferred_for_session`].
    #[cfg(test)]
    pub async fn open_edge(
        self: &Arc<Self>,
        transport: &dyn EdgeTransport,
        quad: Quad,
    ) -> Result<RemoteEdge, Error> {
        let stream = transport.connect(quad).await?;
        Ok(self.open_edge_with_stream(stream, quad))
    }

    /// Opens an authenticated edge using the manager's configured shared
    /// secret and the Job/generation/peer identity supplied by graph build.
    pub async fn open_edge_for_session(
        self: &Arc<Self>,
        transport: &dyn EdgeTransport,
        quad: Quad,
        peer_node: &str,
        job_id: &str,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        let stream = transport.connect(quad).await?;
        let auth = self.session_auth(peer_node, job_id, generation, quad)?;
        Ok(self.open_edge_with_auth(stream, quad, Some(auth)))
    }

    /// Like [`Self::open_edge`] but over a caller-provided stream (tests,
    /// without the session handshake).
    #[cfg(test)]
    pub fn open_edge_with_stream(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
    ) -> RemoteEdge {
        self.open_edge_with_auth(stream, quad, None)
    }

    pub fn open_edge_with_stream_for_session(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        peer_node: &str,
        job_id: &str,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        let auth = self.session_auth(peer_node, job_id, generation, quad)?;
        Ok(self.open_edge_with_auth(stream, quad, Some(auth)))
    }

    fn open_edge_with_auth(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        auth: Option<SessionAuth>,
    ) -> RemoteEdge {
        let (sender, receiver) =
            flume::bounded::<super::envelope::Envelope>(self.config.channel_capacity);
        let pending = Arc::new(PendingReceipts::with_byte_budget(
            self.config.max_pending_receipts,
            self.config.max_pending_bytes,
        ));
        let session_key = auth
            .as_ref()
            .map(SessionAuth::edge_key)
            .unwrap_or_else(|| EdgeSessionKey::legacy(quad));
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(session_key.clone(), pending.clone());
        self.attach_stream(stream, quad, receiver, pending, auth, session_key);
        RemoteEdge { sender }
    }

    /// Like [`Self::open_edge_with_stream`] but connects on a background task,
    /// so synchronous graph builders can wire remote edges without awaiting.
    /// The edge channel already accepts envelopes while connecting — its bound
    /// applies backpressure. A failed connect fails the edge closed: pending
    /// branch acknowledgements abort and the sender's sends error out.
    ///
    /// Test-only: bypasses the authenticated session handshake. Production
    /// graph builders open edges through [`Self::open_edge_deferred_for_session`].
    #[cfg(test)]
    pub fn open_edge_deferred(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
    ) -> RemoteEdge {
        self.open_edge_deferred_with_key(transport, quad, EdgeSessionKey::legacy(quad), None)
    }

    fn open_edge_deferred_with_key(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
        session_key: EdgeSessionKey,
        auth: Option<SessionAuth>,
    ) -> RemoteEdge {
        let (sender, receiver) =
            flume::bounded::<super::envelope::Envelope>(self.config.channel_capacity);
        let pending = Arc::new(PendingReceipts::with_byte_budget(
            self.config.max_pending_receipts,
            self.config.max_pending_bytes,
        ));
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(session_key.clone(), pending.clone());
        let manager = self.clone();
        let failures = self.failure_sender_for_key(&session_key);
        tokio::spawn(async move {
            match transport.connect(quad).await {
                Ok(stream) => {
                    manager.attach_stream_with_redial(
                        stream,
                        quad,
                        receiver,
                        pending,
                        auth,
                        session_key.clone(),
                        Some(transport),
                    );
                }
                Err(error) => {
                    let _ = failures
                        .send_async(Error::Process(format!(
                            "remote edge connect for quad {quad:?} failed: {error}"
                        )))
                        .await;
                    pending.abort_all();
                    // Remove the stale registry entry so a later edge for the
                    // same quad does not route receipts into an aborted map.
                    self_remove_outbound(&manager, &session_key);
                    // Drop the receiver: the graph's sender side observes the
                    // closed edge on its next send.
                    drop(receiver);
                }
            }
        });
        RemoteEdge { sender }
    }

    pub fn open_edge_deferred_for_session(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
        peer_node: String,
        job_id: String,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        let auth = self.session_auth(&peer_node, &job_id, generation, quad)?;
        let session_key = auth.edge_key();
        Ok(self.open_edge_deferred_with_key(transport, quad, session_key, Some(auth)))
    }

    /// Opens a deferred, authenticated edge for a planned remote edge. A
    /// manager without data-plane credentials fails the graph build here
    /// instead of degrading to an unauthenticated edge.
    pub fn open_edge_deferred_for_job(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
        peer_node: String,
        job_id: String,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        self.open_edge_deferred_for_session(transport, quad, peer_node, job_id, generation)
    }

    fn session_auth(
        &self,
        peer_node: &str,
        job_id: &str,
        generation: u64,
        quad: Quad,
    ) -> Result<SessionAuth, Error> {
        self.config
            .credentials
            .as_ref()
            .map(|credentials| credentials.session(peer_node, job_id, generation, quad))
            .ok_or_else(|| {
                Error::Config(
                    "authenticated remote edge requested without data-plane credentials".into(),
                )
            })
    }

    /// Records a session loss and returns the registration generation the
    /// grace watcher should compare against. The generation is set BELOW the
    /// next registration so the first (re)registration after the loss makes
    /// the comparison mismatch and suppresses the deferred failure.
    fn bump_session_loss(&self, key: &EdgeSessionKey) -> u64 {
        let mut registrations = self
            .session_registrations
            .lock()
            .expect("session registration lock");
        let next = registrations.get(key).copied().unwrap_or(0) + 1;
        // Ensure any re-registration strictly exceeds this snapshot.
        registrations.insert(key.clone(), next);
        next
    }

    /// Marks a session key as (re)registered by a live connection, advancing
    /// its generation so pending loss watchers observe the recovery.
    fn bump_session_registration(&self, key: &EdgeSessionKey) {
        let mut registrations = self
            .session_registrations
            .lock()
            .expect("session registration lock");
        let next = registrations.get(key).copied().unwrap_or(0) + 1;
        registrations.insert(key.clone(), next);
    }

    fn report_failure(&self, error: Error) {
        // This path is synchronous because it is also used by the listener
        // and Ack callbacks. A blocking send preserves the bounded queue's
        // backpressure without silently losing the failure that must cancel
        // the affected edge.
        let _ = self.failures.send(error);
    }

    /// Spawns the receipt read loop and outbound pump over an established
    /// stream (shared by the eager and deferred open paths).
    fn attach_stream(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        receiver: flume::Receiver<super::envelope::Envelope>,
        pending: Arc<PendingReceipts>,
        auth: Option<SessionAuth>,
        session_key: EdgeSessionKey,
    ) {
        self.attach_stream_with_redial(stream, quad, receiver, pending, auth, session_key, None);
    }

    /// [`Self::attach_stream`] with a redial handle: when the transport is
    /// provided, a stream-level failure transparently reconnects within the
    /// configured attempt budget, replaying every unacknowledged data frame
    /// (the receiver dedups by sequence number). Budget exhaustion, shutdown,
    /// or a session removed mid-recovery fails the edge closed exactly as a
    /// stream failure does without reconnect.
    #[allow(clippy::too_many_arguments)]
    fn attach_stream_with_redial(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        receiver: flume::Receiver<super::envelope::Envelope>,
        pending: Arc<PendingReceipts>,
        auth: Option<SessionAuth>,
        session_key: EdgeSessionKey,
        redial: Option<Arc<dyn EdgeTransport>>,
    ) {
        // A deferred connect can finish after its Job has been stopped.  Do
        // not attach that late stream or recreate the failure channel that
        // `remove_job_session` deliberately dropped.
        if !self
            .outbound
            .read()
            .expect("outbound registry lock")
            .contains_key(&session_key)
        {
            pending.abort_all();
            return;
        }
        let failures = self.failure_sender_for_key(&session_key);
        let config = self.config.clone();
        let manager = self.clone();
        let reconnectable = redial.is_some() && config.reconnect_attempts > 0;
        tokio::spawn(async move {
            let mut attempts_left = config.reconnect_attempts;
            let mut current = Some(stream);
            let mut final_error: Option<Error> = None;
            let mut clean_exit = false;
            while let Some(stream) = current.take() {
                let (pump_result, read_result) = run_edge_connection(
                    stream,
                    quad,
                    receiver.clone(),
                    pending.clone(),
                    config.clone(),
                    auth.clone(),
                    reconnectable,
                    failures.clone(),
                    &manager.shutdown,
                )
                .await;
                // Clean requires BOTH halves to end cleanly: the read loop
                // already classifies a post-drain peer close as Ok, so any Err
                // on either half is a stream-level failure.
                let error = pump_result.err().or(read_result.err());
                match error {
                    None => {
                        clean_exit = true;
                        break;
                    }
                    Some(error) => {
                        // Either the pump hit a wire failure or the read half
                        // dropped the connection; both are stream-level. Retry
                        // within the budget when a redial handle exists and the
                        // session is still live.
                        let retry = reconnectable
                            && attempts_left > 0
                            && !manager.shutdown.is_cancelled()
                            && manager
                                .outbound
                                .read()
                                .expect("outbound registry lock")
                                .contains_key(&session_key);
                        if !retry {
                            final_error = Some(error);
                            break;
                        }
                        attempts_left -= 1;
                        tracing::warn!(
                            attempts_left,
                            quad = ?quad,
                            "remote edge stream failed; transparently reconnecting"
                        );
                        tokio::time::sleep(connect_backoff(
                            config.reconnect_attempts - attempts_left,
                        ))
                        .await;
                        let redial = redial.clone().expect("retry implies redial");
                        match redial.connect(quad).await {
                            Ok(mut stream) => {
                                if let Err(error) =
                                    replay_pending(&mut stream, quad, &pending, &auth, &config)
                                        .await
                                {
                                    final_error = Some(error);
                                    break;
                                }
                                current = Some(stream);
                            }
                            Err(error) => {
                                final_error = Some(Error::Process(format!(
                                    "remote edge reconnect for quad {quad:?} failed: {error}"
                                )));
                                break;
                            }
                        }
                    }
                }
            }
            let failed = final_error.is_some();
            if let Some(error) = final_error {
                let _ = failures.send_async(error).await;
            }
            if !clean_exit || failed {
                // The connection is gone and no receipt can arrive: settle
                // every still-registered branch through the abort path.
                pending.abort_all();
            }
            self_remove_outbound(&manager, &session_key);
        });
    }

    /// Handles one inbound connection (downstream side): decodes data frames
    /// into the registered local channel, forwards signals, and writes the
    /// receipts produced by `RemoteAck`s on this connection back upstream.
    async fn serve_stream(self: Arc<Self>, stream: Box<dyn RemoteStream>) {
        let result = self.serve_stream_inner(stream).await;
        if let Err(failure) = result {
            let sender = match failure.job.as_ref() {
                Some(job) => self.failure_sender_for_registered_job(job),
                None => Some(self.failures.clone()),
            };
            let Some(sender) = sender else {
                // The Job was stopped while the connection was unwinding. Its
                // per-session receiver has already been removed, so reporting
                // this late error must not recreate stale state or fall into a
                // different Job's global queue.
                return;
            };
            // A connection error is part of the Job's failure contract.  Do
            // not use try_send here: a full bounded queue must apply
            // backpressure rather than silently dropping the only signal that
            // can cancel the affected Job.
            let _ = sender.send_async(failure.error).await;
        }
    }

    async fn serve_stream_inner(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
    ) -> Result<(), ConnectionFailure> {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (receipt_tx, receipt_rx) =
            flume::bounded::<(Quad, ReceiptFrame)>(self.config.max_receipt_queue);
        let mut decoder = DataDecoder::new();
        let mut served_quads: BTreeMap<EdgeSessionKey, flume::Sender<super::envelope::Envelope>> =
            BTreeMap::new();
        let mut eos_seen: BTreeSet<EdgeSessionKey> = BTreeSet::new();
        // Session keys this connection registered, advanced once per key so a
        // reconnecting connection marks recovery for pending loss watchers.
        let mut registered_keys: BTreeSet<EdgeSessionKey> = BTreeSet::new();
        // (session key, our receipt sender) pairs whose writer slots this
        // connection installed; released with identity guards on exit.
        let mut writer_slots_installed: Vec<(EdgeSessionKey, flume::Sender<(Quad, ReceiptFrame)>)> =
            Vec::new();
        let mut authenticated_session = None;
        let mut failure_sender = self.failures.clone();
        let connection_cancel = self.shutdown.child_token();

        if let Some(credentials) = self.config.credentials.as_ref() {
            let (header, payload) = read_frame_with_limits(
                &mut reader,
                self.config.max_frame_len,
                Some(self.config.read_idle_timeout),
            )
            .await?;
            let (session, request) = self.authorize_inbound(header, &payload).await?;
            let job = JobSessionKey {
                job_id: request.job_id.clone(),
                generation: request.generation,
            };
            let session_key =
                EdgeSessionKey::for_job(request.job_id.clone(), request.generation, request.quad);
            failure_sender = self.failure_sender_for_job_key(&job);
            authenticated_session = Some(session_key.clone());
            let response = session.server_ack(&request, &credentials.local_node);
            let response_payload = match serde_json::to_vec(&response) {
                Ok(payload) => payload,
                Err(error) => {
                    self.remove_inbound_session(&session_key);
                    return Err(ConnectionFailure {
                        job: Some(job.clone()),
                        error: Error::Process(format!(
                            "remote edge handshake encode failed: {error}"
                        )),
                    });
                }
            };
            if let Err(error) = write_frame_with_limit(
                &mut writer,
                request.quad,
                FrameKind::Handshake,
                &response_payload,
                self.config.max_frame_len,
            )
            .await
            {
                self.remove_inbound_session(&session_key);
                return Err(ConnectionFailure {
                    job: Some(job.clone()),
                    error,
                });
            }
            if let Err(error) = writer.flush().await {
                self.remove_inbound_session(&session_key);
                return Err(ConnectionFailure {
                    job: Some(job.clone()),
                    error: Error::Process(format!("remote edge handshake flush failed: {error}")),
                });
            }
            if wait_for_route(
                &self.inbound,
                &mut served_quads,
                session_key.clone(),
                self.config.registration_grace,
            )
            .await
            .is_none()
            {
                self.remove_inbound_session(&session_key);
                return Err(ConnectionFailure {
                    job: Some(job),
                    error: Error::Process(format!(
                        "authenticated remote edge quad {:?} is no longer registered",
                        request.quad
                    )),
                });
            }
        }

        // Receipt writer: drains RemoteAck outboxes onto the wire, flushed
        // immediately — receipts gate upstream source progress.
        let receipt_config = self.config.clone();
        let receipt_cancel = connection_cancel.clone();
        let receipt_failures = failure_sender.clone();
        let receipt_writer_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let receipt_writer_failed_task = receipt_writer_failed.clone();
        let receipt_writer_handle = tokio::spawn(async move {
            let mut writer = tokio::io::BufWriter::with_capacity(16 * 1024, writer);
            let result: Result<(), Error> = loop {
                let send = tokio::select! {
                    _ = receipt_cancel.cancelled() => {
                        break Ok(());
                    }
                    next = receipt_rx.recv_async() => next,
                };
                let Ok((quad, receipt)) = send else {
                    break Ok(());
                };
                let payload = match serde_json::to_vec(&receipt) {
                    Ok(payload) => payload,
                    Err(error) => {
                        break Err(Error::Process(format!(
                            "remote edge receipt encode failed: {error}"
                        )))
                    }
                };
                if let Err(error) = write_frame_with_limit(
                    &mut writer,
                    quad,
                    FrameKind::Receipt,
                    &payload,
                    receipt_config.max_frame_len,
                )
                .await
                {
                    break Err(Error::Process(format!(
                        "remote edge receipt write failed: {error}"
                    )));
                }
                if let Err(error) = writer.flush().await {
                    break Err(Error::Process(format!(
                        "remote edge receipt flush failed: {error}"
                    )));
                }
            };
            if let Err(error) = result {
                receipt_writer_failed_task.store(true, std::sync::atomic::Ordering::Release);
                receipt_cancel.cancel();
                // This is deliberately awaited.  Receipt write failure is a
                // fatal data-plane event and must not disappear when the
                // connection task is cleaned up.
                let _ = receipt_failures.send_async(error).await;
            }
        });

        // Quads whose chain observed a forwarded Eos. A connection that ends
        // WITHOUT Eos for a served quad is an upstream death mid-stream, not a
        // clean finish: the chain must fail (at-least-once) instead of
        // silently publishing a prefix and reporting the subgraph finished.
        let failure_reason: Result<(), Error> = loop {
            let frame = tokio::select! {
                _ = connection_cancel.cancelled() => break Ok(()),
                frame = read_frame_with_limits(
                    &mut reader,
                    self.config.max_frame_len,
                    Some(self.config.read_idle_timeout),
                ) => frame,
            };
            match frame {
                Ok((header, payload)) => {
                    if header.kind == FrameKind::Handshake {
                        break Err(Error::Process("duplicate remote edge handshake".into()));
                    }
                    let route_key = authenticated_session
                        .clone()
                        .unwrap_or_else(|| EdgeSessionKey::legacy(header.quad));
                    if authenticated_session
                        .as_ref()
                        .is_some_and(|session| session.quad != header.quad)
                    {
                        break Err(Error::Process(format!(
                            "remote edge frame quad {:?} does not match authenticated quad {:?}",
                            header.quad, authenticated_session
                        )));
                    }
                    let sender = match wait_for_route(
                        &self.inbound,
                        &mut served_quads,
                        route_key.clone(),
                        self.config.registration_grace,
                    )
                    .await
                    {
                        Some(sender) => sender,
                        None => {
                            break Err(Error::Process(format!(
                                "inbound frame for unregistered quad {:?}",
                                header.quad
                            )))
                        }
                    };
                    if registered_keys.insert(route_key.clone()) {
                        self.bump_session_registration(&route_key);
                    }
                    // This connection now serves the session's receipts:
                    // create the route (idempotent) and take the writer slot
                    // (recording the sender so the guarded release on exit
                    // can tell ours from a replacement's).
                    let _ = self.session_receipt_route(&route_key);
                    let ours = receipt_tx.clone();
                    self.set_session_receipt_writer(&route_key, Some(ours.clone()));
                    writer_slots_installed.push((route_key.clone(), ours));
                    match header.kind {
                        FrameKind::Data => {
                            let (batch, seq) = match decoder.decode(&payload) {
                                Ok(decoded) => decoded,
                                Err(error) => {
                                    break Err(Error::Process(format!(
                                        "inbound data frame decode failed: {error}"
                                    )))
                                }
                            };
                            // Delivery-level dedup: a transparent reconnect
                            // replays every frame the upstream has not
                            // receipted. A sequence at or below the last one
                            // DELIVERED (recorded only after the local
                            // channel accepted it) is dropped silently: its
                            // original acknowledgement is still in flight
                            // through the session-scoped receipt route, which
                            // survives the connection swap. No mirror-ack —
                            // completing the upstream branch before the local
                            // chain finishes processing would let source
                            // offsets advance past unprocessed data.
                            // Serialize check→deliver→record per session key:
                            // during a transparent reconnect the old
                            // connection can still be parked on a
                            // backpressured send while the replacement
                            // replays the same sequences. The lock makes the
                            // second passer see the first one's delivered
                            // mark, and the max-update below keeps a late
                            // completion from regressing it.
                            let delivery_lock = self.delivery_lock(&route_key);
                            let _delivery_guard = delivery_lock.lock().await;
                            let duplicate = {
                                let delivered =
                                    self.delivered_seq.lock().expect("delivered seq lock");
                                match delivered.get(&route_key) {
                                    Some(last) => seq <= *last,
                                    None => false,
                                }
                            };
                            if duplicate {
                                continue;
                            }
                            // Session-scoped receipts: the ack hands its
                            // receipt to the session queue, so it completes
                            // on whichever connection serves the session by
                            // the time processing finishes.
                            let outbox = self.session_receipt_route(&route_key);
                            let envelope = Envelope::Data(
                                Arc::new(batch),
                                Arc::new(RemoteAck {
                                    outbox,
                                    quad: header.quad,
                                    seq,
                                    failures: failure_sender.clone(),
                                }),
                            );
                            if sender.send_async(envelope).await.is_err() {
                                break Ok(()); // local chain gone; close the edge
                            }
                            {
                                let mut delivered =
                                    self.delivered_seq.lock().expect("delivered seq lock");
                                let mark = delivered.entry(route_key.clone()).or_insert(0);
                                if seq > *mark {
                                    *mark = seq;
                                }
                            }
                        }
                        FrameKind::Signal => {
                            let signal = match serde_json::from_slice::<WireSignal>(&payload) {
                                Ok(signal) => signal,
                                Err(error) => {
                                    break Err(Error::Process(format!(
                                        "inbound signal frame malformed: {error}"
                                    )))
                                }
                            };
                            if matches!(signal, WireSignal::Eos) {
                                eos_seen.insert(route_key);
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
                        FrameKind::Handshake => unreachable!("handled before frame dispatch"),
                    }
                }
                Err(error) => {
                    break Err(error);
                }
            }
        };

        connection_cancel.cancel();

        // Session-loss reporting: with transparent reconnect enabled, a lost
        // connection without Eos defers its failure by the reconnect grace —
        // a re-registering upstream within the window suppresses it (the
        // supervisor on the peer redials and replays unacknowledged frames).
        // Protocol-level errors fail immediately: they recur deterministically.
        let reconnect_enabled = self.config.reconnect_attempts > 0;
        let report = |key: EdgeSessionKey, message: String| {
            if !reconnect_enabled {
                let sender = failure_sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send_async(Error::Process(message)).await;
                });
                return;
            }
            let generation = self.bump_session_loss(&key);
            let manager = self.clone();
            let sender = failure_sender.clone();
            let grace = self.config.reconnect_grace;
            tokio::spawn(async move {
                tokio::time::sleep(grace).await;
                if manager.shutdown.is_cancelled() {
                    return;
                }
                let current = manager
                    .session_registrations
                    .lock()
                    .expect("session registration lock")
                    .get(&key)
                    .copied();
                if current.is_some() && current != Some(generation) {
                    // The upstream re-registered: the loss was recovered.
                    return;
                }
                // Confirmed loss: the registration kept for the redialing
                // peer is now stale and must not leak into later jobs.
                manager.remove_session_receipt_route(&key);
                manager
                    .inbound
                    .write()
                    .expect("inbound registry lock")
                    .remove(&key);
                manager
                    .inbound_auth
                    .write()
                    .expect("inbound auth registry lock")
                    .remove(&key);
                let _ = sender.send_async(Error::Process(message)).await;
            });
        };

        if let Err(error) = failure_reason {
            let mut reported_without_eos = false;
            let defer_loss = is_unexpected_eof(&error);
            for key in served_quads.keys() {
                if !eos_seen.contains(key) {
                    reported_without_eos = true;
                    let message = format!(
                        "remote edge for quad {:?} ended without Eos: {error}",
                        key.quad
                    );
                    if defer_loss {
                        report(key.clone(), message);
                    } else {
                        let sender = failure_sender.clone();
                        let _ = sender.send_async(Error::Process(message)).await;
                    }
                }
            }
            // A peer that forwarded Eos for every served quad is allowed to
            // close its write half.  `read_frame_with_limits` reports that
            // close as UnexpectedEof, but it is the normal completion signal,
            // not a failed remote Job.  Keep reporting protocol and transport
            // errors; only suppress the clean EOF case.
            let clean_eos_close =
                !served_quads.is_empty() && !reported_without_eos && is_unexpected_eof(&error);
            if !clean_eos_close && !reported_without_eos {
                let _ = failure_sender.send_async(error).await;
            }
        } else if !self.shutdown.is_cancelled()
            && !receipt_writer_failed.load(std::sync::atomic::Ordering::Acquire)
        {
            // Dropping the served quads' senders closes the chains' input
            // channels. A quad whose connection died without a forwarded Eos
            // is an upstream death mid-stream, not a clean finish.
            for (key, sender) in &served_quads {
                if !eos_seen.contains(key) {
                    report(
                        key.clone(),
                        format!(
                            "remote edge for quad {:?} ended without Eos; upstream likely died mid-stream",
                            key.quad
                        ),
                    );
                    let _ = sender;
                }
            }
        }

        // Dropping the served quads' senders closes the chains' input channels.
        // Release the session writer slots this connection still owns. A
        // reconnecting replacement may have already installed its own
        // sender, so the clear is guarded by channel identity — clobbering
        // the live slot would stall receipts until the next reconnect.
        for (key, ours) in &writer_slots_installed {
            self.clear_session_receipt_writer_if_owned(key, ours);
        }

        // Only entries this connection inserted are removed; a reconnecting
        // upstream re-registers through the graph anyway. A loss under the
        // reconnect grace keeps the registration so the peer's redialing
        // connection can re-route immediately: the confirmed-loss watcher
        // removes it instead when the grace expires without recovery.
        for (key, sender) in served_quads {
            let loss_deferred =
                reconnect_enabled && !eos_seen.contains(&key) && !self.shutdown.is_cancelled();
            if loss_deferred {
                continue;
            }
            let mut registry = self.inbound.write().expect("inbound registry lock");
            if registry
                .get(&key)
                .is_some_and(|registered| registered.same_channel(&sender))
            {
                registry.remove(&key);
                self.inbound_auth
                    .write()
                    .expect("inbound auth registry lock")
                    .remove(&key);
            }
        }
        if receipt_writer_failed.load(std::sync::atomic::Ordering::Acquire) {
            let _ = receipt_writer_handle.await;
        } else {
            receipt_writer_handle.abort();
        }
        Ok(())
    }

    async fn authorize_inbound(
        &self,
        header: FrameHeader,
        payload: &[u8],
    ) -> Result<(SessionAuth, HandshakePayload), Error> {
        if header.kind != FrameKind::Handshake {
            return Err(Error::Process(
                "remote edge frame arrived before handshake".into(),
            ));
        }
        let request: HandshakePayload = serde_json::from_slice(payload)
            .map_err(|error| Error::Process(format!("remote edge handshake malformed: {error}")))?;
        let credentials =
            self.config.credentials.as_ref().ok_or_else(|| {
                Error::Process("remote edge credentials are not configured".into())
            })?;
        if request.protocol_version != DATA_PLANE_PROTOCOL_VERSION
            || request.acknowledgement
            || request.quad != header.quad
            || request.destination_node != credentials.local_node
            || request.nonce.len() != HANDSHAKE_NONCE_LEN
        {
            return Err(Error::Process(
                "remote edge handshake protocol or identity mismatch".into(),
            ));
        }
        verify_handshake_mac(&credentials.shared_secret, &request, b"client")?;
        self.claim_handshake_nonce(&request.nonce)?;
        let session_key =
            EdgeSessionKey::for_job(request.job_id.clone(), request.generation, request.quad);
        let deadline = tokio::time::Instant::now() + self.config.registration_grace;
        loop {
            let expected = {
                let registry = self
                    .inbound_auth
                    .read()
                    .expect("inbound auth registry lock");
                registry.get(&session_key).cloned().or_else(|| {
                    // Keep a useful stale-generation diagnostic without
                    // authorizing against a different session. The exact key
                    // above remains the only successful lookup.
                    registry
                        .iter()
                        .find(|(key, _)| key.quad == request.quad)
                        .map(|(_, expected)| expected.clone())
                })
            };
            if let Some(expected) = expected {
                if expected.source_node != request.source_node
                    || expected.job_id != request.job_id
                    || expected.generation != request.generation
                {
                    return Err(Error::Process(format!(
                        "remote edge handshake authorization mismatch for quad {:?}",
                        request.quad
                    )));
                }
                return Ok((
                    credentials.session(
                        request.source_node.clone(),
                        request.job_id.clone(),
                        request.generation,
                        request.quad,
                    ),
                    request,
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Process(format!(
                    "remote edge handshake for quad {:?} has no registered authorization",
                    request.quad
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
}

fn is_unexpected_eof(error: &Error) -> bool {
    matches!(error, Error::Process(message) if message.contains("early eof")
        || message.contains("unexpected end of file"))
}

fn self_remove_outbound(manager: &NetworkManager, key: &EdgeSessionKey) {
    manager
        .outbound
        .write()
        .expect("outbound registry lock")
        .remove(key);
}

/// Looks up (and caches on first sight) the local input channel serving a
/// quad on this connection.
fn resolve_route(
    registry: &std::sync::RwLock<
        BTreeMap<EdgeSessionKey, flume::Sender<super::envelope::Envelope>>,
    >,
    served_quads: &mut BTreeMap<EdgeSessionKey, flume::Sender<super::envelope::Envelope>>,
    key: EdgeSessionKey,
) -> Option<flume::Sender<super::envelope::Envelope>> {
    if let Some(sender) = served_quads.get(&key) {
        return Some(sender.clone());
    }
    let registered = registry
        .read()
        .expect("inbound registry lock")
        .get(&key)
        .cloned()?;
    served_quads.insert(key, registered.clone());
    Some(registered)
}

async fn wait_for_route(
    registry: &std::sync::RwLock<
        BTreeMap<EdgeSessionKey, flume::Sender<super::envelope::Envelope>>,
    >,
    served_quads: &mut BTreeMap<EdgeSessionKey, flume::Sender<super::envelope::Envelope>>,
    key: EdgeSessionKey,
    grace: std::time::Duration,
) -> Option<flume::Sender<super::envelope::Envelope>> {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        if let Some(sender) = resolve_route(registry, served_quads, key.clone()) {
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
/// Runs one connection's receipt read loop and outbound pump to completion,
/// returning both results. A failure on either half cancels the shared token,
/// so the other half exits promptly; the caller decides whether to reconnect
/// (replaying unacknowledged frames) or fail the edge closed. With
/// `reconnectable`, the pump defers its failure-path branch aborts to the
/// supervisor: the branches must stay registered so a reconnect can replay
/// them, and the supervisor aborts them if recovery does not succeed.
#[allow(clippy::too_many_arguments)]
async fn run_edge_connection(
    stream: Box<dyn RemoteStream>,
    quad: Quad,
    receiver: flume::Receiver<super::envelope::Envelope>,
    pending: Arc<PendingReceipts>,
    config: NetworkManagerConfig,
    auth: Option<SessionAuth>,
    reconnectable: bool,
    failures: flume::Sender<Error>,
    shutdown: &tokio_util::sync::CancellationToken,
) -> (Result<(), Error>, Result<(), Error>) {
    let (reader, writer) = tokio::io::split(stream);
    let connection_cancel = shutdown.child_token();
    let outbound_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Receipt read loop: routes receipts to the pending map. Errors cancel
    // the connection; classification (clean EOF after drain, late-failure
    // sweep) is left to the supervisor.
    let receipt_pending = pending.clone();
    let receipt_cancel = connection_cancel.clone();
    let receipt_outbound_closed = outbound_closed.clone();
    let receipt_failures = failures.clone();
    let receipt_auth = auth.clone();
    let read_task = tokio::spawn(async move {
        let mut reader = reader;
        let cancelled = receipt_cancel.clone();
        let mut first_frame = true;
        let mut failure: Option<Error> = None;
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => break,
                frame = read_receipt_frame(
                    &mut reader,
                    config.max_frame_len,
                    config.read_idle_timeout,
                    config.receipt_wait_timeout,
                    &receipt_pending,
                ) => match frame {
                    Ok(TieredFrame::RacedIdleRestart) => {
                        // A pending registration raced the dead-peer timer;
                        // the next iteration reads under the receipt-wait
                        // budget.
                        continue;
                    }
                    Ok(TieredFrame::Frame((header, payload))) => {
                        if first_frame && receipt_auth.is_some() {
                            first_frame = false;
                            let Some(auth) = receipt_auth.as_ref() else {
                                unreachable!()
                            };
                            let result = if header.kind == FrameKind::Handshake
                                && header.quad == auth.quad
                            {
                                serde_json::from_slice::<HandshakePayload>(&payload)
                                    .map_err(|error| Error::Process(format!("remote edge handshake acknowledgement malformed: {error}")))
                                    .and_then(|payload| auth.verify_server_ack(&payload))
                            } else {
                                Err(Error::Process("remote edge receipt arrived before handshake acknowledgement".into()))
                            };
                            if let Err(error) = result {
                                failure = Some(error);
                                break;
                            }
                        } else if header.kind == FrameKind::Receipt {
                            first_frame = false;
                            if let Ok(receipt) = serde_json::from_slice::<ReceiptFrame>(&payload)
                            {
                                receipt_pending.apply(receipt, &receipt_failures);
                            } else {
                                failure = Some(Error::Process(
                                    "remote edge receipt frame malformed".into(),
                                ));
                                break;
                            }
                        } else {
                            failure = Some(Error::Process(format!(
                                "unexpected {:?} frame on a receipt channel",
                                header.kind
                            )));
                            break;
                        }
                    }
                    Err(error) => {
                        // A normal outbound close drops the writer half after
                        // the local edge has drained; the peer may then close
                        // its half with no more frames. Only treat that as
                        // clean when no receipt can still be outstanding.
                        if !receipt_outbound_closed.load(std::sync::atomic::Ordering::Acquire)
                            || !receipt_pending.is_empty()
                        {
                            failure = Some(error);
                        }
                        break;
                    }
                },
            }
        }
        match failure {
            Some(error) => {
                cancelled.cancel();
                Err(error)
            }
            None => Ok(()),
        }
    });

    let pump_pending = pending.clone();
    let pump_cancel = connection_cancel.clone();
    let pump_outbound_closed = outbound_closed.clone();
    let pump_task = tokio::spawn(async move {
        let result = pump_edge(
            receiver,
            writer,
            quad,
            pump_pending,
            pump_cancel.clone(),
            config,
            auth,
            reconnectable,
        )
        .await;
        match result {
            Ok(()) => {
                pump_outbound_closed.store(true, std::sync::atomic::Ordering::Release);
            }
            Err(_) => {
                pump_cancel.cancel();
            }
        }
        result
    });

    let pump_result = pump_task.await.unwrap_or_else(|error| {
        Err(Error::Process(format!(
            "remote edge pump task failed: {error}"
        )))
    });
    if pump_result.is_err() {
        // A failed pump tears the connection down: no receipt can complete a
        // frame that never reached the wire, and the supervisor decides on
        // reconnect. A cleanly drained pump is the opposite case — the read
        // half stays alive to apply the peers' late receipts, exactly as the
        // unsupervised pump did, and ends on its own (peer close, idle
        // timeout, or the manager's shutdown).
        connection_cancel.cancel();
    }
    let read_result = read_task.await.unwrap_or_else(|error| {
        Err(Error::Process(format!(
            "remote edge read task failed: {error}"
        )))
    });
    (pump_result, read_result)
}

/// Replays the session preamble on a freshly reconnected stream: the
/// authenticated handshake plus every unacknowledged data frame in sequence
/// order, reusing each frame's original sequence number so the receiver's
/// delivery-level dedup can drop the ones it already routed.
async fn replay_pending(
    stream: &mut Box<dyn RemoteStream>,
    quad: Quad,
    pending: &Arc<PendingReceipts>,
    auth: &Option<SessionAuth>,
    config: &NetworkManagerConfig,
) -> Result<(), Error> {
    use tokio::io::AsyncWriteExt;
    let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, &mut **stream);
    if let Some(auth) = auth {
        let handshake = auth.client_handshake()?;
        let payload = serde_json::to_vec(&handshake).map_err(|error| {
            Error::Process(format!("remote edge handshake encode failed: {error}"))
        })?;
        write_frame_with_limit(
            &mut writer,
            quad,
            FrameKind::Handshake,
            &payload,
            config.max_frame_len,
        )
        .await?;
    }
    let mut encoder = DataEncoder::new();
    for (seq, batch) in pending.replay_snapshot() {
        let payload = encoder.encode(&batch, seq)?;
        write_frame_with_limit(
            &mut writer,
            quad,
            FrameKind::Data,
            &payload,
            config.max_frame_len,
        )
        .await?;
    }
    writer
        .flush()
        .await
        .map_err(|error| Error::Process(format!("remote edge replay flush failed: {error}")))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn pump_edge(
    receiver: flume::Receiver<super::envelope::Envelope>,
    writer: impl AsyncWrite + Unpin + Send + 'static,
    quad: Quad,
    pending: Arc<PendingReceipts>,
    shutdown: tokio_util::sync::CancellationToken,
    config: NetworkManagerConfig,
    auth: Option<SessionAuth>,
    reconnectable: bool,
) -> Result<(), Error> {
    let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, writer);
    let mut encoder = DataEncoder::new();
    let mut flush_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    if let Some(auth) = auth {
        let handshake = auth.client_handshake()?;
        let payload = serde_json::to_vec(&handshake).map_err(|error| {
            Error::Process(format!("remote edge handshake encode failed: {error}"))
        })?;
        write_frame_with_limit(
            &mut writer,
            quad,
            FrameKind::Handshake,
            &payload,
            config.max_frame_len,
        )
        .await?;
        writer.flush().await.map_err(|error| {
            Error::Process(format!("remote edge handshake flush failed: {error}"))
        })?;
    }
    let result: Result<(), Error>;
    // A drained channel (upstream dropped the sender) is the one clean exit
    // where the connection stays alive: the receipt read loop on the same
    // connection keeps applying late receipts and owns the final abort
    // sweep. Every other exit — wire failure, shutdown, registration
    // failure — tears the connection down, so no receipt can arrive and the
    // pump must abort everything still registered itself.
    let mut drained = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                result = Ok(());
                break;
            }
            _ = flush_tick.tick() => {
                if writer.flush().await.is_err() {
                    result = Err(Error::Process("remote edge flush failed".into()));
                    break;
                }
            }
            envelope = receiver.recv_async() => {
                let Ok(envelope) = envelope else {
                    drained = true;
                    result = Ok(());
                    break;
                };
                match envelope {
                    Envelope::Data(batch, branch) => {
                        // Register before writing so a concurrent receipt can
                        // never race the pending map; the wire is strict FIFO
                        // per quad, so receipts cannot precede their frame.
                        let seq = match pending.register(
                            &branch,
                            1,
                            reconnectable.then(|| batch.clone()),
                        ) {
                            Ok(seq) => seq,
                            Err(error) => {
                                let abort_error = branch.abort().await.err();
                                if let Some(abort_error) = abort_error {
                                    result = Err(Error::Process(format!(
                                        "{error}; failed to abort batch rejected by pending receipt limit: {abort_error}"
                                    )));
                                    break;
                                }
                                result = Err(error);
                                break;
                            }
                        };
                        let payload = match encoder.encode(&batch, seq) {
                            Ok(payload) => payload,
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        };
                        if let Err(error) = write_frame_with_limit(
                            &mut writer,
                            quad,
                            FrameKind::Data,
                            &payload,
                            config.max_frame_len,
                        )
                        .await
                        {
                            result = Err(Error::Process(format!(
                                "remote edge data write failed: {error}"
                            )));
                            break;
                        }
                    }
                    other => {
                        let Some(signal) = WireSignal::from_envelope(&other) else {
                            continue;
                        };
                        let payload = match serde_json::to_vec(&signal) {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                result = Err(Error::Process(format!(
                                    "signal frame encode failed: {error}"
                                )));
                                break;
                            }
                        };
                        // Control elements broadcast on every quad of the edge;
                        // each pump owns exactly one quad, so one frame each.
                        if let Err(error) = write_frame_with_limit(
                            &mut writer,
                            quad,
                            FrameKind::Signal,
                            &payload,
                            config.max_frame_len,
                        )
                        .await
                        {
                            result = Err(Error::Process(format!(
                                "remote edge signal write failed: {error}"
                            )));
                            break;
                        }
                    }
                }
            }
        }
    }
    let _ = writer.flush().await;
    // Abort pendings only on failure or forced shutdown: those paths tear
    // the connection down, so no receipt can ever arrive and leaving the
    // branches registered would leak source acks indefinitely. A drained
    // channel is the opposite case — the receipt read loop on the same
    // connection is still running, late receipts for flushed frames must
    // keep applying, and that loop owns the final abort sweep once the peer
    // closes or the read idle timeout fires.
    if reconnectable && (result.is_err() || !drained) {
        // A supervisor owns recovery: the branches stay registered so a
        // transparent reconnect can replay them, and the supervisor aborts
        // them if the reconnect budget is exhausted. This includes exits
        // driven by the connection token (a read-half failure cancelling the
        // pump mid-stream): aborting here would erase the replay set.
        return result;
    }
    if result.is_err() || !drained {
        pending.abort_all();
    }
    result
}

#[cfg(test)]
mod tests {
    /// Polling budgets for asynchronous disconnect/abort propagation.
    /// Under a fully parallel workspace test run, tokio timers can lag
    /// several seconds; these waits only observe conditions that normally
    /// complete within a tick (~100ms), so a generous ceiling costs nothing
    /// when everything is healthy and keeps loaded CI stable.
    const TEST_PROPAGATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);

    use super::*;
    use crate::input::Ack as _;
    use datafusion::arrow::array::{DictionaryArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    fn dictionary_batch(input_name: Option<&str>) -> MessageBatch {
        let dictionary: DictionaryArray<Int32Type> =
            vec!["alpha", "beta", "alpha"].into_iter().collect();
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
            trace_context: None,
        });
        let envelope = Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "c-7".into(),
            generation: 9,
            trace_context: None,
        });
        assert_eq!(WireSignal::from_envelope(&envelope), Some(barrier.clone()));
        match barrier.clone().into_envelope() {
            Envelope::Barrier(rebuilt) => {
                assert_eq!(
                    rebuilt,
                    CheckpointBarrier {
                        checkpoint_id: "c-7".into(),
                        generation: 9,
                        trace_context: None
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

        for kind in [
            ReceiptKind::Acked,
            ReceiptKind::Held,
            ReceiptKind::Released,
            ReceiptKind::Failed,
        ] {
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

    #[test]
    fn ipc_body_bounds_are_checked_without_panicking() {
        let piece = Piece {
            bytes: &[0; 8],
            flatbuf_len: 0,
        };
        let result = std::panic::catch_unwind(|| piece.body(1));
        assert!(result.is_ok(), "malformed IPC body must not panic");
        assert!(result.unwrap().is_err());

        let truncated = vec![4, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3, 4];
        assert!(read_piece(&truncated, 0).is_err());
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

    #[tokio::test]
    async fn idle_timeout_resets_after_partial_read_progress() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let (first_written_tx, first_written_rx) = tokio::sync::oneshot::channel();
        let writer_task = tokio::spawn(async move {
            writer.write_all(&[1]).await.unwrap();
            first_written_tx.send(()).unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            writer.write_all(&[2]).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            writer.write_all(&[3]).await.unwrap();
        });
        first_written_rx.await.unwrap();
        let mut bytes = [0_u8; 3];
        read_exact_with_idle(
            &mut reader,
            &mut bytes,
            Some(std::time::Duration::from_millis(50)),
        )
        .await
        .expect("a slow but progressing frame is not idle");
        writer_task.await.unwrap();
        assert_eq!(bytes, [1, 2, 3]);
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
            self.released
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        async fn abort(&self) -> Result<(), Error> {
            self.aborted
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// Shared kill state: the flag plus the currently registered reader
    /// waker, so `kill()` wakes a read parked inside the inner stream.
    #[derive(Default)]
    struct KillState {
        killed: std::sync::atomic::AtomicBool,
        waker: std::sync::Mutex<Option<std::task::Waker>>,
    }

    impl KillState {
        fn kill(&self) {
            self.killed
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    /// Handle side for tests: break the wrapped stream half.
    #[derive(Clone, Default)]
    struct KillHandle(Arc<KillState>);

    impl KillHandle {
        fn kill(&self) {
            self.0.kill();
        }
    }

    /// Wraps a stream half so the test can "break the connection" after the
    /// manager has taken ownership: once killed, pending reads wake and fail
    /// (like a reset connection) and writes fail with BrokenPipe.
    struct KillableStream {
        inner: Box<dyn RemoteStream>,
        state: Arc<KillState>,
    }

    impl AsyncRead for KillableStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.state.killed.load(std::sync::atomic::Ordering::Acquire) {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "killed",
                )));
            }
            *self.state.waker.lock().unwrap() = Some(cx.waker().clone());
            let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
            if result.is_pending() {
                // Keep the waker registered for a kill() wake.
            } else {
                *self.state.waker.lock().unwrap() = None;
            }
            result
        }
    }

    impl AsyncWrite for KillableStream {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.state.killed.load(std::sync::atomic::Ordering::Acquire) {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "killed",
                )));
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// Hands out pre-made stream halves in order: each `connect` pops the
    /// next queued client half, letting tests break a connection by dropping
    /// its server half and observe the transparent reconnect onto the
    /// following one.
    struct QueuedTransport {
        streams: std::sync::Mutex<Vec<Box<dyn RemoteStream>>>,
        fail_after: Option<usize>,
    }

    impl QueuedTransport {
        fn of(streams: Vec<Box<dyn RemoteStream>>) -> Arc<Self> {
            Arc::new(Self {
                streams: std::sync::Mutex::new(streams),
                fail_after: None,
            })
        }

        fn failing_after(streams: Vec<Box<dyn RemoteStream>>, fail_after: usize) -> Arc<Self> {
            Arc::new(Self {
                streams: std::sync::Mutex::new(streams),
                fail_after: Some(fail_after),
            })
        }
    }

    #[async_trait::async_trait]
    impl EdgeTransport for QueuedTransport {
        async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
            let mut streams = self.streams.lock().unwrap();
            if let Some(limit) = self.fail_after {
                if streams.len() <= limit {
                    return Err(Error::Process("test transport exhausted".into()));
                }
            }
            if streams.is_empty() {
                return Err(Error::Process("test transport exhausted".into()));
            }
            Ok(streams.remove(0))
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

    fn authenticated_manager(node: &str, secret: &str) -> Arc<NetworkManager> {
        let credentials = DataPlaneCredentials::new(node, secret).expect("test credentials");
        let config = NetworkManagerConfig {
            credentials: Some(credentials),
            registration_grace: std::time::Duration::from_millis(100),
            ..Default::default()
        };
        NetworkManager::with_config(config).expect("valid test network config")
    }

    async fn next_envelope(rx: &flume::Receiver<Envelope>) -> Envelope {
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, rx.recv_async())
            .await
            .expect("envelope within timeout")
            .expect("channel open")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn authenticated_session_roundtrips_and_binds_job_generation() {
        let quad = quad_a_to_b();
        let upstream = authenticated_manager("node-a", "shuffle-secret");
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        upstream.spawn();
        downstream.spawn();
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-1".into(),
                    generation: 7,
                },
            )
            .unwrap();
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream
            .open_edge_with_stream_for_session(Box::new(client_side), quad, "node-b", "job-1", 7)
            .unwrap();
        let branch = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(Some("authenticated"))),
                branch.clone(),
            ))
            .await
            .unwrap();
        let received = next_envelope(&input_rx).await;
        let Envelope::Data(batch, ack) = received else {
            panic!("expected authenticated data envelope");
        };
        assert_eq!(batch.get_input_name(), Some("authenticated".into()));
        ack.ack().await.unwrap();
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("authenticated receipt");
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn authenticated_session_rejects_stale_generation_and_wrong_quad() {
        let expected_quad = quad_a_to_b();
        let wrong_quad = Quad {
            dst_subtask: expected_quad.dst_subtask + 1,
            ..expected_quad
        };
        for (secret, quad, generation, expected_message) in [
            ("shuffle-secret", expected_quad, 8, "authorization mismatch"),
            (
                "shuffle-secret",
                wrong_quad,
                7,
                "no registered authorization",
            ),
            ("wrong-secret", expected_quad, 7, "credential mismatch"),
        ] {
            let upstream = authenticated_manager("node-a", secret);
            let downstream = authenticated_manager("node-b", "shuffle-secret");
            upstream.spawn();
            downstream.spawn();
            let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
            downstream
                .register_inbound_for_session(
                    expected_quad,
                    input_tx,
                    PeerExpectation {
                        source_node: "node-a".into(),
                        job_id: "job-1".into(),
                        generation: 7,
                    },
                )
                .unwrap();
            let failures = downstream.failure_receiver();
            let (client_side, server_side) = tokio::io::duplex(64 * 1024);
            downstream.accept_stream(Box::new(server_side));
            upstream
                .open_edge_with_stream_for_session(
                    Box::new(client_side),
                    quad,
                    "node-b",
                    "job-1",
                    generation,
                )
                .unwrap();
            let failure =
                tokio::time::timeout(std::time::Duration::from_secs(2), failures.recv_async())
                    .await
                    .expect("authentication failure")
                    .unwrap();
            assert!(
                failure.to_string().contains(expected_message),
                "unexpected authentication failure: {failure}"
            );
            upstream.shutdown();
            downstream.shutdown();
        }
    }

    #[tokio::test]
    async fn authenticated_handshake_nonce_cannot_be_replayed() {
        let quad = quad_a_to_b();
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-replay".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let auth = DataPlaneCredentials::new("node-a", "shuffle-secret")
            .unwrap()
            .session("node-b", "job-replay", 1, quad);
        let request = auth.client_handshake().unwrap();
        let payload = serde_json::to_vec(&request).unwrap();
        let header = FrameHeader {
            quad,
            len: payload.len() as u32,
            kind: FrameKind::Handshake,
        };
        downstream
            .authorize_inbound(header, &payload)
            .await
            .expect("first handshake is accepted");
        let error = match downstream.authorize_inbound(header, &payload).await {
            Ok(_) => panic!("a client nonce must not be reusable"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("replayed"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clean_eos_close_does_not_report_a_remote_failure() {
        let manager = NetworkManager::new(8);
        manager.spawn();
        let quad = quad_a_to_b();
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad, input_tx);
        let failures = manager.failure_receiver();
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server));
        let payload = serde_json::to_vec(&WireSignal::Eos).unwrap();
        write_frame(&mut client, quad, FrameKind::Signal, &payload)
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        assert!(matches!(next_envelope(&input_rx).await, Envelope::Eos));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), failures.recv_async(),)
                .await
                .is_err()
        );
        manager.shutdown();
    }

    #[test]
    fn unauthenticated_job_sessions_are_singleton_and_reusable_after_cleanup() {
        let manager = NetworkManager::new(8);
        let quad = quad_a_to_b();
        let (first_tx, _first_rx) = flume::bounded::<Envelope>(1);
        manager
            .register_inbound_for_session(
                quad,
                first_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-1".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let (second_tx, _second_rx) = flume::bounded::<Envelope>(1);
        let error = manager
            .register_inbound_for_session(
                quad,
                second_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-2".into(),
                    generation: 1,
                },
            )
            .expect_err("legacy routing cannot safely multiplex Jobs");
        assert!(error.to_string().contains("cannot carry Job"), "{error}");
        manager.remove_job_session("job-1", 1);
        let (second_tx, _second_rx) = flume::bounded::<Envelope>(1);
        manager
            .register_inbound_for_session(
                quad,
                second_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-2".into(),
                    generation: 1,
                },
            )
            .unwrap();
    }

    #[test]
    fn removing_a_job_session_reclaims_authenticated_registries() {
        let manager = authenticated_manager("node-b", "shuffle-secret");
        let quad = quad_a_to_b();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(1);
        manager
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-cleanup".into(),
                    generation: 4,
                },
            )
            .unwrap();
        let _failures = manager.failure_receiver_for_job("job-cleanup", 4);
        assert_eq!(manager.inbound.read().unwrap().len(), 1);
        assert_eq!(manager.failure_channels.read().unwrap().len(), 1);
        manager.remove_job_session("job-cleanup", 4);
        assert!(manager.inbound.read().unwrap().is_empty());
        assert!(manager.inbound_auth.read().unwrap().is_empty());
        assert!(manager.failure_channels.read().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn authenticated_listener_rejects_pre_handshake_frame() {
        let quad = quad_a_to_b();
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        downstream.spawn();
        let failures = downstream.failure_receiver();
        let (mut client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        write_frame(&mut client_side, quad, FrameKind::Signal, b"{}")
            .await
            .unwrap();
        let failure =
            tokio::time::timeout(std::time::Duration::from_secs(2), failures.recv_async())
                .await
                .expect("pre-handshake failure")
                .unwrap();
        assert!(failure.to_string().contains("before handshake"));
        downstream.shutdown();
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
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
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
                trace_context: None,
            }))
            .await
            .expect("send barrier");
        edge.sender
            .send_async(Envelope::Watermark(42))
            .await
            .expect("send watermark");
        edge.sender
            .send_async(Envelope::Eos)
            .await
            .expect("send eos");
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
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
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
    async fn pending_receipt_limit_aborts_the_rejected_batch() {
        let quad = quad_a_to_b();
        let upstream_config = NetworkManagerConfig {
            channel_capacity: 8,
            max_pending_receipts: 1,
            reconnect_attempts: 0,
            ..NetworkManagerConfig::default()
        };
        let upstream = NetworkManager::with_config(upstream_config).unwrap();
        let downstream = NetworkManager::new(8);
        upstream.spawn();
        downstream.spawn();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);
        let failures = upstream.failure_receiver();

        let first = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                first.clone(),
            ))
            .await
            .unwrap();
        let _ = next_envelope(&input_rx).await;

        let rejected = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                rejected.clone(),
            ))
            .await
            .unwrap();

        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !rejected.aborted.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the batch rejected by the pending limit must be aborted");
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !first.aborted.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the already pending batch must be aborted with the edge");
        let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
            .await
            .expect("pending limit failure within timeout")
            .unwrap();
        assert!(
            failure.to_string().contains("pending receipt limit"),
            "{failure}"
        );

        upstream.shutdown();
        downstream.shutdown();
    }

    /// Spec: pending replay is bounded by an approximate byte budget: a
    /// registration that would exceed the budget is rejected, and a
    /// completed acknowledgement releases its accounting back to the budget.
    #[tokio::test]
    async fn pending_replay_byte_budget_rejects_and_releases() {
        struct NoopAck;
        #[async_trait::async_trait]
        impl crate::input::Ack for NoopAck {
            async fn ack(&self) -> Result<(), Error> {
                Ok(())
            }
            async fn abort(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let batch = dictionary_batch(None);
        let batch_bytes = batch.record_batch().get_array_memory_size() as u64;
        let pending = PendingReceipts::with_byte_budget(64, batch_bytes);
        let branch = Arc::new(NoopAck) as Arc<dyn crate::input::Ack>;
        let (failures_tx, _failures_rx) = flume::unbounded::<Error>();

        let first = pending
            .register(&branch, 1, Some(Arc::new(batch)))
            .expect("a single batch fits the budget");
        let error = pending
            .register(&branch, 1, Some(Arc::new(dictionary_batch(None))))
            .expect_err("the second batch exceeds the budget");
        assert!(error.to_string().contains("byte budget"), "{error}");
        // Completing the first entry releases its bytes; a new registration
        // of the same size fits again.
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq: first,
            },
            &failures_tx,
        );
        // The ack runs on a spawned task; the map entry and its byte
        // accounting are released synchronously before that task runs.
        pending
            .register(&branch, 1, Some(Arc::new(dictionary_batch(None))))
            .expect("released bytes are available to new registrations");
    }

    /// Spec: a receipt-overflow escalation with BOTH queues full must not
    /// synchronously block the calling (potentially tokio-worker) thread.
    #[test]
    fn receipt_overflow_escalation_does_not_block() {
        let (outbox_tx, _outbox_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
        let (failures_tx, _failures_rx) = flume::bounded::<Error>(1);
        // Fill both queues so both try_send paths fail.
        outbox_tx
            .send((
                quad_a_to_b(),
                ReceiptFrame {
                    kind: ReceiptKind::Acked,
                    seq: 0,
                },
            ))
            .unwrap();
        failures_tx.send(Error::Process("filler".into())).unwrap();
        let ack = RemoteAck {
            outbox: outbox_tx,
            quad: quad_a_to_b(),
            seq: 7,
            failures: failures_tx,
        };
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            crate::input::Ack::mark_held(&ack);
            crate::input::Ack::release_held(&ack);
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("mark_held/release_held must not block on full queues");
        worker.join().expect("escalation thread finishes");
    }

    /// Spec: a connection with receipts still pending is a (possibly slow)
    /// live downstream — the short dead-peer idle timeout must not tear it
    /// down; only the long receipt-wait budget may.
    #[tokio::test(flavor = "multi_thread")]
    async fn receipt_wait_budget_keeps_a_busy_connection_alive() {
        let quad = quad_a_to_b();
        let upstream_config = NetworkManagerConfig {
            channel_capacity: 8,
            read_idle_timeout: std::time::Duration::from_millis(200),
            receipt_wait_timeout: std::time::Duration::from_secs(60),
            reconnect_attempts: 0,
            ..NetworkManagerConfig::default()
        };
        let upstream = NetworkManager::with_config(upstream_config).unwrap();
        let downstream = NetworkManager::new(8);
        upstream.spawn();
        downstream.spawn();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);
        let failures = upstream.failure_receiver();

        // One delivered-but-unacknowledged envelope keeps a receipt pending.
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                Arc::new(RecordingAck::default()),
            ))
            .await
            .unwrap();
        let _ = next_envelope(&input_rx).await;

        // Well past the 200ms dead-peer timeout, with the receipt still
        // outstanding, the edge must remain usable.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        if let Ok(failure) = failures.try_recv() {
            panic!("a busy edge must survive the dead-peer idle timeout: {failure}");
        }
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                Arc::new(RecordingAck::default()),
            ))
            .await
            .expect("the connection is still alive");
        let _ = next_envelope(&input_rx).await;

        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transparent_reconnect_replays_unreceipted_frames() {
        let quad = quad_a_to_b();
        let reconnect = NetworkManagerConfig {
            reconnect_attempts: 3,
            reconnect_grace: std::time::Duration::from_secs(5),
            ..NetworkManagerConfig::default()
        };
        let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
        let downstream = NetworkManager::with_config(reconnect).unwrap();
        upstream.spawn();
        downstream.spawn();
        let failures = upstream.failure_receiver();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);

        let (client1, server1) = tokio::io::duplex(64 * 1024);
        let (client2, server2) = tokio::io::duplex(64 * 1024);
        // Kill the DOWNSTREAM-owned half: the serve loop's read errors and
        // unwinds, which closes the real duplex and EOFs the upstream's read
        // immediately — exactly like a peer process dying mid-stream.
        let kill1 = KillHandle::default();
        let kill_state = kill1.0.clone();
        downstream.accept_stream(Box::new(KillableStream {
            inner: Box::new(server1),
            state: kill_state,
        }));
        downstream.accept_stream(Box::new(server2));
        let transport = QueuedTransport::of(vec![Box::new(client1), Box::new(client2)]);
        let edge = upstream.open_edge_deferred(transport.clone(), quad);
        // First connection: the deferred open dials client1.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let branch = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                branch.clone(),
            ))
            .await
            .unwrap();
        // The downstream chain receives the frame: delivered, not yet acked.
        let delivered_envelope = next_envelope(&input_rx).await;
        let delivered_ack = match &delivered_envelope {
            Envelope::Data(_, ack) => ack.clone(),
            _ => panic!("expected a data envelope"),
        };
        // Break the connection: killing the shared server half resets the
        // connection from both ends mid-stream.
        kill1.kill();

        // The transparent reconnect dials client2 and replays seq 0; the
        // receiver drops the duplicate silently (no re-delivery).
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                next_envelope(&input_rx)
            )
            .await
            .is_err(),
            "replayed duplicate must not re-deliver to the local chain"
        );
        // The ORIGINAL acknowledgement still completes — through the
        // session-scoped receipt route on the new connection. Only after
        // the downstream acknowledges processing does the upstream branch
        // settle (no mirror-ack: offsets never advance past unprocessed
        // data).
        delivered_ack.ack().await.expect("original ack completes");
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("branch acked through the session receipt route");
        assert!(
            !branch.aborted.load(std::sync::atomic::Ordering::SeqCst),
            "transparent recovery must not abort the branch"
        );
        // No edge failure surfaced: the reconnect healed the loss.
        if let Ok(failure) =
            tokio::time::timeout(std::time::Duration::from_millis(300), failures.recv_async()).await
        {
            panic!("unexpected failure: {failure:?}");
        }
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn overlapping_connections_neither_double_deliver_nor_regress_the_dedup_mark() {
        let quad = quad_a_to_b();
        let reconnect = NetworkManagerConfig {
            reconnect_attempts: 3,
            reconnect_grace: std::time::Duration::from_secs(5),
            ..NetworkManagerConfig::default()
        };
        let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
        let downstream = NetworkManager::with_config(reconnect).unwrap();
        upstream.spawn();
        downstream.spawn();
        let failures = upstream.failure_receiver();

        // Capacity 1: seq 0 fills the local channel, seq 1 parks the first
        // connection's delivery inside backpressure — the exact window a
        // transparent reconnect overlaps with a still-serving old wire.
        let (input_tx, input_rx) = flume::bounded::<Envelope>(1);
        downstream.register_inbound(quad, input_tx);

        let (client1, server1) = tokio::io::duplex(64 * 1024);
        let (client2, server2) = tokio::io::duplex(64 * 1024);
        let (client3, server3) = tokio::io::duplex(64 * 1024);
        let kill1 = KillHandle::default();
        let kill1_state = kill1.0.clone();
        let kill2 = KillHandle::default();
        let kill2_state = kill2.0.clone();
        downstream.accept_stream(Box::new(KillableStream {
            inner: Box::new(server1),
            state: kill1_state,
        }));
        downstream.accept_stream(Box::new(KillableStream {
            inner: Box::new(server2),
            state: kill2_state,
        }));
        downstream.accept_stream(Box::new(server3));
        let transport = QueuedTransport::of(vec![
            Box::new(client1),
            Box::new(client2),
            Box::new(client3),
        ]);
        let edge = upstream.open_edge_deferred(transport, quad);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Three unreceipted frames: seq 0 is delivered (channel now full),
        // seq 1 parks the old connection's send, seq 2 sits behind it.
        let branch = Arc::new(RecordingAck::default());
        for _ in 0..3 {
            edge.sender
                .send_async(Envelope::Data(
                    Arc::new(dictionary_batch(None)),
                    branch.clone(),
                ))
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        kill1.kill();
        // The reconnect dials client2 and replays every unreceipted frame
        // while the old connection's parked send is still in flight.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        // Drain with a generous budget: exactly three envelopes (seq 0..=2)
        // may arrive. The delivery lock serializes the parked send against
        // the replay, so no sequence enters the local channel twice.
        for expected in 0..3 {
            tokio::time::timeout(TEST_PROPAGATION_BUDGET, next_envelope(&input_rx))
                .await
                .unwrap_or_else(|_| panic!("expected envelope {expected} to arrive"));
        }
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(800),
                next_envelope(&input_rx)
            )
            .await
            .is_err(),
            "overlapping connections must not double-deliver any sequence"
        );

        // Prove the delivered mark never regressed: nothing was acked, so a
        // second reconnect replays seq 0..=2 — every frame must be dropped.
        kill2.kill();
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(800),
                next_envelope(&input_rx)
            )
            .await
            .is_err(),
            "replay after the overlap must drop every sequence (mark regressed?)"
        );
        if let Ok(failure) =
            tokio::time::timeout(std::time::Duration::from_millis(300), failures.recv_async()).await
        {
            panic!("unexpected failure: {failure:?}");
        }
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_budget_exhaustion_fails_closed() {
        let quad = quad_a_to_b();
        let reconnect = NetworkManagerConfig {
            reconnect_attempts: 2,
            reconnect_grace: std::time::Duration::from_millis(200),
            ..NetworkManagerConfig::default()
        };
        let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
        let downstream = NetworkManager::with_config(reconnect).unwrap();
        upstream.spawn();
        downstream.spawn();
        let failures = upstream.failure_receiver();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);

        let (client1, server1) = tokio::io::duplex(64 * 1024);
        let kill1 = KillHandle::default();
        let kill_state = kill1.0.clone();
        downstream.accept_stream(Box::new(KillableStream {
            inner: Box::new(server1),
            state: kill_state,
        }));
        // One stream only: after the first break, every redial fails.
        let transport = QueuedTransport::failing_after(vec![Box::new(client1)], 0);
        let edge = upstream.open_edge_deferred(transport, quad);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let branch = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                branch.clone(),
            ))
            .await
            .unwrap();
        let _ = next_envelope(&input_rx).await;
        kill1.kill();

        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("budget exhaustion must abort the branch");
        let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
            .await
            .expect("budget exhaustion must report the edge failure")
            .unwrap();
        assert!(failure.to_string().contains("reconnect"), "{failure}");
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disconnect_aborts_pending_and_closes_edge() {
        let quad = quad_a_to_b();
        // Reconnect disabled: this test pins the immediate fail-closed
        // behavior that a zero reconnect budget preserves.
        let offline = NetworkManagerConfig {
            reconnect_attempts: 0,
            ..NetworkManagerConfig::default()
        };
        let upstream = NetworkManager::with_config(offline).unwrap();
        let downstream = NetworkManager::with_config(NetworkManagerConfig {
            reconnect_attempts: 0,
            ..NetworkManagerConfig::default()
        })
        .unwrap();
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

        // The failure broadcast happens after the manager aborts pending
        // branches, but abort_all only *spawns* the abort tasks - poll the
        // flag explicitly rather than assuming it is already set.
        let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
            .await
            .expect("failure within timeout")
            .expect("failure channel open");
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
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
        assert!(
            failure.to_string().contains("remote edge") || failure.to_string().contains("inbound")
        );

        // The edge channel sender fails for new sends after the pump exits.
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
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
            let send = edge.sender.send_async(Envelope::Data(
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
        let blocked_at = blocked
            .expect("no send ever blocked with a stalled consumer — buffers grew without bound");
        assert!(
            blocked_at < 16,
            "backpressure engaged only after {blocked_at} in-flight batches"
        );

        // Draining the consumer releases the pressure: the pending send
        // completes once the downstream channel accepts frames again.
        let mut received = 0;
        let _ = tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
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
        let (failures, _failure_rx) = flume::unbounded::<Error>();
        let ack = RemoteAck {
            outbox,
            quad: quad_a_to_b(),
            seq: 3,
            failures,
        };
        ack.mark_held(); // must not panic on a closed outbox
        ack.release_held();
        assert!(
            ack.ack().await.is_err(),
            "acking a dead edge reports failure"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failure_reporting_backpressures_instead_of_dropping() {
        let config = NetworkManagerConfig {
            max_failure_queue: 1,
            ..NetworkManagerConfig::default()
        };
        let manager = NetworkManager::with_config(config).unwrap();
        let failures = manager.failure_receiver();
        manager.report_failure(Error::Process("first failure".into()));

        let reporter = manager.clone();
        let second = tokio::spawn(async move {
            reporter.report_failure(Error::Process("second failure".into()));
        });
        let first = failures.recv_async().await.unwrap();
        assert!(first.to_string().contains("first failure"), "{first}");
        second.await.unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(1), failures.recv_async())
            .await
            .expect("second failure must remain queued")
            .unwrap();
        assert!(second.to_string().contains("second failure"), "{second}");
    }

    fn large_batch(rows: usize) -> datafusion::arrow::record_batch::RecordBatch {
        use datafusion::arrow::array::{Int64Array, LargeBinaryArray};
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

        let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
            .await
            .expect("failure within timeout")
            .expect("failure channel open");
        assert!(
            failure.to_string().contains("without Eos"),
            "expected an abnormal-termination failure, got: {failure}"
        );

        downstream.shutdown();
    }

    #[tokio::test]
    async fn bind_tcp_refuses_without_credentials() {
        let manager = NetworkManager::new(64);
        let error = manager
            .bind_tcp("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect_err("bind must refuse without credentials");
        assert!(
            error.to_string().contains("without credentials"),
            "expected a credentials bind refusal, got: {error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tcp_transport_connects_and_receives() {
        let quad = quad_a_to_b();
        let upstream = authenticated_manager("node-a", "shuffle-secret");
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        upstream.spawn();
        downstream.spawn();

        let port = downstream
            .bind_tcp("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("bind");
        let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "tcp-job".into(),
                    generation: 1,
                },
            )
            .expect("inbound registration succeeds");

        let transport = TcpEdgeTransport {
            tls: None,
            addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            max_attempts: 3,
        };
        let edge = upstream
            .open_edge_for_session(&transport, quad, "node-b", "tcp-job", 1)
            .await
            .expect("connect");

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
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
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
    /// One shared fleet CA with a per-node certificate, as a real fleet
    /// deploys: all peers verify against the same CA.
    fn generate_fleet_material() -> (Vec<(String, String)>, String) {
        use rcgen::CertificateParams;
        use rcgen::KeyPair;
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let mut nodes = Vec::new();
        for _ in 0..2 {
            let node_params =
                CertificateParams::new(vec![super::DATA_PLANE_TLS_SERVER_NAME.to_owned()]).unwrap();
            let node_key = KeyPair::generate().unwrap();
            let node = node_params.signed_by(&node_key, &ca, &ca_key).unwrap();
            nodes.push((node.pem(), node_key.serialize_pem()));
        }
        (nodes, ca.pem())
    }

    fn tls_manager_config(
        node: &str,
        secret: &str,
        cert: &str,
        key: &str,
        ca: &str,
    ) -> NetworkManagerConfig {
        let credentials = DataPlaneCredentials::new(node, secret).expect("test credentials");
        NetworkManagerConfig {
            credentials: Some(credentials),
            registration_grace: std::time::Duration::from_millis(100),
            tls: Some(DataPlaneTlsConfig::from_pem(cert, key, ca).expect("tls")),
            ..Default::default()
        }
    }

    /// Fleet-CA mTLS end to end over loopback: the full stack (TLS
    /// handshake, HMAC session handshake, frames, receipts) behaves exactly
    /// like the plaintext path.
    #[tokio::test(flavor = "multi_thread")]
    async fn tls_transport_connects_and_receives() {
        let quad = quad_a_to_b();
        let (nodes, ca) = generate_fleet_material();
        let (cert_a, key_a) = &nodes[0];
        let (cert_b, key_b) = &nodes[1];
        let upstream = std::sync::Arc::new(
            NetworkManager::with_config(tls_manager_config(
                "node-a",
                "shuffle-secret",
                cert_a,
                key_a,
                &ca,
            ))
            .unwrap(),
        );
        let downstream = std::sync::Arc::new(
            NetworkManager::with_config(tls_manager_config(
                "node-b",
                "shuffle-secret",
                cert_b,
                key_b,
                &ca,
            ))
            .unwrap(),
        );
        upstream.spawn();
        downstream.spawn();

        let port = downstream
            .bind_tcp("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("bind");
        let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "tls-job".into(),
                    generation: 1,
                },
            )
            .expect("inbound registration succeeds");

        let transport = TcpEdgeTransport {
            addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            max_attempts: 3,
            tls: Some(DataPlaneTlsConfig::from_pem(cert_a, key_a, &ca).expect("client tls")),
        };
        let edge = upstream
            .open_edge_for_session(&transport, quad, "node-b", "tls-job", 1)
            .await
            .expect("connect");

        let branch = std::sync::Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                std::sync::Arc::new(dictionary_batch(Some("tls"))),
                branch.clone(),
            ))
            .await
            .expect("send");
        let received = next_envelope(&input_rx).await;
        let Envelope::Data(batch, ack) = received else {
            panic!("expected data over TLS");
        };
        assert_eq!(batch.get_input_name(), Some("tls".to_owned()));
        ack.ack().await.expect("ack");
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("receipt over TLS");
        upstream.shutdown();
        downstream.shutdown();
    }

    /// A plaintext client against a TLS server never completes a session:
    /// the connection fails closed (no protocol fallback).
    #[tokio::test(flavor = "multi_thread")]
    async fn plaintext_transport_against_tls_server_fails_closed() {
        let quad = quad_a_to_b();
        let (nodes, ca) = generate_fleet_material();
        let (cert_b, key_b) = &nodes[1];
        let downstream = std::sync::Arc::new(
            NetworkManager::with_config(tls_manager_config(
                "node-b",
                "shuffle-secret",
                cert_b,
                key_b,
                &ca,
            ))
            .unwrap(),
        );
        downstream.spawn();
        let port = downstream
            .bind_tcp("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("bind");
        let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "plain-job".into(),
                    generation: 1,
                },
            )
            .expect("inbound registration succeeds");

        let transport = TcpEdgeTransport {
            addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            max_attempts: 1,
            tls: None,
        };
        // The plaintext client may complete the TCP connect, but its bytes
        // are not a TLS record: the server handshake fails and NOTHING is
        // ever delivered into the session channel.
        let upstream = authenticated_manager("node-a", "shuffle-secret");
        upstream.spawn();
        let edge = upstream
            .open_edge_for_session(&transport, quad, "node-b", "plain-job", 1)
            .await
            .expect("stream-level connect (failure surfaces per-session)");
        let branch = std::sync::Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                std::sync::Arc::new(dictionary_batch(Some("plain"))),
                branch.clone(),
            ))
            .await
            .expect("send");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(300), input_rx.recv_async())
                .await
                .is_err(),
            "a plaintext stream must never deliver frames into a TLS session"
        );
        upstream.shutdown();
        downstream.shutdown();
    }

    /// A client whose certificate chains to a DIFFERENT CA fails the
    /// handshake at connect time — explicit, no protocol fallback.
    #[tokio::test(flavor = "multi_thread")]
    async fn foreign_ca_client_is_rejected_at_connect() {
        let quad = quad_a_to_b();
        let (nodes, ca) = generate_fleet_material();
        let (cert_b, key_b) = &nodes[1];
        let downstream = std::sync::Arc::new(
            NetworkManager::with_config(tls_manager_config(
                "node-b",
                "shuffle-secret",
                cert_b,
                key_b,
                &ca,
            ))
            .unwrap(),
        );
        downstream.spawn();
        let port = downstream
            .bind_tcp("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("bind");

        // A second, unrelated fleet.
        let (foreign_nodes, foreign_ca) = generate_fleet_material();
        let (foreign_cert, foreign_key) = &foreign_nodes[0];
        let upstream = std::sync::Arc::new(
            NetworkManager::with_config(tls_manager_config(
                "node-a",
                "shuffle-secret",
                foreign_cert,
                foreign_key,
                &foreign_ca,
            ))
            .unwrap(),
        );
        upstream.spawn();
        let transport = TcpEdgeTransport {
            addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            max_attempts: 1,
            tls: Some(
                DataPlaneTlsConfig::from_pem(foreign_cert, foreign_key, &foreign_ca)
                    .expect("client tls"),
            ),
        };
        let result = upstream
            .open_edge_for_session(&transport, quad, "node-b", "foreign-job", 1)
            .await;
        assert!(
            result.is_err(),
            "a foreign-CA client must fail the TLS handshake at connect"
        );
        upstream.shutdown();
        downstream.shutdown();
    }

    /// Partial TLS material is an explicit construction error.
    #[test]
    fn tls_partial_material_is_rejected() {
        let (nodes, ca) = generate_fleet_material();
        let (cert, key) = &nodes[0];
        let ca = ca.as_str();
        assert!(DataPlaneTlsConfig::from_pem(cert, key, "").is_err());
        assert!(DataPlaneTlsConfig::from_pem("", key, ca).is_err());
        assert!(DataPlaneTlsConfig::from_pem(cert, "", ca).is_err());
        assert!(DataPlaneTlsConfig::from_pem(cert, key, ca).is_ok());
    }

    // ---------- codec / framing error-path coverage ----------

    #[test]
    fn credentials_debug_redacts_the_secret_and_empty_secrets_are_rejected() {
        let credentials =
            DataPlaneCredentials::new("node-a", "super-secret").expect("valid credentials");
        let debug = format!("{credentials:?}");
        assert!(debug.contains("node-a"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(!debug.contains("super-secret"), "{debug}");

        let error = DataPlaneCredentials::new("node-a", "")
            .expect_err("an empty shared secret must be rejected");
        assert!(error.to_string().contains("must not be empty"), "{error}");
    }

    #[test]
    fn server_ack_verification_rejects_identity_mismatch() {
        let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
        let auth = credentials.session("node-b", "job-1", 3, quad_a_to_b());
        let request = auth.client_handshake().unwrap();
        let mut ack = auth.server_ack(&request, "node-b");
        // A well-formed ack with the same identity verifies.
        auth.verify_server_ack(&ack).expect("matching ack verifies");
        // Any identity drift is a protocol error before the MAC check.
        ack.job_id = "other-job".into();
        let error = auth.verify_server_ack(&ack).unwrap_err();
        assert!(error.to_string().contains("identity mismatch"), "{error}");
    }

    #[test]
    fn wire_signal_ignores_data_envelopes_and_default_encoder_matches_new() {
        let batch = Arc::new(dictionary_batch(None));
        let envelope = Envelope::Data(batch, Arc::new(crate::input::NoopAck));
        assert!(WireSignal::from_envelope(&envelope).is_none());

        // `DataEncoder::default` is the same encoder as `new`.
        let sample = dictionary_batch(Some("default"));
        let mut via_default = DataEncoder::default();
        let mut via_new = DataEncoder::new();
        let payload_default = via_default.encode(&sample, 1).expect("default encode");
        let payload_new = via_new.encode(&sample, 1).expect("new encode");
        let mut decoder = DataDecoder::new();
        let (decoded, seq) = decoder.decode(&payload_default).expect("default decode");
        assert_eq!(seq, 1);
        assert_eq!(decoded.get_input_name(), Some("default".to_owned()));
        // Both encoders emit byte-identical payloads for the same input.
        assert_eq!(payload_default, payload_new);
    }

    /// Split a data payload at its meta block: returns the rewritten prefix
    /// (with `has_schema` forced to the given value) and the cursor position
    /// just after the original meta block.
    fn rewrite_meta(payload: &[u8], has_schema: bool) -> (Vec<u8>, usize) {
        let meta_len = u32::from_le_bytes(payload[0..4].try_into().expect("sliced")) as usize;
        let mut meta: WireDataMeta =
            serde_json::from_slice(&payload[4..4 + meta_len]).expect("meta");
        meta.has_schema = has_schema;
        let rewritten = serde_json::to_vec(&meta).expect("meta encode");
        let mut out = Vec::new();
        out.extend_from_slice(&(rewritten.len() as u32).to_le_bytes());
        out.extend_from_slice(&rewritten);
        (out, 4 + meta_len)
    }

    /// Re-encode a length-prefixed IPC piece from raw flatbuffer bytes.
    fn craft_piece(flatbuf: &[u8]) -> Vec<u8> {
        let mut piece = Vec::new();
        let total = 8 + flatbuf.len();
        piece.extend_from_slice(&(total as u32).to_le_bytes());
        piece.extend_from_slice(&(flatbuf.len() as u32).to_le_bytes());
        piece.extend_from_slice(flatbuf);
        piece
    }

    #[test]
    fn decoder_rejects_schema_id_mismatch_and_unparsable_schema() {
        let mut encoder_a = DataEncoder::new();
        let payload_a = encoder_a
            .encode(&dictionary_batch(None), 0)
            .expect("encode A");
        let mut decoder = DataDecoder::new();
        decoder.decode(&payload_a).expect("learn schema A");

        // A second schema encoded by a fresh encoder, with its schema piece
        // stripped and the meta forced to `has_schema=false`, declares an id
        // the decoder never learned.
        let plain = int64_only_batch();
        let mut encoder_b = DataEncoder::new();
        let payload_b = encoder_b.encode(&plain, 1).expect("encode B");
        let (mut corrupted, cursor) = rewrite_meta(&payload_b, false);
        // Skip B's schema piece: parse and drop it.
        let (_, after_schema) = read_piece(&payload_b, cursor).expect("schema piece");
        corrupted.extend_from_slice(&payload_b[after_schema..]);
        let error = decoder.decode(&corrupted).unwrap_err();
        assert!(
            error.to_string().contains("does not match declared id"),
            "{error}"
        );

        // A schema-bearing frame whose schema flatbuffer is garbage fails to
        // parse instead of panicking.
        let (mut garbage, _) = rewrite_meta(&payload_b, true);
        garbage.extend_from_slice(&craft_piece(&[0xde, 0xad, 0xbe, 0xef, 0xba, 0xad]));
        let mut fresh_decoder = DataDecoder::new();
        let error = fresh_decoder.decode(&garbage).unwrap_err();
        assert!(
            error.to_string().contains("schema decode failed"),
            "{error}"
        );
    }

    fn int64_only_batch() -> MessageBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("valid batch");
        MessageBatch::new_arrow(batch)
    }

    #[test]
    fn decoder_rejects_invalid_ipc_messages_and_stray_schema_messages() {
        let mut encoder = DataEncoder::new();
        let payload = encoder.encode(&dictionary_batch(None), 0).expect("encode");
        let mut decoder = DataDecoder::new();
        decoder.decode(&payload).expect("prime the schema cache");

        // A payload whose trailing piece is not a valid IPC message.
        let (mut garbage, _) = rewrite_meta(&payload, false);
        garbage.extend_from_slice(&craft_piece(&[0x11, 0x22, 0x33, 0x44, 0x55]));
        let error = decoder.decode(&garbage).unwrap_err();
        assert!(error.to_string().contains("IPC message invalid"), "{error}");

        // A schema message arriving without `has_schema` set is a protocol
        // error: keep the original pieces (schema first) but claim no schema.
        let (mut stray, _) = rewrite_meta(&payload, false);
        stray.extend_from_slice(&payload[4 + meta_len_of(&payload)..]);
        let error = decoder.decode(&stray).unwrap_err();
        assert!(
            error.to_string().contains("unexpected schema message"),
            "{error}"
        );
    }

    fn meta_len_of(payload: &[u8]) -> usize {
        u32::from_le_bytes(payload[0..4].try_into().expect("sliced")) as usize
    }

    #[test]
    fn decoder_rejects_truncated_dictionary_and_record_batch_bodies() {
        let mut encoder = DataEncoder::new();
        let payload = encoder.encode(&dictionary_batch(None), 0).expect("encode");
        let meta_len = meta_len_of(&payload);

        // Collect (start, end, flatbuf_len) for every IPC piece after the
        // meta block.
        let mut pieces = Vec::new();
        let mut cursor = 4 + meta_len;
        while cursor < payload.len() {
            let (piece, next) = read_piece(&payload, cursor).expect("piece");
            pieces.push((cursor, next, piece.flatbuf_len));
            cursor = next;
        }
        // [schema, dictionary..., record batch]
        assert!(
            pieces.len() >= 3,
            "expected schema, dictionary and batch pieces, got {}",
            pieces.len()
        );

        // Prime a decoder with the intact frame so the schema is cached.
        let mut primed = DataDecoder::new();
        primed.decode(&payload).expect("prime the schema cache");

        // Corrupting the body of the dictionary piece (index 1) or the
        // record batch piece (last) must fail the decode instead of yielding
        // wrong data: Arrow validates offsets/lengths against the body.
        // Truncating the body of the dictionary piece (index 1) or the record
        // batch piece (last) past the IPC padding must fail the decode: the
        // declared IPC body no longer fits inside the piece. (Mere byte
        // corruption is NOT reliably detected — the IPC reader defers array
        // validation — so the length contract is the enforced invariant.)
        for index in [1, pieces.len() - 1] {
            let (target_start, _target_end, _flatbuf_len) = pieces[index];
            let mut rebuilt = Vec::new();
            let mut meta: WireDataMeta =
                serde_json::from_slice(&payload[4..4 + meta_len]).expect("meta");
            meta.has_schema = false;
            let meta_bytes = serde_json::to_vec(&meta).expect("meta encode");
            rebuilt.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            rebuilt.extend_from_slice(&meta_bytes);
            // Skip the schema piece (index 0): the primed decoder already
            // cached it and a stray schema message is a different error.
            for &(start, end, _) in pieces.iter().skip(1) {
                let mut bytes = payload[start..end].to_vec();
                if start == target_start && bytes.len() > 16 {
                    let smaller = (bytes.len() - 16) as u32;
                    bytes.truncate(smaller as usize);
                    bytes[0..4].copy_from_slice(&smaller.to_le_bytes());
                }
                rebuilt.extend_from_slice(&bytes);
            }
            let error = primed.decode(&rebuilt).unwrap_err();
            assert!(
                error.to_string().contains("decode failed")
                    || error.to_string().contains("IPC body exceeds IPC piece"),
                "piece {index}: {error}"
            );
        }
    }

    #[test]
    fn read_piece_rejects_flatbuf_exceeding_the_piece_length() {
        // total = 12 but the flatbuf claims 100 bytes: structurally invalid.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&12u32.to_le_bytes());
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[1, 2, 3, 4]);
        let error = match read_piece(&bytes, 0) {
            Err(error) => error,
            Ok(_) => panic!("a flatbuf longer than its piece must be rejected"),
        };
        assert!(
            error.to_string().contains("flatbuf exceeds piece length"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn read_frame_reports_truncated_payloads_and_idle_timeouts() {
        let quad = quad_a_to_b();
        let (mut client, mut server) = tokio::io::duplex(64);
        // A header declaring 4 payload bytes, then the writer disappears.
        let mut header = Vec::new();
        FrameHeader {
            quad,
            len: 4,
            kind: FrameKind::Signal,
        }
        .encode_into(&mut header);
        client.write_all(&header).await.unwrap();
        client.shutdown().await.unwrap();
        let error = read_frame(&mut server).await.unwrap_err();
        assert!(
            error.to_string().contains("closed while reading payload"),
            "{error}"
        );

        // A silent peer under a tiny idle timeout times out on the header.
        let (_quiet_writer, quiet_reader) = tokio::io::duplex(8);
        let error = read_frame_with_limits(
            &mut { quiet_reader },
            MAX_FRAME_LEN,
            Some(std::time::Duration::from_millis(20)),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("read idle timeout"), "{error}");
    }

    #[tokio::test]
    async fn write_frame_with_limit_rejects_oversized_payloads() {
        let (mut client, _server) = tokio::io::duplex(64);
        let error = write_frame_with_limit(
            &mut client,
            quad_a_to_b(),
            FrameKind::Signal,
            b"0123456789abcdef",
            8,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("exceeds the 8 limit"), "{error}");
    }

    #[test]
    fn plaintext_transport_constructor_sets_fields() {
        let transport = TcpEdgeTransport::plaintext("127.0.0.1:9000".parse().unwrap(), 3);
        assert_eq!(transport.addr.port(), 9000);
        assert_eq!(transport.max_attempts, 3);
        assert!(transport.tls.is_none());
    }

    #[test]
    fn tls_rejects_an_unparsable_fleet_ca() {
        let (nodes, _ca) = generate_fleet_material();
        let (cert, key) = &nodes[0];
        // A syntactically valid PEM block whose DER payload is garbage: the
        // CA store ends up empty and startup must fail explicitly.
        let junk_ca = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let error = match DataPlaneTlsConfig::from_pem(cert, key, junk_ca) {
            Err(error) => error,
            Ok(_) => panic!("an unparsable fleet CA must be rejected"),
        };
        assert!(
            error.to_string().contains("no parsable certificates"),
            "{error}"
        );
    }

    #[test]
    fn network_config_validation_rejects_invalid_limits() {
        let invalid = |config: NetworkManagerConfig, what: &str| {
            assert!(config.validate().is_err(), "{what} must be rejected");
        };
        invalid(
            NetworkManagerConfig {
                channel_capacity: 0,
                ..Default::default()
            },
            "a zero channel capacity",
        );
        invalid(
            NetworkManagerConfig {
                max_frame_len: 0,
                ..Default::default()
            },
            "a zero frame limit",
        );
        let error = NetworkManagerConfig {
            max_frame_len: MAX_FRAME_LEN + 1,
            ..Default::default()
        }
        .validate()
        .unwrap_err();
        assert!(error.to_string().contains("must not exceed"), "{error}");
        invalid(
            NetworkManagerConfig {
                read_idle_timeout: std::time::Duration::ZERO,
                ..Default::default()
            },
            "a zero read idle timeout",
        );
        invalid(
            NetworkManagerConfig {
                reconnect_grace: std::time::Duration::ZERO,
                ..Default::default()
            },
            "a zero reconnect grace",
        );
        invalid(
            NetworkManagerConfig {
                registration_grace: std::time::Duration::ZERO,
                ..Default::default()
            },
            "a zero registration grace",
        );
        invalid(
            NetworkManagerConfig {
                handshake_replay_ttl: std::time::Duration::ZERO,
                ..Default::default()
            },
            "a zero replay TTL",
        );
        let error = NetworkManagerConfig {
            credentials: Some(DataPlaneCredentials::new("  ", "s").unwrap()),
            ..Default::default()
        }
        .validate()
        .unwrap_err();
        assert!(
            error.to_string().contains("local_node must not be empty"),
            "{error}"
        );
        assert!(NetworkManagerConfig::default().validate().is_ok());
    }

    #[test]
    fn trace_context_extraction_rejects_unparsable_values() {
        assert!(extract_trace_context("garbage").is_none());
        assert!(extract_trace_context("").is_none());
        let valid = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        assert!(extract_trace_context(valid).is_some());
    }

    // ---------- pending receipt / ack lifecycle coverage ----------

    /// An acknowledgement whose lifecycle calls always fail.
    struct FailingLifecycleAck;

    #[async_trait::async_trait]
    impl crate::input::Ack for FailingLifecycleAck {
        async fn ack(&self) -> Result<(), Error> {
            Err(Error::Process("ack boom".into()))
        }
        async fn abort(&self) -> Result<(), Error> {
            Err(Error::Process("abort boom".into()))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failing_branch_lifecycles_surface_as_failures() {
        let pending = PendingReceipts::new(64);
        let (failures_tx, failures_rx) = flume::unbounded::<Error>();

        let seq = pending
            .register(
                &(Arc::new(FailingLifecycleAck) as Arc<dyn crate::input::Ack>),
                1,
                None,
            )
            .expect("register");
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq,
            },
            &failures_tx,
        );
        let failure = recv_within(&failures_rx, std::time::Duration::from_secs(5)).await;
        assert!(
            failure.contains("branch acknowledgement failed"),
            "{failure}"
        );

        let seq = pending
            .register(
                &(Arc::new(FailingLifecycleAck) as Arc<dyn crate::input::Ack>),
                1,
                None,
            )
            .expect("register");
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Failed,
                seq,
            },
            &failures_tx,
        );
        let failure = recv_within(&failures_rx, std::time::Duration::from_secs(5)).await;
        assert!(failure.contains("branch abort failed"), "{failure}");
    }

    async fn recv_within(receiver: &flume::Receiver<Error>, budget: std::time::Duration) -> String {
        tokio::time::timeout(budget, receiver.recv_async())
            .await
            .expect("failure within timeout")
            .expect("failure channel open")
            .to_string()
    }

    #[tokio::test]
    async fn remote_ack_abort_on_a_dead_outbox_reports_failure() {
        let (outbox, _outbox_rx) = flume::unbounded::<(Quad, ReceiptFrame)>();
        drop(_outbox_rx);
        let (failures, _failure_rx) = flume::unbounded::<Error>();
        let ack = RemoteAck {
            outbox,
            quad: quad_a_to_b(),
            seq: 9,
            failures,
        };
        let error = ack.abort().await.unwrap_err();
        assert!(
            error.to_string().contains("receipt channel closed"),
            "{error}"
        );
    }

    // ---------- session receipt routing coverage ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn session_receipt_forwarder_retries_until_a_live_writer_appears() {
        let manager = NetworkManager::new(8);
        let quad = quad_a_to_b();
        let key = EdgeSessionKey::for_job("route-job", 1, quad);

        // No writer installed yet: the forwarder parks and retries.
        let queue = manager.session_receipt_route(&key);
        queue
            .send_async((
                quad,
                ReceiptFrame {
                    kind: ReceiptKind::Acked,
                    seq: 0,
                },
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // A dead writer slot is retried past as well.
        let (dead_tx, dead_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
        drop(dead_rx);
        manager.set_session_receipt_writer(&key, Some(dead_tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // A live writer drains the queued receipt.
        let (live_tx, live_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
        manager.set_session_receipt_writer(&key, Some(live_tx));
        let received =
            tokio::time::timeout(std::time::Duration::from_secs(5), live_rx.recv_async())
                .await
                .expect("receipt forwarded within timeout")
                .expect("writer channel open");
        assert_eq!(received.1.seq, 0);

        // Clearing an unknown key is a no-op; clearing routes cancels the
        // forwarder task.
        let (foreign_tx, _foreign_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
        manager.clear_session_receipt_writer_if_owned(
            &EdgeSessionKey::for_job("other-job", 1, quad),
            &foreign_tx,
        );
        manager.remove_session_receipt_route(&key);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[test]
    fn legacy_failure_receiver_is_process_wide() {
        let manager = NetworkManager::new(8);
        // Without credentials the per-Job receiver falls back to the shared
        // process-wide channel.
        let failures = manager.failure_receiver_for_job("legacy", 4);
        manager.report_failure(Error::Process("legacy failure".into()));
        let failure = failures
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(failure.to_string().contains("legacy failure"), "{failure}");
    }

    #[test]
    fn tls_config_accessor_reflects_the_configuration() {
        assert!(NetworkManager::new(8).tls_config().is_none());
        let (nodes, ca) = generate_fleet_material();
        let (cert, key) = &nodes[0];
        let manager = NetworkManager::with_config(NetworkManagerConfig {
            credentials: Some(DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap()),
            tls: Some(DataPlaneTlsConfig::from_pem(cert, key, &ca).unwrap()),
            ..Default::default()
        })
        .unwrap();
        assert!(manager.tls_config().is_some());
    }

    #[tokio::test]
    async fn accept_stream_enforces_the_connection_limit() {
        let manager = NetworkManager::with_config(NetworkManagerConfig {
            max_connections: 1,
            ..Default::default()
        })
        .unwrap();
        let failures = manager.failure_receiver();
        let (_c1, s1) = tokio::io::duplex(64);
        manager.accept_stream(Box::new(s1));
        let (_c2, s2) = tokio::io::duplex(64);
        manager.accept_stream(Box::new(s2));
        let failure = failures
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(
            failure
                .to_string()
                .contains("accepted-connection limit 1 reached"),
            "{failure}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn eager_open_edge_connects_through_the_transport() {
        let downstream = NetworkManager::new(8);
        downstream.spawn();
        let quad = quad_a_to_b();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);
        let (client, server) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server));
        let transport = QueuedTransport::of(vec![Box::new(client)]);
        let upstream = NetworkManager::new(8);
        upstream.spawn();
        let edge = upstream
            .open_edge(&*transport, quad)
            .await
            .expect("eager edge opens over the queued transport");
        edge.sender
            .send_async(Envelope::Eos)
            .await
            .expect("edge accepts envelopes");
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test]
    async fn authenticated_edges_require_credentials() {
        let manager = NetworkManager::new(8);
        let transport: Arc<dyn EdgeTransport> = QueuedTransport::of(vec![]);
        let error = match manager.open_edge_deferred_for_session(
            transport.clone(),
            quad_a_to_b(),
            "node-b".into(),
            "job".into(),
            1,
        ) {
            Err(error) => error,
            Ok(_) => panic!("an unauthenticated manager must refuse a session edge"),
        };
        assert!(
            error.to_string().contains("without data-plane credentials"),
            "{error}"
        );
        let error = match manager.open_edge_deferred_for_job(
            transport,
            quad_a_to_b(),
            "node-b".into(),
            "job".into(),
            1,
        ) {
            Err(error) => error,
            Ok(_) => panic!("an unauthenticated manager must refuse a job edge"),
        };
        assert!(
            error.to_string().contains("without data-plane credentials"),
            "{error}"
        );
    }

    /// A transport whose `connect` blocks until the test releases a gate, so
    /// a deferred edge's attach can be observed after job-session teardown.
    struct GatedTransport {
        gate: Arc<tokio::sync::Mutex<()>>,
        stream: tokio::sync::Mutex<Option<Box<dyn RemoteStream>>>,
    }

    #[async_trait::async_trait]
    impl EdgeTransport for GatedTransport {
        async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
            let _guard = self.gate.lock().await;
            self.stream
                .lock()
                .await
                .take()
                .ok_or_else(|| Error::Process("gated transport exhausted".into()))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_attach_after_job_removal_aborts_without_recreating_state() {
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        let quad = quad_a_to_b();
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let _hold = gate.clone().lock_owned().await;
        let (client, server) = tokio::io::duplex(64 * 1024);
        let transport = Arc::new(GatedTransport {
            gate: gate.clone(),
            stream: tokio::sync::Mutex::new(Some(Box::new(client))),
        });
        let _server_kept = server;
        let edge = downstream
            .open_edge_deferred_for_session(
                transport.clone(),
                quad,
                "node-a".into(),
                "late-attach-job".into(),
                1,
            )
            .expect("deferred authenticated edge");
        // Create session-scoped routing state for the job, then tear the job
        // session down while the deferred connect is still parked.
        let key = EdgeSessionKey::for_job("late-attach-job", 1, quad);
        let _route = downstream.session_receipt_route(&key);
        let failures = downstream.failure_receiver_for_job("late-attach-job", 1);
        downstream.remove_job_session("late-attach-job", 1);
        assert!(job_session_state_is_cleared(&downstream));
        drop(_hold); // release the connect
                     // The edge fails closed: the sender errors once the aborted receiver
                     // is dropped by the attach path.
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            loop {
                if edge.sender.send_async(Envelope::Eos).await.is_err() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("edge must fail closed after job removal");
        // No failure may land in the recreated per-Job channel: the Job is
        // gone and its state must not be resurrected.
        if let Ok(Ok(failure)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), failures.recv_async()).await
        {
            panic!("unexpected late failure: {failure}");
        }
        downstream.shutdown();
    }

    fn job_session_state_is_cleared(manager: &NetworkManager) -> bool {
        manager.inbound.read().unwrap().is_empty()
            && manager.failure_channels.read().unwrap().is_empty()
    }

    // ---------- connection supervision coverage ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_replay_flush_failure_fails_the_edge_closed() {
        let quad = quad_a_to_b();
        let reconnect = NetworkManagerConfig {
            reconnect_attempts: 2,
            reconnect_grace: std::time::Duration::from_millis(200),
            ..Default::default()
        };
        let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
        let downstream = NetworkManager::with_config(reconnect).unwrap();
        upstream.spawn();
        downstream.spawn();
        let failures = upstream.failure_receiver();

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream.register_inbound(quad, input_tx);

        let (client1, server1) = tokio::io::duplex(64 * 1024);
        let kill1 = KillHandle::default();
        let kill_state = kill1.0.clone();
        downstream.accept_stream(Box::new(KillableStream {
            inner: Box::new(server1),
            state: kill_state,
        }));
        // The second connection dials into a stream whose peer is gone: the
        // replay flush fails and the edge fails closed.
        let (client2, server2) = tokio::io::duplex(64 * 1024);
        drop(server2);
        let transport = QueuedTransport::of(vec![Box::new(client1), Box::new(client2)]);
        let edge = upstream.open_edge_deferred(transport, quad);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let branch = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                branch.clone(),
            ))
            .await
            .unwrap();
        let _ = next_envelope(&input_rx).await;
        kill1.kill();

        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the unreplayable branch must abort");
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.contains("replay flush failed") || failure.contains("reconnect"),
            "{failure}"
        );
        upstream.shutdown();
        downstream.shutdown();
    }

    /// A stream whose reads never complete and whose writes always fail:
    /// isolates the pump's flush-tick failure exit.
    struct DeadWriteStream;

    impl AsyncRead for DeadWriteStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    impl AsyncWrite for DeadWriteStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "dead write",
            )))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "dead flush",
            )))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pump_flush_tick_failure_fails_the_edge() {
        let manager = NetworkManager::new(8);
        manager.spawn();
        let quad = quad_a_to_b();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad, input_tx);
        // A plaintext (unauthenticated) edge: the pump has no handshake to
        // write, so the first failure comes from the flush tick.
        let edge = manager.open_edge_with_stream(Box::new(DeadWriteStream), quad);
        let failures = manager.failure_receiver();
        let _ = edge;
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(failure.contains("flush failed"), "{failure}");
        manager.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn authenticated_handshake_flush_failure_fails_the_edge() {
        let manager = authenticated_manager("node-a", "shuffle-secret");
        manager.spawn();
        let quad = quad_a_to_b();
        let (client, server) = tokio::io::duplex(64 * 1024);
        drop(server);
        let failures = manager.failure_receiver_for_job("flush-job", 1);
        let _edge = manager
            .open_edge_with_stream_for_session(Box::new(client), quad, "node-b", "flush-job", 1)
            .unwrap();
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(failure.contains("handshake flush failed"), "{failure}");
        manager.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pump_register_rejection_with_failing_abort_combines_errors() {
        let quad = quad_a_to_b();
        let config = NetworkManagerConfig {
            max_pending_receipts: 1,
            ..Default::default()
        };
        let pending = Arc::new(PendingReceipts::new(1));
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (writer, _reader) = tokio::io::duplex(64 * 1024);
        let (tx, rx) = flume::bounded::<Envelope>(4);
        let first = Arc::new(RecordingAck::default());
        tx.send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            first.clone(),
        ))
        .await
        .unwrap();
        tx.send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            Arc::new(FailingLifecycleAck),
        ))
        .await
        .unwrap();
        drop(tx);
        let result = pump_edge(rx, writer, quad, pending, shutdown, config, None, false).await;
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to abort batch rejected by pending receipt limit"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receipt_channel_rejects_protocol_violations() {
        let quad = quad_a_to_b();
        let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
        let auth = credentials.session("node-b", "rcv-job", 1, quad);
        let config = NetworkManagerConfig {
            read_idle_timeout: std::time::Duration::from_secs(2),
            ..Default::default()
        };

        // (a) A receipt before the handshake acknowledgement.
        {
            let (client, mut server) = tokio::io::duplex(64 * 1024);
            let receipt = serde_json::to_vec(&ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq: 0,
            })
            .unwrap();
            // Frames are written up front (the duplex buffers them) and the
            // peer half stays alive until the harness settles, so the pump's
            // own handshake write cannot race a dropped peer.
            write_frame(&mut server, quad, FrameKind::Receipt, &receipt)
                .await
                .unwrap();
            let (_pump, read) =
                run_receipt_harness(Box::new(client), quad, Some(auth.clone()), config.clone())
                    .await;
            drop(server);
            let error = read.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("arrived before handshake acknowledgement"),
                "{error}"
            );
        }

        // (b) A malformed receipt after a valid acknowledgement.
        {
            let (client, mut server) = tokio::io::duplex(64 * 1024);
            let request = auth.client_handshake().unwrap();
            let ack = auth.server_ack(&request, "node-b");
            let ack_payload = serde_json::to_vec(&ack).unwrap();
            write_frame(&mut server, quad, FrameKind::Handshake, &ack_payload)
                .await
                .unwrap();
            write_frame(&mut server, quad, FrameKind::Receipt, b"not json")
                .await
                .unwrap();
            let (_pump, read) =
                run_receipt_harness(Box::new(client), quad, Some(auth.clone()), config.clone())
                    .await;
            drop(server);
            let error = read.unwrap_err();
            assert!(
                error.to_string().contains("receipt frame malformed"),
                "{error}"
            );
        }

        // (c) A data frame on the receipt channel.
        {
            let (client, mut server) = tokio::io::duplex(64 * 1024);
            let request = auth.client_handshake().unwrap();
            let ack = auth.server_ack(&request, "node-b");
            let ack_payload = serde_json::to_vec(&ack).unwrap();
            write_frame(&mut server, quad, FrameKind::Handshake, &ack_payload)
                .await
                .unwrap();
            write_frame(&mut server, quad, FrameKind::Data, b"stray")
                .await
                .unwrap();
            let (_pump, read) =
                run_receipt_harness(Box::new(client), quad, Some(auth), config).await;
            drop(server);
            let error = read.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("unexpected Data frame on a receipt channel"),
                "{error}"
            );
        }
    }

    /// Drive `run_edge_connection` over a drained envelope channel and return
    /// its (pump, read) results.
    async fn run_receipt_harness(
        client: Box<dyn RemoteStream>,
        quad: Quad,
        auth: Option<SessionAuth>,
        config: NetworkManagerConfig,
    ) -> (Result<(), Error>, Result<(), Error>) {
        let (tx, rx) = flume::bounded::<Envelope>(1);
        drop(tx);
        let pending = Arc::new(PendingReceipts::new(64));
        let (failures_tx, _failures_rx) = flume::unbounded::<Error>();
        let shutdown = tokio_util::sync::CancellationToken::new();
        tokio::time::timeout(
            TEST_PROPAGATION_BUDGET,
            run_edge_connection(
                client,
                quad,
                rx,
                pending,
                config,
                auth,
                false,
                failures_tx,
                &shutdown,
            ),
        )
        .await
        .expect("connection harness settles within timeout")
    }

    // ---------- inbound serve-loop error coverage ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_handshake_and_quad_mismatch_fail_closed() {
        let quad = quad_a_to_b();
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        downstream.spawn();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "hs-job".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let failures = downstream.failure_receiver_for_job("hs-job", 1);

        let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
        let auth = credentials.session("node-b", "hs-job", 1, quad);

        // A second handshake frame on an authenticated connection.
        let (mut client1, server1) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server1));
        let request = serde_json::to_vec(&auth.client_handshake().unwrap()).unwrap();
        write_frame(&mut client1, quad, FrameKind::Handshake, &request)
            .await
            .unwrap();
        // Drain the server's acknowledgement before sending the duplicate.
        let (_ack_header, _ack_payload) = read_frame(&mut client1).await.unwrap();
        write_frame(&mut client1, quad, FrameKind::Handshake, &request)
            .await
            .unwrap();
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure
                .to_string()
                .contains("duplicate remote edge handshake"),
            "{failure}"
        );

        // A data frame for a different quad than the authenticated one.
        let wrong_quad = Quad {
            dst_subtask: quad.dst_subtask + 1,
            ..quad
        };
        let (mut client2, server2) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server2));
        // A fresh handshake: the server's nonce cache rejects replayed ones.
        let fresh_request = serde_json::to_vec(&auth.client_handshake().unwrap()).unwrap();
        write_frame(&mut client2, quad, FrameKind::Handshake, &fresh_request)
            .await
            .unwrap();
        let (_ack_header, _ack_payload) = read_frame(&mut client2).await.unwrap();
        write_frame(
            &mut client2,
            wrong_quad,
            FrameKind::Signal,
            serde_json::to_vec(&WireSignal::Eos).unwrap().as_slice(),
        )
        .await
        .unwrap();
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure
                .to_string()
                .contains("does not match authenticated quad"),
            "{failure}"
        );
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inbound_protocol_violations_fail_the_connection() {
        let quad = quad_a_to_b();
        let config = NetworkManagerConfig {
            registration_grace: std::time::Duration::from_millis(100),
            reconnect_attempts: 0,
            ..Default::default()
        };
        let manager = NetworkManager::with_config(config).unwrap();
        manager.spawn();
        let failures = manager.failure_receiver();

        // An unregistered quad: the registration grace expires and the frame
        // is rejected.
        let (mut client1, server1) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server1));
        write_frame(&mut client1, quad, FrameKind::Signal, b"{}")
            .await
            .unwrap();
        let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
        assert!(
            failure
                .to_string()
                .contains("inbound frame for unregistered quad"),
            "{failure}"
        );

        // Registered quads, garbage payloads: data and signal frames are
        // rejected as decode failures, receipts are rejected outright. Each
        // sub-case uses a fresh quad so a prior connection's cleanup cannot
        // deregister the route mid-test.
        let quad_b = Quad {
            dst_op: quad.dst_op + 1,
            ..quad
        };
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad_b, input_tx);

        let (mut client2, server2) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server2));
        write_frame(&mut client2, quad_b, FrameKind::Data, b"garbage")
            .await
            .unwrap();
        let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
        assert!(
            failure
                .to_string()
                .contains("inbound data frame decode failed"),
            "{failure}"
        );

        let quad_c = Quad {
            dst_op: quad.dst_op + 2,
            ..quad
        };
        let (input_tx3, _input_rx3) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad_c, input_tx3);
        let (mut client3, server3) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server3));
        write_frame(&mut client3, quad_c, FrameKind::Signal, b"garbage")
            .await
            .unwrap();
        let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
        assert!(
            failure
                .to_string()
                .contains("inbound signal frame malformed"),
            "{failure}"
        );

        let quad_d = Quad {
            dst_op: quad.dst_op + 3,
            ..quad
        };
        let (input_tx4, _input_rx4) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad_d, input_tx4);
        let (mut client4, server4) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server4));
        write_frame(
            &mut client4,
            quad_d,
            FrameKind::Receipt,
            serde_json::to_vec(&ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq: 0,
            })
            .unwrap()
            .as_slice(),
        )
        .await
        .unwrap();
        let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
        assert!(
            failure
                .to_string()
                .contains("receipt frame on an inbound (downstream) connection"),
            "{failure}"
        );
        drop(input_rx);
        manager.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_chain_death_breaks_the_edge_and_reports_midstream_loss() {
        let quad = quad_a_to_b();
        let manager = NetworkManager::with_config(NetworkManagerConfig {
            reconnect_attempts: 0,
            ..Default::default()
        })
        .unwrap();
        manager.spawn();
        let failures = manager.failure_receiver();

        // The local chain vanishes: the serve loop's channel send fails and
        // the connection ends reporting an upstream death without Eos.
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad, input_tx);
        drop(input_rx);
        let mut encoder = DataEncoder::new();
        let data_payload = encoder.encode(&dictionary_batch(None), 0).unwrap();
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server));
        write_frame(&mut client, quad, FrameKind::Data, &data_payload)
            .await
            .unwrap();
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.to_string().contains("ended without Eos"),
            "{failure}"
        );

        // A non-Eos signal onto a dead local chain breaks cleanly too (an
        // Eos would mark the quad complete and report nothing).
        let (input_tx2, input_rx2) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad, input_tx2);
        drop(input_rx2);
        let (mut client2, server2) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server2));
        write_frame(
            &mut client2,
            quad,
            FrameKind::Signal,
            serde_json::to_vec(&WireSignal::Watermark(1))
                .unwrap()
                .as_slice(),
        )
        .await
        .unwrap();
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.to_string().contains("ended without Eos"),
            "{failure}"
        );
        manager.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_loss_watcher_stops_at_shutdown_without_reporting() {
        let quad = quad_a_to_b();
        let manager = NetworkManager::with_config(NetworkManagerConfig {
            reconnect_grace: std::time::Duration::from_millis(300),
            ..Default::default()
        })
        .unwrap();
        manager.spawn();
        let failures = manager.failure_receiver();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad, input_tx);
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server));
        write_frame(
            &mut client,
            quad,
            FrameKind::Signal,
            serde_json::to_vec(&WireSignal::Eos).unwrap().as_slice(),
        )
        .await
        .unwrap();
        client.shutdown().await.unwrap();
        // The peer closed without Eos reaching any served quad? Eos was
        // forwarded, so this close is clean; instead simulate a mid-stream
        // death by registering a second quad that never sees Eos.
        let quad2 = Quad {
            dst_op: quad.dst_op + 1,
            ..quad
        };
        let (input_tx2, _input_rx2) = flume::bounded::<Envelope>(8);
        manager.register_inbound(quad2, input_tx2);
        let (mut client2, server2) = tokio::io::duplex(64 * 1024);
        manager.accept_stream(Box::new(server2));
        let mut encoder = DataEncoder::new();
        let data_payload = encoder.encode(&dictionary_batch(None), 0).unwrap();
        write_frame(&mut client2, quad2, FrameKind::Data, &data_payload)
            .await
            .unwrap();
        drop(client2);
        // Shut the manager down inside the grace window: the deferred loss
        // watcher must not report a failure afterwards.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        manager.shutdown();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(600), failures.recv_async())
                .await
                .is_err(),
            "shutdown must suppress the deferred loss report"
        );
    }

    #[tokio::test]
    async fn authorize_inbound_rejects_identity_drift() {
        let quad = quad_a_to_b();
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
        let auth = credentials.session("node-b", "id-job", 1, quad);
        let mut request = auth.client_handshake().unwrap();
        // A protocol-version drift is rejected by the identity check before
        // any MAC or registry work.
        request.protocol_version = DATA_PLANE_PROTOCOL_VERSION + 1;
        let payload = serde_json::to_vec(&request).unwrap();
        let header = FrameHeader {
            quad,
            len: payload.len() as u32,
            kind: FrameKind::Handshake,
        };
        let error = match downstream.authorize_inbound(header, &payload).await {
            Err(error) => error,
            Ok(_) => panic!("a drifted handshake identity must be rejected"),
        };
        assert!(
            error.to_string().contains("protocol or identity mismatch"),
            "{error}"
        );
    }

    /// Authenticated transparent reconnect currently fails closed: the
    /// reconnect path writes the session handshake twice on the new wire —
    /// once by `replay_pending` and again by `pump_edge`'s startup handshake —
    /// and the peer's duplicate-handshake guard tears the connection down.
    /// The edge then exhausts its redial budget and fails closed (branches
    /// abort, failure surfaces), which is the safe outcome; the double
    /// handshake itself is a product defect this test pins observably.
    #[tokio::test(flavor = "multi_thread")]
    async fn authenticated_reconnect_currently_fails_closed_after_handshake_duplication() {
        let quad = quad_a_to_b();
        let node_config = |node: &str| NetworkManagerConfig {
            credentials: Some(DataPlaneCredentials::new(node, "shuffle-secret").unwrap()),
            registration_grace: std::time::Duration::from_millis(100),
            reconnect_grace: std::time::Duration::from_millis(800),
            reconnect_attempts: 2,
            ..Default::default()
        };
        let upstream = NetworkManager::with_config(node_config("node-a")).unwrap();
        let downstream = NetworkManager::with_config(node_config("node-b")).unwrap();
        upstream.spawn();
        downstream.spawn();
        let failures = upstream.failure_receiver_for_job("auth-rc-job", 2);

        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "auth-rc-job".into(),
                    generation: 2,
                },
            )
            .unwrap();

        let (client1, server1) = tokio::io::duplex(64 * 1024);
        let (client2, server2) = tokio::io::duplex(64 * 1024);
        let kill1 = KillHandle::default();
        let kill_state = kill1.0.clone();
        downstream.accept_stream(Box::new(KillableStream {
            inner: Box::new(server1),
            state: kill_state,
        }));
        downstream.accept_stream(Box::new(server2));
        let transport = QueuedTransport::of(vec![Box::new(client1), Box::new(client2)]);
        let edge = upstream
            .open_edge_deferred_for_session(
                transport,
                quad,
                "node-b".into(),
                "auth-rc-job".into(),
                2,
            )
            .expect("deferred authenticated edge");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let branch = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(Some("auth-rc"))),
                branch.clone(),
            ))
            .await
            .unwrap();
        let delivered = next_envelope(&input_rx).await;
        let Envelope::Data(_batch, _ack) = delivered else {
            panic!("expected a data envelope");
        };
        kill1.kill();

        // The redial budget exhausts and the edge fails closed.
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the unrecoverable branch must abort");
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.contains("reconnect"),
            "expected the reconnect budget failure, got: {failure}"
        );
        upstream.shutdown();
        downstream.shutdown();
    }

    // ---------- remaining supervision / registry coverage ----------

    /// A stream whose writes fail (reads keep working) after `kill` is set,
    /// isolating the receipt writer's write-failure exit without tearing the
    /// read half down at the same time.
    struct WriteKillStream {
        inner: Box<dyn RemoteStream>,
        killed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl AsyncRead for WriteKillStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for WriteKillStream {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.killed.load(std::sync::atomic::Ordering::Acquire) {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "write killed",
                )));
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// A stream whose writes always fail while flushes succeed, so the pump's
    /// periodic flush tick cannot mask the per-frame write failure.
    struct WriteFailStream;

    impl AsyncRead for WriteFailStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    impl AsyncWrite for WriteFailStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write failed",
            )))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn tcp_transport_reports_failure_after_the_attempt_budget() {
        // Bind, note the port, and drop the listener: the port is (in
        // practice) unbound, so both attempts are refused.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let transport =
            TcpEdgeTransport::plaintext(format!("127.0.0.1:{port}").parse().unwrap(), 2);
        let error = match transport.connect(quad_a_to_b()).await {
            Err(error) => error,
            Ok(_) => panic!("an unbound peer must exhaust the attempt budget"),
        };
        assert!(
            error.to_string().contains("failed after 2 attempts"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receipt_routing_settles_partial_replicas_and_ignores_strays() {
        let pending = PendingReceipts::new(64);
        let (failures_tx, _failures_rx) = flume::unbounded::<Error>();
        let first = Arc::new(RecordingAck::default());
        // Two replicas: the first Acked receipt must not complete the branch.
        let first_branch: Arc<dyn crate::input::Ack> = first.clone();
        let seq = pending.register(&first_branch, 2, None).unwrap();
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq,
            },
            &failures_tx,
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !first.acked.load(std::sync::atomic::Ordering::SeqCst),
            "one of two replicas must not complete the branch"
        );
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq,
            },
            &failures_tx,
        );
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !first.acked.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the second replica completes the branch");
        // A duplicate receipt for the completed sequence is dropped.
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq,
            },
            &failures_tx,
        );

        // Double Held re-asserts only on the 0→1 transition; the release
        // only forwards once every held replica reported back.
        let second = Arc::new(RecordingAck::default());
        let second_branch: Arc<dyn crate::input::Ack> = second.clone();
        let seq = pending.register(&second_branch, 1, None).unwrap();
        for _ in 0..2 {
            pending.apply(
                ReceiptFrame {
                    kind: ReceiptKind::Held,
                    seq,
                },
                &failures_tx,
            );
        }
        assert!(second.held.load(std::sync::atomic::Ordering::SeqCst));
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Released,
                seq,
            },
            &failures_tx,
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !second.released.load(std::sync::atomic::Ordering::SeqCst),
            "one of two held replicas must not release the branch"
        );
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Released,
                seq,
            },
            &failures_tx,
        );
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
            while !second.released.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the last held replica releases the branch");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receipt_forwarder_stops_on_manager_shutdown() {
        let manager = NetworkManager::new(8);
        let key = EdgeSessionKey::for_job("fwd-shutdown-job", 1, quad_a_to_b());
        // No writer installed: the forwarder parks in its retry loop, then
        // the manager's shutdown must retire it.
        let queue = manager.session_receipt_route(&key);
        queue
            .send_async((
                quad_a_to_b(),
                ReceiptFrame {
                    kind: ReceiptKind::Acked,
                    seq: 0,
                },
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        manager.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    fn raw_handshake(node: &str, job: &str, generation: u64, quad: Quad) -> (FrameHeader, Vec<u8>) {
        let credentials =
            DataPlaneCredentials::new(node, "shuffle-secret").expect("test credentials");
        let auth = credentials.session("node-b", job, generation, quad);
        let payload = serde_json::to_vec(&auth.client_handshake().unwrap()).unwrap();
        let header = FrameHeader {
            quad,
            len: payload.len() as u32,
            kind: FrameKind::Handshake,
        };
        (header, payload)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn authenticated_protocol_violation_reports_through_the_job_channel() {
        let quad = quad_a_to_b();
        let upstream = authenticated_manager("node-a", "shuffle-secret");
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        upstream.spawn();
        downstream.spawn();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-pv".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let failures = downstream.failure_receiver_for_job("job-pv", 1);
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server));
        let (_header, payload) = raw_handshake("node-a", "job-pv", 1, quad);
        write_frame(&mut client, quad, FrameKind::Handshake, &payload)
            .await
            .unwrap();
        let (_ack_header, _ack_payload) = read_frame(&mut client).await.unwrap();
        // A malformed signal frame after authentication is a connection
        // failure routed through the Job's registered failure channel.
        write_frame(&mut client, quad, FrameKind::Signal, b"garbage")
            .await
            .unwrap();
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.contains("inbound signal frame malformed"),
            "{failure}"
        );
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handshake_write_failure_releases_the_inbound_registration() {
        let quad = quad_a_to_b();
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        downstream.spawn();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-hw".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let failures = downstream.failure_receiver_for_job("job-hw", 1);
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server));
        let (_header, payload) = raw_handshake("node-a", "job-hw", 1, quad);
        write_frame(&mut client, quad, FrameKind::Handshake, &payload)
            .await
            .unwrap();
        // The peer disappears before the acknowledgement can be written.
        drop(client);
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.contains("remote edge write failed") || failure.contains("handshake flush"),
            "{failure}"
        );
        // The session's registration is released with the failed handshake.
        assert!(downstream.inbound.read().unwrap().is_empty());
        assert!(downstream.inbound_auth.read().unwrap().is_empty());
        downstream.shutdown();
    }

    #[tokio::test]
    async fn handshake_nonce_cache_overflow_rejects_new_handshakes() {
        let quad = quad_a_to_b();
        let config = NetworkManagerConfig {
            credentials: Some(
                DataPlaneCredentials::new("node-b", "shuffle-secret").expect("credentials"),
            ),
            max_connections: 1,
            registration_grace: std::time::Duration::from_millis(100),
            ..Default::default()
        };
        let downstream = NetworkManager::with_config(config).expect("valid config");
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-nl".into(),
                    generation: 1,
                },
            )
            .unwrap();
        // Capacity is max_connections * 4: four distinct nonces fill it.
        for _ in 0..4 {
            let (header, payload) = raw_handshake("node-a", "job-nl", 1, quad);
            downstream
                .authorize_inbound(header, &payload)
                .await
                .expect("distinct nonces are accepted");
        }
        let (header, payload) = raw_handshake("node-a", "job-nl", 1, quad);
        let error = match downstream.authorize_inbound(header, &payload).await {
            Err(error) => error,
            Ok(_) => panic!("the replay cache is bounded"),
        };
        assert!(
            error.to_string().contains("replay cache is full"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_connection_error_after_job_removal_is_swallowed() {
        let quad = quad_a_to_b();
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        downstream.spawn();
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-late".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server));
        let (_header, payload) = raw_handshake("node-a", "job-late", 1, quad);
        write_frame(&mut client, quad, FrameKind::Handshake, &payload)
            .await
            .unwrap();
        let (_ack_header, _ack_payload) = read_frame(&mut client).await.unwrap();
        let mut encoder = DataEncoder::new();
        let data = encoder.encode(&dictionary_batch(None), 0).unwrap();
        write_frame(&mut client, quad, FrameKind::Data, &data)
            .await
            .unwrap();
        let _ = next_envelope(&input_rx).await;
        // The Job is torn down, then the connection fails: reporting the
        // error must not recreate the removed Job's state.
        downstream.remove_job_session("job-late", 1);
        write_frame(&mut client, quad, FrameKind::Signal, b"garbage")
            .await
            .unwrap();
        let global_failures = downstream.failure_receiver();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(300),
                global_failures.recv_async()
            )
            .await
            .is_err(),
            "a late error after job removal must not surface globally"
        );
        assert!(
            downstream.failure_channels.read().unwrap().is_empty(),
            "the removed Job's failure channel must not be recreated"
        );
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receipt_write_failure_fails_the_serving_connection() {
        let quad = quad_a_to_b();
        let upstream = authenticated_manager("node-a", "shuffle-secret");
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        upstream.spawn();
        downstream.spawn();
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-rw".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let failures = downstream.failure_receiver_for_job("job-rw", 1);
        let killed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (client, server) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(WriteKillStream {
            inner: Box::new(server),
            killed: killed.clone(),
        }));
        let edge = upstream
            .open_edge_with_stream_for_session(Box::new(client), quad, "node-b", "job-rw", 1)
            .unwrap();
        let branch = Arc::new(RecordingAck::default());
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(Some("rw"))),
                branch.clone(),
            ))
            .await
            .unwrap();
        let received = next_envelope(&input_rx).await;
        let Envelope::Data(_, ack) = received else {
            panic!("expected data");
        };
        killed.store(true, std::sync::atomic::Ordering::Release);
        ack.ack().await.expect("the local chain acknowledges");
        let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
        assert!(
            failure.contains("receipt write failed") || failure.contains("receipt flush failed"),
            "{failure}"
        );
        upstream.shutdown();
        downstream.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_session_loss_is_suppressed_when_the_upstream_reregisters() {
        let quad = quad_a_to_b();
        let downstream = NetworkManager::with_config(NetworkManagerConfig {
            credentials: Some(
                DataPlaneCredentials::new("node-b", "shuffle-secret").expect("credentials"),
            ),
            registration_grace: std::time::Duration::from_millis(100),
            // A generous grace keeps the recovery window open even when the
            // test binary runs under load.
            reconnect_grace: std::time::Duration::from_secs(3),
            ..Default::default()
        })
        .unwrap();
        downstream.spawn();
        let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-rl".into(),
                    generation: 1,
                },
            )
            .unwrap();
        let failures = downstream.failure_receiver_for_job("job-rl", 1);

        // First connection serves a delivery, then dies without Eos.
        let upstream1 = authenticated_manager("node-a", "shuffle-secret");
        upstream1.spawn();
        let (client1, server1) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server1));
        let edge1 = upstream1
            .open_edge_with_stream_for_session(Box::new(client1), quad, "node-b", "job-rl", 1)
            .unwrap();
        edge1
            .sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                Arc::new(crate::input::NoopAck),
            ))
            .await
            .unwrap();
        let _ = next_envelope(&input_rx).await;
        // Shut the first upstream down: its connection dies mid-stream (no
        // Eos), which the downstream defers through the reconnect grace.
        drop(edge1);
        upstream1.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        // A replacement registers the same session within the grace.
        let upstream2 = authenticated_manager("node-a", "shuffle-secret");
        upstream2.spawn();
        let (client2, server2) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server2));
        let edge2 = upstream2
            .open_edge_with_stream_for_session(Box::new(client2), quad, "node-b", "job-rl", 1)
            .unwrap();
        // A control signal re-registers the session (data frames from a fresh
        // upstream restart at sequence zero and are dropped by the
        // delivery-level dedup, which exists for same-upstream replays).
        edge2
            .sender
            .send_async(Envelope::Barrier(CheckpointBarrier {
                checkpoint_id: "c-rl".into(),
                generation: 1,
                trace_context: None,
            }))
            .await
            .unwrap();
        assert!(matches!(
            next_envelope(&input_rx).await,
            Envelope::Barrier(_)
        ));

        // Past the grace the loss stays suppressed: the upstream recovered.
        tokio::time::sleep(std::time::Duration::from_millis(3_400)).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(300), failures.recv_async())
                .await
                .is_err(),
            "a recovered session must not report a loss"
        );
        upstream2.shutdown();
        downstream.shutdown();
    }

    #[tokio::test]
    async fn authorize_inbound_requires_configured_credentials() {
        let manager = NetworkManager::new(8);
        let (header, payload) = raw_handshake("node-a", "job-nc", 1, quad_a_to_b());
        let error = match manager.authorize_inbound(header, &payload).await {
            Err(error) => error,
            Ok(_) => panic!("a plaintext manager cannot authorize handshakes"),
        };
        assert!(
            error.to_string().contains("credentials are not configured"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pump_register_rejection_with_a_clean_abort_reports_the_limit() {
        let quad = quad_a_to_b();
        let config = NetworkManagerConfig {
            max_pending_receipts: 1,
            ..Default::default()
        };
        let pending = Arc::new(PendingReceipts::new(1));
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (writer, _reader) = tokio::io::duplex(64 * 1024);
        let (tx, rx) = flume::bounded::<Envelope>(4);
        tx.send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .unwrap();
        tx.send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .unwrap();
        drop(tx);
        let error = pump_edge(rx, writer, quad, pending, shutdown, config, None, false)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("pending receipt limit 1 reached"),
            "{error}"
        );
        assert!(
            !error.to_string().contains("failed to abort"),
            "a clean abort must not decorate the limit error: {error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pump_data_write_failures_fail_the_edge() {
        let quad = quad_a_to_b();
        // A data frame larger than the pump's write buffer: it is handed to
        // the stream directly, so a dead wire fails the edge on the spot.
        let (tx, rx) = flume::bounded::<Envelope>(4);
        tx.send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(large_batch(4_000))),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .unwrap();
        drop(tx);
        let error = pump_edge(
            rx,
            Box::new(WriteFailStream),
            quad,
            Arc::new(PendingReceipts::new(64)),
            tokio_util::sync::CancellationToken::new(),
            NetworkManagerConfig::default(),
            None,
            false,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("data write failed"), "{error}");
    }
}
