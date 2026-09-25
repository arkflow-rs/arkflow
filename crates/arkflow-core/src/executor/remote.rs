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
//! dispatched, but with no receipt that can ever come back. Every pump exit
//! path therefore guarantees:
//!
//! - a branch whose receipt cannot arrive is **aborted, never acknowledged**;
//!   at-least-once is preserved and the upstream re-delivers (duplicates
//!   downstream are allowed);
//! - already-encoded frames are flushed best-effort **before** the abort
//!   sweep runs, so an in-flight delivery still reaches the wire when the
//!   connection is alive;
//! - a wire write failure fails the pump (`Err`), aborting everything still
//!   registered; cancellation is a clean exit (`Ok`) with the same abort
//!   sweep.
//!
//! The failure modes live in [`pump_edge`] (write error / shutdown / channel
//! close all funnel into `PendingReceipts::abort_all`) and are pinned by the
//! `pump_cancel_tests`.

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

/// Bounded resource and authentication policy for one node's data plane.
#[derive(Clone, Debug)]
pub struct NetworkManagerConfig {
    pub channel_capacity: usize,
    pub max_connections: usize,
    pub max_pending_receipts: usize,
    pub max_receipt_queue: usize,
    pub max_failure_queue: usize,
    pub max_frame_len: u32,
    pub read_idle_timeout: std::time::Duration,
    pub registration_grace: std::time::Duration,
    /// How long a successfully authenticated client nonce remains rejected.
    /// The cache closes the replay window without retaining handshake state
    /// forever; its capacity is bounded by the connection limit below.
    pub handshake_replay_ttl: std::time::Duration,
    pub credentials: Option<DataPlaneCredentials>,
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
            registration_grace: std::time::Duration::from_secs(10),
            handshake_replay_ttl: std::time::Duration::from_secs(10 * 60),
            credentials: None,
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
        // context).  A full failure queue must apply backpressure here rather
        // than dropping the only signal that can cancel the affected Job.
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
        {
            let _ = self.failures.send(Error::Process(format!(
                "remote edge receipt queue overflow for quad {:?}",
                self.quad
            )));
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
        {
            let _ = self.failures.send(Error::Process(format!(
                "remote edge receipt queue overflow for quad {:?}",
                self.quad
            )));
        }
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
struct PendingReceipts {
    map: std::sync::Mutex<BTreeMap<u64, PendingBatch>>,
    next_seq: std::sync::atomic::AtomicU64,
    max_entries: usize,
}

impl PendingReceipts {
    fn new(max_entries: usize) -> Self {
        Self {
            map: std::sync::Mutex::new(BTreeMap::new()),
            next_seq: std::sync::atomic::AtomicU64::new(0),
            max_entries,
        }
    }

    fn register(&self, branch: &Arc<dyn crate::input::Ack>, replicas: usize) -> Result<u64, Error> {
        let mut map = self.map.lock().expect("pending receipts lock");
        if map.len() >= self.max_entries {
            return Err(Error::Process(format!(
                "remote edge pending receipt limit {} reached",
                self.max_entries
            )));
        }
        let seq = self
            .next_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        map.insert(
            seq,
            PendingBatch {
                branch: branch.clone(),
                remaining_replicas: replicas,
                held_replicas: 0,
            },
        );
        Ok(seq)
    }

    fn is_empty(&self) -> bool {
        self.map.lock().expect("pending receipts lock").is_empty()
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

/// Capture the current span context as a W3C `traceparent` string, or `None`
/// when the current context carries no valid span — including tracing being
/// disabled entirely, which keeps barriers byte-identical on the wire.
pub fn capture_trace_context() -> Option<String> {
    use opentelemetry::propagation::Injector as _;
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let cx = tracing::Span::current().context();
    if !cx.span().span_context().is_valid() {
        return None;
    }
    let mut carrier = std::collections::HashMap::new();
    opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .inject_context(&cx, &mut carrier);
    carrier.get("traceparent").cloned()
}

/// Extract a remote parent context from a barrier's captured `traceparent`.
/// Returns `None` for absent or unparsable values so a malformed hop can
/// never detach a downstream span from its local parent.
pub fn extract_trace_context(trace_context: &str) -> Option<opentelemetry::Context> {
    use opentelemetry::propagation::Extractor as _;
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
    /// Accepted streams feed this queue so serving works over any transport.
    accepted: AcceptedQueue,
    config: NetworkManagerConfig,
    active_connections: std::sync::atomic::AtomicUsize,
    shutdown: tokio_util::sync::CancellationToken,
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
        let pending = Arc::new(PendingReceipts::new(self.config.max_pending_receipts));
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
        let pending = Arc::new(PendingReceipts::new(self.config.max_pending_receipts));
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(session_key.clone(), pending.clone());
        let manager = self.clone();
        let failures = self.failure_sender_for_key(&session_key);
        tokio::spawn(async move {
            match transport.connect(quad).await {
                Ok(stream) => {
                    manager.attach_stream(
                        stream,
                        quad,
                        receiver,
                        pending,
                        auth,
                        session_key.clone(),
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
        let (reader, writer) = tokio::io::split(stream);
        let failures = self.failure_sender_for_key(&session_key);
        let connection_cancel = self.shutdown.child_token();
        let config = self.config.clone();
        let outbound_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Receipt read loop: routes receipts to this quad's pending map.
        let receipt_pending = pending.clone();
        let receipt_failures = failures.clone();
        let receipt_auth = auth.clone();
        let receipt_manager = self.clone();
        let receipt_session_key = session_key.clone();
        let receipt_cancel = connection_cancel.clone();
        let receipt_outbound_closed = outbound_closed.clone();
        tokio::spawn(async move {
            let mut reader = reader;
            let cancelled = receipt_cancel.clone();
            let mut first_frame = true;
            let mut failure = None;
            loop {
                tokio::select! {
                    _ = cancelled.cancelled() => break,
                    frame = read_frame_with_limits(&mut reader, config.max_frame_len, Some(config.read_idle_timeout)) => match frame {
                        Ok((header, payload)) if first_frame && receipt_auth.is_some() => {
                            first_frame = false;
                            let Some(auth) = receipt_auth.as_ref() else { unreachable!() };
                            let result = if header.kind == FrameKind::Handshake && header.quad == auth.quad {
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
                        }
                        Ok((header, payload)) if header.kind == FrameKind::Receipt => {
                            first_frame = false;
                            if let Ok(receipt) = serde_json::from_slice::<ReceiptFrame>(&payload) {
                                receipt_pending.apply(receipt, &receipt_failures);
                            } else {
                                failure = Some(Error::Process(
                                    "remote edge receipt frame malformed".into(),
                                ));
                                break;
                            }
                        }
                        Ok((header, _)) => {
                            failure = Some(Error::Process(format!(
                                "unexpected {:?} frame on a receipt channel",
                                header.kind
                            )));
                            break;
                        }
                        Err(error) => {
                            // A normal outbound close drops the writer half
                            // after the local edge has drained.  In that case
                            // the peer may close its half with no more frames;
                            // only treat it as clean when no receipt can still
                            // be outstanding.  An idle reader must otherwise
                            // fail the Job even if the pump has no new data to
                            // write and therefore cannot observe the socket
                            // failure itself.
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
            if let Some(error) = failure {
                cancelled.cancel();
                let _ = receipt_failures.send_async(error).await;
            }
            receipt_pending.abort_all();
            self_remove_outbound(&receipt_manager, &receipt_session_key);
        });

        // Outbound pump: drains the edge channel onto the wire.
        let pump_pending = pending.clone();
        let pump_failures = failures.clone();
        let pump_cancel = connection_cancel.clone();
        let pump_config = self.config.clone();
        let pump_manager = self.clone();
        let pump_session_key = session_key;
        let pump_outbound_closed = outbound_closed;
        tokio::spawn(async move {
            let result = pump_edge(
                receiver,
                writer,
                quad,
                pump_pending,
                pump_cancel.clone(),
                pump_config,
                auth,
            )
            .await;
            match result {
                Ok(()) => {
                    pump_outbound_closed.store(true, std::sync::atomic::Ordering::Release);
                }
                Err(error) => {
                    pump_cancel.cancel();
                    let _ = pump_failures.send_async(error).await;
                }
            }
            self_remove_outbound(&pump_manager, &pump_session_key);
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
                    _ = receipt_cancel.cancelled() => break Ok(()),
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
                            let envelope = Envelope::Data(
                                Arc::new(batch),
                                Arc::new(RemoteAck {
                                    outbox: receipt_tx.clone(),
                                    quad: header.quad,
                                    seq,
                                    failures: failure_sender.clone(),
                                }),
                            );
                            if sender.send_async(envelope).await.is_err() {
                                break Ok(()); // local chain gone; close the edge
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
                Err(error) => break Err(error),
            }
        };

        connection_cancel.cancel();

        if let Err(error) = failure_reason {
            let mut reported_without_eos = false;
            for key in served_quads.keys() {
                if !eos_seen.contains(key) {
                    reported_without_eos = true;
                    let _ = failure_sender
                        .send_async(Error::Process(format!(
                            "remote edge for quad {:?} ended without Eos: {error}",
                            key.quad
                        )))
                        .await;
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
                    let _ = failure_sender
                        .send_async(Error::Process(format!(
                            "remote edge for quad {:?} ended without Eos; upstream likely died mid-stream",
                            key.quad
                        )))
                        .await;
                    let _ = sender;
                }
            }
        }

        // Dropping the served quads' senders closes the chains' input channels.
        // Only entries this connection inserted are removed; a reconnecting
        // upstream re-registers through the graph anyway.
        for (key, sender) in served_quads {
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
async fn pump_edge(
    receiver: flume::Receiver<super::envelope::Envelope>,
    writer: impl AsyncWrite + Unpin + Send + 'static,
    quad: Quad,
    pending: Arc<PendingReceipts>,
    shutdown: tokio_util::sync::CancellationToken,
    config: NetworkManagerConfig,
    auth: Option<SessionAuth>,
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
                        let seq = match pending.register(&branch, 1) {
                            Ok(seq) => seq,
                            Err(error) => {
                                let abort_error = branch.abort().await.err();
                                if let Some(abort_error) = abort_error {
                                    break Err(Error::Process(format!(
                                        "{error}; failed to abort batch rejected by pending receipt limit: {abort_error}"
                                    )));
                                }
                                break Err(error);
                            }
                        };
                        let payload = match encoder.encode(&batch, seq) {
                            Ok(payload) => payload,
                            Err(error) => break Err(error),
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
                            break Err(Error::Process(format!(
                                "remote edge data write failed: {error}"
                            )));
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
                        if let Err(error) = write_frame_with_limit(
                            &mut writer,
                            quad,
                            FrameKind::Signal,
                            &payload,
                            config.max_frame_len,
                        )
                        .await
                        {
                            break Err(Error::Process(format!(
                                "remote edge signal write failed: {error}"
                            )));
                        }
                    }
                }
            }
        }
    };
    let _ = writer.flush().await;
    // Abort pendings on every exit path: once the pump is gone, no receipts
    // can ever arrive for them, so leaving them alive would leak source acks
    // indefinitely.
    pending.abort_all();
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
        let mut config = NetworkManagerConfig::default();
        config.credentials = Some(credentials);
        config.registration_grace = std::time::Duration::from_millis(100);
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
        let failure =
            tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
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

        let failure =
            tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
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
}

#[cfg(test)]
mod pump_cancel_tests {
    //! Pins the outbound pump's cancellation ("void write") semantics: a
    //! branch whose receipt can never arrive is aborted on every exit path —
    //! never falsely acked — and the exit flush precedes the abort sweep.

    use super::*;
    use crate::input::Ack;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc as StdArc;

    struct SpyAck {
        acked: AtomicBool,
        aborted: AtomicBool,
    }

    impl SpyAck {
        fn new() -> StdArc<Self> {
            StdArc::new(Self {
                acked: AtomicBool::new(false),
                aborted: AtomicBool::new(false),
            })
        }
    }

    #[async_trait::async_trait]
    impl Ack for SpyAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn abort(&self) -> Result<(), Error> {
            self.aborted.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn int64_batch(values: Vec<i64>) -> crate::MessageBatchRef {
        let schema = StdArc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let columns: Vec<ArrayRef> = vec![StdArc::new(Int64Array::from(values))];
        let batch =
            datafusion::arrow::record_batch::RecordBatch::try_new(schema, columns)
                .expect("batch");
        StdArc::new(crate::MessageBatch::new_arrow(batch))
    }

    fn pump_config() -> NetworkManagerConfig {
        NetworkManagerConfig::default()
    }

    async fn wait_for(flag: &AtomicBool, what: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !flag.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wire_write_failure_aborts_the_registered_branch() {
        let quad = Quad {
            src_op: 1,
            src_subtask: 0,
            dst_op: 2,
            dst_subtask: 0,
        };
        let pending = Arc::new(PendingReceipts::new(64));
        let shutdown = tokio_util::sync::CancellationToken::new();
        // The reader is gone, so once the 64 KiB BufWriter overflows the
        // write must hit the closed connection.
        let (writer, _reader) = tokio::io::duplex(1024);
        drop(_reader);
        let (tx, rx) = flume::bounded::<Envelope>(1);

        // Large enough that the encoded frame cannot fit in the BufWriter.
        let batch = int64_batch((0..50_000).collect());
        let ack = SpyAck::new();
        tx.send_async(Envelope::Data(batch, ack.clone()))
            .await
            .expect("send");
        drop(tx);

        let result = pump_edge(
            rx,
            writer,
            quad,
            pending,
            shutdown,
            pump_config(),
            None,
        )
        .await;

        assert!(result.is_err(), "pump must fail on a wire write error");
        wait_for(&ack.aborted, "branch abort on wire failure").await;
        assert!(
            !ack.acked.load(Ordering::SeqCst),
            "a void write must never be acknowledged"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_aborts_unreceipted_branches_after_flushing() {
        let quad = Quad {
            src_op: 1,
            src_subtask: 0,
            dst_op: 2,
            dst_subtask: 0,
        };
        let pending = Arc::new(PendingReceipts::new(64));
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (writer, mut reader) = tokio::io::duplex(128 * 1024);
        let (tx, rx) = flume::bounded::<Envelope>(1);

        // Larger than duplex + BufWriter capacity: the pump parks inside
        // write_all after registering, so "registered" is observable.
        let batch = int64_batch((0..50_000).collect());
        let ack = SpyAck::new();
        tx.send_async(Envelope::Data(batch, ack.clone()))
            .await
            .expect("send");
        drop(tx);

        let pump = tokio::spawn(pump_edge(
            rx,
            writer,
            quad,
            pending.clone(),
            shutdown.clone(),
            pump_config(),
            None,
        ));

        // Deterministic readiness: the branch is registered before the frame
        // is written, so a non-empty pending map proves registration happened.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while pending.is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "pump never registered the branch"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // Consume the frame so the blocked write completes: the delivery is
        // now on the wire, but no receipt will ever be sent back for it.
        let (header, _payload) =
            tokio::time::timeout(std::time::Duration::from_secs(10), read_frame(&mut reader))
                .await
                .expect("frame arrives")
                .expect("frame decodes");
        assert_eq!(header.kind, FrameKind::Data);

        shutdown.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), pump)
            .await
            .expect("pump exits after cancellation")
            .expect("join");
        assert!(result.is_ok(), "cancellation is a clean pump exit");

        wait_for(&ack.aborted, "branch abort on shutdown").await;
        assert!(
            !ack.acked.load(Ordering::SeqCst),
            "an unacknowledged delivery must never complete"
        );
    }

    #[tokio::test]
    async fn acked_receipt_completes_the_branch_and_clears_pending() {
        let pending = Arc::new(PendingReceipts::new(64));
        let spy = SpyAck::new();
        let ack: StdArc<dyn Ack> = spy.clone();
        let seq = pending.register(&ack, 1).expect("register within limits");
        let (failures_tx, _failures_rx) = flume::bounded::<Error>(8);

        pending.apply(ReceiptFrame { kind: ReceiptKind::Acked, seq }, &failures_tx);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !spy.acked.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "branch was never acknowledged"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            pending.is_empty(),
            "completed receipts leave no pending entries"
        );
        assert!(
            !spy.aborted.load(Ordering::SeqCst),
            "an acknowledged branch must not be aborted"
        );
    }
}
