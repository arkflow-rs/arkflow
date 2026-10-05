//! Data payload encode/decode: Arrow IPC record batches packed into
//! length-prefixed pieces behind a per-connection metadata block.

use super::wire::WireDataMeta;
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
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
pub(crate) struct DataEncoder {
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
pub(crate) struct DataDecoder {
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
pub(crate) struct Piece<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) flatbuf_len: usize,
}

impl<'a> Piece<'a> {
    fn flatbuf(&self) -> &'a [u8] {
        &self.bytes[8..8 + self.flatbuf_len]
    }

    pub(crate) fn body(&self, body_len: usize) -> Result<Buffer, Error> {
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

pub(crate) fn read_piece<'a>(
    payload: &'a [u8],
    cursor: usize,
) -> Result<(Piece<'a>, usize), Error> {
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
