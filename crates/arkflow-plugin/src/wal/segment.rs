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

//! Segment object format for the object-store WAL backend.
//!
//! A segment is a sequence of length-prefixed, CRC32-checksummed entries
//! concatenated into a single byte buffer:
//!
//! ```text
//! [ seq(u64 BE) | len(u32 BE) | payload | crc32(payload)(u32 BE) ] × N
//! ```
//!
//! `payload` is the same length-prefixed Arrow IPC frame the local `redb`
//! backend stores verbatim (`serialize()` from `arkflow-core::wal::store`),
//! so an entry is portable across backends.
//!
//! On decode, each record is verified independently. A bad crc on entry `i`
//! means `i` was truncated mid-PUT; we drop everything from `i` onwards and
//! return the prefix. The decoder never errors on a torn tail — that's a
//! recovery expectation, not a corruption.

use arkflow_core::wal::store::deserialize;
use arkflow_core::{Error, MessageBatchRef};
use std::sync::Arc;

use super::crc;

/// Result of decoding a single segment. May be shorter than `bytes` when the
/// trailing entry was truncated by a mid-PUT crash (D5).
#[derive(Debug)]
pub(crate) struct DecodedSegment {
    pub entries: Vec<(u64, MessageBatchRef)>,
    pub bytes_consumed: usize,
}

/// Encode a batch of `(seq, payload)` entries into a single segment byte
/// buffer. The buffer is appended to `out` to reuse the allocation across
/// many segment flushes.
pub(crate) fn encode(entries: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<(), Error> {
    for (seq, payload) in entries {
        let seq_bytes = seq.to_be_bytes();
        let len_bytes = u32::try_from(payload.len())
            .map_err(|_| Error::Process("segment entry payload too long".into()))?
            .to_be_bytes();
        let c = crc::crc32(payload);
        let c_bytes = c.to_be_bytes();

        out.extend_from_slice(&seq_bytes);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(payload);
        out.extend_from_slice(&c_bytes);
    }
    Ok(())
}

/// Encode + compress into `out`. The compression algorithm is selected by
/// `kind` (`"none"`, `"zstd"`, `"lz4"`). `level` is algorithm-specific (used
/// by zstd; ignored by lz4 / none).
pub(crate) fn encode_compressed(
    entries: &[(u64, Vec<u8>)],
    out: &mut Vec<u8>,
    kind: &str,
    level: i32,
) -> Result<(), Error> {
    let mut raw = Vec::new();
    encode(entries, &mut raw)?;
    let compressed = crate::wal::compression::compress(kind, level, &raw)?;
    out.clear();
    out.extend_from_slice(&compressed);
    Ok(())
}

/// Decode a segment byte buffer. Returns the entries whose CRC verifies; if
/// the trailing entry was truncated mid-PUT (a torn tail), the consumed
/// length is reported and `entries` contains only the intact prefix.
pub(crate) fn decode(bytes: &[u8]) -> Result<DecodedSegment, Error> {
    decode_with_kind(bytes, "none")
}

/// Decode a segment byte buffer with explicit compression kind. Used during
/// recovery when the manifest records the algorithm (task 4.7).
pub(crate) fn decode_with_kind(bytes: &[u8], kind: &str) -> Result<DecodedSegment, Error> {
    let raw = if kind == "none" {
        bytes.to_vec()
    } else {
        crate::wal::compression::decompress(kind, bytes)?
    };
    decode_raw(&raw)
}

fn decode_raw(bytes: &[u8]) -> Result<DecodedSegment, Error> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    let mut last_good = 0usize;

    while pos < bytes.len() {
        // Header: 8 bytes seq + 4 bytes len = 12 bytes minimum.
        if bytes.len() - pos < 12 {
            // Trailing partial header — torn tail.
            break;
        }
        let mut seq_buf = [0u8; 8];
        seq_buf.copy_from_slice(&bytes[pos..pos + 8]);
        let seq = u64::from_be_bytes(seq_buf);
        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&bytes[pos + 8..pos + 12]);
        let len = u32::from_be_bytes(len_buf) as usize;

        let header_end = pos + 12;
        let payload_end = header_end
            .checked_add(len)
            .ok_or_else(|| Error::Process("segment entry length overflow".into()))?;
        let crc_end = payload_end
            .checked_add(4)
            .ok_or_else(|| Error::Process("segment entry CRC overflow".into()))?;

        if crc_end > bytes.len() {
            // Trailing entry was truncated mid-PUT. Drop it; everything up
            // to `last_good` is intact.
            break;
        }

        let payload = &bytes[header_end..payload_end];
        let mut crc_buf = [0u8; 4];
        crc_buf.copy_from_slice(&bytes[payload_end..crc_end]);
        let want = u32::from_be_bytes(crc_buf);
        let got = crc::crc32(payload);
        if want != got {
            // Mid-record corruption — same disposition as torn tail.
            break;
        }

        let mb: MessageBatchRef = Arc::new(deserialize(payload)?);
        entries.push((seq, mb));
        pos = crc_end;
        last_good = pos;
    }

    Ok(DecodedSegment {
        entries,
        bytes_consumed: last_good,
    })
}

// We need `MessageBatchRef` and `Arc` from `arkflow-core::MessageBatch`.
// Avoid dragging in `DeserializeMessage` as a trait; just call the function.

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::wal::store::serialize;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    fn make_payload() -> Vec<u8> {
        let schema = StdArc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(vec![1]))]).unwrap();
        let mut mb = MessageBatch::new_arrow(batch);
        mb.set_input_name(Some("test".into()));
        serialize(&mb).unwrap()
    }

    #[test]
    fn round_trip_through_segment() {
        let entries = vec![
            (1u64, make_payload()),
            (2u64, make_payload()),
            (3u64, make_payload()),
        ];
        let mut bytes = Vec::new();
        encode(&entries, &mut bytes).unwrap();

        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[0].0, 1);
        assert_eq!(decoded.entries[1].0, 2);
        assert_eq!(decoded.entries[2].0, 3);
        assert_eq!(decoded.bytes_consumed, bytes.len());
    }

    /// 4.3: a torn tail (the trailing entry's payload is truncated by a
    /// mid-PUT crash) is silently dropped, and the consumed length stops at
    /// the last intact entry.
    #[test]
    fn torn_tail_is_truncated() {
        let entries = vec![
            (1u64, make_payload()),
            (2u64, make_payload()),
            (3u64, make_payload()),
        ];
        let mut bytes = Vec::new();
        encode(&entries, &mut bytes).unwrap();

        // Chop the trailing entry in the middle: full record for seq=1, full
        // record for seq=2, half of seq=3 (header present, payload
        // truncated, no crc). The decoder must stop at seq=2.
        let mut cut = bytes.clone();
        // Header is 12 bytes; payload is `len` bytes; crc is 4 bytes.
        // Drop the last 4 (crc) plus half the payload.
        let last_payload_len = make_payload().len();
        cut.truncate(bytes.len() - 4 - (last_payload_len / 2));

        let decoded = decode(&cut).unwrap();
        assert_eq!(decoded.entries.len(), 2, "torn tail must be discarded");
        assert_eq!(decoded.entries[0].0, 1);
        assert_eq!(decoded.entries[1].0, 2);
        assert!(decoded.bytes_consumed <= cut.len());
    }

    /// 4.3: a crc mismatch on a non-trailing entry also stops the decoder
    /// (no panic, no garbage surfaced).
    #[test]
    fn bad_crc_in_middle_truncates_remainder() {
        let entries = vec![(1u64, make_payload()), (2u64, make_payload())];
        let mut bytes = Vec::new();
        encode(&entries, &mut bytes).unwrap();

        // Flip a bit in seq=1's payload area to break its crc. Layout: header
        // (12) + payload (N) + crc (4) for seq=1.
        let payload_len = make_payload().len();
        let crc_start = 12 + payload_len;
        bytes[crc_start] ^= 0xFF;

        let decoded = decode(&bytes).unwrap();
        assert_eq!(
            decoded.entries.len(),
            0,
            "bad crc on the first record must drop everything (it might be the active segment's leading entry)"
        );
    }

    /// 4.11: round-trip through compressed encode + decode
    #[test]
    fn round_trip_compressed_zstd() {
        // Build a segment big enough that compression actually reduces size.
        let mut entries = Vec::new();
        for i in 0..100u64 {
            entries.push((i + 1, make_payload()));
        }
        let mut raw = Vec::new();
        encode(&entries, &mut raw).unwrap();

        let mut compressed = Vec::new();
        encode_compressed(&entries, &mut compressed, "zstd", 3).unwrap();
        assert!(
            compressed.len() < raw.len(),
            "compressed {} should be < raw {}",
            compressed.len(),
            raw.len()
        );

        let decoded = decode_with_kind(&compressed, "zstd").unwrap();
        assert_eq!(decoded.entries.len(), 100);
        assert_eq!(decoded.entries[0].0, 1);
        assert_eq!(decoded.entries[99].0, 100);
    }

    #[test]
    fn round_trip_compressed_lz4() {
        let mut entries = Vec::new();
        for i in 0..100u64 {
            entries.push((i + 1, make_payload()));
        }
        let mut compressed = Vec::new();
        encode_compressed(&entries, &mut compressed, "lz4", 0).unwrap();

        let decoded = decode_with_kind(&compressed, "lz4").unwrap();
        assert_eq!(decoded.entries.len(), 100);
    }

    #[test]
    fn encode_compressed_none_passes_through() {
        let entries = vec![(1u64, make_payload()), (2u64, make_payload())];
        let mut a = Vec::new();
        encode(&entries, &mut a).unwrap();
        let mut b = Vec::new();
        encode_compressed(&entries, &mut b, "none", 0).unwrap();
        assert_eq!(a, b);
    }
}
