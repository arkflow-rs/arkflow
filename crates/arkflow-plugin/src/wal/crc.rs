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

//! CRC32 (IEEE polynomial 0xEDB88320) for the WAL segment torn-tail detector.
//!
//! Each entry in a sealed segment is prefixed with a `[seq(8) | len(4) |
//! payload(N) | crc32(4)]` record; on recovery, a bad crc on the trailing
//! entry signals a mid-PUT crash and the segment is truncated to the last
//! good record. The same CRC is used on both encode and decode paths so a
//! bad payload always round-trips to a different value than the one written.
//!
//! No external dependency: a 256-entry table is built once at startup and the
//! per-byte update is two XOR + a table lookup. `crc32fast` (already in the
//! transitive tree via `object_store`) would do the same, but pulling it
//! directly just for this is not worth the dep entry.

/// Initial CRC value (all-ones).
const INIT: u32 = 0xFFFF_FFFF;
/// Final XOR (post-complement).
const FINAL_XOR: u32 = 0xFFFF_FFFF;

/// Precomputed table: `TABLE[i] = CRC32(i)` for byte values 0..=255.
const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut j = 0;
        while j < 8 {
            if c & 1 != 0 {
                c = (c >> 1) ^ 0xEDB8_8320;
            } else {
                c >>= 1;
            }
            j += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

/// CRC32 (IEEE 0xEDB88320) over `bytes`. Finalised value, ready to write.
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = INIT;
    for &b in bytes {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    crc ^ FINAL_XOR
}

/// Streaming CRC32 builder. Useful for incremental computation if a segment
/// is built entry-by-entry in memory and we want to write the per-entry crc
/// inline. The current encoder writes everything at once so it uses
/// [`crc32`] directly; this exists for future proofing (e.g. multipart PUT).
#[derive(Clone)]
pub(crate) struct Crc32 {
    state: u32,
}

impl Crc32 {
    pub(crate) fn new() -> Self {
        Self { state: INIT }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        let mut crc = self.state;
        for &b in bytes {
            let idx = ((crc ^ b as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ TABLE[idx];
        }
        self.state = crc;
    }

    pub(crate) fn finalize(self) -> u32 {
        self.state ^ FINAL_XOR
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference values from the IEEE 802.3 CRC32 spec.
    #[test]
    fn empty_crc_is_zero() {
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn ascii_a_crc_is_known() {
        // "a" -> 0xE8B7BE43
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
    }

    #[test]
    fn ascii_hello_crc_is_known() {
        // "hello" -> 0x3610A686
        assert_eq!(crc32(b"hello"), 0x3610_A686);
    }

    #[test]
    fn streaming_matches_one_shot() {
        let mut c = Crc32::new();
        c.update(b"hel");
        c.update(b"lo");
        assert_eq!(c.finalize(), crc32(b"hello"));
    }
}
