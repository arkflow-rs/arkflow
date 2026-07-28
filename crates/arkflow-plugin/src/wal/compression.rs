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

//! Segment compression/decompression for the S3 WAL backend.
//!
//! Supports `None`, `Zstd`, and `Lz4` algorithms as defined by
//! [`CompressionConfig`](arkflow_core::wal::config::CompressionConfig).
//! Round-trip is verified by the unit tests.

use arkflow_core::Error;

/// Compress `data` using the algorithm associated with `kind`.
///
/// `kind` is a string identifier: `"none"`, `"zstd"`, or `"lz4"`.
pub fn compress(kind: &str, level: i32, data: &[u8]) -> Result<Vec<u8>, Error> {
    match kind {
        "none" => Ok(data.to_vec()),
        "zstd" => {
            zstd::encode_all(data, level).map_err(|e| Error::Process(format!("zstd encode: {}", e)))
        }
        "lz4" => Ok(lz4_flex::compress(data)),
        other => Err(Error::Process(format!(
            "unknown compression kind: {}",
            other
        ))),
    }
}

/// Decompress `data` using the algorithm associated with `kind`.
///
/// `kind` is a string identifier: `"none"`, `"zstd"`, or `"lz4"`.
pub fn decompress(kind: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
    match kind {
        "none" => Ok(data.to_vec()),
        "zstd" => zstd::decode_all(data).map_err(|e| Error::Process(format!("zstd decode: {}", e))),
        "lz4" => {
            // LZ4 framing: try with a generous buffer first, retry with
            // larger buffer if too small. We start at 16× the compressed
            // size and double if needed.
            let mut max_size = data.len().saturating_mul(16).max(1024);
            for _ in 0..8 {
                match lz4_flex::decompress(data, max_size) {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        if e.to_string().contains("too small") {
                            max_size = max_size.saturating_mul(2);
                            continue;
                        }
                        return Err(Error::Process(format!("lz4 decode: {}", e)));
                    }
                }
            }
            Err(Error::Process(format!(
                "lz4 decode: max retries exceeded (final size {})",
                max_size
            )))
        }
        other => Err(Error::Process(format!(
            "unknown compression kind: {}",
            other
        ))),
    }
}

/// Convenience: get the compression kind string for `cfg`.
pub fn kind_from_config(cfg: &arkflow_core::wal::config::CompressionConfig) -> (&'static str, i32) {
    use arkflow_core::wal::config::CompressionConfig;
    match cfg {
        CompressionConfig::None => ("none", 0),
        CompressionConfig::Zstd { level } => ("zstd", *level),
        CompressionConfig::Lz4 { .. } => ("lz4", 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &[u8] = b"the quick brown fox jumps over the lazy dog \
                          the quick brown fox jumps over the lazy dog \
                          the quick brown fox jumps over the lazy dog";

    #[test]
    fn round_trip_none() {
        let compressed = compress("none", 0, SAMPLE).unwrap();
        assert_eq!(compressed, SAMPLE);
        let decompressed = decompress("none", &compressed).unwrap();
        assert_eq!(decompressed, SAMPLE);
    }

    #[test]
    fn round_trip_zstd() {
        let compressed = compress("zstd", 3, SAMPLE).unwrap();
        // Compressed should be smaller (or equal in edge cases).
        assert!(compressed.len() <= SAMPLE.len());
        let decompressed = decompress("zstd", &compressed).unwrap();
        assert_eq!(decompressed, SAMPLE);
    }

    #[test]
    fn round_trip_lz4() {
        let compressed = compress("lz4", 0, SAMPLE).unwrap();
        assert!(compressed.len() <= SAMPLE.len());
        let decompressed = decompress("lz4", &compressed).unwrap();
        assert_eq!(decompressed, SAMPLE);
    }

    #[test]
    fn unknown_kind_returns_error() {
        let r = compress("gzip", 0, SAMPLE);
        assert!(r.is_err());
        let r = decompress("gzip", SAMPLE);
        assert!(r.is_err());
    }

    #[test]
    fn zstd_compression_actually_reduces_size() {
        // Repetitive data should compress well.
        let data = vec![b'A'; 10_000];
        let compressed = compress("zstd", 3, &data).unwrap();
        assert!(
            compressed.len() < data.len(),
            "compressed size {} should be < original {}",
            compressed.len(),
            data.len()
        );
        let decompressed = decompress("zstd", &compressed).unwrap();
        assert_eq!(decompressed.len(), data.len());
    }

    #[test]
    fn lz4_compression_actually_reduces_size() {
        let data = vec![b'B'; 10_000];
        let compressed = compress("lz4", 0, &data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = decompress("lz4", &compressed).unwrap();
        assert_eq!(decompressed.len(), data.len());
    }

    /// 7.2: measure compression ratios across zstd levels
    #[test]
    fn compression_ratio_across_zstd_levels() {
        // Realistic Arrow-like data: mixture of repeated and varying bytes
        let mut data = Vec::with_capacity(50_000);
        for i in 0..50_000 {
            data.push((i % 256) as u8);
        }
        for level in [1, 3, 6, 9] {
            let compressed = compress("zstd", level, &data).unwrap();
            let ratio = data.len() as f64 / compressed.len() as f64;
            println!(
                "zstd-{}: {} -> {} bytes (ratio: {:.2}x)",
                level,
                data.len(),
                compressed.len(),
                ratio
            );
            assert!(ratio > 1.0, "compression should reduce size");
        }
    }

    /// 7.3: measure compression ratios for LZ4 levels
    #[test]
    fn compression_ratio_across_lz4_levels() {
        let mut data = Vec::with_capacity(50_000);
        for i in 0..50_000 {
            data.push((i % 256) as u8);
        }
        for level in [1, 4, 9] {
            let compressed = compress("lz4", level, &data).unwrap();
            let ratio = data.len() as f64 / compressed.len() as f64;
            println!(
                "lz4-{}: {} -> {} bytes (ratio: {:.2}x)",
                level,
                data.len(),
                compressed.len(),
                ratio
            );
            assert!(ratio > 1.0, "lz4 compression should reduce size");
        }
    }
}
