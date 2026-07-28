# Capability: WAL Compression

## Purpose

Reduce S3 storage and network transfer costs by compressing segment data before upload and decompressing during recovery. Support multiple compression algorithms with configurable compression levels.

## ADDED Requirements

### Requirement: Compression algorithm selection
The S3 WAL backend SHALL support configurable compression via a `compression` parameter. Valid options SHALL be `none` (default), `zstd`, and `lz4`.

#### Scenario: No compression (default)
- **WHEN** a stream does not configure `compression`
- **THEN** segments are uploaded to S3 uncompressed (current behavior)

#### Scenario: Zstandard compression
- **WHEN** a stream configures `compression: zstd`
- **THEN** segments are compressed with zstd before upload and decompressed during recovery

#### Scenario: LZ4 compression
- **WHEN** a stream configures `compression: lz4`
- **THEN** segments are compressed with LZ4 before upload and decompressed during recovery

### Requirement: Configurable compression level
The system SHALL support configurable compression levels via a `compression_level` parameter (integer, algorithm-specific range). Default levels SHALL be `3` for zstd and `4` for LZ4.

#### Scenario: Default zstd level
- **WHEN** a stream configures `compression: zstd` without `compression_level`
- **THEN** zstd level 3 is used

#### Scenario: Custom zstd level
- **WHEN** a stream configures `compression: zstd` and `compression_level: 9`
- **THEN** zstd level 9 is used (higher compression, slower)

#### Scenario: Custom LZ4 level
- **WHEN** a stream configures `compression: lz4` and `compression_level: 1`
- **THEN** LZ4 acceleration level 1 is used (faster, lower compression)

### Requirement: Transparent decompression during recovery
During WAL recovery, the system SHALL automatically detect compression format from segment metadata (or file extension) and decompress before parsing entries. No user configuration is required for decompression.

#### Scenario: Auto-detect compression format
- **WHEN** recovery reads a segment compressed with zstd
- **THEN** the system detects zstd format and decompresses before parsing

#### Scenario: Mixed compression recovery
- **WHEN** recovery reads segments with different compression (some none, some zstd)
- **THEN** each segment is decompressed according to its format

### Requirement: Compression metadata in manifest
The segment manifest SHALL include a `compression` field for each segment, indicating the algorithm used. This enables recovery without attempting multiple decompression attempts.

#### Scenario: Manifest records compression
- **WHEN** a compressed segment is uploaded
- **THEN** the manifest entry includes `compression: zstd` (or `lz4`, `none`)

#### Scenario: Recovery uses manifest metadata
- **WHEN** recovery reads the manifest and processes segments
- **THEN** decompression uses the `compression` field from the manifest entry

### Requirement: Compression ratio metrics
The system SHALL emit metrics for compression ratio (original size / compressed size) per segment and per stream. This SHALL be exposed via the existing metrics system.

#### Scenario: Compression ratio tracking
- **WHEN** a segment is compressed and uploaded
- **THEN** metrics include `wal_compression_ratio` with the segment's ratio

#### Scenario: Per-stream aggregate
- **WHEN** multiple segments are compressed for a stream
- **THEN** metrics include aggregate compression ratio for the stream

### Requirement: Compression validation
The system SHALL validate compression configuration at load time. Invalid algorithms or compression levels SHALL cause configuration rejection with a clear error message.

#### Scenario: Invalid compression algorithm
- **WHEN** a stream configures `compression: gzip`
- **THEN** configuration loading fails with an error listing valid algorithms (`none`, `zstd`, `lz4`)

#### Scenario: Invalid compression level
- **WHEN** a stream configures `compression: zstd` and `compression_level: 25`
- **THEN** configuration loading fails with an error indicating zstd supports levels 0-22

### Requirement: Conditional compression based on size
The system SHALL support a `compression_min_size` parameter to skip compression for small segments (default 10KB). Segments smaller than this threshold SHALL be uploaded uncompressed.

#### Scenario: Skip compression for small segment
- **WHEN** a segment is 5KB and `compression_min_size: 10KB` is configured
- **THEN** the segment is uploaded uncompressed

#### Scenario: Compress larger segment
- **WHEN** a segment is 50KB and `compression_min_size: 10KB` is configured
- **THEN** the segment is compressed before upload
