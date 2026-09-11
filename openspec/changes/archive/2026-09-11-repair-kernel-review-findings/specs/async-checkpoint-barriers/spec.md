## MODIFIED Requirements

### Requirement: Alignment at multi-input vertices

A vertex with multiple input edges SHALL buffer data from other inputs after one
input's barrier arrives until barriers from all inputs align, then release the
buffered data.

#### Scenario: Two-input alignment

- **WHEN** input A delivers a barrier while input B still streams pre-barrier data
- **THEN** the vertex buffers B's data, snapshots when B's barrier arrives, and forwards B's buffered data afterwards

#### Scenario: Alignment buffer is bounded

- **WHEN** alignment buffering exceeds the aligner's fixed cap
- **THEN** the checkpoint fails with a bounded-alignment error and data flow resumes, rather than growing memory unboundedly
