## MODIFIED Requirements

### Requirement: Decoded messages share a stable schema
`protobuf_to_arrow` SHALL build its schema from the message descriptor's full field set with every field nullable, so every decoded message yields the same schema regardless of which fields are present. Concatenating per-message batches SHALL NOT fail with a schema mismatch. The standalone protobuf codec and protobuf processor decode paths SHALL decode a batch of messages sharing one descriptor via columnar accumulation (single converter, one multi-row batch per decode call) instead of building one single-row RecordBatch per message and re-merging; the produced schema (full field set, all nullable, declaration order) and row values SHALL be identical to the per-message-then-merge result, and on-error skip SHALL omit exactly the malformed messages.

#### Scenario: Absent field does not break concat
- **WHEN** message A has field `b` set and message B does not
- **THEN** both decode to the same schema (field `b` present, nullable), and concatenating the two batches succeeds with a null in B's `b` column

#### Scenario: Columnar batch equals per-message merge
- **WHEN** a decode call receives multiple messages sharing the codec's single descriptor
- **THEN** the output is one multi-row batch whose schema and row values equal the previous per-message-decode-then-concat result, produced without per-message schema construction

#### Scenario: Skip mode omits only malformed messages
- **WHEN** a batch contains one malformed message among valid ones and the codec runs with on-error skip
- **THEN** the output batch contains exactly the valid messages in order, and the malformed one is counted as skipped
