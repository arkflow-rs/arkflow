## ADDED Requirements

### Requirement: Committed cursor tracks acknowledgements
The S3 WAL backend SHALL advance the committed cursor in response to acknowledgements. `advance_cursor(seq)` MUST record `seq` (it SHALL NOT discard it), and the manifest cursor SHALL be derived from the highest acknowledged sequence clamped to the highest sequence durably sealed to object storage (`max_sealed_seq`). The cursor SHALL NOT advance past `max_sealed_seq`, so an entry that has been acknowledged but not yet sealed remains replayable on restart (at-least-once). The backend's next-sequence hint SHALL be derived from the highest written sequence (`max(max_sealed_seq, active segment last_seq) + 1`), not from `cursor()+1`, so a restart never reuses a sequence number already present on the store.

#### Scenario: Acknowledged entries are not replayed after a clean restart
- **WHEN** entries seq 1..=N are ingested and sealed, and all of them are acknowledged via `advance_cursor`
- **THEN** after closing and reopening the WAL at the same namespace, `read_after_cursor()` returns no entries
- **AND** `next_seq_hint()` returns N+1

#### Scenario: Cursor does not advance past unsealed data
- **WHEN** an entry is acknowledged while it is still in the active (not yet sealed) segment, so `acked_seq > max_sealed_seq`
- **THEN** the committed cursor is clamped to `max_sealed_seq` and does not advance past the unsealed entry
- **AND** once that entry is sealed, a restart replays it because the cursor never advanced past it (no loss of sealed data; unsealed in-memory entries remain subject to the existing group-commit loss window)

#### Scenario: Next sequence hint does not reuse a sealed-but-unacked sequence
- **WHEN** entries are sealed up to sequence M but only acknowledged up to sequence K (K < M) and the WAL is reopened at the same namespace
- **THEN** `next_seq_hint()` returns M+1, not K+1
- **AND** the next append does not collide with the existing sealed sequences K+1..=M
