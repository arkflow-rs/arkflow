# streaming-job-api 变更（Delta）

## ADDED Requirements

### Requirement: Unsupported Join rejection guidance is honest

The validation error that rejects a Job-declared `OperatorKind::Join` SHALL state
that stream-stream join is not yet supported anywhere in the engine, SHALL NOT
suggest any entry point that does not exist (in particular it SHALL NOT defer to
"a dedicated multi-input Join runtime" as if it were available), and SHALL be
consistent with the Stream compiler's `join` buffer rejection message so the two
rejection paths never contradict each other.

#### Scenario: Join rejection names the same boundary as the Stream compiler

- **WHEN** a Job declares a Join operator and is rejected
- **THEN** the validation error states stream-stream join is not yet supported and offers guidance consistent with the Stream `join` buffer rejection, without referencing an unavailable runtime or entry point
