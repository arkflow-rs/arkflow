## Why

The control-plane configuration page currently treats a format selector as metadata while leaving the editor content unchanged (`console/src/features.tsx:115-116`), and it reloads immediately after receiving an asynchronous publish operation instead of waiting for its terminal result (`console/src/features.tsx:106-108`). As a result, operators can validate one representation, publish another, lose operation failures, or accidentally work from a redacted runtime snapshot. Configuration parsing errors also lose their location because the server maps parser failures to an empty path (`crates/arkflow-core/src/configuration.rs:127-132`).

## What Changes

- Make the editor's format and content a single consistent candidate; convert content when changing between supported formats and invalidate validation whenever either changes.
- Keep redacted active configuration display-only, while allowing a separately loaded/saved draft to be edited and published.
- Render syntax and semantic validation failures with usable path/line context and prevent publishing anything other than the exact successfully validated candidate.
- Track publish and rollback operations through terminal state, refresh active configuration only after success, and preserve actionable failure feedback.
- Add focused console and core/server regression coverage for format changes, stale validation, redacted configuration, and asynchronous mutation outcomes.

## Capabilities

### New Capabilities

- `console-configuration-workflow`: Consistent multi-format editing, validation, draft/publish, and operation feedback in the control-plane console.

### Modified Capabilities

<!-- No checked-in main capability spec currently covers the console configuration page. -->

## Impact

- Frontend: `console/src/features.tsx`, `console/src/api.ts`, and console tests.
- Core/server: configuration parsing error metadata and validation endpoint behavior/tests where needed.
- Adds the small frontend `yaml` parser/serializer dependency; no runtime endpoint removals, and request/response shapes remain backward-compatible except for richer validation issue paths.

## Non-goals

- Redesigning the overall control-plane navigation or runtime pages.
- Exposing unredacted active configuration secrets.
- Adding a new configuration language beyond the formats already accepted by the API.
