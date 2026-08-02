## Context

The console is a small React/Vite application with a single configuration workflow. The API accepts YAML, JSON, and TOML candidates and returns asynchronous operations for apply/rollback. The active configuration endpoint is intentionally redacted, so it cannot safely be treated as an editable source of truth. Existing local draft state, validation state, and operation state are currently coupled too loosely.

## Goals / Non-Goals

**Goals:**

- Preserve an exact `(format, content)` pair for every editor state and validation result.
- Convert structured configuration between supported formats when the operator changes the selector, rejecting conversion when content is not parseable.
- Keep redacted active content read-only until a real draft exists.
- Wait for apply/rollback operations to reach a terminal state before refreshing data.
- Give syntax failures a line/column path and retain semantic field paths.

**Non-Goals:**

- Replacing the textarea with a full IDE/editor.
- Recovering secret values from redacted active configuration.
- Changing the control-plane operation state machine.

## Decisions

1. **Represent validation identity by both format and content.** The UI will store a serialized candidate identity (format plus content), clear validation on either change, and enable publish only when the exact current candidate has a successful report. This avoids stale validation after format-only edits.

2. **Use a small frontend YAML/JSON conversion path.** The page currently exposes YAML and JSON selectors, so it will parse either into a format-neutral value with the `yaml` package and stringify into the target format. TOML remains an API-supported candidate format but is not presented as a browser-convertible option. The original content is kept if conversion fails while showing an error.

3. **Model redacted active configuration as read-only.** When no draft exists, the active response is rendered as a display snapshot and publish/save controls remain unavailable until the operator edits or creates a draft. A redaction marker is never sent back as a secret replacement implicitly.

4. **Reuse operation polling semantics.** Configuration apply and rollback will use the same terminal-state polling helper as lifecycle commands. Only succeeded/converged operations trigger `load`; failure leaves the editor and failure details intact.

5. **Enrich parser issue locations without changing endpoint shape.** `ConfigIssue.path` remains a string; parser errors are normalized to `line N, column M` when available, while semantic validation continues to use paths such as `streams[0]`.

## Risks / Trade-offs

- [Risk] YAML/TOML serialization can change comments or stylistic formatting. → Treat conversion as a semantic conversion and clearly invalidate validation; preserve text until the operator explicitly changes format.
- [Risk] A redacted active snapshot may not be publishable as-is. → Keep it display-only and require a separately saved draft for mutation.
- [Risk] Polling can outlive the page interaction. → Bound polling and report timeout without refreshing active configuration.
