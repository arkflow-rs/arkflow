## ADDED Requirements

### Requirement: Consistent configuration candidates

The console SHALL treat configuration format and content as one candidate. Changing either SHALL invalidate the previous validation result, and changing format SHALL convert parseable content before making it the new candidate.

#### Scenario: Convert a valid candidate
- **WHEN** an operator changes a valid YAML candidate to JSON
- **THEN** the editor contains equivalent JSON content, the selected format is JSON, and the candidate requires validation again

#### Scenario: Reject an unconvertible candidate
- **WHEN** an operator changes a malformed candidate's format
- **THEN** the original content is preserved, validation is cleared, and a visible conversion error is shown

### Requirement: Safe draft and redacted active configuration

The console MUST distinguish an editable draft from the redacted active configuration. Redacted active values SHALL remain display-only and SHALL never be implicitly published as replacement secrets.

#### Scenario: No draft exists
- **WHEN** the configuration page loads only a redacted active configuration
- **THEN** the page displays it as read-only and does not enable Save draft or Publish

#### Scenario: Draft exists
- **WHEN** an editable draft is loaded
- **THEN** the editor uses the draft format/content and permits saving and validating that draft

### Requirement: Exact validation gate

Publish SHALL be enabled only when the exact current format/content candidate has a successful validation report and is not dirty relative to the saved draft. Validation issues SHALL include a document path or a line/column location for syntax errors and structured field paths for semantic errors.

#### Scenario: Edit after validation
- **WHEN** an operator changes content or format after a successful validation
- **THEN** Publish becomes disabled until the new exact candidate is successfully validated

#### Scenario: Invalid candidate
- **WHEN** validation returns syntax or semantic errors
- **THEN** the UI renders each issue with its path/location and does not call the publish endpoint

### Requirement: Terminal configuration mutations

Apply and rollback SHALL track the returned operation through a terminal success or failure state. The console SHALL refresh active configuration and version history only after success, and SHALL preserve actionable failure information otherwise.

#### Scenario: Successful publish
- **WHEN** a validated draft is published and its operation converges successfully
- **THEN** the console refreshes configuration/version data and reports the successful terminal result

#### Scenario: Failed publish
- **WHEN** a publish operation fails, times out, or is blocked
- **THEN** the console reports the failure and does not replace the current editor content or reload active configuration
