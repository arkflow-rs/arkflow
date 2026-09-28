## ADDED Requirements

### Requirement: In-app destructive action confirmation

The console SHALL present destructive or lifecycle-changing actions (stream start/stop/restart, node drain/maintain/resume, job stop, configuration publish/rollback, rollout create/pause/resume/cancel/rollback, upgrade restore) behind an in-application confirmation dialog with focus containment, Escape-to-cancel, and Enter-to-confirm. The console SHALL NOT use native `window.confirm` or `window.prompt` dialogs. The outcome of such actions SHALL be reported through a transient toast notification in addition to the existing operation tracking.

#### Scenario: Confirm a node drain
- **WHEN** an operator triggers a node drain and confirms the dialog
- **THEN** the drain request is sent, a toast reports the accepted outcome, and the dialog closes

#### Scenario: Cancel a destructive action
- **WHEN** an operator dismisses the confirmation dialog with Escape or the cancel button
- **THEN** no request is sent and the current view is unchanged

### Requirement: Loading and empty states

The console SHALL render skeleton placeholders for tables and metric panels while their data is loading, and empty states SHALL pair a plain-language explanation with the next available action (such as a create button or filter reset) instead of bare text alone.

#### Scenario: Loading a table
- **WHEN** a list page is fetching its first page of data
- **THEN** skeleton rows are shown instead of an empty table or a blank panel

#### Scenario: Empty state offers the next action
- **WHEN** a filtered list matches nothing
- **THEN** the empty state explains the miss and offers the nearest action (for example clearing the filter or creating the first resource)
