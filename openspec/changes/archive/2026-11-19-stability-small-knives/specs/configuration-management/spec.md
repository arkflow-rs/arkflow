## MODIFIED Requirements

### Requirement: Validation resources SHALL be released before real startup

The configuration validator SHALL close any temporary WAL, temporary component, state backend, and connector handle created during deep validation before the candidate is rebuilt for actual execution. Deep validation of a Job SHALL NOT open the live durable state backend referenced by the candidate's state root: when the Job declares durable state, validation SHALL construct its state backend against a temporary directory (the same pattern the stream validation path uses), so validation cannot contend with, or fail on the exclusive lock of, a currently running Job's state database.

#### Scenario: Durable candidate is validated and started

- **WHEN** a durability-enabled candidate passes dry-run validation and is immediately started
- **THEN** the validation WAL has been flushed and closed, the real runtime can reopen the same path, and no exclusive-lock failure is caused by the validator

#### Scenario: Validation does not contend with a running Job's state database

- **WHEN** a Job with durable state is running and a validation request (including a Hub-dispatched validate or an offline `--validate`) targets the same state root
- **THEN** validation succeeds using a temporary state backend and does not attempt to open the running Job's redb database exclusively
