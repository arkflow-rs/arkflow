## MODIFIED Requirements

### Requirement: Recovery compatibility SHALL be evaluated consistently

Hub authorization, Agent validation, repository validation, and runtime restore SHALL apply the same compatibility result for Job identity, generation, state namespaces, operator identities, format migration, task membership, and checksums. The compatibility result SHALL be applied identically for every path that selects a recovery artifact — savepoint upgrade, version rollback, and automatic latest-checkpoint selection — so no accepted deployment reaches the Agent with an artifact the Agent will reject, and a filtered-out artifact SHALL surface to the operator rather than degrading to a silent stateless start.

#### Scenario: Hub and Agent validate the same compatible artifact

- **WHEN** the Hub authorizes a savepoint upgrade whose format migration is registered
- **THEN** the Agent accepts the same artifact under the same compatibility result and does not reject it merely because the Job version changed

#### Scenario: A version rollback applies the same compatibility result

- **WHEN** an operator rolls a Job back to an earlier version whose state format the current artifact does not declare
- **THEN** the Hub rejects the rollback with the shared compatibility result instead of accepting it and leaving the Agent to discard the artifact at recovery time

#### Scenario: A rejected artifact does not silently degrade recovery

- **WHEN** the recovery selection filters out an incompatible artifact
- **THEN** the outcome is surfaced to the operator as a rejected request or an explicit recovery error rather than reported as a successful deployment that starts without state
