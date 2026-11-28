## ADDED Requirements

### Requirement: Node identity and operations SHALL be audited with node attribution

Every accepted or rejected `agent.register`, node-credential lifecycle mutation (`node_credential.create`, `node_credential.enroll`, `node_credential.rotate`, `node_credential.revoke`), agent mTLS/credential rejection (`agent.auth_rejected`), and terminal node command result (`agent.command_result`) SHALL be recorded in the Hub audit trail (best-effort through `record_audit_best_effort` under category `"node"` so storage failures increment `arkflow_audit_write_failures_total{category="node"}` without masking security responses). Audit records SHALL identify the acting node (`actor = "node:<node_id>"` for node-initiated events or operator principal id for admin mutations), `node_id`, `credential_id` or certificate subject when present, outcome, and stable `failure_code` where applicable, and MUST NOT contain token plaintexts, secret hashes, or private key material.

#### Scenario: Node registration and terminal command execution identify the node

- **WHEN** `"node-a"` registers with a per-node credential and later reports a terminal `Succeeded` result for a command
- **THEN** the audit log contains an accepted `agent.register` record and an `agent.command_result` record identifying `node_id = "node-a"`, actor `"node:node-a"`, and the authenticating `credential_id` without secret material

#### Scenario: Impersonation and credential mutations are audited without secrets

- **WHEN** an operator creates, rotates, or revokes a node credential, or a caller attempts to register `"node-b"` with `"node-a"`'s credential
- **THEN** the audit log records the actor, target `node_id`, `credential_id`, outcome, and `failure_code` (`node_identity_mismatch`) with no plaintext token or hash present
