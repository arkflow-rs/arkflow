## MODIFIED Requirements

### Requirement: Approver SHALL differ from the requester

While approval policy is enabled, the Hub SHALL grant the approve action only to principals holding the dedicated `ApproveRollouts` permission (held by the admin role, not the operator role) and SHALL reject an approval whose principal is the same as the request's requester. Rejecting one's own submission is the conservative direction and MUST remain available: the requester MAY withdraw (reject) a pending request they submitted. Threat model boundary: this segregation enforces two-principal four-eyes review (`approver_id != requester_id`) to prevent accidental self-approval and operator self-promotion; because the `admin` role holds both `ManageUsers` and `ApproveRollouts` (and static `ARKFLOW_OPERATOR_TOKEN` is break-glass), the Hub does NOT defend in software against a malicious admin — whether static, directory-managed, or OIDC-authenticated — creating a second local user and credential to self-approve. Detection relies on external review and alerting over the actor-attributed audit trail (`user.create`, `credential.create`, `config_approval.submit`, `config_approval.approve`); any policy restricting local user/credential issuance must be enforced externally by operators rather than assumed as a Hub guarantee.

#### Scenario: Self-approval is rejected

- **WHEN** the principal that submitted a pending request approves it
- **THEN** the decision fails with an error naming the segregation rule

#### Scenario: The requester may withdraw their own submission

- **WHEN** the principal that submitted a pending request rejects it
- **THEN** the request becomes rejected with that principal recorded, and no rollout is created

#### Scenario: Operator role cannot approve

- **WHEN** a principal holding only the operator role attempts to approve a pending request
- **THEN** the decision is not authorized

#### Scenario: Distinct admin approval is audited end to end

- **WHEN** one admin creates a second admin identity and submits an approval request that the second admin approves
- **THEN** approval succeeds as a distinct principal and the audit trail records the real acting principal for user creation, credential issuance, submission, and approval
