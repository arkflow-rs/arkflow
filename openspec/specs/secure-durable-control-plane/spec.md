# secure-durable-control-plane Specification

## Purpose
Secure and durable defaults for the Hub control plane: an externally reachable Hub requires durable storage and credentials at startup, readiness follows durable recovery, and missing credentials fail closed. Created by syncing change secure-durable-control-plane.

## Requirements

### Requirement: External Hub startup SHALL require durable storage and credentials

When the Hub binds a non-loopback address, startup SHALL require a configured durable storage path, operator credential, and node credential. The server MUST refuse to bind or report ready when any required item is missing. An insecure local mode MAY bypass these checks only when explicitly enabled and the bind address is loopback.

#### Scenario: External bind lacks storage

- **WHEN** the Hub is configured to bind a non-loopback address without durable storage
- **THEN** startup fails before the listener binds and explains how to configure storage

#### Scenario: External bind lacks credentials

- **WHEN** the Hub is configured to bind a non-loopback address without an operator or node credential
- **THEN** startup fails before the listener binds and does not expose unauthenticated APIs

#### Scenario: Explicit local development mode

- **WHEN** the Hub binds loopback, durable storage is omitted, and insecure-local mode is explicitly enabled
- **THEN** the server may start for development while marking the mode in diagnostics

#### Scenario: Insecure mode is attempted externally

- **WHEN** insecure-local mode is enabled with a non-loopback bind address
- **THEN** startup fails instead of weakening external authentication

### Requirement: Hub readiness SHALL follow durable recovery

The Hub SHALL restore persisted Jobs, operations, desired state, leases, and checkpoint pointers before binding its listener or reporting ready. Storage recovery errors SHALL leave the server unavailable.

#### Scenario: Storage recovery succeeds

- **WHEN** the configured store opens and persisted records are restored successfully
- **THEN** the listener binds and readiness can become healthy with the restored control-plane view

#### Scenario: Storage recovery fails

- **WHEN** opening or restoring the configured store returns an error
- **THEN** startup fails or readiness remains unavailable, and the Hub does not serve a partial in-memory state

### Requirement: Missing credentials SHALL fail closed

In secure mode, an absent operator token SHALL not create an implicit Admin principal, and an absent node token SHALL not authorize registration. Valid credentials SHALL continue to use constant-time comparison and the existing session-token checks.

#### Scenario: Operator request omits a token

- **WHEN** a secure Hub receives an operator API request without credentials
- **THEN** it returns unauthorized and performs no mutating operation

#### Scenario: Node registers without a token

- **WHEN** a secure Hub receives a registration request without the configured node credential
- **THEN** registration is rejected and no node lease is created

#### Scenario: Valid credentials are supplied

- **WHEN** the operator or node presents the configured credential
- **THEN** the request passes the existing authorization and auditing path
