# split-side-edge-placement Specification

## Purpose
Validation rules for side edges (error outputs and late-event routes) under split placement: side edges must stay co-located on one node while ordinary data edges may cross nodes via the network shuffle. Created by syncing change validate-split-side-edges.

## Requirements

### Requirement: Split placement SHALL keep side edges co-located

When a Job uses split placement, every error-output and late-event route SHALL be represented as a side edge and both endpoints SHALL be assigned to the same node. The Hub SHALL reject an assignment that splits any side edge before dispatch, and the Agent SHALL repeat the check before graph construction.

#### Scenario: Session window late route is split

- **WHEN** a session window task is assigned to node A and its late-event route task is assigned to node B
- **THEN** validation rejects the placement with the window, route, and node identities

#### Scenario: Error side edge is split

- **WHEN** a processor's error output task is assigned to a different node from its origin task
- **THEN** validation rejects the placement before starting either partial Job assignment

#### Scenario: All side edges are colocated

- **WHEN** split placement assigns every side-edge endpoint pair to one node while ordinary data edges cross nodes
- **THEN** the placement is accepted and only ordinary data edges use the network shuffle

#### Scenario: Partial Agent assignment omits a side endpoint

- **WHEN** an Agent receives a task assignment that cannot resolve one side-edge endpoint or its node
- **THEN** graph construction fails closed with an incomplete side-edge assignment error
