## ADDED Requirements

### Requirement: Audit write failures SHALL be observable

The Hub SHALL export a closed-label Prometheus counter `arkflow_audit_write_failures_total{category}` counting failed standalone audit persistence attempts routed through `Hub::record_audit_event_for` / `Hub::record_audit_best_effort` against a configured durable store (transactional audit inserts such as `replace_node_labels` fail and roll back their enclosing transaction directly). The `category` label SHALL be drawn from a fixed low-cardinality enumeration (`auth_denial`, `identity`, `job`, `approval`, `rollout`, `node`, `other`); resource IDs, actor names, correlation IDs, error messages, and secrets MUST NOT appear as labels. Counters MAY reset on Hub restart in line with Prometheus counter semantics.

#### Scenario: Failed audit write increments the closed-label counter

- **WHEN** an audit persistence attempt fails on a Hub with durable storage configured
- **THEN** `arkflow_audit_write_failures_total` for that category increments and is exported on the Hub `/metrics` endpoint without dynamic labels
