# Tasks

- [ ] Add `EventTimePartition::in_namespace(ns)` plus a stripping helper in `event_time.rs`.
- [ ] In `event_time_gates`, tag shared trackers; namespace seed + observation keys for shared groups only.
- [ ] Persist raw keys in `known_partitions`; re-apply the namespace during checkpoint restore (both local and Agent paths).
- [ ] Regression: two chains / same topic+partition, fast chain must not close the slow chain's windows.
- [ ] Regression: restore round-trip for namespaced and legacy (raw) checkpoint watermarks.
- [ ] Docs: delivery-semantics note on multi-source window watermarks.
