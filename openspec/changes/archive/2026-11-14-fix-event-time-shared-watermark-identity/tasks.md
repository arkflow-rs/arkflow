# Tasks

- [x] Add `EventTimePartition::in_namespace(ns)` plus a stripping helper in `event_time.rs`.
- [x] In `event_time_gates`, tag shared trackers; namespace seed + observation keys for shared groups only.
- [x] Persist raw keys in `known_partitions`; re-apply the namespace during checkpoint restore (both local and Agent paths).
- [x] Regression: two chains / same topic+partition, fast chain must not close the slow chain's windows.
- [x] Regression: restore round-trip for namespaced and legacy (raw) checkpoint watermarks.
- [x] Docs: delivery-semantics note on multi-source window watermarks.
