## Why

The Console currently requires operators to write a complete JobSpec JSON document. This makes topology changes, component configuration, event-time setup, and savepoint upgrades unnecessarily error-prone even though the Hub already validates and persists the same Job contract.

## What Changes

- Replace the Job JSON editor with one shared visual DAG orchestrator for creation and savepoint upgrades.
- Populate a source, processor, and sink palette from the registered `/components` catalogue and edit component configuration through JSON Schema forms.
- Convert between session-only React Flow layout and the unchanged persisted JobSpec topology.
- Require validation of the exact graph and target nodes before submission, and expose plan, warnings, capabilities, and per-node incompatibilities.

## Impact

- Affects `console/` and the Hub component-catalogue startup path; no persistence migrations or JobSpec wire-format changes.
- New Jobs continue to use `desired_state: stopped`; upgrade requests retain savepoint and generation fencing before the existing explicit start action.
