## Decisions

React Flow holds node positions only in browser state. A pure conversion layer derives `operators`, `sources`, `sinks`, and `edges` from graph state and reconstructs a graph from any stored JobSpec, so layout cannot alter the API contract.

The palette classifies registered input components as sources, output components as sinks, and processor components as processors. Processor component identity is written to `operator.config.type`; the selectable Job operator kind remains a valid runtime `OperatorKind`.

The Console provides immediate structural feedback for self-loops, duplicate edges, source inputs, sink outputs, and cycles. Hub `/jobs/validate` remains the authority: any graph, node-target, or field change invalidates the last result and prevents create/upgrade until the current request returns valid.

Schema forms support scalar, enum, object, array, composition, and open-object properties. There is no raw JSON authoring entry point.

The external Hub router exposes the same component catalogue and schema routes as the local server. Hub startup initializes the shared plugin registry once before serving requests, so catalogue data is available without an Agent or local Engine process.
