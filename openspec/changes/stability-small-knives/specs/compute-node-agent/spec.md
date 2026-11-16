## ADDED Requirements

### Requirement: Lifecycle stops settle within a bounded wait

The JobRuntime stop and stop-all paths SHALL bound their wait on a kernel teardown join with the same bound the start path uses for superseded kernels. When the bound expires, the stop SHALL proceed detached: the wedged kernel finishes on its own, the stop path releases its start mutex, removes the data-plane session, and returns without blocking subsequent lifecycle operations. A wedged teardown SHALL be reported through a warning log naming the job and the bound.

#### Scenario: Wedged kernel does not wedge lifecycle

- **WHEN** a kernel ignores cancellation and its teardown join does not complete within the bound
- **THEN** stop/stop-all log a warning, detach the kernel, release the start mutex within the bound, and a subsequent start of the same job proceeds

#### Scenario: Normal stop remains unchanged

- **WHEN** a kernel tears down promptly after cancellation
- **THEN** stop waits for the join outcome and performs state close and session removal exactly as before
