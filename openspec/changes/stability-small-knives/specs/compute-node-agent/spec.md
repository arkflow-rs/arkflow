## ADDED Requirements

### Requirement: Lifecycle stops settle within a bounded wait

The JobRuntime stop and stop-all paths SHALL bound their wait on a kernel teardown join with the same bound the start path uses for superseded kernels. When the bound expires, the stop SHALL proceed detached: the wedged kernel finishes on its own, the stop path releases its start mutex, removes the data-plane session, and returns without blocking subsequent lifecycle operations. The wedged kernel's state close and dedicated-runtime shutdown SHALL transfer to a background finisher that joins the detached kernel without a bound — cutting them off inline would cancel the kernel's yielding cleanup tasks mid-flight. A wedged teardown SHALL be reported through a warning log naming the job and the bound.

#### Scenario: Wedged kernel does not wedge lifecycle

- **WHEN** a kernel ignores cancellation and its teardown join does not complete within the bound
- **THEN** stop/stop-all log a warning, detach the kernel, release the start mutex within the bound, and a subsequent start of the same job proceeds

#### Scenario: Wedged kernel cleanup completes in the background

- **WHEN** a stop times out waiting for a wedged kernel
- **THEN** the stop does not close the kernel's state or shut down its dedicated runtime inline; a background finisher joins the detached kernel first and then closes state and shuts the runtime down

#### Scenario: Normal stop remains unchanged

- **WHEN** a kernel tears down promptly after cancellation
- **THEN** stop waits for the join outcome and performs state close and session removal exactly as before
