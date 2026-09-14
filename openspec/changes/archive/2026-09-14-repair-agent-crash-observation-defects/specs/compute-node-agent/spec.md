## ADDED Requirements

### Requirement: Same-generation lifecycle starts are idempotent for live kernels and crash-visible for dead kernels

When the Agent receives a lifecycle start for a Job that already has a registered kernel at the same generation, it SHALL reply with success as a no-op if and only if that kernel is still running, without cancelling or restarting it. If the registered kernel at that generation has exited, or a higher-generation start supersedes a kernel that has exited, the Agent SHALL drain the exited kernel, release its state backend, and surface the exit outcome to the Hub through the job observation channel. The Agent SHALL NOT report success for an exited kernel as if the Job were running, and SHALL NOT silently discard the exit outcome of a replaced kernel. A superseded kernel that tears down cleanly SHALL NOT produce a crash observation.

#### Scenario: Healthy same-generation redelivery is a no-op success

- **WHEN** the Hub re-delivers a `job_start` for the generation whose kernel is registered and still running
- **THEN** the Agent reports success for the command without cancelling, restarting, or otherwise churning the running kernel

#### Scenario: Start arrives for a crashed kernel at the same generation

- **WHEN** a `job_start` arrives for a generation whose registered kernel has exited with an error before the polling drain observed the exit
- **THEN** the Agent removes the exited entry, reports the crash as a failed job observation for that generation, and starts a fresh kernel at that generation whose terminal result reflects the new start

#### Scenario: Generation bump replaces a crashed kernel

- **WHEN** a higher-generation `job_start` supersedes a registered kernel that has exited with an error
- **THEN** the crash outcome is reported through the job observation channel for the superseded generation, the superseded kernel's state backend is released, and the new generation proceeds

#### Scenario: Graceful replacement produces no crash observation

- **WHEN** a superseded kernel completes its teardown cleanly (success)
- **THEN** the Agent parks no failed observation for it and the new start proceeds normally
