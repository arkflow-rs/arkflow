## Context

The previous runtime hardening change introduced acknowledged-cut checkpoints, state journals, resource guards, partition-aware windows, and Job-level reconciliation. Review found that several boundaries still use a weaker model than the new guarantees require:

- WAL and state acknowledgements can complete out of order, but durable cursors and rollback currently act as if completion were serialized.
- A single connector task may read many physical Kafka partitions, while event-time hooks and window state can see only one logical partition.
- Local checkpoint participants are execution chains, whereas compatibility validation uses logical plan tasks.
- Agent registration creates a new process runtime, but persisted successful operations can suppress the commands needed to reconstruct it.
- Worker-pool error handling and validation fallbacks lose payload or schema errors that the single-worker path exposes.

The implementation must preserve the existing public Job model, at-least-once delivery contract, bounded backpressure, and compatible savepoint rules while making failures replayable and recovery decisions conservative.

## Goals / Non-Goals

**Goals:**

- Make source/WAL durability advance only through a contiguous acknowledged frontier, including fan-out and retry paths.
- Make state and window commits happen before source acknowledgement, and prevent an older rollback from erasing a newer commit or its TTL.
- Carry physical input-partition identity through event-time evaluation and compute the minimum watermark over active partitions.
- Keep local checkpoint manifests, configured state format, and execution participants derived from one canonical execution view.
- Reconstruct desired-running local Jobs after an Agent process restart and preserve failed worker-pool deliveries on error edges.
- Turn silent configuration/metric fallbacks into explicit validation or complete accounting.

**Non-Goals:**

- No new external transaction coordinator, sink protocol, or connector is introduced.
- No change to Kafka's one-task subscribe-all versus multi-task explicit-assignment topology.
- No change to the public Job configuration shape or existing at-least-once semantics.
- No redesign of the Hub storage schema beyond the session/reconciliation fields needed for restart correctness.

## Decisions

### 1. Use one contiguous frontier for every durable input acknowledgement

WAL-backed source acknowledgements will be anchored to the original input sequence and recorded in a per-input `CommitFrontier`. A child/fan-out acknowledgement marks its sequence complete, but the WAL cursor advances only through the highest contiguous completed sequence. Duplicate acknowledgements remain idempotent; a failed child leaves the gap open. This is chosen over writing child sequence numbers directly because replay must never skip an earlier unacknowledged record.

The same frontier is used when a checkpoint captures positions. A restored cursor seeds the frontier, so a checkpoint immediately after restore cannot replace the restored position with an empty report.

### 2. Make state acknowledgement transactions ordered and version-conditional

State/window output acknowledgements will be assembled so the journal commit is attempted before source/WAL acknowledgements. A transient failure rolls the transaction back to its staged state and keeps it retryable. The journal records the version/value/TTL observed before apply and the version produced by apply; undo is allowed only when the current value still carries the transaction's applied version. This prevents transaction A from restoring bytes over a later transaction B.

TTL metadata is part of the mutation snapshot and is restored with the value. Per-key commit serialization is retained as an additional ordering guard, but conditional undo remains necessary for asynchronous acknowledgement completion.

### 3. Treat physical input partition as event-time identity

Connectors that multiplex physical partitions will attach the physical partition to each delivery (using the existing metadata path). The event-time gate will evaluate each row against that partition, and window watermark state will be keyed by source/operator/physical partition. A fixed task partition is used only when the delivery has no physical partition metadata. This preserves one-task multi-partition subscription while avoiding a false partition-0 watermark.

Watermark recomputation establishes the active partition set before advancing and takes the minimum of active progress. A global watermark remains monotonic only after the active set is valid; a newly observed slower partition cannot be hidden by an earlier provisional maximum.

### 4. Derive checkpoint membership and format from the execution model

Local manifests will list the complete logical task set expected by the Job plan, while chain-specific snapshots retain their execution-chain identity in the state payload. Checkpoint aggregation will use the configured Job state format even when all reports are stateless or empty. The recovery validator will therefore compare like-for-like sets and format versions without making fused processors unrecoverable.

### 5. Reconcile Agent process identity independently from session authentication

Registration will establish a fresh process boot identity and the first report will use that identity, not a session credential. A new boot invalidates in-memory assumptions about successful starts; reconciliation must dispatch desired-running Jobs whose local runtime is absent, even if an older operation record is terminal-successful.

This keeps delayed reports from older processes fenced while allowing the new process to reconstruct its runtime state.

### 6. Preserve failure payloads and reject ambiguous configuration

The worker-pool path will use the same failed batch/ack/error-output routing as the single-worker path. Window aggregation will distinguish an empty `value_fields` configuration from a non-empty configuration whose fields cannot be resolved; the latter is a validation error. Late metrics will count rows, including drop and invalid-timestamp outcomes, and routed invalid timestamps will carry a dedicated marker.

## Risks / Trade-offs

- **[Risk]** A stricter contiguous frontier can delay cursor advancement when one fan-out branch is slow. → **Mitigation:** retain bounded backpressure and expose the pending gap through existing acknowledgement/checkpoint errors rather than skipping it.
- **[Risk]** Conditional rollback can leave a transaction staged after a concurrent newer commit. → **Mitigation:** serialize per-key finalization and surface the conflict as a retryable state error; never overwrite newer bytes.
- **[Risk]** Physical partition metadata may be absent from custom inputs. → **Mitigation:** retain the explicit task partition fallback and require multiplexing connectors to populate metadata in their connector tests.
- **[Risk]** Re-dispatch after Agent restart may duplicate an already running remote assignment. → **Mitigation:** retain generation/attempt fencing and make start commands idempotent for the new boot session.
- **[Risk]** Rejecting missing window fields changes a silent count fallback into a configuration failure. → **Mitigation:** apply the rejection only when `value_fields` is explicitly non-empty and report the missing field names.
