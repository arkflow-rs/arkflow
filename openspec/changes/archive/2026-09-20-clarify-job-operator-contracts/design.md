## Context

`OperatorKind::Join` is part of the public schema, but the execution graph invokes the single-batch Processor builder for non-window operators and has no multi-input processor or barrier alignment contract. Treating Join as accepted is therefore misleading. State namespaces are also assembled in the graph from only Job id and task id, ignoring `StateSpec.namespace`, while `RedbStateBackend::with_max_bytes` exists but is not wired to Job configuration.

## Goals / Non-Goals

**Goals:**

- Reject unsupported distributed multi-input Join plans before deployment.
- Centralize effective namespace construction and use it in stateful/window operators.
- Add a validated `max_bytes` limit and pass it to every configured Redb Job backend.
- Keep public schema and generated documentation accurate.

**Non-Goals:**

- A full streaming Join implementation.
- Rewriting SQL/DataFusion execution.

## Decisions

1. **Reject `OperatorKind::Join` at Job validation.**
   This is safer than pretending a single-input Processor can implement two-input stateful semantics. The error names the operator and directs users to supported SQL/local paths. A future Join proposal can introduce a `MultiInputProcessor` trait, keyed state, watermarks, and barrier alignment.

2. **Treat `StateSpec.namespace` as a stable logical prefix.**
   The effective namespace is `job:<job>:state:<prefix>:operator:<operator>:task:<task>`, with `default` when no prefix is provided. This preserves Job/operator/task isolation and makes the field useful without allowing two Jobs to collide.

3. **Use `max_bytes` as a backend write budget.**
   `StateSpec.max_bytes` is optional for compatibility; when set it must be positive and is passed to `RedbStateBackend::with_max_bytes`. The backend already performs atomic accounting and returns a budget error; the runtime propagates it and leaves the input acknowledgement replayable.

4. **Apply the same helper to windows and generic stateful processors.**
   No operator constructs a namespace ad hoc. Checkpoint snapshot/restore already carries namespace strings, so stable construction makes compatibility checks deterministic.

## Risks / Trade-offs

- [Risk] Existing configurations using `join` stop at validation. → This is an intentional contract correction; provide an explicit migration error and keep legacy join diagnostics.
- [Risk] Namespace changes make old snapshots incompatible. → Treat the effective namespace as part of compatibility and require a savepoint migration for renames.
- [Risk] A low byte budget causes processing failures. → Expose live state bytes and the configured bound in metrics/errors and document replay behavior.

## Migration Plan

1. Add schema fields and validation errors without changing supported map/filter/window Jobs.
2. Update examples/docs that claim Join is deployable without a multi-input runtime.
3. For custom namespaces, create a compatible savepoint before upgrading; otherwise start a new Job id intentionally.
4. Configure `max_bytes` after measuring state size; unset retains the existing unlimited compatibility behavior until operators opt in.
