## Context

The physical Job plan has ordinary data edges and logical side edges. The graph builder materializes late-event routes dynamically, so a source-to-route check alone does not prove that the actual window-to-route path is colocated. The Hub already builds the complete task-to-node assignment before dispatch and is the right place to reject an invalid split, while the Agent/graph builder remains a defense-in-depth validator.

## Goals / Non-Goals

**Goals:**

- Derive one complete side-edge relation for error and late-event paths.
- Reject every split side edge before a `job_start` command is dispatched.
- Keep graph construction and Hub validation consistent.
- Produce errors naming the side edge and the two assigned nodes.

**Non-Goals:**

- Remote transport for side edges.
- Changes to normal partitioned data edges or event-time policy.

## Decisions

1. **Represent side edges as logical operator pairs, not normal `EdgeSpec`s.**
   A helper on `JobPlan`/`JobSpec` returns `(source/window/error origin, target, kind)` for every configured error output and late-event route. This preserves the existing distinction that a late route need not be repeated in `edges` while giving placement one complete relation.

2. **Validate assignment after task placement and before dispatch.**
   Compare the assigned node of every task on each endpoint. If any pair differs under `Split`, return a Hub validation error. Colocated placement keeps its existing connected-component behavior. Agent graph construction repeats the check for partial assignment payloads.

3. **Reject rather than implicitly co-locate.**
   Silently moving one endpoint would change the Hub's placement and task ownership after reconciliation. Rejection is deterministic and avoids a partial graph with a side channel that cannot carry the required control semantics.

4. **Keep remote side-edge support separate.**
   A future implementation would need explicit side-edge quads, late-only routing, Barrier/EOS propagation, and receipt semantics. This change must not accidentally route normal data through a late-event sink.

## Risks / Trade-offs

- [Risk] Some split placements become invalid. → Return an actionable error and recommend colocated placement or an explicit remote-side-edge feature.
- [Risk] A plan has no concrete task for a logical side target on a partial Agent. → Fail closed with an incomplete-assignment error rather than assuming local placement.
- [Risk] Error-output metadata is represented differently by legacy Streams. → Cover both Job side routes and legacy compiler-generated links in tests.

## Migration Plan

1. Add the side-edge helper and unit tests without changing ordinary data placement.
2. Add Hub pre-dispatch validation and Agent/graph defense-in-depth validation.
3. Run existing two-node split tests; update fixtures that intentionally split side edges to use colocated mode.
4. Keep remote side-edge implementation as a future proposal.
