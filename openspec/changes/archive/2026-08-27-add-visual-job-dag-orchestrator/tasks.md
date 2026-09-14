## 1. Visual DAG model

- [x] 1.1 Add React Flow and a bidirectional JobSpec/graph conversion layer.
- [x] 1.2 Reject invalid graph connections and cycles before they can be submitted.
- [x] 1.3 Add model coverage for persisted-spec rehydration and JobSpec serialization.

## 2. Console orchestration workflow

- [x] 2.1 Replace the visible Job JSON editing workflow with a component-backed graph palette and node inspector.
- [x] 2.2 Render component schemas, open-object editors, global state/checkpoint/recovery settings, and source event-time controls.
- [x] 2.3 Reuse the editor for stopped creation and savepoint upgrade with generation fencing.
- [x] 2.4 Invalidate validation on every graph or target change and surface Hub plan and compatibility results.

## 3. Validation

- [x] 3.1 Run Console unit tests, typecheck, production build, strict OpenSpec validation, and `git diff --check`.

## 4. Hub component catalogue regression

- [x] 4.1 Expose component catalogue/schema routes from the external Hub and initialize plugin metadata once at Hub startup.
- [x] 4.2 Distinguish palette loading, empty, and retryable failure states in the Console.
- [x] 4.3 Add Hub route and Console loading-state coverage, then rerun focused validation.

## 5. Compact component browsing

- [x] 5.1 Add shared component search and Input/Processor/Output category controls.
- [x] 5.2 Replace the expanded Job palette and catalogue card grid with compact filtered views and selected-item details.
- [x] 5.3 Add filtering/selection coverage and rerun Console validation.
