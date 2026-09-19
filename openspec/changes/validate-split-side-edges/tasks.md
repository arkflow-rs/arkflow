## 1. Side-edge model and validation

- [x] 1.1 Add a shared JobPlan/JobSpec helper that enumerates error and late-event side-edge endpoint pairs with stable labels.
- [x] 1.2 Validate all side-edge task assignments in Hub split placement and return actionable node/task errors.
- [x] 1.3 Add Agent/ExecutionGraph defense-in-depth validation for partial assignments before materializing remote or local edges.

## 2. Regression coverage

- [x] 2.1 Add a two-node session-window late-route split-placement rejection test.
- [x] 2.2 Add error-output and colocated-side-edge acceptance tests while ordinary data edges remain splittable.
- [x] 2.3 Add incomplete-assignment and graph-construction tests; focused server/core tests and clippy remain in final verification.
