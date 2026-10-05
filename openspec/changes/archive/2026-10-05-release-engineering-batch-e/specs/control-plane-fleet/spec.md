# Delta: control-plane-fleet

## ADDED Requirements

### Requirement: Fleet operational limits SHALL be documented for users

The distributed-jobs documentation (English and zh-Hans) SHALL state the supported fleet scale ceiling (MAX_NODES = 256) and summarize the long-run stability testing evidence behind it, so operators can size deployments against tested limits instead of discovering them from internal planning notes.

#### Scenario: An operator sizes a deployment

- **WHEN** an operator plans a fleet larger than 256 nodes and reads the distributed-jobs page
- **THEN** the page states the tested ceiling and directs larger deployments to seek maintainer guidance rather than implying unbounded scale

#### Scenario: The limit changes

- **WHEN** the MAX_NODES ceiling or the stability evidence changes in the implementation
- **THEN** the distributed-jobs page (en and zh-Hans) is updated in the same change
