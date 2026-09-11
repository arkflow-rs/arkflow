## MODIFIED Requirements

### Requirement: Ordered delivery per edge

Data envelopes on one edge SHALL be delivered in send order; forward chains
SHALL preserve end-to-end batch order from source to sink. With a processor
worker pool (`pipeline.thread_num > 1`), the pool SHALL publish processed
outputs in submission order, and control events that generate data (such as
idle ticks) SHALL NOT overtake deliveries already submitted to the pool.

#### Scenario: Forward chain order

- **WHEN** a forward-only chain writes two batches to its sink
- **THEN** the sink observes them in source order

#### Scenario: Tick does not overtake pooled data

- **WHEN** a pooled chain receives an idle tick while earlier data deliveries are still being processed by workers
- **THEN** the tick's generated batches are sent downstream only after those earlier deliveries have been published

#### Scenario: Control fence completes while workers are healthy

- **WHEN** a barrier or watermark fence waits for the pool's in-flight results and every worker completes them
- **THEN** the fence returns without stalling, regardless of scheduling interleavings between the collector's notifications and the fence
