## MODIFIED Requirements

### Requirement: Liveness under input end and cancellation
Every chain SHALL terminate on input end (EOF/EOS) or cancellation without deadlocking on a backpressure wait: a producer awaiting send on a full edge SHALL be released when the downstream consumer stops and closes its endpoint, and consumers SHALL keep draining in order until the edge is empty so that pending envelopes and EOS can flow through.

#### Scenario: Drains and exits despite backpressure at input EOF
- **WHEN** the source reaches EOF while edges are at capacity and producers are awaiting send
- **THEN** consumers keep draining in order, producers send the remaining envelopes as capacity is released, EOS is forwarded through every chain, and all chains exit

#### Scenario: Cancellation unblocks a full edge
- **WHEN** a producing chain is awaiting send on a full edge and the job is cancelled so that the consumer stops and its channel endpoint closes
- **THEN** the blocked send is released because the endpoint closed, the chain unwinds its event loop, and the task set terminates without hanging or leaking tasks
