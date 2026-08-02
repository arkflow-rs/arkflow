---
sidebar_position: 2
---

# Backpressure & ordered delivery

## Backpressure

A stream's input worker reads as fast as the source allows, while processors
and outputs work at their own speed. Without a bound, a slow processor would
let unbounded batches pile up in memory and eventually exhaust it.

ArkFlow bounds this with a **backpressure threshold** on the channel between
the input and the processor workers. When the number of in-flight (pending)
messages in that channel exceeds **1024**, the input worker **blocks** until
the processors drain enough to drop back below the threshold.

Effectively:

- the source is not read faster than the pipeline can absorb;
- memory use stays bounded under sustained load;
- replayable sources simply pause their fetch, so no data is lost (the broker
  retains it).

For windowing buffers (tumbling / sliding / session), the buffer itself holds
batches until the window closes, so the threshold interacts with the window's
size and flush interval — size the window so it does not stall the input.

## Ordered output

Processor workers run in parallel (`thread_num > 1`), so batches can finish
out of order. The output worker nonetheless writes them **in the order the
input produced them**, using sequence numbers:

- each batch received from the input gets a monotonically increasing **sequence
  number**;
- the output worker tracks the next expected sequence and waits for any
  out-of-order batch before writing the ones that finished earlier;
- an atomic counter records the next expected sequence, so writes to the sink
  happen strictly in input order.

This matters for sinks where order is observable (Kafka partition keys, a SQL
table without its own ordering, append-only files). Source-side acknowledgment
(alignment / ack) happens only after the output confirms the write, which is
what makes the at-least-once contract hold — see
[Delivery semantics](./4-delivery-semantics.md).
