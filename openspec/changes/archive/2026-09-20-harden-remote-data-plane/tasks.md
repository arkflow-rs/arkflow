## 1. Protocol and configuration foundations

- [x] 1.1 Add the authenticated-session, peer identity, and bounded resource configuration types while preserving an explicit in-memory test transport path.
- [x] 1.2 Add the handshake frame/payload and shared-secret challenge verification, including constant-time credential comparison and protocol-version checks.
- [x] 1.3 Wire Agent/graph remote-edge construction with node, Job, generation, quad, and data-plane authentication context; bind the listener to the configured host instead of an unconditional wildcard.

## 2. Decoder and bounded transport hardening

- [x] 2.1 Make IPC piece/body parsing fully bounds-checked and add malformed-body regression tests proving no panic.
- [x] 2.2 Enforce frame, accepted-connection, idle-read, receipt-queue, and pending-receipt limits before allocation or retention.
- [x] 2.3 Add shared connection cleanup/supervision so protocol errors, I/O failures, cancellation, and non-EOS disconnects remove registries and abort pending branches.

## 3. Verification

- [x] 3.1 Add authentication-negative, stale-generation, wrong-quad, pre-handshake, and cleanup tests over duplex and TCP transports.
- [x] 3.2 Run focused core remote tests and the workspace test/lint gates; update the task only after the bounded backpressure and receipt semantics remain green.
