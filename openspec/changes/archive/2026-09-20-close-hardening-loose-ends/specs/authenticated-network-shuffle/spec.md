# Delta: authenticated-network-shuffle

## ADDED Requirements

### Requirement: The unauthenticated transport surface SHALL be test-only

A `NetworkManager` without configured data-plane credentials SHALL NOT open remote edges or bind the production TCP listener outside the test boundary. The legacy unauthenticated edge constructors SHALL exist only within `cfg(test)` builds, and binding the data-plane listener without credentials MUST fail explicitly instead of serving unauthenticated frames.

#### Scenario: Credentials-less manager cannot bind a listener

- **WHEN** a `NetworkManager` is constructed without data-plane credentials and `bind_tcp` is called
- **THEN** the bind fails with an explicit configuration error and no listener socket is created

#### Scenario: Unauthenticated edge constructors are absent from non-test builds

- **WHEN** the workspace compiles a non-test target
- **THEN** the unauthenticated edge constructors (`open_edge`, `open_edge_with_stream`, `open_edge_deferred`) are not part of `NetworkManager`'s public API, and remote edges open only through the authenticated session APIs

#### Scenario: Test boundary keeps in-memory transports

- **WHEN** the executor's own test suite builds
- **THEN** the legacy constructors remain available so in-memory (duplex-stream) tests can exercise the frame codec and edge semantics without network secrets, while TCP-path tests use authenticated sessions
