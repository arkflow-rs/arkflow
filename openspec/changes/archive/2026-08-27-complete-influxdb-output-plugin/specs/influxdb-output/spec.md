## ADDED Requirements

### Requirement: InfluxDB output accepts mapped Arrow batches
The output SHALL map configured Arrow columns to InfluxDB measurements, tags, fields, and optional nanosecond timestamps without converting the batch to opaque binary data first.

#### Scenario: Convert a typed record to line protocol
- **WHEN** a batch contains configured string tags, numeric/boolean/string fields, and an integer timestamp
- **THEN** the output emits one valid line containing the measurement, non-null tags, typed field suffixes/quoting, and the configured timestamp

#### Scenario: Skip nulls and empty rows
- **WHEN** a configured tag or field is null
- **THEN** that tag or field is omitted, and a row with no emitted fields is not sent

#### Scenario: Preserve Arrow integer widths
- **WHEN** a configured float or integer mapping receives any Arrow signed or unsigned integer type
- **THEN** all representable values are converted without being silently omitted, and an unsigned value outside InfluxDB's signed 64-bit integer range returns an explicit error

### Requirement: InfluxDB line protocol is escaped correctly
The output SHALL escape measurement names, tag keys, field keys, tag values, and string field values according to InfluxDB Line Protocol rules.

#### Scenario: Reserved characters are escaped
- **WHEN** a configured name or value contains spaces, commas, equals signs, backslashes, or quotes
- **THEN** the emitted line contains the corresponding escaped representation and remains parseable as one point

### Requirement: InfluxDB v2 writes are batched and retried
The output SHALL send buffered lines to `/api/v2/write` with token authentication, encoded `org`/`bucket` query parameters, and retry failed requests according to configuration; a failed flush SHALL retain the buffered lines.

#### Scenario: Successful batch flush
- **WHEN** the configured batch threshold is reached or the output closes with pending lines
- **THEN** one authenticated text/plain request is sent and the buffer is cleared only after a successful HTTP status

#### Scenario: Failed request is retryable
- **WHEN** all configured attempts fail or return a non-success status
- **THEN** `write`/`close` returns a connection error and the pending lines remain buffered

### Requirement: Plugin configuration and lifecycle are discoverable
The output SHALL register as `influxdb`, expose a schema matching its deserializable configuration, reject opaque codecs with a configuration error, establish its HTTP client on `connect`, and flush pending data before `close` completes.

#### Scenario: Build a valid output
- **WHEN** a valid InfluxDB v2 configuration without a codec is supplied
- **THEN** the registered builder returns an output that can connect and write

#### Scenario: Reject incompatible codec configuration
- **WHEN** a codec is attached to the InfluxDB output
- **THEN** building the output fails with a configuration error explaining that typed field mappings require the original Arrow batch
