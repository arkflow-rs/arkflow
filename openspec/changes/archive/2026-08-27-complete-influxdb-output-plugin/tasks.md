## 1. Configuration and mapping contract

- [x] 1.1 Align InfluxDB mapping structs, validation, metadata schema, and example configuration.
- [x] 1.2 Reject codecs at build time and preserve typed Arrow batches for conversion.
- [x] 1.3 Implement robust primitive value conversion, null handling, timestamp fallback, and Line Protocol escaping.

## 2. HTTP output behavior

- [x] 2.1 Implement encoded v2 write query parameters, auth/content headers, timeout, and retry handling.
- [x] 2.2 Flush by configured line count/interval checks and on close while retaining data after failed flushes.
- [x] 2.3 Ensure connect/close lifecycle errors are propagated and connection state is consistent.

## 3. Tests and verification

- [x] 3.1 Add conversion tests covering typed fields, nulls, timestamps, and escaping.
- [x] 3.2 Add request/lifecycle tests using a local HTTP responder for successful and failed flushes. The success responder test is present but ignored in this sandbox because local TCP bind is denied; the failed-flush retention test runs normally.
- [x] 3.3 Run formatting, focused plugin tests, workspace tests as practical, and strict OpenSpec validation.
- [x] 3.4 Resolve review feedback by covering every Arrow integer width and explicitly rejecting unrepresentable unsigned integer values.
