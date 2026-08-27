# mongodb-output Specification

## Purpose
TBD - created by archiving change add-mongodb-output. Update Purpose after archive.
## Requirements
### Requirement: MongoDB output configuration

The MongoDB output MUST be registered under the `mongodb` type and MUST require a MongoDB URI, database name, and collection name. Invalid or missing configuration MUST fail during output construction with an ArkFlow configuration error.

#### Scenario: Valid destination configuration

- **WHEN** an output is configured with `type: mongodb`, `uri`, `database`, and `collection`
- **THEN** the builder creates a MongoDB output without connecting yet

#### Scenario: Missing destination configuration

- **WHEN** one of `uri`, `database`, or `collection` is missing or empty
- **THEN** output construction fails with a configuration error

#### Scenario: Unsupported codec

- **WHEN** a codec is configured on a MongoDB output
- **THEN** output construction fails with a configuration error explaining that structured BSON output does not support codecs

### Requirement: MongoDB connection lifecycle

The output MUST create its MongoDB client and target collection during `connect`, retain the handle for writes, and release it during `close`. Connection and close failures MUST be returned as ArkFlow errors.

#### Scenario: Connect to destination

- **WHEN** `connect` is called with a valid reachable MongoDB URI
- **THEN** the output stores a usable collection handle for the configured database and collection

#### Scenario: Write before connect

- **WHEN** `write` is called before a successful `connect`
- **THEN** the output returns a disconnection/process error and does not acknowledge the batch

### Requirement: Batch rows become BSON documents

For every input row, the output MUST create one BSON document whose fields use the Arrow column names. It MUST preserve non-null UTF-8 strings, signed and unsigned integers, floating-point values, booleans, binary values, and nulls using their corresponding BSON representations. Unsupported Arrow types MUST fail with an error identifying the column.

#### Scenario: Convert scalar row

- **WHEN** a batch contains supported scalar columns and a row has non-null values
- **THEN** the corresponding BSON document contains the same field names and equivalent BSON values

#### Scenario: Preserve null

- **WHEN** a supported column is null for a row
- **THEN** the document contains that field with BSON null

#### Scenario: Reject unsupported type

- **WHEN** a row contains an Arrow type not supported by the output
- **THEN** conversion fails before insertion and identifies the offending field and type

### Requirement: Batch insertion and error propagation

Each successful `write` MUST insert all documents from the input `MessageBatch` with one MongoDB bulk insertion operation. The output MUST classify network/lifecycle driver errors as ArkFlow connection errors, authentication failures as authentication errors, and other insertion failures as process errors. It MUST not report success when insertion fails. Empty batches MUST complete successfully without issuing an insertion.

#### Scenario: Insert non-empty batch

- **WHEN** a connected output receives a batch with one or more rows
- **THEN** it calls MongoDB bulk insertion with one document per row and returns success after the operation succeeds

#### Scenario: MongoDB insertion fails

- **WHEN** MongoDB rejects the bulk insertion
- **THEN** `write` returns the appropriate ArkFlow error category containing the driver failure

#### Scenario: Insert empty batch

- **WHEN** a connected output receives a batch with zero rows
- **THEN** `write` returns success without calling MongoDB

