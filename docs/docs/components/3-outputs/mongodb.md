---
description: ArkFlow MongoDB output component.
---

# MongoDB

The MongoDB output writes each Arrow row as a BSON document in a configured MongoDB collection. Column names become document field names, and scalar values and nulls are preserved.

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | Fixed value `"mongodb"`. |
| uri | string | yes | MongoDB connection URI, including authentication/options when needed. |
| database | string | yes | Destination database name. |
| collection | string | yes | Destination collection name. |

The output supports UTF-8 strings, signed and unsigned integers that fit BSON int64, floating-point values, booleans, binary values, and nulls. Nested Arrow values and codecs are not supported in this initial version.

## Example

```yaml
output:
  type: "mongodb"
  uri: "mongodb://localhost:27017"
  database: "arkflow"
  collection: "events"
```

The output connects and pings MongoDB before accepting writes. Each non-empty message batch is sent with one bulk insertion operation.
