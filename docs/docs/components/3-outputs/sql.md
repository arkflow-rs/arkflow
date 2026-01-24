# SQL

The SQL output component writes messages to SQL databases. It supports batch inserts, UPSERT operations, and connection pooling.

## Configuration

### **type**

Database type.

Supported values: `mysql`, `postgresql`, `sqlite`

type: `string`

optional: `false`

### **dsn**

Data Source Name (connection string).

type: `string`

optional: `false`

#### MySQL DSN Format
```
username:password@tcp(host:port)/database
```

Example: `user:pass@tcp(localhost:3306)/mydb`

#### PostgreSQL DSN Format
```
postgresql://username:password@host:port/database
```

Example: `postgresql://user:pass@localhost:5432/mydb`

#### SQLite DSN Format
```
path/to/database.db
```

Example: `/data/app.db`

### **table**

Target table name.

type: `string`

optional: `false`

### **batch_size** (optional)

Number of rows to batch before inserting.

type: `integer`

default: `100`

optional: `true`

### **primary_key** (optional)

Column name(s) to use for UPSERT operations. When specified, performs INSERT ... ON CONFLICT/DUPLICATE KEY UPDATE.

type: `string` or `array` of `string`

optional: `true`

## Examples

### MySQL Batch Insert

```yaml
- output:
    type: "sql"
    type: "mysql"
    dsn: "user:password@tcp(mysql-server:3306)/analytics"
    table: "events"
    batch_size: 500
```

### PostgreSQL with UPSERT

```yaml
- output:
    type: "sql"
    type: "postgresql"
    dsn: "postgresql://user:pass@localhost:5432/production"
    table: "metrics"
    primary_key: "id"
    batch_size: 1000
```

### SQLite Database

```yaml
- output:
    type: "sql"
    type: "sqlite"
    dsn: "/data/local.db"
    table: "sensor_readings"
    batch_size: 100
```

### PostgreSQL with Composite Primary Key

```yaml
- output:
    type: "sql"
    type: "postgresql"
    dsn: "postgresql://user:pass@postgres:5432/app"
    table: "daily_stats"
    primary_key: ["device_id", "date"]
    batch_size: 200
```

### High Throughput MySQL

```yaml
- output:
    type: "sql"
    type: "mysql"
    dsn: "write_user:write_pass@tcp(mysql-primary:3306)/high_traffic"
    table: "event_log"
    batch_size: 5000
```

## Features

- **Multiple Databases**: Support for MySQL, PostgreSQL, and SQLite
- **Batch Inserts**: Efficient batch operations for high throughput
- **UPSERT Support**: Insert or update based on primary key
- **Connection Pooling**: Automatic connection pooling for performance
- **Transaction Support**: Automatic transaction management
- **Composite Keys**: Support for multi-column primary keys

## Database-Specific Notes

### MySQL
- Uses `INSERT ... ON DUPLICATE KEY UPDATE` for UPSERT
- Requires at least one unique key or primary key
- DSN format: `username:password@protocol(host:port)/database`

### PostgreSQL
- Uses `INSERT ... ON CONFLICT DO UPDATE` for UPSERT
- Supports UPSERT on any column with unique constraint
- DSN format: `postgresql://username:password@host:port/database`

### SQLite
- Uses `INSERT OR REPLACE` for UPSERT
- Single file database
- No server required
- DSN format: path to database file

## Best Practices

1. **Use Appropriate Batch Sizes**: Larger batches (100-5000) improve throughput but increase memory usage
2. **Set Primary Keys for UPSERT**: Enable upsert functionality for idempotent writes
3. **Monitor Connection Pool**: Watch for connection exhaustion under high load
4. **Use Transactions**: Batches are automatically wrapped in transactions
5. **Handle Timeouts**: Configure appropriate timeout values in DSN
6. **Schema Design**: Ensure table schema matches your data structure
7. **Indexing**: Create indexes on frequently queried columns

## Error Handling

The output component will:
- Retry failed batch operations
- Log detailed error messages
- Continue processing on non-fatal errors
- Validate connection on startup

## Performance Tips

1. **Batch Size**: Start with 500-1000 and tune based on your workload
2. **Connection Pool**: The connection pool size is automatically managed
3. **Network Latency**: For remote databases, larger batches reduce round-trip overhead
4. **Database Tuning**: Configure database server for optimal write performance
5. **Disable Indexes**: For bulk loads, consider temporarily disabling non-critical indexes

## Example Schema

### MySQL Table Schema

```sql
CREATE TABLE events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    timestamp BIGINT NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    status VARCHAR(50),
    INDEX idx_device (device_id),
    INDEX idx_timestamp (timestamp)
);
```

### PostgreSQL Table Schema

```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    status VARCHAR(50)
);

CREATE INDEX idx_device ON events(device_id);
CREATE INDEX idx_timestamp ON events(timestamp);
```

### SQLite Table Schema

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    device_id TEXT NOT NULL,
    temperature REAL,
    humidity REAL,
    status TEXT
);

CREATE INDEX idx_device ON events(device_id);
CREATE INDEX idx_timestamp ON events(timestamp);
```
