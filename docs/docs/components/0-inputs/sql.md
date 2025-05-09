# SQL

The SQL input component allows you to query data from various input sources using SQL. It supports both local file-based data sources and database connections, with optional distributed computing capabilities through Ballista.

Reference to [SQL](../../category/sql).
## Configuration

### **select_sql**

The SQL query statement to execute. This query will be applied to the data source specified in `input_type`.

type: `string`

required: `true`

### **ballista (experimental)**

Optional configuration for distributed computing using Ballista. When configured, SQL queries will be executed in a distributed manner.

type: `object`

required: `false`

properties:
- `remote_url`: Ballista server URL (e.g., "df://localhost:50050")
  
  type: `string`

  required: `true`

### **input_type**

Specifies the type and configuration of the input source to query from.

type: `object`

required: `true`

The configuration varies based on the input type selected. Available input types are detailed below.

## Input Type Configurations

### **Avro**
- `table_name`: Optional table name used in SQL queries (defaults to "flow")
  
  type: `string`

  required: `false`
- `path`: Path to Avro file
  
  type: `string`

  required: `true`

### **Arrow**
- `table_name`: Optional table name used in SQL queries (defaults to "flow")
  
  type: `string`

  required: `false`
- `path`: Path to Arrow file
  
  type: `string`

  required: `true`

### **Json**
- `table_name`: Optional table name used in SQL queries (defaults to "flow")
  
  type: `string`

  required: `false`
- `path`: Path to JSON file
  
  type: `string`

  required: `true`

### **Csv**
- `table_name`: Optional table name used in SQL queries (defaults to "flow")
  
  type: `string`

  required: `false`
- `path`: Path to CSV file
  
  type: `string`

  required: `true`

### **Parquet**
- `table_name`: Optional table name used in SQL queries (defaults to "flow")
  
  type: `string`

  required: `false`

- `path`: Path to Parquet file
  
  type: `string`

  required: `true`

### **Mysql**
- `name`: Optional connection name (defaults to "flow")
  
  type: `string`

  required: `false`

- `uri`: MySQL connection URI
  
  type: `string`

  required: `true`

- `ssl`:
  - `ssl_mode`: SSL mode for connection security
    
    type: `string`

    required: `true`

  - `root_cert`: Optional root certificate path
    
    type: `string`

    required: `false`

### **DuckDB**
- `name`: Optional connection name (defaults to "flow")
  
  type: `string`

  required: `false`

- `path`: Path to DuckDB file
  
  type: `string`

  required: `true`

### **Postgres**
- `name`: Optional connection name (defaults to "flow")
  
  type: `string`

  required: `false`

- `uri`: PostgreSQL connection URI
  
  type: `string`

  required: `true`

- `ssl`:
  - `ssl_mode`: SSL mode for connection security
    
    type: `string`

    required: `true`

  - `root_cert`: Optional root certificate path
    
    type: `string`

    required: `false`

### **Sqlite**
- `name`: Optional connection name (defaults to "flow")
  
  type: `string`

  required: `false`

- `path`: Path to SQLite file
  
  type: `string`

  required: `true`

## Examples

### Basic MySQL Connection
```yaml
input:
  type: "sql"
  select_sql: "SELECT * FROM flow"
  input_type:
    type: "mysql"
    name: "my_mysql"
    uri: "mysql://user:password@localhost:3306/db"
    ssl:
      ssl_mode: "verify_identity"
      root_cert: "/path/to/cert.pem"
```

### Distributed Query with Ballista
```yaml
input:
  type: "sql"
  select_sql: "SELECT * FROM flow where id > 1000"
  ballista:
    remote_url: "df://localhost:50050"
  input_type:
    type: "parquet"
    table_name: "parquet_table"
    path: "/path/to/data.parquet"
```