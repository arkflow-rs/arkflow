# SQL

The SQL input component allows you to query data from various input sources using SQL.

Reference to [SQL](../../category/sql).

## Configuration

### **select_sql**

The SQL query statement to execute.

type: `string`

### **input_type**

The type of input source to query from.

type: `enum`

options:
- `avro`
- `arrow`
- `json`
- `csv`
- `parquet`
- `mysql`
- `duckdb`
- `postgres`
- `sqlite`

## Input Type Configurations

### **Avro**
- `table_name`: Optional table name (used in SQL queries)
  
  type: `string`
- `path`: Path to Avro file
  
  type: `string`


### **Arrow**
- `table_name`: Optional table name (used in SQL queries)

  type: `string`
- `path`: Path to Arrow file

  type: `string`

### **Json**
- `table_name`: Optional table name (used in SQL queries)

  type: `string`
- `path`: Path to JSON file

  type: `string`

### **Csv**
- `table_name`: Optional table name (used in SQL queries)

  type: `string`
- `path`: Path to CSV file

  type: `string`

### **Parquet**
- `table_name`: Optional table name (used in SQL queries)

  type: `string`
- `path`: Path to Parquet file

  type: `string`

### **Mysql**
- `name`: Optional connection name

  type: `string`
- `uri`: MySQL connection URI

  type: `string`
- `ssl`:
  - `ssl_mode`: SSL mode

    type: `string`
  - `root_cert`: Optional root certificate path

    type: `string`

### **DuckDB**
- `name`: Optional connection name

  type: `string`
- `path`: Path to DuckDB file

  type: `string`

### **Postgres**
- `name`: Optional connection name

  type: `string`
- `uri`: PostgreSQL connection URI

  type: `string`
- `ssl`:
  - `ssl_mode`: SSL mode

    type: `string`
  - `root_cert`: Optional root certificate path

    type: `string`

### **Sqlite**
- `name`: Optional connection name

  type: `string`
- `path`: Path to SQLite file

  type: `string`

## Examples

```yaml
- input:
    type: "sql"
    select_sql: "SELECT * FROM table"
    input_type:
      mysql:
        name: "my_mysql"
        uri: "mysql://user:password@localhost:3306/db"
        ssl:
          ssl_mode: "verify_identity"
          root_cert: "/path/to/cert.pem"
```