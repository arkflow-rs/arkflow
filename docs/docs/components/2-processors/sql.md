# SQL

The SQL processor component allows you to process data using SQL queries. It uses DataFusion as the query engine to execute SQL statements on the data.

Reference to [SQL](../../category/sql).

## Configuration

### **query**

The SQL query statement to execute on the data.

type: `string`

### **table_name**

The table name to use in SQL queries. This is the name that will be used to reference the data in your SQL queries.

type: `string`

default: `flow`

### **ballista (experimental)**

Optional configuration for distributed computing using Ballista. When configured, SQL queries will be executed in a distributed manner.

type: `object`

required: `false`

properties:
- `remote_url`: Ballista server URL (e.g., "df://localhost:50050")

  type: `string`

  required: `true`

### **temporary_list**

Optional list of temporary data sources that can be referenced in SQL queries. Each temporary source allows you to access external data during query execution.

type: `array`

required: `false`

properties:
- `name`: Name of the temporary data source to reference

  type: `string`

  required: `true`

- `table_name`: Table name to use for this temporary data in SQL queries

  type: `string`

  required: `true`

- `key`: Key expression or value used to retrieve data from the temporary source

  type: `object`

  required: `true`

  properties:
  - `expr`: Expression string to evaluate for the key
  
    type: `string`
    
    required: `false`
    
  - `value`: Static string value to use as the key
  
    type: `string`
    
    required: `false`


## Examples

### Basic SQL Query

```yaml
- processor:
    type: "sql"
    query: "SELECT id, name, age FROM users WHERE age > 18"
    table_name: "users"
```

### SQL Query with Temporary Data Sources

```yaml
- temporary:
    - name: user_profiles
      type: "redis"
      mode:
        type: single
        url: redis://127.0.0.1:6379
      redis_type:
        type: string

  processor:
    type: "sql"
    query: "SELECT u.id, u.name, p.title FROM users u JOIN profiles p ON u.id = p.user_id"
    table_name: "users"
    temporary_list:
      - name: "user_profiles"
        table_name: "profiles"
        key:
          expr: "user_id"
```

### SQL Query with Ballista (Distributed Computing)

```yaml
- processor:
    type: "sql"
    query: "SELECT COUNT(*) as total FROM large_dataset"
    table_name: "large_dataset"
    ballista:
      remote_url: "df://localhost:50050"
```
