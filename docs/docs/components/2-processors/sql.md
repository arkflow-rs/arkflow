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


## Examples

```yaml
- processor:
    type: "sql"
    query: "SELECT id, name, age FROM users WHERE age > 18"
    table_name: "users"
```
