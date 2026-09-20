---
components: [input/sql]
sidebar_label: SQL
---

# SQL

SQL 输入(Input)通过 DataFusion 执行 `select_sql` 查询,从数据库(MySQL、PostgreSQL、SQLite、DuckDB)或文件格式读取数据。Ballista 为可选项,用于分布式查询。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"sql"` |
| select_sql | string | yes | — | SQL 查询语句 |
| input_type | object | yes | — | 数据源类型及其配置(带标签的枚举),见下表 |
| ballista | object | no | — | 分布式查询配置,见下表 |

### ballista

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| remote_url | string | yes | Ballista 服务器 URL |

### input_type

`input_type` 是一个带标签的枚举(tagged enum,通过 `type` 字段区分)。各变体如下。

#### mysql / postgres

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"mysql"` 或 `"postgres"` |
| uri | string | yes | 数据库连接 URI |
| name | string | no | 注册的表名(用于在查询中引用) |
| ssl | object | yes | SSL 配置,见下文 |

`ssl`:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| ssl_mode | string | yes | SSL 模式 |
| root_cert | string | no | 根证书路径 |

#### duckdb / sqlite

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"duckdb"` 或 `"sqlite"` |
| path | string | yes | 数据库文件路径 |
| name | string | no | 注册的表名 |

## 示例

```yaml validate=fragment wrap=input
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

```yaml validate=fragment wrap=input
input:
  type: "sql"
  select_sql: "SELECT * FROM flow where id > 1000"
  ballista:
    remote_url: "df://localhost:50050"
  input_type:
    type: "sqlite"
    path: "/path/to/data.db"
```
