---
components: [output/sql]
description: ArkFlow 文档页面。
---

# SQL

SQL 输出(Output)将记录批量插入 MySQL 或 PostgreSQL 数据库。每一行都从 Arrow 转换为带类型的 SQL 值,并通过单条参数化语句插入。可选的 upsert 模式将插入变为幂等(idempotent)写入(`ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE`)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"sql"` |
| output_type | object | yes | — | 数据库驱动与连接设置(见下文)。 |
| table_name | string | yes | — | 目标表名。 |
| upsert | boolean | no | `false` | 使用 upsert 而非普通插入。 |
| upsert_keys | string[] | yes (if `upsert`) | — | 用作 upsert 冲突目标的列。 |

### output_type

`output_type` 是一个带标签对象(由其 `type` 字段选择)。支持的驱动:`mysql` 与 `postgres`。

#### mysql

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `mysql`。 |
| uri | string | yes | — | MySQL 连接 URI(例如 `mysql://user:pass@host:3306/db`)。 |
| ssl | object | no | — | 可选的 SSL 配置(见下文)。 |

#### postgres

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `postgres`。 |
| uri | string | yes | — | PostgreSQL 连接 URI(例如 `postgres://user:pass@host:5432/db`)。 |
| ssl | object | no | — | 可选的 SSL 配置(见下文)。 |

### ssl

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| ssl_mode | string | yes | — | SSL 模式(`disable`、`prefer`、`require`、`verify_ca` 或 `verify_full`)。 |
| root_cert | string | no | — | 根 CA 证书路径。 |
| client_cert | string | no | — | 客户端证书路径。 |
| client_key | string | no | — | 客户端私钥路径。 |

## 示例

### MySQL

```yaml validate=fragment wrap=output
output:
  type: "sql"
  output_type:
    type: "mysql"
    uri: "mysql://user:password@mysql-server:3306/analytics"
  table_name: "events"
```

### PostgreSQL

```yaml validate=fragment wrap=output
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@localhost:5432/production"
  table_name: "metrics"
```

### 使用 SSL 的 PostgreSQL

```yaml validate=fragment wrap=output
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@postgres:5432/app"
    ssl:
      ssl_mode: "verify_full"
      root_cert: "/etc/ssl/certs/pg-root.crt"
      client_cert: "/etc/ssl/client.crt"
      client_key: "/etc/ssl/client.key"
  table_name: "daily_stats"
```

### PostgreSQL upsert

```yaml validate=fragment wrap=output
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@localhost:5432/production"
  table_name: "events"
  upsert: true
  upsert_keys: ["id"]
```

## Upsert 语义

设置 `upsert: true` 后,插入变为幂等写入:当某行的 `upsert_keys` 与现有行冲突时,更新非键列,而不是追加重复行。

- PostgreSQL: `INSERT ... ON CONFLICT ("id") DO UPDATE SET "col" = EXCLUDED."col", ...`
- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE \`col\` = VALUES(\`col\`), ...`

冲突检测依赖目标表的主键/唯一索引与 `upsert_keys` 匹配。如果每一列都是 upsert 键,则这些键自身被赋值(即空更新),以保证语句有效。正是这种吸收重复行的行为,使 SQL 输出适合在恢复后重放的精确一次(exactly-once)风格的流水线(Pipeline)。

## 注意事项

- 支持的列类型:Utf8、Int64、UInt64、Float64、Boolean。其他 Arrow 类型会被拒绝并报进程错误。
- 标识符引用遵循各自方言:MySQL 使用反引号,PostgreSQL 使用双引号。
- `upsert: true` 要求 `upsert_keys` 列表非空且无重复;违规会在配置加载时、流(Stream)启动之前被拒绝。
- `upsert_keys` 列必须存在于传入批次(Batch)模式中,否则写入失败并报错。
