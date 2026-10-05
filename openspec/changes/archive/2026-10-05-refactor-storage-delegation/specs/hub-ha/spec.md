## ADDED Requirements

### Requirement: Storage backends SHALL carry a schema version and reject newer databases

每个存储后端（SQLite 与 PostgreSQL）SHALL 在启动 DDL 应用后校验并维护 schema 版本戳（SQLite `PRAGMA user_version`、PostgreSQL `cp_schema_meta` 表的 `schema_version` 键）：数据库版本新于二进制支持的版本时 SHALL 以显式配置错误拒绝启动（错误信息包含两侧版本号），防止旧进程静默操作新 schema；版本等于二进制版本或未盖章（新库）时 SHALL 盖章为二进制版本并继续启动。`arkflow-server migrate` SHALL 校验源库版本不新于二进制版本，并在复制完成后将目标库版本戳设为二进制版本。

#### Scenario: 新库首次启动盖章

- **WHEN** 后端在无版本戳的数据库上初始化
- **THEN** DDL 应用后版本戳被写为二进制支持的 schema 版本，启动成功

#### Scenario: 库版本新于二进制拒绝启动

- **WHEN** 数据库携带的 schema 版本大于二进制支持版本
- **THEN** 初始化以显式错误失败，错误信息同时指明数据库版本与二进制版本，进程不进入运行状态

#### Scenario: 等版本重复启动幂等

- **WHEN** 后端在版本戳等于二进制版本的数据库上重复初始化
- **THEN** 校验通过且不改变版本戳，启动成功

#### Scenario: migrate 校验并转移版本

- **WHEN** `arkflow-server migrate` 从源库复制到目标库
- **THEN** 源库版本新于二进制时迁移以显式错误失败；否则复制完成后目标库版本戳等于二进制支持的 schema 版本
