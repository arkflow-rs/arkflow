# hub-ha 变更（Delta）

## ADDED Requirements

### Requirement: PostgreSQL 存储后端

`ARKFLOW_HUB_STORAGE` 以 `postgres://` 开头时，Hub SHALL 使用 PostgreSQL（sqlx PgPool）作为控制面存储；裸路径 SHALL 保持 SQLite 行为逐字节不变。两种 backend SHALL 共享同一命令面与 FIFO 顺序语义（actor 串行保持）；17 张 `cp_*` 表的 PG DDL SHALL 幂等且在启动时执行；连接失败 SHALL 使 Hub 启动快败。

#### Scenario: scheme 分派向后兼容

- **WHEN** `ARKFLOW_HUB_STORAGE` 分别为裸路径与 `postgres://` URL
- **THEN** 前者行为与现状一致，后者连接 PG 并执行幂等 DDL

#### Scenario: 两种 backend 通过同一契约套件

- **WHEN** 以同一命令序列分别驱动 SQLite 与 PG backend
- **THEN** 两者产出一致的存储状态与查询结果

### Requirement: SQLite 到 PostgreSQL 一次性迁移

SHALL 提供 `arkflow-server migrate --from sqlite:<path> --to postgres:<url>`：按依赖序逐表分块拷贝、重置 IDENTITY 序列、行数对账校验，不一致时非零退出。

#### Scenario: 迁移后行数对账一致

- **WHEN** 对含全部 17 表数据的 SQLite 库执行迁移
- **THEN** PG 端各表行数与源一致且抽样主键可查
