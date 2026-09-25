## Why

对 #1247 的代码审查在 P1/P2 之外还留有 7 项 P3 问题：三个 REST 向量后端把集合名未编码地拼进 URL 路径（`/`、`?`、`#`、空格等会产生错误请求）；`embedding` 处理器的分块请求严格串行（尾延迟无谓放大）；`pgvector_search` 对 id 列为 NULL 的行报出不透明的 sqlx 解码错误；`qdrant` 的 `random_uuid_v4` 有 8 空格缩进滑脱；基准套件静默丢弃插件初始化错误、在 async 工作线程上跑数千次同步 fsync 提交、用 `assert!`/`expect` 处理参数校验，示例入口对缺失参数值直接 panic。

P1（#1251）与 P2（#1252）已合入；本变更清点收尾剩余的低危缺陷。全部为实现层修复，无规格变更（既有规格均未约束这些行为；URL 对路径段做百分号编码不改变已文档化的请求形状——服务端解码后语义一致，修复的是含特殊字符的集合名此前根本无法正确请求）。

## What Changes

- `vector_util` 新增路径段百分号编码助手；`vector_search`（`/collections/{name}/points/search`）与 `qdrant`（`/collections/{name}/points`）的集合名经编码后拼入 URL（milvus 的集合名在请求体 `collectionName` 中，无需处理）。
- `embedding` 的分块请求以有界并发（复用 `concurrency` 配置，`buffered` + `try_collect`）流水化，行序保持不变。
- `pgvector_search` 的行解码改为容忍 NULL id：该行以指明列名与 SQL 的 `Error::Process` 报错，不再抛出不透明 sqlx 解码错误。
- `qdrant::random_uuid_v4` 缩进修整。
- `benchmark`：`setup_plugins` 传播首个初始化错误；`state_backend` 场景移入 `spawn_blocking`；`run_suite` 以 `Result` 校验参数（`runs == 0` / `count == 0` 返回 `Error::Config` 而非 panic）；示例入口对缺失参数值输出用法提示并以非零码退出。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（无——全部为实现层修复；`benchmark-suite` 规格未约束初始化错误处理、线程模型或参数校验方式，`vector-search`/`milvus-output` 等规格文档化的请求形状在百分号编码后保持不变。）

## Impact

- 代码：`crates/arkflow-plugin/src/{vector_util.rs,processor/vector_search.rs,output/qdrant.rs,processor/embedding.rs,processor/pgvector_search.rs,benchmark.rs}`、`crates/arkflow/examples/benchmark.rs` 及对应测试。
- 兼容性：无 API/配置/线格式变更；`run_suite` 的 panic 路径变为 `Err` 返回是行为改进（唯一调用方是示例入口与测试）。
- `secret.rs` 的同步 `std::fs` 读取维持 P2 变更中记录的已接受权衡（同步 API 面贯穿配置层，改造非手术式），不在本变更范围。

## Non-goals

- 不改动任何场景的测量口径或默认参数（`benchmark-suite` 规格的默认时长约束保持）。
- 不为 embedding/milvus 等新增配置项（embedding 并发复用既有 `concurrency`，默认值不变）。
