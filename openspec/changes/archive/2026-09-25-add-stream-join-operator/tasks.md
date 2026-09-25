# Tasks: add-stream-join-operator

## 1. 算子本体

- [x] 1.1 `executor/join.rs`：`JoinOperatorConfig` + `JoinOperator`（侧别路由、keyed interval 匹配、即时发射、watermark/容量双逐出、`l_*`/`r_*`/`join_key` 输出）。
- [x] 1.2 单测：匹配/窗口外/watermark 逐出/容量逐出/缺标记拒绝/扇出（7 个）。

## 2. 内核接线

- [x] 2.1 `Chain.tags_input_index` + 构造点默认值；join 链 `processor_parallelism = 1`。
- [x] 2.2 `graph.rs`：Join 算子构造（配置解析 + validate）。
- [x] 2.3 `task.rs`：`tag_input_index` 辅助 + join 链数据路径打标。
- [x] 2.4 `job.rs`：Join 放开 + 两入边元数校验 + 配置校验；测试更新（元数错误 + 合法双入边）。

## 3. 端到端与文案

- [x] 3.1 E2E 测试：双源 → join → sink 全链路（含链双输入与打标断言）。
- [x] 3.2 stream_compiler join buffer 报错指向 Job DAG join 算子（两处 + 测试）。

## 4. 文档与规格

- [x] 4.1 distributed-jobs（en/zh）join 章节改为「已支持 + 语义边界」。
- [x] 4.2 示例 YAML + example-manifest 注册。
- [x] 4.3 `cargo test -p arkflow-core` 全绿 + workspace 回归。
