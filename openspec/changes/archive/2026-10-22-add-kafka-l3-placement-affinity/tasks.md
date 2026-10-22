## 1. 核心机制（arkflow-core）

- [x] 1.1 `input/mod.rs`、`output/mod.rs`：`InputBuilder` / `OutputBuilder` 增加默认返回 `Ok(None)` 的 `placement_affinity(&self, config: &Option<serde_json::Value>) -> Result<Option<String>, Error>` 方法；新增模块级 `placement_affinity(type_name, config)` 解析入口——注册表查不到类型返回 `Ok(None)`，注册则调用钩子并传播其错误（含单测：默认 None、未注册类型 None、畸形配置报错）
- [x] 1.2 `job.rs`：`AffinityGroupSpec { key, operator_ids }` + `JobSpec::affinity_groups()`（对每个 source/sink 经钩子归并、BTreeMap 确定性排序；惰性纯函数，不入序列化面），含单测
- [x] 1.3 `job.rs`：`assignments_split` 轮转后增加亲和钉扎 pass（组锚 = `self.tasks` 顺序首个成员的轮转落位；重派组内全部成员算子的所有 subtask；TaskAttempt id 随 node 重生成），钉扎后先 `validate_affinity_assignments` 再既有旁路边校验；`assignments_for_nodes` 对 colocated 分支也追加亲和校验
- [x] 1.4 `job.rs`：公开 `validate_affinity_assignments(&[TaskAttempt])` / `validate_affinity_nodes(&BTreeMap<String,String>)`，错误指名亲和 key、两端任务与节点，缺 assignment fail-closed（与旁路边校验同风格）
- [x] 1.5 `executor/graph.rs`：`build_subgraph` 在 `validate_side_edge_nodes` 旁并列 `validate_affinity_nodes`
- [x] 1.6 core 单测（stub builder 注册实现 `placement_affinity`）：split 轮转后配对钉到 source 节点且中间算子仍在另一节点、同输入同节点列表结果逐字节一致、手工拆分 assignment 被拒（错误指名两端）、钉扎拆开旁路边时整份拒绝、colocated 不连通配对被拒、未注册类型不参与亲和

## 2. Kafka 插件（arkflow-plugin）

- [x] 2.1 `input/kafka.rs` builder：`transactional_offsets: true` → `Some("kafka-txn:{consumer_group}")`；`output/kafka.rs` builder：`offset_commit_group` 设置 → 同 key；各自单测（含 `exactly_once` 未开时不报亲和错误、配置解析失败传播）
- [x] 2.2 插件级放置测试：真实 Kafka builder + JobSpec（纯配置推导、无 broker）断言 `assignments_for_nodes` 双节点钉扎结果与 `validate_affinity_nodes` 拒绝拆分 map
- [x] 2.3 `kafka_txn.rs`：`validate_pairings` 与输出写入期错误文案追加分布式部署提示（分布式下该错误 = assignment 未经亲和校验）

## 3. 服务端验证（arkflow-server）

- [x] 3.1 hub 放置测试：两节点 split + L3 配对的 assignment 钉扎后通过全部放置校验；节点故障增量重放置（替换失败槽）后配对仍同节点、幸存节点 assignment 逐字节不变
- [x] 3.2 `build_subgraph` 双路径断言（落在 core `executor/tests.rs`，无需真 broker）：拆分 task map 以亲和错误拒绝；配对同节点时建图成功

## 4. 规范与文档

- [x] 4.1 核对三份 spec 增量与实现一致（`exactly-once-output`、`split-placement`、`distributed-job-runtime`），归档前 `openspec` 校验通过
- [x] 4.2 `docs/docs/build/exactly-once.md`：诚实边界改写——分布式 Job 由放置亲和共位、拆分即拒；`#### Scenario` 与既有 same-process 表述对齐
- [x] 4.3 `docs/docs/build/distributed-jobs.md`：split 放置小节补充亲和钉扎说明（含 yaml 片段按 fence 规则标注 validate 分类）
- [x] 4.4 zh-Hans `current` 树两页同步（`exactly-once.md`、`distributed-jobs.md`）
- [x] 4.5 `pnpm docs:check` + `cargo test -p arkflow --test docs_snippets_validate` 通过；如改动涉及 yaml 片段，确认 example-manifest 无需新增

## 5. 收尾门禁

- [x] 5.1 `cargo test --workspace --all-targets`、`cargo clippy --workspace --all-targets` 通过（含 redis_cluster 环境项跳过说明）
- [x] 5.2 `openspec validate add-kafka-l3-placement-affinity` 通过；PLANNING.md 第八节"剩余边界"更新（L3 跨进程 → 已由放置亲和闭环，跨节点协议仍 Non-goal）
