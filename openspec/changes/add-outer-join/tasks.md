# add-outer-join — Tasks

## 1. 配置面:join_type

- [x] 1.1 `JoinOperatorConfig` 新增 `join_type` 字段(枚举 `inner|left_outer|right_outer|full_outer`,`#[serde(default)]` 缺省 inner),`crates/arkflow-core/src/executor/join.rs` + `job.rs` 校验路径透传;单测:缺省反序列化为 inner、四个合法值可解析、非法值报错
- [x] 1.2 确认含 join 的既有配置零漂移:未声明 `join_type` 时 `cargo test -p arkflow-core` join 相关既有测试全绿(逐位兼容验证)

## 2. outer 核心实现(join.rs)

- [x] 2.1 `BufferedRow` 增加 `matched` 标记,匹配成功路径对两侧参与行置位;单测:一次匹配后两侧行均置位
- [x] 2.2 `SideBuffer::evict` 改为返回被逐出行,`on_watermark` 按 `join_type` 将 outer 侧未匹配行组装为批次发射(inner 模式维持丢弃);单测:left_outer 未匹配行随 watermark 发射、inner 不发射
- [x] 2.3 已匹配抑制:淘汰时 `matched` 行不作为未匹配发射;单测:full_outer 下已匹配行淘汰后无输出
- [x] 2.4 `max_per_key` 容量逐出在 outer 侧同样发射(无 watermark 保证,at-least-once 双发语义);单测:容量逐出发射 + 其后匹配对照常发射
- [x] 2.5 未匹配行 null 装配(`new_null_array`)与输出 schema outer 侧强制 `nullable = true`(inner 输出 schema 不变);单测:left_outer 未匹配行 `r_*` 全 null 且 schema nullable、inner schema 无漂移
- [x] 2.6 `right_outer` / `full_outer` 打通(outer 侧集合驱动单/双侧发射);单测:full_outer 双侧未匹配各发射一次

## 3. 构建期侧别校验(graph.rs)

- [x] 3.1 图构建期校验:join 每侧声明的生产者恰好贡献一个通道,上游并行度 > 1 时构建期报错(错误文案指引「上游算子 parallelism 设为 1」),运行期 tag 校验保留;单测:多子任务上游构建失败、单子任务构建通过

## 4. 端到端与恢复验证

- [x] 4.1 executor 集成测试:真实链路(双源 + join_type=left_outer)watermark 推进后未匹配行产出,输出列结构符合 spec(对照 delta「left outer 未匹配行的 null 装配」场景)
- [x] 4.2 重放确定性测试:恢复重放窗口内,先左后右到达的匹配序列不产生「假未匹配」(对照 delta「outer 重放的未匹配确定性」场景)
- [ ] 4.3 `cargo test -p arkflow-core --all-targets` + `cargo test -p arkflow-plugin` 通过,`cargo clippy --workspace --all-targets` 无新增告警

## 5. 示例与文档

- [x] 5.1 新增 `examples/job_join_outer.yaml`(left_outer 场景)并注册 `docs/reference/example-manifest.json`;本地实跑验证未匹配行输出符合预期
- [x] 5.2 `docs/docs/` join/分布式 Job 相关页更新 `join_type` 说明(含 outer 语义、watermark 驱动前置、容量逐出双发契约),yaml 代码块带 validate 分类标记;zh-Hans 对应页同步
- [x] 5.3 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 与 `pnpm docs:check` 通过(join 非插件组件,核对 README 组件清单不受影响)

## 6. 收口

- [x] 6.1 `cargo test --workspace --all-targets` 全绿(与 CI 同口径)
- [ ] 6.2 对照 delta spec 逐场景核对实现与测试覆盖,准备归档(verify → archive)
