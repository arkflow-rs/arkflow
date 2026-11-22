## 1. 共享基建（纯移动 + 扩展，分 commit）

- [x] 1.1 新建 `crates/arkflow-plugin/src/retry.rs`：`RetryUtils`/`RetryConfig`/`validate_retry_config` 自 `pulsar/common.rs:94-170` 迁入（纯移动 commit）；随后扩展：退避改 `saturating_pow` + `max_delay_ms` 封顶、结构体增 `jitter`（默认 0）、新增 `is_retryable_status`；单测（分类表、attempt=64 封顶无 panic、jitter 边界）
- [x] 1.2 新建 `crates/arkflow-plugin/src/http_util.rs`：`build_http_client`（自 `vector_util.rs:208`）、`truncate_body`（自 :198）迁入 + 新增 `join_url_base`/`apply_headers`；`vector_util.rs` 收缩为向量专属并更新全部引用点（vector_search/milvus_search/qdrant/milvus output/pgvector 系）
- [x] 1.3 消灭逐字符副本：`embedding.rs`（truncate :383-387、client :260-275）、`llm.rs`（truncate :486-491、client :397-411、append_column 保持本地——错误前缀不同，评估后收编或留）、`lookup.rs`（truncate :967-971、client :930-947）、`output/milvus.rs`（client :441-454）、4 处 URL 拼接、8 处 bearer 头循环 → 共享实现；纯搬运 commit，行为零变化

## 2. 通道容量对齐

- [x] 2.1 plugin crate 新增 `DELIVERY_CHANNEL_CAPACITY = 1024` 常量（注释指向内核 `executor/graph.rs`）；替换 7 处 `bounded(1000)`：`input/mqtt.rs:83`、`nats.rs:105`、`websocket.rs:77`、`pulsar.rs:114`、`redis.rs:185,186`、`http.rs:111`

## 3. 重试采用方

- [x] 3.1 `output/qdrant.rs:180-228`：复制循环 → 共享 `RetryUtils`（默认 retry_count=3 不变；429/5xx/传输重试、4xx 立即失败语义不变）；既有 MockApi 场景测试全绿
- [x] 3.2 `output/milvus.rs:183-260`：同 3.1（默认 0 不变）；溢出场景测试：`retry_count: 64` 无 panic
- [x] 3.3 `input/pulsar.rs:223-334`：手写循环 → 共享 `RetryUtils`（cancellation 分支保留，`input-cancellation-safety` 测试不回退）；`RetryConfig` 生效测试（配置 delay 实际作用于重试间隔）；重试耗尽→`Error::Disconnection`
- [x] 3.4 四个 AI 处理器新增可选 `retry_count`（默认 3，0=关）：`embedding.rs`、`llm.rs`、`vector_search.rs`、`milvus_search.rs` 接入共享重试；各加 429 重试用例 + 401 立即失败用例 + `retry_count: 0` 直通用例

## 4. 卫生钉扎

- [x] 4.1 卫生测试（与 2.1 同文件或独立）：扫描 `src/input/` 禁止 `bounded(1000)` 与裸容量魔数；扫描 `src/output/` 禁止裸 `2u64.pow` 退避计算
- [x] 4.2 组件 schema 再生成：`ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`（4 个处理器新增 `retry_count` 字段进 inventory/config-schema）

## 5. 验证

- [x] 5.1 `cargo test --workspace --all-targets --locked` 全绿；`cargo clippy --workspace --all-targets --locked -D warnings` 干净（先 `rustup update stable` 对齐 CI 工具链）
- [x] 5.2 CHANGELOG：三条行为变化（AI 处理器默认重试 3 次、pulsar RetryConfig 生效、delivery 通道容量 1000→1024）

## 6. 文档（EN+zh 锁步）

- [x] 6.1 四个 AI 处理器组件页补 `retry_count` 字段与重试语义；qdrant/milvus output 页注明重试经统一实现（用户可见行为不变）；pulsar input 页注明 RetryConfig 生效
- [x] 6.2 `pnpm docs:check` 通过；示例 YAML 若新增字段示例则登记 `example-manifest.json`（可选字段不改既有示例也合规，确认即可）
