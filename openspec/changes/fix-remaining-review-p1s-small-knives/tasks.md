## 1. HTTP output 去锁（#13）

- [x] 1.1 `output/http.rs` send() 改 clone-out：短临界区内 clone `Client` 即释放锁，请求构建 + 发送 + 重试循环不再持有任何锁
- [x] 1.2 验证：`cargo check -p arkflow-plugin` 通过；可本机编译的 http output 测试通过（其余依赖 CI）

## 2. Kafka output flusher 生命周期（#14）

- [x] 2.1 `output/kafka.rs` flusher `tokio::spawn` 从 `new_with_claim` 移入 `connect()`（加幂等守卫，connect 语义不变）
- [x] 2.2 为 `KafkaOutput` 实现 `Drop` 调 `cancellation_token.cancel()`；钉扎测试：build→drop（未 close）后 token 为 cancelled，flusher 任务退出

## 3. embedding 维度一致性（#15）

- [x] 3.1 `processor/embedding.rs` `append_vector_column` 构建列前全量校验向量维度一致，不一致返回 `Error::Process` 并同时指名两个维度值
- [x] 3.2 钉扎测试：跨维度向量集合（如 128+64）报错不 panic；一致集合输出与既有行为逐字节一致

## 4. EXPR_CACHE 键根治（#19）

- [x] 4.1 `expr/mod.rs` 缓存键改长度前缀编码（每个字段的 name 与渲染 type 各带长度前缀，尾部接 `\u{0}{expr_str}`）
- [x] 4.2 钉扎测试：单字段 `x:Int64,y`(Utf8) 与两字段 `x`(Int64)+`y`(Utf8) 键不同且各自求值正确；含 `,`/`:` 列名批次路由结果与无缓存路径一致

## 5. Pulsar ca_file 移除（#20，BREAKING）

- [x] 5.1 `pulsar/common.rs` config struct 移除 `ca_file` 字段与相关测试；builder 在 raw `serde_json::Value` 上检测 `ca_file` 键存在即报错（信息指名驱动无自定义 CA API、指导移除）
- [x] 5.2 schema/inventory 快照同步（`arkflow components show`/`arkflow schema` 输出不含 `ca_file`）；测试断言含 `ca_file` 的配置构建报错、schema 无该字段
- [x] 5.3 docs 任务：移除 docs/docs/（EN）与 zh-Hans 对应文档页中的 `ca_file` 配置项及示例，CHANGELOG/发布说明标注 BREAKING

## 6. docs-check CI 门禁（#30）

- [x] 6.1 `.github/workflows/docs-check.yml` `paths` 过滤加入 `examples/**`；本地验证 YAML 语法与 diff

## 7. 收尾验证

- [x] 7.1 `cargo check --workspace` 通过；可本机编译的针对性测试全部通过（libduckdb 依赖模块的完整插件测试依赖 CI）
- [x] 7.2 `openspec validate --all --strict` 通过
