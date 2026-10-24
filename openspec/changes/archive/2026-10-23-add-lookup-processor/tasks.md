## 1. 配置与后端契约

- [x] 1.1 新建 `crates/arkflow-plugin/src/processor/lookup.rs`：`LookupConfig`（`key_columns`/`backend`/`columns`/`ttl_ms`/`negative_ttl_ms`/`max_entries`/`concurrency`/`timeout_ms`/`on_missing`/`on_backend_error`）+ serde 反序列化与 `parse_config` 接入
- [x] 1.2 定义 `LookupBackend` trait（`async fn lookup(&self, keys: &[String]) -> Result<Vec<Option<Value>>>`）与构建期分发；未知 backend kind 显式报错（单测断言）
- [x] 1.3 构建期校验：缺 `url`/空 `columns`/key 列类型不属于 {Utf8, Int64, UInt64, Boolean}（报列名+类型）/浮点 key 拒绝——各一个失败单测

## 2. 缓存

- [x] 2.1 实现 TTL + `max_entries` 近似 LRU 缓存（`Arc<str>` 键，值含命中/负缓存标记），过期惰性清除 + 超界先清过期再逐出队首
- [x] 2.2 缓存单测：TTL 内不重复解析、负缓存在 `negative_ttl_ms` 内生效、过期后重新解析、distinct-key 洪泛下条目数 ≤ `max_entries`

## 3. HTTP 后端

- [x] 3.1 实现 `{{key}}` URL 模板替换（默认 percent-encode，`encode_key: false` 关闭）、method/headers 注入、`timeout_ms`
- [x] 3.2 实现 2xx JSON 解析 + `columns` 的 dot-path 提取与类型化（utf8/int64/float64/bool；缺 path 或不可 coerce → null）；缓存值 = 映射后片段而非原始响应
- [x] 3.3 wiremock 集成测试：命中映射行对齐、非 2xx 判后端错误、类型不符出 null 不报错、key 含 `/`/`?` 被编码

## 4. Processor 核心

- [x] 4.1 key 提取与规范化（整数/布尔 → 字符串、多列 U+001F 复合键）；运行期 lookup 列与输入列重名报错（单测）
- [x] 4.2 批内 distinct key 去重 + `buffered(concurrency)` 保序收集 + key→行索引写回；重复 key 只发一次请求的断言测试
- [x] 4.3 列追加：保留全部输入列（含 `__meta_`）+ 按序追加 lookup 列；`on_missing: null|discard` 语义（discard 输出行数 < 输入且成功）
- [x] 4.4 `on_backend_error: fail|null` 语义：默认 Err 传播；`null` 降级为未命中且不写负缓存（单测断言缓存无该键条目）

## 5. 注册与组件元数据

- [x] 5.1 `register_processor_builder("lookup", ...)` + `ComponentMetadata::with_schema`（JSON Schema 覆盖全部配置字段与默认值）+ `processor/mod.rs` `init()` 接线
- [x] 5.2 `cargo test -p arkflow-plugin --test registry_consistency` 与 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 通过（生成物更新）

## 6. 文档义务（en + zh-Hans）

- [x] 6.1 `docs/docs/components/2-processors/lookup.md`（`components: [lookup]` front matter + 分类 fence 标记）+ zh-Hans 对应页
- [x] 6.2 `examples/` 新增 lookup 示例 YAML（wiremock 不可用的离线形态需 `validate: false` + 理由，或用可离线校验的配置形态）+ `docs/reference/example-manifest.json` 登记
- [x] 6.3 `README.md` 与 `README_zh.md` 的 `README_COMPONENTS:processor` 清单各加 `**Lookup** (`lookup`)` 条目

## 7. 收口验证

- [x] 7.1 `cargo test --workspace --all-targets` 全绿 + `cargo clippy --workspace --all-targets` 无新增告警（38 套件全过；`redis_cluster` 两度失败均为 AGENTS.md 已知环境问题——残留 testcontainers 容器占用 :16379，`docker rm -f` 后重跑通过；clippy 0 告警）
- [x] 7.2 `pnpm docs:check` 通过（组件归属、示例清单、内部链接、fence 分类）
- [x] 7.3 `./target/release/arkflow components list` 出现 `lookup` 且 `--validate` 校验示例 YAML 通过（以 debug 二进制等效验证：`components list` 已含 `lookup`，`--validate` exit=0；实跑验证：本地维表服务 + memory→json_to_arrow→lookup→stdout 全链路，3 行正确 enrich、重复键只发一次请求）
