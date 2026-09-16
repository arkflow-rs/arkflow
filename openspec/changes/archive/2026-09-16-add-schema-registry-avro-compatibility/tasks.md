## 1. 依赖与 resolver 分派

- [x] 1.1 `apache-avro` 加入 `[workspace.dependencies]` 与 arkflow-plugin；`cargo tree` 核对版本收敛（单一 0.22.0，datafusion 走 arrow-avro 无冲突）
- [x] 1.2 `RestSchemaResolver` 按响应 `schemaType` 返回带类型的 schema（PROTOBUF/AVRO，缺省 PROTOBUF，其余报错）；`FetchedSchema` 枚举与 wiremock 测试
- [x] 1.3 缓存条目改枚举 `CachedSchema`，decode 按类型分派；`message_type` 配置改可选（protobuf id 未配置时报错，含测试）

## 2. Avro→Arrow 映射

- [x] 2.1 新增 `codec/avro_arrow.rs`：基础类型 + `[null,T]` union 映射（Null/Boolean/Int32/Int64/Float32/Float64/Binary/Utf8/Enum→Utf8）
- [x] 2.2 逻辑类型映射：date/time-millis/time-micros/timestamp-millis/timestamp-micros（UTC）/local-timestamp（无时区）/decimal(p≤38)/uuid
- [x] 2.3 嵌套 record/array/map 与多分支 union 报错（含单测）
- [x] 2.4 端到端单测：apache-avro 编码 payload → wire format → decode → 断言列类型与值；多版本 Avro id 解码

## 3. 主题兼容性门禁

- [x] 3.1 配置 `subject`/`min_compatibility`（serde 校验枚举值），schema 元数据同步
- [x] 3.2 门禁实现：首次 decode 惰性 `GET /config/{subject}?defaultToGlobal=true`，秩比较（NONE=0<BACKWARD/FORWARD(+T)=1<FULL(+T)=2），`OnceCell` 缓存结果（含失败缓存）
- [x] 3.3 测试：通过/拒绝/transitive 变体/未配置不请求/结果缓存（InMemory resolver 计数断言）+ wiremock 测 config 端点

## 4. 文档与生成物

- [x] 4.1 更新 `docs/docs/components/5-codecs/schema-registry.md`（Avro 映射表、门禁、改写 Non-goals、message_type 可选）
- [x] 4.2 修正 `examples/howto_cdc_schema_registry.yaml` envelope 扁平化失实注释（并使 SQL 列与扁平映射一致）
- [x] 4.3 新增 `examples/schema_registry_avro.yaml` + manifest 注册；`ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema；`generate-component-inventory.mjs` 同步 .md；README 无 codec 清单引用（grep 核实，无需改）

## 5. 全量回归

- [x] 5.1 `cargo test --workspace --all-targets` 通过（唯一失败 `example_equivalence` 为既有偶发：stash 全部改动后在干净代码树上同样失败，已验证与本次无关）
- [x] 5.2 `cargo clippy --workspace --all-targets` 无新警告；`pnpm docs:check` 通过
