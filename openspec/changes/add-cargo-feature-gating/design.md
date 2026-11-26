## Context

登记簿 PA-P1-01（已复核）与 proposal 已确认：重依赖全部集中在 `arkflow-plugin`（`datafusion-table-providers`/Ballista/`spiceai_duckdb_fork`/vendored rdkafka/mongodb/pulsar/hdfs/PyO3），workspace 无任何 `[features]`。`arkflow-core` 只有 datafusion 本体（引擎必需，不门控）。组件注册收敛在 `arkflow-plugin` 的单一 `initialize()`（`src/lib.rs:121`），`arkflow` 二进制与 `arkflow-server` 都只依赖 `arkflow-plugin` + `arkflow-core`。现有 spec（`component-registry-export`、`ci-hygiene-gates` 等）描述的都是"每个构建"的行为，本 change 不改变其语义。

## Goals / Non-Goals

**Goals:**
- 默认构建零行为变化；`--no-default-features --features engine-minimal` 产出可编译、可运行的最小集。
- feature 边界按代码家族划分，`[features]` 自带注释说明覆盖的依赖与组件。
- CI 三组合矩阵（minimal / minimal+kafka / 全量）编译+对应测试。
- 缺失 feature 时给出可操作的错误提示。
- 生成物门禁固定在全量特性集下运行，产物确定性不受本地 feature 选择影响。

**Non-Goals:**
- 不翻默认值（默认全开；把默认切到最小集留待未来破坏性窗口）。
- 不做逐组件细粒度裁剪（mqtt/nats/redis/websocket/vrl/sql 输出等轻量族保持无条件编译）。
- 不动依赖版本与钉死结构（#1380 依赖路线图独立推进）。
- 不重构注册机制（只在 `initialize()` 调用点与 `pub mod` 声明加 `#[cfg]`）。

## Decisions

**D1 — 完整覆盖矩阵：全部 50 个注册组件逐一归属（14 feature + 无条件）**
对注册表（`docs/reference/component-inventory.json`，50 个）逐组件核对模块与依赖后划分。对登记簿清单的两处修正保留：`connectors-file` → **`connectors-sql`**（该家族是 SQL 输入/输出，与文件连接器无关）；**补 `connectors-pulsar`**（登记簿遗漏的重驱动）。此外登记簿未考虑、本次纳入的家族：mqtt/nats/redis/websocket/modbus 连接器、vrl 处理器、protobuf+schema_registry 编解码族、sql 输出（sqlx）。

| Feature | 门控组件 | 依赖（Cargo.toml 归属调整） |
| --- | --- | --- |
| `connectors-sql` | input/sql、output/sql | duckdb fork、ballista、datafusion-table-providers、sqlx（与 ai-vector 共享，optional + `dep:sqlx`） |
| `connectors-kafka` | input/kafka、output/kafka | rdkafka、rdkafka-sys、sasl2-sys |
| `connectors-python` | processor/python | pyo3、arrow-pyarrow（全仓仅 python.rs:18 使用） |
| `connectors-mongodb` | output/mongodb | mongodb |
| `connectors-pulsar` | input/pulsar、output/pulsar | pulsar |
| `connectors-hdfs` | input/file 的 hdfs scheme 分支 | hdfs-native-object-store |
| `connectors-mqtt` | input/mqtt、output/mqtt | rumqttc（含 mqtt_tls.rs） |
| `connectors-nats` | input/nats、output/nats | async-nats |
| `connectors-redis` | input/redis、output/redis、temporary/redis | redis（含共享 component/redis.rs） |
| `connectors-websocket` | input/websocket | tokio-tungstenite |
| `connectors-modbus` | input/modbus | tokio-modbus |
| `ai-vector` | processors: embedding/llm/vector_search/milvus_search/pgvector_search；outputs: qdrant/milvus/pgvector | 依赖多为 reqwest 系（裁组件面为主）；sqlx 经 pgvector 共享 |
| `processors-vrl` | processor/vrl | vrl（compiler+stdlib，编译重量级） |
| `codecs-protobuf` | codecs: protobuf/schema_registry；processors: arrow_to_protobuf/protobuf_to_arrow | prost-reflect、prost-types、protox（共享 component/protobuf.rs 同门） |

无条件保留（每项有归属理由）：**object_store**（input/file + **wal/s3.rs S3 WAL 后端**，核心持久化）；**datafusion / datafusion-functions-json**（引擎级 + processor/sql）；**apache-avro**（benchmark.rs:28 与 schema_registry 共用，纯 Rust 轻依赖，不随 codecs-protobuf 门控以免拖进 benchmark）；**reqwest 系**（input/output/http、output/influxdb）；**纯 std/core 组件**（processor/lookup 零外部依赖、batch、arrow_to_json、json_to_arrow，buffers 全部 4 个窗口/内存 buffer，codecs json/debezium_json，input generate/memory/multiple_inputs，output drop/stdout）。`engine-minimal` 下的最小面：file/generate/http/memory/multiple_inputs 输入、drop/http/influxdb/stdout 输出、lookup/batch/arrow_to_json/json_to_arrow/sql 处理器、全 buffer、json/debezium_json 编解码——真正的流式核心。

**D2 — `engine-minimal` 只能经 `--no-default-features` 使用（Cargo 可加性约束）**
feature 只能加不能减，不存在"关掉东西"的 feature。因此：`default = [14 个家族 feature 全集]`（全开，兼容），`engine-minimal` 是一个不启用任何家族的元 feature，**必须**与 `--no-default-features` 搭配：`cargo build --no-default-features --features engine-minimal`。备选方案"default=minimal + `full` 元 feature"被否——那会改变默认构建，违反本 change 非破坏性前提。三个 crate（plugin/二进制/server）定义同名 feature 并互相转发，使 workspace 级裸 `-F engine-minimal` 可用（无歧义）。共享依赖用 optional + `dep:` 处理：sqlx 由 `connectors-sql` 与 `ai-vector` 双特性启用，minimal 下两者皆关则 sqlx 整体退出依赖树。

**D3 — cfg 门的位置：模块声明 + 各 kind 的 init 调用点，不在组件代码内部撒 cfg**
`initialize()`（lib.rs:121）按 kind 分发（input/output/processor/... 各自 `init()`，如 input/mod.rs:70、output/mod.rs:71、processor/mod.rs:65），因此每个 gated 家族的门加在两处：`lib.rs` 的 `pub mod` 声明 + **对应 kind `mod.rs` 内的 init/register 调用行**。唯一需要模块内门的例外是 `input/file.rs` 的 hdfs scheme 分支（`connectors-hdfs` 门住 `hdfs_native_object_store` 的 scheme 处理）。跨家族共享模块随其唯一服务家族同门：`component/redis.rs` 随 `connectors-redis`、`component/protobuf.rs` 随 `codecs-protobuf`；`output/codec_helper.rs` 的 sqlx 引用实施时核实调用方——若仅 sql/pgvector 输出使用则随双特性门控，若有更广调用方则把 sql 相关 helper 拆到 `connectors-sql` 侧。依赖层同步收紧两处：`rdkafka-sys`/`sasl2-sys` 随 rdkafka 进 `connectors-kafka`；`arrow-pyarrow` 全仓仅 `processor/python.rs:18` 一处使用，随 PyO3 进 `connectors-python`（AGENTS.md 的"bump Arrow 勿破坏 arrow-pyarrow 桥"告警只适用于启用 python 的构建，门控后桥不编译即无风险）。另清理一处偶然耦合：`processor/vrl.rs:13` 的 `use duckdb::arrow::datatypes::FieldRef` 改为直接使用 arrow crate 的类型，否则 vrl 处理器会拖住 duckdb。

**D4 — 缺失 feature 的错误提示用一张无条件编译的静态表**
注册表只含已注册组件，gating 后 `kafka` 不在表里，报错只是 "unknown component type"，用户无从知道差一个 feature。在 plugin 内放一张 `const` 映射（组件类型名 → feature 名，几十行，全量构建也编译），未知组件错误路径查表追加提示：`unknown component type 'kafka' (requires the 'connectors-kafka' feature)`。备选"只在文档说明"被否——报错现场给答案才是 DX。

**D5 — 全量门禁绑定默认特性集，minimal job 只做 check/clippy/核心测试**
`examples_validate.rs`、`registry_consistency.rs`、`metadata_conformance.rs`、`docs_inventory_snapshot`、docs snippets 校验都断言完整注册面，在 minimal 构建下必然红。处置：CI 的 default job 跑全部现有测试目标（`--all-targets`）；minimal/kafka 两个矩阵 job 跑 `cargo check` + `cargo clippy` + core crate 测试，不编译 plugin tests。同时在这些门禁测试体内加一道自守卫：`cfg!(feature = "connectors-kafka")` 不为真时直接 panic 并提示"本测试必须在默认特性集下运行"——防止本地人用 minimal 特性集跑出误导性失败。

**D6 — 基线先行，裁剪目标以实测为准**
tasks 第一步先测：默认 vs minimal 的冷构建时间、release 二进制体积（strip 后）、`cargo tree` 包数。登记簿的 "minimal ≥40%" 是目标不是门（登记簿原文"先测基线再定目标"）；实测若不达标，先回头调 feature 边界（如把 vrl 或 sqlx 族纳入门控），不硬改目标。**预期校正：二进制内嵌 server 栈（jsonwebtoken/rsa、sqlx、rusqlite、prometheus、sysinfo），本 change 不门控它（见 proposal Non-goals），minimal 的体积降幅以插件侧贡献为主——基线表把"插件侧裁剪量"与"server 侧存量"分开列，降幅数字不因此失真。**`cargo tree` 断言（minimal 树中不含 rdkafka/pyo3/mongodb/pulsar/hdfs/duckdb/ballista）进 CI 作为结构性检查——它比体积数字稳定，不受构建环境影响。

## Risks / Trade-offs

- [cfg 门漏网：某处引用 gated 依赖但模块没门] → 三组合矩阵在 CI 兜底；实现时按家族逐个 `cargo check --no-default-features` 验证。
- [workspace feature 统一解析把 minimal 标志"漏"回全量] → 三个 crate 同名同集转发（D2），矩阵命令用裸 `-F` 全 workspace 统一；验证 `cargo tree -p arkflow-plugin -e features` 输出。
- [dev-dependencies/testcontainers 在 minimal 下编译失败] → minimal job 不编译 tests（D5）；tests 体内的组件引用靠自守卫提示。
- [生成物在非默认特性集下被重生成污染] → 门禁测试自守卫（D5）+ 生成脚本固定全量。
- [文档与 inventory 声称的组件集与实际构建不符] → inventory 描述的是全量构建，构建文档页明确列出"此特性集包含哪些组件"；`components list` 的输出天然按构建自洽。

## Migration Plan

纯增量：合并即生效，默认构建者无感。回滚 = revert PR。发布说明（CHANGELOG）列出新 feature 名与 `--no-default-features` 用法；无数据/配置迁移。

## Open Questions

- vrl（compiler+stdlib，编译重量级）与 sqlx 族是否在本次纳入门控——待基线实测后定（D6 的决策规则），不阻塞立项。
- server 侧能力门控（OIDC/PG 后端裁剪，jsonwebtoken→rsa 是 server 侧最大编译重量）是否立项——超出本 change 范围，若 minimal 基线不理想可作为后续独立 change 评估。
