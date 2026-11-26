## Why

全 workspace 没有任何 `[features]` 段（`grep -n "\[features\]" Cargo.toml crates/*/Cargo.toml` 零命中）：`crates/arkflow-plugin/Cargo.toml` 无条件携带全部重依赖——DataFusion 表提供器栈（`datafusion-table-providers` mysql/postgres/duckdb/sqlite，:27-32）、Ballista（:33）、`spiceai_duckdb_fork`（:34）、vendored rdkafka + SASL（:58-66）、mongodb（:79）、pulsar（:88）、hdfs-native-object-store（:98）、PyO3 auto-initialize（:105）。结果：Cargo.lock 超 1000 包、debug 二进制 748 MB、冷构建与 CVE 面都被用不到的组件抬高，无法交付"最小发行版"。登记簿 `PRODUCT-ITERATION-2026-11-22.md` PA-P1-01（已复核）将其列为迭代 2 首项。

## What Changes

- `arkflow-plugin` 引入 feature 层，按依赖家族裁剪（默认全开，行为不变），**覆盖全部 50 个注册组件**（完整归属矩阵见 design D1）：
  - 连接器驱动族（11）：`connectors-sql`（sql 输入 + sql 输出）、`connectors-kafka`、`connectors-python`、`connectors-mongodb`、`connectors-pulsar`、`connectors-hdfs`（file 输入 hdfs 分支）、`connectors-mqtt`、`connectors-nats`、`connectors-redis`（input/output/temporary）、`connectors-websocket`、`connectors-modbus`
  - 处理器/编解码族（3）：`ai-vector`（embedding/llm/vector_search/milvus_search/pgvector_search 处理器 + qdrant/milvus/pgvector 输出）、`processors-vrl`、`codecs-protobuf`（protobuf/schema_registry 编解码 + arrow_to_protobuf/protobuf_to_arrow 处理器）
  - `engine-minimal` — 仅核心引擎与轻量组件的元 feature（配合 `--no-default-features` 使用）
  - 无条件保留（各有归属理由）：object_store（file 输入 + **S3 WAL 后端**）、datafusion（引擎与 sql 处理器）、apache-avro（benchmark + schema_registry 共用，纯 Rust 轻依赖）、reqwest 系（http 输入输出/influxdb）、纯 std 组件（lookup、窗口 buffer、json/debezium 编解码等）
- 对登记簿清单的两处修正（design.md 详述理由）：`connectors-file` 更名 `connectors-sql`（该家族是 SQL 输入而非文件连接器）；补 `connectors-pulsar`（与 kafka/mongodb 同级的重驱动，清单遗漏）。
- `arkflow` 二进制与 `arkflow-server` 镜像同一 feature 集（默认转发全开）。
- 关闭未启用 feature 的组件时，未知组件错误提示所需 feature（如 `kafka` → "requires the `connectors-kafka` feature"）。
- CI 增加 feature 矩阵：`engine-minimal`、`engine-minimal+connectors-kafka`、默认全量，三者可编译并通过对应测试。
- 记录冷构建时间与 default/minimal 二进制体积基线（先测基线再定裁剪目标）；`[features]` 注释说明每个 feature 覆盖的依赖与组件。
- 生成物门禁（docs inventory、config schema、examples 深校验、registry/metadata conformance 测试）固定在默认全量特性集下运行，产物保持确定性。

## Non-goals

- 不翻默认值（默认全开；切最小集留待未来破坏性窗口）。
- **不做 server 侧 feature 门控**：`arkflow-server` 的 jsonwebtoken/rsa、sqlx、rusqlite、prometheus、sysinfo 等随二进制的 Hub/Agent 模式无条件保留——最小构建的体积裁剪只来自插件侧；server 能力门控（如 OIDC、PG 后端）另行立项。
- 裁剪粒度到**依赖家族**为止（14 个 feature），不做逐组件细粒度拆分（如 http 输入/输出、influxdb、lookup、窗口 buffer、json/debezium 编解码不单列 feature）。
- 不动依赖版本与钉死结构（#1380 依赖路线图独立推进）。
- 不重构注册机制。

### New Capabilities
- `build-feature-gating`: Cargo feature 裁剪层——feature 定义与默认全开兼容性、最小构建的注册表子集、缺失 feature 的可操作错误提示、CI feature 矩阵、生成物门禁与文档生成固定于全量构建。

### Modified Capabilities
<!-- 现有 spec（component-registry-export、ci-hygiene-gates 等）描述的都是"每个构建"的行为，gating 不改变其语义；生成物确定性与矩阵要求归入新 capability。 -->

## Impact

- **代码**：`crates/arkflow-plugin/Cargo.toml`（feature 定义 + 依赖按 feature 分组）、`src/lib.rs`/各 `mod.rs`/`input/file.rs` 的 `#[cfg(feature)]` 门（注册集中在单一 `initialize()`，lib.rs:121）；`crates/arkflow/Cargo.toml` 与 `crates/arkflow-server/Cargo.toml` 镜像转发。
- **CI**：feature 矩阵 job；现有全量测试目标（`examples_validate.rs`、`registry_consistency.rs`、`metadata_conformance.rs`、`docs_inventory_snapshot`、docs snippets 校验）绑定默认特性集。
- **文档**：构建/安装文档页新增 feature 说明（EN + zh-Hans 锁步）；CHANGELOG。
- **兼容性**：默认构建零行为变化（非 BREAKING）；`--no-default-features` 是新引入的可选路径。
