## 1. 基线测量（先测再裁）

- [x] 1.1 记录默认构建基线：`cargo build --release` 冷构建时间、strip 后二进制体积、`cargo tree | wc -l` 包数，存入 PR 描述
- [x] 1.2 记录 minimal 预估对照（落地后实测替代手工模拟）：`cargo build --release --no-default-features --features arkflow/engine-minimal`（feature 层落地前先在草稿分支手工注释依赖模拟），同口径三项数字

## 2. Feature 定义层

- [x] 2.1 `crates/arkflow-plugin/Cargo.toml`：定义 14 个家族 feature + `engine-minimal` + 全集 `default`，每个 feature 带注释（覆盖的依赖与组件）；依赖按 feature 分组搬运并 optional 化（`rdkafka-sys`/`sasl2-sys`→`connectors-kafka`；`arrow-pyarrow`→`connectors-python`；`rumqttc`→`connectors-mqtt`；`async-nats`→`connectors-nats`；`redis`→`connectors-redis`；`tokio-tungstenite`→`connectors-websocket`；`tokio-modbus`→`connectors-modbus`；`vrl`→`processors-vrl`；`prost-reflect`/`prost-types`/`protox`→`codecs-protobuf`；`sqlx` 双特性 `dep:` 绑定；`object_store`/`apache-avro`/`datafusion` 保持无条件——S3 WAL、benchmark 共用、引擎级）
- [x] 2.2 `crates/arkflow/Cargo.toml` 与 `crates/arkflow-server/Cargo.toml`：同名 feature 转发到 `arkflow-plugin/*`，default 全开
- [x] 2.3 验证 workspace 级裸 `-F` 无歧义：`cargo tree -p arkflow-plugin -e features` 在三种组合下输出符合预期

## 3. cfg 门机械落地（每家族完成后立即跑一次 `cargo check --workspace --no-default-features -F engine-minimal` 增量验证，不攒到最后）

- [x] 3.1 `connectors-sql`：门住 `input/sql.rs` 与 `output/sql.rs`；`processor/vrl.rs` 的 `duckdb::arrow` 引用改为 arrow crate 直用（解耦 duckdb）
- [x] 3.2 `connectors-kafka`：门住 kafka input/output + `kafka_security.rs`/`kafka_txn.rs`
- [x] 3.3 `connectors-python`：门住 `processor/python.rs`（arrow-pyarrow 随门，不编译即无桥风险）
- [x] 3.4 `connectors-mongodb`：门住 `output/mongodb.rs`
- [x] 3.5 `connectors-pulsar`：门住 `input/pulsar.rs`/`output/pulsar.rs`/`pulsar/` 模块
- [x] 3.6 `connectors-hdfs`：`input/file.rs` 内 hdfs scheme 分支加模块内门（其余 file/object_store 保持无条件）
- [x] 3.7 `connectors-mqtt`：门住 input/output mqtt + `mqtt_tls.rs`
- [ ] 3.8 `connectors-nats`：门住 input/output nats
- [ ] 3.9 `connectors-redis`：门住 input/output/temporary redis + 共享 `component/redis.rs`
- [ ] 3.10 `connectors-websocket`（input/websocket.rs）+ `connectors-modbus`（input/modbus.rs），两个单模块轻家族一并处理
- [ ] 3.11 `ai-vector`：门住 embedding/llm/vector_search/milvus_search/pgvector_search 处理器与 qdrant/milvus/pgvector 输出；核实 `output/codec_helper.rs` 的 sqlx 引用调用方归属（仅 sql/pgvector 用则随双特性门，否则把 sql helper 拆到 `connectors-sql` 侧）
- [ ] 3.12 `processors-vrl`：门住 `processor/vrl.rs`
- [ ] 3.13 `codecs-protobuf`：门住 protobuf/schema_registry 编解码 + arrow_to_protobuf/protobuf_to_arrow 处理器 + 共享 `component/protobuf.rs`

## 4. 缺失 feature 错误提示

- [x] 4.1 plugin 内加无条件编译的类型名→feature 名静态映射表（含单测：每个映射值是真实存在的 feature 名）
- [x] 4.2 未知组件错误路径接入查表：gated 类型报错带 `(requires the '<feature>' feature)`；纯未知类型保持原报错；补测试覆盖两类路径

## 5. 门禁自守卫与测试绑定

- [x] 5.1 `docs_inventory_snapshot`、`registry_consistency.rs`、`metadata_conformance.rs`、`examples_validate.rs`、docs snippets 测试体加自守卫：非默认特性集下 panic 并提示在默认特性集运行
- [x] 5.2 自守卫突变验证：用 minimal 特性集跑任一门禁测试，确认是提示性 panic 而非大面红

## 6. CI 矩阵

- [x] 6.1 CI 增加 feature 矩阵 job：`engine-minimal`、`engine-minimal,connectors-kafka`、默认全量；前两个跑 `cargo check --workspace` 与 `cargo clippy --workspace`（**均不带 `--all-targets`**，tests 引用 gated 组件、只在默认 job 编译）+ core crate 测试；默认跑 `--workspace --all-targets`
- [x] 6.2 minimal job 加 `cargo tree` 结构断言（不含 rdkafka/pyo3/mongodb/pulsar/hdfs/ballista/duckdb fork/rumqttc/async-nats/redis/tokio-tungstenite/tokio-modbus/vrl/prost-reflect/protox/sqlx）

## 7. 基线复测与达标判断

- [x] 7.1 三组合全量实测：冷构建时间、strip 后体积、包数，更新 PR 描述基线表
- [x] 7.2 若 minimal 降幅不足 40%：按 design D6 决策规则回头扩 feature 边界（vrl/sqlx 族候选）再测，或记录差距与理由，不硬改登记簿目标

基线实测（2026-11-25，本机 release + strip，冷构建含全部依赖编译）：

| 口径 | default | engine-minimal | 变化 |
| --- | --- | --- | --- |
| 冷构建时间 | 1289s | 848s | -34% |
| 二进制体积 | 151,098,832 B (144.1 MB) | 84,892,704 B (81.0 MB) | **-43.8%（达标 ≥40%）** |
| cargo tree 行数 | 3648 | 1789 | -51% |

server 栈（jsonwebtoken/rsa、sqlx、rusqlite、prometheus、sysinfo）按 Non-goals 保留在两侧行列中——-43.8% 已是含 server 存量的净值。

## 8. 文档与发布面

- [x] 8.1 构建文档页新增 feature 说明（feature 表 + `--no-default-features` 用法 + 各特性集包含的组件），EN 与 zh-Hans 锁步
- [x] 8.2 CHANGELOG [Unreleased] 新增条目（Added: feature gating；默认构建无变化说明）
- [x] 8.3 `pnpm docs:check` 绿（内部链接、sidebar 归属、yaml 块分类标记齐全）
- [x] 8.4 生成物核对：默认特性集下 inventory/schema 与已提交文件字节一致（组件注册面未变则无需重生成）

## 9. 全量门禁与评审

- [ ] 9.1 本地三组合矩阵全绿 + `cargo test --workspace --all-targets`（默认特性集）+ `cargo clippy --workspace --all-targets` + rustfmt 全绿
- [ ] 9.2 开 PR，跑 CodeRabbit 评审循环，逐条核实处置、线程清零（"Fixed in <sha>" 开头回复可自动解决）
- [ ] 9.3 CI 全绿（含三个镜像构建）后交用户合并
