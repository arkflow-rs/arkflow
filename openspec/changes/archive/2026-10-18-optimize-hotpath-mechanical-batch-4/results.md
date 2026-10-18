# Results — optimize-hotpath-mechanical-batch-4

## 1. Avro L2：批内 reader 复用 ✅

### 隔离 A/B（`avro_reader_reuse_timing`，release，20 万消息/臂 ×3 取最优）

`cargo test --release -p arkflow-plugin --lib -- --ignored avro_reader_reuse_timing --nocapture`

| 宽度 | 每消息构建（现状路径） | 批内复用 | 提升 |
| --- | --- | --- | --- |
| w5 | 1,753,013 rows/s | 2,484,436 rows/s | **+41.7%** |
| w25 | 483,411 rows/s | 545,079 rows/s | **+12.8%** |
| w100 | 125,372 rows/s | 133,769 rows/s | **+6.7%** |

reader 构建（`ResolvedSchema` 名字表）在窄 schema 上占解码成本近三成；宽 schema 收敛到解码本身。

### 官方套件端到端（`cargo run -p arkflow --release --example benchmark`，best of 3，同机）

| 场景 | PLANNING 10.1 基线（2026-10-07） | 本次（2026-10-18） | 变化 |
| --- | --- | --- | --- |
| avro-decode-w5 | 1.05M rows/s | 1,712,571 rows/s | **+63%** |
| avro-decode-w25 | 396K rows/s | 441,785 rows/s | **+11.6%** |
| avro-decode-w100 | 109K rows/s | 115,553 rows/s | **+6.0%** |

端到端增幅高于隔离 A/B，来源是本 change 同时消除了每消息的 `resolve_cached` DashMap 查找（schema 解析从每消息一次摊销到每 id 一次）。基线为 11 天前同机记录，跨日运行存在正常波动；隔离 A/B 是严格同进程对照，为严谨口径。SQL 三场景与 codec-json 同轮数值在基线噪声带内（路径未触碰）。

### 语义门槛

`cargo test -p arkflow-plugin --lib`：852/852 通过，含两个错误顺序钉死测试（`fetch_error_precedes_later_bad_wire_header`、`payload_decode_error_precedes_schema_shape_error`）、交织 id 首现分组、单 id 列式累积、多版本并集/类型冲突、缓存命中、兼容性门禁。输出与错误路径逐字节等价。

## 2. event_time_gate 混合组拷贝：结案不做 ✅

`event_time_gate_group_copy_timing`（release，1024 行/批 × 2000 轮 ×3 取最优；修正首版场景混淆——恒定 anchor 防 watermark 棘轮、正确行计数）：

| 臂 | 吞吐 | 换算 |
| --- | --- | --- |
| held → uniform Emit（1 组，all-true 零拷贝） | 6,355,722 rows/s | ≈161µs/轮（1024 行） |
| fresh Hold+Drop 混合（2 组，部分掩码） | 2,271,318 rows/s | ≈451µs/轮（2048 行） |
| 裸 filter all-true（零拷贝路径） | 6.94G rows/s | **0.15µs/批** |
| 裸 filter 2 组半掩码 × 2048 行源（真实分拆形状） | 211.0M rows/s | **9.7µs/轮（两次全量掩码过滤）** |

判定：2 组分拆的**实测**总拷贝 = 9.7µs/2048 行 ≈ **4.7ns/行**——占混合臂自身开销（~441ns/行）约 2.2%，占端到端基线（linear-sql ≈1.48µs/行）**≈0.32%**。混合臂相对单组臂的其余差距是分组簿记与迟到路径分类的固有成本，不是拷贝。唯一逃逸（连续段 `slice()` 零拷贝）只覆盖前缀/后缀分界形态，为 ~0.3% 增加掩码连续性检测与分支不成立。**结案：不做，证据留档。**

> 测量口径修正（coderabbit 复审指出）：首版裸 filter 臂用 1,024 行源 + 交替掩码单次过滤外推"5.4µs/2.6ns/行"，与混合臂的真实形状（**2048 行源 × 每组一次全量掩码过滤，共两次**）不符；已把对照臂改为与真实分拆同形状的 `filter_record_batch(2·rows 源, Hold 掩码) + filter_record_batch(2·rows 源, Drop 掩码)` 并重测，上表与本文数字均为修正后实测。结论不变。

## 3. 验证清单

- [x] `cargo test -p arkflow-plugin --lib` 852/852（含 schema_registry 37/37）
- [x] `cargo test -p arkflow-core --lib` event_time_gate 40/40（计时测试 `#[ignore]` 不进 CI）
- [x] `cargo test --workspace --all-targets` 全量通过（`redis_cluster` 首轮失败为 AGENTS.md 已知环境问题，清理残留容器后 2/2）
- [x] `cargo clippy --workspace --all-targets` 0 告警
- [x] `openspec validate` 通过
