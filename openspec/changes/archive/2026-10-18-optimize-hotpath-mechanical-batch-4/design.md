# Design — optimize-hotpath-mechanical-batch-4

## 1. Avro 批内 reader 复用

### 1.1 为什么"上游阻塞"只阻塞了一半

原审计的三条约束（`decode_internal` 私有、reader 只收借用型 `ResolvedSchema`、`ResolvedOwnedSchema` 不可被 reader 消费）针对的是**跨批长生命周期 reader 缓存**：`GenericDatumReader<'s>` 的 `'s` 绑定 `&'s Schema`，要把 reader 存进与 schema 缓存并列的结构体，就会自引用（reader 借用紧挨着自己持有的 Arc），纯 safe Rust 无法表达。

但**批内作用域复用**绕开了这个问题：解码函数体内持有 `Arc<CachedSchema>` 的 map，整个解码期不可变；从 map 值里取出的 `&AvroSchema` 的生命周期覆盖整个 pass，`AvroValueReader<'a>` 按 id 存进另一个 map（只读），借用与所有权分离在两个局部变量上——不需要自引用结构、不需要 unsafe、不需要新依赖。关键事实：`GenericDatumReader::read_value` 接收 `&self`（apache-avro 0.22 reader/datum.rs:132），构造后可反复调用。

公开替代路径的排除：`avro::decode(schema, reader)`（decode.rs:72）内部每次 `ResolvedSchema::try_from(schema)`——公开 API 里没有任何"预构建名字表 + 解码"的组合，`decode_internal` 是 `pub(crate)`。

### 1.2 错误顺序是行为契约，不是实现细节

`schema_registry.rs` 测试里有两个 CR 时刻意加入的顺序钉死测试：

- `fetch_error_precedes_later_bad_wire_header`：消息 0 的 schema 缺失必须赢过消息 1 的坏 magic——"per-message 路径一贯的顺序"。
- `payload_decode_error_precedes_schema_shape_error`：同一条消息内，payload 解码失败必须先于 accumulator 的 schema 形状检查（accumulator 在首次成功 read 之后才构建）。

这排除了最简单的"按 id 分组解码"重构（pass 1 全量解析 wire format、pass 2 按 id 组解码）——它会把 wire 错误提到 fetch 错误之前、把同 id 的形状检查提到 payload 解码之前，两个测试都红。因此采用**两段式但顺序保真**的结构：

- **Pass 1（逐消息，可中止）**：`parse_wire_format(msg) → resolve_cached(id)`，与历史管线相同的顺序——消息 k 的 parse/fetch 失败先于消息 k+1 被触碰。schema `Arc` 按 id 暂存 `cached_by_id: HashMap<u32, Arc<CachedSchema>>`（同一 id 只 fetch 一次；缓存命中语义不变，只是重复 id 不再重复 DashMap 读）。
- **Pass 2（逐消息，reader 复用）**：按消息序遍历 staged `(id, payload)`；Avro 分支经 `readers: HashMap<u32, AvroValueReader<'_>>` 取 reader——Vacant 时构建（reader build 失败先于 decode，保持单消息内顺序），Occupied 直接复用。其后与历史代码逐字相同：首次遇到该 id 构建 accumulator（形状检查在成功 read 之后），`groups.iter_mut().find` 按首现顺序分组。

借用成立的关键：`cached_by_id` 在 pass 2 全程不可变，`readers` 的值借用它——两个 map 是独立的所有者，`&mut readers` 与 `&cached_by_id` 不相交，borrow checker 直接受。

### 1.3 收益边界

reader 构建（`ResolvedSchema::try_from`，全 schema 树走一遍建名字表）从"每消息"摊销到"每批每 id"。A/B 实测（`avro_reader_reuse_timing`，release，20 万消息/臂取三次最优）：

| 宽度 | 每消息构建（现状） | 批内复用 | 提升 |
| --- | --- | --- | --- |
| w5 | 1,753,013 rows/s | 2,484,436 rows/s | +41.7% |
| w25 | 483,411 rows/s | 545,079 rows/s | +12.8% |
| w100 | 125,372 rows/s | 133,769 rows/s | +6.7% |

窄 schema 收益最大（reader 构建占比高），宽 schema 收敛到解码本身的成本。端到端数字以官方 benchmark `avro-decode-w5/w25/w100` 前后对比为准（记入 results.md）。

## 2. event_time_gate 混合组拷贝：定夺不做

### 2.1 机制回顾

`collect_outcomes`（event_time_gate.rs:856）把一个批按行决策分成至多五个组，每组经 `filter_batch` → `filter_record_batch`（布尔掩码 gather）。全组批（全 true / 全 false 掩码）走 arrow-select 的 `IterationStrategy::All` 零拷贝快路径；**混合批每组部分掩码，走 gather**。

### 2.2 测量与判定

`event_time_gate_group_copy_timing`（release，1024 行/批 × 2000 轮取三次最优；裸 filter 臂为判定仪器）：

- 裸 `filter_record_batch` all-true：**0.15µs/批**（零拷贝路径，几乎免费）
- 裸 `filter_record_batch` 50% 掩码：**2.71µs/批**（gather 512 行）
- 2 组分拆总拷贝 ≈ 5.4µs/2048 行 ≈ **2.6ns/行**

对照内核端到端基线 linear-sql ≈1.48µs/行：混合组拷贝端到端占比 **<0.2%**。且这是理论下界——分拆必须物化每组（下游拥有自己的批），每行恰好拷贝一次；唯一逃逸是连续段 `slice()` 零拷贝，但它只在"批内决策连续"（前缀/后缀分界）时可用，混合场景本就稀疏（watermark 恰在批中间跨窗口边界、迟到行混入），为 <0.2% 加掩码连续性检测与 slice 分支不成立。结案：不做，证据留档。

（首版计时测试的场景设计有两个混淆——计数把 ack 组数当行数、anchor 逐轮递增使 watermark 棘轮让混合批 100 轮后退化全-Drop——已修正为恒定 anchor + 正确计数后再测，判定只依赖不受混淆影响的裸 filter 臂。）

## 3. 不做的事

- 跨批长生命周期 reader 缓存（需上游 owned-reader API，Non-goal）。
- gate 连续段 slice 化（收益 <0.2%，Non-goal）。
- Avro Arrow 映射 / L1 schema 缓存 / L3 列式累积 / Protobuf 路径（均已闭环，本 change 不触碰）。
