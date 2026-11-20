## Context

锚点已就位（`add-hot-path-benchmark-anchors`）：`window-journal-n1/n100/n1k`（每档 ~7-10× 增长）、`sql-hot-cache`（26ms/批稳态 dev）、`json-decode`（349ms/千行批）+ `json-decode-w200`（3.7s，推断占比最高）。验收 = 锚点 commit 与本 change commit 的同机同命令 JSON 报告 diff。

已核实的关键事实：
- DataFusion 54.1.0 `SessionContext::state()` = `self.state.read().clone()`（整个 SessionState 深克隆）+ `mark_start_execution()`；`state_ref()` 为 Arc 版廉价替代（registry 源码）。
- 窗口 spec 既有先例：补偿快照已改 touched-slot 且写明 "per-batch snapshot cost SHALL NOT grow with the number of open windows"——本 change 把同一成本边界延伸到持久化路径。
- sql spec 既有语义：时间依赖查询（now/current_date/...）不参与 plan 复用、逐批重优化——缓存命中路径必然非时间依赖。

## Goals / Non-Goals

**Goals:**
- 三处每批固定成本消除：persist 只付 dirty 键代价；缓存命中零深克隆；固定 payload 的 JSON 解码跳过推断。
- 每项修复带钉扎测试（行为不变）+ 锚点对比（性能可验证）。

**Non-Goals:**
- fire 路径逐键 commit 合并、C 批 remote、SQL 连接池重构（见 proposal）。

## Decisions

**D1 #25：定向查找 + 条件扫描（不引入"移除登记表"重构）。**
- 整表克隆删除：dirty 键经 `buffers.get(key)` 定向取值（`current` BTreeMap 仅服务 put_compact 与 stale 过滤，二者都可定向化）。
- stale 扫描条件化：新增 `stale_scan_pending: bool`（窗口 operator 内部状态）。置位点：①`fire_ready` 移除缓冲（过期/lateness 删除）；②`max_buffered_keys` 驱逐；③`load_from_backend` 恢复后首个持久化（保守起见置位，尽管分析表明恢复后 current=全部后端条目、扫描本为空转——保留一个全量扫描的安全网，每次运行一次，代价可忽略）。清除点：扫描执行后。
- 不采用"移除登记表直接 journal-delete"方案：需要审计**所有**移除路径（fire/驱逐/回滚恢复）并处理回滚撤销登记，不变量手术面大；条件扫描保留原扫描机制做安全网，仅在无删除时跳过——收益（稳态批次零扫描）相同，风险小一个量级。
- 不变量保持：put_compact 仍按 dirty 事务提交；stale 条目仍在下一次持久化时清理；崩溃复活窗口窗口与现状一致（扫描本就清理不了"已加载"的条目）。

**D2 #26：`state()` 移入 miss / 时间依赖分支，不改执行语义。**
- 缓存命中路径（analyzed/optimized/physical 全命中且非时间依赖）：不取 state，直接 `collect(physical, ctx.task_ctx())`。
- miss 路径：照旧 `ctx.state()`（planning 需要）。时间依赖路径（`plan_folds_time`）：逐批重优化照旧取 state——`mark_start_execution()` 刷新的 execution start time 正是折叠时间戳保持新鲜的机制（现注释所述），该路径本来就不缓存。
- 临时表路径同理：statement 构建点取 state。
- 风险检查：命中路径是否存在对 start-time 敏感的执行？——命中计划非时间依赖（spec 排除），执行读 `task_ctx`；`state()` 的唯一其他副作用是读锁获取。若实现中发现命中路径某分支仍引用 state（如 schema 变更探测），该分支本就属于 miss 侧。

**D3 #27：内容哈希键控的 schema 缓存（精确匹配，唯一零健壮性风险的方案）。**
- `try_to_arrow` 层缓存：键 = `blake3/xxhash(内容) ++ fields_to_include 投影集`，值 = 推断出的 Arc<Schema>；命中直接 `decode_with_schema`（跳过推断一遍）。缓存有界（LRU ~128 条目），per-process。
- 健壮性论证：同字节 ⇒ 同 schema（确定性推断），命中不可能错。哈希碰撞（len+hash 相同而内容不同）概率天文级，后果仍是"按错误 schema 解码"而非 UB——接受并在代码注释声明。
- 收益画像诚实声明：精确重复 payload（模板化 IoT/心跳/回放）全命中；逐条变化负载零命中零回归。锚点 `json-decode`/`w200` 的负载是固定模板构造——恰好测命中路径的上界。
- 拒绝方案：①单调累积推断器——arrow-json 按固定 schema 解码时对新增字段**静默丢弃**，漂移检测无解，不健全；②键扫描守卫（仅扫字段名）——类型加宽（int→float）逃逸守卫，arrow-json 宽化行为未核实，不健全；③Value 树单遍（`values_to_arrow`）——serde Value 解析 ~2× 慢于裸解析，1 次 ≈ 2 次裸解析，与现状打平（当前双遍正是为此改的流式推断）。

**D4 验收协议：逐项锚点对比。** 三个修复项各自对应锚点场景；实现完跑 `cargo run -p arkflow --release --example benchmark -- --json`（本机 libduckdb release 构建受限时以 dev profile + CI benchmark.yml 为准），与锚点 commit 报告 diff。预期：#25 使 n1/n100/n1k 曲线斜率显著下降（消除 clone+scan 两个 O(状态) 项；必要 O(dirty) staging 保留）；#26 使 sql-hot-cache 每批耗时下降；#27 使 json-decode/w200 上界接近单遍解码。

## Risks / Trade-offs

- [D1 置位点遗漏某移除路径 → stale 条目滞留后端] → 三个置位点枚举 + 钉扎测试（fire 删除后下一次 persist 必须删后端条目）；`load_from_backend` 置位作安全网。
- [D1 条件扫描使"驱逐清理"从每批变懒] → 驱逐置位标志，下一次持久化即清理——语义等价（此前也是"下一次 persist"时点）。
- [D2 时间依赖查询误入缓存路径 → 折叠时间戳陈旧] → 既有 `cacheable` 门禁保证其不在缓存；钉扎：时间依赖查询输出仍逐批新鲜（既有测试覆盖）。
- [D3 缓存内存增长] → LRU 有界（128）；schema 条目本身很小。
- [三处修复的收益被锚点中其他成本淹没] → **实测证实（1.4）**：window-journal 三档前后无可见差异——fire 逐键提交主导全档位，且静态模板使 dirty=live，clone+scan 占比小。渐近修复的正确性由扫描计数钉扎直接证明（"SHALL NOT grow" 的可观测面是扫描次数恒定），锚点保留作回归守护；生产 sparse-update 形态的收益验证留给 in-core `#[ignore]` 计时测试（可选后续，不在本 change）。

## Migration Plan

无状态迁移（schema 缓存为进程内存态；persist 行为对持久化数据格式零变更）。回滚 = revert 单 PR。

## Open Questions

无——D2 的"命中路径是否引用 state"在实现中顺带核实（见 Risk 行）。
