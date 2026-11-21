## 1. #25 窗口持久化增量化

- [x] 1.1 `executor/window/operator.rs` `persist_buffers_for_fired` 去整表克隆：dirty 键经 `buffers.get(key)` 定向取值完成 put_compact
- [x] 1.2 新增 `stale_scan_pending` 标志：`fire_ready` 缓冲移除、`max_buffered_keys` 驱逐、`load_from_backend` 恢复三处置位；stale 扫描仅在标志置位时执行并在扫描后清除
- [x] 1.3 钉扎测试：稳态批（无移除）跳过后端扫描；fire 删除后下一次 persist 删除对应后端条目（重启不复活）；恢复后首次 persist 安全网（CountingBackend 计数扫描次数）
- [x] 1.4 锚点对比（实测结论）：count=20000 dev profile 前后 n1 1480→1466ms / n100 5928→6052ms / n1k 47861→48791ms——**噪声范围内无可见差异**。原因：①fire 逐键提交主导全部档位（n1k ~45s 中 during-run 仅数秒）；②generate 静态模板使每批 dirty=live，clone+scan 相对必要 staging 占比小。修复的渐近收益（live≫dirty 的 sparse-update 生产形态、后端条目大时）在锚点形状下不可见；"SHALL NOT grow" 契约由扫描计数钉扎测试直接证明（稳态批扫描次数恒定，不随后端条目增长）。锚点此后仍可用于守护回归。

## 2. #26 state 克隆移出缓存命中路径

- [x] 2.1 `processor/sql.rs` `execute_with_cached_plan`：`ctx.state()` 仅在缓存 miss 与时间依赖分支执行，命中路径直接 `collect(physical, ctx.task_ctx())`
- [x] 2.2 临时表路径 `execute_query_with_statement`：核实其 `state()` 是规划必需（逐批注册临时表后重建逻辑计划需新鲜注册表），保持现状并注明——设计 D2 的"同理"即指此定论
- [x] 2.3 钉扎测试：缓存命中路径输出与逐批克隆行为一致（18 个 sql 测试全过，含 plan-cache 语义测试）；时间依赖查询折叠时间戳仍逐批新鲜（:1378 now() 递进测试）
- [x] 2.4 锚点对比（count=20000 dev）：sql-hot-cache 105→92ms（单次）/ 105→94ms（best-of-3，-10.5% 稳健复现）——缓存命中路径的深克隆消除生效；幅度与 SessionState 克隆在总成本中的占比一致

## 3. #27 schema 内容哈希缓存

- [x] 3.1 `component/json.rs` `try_to_arrow` 增加 schema 缓存：键 = 内容哈希（std DefaultHasher，进程内确定性）+ `fields_to_include` 投影集（域分隔），值 = 推断 schema；FIFO 有界（128）；命中跳过推断直接 `decode_with_schema`——零新依赖
- [x] 3.2 测试：同内容命中与推断路径逐列一致；不同内容各自推断（新增字段出现、无串扰）；投影集参与键区分（full/projected 并存）
- [x] 3.3 锚点对比（dev，同机同参）：json-decode 349→126ms@5000（-64%；20000 档 501ms）；json-decode-w200 3688→1914ms@5000（-48%）/ 15381→6126ms@20000（-60%，best-of-3）——宽 schema 推断占比最高，收益最大；变量负载零回归（miss 路径不变）

## 4. 收尾验证

- [x] 4.1 `cargo check --workspace --all-targets` 通过、clippy 零告警（修掉一处 bool assert 告警后复跑零告警）、arkflow-core 1174 passed / arkflow-plugin 900 passed（remote 子系统 1 个计时 flake 单独复跑两次均过，与本 change 无关）
- [x] 4.2 `openspec validate --all --strict` 通过
