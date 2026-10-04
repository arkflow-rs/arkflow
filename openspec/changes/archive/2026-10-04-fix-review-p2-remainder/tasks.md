# Tasks: fix-review-p2-remainder

## 1. 内核 — window 缓冲上限（columnar-window-operators）

- [x] 1.1 `WindowOperatorConfig` 增加 `max_buffered_keys`（serde default 65536），validate 拒绝 0
- [x] 1.2 缓冲插入公共路径实现条目上限驱逐（最老 window-start 优先）+ 节流告警（驱逐量/深度/上限），tumbling/sliding/session 三族共用
- [x] 1.3 单测：超限驱逐顺序、告警节流、上限内零行为变化、驱逐后窗口不发射

## 2. 内核 — 并行度显式拒绝 + barrier 取消守卫 + 专用 variant（unified-execution-kernel）

- [x] 2.1 `graph.rs`：join/stateful 链上用户显式配置 `__arkflow_processor_parallelism > 1` 时构建期报错（指名原因与修正指引）；未配置默认 1 不变
- [x] 2.2 `task.rs` barrier 分支：`current_positions`/`snapshot_state`/`send_downstream` 三个 await 包 cancellation select，取消走既有 round 失败路径
- [x] 2.3 新增 `Error::InputChannelClosed` variant；替换 `task.rs` 铸造点与字符串匹配点；全仓 grep 确认无其他 `"input channel closed"` 匹配
- [x] 2.4 单测：显式并行度拒绝、barrier 取消不悬挂（cancellation token 触发下分支退出）、variant 匹配

## 3. 网络 shuffle — remote.rs（network-shuffle-data-plane）

- [x] 3.1 `mark_held`/`release_held` 溢出路径改 `try_send` + `tracing::error!`（含 quad），删除阻塞 send
- [x] 3.2 pending replay 字节记账：register 累加 / 完成扣减 `MessageBatchRef` 内存估算；`NetworkManagerConfig` 增加 `max_pending_bytes`（默认 256 MiB），超预算拒绝注册走既有失败路径
- [x] 3.3 receipt 读循环空闲分级：`receipt_wait_timeout`（默认 10 分钟，新配置），pending 非空用它、空则维持 `read_idle_timeout`
- [x] 3.4 单测：溢出非阻塞、字节预算拒绝（条数未满但字节满）、pending 非空时不拆慢连接、空 pending 30s 语义不变

## 4. 控制面 — server（control-plane-service / control-plane-identity / hub-oidc-auth）

- [x] 4.1 Storage actor 命令执行包 `spawn_blocking`（`Handle::current().block_on(dispatch)`），FIFO 不变；队列深度 gauge + 派发 tracing（操作名/时长）
- [x] 4.2 维护循环 `lib.rs` 13 处 `let _ =` 改显式日志（任务名 + 错误）
- [x] 4.3 placement plan 编译缓存：(job_id, spec hash) 键、容量 1024 超限 clear；单测覆盖命中/失效/容量
- [x] 4.4 `parse_operator_credential` 返回 Option；含 `|` 解析失败 → None；启动校验拒绝启动（脱敏列出）；鉴权路径 None 视为不可匹配；纯静态 token（无 `|`）保持 Admin 模式；单测覆盖四种畸形 + 纯 token
- [x] 4.5 OIDC JWKS 周期刷新任务（可配置间隔默认 1h；失败保留旧表 + warn）；单测：刷新后旧 kid 吊销、失败保留、未配置零行为
- [x] 4.6 server 测试回归（`cargo test -p arkflow-server`）

## 5. 控制面 — core（configuration-management）

- [x] 5.1 `control_plane.rs` apply/rollback：执行失败时 best-effort 补偿删除刚保存版本；`ConfigVersionStore` 增加最小 `delete_version`
- [x] 5.2 版本历史目录读 `ARKFLOW_CONFIG_HISTORY_DIR` env（默认不变）；单测：失败不留坏版本、目录覆盖生效

## 6. console（control-console + 既有 console-i18n 缺口）

- [x] 6.1 `api.ts` request() 增加 30s AbortSignal 超时（与外部 signal 合并），超时映射可读错误
- [x] 6.2 SSE 重连指数退避 1s→30s 上限、成功重置
- [x] 6.3 diff 基线取直接前驱（按序列降序）；无前驱禁用 diff
- [x] 6.4 i18n：jobs 状态 4 选项 + job-dag 6 条连线校验文案 + job-editor issues 渲染走 t()，en/zh 补词条
- [x] 6.5 console 构建 + 既有测试通过（`pnpm build` / `pnpm test` 按目录脚本）

## 7. 杂项 — core（component-registry-export / Windows）

- [x] 7.1 `engine/mod.rs` 信号处理 `cfg(unix)`/`cfg(windows)` 门控（unix 路径零变化）
- [x] 7.2 health_check schema 补 9 字段（hub_url 标 deprecated、observability 子对象展开）；`thread_num` 去掉 `default: 1` 改描述
- [x] 7.3 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 重新生成 config-schema.json；`--validate` 用含新字段的配置验证通过

## 8. 收尾验证

- [x] 8.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新增告警
- [x] 8.2 `pnpm docs:check` 通过（含 zh-Hans 树）；如本 change 语义涉及文档页（window `max_buffered_keys`、remote 新配置、OIDC 刷新间隔、`ARKFLOW_CONFIG_HISTORY_DIR`），补 en/zh 文档任务
- [x] 8.3 文档：`docs/docs/` 与 zh-Hans 对应页补新配置字段说明（window 上限、network shuffle 预算/超时、OIDC 刷新、环境变量）

> 验证备注（2026-10-03）：`cargo test --workspace --all-targets` 除 `kafka_eos` 的 5 个 testcontainers 测试外全部通过——该 5 个测试需要真实 Kafka broker，在本机干净树（stash 全部改动）上同样失败（"Kafka broker not ready within 90s"），属环境问题、与本 change 无关；clippy 零告警、`pnpm docs:check` 通过、console `pnpm test` 61/61、`--validate` 对含 health_check 新字段的配置通过。
